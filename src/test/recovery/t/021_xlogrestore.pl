#
# Test for xlogrestore with max_restore_command_workers parameter
#

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

sub measure_replica_restore_time
{
	my ( $replica_name, $node_primary, $backup_name, $last_lsn, $tab_int_count, $config ) = @_;
	my $timer = time();

	# Initialize replica node from backup, fetching WAL from archives
	my $node_replica = get_new_node( $replica_name );
	$node_replica->init_from_backup( $node_primary, $backup_name,
		has_restoring => 1 );
	$node_replica->append_conf( 'postgresql.conf', $config );
	$node_replica->start();

	# Wait until necessary replay has been done on replica
	my $caughtup_query =
	  "SELECT '$last_lsn'::pg_lsn <= pg_last_wal_replay_lsn()";
	$node_replica->poll_query_until( 'postgres', $caughtup_query )
	  or die "Timed out while waiting for replica to catch up";

	# Check tab_int's rows count
	my $replica_tab_int_count =
	  $node_replica->safe_psql( 'postgres', "SELECT count(*) FROM tab_int" );
	is( $replica_tab_int_count, $tab_int_count, 'tab_int sizes are equal' );

	# Check the presence of temporary files specifically generated during
	# archive recovery.
	$node_replica->promote();

	my $node_replica_data = $node_replica->data_dir;
	ok( !-f "$node_replica_data/pg_wal/RECOVERYHISTORY",
		"RECOVERYHISTORY removed after promotion");
	ok( !-f "$node_replica_data/pg_wal/RECOVERYXLOG",
			"RECOVERYXLOG removed after promotion");
	ok( !-d "$node_replica_data/pg_wal/prefetch",
		"pg_wal/prefetch dir removed after promotion");

	my $res = time() - $timer;

	$node_replica->stop();
	return $res;
}

# WAL produced count
my $wal_count = 64;

# Size of data portion
my $wal_data_portion = 128;

# Restore bgworkers count
my $max_restore_command_workers = 4;

# Sleep to imitate restore delays
my $restore_sleep = 0.256;

# Minimum expected acceleration of the restore process.
# Is this formula correct?
my $handicap = ( $wal_count * $restore_sleep ) / $max_restore_command_workers;

# Initialize primary node, doing archives
my $node_primary = get_new_node( 'primary' );
$node_primary->init(
	has_archiving    => 1,
	allows_streaming => 1
);

# Start it
$node_primary->start;

# Take backup for replica.
my $backup_name = 'my_backup';
$node_primary->backup( $backup_name );

# Create some content on primary server that will be not present on replicas.
for ( my $i = 0; $i < $wal_count; $i++ )
{
	if ( $i == 0 ) {
		$node_primary->safe_psql('postgres',
			"CREATE TABLE tab_int ( a SERIAL NOT NULL PRIMARY KEY );")
	} else {
		$node_primary->safe_psql('postgres',
			"INSERT INTO tab_int SELECT FROM generate_series( 1, $wal_data_portion );")
	}
	$node_primary->safe_psql('postgres', "SELECT pg_switch_wal()");
}

my $last_lsn = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn();");
my $tab_int_count = $node_primary->safe_psql('postgres', "SELECT count(*) FROM tab_int;");

$node_primary->stop();

#	Restore command
my $restore_command;
my $path = TestLib::perl2host( $node_primary->archive_dir );
if ( $TestLib::windows_os ) {
	$path =~ s{\\}{\\\\}g;
	$restore_command = qq(perl -e "select( undef, undef, undef, $restore_sleep );" & copy "$path\\\\%f" "%p);
} else {
	$restore_command = qq(sleep ) . $restore_sleep . qq( && cp "$path/%f" "%p");
}

#diag( $restore_command );

# Compare the replica restore times with different max_restore_command_workers values.
#diag( 'multiple_workers_restore_time' );
my $multiple_workers_restore_time = measure_replica_restore_time(
	'fast_restored_replica',
	$node_primary,
	$backup_name,
	$last_lsn,
	$tab_int_count,
qq(
wal_retrieve_retry_interval = '100ms'
max_restore_command_workers = $max_restore_command_workers
restore_command = '$restore_command'
));

#diag( 'single_worker_restore_time' );
my $single_worker_restore_time = measure_replica_restore_time(
	'normal_restored_replica',
	$node_primary,
	$backup_name,
	$last_lsn,
	$tab_int_count,
qq(
wal_retrieve_retry_interval = '100ms'
max_restore_command_workers = 0
restore_command = '$restore_command'
));

#diag( $multiple_workers_restore_time );
#diag( $single_worker_restore_time );
#diag( $handicap );

ok( $multiple_workers_restore_time + $handicap < $single_worker_restore_time, "Multiple workers are faster than a single worker" );
