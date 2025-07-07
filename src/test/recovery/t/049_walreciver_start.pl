# Copyright (c) 2021, PostgreSQL Global Development Group

# Checks for wal_receiver_start_at = 'consistency'
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use File::Copy;
use Test::More tests => 2;

# Initialize primary node and start it.
my $node_primary = PostgreSQL::Test::Cluster->new('test');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# Initial workload.
$node_primary->safe_psql(
	'postgres', qq {
CREATE TABLE test_walreceiver_start(i int);
SELECT pg_switch_wal();
});

# Take backup.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Run a post-backup workload, whose WAL we will manually copy over to the
# standby before it starts.
my $wal_file_to_copy = $node_primary->safe_psql('postgres',
	"SELECT pg_walfile_name(pg_current_wal_lsn());");
$node_primary->safe_psql(
	'postgres', qq {
INSERT INTO test_walreceiver_start VALUES(1);
SELECT pg_switch_wal();
});

# Initialize standby node from the backup and copy over the post-backup WAL.
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
copy($node_primary->data_dir . '/pg_wal/' . $wal_file_to_copy,
	$node_standby->data_dir . '/pg_wal')
  or die "Copy failed: $!";

# Set up a long delay to prevent the standby from replaying past the first
# commit outside the backup.
$node_standby->append_conf('postgresql.conf',
	"recovery_min_apply_delay = '2h'");
# Set up the walreceiver to start as soon as consistency is reached.
$node_standby->append_conf('postgresql.conf',
	"wal_receiver_start_at = 'consistency'");

$node_standby->start();

# The standby should have reached consistency and should be blocked waiting for
# recovery_min_apply_delay.
$node_standby->poll_query_until(
	'postgres', qq{
SELECT wait_event = 'RecoveryApplyDelay' FROM pg_stat_activity
WHERE backend_type='startup';
}) or die "Timed out checking if startup is in recovery_min_apply_delay";

# The walreceiver should have started, streaming from the end of valid locally
# available WAL, i.e from the WAL file that was copied over.
$node_standby->poll_query_until('postgres',
	"SELECT COUNT(1) = 1 FROM pg_stat_wal_receiver;")
  or die "Timed out while waiting for streaming to start";
my $receive_start_lsn = $node_standby->safe_psql('postgres',
	'SELECT receive_start_lsn FROM pg_stat_wal_receiver');
is( $node_primary->safe_psql(
		'postgres', "SELECT pg_walfile_name('$receive_start_lsn');"),
	$wal_file_to_copy,
	"walreceiver started from end of valid locally available WAL");

# Now run a workload which should get streamed over.
$node_primary->safe_psql(
	'postgres', qq {
SELECT pg_switch_wal();
INSERT INTO test_walreceiver_start VALUES(2);
});

# The walreceiver should be caught up, including all WAL generated post backup.
$node_primary->wait_for_catchup('standby', 'flush');

# Now clear the delay so that the standby can replay the received WAL.
$node_standby->safe_psql('postgres',
	'ALTER SYSTEM SET recovery_min_apply_delay TO 0;');
$node_standby->reload;

# Now the replay should catch up.
$node_primary->wait_for_catchup('standby', 'replay');
is( $node_standby->safe_psql(
		'postgres', 'SELECT count(*) FROM test_walreceiver_start;'),
	2,
	"querying test_walreceiver_start now should return 2 rows");
