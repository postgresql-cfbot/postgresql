# Checks waiting for the lsn replay on standby using
# pg_wait_for_wal_replay_lsn() procedure.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# And some content and take a backup
$node_primary->safe_psql('postgres',
	"CREATE TABLE wait_test AS SELECT generate_series(1,10) AS a");
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Create a streaming standby with a 1 second delay from the backup
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
my $delay = 3;
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf(
	'postgresql.conf', qq[
	recovery_min_apply_delay = '${delay}s'
]);
$node_standby->start;


# Make sure that pg_wait_for_wal_replay_lsn() works: add new content to
# primary and memorize primary's insert LSN, then wait for that LSN to be
# replayed on standby.
$node_primary->safe_psql('postgres',
	"INSERT INTO wait_test VALUES (generate_series(11, 20))");
my $lsn1 =
  $node_primary->safe_psql('postgres', "SELECT pg_current_wal_insert_lsn()");
my $output = $node_standby->safe_psql(
	'postgres', qq[
	CALL pg_wait_for_wal_replay_lsn('${lsn1}', 1000);
	SELECT pg_lsn_cmp(pg_last_wal_replay_lsn(), '${lsn1}'::pg_lsn);
]);

# Make sure the current LSN on standby is at least as big as the LSN we
# observed on primary's before.
ok( $output >= 0,
	"standby reached the same LSN as primary after pg_wait_for_wal_replay_lsn()"
);

# Check that waiting for unreachable LSN triggers the timeout.
my $lsn2 =
  $node_primary->safe_psql('postgres',
	"SELECT pg_current_wal_insert_lsn() + 1");
my $stderr;
$node_standby->safe_psql('postgres',
	"CALL pg_wait_for_wal_replay_lsn('${lsn1}', 0.01);");
$node_standby->psql(
	'postgres',
	"CALL pg_wait_for_wal_replay_lsn('${lsn2}', 1);",
	stderr => \$stderr);
ok( $stderr =~ /canceling waiting for LSN due to timeout/,
	"get timeout on waiting for unreachable LSN");

# Check that new data is visible after calling pg_wait_for_wal_replay_lsn()
$node_primary->safe_psql('postgres',
	"INSERT INTO wait_test VALUES (generate_series(21, 30))");
my $lsn3 =
  $node_primary->safe_psql('postgres', "SELECT pg_current_wal_insert_lsn()");
$output = $node_standby->safe_psql(
	'postgres', qq[
	CALL pg_wait_for_wal_replay_lsn('${lsn3}');
	SELECT count(*) FROM wait_test;
]);

# Make sure the current LSN on standby and is the same as primary's LSN
ok($output eq 30, "standby reached the same LSN as primary");

$node_standby->stop;
$node_primary->stop;
done_testing();
