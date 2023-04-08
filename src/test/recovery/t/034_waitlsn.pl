# Checks wait lsn
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

# Using the backup, create a streaming standby with a 1 second delay
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
my $delay        = 1;
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf', qq[
	recovery_min_apply_delay = '${delay}s'
]);
$node_standby->start;

# Check that timeouts make us wait for the specified time (1s here)
my $current_time = $node_standby->safe_psql('postgres', "SELECT now()");
my $two_seconds = 2000; # in milliseconds
my $start_time = time();
$node_standby->safe_psql('postgres',
	"SELECT pg_wait_lsn('0/FFFFFFFF', $two_seconds)");
my $time_waited = (time() - $start_time) * 1000; # convert to milliseconds
ok($time_waited >= $two_seconds, "wait lsn waits for enough time");

# Check that timeouts let us stop waiting right away, before reaching target LSN
# Wait for no wait
$node_primary->safe_psql('postgres',
	"INSERT INTO wait_test VALUES (generate_series(11, 20))");
my $lsn1 = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
my ($ret, $out, $err) = $node_standby->psql('postgres',
	"SELECT pg_wait_lsn('$lsn1', 1)");

ok($ret == 0, "zero return value when failed to wait lsn on standby");
ok($err =~ /WARNING:  LSN was not reached/,
	"correct error message when failed to wait lsn on standby");
ok($out eq "f", "if given too little wait time, WAIT doesn't reach target LSN");


# Check that wait lsn works fine and reaches target LSN if given no timeout
# Wait for infinite

# Add data on primary, memorize primary's last LSN
$node_primary->safe_psql('postgres',
	"INSERT INTO wait_test VALUES (generate_series(21, 30))");
my $lsn2 = $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");

# Wait for it to appear on replica, memorize replica's last LSN
$node_standby->safe_psql('postgres',
	"SELECT pg_wait_lsn('$lsn2', 0)");
my $reached_lsn = $node_standby->safe_psql('postgres',
	"SELECT pg_last_wal_replay_lsn()");

# Make sure that primary's and replica's LSNs are the same after WAIT
my $compare_lsns = $node_standby->safe_psql('postgres',
	"SELECT pg_lsn_cmp('$reached_lsn'::pg_lsn, '$lsn2'::pg_lsn)");
ok($compare_lsns eq 0,
	"standby reached the same LSN as primary before starting transaction");

$node_standby->stop;
$node_primary->stop;

done_testing();
