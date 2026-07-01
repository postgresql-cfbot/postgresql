# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that "recovery still waiting" is logged after deadlock_timeout
# during a buffer pin recovery conflict, rather than only at
# max_standby_streaming_delay.
#
# This guards against a regression where the startup process waits the
# full max_standby_streaming_delay before returning to the caller, which
# delays the conflict log emitted from LockBufferForCleanup().
#
# Reported by Ilmar Yunusov on the v2 thread of "Standby deadlock check
# repeats signal every deadlock_timeout".

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);

# Ilmar's reported environment: short deadlock_timeout, long streaming
# delay, conflict-wait logging enabled.
$node_primary->append_conf(
	'postgresql.conf', qq[
log_recovery_conflict_waits = on
deadlock_timeout = 100ms
max_standby_streaming_delay = 5s
]);
$node_primary->start;

my $backup_name = 'bp_backup';
$node_primary->backup($backup_name);

my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->start;

my $test_db = 'bp_db';
$node_primary->safe_psql('postgres', "CREATE DATABASE $test_db");

my $table = 'bp_table';
$node_primary->safe_psql(
	$test_db, qq[
CREATE TABLE $table (a int, b int);
INSERT INTO $table SELECT 0, 0 FROM generate_series(1, 20);
]);

# Create dead tuples that VACUUM FREEZE will prune; this is what
# triggers the buffer pin recovery conflict on the standby.
$node_primary->safe_psql(
	$test_db, qq[
BEGIN;
INSERT INTO $table VALUES (1, 0);
ROLLBACK;
BEGIN; LOCK $table; COMMIT;
]);
$node_primary->wait_for_replay_catchup($node_standby);

my $psql_standby =
  $node_standby->background_psql($test_db, on_error_stop => 0);

my $cursor = 'bp_cursor';
my $res = $psql_standby->query_safe(
	qq[
BEGIN;
DECLARE $cursor CURSOR FOR SELECT b FROM $table;
FETCH FORWARD FROM $cursor;
]);
like($res, qr/^0$/m, 'standby cursor pins buffer');

my $log_location = -s $node_standby->logfile;

# Trigger the conflict.
$node_primary->safe_psql($test_db, "VACUUM FREEZE $table;");

# Wait for the "still waiting" log. Bound the wait below
# max_standby_streaming_delay (5s) so a regression where the log only
# appears at the streaming-delay boundary fails fast.
my $deadline = time() + 7;
my $found = 0;
my $elapsed_ms;
while (time() < $deadline)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($node_standby->logfile,
		$log_location);
	if ($log =~ /recovery still waiting after (\d+)\.(\d+) ms: .*buffer pin/)
	{
		$elapsed_ms = $1 + 0;
		$found = 1;
		last;
	}
	usleep(50_000);    # 50ms poll
}

ok($found,
	"buffer pin conflict 'still waiting' log appears before "
	  . "max_standby_streaming_delay");

if ($found)
{
	# The log should appear close to deadlock_timeout (100ms), not
	# anywhere near max_standby_streaming_delay (5000ms).  The v2
	# regression manifests as the log appearing at approximately 5001ms.
	# Use a 4000ms upper bound: tight enough to catch the regression
	# (which fires at 5001ms) but loose enough to absorb scheduling
	# noise on slow buildfarm animals (valgrind, CLOBBER_CACHE_ALWAYS,
	# ASan, etc.) where the typical-case latency of ~100ms can grow
	# 10x.
	cmp_ok($elapsed_ms, '<', 4000,
		"'still waiting' elapsed ($elapsed_ms ms) is near "
		  . "deadlock_timeout, not max_standby_streaming_delay");
}

$psql_standby->reconnect_and_clear();

# The standby session was terminated by the conflict cancel.  Wait for
# replay to catch up before stopping the cluster, mirroring the pattern
# in t/031_recovery_conflict.pl; without this, shutdown can race the
# FATAL message and leave the test output flaky on slow buildfarm
# animals.
$node_primary->wait_for_replay_catchup($node_standby);

$node_standby->stop;
$node_primary->stop;

done_testing();
