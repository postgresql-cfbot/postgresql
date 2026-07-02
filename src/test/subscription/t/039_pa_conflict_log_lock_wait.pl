# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that a parallel apply (PA) worker correctly inserts a deferred
# conflict-log tuple even when, by the time it reaches
# ProcessPendingConflictLogTuple(), the conflict log table is held under
# ACCESS EXCLUSIVE by another session.
#
# The window we want to exercise is narrow: PA must already be past
# ReportApplyConflict() (so the error has fired and PA is in PG_CATCH),
# and the locker must take the CLT lock *before* PA reaches the second
# CLT open inside ProcessPendingConflictLogTuple().  An injection point
# pauses PA at exactly that point so the locker can grab the lock first;
# without it, the locker either takes the lock too early (PA blocks in
# the inline CLT open inside ReportApplyConflict, before the error has
# fired) or too late (PA inserts before the lock is taken).

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# ---------------------------------------------------------------------
# Set up publisher and subscriber.  Force every transaction to stream so
# the conflict is handled by a parallel apply worker rather than the
# leader.
# ---------------------------------------------------------------------
my $node_pub = PostgreSQL::Test::Cluster->new('publisher');
$node_pub->init(allows_streaming => 'logical');
$node_pub->append_conf('postgresql.conf', q{
debug_logical_replication_streaming = immediate
logical_decoding_work_mem = 64kB
});
$node_pub->start;

my $node_sub = PostgreSQL::Test::Cluster->new('subscriber');
$node_sub->init;
$node_sub->append_conf('postgresql.conf', q{
shared_preload_libraries = 'injection_points'
max_logical_replication_workers = 4
max_parallel_apply_workers_per_subscription = 2
});
$node_sub->start;

# Replicated table; the pre-existing row on the subscriber is what makes
# the publisher's INSERT (id=1) trigger an INSERT_EXISTS conflict.
$node_pub->safe_psql('postgres', q{
	CREATE TABLE t (id int PRIMARY KEY, val text);
	ALTER TABLE t REPLICA IDENTITY FULL;
	CREATE PUBLICATION p FOR TABLE t;
});

$node_sub->safe_psql('postgres', q{
	CREATE TABLE t (id int PRIMARY KEY, val text);
	INSERT INTO t VALUES (1, 'pre-existing');
	CREATE EXTENSION injection_points;
});

my $pub_connstr = $node_pub->connstr . ' dbname=postgres';
$node_sub->safe_psql('postgres', qq{
	CREATE SUBSCRIPTION s
	CONNECTION '$pub_connstr'
	PUBLICATION p
	WITH (streaming = parallel,
	      conflict_log_destination = 'all',
	      disable_on_error = true);
});

$node_sub->wait_for_subscription_sync($node_pub, 's');

# ---------------------------------------------------------------------
# Send a non-conflicting INSERT and then wait until pg_subscription_rel
# reaches 'r' (ready) on every relation.  pa_can_start() requires
# AllTablesyncsReady(), which returns true only when every
# pg_subscription_rel row is 'r'.  The 's' (syncdone) -> 'r' transition
# fires inside ProcessSyncingTablesForApply, which only flips the state
# when the apply worker's last_received LSN has advanced past the
# tablesync end LSN -- so we need a triggering commit on the publisher
# to drive last_received forward.  Without this step, the conflict txn
# below would arrive while the table is still 's', pa_can_start() would
# return false, the leader would spool to file and apply serially, and
# no parallel apply worker would ever spawn.
# ---------------------------------------------------------------------
$node_pub->safe_psql('postgres', "INSERT INTO t VALUES (1000, 'warmup');");
$node_sub->poll_query_until('postgres',
	"SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');"
) or die "subscription tables did not reach READY state";

# ---------------------------------------------------------------------
# Look up the per-subscription CLT name.
# ---------------------------------------------------------------------
my $sub_oid = $node_sub->safe_psql('postgres',
	"SELECT oid FROM pg_subscription WHERE subname = 's'");
my $clt = "pg_conflict.pg_conflict_log_$sub_oid";
note "conflict log table for subscription s: $clt";

# ---------------------------------------------------------------------
# Arm the injection point.  This pauses the PA worker inside
# ProcessPendingConflictLogTuple() — i.e. *after* the error has fired
# and the PG_CATCH has run, *before* the second open of the CLT.  This
# is the exact window the deferred-insert path needs to be tested in.
# ---------------------------------------------------------------------
$node_sub->safe_psql('postgres',
	"SELECT injection_points_attach('clt-pending-flush-before-open', 'wait');");

# ---------------------------------------------------------------------
# Drive the conflict.  PA receives the streamed txn, hits INSERT_EXISTS
# inside ReportApplyConflict (which opens/closes the CLT cleanly while
# preparing the deferred tuple), then ereport(ERROR) fires, PG_CATCH
# runs, and PA enters ProcessPendingConflictLogTuple — where it pauses
# at the injection point.
# ---------------------------------------------------------------------
my $log_offset = -s $node_sub->logfile;

$node_pub->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t SELECT g, repeat('x', 1000) FROM generate_series(2, 200) g;
	INSERT INTO t VALUES (1, 'conflict');
	COMMIT;
});

# Wait until PA is parked at the injection point.
$node_sub->wait_for_event('logical replication parallel worker',
	'clt-pending-flush-before-open');

# ---------------------------------------------------------------------
# Now take ACCESS EXCLUSIVE on the CLT.  TRUNCATE is permitted on CLTs;
# At this point the CLT is empty, so the TRUNCATE is effectively a no-op
# that just acquires the lock.
# Because PA is paused at the injection point, this lock is guaranteed
# to be acquired *before* PA tries to open the CLT.
# ---------------------------------------------------------------------
my $locker = $node_sub->background_psql('postgres');
$locker->query_safe(qq{
	BEGIN;
	TRUNCATE $clt;
});

# ---------------------------------------------------------------------
# Wake the PA from the injection point.  It will now try to open the
# CLT inside ProcessPendingConflictLogTuple and block on the lock the
# locker session holds.
# ---------------------------------------------------------------------
$node_sub->safe_psql('postgres',
	"SELECT injection_points_wakeup('clt-pending-flush-before-open');
	 SELECT injection_points_detach('clt-pending-flush-before-open');");

# Confirm the PA worker is actually parked waiting on the CLT lock —
# this verifies we are exercising the deferred-insert lock-wait path,
# not racing past it.
my $clt_oid = $node_sub->safe_psql('postgres',
	"SELECT '$clt'::regclass::oid");
ok( $node_sub->poll_query_until(
		'postgres', qq{
		SELECT EXISTS (
			SELECT 1
			FROM pg_locks l
			JOIN pg_stat_activity a ON l.pid = a.pid
			WHERE NOT l.granted
			  AND l.relation = $clt_oid
			  AND a.backend_type = 'logical replication parallel worker'
		);
	}, 't'),
	'PA worker is blocked on the CLT lock inside ProcessPendingConflictLogTuple');

# ---------------------------------------------------------------------
# Release the lock.  PA wakes, inserts the deferred row, commits its
# CLT txn, re-throws the original error to the leader, and the leader
# disables the subscription (disable_on_error = true).
# ---------------------------------------------------------------------
$locker->query_safe('COMMIT;');
ok($locker->quit, 'locker session closed cleanly');

ok( $node_sub->poll_query_until(
		'postgres',
		"SELECT subenabled = false FROM pg_subscription WHERE subname = 's'",
		't'),
	'subscription disabled after the conflict');

# ---------------------------------------------------------------------
# Verify the deferred conflict log tuple survived the lock wait.
# ---------------------------------------------------------------------
my $rows = $node_sub->safe_psql('postgres',
	"SELECT count(*) FROM $clt WHERE conflict_type = 'insert_exists'");
is($rows, '1',
	'deferred CLT insert by PA worker succeeded after lock release');

# ---------------------------------------------------------------------
# Also verify the conflict was reported in the server log
# (conflict_log_destination = 'all' logs to both the table and the log).
# ---------------------------------------------------------------------
my $log_contents = slurp_file($node_sub->logfile, $log_offset);
like(
	$log_contents,
	qr/ERROR:\s+conflict detected on relation "public\.t": conflict=insert_exists/,
	'conflict reported in server log');

done_testing();
