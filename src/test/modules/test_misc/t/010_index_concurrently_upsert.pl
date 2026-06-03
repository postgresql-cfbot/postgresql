
# Copyright (c) 2026, PostgreSQL Global Development Group

# Test INSERT ON CONFLICT DO UPDATE behavior concurrent with
# CREATE INDEX CONCURRENTLY and REINDEX CONCURRENTLY.
#
# These tests verify the fix for "duplicate key value violates unique
# constraint" errors that occurred when infer_arbiter_indexes() only considered
# indisvalid indexes, causing different transactions to use different arbiter
# indexes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Session;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

plan skip_all => 'Injection points not supported by this build'
  unless $ENV{enable_injection_points} eq 'yes';

# Node initialization
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init();
$node->start();

# Check if the extension injection_points is available
plan skip_all => 'Extension injection_points not installed'
  unless $node->check_extension('injection_points');

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tblpk (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tblpk SET (parallel_workers=0);

CREATE TABLE test.tblparted(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
CREATE TABLE test.tbl_partition PARTITION OF test.tblparted
    FOR VALUES FROM (0) TO (10000)
    WITH (parallel_workers = 0);

CREATE UNLOGGED TABLE test.tblexpr(i int, updated_at timestamp);
CREATE UNIQUE INDEX tbl_pkey_special ON test.tblexpr(abs(i)) WHERE i < 1000;
ALTER TABLE test.tblexpr SET (parallel_workers=0);

]);

############################################################################
note('Test: REINDEX CONCURRENTLY + UPSERT (wakeup at set-dead phase)');

# Create sessions for concurrent operations
my $s1 = PostgreSQL::Test::Session->new(node => $node);
my $s2 = PostgreSQL::Test::Session->new(node => $node);
my $s3 = PostgreSQL::Test::Session->new(node => $node);

# Setup injection points for each session
$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

# s3 starts REINDEX (will block on reindex-relation-concurrently-before-set-dead)
$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

# Wait for s3 to hit injection point
ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

# s1 starts UPSERT (will block on check-exclusion-or-unique-constraint-no-conflict)
$s1->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

# Wait for s1 to hit injection point
ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

# Wakeup s3 to continue (reindex-relation-concurrently-before-set-dead)
wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');

# s2 starts UPSERT (will block on exec-insert-before-insert-speculative)
$s2->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

# Wait for s2 to hit injection point
ok_injection_point($node, 'exec-insert-before-insert-speculative');

# Wakeup s1 (check-exclusion-or-unique-constraint-no-conflict)
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

# Wakeup s2 (exec-insert-before-insert-speculative)
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

# Cleanup test 1
$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX CONCURRENTLY + UPSERT (wakeup at swap phase)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s1->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX CONCURRENTLY + UPSERT (s1 wakes before reindex)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s1->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

# Start s2 BEFORE waking reindex (key difference from permutation 1)
$s2->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX + UPSERT ON CONSTRAINT (set-dead phase)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s1->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');

$s2->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX + UPSERT ON CONSTRAINT (swap phase)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s1->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX + UPSERT ON CONSTRAINT (s1 wakes before reindex)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tblpk_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s1->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

# Start s2 BEFORE waking reindex
$s2->do_async(q[INSERT INTO test.tblpk VALUES (13, now()) ON CONFLICT ON CONSTRAINT tblpk_pkey DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblpk');

############################################################################
note('Test: REINDEX on partitioned table (set-dead phase)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s1->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');

$s2->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblparted');

############################################################################
note('Test: REINDEX on partitioned table (swap phase)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s1->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblparted');

############################################################################
note('Test: REINDEX on partitioned table (s1 wakes before reindex)');

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->do_async(q[REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s1->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

# Start s2 BEFORE waking reindex
$s2->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node,
	'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblparted');

############################################################################
note(
	'Test: REINDEX on partitioned table, cache inval between two get_partition_ancestors'
);

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-init-partition-after-get-partition-ancestors', 'wait');
]);

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s2->do_async(q[REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;]);

ok_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s1->do_async(q[INSERT INTO test.tblparted VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node,
	'exec-init-partition-after-get-partition-ancestors');

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

wakeup_injection_point($node,
	'exec-init-partition-after-get-partition-ancestors');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblparted');

############################################################################
note('Test: CREATE INDEX CONCURRENTLY + UPSERT');
# Uses invalidate-catalog-snapshot-end to test catalog invalidation
# during UPSERT

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

# Get the session's backend PID before attaching injection points
my $s1_pid = $s1->query_oneval('SELECT pg_backend_pid()');

# s1 attaches BOTH injection points - the unique constraint check AND catalog snapshot
$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

# In cases of cache clobbering, s1 may hit the injection point during attach.
# Start attach asynchronously so we can check if it blocks.
$s1->do_async(q[SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');]);

# Wait for that session to become idle (attach completed), or wake it up if
# it becomes stuck on injection point.
if (!wait_for_idle($node, $s1_pid))
{
	ok_injection_point(
		$node,
		'invalidate-catalog-snapshot-end',
		's1 hit injection point during attach (cache clobbering mode)');
	$node->safe_psql(
		'postgres', q[
		SELECT injection_points_wakeup('invalidate-catalog-snapshot-end');
	]);
}
# Wait for async command to complete
$s1->wait_for_completion;

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
]);

# s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
$s3->do_async(q[CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_duplicate ON test.tblpk(i);]);

ok_injection_point($node, 'define-index-before-set-valid');

# s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
$s1->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'invalidate-catalog-snapshot-end');

# Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
wakeup_injection_point($node, 'define-index-before-set-valid');

# s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
$s2->do_async(q[INSERT INTO test.tblpk VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'invalidate-catalog-snapshot-end');

ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblparted');

############################################################################
note('Test: CREATE INDEX CONCURRENTLY on partial index + UPSERT');
# Uses invalidate-catalog-snapshot-end to test catalog invalidation during UPSERT

$s1 = PostgreSQL::Test::Session->new(node => $node);
$s2 = PostgreSQL::Test::Session->new(node => $node);
$s3 = PostgreSQL::Test::Session->new(node => $node);

$s1_pid = $s1->query_oneval('SELECT pg_backend_pid()');

# s1 attaches BOTH injection points - the unique constraint check AND catalog snapshot
$s1->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s1->do(q[SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');]);

# In cases of cache clobbering, s1 may hit the injection point during attach.
# Wait for that session to become idle (attach completed), or wake it up if
# it becomes stuck on injection point.
if (!wait_for_idle($node, $s1_pid))
{
	ok_injection_point(
		$node,
		'invalidate-catalog-snapshot-end',
		's1 hit injection point during attach (cache clobbering mode)');
	$node->safe_psql(
		'postgres', q[
		SELECT injection_points_wakeup('invalidate-catalog-snapshot-end');
	]);
}

$s2->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->do(
	q[
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
]);

# s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
$s3->do_async(q[CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_special_duplicate ON test.tblexpr(abs(i)) WHERE i < 10000;]);

ok_injection_point($node, 'define-index-before-set-valid');

# s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
$s1->do_async(q[INSERT INTO test.tblexpr VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'invalidate-catalog-snapshot-end');

# Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
wakeup_injection_point($node, 'define-index-before-set-valid');

# s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
$s2->do_async(q[INSERT INTO test.tblexpr VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();]);

ok_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node, 'invalidate-catalog-snapshot-end');
ok_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node,
	'check-exclusion-or-unique-constraint-no-conflict');

clean_safe_quit_ok($s1, $s2, $s3);

$node->safe_psql('postgres', 'TRUNCATE TABLE test.tblexpr');

done_testing();

############################################################################
# Helper functions
#
############################################################################

# Helper: Wait for a session to hit an injection point.
# Optional second argument is timeout in seconds.
# Returns true if found, false if timeout.
# On timeout, logs diagnostic information about all active queries.
sub wait_for_injection_point
{
	my ($node, $point_name, $timeout) = @_;
	$timeout //= $PostgreSQL::Test::Utils::timeout_default / 2;

	for (my $elapsed = 0; $elapsed < $timeout * 10; $elapsed++)
	{
		my $pid = $node->safe_psql(
			'postgres', qq[
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'InjectionPoint'
			  AND wait_event = '$point_name'
			LIMIT 1;
		]);
		return 1 if $pid ne '';
		usleep(100_000);
	}

	# Timeout - report diagnostic information
	my $activity = $node->safe_psql(
		'postgres', q[
		SELECT format('pid=%s, state=%s, wait_event_type=%s, wait_event=%s, backend_xmin=%s, backend_xid=%s, query=%s',
			pid, state, wait_event_type, wait_event, backend_xmin, backend_xid, left(query, 100))
		FROM pg_stat_activity
		ORDER BY pid;
	]);
	diag(   "wait_for_injection_point timeout waiting for: $point_name\n"
		  . "Current queries in pg_stat_activity:\n$activity");

	return 0;
}

# Test helper: ok() a wait for the given injection point
# Third argument is an optional test name.
sub ok_injection_point
{
	my ($node, $injection_point, $testname) = @_;
	$testname //= "hit injection point $injection_point";

	ok(wait_for_injection_point($node, $injection_point), $testname);
}

# Helper: Wait for a specific backend to become idle.
# Returns true if idle, false if waiting for injection point or timeout.
sub wait_for_idle
{
	my ($node, $pid, $timeout) = @_;
	$timeout //= $PostgreSQL::Test::Utils::timeout_default / 2;

	for (my $elapsed = 0; $elapsed < $timeout * 10; $elapsed++)
	{
		my $result = $node->safe_psql(
			'postgres', qq[
			SELECT state, wait_event_type FROM pg_stat_activity WHERE pid = $pid;
		]);
		my ($state, $wait_event_type) = split(/\|/, $result, 2);
		$state           //= '';
		$wait_event_type //= '';
		return 1 if $state eq 'idle';
		return 0 if $wait_event_type eq 'InjectionPoint';

		usleep(100_000);
	}
	return 0;
}

# Helper: Detach and wakeup an injection point
sub wakeup_injection_point
{
	my ($node, $point_name) = @_;
	$node->safe_psql(
		'postgres', qq[
SELECT injection_points_detach('$point_name');
SELECT injection_points_wakeup('$point_name');
]);
}

# Wait for any pending query to complete and close the session.
# Returns empty string on success, error message on failure.
sub safe_quit
{
	my ($session) = @_;

	# Wait for any async queries to complete
	$session->wait_for_completion;

	# Check connection status
	my $status = $session->conn_status;

	# Close the session
	$session->close;

	# Return empty string if connection was OK, otherwise return error
	return ($status == CONNECTION_OK) ? '' : 'connection error';
}

# Helper function: verify that the given sessions exit cleanly.
sub clean_safe_quit_ok
{
	my $i = 1;
	foreach my $session (@_)
	{
		is(safe_quit($session), '', "session " . $i++ . " quit cleanly");
	}
}
