# Test to check that invalidation of a cached plan during ExecutorStart
# correctly triggers replanning and re-execution.

setup
{
  CREATE TABLE foo (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE foo1 PARTITION OF foo FOR VALUES IN (1) PARTITION BY LIST (a);
  CREATE TABLE foo11 PARTITION OF foo1 FOR VALUES IN (1);
  CREATE INDEX foo11_a ON foo1 (a);
  CREATE TABLE foo2 PARTITION OF foo FOR VALUES IN (2);
  CREATE VIEW foov AS SELECT * FROM foo;
}

teardown
{
  DROP VIEW foov;
  DROP TABLE foo;
}

session "s1"
# Creates a prepared statement and forces creation of a generic plan
step "s1prep"   { SET plan_cache_mode = force_generic_plan;
		  PREPARE q AS SELECT * FROM foov WHERE a = $1;
		  EXPLAIN (COSTS OFF) EXECUTE q (1); }

step "s1prep2"   { SET plan_cache_mode = force_generic_plan;
		  SET enable_partitionwise_aggregate = on;
		  SET enable_partitionwise_join = on;
		  PREPARE q2 AS SELECT t1.a, count(t2.b) FROM foo t1, foo t2 WHERE t1.a = t2.a GROUP BY 1;
		  EXPLAIN (COSTS OFF) EXECUTE q2; }
# Executes a generic plan
step "s1exec"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q (1); }
step "s1exec2"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q2; }

session "s2"
step "s2lock"	{ SELECT pg_advisory_lock(12345); }
step "s2unlock"	{ SELECT pg_advisory_unlock(12345); }
step "s2dropi"	{ DROP INDEX foo11_a; }

# While "s1exec" waits to acquire the advisory lock, "s2drop" is able to drop
# the index being used in the cached plan for `q`, so when "s1exec" is then
# unblocked and initializes the cached plan for execution, it detects the
# concurrent index drop and causes the cached plan to be discarded and
# recreated without the index.
permutation "s1prep" "s2lock" "s1exec" "s2dropi" "s2unlock"
permutation "s1prep2" "s2lock" "s1exec2" "s2dropi" "s2unlock"
