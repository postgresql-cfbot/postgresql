# Test to check that invalidation of cached generic plans during ExecutorStart
# correctly triggers replanning and re-execution.

setup
{
  CREATE TABLE foo (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE foo12 PARTITION OF foo FOR VALUES IN (1, 2) PARTITION BY LIST (a);
  CREATE TABLE foo12_1 PARTITION OF foo12 FOR VALUES IN (1);
  CREATE TABLE foo12_2 PARTITION OF foo12 FOR VALUES IN (2);
  CREATE INDEX foo12_1_a ON foo12_1 (a);
  CREATE TABLE foo3 PARTITION OF foo FOR VALUES IN (3);
  CREATE VIEW foov AS SELECT * FROM foo;
  CREATE FUNCTION one () RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE PLPGSQL STABLE;
  CREATE FUNCTION two () RETURNS int AS $$ BEGIN RETURN 2; END; $$ LANGUAGE PLPGSQL STABLE;
  CREATE RULE update_foo AS ON UPDATE TO foo DO ALSO SELECT 1;
}

teardown
{
  DROP VIEW foov;
  DROP RULE update_foo ON foo;
  DROP TABLE foo;
  DROP FUNCTION one(), two();
}

session "s1"
# Append with run-time pruning
step "s1prep"   { SET plan_cache_mode = force_generic_plan;
		  PREPARE q AS SELECT * FROM foov WHERE a = $1 FOR UPDATE;
		  EXPLAIN (COSTS OFF) EXECUTE q (1); }

# Another case with Append with run-time pruning
step "s1prep2"   { SET plan_cache_mode = force_generic_plan;
		  PREPARE q2 AS SELECT * FROM foov WHERE a = one() or a = two();
		  EXPLAIN (COSTS OFF) EXECUTE q2; }

# Case with a rule adding another query
step "s1prep3"   { SET plan_cache_mode = force_generic_plan;
		  PREPARE q3 AS UPDATE foov SET a = a WHERE a = one() or a = two();
		  EXPLAIN (COSTS OFF) EXECUTE q3; }

# Another case with Append with run-time pruning in a subquery
step "s1prep4"   { SET plan_cache_mode = force_generic_plan;
		  SET enable_seqscan TO off;
		  PREPARE q4 AS SELECT * FROM generate_series(1, 1) WHERE EXISTS (SELECT * FROM foov WHERE a = $1 FOR UPDATE);
		  EXPLAIN (COSTS OFF) EXECUTE q4 (1); }

# Executes a generic plan
step "s1exec"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q (1); }
step "s1exec2"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q2; }
step "s1exec3"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q3; }
step "s1exec4"	{ LOAD 'delay_execution';
		  SET delay_execution.executor_start_lock_id = 12345;
		  EXPLAIN (COSTS OFF) EXECUTE q4 (1); }

session "s2"
step "s2lock"	{ SELECT pg_advisory_lock(12345); }
step "s2unlock"	{ SELECT pg_advisory_unlock(12345); }
step "s2dropi"	{ DROP INDEX foo12_1_a; }

# While "s1exec", etc. wait to acquire the advisory lock, "s2drop" is able to
# drop the index being used in the cached plan.  When "s1exec" is then
# unblocked and initializes the cached plan for execution, it detects the
# concurrent index drop and causes the cached plan to be discarded and
# recreated without the index.
permutation "s1prep" "s2lock" "s1exec" "s2dropi" "s2unlock"
permutation "s1prep2" "s2lock" "s1exec2" "s2dropi" "s2unlock"
permutation "s1prep3" "s2lock" "s1exec3" "s2dropi" "s2unlock"
permutation "s1prep4" "s2lock" "s1exec4" "s2dropi" "s2unlock"
