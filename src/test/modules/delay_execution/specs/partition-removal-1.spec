# Test removal of a partition with less-than-exclusive locking.

setup
{
  CREATE TABLE partrem (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE partrem1 PARTITION OF partrem FOR VALUES IN (1);
  CREATE TABLE partrem2 PARTITION OF partrem FOR VALUES IN (2);
  CREATE TABLE partrem3 PARTITION OF partrem FOR VALUES IN (3);
  INSERT INTO partrem VALUES (1, 'ABC');
  INSERT INTO partrem VALUES (2, 'JKL');
  INSERT INTO partrem VALUES (3, 'DEF');
}

teardown
{
  DROP TABLE IF EXISTS partrem, partrem2;
}

# The SELECT will be planned with all three partitions shown above,
# of which we expect partrem1 to be pruned at planning and partrem3 at
# execution. Then we'll block, and by the time the query is actually
# executed, detach of partrem2 is already underway; however we expect
# its rows to still appear in the result.

session "s1"
setup		{ LOAD 'delay_execution';
		  SET delay_execution.post_planning_lock_id = 12543; }
step "s1b"	{ BEGIN; }
step "s1brr"	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s1exec"	{ SELECT * FROM partrem WHERE a <> 1 AND a <> (SELECT 3); }
step "s1exec2"	{ SELECT * FROM partrem WHERE a <> (SELECT 2); }
step "s1c"	{ COMMIT; }

session "s2"
step "s2remp"	{ ALTER TABLE partrem DETACH PARTITION partrem2 CONCURRENTLY; }

session "s3"
step "s3lock"	{ SELECT pg_advisory_lock(12543); }
step "s3unlock"	{ SELECT pg_advisory_unlock(12543); }

permutation "s3lock" "s1b" "s1exec" "s2remp" "s3unlock" "s1c"
permutation "s3lock" "s1brr" "s1exec" "s2remp" "s3unlock" "s1c"

permutation "s3lock" "s1b" "s1exec2" "s2remp" "s3unlock" "s1c"
permutation "s3lock" "s1brr" "s1exec2" "s2remp" "s3unlock" "s1c"
