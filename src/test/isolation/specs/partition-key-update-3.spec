# Concurrency error from ExecLockRows

# Like partition-key-update-1.spec, throw an error where a session trying to
# lock a row that has been moved to another partition due to a concurrent
# update by other seesion.

setup
{
  CREATE TABLE foo_r (a int, b text) PARTITION BY RANGE(a);
  CREATE TABLE foo_r1 PARTITION OF foo_r FOR VALUES FROM (1) TO (10);
  CREATE TABLE foo_r2 PARTITION OF foo_r FOR VALUES FROM (10) TO (20);
  INSERT INTO foo_r VALUES(7, 'ABC');
  CREATE UNIQUE INDEX foo_r1_a_unique ON foo_r1 (a);
  CREATE TABLE bar (a int REFERENCES foo_r1(a));
}

teardown
{
  DROP TABLE bar, foo_r;
}

session "s1"
setup		{ BEGIN; }
step "s1u3"	{ UPDATE foo_r SET a=11 WHERE a=7 AND b = 'ABC'; }
step "s1c"	{ COMMIT; }

session "s2"
step "s2i"	{ INSERT INTO bar VALUES(7); }

permutation "s1u3" "s2i" "s1c"
