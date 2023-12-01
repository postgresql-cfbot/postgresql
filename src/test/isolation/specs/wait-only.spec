setup
{
  CREATE TABLE t1 (id bigserial);
  CREATE TABLE t2 (id bigserial);
}

teardown
{
  DROP TABLE t1;
  DROP TABLE t2;
}

# use READ COMMITTED so we can observe the effects of a committed INSERT after
# waiting

session writer1
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step w1in1	{ INSERT INTO t1 VALUES (DEFAULT); }
step w1ae1	{ LOCK TABLE t1 IN ACCESS EXCLUSIVE MODE; }
step w1ae2	{ LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE; }
step w1c	{ COMMIT; }

session writer2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step w2in1	{ INSERT INTO t1 VALUES (DEFAULT); }
step w2c	{ COMMIT; }

session reader
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step rsv	{ SAVEPOINT foo; }
step rl		{ LOCK TABLE t1, t2 IN SHARE MODE; }
step rrb	{ ROLLBACK TO foo; }
step rlwo	{ LOCK TABLE t1, t2 IN SHARE MODE WAIT ONLY; }
step rsel1	{ SELECT id from t1; }
step rc		{ COMMIT; }

# reader waits for both writers
permutation w1ae2 w2in1 rlwo w2c w1c rsel1 rc

# no waiting if writers already committed (obviously)
permutation w1ae2 w2in1 w2c w1c rlwo rsel1 rc

# reader waiting for writer1 doesn't block writer2...
permutation w1in1 rlwo w2in1 w2c w1c rsel1 rc
# ...while actually taking the lock does block writer2 (even if we release it
# ASAP)
permutation w1in1 rsv rl w2in1 w1c rrb w2c rsel1 rc

# reader waiting for two tables with only t1 having a conflicting lock doesn't
# prevent taking an ACCESS EXCLUSIVE lock on t2, or a lesser lock on t1, and the
# reader doesn't wait for either later lock to be released...
permutation w2in1 rlwo w1ae2 w1in1 w2c rsel1 w1c rc
# ...while actually taking the locks is blocked by the ACCESS EXCLUSIVE lock and
# would deadlock with the subsequent insert w1in1 (removed here)
permutation w2in1 rsv rl w1ae2 w2c w1c rrb rsel1 rc

# reader waits only for conflicting lock already held by writer1, not for
# writer2 which was waiting to take a conflicting lock...
permutation w1ae1 w2in1 rlwo w1c rsel1 w2c rc
# ...while actually taking the lock also waits for writer2 to release its lock
permutation w1ae1 w2in1 rl w1c w2c rsel1 rc
