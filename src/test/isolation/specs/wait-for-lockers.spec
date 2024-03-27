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
setup			{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step w1_in1		{ INSERT INTO t1 VALUES (DEFAULT); }
step w1_lae1	{ LOCK TABLE t1 IN ACCESS EXCLUSIVE MODE; }
step w1_lae2	{ LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE; }
step w1_c	{ COMMIT; }

session writer2
setup		{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step w2_in1	{ INSERT INTO t1 VALUES (DEFAULT); }
step w2_c	{ COMMIT; }

session reader
setup			{ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED; }
step r_sv		{ SAVEPOINT foo; }
step r_l		{ LOCK TABLE t1, t2 IN SHARE MODE; }
step r_rb		{ ROLLBACK TO foo; }
step r_wfl		{ SELECT pg_wait_for_lockers(array['t1','t2']::regclass[],
											 'RowExclusiveLock', FALSE); }
step r_wflc		{ SELECT pg_wait_for_lockers(array['t1','t2']::regclass[],
											 'ShareLock', TRUE); }
step r_sel1		{ SELECT id from t1; }
step r_c		{ COMMIT; }


# Basic sanity checks of pg_wait_for_lockers():

# no waiting if no lockers (writers already committed)
permutation w1_lae2 w2_in1 w2_c w1_c r_wflc r_sel1 r_c

# reader waits only for writer2 holding a lock in ROW EXCLUSIVE mode, not for
# writer1 holding a lock in ACCESS EXCLUSIVE mode
permutation w1_lae2 w2_in1 r_wfl w2_c r_sel1 w1_c r_c

# reader waits for both writers conflicting with SHARE mode
permutation w1_lae2 w2_in1 r_wflc w2_c w1_c r_sel1 r_c


# Comparisons between pg_wait_for_lockers() and nearest equivalent LOCK +
# ROLLBACK:

# reader waiting for writer1 allows writer2 to take a matching lock...
permutation w1_in1 r_wflc w2_in1 w2_c w1_c r_sel1 r_c
# ...whereas reader actually taking a conflicting lock blocks writer2 until
# writer1 releases its lock (even if reader releases ASAP)
permutation w1_in1 r_sv r_l w2_in1 w1_c r_rb w2_c r_sel1 r_c

# reader waits only for matching lock already held by writer1, not for writer2
# which was waiting to take a matching lock...
permutation w1_lae1 w2_in1 r_wflc w1_c r_sel1 w2_c r_c
# ...whereas actually taking a conflicting lock also waits for writer2 to take
# and release its lock
permutation w1_lae1 w2_in1 r_l w1_c w2_c r_sel1 r_c

# reader waiting for two tables, with only writer2 holding a matching ROW
# EXCLUSIVE lock on t1, allows writer1 to then take an ACCESS EXCLUSIVE lock on
# t2 and another ROW EXCLUSIVE lock on t1, and reader doesn't wait for writer1's
# later locks...
permutation w2_in1 r_wflc w1_lae2 w1_in1 w2_c r_sel1 w1_c r_c
# ...whereas reader actually taking conflicting locks on the two tables first
# waits for writer2's ROW EXCLUSIVE lock (same as above), and then for writer1's
# *later* ACCESS EXCLUSIVE lock (due to LOCK's one-by-one locking); note that
# writer1's later insert w1_in1 would deadlock so it's omitted altogether
permutation w2_in1 r_sv r_l w1_lae2 w2_c w1_c r_rb r_sel1 r_c
