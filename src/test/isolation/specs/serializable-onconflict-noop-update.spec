# Write-skew via INSERT ... ON CONFLICT DO UPDATE with an unsatisfied WHERE
# (a no-op update) under SERIALIZABLE.
#
#   s1 examines k=1 through the ON CONFLICT arbiter and writes k=2
#   s2 reads k=2 and writes k=1
#
# This is a dangerous structure, so SSI must cancel one transaction.
#
# The question this exercises: the arbiter's conflict probe is a read that
# decides the statement's outcome -- exactly like the plain SELECT in the
# control permutation -- so it must participate in SSI and take an SIREAD lock.
# Until that read was predicate-locked, only the control was cancelled while
# the ON CONFLICT no-op variant committed a non-serializable result.

setup     { CREATE TABLE noc (k int PRIMARY KEY, v int NOT NULL); }
teardown  { DROP TABLE noc; }

session s1
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s1read { SELECT v FROM noc WHERE k = 1; }
step s1noop { INSERT INTO noc(k, v) VALUES (1, 99) ON CONFLICT (k) DO UPDATE SET v = 100 WHERE noc.v = 42; }
step s1w2   { UPDATE noc SET v = 1 WHERE k = 2; }
step s1c    { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }
step s2r2 { SELECT v FROM noc WHERE k = 2; }
step s2w1 { UPDATE noc SET v = 42 WHERE k = 1; }
step s2c  { COMMIT; }

# Non-transactional helper: resets the rows before each permutation and reports
# the final committed state plus a serializability invariant.
session ctl
step reset { DELETE FROM noc; INSERT INTO noc VALUES (1, 0), (2, 0); }
step ck    { SELECT k, v FROM noc ORDER BY k;
             SELECT CASE WHEN (SELECT v FROM noc WHERE k = 1) = 42
                          AND (SELECT v FROM noc WHERE k = 2) = 1
                         THEN 'bad_both_committed'
                         ELSE 'ok_aborted_or_serial' END AS invariant; }

permutation reset s1read s2r2 s1w2 s2w1 s1c s2c ck
permutation reset s1noop s2r2 s1w2 s2w1 s1c s2c ck
