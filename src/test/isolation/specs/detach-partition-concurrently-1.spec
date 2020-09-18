# Test that detach partition concurrently makes the partition invisible at the
# correct time.

setup
{
  DROP TABLE IF EXISTS d_listp, d_listp1, d_listp2;
  CREATE TABLE d_listp (a int) PARTITION BY LIST(a);
  CREATE TABLE d_listp1 PARTITION OF d_listp FOR VALUES IN (1);
  CREATE TABLE d_listp2 PARTITION OF d_listp FOR VALUES IN (2);
  INSERT INTO d_listp VALUES (1),(2);
}

teardown {
    DROP TABLE IF EXISTS d_listp, d_listp2, d_listp_foobar;
}

session "s1"
step "s1b"		{ BEGIN; }
step "s1brr"	{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step "s1s"		{ SELECT * FROM d_listp; }
step "s1ins"	{ INSERT INTO d_listp VALUES (1); }
step "s1prep"	{ PREPARE f(int) AS INSERT INTO d_listp VALUES ($1); }
step "s1prep1"	{ PREPARE f(int) AS INSERT INTO d_listp VALUES (1); }
step "s1exec"	{ EXECUTE f(1); }
step "s1exec2"	{ EXECUTE f(2); }
step "s1dealloc" { DEALLOCATE f; }
step "s1c"		{ COMMIT; }

session "s2"
step "s2d"		{ ALTER TABLE d_listp DETACH PARTITION d_listp2 CONCURRENTLY; }
step "s2drop"	{ DROP TABLE d_listp2; }

session "s3"
step "s3s"		{ SELECT * FROM d_listp; }
step "s3i"		{ SELECT relpartbound IS NULL FROM pg_class where relname = 'd_listp2'; }
step "s3drop1"	{ DROP TABLE d_listp; }
step "s3drop2"	{ DROP TABLE d_listp2; }
step "s3detach"	{ ALTER TABLE d_listp DETACH PARTITION d_listp2; }
step "s3rename"	{ ALTER TABLE d_listp2 RENAME TO d_listp_foobar; }

# The transaction that detaches hangs until it sees any older transaction
# terminate.
permutation "s1b" "s1s" "s2d" "s1s" "s1c" "s2drop" "s1s"

# relpartbound remains set until s1 commits
# XXX this could be timing dependent :-(
permutation "s1b" "s1s" "s2d" "s1s" "s3s" "s3i" "s1c" "s3i" "s2drop" "s1s"

# Session s1 no longer sees the partition ...
permutation "s1b" "s1s" "s2d" "s1ins" "s1c"
# ... regardless of the isolation level.
permutation "s1brr" "s1s" "s2d" "s1ins" "s1c"
# However, DETACH CONCURRENTLY is not isolation-level-correct
permutation "s1brr" "s1s" "s2d" "s1s" "s1c"

# a prepared query is not blocked
permutation "s1brr" "s1prep" "s1s" "s2d" "s1s" "s1exec" "s1dealloc" "s1c"
permutation "s1brr" "s1prep" "s1s" "s2d" "s1s" "s1exec2" "s1dealloc" "s1c"
permutation "s1brr" "s1prep" "s1s" "s2d" "s1s" "s1exec2" "s1c" "s1dealloc"
permutation "s1brr" "s1prep" "s2d" "s1s" "s1exec2" "s1c" "s1dealloc"
permutation "s1brr" "s1prep1" "s2d" "s1s" "s1exec2" "s1c" "s1dealloc"


# Note: the following tests are not very interesting in isolationtester because
# the way this tool handles multiple sessions: it'll only run the drop/detach/
# rename commands after s2 is blocked waiting.  It would be more useful to
# run the DDL when s2 commits its first transaction instead.  Maybe these should
# become TAP tests someday.

# What happens if the partition or the parent table is dropped while waiting?
permutation "s1b" "s1s" "s2d" "s3drop1" "s1c"
permutation "s1b" "s1s" "s2d" "s3drop2" "s1c"
# Also try a non-concurrent detach while the other one is waiting
permutation "s1b" "s1s" "s2d" "s3detach" "s1c"
# and some other DDL
permutation "s1b" "s1s" "s2d" "s3rename" "s1c"
