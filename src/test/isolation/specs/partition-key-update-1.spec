# Concurrency error from ExecUpdate and ExecDelete.

# Throw an error to indicate that the targeted row has been already moved to
# another partition in the case of concurrency where a session trying to
# update/delete a row that's locked for a concurrent update by the another
# session cause tuple movement to the another partition due update of partition
# key.

setup
{
  CREATE TABLE foo (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE foo1 PARTITION OF foo FOR VALUES IN (1);
  CREATE TABLE foo2 PARTITION OF foo FOR VALUES IN (2);
  INSERT INTO foo VALUES (1, 'ABC');
}

teardown
{
  DROP TABLE foo;
}

session "s1"
setup		{ BEGIN; }
step "s1u"	{ UPDATE foo SET a=2 WHERE a=1; }
step "s1c"	{ COMMIT; }

session "s2"
step "s2u"	{ UPDATE foo SET b='EFG' WHERE a=1; }
step "s2d"	{ DELETE FROM foo WHERE a=1; }

permutation "s1u" "s1c" "s2u"
permutation "s1u" "s2u" "s1c"
permutation "s2u" "s1u" "s1c"

permutation "s1u" "s1c" "s2d"
permutation "s1u" "s2d" "s1c"
permutation "s2d" "s1u" "s1c"
