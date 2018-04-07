# Throw an error to indicate that the targeted row has been already moved to
# another partition in the case of concurrency where a session trying to
# lock/update/delete a row that's locked for a concurrent update by the another
# session cause tuple movement to the another partition due update of partition
# key.

setup
{
  --
  -- Setup to test an error from ExecUpdate and ExecDelete.
  --
  CREATE TABLE foo (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE foo1 PARTITION OF foo FOR VALUES IN (1);
  CREATE TABLE foo2 PARTITION OF foo FOR VALUES IN (2);
  INSERT INTO foo VALUES (1, 'ABC');

  --
  -- Setup to test an error from GetTupleForTrigger
  --
  CREATE TABLE footrg (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE footrg1 PARTITION OF footrg FOR VALUES IN (1);
  CREATE TABLE footrg2 PARTITION OF footrg FOR VALUES IN (2);
  INSERT INTO footrg VALUES (1, 'ABC');
  CREATE FUNCTION func_footrg_mod_a() RETURNS TRIGGER AS $$
    BEGIN
	  NEW.a = 2; -- This is changing partition key column.
   RETURN NEW;
  END $$ LANGUAGE PLPGSQL;
  CREATE TRIGGER footrg_mod_a BEFORE UPDATE ON footrg1
   FOR EACH ROW EXECUTE PROCEDURE func_footrg_mod_a();

  --
  -- Setup to test an error from ExecLockRows
  --
  CREATE TABLE foo_rang_parted (a int, b text) PARTITION BY RANGE(a);
  CREATE TABLE foo_rang_parted1 PARTITION OF foo_rang_parted FOR VALUES FROM (1) TO (10);
  CREATE TABLE foo_rang_parted2 PARTITION OF foo_rang_parted FOR VALUES FROM (10) TO (20);
  INSERT INTO foo_rang_parted VALUES(7, 'ABC');
  CREATE UNIQUE INDEX foo_rang_parted1_a_unique ON foo_rang_parted1 (a);
  CREATE TABLE bar (a int REFERENCES foo_rang_parted1(a));
}

teardown
{
  DROP TABLE foo;
  DROP TRIGGER footrg_mod_a ON footrg1;
  DROP FUNCTION func_footrg_mod_a();
  DROP TABLE footrg;
  DROP TABLE bar, foo_rang_parted;
}

session "s1"
step "s1b"	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s1u"	{ UPDATE foo SET a=2 WHERE a=1; }
step "s1u2"	{ UPDATE footrg SET b='EFG' WHERE a=1; }
step "s1u3"	{ UPDATE foo_rang_parted SET a=11 WHERE a=7 AND b = 'ABC'; }
step "s1c"	{ COMMIT; }

session "s2"
step "s2b"	{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step "s2u"	{ UPDATE foo SET b='EFG' WHERE a=1; }
step "s2u2"	{ UPDATE footrg SET b='XYZ' WHERE a=1; }
step "s2i"	{ INSERT INTO bar VALUES(7); }
step "s2d"	{ DELETE FROM foo WHERE a=1; }
step "s2c"	{ COMMIT; }

# Concurrency error from ExecUpdate and ExecDelete.
permutation "s1b" "s2b" "s1u" "s1c" "s2d" "s2c"
permutation "s1b" "s2b" "s1u" "s2d" "s1c" "s2c"
permutation "s1b" "s2b" "s2d" "s1u" "s2c" "s1c"

# Concurrency error from GetTupleForTrigger
permutation "s1b" "s2b" "s1u2" "s1c" "s2u2" "s2c"
permutation "s1b" "s2b" "s1u2" "s2u2" "s1c" "s2c"
permutation "s1b" "s2b" "s2u2" "s1u2" "s2c" "s1c"

# Concurrency error from ExecLockRows
permutation "s1b" "s2b" "s1u3" "s2i" "s1c" "s2c"
