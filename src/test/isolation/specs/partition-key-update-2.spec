# Concurrency error from GetTupleForTrigger

# Like partition-key-update-1.spec, throw an error where a session trying to
# update a row that has been moved to another partition due to a concurrent
# update by other seesion.

setup
{
  CREATE TABLE foo (a int, b text) PARTITION BY LIST(a);
  CREATE TABLE foo1 PARTITION OF foo FOR VALUES IN (1);
  CREATE TABLE foo2 PARTITION OF foo FOR VALUES IN (2);
  INSERT INTO foo VALUES (1, 'ABC');
  CREATE FUNCTION func_foo_mod_a() RETURNS TRIGGER AS $$
    BEGIN
	  NEW.a = 2; -- This is changing partition key column.
   RETURN NEW;
  END $$ LANGUAGE PLPGSQL;
  CREATE TRIGGER foo_mod_a BEFORE UPDATE ON foo1
   FOR EACH ROW EXECUTE PROCEDURE func_foo_mod_a();
}

teardown
{
  DROP TRIGGER foo_mod_a ON foo1;
  DROP FUNCTION func_foo_mod_a();
  DROP TABLE foo;
}

session "s1"
setup		{ BEGIN; }
step "s1u"	{ UPDATE foo SET b='EFG' WHERE a=1; }
step "s1c"	{ COMMIT; }

session "s2"
step "s2u"	{ UPDATE foo SET b='XYZ' WHERE a=1; }

permutation "s1u" "s1c" "s2u"
permutation "s1u" "s2u" "s1c"
permutation "s2u" "s1u" "s1c"
