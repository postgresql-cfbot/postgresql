# Ordinary SQL function DDL must not wait on function dependents due to
# key-join proof prelocking.
#
# f() has a normal SQL-body dependency on g().  Session s1 holds an ALTER
# FUNCTION lock on f().  Altering g() in session s2 should not lock f(), since
# no stored FOR KEY proof is involved in this workload.

setup
{
 CREATE SCHEMA key_join_plain_function_prelock;
 CREATE FUNCTION key_join_plain_function_prelock.g() RETURNS int
     LANGUAGE sql RETURN 1;
 CREATE FUNCTION key_join_plain_function_prelock.f() RETURNS int
     LANGUAGE sql RETURN key_join_plain_function_prelock.g();
}

teardown
{
 DROP SCHEMA key_join_plain_function_prelock CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_alter_f { ALTER FUNCTION key_join_plain_function_prelock.f() COST 11; }
step s1_commit { COMMIT; }

session s2
step s2_alter_g { ALTER FUNCTION key_join_plain_function_prelock.g() COST 12; }

permutation s1_begin s1_alter_f s2_alter_g s1_commit
