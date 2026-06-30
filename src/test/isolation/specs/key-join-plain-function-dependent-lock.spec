# Ordinary SQL function DDL must not hold locks on ordinary dependent
# functions due to key-join proof prelocking.
#
# f() has a normal SQL-body dependency on g().  Session s1 alters g() and
# keeps the transaction open.  Altering f() in session s2 should not wait on
# s1, since f() contains no stored FOR KEY proof.

setup
{
 CREATE SCHEMA key_join_plain_function_dependent_lock;
 CREATE FUNCTION key_join_plain_function_dependent_lock.g() RETURNS int
     LANGUAGE sql RETURN 1;
 CREATE FUNCTION key_join_plain_function_dependent_lock.f() RETURNS int
     LANGUAGE sql RETURN key_join_plain_function_dependent_lock.g();
}

teardown
{
 DROP SCHEMA key_join_plain_function_dependent_lock CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_alter_g { ALTER FUNCTION key_join_plain_function_dependent_lock.g() COST 11; }
step s1_commit { COMMIT; }

session s2
step s2_alter_f { ALTER FUNCTION key_join_plain_function_dependent_lock.f() COST 12; }

permutation s1_begin s1_alter_g s2_alter_f s1_commit
