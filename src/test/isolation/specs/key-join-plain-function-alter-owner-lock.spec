# Unauthorized ALTER FUNCTION must not wait on key-join proof serialization.
#
# Session s1 alters f() and keeps the transaction open.  Session s2 lacks
# ownership of f(), so ALTER FUNCTION should fail the owner check immediately.
# It should not wait on s1's key-join proof serialization lock.

setup
{
 CREATE ROLE key_join_plain_function_alter_owner;
 CREATE ROLE key_join_plain_function_alter_other;
 CREATE SCHEMA key_join_plain_function_alter_owner_lock
     AUTHORIZATION key_join_plain_function_alter_owner;
 GRANT USAGE ON SCHEMA key_join_plain_function_alter_owner_lock
     TO key_join_plain_function_alter_other;
 SET ROLE key_join_plain_function_alter_owner;
 CREATE FUNCTION key_join_plain_function_alter_owner_lock.f() RETURNS int
     LANGUAGE sql RETURN 1;
 CREATE FUNCTION key_join_plain_function_alter_owner_lock.g() RETURNS int
     LANGUAGE sql RETURN 1;
 RESET ROLE;
}

teardown
{
 DROP SCHEMA key_join_plain_function_alter_owner_lock CASCADE;
 DROP ROLE key_join_plain_function_alter_owner;
 DROP ROLE key_join_plain_function_alter_other;
}

session s1
step s1_begin { BEGIN; }
step s1_alter_f
{
 ALTER FUNCTION key_join_plain_function_alter_owner_lock.f() COST 11;
}
step s1_alter_g
{
 ALTER FUNCTION key_join_plain_function_alter_owner_lock.g() COST 21;
}
step s1_owner_g
{
 ALTER FUNCTION key_join_plain_function_alter_owner_lock.g()
     OWNER TO key_join_plain_function_alter_other;
}
step s1_commit { COMMIT; }

session s2
step s2_auth { SET ROLE key_join_plain_function_alter_other; }
step s2_auth_owner
{
 RESET ROLE;
 SET ROLE key_join_plain_function_alter_owner;
}
step s2_alter_f
{
 ALTER FUNCTION key_join_plain_function_alter_owner_lock.f() COST 12;
}
step s2_alter_g
{
 ALTER FUNCTION key_join_plain_function_alter_owner_lock.g() COST 22;
}

permutation s1_begin s1_alter_f s2_auth s2_alter_f s1_commit
permutation s1_begin s1_alter_g s2_auth_owner s2_alter_g s1_owner_g s1_commit
