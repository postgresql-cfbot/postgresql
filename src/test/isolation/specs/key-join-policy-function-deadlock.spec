# ALTER FUNCTION must not deadlock with ALTER POLICY revalidation.
#
# The original policy depends on f() as an ordinary RLS expression, not as a
# FOR KEY proof dependency.  ALTER FUNCTION used to lock f(), then revalidate
# the dependent policy by taking AccessExclusiveLock on the guarded table.
# That inverted ALTER POLICY's table-then-proof-function lock order.

setup
{
 CREATE SCHEMA key_join_policy_function_deadlock;
 CREATE TABLE key_join_policy_function_deadlock.p
 (
     region int NOT NULL,
     id int NOT NULL,
     PRIMARY KEY (region, id)
 );
 CREATE TABLE key_join_policy_function_deadlock.c
 (
     cid int PRIMARY KEY,
     region int NOT NULL,
     parent_id int NOT NULL,
     FOREIGN KEY (region, parent_id)
         REFERENCES key_join_policy_function_deadlock.p (region, id)
 );
 CREATE TABLE key_join_policy_function_deadlock.guarded (id int);
 INSERT INTO key_join_policy_function_deadlock.p VALUES (1, 1), (2, 1);
 INSERT INTO key_join_policy_function_deadlock.c VALUES (10, 1, 1), (20, 2, 1);
 CREATE FUNCTION key_join_policy_function_deadlock.f() RETURNS int
     LANGUAGE sql STABLE AS 'SELECT 1';
 ALTER TABLE key_join_policy_function_deadlock.guarded ENABLE ROW LEVEL SECURITY;
 CREATE POLICY pol ON key_join_policy_function_deadlock.guarded
     USING (key_join_policy_function_deadlock.f() = 1);
}

teardown
{
 DROP SCHEMA key_join_policy_function_deadlock CASCADE;
}

session s1
step s1_lock_table
{
 BEGIN;
 SET deadlock_timeout = '10ms';
 LOCK TABLE key_join_policy_function_deadlock.guarded IN ACCESS EXCLUSIVE MODE;
}
step s1_alter_policy
{
 ALTER POLICY pol ON key_join_policy_function_deadlock.guarded
 USING (EXISTS (
     SELECT 1
     FROM (SELECT * FROM key_join_policy_function_deadlock.p
           WHERE region = key_join_policy_function_deadlock.f()) p
     JOIN (SELECT * FROM key_join_policy_function_deadlock.c
           WHERE region = key_join_policy_function_deadlock.f()) c
         FOR KEY (region, parent_id) -> p (region, id)
 ));
}
step s1_rollback { ROLLBACK; }

session s2
step s2_alter_function
{
 SET deadlock_timeout = '10s';
 ALTER FUNCTION key_join_policy_function_deadlock.f() VOLATILE;
}

permutation s1_lock_table s2_alter_function s1_alter_policy s1_rollback
