# Key-join revalidation must tolerate a concurrently dropped dependent policy.
#
# Revalidation may find an RLS policy in pg_depend, then wait before reading
# pg_policy.  If the policy is dropped while it waits, the stored proof is gone
# and the triggering DDL should not dereference the stale policy OID.

setup
{
 CREATE SCHEMA key_join_stale_policy_dependency;
 CREATE TABLE key_join_stale_policy_dependency.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_stale_policy_dependency.c
 (
     id int PRIMARY KEY,
     parent_id int CONSTRAINT c_parent_id_not_null NOT NULL,
     CONSTRAINT c_parent_id_fkey
         FOREIGN KEY (parent_id)
         REFERENCES key_join_stale_policy_dependency.p (id)
 );
 CREATE TABLE key_join_stale_policy_dependency.guarded
 (
     id int
 );
 INSERT INTO key_join_stale_policy_dependency.p VALUES (1);
 INSERT INTO key_join_stale_policy_dependency.c VALUES (10, 1);
 ALTER TABLE key_join_stale_policy_dependency.guarded
     ENABLE ROW LEVEL SECURITY;
 CREATE POLICY pol ON key_join_stale_policy_dependency.guarded
 USING (EXISTS (
     SELECT 1
     FROM key_join_stale_policy_dependency.p p
     JOIN key_join_stale_policy_dependency.c c
         FOR KEY (parent_id) -> p (id)
 ));
}

teardown
{
 DROP SCHEMA key_join_stale_policy_dependency CASCADE;
}

session s1
step s1_lock_policy
{
 BEGIN;
 SET deadlock_timeout = '100ms';
 LOCK TABLE pg_policy IN ACCESS EXCLUSIVE MODE;
}
step s1_drop_policy
{
 DROP POLICY pol ON key_join_stale_policy_dependency.guarded;
 COMMIT;
}

session s2
step s2_alter_fk
{
 SET deadlock_timeout = '10s';
 ALTER TABLE key_join_stale_policy_dependency.c
     ALTER CONSTRAINT c_parent_id_fkey NOT ENFORCED;
}

permutation s1_lock_policy s2_alter_fk s1_drop_policy
