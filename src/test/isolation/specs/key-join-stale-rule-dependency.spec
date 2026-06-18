# Key-join revalidation must tolerate a concurrently dropped dependent rule.
#
# Revalidation scans pg_depend for stored key-join proof dependencies, then
# resolves a dependent view's rewrite rule to find the relation to revalidate.
# If that view is dropped after the dependency scan but before the rule lookup,
# the revalidation target is gone and the triggering DDL should continue.

setup
{
 CREATE SCHEMA key_join_stale_rule_dependency;
 CREATE TABLE key_join_stale_rule_dependency.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_stale_rule_dependency.c
 (
     id int PRIMARY KEY,
     parent_id int CONSTRAINT c_parent_id_not_null NOT NULL,
     CONSTRAINT c_parent_id_fkey
         FOREIGN KEY (parent_id)
         REFERENCES key_join_stale_rule_dependency.p (id)
 );
 INSERT INTO key_join_stale_rule_dependency.p VALUES (1);
 INSERT INTO key_join_stale_rule_dependency.c VALUES (10, 1);
 CREATE VIEW key_join_stale_rule_dependency.consumer AS
 SELECT p.id AS parent_id, c.id AS child_id
 FROM key_join_stale_rule_dependency.p p
 JOIN key_join_stale_rule_dependency.c c
     FOR KEY (parent_id) -> p (id);
}

teardown
{
 DROP SCHEMA key_join_stale_rule_dependency CASCADE;
}

session s1
step s1_lock_rewrite
{
 BEGIN;
 SET deadlock_timeout = '100ms';
 LOCK TABLE pg_rewrite IN ACCESS EXCLUSIVE MODE;
}
step s1_drop_consumer
{
 DROP VIEW key_join_stale_rule_dependency.consumer;
 COMMIT;
}

session s2
step s2_alter_fk
{
 SET deadlock_timeout = '10s';
 ALTER TABLE key_join_stale_rule_dependency.c
     ALTER CONSTRAINT c_parent_id_fkey NOT ENFORCED;
}

permutation s1_lock_rewrite s2_alter_fk s1_drop_consumer
