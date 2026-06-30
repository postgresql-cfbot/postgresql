# Key-join revalidation must not use ordinary dependencies as traversal
# bridges.
#
# Disabling RLS on x changes an ordinary dependency of dep but not a proof
# surface consumed by consumer, so it must not wait on consumer's locked child
# table.  Replacing dep does change the consumed proof surface and must still
# wait on consumer without deadlocking.

setup
{
 CREATE SCHEMA key_join_recursive_revalidation_lock;
 CREATE TABLE key_join_recursive_revalidation_lock.x (id int);
 ALTER TABLE key_join_recursive_revalidation_lock.x ENABLE ROW LEVEL SECURITY;
 CREATE TABLE key_join_recursive_revalidation_lock.p
 (
     region int NOT NULL,
     id int NOT NULL,
     PRIMARY KEY (region, id)
 );
 CREATE TABLE key_join_recursive_revalidation_lock.c
 (
     cid int PRIMARY KEY,
     region int NOT NULL,
     parent_id int NOT NULL,
     FOREIGN KEY (region, parent_id)
         REFERENCES key_join_recursive_revalidation_lock.p (region, id)
 );
 INSERT INTO key_join_recursive_revalidation_lock.x VALUES (1);
 INSERT INTO key_join_recursive_revalidation_lock.p VALUES (1, 1), (2, 1);
 INSERT INTO key_join_recursive_revalidation_lock.c
     VALUES (10, 1, 1), (20, 2, 1);
 CREATE VIEW key_join_recursive_revalidation_lock.dep AS
 SELECT p.region, p.id,
        (SELECT count(*) FROM key_join_recursive_revalidation_lock.x) AS marker
 FROM key_join_recursive_revalidation_lock.p;
 CREATE VIEW key_join_recursive_revalidation_lock.consumer AS
 SELECT c.cid, dep.id
 FROM key_join_recursive_revalidation_lock.dep
 JOIN key_join_recursive_revalidation_lock.c
     FOR KEY (region, parent_id) -> dep (region, id);
}

teardown
{
 DROP SCHEMA key_join_recursive_revalidation_lock CASCADE;
}

session s1
step s1_disable_rls
{
 SET deadlock_timeout = '10ms';
 ALTER TABLE key_join_recursive_revalidation_lock.x
     DISABLE ROW LEVEL SECURITY;
}

session s2
step s2_replace_dep
{
 SET deadlock_timeout = '10s';
 BEGIN;
 ALTER VIEW key_join_recursive_revalidation_lock.dep
     SET (security_barrier=false);
 CREATE OR REPLACE VIEW key_join_recursive_revalidation_lock.dep AS
 SELECT p.region, p.id,
        (SELECT count(*) FROM key_join_recursive_revalidation_lock.x) AS marker
 FROM key_join_recursive_revalidation_lock.p;
 COMMIT;
}

session s3
step s3_lock_child
{
 BEGIN;
 LOCK TABLE key_join_recursive_revalidation_lock.c
     IN ACCESS EXCLUSIVE MODE;
}
step s3_commit { COMMIT; }

permutation s3_lock_child s1_disable_rls s2_replace_dep s3_commit
