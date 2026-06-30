# Ordinary dependent views must not be locked during key-join revalidation.
#
# Enabling RLS on a base table takes AccessExclusiveLock on the base table.
# It should not also wait for a lock on an ordinary dependent view that does
# not contain a FOR KEY join.  The view DDL below holds a lock on the view
# only; ALTER TABLE should still complete without waiting for that lock.

setup
{
 CREATE SCHEMA key_join_plain_view_rls_lock;
 CREATE TABLE key_join_plain_view_rls_lock.t
 (
     id int PRIMARY KEY
 );
 CREATE VIEW key_join_plain_view_rls_lock.v AS
 SELECT id
 FROM key_join_plain_view_rls_lock.t;
}

teardown
{
 DROP SCHEMA key_join_plain_view_rls_lock CASCADE;
}

session s1
step s1_lock_view
{
 BEGIN;
 ALTER VIEW key_join_plain_view_rls_lock.v
     SET (security_barrier=true);
}
step s1_commit { COMMIT; }

session s2
step s2_enable_rls
{
 ALTER TABLE key_join_plain_view_rls_lock.t
     ENABLE ROW LEVEL SECURITY;
}

permutation s1_lock_view s2_enable_rls s1_commit
