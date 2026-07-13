# Ordinary relation DDL must not hold locks on ordinary dependent views due
# to key-join proof prelocking.
#
# v has a normal view dependency on t and contains no stored FOR KEY proof.
# Session s1 changes t in a way that triggers key-join dependent prelocking
# and keeps the transaction open.  Altering v in session s2 should not wait
# on s1.

setup
{
 CREATE SCHEMA key_join_plain_relation_dependent_lock;
 CREATE TABLE key_join_plain_relation_dependent_lock.t
 (
     id int PRIMARY KEY
 );
 CREATE VIEW key_join_plain_relation_dependent_lock.v AS
 SELECT id
 FROM key_join_plain_relation_dependent_lock.t;
}

teardown
{
 DROP SCHEMA key_join_plain_relation_dependent_lock CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_enable_rls
{
 ALTER TABLE key_join_plain_relation_dependent_lock.t
     ENABLE ROW LEVEL SECURITY;
}
step s1_commit { COMMIT; }

session s2
step s2_alter_view
{
 ALTER VIEW key_join_plain_relation_dependent_lock.v
     SET (security_barrier=true);
}

permutation s1_begin s1_enable_rls s2_alter_view s1_commit
