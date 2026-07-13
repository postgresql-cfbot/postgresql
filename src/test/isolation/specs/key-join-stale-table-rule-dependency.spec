# Key-join revalidation races against a dropped table rewrite rule.
#
# pg_depend points from a stored key-join proof to the rewrite rule that owns
# it.  If that rule is dropped after dependency scanning but before the owning
# relation can be opened, the relation may still exist with no rules left.

setup
{
 CREATE SCHEMA key_join_stale_table_rule_dependency;
 CREATE TABLE key_join_stale_table_rule_dependency.p
 (
     id integer PRIMARY KEY
 );
 CREATE TABLE key_join_stale_table_rule_dependency.c
 (
     id integer PRIMARY KEY,
     parent_id integer CONSTRAINT c_parent_id_not_null NOT NULL,
     CONSTRAINT c_parent_id_fkey
         FOREIGN KEY (parent_id)
         REFERENCES key_join_stale_table_rule_dependency.p (id)
 );
 CREATE TABLE key_join_stale_table_rule_dependency.t
 (
     id integer
 );
 INSERT INTO key_join_stale_table_rule_dependency.p VALUES (1);
 INSERT INTO key_join_stale_table_rule_dependency.c VALUES (10, 1);
 CREATE RULE r AS ON INSERT TO key_join_stale_table_rule_dependency.t
 DO ALSO
     SELECT p.id AS pid, c.id AS cid
     FROM key_join_stale_table_rule_dependency.p p
     JOIN key_join_stale_table_rule_dependency.c c
       FOR KEY (parent_id) -> p (id);
}

teardown
{
 DROP SCHEMA key_join_stale_table_rule_dependency CASCADE;
}

session ddl
step ddl_begin { BEGIN; }
step lock_rule_target
{
 LOCK TABLE key_join_stale_table_rule_dependency.t
 IN ACCESS EXCLUSIVE MODE;
}
step drop_rule
{
 DROP RULE r ON key_join_stale_table_rule_dependency.t;
}
step ddl_commit { COMMIT; }

session alterer
step alter_fk
{
 ALTER TABLE key_join_stale_table_rule_dependency.c
 ALTER CONSTRAINT c_parent_id_fkey NOT ENFORCED;
}

permutation ddl_begin lock_rule_target alter_fk drop_rule ddl_commit
