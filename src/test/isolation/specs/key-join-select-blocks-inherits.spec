# A live key-join SELECT blocks concurrent inheritance DDL on a referenced table.
#
# A key join is sound only while the referenced table has no inheritance
# children (a plain table's unique index does not span its children).  While a
# transaction that ran a key-join SELECT is still open, a concurrent
# CREATE TABLE ... INHERITS on the referenced table must wait for that
# transaction to commit before it can add the first child -- so the child can
# never appear underneath a key join that is still in force.

setup
{
 CREATE SCHEMA key_join_select_blocks_inherits;
 CREATE TABLE key_join_select_blocks_inherits.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_select_blocks_inherits.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL
         REFERENCES key_join_select_blocks_inherits.p (id)
 );
 INSERT INTO key_join_select_blocks_inherits.p VALUES (1), (2), (3);
 INSERT INTO key_join_select_blocks_inherits.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA key_join_select_blocks_inherits CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_key_join
{
 SELECT c.id, c.parent_id
 FROM key_join_select_blocks_inherits.p AS p
 JOIN key_join_select_blocks_inherits.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_create_child
{
 CREATE TABLE key_join_select_blocks_inherits.p_child ()
 INHERITS (key_join_select_blocks_inherits.p);
}

permutation s1_begin s1_key_join s2_create_child s1_commit
