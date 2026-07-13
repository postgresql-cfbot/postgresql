# A key join SELECT does not block concurrent DML.
#
# A concurrent INSERT into a table referenced by a key join SELECT completes
# immediately, without waiting for the SELECT's transaction to commit.

setup
{
 CREATE SCHEMA key_join_select_blocks_dml;
 CREATE TABLE key_join_select_blocks_dml.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_select_blocks_dml.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL REFERENCES key_join_select_blocks_dml.p (id)
 );
 INSERT INTO key_join_select_blocks_dml.p VALUES (1), (2), (3);
 INSERT INTO key_join_select_blocks_dml.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA key_join_select_blocks_dml CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_key_join
{
 SELECT c.id, c.parent_id
 FROM key_join_select_blocks_dml.p AS p
 JOIN key_join_select_blocks_dml.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_insert_parent
{
 INSERT INTO key_join_select_blocks_dml.p VALUES (99);
}

permutation s1_begin s1_key_join s2_insert_parent s1_commit
