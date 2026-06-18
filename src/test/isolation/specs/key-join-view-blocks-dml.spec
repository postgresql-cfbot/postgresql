# CREATE VIEW with a key join does not block concurrent DML.
#
# A concurrent INSERT into a table referenced by a key join view completes
# immediately, without waiting for the CREATE VIEW transaction to commit.

setup
{
 CREATE SCHEMA key_join_view_blocks_dml;
 CREATE TABLE key_join_view_blocks_dml.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_view_blocks_dml.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL REFERENCES key_join_view_blocks_dml.p (id)
 );
 INSERT INTO key_join_view_blocks_dml.p VALUES (1), (2), (3);
 INSERT INTO key_join_view_blocks_dml.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA key_join_view_blocks_dml CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_create_view
{
 CREATE VIEW key_join_view_blocks_dml.v AS
 SELECT c.id, c.parent_id
 FROM key_join_view_blocks_dml.p AS p
 JOIN key_join_view_blocks_dml.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_insert_parent
{
 INSERT INTO key_join_view_blocks_dml.p VALUES (99);
}

permutation s1_begin s1_create_view s2_insert_parent s1_commit
