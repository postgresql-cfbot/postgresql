# CREATE MATERIALIZED VIEW with a key join blocks concurrent inheritance DDL.
#
# The matview stores a _RETURN rule with a key-join proof.  While the matview
# transaction is still open, a concurrent CREATE TABLE ... INHERITS on the
# referenced table must wait for the stored-proof lock, then revalidate against
# the committed matview proof.

setup
{
 CREATE SCHEMA key_join_matview_stored_proof_blocks_inherits;
 CREATE TABLE key_join_matview_stored_proof_blocks_inherits.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_matview_stored_proof_blocks_inherits.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL
         REFERENCES key_join_matview_stored_proof_blocks_inherits.p (id)
 );
 INSERT INTO key_join_matview_stored_proof_blocks_inherits.p VALUES (1), (2), (3);
 INSERT INTO key_join_matview_stored_proof_blocks_inherits.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA key_join_matview_stored_proof_blocks_inherits CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_create_matview
{
 CREATE MATERIALIZED VIEW key_join_matview_stored_proof_blocks_inherits.mv AS
 SELECT c.id, c.parent_id
 FROM key_join_matview_stored_proof_blocks_inherits.p AS p
 JOIN key_join_matview_stored_proof_blocks_inherits.c AS c
 FOR KEY (parent_id) -> p (id)
 WITH NO DATA;
}
step s1_commit { COMMIT; }

session s2
step s2_create_child
{
 CREATE TABLE key_join_matview_stored_proof_blocks_inherits.p_child ()
 INHERITS (key_join_matview_stored_proof_blocks_inherits.p);
}

permutation s1_begin s1_create_matview s2_create_child s1_commit
