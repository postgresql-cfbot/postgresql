# Two concurrent CREATE OR REPLACE VIEWs with key joins block each other.
#
# Both views depend on the same unique index via their key join proofs.  The
# second CREATE VIEW waits until the first transaction commits.

setup
{
 CREATE SCHEMA kj_proof_blocks_view;
 CREATE TABLE kj_proof_blocks_view.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE kj_proof_blocks_view.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL REFERENCES kj_proof_blocks_view.p (id)
 );
 INSERT INTO kj_proof_blocks_view.p VALUES (1), (2), (3);
 INSERT INTO kj_proof_blocks_view.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA kj_proof_blocks_view CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_create_view
{
 CREATE OR REPLACE VIEW kj_proof_blocks_view.v1 AS
 SELECT c.id, c.parent_id
 FROM kj_proof_blocks_view.p AS p
 JOIN kj_proof_blocks_view.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_create_view
{
 CREATE OR REPLACE VIEW kj_proof_blocks_view.v2 AS
 SELECT c.id, c.parent_id
 FROM kj_proof_blocks_view.p AS p
 JOIN kj_proof_blocks_view.c AS c
 FOR KEY (parent_id) -> p (id);
}

permutation s1_begin s1_create_view s2_create_view s1_commit
