# CREATE VIEW with a key join blocks concurrent DROP INDEX CONCURRENTLY.
#
# DROP INDEX CONCURRENTLY on a usable unique index of a key join operand table
# waits until the CREATE VIEW transaction commits.  The standalone unique index
# p_code_idx is not constraint-backed, so it can be dropped concurrently.

setup
{
 CREATE SCHEMA kj_proof_blocks_drop_idx;
 CREATE TABLE kj_proof_blocks_drop_idx.p
 (
     id int PRIMARY KEY,
     code int NOT NULL
 );
 CREATE UNIQUE INDEX p_code_idx ON kj_proof_blocks_drop_idx.p (code);
 CREATE TABLE kj_proof_blocks_drop_idx.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL REFERENCES kj_proof_blocks_drop_idx.p (id)
 );
 INSERT INTO kj_proof_blocks_drop_idx.p VALUES (1, 10), (2, 20), (3, 30);
 INSERT INTO kj_proof_blocks_drop_idx.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA kj_proof_blocks_drop_idx CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_create_view
{
 CREATE VIEW kj_proof_blocks_drop_idx.v AS
 SELECT c.id, c.parent_id
 FROM kj_proof_blocks_drop_idx.p AS p
 JOIN kj_proof_blocks_drop_idx.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_drop_index
{
 DROP INDEX CONCURRENTLY kj_proof_blocks_drop_idx.p_code_idx;
}

permutation s1_begin s1_create_view s2_drop_index s1_commit
