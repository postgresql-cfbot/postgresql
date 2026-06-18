# A stored key-join proof does not block routine maintenance on a referenced table.
#
# While a transaction holding a key-join view is still open, concurrent ANALYZE
# and VACUUM on the referenced table complete immediately, without waiting for
# the proof's transaction to commit.  (The proof only needs to fence off the
# addition of inheritance children, not ordinary table maintenance.)

setup
{
 CREATE SCHEMA key_join_proof_allows_maintenance;
 CREATE TABLE key_join_proof_allows_maintenance.p
 (
     id int PRIMARY KEY
 );
 CREATE TABLE key_join_proof_allows_maintenance.c
 (
     id int PRIMARY KEY,
     parent_id int NOT NULL
         REFERENCES key_join_proof_allows_maintenance.p (id)
 );
 INSERT INTO key_join_proof_allows_maintenance.p VALUES (1), (2), (3);
 INSERT INTO key_join_proof_allows_maintenance.c VALUES (10, 1), (20, 2);
}

teardown
{
 DROP SCHEMA key_join_proof_allows_maintenance CASCADE;
}

session s1
step s1_begin { BEGIN; }
step s1_create_view
{
 CREATE VIEW key_join_proof_allows_maintenance.v AS
 SELECT c.id, c.parent_id
 FROM key_join_proof_allows_maintenance.p AS p
 JOIN key_join_proof_allows_maintenance.c AS c
 FOR KEY (parent_id) -> p (id);
}
step s1_commit { COMMIT; }

session s2
step s2_analyze { ANALYZE key_join_proof_allows_maintenance.p; }
step s2_vacuum  { VACUUM key_join_proof_allows_maintenance.p; }

permutation s1_begin s1_create_view s2_analyze s2_vacuum s1_commit
