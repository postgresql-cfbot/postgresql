-- Regression test: REINDEX CONCURRENTLY must not wedge a stored FOR KEY proof.
--
-- A stored proof that consumes a uniqueness fact from a bare unique index (one
-- with no owning constraint) records its evidence on that index.  REINDEX INDEX
-- CONCURRENTLY rebuilds the index under a fresh OID.  pg_depend is the single
-- source of truth for a proof's evidence, so a later revalidation re-derives the
-- proof and reconciles pg_depend to whatever unique evidence the current catalog
-- supports -- rather than failing because the recorded OID changed.

CREATE TABLE kjr_t1 (c1 int NOT NULL, c2 int);
-- Bare unique index created first -> lower OID -> consumed by the proof.
CREATE UNIQUE INDEX kjr_t1_c1_bare ON kjr_t1 (c1);
-- A unique constraint as well, so an FK can reference c1.
ALTER TABLE kjr_t1 ADD CONSTRAINT kjr_t1_c1_key UNIQUE (c1);
CREATE TABLE kjr_t2 (c3 int NOT NULL REFERENCES kjr_t1 (c1), c4 int);
INSERT INTO kjr_t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO kjr_t2 VALUES (1, 100), (3, 300);

-- Stored proof over ONLY scans, so attaching a child to the referencing table
-- forces revalidation without changing kjr_t1's own facts.
CREATE VIEW kjr_v AS
  SELECT kjr_t2.c3, kjr_t2.c4
  FROM ONLY kjr_t1 JOIN ONLY kjr_t2 FOR KEY (c3) -> kjr_t1 (c1);

-- Precondition: the proof's uniqueness evidence is the bare index.
SELECT d.refobjid::regclass AS unique_evidence
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
WHERE r.ev_class = 'kjr_v'::regclass AND d.deptype = 'k'
  AND d.refobjid IN ('kjr_t1_c1_bare'::regclass, 'kjr_t1_c1_key'::regclass);

SELECT * FROM kjr_v ORDER BY c3;

-- Rebuild the bare index: it gets a fresh OID and pg_depend is relinked to it.
REINDEX INDEX CONCURRENTLY kjr_t1_c1_bare;

-- Force revalidation of the stored proof.  Before the fix this raised
-- "stored key join proof would require new dependencies"; now the proof is
-- re-derived and pg_depend reconciled to the evidence the catalog supports.
CREATE TABLE kjr_t2_child () INHERITS (kjr_t2);

-- The proof no longer depends on the bare index ...
SELECT count(*) AS still_on_bare_index
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
WHERE r.ev_class = 'kjr_v'::regclass AND d.deptype = 'k'
  AND d.refobjid = 'kjr_t1_c1_bare'::regclass;

-- ... it reconciled onto the constraint-backed uniqueness instead.
SELECT c.conname AS unique_evidence
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid AND d.classid = 'pg_rewrite'::regclass
JOIN pg_constraint c ON c.oid = d.refobjid AND d.refclassid = 'pg_constraint'::regclass
WHERE r.ev_class = 'kjr_v'::regclass AND d.deptype = 'k' AND c.contype = 'u';

SELECT * FROM kjr_v ORDER BY c3;

DROP TABLE kjr_t2_child;
DROP VIEW kjr_v;
DROP TABLE kjr_t2;
DROP TABLE kjr_t1;
