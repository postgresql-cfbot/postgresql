--
-- VACUUM
--

CREATE FUNCTION wait_barrier() RETURNS void LANGUAGE plpgsql AS $$
DECLARE vxids text[];
BEGIN
 -- wait for all transactions to commit to make deleted tuples vacuumable
 SELECT array_agg(DISTINCT virtualtransaction) FROM pg_locks WHERE pid <> pg_backend_pid() INTO vxids;
 WHILE (SELECT count(virtualtransaction) FROM pg_locks WHERE pid <> pg_backend_pid() AND virtualtransaction = ANY(vxids)) > 0 LOOP
    PERFORM pg_sleep(0.1);
 END LOOP;
END
$$;

CREATE TABLE vactst (i INT) WITH (autovacuum_enabled = false);
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT wait_barrier();
SELECT * FROM vactst;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT wait_barrier();
VACUUM (FULL) vactst;
DELETE FROM vactst;
SELECT wait_barrier();
SELECT * FROM vactst;

VACUUM (FULL, FREEZE) vactst;
VACUUM (ANALYZE, FULL) vactst;

CREATE TABLE vaccluster (i INT PRIMARY KEY);
ALTER TABLE vaccluster CLUSTER ON vaccluster_pkey;
CLUSTER vaccluster;

CREATE FUNCTION do_analyze() RETURNS VOID VOLATILE LANGUAGE SQL
	AS 'ANALYZE pg_am';
CREATE FUNCTION wrap_do_analyze(c INT) RETURNS INT IMMUTABLE LANGUAGE SQL
	AS 'SELECT $1 FROM do_analyze()';
CREATE INDEX ON vaccluster(wrap_do_analyze(i));
INSERT INTO vaccluster VALUES (1), (2);
ANALYZE vaccluster;

-- Test ANALYZE in transaction, where the transaction surrounding
-- analyze performed modifications. This tests for the bug at
-- https://postgr.es/m/c7988239-d42c-ddc4-41db-171b23b35e4f%40ssinger.info
-- (which hopefully is unlikely to be reintroduced), but also seems
-- independently worthwhile to cover.
INSERT INTO vactst SELECT generate_series(1, 300);
DELETE FROM vactst WHERE i % 7 = 0; -- delete a few rows outside
BEGIN;
INSERT INTO vactst SELECT generate_series(301, 400);
DELETE FROM vactst WHERE i % 5 <> 0; -- delete a few rows inside
ANALYZE vactst;
COMMIT;

VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_database;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

VACUUM (DISABLE_PAGE_SKIPPING) vaccluster;

-- INDEX_CLEANUP option
CREATE TABLE no_index_cleanup (i INT PRIMARY KEY, t TEXT);
-- Use uncompressed data stored in toast.
CREATE INDEX no_index_cleanup_idx ON no_index_cleanup(t);
ALTER TABLE no_index_cleanup ALTER COLUMN t SET STORAGE EXTERNAL;
INSERT INTO no_index_cleanup(i, t) VALUES (generate_series(1,30),
    repeat('1234567890',300));
-- index cleanup option is ignored if VACUUM FULL
VACUUM (INDEX_CLEANUP TRUE, FULL TRUE) no_index_cleanup;
VACUUM (FULL TRUE) no_index_cleanup;
-- Toast inherits the value from its parent table.
ALTER TABLE no_index_cleanup SET (vacuum_index_cleanup = false);
DELETE FROM no_index_cleanup WHERE i < 15;
-- Nothing is cleaned up.
VACUUM no_index_cleanup;
-- Both parent relation and toast are cleaned up.
ALTER TABLE no_index_cleanup SET (vacuum_index_cleanup = true);
VACUUM no_index_cleanup;
-- Parameter is set for both the parent table and its toast relation.
INSERT INTO no_index_cleanup(i, t) VALUES (generate_series(31,60),
    repeat('1234567890',300));
DELETE FROM no_index_cleanup WHERE i < 45;
-- Only toast index is cleaned up.
ALTER TABLE no_index_cleanup SET (vacuum_index_cleanup = false,
    toast.vacuum_index_cleanup = true);
VACUUM no_index_cleanup;
-- Only parent is cleaned up.
ALTER TABLE no_index_cleanup SET (vacuum_index_cleanup = true,
    toast.vacuum_index_cleanup = false);
VACUUM no_index_cleanup;
-- Test some extra relations.
VACUUM (INDEX_CLEANUP FALSE) vaccluster;
VACUUM (INDEX_CLEANUP FALSE) vactst; -- index cleanup option is ignored if no indexes
VACUUM (INDEX_CLEANUP FALSE, FREEZE TRUE) vaccluster;

-- TRUNCATE option
CREATE TABLE vac_truncate_test(i INT NOT NULL, j text)
	WITH (vacuum_truncate=true, autovacuum_enabled=false);
INSERT INTO vac_truncate_test VALUES (1, NULL), (NULL, NULL);
VACUUM (TRUNCATE FALSE) vac_truncate_test;
SELECT pg_relation_size('vac_truncate_test') > 0;
VACUUM vac_truncate_test;
SELECT pg_relation_size('vac_truncate_test') = 0;
VACUUM (TRUNCATE FALSE, FULL TRUE) vac_truncate_test;
DROP TABLE vac_truncate_test;

-- partitioned table
CREATE TABLE vacparted (a int, b char) PARTITION BY LIST (a);
CREATE TABLE vacparted1 PARTITION OF vacparted FOR VALUES IN (1);
INSERT INTO vacparted VALUES (1, 'a');
UPDATE vacparted SET b = 'b';
VACUUM (ANALYZE) vacparted;
VACUUM (FULL) vacparted;
VACUUM (FREEZE) vacparted;

-- check behavior with duplicate column mentions
VACUUM ANALYZE vacparted(a,b,a);
ANALYZE vacparted(a,b,b);

-- multiple tables specified
VACUUM vaccluster, vactst;
VACUUM vacparted, does_not_exist;
VACUUM (FREEZE) vacparted, vaccluster, vactst;
VACUUM (FREEZE) does_not_exist, vaccluster;
VACUUM ANALYZE vactst, vacparted (a);
VACUUM ANALYZE vactst (does_not_exist), vacparted (b);
VACUUM FULL vacparted, vactst;
VACUUM FULL vactst, vacparted (a, b), vaccluster (i);
ANALYZE vactst, vacparted;
ANALYZE vacparted (b), vactst;
ANALYZE vactst, does_not_exist, vacparted;
ANALYZE vactst (i), vacparted (does_not_exist);
ANALYZE vactst, vactst;
BEGIN;  -- ANALYZE behaves differently inside a transaction block
ANALYZE vactst, vactst;
COMMIT;

-- parenthesized syntax for ANALYZE
ANALYZE (VERBOSE) does_not_exist;
ANALYZE (nonexistent-arg) does_not_exist;
ANALYZE (nonexistentarg) does_not_exit;

-- large mwm vacuum runs
CREATE UNLOGGED TABLE vactst2 (i INT) WITH (autovacuum_enabled = false);
INSERT INTO vactst2 SELECT * from generate_series(1,300000);
CREATE INDEX ix_vactst2 ON vactst2 (i);
DELETE FROM vactst2 WHERE i % 4 != 1;
SET maintenance_work_mem = 1024;
SELECT wait_barrier();
VACUUM vactst2;
SET maintenance_work_mem TO DEFAULT;
DROP INDEX ix_vactst2;
TRUNCATE TABLE vactst2;
 
INSERT INTO vactst2 SELECT * from generate_series(1,40);
CREATE INDEX ix_vactst2 ON vactst2 (i);
DELETE FROM vactst2;
SELECT wait_barrier();
VACUUM vactst2;
EXPLAIN (ANALYZE, BUFFERS, COSTS off, TIMING off, SUMMARY off) SELECT * FROM vactst2;
SELECT count(*) FROM vactst2;
DROP INDEX ix_vactst2;
TRUNCATE TABLE vactst2;

-- ensure argument order independence, and that SKIP_LOCKED on non-existing
-- relation still errors out.  Suppress WARNING messages caused by concurrent
-- autovacuums.
SET client_min_messages TO 'ERROR';
ANALYZE (SKIP_LOCKED, VERBOSE) does_not_exist;
ANALYZE (VERBOSE, SKIP_LOCKED) does_not_exist;

-- SKIP_LOCKED option
VACUUM (SKIP_LOCKED) vactst;
VACUUM (SKIP_LOCKED, FULL) vactst;
ANALYZE (SKIP_LOCKED) vactst;
RESET client_min_messages;

-- ensure VACUUM and ANALYZE don't have a problem with serializable
SET default_transaction_isolation = serializable;
VACUUM vactst;
ANALYZE vactst;
RESET default_transaction_isolation;
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
ANALYZE vactst;
COMMIT;

DROP TABLE vaccluster;
DROP TABLE vactst;
DROP TABLE vacparted;
DROP TABLE no_index_cleanup;

-- relation ownership, WARNING logs generated as all are skipped.
CREATE TABLE vacowned (a int);
CREATE TABLE vacowned_parted (a int) PARTITION BY LIST (a);
CREATE TABLE vacowned_part1 PARTITION OF vacowned_parted FOR VALUES IN (1);
CREATE TABLE vacowned_part2 PARTITION OF vacowned_parted FOR VALUES IN (2);
CREATE ROLE regress_vacuum;
SET ROLE regress_vacuum;
-- Simple table
VACUUM vacowned;
ANALYZE vacowned;
VACUUM (ANALYZE) vacowned;
-- Catalog
VACUUM pg_catalog.pg_class;
ANALYZE pg_catalog.pg_class;
VACUUM (ANALYZE) pg_catalog.pg_class;
-- Shared catalog
VACUUM pg_catalog.pg_authid;
ANALYZE pg_catalog.pg_authid;
VACUUM (ANALYZE) pg_catalog.pg_authid;
-- Partitioned table and its partitions, nothing owned by other user.
-- Relations are not listed in a single command to test ownership
-- independently.
VACUUM vacowned_parted;
VACUUM vacowned_part1;
VACUUM vacowned_part2;
ANALYZE vacowned_parted;
ANALYZE vacowned_part1;
ANALYZE vacowned_part2;
VACUUM (ANALYZE) vacowned_parted;
VACUUM (ANALYZE) vacowned_part1;
VACUUM (ANALYZE) vacowned_part2;
RESET ROLE;
-- Partitioned table and one partition owned by other user.
ALTER TABLE vacowned_parted OWNER TO regress_vacuum;
ALTER TABLE vacowned_part1 OWNER TO regress_vacuum;
SET ROLE regress_vacuum;
VACUUM vacowned_parted;
VACUUM vacowned_part1;
VACUUM vacowned_part2;
ANALYZE vacowned_parted;
ANALYZE vacowned_part1;
ANALYZE vacowned_part2;
VACUUM (ANALYZE) vacowned_parted;
VACUUM (ANALYZE) vacowned_part1;
VACUUM (ANALYZE) vacowned_part2;
RESET ROLE;
-- Only one partition owned by other user.
ALTER TABLE vacowned_parted OWNER TO CURRENT_USER;
SET ROLE regress_vacuum;
VACUUM vacowned_parted;
VACUUM vacowned_part1;
VACUUM vacowned_part2;
ANALYZE vacowned_parted;
ANALYZE vacowned_part1;
ANALYZE vacowned_part2;
VACUUM (ANALYZE) vacowned_parted;
VACUUM (ANALYZE) vacowned_part1;
VACUUM (ANALYZE) vacowned_part2;
RESET ROLE;
-- Only partitioned table owned by other user.
ALTER TABLE vacowned_parted OWNER TO regress_vacuum;
ALTER TABLE vacowned_part1 OWNER TO CURRENT_USER;
SET ROLE regress_vacuum;
VACUUM vacowned_parted;
VACUUM vacowned_part1;
VACUUM vacowned_part2;
ANALYZE vacowned_parted;
ANALYZE vacowned_part1;
ANALYZE vacowned_part2;
VACUUM (ANALYZE) vacowned_parted;
VACUUM (ANALYZE) vacowned_part1;
VACUUM (ANALYZE) vacowned_part2;
RESET ROLE;
DROP TABLE vactst2;
DROP FUNCTION wait_barrier();
DROP TABLE vacowned;
DROP TABLE vacowned_parted;
DROP ROLE regress_vacuum;
