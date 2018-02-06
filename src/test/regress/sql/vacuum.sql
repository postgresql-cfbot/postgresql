--
-- VACUUM
--

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
VACUUM (FULL) vactst;
DELETE FROM vactst;
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

VACUUM FULL pg_am;
VACUUM FULL pg_class;
VACUUM FULL pg_database;
VACUUM FULL vaccluster;
VACUUM FULL vactst;

VACUUM (DISABLE_PAGE_SKIPPING) vaccluster;

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

-- large mwm vacuum runs
INSERT INTO vactst SELECT * from generate_series(1,400000);
CREATE INDEX ix_vactst ON vactst (i);
DELETE FROM vactst WHERE i in (SELECT i FROM vactst ORDER BY random() LIMIT 300000);
SET maintenance_work_mem = 1024;
VACUUM vactst;
SET maintenance_work_mem TO DEFAULT;
DROP INDEX ix_vactst;
TRUNCATE TABLE vactst;

INSERT INTO vactst SELECT * from generate_series(1,40);
CREATE INDEX ix_vactst ON vactst (i);
DELETE FROM vactst;
VACUUM vactst;
EXPLAIN (ANALYZE, BUFFERS, COSTS off, TIMING off, SUMMARY off) SELECT * FROM vactst;
SELECT count(*) FROM vactst;
DROP INDEX ix_vactst;
TRUNCATE TABLE vactst;

DROP TABLE vaccluster;
DROP TABLE vactst;
DROP TABLE vacparted;
