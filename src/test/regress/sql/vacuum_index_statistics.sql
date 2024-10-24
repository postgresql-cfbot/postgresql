--
-- Test cumulative vacuum stats system
--
-- Check the wall statistics collected during vacuum operation:
-- number of frozen and visible pages set by vacuum;
-- number of frozen and visible pages removed by backend.
-- Statistic wal_fpi is not displayed in this test because its behavior is unstable.
--
-- conditio sine qua non
SHOW track_counts;  -- must be on
-- not enabled by default, but we want to test it...
SET track_functions TO 'all';


-- ensure pending stats are flushed
SELECT pg_stat_force_next_flush();

\set sample_size 10000
SET vacuum_freeze_min_age = 0;
SET vacuum_freeze_table_age = 0;
--SET stats_fetch_consistency = snapshot;
CREATE TABLE vestat (x int primary key) WITH (autovacuum_enabled = off, fillfactor = 10);
INSERT INTO vestat SELECT x FROM generate_series(1,:sample_size) as x;
ANALYZE vestat;

SELECT oid AS ioid from pg_class where relname = 'vestat_pkey' \gset

DELETE FROM vestat WHERE x % 2 = 0;
-- Before the first vacuum execution extended stats view is empty.
SELECT vt.relname,relpages,pages_deleted,tuples_deleted
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid;
SELECT relpages AS irp
FROM pg_class c
WHERE relname = 'vestat_pkey' \gset

VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP ON) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- The table and index extended vacuum statistics should show us that
-- vacuum frozed pages and clean up pages, but pages_removed stayed the same
-- because of not full table have cleaned up
SELECT vt.relname,relpages-:irp = 0 AS relpages,pages_deleted = 0 AS pages_deleted,tuples_deleted > 0 AS tuples_deleted
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid;
SELECT vt.relname,relpages AS irp,pages_deleted AS ipd,tuples_deleted AS itd
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records AS iwr,wal_bytes AS iwb,wal_fpi AS ifpi FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

-- Look into WAL records deltas.
SELECT wal_records > 0 AS diWR, wal_bytes > 0 AS diWB
FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey';

DELETE FROM vestat;;
VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP ON) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- pages_removed must be increased
SELECT vt.relname,relpages-:irp = 0 AS relpages,pages_deleted-:ipd > 0 AS pages_deleted,tuples_deleted-:itd > 0 AS tuples_deleted
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid;
SELECT vt.relname,relpages AS irp,pages_deleted AS ipd,tuples_deleted AS itd
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records-:iwr AS diwr, wal_bytes-:iwb AS diwb, wal_fpi-:ifpi AS difpi
FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

-- WAL advance should be detected.
SELECT :diwr > 0 AS diWR, :diwb > 0 AS diWB;

-- Store WAL advances into variables
SELECT wal_records AS iwr,wal_bytes AS iwb,wal_fpi AS ifpi FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

INSERT INTO vestat SELECT x FROM generate_series(1,:sample_size) as x;
DELETE FROM vestat WHERE x % 2 = 0;
-- VACUUM FULL doesn't report to stat collector. So, no any advancements of statistics
-- are detected here.
VACUUM FULL vestat;
-- It is necessary to check the wal statistics
CHECKPOINT;

-- Store WAL advances into variables
SELECT wal_records-:iwr AS diwr2, wal_bytes-:iwb AS diwb2, wal_fpi-:ifpi AS difpi2
FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

-- WAL and other statistics advance should not be detected.
SELECT :diwr2=0 AS diWR, :difpi2=0 AS iFPI, :diwb2=0 AS diWB;

SELECT vt.relname,relpages-:irp < 0 AS relpages,pages_deleted-:ipd = 0 AS pages_deleted,tuples_deleted-:itd = 0 AS tuples_deleted
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid;
SELECT vt.relname,relpages AS irp,pages_deleted AS ipd,tuples_deleted AS itd
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records AS iwr,wal_bytes AS iwb,wal_fpi AS ifpi FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

DELETE FROM vestat;
TRUNCATE vestat;
VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP ON) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- Store WAL advances into variables after removing all tuples from the table
SELECT wal_records-:iwr AS diwr3, wal_bytes-:iwb AS diwb3, wal_fpi-:ifpi AS difpi3
FROM pg_stat_vacuum_indexes WHERE relname = 'vestat_pkey' \gset

--There are nothing changed
SELECT :diwr3=0 AS diWR, :difpi3=0 AS iFPI, :diwb3=0 AS diWB;

--
-- Now, the table and index is compressed into zero number of pages. Check it
-- in vacuum extended statistics.
-- The pages_frozen, pages_scanned values shouldn't be changed
--
SELECT vt.relname,relpages-:irp = 0 AS relpages,pages_deleted-:ipd = 0 AS pages_deleted,tuples_deleted-:itd = 0 AS tuples_deleted
FROM pg_stat_vacuum_indexes vt, pg_class c
WHERE vt.relname = 'vestat_pkey' AND vt.relid = c.oid;

SELECT min(relid) FROM pg_stat_vacuum_indexes(0);

DROP TABLE vestat;
