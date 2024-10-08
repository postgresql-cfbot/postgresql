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

--SET stats_fetch_consistency = snapshot;
CREATE TABLE vestat (x int) WITH (autovacuum_enabled = off, fillfactor = 10);
INSERT INTO vestat SELECT x FROM generate_series(1,:sample_size) as x;
ANALYZE vestat;

SELECT oid AS roid from pg_class where relname = 'vestat' \gset

DELETE FROM vestat WHERE x % 2 = 0;
-- Before the first vacuum execution extended stats view is empty.
SELECT vt.relname,pages_frozen,tuples_deleted,relpages,pages_scanned,pages_removed
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid;
SELECT relpages AS rp
FROM pg_class c
WHERE relname = 'vestat' \gset

VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP OFF) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- The table and index extended vacuum statistics should show us that
-- vacuum frozed pages and clean up pages, but pages_removed stayed the same
-- because of not full table have cleaned up
SELECT vt.relname,pages_frozen > 0 AS pages_frozen,tuples_deleted > 0 AS tuples_deleted,relpages-:rp = 0 AS relpages,pages_scanned > 0 AS pages_scanned,pages_removed = 0 AS pages_removed
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid;
SELECT pages_frozen AS fp,tuples_deleted AS td,relpages AS rp, pages_scanned AS ps, pages_removed AS pr
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records AS hwr,wal_bytes AS hwb,wal_fpi AS hfpi FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

-- Look into WAL records deltas.
SELECT wal_records > 0 AS dWR, wal_bytes > 0 AS dWB
FROM pg_stat_vacuum_tables WHERE relname = 'vestat';

DELETE FROM vestat;;
VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP OFF) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- pages_removed must be increased
SELECT vt.relname,pages_frozen-:fp > 0 AS pages_frozen,tuples_deleted-:td > 0 AS tuples_deleted,relpages -:rp = 0 AS relpages,pages_scanned-:ps > 0 AS pages_scanned,pages_removed-:pr > 0 AS pages_removed
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid;
SELECT pages_frozen AS fp,tuples_deleted AS td,relpages AS rp, pages_scanned AS ps, pages_removed AS pr
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records-:hwr AS dwr, wal_bytes-:hwb AS dwb, wal_fpi-:hfpi AS dfpi
FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

-- WAL advance should be detected.
SELECT :dwr > 0 AS dWR, :dwb > 0 AS dWB;

-- Store WAL advances into variables
SELECT wal_records AS hwr,wal_bytes AS hwb,wal_fpi AS hfpi FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

INSERT INTO vestat SELECT x FROM generate_series(1,:sample_size) as x;
DELETE FROM vestat WHERE x % 2 = 0;
-- VACUUM FULL doesn't report to stat collector. So, no any advancements of statistics
-- are detected here.
VACUUM FULL vestat;
-- It is necessary to check the wal statistics
CHECKPOINT;

-- Store WAL advances into variables
SELECT wal_records-:hwr AS dwr2, wal_bytes-:hwb AS dwb2, wal_fpi-:hfpi AS dfpi2
FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

-- WAL and other statistics advance should not be detected.
SELECT :dwr2=0 AS dWR, :dfpi2=0 AS dFPI, :dwb2=0 AS dWB;

SELECT vt.relname,pages_frozen-:fp = 0 AS pages_frozen,tuples_deleted-:td = 0 AS tuples_deleted,relpages -:rp < 0 AS relpages,pages_scanned-:ps = 0 AS pages_scanned,pages_removed-:pr = 0 AS pages_removed
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid;
SELECT pages_frozen AS fp,tuples_deleted AS td,relpages AS rp, pages_scanned AS ps,pages_removed AS pr
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid \gset

-- Store WAL advances into variables
SELECT wal_records AS hwr,wal_bytes AS hwb,wal_fpi AS hfpi FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

DELETE FROM vestat;
TRUNCATE vestat;
VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128, INDEX_CLEANUP OFF) vestat;
-- it is necessary to check the wal statistics
CHECKPOINT;

-- Store WAL advances into variables after removing all tuples from the table
SELECT wal_records-:hwr AS dwr3, wal_bytes-:hwb AS dwb3, wal_fpi-:hfpi AS dfpi3
FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

--There are nothing changed
SELECT :dwr3>0 AS dWR, :dfpi3=0 AS dFPI, :dwb3>0 AS dWB;

--
-- Now, the table and index is compressed into zero number of pages. Check it
-- in vacuum extended statistics.
-- The pages_frozen, pages_scanned values shouldn't be changed
--
SELECT vt.relname,pages_frozen-:fp = 0 AS pages_frozen,tuples_deleted-:td = 0 AS tuples_deleted,relpages -:rp = 0 AS relpages,pages_scanned-:ps = 0 AS pages_scanned,pages_removed-:pr = 0 AS pages_removed
FROM pg_stat_vacuum_tables vt, pg_class c
WHERE vt.relname = 'vestat' AND vt.relid = c.oid;

INSERT INTO vestat SELECT x FROM generate_series(1,:sample_size) as x;
ANALYZE vestat;

-- must be empty
SELECT pages_frozen, pages_all_visible, rev_all_frozen_pages,rev_all_visible_pages
FROM pg_stat_vacuum_tables WHERE relname = 'vestat';

VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128) vestat;

-- backend defreezed pages
SELECT pages_frozen > 0 AS pages_frozen,pages_all_visible > 0 AS pages_all_visible,rev_all_frozen_pages = 0 AS rev_all_frozen_pages,rev_all_visible_pages = 0 AS rev_all_visible_pages
FROM pg_stat_vacuum_tables WHERE relname = 'vestat';
SELECT pages_frozen AS pf, pages_all_visible AS pv, rev_all_frozen_pages AS hafp,rev_all_visible_pages AS havp
FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

UPDATE vestat SET x = x1001;
VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128) vestat;

SELECT pages_frozen > :pf AS pages_frozen,pages_all_visible > :pv AS pages_all_visible,rev_all_frozen_pages > :hafp AS rev_all_frozen_pages,rev_all_visible_pages > :havp AS rev_all_visible_pages
FROM pg_stat_vacuum_tables WHERE relname = 'vestat';
SELECT pages_frozen AS pf, pages_all_visible AS pv, rev_all_frozen_pages AS hafp,rev_all_visible_pages AS havp
FROM pg_stat_vacuum_tables WHERE relname = 'vestat' \gset

VACUUM (PARALLEL 0, BUFFER_USAGE_LIMIT 128) vestat;

-- vacuum freezed pages
SELECT pages_frozen = :pf AS pages_frozen,pages_all_visible = :pv AS pages_all_visible,rev_all_frozen_pages = :hafp AS rev_all_frozen_pages,rev_all_visible_pages = :havp AS rev_all_visible_pages
FROM pg_stat_vacuum_tables WHERE relname = 'vestat';

SELECT min(relid) FROM pg_stat_vacuum_tables(0) where relid > 0;

DROP TABLE vestat CASCADE;