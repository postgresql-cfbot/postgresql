--
-- Extended vacuum statistics views (pg_stat_vacuum_tables, _indexes, _database)
--
SET track_vacuum_statistics = on;

CREATE TABLE vacstat_t (id int PRIMARY KEY, v text)
  WITH (autovacuum_enabled = off);
INSERT INTO vacstat_t SELECT g, repeat('x', 20) FROM generate_series(1, 1000) g;
DELETE FROM vacstat_t WHERE id % 2 = 0;
VACUUM vacstat_t;
SELECT pg_stat_force_next_flush();

-- core heap-page and tuple metrics.  This VACUUM runs without concurrent
-- activity: the surviving tuples are too fresh to be frozen (tuples_frozen = 0)
-- and the interleaved deletions leave no trailing empty pages to truncate
-- (pages_removed = 0).
SELECT pages_scanned > 0 AS pages_scanned,
       pages_removed = 0 AS pages_removed,
       tuples_deleted = 500 AS tuples_deleted,
       tuples_frozen = 0 AS tuples_frozen
  FROM pg_stat_vacuum_tables WHERE relname = 'vacstat_t';

-- pages_removed path: deleting every tuple lets VACUUM truncate the now-empty
-- trailing heap pages, so pages_removed advances.
CREATE TABLE vacstat_trunc (id int)
  WITH (autovacuum_enabled = off);
INSERT INTO vacstat_trunc SELECT generate_series(1, 10000);
DELETE FROM vacstat_trunc;
VACUUM vacstat_trunc;
SELECT pg_stat_force_next_flush();
SELECT pages_removed > 0 AS pages_removed,
       tuples_deleted = 10000 AS tuples_deleted
  FROM pg_stat_vacuum_tables WHERE relname = 'vacstat_trunc';
DROP TABLE vacstat_trunc;

-- tuples_frozen path: an aggressive VACUUM (FREEZE) freezes all live tuples,
-- so tuples_frozen advances.
CREATE TABLE vacstat_freeze (x int)
  WITH (autovacuum_enabled = off);
INSERT INTO vacstat_freeze SELECT generate_series(1, 1000);
VACUUM (FREEZE) vacstat_freeze;
SELECT pg_stat_force_next_flush();
SELECT tuples_frozen > 0 AS tuples_frozen
  FROM pg_stat_vacuum_tables WHERE relname = 'vacstat_freeze';
DROP TABLE vacstat_freeze;

-- per-index view: the primary key index is processed by the same VACUUM.
-- No btree leaf empties out (interleaved deletions), so pages_deleted = 0,
-- while every index entry for a removed heap tuple is deleted.
SELECT indexrelname,
       pages_deleted = 0 AS pages_deleted,
       tuples_deleted = 500 AS tuples_deleted
  FROM pg_stat_vacuum_indexes WHERE relname = 'vacstat_t' ORDER BY indexrelname;

-- index page-deletion path: deleting a contiguous key range empties whole
-- btree leaf pages, which VACUUM then deletes (pages_deleted > 0), and every
-- removed index entry is counted (tuples_deleted).
CREATE TABLE vacstat_idxdel (id int PRIMARY KEY, v text)
  WITH (autovacuum_enabled = off);
INSERT INTO vacstat_idxdel SELECT g, repeat('x', 20) FROM generate_series(1, 10000) g;
VACUUM vacstat_idxdel;
SELECT pg_stat_force_next_flush();
DELETE FROM vacstat_idxdel WHERE id <= 9000;
VACUUM vacstat_idxdel;
SELECT pg_stat_force_next_flush();
SELECT indexrelname,
       pages_deleted > 0 AS pages_deleted,
       tuples_deleted = 9000 AS tuples_deleted
  FROM pg_stat_vacuum_indexes WHERE relname = 'vacstat_idxdel' ORDER BY indexrelname;
DROP TABLE vacstat_idxdel;
