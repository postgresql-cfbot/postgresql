--
-- BTREE_INDEX
-- test retrieval of min/max keys for each index
--

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno < 1;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno >= 9999;

SELECT b.*
   FROM bt_i4_heap b
   WHERE b.seqno = 4500;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno < '1'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno >= '9999'::name;

SELECT b.*
   FROM bt_name_heap b
   WHERE b.seqno = '4500'::name;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno < '1'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno >= '9999'::text;

SELECT b.*
   FROM bt_txt_heap b
   WHERE b.seqno = '4500'::text;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno < '1'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno >= '9999'::float8;

SELECT b.*
   FROM bt_f8_heap b
   WHERE b.seqno = '4500'::float8;

--
-- Check correct optimization of LIKE (special index operator support)
-- for both indexscan and bitmapscan cases
--

set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
explain (costs off)
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike '00%foo' order by 1;
select proname from pg_proc where proname ilike '00%foo' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike 'ri%foo' order by 1;

set enable_indexscan to false;
set enable_bitmapscan to true;
explain (costs off)
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
select proname from pg_proc where proname like E'RI\\_FKey%del' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike '00%foo' order by 1;
select proname from pg_proc where proname ilike '00%foo' order by 1;
explain (costs off)
select proname from pg_proc where proname ilike 'ri%foo' order by 1;

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;

--
-- Test B-tree fast path (cache rightmost leaf page) optimization.
--

-- First create a tree that's at least three levels deep (i.e. has one level
-- between the root and leaf levels). The text inserted is long.  It won't be
-- compressed because we use plain storage in the table.  Only a few index
-- tuples fit on each internal page, allowing us to get a tall tree with few
-- pages.  (A tall tree is required to trigger caching.)
--
-- The text column must be the leading column in the index, since suffix
-- truncation would otherwise truncate tuples on internal pages, leaving us
-- with a short tree.
create table btree_tall_tbl(id int4, t text);
alter table btree_tall_tbl alter COLUMN t set storage plain;
create index btree_tall_idx on btree_tall_tbl (t, id) with (fillfactor = 10);
insert into btree_tall_tbl select g, repeat('x', 250)
from generate_series(1, 130) g;

--
-- Test vacuum_cleanup_index_scale_factor
--

-- Simple create
create table btree_test(a int);
create index btree_idx1 on btree_test(a) with (vacuum_cleanup_index_scale_factor = 40.0);
select reloptions from pg_class WHERE oid = 'btree_idx1'::regclass;

-- Fail while setting improper values
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = -10.0);
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = 100.0);
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = 'string');
create index btree_idx_err on btree_test(a) with (vacuum_cleanup_index_scale_factor = true);

-- Simple ALTER INDEX
alter index btree_idx1 set (vacuum_cleanup_index_scale_factor = 70.0);
select reloptions from pg_class WHERE oid = 'btree_idx1'::regclass;

---
--- Test B-tree distance ordering
---

SET enable_bitmapscan = OFF;

-- temporarily disable bt_i4_index index on bt_i4_heap(seqno)
UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'bt_i4_index'::regclass;

CREATE INDEX bt_i4_heap_random_idx ON bt_i4_heap USING btree(random, seqno);

-- test unsupported orderings (by non-first index attribute or by more than one order keys)
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY seqno <-> 0;
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY random <-> 0, seqno <-> 0;
EXPLAIN (COSTS OFF) SELECT * FROM bt_i4_heap ORDER BY random <-> 0, random <-> 1;

EXPLAIN (COSTS OFF)
SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 10000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 0;

EXPLAIN (COSTS OFF)
SELECT * FROM bt_i4_heap
WHERE
	random > 1000000 AND (random, seqno) < (6000000, 0) AND
	random IN (1809552, 1919087, 2321799, 2648497, 3000193, 3013326, 4157193, 4488889, 5257716, 5593978, NULL)
ORDER BY random <-> 3000000;

SELECT * FROM bt_i4_heap
WHERE
	random > 1000000 AND (random, seqno) < (6000000, 0) AND
	random IN (1809552, 1919087, 2321799, 2648497, 3000193, 3013326, 4157193, 4488889, 5257716, 5593978, NULL)
ORDER BY random <-> 3000000;

DROP INDEX bt_i4_heap_random_idx;

CREATE INDEX bt_i4_heap_random_idx ON bt_i4_heap USING btree(random DESC, seqno);

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 4000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 10000000;

SELECT * FROM bt_i4_heap
WHERE random > 1000000 AND (random, seqno) < (6000000, 0)
ORDER BY random <-> 0;

DROP INDEX bt_i4_heap_random_idx;

-- test parallel KNN scan

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
BEGIN ISOLATION LEVEL REPEATABLE READ;

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers = 4;
SET max_parallel_workers_per_gather = 4;
SET cpu_operator_cost = 0;

RESET enable_indexscan;

\set bt_knn_row_count 100000

CREATE TABLE bt_knn_test AS SELECT i * 10 AS i FROM generate_series(1, :bt_knn_row_count) i;
CREATE INDEX bt_knn_test_idx ON bt_knn_test (i);
ALTER TABLE bt_knn_test SET (parallel_workers = 4);
ANALYZE bt_knn_test;

-- set the point inside the range
\set bt_knn_point (4 * :bt_knn_row_count + 3)

CREATE TABLE bt_knn_test2 AS
	SELECT row_number() OVER (ORDER BY i * 10 <-> :bt_knn_point) AS n, i * 10 AS i
	FROM generate_series(1, :bt_knn_row_count) i;

SET enable_sort = OFF;

EXPLAIN (COSTS OFF)
WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

RESET enable_sort;

DROP TABLE bt_knn_test2;

-- set the point to the right of the range
\set bt_knn_point (11 * :bt_knn_row_count)

CREATE TABLE bt_knn_test2 AS
	SELECT row_number() OVER (ORDER BY i * 10 <-> :bt_knn_point) AS n, i * 10 AS i
	FROM generate_series(1, :bt_knn_row_count) i;

SET enable_sort = OFF;

EXPLAIN (COSTS OFF)
WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

RESET enable_sort;

DROP TABLE bt_knn_test2;

-- set the point to the left of the range
\set bt_knn_point (-:bt_knn_row_count)

CREATE TABLE bt_knn_test2 AS
	SELECT row_number() OVER (ORDER BY i * 10 <-> :bt_knn_point) AS n, i * 10 AS i
	FROM generate_series(1, :bt_knn_row_count) i;

SET enable_sort = OFF;

EXPLAIN (COSTS OFF)
WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

WITH bt_knn_test1 AS (
	SELECT row_number() OVER (ORDER BY i <-> :bt_knn_point) AS n, i FROM bt_knn_test
)
SELECT * FROM bt_knn_test1 t1 JOIN bt_knn_test2 t2 USING (n) WHERE t1.i <> t2.i;

RESET enable_sort;

DROP TABLE bt_knn_test;

\set knn_row_count 30000
CREATE TABLE bt_knn_test AS SELECT i FROM generate_series(1, 10) i, generate_series(1, :knn_row_count) j;
CREATE INDEX bt_knn_test_idx ON bt_knn_test (i);
ALTER TABLE bt_knn_test SET (parallel_workers = 4);
ANALYZE bt_knn_test;

SET enable_sort = OFF;

EXPLAIN (COSTS OFF)
WITH
t1 AS (
	SELECT row_number() OVER () AS n, i
	FROM bt_knn_test
	WHERE i IN (3, 4, 7, 8, 2)
	ORDER BY i <-> 4
),
t2 AS (
	SELECT i * :knn_row_count + j AS n, (ARRAY[4, 3, 2, 7, 8])[i + 1] AS i
	FROM generate_series(0, 4) i, generate_series(1, :knn_row_count) j
)
SELECT * FROM t1 JOIN t2 USING (n) WHERE t1.i <> t2.i;

WITH
t1 AS (
	SELECT row_number() OVER () AS n, i
	FROM bt_knn_test
	WHERE i IN (3, 4, 7, 8, 2)
	ORDER BY i <-> 4
),
t2 AS (
	SELECT i * :knn_row_count + j AS n, (ARRAY[4, 3, 2, 7, 8])[i + 1] AS i
	FROM generate_series(0, 4) i, generate_series(1, :knn_row_count) j
)
SELECT * FROM t1 JOIN t2 USING (n) WHERE t1.i <> t2.i;

RESET enable_sort;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers;
RESET max_parallel_workers_per_gather;
RESET cpu_operator_cost;

ROLLBACK;

-- enable bt_i4_index index on bt_i4_heap(seqno)
UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'bt_i4_index'::regclass;


CREATE TABLE tenk3 AS SELECT thousand, tenthous FROM tenk1;

INSERT INTO tenk3 VALUES (NULL, 1), (NULL, 2), (NULL, 3);

-- Test distance ordering by ASC index
CREATE INDEX tenk3_idx ON tenk3 USING btree(thousand, tenthous);

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 0;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000) AND thousand < 1000
ORDER BY thousand <-> 10000;

SELECT thousand, tenthous FROM tenk3
ORDER BY thousand <-> 500
OFFSET 9970;

EXPLAIN (COSTS OFF)
SELECT * FROM tenk3
WHERE thousand > 100 AND thousand < 800 AND
	thousand = ANY(ARRAY[0, 123, 234, 345, 456, 678, 901, NULL]::int2[])
ORDER BY thousand <-> 300::int8;

SELECT * FROM tenk3
WHERE thousand > 100 AND thousand < 800 AND
	thousand = ANY(ARRAY[0, 123, 234, 345, 456, 678, 901, NULL]::int2[])
ORDER BY thousand <-> 300::int8;

DROP INDEX tenk3_idx;

-- Test order by distance ordering on non-first column
SET enable_sort = OFF;

-- Ranges are not supported
EXPLAIN (COSTS OFF)
SELECT thousand, tenthous
FROM tenk1
WHERE thousand > 120
ORDER BY tenthous <-> 3500;

-- Equality restriction on the first column is supported
EXPLAIN (COSTS OFF)
SELECT thousand, tenthous
FROM tenk1
WHERE thousand = 120
ORDER BY tenthous <-> 3500;

SELECT thousand, tenthous
FROM tenk1
WHERE thousand = 120
ORDER BY tenthous <-> 3500;

-- IN restriction on the first column is not supported without 'ORDER BY col1 ASC'
EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk1 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY tenthous <-> 3500;

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk1 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand DESC, tenthous <-> 3500;

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk1 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand, tenthous <-> 3500;

SELECT thousand, tenthous FROM tenk1 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand, tenthous <-> 3500;

-- Test kNN search using 4-column index
CREATE INDEX tenk1_knn_idx ON tenk1(ten, hundred, thousand, tenthous);

-- Ordering by distance to 3rd column
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 ORDER BY thousand <-> 600;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 ORDER BY thousand <-> 600;

EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 AND tenthous > 3000 ORDER BY thousand <-> 600;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 AND tenthous > 3000 ORDER BY thousand <-> 600;

-- Ordering by distance to 4th column
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 AND thousand = 643 ORDER BY tenthous <-> 4000;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten = 3 AND hundred = 43 AND thousand = 643 ORDER BY tenthous <-> 4000;

-- Array ops on non-first columns are not supported
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred IN (23, 24, 35) AND thousand IN (843, 132, 623, 243)
ORDER BY tenthous <-> 6000;

-- All array columns should be included into ORDER BY
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY tenthous <-> 6000;

-- Eq-restricted columns can be omitted from ORDER BY
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, tenthous <-> 6000;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, tenthous <-> 6000;

EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, hundred, tenthous <-> 6000;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, hundred, tenthous <-> 6000;

EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4 ,5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, thousand, tenthous <-> 6000;

SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, thousand, tenthous <-> 6000;

EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 AND thousand = 123 AND tenthous > 2000
ORDER BY ten, hundred, thousand, tenthous <-> 6000;

-- Extra ORDER BY columns after order-by-op are not supported
EXPLAIN (COSTS OFF)
SELECT ten, hundred, thousand, tenthous FROM tenk1
WHERE ten IN (3, 4, 5) AND hundred = 23 ORDER BY ten, thousand <-> 6000, tenthous;

DROP INDEX tenk1_knn_idx;

RESET enable_sort;

-- Test distance ordering by DESC index
CREATE INDEX tenk3_idx ON tenk3 USING btree(thousand DESC, tenthous);

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 998;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000)
ORDER BY thousand <-> 0;

SELECT thousand, tenthous FROM tenk3
WHERE (thousand, tenthous) >= (997, 5000) AND thousand < 1000
ORDER BY thousand <-> 10000;

SELECT thousand, tenthous FROM tenk3
ORDER BY thousand <-> 500
OFFSET 9970;

-- IN restriction on the first column is not supported without 'ORDER BY col1 DESC'
EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk3 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY tenthous <-> 3500;

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk3 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand, tenthous <-> 3500;

EXPLAIN (COSTS OFF)
SELECT thousand, tenthous FROM tenk3 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand DESC, tenthous <-> 3500;

SELECT thousand, tenthous FROM tenk3 WHERE thousand IN (5, 120, 3456, 23)
ORDER BY thousand DESC, tenthous <-> 3500;

DROP INDEX tenk3_idx;

DROP TABLE tenk3;

-- Test distance ordering on by-ref types
CREATE TABLE knn_btree_ts (ts timestamp);

INSERT INTO knn_btree_ts
SELECT timestamp '2017-05-03 00:00:00' + tenthous * interval '1 hour'
FROM tenk1;

CREATE INDEX knn_btree_ts_idx ON knn_btree_ts USING btree(ts);

SELECT ts, ts <-> timestamp '2017-05-01 00:00:00' FROM knn_btree_ts ORDER BY 2 LIMIT 20;
SELECT ts, ts <-> timestamp '2018-01-01 00:00:00' FROM knn_btree_ts ORDER BY 2 LIMIT 20;

DROP TABLE knn_btree_ts;

RESET enable_bitmapscan;

-- Test backward kNN scan

SET enable_sort = OFF;

EXPLAIN (COSTS OFF) SELECT thousand, tenthous FROM tenk1 ORDER BY thousand  <-> 510;

BEGIN work;

DECLARE knn SCROLL CURSOR FOR
SELECT thousand, tenthous FROM tenk1 ORDER BY thousand <-> 510;

FETCH LAST FROM knn;
FETCH BACKWARD 15 FROM knn;
FETCH RELATIVE -200 FROM knn;
FETCH BACKWARD 20 FROM knn;
FETCH FIRST FROM knn;
FETCH LAST FROM knn;
FETCH RELATIVE -215 FROM knn;
FETCH BACKWARD 20 FROM knn;

ROLLBACK work;

RESET enable_sort;
