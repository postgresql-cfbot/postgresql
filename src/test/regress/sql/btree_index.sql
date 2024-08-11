--
-- BTREE_INDEX
--

-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);

CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);

CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
);

\set filename :abs_srcdir '/data/desc.data'
COPY bt_i4_heap FROM :'filename';

\set filename :abs_srcdir '/data/hash.data'
COPY bt_name_heap FROM :'filename';

\set filename :abs_srcdir '/data/desc.data'
COPY bt_txt_heap FROM :'filename';

\set filename :abs_srcdir '/data/hash.data'
COPY bt_f8_heap FROM :'filename';

ANALYZE bt_i4_heap;
ANALYZE bt_name_heap;
ANALYZE bt_txt_heap;
ANALYZE bt_f8_heap;

--
-- BTREE ascending/descending cases
--
-- we load int4/text from pure descending data (each key is a new
-- low key) and name/f8 from pure ascending data (each key is a new
-- high key).  we had a bug where new low keys would sometimes be
-- "lost".
--
CREATE INDEX bt_i4_index ON bt_i4_heap USING btree (seqno int4_ops);

CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);

CREATE INDEX bt_txt_index ON bt_txt_heap USING btree (seqno text_ops);

CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops);

--
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
-- Add coverage for optimization of backwards scan index descents
--
-- Here we expect _bt_search to descend straight to a leaf page containing a
-- non-pivot tuple with the value '47', which comes last (after 11 similar
-- non-pivot tuples).  Query execution should only need to visit a single
-- leaf page here.
--
-- Test case relies on tenk1_hundred index having a leaf page whose high key
-- is '(48, -inf)'.  We use a low cardinality index to make our test case less
-- sensitive to implementation details that may change in the future.
set enable_seqscan to false;
set enable_indexscan to true;
set enable_bitmapscan to false;
explain (costs off)
select hundred, twenty from tenk1 where hundred < 48 order by hundred desc limit 1;
select hundred, twenty from tenk1 where hundred < 48 order by hundred desc limit 1;

-- This variant of the query need only return a single tuple located to the immediate
-- right of the '(48, -inf)' high key.  It also only needs to scan one single
-- leaf page (the right sibling of the page scanned by the last test case):
explain (costs off)
select hundred, twenty from tenk1 where hundred <= 48 order by hundred desc limit 1;
select hundred, twenty from tenk1 where hundred <= 48 order by hundred desc limit 1;

--
-- Add coverage for ScalarArrayOp btree quals with pivot tuple constants
--
explain (costs off)
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82);
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82);

explain (costs off)
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82) order by hundred desc;
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82) order by hundred desc;

explain (costs off)
select thousand from tenk1 where thousand in (364, 366,380) and tenthous = 200000;
select thousand from tenk1 where thousand in (364, 366,380) and tenthous = 200000;

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

-- Also check LIKE optimization with binary-compatible cases

create temp table btree_bpchar (f1 text collate "C");
create index on btree_bpchar(f1 bpchar_ops) WITH (deduplicate_items=on);
insert into btree_bpchar values ('foo'), ('fool'), ('bar'), ('quux');
-- doesn't match index:
explain (costs off)
select * from btree_bpchar where f1 like 'foo';
select * from btree_bpchar where f1 like 'foo';
explain (costs off)
select * from btree_bpchar where f1 like 'foo%';
select * from btree_bpchar where f1 like 'foo%';
-- these do match the index:
explain (costs off)
select * from btree_bpchar where f1::bpchar like 'foo';
select * from btree_bpchar where f1::bpchar like 'foo';
explain (costs off)
select * from btree_bpchar where f1::bpchar like 'foo%';
select * from btree_bpchar where f1::bpchar like 'foo%';

-- get test coverage for "single value" deduplication strategy:
insert into btree_bpchar select 'foo' from generate_series(1,1500);

--
-- Perform unique checking, with and without the use of deduplication
--
CREATE TABLE dedup_unique_test_table (a int) WITH (autovacuum_enabled=false);
CREATE UNIQUE INDEX dedup_unique ON dedup_unique_test_table (a) WITH (deduplicate_items=on);
CREATE UNIQUE INDEX plain_unique ON dedup_unique_test_table (a) WITH (deduplicate_items=off);
-- Generate enough garbage tuples in index to ensure that even the unique index
-- with deduplication enabled has to check multiple leaf pages during unique
-- checking (at least with a BLCKSZ of 8192 or less)
DO $$
BEGIN
    FOR r IN 1..1350 LOOP
        DELETE FROM dedup_unique_test_table;
        INSERT INTO dedup_unique_test_table SELECT 1;
    END LOOP;
END$$;

-- Exercise the LP_DEAD-bit-set tuple deletion code with a posting list tuple.
-- The implementation prefers deleting existing items to merging any duplicate
-- tuples into a posting list, so we need an explicit test to make sure we get
-- coverage (note that this test also assumes BLCKSZ is 8192 or less):
DROP INDEX plain_unique;
DELETE FROM dedup_unique_test_table WHERE a = 1;
INSERT INTO dedup_unique_test_table SELECT i FROM generate_series(0,450) i;

--
-- Test B-tree fast path (cache rightmost leaf page) optimization.
--

-- First create a tree that's at least three levels deep (i.e. has one level
-- between the root and leaf levels). The text inserted is long.  It won't be
-- TOAST compressed because we use plain storage in the table.  Only a few
-- index tuples fit on each internal page, allowing us to get a tall tree with
-- few pages.  (A tall tree is required to trigger caching.)
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
-- Test for multilevel page deletion
--
CREATE TABLE delete_test_table (a bigint, b bigint, c bigint, d bigint);
INSERT INTO delete_test_table SELECT i, 1, 2, 3 FROM generate_series(1,80000) i;
ALTER TABLE delete_test_table ADD PRIMARY KEY (a,b,c,d);
-- Delete most entries, and vacuum, deleting internal pages and creating "fast
-- root"
DELETE FROM delete_test_table WHERE a < 79990;
VACUUM delete_test_table;

--
-- Test B-tree insertion with a metapage update (XLOG_BTREE_INSERT_META
-- WAL record type). This happens when a "fast root" page is split.  This
-- also creates coverage for nbtree FSM page recycling.
--
-- The vacuum above should've turned the leaf page into a fast root. We just
-- need to insert some rows to cause the fast root page to split.
INSERT INTO delete_test_table SELECT i, 1, 2, 3 FROM generate_series(1,1000) i;

-- Test unsupported btree opclass parameters
create index on btree_tall_tbl (id int4_ops(foo=1));

-- Test case of ALTER INDEX with abuse of column names for indexes.
-- This grammar is not officially supported, but the parser allows it.
CREATE INDEX btree_tall_idx2 ON btree_tall_tbl (id);
ALTER INDEX btree_tall_idx2 ALTER COLUMN id SET (n_distinct=100);
DROP INDEX btree_tall_idx2;
-- Partitioned index
CREATE TABLE btree_part (id int4) PARTITION BY RANGE (id);
CREATE INDEX btree_part_idx ON btree_part(id);
ALTER INDEX btree_part_idx ALTER COLUMN id SET (n_distinct=100);
DROP TABLE btree_part;

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