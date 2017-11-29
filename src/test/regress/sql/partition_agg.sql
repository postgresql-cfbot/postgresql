--
-- PARTITION_AGG
-- Test partition-wise aggregation on partitioned tables
--

-- Enable partition-wise agg, which by default is disabled.
SET enable_partition_wise_agg TO true;
-- Enable partition-wise join, which by default is disabled.
SET enable_partition_wise_join TO true;
-- Also, disable parallel paths.
SET max_parallel_workers_per_gather TO 0;

--
-- Tests for list partitioned tables.
--
CREATE TABLE pagg_tab (a int, b int, c text, d int) PARTITION BY LIST(c);
CREATE TABLE pagg_tab_p1 PARTITION OF pagg_tab FOR VALUES IN ('0000', '0001', '0002', '0003');
CREATE TABLE pagg_tab_p2 PARTITION OF pagg_tab FOR VALUES IN ('0004', '0005', '0006', '0007');
CREATE TABLE pagg_tab_p3 PARTITION OF pagg_tab FOR VALUES IN ('0008', '0009', '0010', '0011');
INSERT INTO pagg_tab SELECT i % 20, i % 30, to_char(i % 12, 'FM0000'), i % 30 FROM generate_series(0, 2999) i;
ANALYZE pagg_tab;

-- When GROUP BY clause matches with PARTITION KEY.
-- Should choose full partition-wise aggregation path
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT c, sum(a), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- When GROUP BY clause not matches with PARTITION KEY.
-- Should choose partial partition-wise aggregation path
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- Check with multiple columns in GROUP BY
EXPLAIN (COSTS OFF)
SELECT a, c, count(*) FROM pagg_tab GROUP BY a, c;
-- Check with multiple columns in GROUP BY, order in GROUP BY is reversed
EXPLAIN (COSTS OFF)
SELECT a, c, count(*) FROM pagg_tab GROUP BY c, a;
-- Check with multiple columns in GROUP BY, order in target-list is reversed
EXPLAIN (COSTS OFF)
SELECT c, a, count(*) FROM pagg_tab GROUP BY a, c;

-- Test when input relation for grouping is dummy
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab WHERE 1 = 2 GROUP BY c;
SELECT c, sum(a) FROM pagg_tab WHERE 1 = 2 GROUP BY c;

-- Check with SORTED paths. Disable hashagg to get group aggregate
SET enable_hashagg TO false;

-- When GROUP BY clause matches with PARTITION KEY.
-- Should choose full partition-wise aggregation path
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT c, sum(a), avg(b), count(*) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- When GROUP BY clause not matches with PARTITION KEY.
-- Should choose partial partition-wise aggregation path
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(d) < 15 ORDER BY 1, 2, 3;

-- No aggregates, but still able to perform partition-wise aggregates
EXPLAIN (COSTS OFF)
SELECT c FROM pagg_tab GROUP BY c ORDER BY 1;
EXPLAIN (COSTS OFF)
SELECT a FROM pagg_tab GROUP BY a ORDER BY 1;

RESET enable_hashagg;

-- ROLLUP, partition-wise aggregation does not apply
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab GROUP BY rollup(c) ORDER BY 1, 2;

-- ORDERED SET within aggregate
EXPLAIN (COSTS OFF)
SELECT a, sum(b order by a) FROM pagg_tab GROUP BY a ORDER BY 1, 2;

-- Cleanup
DROP TABLE pagg_tab;


-- JOIN query

CREATE TABLE pagg_tab1(x int, y int) PARTITION BY RANGE(x);
CREATE TABLE pagg_tab1_p1 PARTITION OF pagg_tab1 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab1_p2 PARTITION OF pagg_tab1 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab1_p3 PARTITION OF pagg_tab1 FOR VALUES FROM (20) TO (30);

CREATE TABLE pagg_tab2(x int, y int) PARTITION BY RANGE(y);
CREATE TABLE pagg_tab2_p1 PARTITION OF pagg_tab2 FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab2_p2 PARTITION OF pagg_tab2 FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab2_p3 PARTITION OF pagg_tab2 FOR VALUES FROM (20) TO (30);

INSERT INTO pagg_tab1 SELECT i%30, i%20 FROM generate_series(0, 299, 2) i;
INSERT INTO pagg_tab2 SELECT i%20, i%30 FROM generate_series(0, 299, 3) i;

ANALYZE pagg_tab1;
ANALYZE pagg_tab2;

-- When GROUP BY clause matches with PARTITION KEY.
-- Should choose full partition-wise aggregation path.
-- Disable mergejoin to get hash aggregate.
SET enable_mergejoin TO false;
EXPLAIN (COSTS OFF)
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
SELECT t1.x, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;
RESET enable_mergejoin;

-- GROUP BY having other matching key
EXPLAIN (COSTS OFF)
SELECT t2.y, sum(t1.y), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t2.y ORDER BY 1, 2, 3;

-- When GROUP BY clause not matches with PARTITION KEY.
-- Should choose partial partition-wise aggregation path.
-- Also check with SORTED paths. Disable hashagg to get group aggregate.
SET enable_hashagg TO false;
EXPLAIN (COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;
RESET enable_hashagg;

-- Check with LEFT/RIGHT/FULL OUTER JOINs which produces NULL values for
-- aggregation

-- LEFT JOIN, should produce partial partition-wise aggregation plan as
-- GROUP BY is on nullable column
EXPLAIN (COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;
SELECT b.y, sum(a.y) FROM pagg_tab1 a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;

-- RIGHT JOIN, should produce full partition-wise aggregation plan as
-- GROUP BY is on non-nullable column
EXPLAIN (COSTS OFF)
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;
SELECT b.y, sum(a.y) FROM pagg_tab1 a RIGHT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;

-- FULL JOIN, should produce partial partition-wise aggregation plan as
-- GROUP BY is on nullable column
EXPLAIN (COSTS OFF)
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1 ORDER BY 1 NULLS LAST;

-- LEFT JOIN, with dummy relation on right side,
-- should produce full partition-wise aggregation plan as GROUP BY is on
-- non-nullable columns
EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a LEFT JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY 1, 2 ORDER BY 1, 2;
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a LEFT JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY 1, 2 ORDER BY 1, 2;

-- FULL JOIN, with dummy relations on both sides,
-- should produce partial partition-wise aggregation plan as GROUP BY is on
-- nullable columns
EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a FULL JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY 1, 2 ORDER BY 1, 2;
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a FULL JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20 GROUP BY 1, 2 ORDER BY 1, 2;

-- Empty relations on LEFT side, no partition-wise agg plan.
EXPLAIN (COSTS OFF)
SELECT a.x, a.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x = 1 AND x = 2) a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1, 2 ORDER BY 1, 2;
SELECT a.x, a.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x = 1 AND x = 2) a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY 1, 2 ORDER BY 1, 2;

-- Cleanup
DROP TABLE pagg_tab2;
DROP TABLE pagg_tab1;


-- Partition by multiple columns

CREATE TABLE pagg_tab (a int, b int, c int) PARTITION BY RANGE(a, ((a+b)/2));
CREATE TABLE pagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0, 0) TO (10, 10);
CREATE TABLE pagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10, 10) TO (20, 20);
CREATE TABLE pagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20, 20) TO (30, 30);
INSERT INTO pagg_tab SELECT i % 30, i % 40, i % 50 FROM generate_series(0, 2999) i;
ANALYZE pagg_tab;

-- Partial aggregation as GROUP BY clause does not match with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a HAVING avg(c) < 22 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a HAVING avg(c) < 22 ORDER BY 1, 2, 3;

-- Full aggregation as GROUP BY clause matches with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;
SELECT a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a, (a+b)/2 HAVING sum(b) < 50 ORDER BY 1, 2, 3;

-- Full aggregation as PARTITION KEY part of GROUP BY clause
EXPLAIN (COSTS OFF)
SELECT c, a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a, (a+b)/2, c HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
SELECT c, a, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY a, (a+b)/2, c HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
EXPLAIN (COSTS OFF)
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY c, a, (a+b)/2 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab GROUP BY c, a, (a+b)/2 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;

-- Cleanup
DROP TABLE pagg_tab;


-- Test with multi-level partitioning scheme
-- Partition-wise aggregation is tried only on first level.

CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);
CREATE TABLE pagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) PARTITION BY LIST (c);
CREATE TABLE pagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) PARTITION BY RANGE (b);
CREATE TABLE pagg_tab_p2_s1 PARTITION OF pagg_tab_p2 FOR VALUES IN ('0000', '0001');
CREATE TABLE pagg_tab_p2_s2 PARTITION OF pagg_tab_p2 FOR VALUES IN ('0002', '0003');
CREATE TABLE pagg_tab_p3_s1 PARTITION OF pagg_tab_p3 FOR VALUES FROM (0) TO (5);
CREATE TABLE pagg_tab_p3_s2 PARTITION OF pagg_tab_p3 FOR VALUES FROM (5) TO (10);
INSERT INTO pagg_tab SELECT i % 30, i % 10, to_char(i % 4, 'FM0000') FROM generate_series(0, 2999) i;
ANALYZE pagg_tab;

-- Full aggregation as GROUP BY clause matches with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

-- Partial aggregation as GROUP BY clause does not match with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT b, sum(a), count(*) FROM pagg_tab GROUP BY b ORDER BY 1, 2, 3;
SELECT b, sum(a), count(*) FROM pagg_tab GROUP BY b ORDER BY 1, 2, 3;

-- Test on middle level partitioned table which is further partitioned on b.
-- Full aggregation as GROUP BY clause matches with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT b, sum(a), count(*) FROM pagg_tab_p3 GROUP BY b ORDER BY 1, 2, 3;
SELECT b, sum(a), count(*) FROM pagg_tab_p3 GROUP BY b ORDER BY 1, 2, 3;

-- Full aggregation as GROUP BY clause matches with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab GROUP BY a, b HAVING avg(b) < 3 ORDER BY 1, 2, 3;
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab GROUP BY a, b HAVING avg(b) < 3 ORDER BY 1, 2, 3;

-- Cleanup
DROP TABLE pagg_tab;


-- Parallelism within partition-wise aggregates

RESET max_parallel_workers_per_gather;
SET min_parallel_table_scan_size TO '8kB';
SET parallel_setup_cost TO 0;

CREATE TABLE pagg_tab_para(x int, y int) PARTITION BY RANGE(x);
CREATE TABLE pagg_tab_para_p1 PARTITION OF pagg_tab_para FOR VALUES FROM (0) TO (10);
CREATE TABLE pagg_tab_para_p2 PARTITION OF pagg_tab_para FOR VALUES FROM (10) TO (20);
CREATE TABLE pagg_tab_para_p3 PARTITION OF pagg_tab_para FOR VALUES FROM (20) TO (30);

INSERT INTO pagg_tab_para SELECT i%30, i%20 FROM generate_series(0, 29999) i;

ANALYZE pagg_tab_para;

SHOW max_parallel_workers_per_gather;

-- When GROUP BY clause matches with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

-- When GROUP BY clause not matches with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;

-- Group Aggregate, look for Gather Merge
SET enable_hashagg TO false;

-- When GROUP BY clause matches with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

-- When GROUP BY clause not matches with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;

RESET enable_hashagg;

-- Cleanup
DROP TABLE pagg_tab_para;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;
RESET enable_partition_wise_join;
RESET enable_partition_wise_agg;
