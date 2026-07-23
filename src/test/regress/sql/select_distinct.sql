--
-- SELECT_DISTINCT
--

--
-- awk '{print $3;}' onek.data | sort -n | uniq
--
SELECT DISTINCT two FROM onek ORDER BY 1;

--
-- awk '{print $5;}' onek.data | sort -n | uniq
--
SELECT DISTINCT ten FROM onek ORDER BY 1;

--
-- awk '{print $16;}' onek.data | sort -d | uniq
--
SELECT DISTINCT string4 FROM onek ORDER BY 1;

--
-- awk '{print $3,$16,$5;}' onek.data | sort -d | uniq |
-- sort +0n -1 +1d -2 +2n -3
--
SELECT DISTINCT two, string4, ten
   FROM onek
   ORDER BY two using <, string4 using <, ten using <;

--
-- awk '{print $2;}' person.data |
-- awk '{if(NF!=1){print $2;}else{print;}}' - emp.data |
-- awk '{if(NF!=1){print $2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $5;}else{print;}}' - stud_emp.data |
-- sort -n -r | uniq
--
SELECT DISTINCT p.age FROM person* p ORDER BY age using >;

--
-- Check mentioning same column more than once
--

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM
  (SELECT DISTINCT two, four, two FROM tenk1) ss;

SELECT count(*) FROM
  (SELECT DISTINCT two, four, two FROM tenk1) ss;

--
-- Compare results between plans using sorting and plans using hash
-- aggregation. Force spilling in both cases by setting work_mem low.
--

SET work_mem='64kB';

-- Produce results with sorting.

SET enable_hashagg=FALSE;

SET jit_above_cost=0;

EXPLAIN (costs off)
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

CREATE TABLE distinct_group_1 AS
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

SET jit_above_cost TO DEFAULT;

CREATE TABLE distinct_group_2 AS
SELECT DISTINCT (g%1000)::text FROM generate_series(0,9999) g;

SET enable_seqscan = 0;

-- Check to see we get an incremental sort plan
EXPLAIN (costs off)
SELECT DISTINCT hundred, two FROM tenk1;

RESET enable_seqscan;

SET enable_hashagg=TRUE;

-- Produce results with hash aggregation.

SET enable_groupagg=FALSE;

SET jit_above_cost=0;

EXPLAIN (costs off)
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

CREATE TABLE distinct_hash_1 AS
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

SET jit_above_cost TO DEFAULT;

CREATE TABLE distinct_hash_2 AS
SELECT DISTINCT (g%1000)::text FROM generate_series(0,9999) g;

SET enable_groupagg=TRUE;

SET work_mem TO DEFAULT;

-- Compare results

(SELECT * FROM distinct_hash_1 EXCEPT SELECT * FROM distinct_group_1)
  UNION ALL
(SELECT * FROM distinct_group_1 EXCEPT SELECT * FROM distinct_hash_1);

(SELECT * FROM distinct_hash_1 EXCEPT SELECT * FROM distinct_group_1)
  UNION ALL
(SELECT * FROM distinct_group_1 EXCEPT SELECT * FROM distinct_hash_1);

DROP TABLE distinct_hash_1;
DROP TABLE distinct_hash_2;
DROP TABLE distinct_group_1;
DROP TABLE distinct_group_2;

-- Test parallel DISTINCT
SET parallel_tuple_cost=0;
SET parallel_setup_cost=0;
SET min_parallel_table_scan_size=0;
SET max_parallel_workers_per_gather=2;

-- Ensure we get a parallel plan
EXPLAIN (costs off)
SELECT DISTINCT four FROM tenk1;

-- Ensure the parallel plan produces the correct results
SELECT DISTINCT four FROM tenk1;

CREATE OR REPLACE FUNCTION distinct_func(a INT) RETURNS INT AS $$
  BEGIN
    RETURN a;
  END;
$$ LANGUAGE plpgsql PARALLEL UNSAFE;

-- Ensure we don't do parallel distinct with a parallel unsafe function
EXPLAIN (COSTS OFF)
SELECT DISTINCT distinct_func(1) FROM tenk1;

-- make the function parallel safe
CREATE OR REPLACE FUNCTION distinct_func(a INT) RETURNS INT AS $$
  BEGIN
    RETURN a;
  END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

-- Ensure we do parallel distinct now that the function is parallel safe
EXPLAIN (COSTS OFF)
SELECT DISTINCT distinct_func(1) FROM tenk1;

RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

--
-- Test the planner's ability to use a LIMIT 1 instead of a Unique node when
-- all of the distinct_pathkeys have been marked as redundant
--

-- Ensure we get a plan with a Limit 1
EXPLAIN (COSTS OFF)
SELECT DISTINCT four FROM tenk1 WHERE four = 0;

-- Ensure the above gives us the correct result
SELECT DISTINCT four FROM tenk1 WHERE four = 0;

-- Ensure we get a plan with a Limit 1
EXPLAIN (COSTS OFF)
SELECT DISTINCT four FROM tenk1 WHERE four = 0 AND two <> 0;

-- Ensure no rows are returned
SELECT DISTINCT four FROM tenk1 WHERE four = 0 AND two <> 0;

-- Ensure we get a plan with a Limit 1 when the SELECT list contains constants
EXPLAIN (COSTS OFF)
SELECT DISTINCT four,1,2,3 FROM tenk1 WHERE four = 0;

-- Ensure we only get 1 row
SELECT DISTINCT four,1,2,3 FROM tenk1 WHERE four = 0;

SET parallel_setup_cost=0;
SET min_parallel_table_scan_size=0;
SET max_parallel_workers_per_gather=2;

-- Ensure we get a plan with a Limit 1 in both partial distinct and final
-- distinct
EXPLAIN (COSTS OFF)
SELECT DISTINCT four FROM tenk1 WHERE four = 10;

RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;

--
-- Also, some tests of IS DISTINCT FROM, which doesn't quite deserve its
-- very own regression file.
--

CREATE TEMP TABLE disttable (f1 integer);
INSERT INTO DISTTABLE VALUES(1);
INSERT INTO DISTTABLE VALUES(2);
INSERT INTO DISTTABLE VALUES(3);
INSERT INTO DISTTABLE VALUES(NULL);

-- basic cases
SELECT f1, f1 IS DISTINCT FROM 2 as "not 2" FROM disttable;
SELECT f1, f1 IS DISTINCT FROM NULL as "not null" FROM disttable;
SELECT f1, f1 IS DISTINCT FROM f1 as "false" FROM disttable;
SELECT f1, f1 IS DISTINCT FROM f1+1 as "not null" FROM disttable;

-- check that optimizer constant-folds it properly
SELECT 1 IS DISTINCT FROM 2 as "yes";
SELECT 2 IS DISTINCT FROM 2 as "no";
SELECT 2 IS DISTINCT FROM null as "yes";
SELECT null IS DISTINCT FROM null as "no";

-- negated form
SELECT 1 IS NOT DISTINCT FROM 2 as "no";
SELECT 2 IS NOT DISTINCT FROM 2 as "yes";
SELECT 2 IS NOT DISTINCT FROM null as "no";
SELECT null IS NOT DISTINCT FROM null as "yes";

--
-- Test the planner's ability to reorder the distinctClause Pathkeys to match
-- the input path's ordering
--

CREATE TABLE distinct_tbl (x int, y int);
INSERT INTO distinct_tbl SELECT i%10, i%10 FROM generate_series(1, 1000) AS i;
CREATE INDEX distinct_tbl_x_y_idx ON distinct_tbl (x, y);
ANALYZE distinct_tbl;

-- Produce results with sorting.
SET enable_hashagg TO OFF;

-- Ensure we avoid the need to re-sort by reordering the distinctClause
-- Pathkeys to match the ordering of the input path
EXPLAIN (COSTS OFF)
SELECT DISTINCT y, x FROM distinct_tbl;
SELECT DISTINCT y, x FROM distinct_tbl;

-- Ensure we leverage incremental-sort by reordering the distinctClause
-- Pathkeys to partially match the ordering of the input path
EXPLAIN (COSTS OFF)
SELECT DISTINCT y, x FROM (SELECT * FROM distinct_tbl ORDER BY x) s;
SELECT DISTINCT y, x FROM (SELECT * FROM distinct_tbl ORDER BY x) s;

-- Ensure we avoid the need to re-sort in partial distinct by reordering the
-- distinctClause Pathkeys to match the ordering of the input path
SET parallel_tuple_cost=0;
SET parallel_setup_cost=0;
SET min_parallel_table_scan_size=0;
SET min_parallel_index_scan_size=0;
SET max_parallel_workers_per_gather=2;

EXPLAIN (COSTS OFF)
SELECT DISTINCT y, x FROM distinct_tbl limit 10;
SELECT DISTINCT y, x FROM distinct_tbl limit 10;

RESET max_parallel_workers_per_gather;
RESET min_parallel_index_scan_size;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

-- Ensure we reorder the distinctClause Pathkeys to match the ordering of the
-- input path even if there is ORDER BY clause
EXPLAIN (COSTS OFF)
SELECT DISTINCT y, x FROM distinct_tbl ORDER BY y;
SELECT DISTINCT y, x FROM distinct_tbl ORDER BY y;

RESET enable_hashagg;

DROP TABLE distinct_tbl;

--
-- Test that DISTINCT is removed when GROUP BY already guarantees uniqueness
-- of the output rows (via populate_uniquekeys_from_pathkeys() feeding the
-- generic relation_is_distinct_for() check in create_distinct_paths()).
--

CREATE TABLE distinct_groupby_tbl (a int, b int, c int);
INSERT INTO distinct_groupby_tbl VALUES
    (1, 1, 10), (1, 2, 20), (2, 1, 30), (2, 2, 40),
    (1, 1, 50);
ANALYZE distinct_groupby_tbl;

-- GROUP BY (a, b) guarantees unique output rows, so DISTINCT is redundant.
-- Expect a single aggregation node, no extra Unique node on top.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a, b FROM distinct_groupby_tbl GROUP BY a, b;

-- Verify correct results
SELECT DISTINCT a, b FROM distinct_groupby_tbl GROUP BY a, b ORDER BY a, b;

-- Different column order in DISTINCT vs GROUP BY -- still redundant.
EXPLAIN (COSTS OFF)
SELECT DISTINCT b, a FROM distinct_groupby_tbl GROUP BY a, b;

SELECT DISTINCT b, a FROM distinct_groupby_tbl GROUP BY a, b ORDER BY a, b;

-- Aggregate in SELECT list does not prevent elimination.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a, b, sum(c) FROM distinct_groupby_tbl GROUP BY a, b;

SELECT DISTINCT a, b, sum(c) FROM distinct_groupby_tbl GROUP BY a, b ORDER BY a, b;

-- DISTINCT is NOT redundant: GROUP BY key 'b' is absent from DISTINCT.
-- Different (a, b) groups can produce the same 'a' output value.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_groupby_tbl GROUP BY a, b;

SELECT DISTINCT a FROM distinct_groupby_tbl GROUP BY a, b ORDER BY a;

-- DISTINCT is NOT redundant: no GROUP BY clause.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a, b FROM distinct_groupby_tbl;

-- DISTINCT is NOT redundant: GROUPING SETS can introduce extra NULL rows,
-- so two grouping sets could yield the same DISTINCT output.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_groupby_tbl
GROUP BY GROUPING SETS ((a), ());

-- DISTINCT ON is unaffected (different semantics).
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a, b FROM distinct_groupby_tbl GROUP BY a, b ORDER BY a;

-- DISTINCT is NOT redundant: a set-returning function in the targetlist is
-- expanded after grouping but before DISTINCT, so it can (re-)introduce
-- duplicate rows even though every GROUP BY key is covered by DISTINCT.
EXPLAIN (COSTS OFF)
SELECT DISTINCT a, unnest(ARRAY[1,1]) AS u FROM distinct_groupby_tbl GROUP BY a;

SELECT DISTINCT a, unnest(ARRAY[1,1]) AS u FROM distinct_groupby_tbl GROUP BY a ORDER BY a, u;

-- DISTINCT is still redundant when the grouped column is nullable: GROUP BY
-- collapses all NULL-valued rows of a column into a single output row, so a
-- NULL group is no more of a duplicate risk than any other group value.
INSERT INTO distinct_groupby_tbl VALUES (NULL, 1, 60), (NULL, 1, 70);

EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_groupby_tbl GROUP BY a;

SELECT DISTINCT a FROM distinct_groupby_tbl GROUP BY a ORDER BY a;

DROP TABLE distinct_groupby_tbl;
