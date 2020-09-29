-- Perform tests on the Result Cache node.

-- Ensure we get the expected plan with sub plans.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT unique1, (SELECT count(*) FROM tenk1 t2 WHERE t2.twenty = t1.twenty)
FROM tenk1 t1 WHERE t1.unique1 < 1000;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT unique1, (SELECT count(*) FROM tenk1 t2 WHERE t2.thousand = t1.thousand)
FROM tenk1 t1;

-- Reduce work_mem so that we see some cache evictions
SET work_mem TO '64kB';
-- Ensure we get some evitions.  The number is likely to vary on different machines, so
-- XXX I'll likely need to think about how to check this better.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT unique1, (SELECT count(*) FROM tenk1 t2 WHERE t2.thousand = t1.thousand)
FROM tenk1 t1;
RESET work_mem;

-- Ensure the cache works as expected with a parallel scan.
SET min_parallel_table_scan_size TO 0;
SET parallel_setup_cost TO 0;
SET parallel_tuple_cost TO 0;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT unique1, (SELECT count(*) FROM tenk1 t2 WHERE t2.hundred = t1.hundred)
FROM tenk1 t1 WHERE t1.unique1 < 1000;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

-- Ensure we get a result cache on the inner side of the nested loop
SET enable_hashjoin TO off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT COUNT(*),AVG(t1.unique1) FROM tenk1 t1
INNER JOIN tenk1 t2 ON t1.unique1 = t2.twenty
WHERE t2.unique1 < 1000;

-- And check we get the expected results.
SELECT COUNT(*),AVG(t1.unique1) FROM tenk1 t1
INNER JOIN tenk1 t2 ON t1.unique1 = t2.twenty
WHERE t2.unique1 < 1000;

-- Try with LATERAL joins
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT COUNT(*),AVG(t2.unique1) FROM tenk1 t1,
LATERAL (SELECT t2.unique1 FROM tenk1 t2 WHERE t1.twenty = t2.unique1) t2
WHERE t1.unique1 < 1000;

-- And check we get the expected results.
SELECT COUNT(*),AVG(t2.unique1) FROM tenk1 t1,
LATERAL (SELECT t2.unique1 FROM tenk1 t2 WHERE t1.twenty = t2.unique1) t2
WHERE t1.unique1 < 1000;

RESET enable_hashjoin;
