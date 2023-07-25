--
-- Const merging functionality
--
CREATE EXTENSION pg_stat_statements;

CREATE TABLE test_merge (id int, data int);

-- IN queries

-- No merging is performed, as a baseline result
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Normal scenario, too many simple constants for an IN query
SET query_id_const_merge = on;

SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1);
SELECT * FROM test_merge WHERE id IN (1, 2, 3);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Second order of magnitude, brace yourself
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- With gaps on the threshold
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- No unmerged constants
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- More conditions in the query
SELECT pg_stat_statements_reset();

SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9) and data = 2;
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) and data = 2;
SELECT * FROM test_merge WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) and data = 2;
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- On table, numeric type causes every constant being wrapped into functions.
CREATE TABLE test_merge_numeric (id int, data numeric(5, 2));
SELECT pg_stat_statements_reset();
SELECT * FROM test_merge_numeric WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
SELECT query, calls FROM pg_stat_statements ORDER BY query COLLATE "C";

-- Test constants evaluation
WITH cte AS (
    SELECT 'const' as const FROM test_merge
)
SELECT ARRAY['a', 'b', 'c', const::varchar] AS result
FROM cte;

RESET query_id_const_merge;
