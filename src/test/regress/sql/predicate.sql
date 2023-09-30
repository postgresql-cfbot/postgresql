--
-- Tests for predicate transformation
--

CREATE TABLE pred_tab (a INT NOT NULL, b INT);

--
-- test that restrictions that we detect as always true are ignored
--

-- An IS NOT NULL qual in restriction clauses can be ignored if it's on a NOT
-- NULL column
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NOT NULL;

-- A more complex variant of the above.  Ensure we're left only with the
-- t.b = 1 qual since t.a IS NOT NULL is always true
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b = 1 AND t.a IS NOT NULL;

-- Ensure t.b IS NOT NULL is not removed as t.b allows NULL values.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NOT NULL;

-- Ensure the t.a IS NOT NULL is detected as always true.  We shouldn't see
-- any quals in the scan.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NOT NULL OR t.b = 1;

-- Ensure the quals remain as t.b allows NULL values.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NOT NULL OR t.a = 1;

-- An IS NOT NULL NullTest in join clauses can be ignored if
-- a) it's on a NOT NULL column, and;
-- b) its Var is not nulled by any outer joins

-- Ensure t2.a IS NOT NULL is not seen in the plan
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1 LEFT JOIN pred_tab t2 ON true
LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL;

-- When some t2 rows are missing due to the left join we cannot forego
-- including the t2.a is not null in the plan.  Ensure it remains.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1 LEFT JOIN pred_tab t2 ON t1.a = 1
LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL;

-- Ensure t2.a IS NULL is detected as constantly TRUE and results in none of
-- the quals appearing in the plan.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1 LEFT JOIN pred_tab t2 ON true
LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL OR t2.b = 1;

-- Ensure that the IS NOT NULL qual isn't removed as t2.a is nullable from
-- t2's left join to t1.
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1 LEFT JOIN pred_tab t2 ON t1.a = 1
LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL OR t2.b = 1;

--
-- Ensure that impossible IS NULL NullTests are detected when the Var cannot
-- be NULL
--

-- Ensure we detect t.a IS NULL is impossible AND forego the scan
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NULL;

-- As above, but add an additional qual
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a = 1234 AND t.a IS NULL;

DROP TABLE pred_tab;

