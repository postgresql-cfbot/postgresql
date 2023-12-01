--
-- Tests for predicate handling
--

--
-- test that restrictions that are always true are ignored, and that are always
-- false are replaced with constant-FALSE
--
-- currently we only check for NullTest quals and OR clauses that include
-- NullTest quals.  We may extend it in the future.
--
CREATE TABLE pred_tab (a int NOT NULL, b int, c int NOT NULL);

--
-- Test restriction clauses
--

-- Ensure the IS_NOT_NULL qual is ignored, since it's on a NOT NULL column
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NOT NULL;

-- Ensure the IS_NOT_NULL qual is not ignored, since it's not on a NOT NULL
-- column
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NOT NULL;

-- Ensure the IS_NULL qual is reduced to constant-FALSE, since it's on a NOT
-- NULL column
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NULL;

-- Ensure the IS_NULL qual is not reduced to constant-FALSE, since it's not on
-- a NOT NULL column
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NULL;

-- Tests for OR clauses in restriction clauses

-- Ensure the OR clause is ignored
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NOT NULL OR t.b = 1;

-- Ensure the OR clause is not ignored
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NOT NULL OR t.a = 1;

-- Ensure the OR clause is reduced to constant-FALSE
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.a IS NULL OR t.c IS NULL;

-- Ensure the OR clause is not reduced to constant-FALSE
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t WHERE t.b IS NULL OR t.c IS NULL;

--
-- Test join clauses
--

-- Ensure the IS_NOT_NULL qual is ignored, since a) it's on a NOT NULL column,
-- and b) its Var is not nullable by any outer joins
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON TRUE
    LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL;

-- Ensure the IS_NOT_NULL qual is not ignored, since its Var is nullable by
-- outer join
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON t1.a = 1
    LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL;

-- Ensure the IS_NULL qual is reduced to constant-FALSE, since a) it's on a NOT
-- NULL column, and b) its Var is not nullable by any outer joins
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON TRUE
    LEFT JOIN pred_tab t3 ON t2.a IS NULL AND t2.b = 1;

-- Ensure the IS_NULL qual is not reduced to constant-FALSE, since its Var is
-- nullable by outer join
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON t1.a = 1
    LEFT JOIN pred_tab t3 ON t2.a IS NULL;

-- Tests for OR clauses in join clauses

-- Ensure the OR clause is ignored
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON TRUE
    LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL OR t2.b = 1;

-- Ensure the OR clause is not ignored
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON t1.a = 1
    LEFT JOIN pred_tab t3 ON t2.a IS NOT NULL OR t2.b = 1;

-- Ensure the OR clause is reduced to constant-FALSE
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON TRUE
    LEFT JOIN pred_tab t3 ON (t2.a IS NULL OR t2.c IS NULL) AND t2.b = 1;

-- Ensure the OR clause is not reduced to constant-FALSE
EXPLAIN (COSTS OFF)
SELECT * FROM pred_tab t1
    LEFT JOIN pred_tab t2 ON t1.a = 1
    LEFT JOIN pred_tab t3 ON t2.a IS NULL OR t2.c IS NULL;

DROP TABLE pred_tab;
