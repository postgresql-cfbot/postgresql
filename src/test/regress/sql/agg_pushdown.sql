BEGIN;

CREATE TABLE agg_pushdown_parent (
	i int primary key,
	x int);

CREATE TABLE agg_pushdown_child1 (
	j int primary key,
	parent int references agg_pushdown_parent,
	v double precision);

CREATE INDEX ON agg_pushdown_child1(parent);

CREATE TABLE agg_pushdown_child2 (
	k int primary key,
	parent int references agg_pushdown_parent,
	v double precision);

INSERT INTO agg_pushdown_parent(i)
SELECT n
FROM generate_series(0, 7) AS s(n);

INSERT INTO agg_pushdown_child1(j, parent, v)
SELECT 64 * i + n, i, random()
FROM generate_series(0, 63) AS s(n), agg_pushdown_parent;

INSERT INTO agg_pushdown_child2(k, parent, v)
SELECT 64 * i + n, i, random()
FROM generate_series(0, 63) AS s(n), agg_pushdown_parent;

ANALYZE;

SET enable_agg_pushdown TO on;

-- Perform scan of a table and aggregate the result. In addition, check that
-- functionally dependent column (c.x) can be referenced by SELECT although
-- GROUP BY references p.i.
EXPLAIN (COSTS off)
SELECT p.x, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

-- Scan index on agg_pushdown_child1(parent) column and aggregate the result
-- using AGG_SORTED strategy.
SET enable_seqscan TO off;
EXPLAIN (COSTS off)
SELECT p.i, avg(c1.v) FROM agg_pushdown_parent AS p JOIN agg_pushdown_child1
AS c1 ON c1.parent = p.i GROUP BY p.i;

SET enable_seqscan TO on;

-- Perform nestloop join between agg_pushdown_child1 and agg_pushdown_child2
-- and aggregate the result.
SET enable_nestloop TO on;
SET enable_hashjoin TO off;
SET enable_mergejoin TO off;

EXPLAIN (COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

-- The same for hash join.
SET enable_nestloop TO off;
SET enable_hashjoin TO on;

EXPLAIN (COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

-- The same for merge join.
SET enable_hashjoin TO off;
SET enable_mergejoin TO on;
SET enable_seqscan TO off;

EXPLAIN (COSTS off)
SELECT p.i, avg(c1.v + c2.v) FROM agg_pushdown_parent AS p JOIN
agg_pushdown_child1 AS c1 ON c1.parent = p.i JOIN agg_pushdown_child2 AS c2 ON
c2.parent = p.i WHERE c1.j = c2.k GROUP BY p.i;

ROLLBACK;
