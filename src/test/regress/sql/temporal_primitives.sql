--
-- TEMPORAL PRIMITIVES: ALIGN AND NORMALIZE
--
SET datestyle TO ymd;

CREATE TYPE varcharrange AS RANGE (SUBTYPE=varchar);
CREATE TYPE floatrange AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi);

CREATE TEMP TABLE table1_int4r (a char, b char, t int4range);
CREATE TEMP TABLE table2_int4r (c int, d char, t int4range);

INSERT INTO table1_int4r VALUES
('a','B','[1,7)'),
('b','B','[3,9)'),
('c','G','[8,10)');
INSERT INTO table2_int4r VALUES
(1,'B','[2,5)'),
(2,'B','[3,4)'),
(3,'B','[7,9)');

-- VALID TIME columns (i.e., ts and te) are no longer at the end of the
-- targetlist.
CREATE TEMP TABLE table1_int4r_mix AS SELECT a, t, b FROM table1_int4r;
CREATE TEMP TABLE table2_int4r_mix AS SELECT t, c, d FROM table2_int4r;

-- VALID TIME columns as VARCHARs
CREATE TEMP TABLE table1_varcharr (a int, t varcharrange);
CREATE TEMP TABLE table2_varcharr (a int, t varcharrange);

INSERT INTO table1_varcharr VALUES
(0, varcharrange('A', 'D')),
(1, varcharrange('C', 'X')),
(0, varcharrange('ABC', 'BCD')),
(0, varcharrange('xABC', 'xBCD')),
(0, varcharrange('BAA', 'BBB'));

INSERT INTO table2_varcharr VALUES
(0, varcharrange('A', 'D')),
(1, varcharrange('C', 'X'));

-- Tables to check different data types, and corner cases
CREATE TEMP TABLE table_tsrange (a int, t tsrange);
CREATE TEMP TABLE table1_int4r0 (a int, t floatrange);
CREATE TEMP TABLE table1_int4r1 AS TABLE table1_int4r0;

INSERT INTO table_tsrange VALUES
(0, '[2000-01-01, 2000-01-10)'),
(1, '[2000-01-05, 2000-01-20)');

INSERT INTO table1_int4r0 VALUES
(0, floatrange(1.0, 1.1111)),
(1, floatrange(1.11109999, 2.0));

INSERT INTO table1_int4r1 VALUES
(0, floatrange(1.0, 'Infinity')),
(1, floatrange('-Infinity', 2.0));


--
-- TEMPORAL ALIGNER: BASICS
--

-- Equality qualifiers
SELECT * FROM (
	table1_int4r ALIGN table2_int4r
		ON b = d
		WITH (t, t)
	) x;

-- Equality qualifiers with FQN inside ON- and WITH-clause
SELECT * FROM (
	table1_int4r ALIGN table2_int4r
		ON table1_int4r.b = table2_int4r.d
		WITH (table1_int4r.t, table2_int4r.t)
	) x;

-- Alignment with aggregation
-- NB: Targetlist of outer query is *not* A_STAR...
SELECT a, COUNT(a) FROM (
	table1_int4r ALIGN table2_int4r
		ON b = d
		WITH (t, t)
	) x
	GROUP BY a ORDER BY a;

-- Equality qualifiers
-- Test column positions where ts and te are not the last two columns.
-- Please note: This was a restriction in an early implementation.
SELECT * FROM (
	table1_int4r_mix ALIGN table2_int4r_mix
		ON b = d
		WITH (t, t)
	) x;

-- Equality qualifiers with FQN inside ON- and WITH-clause
-- Test column positions where ts and te are not the last two columns.
-- Please note: This was a restriction in an early implementation.
SELECT * FROM (
	table1_int4r_mix ALIGN table2_int4r_mix
		ON table1_int4r_mix.b = table2_int4r_mix.d
		WITH (table1_int4r_mix.t, table2_int4r_mix.t)
	) x;

-- Alignment with aggregation where targetlist of outer query is *not* A_STAR...
-- Test column positions where ts and te are not the last two columns.
-- Please note: This was a restriction in an early implementation.
SELECT a, COUNT(a) FROM (
	table1_int4r_mix ALIGN table2_int4r_mix
		ON b = d
		WITH (t, t)
	) x
	GROUP BY a ORDER BY a;

-- Test relations with differently named temporal bound attributes and relation
-- and column aliases.
SELECT * FROM (
	table1_int4r ALIGN table2_int4r x(c,d,s)
		ON b = d
		WITH (t, s)
	) x;


--
-- TEMPORAL ALIGNER: TEMPORAL JOIN EXAMPLE
--

-- Full temporal join example with absorbing where clause, timestamp
-- propagation (see CTEs targetlists with V and U) and range types
WITH t1 AS (SELECT *, t u FROM table1_int4r),
	 t2 AS (SELECT c a, d b, t, t v FROM table2_int4r)
SELECT t, b, x.a, y.a FROM (
	t1 ALIGN t2
		ON t1.b = t2.b
		WITH (t, t)
	) x
	LEFT OUTER JOIN (
		SELECT * FROM (
		t2 ALIGN t1
			ON t1.b = t2.b
			WITH (t, t)
		) y
	) y
	USING (b, t)
	WHERE (
			(lower(t) = lower(u) OR lower(t) = lower(v))
			AND
			(upper(t) = upper(u) OR upper(t) = upper(v))
		)
		OR u IS NULL
		OR v IS NULL
	ORDER BY 1,2,3,4;

-- Collation and varchar boundaries
SELECT * FROM (
	table1_varcharr x ALIGN table1_varcharr y
		ON TRUE
		WITH (t, t)
	) x;

--
-- TEMPORAL ALIGNER: SELECTION PUSH-DOWN
--

-- VALID TIME columns are not safe to be pushed down, for the rest everything
-- should work as usual.
EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r ALIGN table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3;

EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r ALIGN table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3 AND lower(t) > 3;

EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r ALIGN table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3 OR lower(t) > 3;

--
-- TEMPORAL ALIGNER: DATA TYPES
--

-- Data types: Timestamps
-- We use to_char here to be sure that we have the same output format on all
-- platforms and locale configuration
SELECT a, to_char(lower(t), 'YYYY-MM-DD') ts, to_char(upper(t), 'YYYY-MM-DD') te
FROM (
	table_tsrange t1 ALIGN table_tsrange t2
		ON t1.a = 0
		WITH (t, t)
	) x;

-- Data types: Double precision
SELECT a, t FROM (
	table1_int4r0 t1 ALIGN table1_int4r0 t2
		ON t1.a = 0
		WITH (t, t)
	) x;

-- Data types: Double precision with +/- infinity
SELECT a, t FROM (
	table1_int4r1 t1 ALIGN table1_int4r1 t2
		ON t1.a = 0
		WITH (t, t)
	) x;


--
-- TEMPORAL NORMALIZER: BASICS
--

-- Equality qualifiers
SELECT * FROM (
	table1_int4r NORMALIZE table2_int4r
		ON b = d
		WITH (t, t)
	) x;

-- Equality qualifiers with FQN inside ON- and WITH-clause
SELECT * FROM (
	table1_int4r NORMALIZE table2_int4r
		ON table1_int4r.b = table2_int4r.d
		WITH (table1_int4r.t, table2_int4r.t)
	) x;

-- Alignment with aggregation
-- NB: Targetlist of outer query is *not* A_STAR...
SELECT a, COUNT(a) FROM (
	table1_int4r NORMALIZE table2_int4r
		ON b = d
		WITH (t, t)
	) x
	GROUP BY a ORDER BY a;

-- Equality qualifiers
-- Test column positions where ts and te are not the last two columns.
-- Please note: This was a restriction in an early implementation.
SELECT * FROM (
	table1_int4r_mix NORMALIZE table2_int4r_mix
		ON b = d
		WITH (t, t)
	) x;

-- Equality qualifiers with FQN inside ON- and WITH-clause
-- Test column positions where t is not at the last column.
-- Please note: This was a restriction in an early implementation.
SELECT * FROM (
	table1_int4r_mix NORMALIZE table2_int4r_mix
		ON table1_int4r_mix.b = table2_int4r_mix.d
		WITH (table1_int4r_mix.t, table2_int4r_mix.t)
	) x;

-- Alignment with aggregation where targetlist of outer query is *not* A_STAR...
-- Test column positions where t is not at the last column.
-- Please note: This was a restriction in an early implementation.
SELECT a, COUNT(a) FROM (
	table1_int4r_mix NORMALIZE table2_int4r_mix
		ON b = d
		WITH (t, t)
	) x
	GROUP BY a ORDER BY a;

-- Test relations with differently named temporal bound attributes and relation
-- and column aliases.
SELECT * FROM (
	table1_int4r NORMALIZE table2_int4r x(c,d,s)
		ON b = d
		WITH (t, s)
	) x;

-- Normalizer's USING clause (self-normalization)
SELECT * FROM (
	table1_int4r t1 NORMALIZE table1_int4r t2
		USING (a)
		WITH (t, t)
	) x;

-- Collation and varchar boundaries
SELECT * FROM (
	table1_varcharr x NORMALIZE table1_varcharr y
		ON TRUE
		WITH (t, t)
	) x;


--
-- TEMPORAL NORMALIZER: SELECTION PUSH-DOWN
--

-- VALID TIME columns are not safe to be pushed down, for the rest everything
-- should work as usual.

EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r NORMALIZE table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3;

EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r NORMALIZE table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3 AND lower(t) > 3;

EXPLAIN (COSTS OFF) SELECT * FROM (
	table2_int4r NORMALIZE table1_int4r
		ON TRUE
		WITH (t, t)
	) x
	WHERE c < 3 OR lower(t) > 3;

--
-- TEMPORAL NORMALIZER: DATA TYPES
--

-- Data types: Timestamps
-- We use to_char here to be sure that we have the same output format on all
-- platforms and locale configuration
SELECT a, to_char(lower(t), 'YYYY-MM-DD') ts, to_char(upper(t), 'YYYY-MM-DD') te FROM (
	table_tsrange t1 NORMALIZE table_tsrange t2
		ON t1.a = 0
		WITH (t, t)
	) x;

-- Data types: Double precision
SELECT a, t FROM (
	table1_int4r0 t1 NORMALIZE table1_int4r0 t2
		ON t1.a = 0
		WITH (t, t)
	) x;

-- Data types: Double precision with +/- infinity
SELECT a, t FROM (
	table1_int4r1 t1 NORMALIZE table1_int4r1 t2
		ON t1.a = 0
		WITH (t, t)
	) x;

--
-- TEMPORAL ALIGNER AND NORMALIZER: VIEWS
--

-- Views with temporal normalization
CREATE TEMP VIEW v AS SELECT * FROM (
	table1_int4r NORMALIZE table2_int4r
		ON b = d
		WITH (t, t)
	) x;

TABLE v;
DROP VIEW v;

-- Views with temporal alignment
CREATE TEMP VIEW v AS SELECT * FROM (
	table1_int4r ALIGN table2_int4r
		ON b = d
		WITH (t, t)
	) x;

TABLE v;
DROP VIEW v;

-- Testing temporal normalization with ambiguous columns, i.e. columns that
-- are used internally...
CREATE TEMP VIEW v AS SELECT * FROM (
	table1_int4r AS r(p1, p1_0, "p1_-1") NORMALIZE table2_int4r s
		ON r.p1_0 = s.d
		WITH ("p1_-1", t)
	) x;

TABLE v;
DROP VIEW v;

-- Testing temporal alignment with ambiguous columns, i.e. columns that
-- are used internally...
CREATE TEMP VIEW v AS SELECT * FROM (
	table1_int4r AS r(p1, p1_0, p1_1) ALIGN table2_int4r s
		ON r.p1_0 = s.d
		WITH (p1_1,t)
	) x;

TABLE v;
DROP VIEW v;


