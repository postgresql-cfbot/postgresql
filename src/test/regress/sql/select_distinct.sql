--
-- SELECT_DISTINCT
--

--
-- awk '{print $3;}' onek.data | sort -n | uniq
--
SELECT DISTINCT two FROM tmp ORDER BY 1;

--
-- awk '{print $5;}' onek.data | sort -n | uniq
--
SELECT DISTINCT ten FROM tmp ORDER BY 1;

--
-- awk '{print $16;}' onek.data | sort -d | uniq
--
SELECT DISTINCT string4 FROM tmp ORDER BY 1;

--
-- awk '{print $3,$16,$5;}' onek.data | sort -d | uniq |
-- sort +0n -1 +1d -2 +2n -3
--
SELECT DISTINCT two, string4, ten
   FROM tmp
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

SET enable_hashagg=TRUE;

-- Produce results with hash aggregation.

SET enable_sort=FALSE;

SET jit_above_cost=0;

EXPLAIN (costs off)
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

CREATE TABLE distinct_hash_1 AS
SELECT DISTINCT g%1000 FROM generate_series(0,9999) g;

SET jit_above_cost TO DEFAULT;

CREATE TABLE distinct_hash_2 AS
SELECT DISTINCT (g%1000)::text FROM generate_series(0,9999) g;

SET enable_sort=TRUE;

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

-- index only skip scan
CREATE TABLE distinct_a (a int, b int, c int);
INSERT INTO distinct_a (
    SELECT five, tenthous, 10 FROM
    generate_series(1, 5) five,
    generate_series(1, 10000) tenthous
);
CREATE INDEX ON distinct_a (a, b);
CREATE INDEX ON distinct_a ((a + 1));
ANALYZE distinct_a;

SELECT DISTINCT a FROM distinct_a;
SELECT DISTINCT a FROM distinct_a WHERE a = 1;
SELECT DISTINCT a FROM distinct_a ORDER BY a DESC;

EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_a;

-- test index skip scan with a condition on a non unique field
SELECT DISTINCT ON (a) a, b FROM distinct_a WHERE b = 2;

-- test index skip scan backwards
SELECT DISTINCT ON (a) a, b FROM distinct_a ORDER BY a DESC, b DESC;

-- test index skip scan for expressions
EXPLAIN (COSTS OFF)
SELECT DISTINCT (a + 1) FROM distinct_a ORDER BY (a + 1);
SELECT DISTINCT (a + 1) FROM distinct_a ORDER BY (a + 1);

-- check colums order
CREATE INDEX distinct_a_b_a on distinct_a (b, a);

SELECT DISTINCT a FROM distinct_a WHERE b = 2;
SELECT DISTINCT on (a, b) a, b FROM distinct_a WHERE b = 2;

EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_a WHERE b = 2;

EXPLAIN (COSTS OFF)
SELECT DISTINCT on (a, b) a, b FROM distinct_a WHERE b = 2;

DROP INDEX distinct_a_b_a;

-- test opposite scan/index directions inside a cursor
-- forward/backward
BEGIN;
DECLARE c SCROLL CURSOR FOR
SELECT DISTINCT ON (a) a,b FROM distinct_a ORDER BY a, b;

FETCH FROM c;
FETCH BACKWARD FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

END;

-- backward/forward
BEGIN;
DECLARE c SCROLL CURSOR FOR
SELECT DISTINCT ON (a) a,b FROM distinct_a ORDER BY a DESC, b DESC;

FETCH FROM c;
FETCH BACKWARD FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

END;

-- test missing values and skipping from the end
CREATE TABLE distinct_abc(a int, b int, c int);
CREATE INDEX ON distinct_abc(a, b, c);
INSERT INTO distinct_abc
	VALUES (1, 1, 1),
		   (1, 1, 2),
		   (1, 2, 2),
		   (1, 2, 3),
		   (2, 2, 1),
		   (2, 2, 3),
		   (3, 1, 1),
		   (3, 1, 2),
		   (3, 2, 2),
		   (3, 2, 3);

EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a,b,c FROM distinct_abc WHERE c = 2;

BEGIN;
DECLARE c SCROLL CURSOR FOR
SELECT DISTINCT ON (a) a,b,c FROM distinct_abc WHERE c = 2;

FETCH ALL FROM c;
FETCH BACKWARD ALL FROM c;

END;

EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a,b,c FROM distinct_abc WHERE c = 2
ORDER BY a DESC, b DESC;

BEGIN;
DECLARE c SCROLL CURSOR FOR
SELECT DISTINCT ON (a) a,b,c FROM distinct_abc WHERE c = 2
ORDER BY a DESC, b DESC;

FETCH ALL FROM c;
FETCH BACKWARD ALL FROM c;

END;

DROP TABLE distinct_abc;

-- index skip scan
SELECT DISTINCT ON (a) a, b, c
FROM distinct_a ORDER BY a;
SELECT DISTINCT ON (a) a, b, c
FROM distinct_a WHERE a = 1 ORDER BY a;

EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a, b, c
FROM distinct_a ORDER BY a;
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a, b, c
FROM distinct_a WHERE a = 1 ORDER BY a;
EXPLAIN (COSTS OFF)
SELECT DISTINCT *
FROM distinct_a;

-- check colums order
SELECT DISTINCT a FROM distinct_a WHERE b = 2 AND c = 10;

EXPLAIN (COSTS OFF)
SELECT DISTINCT a FROM distinct_a WHERE b = 2 AND c = 10;

-- check projection case
SELECT DISTINCT a, a FROM distinct_a WHERE b = 2;
SELECT DISTINCT a, 1 FROM distinct_a WHERE b = 2;

-- test cursor forward/backward movements
BEGIN;
DECLARE c SCROLL CURSOR FOR SELECT DISTINCT a FROM distinct_a;

FETCH FROM c;
FETCH BACKWARD FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

FETCH 6 FROM c;
FETCH BACKWARD 6 FROM c;

END;

DROP TABLE distinct_a;

-- test tuples visibility
CREATE TABLE distinct_visibility (a int, b int);
INSERT INTO distinct_visibility (select a, b from generate_series(1,5) a, generate_series(1, 10000) b);
CREATE INDEX ON distinct_visibility (a, b);
ANALYZE distinct_visibility;

SELECT DISTINCT ON (a) a, b FROM distinct_visibility ORDER BY a, b;
DELETE FROM distinct_visibility WHERE a = 2 and b = 1;
SELECT DISTINCT ON (a) a, b FROM distinct_visibility ORDER BY a, b;

SELECT DISTINCT ON (a) a, b FROM distinct_visibility ORDER BY a DESC, b DESC;
DELETE FROM distinct_visibility WHERE a = 2 and b = 10000;
SELECT DISTINCT ON (a) a, b FROM distinct_visibility ORDER BY a DESC, b DESC;
DROP TABLE distinct_visibility;

-- test page boundaries
CREATE TABLE distinct_boundaries AS
    SELECT a, b::int2 b, (b % 2)::int2 c FROM
        generate_series(1, 5) a,
        generate_series(1,366) b;

CREATE INDEX ON distinct_boundaries (a, b, c);
ANALYZE distinct_boundaries;

EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (a) a, b, c from distinct_boundaries
WHERE b >= 1 and c = 0 ORDER BY a, b;

SELECT DISTINCT ON (a) a, b, c from distinct_boundaries
WHERE b >= 1 and c = 0 ORDER BY a, b;

DROP TABLE distinct_boundaries;

-- test tuple killing

-- DESC ordering
CREATE TABLE distinct_killed AS
    SELECT a, b, b % 2 AS c, 10 AS d
        FROM generate_series(1, 5) a,
             generate_series(1,1000) b;

CREATE INDEX ON distinct_killed (a, b, c, d);

DELETE FROM distinct_killed where a = 3;

BEGIN;
    DECLARE c SCROLL CURSOR FOR
    SELECT DISTINCT ON (a) a,b,c,d
    FROM distinct_killed ORDER BY a DESC, b DESC;
    FETCH FORWARD ALL FROM c;
    FETCH BACKWARD ALL FROM c;
COMMIT;

DROP TABLE distinct_killed;

-- regular ordering
CREATE TABLE distinct_killed AS
    SELECT a, b, b % 2 AS c, 10 AS d
        FROM generate_series(1, 5) a,
             generate_series(1,1000) b;

CREATE INDEX ON distinct_killed (a, b, c, d);

DELETE FROM distinct_killed where a = 3;

BEGIN;
    DECLARE c SCROLL CURSOR FOR
    SELECT DISTINCT ON (a) a,b,c,d
    FROM distinct_killed ORDER BY a, b;
    FETCH FORWARD ALL FROM c;
    FETCH BACKWARD ALL FROM c;
COMMIT;

DROP TABLE distinct_killed;

-- partial delete
CREATE TABLE distinct_killed AS
    SELECT a, b, b % 2 AS c, 10 AS d
        FROM generate_series(1, 5) a,
             generate_series(1,1000) b;

CREATE INDEX ON distinct_killed (a, b, c, d);

DELETE FROM distinct_killed WHERE a = 3 AND b <= 999;

BEGIN;
    DECLARE c SCROLL CURSOR FOR
    SELECT DISTINCT ON (a) a,b,c,d
    FROM distinct_killed ORDER BY a DESC, b DESC;
    FETCH FORWARD ALL FROM c;
    FETCH BACKWARD ALL FROM c;
COMMIT;

DROP TABLE distinct_killed;

-- test posting lists
CREATE TABLE distinct_posting (a int, b int, c int);
CREATE INDEX ON distinct_posting (a, b, c);
INSERT INTO distinct_posting
	VALUES (1, 1, 1),
		   (1, 1, 2),
		   (1, 2, 2),
		   (1, 2, 3),
		   (2, 2, 1),
		   (2, 2, 3),
		   (3, 1, 1),
		   (3, 1, 2),
		   (3, 2, 2),
		   (3, 2, 3);

INSERT INTO distinct_posting (
    SELECT 1 as a, 1 as b, 1 AS c
        FROM generate_series(1,1000) i
);

BEGIN;
    DECLARE c SCROLL CURSOR FOR
    SELECT DISTINCT ON (a) a,b,c FROM distinct_posting WHERE c = 2
    ORDER BY a DESC, b DESC;
    FETCH ALL FROM c;

    FETCH BACKWARD ALL FROM c;
COMMIT;

-- test that quals are check for indexability before applied
CREATE TABLE Indexable_quals (a text, b text, c text);
CREATE INDEX ON indexable_quals (a, b, c);

INSERT INTO indexable_quals VALUES ('a1', 'b', 'xxx');
INSERT INTO indexable_quals VALUES ('a1', 'b', 'yyy');
INSERT INTO indexable_quals VALUES ('a2', 'b', 'xxx');
INSERT INTO indexable_quals VALUES ('a2', 'b', 'yyy');

SELECT DISTINCT ON (a, b)  a, b
FROM indexable_quals WHERE c LIKE '%y%' AND a LIKE 'a%' AND b = 'b';
