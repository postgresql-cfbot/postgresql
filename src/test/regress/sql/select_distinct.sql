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


CREATE TABLE uqk1(a int, pk int primary key, c int,  d int);
CREATE TABLE uqk2(a int, pk int primary key, c int,  d int);
INSERT INTO uqk1 VALUES(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);
INSERT INTO uqk2 VALUES(1, 1, 1, 1), (4, 4, 4, 4), (5, 5, 5, 5);
ANALYZE uqk1;
ANALYZE uqk2;

-- Test single table
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM uqk1;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1;

CREATE UNIQUE INDEX uqk1_ukcd ON uqk1(c, d);

EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;
ALTER TABLE uqk1 ALTER COLUMN d SET NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE c is NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM uqk1 WHERE pk = 1;


-- Test join
-- both uqk1 (c, d) and uqk2(pk) are unique key, so distinct is not needed.

EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1, uqk2
WHERE uqk1.c is NOT NULL AND uqk1.a = uqk2.pk;

-- Distinct is needed since the outer join
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.pk FROM uqk1 RIGHT JOIN uqk2 ON (uqk1.a = uqk2.pk)
order BY 1;

SELECT DISTINCT uqk1.pk FROM uqk1 RIGHT JOIN uqk2 ON (uqk1.a = uqk2.pk) order BY 1;

-- Distinct is not needed since uqk1 is the left table in outer join
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1 LEFT JOIN uqk2 ON (uqk1.a = uqk2.pk)
WHERE uqk1.c is NOT NULL order BY 1, 2;

SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1 LEFT JOIN uqk2 ON (uqk1.a = uqk2.pk)
WHERE uqk1.c is NOT NULL order BY 1, 2;

-- Distinct is ok even with NOT clause-list both UNIQUE keys shown in targetlist
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM uqk1, uqk2;
SELECT DISTINCT * FROM uqk1, uqk2 order BY 1, 2, 3, 4, 5, 6;

-- Test Semi/Anti JOIN
EXPLAIN (COSTS OFF) SELECT DISTINCT pk FROM uqk1 WHERE d in (SELECT d FROM uqk2);
EXPLAIN (COSTS OFF) SELECT DISTINCT pk FROM uqk1 WHERE d NOT in (SELECT d FROM uqk2);

-- Test Unique Key FOR one-row case, DISTINCT is NOT needed as well.
-- uqk1.d is the a uniquekey due to onerow rule. uqk2.pk is pk
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk1.d = uqk2.pk;
SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk1.d = uqk2.pk order BY 1;

-- Both uqk1.d AND uqk2.c are the a uniquekey due to onerow rule
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 2
AND uqk2.pk = 1 AND uqk1.d = uqk2.d ;

SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk2.pk = 1
AND uqk1.d = uqk2.d order BY 1;

-- Both UniqueKey in targetList
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk2.c FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk2.pk = 1;
SELECT DISTINCT uqk1.c, uqk2.c FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk2.pk = 1 order BY 1, 2;

-- Test SubQuery
-- t2(a, c) is UNIQUE key because OF group BY
EXPLAIN (COSTS OFF) SELECT DISTINCT t2.a, t2.c FROM uqk1 t1 inner JOIN
(SELECT a, c, sum(pk) as t2b FROM uqk2 group BY a, c) t2
ON (t2.t2b = t1.pk);

-- Test Partition TABLE
-- Test partitioned TABLE
CREATE TABLE dist_p (a int, b int NOT NULL, c int NOT NULL, d int) PARTITION BY RANGE (b);
CREATE TABLE dist_p0 PARTITION OF dist_p FOR VALUES FROM (1) to (10);
CREATE TABLE dist_p1 PARTITION OF dist_p FOR VALUES FROM (11) to (20);

-- CREATE unqiue INDEX ON dist_p
CREATE UNIQUE INDEX dist_p_uk_b_c ON dist_p(b, c);
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM dist_p;
DROP INDEX dist_p_uk_b_c;

-- we also support CREATE unqiue INDEX ON each child tables
CREATE UNIQUE INDEX dist_p0_uk_bc ON dist_p0(b, c);
-- NOT ok, since dist_p1 have no such INDEX
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM dist_p;
CREATE UNIQUE INDEX dist_p1_uk_bc ON dist_p1(b, c);
-- OK now
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM dist_p;

DROP INDEX dist_p0_uk_bc;
DROP INDEX dist_p1_uk_bc;

-- uk is same ON all child tables, however it doesn't include the partkey, so NOT ok as well.
CREATE UNIQUE INDEX dist_p0_uk_c ON dist_p0(c);
CREATE UNIQUE INDEX dist_p1_uk_c ON dist_p1(c);
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM dist_p;

DROP TABLE dist_p;

-- Test some VIEW
CREATE VIEW distinct_v1 as SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM distinct_v1;
ALTER TABLE uqk1 ALTER COLUMN d DROP NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM distinct_v1;

-- Test generic plan
ALTER TABLE uqk1 ALTER COLUMN d SET NOT NULL;
prepare pt as SELECT * FROM distinct_v1;
EXPLAIN (COSTS OFF)  execute pt;
ALTER TABLE uqk1 ALTER COLUMN d DROP NOT NULL;
EXPLAIN (COSTS OFF) execute pt;
DEALLOCATE pt;

DROP VIEW distinct_v1;
DROP TABLE uqk1;
DROP TABLE uqk2;
