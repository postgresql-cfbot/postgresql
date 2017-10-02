-- create another schema so that tests can be run in parallel with the
-- partition_join and multi_level_partition_join tests.
create schema pwj_extra;
set search_path=pwj_extra,public;

--=========================================================================================================================================================
--DEV TEST CASES
--
-- PARTITION_JOIN
-- Test partition-wise join between partitioned tables
--

-- Usually partition-wise join paths are chosen when data is large, which would
-- take regression tests to run longer. So, weigh partition-wise joins cheaper
-- to force those even for smaller data.
SET enable_partition_wise_join=on;

--
-- partitioned by a single column
--
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1;
ANALYZE prt1_p1;
ANALYZE prt1_p2;
ANALYZE prt1_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt1 AS SELECT * FROM prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
INSERT INTO prt2 SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
ANALYZE prt2;
ANALYZE prt2_p1;
ANALYZE prt2_p2;
ANALYZE prt2_p3;
CREATE TABLE uprt2 AS SELECT * FROM prt2;

-- inner join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1 t1, uprt2 t2 WHERE t1.a = t2.b AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

-- left outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1 t1 RIGHT JOIN uprt2 t2 ON t1.a = t2.b WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

-- full outer join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM uprt1 WHERE uprt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uprt2 WHERE uprt2.b % 25 = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t1.phv, t2.b, t2.c, t2.phv FROM (SELECT 25 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t1.phv, t2.b, t2.c, t2.phv FROM (SELECT 25 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t1.phv, t2.b, t2.c, t2.phv FROM (SELECT 25 phv, * FROM uprt1 WHERE uprt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM uprt2 WHERE uprt2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1 t1, uprt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM uprt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1 WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM uprt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1 WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2 WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uprt1 t1 WHERE t1.a IN (SELECT t1.b FROM uprt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;

-- lateral reference
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1 t2 JOIN uprt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1 t2 JOIN uprt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;

--
-- partitioned by expression
--
CREATE TABLE prt1_e (a int, b int, c varchar) PARTITION BY RANGE(((a + b)/2));
CREATE TABLE prt1_e_p1 PARTITION OF prt1_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_e_p2 PARTITION OF prt1_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_e_p3 PARTITION OF prt1_e FOR VALUES FROM (500) TO (600);
INSERT INTO prt1_e SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1_e;
ANALYZE prt1_e_p1;
ANALYZE prt1_e_p2;
ANALYZE prt1_e_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt1_e AS SELECT * FROM prt1_e;

CREATE TABLE prt2_e (a int, b int, c varchar) PARTITION BY RANGE(((b + a)/2));
CREATE TABLE prt2_e_p1 PARTITION OF prt2_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_e_p2 PARTITION OF prt2_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_e_p3 PARTITION OF prt2_e FOR VALUES FROM (500) TO (600);
INSERT INTO prt2_e SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE prt2_e;
ANALYZE prt2_e_p1;
ANALYZE prt2_e_p2;
ANALYZE prt2_e_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt2_e AS SELECT * FROM prt2_e;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_e t1, uprt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1 LEFT JOIN prt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1 LEFT JOIN prt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_e t1 LEFT JOIN uprt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

--
-- N-way join
--
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM prt1 t1, prt2 t2, prt1_e t3 WHERE t1.a = t2.b AND t1.a = (t3.a + t3.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM prt1 t1, prt2 t2, prt1_e t3 WHERE t1.a = t2.b AND t1.a = (t3.a + t3.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM uprt1 t1, uprt2 t2, uprt1_e t3 WHERE t1.a = t2.b AND t1.a = (t3.a + t3.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b) LEFT JOIN uprt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b) LEFT JOIN uprt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 RIGHT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT * FROM prt1_e WHERE prt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT * FROM prt1_e WHERE prt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM uprt1 WHERE uprt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2 WHERE uprt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT * FROM uprt1_e WHERE uprt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) ORDER BY t1.a, t2.b, t3.a + t3.b;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, t3.a + t3.b, t3.phv FROM ((SELECT 50 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT 50 phv, * FROM prt1_e WHERE prt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a = t1.phv OR t2.b = t2.phv OR (t3.a + t3.b)/2 = t3.phv ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.phv, t2.b, t2.phv, t3.a + t3.b, t3.phv FROM ((SELECT 50 phv, * FROM prt1 WHERE prt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT 50 phv, * FROM prt1_e WHERE prt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a = t1.phv OR t2.b = t2.phv OR (t3.a + t3.b)/2 = t3.phv ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.phv, t2.b, t2.phv, t3.a + t3.b, t3.phv FROM ((SELECT 50 phv, * FROM uprt1 WHERE uprt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uprt2 WHERE uprt2.b % 25 = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT 50 phv, * FROM uprt1_e WHERE uprt1_e.a % 25 = 0) t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a = t1.phv OR t2.b = t2.phv OR (t3.a + t3.b)/2 = t3.phv ORDER BY t1.a, t2.b, t3.a + t3.b;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.b % 25 = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.b % 25 = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uprt1 t1 WHERE t1.a IN (SELECT t1.b FROM uprt2 t1, uprt1_e t2 WHERE t1.b % 25 = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uprt1 t1 WHERE t1.a IN (SELECT t1.b FROM uprt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM uprt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- test merge joins with and without using indexes
SET enable_hashjoin TO off;
SET enable_nestloop TO off;

CREATE INDEX iprt1_p1_a on prt1_p1(a);
CREATE INDEX iprt1_p2_a on prt1_p2(a);
CREATE INDEX iprt1_p3_a on prt1_p3(a);
CREATE INDEX iprt2_p1_b on prt2_p1(b);
CREATE INDEX iprt2_p2_b on prt2_p2(b);
CREATE INDEX iprt2_p3_b on prt2_p3(b);
CREATE INDEX iprt1_e_p1_ab2 on prt1_e_p1(((a+b)/2));
CREATE INDEX iprt1_e_p2_ab2 on prt1_e_p2(((a+b)/2));
CREATE INDEX iprt1_e_p3_ab2 on prt1_e_p3(((a+b)/2));

ANALYZE prt1;
ANALYZE prt1_p1;
ANALYZE prt1_p2;
ANALYZE prt1_p3;
ANALYZE prt2;
ANALYZE prt2_p1;
ANALYZE prt2_p2;
ANALYZE prt2_p3;
ANALYZE prt1_e;
ANALYZE prt1_e_p1;
ANALYZE prt1_e_p2;
ANALYZE prt1_e_p3;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 RIGHT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uprt1 t1 WHERE t1.a IN (SELECT t1.b FROM uprt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM uprt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

SET enable_seqscan TO off;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 RIGHT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t2.b = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uprt1 t1 WHERE t1.a IN (SELECT t1.b FROM uprt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM uprt1_e t1 WHERE t1.a %25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uprt1 t1 LEFT JOIN uprt2 t2 ON t1.a = t2.b) RIGHT JOIN uprt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

-- lateral references and parameterized paths
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1 t2 JOIN uprt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1 t2 JOIN uprt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;

RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_seqscan;

--
-- partitioned by multiple columns
--
CREATE TABLE prt1_m (a int, b int, c varchar) PARTITION BY RANGE(a, ((a + b)/2));
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0, 0) TO (250, 250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (250, 250) TO (500, 500);
CREATE TABLE prt1_m_p3 PARTITION OF prt1_m FOR VALUES FROM (500, 500) TO (600, 600);
INSERT INTO prt1_m SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1_m;
ANALYZE prt1_m_p1;
ANALYZE prt1_m_p2;
ANALYZE prt1_m_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt1_m AS SELECT * FROM prt1_m;

CREATE TABLE prt2_m (a int, b int, c varchar) PARTITION BY RANGE(((b + a)/2), b);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (0, 0) TO (250, 250);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250, 250) TO (500, 500);
CREATE TABLE prt2_m_p3 PARTITION OF prt2_m FOR VALUES FROM (500, 500) TO (600, 600);
INSERT INTO prt2_m SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE prt2_m;
ANALYZE prt2_m_p1;
ANALYZE prt2_m_p2;
ANALYZE prt2_m_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt2_m AS SELECT * FROM prt2_m;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_m t1 RIGHT JOIN uprt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE prt1_m.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_m WHERE prt2_m.b % 25 = 0) t2 ON (t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE prt1_m.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_m WHERE prt2_m.b % 25 = 0) t2 ON (t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_m t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2_m t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2) ORDER BY t1.a, t2.b;

--
-- tests for list partitioned tables.
--
CREATE TABLE plt1 (a int, b int, c text) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0000', '0003', '0004', '0010');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0001', '0005', '0002', '0009');
CREATE TABLE plt1_p3 PARTITION OF plt1 FOR VALUES IN ('0006', '0007', '0008', '0011');
INSERT INTO plt1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE plt1;
ANALYZE plt1_p1;
ANALYZE plt1_p2;
ANALYZE plt1_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt1 AS SELECT * FROM plt1;

CREATE TABLE plt2 (a int, b int, c text) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0000', '0003', '0004', '0010');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0001', '0005', '0002', '0009');
CREATE TABLE plt2_p3 PARTITION OF plt2 FOR VALUES IN ('0006', '0007', '0008', '0011');
INSERT INTO plt2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE plt2;
ANALYZE plt2_p1;
ANALYZE plt2_p2;
ANALYZE plt2_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt2 AS SELECT * FROM plt2;

--
-- list partitioned by expression
--
CREATE TABLE plt1_e (a int, b int, c text) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0000', '0003', '0004', '0010');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0001', '0005', '0002', '0009');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e FOR VALUES IN ('0006', '0007', '0008', '0011');
INSERT INTO plt1_e SELECT i, i, 'A' || to_char(i/50, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE plt1_e;
ANALYZE plt1_e_p1;
ANALYZE plt1_e_p2;
ANALYZE plt1_e_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt1_e AS SELECT * FROM plt1_e;

--
-- N-way join
--
EXPLAIN (VERBOSE, COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM uplt1 t1, uplt2 t2, uplt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 LEFT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN uplt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 LEFT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN uplt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 LEFT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN uplt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 RIGHT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN uplt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM uplt1_e WHERE uplt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uplt1 t1 WHERE t1.c IN (SELECT t1.c FROM uplt2 t1, uplt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uplt1 t1 WHERE t1.c IN (SELECT t1.c FROM uplt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM uplt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--
-- negative testcases
--

-- joins where one of the relations is proven empty
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a = 1 AND t1.a = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a = 1 AND a = 2) t1 LEFT JOIN prt2 t2 ON t1.a = t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a = 1 AND a = 2) t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a = 1 AND a = 2) t1 FULL JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

CREATE TABLE prt1_n (a int, b int, c varchar) PARTITION BY RANGE(c);
CREATE TABLE prt1_n_p1 PARTITION OF prt1_n FOR VALUES FROM ('0000') TO ('0250');
CREATE TABLE prt1_n_p2 PARTITION OF prt1_n FOR VALUES FROM ('0250') TO ('0500');
INSERT INTO prt1_n SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 499, 2) i;
ANALYZE prt1_n;
ANALYZE prt1_n_p1;
ANALYZE prt1_n_p2;

CREATE TABLE prt2_n (a int, b int, c text) PARTITION BY LIST(c);
CREATE TABLE prt2_n_p1 PARTITION OF prt2_n FOR VALUES IN ('0000', '0003', '0004', '0010', '0006', '0007');
CREATE TABLE prt2_n_p2 PARTITION OF prt2_n FOR VALUES IN ('0001', '0005', '0002', '0009', '0008', '0011');
INSERT INTO prt2_n SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt2_n;
ANALYZE prt2_n_p1;
ANALYZE prt2_n_p2;

CREATE TABLE prt3_n (a int, b int, c text) PARTITION BY LIST(c);
CREATE TABLE prt3_n_p1 PARTITION OF prt3_n FOR VALUES IN ('0000', '0004', '0006', '0007');
CREATE TABLE prt3_n_p2 PARTITION OF prt3_n FOR VALUES IN ('0001', '0002', '0008', '0010');
CREATE TABLE prt3_n_p3 PARTITION OF prt3_n FOR VALUES IN ('0003', '0005', '0009', '0011');
INSERT INTO prt2_n SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt3_n;
ANALYZE prt3_n_p1;
ANALYZE prt3_n_p2;
ANALYZE prt3_n_p3;

CREATE TABLE prt4_n (a int, b int, c text) PARTITION BY RANGE(a);
CREATE TABLE prt4_n_p1 PARTITION OF prt4_n FOR VALUES FROM (0) TO (300);
CREATE TABLE prt4_n_p2 PARTITION OF prt4_n FOR VALUES FROM (300) TO (500);
CREATE TABLE prt4_n_p3 PARTITION OF prt4_n FOR VALUES FROM (500) TO (600);
INSERT INTO prt4_n SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt4_n;
ANALYZE prt4_n_p1;
ANALYZE prt4_n_p2;
ANALYZE prt4_n_p3;

-- partition-wise join can not be applied if the partition ranges differ
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt4_n t2 WHERE t1.a = t2.a;
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 FULL JOIN prt4_n t2 ON t1.a = t2.a;

-- partition-wise join can not be applied if there are no equi-join conditions
-- between partition keys
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a < t2.b);

-- equi-join with join condition on partial keys does not qualify for
-- partition-wise join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = (t2.b + t2.a)/2 AND t1.a % 25 = 0;

-- equi-join between out-of-order partition key columns does not qualify for
-- partition-wise join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b WHERE t1.a % 25 = 0;

-- equi-join between non-key columns does not qualify for partition-wise join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.c = t2.c WHERE t1.a % 25 = 0;

-- partition-wise join can not be applied for a join between list and range
-- partitioned table
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1, prt2_n t2 WHERE t1.c = t2.c;
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1 LEFT JOIN prt2_n t2 ON (t1.c = t2.c);

-- partition-wise join can not be applied between tables with different
-- partition lists
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1 RIGHT JOIN prt1 t2 ON (t1.c = t2.c);
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1 FULL JOIN prt1 t2 ON (t1.c = t2.c);

--
-- multi-leveled partitions
--
CREATE TABLE prt1_l (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_l_p1 PARTITION OF prt1_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_l_p2 PARTITION OF prt1_l FOR VALUES FROM (250) TO (500) PARTITION BY RANGE (c);
CREATE TABLE prt1_l_p2_p1 PARTITION OF prt1_l_p2 FOR VALUES FROM ('0250') TO ('0400');
CREATE TABLE prt1_l_p2_p2 PARTITION OF prt1_l_p2 FOR VALUES FROM ('0400') TO ('0500');
CREATE TABLE prt1_l_p3 PARTITION OF prt1_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (b);
CREATE TABLE prt1_l_p3_p1 PARTITION OF prt1_l_p3 FOR VALUES FROM (500) TO (550);
CREATE TABLE prt1_l_p3_p2 PARTITION OF prt1_l_p3 FOR VALUES FROM (550) TO (600);
INSERT INTO prt1_l SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1_l;
ANALYZE prt1_l_p1;
ANALYZE prt1_l_p2;
ANALYZE prt1_l_p2_p1;
ANALYZE prt1_l_p2_p2;
ANALYZE prt1_l_p3;
ANALYZE prt1_l_p3_p1;
ANALYZE prt1_l_p3_p2;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt1_l AS SELECT * FROM prt1_l;

CREATE TABLE prt2_l (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_l_p1 PARTITION OF prt2_l FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_l_p2 PARTITION OF prt2_l FOR VALUES FROM (250) TO (500) PARTITION BY RANGE (c);
CREATE TABLE prt2_l_p2_p1 PARTITION OF prt2_l_p2 FOR VALUES FROM ('0250') TO ('0400');
CREATE TABLE prt2_l_p2_p2 PARTITION OF prt2_l_p2 FOR VALUES FROM ('0400') TO ('0500');
CREATE TABLE prt2_l_p3 PARTITION OF prt2_l FOR VALUES FROM (500) TO (600) PARTITION BY RANGE (a);
CREATE TABLE prt2_l_p3_p1 PARTITION OF prt2_l_p3 FOR VALUES FROM (500) TO (525);
CREATE TABLE prt2_l_p3_p2 PARTITION OF prt2_l_p3 FOR VALUES FROM (525) TO (600);
INSERT INTO prt2_l SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE prt2_l;
ANALYZE prt2_l_p1;
ANALYZE prt2_l_p2;
ANALYZE prt2_l_p2_p1;
ANALYZE prt2_l_p2_p2;
ANALYZE prt2_l_p3;
ANALYZE prt2_l_p3_p1;
ANALYZE prt2_l_p3_p2;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uprt2_l AS SELECT * FROM prt2_l;

-- inner join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.c = t2.c AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.c = t2.c AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_l t1, uprt2_l t2 WHERE t1.a = t2.b AND t1.c = t2.c AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

-- left join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 LEFT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 LEFT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_l t1 LEFT JOIN uprt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

-- right join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_l t1 RIGHT JOIN uprt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

-- full join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE prt1_l.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_l WHERE prt2_l.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE prt1_l.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_l WHERE prt2_l.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE uprt1_l.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2_l WHERE uprt2_l.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;

-- lateral partition-wise join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1_l t2 JOIN uprt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;

-- lateral references with clauses without equi-join on partition key
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.b = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.b = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM uprt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM uprt1_l t2 JOIN uprt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.b = ss.t2a AND t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY t1.a;
--==========================================================================================================================================================
-- EXTRA replication TESTCASES, now removed by dev
CREATE TABLE plt2_e (a int, b int, c text) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0000', '0003', '0004', '0010');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0001', '0005', '0002', '0009');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e FOR VALUES IN ('0006', '0007', '0008', '0011');
INSERT INTO plt2_e SELECT i, i, 'A' || to_char(i/50, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE plt2_e;
ANALYZE plt2_e_p1;
ANALYZE plt2_e_p2;
ANALYZE plt2_e_p3;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt2_e AS SELECT * FROM plt2_e;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1 RIGHT JOIN prt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1 RIGHT JOIN prt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_e t1 RIGHT JOIN uprt2_e t2 ON (t1.a + t1.b)/2 = (t2.b + t2.a)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_e WHERE prt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_e WHERE prt2_e.b % 25 = 0) t2 ON ((t1.a + t1.b)/2 = (t2.b + t2.a)/2) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_e WHERE prt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_e WHERE prt2_e.b % 25 = 0) t2 ON ((t1.a + t1.b)/2 = (t2.b + t2.a)/2) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_e t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2_e t2 WHERE t2.b % 25 = 0) t2 ON ((t1.a + t1.b)/2 = (t2.b + t2.a)/2) ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_m t1, uprt2_m t2 WHERE t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_m t1 LEFT JOIN uprt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1, plt2 t2 WHERE t1.c = t2.c AND t1.a = t2.a AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1, plt2 t2 WHERE t1.c = t2.c AND t1.a = t2.a AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1 t1, uplt2 t2 WHERE t1.c = t2.c AND t1.a = t2.a AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1 t1 LEFT JOIN uplt2 t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1 t1 RIGHT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

-- Join with pruned partitions from joining relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM plt1 t1, plt2 t2 WHERE t1.c = t2.c AND t1.c NOT IN ('0001', '0005', '0002', '0009') AND t2.c NOT IN ('0000', '0003', '0004', '0010') GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM plt1 t1, plt2 t2 WHERE t1.c = t2.c AND t1.c NOT IN ('0001', '0005', '0002', '0009') AND t2.c NOT IN ('0000', '0003', '0004', '0010') GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM uplt1 t1, uplt2 t2 WHERE t1.c = t2.c AND t1.c NOT IN ('0001', '0005', '0002', '0009') AND t2.c NOT IN ('0000', '0003', '0004', '0010') GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 LEFT JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 LEFT JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM uplt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 LEFT JOIN (SELECT * FROM uplt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 RIGHT JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 RIGHT JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM uplt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 RIGHT JOIN (SELECT * FROM uplt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 FULL JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM plt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 FULL JOIN (SELECT * FROM plt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t2.b), t2.c FROM (SELECT * FROM uplt1 t1 WHERE t1.c NOT IN ('0001', '0005', '0002', '0009')) t1 FULL JOIN (SELECT * FROM uplt2 t2 WHERE t2.c NOT IN ('0000', '0003', '0004', '0010')) t2 ON t1.c = t2.c GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uplt1 t1 WHERE t1.c IN (SELECT t1.c FROM uplt2 t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1, plt2_e t2 WHERE t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1, plt2_e t2 WHERE t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_e t1, uplt2_e t2 WHERE t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_e t1 LEFT JOIN uplt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_e t1 RIGHT JOIN uplt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_e t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2_e t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY t1.a, t2.b;

--==========================================================================================================================================================
---added as replication based on dev range test cases
-- lateral reference
EXPLAIN (VERBOSE, COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
--TODO - this query need to remove once testing done.
SELECT distinct * FROM uplt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM uplt1 t2 JOIN uplt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
--TODO - this query need to remove once testing done.
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM uplt1_e WHERE uplt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

-- test merge join with and without index scan
CREATE INDEX iplt1_p1_c on plt1_p1(c);
CREATE INDEX iplt1_p2_c on plt1_p2(c);
CREATE INDEX iplt1_p3_c on plt1_p3(c);
CREATE INDEX iplt2_p1_c on plt2_p1(c);
CREATE INDEX iplt2_p2_c on plt2_p2(c);
CREATE INDEX iplt2_p3_c on plt2_p3(c);
CREATE INDEX iplt1_e_p1_c on plt1_e_p1(ltrim(c, 'A'));
CREATE INDEX iplt1_e_p2_c on plt1_e_p2(ltrim(c, 'A'));
CREATE INDEX iplt1_e_p3_c on plt1_e_p3(ltrim(c, 'A'));

ANALYZE plt1;
ANALYZE plt1_p1;
ANALYZE plt1_p2;
ANALYZE plt1_p3;
ANALYZE plt2;
ANALYZE plt2_p1;
ANALYZE plt2_p2;
ANALYZE plt2_p3;
ANALYZE plt1_e;
ANALYZE plt1_e_p1;
ANALYZE plt1_e_p2;
ANALYZE plt1_e_p3;

SET enable_hashjoin TO off;
SET enable_nestloop TO off;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 RIGHT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN uplt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM uplt1_e WHERE uplt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uplt1 t1 WHERE t1.c IN (SELECT t1.c FROM uplt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM uplt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

SET enable_seqscan TO off;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (uplt1 t1 RIGHT JOIN uplt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN uplt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM uplt1 WHERE uplt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2 WHERE uplt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM uplt1_e WHERE uplt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM uplt1 t1 WHERE t1.c IN (SELECT t1.c FROM uplt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM uplt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- lateral references and parameterized paths
EXPLAIN (VERBOSE, COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
--TODO - this query need to remove once testing done.
SELECT distinct * FROM uplt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM uplt1 t2 JOIN uplt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
--TODO - this query need to remove once testing done.
SELECT distinct * FROM uplt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM uplt1 t2 JOIN uplt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_seqscan;
--
-- negative testcases
--
-- joins where one of the relations is proven empty
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1, plt2 t2 WHERE t1.a = t2.b AND t1.a = 1 AND t1.a = 2;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1 t1, plt2 t2 WHERE t1.a = t2.b AND t1.a = 1 AND t1.a = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 LEFT JOIN plt2 t2 ON t1.a = t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 LEFT JOIN plt2 t2 ON t1.a = t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1 WHERE a = 1 AND a = 2) t1 RIGHT JOIN uplt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 FULL JOIN plt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1 WHERE a = 1 AND a = 2) t1 FULL JOIN plt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1 WHERE a = 1 AND a = 2) t1 FULL JOIN uplt2 t2 ON t1.a = t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (SELECT 50 phv, * FROM prt1_l WHERE prt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_l WHERE prt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and t2.a + t2.b = t1.b + t1.a) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;
SELECT * FROM (SELECT 50 phv, * FROM prt1_l WHERE prt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_l WHERE prt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and t2.a + t2.b = t1.b + t1.a) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;
--TODO - this query need to remove once testing done.
SELECT * FROM (SELECT 50 phv, * FROM uprt1_l WHERE uprt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uprt2_l WHERE uprt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and t2.a + t2.b = t1.b + t1.a) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;

-- Join with pruned partitions from joining relations
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM uprt1_l t1, uprt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM uprt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM prt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM prt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM uprt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM prt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM uprt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM prt1_l t1 WHERE t1.a IN (SELECT t1.b FROM prt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_l t1 WHERE t1.a IN (SELECT t1.b FROM prt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
--TODO - this query need to remove once testing done.
SELECT t1.* FROM uprt1_l t1 WHERE t1.a IN (SELECT t1.b FROM uprt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;

--
-- negative testcases
--

-- joins where one of the relations is proven empty
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b AND t1.a = 1 AND t1.a = 2;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b AND t1.a = 1 AND t1.a = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 LEFT JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 LEFT JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN uprt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uprt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN uprt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

--creating test data set for multi-level list partitioning
CREATE TABLE plt1_l (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_l_p1 PARTITION OF plt1_l FOR VALUES IN ('0000', '0003', '0004', '0010') PARTITION BY LIST (c);
CREATE TABLE plt1_l_p1_p1 PARTITION OF plt1_l_p1 FOR VALUES IN ('0000', '0003');
CREATE TABLE plt1_l_p1_p2 PARTITION OF plt1_l_p1 FOR VALUES IN ('0004', '0010');
CREATE TABLE plt1_l_p2 PARTITION OF plt1_l FOR VALUES IN ('0001', '0005', '0002', '0009') PARTITION BY LIST (c);
CREATE TABLE plt1_l_p2_p1 PARTITION OF plt1_l_p2 FOR VALUES IN ('0001', '0005');
CREATE TABLE plt1_l_p2_p2 PARTITION OF plt1_l_p2 FOR VALUES IN ('0002', '0009');
CREATE TABLE plt1_l_p3 PARTITION OF plt1_l FOR VALUES IN ('0006', '0007', '0008', '0011') PARTITION BY LIST (ltrim(c,'A'));
CREATE TABLE plt1_l_p3_p1 PARTITION OF plt1_l_p3 FOR VALUES IN ('0006', '0007');
CREATE TABLE plt1_l_p3_p2 PARTITION OF plt1_l_p3 FOR VALUES IN ('0008', '0011');
INSERT INTO plt1_l SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE plt1_l;
ANALYZE plt1_l_p1;
ANALYZE plt1_l_p1_p1;
ANALYZE plt1_l_p1_p2;
ANALYZE plt1_l_p2;
ANALYZE plt1_l_p2_p1;
ANALYZE plt1_l_p2_p2;
ANALYZE plt1_l_p3;
ANALYZE plt1_l_p3_p1;
ANALYZE plt1_l_p3_p2;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt1_l AS SELECT * FROM plt1_l;

CREATE TABLE plt2_l (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_l_p1 PARTITION OF plt2_l FOR VALUES IN ('0000', '0003', '0004', '0010') PARTITION BY LIST (c);
CREATE TABLE plt2_l_p1_p1 PARTITION OF plt2_l_p1 FOR VALUES IN ('0000', '0003');
CREATE TABLE plt2_l_p1_p2 PARTITION OF plt2_l_p1 FOR VALUES IN ('0004', '0010');
CREATE TABLE plt2_l_p2 PARTITION OF plt2_l FOR VALUES IN ('0001', '0005', '0002', '0009') PARTITION BY LIST (c);
CREATE TABLE plt2_l_p2_p1 PARTITION OF plt2_l_p2 FOR VALUES IN ('0001', '0005');
CREATE TABLE plt2_l_p2_p2 PARTITION OF plt2_l_p2 FOR VALUES IN ('0002', '0009');
CREATE TABLE plt2_l_p3 PARTITION OF plt2_l FOR VALUES IN ('0006', '0007', '0008', '0011') PARTITION BY LIST (ltrim(c,'A'));
CREATE TABLE plt2_l_p3_p1 PARTITION OF plt2_l_p3 FOR VALUES IN ('0006', '0007');
CREATE TABLE plt2_l_p3_p2 PARTITION OF plt2_l_p3 FOR VALUES IN ('0008', '0011');
INSERT INTO plt2_l SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE plt2_l;
ANALYZE plt2_l_p1;
ANALYZE plt2_l_p1_p1;
ANALYZE plt2_l_p1_p2;
ANALYZE plt2_l_p2;
ANALYZE plt2_l_p2_p1;
ANALYZE plt2_l_p2_p2;
ANALYZE plt2_l_p3;
ANALYZE plt2_l_p3_p1;
ANALYZE plt2_l_p3_p2;
-- TODO: This table is created only for testing the results. Remove once
-- results are tested.
CREATE TABLE uplt2_l AS SELECT * FROM plt2_l;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_l t1, uplt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1 LEFT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1 LEFT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_l t1 LEFT JOIN uplt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1 RIGHT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1 RIGHT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_l t1 RIGHT JOIN uplt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE plt1_l.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_l WHERE plt2_l.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE plt1_l.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_l WHERE plt2_l.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2_l t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) ORDER BY t1.a, t2.b;

-- lateral reference
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM plt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM plt1_l t2 JOIN plt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.a = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM plt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM plt1_l t2 JOIN plt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.a = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;
--TODO - this query need to remove once testing done.
SELECT * FROM uplt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM uplt1_l t2 JOIN uplt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.a = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM plt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM plt1_l t2 JOIN plt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.b = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;
SELECT * FROM plt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM plt1_l t2 JOIN plt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.b = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;
--TODO - this query need to remove once testing done.
SELECT * FROM uplt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.a AS t3a, least(t1.c,t2.c,t3.c) FROM uplt1_l t2 JOIN uplt2_l t3 ON (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A'))) ss
			  ON t1.b = ss.t2a AND t1.b = ss.t2a AND t1.c = ss.t2c AND ltrim(t1.c,'A') = ltrim(ss.t2c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (SELECT 50 phv, * FROM plt1_l WHERE plt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2_l WHERE plt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and ltrim(t2.c,'A') = ltrim(t1.c,'A')) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;
SELECT * FROM (SELECT 50 phv, * FROM plt1_l WHERE plt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2_l WHERE plt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and ltrim(t2.c,'A') = ltrim(t1.c,'A')) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;
--TODO - this query need to remove once testing done.
SELECT * FROM (SELECT 50 phv, * FROM uplt1_l WHERE uplt1_l.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM uplt2_l WHERE uplt2_l.b % 25 = 0) t2 ON (t1.a = t2.b and t1.b = t2.a and t1.c = t2.c and ltrim(t2.c,'A') = ltrim(t1.c,'A')) WHERE t1.phv = t1.b OR t2.phv = t2.b ORDER BY 1,2,3,4,5,6,7,8;

-- Join with pruned partitions from joining relations
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM uplt1_l t1, uplt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a < 450 AND t2.b > 250 AND t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM plt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM plt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l WHERE a < 450) t1 LEFT JOIN (SELECT * FROM uplt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM plt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM plt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l WHERE a < 450) t1 RIGHT JOIN (SELECT * FROM uplt2_l WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l WHERE a < 450 AND a % 25 = 0) t1 FULL JOIN (SELECT * FROM uplt2_l WHERE b > 250 AND b % 25 = 0) t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.* FROM plt1_l t1 WHERE t1.c IN (SELECT t1.c FROM plt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1_l t1 WHERE t1.c IN (SELECT t1.c FROM plt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;
--TODO - this query need to remove once testing done.
SELECT t1.* FROM uplt1_l t1 WHERE t1.c IN (SELECT t1.c FROM uplt2_l t1 WHERE t1.b % 25 = 0) AND t1.a % 25 = 0 ORDER BY t1.a;

--
-- negative testcases
--

-- joins where one of the relations is proven empty
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a = 1 AND t1.a = 2;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_l t1, plt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') AND t1.a = 1 AND t1.a = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 LEFT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A');
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 LEFT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A');

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN uplt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN plt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM uplt1_l WHERE a = 1 AND a = 2) t1 FULL JOIN uplt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') WHERE t2.a % 25 = 0 ORDER BY t1.a, t2.b;

--=========================================================================================================================================================
--added test cases based on join.sql file
--creating test dataset
create temp table tplt1 (x1 int, x2 int) partition by list(x1);
CREATE temp TABLE tplt1_p1 PARTITION OF tplt1 FOR VALUES IN (1,2,3);
CREATE temp TABLE tplt1_p2 PARTITION OF tplt1 FOR VALUES IN (4,5);
insert into tplt1 values (1,11);
insert into tplt1 values (2,22);
insert into tplt1 values (3,null);
insert into tplt1 values (4,44);
insert into tplt1 values (5,null);
create temp table tplt2 (y1 int, y2 int) partition by list(y1);
CREATE temp TABLE tplt2_p1 PARTITION OF tplt2 FOR VALUES IN (1,2,3);
CREATE temp TABLE tplt2_p2 PARTITION OF tplt2 FOR VALUES IN (4,5);
insert into tplt2 values (1,111);
insert into tplt2 values (2,222);
insert into tplt2 values (3,333);
insert into tplt2 values (4,null);

-- test with temp table and for propagation of nullability constraints into sub-joins
EXPLAIN (COSTS OFF)
select * from (tplt1 left join tplt2 on (x1 = y1)) left join tplt1 xx(xx1,xx2) 
on (x1 = xx1 and xx2 is not null);
select * from (tplt1 left join tplt2 on (x1 = y1)) left join tplt1 xx(xx1,xx2) 
on (x1 = xx1 and xx2 is not null);

-- test with temp table and for propagation of nullability constraints into sub-joins
EXPLAIN (COSTS OFF)
select * from (tplt1 left join tplt2 on (x1 = y1)) left join tplt1 xx(xx1,xx2) 
on (x1 = xx1) where (xx2 is not null);
select * from (tplt1 left join tplt2 on (x1 = y1)) left join tplt1 xx(xx1,xx2) 
on (x1 = xx1) where (xx2 is not null);

-- CORRELATION NAMES
EXPLAIN (VERBOSE, COSTS OFF)
SELECT '' AS "xxx", * FROM plt1 t1 (a, b, c) JOIN plt2 t2 (a, b, c) USING (c) 
WHERE t1.a % 25 = 0 and t2.b % 25 = 0 ORDER BY t1.a, t2.a;
SELECT '' AS "xxx", * FROM plt1 t1 (a, b, c) JOIN plt2 t2 (a, b, c) USING (c) 
WHERE t1.a % 25 = 0 and t2.b % 25 = 0 ORDER BY t1.a, t2.a;
--TODO - this query need to remove once testing done.
SELECT '' AS "xxx", * FROM uplt1 t1 (a, b, c) JOIN uplt2 t2 (a, b, c) USING (c) 
WHERE t1.a % 25 = 0 and t2.b % 25 = 0 ORDER BY t1.a, t2.a;

--equi-joins +  Non-equi-joins
EXPLAIN (VERBOSE, COSTS OFF)
SELECT '' AS "xxx", * FROM prt1 t1 JOIN prt2 t2 ON (t1.a = t2.b) AND (t1.a <= t2.b) 
WHERE t1.a % 25 = 0 order by 1,2,3,4;
SELECT '' AS "xxx", * FROM prt1 t1 JOIN prt2 t2 ON (t1.a = t2.b) AND (t1.a <= t2.b) 
WHERE t1.a % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
SELECT '' AS "xxx", * FROM uprt1 t1 JOIN uprt2 t2 ON (t1.a = t2.b) AND (t1.a <= t2.b) 
WHERE t1.a % 25 = 0 order by 1,2,3,4;

--full join using
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM plt1 t1 FULL JOIN plt2 t2 USING (c) FULL JOIN plt1 t3 USING (c) 
where t1.a %150 =0 and t2.b % 150 = 0 and t3.a % 150 = 0 order by 1,2,3;
SELECT * FROM plt1 t1 FULL JOIN plt2 t2 USING (c) FULL JOIN plt1 t3 USING (c) 
where t1.a %150 =0 and t2.b % 150 = 0 and t3.a % 150 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
SELECT * FROM uplt1 t1 FULL JOIN uplt2 t2 USING (c) FULL JOIN plt1 t3 USING (c) 
where t1.a %150 =0 and t2.b % 150 = 0 and t3.a % 150 = 0 order by 1,2,3;

--natural join with subquery
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (SELECT c, a as s1_n FROM plt1) as s1 NATURAL FULL JOIN (SELECT * FROM 
(SELECT c, b as s2_n, 2 as s2_2 FROM plt2 where a % 25 = 0 and b % 25 =0) as s2 
NATURAL FULL JOIN(SELECT c, a as s3_n FROM plt1 where a % 25 = 0 and b % 25 =0 ) as s3 ) ss2 
where s1_n % 25 = 0 ORDER by 1,2,3,4,5;
SELECT * FROM (SELECT c, a as s1_n FROM plt1) as s1 NATURAL FULL JOIN (SELECT * FROM 
(SELECT c, b as s2_n, 2 as s2_2 FROM plt2 where a % 25 = 0 and b % 25 =0) as s2 
NATURAL FULL JOIN(SELECT c, a as s3_n FROM plt1 where a % 25 = 0 and b % 25 =0 ) as s3 ) ss2 
where s1_n % 25 = 0 ORDER by 1,2,3,4,5;
--TODO - this query need to remove once testing done.
SELECT * FROM (SELECT c, a as s1_n FROM uplt1) as s1 NATURAL FULL JOIN (SELECT * FROM 
(SELECT c, b as s2_n, 2 as s2_2 FROM uplt2 where a % 25 = 0 and b % 25 =0) as s2 
NATURAL FULL JOIN(SELECT c, a as s3_n FROM uplt1 where a % 25 = 0 and b % 25 =0 ) as s3 ) ss2 
where s1_n % 25 = 0 ORDER by 1,2,3,4,5;

--check for failure to generate a plan with multiple degenerate IN clauses
EXPLAIN (VERBOSE, COSTS OFF)
select count(*) from prt1 x where (x.a,x.b) in (select t1.a,t2.b from prt1 t1,prt2 t2 where t1.a=t2.b) 
and (x.c) in (select t3.c from plt1 t3,plt2 t4 where t3.c=t4.c);
select count(*) from prt1 x where (x.a,x.b) in (select t1.a,t2.b from prt1 t1,prt2 t2 where t1.a=t2.b) 
and (x.c) in (select t3.c from plt1 t3,plt2 t4 where t3.c=t4.c);
--TODO - this query need to remove once testing done.
select count(*) from uprt1 x where (x.a,x.b) in (select t1.a,t2.b from uprt1 t1,uprt2 t2 where t1.a=t2.b) 
and (x.c) in (select t3.c from uplt1 t3,uplt2 t4 where t3.c=t4.c);

--check handling of outer joins within semijoin and anti joins
EXPLAIN (VERBOSE, COSTS OFF)
select * from prt1 t1 where exists (select 1 from prt1 t2 left join prt2 t3 on t2.a = t3.b 
where not exists (select 1 from prt1 t4 left join prt2 t5 on t4.a = t5.b where t1.a % 25 <> 0));
select * from prt1 t1 where exists (select 1 from prt1 t2 left join prt2 t3 on t2.a = t3.b 
where not exists (select 1 from prt1 t4 left join prt2 t5 on t4.a = t5.b where t1.a % 25 <> 0));
--TODO - this query need to remove once testing done.
select * from uprt1 t1 where exists (select 1 from uprt1 t2 left join uprt2 t3 on t2.a = t3.b 
where not exists (select 1 from uprt1 t4 left join uprt2 t5 on t4.a = t5.b where t1.a % 25 <> 0));

-- test placement of movable quals in a parameterized join tree
EXPLAIN (VERBOSE, COSTS OFF)
select * from prt1 t1 left join (prt2 t2 join prt1 t3 on t2.b = t3.a) on t1.a = t2.b and t1.a = t3.a where t1.a %25 = 0;
select * from prt1 t1 left join (prt2 t2 join prt1 t3 on t2.b = t3.a) on t1.a = t2.b and t1.a = t3.a where t1.a %25 = 0;
--TODO - this query need to remove once testing done.
select * from uprt1 t1 left join (uprt2 t2 join uprt1 t3 on t2.b = t3.a) on t1.a = t2.b and t1.a = t3.a where t1.a %25 = 0;

-- test placement of movable quals in a parameterized join tree
EXPLAIN (VERBOSE, COSTS OFF)
select b.b from prt1 a join prt2 b on a.a = b.b left join plt1 c on b.a % 25 = 0 and c.c = a.c join prt1 i1 on b.b = i1.a
right join prt2 i2 on i2.b = b.b where b.a % 25 = 0 order by 1;
select b.b from prt1 a join prt2 b on a.a = b.b left join plt1 c on b.a % 25 = 0 and c.c = a.c join prt1 i1 on b.b = i1.a
right join prt2 i2 on i2.b = b.b where b.a % 25 = 0 order by 1;
--TODO - this query need to remove once testing done.
select b.b from uprt1 a join uprt2 b on a.a = b.b left join uplt1 c on b.a % 25 = 0 and c.c = a.c join uprt1 i1 on b.b = i1.a
right join uprt2 i2 on i2.b = b.b where b.a % 25 = 0 order by 1;

-- lateral with VALUES
EXPLAIN (VERBOSE, COSTS OFF)
select count(*) from prt1 a, prt2 b join lateral (values(a.a)) ss(x) on b.b = ss.x;
select count(*) from prt1 a, prt2 b join lateral (values(a.a)) ss(x) on b.b = ss.x;
--TODO - this query need to remove once testing done.
select count(*) from uprt1 a, uprt2 b join lateral (values(a.a)) ss(x) on b.b = ss.x;

--========================================================================================================================================================
-- added test cases for sql objects
--creating data set for range partition
create view prt1_view as select * from prt1;
create view prt2_view as select * from prt2;
create view prt1_prt2_view as select t1.a,t2.b,t1.c from prt1 t1 inner join prt2 t2 on (t1.a = t2.b);

--cross join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1 t1 cross join prt2 t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;
select t1.a,t2.b from prt1 t1 cross join prt2 t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1 t1 cross join uprt2 t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;

--inner join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from uprt1 t1 inner join prt2 t2 on (t1.a = t2.b) inner join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from uprt1 t1 inner join prt2 t2 on (t1.a = t2.b) inner join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 order by 1,2,3;

--left outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) left outer join uprt1 t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) left outer join uprt1 t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--right outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
 select t1.a,t2.b,t3.a from prt1 t1 right outer join prt2 t2 on (t1.a = t2.b) right outer join uprt1 t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
 select t1.a,t2.b,t3.a from prt1 t1 right outer join prt2 t2 on (t1.a = t2.b) right outer join uprt1 t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--full outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 full outer join prt2 t2 on (t1.a = t2.b) full outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 full outer join prt2 t2 on (t1.a = t2.b) full outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 order by 1,2,3;

-- natural join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1 t1 natural join prt2 t2 where t1.a % 25 = 0 order by 1,2;
select t1.a,t2.b from prt1 t1 natural join prt2 t2 where t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1 t1 natural join uprt2 t2 where t1.a % 25 = 0 order by 1,2;

-- semi join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.a from prt1 t1 where exists (select 1 from prt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;
select t1.a,t1.b,t1.a from prt1 t1 where exists (select 1 from prt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.a from uprt1 t1 where exists (select 1 from uprt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;

-- anti join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.a from prt1 t1 where not exists (select 1 from prt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;
select t1.a,t1.b,t1.a from prt1 t1 where not exists (select 1 from prt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.a from uprt1 t1 where not exists (select 1 from uprt2 t2 where t1.a = t2.b) and t1.a % 25 = 0 order by 1,2;

-- self join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b as t1_b,t2.b as t2_b from prt1 t1, prt2 t2 where t1.a = t2.b and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b as t1_b,t2.b as t2_b from prt1 t1, prt2 t2 where t1.a = t2.b and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b as t1_b,t2.b as t2_b from uprt1 t1, uprt2 t2 where t1.a = t2.b and t1.a % 25 = 0 order by 1,2,3;

-- join with CTE
EXPLAIN (VERBOSE, COSTS OFF)
with ED as (select t1.b,t2.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;
with ED as (select t1.b,t2.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
with ED as (select t1.b,t2.a from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;

--Join combinations
--cross and inner join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 inner join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 inner join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 cross join uprt2 t2 inner join uprt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;

--cross and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 left outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 left outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 cross join uprt2 t2 left outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;

--cross and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 cross join uprt2 t2 right outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;

--cross and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 full outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 cross join prt2 t2 full outer join prt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 cross join uprt2 t2 full outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;

--inner and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) left outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) left outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) left outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) right outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) full outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) full outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) full outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) right outer join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 left outer join uprt2 t2 on (t1.a = t2.b) right outer join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) full join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 left outer join prt2 t2 on (t1.a = t2.b) full join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 left outer join uprt2 t2 on (t1.a = t2.b) full join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1 t1 right join prt2 t2 on (t1.a = t2.b) full join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1 t1 right join prt2 t2 on (t1.a = t2.b) full join prt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1 t1 right join uprt2 t2 on (t1.a = t2.b) full join uprt1 t3 on (t2.b = t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
-- Join with views
--join of two partition table simple views
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_view t1 inner join prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_view t1 inner join prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of one partition table and one partition table simple view
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1 t1 inner join prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1 t1 inner join prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of two partition table complex views
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_prt2_view t1 inner join prt1_prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_prt2_view t1 inner join prt1_prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of one partition table and other partition table complex view
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1 t1 inner join prt1_prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1 t1 inner join prt1_prt2_view t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;

-- join with expressions and system functions
-- join with like operator
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t2.b from prt1 t1 inner join prt2 t2 on t1.a = t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t2.b from prt1 t1 inner join prt2 t2 on t1.a = t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t2.b from uprt1 t1 inner join uprt2 t2 on t1.a = t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;

-- join with rank
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;

-- join with array expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a,t1.b,t2.b FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;
SELECT t1.a,t1.b,t2.b FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
SELECT t1.a,t1.b,t2.b FROM uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;

-- join with group by and having
EXPLAIN (VERBOSE, COSTS OFF)
select t2.b,sum(t1.a) from prt1 t1 inner join prt2 t2 on(t1.a = t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;
select t2.b,sum(t1.a) from prt1 t1 inner join prt2 t2 on(t1.a = t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;
--TODO - this query need to remove once testing done.
select t2.b,sum(t1.a) from uprt1 t1 inner join uprt2 t2 on(t1.a = t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;

--join with prepare statement 
PREPARE ij(int) AS select t1.a,t2.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b and t1.a % $1 = 0) ORDER BY 1,2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE ij(25);
EXECUTE ij(25);
DEALLOCATE ij;

--join with for share clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;
select t1.a,t2.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;

--join with for update clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;
select t1.a,t2.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1 t1 inner join uprt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;

-- join in cursor
BEGIN;
DECLARE ffc CURSOR FOR SELECT t1.a,t2.b FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0;
FETCH ALL from ffc;
END;

-- join in function
CREATE FUNCTION fun_fft() RETURNS refcursor AS $$
DECLARE
        ref_cursor REFCURSOR := 'cur_fft';
BEGIN
        OPEN ref_cursor FOR SELECT t1.a,t2.b FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0;
        RETURN (ref_cursor);    
END;
$$ LANGUAGE plpgsql;
BEGIN;
SELECT fun_fft();
FETCH ALL from cur_fft; 
COMMIT;
DROP FUNCTION fun_fft();

-- join in user defined functions
CREATE FUNCTION pwj_range_sum(int,int) RETURNS int AS $$
BEGIN
RETURN $1 + $2;
END
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT t1.a, t2.b, pwj_range_sum(t1.a,t2.b) FROM prt1 t1 inner join prt2 t2 on (t1.a = t2.b) where t1.a % 25 = 0 order by 1,2,3;
DROP FUNCTION pwj_range_sum(int,int) ;


--creating data set for list partition
create view plt1_view as select * from plt1;
create view plt2_view as select * from plt2;
create view plt1_plt2_view as select t1.a,t2.b,t1.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c);

--cross join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t2.b,t2.c from plt1 t1 cross join plt2 t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;
select t1.a,t1.c,t2.b,t2.c from plt1 t1 cross join plt2 t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t2.b,t2.c from uplt1 t1 cross join uplt2 t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;

--inner join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) inner join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.a % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) inner join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.a % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;

--left outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) left outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) left outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;

--right outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 right outer join plt2 t2 on (t1.c = t2.c) right outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 right outer join plt2 t2 on (t1.c = t2.c) right outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 125 = 0 and t3.a % 125 = 0 order by 1,2,3,4;

--full outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 full outer join plt2 t2 on (t1.c = t2.c) full outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1 t1 full outer join plt2 t2 on (t1.c = t2.c) full outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3,4;

-- natural join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t2.b,t2.c from plt1 t1 natural join plt2 t2 where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t1.c,t2.b,t2.c from plt1 t1 natural join plt2 t2 where t1.a % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t2.b,t2.c from uplt1 t1 natural join uplt2 t2 where t1.a % 25 = 0 order by 1,2,3,4;

-- semi join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.b from plt1 t1 where exists (select 1 from plt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t1.b from plt1 t1 where exists (select 1 from plt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.b from uplt1 t1 where exists (select 1 from uplt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;

-- anti join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.b from plt1 t1 where not exists (select 1 from plt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t1.b from plt1 t1 where not exists (select 1 from plt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.b from uplt1 t1 where not exists (select 1 from uplt2 t2 where t1.c = t2.c) and t1.a % 25 = 0 order by 1,2,3;

-- self join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from plt1 t1, plt2 t2 where t1.c = t2.c and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from plt1 t1, plt2 t2 where t1.c = t2.c and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from uplt1 t1, uplt2 t2 where t1.c = t2.c and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;

-- join with CTE
EXPLAIN (VERBOSE, COSTS OFF)
with ED as (select t1.b,t1.c,t2.a from plt1 t1 inner join plt2 t2 on (t1.c = t2.c)) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;
with ED as (select t1.b,t1.c,t2.a from plt1 t1 inner join plt2 t2 on (t1.c = t2.c)) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;
--TODO - this query need to remove once testing done.
with ED as (select t1.b,t1.c,t2.a from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c)) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;

--Join combinations
--cross and inner join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 inner join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 inner join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 cross join uplt2 t2 inner join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 left outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 left outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 cross join uplt2 t2 left outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 cross join uplt2 t2 right outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 full outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 cross join plt2 t2 full outer join plt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 cross join uplt2 t2 full outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--inner and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) left outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) right outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) full outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 left outer join uplt2 t2 on (t1.c = t2.c) right outer join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 left outer join uplt2 t2 on (t1.c = t2.c) full join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1 t1 right join uplt2 t2 on (t1.c = t2.c) full join uplt1 t3 on (t2.c = t3.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

-- Join with views
--join of two partition table simple views
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_view t1 inner join plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_view t1 inner join plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;

--join of one partition table and one partition table simple view
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1 t1 inner join plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1 t1 inner join plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;

--join of two partition table complex views
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_plt2_view t1 inner join plt1_plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t1.b % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_plt2_view t1 inner join plt1_plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t1.b % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;

--join of one partition table and other partition table complex view
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1 t1 inner join plt1_plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1 t1 inner join plt1_plt2_view t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;

-- join with expressions and system functions
-- join with like operator
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t1.b,t2.b from plt1 t1 inner join plt2 t2 on t1.c = t2.c and t1.c like '0%0' and t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2;
select t1.a,t1.c,t1.b,t2.b from plt1 t1 inner join plt2 t2 on t1.c = t2.c and t1.c like '0%0' and t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t1.b,t2.b from uplt1 t1 inner join uplt2 t2 on t1.c = t2.c and t1.c like '0%0' and t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2;

-- join with rank
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;

-- join with array expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a,t1.c,t1.b,t2.b FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;
SELECT t1.a,t1.c,t1.b,t2.b FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
SELECT t1.a,t1.c,t1.b,t2.b FROM uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;

-- join with group by and having
EXPLAIN (VERBOSE, COSTS OFF)
select t2.b,sum(t1.a) from plt1 t1 inner join plt2 t2 on(t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;
select t2.b,sum(t1.a) from plt1 t1 inner join plt2 t2 on(t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;
--TODO - this query need to remove once testing done.
select t2.b,sum(t1.a) from uplt1 t1 inner join uplt2 t2 on(t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;

--join with prepare statement 
PREPARE ij(int) AS select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c and t1.a % $1 = 0 and t2.b % $1 = 0) ORDER BY 1,2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE ij(25);
EXECUTE ij(25);
DEALLOCATE ij;

--join with for share clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;
select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;
--TODO - this query need to remove once testing done.
select t1.c,t2.c from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;

--join with for update clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;
select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;
--TODO - this query need to remove once testing done.
select t1.c,t2.c from uplt1 t1 inner join uplt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;

-- join in cursor
BEGIN;
DECLARE ffc CURSOR FOR SELECT t1.c,t2.c FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 ;
FETCH ALL from ffc;
END;

-- join in function
CREATE FUNCTION fun_fft() RETURNS refcursor AS $$
DECLARE
        ref_cursor REFCURSOR := 'cur_fft';
BEGIN
        OPEN ref_cursor FOR SELECT t1.c,t2.c FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0 ;
        RETURN (ref_cursor);    
END;
$$ LANGUAGE plpgsql;
BEGIN;
SELECT fun_fft();
FETCH ALL from cur_fft; 
COMMIT;
DROP FUNCTION fun_fft();

-- join in user defined functions
CREATE FUNCTION pwj_range_sum(int,int) RETURNS int AS $$
BEGIN
RETURN $1 + $2;
END
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT t1.c, t2.c, pwj_range_sum(t1.a,t2.b) FROM plt1 t1 inner join plt2 t2 on (t1.c = t2.c) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2,3;
DROP FUNCTION pwj_range_sum(int,int) ;

--creating data set for multilevel-range partition
create view prt1_l_view as select * from prt1_l;
create view prt2_l_view as select * from prt2_l;
create view prt1_l_prt2_l_view as select t1.a,t2.b,t1.c from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b);

--cross join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1_l t1 cross join prt2_l t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;
select t1.a,t2.b from prt1_l t1 cross join prt2_l t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1_l t1 cross join uprt2_l t2 where t1.a % 150 = 0 and t2.b % 150 = 0 order by 1,2;

--inner join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from uprt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) inner join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from uprt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) inner join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 order by 1,2,3;

--left outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) left outer join uprt1_l t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) left outer join uprt1_l t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--right outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
 select t1.a,t2.b,t3.a from prt1_l t1 right outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join uprt1_l t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
 select t1.a,t2.b,t3.a from prt1_l t1 right outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join uprt1_l t3 on (t1.a = t2.a) where t1.a % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--full outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 full outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 full outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 order by 1,2,3;

-- natural join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1_l t1 natural join prt2_l t2 where t1.a % 25 = 0 order by 1,2;
select t1.a,t2.b from prt1_l t1 natural join prt2_l t2 where t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1_l t1 natural join uprt2_l t2 where t1.a % 25 = 0 order by 1,2;

-- semi join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.a from prt1_l t1 where exists (select 1 from prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;
select t1.a,t1.b,t1.a from prt1_l t1 where exists (select 1 from prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.a from uprt1_l t1 where exists (select 1 from uprt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;

-- anti join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.a from prt1_l t1 where not exists (select 1 from prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;
select t1.a,t1.b,t1.a from prt1_l t1 where not exists (select 1 from prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.a from uprt1_l t1 where not exists (select 1 from uprt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) and t1.a % 25 = 0 order by 1,2;

-- self join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b as t1_b,t2.b as t2_b from prt1_l t1, prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b as t1_b,t2.b as t2_b from prt1_l t1, prt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b as t1_b,t2.b as t2_b from uprt1_l t1, uprt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.a % 25 = 0 order by 1,2,3;

-- join with CTE
EXPLAIN (VERBOSE, COSTS OFF)
with ED as (select t1.b,t2.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;
with ED as (select t1.b,t2.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;
--TODO - this query need to remove once testing done.
with ED as (select t1.b,t2.a from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b)) select b,a from ED where a % 25 = 0 order by 1,2;

--Join combinations
--cross and inner join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 inner join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 inner join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 cross join uprt2_l t2 inner join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;

--cross and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 left outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 left outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 cross join uprt2_l t2 left outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;

--cross and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 cross join uprt2_l t2 right outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 25= 0 order by 1,2,3;

--cross and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 full outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 cross join prt2_l t2 full outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 cross join uprt2_l t2 full outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 125 = 0 and t2.b % 125= 0 order by 1,2,3;

--inner and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) left outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) left outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) left outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 left outer join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) right outer join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 left outer join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 left outer join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.a from prt1_l t1 right join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
select t1.a,t2.b,t3.a from prt1_l t1 right join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join prt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.a from uprt1_l t1 right join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) full join uprt1_l t3 on (t2.b = t3.a AND t2.a = t3.b AND t2.c = t3.c AND t2.a + t2.b = t3.b + t3.a) where t1.a % 25 = 0 and t2.b % 25= 0 order by 1,2,3;

-- Join with views
--join of two partition table simple views
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_l_view t1 inner join prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_l_view t1 inner join prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of one partition table and one partition table simple view
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_l t1 inner join prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_l t1 inner join prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of two partition table complex views
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_l_prt2_l_view t1 inner join prt1_l_prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_l_prt2_l_view t1 inner join prt1_l_prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;

--join of one partition table and other partition table complex view
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.a,t2.b from prt1_l t1 inner join prt1_l_prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
select t1.a,t2.a,t2.b from prt1_l t1 inner join prt1_l_prt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;

-- join with expressions and system functions
-- join with like operator
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t2.b from prt1_l t1 inner join prt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t2.b from prt1_l t1 inner join prt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t2.b from uprt1_l t1 inner join uprt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.c like '0%0' and t1.a % 25 = 0 order by 1,2,3;

-- join with rank
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;

-- join with array expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a,t1.b,t2.b FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;
SELECT t1.a,t1.b,t2.b FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
SELECT t1.a,t1.b,t2.b FROM uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3;

-- join with group by and having
EXPLAIN (VERBOSE, COSTS OFF)
select t2.b,sum(t1.a) from prt1_l t1 inner join prt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;
select t2.b,sum(t1.a) from prt1_l t1 inner join prt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;
--TODO - this query need to remove once testing done.
select t2.b,sum(t1.a) from uprt1_l t1 inner join uprt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 1,2;

--join with prepare statement 
PREPARE ij(int) AS select t1.a,t2.b from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b and t1.a % $1 = 0) ORDER BY 1,2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE ij(25);
EXECUTE ij(25);
DEALLOCATE ij;

--join with for share clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;
select t1.a,t2.b from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR SHARE;

--join with for update clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;
select t1.a,t2.b from prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;
--TODO - this query need to remove once testing done.
select t1.a,t2.b from uprt1_l t1 inner join uprt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2 FOR UPDATE;

-- join in cursor
BEGIN;
DECLARE ffc CURSOR FOR SELECT t1.a,t2.b FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0;
FETCH ALL from ffc;
END;

-- join in function
CREATE FUNCTION fun_fft() RETURNS refcursor AS $$
DECLARE
        ref_cursor REFCURSOR := 'cur_fft';
BEGIN
        OPEN ref_cursor FOR SELECT t1.a,t2.b FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0;
        RETURN (ref_cursor);    
END;
$$ LANGUAGE plpgsql;
BEGIN;
SELECT fun_fft();
FETCH ALL from cur_fft; 
COMMIT;
DROP FUNCTION fun_fft();

-- join in user defined functions
CREATE FUNCTION pwj_range_sum(int,int) RETURNS int AS $$
BEGIN
RETURN $1 + $2;
END
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT t1.a, t2.b, pwj_range_sum(t1.a,t2.b) FROM prt1_l t1 inner join prt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND t1.b + t1.a = t2.a + t2.b) where t1.a % 25 = 0 order by 1,2,3;
DROP FUNCTION pwj_range_sum(int,int) ;



--creating data set for multilevel-list partition
create view plt1_l_view as select * from plt1_l;
create view plt2_l_view as select * from plt2_l;
create view plt1_l_plt2_l_view as select t1.a,t2.b,t1.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A'));

--cross join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t2.b,t2.c from plt1_l t1 cross join plt2_l t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;
select t1.a,t1.c,t2.b,t2.c from plt1_l t1 cross join plt2_l t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t2.b,t2.c from uplt1_l t1 cross join uplt2_l t2 where t1.a % 125 = 0 and t2.b % 125 = 0 order by 1,2,3,4;

--inner join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) inner join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) inner join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;

--left outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) left outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) left outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;

--right outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 right outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 right outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;

--full outer join
--TODO - this query need to remove once testing done.
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 full outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t2.b,t1.c,t2.c,t3.c from plt1_l t1 full outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 order by 1,2,3,4;

-- natural join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t2.b,t2.c from plt1_l t1 natural join plt2_l t2 where t1.a % 25 = 0 order by 1,2,3,4;
select t1.a,t1.c,t2.b,t2.c from plt1_l t1 natural join plt2_l t2 where t1.a % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t2.b,t2.c from uplt1_l t1 natural join uplt2_l t2 where t1.a % 25 = 0 order by 1,2,3,4;

-- semi join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.b from plt1_l t1 where exists (select 1 from plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t1.b from plt1_l t1 where exists (select 1 from plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.b from uplt1_l t1 where exists (select 1 from uplt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;

-- anti join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.b,t1.b from plt1_l t1 where not exists (select 1 from plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;
select t1.a,t1.b,t1.b from plt1_l t1 where not exists (select 1 from plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t1.b,t1.b from uplt1_l t1 where not exists (select 1 from uplt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) and t1.a % 25 = 0 order by 1,2,3;

-- self join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from plt1_l t1, plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from plt1_l t1, plt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t1.c as t1_c,t2.c as t2_C from uplt1_l t1, uplt2_l t2 where t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.a % 25 = 0 and t2. b  % 25 = 0  order by 1,2,3,4;

-- join with CTE
EXPLAIN (VERBOSE, COSTS OFF)
with ED as (select t1.b,t1.c,t2.a from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A'))) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;
with ED as (select t1.b,t1.c,t2.a from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A'))) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;
--TODO - this query need to remove once testing done.
with ED as (select t1.b,t1.c,t2.a from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A'))) select b,c,a from ED where b % 25 = 0 and a % 25 = 0 ;

--Join combinations
--cross and inner join
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 inner join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 inner join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 cross join uplt2_l t2 inner join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 left outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 left outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 cross join uplt2_l t2 left outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 cross join uplt2_l t2 right outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--cross and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 full outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 cross join plt2_l t2 full outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 cross join uplt2_l t2 full outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 125 = 0 and t2.b % 25 = 0 and t3.a % 125 = 0 order by 1,2,3;

--inner and left outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) left outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) left outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) left outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 left outer join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) right outer join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 left outer join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 left outer join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t2.b,t3.c from plt1_l t1 right join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1_l t1 right join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join plt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;
--TODO - this query need to remove once testing done.
select t1.a,t2.b,t3.c from uplt1_l t1 right join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) full join uplt1_l t3 on (t2.a = t3.b AND t2.b = t3.a AND t2.c = t3.c AND ltrim(t2.c,'A') = ltrim(t3.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t3.a % 25 = 0 order by 1,2,3;

-- Join with views
--join of two partition table simple views
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_l_view t1 inner join plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_l_view t1 inner join plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;

--join of one partition table and one partition table simple view
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_l t1 inner join plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_l t1 inner join plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4,5,6;

--join of two partition table complex views
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_l_plt2_l_view t1 inner join plt1_l_plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t1.b % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_l_plt2_l_view t1 inner join plt1_l_plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t1.b % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;

--join of one partition table and other partition table complex view
EXPLAIN (VERBOSE, COSTS OFF)
select  t1.*,t2.* from plt1_l t1 inner join plt1_l_plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;
select  t1.*,t2.* from plt1_l t1 inner join plt1_l_plt2_l_view t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 and t2.a % 25 = 0 order by 1,2,3,4,5,6;

-- join with expressions and system functions
-- join with like operator
EXPLAIN (VERBOSE, COSTS OFF)
select t1.a,t1.c,t1.b,t2.b from plt1_l t1 inner join plt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.c like '0%0' order by 1,2;
select t1.a,t1.c,t1.b,t2.b from plt1_l t1 inner join plt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.c like '0%0' order by 1,2;
--TODO - this query need to remove once testing done.
select t1.a,t1.c,t1.b,t2.b from uplt1_l t1 inner join uplt2_l t2 on t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.c like '0%0' order by 1,2;

-- join with rank
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
SELECT t1.a, t1.b, t2.b,rank() OVER (ORDER BY t2.b DESC) FROM uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 order by 1,2,3,4;

-- join with array expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a,t1.c,t1.b,t2.b FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;
SELECT t1.a,t1.c,t1.b,t2.b FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;
--TODO - this query need to remove once testing done.
SELECT t1.a,t1.c,t1.b,t2.b FROM uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) WHERE t1.a = ANY(ARRAY[t2.b, 1, t2.b + 0]) and t1.a % 25 = 0 order by 1,2,3,4;

-- join with group by and having
EXPLAIN (VERBOSE, COSTS OFF)
select t2.b,sum(t1.a) from plt1_l t1 inner join plt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;
select t2.b,sum(t1.a) from plt1_l t1 inner join plt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;
--TODO - this query need to remove once testing done.
select t2.b,sum(t1.a) from uplt1_l t1 inner join uplt2_l t2 on(t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 group by t2.b having sum(t1.a) > 2 order by 2;

--join with prepare statement 
PREPARE ij(int) AS select t1.c,t2.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A') and t1.a % $1 = 0 and t2.b % $1 = 0) ORDER BY 1,2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE ij(25);
EXECUTE ij(25);
DEALLOCATE ij;

--join with for share clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.c,t2.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;
select t1.c,t2.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;
--TODO - this query need to remove once testing done.
select t1.c,t2.c from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR SHARE;

--join with for update clause
EXPLAIN (VERBOSE, COSTS OFF)
select t1.c,t2.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;
select t1.c,t2.c from plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;
--TODO - this query need to remove once testing done.
select t1.c,t2.c from uplt1_l t1 inner join uplt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2 FOR UPDATE;

-- join in cursor
BEGIN;
DECLARE ffc CURSOR FOR SELECT t1.c,t2.c FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 ;
FETCH ALL from ffc;
END;

-- join in function
CREATE FUNCTION fun_fft() RETURNS refcursor AS $$
DECLARE
        ref_cursor REFCURSOR := 'cur_fft';
BEGIN
        OPEN ref_cursor FOR SELECT t1.c,t2.c FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0 ;
        RETURN (ref_cursor);    
END;
$$ LANGUAGE plpgsql;
BEGIN;
SELECT fun_fft();
FETCH ALL from cur_fft; 
COMMIT;
DROP FUNCTION fun_fft();

-- join in user defined functions
CREATE FUNCTION pwj_range_sum(int,int) RETURNS int AS $$
BEGIN
RETURN $1 + $2;
END
$$ LANGUAGE plpgsql IMMUTABLE;
SELECT t1.c, t2.c, pwj_range_sum(t1.a,t2.b) FROM plt1_l t1 inner join plt2_l t2 on (t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c AND ltrim(t1.c,'A') = ltrim(t2.c,'A')) where t1.a % 25 = 0 and t2.b % 25 = 0  order by 1,2,3;
DROP FUNCTION pwj_range_sum(int,int) ;

--===============================================================================================================================================================
---issues encountered during testing.
--SERVER CRASH --fixed
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT * FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c) WHERE t1.c = 'P1' AND t1.c  =  'P2';
SELECT * FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c) WHERE t1.c = 'P1' AND t1.c  =  'P2';

--SERVER CRASH --fixed
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT t1.*, t2.*,t3.* FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.a = t3.b) WHERE t1.a % 25 = 0;
SELECT t1.*, t2.*,t3.* FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.a = t3.b) WHERE t1.a % 25 = 0;

--ERROR -- fixed
EXPLAIN (VERBOSE, COSTS OFF) 
select t1.a, count(t2.*) from prt1 t1 left join prt1 t2 on (t1.a = t2.a) group by t1.a;
select t1.a, count(t2.*) from prt1 t1 left join prt1 t2 on (t1.a = t2.a) group by t1.a;

--SERVER CRASH --fixed
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT t1.a, t1.b, t2.b, t2.a FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a  AND t1.a < 450 AND t2.b > 250;
SELECT t1.a, t1.b, t2.b, t2.a FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = t2.a  AND t1.a < 450 AND t2.b > 250;

--ERROR -- fixed
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT t1.* FROM prt1 t1 INNER JOIN prt1 t2 ON (t1.a = t2.a and t1.b = t2.b) WHERE t1.a % 25 = 0 FOR UPDATE;
SELECT t1.* FROM prt1 t1 INNER JOIN prt1 t2 ON (t1.a = t2.a and t1.b = t2.b) WHERE t1.a % 25 = 0 FOR UPDATE;

--SERVER CRASH --fixed
EXPLAIN (VERBOSE, COSTS OFF)
select count(*) from prt1 x where (x.a,x.b) in (select t1.a,t2.b from prt1 t1,prt2 t2 where t1.a=t2.b) 
and (x.c) in (select t3.c from plt1 t3,plt2 t4 where t3.c=t4.c);
select count(*) from prt1 x where (x.a,x.b) in (select t1.a,t2.b from prt1 t1,prt2 t2 where t1.a=t2.b) 
and (x.c) in (select t3.c from plt1 t3,plt2 t4 where t3.c=t4.c);

--SERVER CRASH --fixed
EXPLAIN (VERBOSE, COSTS OFF)
select * from prt1 t1 left join (prt2 t2 join prt1 t3 on t2.b = t3.a) on t1.a = t2.b and t1.a = t3.a where t1.a %25 = 0;
select * from prt1 t1 left join (prt2 t2 join prt1 t3 on t2.b = t3.a) on t1.a = t2.b and t1.a = t3.a where t1.a %25 = 0;
--===============================================================================================================================================================
--test cases covering parameterised path plan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.a) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.b = ss.t2a WHERE t1.a % 25 = 0 ORDER BY t1.a;

--===============================================================================================================================================================
--Test cases covering different join methods hashjoin,nestloop & mergejoin
--hash join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2 WHERE t2.b % 25 = 0 ORDER BY t1.a, t2.b;
--nested loop
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
--merge join
PREPARE ij(int) AS select t1.c,t2.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c and t1.a % $1 = 0 and t2.b % $1 = 0) ORDER BY 1,2;
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE ij(25);
EXECUTE ij(25);
DEALLOCATE ij;

--===============================================================================================================================================================
