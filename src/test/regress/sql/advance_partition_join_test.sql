--
-- Test advance partition-wise join between partitioned tables
--

SET enable_partitionwise_join to true;
--SET client_min_messages to debug3;
--cleanup of existing partition tables
DROP TABLE prt1;
DROP TABLE prt2;
DROP TABLE prt1_m;
DROP TABLE prt2_m;
DROP TABLE prt1_e;
DROP TABLE plt1;
DROP TABLE plt2;
DROP TABLE plt1_e;
--=============================================================================================================================
--
-- partitioned by a single column
--
--=============================================================================================================================
-- prt1 and prt2 both have default partition with excat bound matches.
-- pwj is possible for all cases.

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (MAXVALUE);
CREATE TABLE prt1_p4 PARTITION OF prt1 DEFAULT;
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 2 = 0;
INSERT INTO prt1 SELECT NULL, i % 25, to_char(i, 'FM0000') FROM generate_series(599, 699) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (MAXVALUE);
CREATE TABLE prt2_p4 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 3 = 0;
INSERT INTO prt2 SELECT i % 25, NULL, to_char(i, 'FM0000') FROM generate_series(599, 699) i WHERE i % 3 = 0;
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t1.b, t1.c, t2.a, t2.b, t2.c;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t1.b, t1.c, t2.a, t2.b, t2.c;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY 1,2,3,4;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
-- prt2 have extra partition which is default partition
-- pwj is possible when default partition is outer side of relation.

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 499) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 3 = 0;
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
-- prt2 default partition and prt1 have 500-600 extra range
-- pwj is possible when default partition is outer side of relation. - ??

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 3 = 0;
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
-- prt2 default partition and have gaps in range 
-- pwj is not possible as one partition need to join with more than one partition.

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (-250) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 499) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (-250) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (50) TO (200);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p4 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 799) i WHERE i % 3 = 0 AND (i NOT BETWEEN 0 AND 50 OR i NOT BETWEEN 200 AND 250);
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
-- both partitions have gaps in range which filled by default partition of each other
-- Partition-wise join is not possible when one table need to join
-- with more than one table.

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (300) TO (450);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p4 PARTITION OF prt1 DEFAULT;
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 699) i WHERE i % 2 = 0 AND (i NOT BETWEEN 250 AND 300 OR i NOT BETWEEN 450 AND 500);
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (50) TO (200);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (550) TO (600);
CREATE TABLE prt2_p4 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 699) i WHERE i % 3 = 0 AND (i NOT BETWEEN 0 AND 50 OR i NOT BETWEEN 200 AND 250 OR i NOT BETWEEN 500 AND 550);
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
-- only default is common bounds in both relation
-- partition-wise-join not possible as default range is different

CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (-9);
CREATE TABLE prt1_p2 PARTITION OF prt1 DEFAULT;
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-500, 10) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (10) TO (MAXVALUE);
CREATE TABLE prt2_p2 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-10, 500) i WHERE i % 3 = 0;
ANALYZE prt2;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2 t1 WHERE t1.b IN (SELECT t2.a FROM prt1 t2 WHERE t2.b = 0) AND t1.a = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1 t1 WHERE NOT EXISTS (SELECT 1 FROM prt2 t2 WHERE t1.a = t2.b);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;
SELECT * FROM prt1 t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

-- nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT t1.a FROM prt1 t1 WHERE t1.b = 0)) AND t1.b = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) RIGHT JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 FULL JOIN prt2 t2 ON (t1.a = t2.b) FULL JOIN prt1 t3 ON (t2.b = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) INNER JOIN prt2 t4 ON (t3.a = t4.b) RIGHT JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 LEFT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 INNER JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) RIGHT JOIN prt2 t4 ON (t3.a = t4.b) FULL JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) LEFT JOIN prt1 t3 ON (t2.b = t3.a) FULL JOIN prt2 t4 ON (t3.a = t4.b) INNER JOIN prt1 t5 ON (t4.b = t5.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;
--=============================================================================================================================
--
-- partitioned by a multiple column
--
--=============================================================================================================================
-- prt1_m and prt2_m both have default partition with excat bound matches.
-- pwj is possible for all cases.

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt1_m_p3 PARTITION OF prt1_m FOR VALUES FROM (500,500) TO (MAXVALUE,MAXVALUE);
CREATE TABLE prt1_m_p4 PARTITION OF prt1_m DEFAULT;
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-250, 599) i WHERE i % 2 = 0;
INSERT INTO prt1_m SELECT i, i, i % 35  FROM generate_series(-250, 599) i WHERE i % 2 = 0;
INSERT INTO prt1_m SELECT NULL, i, i % 25  FROM generate_series(599, 699) i WHERE i % 2 = 0;
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt2_m_p3 PARTITION OF prt2_m FOR VALUES FROM (500,500) TO (MAXVALUE,MAXVALUE);
CREATE TABLE prt2_m_p4 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-250, 599) i WHERE i % 3 = 0;
INSERT INTO prt2_m SELECT i, i, i % 35  FROM generate_series(-250, 599) i WHERE i % 3 = 0;
INSERT INTO prt2_m SELECT i, NULL, i % 25  FROM generate_series(599, 699) i WHERE i % 3 = 0;
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1,t2;
SELECT DISTINCT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1,t2;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM (SELECT 50 phv, * FROM prt1_m) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM (SELECT 50 phv, * FROM prt1_m) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT DISTINCT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT DISTINCT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT t1.a, avg(t1.a) FROM prt2_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt1_m t2 WHERE t1.a = t2.b AND t1.b = t2.a) AND t1.a %25 =  0 GROUP BY t1.a ORDER BY t1.a;
SELECT t1.a, avg(t1.a) FROM prt2_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt1_m t2 WHERE t1.a = t2.b AND t1.b = t2.a) AND t1.a %25 =  0 GROUP BY t1.a ORDER BY t1.a;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct t1.a, t2b,least_all FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t2.b AS t2b, t3.a AS t3a, t3.b AS t3b,least(t1.a,t1.b,t2.a,t2.b,t3.a,t3.b) AS least_all FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b AND t2.b = t3.a)) ss ON t1.a = ss.t2a AND t1.b = ss.t2b WHERE t1.c = 0 ORDER BY t1.a;
SELECT distinct t1.a, t2b,least_all FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t2.b AS t2b, t3.a AS t3a, t3.b AS t3b,least(t1.a,t1.b,t2.a,t2.b,t3.a,t3.b) AS least_all FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b AND t2.b = t3.a)) ss ON t1.a = ss.t2a AND t1.b = ss.t2b WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT DISTINCT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t1.b, t2.a, t2.b, t3.a, t3.b FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT DISTINCT t1.a, t2.b, t3.b, t4.a, t5.a FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
-- prt2_m have extra partition which is default partition 
-- pwj is possible when default partition is outer side of relation.

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (250,250) TO (500,500);
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-250, 499) i WHERE i % 2 = 0;
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt2_m_p3 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-250, 599) i WHERE i % 3 = 0;
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
-- prt2_m default partition and prt1_m have 500-600 extra range 
-- pwj is possible when default partition is outer side of relation. - ??

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt1_m_p3 PARTITION OF prt1_m FOR VALUES FROM (500,500) TO (600,600);
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-250, 599) i WHERE i % 2 = 0;
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt2_m_p3 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-250, 599) i WHERE i % 3 = 0;
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
-- prt2_m default partition and have gaps in range 
-- pwj is not possible as one partition need to join with more than one partition.

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (-250,-250) TO (0,0);
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (250,250) TO (500,500);
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-250, 499) i WHERE i % 2 = 0;
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (-250,-250) TO (0,0);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (50,50) TO (200,200);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt2_m_p4 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-250, 799) i WHERE i % 3 = 0 AND (i NOT BETWEEN 0 AND 50 OR i NOT BETWEEN 200 AND 250);
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
-- both partitions have gaps in range which filled by default partition of each other
-- Partition-wise join is not possible when one table need to join
-- with more than one table.

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt1_m_p1 PARTITION OF prt1_m FOR VALUES FROM (0,0) TO (250,250);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m FOR VALUES FROM (300,300) TO (450,450);
CREATE TABLE prt1_m_p3 PARTITION OF prt1_m FOR VALUES FROM (500,500) TO (600,600);
CREATE TABLE prt1_m_p4 PARTITION OF prt1_m DEFAULT;
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-250, 699) i WHERE i % 2 = 0 AND (i NOT BETWEEN 250 AND 300 OR i NOT BETWEEN 450 AND 500);
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (0,0);
CREATE TABLE prt2_m_p1 PARTITION OF prt2_m FOR VALUES FROM (50,50) TO (200,200);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m FOR VALUES FROM (250,250) TO (500,500);
CREATE TABLE prt2_m_p3 PARTITION OF prt2_m FOR VALUES FROM (550,550) TO (600,600);
CREATE TABLE prt2_m_p4 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-250, 699) i WHERE i % 3 = 0 AND (i NOT BETWEEN 0 AND 50 OR i NOT BETWEEN 200 AND 250 OR i NOT BETWEEN 500 AND 550);
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
-- only default is common bounds in both relation
-- partition wise join not possible.

CREATE TABLE prt1_m (a int, b int, c int) PARTITION BY RANGE(a,b);
CREATE TABLE prt1_m_p0 PARTITION OF prt1_m FOR VALUES FROM (MINVALUE,MINVALUE) TO (-9,-9);
CREATE TABLE prt1_m_p2 PARTITION OF prt1_m DEFAULT;
INSERT INTO prt1_m SELECT i, i, i % 25  FROM generate_series(-500, 10) i WHERE i % 2 = 0;
ANALYZE prt1_m;

CREATE TABLE prt2_m (a int, b int, c int) PARTITION BY RANGE(b,a);
CREATE TABLE prt2_m_p0 PARTITION OF prt2_m FOR VALUES FROM (10,10) TO (MAXVALUE,MAXVALUE);
CREATE TABLE prt2_m_p2 PARTITION OF prt2_m DEFAULT;
INSERT INTO prt2_m SELECT i, i, i % 25  FROM generate_series(-10, 500) i WHERE i % 3 = 0;
ANALYZE prt2_m;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.c = 0 ORDER BY t1.a, t2.b;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1_m t1 LEFT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t2.c = 0 ORDER BY t1.a, t2.b;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = t2.b AND t1.b = t2.a) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a AND t1.a < 450 AND t2.b > 250 AND t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t2.b,t2.a FROM prt2_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;
SELECT t1.* FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t2.a,t2.b FROM prt1_m t2 WHERE t2.c = 0) AND t1.c = 0 ORDER BY t1.b;

-- Anti-join with aggregates
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);
SELECT sum(t1.a), avg(t1.a), sum(t1.b), avg(t1.b) FROM prt1_m t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_m t2 WHERE t1.a = t2.b AND t1.b = t2.a);

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;
SELECT * FROM prt1_m t1 LEFT JOIN LATERAL (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1_m t2 JOIN prt2_m t3 ON (t2.a = t3.b)) ss ON t1.a = ss.t2a WHERE t1.c = 0 ORDER BY t1.a;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t2.b FROM (SELECT * FROM prt1_m WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2_m WHERE b > 250) t2 ON t1.a = t2.b AND t1.b = t2.a WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--N-WAY joins
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;
SELECT t1.* FROM prt1_m t1 WHERE (t1.a,t1.b) IN (SELECT t1.b,t1.a FROM prt2_m t1 WHERE (t1.b,t1.a) IN (SELECT t1.a,t1.b FROM prt1_m t1)) AND t1.c = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) RIGHT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 FULL JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) FULL JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) INNER JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) INNER JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) RIGHT JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 LEFT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 INNER JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) RIGHT JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) FULL JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1_m t1 RIGHT JOIN prt2_m t2 ON (t1.a = t2.b AND t1.b = t2.a) LEFT JOIN prt1_m t3 ON (t2.b = t3.a AND t2.a = t3.b) FULL JOIN prt2_m t4 ON (t3.a = t4.b AND t3.b = t4.a) INNER JOIN prt1_m t5 ON (t4.b = t5.a AND t4.a = t5.b) WHERE t1.c = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1_m;
DROP TABLE prt2_m;
--=============================================================================================================================
--
-- partitioned by list and partition by expression as partition key
--
--=============================================================================================================================
-- plt1 and plt2 both have default partition with excat bound matches.
-- pwj is possible for all cases.
-- NOTE: But in N-way joins, although PWJ is considered while creating the
-- paths, the costs come out to be higher than non-PWJ paths (esp.
-- (parallel hash partitioning), so PWJ plan is
-- chosen. Need to increase the data?

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_p3 PARTITION OF plt1 DEFAULT;
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e DEFAULT;
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- plt2 have extra partition which is default partition
-- pwj is possible when default partition is outer side of relation.

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0005','0006','0007','0008');
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,9,10);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0005','0006','0007','0008');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0005','0006','0007','0008');
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,9,10);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0005','0006','0007','0008');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- plt2 have default partition and plt1 have some extra list values 
-- pwj is possible when default partition is outer side of relation. - ?? (right is picking pwj but for range it is not picking)

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_p3 PARTITION OF plt1 FOR VALUES IN ('0007','0008','0009');
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e FOR VALUES IN ('0007','0008','0009');
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- both partitions have some different list values which filled by default partition of each other
-- Partition-wise join is not possible when one table need to join
-- with more than one table.

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0003','0004');
CREATE TABLE plt1_p3 PARTITION OF plt1 DEFAULT;
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (7,8);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0007','0008');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (3,4);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0003','0004');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e DEFAULT;
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (7,8);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0007','0008');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (3,4);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- only default is common bounds in both relation
-- partition-wise-join not possible as default list is different

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0003','0004');
CREATE TABLE plt1_p3 PARTITION OF plt1 DEFAULT;
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (5,6,7,8);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0005','0006');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0007','0008');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (1,2,3,4);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0003','0004');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e DEFAULT;
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (5,6,7,8);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0005','0006');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0007','0008');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (1,2,3,4);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- default with NULL list value.
-- both relation have NULL value with same list bounds and a default partition
-- partition-wise-join possible for all cases.

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_p3 PARTITION OF plt1 DEFAULT;
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt1 SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt2 SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e DEFAULT;
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt1_e SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt2_e SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
-- default with NULL list value.
-- one relation have NULL value and default and other have only default partition
-- partition-wise-join not possible as it need one relation to join with two partitions.

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0004','0005','0006');
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3,7,8,9);
INSERT INTO plt1 SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt2 SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt2;

CREATE TABLE plt1_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0001','0002',NULL);
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0004','0005','0006');
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3,7,8,9);
INSERT INTO plt1_e SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt1_e;

CREATE TABLE plt2_e (a int, b int, c varchar) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt2_e_p1 PARTITION OF plt2_e FOR VALUES IN ('0001','0002');
CREATE TABLE plt2_e_p2 PARTITION OF plt2_e FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_e_p3 PARTITION OF plt2_e DEFAULT;
INSERT INTO plt2_e SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10,3);
INSERT INTO plt2_e SELECT i, i % 47, NULL FROM generate_series(0, 500) i WHERE i % 11 IN (3);
ANALYSE plt2_e;

-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 INNER JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + t2.b = 0 ORDER BY t1.a;

-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- left outer join, with whole-row reference
EXPLAIN (COSTS OFF)
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;
SELECT t1,t2 FROM plt1 t1 LEFT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.b + coalesce(t2.b, 0) = 0 ORDER BY t1.a;

-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + t2.b = 0 ORDER BY t1.a;

-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1 t1 FULL JOIN plt2 t2 ON t1.c = t2.c WHERE coalesce(t1.b, 0) + coalesce(t2.b, 0) = 0 ORDER BY t1.a, t2.a;

-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;
SELECT DISTINCT  t1.c, t2.c FROM (SELECT '0001' phv, * FROM plt1) t1 FULL JOIN (SELECT '0002' phv, * FROM plt2) t2 ON (t1.c = t2.c and t2.b = 0 AND t1.b = 0) WHERE (t1.phv = t1.c OR t2.phv = t2.c) ORDER BY 1,2;

-- semi join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- anti join
EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt1 t1 where not exists (select 1 from plt2 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

EXPLAIN (COSTS OFF)
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;
select t1.a, t1.b, t1.c from plt2 t1 where not exists (select 1 from plt1 t2 WHERE t1.c = t2.c) and t1.b = 0 order by t1.a, t1.b, t1.c;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;
SELECT DISTINCT t1.c, t2c,least_all FROM plt1 t1 LEFT JOIN LATERAL (SELECT t2.c AS t2c, t2.b AS t2b, t3.c AS t3c, t3.b AS t3b,least(t1.c,t2.c,t3.c) AS least_all FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss ON t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.c;

--nullable column
EXPLAIN (COSTS OFF)
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;
SELECT t1.c, t2.c FROM (SELECT * FROM plt1 WHERE c < '0000') t1 LEFT JOIN (SELECT * FROM plt2 WHERE c > '0002') t2 ON t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.c, t2.c;

-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) LEFT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 LEFT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1, plt1_e t2 WHERE t1.c = ltrim(t2.c, 'A')) AND t1.a % 25 = 0 ORDER BY t1.a;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, avg(t2.b), t2.c FROM (SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;
SELECT sum(t1.a), t1.c, sum(t1.phv), avg(t2.b), t2.c, avg(t2.phv) FROM (SELECT 25 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 50 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c AND t1.a = t2.b) GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 LEFT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM plt1_e t1 RIGHT JOIN plt2_e t2 ON t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A') WHERE t2.b % 25 = 0 ORDER BY 1,2,3,4;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2_e WHERE plt2_e.b % 25 = 0) t2 ON (t1.a = t2.b AND ltrim(t1.c, 'A') = ltrim(t2.c, 'A')) ORDER BY 1,2,3,4;

-- lateral reference
EXPLAIN (COSTS OFF)
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;
SELECT distinct * FROM plt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.c AS t2c, t3.c AS t3c, least(t1.c,t2.c,t3.c) FROM plt1 t2 JOIN plt2 t3 ON (t2.c = t3.c)) ss
			  ON t1.c = ss.t2c WHERE t1.a % 25 = 0 ORDER BY 1,2,3,4,5,6;

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');
SELECT t1.a, t1.phv, t2.b, t2.phv, ltrim(t3.c,'A'), t3.phv FROM ((SELECT 50 phv, * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT 75 phv, * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.c = t2.c)) FULL JOIN (SELECT '0002'::text phv, * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.c = ltrim(t3.c,'A')) WHERE t1.a = t1.phv OR t2.b = t2.phv OR ltrim(t3.c,'A') = t3.phv ORDER BY t1.a, t2.b, ltrim(t3.c,'A');

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (plt1 t1 RIGHT JOIN plt2 t2 ON t1.a = t2.b AND t1.c = t2.c) RIGHT JOIN plt1_e t3 ON (t2.b = t3.a AND t2.c = ltrim(t3.c, 'A')) WHERE t3.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM ((SELECT * FROM plt1 WHERE plt1.a % 25 = 0) t1 FULL JOIN (SELECT * FROM plt2 WHERE plt2.b % 25 = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c)) FULL JOIN (SELECT * FROM plt1_e WHERE plt1_e.a % 25 = 0) t3 ON (t1.a = t3.a AND ltrim(t3.c, 'A') = t1.c) ORDER BY 1,2,3,4,5,6;

EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;
SELECT t1.* FROM plt1 t1 WHERE t1.c IN (SELECT t1.c FROM plt2 t1 WHERE t1.c IN (SELECT ltrim(t1.c, 'A') FROM plt1_e t1 WHERE t1.a % 25 = 0)) AND t1.a % 25 = 0 ORDER BY t1.a;

--Join combinations
--inner and left outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) left outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--inner and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 inner join plt2 t2 on (t1.c = t2.c) full outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and right outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) right outer join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--left outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 left outer join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--right outer and full outer join 
EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;
select t1.a,t2.b,t3.c from plt1 t1 right join plt2 t2 on (t1.c = t2.c) full join plt1 t3 on (t2.c = t3.c) where t1.b + t2.b +t3.b = 0 order by 1,2,3;

--join combinations (more than 3 joins)
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) INNER JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) INNER JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) RIGHT JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 LEFT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 INNER JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) RIGHT JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) FULL JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;

DROP TABLE plt1;
DROP TABLE plt1_e;
DROP TABLE plt2;
DROP TABLE plt2_e;
--=============================================================================================================================
--
-- test cases with bugs/error/crashes
--
--=============================================================================================================================
--issue-1
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (-21) TO (-10);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (-10) TO (0);

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (-21) TO (-10);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (-10) TO (-5);

EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b, t3.a FROM prt1 t1 INNER JOIN prt2 t2 ON t1.a = t2.b INNER JOIN prt1 t3 ON t2.b = t3.a ORDER BY 1,2,3;
SELECT t1.a, t2.b, t3.a FROM prt1 t1 INNER JOIN prt2 t2 ON t1.a = t2.b INNER JOIN prt1 t3 ON t2.b = t3.a ORDER BY 1,2,3;

DROP TABLE prt1;
DROP TABLE prt2;

--issue-2
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (550);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0+50, 599-50, 3) i;
ANALYZE prt2;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b LEFT JOIN prt2 t3 ON (t1.a = t3.b) WHERE t1.b + t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b LEFT JOIN prt2 t3 ON (t1.a = t3.b) WHERE t1.b + t2.a = 0 ORDER BY t1.a, t2.b;

--issue-3
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b FULL JOIN prt2 t3 ON (t1.a = t3.b) WHERE t1.b + t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b FULL JOIN prt2 t3 ON (t1.a = t3.b) WHERE t1.b + t2.a = 0 ORDER BY t1.a, t2.b;

DROP TABLE prt1;
DROP TABLE prt2;

--issue-4
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
CREATE INDEX iprt1_p1_a on prt1_p1(a);
CREATE INDEX iprt1_p2_a on prt1_p2(a);
CREATE INDEX iprt1_p3_a on prt1_p3(a);
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (50) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (550);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0+50, 599-50, 3) i;
CREATE INDEX iprt2_p1_b on prt2_p1(b);
CREATE INDEX iprt2_p2_b on prt2_p2(b);
CREATE INDEX iprt2_p3_b on prt2_p3(b);
ANALYZE prt2;

CREATE TABLE prt1_e (a int, b int, c int) PARTITION BY RANGE(((a + b)/2));
CREATE TABLE prt1_e_p1 PARTITION OF prt1_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_e_p2 PARTITION OF prt1_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_e_p3 PARTITION OF prt1_e FOR VALUES FROM (500) TO (600);
INSERT INTO prt1_e SELECT i, i, i % 25 FROM generate_series(0, 599, 2) i;
CREATE INDEX iprt1_e_p1_ab2 on prt1_e_p1(((a+b)/2));
CREATE INDEX iprt1_e_p2_ab2 on prt1_e_p2(((a+b)/2));
CREATE INDEX iprt1_e_p3_ab2 on prt1_e_p3(((a+b)/2));
ANALYZE prt1_e;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

--issue-5
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

--issue-6
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, t3.a + t3.b, t3.phv FROM ((SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT 50 phv, * FROM prt1_e WHERE prt1_e.c = 0) t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a = t1.phv OR t2.b = t2.phv OR (t3.a + t3.b)/2 = t3.phv ORDER BY t1.a, t2.b, t3.a + t3.b;

SET enable_hashjoin TO off;
SET enable_nestloop TO off;

--issue-7
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

--issue-8
-- Add an extra partition to prt2 , Partition-wise join is possible with partitions on inner side are allowed
CREATE TABLE prt2_p4 PARTITION OF prt2 FOR VALUES FROM (600) TO (700);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(600, 699, 3) i;
ANALYZE prt2;

select t1.a, t1.b, t1.c from prt2 t1 where not exists (select 1 from prt1 t2 WHERE t1.b = t2.a) and t1.a = 0 order by t1.a, t1.b, t1.c;

RESET enable_hashjoin;
RESET enable_nestloop;

DROP TABLE prt1;
DROP TABLE prt2;
DROP TABLE prt1_e;

--issue-9
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p4 PARTITION OF prt1 FOR VALUES FROM (600) TO (800);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 0, 2) i;
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(600, 799, 2) i;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (-250) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt2_p4 PARTITION OF prt2 FOR VALUES FROM (600) TO (800);
CREATE TABLE prt2_p5 PARTITION OF prt2 FOR VALUES FROM (800) TO (1000);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 0, 3) i;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(600, 999, 3) i;
ANALYZE prt2;

SELECT t1.a, t1.c, t2.a, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY 1,2,3;

--issue-10
SELECT t1.a, t1.c, t2.a, t2.c FROM prt1 t1 FULL JOIN prt2 t2 ON t1.a = t2.b WHERE coalesce(t1.b, 0) + coalesce(t2.a, 0) = 0 ORDER BY t1.a, t2.a;

--issue-11
select t1.a, t1.b, t1.c from prt2 t1 where not exists (select 1 from prt1 t2 WHERE t1.b = t2.a) and t1.a = 0 order by t1.a, t1.b, t1.c;

DROP TABLE prt1;
DROP TABLE prt2;

--issue-12
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 DEFAULT;
INSERT INTO prt1 SELECT i, i, to_char(i, 'FM0000') FROM
generate_series(0, 599, 2) i;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt2_p3 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE prt2;

EXPLAIN (COSTS OFF)
select t1.a,t2.b,t3.a,t4.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) inner join prt1 t3 on (t2.b = t3.a) inner join prt2 t4 on (t3.a = t4.b) ORDER BY 1,2,3;;
select t1.a,t2.b,t3.a,t4.b from prt1 t1 inner join prt2 t2 on (t1.a = t2.b) inner join prt1 t3 on (t2.b = t3.a) inner join prt2 t4 on (t3.a = t4.b) ORDER BY 1,2,3;;

DROP TABLE prt1;
DROP TABLE prt2;

--issue-13
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 DEFAULT;
INSERT INTO prt1 SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt2_p3 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 3) i;
ANALYZE prt2;

CREATE TABLE prt1_e (a int, b int, c varchar) PARTITION BY RANGE(((a + b)/2));
CREATE TABLE prt1_e_p1 PARTITION OF prt1_e FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_e_p2 PARTITION OF prt1_e FOR VALUES FROM (250) TO (500);
CREATE TABLE prt1_e_p3 PARTITION OF prt1_e DEFAULT;
INSERT INTO prt1_e SELECT i, i, to_char(i, 'FM0000') FROM generate_series(0, 599, 2) i;
ANALYZE prt1_e;

EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a % 25 = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

DROP TABLE prt1;
DROP TABLE prt2;
DROP TABLE prt1_e;

--issue-14
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (-250) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (200);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (300) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p4 PARTITION OF prt1 FOR VALUES FROM (600) TO (MAXVALUE);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 799) i WHERE i % 2 = 0 AND i NOT BETWEEN 200 AND 300;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt2_p4 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 799) i WHERE i % 3 = 0;
ANALYZE prt2;

--ACTIVE
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;
DROP TABLE prt1;
DROP TABLE prt2;

--issue-15
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (-250) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (200);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (300) TO (500);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p4 PARTITION OF prt1 FOR VALUES FROM (600) TO (MAXVALUE);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 799) i WHERE i % 2 = 0 AND (i < 200 OR i >= 300);
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (550);
CREATE TABLE prt2_p4 PARTITION OF prt2 FOR VALUES FROM (600) TO (MAXVALUE);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 799) i WHERE i % 3 = 0 AND (i < 550 OR i >=600);

--ACTIVE
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;
DROP TABLE prt1;
DROP TABLE prt2;

--issue-16

CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002','0003');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt1_p3 PARTITION OF plt1 FOR VALUES IN (NULL,'0008','0009');
CREATE TABLE plt1_p4 PARTITION OF plt1 FOR VALUES IN ('0000','0010');
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 17, 'FM0000') FROM generate_series(0, 500) i WHERE i % 17 NOT IN (7, 11, 12, 13, 14, 15,16);
INSERT INTO plt1 SELECT i, i % 47, case when i % 17 = 7 then NULL else to_char(i % 17, 'FM0000') end FROM generate_series(0, 500) i WHERE i % 17 IN (7,8,9);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0002','0003');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0004','0005','0006');
CREATE TABLE plt2_p3 PARTITION OF plt2 FOR VALUES IN ('0007','0008','0009');
CREATE TABLE plt2_p4 PARTITION OF plt2 FOR VALUES IN ('0000',NULL,'0012');
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 17, 'FM0000') FROM generate_series(0, 500) i WHERE i % 17 NOT IN (1, 10, 11, 13, 14, 15, 16);
INSERT INTO plt2 SELECT i, i % 47, case when i % 17 = 11 then NULL else to_char(i % 17, 'FM0000') end FROM generate_series(0, 500) i WHERE i % 17 IN (0,11,12);
ANALYZE plt2;

CREATE TABLE plt1_e (a int, b int, c text) PARTITION BY LIST(ltrim(c, 'A'));
CREATE TABLE plt1_e_p1 PARTITION OF plt1_e FOR VALUES IN ('0002', '0003');
CREATE TABLE plt1_e_p2 PARTITION OF plt1_e FOR VALUES IN ('0004', '0005', '0006');
CREATE TABLE plt1_e_p3 PARTITION OF plt1_e FOR VALUES IN ('0008', '0009');
CREATE TABLE plt1_e_p4 PARTITION OF plt1_e FOR VALUES IN ('0000');
INSERT INTO plt1_e SELECT i, i % 47, to_char(i % 17, 'FM0000') FROM generate_series(0, 500) i WHERE i % 17 NOT IN (1, 7, 10, 11, 12, 13, 14, 15, 16);
ANALYZE plt1_e;

--ACTIVE
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM
plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM
plt1 t1, plt2 t2, plt1_e t3 WHERE t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

drop table plt1;
drop table plt2;
drop table plt1_e;

--issue-17
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p0 PARTITION OF prt1 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(-250, 499) i WHERE i % 2 = 0;
ANALYZE prt1;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p0 PARTITION OF prt2 FOR VALUES FROM (MINVALUE) TO (0);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 DEFAULT;
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(-250, 599) i WHERE i % 3 = 0;
ANALYZE prt2;

--ACTIVE
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a)  ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON (t1.a = t2.b) INNER JOIN prt1 t3 ON (t2.b = t3.a)  ORDER BY t1.a, t2.b;

drop table prt1;
drop table prt2;

--issue-18
CREATE TABLE plt1 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt1_p1 PARTITION OF plt1 FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt1_p2 PARTITION OF plt1 FOR VALUES IN ('0005','0006','0007','0008');
INSERT INTO plt1 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,9,10);
ANALYSE plt1;

CREATE TABLE plt2 (a int, b int, c varchar) PARTITION BY LIST(c);
CREATE TABLE plt2_p1 PARTITION OF plt2 FOR VALUES IN ('0001','0002','0003','0004');
CREATE TABLE plt2_p2 PARTITION OF plt2 FOR VALUES IN ('0005','0006','0007','0008');
CREATE TABLE plt2_p3 PARTITION OF plt2 DEFAULT;
INSERT INTO plt2 SELECT i, i % 47, to_char(i % 11, 'FM0000') FROM generate_series(0, 500) i WHERE i % 11 NOT IN (0,10);
ANALYSE plt2;

--ACTIVE
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t1.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON (t1.c = t2.c AND t1.b = 0) LEFT JOIN plt1 t3 ON (t2.c = t3.c AND t2.b = 0) FULL JOIN plt2 t4 ON (t3.c = t4.c AND t3.b = 0) INNER JOIN plt1 t5 ON (t4.c = t5.c AND t4.b = 0) WHERE t5.b  = 0 ORDER BY t1.a, t2.b;
drop table plt1;
drop table plt2;
