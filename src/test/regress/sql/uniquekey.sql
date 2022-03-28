CREATE TABLE uqk1(a int, pk int primary key, c int,  d int);
CREATE TABLE uqk2(a int, pk int primary key, c int,  d int);
INSERT INTO uqk1 VALUES(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, null, 4), (5, 5, null, 4);
INSERT INTO uqk2 VALUES(1, 1, 1, 1), (4, 4, 4, 4), (5, 5, 5, 5);
ANALYZE uqk1;
ANALYZE uqk2;

-- Test single table primary key.
EXPLAIN (COSTS OFF) SELECT DISTINCT * FROM uqk1;

-- Test EC case.
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE d = pk;

-- Test UniqueKey indexes.
CREATE UNIQUE INDEX uqk1_ukcd ON uqk1(c, d);

-- Test not null quals and not null per catalog.
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE c = 1;
ALTER TABLE uqk1 ALTER COLUMN d SET NOT NULL;
EXPLAIN (COSTS OFF) SELECT DISTINCT c, d FROM uqk1 WHERE c is NOT NULL;

-- Test UniqueKey column reduction.
EXPLAIN (COSTS OFF) SELECT DISTINCT d FROM uqk1 WHERE c = 1;
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM uqk1 WHERE c = 1 and d = 1;

-- Test Distinct ON
EXPLAIN (COSTS OFF) SELECT DISTINCT ON(pk) d FROM uqk1;

------------------------------------------------------
-- Test UniqueKey on one side still valid after join.
-----------------------------------------------------
-- uqk1(c, d) is the uniquekey with mutli nulls at single relation access.
-- so distinct is not no-op.
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1, uqk2
WHERE uqk1.a = uqk2.pk;

-- Both uqk1 (c,d) are a valid uniquekey. 
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1, uqk2
WHERE uqk1.c is NOT NULL AND uqk1.a = uqk2.pk;

-- uqk1.c is null at baserel, but the null values are removed after join.
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1, uqk2
WHERE  uqk1.a = uqk2.pk and uqk1.c = uqk2.c;

-- uqk1.c is null at baserel, but the null values are removed after join
-- but new null values are generated due to outer join again. so distinct
-- is still needed.
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1 right join uqk2
on uqk1.a = uqk2.pk and uqk1.c = uqk2.c;


------------------------------------------------------
-- Test join: Composited UniqueKey
-----------------------------------------------------
-- both t1.pk and t1.pk is valid uniquekey.
EXPLAIN SELECT DISTINCT t1.pk, t2.pk FROM uqk1 t1 cross join uqk2 t2;
SELECT DISTINCT t1.pk, t2.pk FROM uqk1 t1 cross join uqk2 t2 order by 1, 2;

-- NOT OK, since t1.c includes multi nulls. 
EXPLAIN SELECT DISTINCT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is null;
SELECT DISTINCT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is null order by 1, 2,3;
SELECT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is null order by 1, 2,3;

-- let's remove the t1.c's multi null values
EXPLAIN SELECT DISTINCT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null;
SELECT DISTINCT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null order by 1, 2, 3 ;
SELECT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null order by 1, 2, 3;

-- test onerow case with composited cases.

-- t2.c is onerow. OK
EXPLAIN SELECT DISTINCT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null and t2.pk = 1;
SELECT DISTINCT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null and t2.pk = 1;
SELECT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null and t2.pk = 1;

-- t2.c is onerow, but t1.c has multi-nulls, NOt OK.
EXPLAIN SELECT DISTINCT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is null and t2.pk = 1;
SELECT DISTINCT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is null and t2.pk = 1;
SELECT t1.c, t1.d, t2.c FROM uqk1 t1 cross join uqk2 t2 where t1.c is null and t2.pk = 1;


-- Test Semi/Anti JOIN
EXPLAIN (COSTS OFF) SELECT DISTINCT pk FROM uqk1 WHERE d in (SELECT d FROM uqk2);
EXPLAIN (COSTS OFF) SELECT DISTINCT pk FROM uqk1 WHERE d NOT in (SELECT d FROM uqk2);

-----------------------------------
-- Test Join: Special OneRow case.
-----------------------------------
-- Test Unique Key FOR one-row case, DISTINCT is NOT needed as well.
-- uqk1.d is the a uniquekey due to onerow rule. uqk2.pk is pk.
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 1 AND uqk1.c = uqk2.pk;
SELECT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 1 AND uqk1.c = uqk2.pk order BY 1;
-- Both uqk1.d AND uqk2.c are the a uniquekey due to onerow rule
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 1
AND uqk2.pk = 1 AND uqk1.d = uqk2.d;
SELECT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 1 AND uqk2.pk = 1
AND uqk1.d = uqk2.d order BY 1;
-- Both UniqueKey in targetList, so distinct is not needed.
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.c, uqk2.c FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk2.pk = 1;
SELECT uqk1.c, uqk2.c FROM uqk1, uqk2 WHERE uqk1.pk = 2 AND uqk2.pk = 1 order BY 1, 2;

-----------------------------------------
-- Test more non-unique cases after join.
-----------------------------------------
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.pk FROM uqk1, uqk2 WHERE uqk1.c = uqk2.c;
EXPLAIN (COSTS OFF) SELECT DISTINCT uqk1.d FROM uqk1, uqk2 WHERE uqk1.pk = 1 AND uqk1.c = uqk2.c;

-----------------------------------------
-- Test DISTINCT/GROUP BY CASE.
-----------------------------------------


--------------------------------------------------------------------------------------------
-- Test subquery cases.
-- Note that current the UniqueKey still not push down the interesting UniqueKey to subquery.
-- like uniquekey, so the below test case need a "DISTINCT" in subquery to make sure the
-- UniqueKey is maintain.
--------------------------------------------------------------------------------------------
-- Test a normal case - one side
EXPLAIN SELECT DISTINCT v.* FROM
(SELECT DISTINCT uqk1.c, uqk1.d FROM uqk1, uqk2
WHERE uqk1.a = uqk2.pk AND uqk1.c is not null offset 0) v;

-- Test a normal case - composited side.
EXPLAIN SELECT DISTINCT v.* FROM
(SELECT DISTINCT t1.c, t1.d, t2.pk FROM uqk1 t1 cross join uqk2 t2 where t1.c is not null OFFSET 0)
v;
