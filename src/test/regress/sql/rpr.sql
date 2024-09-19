--
-- Test for row pattern definition clause
--

CREATE TEMP TABLE stock (
       company TEXT,
       tdate DATE,
       price INTEGER
);
INSERT INTO stock VALUES ('company1', '2023-07-01', 100);
INSERT INTO stock VALUES ('company1', '2023-07-02', 200);
INSERT INTO stock VALUES ('company1', '2023-07-03', 150);
INSERT INTO stock VALUES ('company1', '2023-07-04', 140);
INSERT INTO stock VALUES ('company1', '2023-07-05', 150);
INSERT INTO stock VALUES ('company1', '2023-07-06', 90);
INSERT INTO stock VALUES ('company1', '2023-07-07', 110);
INSERT INTO stock VALUES ('company1', '2023-07-08', 130);
INSERT INTO stock VALUES ('company1', '2023-07-09', 120);
INSERT INTO stock VALUES ('company1', '2023-07-10', 130);
INSERT INTO stock VALUES ('company2', '2023-07-01', 50);
INSERT INTO stock VALUES ('company2', '2023-07-02', 2000);
INSERT INTO stock VALUES ('company2', '2023-07-03', 1500);
INSERT INTO stock VALUES ('company2', '2023-07-04', 1400);
INSERT INTO stock VALUES ('company2', '2023-07-05', 1500);
INSERT INTO stock VALUES ('company2', '2023-07-06', 60);
INSERT INTO stock VALUES ('company2', '2023-07-07', 1100);
INSERT INTO stock VALUES ('company2', '2023-07-08', 1300);
INSERT INTO stock VALUES ('company2', '2023-07-09', 1200);
INSERT INTO stock VALUES ('company2', '2023-07-10', 1300);

SELECT * FROM stock;

-- basic test using PREV
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w,
 nth_value(tdate, 2) OVER w AS nth_second
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- basic test using PREV. UP appears twice
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w,
 nth_value(tdate, 2) OVER w AS nth_second
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+ UP+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- basic test using PREV. Use '*'
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w,
 nth_value(tdate, 2) OVER w AS nth_second
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP* DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- basic test with none greedy pattern
SELECT company, tdate, price, count(*) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (A A A)
 DEFINE
  A AS price >= 140 AND price <= 150
);

-- last_value() should remain consistent
SELECT company, tdate, price, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- omit "START" in DEFINE but it is ok because "START AS TRUE" is
-- implicitly defined. per spec.
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w,
 nth_value(tdate, 2) OVER w AS nth_second
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- the first row start with less than or equal to 100
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (LOWPRICE UP+ DOWN+)
 DEFINE
  LOWPRICE AS price <= 100,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- second row raises 120%
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (LOWPRICE UP+ DOWN+)
 DEFINE
  LOWPRICE AS price <= 100,
  UP AS price > PREV(price) * 1.2,
  DOWN AS price < PREV(price)
);

-- using NEXT
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UPDOWN)
 DEFINE
  START AS TRUE,
  UPDOWN AS price > PREV(price) AND price > NEXT(price)
);

-- using AFTER MATCH SKIP TO NEXT ROW
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 AFTER MATCH SKIP TO NEXT ROW
 INITIAL
 PATTERN (START UPDOWN)
 DEFINE
  START AS TRUE,
  UPDOWN AS price > PREV(price) AND price > NEXT(price)
);

-- match everything

SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 AFTER MATCH SKIP PAST LAST ROW
 INITIAL
 PATTERN (A+)
 DEFINE
  A AS TRUE
);

-- backtracking with reclassification of rows
-- using AFTER MATCH SKIP PAST LAST ROW
SELECT company, tdate, price, first_value(tdate) OVER w, last_value(tdate) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 AFTER MATCH SKIP PAST LAST ROW
 INITIAL
 PATTERN (A+ B+)
 DEFINE
  A AS price > 100,
  B AS price > 100
);

-- backtracking with reclassification of rows
-- using AFTER MATCH SKIP TO NEXT ROW
SELECT company, tdate, price, first_value(tdate) OVER w, last_value(tdate) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 AFTER MATCH SKIP TO NEXT ROW
 INITIAL
 PATTERN (A+ B+)
 DEFINE
  A AS price > 100,
  B AS price > 100
);

-- ROWS BETWEEN CURRENT ROW AND offset FOLLOWING
SELECT company, tdate, price, first_value(tdate) OVER w, last_value(tdate) OVER w,
 count(*) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
 AFTER MATCH SKIP PAST LAST ROW
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

--
-- Aggregates
--

-- using AFTER MATCH SKIP PAST LAST ROW
SELECT company, tdate, price,
 first_value(price) OVER w,
 last_value(price) OVER w,
 max(price) OVER w,
 min(price) OVER w,
 sum(price) OVER w,
 avg(price) OVER w,
 count(price) OVER w
FROM stock
WINDOW w AS (
PARTITION BY company
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
AFTER MATCH SKIP PAST LAST ROW
INITIAL
PATTERN (START UP+ DOWN+)
DEFINE
START AS TRUE,
UP AS price > PREV(price),
DOWN AS price < PREV(price)
);

-- using AFTER MATCH SKIP TO NEXT ROW
SELECT company, tdate, price,
 first_value(price) OVER w,
 last_value(price) OVER w,
 max(price) OVER w,
 min(price) OVER w,
 sum(price) OVER w,
 avg(price) OVER w,
 count(price) OVER w
FROM stock
WINDOW w AS (
PARTITION BY company
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
AFTER MATCH SKIP TO NEXT ROW
INITIAL
PATTERN (START UP+ DOWN+)
DEFINE
START AS TRUE,
UP AS price > PREV(price),
DOWN AS price < PREV(price)
);

-- JOIN case
CREATE TEMP TABLE t1 (i int, v1 int);
CREATE TEMP TABLE t2 (j int, v2 int);
INSERT INTO t1 VALUES(1,10);
INSERT INTO t1 VALUES(1,11);
INSERT INTO t1 VALUES(1,12);
INSERT INTO t2 VALUES(2,10);
INSERT INTO t2 VALUES(2,11);
INSERT INTO t2 VALUES(2,12);

SELECT * FROM t1, t2 WHERE t1.v1 <= 11 AND t2.v2 <= 11;

SELECT *, count(*) OVER w FROM t1, t2
WINDOW w AS (
 PARTITION BY t1.i
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (A)
 DEFINE
 A AS v1 <= 11 AND v2 <= 11
);

-- WITH case
WITH wstock AS (
  SELECT * FROM stock WHERE tdate < '2023-07-08'
)
SELECT tdate, price,
first_value(tdate) OVER w,
count(*) OVER w
 FROM wstock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

--
-- Error cases
--

-- row pattern definition variable name must not appear more than once
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price),
  UP AS price > PREV(price)
);

-- subqueries in DEFINE clause are not supported
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START LOWPRICE)
 DEFINE
  START AS TRUE,
  LOWPRICE AS price < (SELECT 100)
);

-- aggregates in DEFINE clause are not supported
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START LOWPRICE)
 DEFINE
  START AS TRUE,
  LOWPRICE AS price < count(*)
);

-- FRAME must start at current row when row patttern recognition is used
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);

-- SEEK is not supported
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 AFTER MATCH SKIP TO NEXT ROW
 SEEK
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
);
