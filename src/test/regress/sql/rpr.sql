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

--
-- Error cases
--

-- row pattern definition variable name must not appear more than once
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 ORDER BY tdate
 INITIAL
 PATTERN (START UP+ DOWN+)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price),
  UP AS price > PREV(price)
);

-- pattern variable name must appear in DEFINE
SELECT company, tdate, price, first_value(price) OVER w, last_value(price) OVER w
 FROM stock
 WINDOW w AS (
 PARTITION BY company
 ORDER BY tdate
 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 INITIAL
 PATTERN (START UP+ DOWN+ END)
 DEFINE
  START AS TRUE,
  UP AS price > PREV(price),
  DOWN AS price < PREV(price)
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
