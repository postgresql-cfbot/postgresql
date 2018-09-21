
BEGIN TRANSACTION;

--
-- Create some helper views and functions to allow us to more easily
-- assert which operations will cause an assertion to be checked.
--

CREATE OR REPLACE VIEW invalidating_operation
  (assertion_name,
   relation_name,
   operation)
AS
SELECT c.conname,
       cl.relname,
       v.operation
  FROM pg_trigger t
  JOIN pg_constraint c ON (t.tgconstraint = c.oid)
  JOIN pg_proc p ON (t.tgfoid = p.oid)
  JOIN pg_class cl ON (t.tgrelid = cl.oid)
  JOIN (VALUES ('INSERT', 1<<2),
               ('DELETE', 1<<3|1<<5),
               ('UPDATE', 1<<4))
    AS v(operation, mask) ON ((v.mask & tgtype) > 0)
 WHERE t.tgisinternal
   AND p.proname = 'assertion_check';

CREATE OR REPLACE VIEW invalidating_by_update_of_column
  (assertion_name,
   relation_name,
   column_name)
AS
SELECT c.conname,
       cl.relname,
       a.attname
  FROM pg_trigger t INNER JOIN pg_constraint c ON (t.tgconstraint = c.oid)
                    INNER JOIN pg_proc p ON (t.tgfoid = p.oid)
                    INNER JOIN pg_class cl ON (t.tgrelid = cl.oid)
                    CROSS JOIN LATERAL UNNEST(t.tgattr) AS co(n)
                    INNER JOIN pg_attribute a ON (a.attrelid = cl.oid AND attnum = co.n)
 WHERE t.tgisinternal
   AND p.proname = 'assertion_check'
   AND (tgtype & (1 << 4)) > 0;

CREATE OR REPLACE VIEW invalidating_summary
  (assertion_name,
   operations)
AS
SELECT assertion_name,
       string_agg(operation, ' ' ORDER BY operation)
  FROM (SELECT assertion_name,
               operation ||'('|| string_agg(relation_name, ', ' ORDER BY relation_name) ||')'
          FROM invalidating_operation
         WHERE operation <> 'UPDATE'
         GROUP BY assertion_name,
                  operation
         UNION
        SELECT assertion_name,
               operation ||'('|| string_agg(relation_name ||'.'|| column_name, ', ' ORDER BY relation_name, column_name) ||')'
          FROM invalidating_by_update_of_column JOIN invalidating_operation USING (assertion_name, relation_name)
         WHERE operation = 'UPDATE'
         GROUP BY assertion_name,
                  operation)
    AS v(assertion_name, operation)
 GROUP BY assertion_name;

CREATE OR REPLACE FUNCTION have_identical_invalidating_operations
  (a text, b text)
RETURNS TABLE (predicate text, correct text)
AS $$
  SELECT 'assertion "'|| a ||'" has identical invalidating operations to assertion "'|| b ||'"',
         CASE WHEN
           NOT EXISTS (SELECT relation_name, operation
                         FROM invalidating_operation
                        WHERE assertion_name = a
                       EXCEPT 
                       SELECT relation_name, operation
                         FROM invalidating_operation
                        WHERE assertion_name = b) AND
           NOT EXISTS (SELECT relation_name, column_name
                         FROM invalidating_by_update_of_column
                        WHERE assertion_name = a
                       EXCEPT
                       SELECT relation_name, column_name
                         FROM invalidating_by_update_of_column
                        WHERE assertion_name = b) THEN 'YES'
                                                  ELSE 'NO' END
$$ LANGUAGE SQL;

COMMIT;


BEGIN TRANSACTION;

--
-- Perform some basic sanity checking on creating assertions
--

CREATE TABLE test1 (a int, b text);

CREATE ASSERTION foo CHECK (1 < 2);
CREATE ASSERTION a2 CHECK ((SELECT count(*) FROM test1) < 5);

DELETE FROM test1;
INSERT INTO test1 VALUES (1, 'one');
INSERT INTO test1 VALUES (2, 'two');
INSERT INTO test1 VALUES (3, 'three');
INSERT INTO test1 VALUES (4, 'four');

SAVEPOINT pre_insert_too_many;
INSERT INTO test1 VALUES (5, 'five');
ROLLBACK TO SAVEPOINT pre_insert_too_many;

SELECT constraint_schema,
       constraint_name
  FROM information_schema.assertions
 ORDER BY 1, 2;

\dQ

ALTER ASSERTION a2 RENAME TO a3;

SAVEPOINT pre_rename_foo;
ALTER ASSERTION foo RENAME TO a3; -- fails
ROLLBACK TO SAVEPOINT pre_rename_foo;

DROP ASSERTION foo;

SAVEPOINT pre_drop_test1;
DROP TABLE test1; -- fails
ROLLBACK TO SAVEPOINT pre_drop_test1;

DROP TABLE test1 CASCADE;

ROLLBACK;



BEGIN TRANSACTION;

--
-- Expressions involving various operators, functions and casts
-- 

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);

INSERT INTO t (n, p) VALUES (20, TRUE);

CREATE ASSERTION is_distinct_from CHECK (10 IS DISTINCT FROM (SELECT MIN(n) FROM t));
CREATE ASSERTION is_not_distinct_from CHECK (20 IS NOT DISTINCT FROM (SELECT MIN(n) FROM t));
CREATE ASSERTION null_if CHECK (NULLIF((SELECT BOOL_AND(p) FROM t), FALSE));
CREATE ASSERTION coerce_to_boolean_from_min CHECK (CAST((SELECT MIN(n) FROM t) AS BOOLEAN));
CREATE ASSERTION coerce_to_boolean_from_bool_and CHECK (CAST((SELECT BOOL_AND(p) FROM t) AS BOOLEAN));
CREATE ASSERTION coerce_to_boolean_from_boolean CHECK (CAST((SELECT TRUE FROM t) AS BOOLEAN));

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving function calls
--

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL, d DATE NOT NULL);

CREATE FUNCTION f(n INTEGER) RETURNS INTEGER
AS $$
  SELECT n * 2
$$ LANGUAGE SQL;

CREATE FUNCTION r(n INTEGER) RETURNS TABLE (m INTEGER )
AS $$
  SELECT m
    FROM t
   WHERE n > r.n
$$ LANGUAGE SQL;

CREATE ASSERTION age_of_d_in_t CHECK (
  NOT EXISTS (
    SELECT FROM t
     WHERE AGE(DATE '01-01-2018') > INTERVAL '10 days' -- AGE(DATE) is built-in SQL
  )
);
CREATE ASSERTION use_f CHECK (f(0) = 0);
CREATE ASSERTION use_f_in_predicate CHECK (NOT EXISTS (SELECT FROM t WHERE f(n) = 20));
CREATE ASSERTION use_f_in_target CHECK (NOT EXISTS (SELECT f(n) FROM t));
CREATE ASSERTION use_r_in_from CHECK (NOT EXISTS (SELECT FROM r(100)));
CREATE ASSERTION use_r_in_target CHECK (NOT EXISTS (SELECT r(100)));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (COALESCE(operations, 'NONE') = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('age_of_d_in_t',      'INSERT(t)'),
       ('use_f',              'NONE'),
       ('use_f_in_predicate', 'INSERT(t) UPDATE(t.n)'),
       ('use_f_in_target',    'INSERT(t)'),
       ('use_r_in_from',      'INSERT(t) UPDATE(t.m, t.n)'), -- TODO should _from and _target be the same?
       ('use_r_in_target',    'INSERT(t) UPDATE(t.n)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

CREATE FUNCTION g(n INTEGER) RETURNS INTEGER
AS $$
BEGIN
  RETURN n * 2;
END;
$$ LANGUAGE PLPGSQL;

-- Use of functions other than internal or those implemented in SQL are illegal
CREATE ASSERTION use_g_in_from CHECK (NOT EXISTS (SELECT FROM g(100))); -- fails

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving EXISTS and NOT EXISTS
--

CREATE TABLE r (n INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);
CREATE TABLE t1 (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);
CREATE TABLE t2 (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);

INSERT INTO r (n) VALUES (1);
INSERT INTO t (n, m) VALUES (0, 0);
INSERT INTO t1 (n, m) VALUES (0, 0);
INSERT INTO t2 (n, m) VALUES (0, 0);

CREATE ASSERTION exists_no_predicate CHECK (
  EXISTS (SELECT FROM r)
);

CREATE ASSERTION exists_with_predicate CHECK (
  EXISTS (SELECT FROM r WHERE n > 0)
);

CREATE ASSERTION not_exists_no_predicate CHECK (
  NOT EXISTS (SELECT FROM s)
);

CREATE ASSERTION not_exists_with_predicate CHECK (
  NOT EXISTS (SELECT FROM r WHERE n < 1)
);

CREATE ASSERTION direct_subject_of_an_exists CHECK (
  EXISTS (SELECT n FROM t WHERE m = 0)
);

-- TODO These can be optimised, at least if the set operation is UNION.
CREATE ASSERTION except_subject_of_an_exists CHECK (
  EXISTS (SELECT n FROM t1 WHERE m = 0 EXCEPT SELECT n FROM t2 WHERE m = 1)
);

CREATE ASSERTION intersect_subject_of_an_exists CHECK (
  EXISTS (SELECT n FROM t1 WHERE m = 0 INTERSECT SELECT n FROM t2 WHERE m = 0)
);

CREATE ASSERTION union_subject_of_an_exists CHECK (
  EXISTS (SELECT n FROM t1 WHERE m = 0 UNION SELECT n FROM t2 WHERE m = 0)
);

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('exists_no_predicate',            'DELETE(r)'),
       ('exists_with_predicate',          'DELETE(r) UPDATE(r.n)'),
       ('not_exists_no_predicate',        'INSERT(s)'),
       ('not_exists_with_predicate',      'INSERT(r) UPDATE(r.n)'),
       ('direct_subject_of_an_exists',    'DELETE(t) UPDATE(t.m)'),
       ('except_subject_of_an_exists',    'DELETE(t1) INSERT(t2) UPDATE(t1.m, t1.n, t2.m, t2.n)'),
       ('intersect_subject_of_an_exists', 'DELETE(t1, t2) UPDATE(t1.m, t1.n, t2.m, t2.n)'),
       ('union_subject_of_an_exists',     'DELETE(t1, t2) UPDATE(t1.m, t1.n, t2.m, t2.n)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving NOT
--

CREATE TABLE r (p BOOLEAN NOT NULL PRIMARY KEY);

INSERT INTO r (p) VALUES (TRUE);

CREATE ASSERTION a CHECK (EXISTS (SELECT FROM r WHERE p));
CREATE ASSERTION b CHECK (NOT EXISTS (SELECT FROM r WHERE NOT p));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('a', 'DELETE(r) UPDATE(r.p)'),
       ('b', 'INSERT(r) UPDATE(r.p)')) -- NOT(x) and x have opposite invalidating operations
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving set operations INTERSECT, UNION, and EXCEPT
--

CREATE TABLE r (n INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY);
CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY);

INSERT INTO t (n) VALUES (0);

CREATE ASSERTION except_operands CHECK (NOT EXISTS (SELECT FROM r EXCEPT SELECT FROM s));
CREATE ASSERTION intersect_operands CHECK (NOT EXISTS (SELECT FROM r INTERSECT SELECT FROM s));
CREATE ASSERTION union_operands CHECK (EXISTS (SELECT FROM s UNION SELECT FROM t));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('except_operands',    'DELETE(s) INSERT(r)'),
       ('intersect_operands', 'INSERT(r, s)'),
       ('union_operands',     'DELETE(s, t)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving COUNT aggregations
--

CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);

INSERT INTO s VALUES (1, 2);

CREATE ASSERTION ge_all_count CHECK (1 >= ALL (SELECT COUNT(*) FROM s));
CREATE ASSERTION ge_any_count CHECK (1 >= ANY (SELECT COUNT(*) FROM s));
CREATE ASSERTION ge_count     CHECK (1 >=     (SELECT COUNT(*) FROM s));
CREATE ASSERTION gt_all_count CHECK (2 >  ALL (SELECT COUNT(*) FROM s));
CREATE ASSERTION gt_any_count CHECK (2 >  ANY (SELECT COUNT(*) FROM s));
CREATE ASSERTION gt_count     CHECK (2 >      (SELECT COUNT(*) FROM s));
CREATE ASSERTION le_all_count CHECK (1 <= ALL (SELECT COUNT(*) FROM s));
CREATE ASSERTION le_any_count CHECK (0 <= ANY (SELECT COUNT(*) FROM s));
CREATE ASSERTION le_count     CHECK (0 <=     (SELECT COUNT(*) FROM s));
CREATE ASSERTION lt_all_count CHECK (0 <  ALL (SELECT COUNT(*) FROM s));
CREATE ASSERTION lt_any_count CHECK (0 <  ANY (SELECT COUNT(*) FROM s));
CREATE ASSERTION lt_count     CHECK (0 <      (SELECT COUNT(*) FROM s));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('ge_all_count', 'INSERT(s)'),
       ('ge_any_count', 'INSERT(s)'),
       ('ge_count',     'INSERT(s)'),
       ('gt_all_count', 'INSERT(s)'),
       ('gt_any_count', 'INSERT(s)'),
       ('gt_count',     'INSERT(s)'),
       ('le_all_count', 'DELETE(s)'),
       ('le_any_count', 'DELETE(s)'),
       ('le_count',     'DELETE(s)'),
       ('lt_all_count', 'DELETE(s)'),
       ('lt_any_count', 'DELETE(s)'),
       ('lt_count',     'DELETE(s)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving MIN aggregations
--

CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);

INSERT INTO s VALUES (1, 2);

CREATE ASSERTION ge_any_min CHECK (1 >= ANY (SELECT MIN(n) FROM s));
CREATE ASSERTION ge_all_min CHECK (1 >= ALL (SELECT MIN(n) FROM s));
CREATE ASSERTION ge_min     CHECK (1 >=     (SELECT MIN(n) FROM s));
CREATE ASSERTION gt_any_min CHECK (2 >  ANY (SELECT MIN(n) FROM s));
CREATE ASSERTION gt_all_min CHECK (2 >  ALL (SELECT MIN(n) FROM s));
CREATE ASSERTION gt_min     CHECK (2 >      (SELECT MIN(n) FROM s));
CREATE ASSERTION le_any_min CHECK (0 <= ANY (SELECT MIN(n) FROM s));
CREATE ASSERTION le_all_min CHECK (0 <= ALL (SELECT MIN(n) FROM s));
CREATE ASSERTION le_min     CHECK (0 <=     (SELECT MIN(n) FROM s));
CREATE ASSERTION lt_any_min CHECK (0 <  ANY (SELECT MIN(n) FROM s));
CREATE ASSERTION lt_all_min CHECK (0 <  ALL (SELECT MIN(n) FROM s));
CREATE ASSERTION lt_min     CHECK (0 <      (SELECT MIN(n) FROM s));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('ge_all_min', 'DELETE(s) UPDATE(s.n)'),
       ('ge_any_min', 'DELETE(s) UPDATE(s.n)'),
       ('ge_min',     'DELETE(s) UPDATE(s.n)'),
       ('gt_all_min', 'DELETE(s) UPDATE(s.n)'),
       ('gt_any_min', 'DELETE(s) UPDATE(s.n)'),
       ('gt_min',     'DELETE(s) UPDATE(s.n)'),
       ('le_all_min', 'INSERT(s) UPDATE(s.n)'),
       ('le_any_min', 'INSERT(s) UPDATE(s.n)'),
       ('le_min',     'INSERT(s) UPDATE(s.n)'),
       ('lt_all_min', 'INSERT(s) UPDATE(s.n)'),
       ('lt_any_min', 'INSERT(s) UPDATE(s.n)'),
       ('lt_min',     'INSERT(s) UPDATE(s.n)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving MAX aggregations
--

CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);

INSERT INTO s VALUES (1, 2);

CREATE ASSERTION ge_any_max CHECK (1 >= ANY (SELECT MAX(n) FROM s));
CREATE ASSERTION ge_all_max CHECK (1 >= ALL (SELECT MAX(n) FROM s));
CREATE ASSERTION ge_max     CHECK (1 >=     (SELECT MAX(n) FROM s));
CREATE ASSERTION gt_all_max CHECK (2 >  ALL (SELECT MAX(n) FROM s));
CREATE ASSERTION gt_any_max CHECK (2 >  ANY (SELECT MAX(n) FROM s));
CREATE ASSERTION gt_max     CHECK (2 >      (SELECT MAX(n) FROM s));
CREATE ASSERTION le_all_max CHECK (0 <= ALL (SELECT MAX(n) FROM s));
CREATE ASSERTION le_any_max CHECK (0 <= ANY (SELECT MAX(n) FROM s));
CREATE ASSERTION le_max     CHECK (0 <=     (SELECT MAX(n) FROM s));
CREATE ASSERTION lt_all_max CHECK (0 <  ALL (SELECT MAX(n) FROM s));
CREATE ASSERTION lt_any_max CHECK (0 <  ANY (SELECT MAX(n) FROM s));
CREATE ASSERTION lt_max     CHECK (0 <      (SELECT MAX(n) FROM s));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('ge_all_max', 'INSERT(s) UPDATE(s.n)'),
       ('ge_any_max', 'INSERT(s) UPDATE(s.n)'),
       ('ge_max',     'INSERT(s) UPDATE(s.n)'),
       ('gt_all_max', 'INSERT(s) UPDATE(s.n)'),
       ('gt_any_max', 'INSERT(s) UPDATE(s.n)'),
       ('gt_max',     'INSERT(s) UPDATE(s.n)'),
       ('le_all_max', 'DELETE(s) UPDATE(s.n)'),
       ('le_any_max', 'DELETE(s) UPDATE(s.n)'),
       ('le_max',     'DELETE(s) UPDATE(s.n)'),
       ('lt_all_max', 'DELETE(s) UPDATE(s.n)'),
       ('lt_any_max', 'DELETE(s) UPDATE(s.n)'),
       ('lt_max',     'DELETE(s) UPDATE(s.n)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving BOOL_AND aggregations
--

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);
INSERT INTO t (n, p) VALUES (0, TRUE);

CREATE ASSERTION eq_bool_and     CHECK (TRUE  =      (SELECT BOOL_AND(p) FROM t));
CREATE ASSERTION eq_any_bool_and CHECK (TRUE  =  ANY (SELECT BOOL_AND(p) FROM t));
CREATE ASSERTION eq_all_bool_and CHECK (TRUE  =  ALL (SELECT BOOL_AND(p) FROM t));
CREATE ASSERTION ne_bool_and     CHECK (FALSE <>     (SELECT BOOL_AND(p) FROM t));
CREATE ASSERTION ne_any_bool_and CHECK (FALSE <> ANY (SELECT BOOL_AND(p) FROM t));
CREATE ASSERTION ne_all_bool_and CHECK (FALSE <> ALL (SELECT BOOL_AND(p) FROM t));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('eq_all_bool_and', 'INSERT(t) UPDATE(t.p)'),
       ('eq_any_bool_and', 'INSERT(t) UPDATE(t.p)'),
       ('eq_bool_and',     'INSERT(t) UPDATE(t.p)'),
       ('ne_all_bool_and', 'INSERT(t) UPDATE(t.p)'),
       ('ne_any_bool_and', 'INSERT(t) UPDATE(t.p)'),
       ('ne_bool_and',     'INSERT(t) UPDATE(t.p)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving BOOL_OR aggregations
--

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);
INSERT INTO t (n, p) VALUES (0, TRUE);

CREATE ASSERTION eq_bool_or     CHECK (TRUE  =      (SELECT BOOL_OR(p) FROM t));
CREATE ASSERTION eq_any_bool_or CHECK (TRUE  =  ANY (SELECT BOOL_OR(p) FROM t));
CREATE ASSERTION eq_all_bool_or CHECK (TRUE  =  ALL (SELECT BOOL_OR(p) FROM t));
CREATE ASSERTION ne_bool_or     CHECK (FALSE <>     (SELECT BOOL_OR(p) FROM t));
CREATE ASSERTION ne_any_bool_or CHECK (FALSE <> ANY (SELECT BOOL_OR(p) FROM t));
CREATE ASSERTION ne_all_bool_or CHECK (FALSE <> ALL (SELECT BOOL_OR(p) FROM t));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('eq_all_bool_or', 'DELETE(t) UPDATE(t.p)'),
       ('eq_any_bool_or', 'DELETE(t) UPDATE(t.p)'),
       ('eq_bool_or',     'DELETE(t) UPDATE(t.p)'),
       ('ne_all_bool_or', 'DELETE(t) UPDATE(t.p)'),
       ('ne_any_bool_or', 'DELETE(t) UPDATE(t.p)'),
       ('ne_bool_or',     'DELETE(t) UPDATE(t.p)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions involving window functions
--

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);

-- Regular window function
CREATE ASSERTION alternate_p CHECK (
  NOT EXISTS (
    SELECT FROM (
      SELECT LAG(p, 1, NOT p) OVER n <> p
         AND LEAD(p, 1, NOT p) OVER n <> p
        FROM t
      WINDOW n AS (ORDER BY n)
    ) AS v(q)
    WHERE NOT q
  )
);

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('alternate_p', 'DELETE(t) INSERT(t) UPDATE(t.n, t.p)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

INSERT INTO t (n, p) VALUES (10, TRUE);
INSERT INTO t (n, p) VALUES (20, FALSE);
INSERT INTO t (n, p) VALUES (30, TRUE);

SAVEPOINT pre_insert;
INSERT INTO t (n, p) VALUES (40, TRUE);
ROLLBACK TO SAVEPOINT pre_insert;

SAVEPOINT pre_delete;
DELETE FROM t WHERE n = 20;
ROLLBACK TO SAVEPOINT pre_delete;

SAVEPOINT pre_update;
UPDATE t SET p = NOT p WHERE n = 20;
ROLLBACK TO SAVEPOINT pre_update;

DROP ASSERTION alternate_p;

-- Aggregate function over a window
CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);

INSERT INTO s (n, m)
SELECT n, (2 * n)
  FROM GENERATE_SERIES(0, 9) AS ns(n);

CREATE ASSERTION max_over_window CHECK ((10, 20) > ALL (SELECT n, MAX(m) OVER (ORDER BY n) FROM s));

SELECT assertion_name,
       COALESCE(operations, 'NONE') AS actual,
       expected,
       CASE WHEN (operations = expected)
         THEN 'YES' ELSE 'NO'
       END AS correct
  FROM invalidating_summary FULL OUTER JOIN (
VALUES ('max_over_window', 'INSERT(s) UPDATE(s.m, s.n)'))
    AS v(assertion_name, expected)
 USING (assertion_name)
 ORDER BY assertion_name, operations;

ROLLBACK;


BEGIN TRANSACTION;

--
-- Expressions containing a comparison should have the same invalidating
-- operations as an expression containing the inverse comparison operation
-- and where the operands have been switched
--

CREATE TABLE r (n INTEGER NOT NULL PRIMARY KEY);

INSERT INTO r (n) VALUES (9);

-- ">" and "<" are opposites
CREATE ASSERTION a CHECK (10 > (SELECT MIN(n) FROM r));
CREATE ASSERTION b CHECK ((SELECT MIN(n) FROM r) < 10);

-- "<" and ">" are opposites
CREATE ASSERTION c CHECK (8 < (SELECT MIN(n) FROM r));
CREATE ASSERTION d CHECK ((SELECT MIN(n) FROM r) > 8);

-- ">=" and "<=" are opposites
CREATE ASSERTION e CHECK (9 >= (SELECT MIN(n) FROM r));
CREATE ASSERTION f CHECK ((SELECT MIN(n) FROM r) <= 9);

-- "<=" and ">=" are opposites
CREATE ASSERTION g CHECK (9 <= (SELECT MIN(n) FROM r));
CREATE ASSERTION h CHECK ((SELECT MIN(n) FROM r) >= 9);

SELECT (test).predicate, (test).correct
  FROM (
VALUES ('a', 'b'),
       ('c', 'd'),
       ('e', 'f'),
       ('g', 'h'))
    AS v(a, b)
 CROSS JOIN LATERAL (SELECT have_identical_invalidating_operations(v.a, v.b))
    AS w(test)
 ORDER BY 1, 2;

ROLLBACK;


BEGIN TRANSACTION;

--
-- The INFORMATION_SCHEMA should contain the correct information
--

CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);
CREATE ASSERTION a CHECK (NOT EXISTS (SELECT FROM t WHERE p));

SELECT predicate,
       CASE WHEN
         truth THEN 'YES'
               ELSE 'NO'
       END AS correct
  FROM ( 
VALUES ('INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE contains only the correct tables',
        EXISTS (
          SELECT FROM information_schema.constraint_table_usage
           WHERE (constraint_name, table_name) = ('a', 't'))
        AND NOT EXISTS (
          SELECT FROM information_schema.constraint_table_usage
           WHERE constraint_name = 'a'
             AND table_name <> 't')),
       ('INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE contains only the correct columns (t.p)',
        EXISTS (
          SELECT FROM information_schema.constraint_column_usage
           WHERE (constraint_name, table_name, column_name) = ('a', 't', 'p'))
        AND NOT EXISTS (
          SELECT FROM information_schema.constraint_column_usage
           WHERE constraint_name ='a'
             AND (table_name, column_name) <> ('t', 'p'))),
       ('INFORMATION_SCHEMA.ASSERTIONS contains only the correct assertions',
        EXISTS (
          SELECT FROM information_schema.assertions
           WHERE constraint_name = 'a')
        AND NOT EXISTS (
          SELECT FROM information_schema.assertions
           WHERE constraint_name <> 'a')))
    AS v(predicate, truth);

ROLLBACK;

BEGIN TRANSACTION;
-- TODO This needs rethinking
/*
CREATE TABLE s (n INTEGER NOT NULL PRIMARY KEY, m INTEGER NOT NULL);
CREATE TABLE t (n INTEGER NOT NULL PRIMARY KEY, p BOOLEAN NOT NULL);
CREATE VIEW v AS SELECT * FROM s INNER JOIN t USING (n);

CREATE ASSERTION a CHECK (NOT EXISTS (SELECT FROM v));

-- we should depend on the view not the tables
SELECT OK(depends_on('a', 'v'),
          'Assertion depends upon the view v it references');
SELECT OK(NOT depends_on('a', 's'),
          'Assertion does not depend upon the table s referenced in the view v');
SELECT OK(NOT depends_on('a', 't'),
          'Assertion does not depend upon the table t referenced in the view v');

-- we should trigger on the tables and not the view
SELECT OK(EXISTS(SELECT FROM assertion_check_operation
                  WHERE (assertion_name, relation_name) = ('a', 's')),
          'Assertion is checked on modifications to table s');
SELECT OK(EXISTS(SELECT FROM assertion_check_operation
                  WHERE (assertion_name, relation_name) = ('a', 't')),
          'Assertion is checked on modifications to table t');
SELECT OK(NOT EXISTS(SELECT FROM assertion_check_operation
                      WHERE (assertion_name, relation_name) = ('a', 'v')),
          'Assertion is not checked on modifications to view v');
*/
ROLLBACK;

-- TODO test commonalities between count, min, max, etc, for optimisations
-- TODO ensure window function aggregates are treated the same as regular aggregates
-- TODO ensure that conflicting aggregate functions are not incorrectly optimised
-- TODO ensure that recorded dependencies are correct (traversal into functions and views)

BEGIN TRANSACTION;

DROP FUNCTION have_identical_invalidating_operations;
DROP VIEW invalidating_summary;
DROP VIEW invalidating_by_update_of_column;
DROP VIEW invalidating_operation;

COMMIT;
