CREATE SCHEMA svartest;

SET search_path = svartest;

CREATE VARIABLE var1 AS integer;
CREATE TEMP VARIABLE var2 AS text;

DROP VARIABLE var1, var2;

-- functional interface
CREATE VARIABLE var1 AS numeric;

CREATE ROLE var_test_role;
GRANT USAGE ON SCHEMA svartest TO var_test_role;

SET ROLE TO var_test_role;

-- should to fail
SELECT var1;

SET ROLE TO DEFAULT;

GRANT READ ON VARIABLE var1 TO var_test_role;

SET ROLE TO var_test_role;
-- should to fail
LET var1 = 10;
-- should to work
SELECT var1;

SET ROLE TO DEFAULT;

GRANT WRITE ON VARIABLE var1 TO var_test_role;

SET ROLE TO var_test_role;

-- should to work
LET var1 = 333;

SET ROLE TO DEFAULT;

REVOKE ALL ON VARIABLE var1 FROM var_test_role;

CREATE OR REPLACE FUNCTION secure_var()
RETURNS int AS $$
  SELECT svartest.var1::int;
$$ LANGUAGE sql SECURITY DEFINER;

SELECT secure_var();

SET ROLE TO var_test_role;

-- should to fail
SELECT svartest.var1;

-- should to work;
SELECT secure_var();

SET ROLE TO DEFAULT;

EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM generate_series(1,100) g(v) WHERE v = var1;

CREATE VIEW schema_var_view AS SELECT var1;

SELECT * FROM schema_var_view;

\c -

SET search_path = svartest;

-- should to work still, but var will be empty
SELECT * FROM schema_var_view;

LET var1 = pi();

SELECT var1;

-- we can look on execution plan
EXPLAIN (VERBOSE, COSTS OFF) LET var1 = pi();

-- LET can be prepared
PREPARE var_pp(int, numeric) AS LET var1 = $1 + $2;

EXECUTE var_pp(100, 1.23456);

SELECT var1;

CREATE VARIABLE var3 AS int;

CREATE OR REPLACE FUNCTION inc(int)
RETURNS int AS $$
BEGIN
  LET svartest.var3 = COALESCE(svartest.var3 + $1, $1);
  RETURN var3;
END;
$$ LANGUAGE plpgsql;

SELECT inc(1);
SELECT inc(1);
SELECT inc(1);

SELECT inc(1) FROM generate_series(1,10);

SET ROLE TO var_test_role;

-- should to fail
LET var3 = 0;

SET ROLE TO DEFAULT;

DROP VIEW schema_var_view;

DROP VARIABLE var1 CASCADE;
DROP VARIABLE var3 CASCADE;

-- composite variables

CREATE TYPE sv_xyz AS (x int, y int, z numeric(10,2));

CREATE VARIABLE v1 AS sv_xyz;
CREATE VARIABLE v2 AS sv_xyz;

\d v1
\d v2

LET v1 = (1,2,3.14);
LET v2 = (10,20,3.14*10);

-- should to work too - there are prepared casts
LET v1 = (1,2,3.14);

SELECT v1;
SELECT v2;
SELECT (v1).*;
SELECT (v2).*;

SELECT v1.x + v1.z;
SELECT v2.x + v2.z;

-- access to composite fields should be safe too
-- should to fail
SET ROLE TO var_test_role;

SELECT v2.x;

SET ROLE TO DEFAULT;

DROP VARIABLE v1;
DROP VARIABLE v2;

REVOKE USAGE ON SCHEMA svartest FROM var_test_role;
DROP ROLE var_test_role;

-- scalar variables should not be in conflict with qualified column
CREATE VARIABLE varx AS text;
SELECT varx.relname FROM pg_class varx WHERE varx.relname = 'pg_class';

-- should to fail
SELECT varx.xxx;

-- variables can be updated under RO transaction

BEGIN;
SET TRANSACTION READ ONLY;
LET varx = 'hello';
COMMIT;

SELECT varx;

DROP VARIABLE varx;

CREATE TYPE t1 AS (a int, b numeric, c text);

CREATE VARIABLE v1 AS t1;
LET v1 = (1, pi(), 'hello');
SELECT v1;
LET v1.b = 10.2222;
SELECT v1;

-- should to fail
LET v1.x = 10;

DROP VARIABLE v1;
DROP TYPE t1;

-- arrays are supported
CREATE VARIABLE va1 AS numeric[];
LET va1 = ARRAY[1.1,2.1];
LET va1[1] = 10.1;
SELECT va1;

CREATE TYPE ta2 AS (a numeric, b numeric[]);
CREATE VARIABLE va2 AS ta2;
LET va2 = (10.1, ARRAY[0.0, 0.0]);
LET va2.a = 10.2;
SELECT va2;
LET va2.b[1] = 10.3;
SELECT va2;

DROP VARIABLE va1;
DROP VARIABLE va2;
DROP TYPE ta2;

-- default values
CREATE VARIABLE v1 AS numeric DEFAULT pi();
LET v1 = v1 * 2;
SELECT v1;

CREATE TYPE t2 AS (a numeric, b text);
CREATE VARIABLE v2 AS t2 DEFAULT (NULL, 'Hello');
LET svartest.v2.a = pi();
SELECT v2;

-- shoudl fail due dependency
DROP TYPE t2;

-- should be ok
DROP VARIABLE v1;
DROP VARIABLE v2;

-- tests of alters
CREATE SCHEMA var_schema1;
CREATE SCHEMA var_schema2;

CREATE VARIABLE var_schema1.var1 AS integer;
LET var_schema1.var1 = 1000;
SELECT var_schema1.var1;
ALTER VARIABLE var_schema1.var1 SET SCHEMA var_schema2;
SELECT var_schema2.var1;

CREATE ROLE var_test_role;

ALTER VARIABLE var_schema2.var1 OWNER TO var_test_role;
SET ROLE TO var_test_role;

-- should fail, no access to schema var_schema2.var
SELECT var_schema2.var1;
DROP VARIABLE var_schema2.var1;

SET ROLE TO DEFAULT;

ALTER VARIABLE var_schema2.var1 SET SCHEMA public;

SET ROLE TO var_test_role;
SELECT public.var1;

ALTER VARIABLE public.var1 RENAME TO var1_renamed;

SELECT public.var1_renamed;

DROP VARIABLE public.var1_renamed;

SET ROLE TO DEFAULt;

DROP ROLE var_test_role;

CREATE VARIABLE xx AS text DEFAULT 'hello';

SELECT xx, upper(xx);

LET xx = 'Hi';

SELECT xx;

DROP VARIABLE xx;

-- ON TRANSACTION END RESET tests
CREATE VARIABLE t1 AS int DEFAULT -1 ON TRANSACTION END RESET;

BEGIN;
  SELECT t1;
  LET t1 = 100;
  SELECT t1;
COMMIT;

SELECT t1;

BEGIN;
  SELECT t1;
  LET t1 = 100;
  SELECT t1;
ROLLBACK;

SELECT t1;

DROP VARIABLE t1;

CREATE VARIABLE v1 AS int DEFAULT 0;
CREATE VARIABLE v2 AS text DEFAULT 'none';

LET v1 = 100;
LET v2 = 'Hello';
SELECT v1, v2;
LET v1 = DEFAULT;
LET v2 = DEFAULT;
SELECT v1, v2;

DROP VARIABLE v1;
DROP VARIABLE v2;

-- ON COMMIT DROP tests
-- should be 0 always
SELECT count(*) FROM pg_variable;

CREATE TEMP VARIABLE g AS int ON COMMIT DROP;

SELECT count(*) FROM pg_variable;

BEGIN;
  CREATE TEMP VARIABLE g AS int ON COMMIT DROP;
COMMIT;

SELECT count(*) FROM pg_variable;

BEGIN;
  CREATE TEMP VARIABLE g AS int ON COMMIT DROP;
ROLLBACK;

SELECT count(*) FROM pg_variable;

-- test on query with workers
CREATE TABLE svar_test(a int);
INSERT INTO svar_test SELECT * FROM generate_series(1,1000000);
ANALYZE svar_test;
CREATE VARIABLE zero int;
LET zero = 0;

-- parallel workers should be used
EXPLAIN (costs off) SELECT count(*) FROM svar_test WHERE a%10 = zero;

-- result should be 100000
SELECT count(*) FROM svar_test WHERE a%10 = zero;

DROP TABLE svar_test;
DROP VARIABLE zero;

-- using variables in views, prepared statements
CREATE VARIABLE v AS numeric;
LET v = 3.14;

PREPARE pv1(int) AS LET v = v + $1;
PREPARE pv2 AS SELECT v;

EXECUTE pv1(1000);
EXECUTE pv2;

CREATE VIEW vv AS SELECT COALESCE(v, 0) + 1000 AS result;
SELECT * FROM vv;

-- start new session
\c

SET search_path to svartest;

SELECT * FROM vv;
LET v = 3.14;
SELECT * FROM vv;

-- should to fail, dependency
DROP VARIABLE v;

-- should be ok
DROP VARIABLE v CASCADE;

-- other features
CREATE VARIABLE dt AS integer DEFAULT 0;

LET dt = 100;
SELECT dt;

DISCARD VARIABLES;

SELECT dt;

DROP VARIABLE dt;

-- NOT NULL
CREATE VARIABLE v1 AS int NOT NULL;
CREATE VARIABLE v2 AS int NOT NULL DEFAULT NULL;

-- should to fail
SELECT v1;
SELECT v2;
LET v1 = NULL;
LET v2 = NULL;
LET v1 = DEFAULT;
LET v2 = DEFAULT;

-- should be ok
LET v1 = 100;
LET v2 = 1000;
SELECT v1, v2;

DROP VARIABLE v1;
DROP VARIABLE v2;

-- test transactional variables
CREATE TRANSACTION VARIABLE tv AS int DEFAULT 0;

BEGIN;
  LET tv = 100;
  SELECT tv;
ROLLBACK;

SELECT tv;

LET tv = 100;
BEGIN;
  LET tv = 1000;
COMMIT;

SELECT tv;

BEGIN;
  LET tv = DEFAULT;
  SELECT tv;
ROLLBACK;

SELECT tv;

-- test subtransactions

BEGIN;
  LET tv = 1;
SAVEPOINT x1;
  LET tv = 2;
SAVEPOINT x2;
  LET tv = 3;
ROLLBACK TO x2;
  SELECT tv;
  LET tv = 10;
ROLLBACK TO x1;
  SELECT tv;
ROLLBACK;

SELECT tv;

BEGIN;
  LET tv = 1;
SAVEPOINT x1;
  LET tv = 2;
SAVEPOINT x2;
  LET tv = 3;
ROLLBACK TO x2;
  SELECT tv;
  LET tv = 10;
ROLLBACK TO x1;
  SELECT tv;
COMMIT;

SELECT tv;


