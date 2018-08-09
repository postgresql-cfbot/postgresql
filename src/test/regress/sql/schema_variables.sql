CREATE VARIABLE var1 AS integer;
CREATE TEMP VARIABLE var2 AS text;

DROP VARIABLE var1, var2;

-- functional interface
CREATE VARIABLE var1 AS numeric;

CREATE ROLE var_test_role;

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
  SELECT public.var1::int;
$$ LANGUAGE sql SECURITY DEFINER;

SELECT secure_var();

SET ROLE TO var_test_role;

-- should to fail
SELECT public.var1;

-- should to work;
SELECT secure_var();

SET ROLE TO DEFAULT;

EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM generate_series(1,100) g(v) WHERE v = var1;

CREATE VIEW schema_var_view AS SELECT var1;

SELECT * FROM schema_var_view;

\c -

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
  LET public.var3 = COALESCE(public.var3 + $1, $1);
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
LET public.v2.a = pi();
SELECT v2;

-- shoudl fail due dependency
DROP TYPE t2;

-- should be ok
DROP VARIABLE v1;
DROP VARIABLE v2;
