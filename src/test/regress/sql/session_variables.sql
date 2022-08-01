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

-- should fail
SELECT var1;

SET ROLE TO DEFAULT;

GRANT READ ON VARIABLE var1 TO var_test_role;

SET ROLE TO var_test_role;
-- should fail
LET var1 = 10;
-- should work
SELECT var1;

SET ROLE TO DEFAULT;

GRANT WRITE ON VARIABLE var1 TO var_test_role;

SET ROLE TO var_test_role;

-- should work
LET var1 = 333;

SET ROLE TO DEFAULT;

REVOKE ALL ON VARIABLE var1 FROM var_test_role;

CREATE OR REPLACE FUNCTION secure_var()
RETURNS int AS $$
  SELECT svartest.var1::int;
$$ LANGUAGE sql SECURITY DEFINER;

SELECT secure_var();

SET ROLE TO var_test_role;

-- should fail
SELECT svartest.var1;

-- should work;
SELECT secure_var();

SET ROLE TO DEFAULT;

EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM generate_series(1,100) g(v) WHERE v = var1;

CREATE VIEW schema_var_view AS SELECT var1;

SELECT * FROM schema_var_view;

\c -

SET search_path = svartest;

-- should work still, but var will be empty
SELECT * FROM schema_var_view;

LET var1 = pi();

SELECT var1;

-- we can see execution plan of LET statement
EXPLAIN (VERBOSE, COSTS OFF) LET var1 = pi();

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

-- should fail
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

-- should work too - there are prepared casts
LET v1 = (1,2,3.14);

SELECT v1;
SELECT v2;
SELECT (v1).*;
SELECT (v2).*;

SELECT v1.x + v1.z;
SELECT v2.x + v2.z;

-- access to composite fields should be safe too
-- should fail
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

-- should fail
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

-- should fail
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

-- should fail due dependency
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

-- default rights test
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON VARIABLES TO var_test_role;

CREATE VARIABLE public.var2 AS int;

SET ROLE TO var_test_role;

-- should be ok
LET public.var2 = 100;
SELECT public.var2;

SET ROLE TO DEFAULt;

DROP VARIABLE public.var2;
DROP OWNED BY var_test_role;

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

LET zero = (SELECT count(*) FROM svar_test);

-- result should be 1000000
SELECT zero;

-- parallel workers should be used
EXPLAIN (costs off) LET zero = (SELECT count(*) FROM svar_test);

DROP TABLE svar_test;
DROP VARIABLE zero;

-- use variables in prepared statements
CREATE VARIABLE v AS numeric;
LET v = 3.14;

-- use variables in views
CREATE VIEW vv AS SELECT COALESCE(v, 0) + 1000 AS result;
SELECT * FROM vv;

-- start a new session
\c

SET search_path to svartest;

SELECT * FROM vv;
LET v = 3.14;
SELECT * FROM vv;

-- should fail, dependency
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

-- should fail
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

CREATE VARIABLE tv AS int;
CREATE VARIABLE IF NOT EXISTS tv AS int;
DROP VARIABLE tv;

CREATE IMMUTABLE VARIABLE iv AS int DEFAULT 100;
SELECT iv;

-- should fail;
LET iv = 10000;

DROP VARIABLE iv;

-- different order
CREATE IMMUTABLE VARIABLE iv AS int DEFAULT 100;
-- should to fail
LET iv = 10000;
-- should be ok
SELECT iv;

DROP VARIABLE iv;

CREATE IMMUTABLE VARIABLE iv AS int;

-- should be ok
LET iv = NULL;

-- should fail
LET iv = NULL;

DROP VARIABLE iv;

-- create variable inside plpgsql block
DO $$
BEGIN
  CREATE VARIABLE do_test_svar AS date DEFAULT '2000-01-01';
END;
$$;

SELECT do_test_svar;

DROP VARIABLE do_test_svar;

-- should fail
CREATE IMMUTABLE VARIABLE xx AS int NOT NULL;



-- REASSIGN OWNED test
CREATE ROLE var_test_role1;
CREATE ROLE var_test_role2;

CREATE VARIABLE xxx_var AS int;

ALTER VARIABLE xxx_var OWNER TO var_test_role1;
REASSIGN OWNED BY var_test_role1 to var_test_role2;

SELECT varowner::regrole FROM pg_variable WHERE varname = 'xxx_var';

DROP OWNED BY var_test_role1;
DROP ROLE var_test_role1;
SELECT count(*) FROM pg_variable WHERE varname = 'xxx_var';

DROP OWNED BY var_test_role2;
DROP ROLE var_test_role2;
SELECT count(*) FROM pg_variable WHERE varname = 'xxx_var';

-- creating, dropping temporary variable
BEGIN;

CREATE TEMP VARIABLE tempvar AS INT ON COMMIT DROP;

LET tempvar = 100;

SAVEPOINT s1;

DROP VARIABLE tempvar;

ROLLBACK TO s1;

SELECT tempvar;

COMMIT;

-- should to fail
LET tempvar = 100;

BEGIN;

SAVEPOINT s1;

CREATE TEMP VARIABLE tempvar AS INT ON COMMIT DROP;

LET tempvar = 100;

ROLLBACK TO s1;

COMMIT;

-- should to fail
LET tempvar = 100;

CREATE VARIABLE var1 AS int;
LET var1 = 100;
BEGIN;
DROP VARIABLE var1;
ROLLBACK;
SELECT var1;

DROP VARIABLE var1;

CREATE VARIABLE var1 AS int DEFAULT 100;
COMMENT ON VARIABLE var1 IS 'some variable comment';

SELECT pg_catalog.obj_description(oid, 'pg_variable') FROM pg_variable WHERE varname = 'var1';

DROP VARIABLE var1;

CREATE TABLE xxtab(avar int);

CREATE TYPE xxtype AS (avar int);

CREATE VARIABLE xxtab AS xxtype;

INSERT INTO xxtab VALUES(10);

-- it is ambiguous, but columns are preferred
SELECT xxtab.avar FROM xxtab;

SET session_variables_ambiguity_warning TO on;

SELECT xxtab.avar FROM xxtab;

SET search_path = svartest;

CREATE VARIABLE testvar as int;

-- plpgsql variables are preferred against session variables
DO $$
<<myblock>>
DECLARE testvar int;
BEGIN
  -- should be ok without warning
  LET testvar = 100;
  -- should be ok without warning
  testvar := 1000;
  -- should be ok without warning
  RAISE NOTICE 'session variable is %', svartest.testvar;
  -- should be ok without warning
  RAISE NOTICE 'plpgsql variable is %', myblock.testvar;
  -- should to print plpgsql variable with warning
  RAISE NOTICE 'variable is %', testvar;
END;
$$;

DROP VARIABLE testvar;

SET session_variables_ambiguity_warning TO default;

-- should be ok
SELECT avar FROM xxtab;

CREATE VARIABLE public.avar AS int;

-- should to fail
SELECT avar FROM xxtab;

-- should be ok
SELECT public.avar FROM xxtab;

DROP VARIABLE xxtab;

SELECT xxtab.avar FROM xxtab;

DROP VARIABLE public.avar;

DROP TYPE xxtype;

DROP TABLE xxtab;

-- test of plan cache invalidation
CREATE VARIABLE xx AS int;

SET plan_cache_mode = force_generic_plan;

PREPARE pp AS SELECT xx;

EXECUTE pp;

DROP VARIABLE xx;

CREATE VARIABLE xx AS int;

-- should to work
EXECUTE pp;

DROP VARIABLE xx;

DEALLOCATE pp;

SET plan_cache_mode = DEFAULT;

CREATE ROLE var_test_role;

CREATE SCHEMA vartest;

GRANT USAGE ON SCHEMA vartest TO var_test_role;

CREATE VARIABLE vartest.x AS int;
CREATE VARIABLE vartest.y AS int;

LET vartest.x = 100;
LET vartest.y = 101;

GRANT READ ON ALL VARIABLES IN SCHEMA vartest TO var_test_role;

SET ROLE TO var_test_role;

SELECT vartest.x, vartest.y;

SET ROLE TO DEFAULT;

REVOKE READ ON ALL VARIABLES IN SCHEMA vartest FROM var_test_role;

SET ROLE TO var_test_role;

-- should to fail
SELECT vartest.x;
SELECT vartest.y;

SET ROLE TO DEFAULT;

DROP VARIABLE vartest.x, vartest.y;

DROP SCHEMA vartest;

DROP ROLE var_test_role;

-- test cached plan
CREATE VARIABLE v1 AS text;
CREATE VARIABLE v2 AS int;
CREATE VARIABLE v3 AS int;

LET v1 = 'test';
LET v2 = 10;
LET v3 = 5;

PREPARE q1 AS SELECT v1 || i FROM generate_series(1, v2) g(i) WHERE i IN (v2, v3);

SET plan_cache_mode to force_generic_plan;

EXECUTE q1;

EXPLAIN EXECUTE q1;

-- dependecy check
DROP VARIABLE v3;

-- recreate v3 again
CREATE VARIABLE v3 AS int DEFAULT 6;

-- should to work, the plan should be recreated
EXECUTE q1;

DEALLOCATE q1;

-- fill v1 by long text
LET v1 = repeat(' ', 10000);

PREPARE q1 AS SELECT length(v1);

EXECUTE q1;

LET v1 = repeat(' ', 5000);

EXECUTE q1;

DEALLOCATE q1;

SET plan_cache_mode to default;

DROP VARIABLE v1, v2, v3;

CREATE ROLE var_test_role;

CREATE VARIABLE public.v1 AS int DEFAULT 0;

-- check acl when variable is acessed by simple eval expr method
CREATE OR REPLACE FUNCTION public.fx_var(int)
RETURNS int AS $$
DECLARE xx int;
BEGIN
  xx := public.v1 + $1;
  RETURN xx;
END;
$$ LANGUAGE plpgsql;

-- should be ok
SELECT public.fx_var(0);

SET ROLE TO var_test_role;

-- should to fail
SELECT public.fx_var(0);

SET ROLE TO default;

GRANT READ ON VARIABLE public.v1 TO var_test_role;

SET ROLE TO var_test_role;

-- should be ok
SELECT public.fx_var(0);

SET ROLE TO default;

REVOKE READ ON VARIABLE public.v1 FROM var_test_role;

SET ROLE TO var_test_role;

-- should be fail
SELECT public.fx_var(0);

SET ROLE TO DEFAULT;

DROP FUNCTION public.fx_var(int);

DROP VARIABLE public.v1;

DROP ROLE var_test_role;

CREATE TYPE public.svar_test_type AS (a int, b int, c numeric);

CREATE VARIABLE public.svar AS public.svar_test_type;

LET public.svar = ROW(10,20,30);

SELECT public.svar;

ALTER TYPE public.svar_test_type DROP ATTRIBUTE c;

-- should to fail
SELECT public.svar;

ALTER TYPE public.svar_test_type ADD ATTRIBUTE c int;

-- should to fail too (different type, different generation number);
SELECT public.svar;

LET public.svar = ROW(10,20,30);

-- should be ok again for new value
SELECT public.svar;

DROP VARIABLE public.svar;

DROP TYPE public.svar_test_type;

CREATE VARIABLE public.svar AS int;

SELECT schema, name, removed FROM pg_debug_show_used_session_variables();

LET public.svar = 100;

SELECT schema, name, removed FROM pg_debug_show_used_session_variables();

BEGIN;

DROP VARIABLE public.svar;

-- value should be in memory
SELECT schema, name, removed FROM pg_debug_show_used_session_variables();

ROLLBACK;

-- value should be in memory
SELECT schema, name, removed FROM pg_debug_show_used_session_variables() WHERE schema = 'public' and name = 'svar';

SELECT public.svar;

BEGIN;

DROP VARIABLE public.svar;

-- value should be in memory
SELECT schema, name, removed FROM pg_debug_show_used_session_variables() WHERE schema = 'public' and name = 'svar';

COMMIT;

-- the memory should be clean;
SELECT schema, name, removed FROM pg_debug_show_used_session_variables() WHERE schema = 'public' and name = 'svar';

BEGIN;

CREATE VARIABLE public.svar AS int;

LET public.svar = 100;

ROLLBACK;

-- the memory should be clean;
SELECT schema, name, removed FROM pg_debug_show_used_session_variables() WHERE schema = 'public' and name = 'svar';

CREATE VARIABLE public.svar AS int;

LET public.svar = 100;

-- repeated aborted transaction
BEGIN; DROP VARIABLE public.svar; ROLLBACK;
BEGIN; DROP VARIABLE public.svar; ROLLBACK;
BEGIN; DROP VARIABLE public.svar; ROLLBACK;

-- the value should be still available
SELECT public.svar;
