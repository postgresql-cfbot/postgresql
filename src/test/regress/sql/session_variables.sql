CREATE ROLE regress_variable_owner;

-- should be ok
CREATE VARIABLE var1 AS int;

-- should fail, pseudotypes are not allowed
CREATE VARIABLE var2 AS anyelement;

-- should be ok, do nothing
DROP VARIABLE IF EXISTS var2;

-- do nothing
CREATE VARIABLE IF NOT EXISTS var1 AS int;

-- should fail
CREATE VARIABLE var1 AS int;

-- should be ok
DROP VARIABLE IF EXISTS var1;

-- check comment on variable
CREATE VARIABLE var1 AS int;
COMMENT ON VARIABLE var1 IS 'some variable comment';
SELECT pg_catalog.obj_description(oid, 'pg_variable') FROM pg_variable WHERE varname = 'var1';

DROP VARIABLE var1;

--- check access rights and supported ALTER
CREATE SCHEMA svartest;
GRANT ALL ON SCHEMA svartest TO regress_variable_owner;

CREATE VARIABLE svartest.var1 AS int;

CREATE ROLE regress_variable_reader;

GRANT SELECT ON VARIABLE svartest.var1 TO regress_variable_reader;
REVOKE ALL ON VARIABLE svartest.var1 FROM regress_variable_reader;

ALTER VARIABLE svartest.var1 OWNER TO regress_variable_owner;
ALTER VARIABLE svartest.var1 RENAME TO varxx;
ALTER VARIABLE svartest.varxx SET SCHEMA public;

DROP VARIABLE public.varxx;

ALTER DEFAULT PRIVILEGES
   FOR ROLE regress_variable_owner
   IN SCHEMA svartest
   GRANT SELECT ON VARIABLES TO regress_variable_reader;

-- creating variable with default privileges
SET ROLE TO regress_variable_owner;
CREATE VARIABLE svartest.var1 AS int;
SET ROLE TO DEFAULT;

\dV+ svartest.var1

DROP VARIABLE svartest.var1;

DROP SCHEMA svartest;

DROP ROLE regress_variable_owner;

-- check access rights
CREATE ROLE regress_noowner;

CREATE VARIABLE var1 AS int;

CREATE OR REPLACE FUNCTION sqlfx(int)
RETURNS int AS $$ SELECT $1 + var1 $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION sqlfx_sd(int)
RETURNS int AS $$ SELECT $1 + var1 $$ LANGUAGE sql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION plpgsqlfx(int)
RETURNS int AS $$ BEGIN RETURN $1 + var1; END $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION plpgsqlfx_sd(int)
RETURNS int AS $$ BEGIN RETURN $1 + var1; END $$ LANGUAGE plpgsql SECURITY DEFINER;

LET var1 = 10;
-- should be ok
SELECT var1;
SELECT sqlfx(20);
SELECT sqlfx_sd(20);
SELECT plpgsqlfx(20);
SELECT plpgsqlfx_sd(20);

-- should to fail
SET ROLE TO regress_noowner;

SELECT var1;
SELECT sqlfx(20);
SELECT plpgsqlfx(20);

-- should be ok
SELECT sqlfx_sd(20);
SELECT plpgsqlfx_sd(20);

SET ROLE TO DEFAULT;
GRANT SELECT ON VARIABLE var1 TO regress_noowner;

-- should be ok
SET ROLE TO regress_noowner;

SELECT var1;
SELECT sqlfx(20);
SELECT plpgsqlfx(20);

SET ROLE TO DEFAULT;
DROP VARIABLE var1;
DROP FUNCTION sqlfx(int);
DROP FUNCTION plpgsqlfx(int);
DROP FUNCTION sqlfx_sd(int);
DROP FUNCTION plpgsqlfx_sd(int);

DROP ROLE regress_noowner;

-- use variables inside views
CREATE VARIABLE var1 AS numeric;

-- use variables in views
CREATE VIEW test_view AS SELECT COALESCE(var1 + v, 0) AS result FROM generate_series(1,2) g(v);
SELECT * FROM test_view;
LET var1 = 3.14;
SELECT * FROM test_view;

-- start a new session
\c

SELECT * FROM test_view;
LET var1 = 3.14;
SELECT * FROM test_view;

-- should fail, dependency
DROP VARIABLE var1;

-- should be ok
DROP VARIABLE var1 CASCADE;

CREATE VARIABLE var1 text;
CREATE VARIABLE var2 text;

-- use variables in SQL functions
CREATE OR REPLACE FUNCTION sqlfx1(varchar)
RETURNS varchar AS $$ SELECT var1 || ', ' || $1 $$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION sqlfx2( varchar)
RETURNS varchar AS $$ SELECT var2 || ', ' || $1 $$ LANGUAGE sql;

LET var1 = 'str1';
LET var2 = 'str2';

SELECT sqlfx1(sqlfx2('Hello'));

-- inlining is blocked
EXPLAIN (COSTS OFF, VERBOSE) SELECT sqlfx1(sqlfx2('Hello'));

DROP FUNCTION sqlfx1(varchar);
DROP FUNCTION sqlfx2(varchar);
DROP VARIABLE var1;
DROP VARIABLE var2;

-- access from cached plans should to work
CREATE VARIABLE var1 AS numeric;

CREATE OR REPLACE FUNCTION plpgsqlfx()
RETURNS numeric AS $$ BEGIN RETURN var1; END $$ LANGUAGE plpgsql;

set plan_cache_mode TO force_generic_plan;

LET var1 = 3.14;
SELECT plpgsqlfx();
LET var1 = 3.14 * 2;
SELECT plpgsqlfx();

DROP VARIABLE var1;

-- dependency (plan invalidation) should to work
CREATE VARIABLE var1 AS numeric;

LET var1 = 3.14 * 3;
SELECT plpgsqlfx();
LET var1 = 3.14 * 4;
SELECT plpgsqlfx();

DROP VARIABLE var1;
DROP FUNCTION plpgsqlfx();

set plan_cache_mode TO DEFAULT;

-- usage LET statement in plpgsql should to work
CREATE VARIABLE var1 int;
CREATE VARIABLE var2 numeric[];

DO $$
BEGIN
  LET var2 = '{}'::int[];
  FOR i IN 1..10
  LOOP
    LET var1 = i;
    LET var2[var1] = i;
  END LOOP;
  RAISE NOTICE 'result array: %', var2;
END;
$$;

DROP VARIABLE var1;
DROP VARIABLE var2;

-- CALL statement is supported
CREATE VARIABLE v int;

LET v = 1;

CREATE PROCEDURE p(arg int) AS $$ BEGIN RAISE NOTICE '%', arg; END $$ LANGUAGE plpgsql;

-- should to work
CALL p(v);

DO $$ BEGIN CALL p(v); END $$;

DROP PROCEDURE p(int);
DROP VARIABLE v;

-- test search path
CREATE SCHEMA svartest;
CREATE VARIABLE svartest.var1 AS numeric;

-- should to fail
LET var1 = pi();
SELECT var1;

-- should be ok
LET svartest.var1 = pi();
SELECT svartest.var1;

SET search_path TO svartest;

-- should be ok
LET var1 = pi() + 10;
SELECT var1;

RESET search_path;
DROP SCHEMA svartest CASCADE;

CREATE VARIABLE var1 AS text;

-- variables can be updated under RO transaction
BEGIN;
SET TRANSACTION READ ONLY;
LET var1 = 'hello';
COMMIT;

SELECT var1;

DROP VARIABLE var1;

-- test of domains
CREATE DOMAIN int_domain AS int NOT NULL CHECK (VALUE > 100);
CREATE VARIABLE var1 AS int_domain;

-- should fail
SELECT var1;

-- should be ok
LET var1 = 1000;
SELECT var1;

-- should fail
LET var1 = 10;

-- should fail
LET var1 = NULL;

-- note - domain defaults are not supported yet (like PLpgSQL)

DROP VARIABLE var1;
DROP DOMAIN int_domain;

CREATE SCHEMA svartest CREATE VARIABLE var1 AS int CREATE TABLE foo(a int);
LET svartest.var1 = 100;
SELECT svartest.var1;

SET search_path to public, svartest;

SELECT var1;

DROP SCHEMA svartest CASCADE;

CREATE VARIABLE var1 AS int;
CREATE VARIABLE var2 AS int[];

LET var1 = 2;
LET var2 = '{}'::int[];

LET var2[var1] = 0;

SELECT var2;

DROP VARIABLE var1, var2;

CREATE VARIABLE var1 AS int;
CREATE VARIABLE var2 AS int[];

LET var1 = 2;
LET var2 = '{}'::int[];

SELECT var2;

DROP VARIABLE var1, var2;

-- the LET statement should be disallowed in CTE
CREATE VARIABLE var1 AS int;
WITH x AS (LET var1 = 100) SELECT * FROM x;

-- should be ok
LET var1 = generate_series(1, 1);

-- should fail
LET var1 = generate_series(1, 2);
LET var1 = generate_series(1, 0);

DROP VARIABLE var1;

-- composite variables
CREATE TYPE sv_xyz AS (x int, y int, z numeric(10,2));

CREATE VARIABLE v1 AS sv_xyz;
CREATE VARIABLE v2 AS sv_xyz;

LET v1 = (1, 2, 3.14);
LET v2 = (10, 20, 3.14 * 10);

-- should work too - there are prepared casts
LET v1 = (1, 2, 3);

SELECT v1;
SELECT v2;
SELECT (v1).*;
SELECT (v2).*;

SELECT v1.x + v1.z;
SELECT v2.x + v2.z;

-- access to composite fields should be safe too
CREATE ROLE regress_var_test_role;

SET ROLE TO regress_var_test_role;

-- should fail
SELECT v2.x;

SET ROLE TO DEFAULT;

DROP VARIABLE v1;
DROP VARIABLE v2;

DROP ROLE regress_var_test_role;

CREATE TYPE t1 AS (a int, b numeric, c text);

CREATE VARIABLE v1 AS t1;
LET v1 = (1, pi(), 'hello');
SELECT v1;
LET v1.b = 10.2222;
SELECT v1;

-- should fail, attribute doesn't exist
LET v1.x = 10;

-- should fail, don't allow multi column query
LET v1 = (NULL::t1).*;

-- allow DROP or ADD ATTRIBUTE on composite types
-- should be ok
ALTER TYPE t1 DROP ATTRIBUTE c;
SELECT v1;

-- should be ok
ALTER TYPE t1 ADD ATTRIBUTE c int;
SELECT v1;

LET v1 = (10, 10.3, 20);
SELECT v1;

-- should be ok
ALTER TYPE t1 DROP ATTRIBUTE b;
SELECT v1;

-- should fail, disallow data type change
ALTER TYPE t1 ALTER ATTRIBUTE c TYPE int;

DROP VARIABLE v1;
DROP TYPE t1;

-- the table type can be used as composite type too
CREATE TABLE svar_test(a int, b numeric, c date);
CREATE VARIABLE var1 AS svar_test;

LET var1 = (10, pi(), '2023-05-26');
SELECT var1;

-- should fail due dependency
ALTER TABLE svar_test ALTER COLUMN a TYPE text;

-- should fail
DROP TABLE svar_test;

DROP VARIABLE var1;
DROP TABLE svar_test;

-- arrays are supported
CREATE VARIABLE var1 AS numeric[];
LET var1 = ARRAY[1.1,2.1];
LET var1[1] = 10.1;
SELECT var1;

-- LET target doesn't allow srf, should fail
LET var1[generate_series(1,3)] = 100;

DROP VARIABLE var1;

-- arrays inside composite
CREATE TYPE t1 AS (a numeric, b numeric[]);
CREATE VARIABLE var1 AS t1;
LET var1 = (10.1, ARRAY[0.0, 0.0]);
LET var1.a = 10.2;
SELECT var1;
LET var1.b[1] = 10.3;
SELECT var1;

DROP VARIABLE var1;
DROP TYPE t1;

-- Encourage use of parallel plans
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;

-- test on query with workers
CREATE TABLE svar_test(a int);
INSERT INTO svar_test SELECT * FROM generate_series(1,1000);
ANALYZE svar_test;
CREATE VARIABLE zero int;
LET zero = 0;

-- result should be 100
SELECT count(*) FROM svar_test WHERE a%10 = zero;

-- parallel execution is not supported yet
EXPLAIN (COSTS OFF) SELECT count(*) FROM svar_test WHERE a%10 = zero;

LET zero = (SELECT count(*) FROM svar_test);

-- result should be 1000
SELECT zero;

DROP VARIABLE zero;
DROP TABLE svar_test;

RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET max_parallel_workers_per_gather;

-- the result of view should be same in parallel mode too
CREATE VARIABLE var1 AS int;
LET var1 = 10;

CREATE VIEW var1view AS SELECT COALESCE(var1, 0) AS result;

SELECT * FROM var1view;

SET debug_parallel_query TO on;

SELECT * FROM var1view;

SET debug_parallel_query TO off;

DROP VIEW var1view;
DROP VARIABLE var1;

CREATE VARIABLE varid int;
CREATE TABLE svar_test(id int, v int);

LET varid = 1;
INSERT INTO svar_test VALUES(varid, 100);
SELECT * FROM svar_test;
UPDATE svar_test SET v = 200 WHERE id = varid;
SELECT * FROM svar_test;
DELETE FROM svar_test WHERE id = varid;
SELECT * FROM svar_test;

DROP TABLE svar_test;
DROP VARIABLE varid;


-- visibility check
-- variables should be shadowed always
CREATE VARIABLE var1 AS text;
SELECT var1.relname FROM pg_class var1 WHERE var1.relname = 'pg_class';

DROP VARIABLE var1;

CREATE TABLE xxtab(avar int);

INSERT INTO xxtab VALUES(333);

CREATE TYPE xxtype AS (avar int);

CREATE VARIABLE xxtab AS xxtype;

INSERT INTO xxtab VALUES(10);

-- it is ambiguous, but columns are preferred
SELECT xxtab.avar FROM xxtab;

-- should be ok
SELECT avar FROM xxtab;

CREATE VARIABLE public.avar AS int;

-- should be ok, see the table
SELECT avar FROM xxtab;

-- should be ok
SELECT public.avar FROM xxtab;

DROP VARIABLE xxtab;

SELECT xxtab.avar FROM xxtab;

DROP VARIABLE public.avar;

DROP TYPE xxtype;

DROP TABLE xxtab;

-- The variable can be shadowed by table or by alias
CREATE TYPE public.svar_type AS (a int, b int, c int);
CREATE VARIABLE public.svar AS public.svar_type;

CREATE TABLE public.svar(a int, b int);

INSERT INTO public.svar VALUES(10, 20);

LET public.svar = (100, 200, 300);

-- should be ok
-- show table
SELECT * FROM public.svar;
SELECT svar.a FROM public.svar;
SELECT svar.* FROM public.svar;

-- show variable
SELECT public.svar;
SELECT public.svar.c;
SELECT (public.svar).*;

-- the variable is shadowed, raise error
SELECT public.svar.c FROM public.svar;

-- can be fixed by alias
SELECT public.svar.c FROM public.svar x;

SELECT svar.a FROM public.svar;
SELECT svar.* FROM public.svar;

-- show variable
SELECT public.svar;
SELECT public.svar.c;
SELECT (public.svar).*;

-- the variable is shadowed, raise error
SELECT public.svar.c FROM public.svar;

-- can be fixed by alias
SELECT public.svar.c FROM public.svar x;

DROP VARIABLE public.svar;
DROP TABLE public.svar;
DROP TYPE public.svar_type;

CREATE TYPE ab AS (a integer, b integer);

CREATE VARIABLE v_ab AS ab;

CREATE TABLE v_ab (a integer, b integer);
INSERT INTO v_ab VALUES(10,20);

-- we should to see table
SELECT v_ab.a FROM v_ab;

CREATE SCHEMA v_ab;

CREATE VARIABLE v_ab.a AS integer;

-- we should to see table
SELECT v_ab.a FROM v_ab;

DROP VARIABLE v_ab;
DROP TABLE v_ab;
DROP TYPE ab;

CREATE TYPE t_am_type AS (b int);
CREATE SCHEMA xxx_am;

SET search_path TO public;

CREATE VARIABLE xxx_am AS t_am_type;
LET xxx_am = ROW(10);

-- should be ok
SELECT xxx_am;

CREATE VARIABLE xxx_am.b AS int;
LET :"DBNAME".xxx_am.b = 20;

-- should be still ok
SELECT xxx_am;

-- should fail, the reference should be ambiguous
SELECT xxx_am.b;

-- enhanced references should be ok
SELECT public.xxx_am.b;
SELECT :"DBNAME".xxx_am.b;

CREATE TABLE xxx_am(b  int);
INSERT INTO xxx_am VALUES(10);

-- we should to see table
SELECT xxx_am.b FROM xxx_am;
SELECT x.b FROM xxx_am x;

DROP TABLE xxx_am;
DROP VARIABLE public.xxx_am;
DROP VARIABLE xxx_am.b;
DROP SCHEMA xxx_am;

CREATE SCHEMA :"DBNAME";

CREATE VARIABLE :"DBNAME".:"DBNAME".:"DBNAME" AS t_am_type;
CREATE VARIABLE :"DBNAME".:"DBNAME".b AS int;

SET search_path TO :"DBNAME";

-- should be ambiguous
SELECT :"DBNAME".b;

-- should be ambiguous too
SELECT :"DBNAME".:"DBNAME".b;

CREATE TABLE :"DBNAME"(b int);

-- should be ok
SELECT :"DBNAME".b FROM :"DBNAME";

DROP TABLE :"DBNAME";

DROP VARIABLE :"DBNAME".:"DBNAME".b;
DROP VARIABLE :"DBNAME".:"DBNAME".:"DBNAME";
DROP SCHEMA :"DBNAME";

RESET search_path;

-- memory cleaning by DISCARD command
CREATE VARIABLE var1 AS varchar;
LET var1 = 'Hello';
SELECT var1;

DISCARD ALL;
SELECT var1;

LET var1 = 'AHOJ';
SELECT var1;

DISCARD VARIABLES;
SELECT var1;

DROP VARIABLE var1;

-- initial test of debug pg_session_variables function
-- should be zero now
DISCARD VARIABLES;

SELECT count(*) FROM pg_session_variables();

CREATE VARIABLE var1 AS varchar;

-- should be zero still
SELECT count(*) FROM pg_session_variables();

LET var1 = 'AHOJ';

SELECT name, typname, can_select, can_update FROM pg_session_variables();

DISCARD VARIABLES;

-- should be zero again
SELECT count(*) FROM pg_session_variables();

-- dropped variables should be removed from memory
-- at the end of transaction or before next usage
-- of any session variable in next transaction.

LET var1 = 'Ahoj';
SELECT name, typname, can_select, can_update FROM pg_session_variables();
DROP VARIABLE var1;

-- should be zero
SELECT count(*) FROM pg_session_variables();

-- the content of value should be preserved when variable is dropped
-- by aborted transaction
CREATE VARIABLE var1 AS varchar;
LET var1 = 'Ahoj';
BEGIN;
DROP VARIABLE var1;

-- should fail
SELECT var1;

ROLLBACK;

-- should be ok
SELECT var1;

-- another test
BEGIN;
DROP VARIABLE var1;
CREATE VARIABLE var1 AS int;
LET var1 = 100;
-- should be ok, result 100
SELECT var1;
ROLLBACK;
-- should be ok, result 'Ahoj'
SELECT var1;

DROP VARIABLE var1;

-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE VARIABLE var1 AS int;
  LET var1 = 100;
  SELECT var1;
  SELECT name, typname, can_select, can_update FROM pg_session_variables();
  DROP VARIABLE var1;
COMMIT;

-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE VARIABLE var1 AS int;
  LET var1 = 100;
  SELECT var1;
  SELECT name, typname, can_select, can_update FROM pg_session_variables();
  DROP VARIABLE var1;
COMMIT;

-- should be zero
SELECT count(*) FROM pg_session_variables();

CREATE VARIABLE var1 AS int;
CREATE VARIABLE var2 AS int;
LET var1 = 10;
LET var2 = 0;
BEGIN;
  SAVEPOINT s1;
  DROP VARIABLE var1;
  -- force cleaning by touching another session variable
  SELECT var2;
  ROLLBACK TO s1;
  SAVEPOINT s2;
  DROP VARIABLE var1;
  SELECT var2;
  ROLLBACK TO s2;
COMMIT;
-- should be ok
SELECT var1;

BEGIN;
  SAVEPOINT s1;
  DROP VARIABLE var1;
  -- force cleaning by touching another session variable
  SELECT var2;
  ROLLBACK TO s1;
  SAVEPOINT s2;
  DROP VARIABLE var1;
  SELECT var2;
ROLLBACK;

-- should be ok
SELECT var1;

BEGIN;
  SAVEPOINT s1;
  DROP VARIABLE var1;
  -- force cleaning by touching another session variable
  SELECT var2;

  SAVEPOINT s2;
  -- force cleaning by touching another session variable
  SELECT var2;
  ROLLBACK TO s1;
  -- force cleaning by touching another session variable
  SELECT var2;
COMMIT;
-- should be ok
SELECT var1;

-- repeated aborted transaction
BEGIN; DROP VARIABLE var1; ROLLBACK;
BEGIN; DROP VARIABLE var1; ROLLBACK;
BEGIN; DROP VARIABLE var1; ROLLBACK;

-- should be ok
SELECT var1;

DROP VARIABLE var1, var2;

CREATE VARIABLE var1 bigint;

CREATE TABLE var_tab_test_table(a int);

INSERT INTO var_tab_test_table SELECT * FROM generate_series(1,10);

VACUUM ANALYZE var_tab_test_table;

EXPLAIN (COSTS OFF) LET var1 = (SELECT count(*) FROM var_tab_test_table);

-- should be NULL
SELECT var1;

EXPLAIN (COSTS OFF, TIMING OFF, ANALYZE, SUMMARY OFF) LET var1 = (SELECT count(*) FROM var_tab_test_table);

-- should be 10
SELECT var1;

SET plan_cache_mode TO force_generic_plan;

PREPARE p1 AS LET var1 = (SELECT count(*) FROM var_tab_test_table);

LET var1 = NULL;

EXPLAIN (COSTS OFF) EXECUTE p1;

-- should be NULL
SELECT var1;

EXPLAIN (COSTS OFF, TIMING OFF, ANALYZE, SUMMARY OFF) EXECUTE p1;

-- should be 10
SELECT var1;

SET plan_cache_mode TO DEFAULT;

DEALLOCATE p1;

DROP VARIABLE var1;
DROP TABLE var_tab_test_table;

CREATE VARIABLE var1 numeric;

SET plan_cache_mode TO force_generic_plan;

PREPARE p1(numeric) AS LET var1 = $1;
PREPARE p2 AS SELECT var1;

EXECUTE p1(pi() + 100);
EXECUTE p2;

-- prepared plan cache invalidation test
DROP VARIABLE var1;
CREATE VARIABLE var1 numeric;

-- should be NULL
EXECUTE p2;

DEALLOCATE p1;
DEALLOCATE p2;

DROP VARIABLE var1;

SET plan_cache_mode TO force_generic_plan;

CREATE VARIABLE var1 numeric[];

PREPARE p1(int, numeric) AS LET var1[$1] = $2;

LET var1 = '{}'::numeric[];
EXECUTE p1(1, 10.2);
EXECUTE p1(2, 10.3);

SELECT var1;

DEALLOCATE p1;
DROP VARIABLE var1;

-- temporary variables
CREATE TEMP VARIABLE var1 AS int;
-- this view should be temporary
CREATE VIEW var_test_view AS SELECT var1;

DROP VARIABLE var1 CASCADE;

BEGIN;
  CREATE TEMP VARIABLE var1 AS int ON COMMIT DROP;
  LET var1 = 100;
  SELECT var1;
COMMIT;

-- should be zero
SELECT count(*) FROM pg_variable WHERE varname = 'var1';
-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE TEMP VARIABLE var1 AS int ON COMMIT DROP;
  LET var1 = 100;
  SELECT var1;
ROLLBACK;

-- should be zero
SELECT count(*) FROM pg_variable WHERE varname = 'var1';
-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE TEMP VARIABLE var1 AS int ON COMMIT DROP;
  LET var1 = 100;
  DROP VARIABLE var1;
COMMIT;

-- should be zero
SELECT count(*) FROM pg_variable WHERE varname = 'var1';
-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE TEMP VARIABLE var1 AS int ON COMMIT DROP;
  LET var1 = 100;
  DROP VARIABLE var1;
ROLLBACK;

-- should be zero
SELECT count(*) FROM pg_variable WHERE varname = 'var1';
-- should be zero
SELECT count(*) FROM pg_session_variables();

BEGIN;
  CREATE TEMP VARIABLE var1 AS int ON COMMIT DROP;
  LET var1 = 100;
  SAVEPOINT s1;
  DROP VARIABLE var1;
  ROLLBACK TO s1;
  SELECT var1;
COMMIT;

-- should be zero
SELECT count(*) FROM pg_variable WHERE varname = 'var1';
-- should be zero
SELECT count(*) FROM pg_session_variables();

CREATE VARIABLE var1 AS int ON TRANSACTION END RESET;

BEGIN;
  LET var1 = 100;
  SELECT var1;
COMMIT;

-- should be NULL;
SELECT var1 IS NULL;

BEGIN;
  LET var1 = 100;
  SELECT var1;
ROLLBACK;

-- should be NULL
SELECT var1 IS NULL;

DROP VARIABLE var1;

CREATE OR REPLACE FUNCTION vartest_fx()
RETURNS int AS $$
BEGIN
  RAISE NOTICE 'vartest_fx executed';
  RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE VARIABLE var1 AS int DEFAULT vartest_fx();

-- vartest_fx should be protected by dep, should fail
DROP FUNCTION vartest_fx();

-- should be ok
SELECT var1;

-- the defexpr should be evaluated only once
SELECT var1;

DISCARD VARIABLES;

-- in this case, the defexpr should not be evaluated
LET var1 = 100;
SELECT var1;

DISCARD VARIABLES;

CREATE OR REPLACE FUNCTION vartest_fx()
RETURNS int AS $$
BEGIN
  RAISE EXCEPTION 'vartest_fx is executing';
  RETURN 0;
END;
$$ LANGUAGE plpgsql;

-- should to fail, but not to crash
SELECT var1;

-- again
SELECT var1;

-- but we can write
LET var1 = 100;
SELECT var1;

DROP VARIABLE var1;
DROP FUNCTION vartest_fx();

-- test NOT NULL
-- should be ok
CREATE VARIABLE var1 AS int NOT NULL;

-- should be ok
LET var1 = 10;
SELECT var1;

DISCARD VARIABLES;

-- should fail
SELECT var1;

-- should be ok
LET var1 = 10;
SELECT var1;

DROP VARIABLE var1;

-- should be ok
CREATE VARIABLE var1 AS int NOT NULL DEFAULT 0;

--should be ok
SELECT var1;

-- should be ok
LET var1 = 10;
SELECT var1;

DISCARD VARIABLES;

-- should to fail
LET var1 = NULL;

DROP VARIABLE var1;

-- test NOT NULL
CREATE OR REPLACE FUNCTION vartest_fx()
RETURNS int AS $$
BEGIN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE VARIABLE var1 AS int NOT NULL DEFAULT vartest_fx();

-- should to fail
SELECT var1;

DISCARD VARIABLES;

-- should be ok
LET var1 = 10;
SELECT var1;

CREATE OR REPLACE FUNCTION vartest_fx()
RETURNS int AS $$
BEGIN
  RETURN 0;
END;
$$ LANGUAGE plpgsql;

DISCARD VARIABLES;

-- should be ok
SELECT var1;

DROP VARIABLE var1;
DROP FUNCTION vartest_fx();

-- test IMMUTBLE
CREATE IMMUTABLE VARIABLE var1 AS int;

-- should be ok
SELECT var1;
-- first write should ok
-- should be ok
LET var1 = 10;
-- should fail
LET var1 = 20;

DISCARD VARIABLES;

-- should be ok
LET var1 = 10;
-- should fail
LET var1 = 20;

DISCARD VARIABLES;

-- should be ok
SELECT var1;
-- should be ok
LET var1 = NULL;
-- should fail
LET var1 = 20;

DROP VARIABLE var1;

CREATE IMMUTABLE VARIABLE var1 AS int DEFAULT 10;

-- don't allow change when variable has DEFAULT value
-- should to fail
LET var1 = 20;

DISCARD VARIABLES;

-- should be ok
SELECT var1;
-- should fail
LET var1 = 20;

DROP VARIABLE var1;

-- should be ok
CREATE IMMUTABLE VARIABLE var1 AS INT NOT NULL DEFAULT 10;

-- should to fail
LET var1 = 10;
LET var1 = 20;

-- should be ok
SELECT var1;

-- should to fail
LET var1 = 30;

DROP VARIABLE var1;

-- test session_variables_ambiguity_warning
CREATE SCHEMA xxtab;

CREATE VARIABLE xxtab.avar int;

CREATE TABLE public.xxtab(avar int);

INSERT INTO public.xxtab VALUES(1);

LET xxtab.avar = 20;

SET session_variables_ambiguity_warning TO on;
--- should to raise warning, show 1
SELECT xxtab.avar FROM public.xxtab;

SET session_variables_ambiguity_warning TO off;

DROP TABLE public.xxtab;
DROP SCHEMA xxtab CASCADE;
