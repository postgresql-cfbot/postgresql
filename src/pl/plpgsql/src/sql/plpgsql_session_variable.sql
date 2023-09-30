-- test of session variables
CREATE VARIABLE plpgsql_sv_var AS numeric;

LET plpgsql_sv_var = pi();

-- passing parameters to DO block
DO $$
BEGIN
  RAISE NOTICE 'value of session variable is %', plpgsql_sv_var;
END;
$$;

-- passing output from DO block;
DO $$
BEGIN
  LET plpgsql_sv_var = 2 * pi();
END
$$;

SELECT plpgsql_sv_var AS "pi_multiply_2";

DROP VARIABLE plpgsql_sv_var;

-- test access from PL/pgSQL
CREATE VARIABLE plpgsql_sv_var1 AS int;
CREATE VARIABLE plpgsql_sv_var2 AS numeric;
CREATE VARIABLE plpgsql_sv_var3 AS varchar;

CREATE OR REPLACE FUNCTION writer_func()
RETURNS void AS $$
BEGIN
  LET plpgsql_sv_var1 = 10;
  LET plpgsql_sv_var2 = pi();
  -- very long value
  LET plpgsql_sv_var3 = format('(%s)', repeat('*', 10000));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION updater_func()
RETURNS void AS $$
BEGIN
  LET plpgsql_sv_var1 = plpgsql_sv_var1 + 100;
  LET plpgsql_sv_var2 = plpgsql_sv_var2 + 100000000000;
  -- very long value
  LET plpgsql_sv_var3 = plpgsql_sv_var3 || format('(%s)', repeat('*', 10000));
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION reader_func()
RETURNS void AS $$
BEGIN
  RAISE NOTICE 'var1 = %', plpgsql_sv_var1;
  RAISE NOTICE 'var2 = %', plpgsql_sv_var2;
  RAISE NOTICE 'length of var3 = %', length(plpgsql_sv_var3);
END;
$$ LANGUAGE plpgsql;

-- execute under transaction
BEGIN;
SELECT writer_func();
SELECT reader_func();
SELECT updater_func();
SELECT reader_func();
END;

-- execute out of transaction
SELECT writer_func();
SELECT reader_func();
SELECT updater_func();
SELECT reader_func();

-- execute inside PL/pgSQL block
DO $$
BEGIN
  PERFORM writer_func();
  PERFORM reader_func();
  PERFORM updater_func();
  PERFORM reader_func();
END;
$$;

-- plan caches should be correctly invalidated
DROP VARIABLE plpgsql_sv_var1, plpgsql_sv_var2, plpgsql_sv_var3;

CREATE VARIABLE plpgsql_sv_var1 AS int;
CREATE VARIABLE plpgsql_sv_var2 AS numeric;
CREATE VARIABLE plpgsql_sv_var3 AS varchar;

-- should to work again
DO $$
BEGIN
  PERFORM writer_func();
  PERFORM reader_func();
  PERFORM updater_func();
  PERFORM reader_func();
END;
$$;

DROP VARIABLE plpgsql_sv_var1, plpgsql_sv_var2, plpgsql_sv_var3;

DROP FUNCTION writer_func;
DROP FUNCTION reader_func;
DROP FUNCTION updater_func;

-- another check of correct plan cache invalidation
CREATE VARIABLE plpgsql_sv_var1 AS int;
CREATE VARIABLE plpgsql_sv_var2 AS int[];

CREATE OR REPLACE FUNCTION test_func()
RETURNS void AS $$
DECLARE v int[] DEFAULT '{}';
BEGIN
  LET plpgsql_sv_var1 = 1;
  v[plpgsql_sv_var1] = 100;
  RAISE NOTICE '%', v;
  LET plpgsql_sv_var2 = v;
  LET plpgsql_sv_var2[plpgsql_sv_var1] = -1;
  RAISE NOTICE '%', plpgsql_sv_var2;
END;
$$ LANGUAGE plpgsql;

SELECT test_func();

DROP VARIABLE plpgsql_sv_var1, plpgsql_sv_var2;

CREATE VARIABLE plpgsql_sv_var1 AS int;
CREATE VARIABLE plpgsql_sv_var2 AS int[];

SELECT test_func();

DROP FUNCTION test_func();

DROP VARIABLE plpgsql_sv_var1, plpgsql_sv_var2;

-- check secure access
CREATE ROLE regress_var_owner_role;
CREATE ROLE regress_var_reader_role;
CREATE ROLE regress_var_exec_role;

GRANT ALL ON SCHEMA public TO regress_var_owner_role, regress_var_reader_role, regress_var_exec_role;

SET ROLE TO regress_var_owner_role;

CREATE VARIABLE plpgsql_sv_var1 AS int;
LET plpgsql_sv_var1 = 10;

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_reader_role;

CREATE OR REPLACE FUNCTION var_read_func()
RETURNS void AS $$
BEGIN
  RAISE NOTICE '%', plpgsql_sv_var1;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_exec_role;

-- should to fail
SELECT var_read_func();

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_owner_role;
GRANT SELECT ON VARIABLE plpgsql_sv_var1 TO regress_var_reader_role;

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_exec_role;

-- should be ok
SELECT var_read_func();

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_owner_role;

DROP VARIABLE plpgsql_sv_var1;

SET ROLE TO DEFAULT;

SET ROLE TO regress_var_exec_role;

-- should to fail, but not crash
SELECT var_read_func();

SET ROLE TO DEFAULT;

DROP FUNCTION var_read_func;

REVOKE ALL ON SCHEMA public FROM regress_var_owner_role, regress_var_reader_role, regress_var_exec_role;

DROP ROLE regress_var_owner_role;
DROP ROLE regress_var_reader_role;
DROP ROLE regress_var_exec_role;

-- returns updated value
CREATE VARIABLE plpgsql_sv_var1 AS int;

CREATE OR REPLACE FUNCTION inc_var_int(int)
RETURNS int AS $$
BEGIN
  LET plpgsql_sv_var1 = COALESCE(plpgsql_sv_var1 + $1, $1);
  RETURN plpgsql_sv_var1;
END;
$$ LANGUAGE plpgsql;

SELECT inc_var_int(1);
SELECT inc_var_int(1);
SELECT inc_var_int(1);

SELECT inc_var_int(1) FROM generate_series(1,10);

CREATE VARIABLE plpgsql_sv_var2 AS numeric;

LET plpgsql_sv_var2 = 0.0;

CREATE OR REPLACE FUNCTION inc_var_num(numeric)
RETURNS int AS $$
BEGIN
  LET plpgsql_sv_var2 = COALESCE(plpgsql_sv_var2 + $1, $1);
  RETURN plpgsql_sv_var2;
END;
$$ LANGUAGE plpgsql;

SELECT inc_var_num(1.0);
SELECT inc_var_num(1.0);
SELECT inc_var_num(1.0);

SELECT inc_var_num(1.0) FROM generate_series(1,10);

DROP VARIABLE plpgsql_sv_var1, plpgsql_sv_var2;

DROP FUNCTION inc_var_int;
DROP FUNCTION inc_var_num;

-- plpgsql variables are preferred against session variables
CREATE VARIABLE plpgsql_sv_var1 AS int;

DO $$
<<myblock>>
DECLARE plpgsql_sv_var1 int;
BEGIN
  LET plpgsql_sv_var1 = 100;

  plpgsql_sv_var1 := 1000;

  -- print 100;
  RAISE NOTICE 'session variable is %', public.plpgsql_sv_var1;

  -- print 1000
  RAISE NOTICE 'plpgsql variable is %', myblock.plpgsql_sv_var1;

  -- print 1000
  RAISE NOTICE 'variable is %', plpgsql_sv_var1;
END;
$$;

-- test of warning when session variable is shadowed
SET session_variables_ambiguity_warning TO on;

DO $$
<<myblock>>
DECLARE plpgsql_sv_var1 int;
BEGIN
  -- should be ok without warning
  LET plpgsql_sv_var1 = 100;

  -- should be ok without warning
  plpgsql_sv_var1 := 1000;

  -- should be ok without warning
  RAISE NOTICE 'session variable is %', public.plpgsql_sv_var1;

  -- should be ok without warning
  RAISE NOTICE 'plpgsql variable is %', myblock.plpgsql_sv_var1;

  -- should to print plpgsql variable with warning
  RAISE NOTICE 'variable is %', plpgsql_sv_var1;
END;
$$;

SET session_variables_ambiguity_warning TO off;

DROP VARIABLE plpgsql_sv_var1;

-- the value should not be corrupted
CREATE VARIABLE plpgsql_sv_v text;
LET plpgsql_sv_v = 'abc';

CREATE FUNCTION ffunc()
RETURNS text AS $$
BEGIN
  RETURN gfunc(plpgsql_sv_v);
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION gfunc(t text)
RETURNS text AS $$
BEGIN
  LET plpgsql_sv_v = 'BOOM!';
  RETURN t;
END;
$$ LANGUAGE plpgsql;

select ffunc();

DROP FUNCTION ffunc();
DROP FUNCTION gfunc(text);

DROP VARIABLE plpgsql_sv_v;
