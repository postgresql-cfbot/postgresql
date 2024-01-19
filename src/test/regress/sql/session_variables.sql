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
