/* contrib/pg_stat_statements/pg_stat_statements--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_stat_statements UPDATE TO '1.6'" to load this file. \quit

/* First we have to remove them from the extension */
ALTER EXTENSION pg_stat_statements DROP VIEW pg_stat_statements;
ALTER EXTENSION pg_stat_statements DROP FUNCTION pg_stat_statements(boolean);
ALTER EXTENSION pg_stat_statements DROP FUNCTION pg_stat_statements_reset();

/* Then we can drop them */
DROP VIEW pg_stat_statements;
DROP FUNCTION pg_stat_statements(boolean);
DROP FUNCTION pg_stat_statements_reset();

-- Register functions.
CREATE FUNCTION pg_stat_statements_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_stat_statements_plan_reset(IN queryid bigint)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_stat_statements_plan_reset(IN queryid bigint, IN plantype cstring)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_stat_statements(IN showtext boolean,
    OUT userid oid,
    OUT dbid oid,
    OUT queryid bigint,
    OUT query text,
    OUT calls int8,
    OUT total_time float8,
    OUT min_time float8,
    OUT max_time float8,
    OUT mean_time float8,
    OUT stddev_time float8,
    OUT rows int8,
    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_dirtied int8,
    OUT shared_blks_written int8,
    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_dirtied int8,
    OUT local_blks_written int8,
    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT blk_read_time float8,
    OUT blk_write_time float8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_statements_1_6'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION pg_stat_statements_plans(IN showtext boolean,
    OUT queryid bigint,
    OUT plan text,
    OUT plan_time float8,
    OUT plan_timestamp timestamp
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_statements_plans_1_6'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

-- Register a view on the function for ease of use.
CREATE VIEW pg_stat_statements AS
  SELECT * FROM pg_stat_statements(true);

CREATE VIEW pg_stat_statements_plans AS
  SELECT * FROM pg_stat_statements_plans(true);

GRANT SELECT ON pg_stat_statements TO PUBLIC;
GRANT SELECT ON pg_stat_statements_plans TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_statements_reset() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_stat_statements_plan_reset(bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_stat_statements_plan_reset(bigint, cstring) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_stat_statements_reset() TO pg_read_all_stats;
