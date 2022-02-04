/* contrib/pg_stat_statements/pg_stat_statements--1.9--1.10.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_stat_statements UPDATE TO '1.10'" to load this file. \quit

/* We need to redefine a view and a function */
/* First we have to remove them from the extension */
ALTER EXTENSION pg_stat_statements DROP VIEW pg_stat_statements;
ALTER EXTENSION pg_stat_statements DROP FUNCTION pg_stat_statements(boolean);

/* Then we can drop them */
DROP VIEW pg_stat_statements;
DROP FUNCTION pg_stat_statements(boolean);

/* Now redefine */
CREATE FUNCTION pg_stat_statements(IN showtext boolean,
    OUT userid oid,
    OUT dbid oid,
    OUT toplevel bool,
    OUT queryid bigint,
    OUT query text,
    OUT plans int8,
    OUT total_plan_time float8,
    OUT min_plan_time float8,
    OUT max_plan_time float8,
    OUT mean_plan_time float8,
    OUT stddev_plan_time float8,
    OUT aux_min_plan_time float8,
    OUT aux_max_plan_time float8,
    OUT calls int8,
    OUT total_exec_time float8,
    OUT min_exec_time float8,
    OUT max_exec_time float8,
    OUT mean_exec_time float8,
    OUT stddev_exec_time float8,
    OUT aux_min_exec_time float8,
    OUT aux_max_exec_time float8,
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
    OUT blk_write_time float8,
    OUT wal_records int8,
    OUT wal_fpi int8,
    OUT wal_bytes numeric,
    OUT stats_since timestamp with time zone,
    OUT aux_stats_since timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_statements_1_10'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_statements AS
  SELECT
    userid,
    dbid,
    toplevel,
    queryid,
    query,
    plans,
    total_plan_time,
    min_plan_time,
    max_plan_time,
    mean_plan_time,
    stddev_plan_time,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    stats_since
  FROM pg_stat_statements(true);

CREATE VIEW pg_stat_statements_aux AS
  SELECT
    userid,
    dbid,
    toplevel,
    queryid,
    query,
    aux_min_plan_time,
    aux_max_plan_time,
    aux_min_exec_time,
    aux_max_exec_time,
    stats_since,
    aux_stats_since
  FROM pg_stat_statements(true);

CREATE FUNCTION pg_stat_statements_aux_reset(IN userid Oid DEFAULT 0,
	IN dbid Oid DEFAULT 0,
	IN queryid bigint DEFAULT 0
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_stat_statements_aux_reset_1_10'
LANGUAGE C STRICT PARALLEL SAFE;

GRANT SELECT ON pg_stat_statements, pg_stat_statements_aux TO PUBLIC;
