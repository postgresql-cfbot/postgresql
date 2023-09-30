/* contrib/pg_tracing/pg_tracing--1.0.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION pg_tracing" to load this file. \quit

--- Define pg_tracing_info
CREATE FUNCTION pg_tracing_info(
    OUT traces bigint,
    OUT spans bigint,
    OUT dropped_spans bigint,
    OUT failed_truncates bigint,
    OUT last_consume timestamp with time zone,
    OUT stats_reset timestamp with time zone,
    OUT sampling_rate double precision
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE FUNCTION pg_tracing_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_tracing_spans(
    IN consume bool,
    OUT trace_id bigint,
    OUT parent_id bigint,
    OUT span_id bigint,
    OUT query_id bigint,
    OUT name text,
    OUT resource text,
    OUT span_start timestamp with time zone,
    OUT span_start_ns smallint,
    OUT duration bigint,
    OUT sql_error_code character(5),
    OUT pid int4,
    OUT nested_level smallint,
    OUT subxact_count smallint,
    OUT top_span bool,

--  Plan counters
    OUT startup_cost float8,
    OUT total_cost float8,
    OUT plan_rows float8,
    OUT plan_width int,

-- Node Counters
    OUT rows int8,
    OUT nloops int8,

    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_dirtied int8,
    OUT shared_blks_written int8,

    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_dirtied int8,
    OUT local_blks_written int8,

    OUT blk_read_time float8,
    OUT blk_write_time float8,

    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT temp_blk_read_time float8,
    OUT temp_blk_write_time float8,

    OUT wal_records int8,
    OUT wal_fpi int8,
    OUT wal_bytes numeric,

    OUT jit_functions int8,
    OUT jit_generation_time float8,
    OUT jit_inlining_time float8,
    OUT jit_optimization_time float8,
    OUT jit_emission_time float8,

--  SpanNode specific data
    OUT startup bigint, -- First tuple
    OUT parameters text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW pg_tracing_info AS
  SELECT * FROM pg_tracing_info();

CREATE VIEW pg_tracing_peek_spans AS
  SELECT * FROM pg_tracing_spans(false);

CREATE VIEW pg_tracing_consume_spans AS
  SELECT * FROM pg_tracing_spans(true);

GRANT SELECT ON pg_tracing_info TO PUBLIC;
GRANT SELECT ON pg_tracing_peek_spans TO PUBLIC;
GRANT SELECT ON pg_tracing_consume_spans TO PUBLIC;
