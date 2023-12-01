-- Some helper functions
CREATE OR REPLACE FUNCTION get_span_start_ns(time_start timestamptz, start_ns smallint) RETURNS bigint AS
$BODY$
BEGIN
    RETURN extract(epoch from time_start) * 1000000000 + start_ns;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_span_end_ns(time_start timestamptz, start_ns smallint, duration bigint) RETURNS bigint AS
$BODY$
BEGIN
    RETURN get_span_start_ns(time_start, start_ns) + duration;
END;
$BODY$
LANGUAGE plpgsql;

-- Create pg_tracing extension with sampling on
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE EXTENSION pg_tracing;

-- This will create the following spans (non exhaustive list):
--
-- +-------------------------------------------------------------------------------------+
-- | A: Utility: Create Extension                                                        |
-- +-+-----------------------------------------------------------------------------------+
--   +----------------------------------------------------------------------------------+
--   |B: ProcessUtility: Create Extension                                               |
--   +---+-----------------------------------+---+--------------------------------------+
--       +-----------------------------------+   +-------------------------------------+
--       |C: Utility: Create Function1       |   |E: Utility: Create Function2         |
--       ++----------------------------------+   ++-----------------------------------++
--        +----------------------------------+    +-----------------------------------+
--        |D: ProcessUtility: Create Function|    |F: ProcessUtility: Create Function2|
--        +----------------------------------+    +-----------------------------------+

-- Extract span_ids, start and end of those spans
SELECT span_id AS span_a_id,
        get_span_start_ns(span_start, span_start_ns) as span_a_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_a_end
		from pg_tracing_peek_spans where parent_id=1 and span_type='Utility' \gset

SELECT span_id AS span_b_id,
        get_span_start_ns(span_start, span_start_ns) as span_b_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_b_end
		from pg_tracing_peek_spans where parent_id=:span_a_id and span_type='ProcessUtility' \gset

SELECT span_id AS span_c_id,
        get_span_start_ns(span_start, span_start_ns) as span_c_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_c_end
		from pg_tracing_peek_spans where parent_id=:span_b_id and span_type='Utility' limit 1 \gset

SELECT span_id AS span_d_id,
        get_span_start_ns(span_start, span_start_ns) as span_d_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_d_end
		from pg_tracing_peek_spans where parent_id=:span_c_id and span_type='ProcessUtility' \gset

SELECT span_id AS span_e_id,
        get_span_start_ns(span_start, span_start_ns) as span_e_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_e_end
		from pg_tracing_peek_spans where parent_id=:span_b_id and span_type='Utility' limit 1 offset 1 \gset

-- Check that the start and end of those spans are within expectation
SELECT :span_a_start < :span_b_start AS span_a_start_first,
		:span_a_end >= :span_b_end AS span_a_end_last,

		:span_d_end <= :span_c_end AS nested_span_ends_before_parent,
		:span_c_end <= :span_e_start AS next_utility_start_after;

-- Clean current spans
select count(*) from pg_tracing_consume_spans;

--
-- Test that no utility is captured with track_utility off
--

-- Set utility off
SET pg_tracing.track_utility = off;

-- Prepare and execute a prepared statement
PREPARE test_prepared_one_param (integer) AS SELECT $1;
EXECUTE test_prepared_one_param(100);

-- Nothing should be generated
select count(*) from pg_tracing_consume_spans;

-- Force a query to start from ExecutorRun
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);

-- Again, nothing should be generated
select count(*) from pg_tracing_consume_spans;

--
-- Test that no utility is captured with track_utility off
--

-- Enable utility tracking and track everything
SET pg_tracing.track_utility = on;
SET pg_tracing.sample_rate = 1.0;

-- Prepare and execute a prepared statement
PREPARE test_prepared_one_param_2 (integer) AS SELECT $1;
EXECUTE test_prepared_one_param_2(100);

-- Check the number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check that the spans are in correct order
select span_operation, parameters from pg_tracing_peek_spans order by span_start, span_start_ns;
-- Check the top span (standalone top span has trace_id=parent_id)
select span_operation, parameters from pg_tracing_consume_spans where trace_id = parent_id order by span_start, span_start_ns;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);

-- Check the number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check that the spans are in correct order
select span_operation, parameters from pg_tracing_peek_spans order by span_start, span_start_ns;
-- Check the top span (standalone top span has trace_id=parent_id)
select span_operation, parameters from pg_tracing_consume_spans where trace_id = parent_id order by span_start, span_start_ns;

-- Second create extension should generate an error that is captured by span
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE EXTENSION pg_tracing;
select span_operation, parameters, sql_error_code from pg_tracing_consume_spans order by span_start, span_start_ns;

-- Create test table
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE TABLE pg_tracing_test (a int, b char(20));
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ CREATE INDEX pg_tracing_index ON pg_tracing_test (a);

-- Check create table and index spans
select trace_id, span_type, span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, duration desc;

-- Cleanup
SET plan_cache_mode='auto';


CREATE OR REPLACE FUNCTION lazy_function(IN anyarray, OUT x anyelement, OUT n int)
    RETURNS SETOF RECORD
    LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE
    AS 'select s from pg_catalog.generate_series(1, 1, 1) as g(s)';

select lazy_function('{1,2,3}'::int[]);

