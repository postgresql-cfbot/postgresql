-- Create test function to sample
CREATE OR REPLACE FUNCTION test_function(a int) RETURNS SETOF oid AS
$BODY$
BEGIN
	RETURN QUERY SELECT oid from pg_class where oid = a;
END;
$BODY$
LANGUAGE plpgsql;

-- Trace a statement with a function call
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000051-0000000000000051-01'*/ select test_function(1);

-- The test function call will generate the following spans (non exhaustive list):
-- +-----------------------------------------------------------------------------------------------+
-- | A: Select test_function(1);                                                                   |
-- +-+--------++----------++----------------++---------------------------------------------------+-+
--   |B: Parse||C: Planner||D: ExecutorStart||E: ExecutorRun                                     |
--   +--------++----------++----------------+++-------------------------------------------------++
--                                            |F: ProjectSet                                    |
--                                            ++---------+------+------------------------------++
--                                             |G: Result|      | H: Select a from b where...  |
--                                             +---------+      +----+--------------+----------+
--                                                                   |I: ExecutorRun|
--                                                                   +--------------+

-- Gather span_id, span start and span end of function call statement
SELECT span_id AS span_a_id,
        get_span_start_ns(span_start, span_start_ns) as span_a_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_a_end
		from pg_tracing_peek_spans where parent_id=81 and name!='Parse' \gset
SELECT span_id AS span_e_id,
        get_span_start_ns(span_start, span_start_ns) as span_e_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_e_end
		from pg_tracing_peek_spans where parent_id=:span_a_id and name='Executor' and resource='Run' \gset
SELECT span_id AS span_f_id,
        get_span_start_ns(span_start, span_start_ns) as span_f_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_f_end
		from pg_tracing_peek_spans where parent_id=:span_e_id and name='ProjectSet' \gset
SELECT span_id AS span_g_id,
        get_span_start_ns(span_start, span_start_ns) as span_g_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_g_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and name='Result' \gset
SELECT span_id AS span_h_id,
        get_span_start_ns(span_start, span_start_ns) as span_h_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_h_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and name='Select' \gset
SELECT span_id AS span_i_id,
        get_span_start_ns(span_start, span_start_ns) as span_i_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_i_end
		from pg_tracing_peek_spans where parent_id=:span_h_id and resource='Run' \gset

-- Check that spans' start and end are within expection
SELECT :span_a_start < :span_e_start AS top_query_before_run,
		:span_a_end >= :span_e_end AS top_end_after_run_end,

		:span_e_start <= :span_f_start AS top_run_start_before_project,

		:span_e_end >= :span_f_end AS top_run_end_after_project_end,
		:span_e_end >= :span_h_end AS top_run_end_before_select_end,
		:span_e_end >= :span_i_end AS top_run_end_after_nested_run_end,

		:span_i_end <= :span_h_end AS run_end_after_select_end;

-- Check that the root span is the longest one
WITH max_duration AS (select max(duration) from pg_tracing_peek_spans)
SELECT duration = max_duration.max from pg_tracing_peek_spans, max_duration
    where span_id = :span_a_id;

-- Check that ExecutorRun is attached to the nested top span
SELECT resource from pg_tracing_peek_spans where parent_id=:span_i_id order by resource;

-- Check tracking with top tracking
set pg_tracing.track = 'top';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000052-0000000000000052-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_consume_spans where trace_id=82;

-- Check tracking with no tracking
set pg_tracing.track = 'none';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000053-0000000000000053-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_consume_spans where trace_id=83;

-- Reset tracking setting
set pg_tracing.track = 'all';

-- Create test procedure
CREATE OR REPLACE PROCEDURE sum_one(i int) AS $$
DECLARE
  r int;
BEGIN
  SELECT (i + i)::int INTO r;
END; $$ LANGUAGE plpgsql;

-- Test tracking of procedure with utility tracking enabled
set pg_tracing.track_utility=on;
/*traceparent='00-00000000000000000000000000000054-0000000000000054-01'*/ CALL sum_one(3);
SELECT resource from pg_tracing_consume_spans order by span_start, span_start_ns, resource;


-- Test again with utility tracking disabled
set pg_tracing.track_utility=off;
/*traceparent='00-00000000000000000000000000000055-0000000000000055-01'*/ CALL sum_one(10);
SELECT resource from pg_tracing_consume_spans order by span_start, span_start_ns, resource;
