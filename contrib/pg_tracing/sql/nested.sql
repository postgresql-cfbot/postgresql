-- Create test function to sample
CREATE OR REPLACE FUNCTION test_function_project_set(a int) RETURNS SETOF oid AS
$BODY$
BEGIN
	RETURN QUERY SELECT oid from pg_class where oid = a;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_function_result(a int, b text) RETURNS void AS
$BODY$
BEGIN
    INSERT INTO pg_tracing_test(a, b) VALUES (a, b);
END;
$BODY$
LANGUAGE plpgsql;


-- Trace a statement with a function call
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000051-0000000000000051-01'*/ select test_function_project_set(1);

-- The test function call will generate the following spans (non exhaustive list):
-- +---------------------------------------------------------------------------------------------------+
-- | A: Select test_function_project_set(1);                                                                       |
-- +-+--------++----------++----------------++--------------------------------------------------------++
--   |B: Parse||C: Planner||D: ExecutorStart||E: ExecutorRun                                          |
--   +--------++----------++----------------+++-----------------------------------------------------+-+
--                                            |F: ProjectSet                                        |
--                                            ++---------+---------+------------------------------+-+
--                                             |G: Result|H: Parse | I: Select a from b where...  |
--                                             +---------+---------+----+--------------+----------+
--                                                                      |J: ExecutorRun|
--                                                                      +--------------+

-- Gather span_id, span start and span end of function call statement
SELECT span_id AS span_a_id,
        get_span_start_ns(span_start, span_start_ns) as span_a_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_a_end
		from pg_tracing_peek_spans where parent_id=81 and span_type!='Parse' \gset
SELECT span_id AS span_e_id,
        get_span_start_ns(span_start, span_start_ns) as span_e_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_e_end
		from pg_tracing_peek_spans where parent_id=:span_a_id and span_type='Executor' and span_operation='ExecutorRun' \gset
SELECT span_id AS span_f_id,
        get_span_start_ns(span_start, span_start_ns) as span_f_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_f_end
		from pg_tracing_peek_spans where parent_id=:span_e_id and span_type='ProjectSet' \gset
SELECT span_id AS span_g_id,
        get_span_start_ns(span_start, span_start_ns) as span_g_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_g_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and span_type='Result' \gset
SELECT span_id AS span_h_id,
        get_span_start_ns(span_start, span_start_ns) as span_h_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_h_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and span_type='Parse' \gset
SELECT span_id AS span_i_id,
        get_span_start_ns(span_start, span_start_ns) as span_i_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_i_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and span_type='Select' \gset
SELECT span_id AS span_j_id,
        get_span_start_ns(span_start, span_start_ns) as span_j_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_j_end
		from pg_tracing_peek_spans where parent_id=:span_i_id and span_operation='ExecutorRun' \gset

-- Check that spans' start and end are within expection
SELECT :span_a_start < :span_e_start AS top_query_before_run,
		:span_a_end >= :span_e_end AS top_ends_after_run_end,

		:span_e_start <= :span_f_start AS top_run_starts_before_project,

		:span_e_end >= :span_f_end AS top_run_ends_after_project_end,
		:span_e_end >= :span_h_end AS top_run_ends_before_select_end,
		:span_e_end >= :span_i_end AS top_run_ends_after_nested_run_end;

SELECT
		:span_g_end >= :span_h_start AS nested_result_ends_before_parse,
		:span_h_end <= :span_i_start AS nested_parse_ends_before_select,

		:span_j_start >= :span_i_start AS run_starts_after_parent_select,
		:span_j_end <= :span_i_end AS run_ends_after_select_end;

-- Check that the root span is the longest one
WITH max_duration AS (select max(duration) from pg_tracing_peek_spans)
SELECT duration = max_duration.max from pg_tracing_peek_spans, max_duration
    where span_id = :span_a_id;

-- Check that ExecutorRun is attached to the nested top span
SELECT span_operation, deparse_info from pg_tracing_peek_spans where parent_id=:span_j_id order by span_operation;

-- Check tracking with top tracking
set pg_tracing.track = 'top';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000052-0000000000000052-01'*/ select test_function_project_set(1);
SELECT count(*) from pg_tracing_consume_spans where trace_id=82;

-- Check tracking with no tracking
set pg_tracing.track = 'none';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000053-0000000000000053-01'*/ select test_function_project_set(1);
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
SELECT span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;


-- Test again with utility tracking disabled
set pg_tracing.track_utility=off;
/*traceparent='00-00000000000000000000000000000055-0000000000000055-01'*/ CALL sum_one(10);
SELECT span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Create immutable function
CREATE OR REPLACE FUNCTION test_immutable_function(a int) RETURNS oid
AS 'SELECT oid from pg_class where oid = a;'
LANGUAGE sql IMMUTABLE;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000056-0000000000000056-01'*/ select test_immutable_function(1);
SELECT span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Create function with generate series

CREATE OR REPLACE FUNCTION test_generate_series(IN anyarray, OUT x anyelement)
    RETURNS SETOF anyelement
    LANGUAGE sql
    AS 'select * from pg_catalog.generate_series(array_lower($1, 1), array_upper($1, 1), 1)';

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000057-0000000000000057-01'*/ select test_generate_series('{1,2,3,4}'::int[]) FROM (VALUES (1,2));

SELECT span_id AS span_project_set_id,
        get_span_start_ns(span_start, span_start_ns) as span_project_set_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_project_set_end
		from pg_tracing_peek_spans where span_type='ProjectSet' \gset

SELECT span_id AS span_result_id,
        get_span_start_ns(span_start, span_start_ns) as span_result_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_result_end
		from pg_tracing_peek_spans where parent_id=:span_project_set_id and span_type='Result' \gset

SELECT span_id AS span_parse,
        get_span_start_ns(span_start, span_start_ns) as span_parse_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_parse_end
		from pg_tracing_peek_spans where parent_id=:span_project_set_id and span_type='Parse' \gset

-- Check that spans' start and end are within expection
SELECT :span_project_set_start < :span_parse_start AS project_set_starts_before_parse,
        :span_result_start < :span_parse_start AS result_starts_before_parse,
        :span_result_end <= :span_parse_start AS result_ends_before_parse,
        :span_project_set_end > :span_parse_end AS project_set_ends_after_parse;

SELECT span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- +---------------------------------------------------------------------------------------------------+
-- | A: Select test_function(1);                                                                       |
-- +-+--------++----------++----------------++--------------------------------------------------------++
--   |B: Parse||C: Planner||D: ExecutorStart||E: ExecutorRun                                          |
--   +--------++----------++----------------+++-----------------------------------------------------+-+
--                                            |F: Result                                            |
--                                            ++---------------+----------------------------------+-+
--                                             |G: Parse       |     H: Insert INTO...            |
--                                             +---------------+--------+--------------+----------+
--                                                                      |I: ExecutorRun|
--                                                                      +--------------+

-- Check function with result node
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000058-0000000000000058-01'*/ select test_function_result(1, 'test');

-- Gather span_id, span start and span end of function call statement
SELECT span_id AS span_a_id,
        get_span_start_ns(span_start, span_start_ns) as span_a_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_a_end
		from pg_tracing_peek_spans where parent_id=88 and span_type!='Parse' \gset
SELECT span_id AS span_e_id,
        get_span_start_ns(span_start, span_start_ns) as span_e_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_e_end
		from pg_tracing_peek_spans where parent_id=:span_a_id and span_type='Executor' and span_operation='ExecutorRun' \gset
SELECT span_id AS span_f_id,
        get_span_start_ns(span_start, span_start_ns) as span_f_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_f_end
		from pg_tracing_peek_spans where parent_id=:span_e_id and span_type='Result' \gset
SELECT span_id AS span_g_id,
        get_span_start_ns(span_start, span_start_ns) as span_g_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_g_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and span_type='Parse' \gset
SELECT span_id AS span_h_id,
        get_span_start_ns(span_start, span_start_ns) as span_h_start,
        get_span_end_ns(span_start, span_start_ns, duration) as span_h_end
		from pg_tracing_peek_spans where parent_id=:span_f_id and span_type='Insert' \gset

-- Check that parse span is correctly positioned
SELECT :span_g_start >= :span_f_start AS parse_start_after_result,
		:span_g_end < :span_f_end AS parse_ends_before_result,
		:span_g_end <= :span_h_start AS parse_ends_before_insert_node;

SELECT span_operation from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;
