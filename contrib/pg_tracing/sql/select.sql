-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Run a simple query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;

-- Get top span id
SELECT span_id AS top_span_id from pg_tracing_peek_spans where parent_id=1 and span_type!='Parse' \gset

-- Check parameters
SELECT parameters from pg_tracing_peek_spans where span_id=:top_span_id;

-- Check the number of children
SELECT count(*) from pg_tracing_peek_spans where parent_id=:'top_span_id';

-- Check span_operation
SELECT span_type, span_operation from pg_tracing_peek_spans where trace_id=1 order by span_start, span_start_ns, span_operation;

-- Check count of query_id
SELECT count(distinct query_id) from pg_tracing_peek_spans where trace_id=1;

-- Check reported number of trace
SELECT traces from pg_tracing_info;

-- Trace a statement with function call
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT count(*) from current_database();

-- Check the generated span span_type, span_operation and order of function call
SELECT span_type, span_operation from pg_tracing_consume_spans where trace_id=3 order by span_operation;

-- Trace a more complex query with multiple function calls
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ SELECT s.relation_size + s.index_size
FROM (SELECT
      pg_relation_size(C.oid) as relation_size,
      pg_indexes_size(C.oid) as index_size
    FROM pg_class C) as s limit 1;

-- Check the generated span span_type, span_operation and order of query with multiple function calls
SELECT span_type, span_operation from pg_tracing_consume_spans where trace_id=4 order by span_operation;

-- Check that we're in a correct state after a timeout
set statement_timeout=200;

-- Trace query triggering a statement timeout
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000007-0000000000000007-01'*/ select * from pg_sleep(10);
-- Cleanup statement setting
set statement_timeout=0;

-- Trace a working query after the timeout to check we're in a consistent state
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000008-0000000000000008-01'*/ select 1;

-- Check the spans order and error code
SELECT span_type, span_operation, sql_error_code from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Create a prepare statement with comment passed as first parameter
PREPARE test_prepared (text, integer) AS /*$1*/ SELECT $2;
-- Execute prepare statement with trace context passed as a parameter
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000009-0000000000000009-01''', 1);

-- Check generated spans and order
SELECT trace_id, span_type, span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
-- Execute prepare statement with trace context passed as a parameter and generic plan
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000010-0000000000000010-01''', 10);

-- Cleanup
SET plan_cache_mode='auto';
DEALLOCATE test_prepared;

-- Check spans are generated even through generic plan
SELECT trace_id, span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Run a statement with node not executed
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000a-000000000000000a-01'*/ select 1 limit 0;
-- Not executed node should not generate any spans
SELECT trace_id, span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Test multiple statements in a single query
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000011-0000000000000012-01'*/ select 1; select 2;
-- Not executed node should not generate any spans
select span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns;

-- Check that parameters are not exported when disabled
SET pg_tracing.export_parameters=false;
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000b-000000000000000b-01'*/ select 1, 2, 3;
SELECT trace_id, span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;
-- Reset export parameters setting
SET pg_tracing.export_parameters=true;

-- Check the result of disabling deparse
SET pg_tracing.deparse_plan=false;
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000c-000000000000000c-01'*/ SELECT * from pg_tracing_test where a=1;
SET pg_tracing.deparse_plan=true;
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000d-000000000000000d-01'*/ SELECT * from pg_tracing_test where a=1;
SELECT trace_id, span_operation, deparse_info, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Check multi statement query
SET pg_tracing.sample_rate = 1.0;
-- Force a multi-query statement with \;
SELECT 1\; SELECT 1, 2;
SELECT span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;


-- Check standalone trace
SELECT 1;
-- Make sure we have unique span ids
select count(span_id) from pg_tracing_consume_spans group by span_id;

-- Cleanup
SET pg_tracing.sample_rate = 0.0;
