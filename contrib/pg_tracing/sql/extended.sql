-- Trace everything
SET pg_tracing.sample_rate = 1.0;

-- Simple query with extended protocol
SELECT $1, $2 \bind 1 2 \g

-- Check generated spans
SELECT span_type, span_operation, parameters FROM pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Trigger an error due to mismatching number of parameters
BEGIN; select $1 \bind \g
ROLLBACK;

-- Check that no spans were generated
SELECT span_type, span_operation, parameters FROM pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Execute queries with extended protocol within an explicit transaction
BEGIN;
SELECT $1 \bind 1 \g
SELECT $1, $2 \bind 2 3 \g
COMMIT;

-- Spans within the same transaction should have been generated with the same trace_id
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
-- Check generated spans order
SELECT span_type, span_operation, parameters FROM pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- Mix extended protocol and simple protocol
BEGIN;
SELECT $1 \bind 1 \g
SELECT 5, 6, 7;
SELECT $1, $2 \bind 2 3 \g
COMMIT;

-- Spans within the same transaction should have been generated with the same trace_id
SELECT count(distinct(trace_id)) = 1 FROM pg_tracing_peek_spans;
-- Check generated spans order
SELECT span_type, span_operation, parameters FROM pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;

-- gdesc calls a single parse command then execute a query. Make sure we handle this case
\gdesc

-- Checking gdesc generated spans
SELECT span_type, span_operation, parameters FROM pg_tracing_consume_spans order by span_start, span_start_ns, span_operation;
