-- Trace nothing
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 0.0;

-- Query with sampling flag
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000021-0000000000000021-01'*/ SELECT 1;
-- Query without trace context
SELECT 1;

-- No spans should have been generated
select count(distinct(trace_id)) from pg_tracing_consume_spans;

-- Enable full sampling
SET pg_tracing.sample_rate = 1.0;

-- Generate queries with sampling flag on, off and no trace context
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000022-0000000000000022-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000023-0000000000000023-00'*/ SELECT 2;
SELECT 3;
SELECT 4;

-- Check number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check span order
select span_operation, parameters from pg_tracing_peek_spans order by span_start, span_start_ns;
-- Top spans should reuse generated ids and have trace_id = parent_id
select span_operation, parameters from pg_tracing_consume_spans where trace_id = parent_id order by span_start, span_start_ns;

-- Only trace query with sampled flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

-- Generate queries with sampling flag on, off and no trace context
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000024-0000000000000024-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000025-0000000000000025-00'*/ SELECT 2;
SELECT 1;

-- Check number of generated spans
select count(distinct(trace_id)) from pg_tracing_peek_spans;
-- Check span ordering
select span_operation, parameters from pg_tracing_consume_spans order by span_start, span_start_ns;
