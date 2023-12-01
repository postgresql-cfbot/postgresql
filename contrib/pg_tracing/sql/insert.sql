-- Only trace queries with sample flag
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 1.0;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE TABLE IF NOT EXISTS pg_tracing_test_table_with_constraint (a int, b char(20), CONSTRAINT PK_tracing_test PRIMARY KEY (a));

-- Check create statement spans
SELECT span_type, span_operation from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, span_operation;

-- Simple insertion
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');

-- Check insert spans
SELECT span_type, span_operation from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, span_operation;

-- Trigger constraint violation
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(1, 'aaa');

-- Check violation spans
SELECT span_type, span_operation, sql_error_code from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, span_operation;

-- Trigger an error while calling pg_tracing_peek_spans which resets tracing
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test_table_with_constraint VALUES(length((select sql_error_code from public.pg_tracing_peek_spans)), 'aaa');

-- Nothing should be generated
SELECT span_type, span_operation, sql_error_code from pg_tracing_consume_spans where trace_id=1 order by span_start, span_start_ns, span_operation;
