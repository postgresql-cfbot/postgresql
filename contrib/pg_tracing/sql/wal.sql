-- Create test table
CREATE TABLE IF NOT EXISTS pg_tracing_test (a int, b char(20));

-- Enable wal instrumentation
set pg_tracing.instrument_wal = true;

-- Generate queries with wal write
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test VALUES(generate_series(1, 10), 'aaa');
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ UPDATE pg_tracing_test SET b = 'bbb' WHERE a > 7;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ DELETE FROM pg_tracing_test WHERE a > 9;

-- Check WAL is generated for the above statements
SELECT trace_id, name, resource,
       wal_records > 0 as wal_records,
       wal_bytes > 0 as wal_bytes
FROM pg_tracing_consume_spans order by span_start, span_start_ns, resource;

-- Redo the same but without wal instrumentation
set pg_tracing.instrument_wal = false;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ INSERT INTO pg_tracing_test VALUES(generate_series(1, 10), 'aaa');
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ UPDATE pg_tracing_test SET b = 'bbb' WHERE a > 7;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ DELETE FROM pg_tracing_test WHERE a > 9;

-- With wal instrumentation disabled, wal counters should be set to 0
SELECT trace_id, name, resource,
       wal_records = 0 as wal_records,
       wal_bytes = 0 as wal_bytes
FROM pg_tracing_consume_spans order by span_start, span_start_ns, resource;

-- Cleanup
set pg_tracing.instrument_wal = true;
