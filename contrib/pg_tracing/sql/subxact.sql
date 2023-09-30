-- Create test table
CREATE TABLE IF NOT EXISTS pg_tracing_test (a int, b char(20));

-- Enable full sampling
SET pg_tracing.sample_rate = 1.0;

-- Start a transaction with subxaction
BEGIN;
SAVEPOINT s1;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s2;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s3;
SELECT 1;
COMMIT;

-- Check that subxact_count is correctly reported
select resource, parameters, subxact_count from pg_tracing_consume_spans order by span_start, span_start_ns;
