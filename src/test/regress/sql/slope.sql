--
-- SLOPE (Scalar function Leveraging Ordered Path Evaluation)
-- Test that monotonic functions can use indexes for ordering
--

-- Create test table with various data types
CREATE TABLE slope_src (
    id serial PRIMARY KEY,
    v_int2 int2,
    v_int4 int4,
    v_int8 int8,
    v_float4 float4,
    v_float8 float8,
    v_numeric numeric,
    ts timestamp,
    tstz timestamptz
);

-- Insert some test data
INSERT INTO slope_src (v_int2, v_int4, v_int8, v_float4, v_float8, v_numeric, ts, tstz)
SELECT
    (i % 100)::int2,
    i,
    i::int8,
    i::float4,
    i::float8,
    i::numeric,
    '2020-01-01'::timestamp + (i || ' hours')::interval,
    '2020-01-01'::timestamptz + (i || ' hours')::interval
FROM generate_series(1, 1000) i;

-- Create indexes on the columns we'll test
CREATE INDEX slope_src_v_int4_idx ON slope_src (v_int4);
CREATE INDEX slope_src_v_int8_idx ON slope_src (v_int8);
CREATE INDEX slope_src_v_float8_idx ON slope_src (v_float8);
CREATE INDEX slope_src_v_numeric_idx ON slope_src (v_numeric);
CREATE INDEX slope_src_ts_idx ON slope_src (ts);
CREATE INDEX slope_src_tstz_idx ON slope_src (tstz);

-- Analyze to get good statistics
ANALYZE slope_src;

-- Disable hash aggregation to force group aggregate plan
SET enable_hashagg = off;

--
-- Test GROUP BY with monotonic function
--

-- Basic: floor(float8) should use index on v_float8
explain (costs off, verbose)
select floor(v_float8), count(*) from slope_src group by 1;

-- ceil(float8) should use index on v_float8
explain (costs off, verbose)
select ceil(v_float8), count(*) from slope_src group by 1;

-- floor(numeric) should use index on v_numeric
explain (costs off, verbose)
select floor(v_numeric), count(*) from slope_src group by 1;

-- timestamp::date cast should use index on ts
explain (costs off, verbose)
select ts::date, count(*) from slope_src group by 1;

-- date_trunc on timestamp should use index
explain (costs off, verbose)
select date_trunc('day', ts), count(*) from slope_src group by 1;

-- date_trunc on timestamptz should use index
explain (costs off, verbose)
select date_trunc('day', tstz), count(*) from slope_src group by 1;

--
-- Test arithmetic operations
--

-- Addition: v_int4 + 10 is increasing in v_int4
explain (costs off, verbose)
select v_int4 + 10, count(*) from slope_src group by 1;

-- Subtraction: v_int4 - 10 is increasing in v_int4
explain (costs off, verbose)
select v_int4 - 10, count(*) from slope_src group by 1;

-- Multiplication by positive constant: v_int4 * 2 is increasing
explain (costs off, verbose)
select v_int4 * 2, count(*) from slope_src group by 1;

-- Division by positive constant: v_int4 / 2 is increasing
explain (costs off, verbose)
select v_int4 / 2, count(*) from slope_src group by 1;

--
-- Test decreasing functions (should use backward scan)
--

-- Unary minus: -v_int4 is decreasing in v_int4
explain (costs off, verbose)
select -v_int4, count(*) from slope_src group by 1;

-- Subtraction from constant: 1000 - v_int4 is decreasing in v_int4
explain (costs off, verbose)
select 1000 - v_int4, count(*) from slope_src group by 1;

-- Multiplication by negative constant: v_int4 * (-2) is decreasing
explain (costs off, verbose)
select v_int4 * (-2), count(*) from slope_src group by 1;

-- Division by negative constant: v_int4 / (-2) is decreasing
explain (costs off, verbose)
select v_int4 / (-2), count(*) from slope_src group by 1;

--
-- Test ORDER BY with monotonic function
--

-- ORDER BY floor(v_float8) should use index
explain (costs off, verbose)
select floor(v_float8), v_float8 from slope_src order by 1 limit 10;

-- ORDER BY -v_int4 DESC should use forward scan (decreasing + DESC = forward)
explain (costs off, verbose)
select -v_int4 from slope_src order by 1 desc limit 10;

-- ORDER BY -v_int4 ASC should use backward scan (decreasing + ASC = backward)
explain (costs off, verbose)
select -v_int4 from slope_src order by 1 limit 10;

--
-- Test nested monotonic function
--

-- floor(floor(x)) should still use index
explain (costs off, verbose)
select floor(floor(v_float8)), count(*) from slope_src group by 1;

-- floor(v + 1) should use index
explain (costs off, verbose)
select floor(v_float8 + 1), count(*) from slope_src group by 1;

--
-- Test all index/query direction+nulls combinations for SLOPE.
-- For an increasing function like floor(), the scan uses the index when both
-- direction and nulls agree (Forward) or both are flipped (Backward).
-- When only one differs, a Sort is required.
--
CREATE TABLE slope_nulls_tmp (v float8);
INSERT INTO slope_nulls_tmp VALUES (1), (NULL), (2);
ANALYZE slope_nulls_tmp;

CREATE TEMPORARY TABLE slope_nulls_results (
    seq serial,
    sign text, index_order text, query_order text, scan_method text,
    example text
);

SET enable_seqscan = off;

DO $$
DECLARE
    r record;
    plan_json json;
    node_type text;
    query text;
    r1 text;
    r2 text;
BEGIN
    FOR r IN
        SELECT idx_dir, idx_nf, qry_dir, qry_nf, sign
        FROM unnest(ARRAY['ASC','DESC']) AS idx_dir,
             unnest(ARRAY['FIRST','LAST']) AS idx_nf,
             unnest(ARRAY['+','-']) AS sign,
             unnest(ARRAY['FIRST','LAST']) AS qry_nf,
             unnest(ARRAY['ASC','DESC']) AS qry_dir
    LOOP
        EXECUTE format('CREATE INDEX slope_nulls_tmp_idx ON slope_nulls_tmp (v %s NULLS %s)',
                        r.idx_dir, r.idx_nf);
        query := format('SELECT floor(0.5 %s v) as x FROM slope_nulls_tmp ORDER BY 1 %s NULLS %s',
            r.sign, r.qry_dir, r.qry_nf);
        EXECUTE format('EXPLAIN (FORMAT JSON) %s', query)
        INTO plan_json;

        set enable_seqscan = on;
        set enable_indexscan = off;
        set enable_indexonlyscan = off;
        execute 'SELECT string_agg(coalesce(x::text, ''NULL''), '','') FROM (' || query || ') tmp(x)' into r1;
        set enable_seqscan = off;
        set enable_indexscan = on;
        set enable_indexonlyscan = on;
        execute 'SELECT string_agg(coalesce(x::text, ''NULL''), '','') FROM (' || query || ') tmp(x)' into r2;
        if r1 <> r2 then
            raise exception 'r1 <> r2';
        end if;
        node_type := plan_json->0->'Plan'->>'Node Type';
        INSERT INTO slope_nulls_results (sign, index_order, query_order, scan_method, example) VALUES (
            r.sign,
            r.idx_dir || ' NULLS ' || r.idx_nf,
            r.qry_dir || ' NULLS ' || r.qry_nf,
            CASE WHEN node_type IN ('Index Only Scan', 'Index Scan')
                 THEN plan_json->0->'Plan'->>'Scan Direction'
                 ELSE node_type
            END,
            r2
        );
        EXECUTE format('DROP INDEX slope_nulls_tmp_idx');
    END LOOP;
END;
$$;

SELECT sign, index_order, query_order, scan_method, example
FROM slope_nulls_results ORDER BY seq;

RESET enable_seqscan;
DROP TABLE slope_nulls_results;
DROP TABLE slope_nulls_tmp;

--
-- Test pathkey chains: f(x), x (descending chain)
-- The order of f(x) is implied by the order of x when f is monotonic.
-- A single index on ts should satisfy ORDER BY ts::date, ts.
--

-- descending chain: ts::date, ts — both from index on ts
explain (costs off, verbose)
select * from slope_src order by ts::date, ts limit 10;

-- descending chain: date_trunc('day', ts), ts
explain (costs off, verbose)
select * from slope_src order by date_trunc('day', ts), ts limit 10;

-- descending chain with siblings: date_trunc('month', ts), ts::date, ts
explain (costs off, verbose)
select * from slope_src order by date_trunc('month', ts), ts::date, ts limit 10;

--
-- Test pathkey chains: x, f(x) (ascending chain)
-- f(x) is constant when x is constant, so f(x) is a redundant tiebreaker.
-- A single index on ts should satisfy ORDER BY ts, ts::date.
--

-- ascending chain: ts, ts::date — f(x) redundant after x
explain (costs off, verbose)
select * from slope_src order by ts, ts::date limit 10;

-- ascending chain: ts, date_trunc('day', ts)
explain (costs off, verbose)
select * from slope_src order by ts, date_trunc('day', ts) limit 10;

-- ascending chain with non-monotonic but immutable function:
-- abs() is immutable but NOT monotonic; still valid as tiebreaker.
explain (costs off, verbose)
select v_float8, abs(v_float8)
from slope_src order by v_float8, abs(v_float8) limit 10;

--
-- Test DISTINCT with monotonic functions
--

-- DISTINCT on ts::date should use index on ts
explain (costs off, verbose)
select distinct ts::date from slope_src;

-- DISTINCT on floor(v_float8)
explain (costs off, verbose)
select distinct floor(v_float8) from slope_src;

--
-- Test window functions with PARTITION BY monotonic function
--

-- Window with PARTITION BY ts::date ORDER BY ts
explain (costs off, verbose)
select ts::date, ts, row_number() over (partition by ts::date order by ts)
from slope_src;

-- Cleanup
RESET enable_hashagg;
