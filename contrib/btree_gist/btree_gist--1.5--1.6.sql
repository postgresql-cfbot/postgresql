/* contrib/btree_gist/btree_gist--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.6'" to load this file. \quit

-- drop btree_gist distance operators from opfamilies

ALTER OPERATOR FAMILY gist_int2_ops USING gist DROP OPERATOR 15 (int2, int2);
ALTER OPERATOR FAMILY gist_int4_ops USING gist DROP OPERATOR 15 (int4, int4);
ALTER OPERATOR FAMILY gist_int8_ops USING gist DROP OPERATOR 15 (int8, int8);
ALTER OPERATOR FAMILY gist_float4_ops USING gist DROP OPERATOR 15 (float4, float4);
ALTER OPERATOR FAMILY gist_float8_ops USING gist DROP OPERATOR 15 (float8, float8);
ALTER OPERATOR FAMILY gist_oid_ops USING gist DROP OPERATOR 15 (oid, oid);
ALTER OPERATOR FAMILY gist_cash_ops USING gist DROP OPERATOR 15 (money, money);
ALTER OPERATOR FAMILY gist_date_ops USING gist DROP OPERATOR 15 (date, date);
ALTER OPERATOR FAMILY gist_time_ops USING gist DROP OPERATOR 15 (time, time);
ALTER OPERATOR FAMILY gist_timestamp_ops USING gist DROP OPERATOR 15 (timestamp, timestamp);
ALTER OPERATOR FAMILY gist_timestamptz_ops USING gist DROP OPERATOR 15 (timestamptz, timestamptz);
ALTER OPERATOR FAMILY gist_interval_ops USING gist DROP OPERATOR 15 (interval, interval);

-- add pg_catalog distance operators to opfamilies

ALTER OPERATOR FAMILY gist_int2_ops USING gist ADD OPERATOR 15 <-> (int2, int2) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD OPERATOR 15 <-> (int4, int4) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD OPERATOR 15 <-> (int8, int8) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_float4_ops USING gist ADD OPERATOR 15 <-> (float4, float4) FOR ORDER BY pg_catalog.float_ops;
ALTER OPERATOR FAMILY gist_float8_ops USING gist ADD OPERATOR 15 <-> (float8, float8) FOR ORDER BY pg_catalog.float_ops;
ALTER OPERATOR FAMILY gist_oid_ops USING gist ADD OPERATOR 15 <-> (oid, oid) FOR ORDER BY pg_catalog.oid_ops;
ALTER OPERATOR FAMILY gist_cash_ops USING gist ADD OPERATOR 15 <-> (money, money) FOR ORDER BY pg_catalog.money_ops;
ALTER OPERATOR FAMILY gist_date_ops USING gist ADD OPERATOR 15 <-> (date, date) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_time_ops USING gist ADD OPERATOR 15 <-> (time, time) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_timestamp_ops USING gist ADD OPERATOR 15 <-> (timestamp, timestamp) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_timestamptz_ops USING gist ADD OPERATOR 15 <-> (timestamptz, timestamptz) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_interval_ops USING gist ADD OPERATOR 15 <-> (interval, interval) FOR ORDER BY pg_catalog.interval_ops;

-- disable implicit pg_catalog search

DO
$$
BEGIN
	EXECUTE 'SET LOCAL search_path TO ' || current_schema() || ', pg_catalog';
END
$$;

-- drop distance operators

ALTER EXTENSION btree_gist DROP OPERATOR <-> (int2, int2);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (int4, int4);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (int8, int8);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (float4, float4);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (float8, float8);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (oid, oid);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (money, money);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (date, date);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (time, time);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (timestamp, timestamp);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (timestamptz, timestamptz);
ALTER EXTENSION btree_gist DROP OPERATOR <-> (interval, interval);

DROP OPERATOR <-> (int2, int2);
DROP OPERATOR <-> (int4, int4);
DROP OPERATOR <-> (int8, int8);
DROP OPERATOR <-> (float4, float4);
DROP OPERATOR <-> (float8, float8);
DROP OPERATOR <-> (oid, oid);
DROP OPERATOR <-> (money, money);
DROP OPERATOR <-> (date, date);
DROP OPERATOR <-> (time, time);
DROP OPERATOR <-> (timestamp, timestamp);
DROP OPERATOR <-> (timestamptz, timestamptz);
DROP OPERATOR <-> (interval, interval);

-- drop distance functions

ALTER EXTENSION btree_gist DROP FUNCTION int2_dist(int2, int2);
ALTER EXTENSION btree_gist DROP FUNCTION int4_dist(int4, int4);
ALTER EXTENSION btree_gist DROP FUNCTION int8_dist(int8, int8);
ALTER EXTENSION btree_gist DROP FUNCTION float4_dist(float4, float4);
ALTER EXTENSION btree_gist DROP FUNCTION float8_dist(float8, float8);
ALTER EXTENSION btree_gist DROP FUNCTION oid_dist(oid, oid);
ALTER EXTENSION btree_gist DROP FUNCTION cash_dist(money, money);
ALTER EXTENSION btree_gist DROP FUNCTION date_dist(date, date);
ALTER EXTENSION btree_gist DROP FUNCTION time_dist(time, time);
ALTER EXTENSION btree_gist DROP FUNCTION ts_dist(timestamp, timestamp);
ALTER EXTENSION btree_gist DROP FUNCTION tstz_dist(timestamptz, timestamptz);
ALTER EXTENSION btree_gist DROP FUNCTION interval_dist(interval, interval);

DROP FUNCTION int2_dist(int2, int2);
DROP FUNCTION int4_dist(int4, int4);
DROP FUNCTION int8_dist(int8, int8);
DROP FUNCTION float4_dist(float4, float4);
DROP FUNCTION float8_dist(float8, float8);
DROP FUNCTION oid_dist(oid, oid);
DROP FUNCTION cash_dist(money, money);
DROP FUNCTION date_dist(date, date);
DROP FUNCTION time_dist(time, time);
DROP FUNCTION ts_dist(timestamp, timestamp);
DROP FUNCTION tstz_dist(timestamptz, timestamptz);
DROP FUNCTION interval_dist(interval, interval);
