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

ALTER OPERATOR FAMILY gist_int2_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (int2, int2) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (int4, int4) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (int8, int8) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_float4_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (float4, float4) FOR ORDER BY pg_catalog.float_ops;
ALTER OPERATOR FAMILY gist_float8_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (float8, float8) FOR ORDER BY pg_catalog.float_ops;
ALTER OPERATOR FAMILY gist_oid_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (oid, oid) FOR ORDER BY pg_catalog.oid_ops;
ALTER OPERATOR FAMILY gist_cash_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (money, money) FOR ORDER BY pg_catalog.money_ops;
ALTER OPERATOR FAMILY gist_date_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (date, date) FOR ORDER BY pg_catalog.integer_ops;
ALTER OPERATOR FAMILY gist_time_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (time, time) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_timestamp_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (timestamp, timestamp) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_timestamptz_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (timestamptz, timestamptz) FOR ORDER BY pg_catalog.interval_ops;
ALTER OPERATOR FAMILY gist_interval_ops USING gist ADD OPERATOR 15 pg_catalog.<-> (interval, interval) FOR ORDER BY pg_catalog.interval_ops;

-- drop distance operators

ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (int2, int2);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (int4, int4);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (int8, int8);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (float4, float4);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (float8, float8);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (oid, oid);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (money, money);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (date, date);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (time, time);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (timestamp, timestamp);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (timestamptz, timestamptz);
ALTER EXTENSION btree_gist DROP OPERATOR @extschema@.<-> (interval, interval);

DROP OPERATOR @extschema@.<-> (int2, int2);
DROP OPERATOR @extschema@.<-> (int4, int4);
DROP OPERATOR @extschema@.<-> (int8, int8);
DROP OPERATOR @extschema@.<-> (float4, float4);
DROP OPERATOR @extschema@.<-> (float8, float8);
DROP OPERATOR @extschema@.<-> (oid, oid);
DROP OPERATOR @extschema@.<-> (money, money);
DROP OPERATOR @extschema@.<-> (date, date);
DROP OPERATOR @extschema@.<-> (time, time);
DROP OPERATOR @extschema@.<-> (timestamp, timestamp);
DROP OPERATOR @extschema@.<-> (timestamptz, timestamptz);
DROP OPERATOR @extschema@.<-> (interval, interval);

-- drop distance functions

ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.int2_dist(int2, int2);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.int4_dist(int4, int4);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.int8_dist(int8, int8);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.float4_dist(float4, float4);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.float8_dist(float8, float8);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.oid_dist(oid, oid);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.cash_dist(money, money);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.date_dist(date, date);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.time_dist(time, time);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.ts_dist(timestamp, timestamp);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.tstz_dist(timestamptz, timestamptz);
ALTER EXTENSION btree_gist DROP FUNCTION @extschema@.interval_dist(interval, interval);

DROP FUNCTION @extschema@.int2_dist(int2, int2);
DROP FUNCTION @extschema@.int4_dist(int4, int4);
DROP FUNCTION @extschema@.int8_dist(int8, int8);
DROP FUNCTION @extschema@.float4_dist(float4, float4);
DROP FUNCTION @extschema@.float8_dist(float8, float8);
DROP FUNCTION @extschema@.oid_dist(oid, oid);
DROP FUNCTION @extschema@.cash_dist(money, money);
DROP FUNCTION @extschema@.date_dist(date, date);
DROP FUNCTION @extschema@.time_dist(time, time);
DROP FUNCTION @extschema@.ts_dist(timestamp, timestamp);
DROP FUNCTION @extschema@.tstz_dist(timestamptz, timestamptz);
DROP FUNCTION @extschema@.interval_dist(interval, interval);
