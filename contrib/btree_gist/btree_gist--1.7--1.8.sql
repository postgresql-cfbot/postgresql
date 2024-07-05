/* contrib/btree_gist/btree_gist--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.7'" to load this file. \quit

-- Add mixed integer operators.
-- This lets Postgres use the index without casting literals to bigints, etc.
-- Whereas btree has one integer_ops opfamily,
-- GiST has gist_int2_ops, gist_int4_ops, and gist_int8_ops.
-- We add the mixed operators to whichever opfamily matches the larger type,
-- sort of like "promoting".
ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD
  OPERATOR 1 <  (int8, int4),
  OPERATOR 2 <= (int8, int4),
  OPERATOR 3 =  (int8, int4),
  OPERATOR 4 >= (int8, int4),
  OPERATOR 5 >  (int8, int4),

  OPERATOR 1 <  (int4, int8),
  OPERATOR 2 <= (int4, int8),
  OPERATOR 3 =  (int4, int8),
  OPERATOR 4 >= (int4, int8),
  OPERATOR 5 >  (int4, int8),

  OPERATOR 1 <  (int8, int2),
  OPERATOR 2 <= (int8, int2),
  OPERATOR 3 =  (int8, int2),
  OPERATOR 4 >= (int8, int2),
  OPERATOR 5 >  (int8, int2),

  OPERATOR 1 <  (int2, int8),
  OPERATOR 2 <= (int2, int8),
  OPERATOR 3 =  (int2, int8),
  OPERATOR 4 >= (int2, int8),
  OPERATOR 5 >  (int2, int8);

ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD
  OPERATOR 1 <  (int4, int2),
  OPERATOR 2 <= (int4, int2),
  OPERATOR 3 =  (int4, int2),
  OPERATOR 4 >= (int4, int2),
  OPERATOR 5 >  (int4, int2),

  OPERATOR 1 <  (int2, int4),
  OPERATOR 2 <= (int2, int4),
  OPERATOR 3 =  (int2, int4),
  OPERATOR 4 >= (int2, int4),
  OPERATOR 5 >  (int2, int4);

-- Add mixed floating point operators.
ALTER OPERATOR FAMILY gist_float8_ops USING GIST ADD
  OPERATOR 1 <  (float8, float4),
  OPERATOR 2 <= (float8, float4),
  OPERATOR 3 =  (float8, float4),
  OPERATOR 4 >= (float8, float4),
  OPERATOR 5 >  (float8, float4),

  OPERATOR 1 <  (float4, float8),
  OPERATOR 2 <= (float4, float8),
  OPERATOR 3 =  (float4, float8),
  OPERATOR 4 >= (float4, float8),
  OPERATOR 5 >  (float4, float8);

-- Add mixed date/time operators.
ALTER OPERATOR FAMILY gist_timestamptz_ops USING GIST ADD
  OPERATOR 1 <  (timestamptz, timestamp),
  OPERATOR 2 <= (timestamptz, timestamp),
  OPERATOR 3 =  (timestamptz, timestamp),
  OPERATOR 4 >= (timestamptz, timestamp),
  OPERATOR 5 >  (timestamptz, timestamp),

  OPERATOR 1 <  (timestamp, timestamptz),
  OPERATOR 2 <= (timestamp, timestamptz),
  OPERATOR 3 =  (timestamp, timestamptz),
  OPERATOR 4 >= (timestamp, timestamptz),
  OPERATOR 5 >  (timestamp, timestamptz),

  OPERATOR 1 <  (timestamptz, date),
  OPERATOR 2 <= (timestamptz, date),
  OPERATOR 3 =  (timestamptz, date),
  OPERATOR 4 >= (timestamptz, date),
  OPERATOR 5 >  (timestamptz, date),

  OPERATOR 1 <  (date, timestamptz),
  OPERATOR 2 <= (date, timestamptz),
  OPERATOR 3 =  (date, timestamptz),
  OPERATOR 4 >= (date, timestamptz),
  OPERATOR 5 >  (date, timestamptz);

ALTER OPERATOR FAMILY gist_timestamp_ops USING GIST ADD
  OPERATOR 1 <  (timestamp, date),
  OPERATOR 2 <= (timestamp, date),
  OPERATOR 3 =  (timestamp, date),
  OPERATOR 4 >= (timestamp, date),
  OPERATOR 5 >  (timestamp, date),

  OPERATOR 1 <  (date, timestamp),
  OPERATOR 2 <= (date, timestamp),
  OPERATOR 3 =  (date, timestamp),
  OPERATOR 4 >= (date, timestamp),
  OPERATOR 5 >  (date, timestamp);
