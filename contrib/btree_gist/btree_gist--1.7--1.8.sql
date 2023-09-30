/* contrib/btree_gist/btree_gist--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.8'" to load this file. \quit

CREATE FUNCTION gbt_bit_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_bool_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_cash_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_enum_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_inet_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_intv_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_time_sortsupport(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

ALTER OPERATOR FAMILY gist_bit_ops USING gist ADD
    FUNCTION    11  (bit, bit) gbt_bit_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_bool_ops USING gist ADD
    FUNCTION    11  (bool, bool) gbt_bool_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_bytea_ops USING gist ADD
    FUNCTION    11  (bytea, bytea) bytea_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_cash_ops USING gist ADD
    FUNCTION    11  (money, money) gbt_cash_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_date_ops USING gist ADD
    FUNCTION    11  (date, date) date_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_enum_ops USING gist ADD
    FUNCTION    11  (anyenum, anyenum) gbt_enum_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_float4_ops USING gist ADD
    FUNCTION    11  (float4, float4) btfloat4sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_float8_ops USING gist ADD
    FUNCTION    11  (float8, float8) btfloat8sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_inet_ops USING gist ADD
    FUNCTION    11  (inet, inet) gbt_inet_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_int2_ops USING gist ADD
    FUNCTION    11  (int2, int2) btint2sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_int4_ops USING gist ADD
    FUNCTION    11  (int4, int4) btint4sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_int8_ops USING gist ADD
    FUNCTION    11  (int8, int8) btint8sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_interval_ops USING gist ADD
    FUNCTION    11  (interval, interval) gbt_intv_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_macaddr_ops USING gist ADD
    FUNCTION    11  (macaddr, macaddr) macaddr_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_macaddr8_ops USING gist ADD
    FUNCTION    11  (macaddr8, macaddr8) gbt_macad8_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_numeric_ops USING gist ADD
    FUNCTION    11  (numeric, numeric) numeric_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_oid_ops USING gist ADD
    FUNCTION    11  (oid, oid) btoidsortsupport (internal) ;

ALTER OPERATOR FAMILY gist_text_ops USING gist ADD
    FUNCTION    11  (text, text) bttextsortsupport (internal) ;

ALTER OPERATOR FAMILY gist_bpchar_ops USING gist ADD
    FUNCTION    11  (bpchar, bpchar) bpchar_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_time_ops USING gist ADD
    FUNCTION    11  (time, time) gbt_time_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_timestamp_ops USING gist ADD
    FUNCTION    11  (timestamp, timestamp) timestamp_sortsupport (internal) ;

ALTER OPERATOR FAMILY gist_uuid_ops USING gist ADD
    FUNCTION    11  (uuid, uuid) uuid_sortsupport (internal) ;
