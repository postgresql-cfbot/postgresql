/* contrib/brin_bloom/brin_bloom--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION brin_bloom" to load this file. \quit

CREATE TYPE brin_bloom_summary;

-- BRIN bloom summary data type
CREATE FUNCTION brin_bloom_summary_in(cstring)
RETURNS brin_bloom_summary
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_bloom_summary_out(brin_bloom_summary)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_bloom_summary_recv(internal)
RETURNS brin_bloom_summary
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_bloom_summary_send(brin_bloom_summary)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE TYPE brin_bloom_summary (
	INTERNALLENGTH = -1,
	INPUT = brin_bloom_summary_in,
	OUTPUT = brin_bloom_summary_out,
	RECEIVE = brin_bloom_summary_recv,
	SEND = brin_bloom_summary_send,
	STORAGE = extended
);

-- BRIN support procedures
CREATE FUNCTION brin_bloom_opcinfo(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_bloom_add_value(internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_bloom_consistent(internal, internal, internal, int4)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_bloom_union(internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_bloom_options(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE OPERATOR FAMILY datetime_bloom_ops USING brin;

CREATE OPERATOR CLASS bpchar_bloom_ops
FOR TYPE bpchar USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashbpchar(bpchar),
STORAGE         bpchar;

CREATE OPERATOR CLASS bytea_bloom_ops
FOR TYPE bytea USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashvarlena(internal),
STORAGE         bytea;

CREATE OPERATOR CLASS char_bloom_ops
FOR TYPE "char" USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashchar("char"),
STORAGE         "char";

CREATE OPERATOR CLASS date_bloom_ops
FOR TYPE date USING brin FAMILY datetime_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashint4(int4),
STORAGE         date;

CREATE OPERATOR FAMILY float_bloom_ops USING brin;

CREATE OPERATOR CLASS float4_bloom_ops
FOR TYPE float4 USING brin FAMILY float_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashfloat4(float4),
STORAGE         float4;

CREATE OPERATOR CLASS float8_bloom_ops
FOR TYPE float8 USING brin FAMILY float_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashfloat8(float8),
STORAGE         float8;

CREATE OPERATOR FAMILY network_bloom_ops USING brin;

CREATE OPERATOR CLASS inet_bloom_ops
FOR TYPE inet USING brin FAMILY network_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashinet(inet),
STORAGE         inet;

CREATE OPERATOR FAMILY integer_bloom_ops USING brin;

CREATE OPERATOR CLASS int2_bloom_ops
FOR TYPE int2 USING brin FAMILY integer_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashint2(int2),
STORAGE         int2;

CREATE OPERATOR CLASS int4_bloom_ops
FOR TYPE int4 USING brin FAMILY integer_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashint4(int4),
STORAGE         int4;

CREATE OPERATOR CLASS int8_bloom_ops
FOR TYPE int8 USING brin FAMILY integer_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashint8(int8),
STORAGE         int8;

CREATE OPERATOR CLASS interval_bloom_ops
FOR TYPE interval USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      interval_hash(interval),
STORAGE         interval;

CREATE OPERATOR CLASS macaddr_bloom_ops
FOR TYPE macaddr USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashmacaddr(macaddr),
STORAGE         macaddr;

CREATE OPERATOR CLASS macaddr8_bloom_ops
FOR TYPE macaddr8 USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashmacaddr8(macaddr8),
STORAGE         macaddr8;

CREATE OPERATOR CLASS name_bloom_ops
FOR TYPE name USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashname(name),
STORAGE         name;

CREATE OPERATOR CLASS numeric_bloom_ops
FOR TYPE numeric USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hash_numeric(numeric),
STORAGE         numeric;

CREATE OPERATOR CLASS oid_bloom_ops
FOR TYPE oid USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashoid(oid),
STORAGE         oid;

CREATE OPERATOR CLASS pg_lsn_bloom_ops
FOR TYPE pg_lsn USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      pg_lsn_hash(pg_lsn),
STORAGE         pg_lsn;

CREATE OPERATOR CLASS text_bloom_ops
FOR TYPE text USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashtext(text),
STORAGE         text;

CREATE OPERATOR CLASS tid_bloom_ops
FOR TYPE tid USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      hashtid(tid),
STORAGE         tid;

CREATE OPERATOR CLASS time_bloom_ops
FOR TYPE time USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      time_hash(time),
STORAGE         time;

CREATE OPERATOR CLASS timestamp_bloom_ops
FOR TYPE timestamp USING brin FAMILY datetime_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      timestamp_hash(timestamp),
STORAGE         timestamp;

CREATE OPERATOR CLASS timestamptz_bloom_ops
FOR TYPE timestamptz USING brin FAMILY datetime_bloom_ops
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      timestamp_hash(timestamp),
STORAGE         timestamptz;

CREATE OPERATOR CLASS timetz_bloom_ops
FOR TYPE timetz USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      timetz_hash(timetz),
STORAGE         timetz;

CREATE OPERATOR CLASS uuid_bloom_ops
FOR TYPE uuid USING brin
AS
    OPERATOR        1       =,
    FUNCTION        1       brin_bloom_opcinfo(internal),
    FUNCTION        2       brin_bloom_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_bloom_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_bloom_union(internal, internal, internal),
    FUNCTION        5       brin_bloom_options(internal),
    FUNCTION        11      uuid_hash(uuid),
STORAGE         uuid;
