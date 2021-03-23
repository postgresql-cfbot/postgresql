/* contrib/brin_minmax_multi/brin_minmax_multi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION brin_minmax_multi" to load this file. \quit

CREATE TYPE brin_minmax_multi_summary;

-- BRIN bloom summary data type
CREATE FUNCTION brin_minmax_multi_summary_in(cstring)
RETURNS brin_minmax_multi_summary
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_minmax_multi_summary_out(brin_minmax_multi_summary)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_minmax_multi_summary_recv(internal)
RETURNS brin_minmax_multi_summary
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION brin_minmax_multi_summary_send(brin_minmax_multi_summary)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE TYPE brin_minmax_multi_summary (
	INTERNALLENGTH = -1,
	INPUT = brin_minmax_multi_summary_in,
	OUTPUT = brin_minmax_multi_summary_out,
	RECEIVE = brin_minmax_multi_summary_recv,
	SEND = brin_minmax_multi_summary_send,
	STORAGE = extended
);

-- BRIN support procedures
CREATE FUNCTION brin_minmax_multi_opcinfo(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_add_value(internal, internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_consistent(internal, internal, internal, int4)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_union(internal, internal, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_options(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

-- distance functions

CREATE FUNCTION brin_minmax_multi_distance_int2(int2, int2)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_int4(int4, int4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_int8(int8, int8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_float4(float4, float4)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_float8(float8, float8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_numeric(numeric, numeric)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_tid(tid, tid)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_uuid(uuid, uuid)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_date(date, date)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_time(time, time)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_interval(interval, interval)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_timetz(timetz, timetz)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_pg_lsn(pg_lsn, pg_lsn)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_macaddr(macaddr, macaddr)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_macaddr8(macaddr8, macaddr8)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_inet(inet, inet)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION brin_minmax_multi_distance_timestamp(timestamp, timestamp)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE;

CREATE OPERATOR FAMILY datetime_minmax_multi_ops USING brin;

CREATE OPERATOR CLASS date_minmax_multi_ops
FOR TYPE date USING brin FAMILY datetime_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_date(date,date),
STORAGE         date;

CREATE OPERATOR FAMILY float_minmax_multi_ops USING brin;

CREATE OPERATOR CLASS float4_minmax_multi_ops
FOR TYPE float4 USING brin FAMILY float_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_float4(float4,float4),
STORAGE         float4;

CREATE OPERATOR CLASS float8_minmax_multi_ops
FOR TYPE float8 USING brin FAMILY float_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_float8(float8,float8),
STORAGE         float8;

ALTER OPERATOR FAMILY float_minmax_multi_ops USING brin ADD

    OPERATOR        1       <(float4,float8),
    OPERATOR        2       <=(float4,float8),
    OPERATOR        3       =(float4,float8),
    OPERATOR        4       >=(float4,float8),
    OPERATOR        5       >(float4,float8),

    OPERATOR        1       <(float8,float4),
    OPERATOR        2       <=(float8,float4),
    OPERATOR        3       =(float8,float4),
    OPERATOR        4       >=(float8,float4),
    OPERATOR        5       >(float8,float4);

CREATE OPERATOR FAMILY network_minmax_multi_ops USING brin;

CREATE OPERATOR CLASS inet_minmax_multi_ops
FOR TYPE inet USING brin FAMILY network_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_inet(inet,inet),
STORAGE         inet;

CREATE OPERATOR FAMILY integer_minmax_multi_ops USING brin;

CREATE OPERATOR CLASS int2_minmax_multi_ops
FOR TYPE int2 USING brin FAMILY integer_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_int2(int2,int2),
STORAGE         int2;

CREATE OPERATOR CLASS int4_minmax_multi_ops
FOR TYPE int4 USING brin FAMILY integer_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_int4(int4,int4),
STORAGE         int4;

CREATE OPERATOR CLASS int8_minmax_multi_ops
FOR TYPE int8 USING brin FAMILY integer_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_int8(int8,int8),
STORAGE         int8;

ALTER OPERATOR FAMILY integer_minmax_multi_ops USING brin ADD

    OPERATOR        1       <(int2,int4),
    OPERATOR        2       <=(int2,int4),
    OPERATOR        3       =(int2,int4),
    OPERATOR        4       >=(int2,int4),
    OPERATOR        5       >(int2,int4),

    OPERATOR        1       <(int2,int8),
    OPERATOR        2       <=(int2,int8),
    OPERATOR        3       =(int2,int8),
    OPERATOR        4       >=(int2,int8),
    OPERATOR        5       >(int2,int8),

    OPERATOR        1       <(int4,int2),
    OPERATOR        2       <=(int4,int2),
    OPERATOR        3       =(int4,int2),
    OPERATOR        4       >=(int4,int2),
    OPERATOR        5       >(int4,int2),

    OPERATOR        1       <(int4,int8),
    OPERATOR        2       <=(int4,int8),
    OPERATOR        3       =(int4,int8),
    OPERATOR        4       >=(int4,int8),
    OPERATOR        5       >(int4,int8),

    OPERATOR        1       <(int8,int2),
    OPERATOR        2       <=(int8,int2),
    OPERATOR        3       =(int8,int2),
    OPERATOR        4       >=(int8,int2),
    OPERATOR        5       >(int8,int2),

    OPERATOR        1       <(int8,int4),
    OPERATOR        2       <=(int8,int4),
    OPERATOR        3       =(int8,int4),
    OPERATOR        4       >=(int8,int4),
    OPERATOR        5       >(int8,int4);

CREATE OPERATOR CLASS interval_minmax_multi_ops
FOR TYPE interval USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_interval(interval,interval),
STORAGE         interval;

CREATE OPERATOR CLASS macaddr_minmax_multi_ops
FOR TYPE macaddr USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_macaddr(macaddr,macaddr),
STORAGE         macaddr;

CREATE OPERATOR CLASS macaddr8_minmax_multi_ops
FOR TYPE macaddr8 USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_macaddr8(macaddr8,macaddr8),
STORAGE         macaddr8;

CREATE OPERATOR CLASS numeric_minmax_multi_ops
FOR TYPE numeric USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_numeric(numeric,numeric),
STORAGE         numeric;

CREATE OPERATOR CLASS oid_minmax_multi_ops
FOR TYPE oid USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_int4(int4,int4),
STORAGE         oid;

CREATE OPERATOR CLASS pg_lsn_minmax_multi_ops
FOR TYPE pg_lsn USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_pg_lsn(pg_lsn,pg_lsn),
STORAGE         pg_lsn;

CREATE OPERATOR CLASS tid_minmax_multi_ops
FOR TYPE tid USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_tid(tid,tid),
STORAGE         tid;

CREATE OPERATOR CLASS time_minmax_multi_ops
FOR TYPE time USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_time(time,time),
STORAGE         time;

CREATE OPERATOR CLASS timestamp_minmax_multi_ops
FOR TYPE timestamp USING brin FAMILY datetime_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,

    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_timestamp(timestamp,timestamp),
STORAGE         timestamp;

CREATE OPERATOR CLASS timestamptz_minmax_multi_ops
FOR TYPE timestamptz USING brin FAMILY datetime_minmax_multi_ops
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_timestamp(timestamp,timestamp),
STORAGE         timestamptz;

ALTER OPERATOR FAMILY datetime_minmax_multi_ops USING brin ADD

    OPERATOR        1       <(timestamp,timestamptz),
    OPERATOR        2       <=(timestamp,timestamptz),
    OPERATOR        3       =(timestamp,timestamptz),
    OPERATOR        4       >=(timestamp,timestamptz),
    OPERATOR        5       >(timestamp,timestamptz),

    OPERATOR        1       <(timestamp,date),
    OPERATOR        2       <=(timestamp,date),
    OPERATOR        3       =(timestamp,date),
    OPERATOR        4       >=(timestamp,date),
    OPERATOR        5       >(timestamp,date),

    OPERATOR        1       <(date,timestamp),
    OPERATOR        2       <=(date,timestamp),
    OPERATOR        3       =(date,timestamp),
    OPERATOR        4       >=(date,timestamp),
    OPERATOR        5       >(date,timestamp),

    OPERATOR        1       <(date,timestamptz),
    OPERATOR        2       <=(date,timestamptz),
    OPERATOR        3       =(date,timestamptz),
    OPERATOR        4       >=(date,timestamptz),
    OPERATOR        5       >(date,timestamptz),

    OPERATOR        1       <(timestamptz,timestamp),
    OPERATOR        2       <=(timestamptz,timestamp),
    OPERATOR        3       =(timestamptz,timestamp),
    OPERATOR        4       >=(timestamptz,timestamp),
    OPERATOR        5       >(timestamptz,timestamp),

    OPERATOR        1       <(timestamptz,date),
    OPERATOR        2       <=(timestamptz,date),
    OPERATOR        3       =(timestamptz,date),
    OPERATOR        4       >=(timestamptz,date),
    OPERATOR        5       >(timestamptz,date);

CREATE OPERATOR CLASS timetz_minmax_multi_ops
FOR TYPE timetz USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_timetz(timetz,timetz),
STORAGE         timetz;

CREATE OPERATOR CLASS uuid_minmax_multi_ops
FOR TYPE uuid USING brin
AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       brin_minmax_multi_opcinfo(internal),
    FUNCTION        2       brin_minmax_multi_add_value(internal, internal, internal, internal),
    FUNCTION        3       brin_minmax_multi_consistent(internal, internal, internal, int4),
    FUNCTION        4       brin_minmax_multi_union(internal, internal, internal),
    FUNCTION        5       brin_minmax_multi_options(internal),
    FUNCTION       11       brin_minmax_multi_distance_uuid(uuid,uuid),
STORAGE         uuid;
