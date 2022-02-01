/* contrib/pg_walinspect/pg_walinspect--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_walinspect" to load this file. \quit

--
-- pg_get_raw_wal_record()
--
CREATE FUNCTION pg_get_raw_wal_record(IN in_lsn pg_lsn,
    OUT lsn pg_lsn,
    OUT record bytea
)
AS 'MODULE_PATHNAME', 'pg_get_raw_wal_record'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_raw_wal_record(pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_raw_wal_record(pg_lsn) TO pg_monitor;
--
-- pg_get_first_valid_wal_record_lsn()
--
CREATE FUNCTION pg_get_first_valid_wal_record_lsn(IN in_lsn pg_lsn,
    OUT lsn pg_lsn
)
AS 'MODULE_PATHNAME', 'pg_get_first_valid_wal_record_lsn'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_first_valid_wal_record_lsn(pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_first_valid_wal_record_lsn(pg_lsn) TO pg_monitor;

--
-- pg_verify_raw_wal_record()
--
CREATE FUNCTION pg_verify_raw_wal_record(IN record bytea,
    OUT is_valid boolean
)
AS 'MODULE_PATHNAME', 'pg_verify_raw_wal_record'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_verify_raw_wal_record(bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_verify_raw_wal_record(bytea) TO pg_monitor;

--
-- pg_get_wal_record_info()
--
CREATE FUNCTION pg_get_wal_record_info(IN in_lsn pg_lsn,
    OUT lsn pg_lsn,
    OUT prev_lsn pg_lsn,
    OUT xid xid,
    OUT rmgr text,
    OUT length int4,
    OUT total_length int4,
	OUT description text,
    OUT block_ref text,
    OUT data bytea,
    OUT data_len int4
)
AS 'MODULE_PATHNAME', 'pg_get_wal_record_info'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_wal_record_info(pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_wal_record_info(pg_lsn) TO pg_monitor;

--
-- pg_get_wal_record_info_2()
--
CREATE FUNCTION pg_get_wal_record_info_2(IN start_lsn pg_lsn,
    IN end_lsn pg_lsn,
    OUT lsn pg_lsn,
    OUT prev_lsn pg_lsn,
    OUT xid xid,
    OUT rmgr text,
    OUT length int4,
    OUT total_length int4,
	OUT description text,
    OUT block_ref text,
    OUT data bytea,
    OUT data_len int4
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_get_wal_record_info_2'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_wal_record_info_2(pg_lsn, pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_wal_record_info_2(pg_lsn, pg_lsn) TO pg_monitor;

--
-- pg_get_wal_stats()
--
CREATE FUNCTION pg_get_wal_stats(IN start_lsn pg_lsn,
    IN end_lsn pg_lsn,
    OUT rmgr text,
    OUT count int8,
    OUT count_per float4,
    OUT record_size int8,
    OUT record_size_per float4,
    OUT fpi_size int8,
    OUT fpi_size_per float4,
    OUT combined_size int8,
    OUT combined_size_per float4
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_get_wal_stats'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_wal_stats(pg_lsn, pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_wal_stats(pg_lsn, pg_lsn) TO pg_monitor;
