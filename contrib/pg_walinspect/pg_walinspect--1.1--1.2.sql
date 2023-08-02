/* contrib/pg_walinspect/pg_walinspect--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_walinspect UPDATE TO '1.2'" to load this file. \quit

-- The function is now in the backend and callers should update to use those.

ALTER EXTENSION pg_walinspect DROP FUNCTION pg_get_wal_record_info;
DROP FUNCTION pg_get_wal_record_info(pg_lsn);

--
-- pg_get_wal_record_info()
--
CREATE FUNCTION pg_get_wal_record_info(IN in_lsn pg_lsn,
    OUT start_lsn pg_lsn,
    OUT end_lsn pg_lsn,
    OUT prev_lsn pg_lsn,
    OUT xid xid,
    OUT resource_manager text,
    OUT record_type text,
    OUT record_length int4,
    OUT main_data_length int4,
    OUT fpi_length int4,
    OUT description text,
    OUT block_ref text
)
AS 'pg_get_wal_record_content'
LANGUAGE INTERNAL STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pg_get_wal_record_info(pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_get_wal_record_info(pg_lsn) TO pg_read_server_files;
