/* src/test/modules/read_wal_from_buffers/read_wal_from_buffers--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION read_wal_from_buffers" to load this file. \quit

--
-- read_wal_from_buffers()
--
-- SQL function to read WAL from WAL buffers. Returns number of bytes read.
--
CREATE FUNCTION read_wal_from_buffers(IN lsn pg_lsn, IN bytes_to_read int,
    bytes_read OUT int)
AS 'MODULE_PATHNAME', 'read_wal_from_buffers'
LANGUAGE C STRICT;

--
-- get_wal_records_info_from_buffers()
--
-- SQL function to get info of WAL records available in WAL buffers.
--
CREATE FUNCTION get_wal_records_info_from_buffers(IN start_lsn pg_lsn,
    IN end_lsn pg_lsn,
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
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'get_wal_records_info_from_buffers'
LANGUAGE C STRICT PARALLEL SAFE;
