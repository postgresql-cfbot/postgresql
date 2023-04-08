/* src/test/modules/test_wal_read_from_buffers/test_wal_read_from_buffers--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_wal_read_from_buffers" to load this file. \quit

--
-- test_wal_read_from_buffers()
--
-- Returns true if WAL data at a given LSN can be read from WAL buffers.
-- Otherwise returns false.
--
CREATE FUNCTION test_wal_read_from_buffers(IN lsn pg_lsn,
    OUT read_from_buffers bool
)
AS 'MODULE_PATHNAME', 'test_wal_read_from_buffers'
LANGUAGE C STRICT PARALLEL UNSAFE;
