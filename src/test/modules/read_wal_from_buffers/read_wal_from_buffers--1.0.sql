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
