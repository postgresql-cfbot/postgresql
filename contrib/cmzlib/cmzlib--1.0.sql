/* contrib/cm_lz4/cmzlib--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION cmzlib" to load this file. \quit

CREATE FUNCTION zlibhandler(internal)
RETURNS compression_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Compression method
CREATE ACCESS METHOD zlib TYPE COMPRESSION HANDLER zlibhandler;
COMMENT ON ACCESS METHOD zlib IS 'zlib compression method';
