/* contrib/pageinspect/pageinspect--1.10--1.11.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.11'" to load this file. \quit

--
-- get_compress_address_header()
--
CREATE FUNCTION get_compress_address_header(IN relname text, IN segno integer,
    OUT nblocks integer,
    OUT allocated_chunks integer,
    OUT chunk_size integer,
    OUT algorithm integer,
    OUT last_synced_nblocks integer,
    OUT last_synced_allocated_chunks integer)
RETURNS record
AS 'MODULE_PATHNAME', 'get_compress_address_header'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- get_compress_address_items()
--
CREATE FUNCTION get_compress_address_items(IN relname text, IN segno integer,
    OUT blkno integer,
    OUT nchunks integer,
    OUT allocated_chunks integer,
    OUT chunknos integer[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'get_compress_address_items'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- page_compress()
--
CREATE FUNCTION page_compress(
    page bytea,
    algorithm text,
    level integer)
RETURNS bytea
AS 'MODULE_PATHNAME', 'page_compress'
LANGUAGE C STRICT PARALLEL SAFE;
