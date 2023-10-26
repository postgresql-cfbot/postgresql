/* src/test/modules/test_memtrack/test_memtrack--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_memtrack" to load this file. \quit

CREATE FUNCTION test_memtrack(
                       num_workers pg_catalog.int4 default 1,
                       type   pg_catalog.int4 default 1,
					   num_blocks pg_catalog.int4 default 1,
					   block_size pg_catalog.int4 default 1024)
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_allocation(
    num_workers pg_catalog.int4 default 1,
    type   pg_catalog.int4 default 1,
    num_blocks pg_catalog.int4 default 1,
    block_size pg_catalog.int4 default 1024)
    RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
