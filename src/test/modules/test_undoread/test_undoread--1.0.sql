/* src/test/modules/test_undoread/test_undoread--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_undoread" to load this file. \quit

CREATE FUNCTION test_undoread_create()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_undoread_close()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_undoread_insert(text)
RETURNS pg_catalog.text STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_undoread_read(text, text, bool)
RETURNS SETOF pg_catalog.text STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_undoread_cache_empty_log()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
