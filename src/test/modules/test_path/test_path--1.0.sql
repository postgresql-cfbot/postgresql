/* src/test/modules/test_path/test_path--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_path" to load this file. \quit

CREATE FUNCTION test_canonicalize_path(text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_platform()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;