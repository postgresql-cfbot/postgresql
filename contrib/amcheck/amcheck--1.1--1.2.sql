/* amcheck--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.2'" to load this file. \quit

--
-- gist_index_check()
--
CREATE FUNCTION gist_index_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'gist_index_check'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gist_index_check(regclass) FROM PUBLIC;
