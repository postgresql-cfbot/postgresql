/* contrib/amcheck/amcheck--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.3'" to load this file. \quit

--
-- gist_index_parent_check()
--
CREATE FUNCTION gist_index_parent_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'gist_index_parent_check'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gist_index_parent_check(regclass) FROM PUBLIC;
