/* contrib/amcheck/amcheck--1.4--1.5.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.5'" to load this file. \quit


-- gist_index_check()
--
CREATE FUNCTION gist_index_check(index regclass, heapallindexed boolean)
RETURNS VOID
AS 'MODULE_PATHNAME', 'gist_index_check'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gist_index_check(regclass, boolean) FROM PUBLIC;

-- gin_index_parent_check()
-- XXX why is this not called simply gin_index_check?
CREATE FUNCTION gin_index_parent_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'gin_index_parent_check'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION gin_index_parent_check(regclass) FROM PUBLIC;
