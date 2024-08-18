/* contrib/pg_freespacemap/pg_freespacemap--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_freespacemap UPDATE TO '1.3'" to load this file. \quit

CREATE FUNCTION pg_truncate_freespace_map(regclass)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_truncate_freespace_map'
LANGUAGE C STRICT
PARALLEL UNSAFE;

REVOKE ALL ON FUNCTION pg_truncate_freespace_map(regclass) FROM PUBLIC;
