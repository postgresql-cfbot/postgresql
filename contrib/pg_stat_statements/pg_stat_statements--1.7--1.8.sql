/* contrib/pg_stat_statements/pg_stat_statements--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_stat_statements UPDATE TO '1.8'" to load this file. \quit

CREATE FUNCTION pg_stat_statements_reset_computed_values()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_statements_reset_computed_values() FROM PUBLIC;
