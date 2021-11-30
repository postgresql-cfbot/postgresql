/* src/test/modules/test_dbgapi/test_dbgapi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_dbgapi" to load this file. \quit

CREATE FUNCTION trace_plpgsql(boolean) RETURNS void
  AS 'MODULE_PATHNAME' LANGUAGE C;
