/* src/test/modules/test_ddl_deparse_regress/test_ddl_deparse_regress--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ddl_deparse_regress" to load this file. \quit

CREATE FUNCTION deparse_drop_ddl(IN objidentity text,
    IN objecttype text)
  RETURNS text IMMUTABLE STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;