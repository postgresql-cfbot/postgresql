/* src/test/modules/test_tidstore/test_tidstore--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_tidstore" to load this file. \quit

CREATE FUNCTION test_tidstore()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
