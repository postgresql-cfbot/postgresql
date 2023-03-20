/* src/test/modules/test_extensions/test_ext_req_schema2--1.0.sql */
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext_req_schema2" to load this file. \quit

CREATE OR REPLACE FUNCTION dep_req() RETURNS @extschema:test_ext_req_schema1@.req
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT ''1update''::@extschema:test_ext_req_schema1@.req';
