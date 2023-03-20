/* src/test/modules/test_extensions/test_ext_req_schema2--1.0.sql */
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext_req_schema2" to load this file. \quit
CREATE DOMAIN req2 AS text
  CONSTRAINT starts_with_2 check(pg_catalog.left(value,1) OPERATOR(pg_catalog.=) '2');

CREATE FUNCTION dep_req2() RETURNS @extschema:test_ext_req_schema1@.req
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT ''1032w''::@extschema:test_ext_req_schema1@.req';

CREATE FUNCTION dep_req3() RETURNS @extschema:test_ext_req_schema2@.dreq
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS 'SELECT ''2032w''::@extschema:test_ext_req_schema2@.dreq';
