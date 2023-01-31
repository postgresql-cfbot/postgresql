/* src/test/modules/test_extensions/test_ext_req_schema1--1.0.sql */
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext_req_schema1" to load this file. \quit

CREATE DOMAIN req AS text
  CONSTRAINT starts_with_1 check(pg_catalog.left(value,1) OPERATOR(pg_catalog.=) '1');
