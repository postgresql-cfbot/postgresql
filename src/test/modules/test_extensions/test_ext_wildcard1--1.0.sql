/* src/test/modules/test_extensions/test_ext_wildcard1--1.0.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION test_ext_wildcard1" to load this file. \quit

CREATE FUNCTION ext_wildcard1_version() returns TEXT
AS 'SELECT 1.0' LANGUAGE 'sql';
