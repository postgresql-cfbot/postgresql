/* src/test/modules/test_extensions/test_ext_wildcard1--%--2.0.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION test_ext_wildcard1 UPDATE TO '2.0'" to load this file. \quit

CREATE OR REPLACE FUNCTION ext_wildcard1_version() returns TEXT
AS 'SELECT 2.0' LANGUAGE 'sql';
