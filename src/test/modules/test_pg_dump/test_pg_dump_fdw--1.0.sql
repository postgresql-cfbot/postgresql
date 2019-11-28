\echo Use "CREATE EXTENSION test_pg_dump_fdw" to load this file. \quit

CREATE FUNCTION test_pg_dump_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER test_pg_dump_fdw
HANDLER test_pg_dump_fdw_handler;
