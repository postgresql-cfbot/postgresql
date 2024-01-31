\echo Use "CREATE EXTENSION test_misc" to load this file. \quit

CREATE FUNCTION test_bitset()
       RETURNS pg_catalog.void
       AS 'MODULE_PATHNAME' LANGUAGE C;
