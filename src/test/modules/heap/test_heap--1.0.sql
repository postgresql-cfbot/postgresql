/* src/test/modules/heap/test_heap--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_heap" to load this file. \quit

CREATE FUNCTION set_next_xid(xid)
    RETURNS void
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;
