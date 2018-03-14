/* src/test/modules/test_subscription/test_subscription--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_subscription" to load this file. \quit

CREATE TYPE dummytext;

CREATE FUNCTION dummytext_in(cstring)
RETURNS dummytext
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION dummytext_out(dummytext)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE dummytext (
       INTERNALLENGTH = -1,
       INPUT = dummytext_in,
       OUTPUT = dummytext_out
);
