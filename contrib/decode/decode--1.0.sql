/* contrib/decode/decode--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION decode" to load this file. \quit

--
--  PostgreSQL code for decode.
--

--
-- Parser support function - allow to specify returning type when
-- system with polymorphic variables is possible to use.
--
CREATE FUNCTION decode_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--
-- decode function - example of function that returns "any" type
--
CREATE FUNCTION decode(variadic "any")
RETURNS "any"
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE SUPPORT decode_support;

