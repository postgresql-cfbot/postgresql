/* contrib/barrier/barrier--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION barrier" to load this file. \quit

CREATE FUNCTION emit_barrier(barrier_type text, count integer default 1)
RETURNS void
AS 'MODULE_PATHNAME', 'emit_barrier'
LANGUAGE C STRICT;

CREATE FUNCTION wait_barrier(barrier_type text)
RETURNS void
AS 'MODULE_PATHNAME', 'wait_barrier'
LANGUAGE C STRICT;
