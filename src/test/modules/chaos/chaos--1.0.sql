/* src/test/modules/chaos/chaos--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION chaos" to load this file. \quit

CREATE FUNCTION clobber_next_oid(size BIGINT)
	RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;
