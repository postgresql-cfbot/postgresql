/* contrib/pg_copy_json/copy_json--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_copy_json" to load this file. \quit

CREATE FUNCTION pg_catalog.json(internal)
	RETURNS copy_handler
	AS 'MODULE_PATHNAME', 'copy_json'
	LANGUAGE C;
