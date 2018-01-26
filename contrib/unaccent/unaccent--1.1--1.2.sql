/* contrib/unaccent/unaccent--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION unaccent UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION unaccent_init(internal,internal)
	RETURNS internal
	AS 'MODULE_PATHNAME', 'unaccent_init'
	LANGUAGE C PARALLEL SAFE;
