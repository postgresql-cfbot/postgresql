/* contrib/dict_int/dict_int--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dict_int UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION dintdict_init(internal, internal)
        RETURNS internal
        AS 'MODULE_PATHNAME'
        LANGUAGE C STRICT;
