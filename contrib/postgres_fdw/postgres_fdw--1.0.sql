/* contrib/postgres_fdw/postgres_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION postgres_fdw" to load this file. \quit

CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION postgres_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER postgres_fdw
  HANDLER postgres_fdw_handler
  VALIDATOR postgres_fdw_validator;

CREATE FUNCTION postgres_fdw_get_connections ()
RETURNS text[]
AS 'MODULE_PATHNAME','postgres_fdw_get_connections'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_disconnect ()
RETURNS bool
AS 'MODULE_PATHNAME','postgres_fdw_disconnect'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_disconnect (text)
RETURNS bool
AS 'MODULE_PATHNAME','postgres_fdw_disconnect'
LANGUAGE C STRICT PARALLEL RESTRICTED;
