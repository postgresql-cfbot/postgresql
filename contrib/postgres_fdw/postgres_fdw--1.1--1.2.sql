/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

DROP FUNCTION postgres_fdw_get_connections ();

CREATE FUNCTION postgres_fdw_get_connections (OUT server_name text,
    OUT user_name text, OUT valid boolean)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_verify_connection (IN server_name text,
        IN user_name text)
RETURNS bool
AS 'MODULE_PATHNAME', 'postgres_fdw_verify_connection_server_user'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_verify_connection (IN server_name text)
RETURNS bool
AS 'MODULE_PATHNAME', 'postgres_fdw_verify_connection_server'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_verify_connection_all ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL RESTRICTED;

CREATE FUNCTION postgres_fdw_can_verify_connection ()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL SAFE;
