/* contrib/postgres_fdw/postgres_fdw--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION postgres_fdw UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION postgres_fdw_connection(oid, oid)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

ALTER FOREIGN DATA WRAPPER postgres_fdw CONNECTION postgres_fdw_connection;
