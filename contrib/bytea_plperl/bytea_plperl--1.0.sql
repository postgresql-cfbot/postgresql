/* contrib/bytea_plperl/bytea_plperl--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bytea_plperl" to load this file. \quit

CREATE FUNCTION bytea_to_plperl(val internal) RETURNS internal
LANGUAGE C STRICT IMMUTABLE
AS 'MODULE_PATHNAME';

CREATE FUNCTION plperl_to_bytea(val internal) RETURNS bytea
LANGUAGE C STRICT IMMUTABLE
AS 'MODULE_PATHNAME';

CREATE TRANSFORM FOR bytea LANGUAGE plperl (
    FROM SQL WITH FUNCTION bytea_to_plperl(internal),
    TO SQL WITH FUNCTION plperl_to_bytea(internal)
);

COMMENT ON TRANSFORM FOR bytea LANGUAGE plperl IS 'transform between bytea and Perl';
