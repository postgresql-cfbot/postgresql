/* contrib/bytea_plperl/bytea_plperlu--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bytea_plperlu" to load this file. \quit

CREATE FUNCTION bytea_to_plperlu(val internal) RETURNS internal
LANGUAGE C STRICT IMMUTABLE
AS 'MODULE_PATHNAME', 'bytea_to_plperl';

CREATE FUNCTION plperlu_to_bytea(val internal) RETURNS bytea
LANGUAGE C STRICT IMMUTABLE
AS 'MODULE_PATHNAME', 'plperl_to_bytea';

CREATE TRANSFORM FOR bytea LANGUAGE plperlu (
    FROM SQL WITH FUNCTION bytea_to_plperlu(internal),
    TO SQL WITH FUNCTION plperlu_to_bytea(internal)
);

COMMENT ON TRANSFORM FOR bytea LANGUAGE plperlu IS 'transform between bytea and Perl';
