/* contrib/catcachebench/catcachebench--0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION catcachebench" to load this file. \quit

CREATE FUNCTION catcachebench(IN type int)
RETURNS double precision
AS 'MODULE_PATHNAME', 'catcachebench'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION catcachereadstats(OUT catid int, OUT reloid oid, OUT searches bigint, OUT hits bigint, OUT neg_hits bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'catcachereadstats'
LANGUAGE C STRICT VOLATILE;
