/* contrib/pageinspect/pageinspect--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.8'" to load this file. \quit

-- decode infomask flags as human readable flag names
CREATE FUNCTION heap_infomask_flags(
       infomask integer,
       infomask2 integer,
       decode_combined boolean DEFAULT false)
RETURNS text[]
AS 'MODULE_PATHNAME', 'heap_infomask_flags'
LANGUAGE C STRICT PARALLEL SAFE;
