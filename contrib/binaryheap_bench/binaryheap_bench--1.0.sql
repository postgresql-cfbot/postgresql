/* contrib/binaryheap_bench/binaryheap_bench--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION binaryheap_bench" to load this file. \quit

CREATE FUNCTION bench_load(
indexed bool,
cnt int8,
OUT cnt int8,
OUT load_ms int8,
OUT xx_load_ms int8,
OUT old_load_ms int8)
RETURNS record
AS 'MODULE_PATHNAME', 'bench_load'
LANGUAGE C STRICT;

CREATE FUNCTION bench_sift_down(
cnt int8,
OUT cnt int8,
OUT sift_ms int8,
OUT xx_sift_ms int8)
RETURNS record
AS 'MODULE_PATHNAME', 'bench_sift_down'
LANGUAGE C STRICT;
