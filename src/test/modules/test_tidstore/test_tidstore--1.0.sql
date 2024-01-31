/* src/test/modules/test_tidstore/test_tidstore--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_tidstore" to load this file. \quit

CREATE FUNCTION tidstore_create(
shared bool)
RETURNS void STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tidstore_set_block_offsets(
blkno bigint,
offsets int2[])
RETURNS void STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tidstore_dump_tids(
t_ctid OUT tid)
RETURNS SETOF tid STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tidstore_lookup_tids(
t_ctids tid[],
t_ctid OUT tid,
found OUT bool)
RETURNS SETOF record STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tidstore_is_full()
RETURNS bool STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION tidstore_destroy()
RETURNS void STRICT PARALLEL UNSAFE
AS 'MODULE_PATHNAME' LANGUAGE C;
