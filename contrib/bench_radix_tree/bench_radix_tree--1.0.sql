/* contrib/bench_radix_tree/bench_radix_tree--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bench_radix_tree" to load this file. \quit

create function bench_shuffle_search(
minblk int4,
maxblk int4,
random_block bool DEFAULT false,
OUT nkeys int8,
OUT rt_mem_allocated int8,
OUT array_mem_allocated int8,
OUT rt_load_ms int8,
OUT array_load_ms int8,
OUT rt_search_ms int8,
OUT array_serach_ms int8
)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

create function bench_seq_search(
minblk int4,
maxblk int4,
random_block bool DEFAULT false,
OUT nkeys int8,
OUT rt_mem_allocated int8,
OUT array_mem_allocated int8,
OUT rt_load_ms int8,
OUT array_load_ms int8,
OUT rt_search_ms int8,
OUT array_serach_ms int8
)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

create function bench_load_random_int(
cnt int8,
OUT mem_allocated int8,
OUT load_ms int8)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

create function bench_search_random_nodes(
cnt int8,
filter_str text DEFAULT NULL,
OUT mem_allocated int8,
OUT load_ms int8,
OUT search_ms int8)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

create function bench_fixed_height_search(
fanout int4,
OUT fanout int4,
OUT nkeys int8,
OUT rt_mem_allocated int8,
OUT rt_load_ms int8,
OUT rt_search_ms int8
)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

create function bench_node128_load(
fanout int4,
OUT fanout int4,
OUT nkeys int8,
OUT rt_mem_allocated int8,
OUT rt_sparseload_ms int8
)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

create function bench_tidstore_load(
minblk int4,
maxblk int4,
OUT mem_allocated int8,
OUT load_ms int8,
OUT iter_ms int8
)
returns record
as 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
