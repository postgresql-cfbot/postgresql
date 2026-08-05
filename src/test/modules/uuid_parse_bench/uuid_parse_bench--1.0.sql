/* src/test/modules/uuid_parse_bench/uuid_parse_bench--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION uuid_parse_bench" to load this file. \quit

--
-- Time each parsing strategy against each input shape.  Passing NULL for
-- paths or shapes selects all of them.
--
-- Valid paths:  scalar, simd, nosimd
-- Valid shapes: canonical, bare32, braced_canonical, braced_bare32,
--               dashed4, invalid_hex
--
CREATE FUNCTION uuid_parse_bench(nuuids int DEFAULT 100000,
								 nloops int DEFAULT 5,
								 paths text[] DEFAULT NULL,
								 shapes text[] DEFAULT NULL,
								 OUT path text,
								 OUT shape text,
								 OUT n_inputs int,
								 OUT best_ms float8,
								 OUT ns_per_parse float8)
	RETURNS SETOF record
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE;
