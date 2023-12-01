/* src/test/modules/test_injection_points/test_injection_points--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_injection_points" to load this file. \quit

--
-- test_injection_points_attach()
--
-- Attaches an injection point using callbacks from one of the predefined
-- modes.
--
CREATE FUNCTION test_injection_points_attach(IN point_name TEXT,
    IN mode text)
RETURNS void
AS 'MODULE_PATHNAME', 'test_injection_points_attach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- test_injection_points_run()
--
-- Executes an injection point.
--
CREATE FUNCTION test_injection_points_run(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'test_injection_points_run'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- test_injection_points_wake()
--
-- Wakes a condition variable executed in an injection point.
--
CREATE FUNCTION test_injection_points_wake()
RETURNS void
AS 'MODULE_PATHNAME', 'test_injection_points_wake'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- test_injection_points_detach()
--
-- Detaches an injection point.
--
CREATE FUNCTION test_injection_points_detach(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'test_injection_points_detach'
LANGUAGE C STRICT PARALLEL UNSAFE;
