/* src/test/modules/test_shm_mcxt/test_shm_mcxt--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_shm_mcxt" to load this file. \quit


CREATE PROCEDURE set_shared_list(i int)
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION get_shared_list()
RETURNS SETOF integer
AS 'MODULE_PATHNAME'
LANGUAGE C;
