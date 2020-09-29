/* src/test/modules/test_atomic_commit/test_atomic_commit--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_atomic_commit" to load this file. \quit

-- test_fdw doesn't use transaction API
CREATE FUNCTION test_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER test_fdw
  HANDLER test_fdw_handler;

-- test_no2pc_fdw uses only COMMIT and ROLLBACK API
CREATE FUNCTION test_no2pc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER test_no2pc_fdw
  HANDLER test_no2pc_fdw_handler;

-- test_2pc uses PREPARE API as well
CREATE FUNCTION test_2pc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER test_2pc_fdw
  HANDLER test_2pc_fdw_handler;

CREATE FUNCTION test_inject_error(
elevel text,
phase text,
server text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_reset_error()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
