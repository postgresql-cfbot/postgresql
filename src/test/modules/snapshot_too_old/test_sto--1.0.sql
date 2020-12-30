/* src/test/modules/snapshot_too_old/test_sto--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_sto" to load this file. \quit

CREATE FUNCTION test_sto_clobber_snapshot_timestamp(now timestamptz)
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_sto_clobber_snapshot_timestamp'
LANGUAGE C STRICT;

CREATE FUNCTION test_sto_reset_all_state()
RETURNS VOID
AS 'MODULE_PATHNAME', 'test_sto_reset_all_state'
LANGUAGE C STRICT;
