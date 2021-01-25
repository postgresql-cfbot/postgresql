/* contrib/resownerbench/resownerbench--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION resownerbench" to load this file. \quit

CREATE FUNCTION snapshotbench_lifo(numkeep int, numsnaps int, numiters int)
RETURNS double precision
AS 'MODULE_PATHNAME', 'snapshotbench_lifo'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION snapshotbench_fifo(numkeep int, numsnaps int, numiters int)
RETURNS double precision
AS 'MODULE_PATHNAME', 'snapshotbench_fifo'
LANGUAGE C STRICT VOLATILE;
