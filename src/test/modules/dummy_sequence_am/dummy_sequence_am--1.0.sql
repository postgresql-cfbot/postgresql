/* src/test/modules/dummy_sequence_am/dummy_sequence_am--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dummy_sequence_am" to load this file. \quit

CREATE FUNCTION dummy_sequenceam_handler(internal)
  RETURNS sequence_am_handler
  AS 'MODULE_PATHNAME'
  LANGUAGE C;

CREATE ACCESS METHOD dummy_sequence_am
  TYPE SEQUENCE HANDLER dummy_sequenceam_handler;
COMMENT ON ACCESS METHOD dummy_sequence_am IS 'dummy sequence access method';
