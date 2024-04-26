/* contrib/snowflake/snowflake--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION snowflake" to load this file. \quit

CREATE FUNCTION snowflake_sequenceam_handler(internal)
  RETURNS sequence_am_handler
  AS 'MODULE_PATHNAME'
  LANGUAGE C;

CREATE ACCESS METHOD snowflake
  TYPE SEQUENCE HANDLER snowflake_sequenceam_handler;
COMMENT ON ACCESS METHOD snowflake IS 'snowflake sequence access method';

CREATE FUNCTION snowflake_get(IN raw int8,
    OUT time_ms int8,
    OUT machine int4,
    OUT counter int4)
  RETURNS record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT
