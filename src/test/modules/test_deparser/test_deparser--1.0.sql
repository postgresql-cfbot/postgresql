\echo Use "CREATE EXTENSION test_deparser" to load this file. \quit

CREATE FUNCTION tdparser_get_cmdcount()
  RETURNS int STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_deparser_drop_command()
RETURNS event_trigger STRICT SECURITY DEFINER
AS 'MODULE_PATHNAME' LANGUAGE C;
