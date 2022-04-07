CREATE FUNCTION test_logging(level int, message text, tags jsonb)
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C;
