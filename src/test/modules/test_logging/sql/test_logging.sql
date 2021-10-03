CREATE EXTENSION test_logging;
SET log_min_messages = NOTICE;
SELECT test_logging(18, 'This is a test message', NULL);
SELECT test_logging(18, 'This is a test message', '{"tag1": "value1", "tag2": "value2"}');

