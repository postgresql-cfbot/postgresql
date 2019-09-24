-- Tests for relation options
CREATE EXTENSION dummy_index_am;

CREATE TABLE dummy_test_tab (i int4);

-- Test with default values.
CREATE INDEX dummy_test_idx ON dummy_test_tab
  USING dummy_index_am (i);
SELECT unnest(reloptions) FROM pg_class WHERE relname = 'dummy_test_idx';
DROP INDEX dummy_test_idx;

-- Test with full set of options.
CREATE INDEX dummy_test_idx ON dummy_test_tab
  USING dummy_index_am (i) WITH (
  option_bool = false,
  option_int = 5,
  option_real = 3.1,
  option_string_val = NULL,
  option_string_null = 'val');
SELECT unnest(reloptions) FROM pg_class WHERE relname = 'dummy_test_idx';

-- ALTER INDEX .. SET
ALTER INDEX dummy_test_idx SET (option_int = 10);
ALTER INDEX dummy_test_idx SET (option_bool = true);
ALTER INDEX dummy_test_idx SET (option_real = 3.2);
ALTER INDEX dummy_test_idx SET (option_string_val = 'val2');
ALTER INDEX dummy_test_idx SET (option_string_null = NULL);
SELECT unnest(reloptions) FROM pg_class WHERE relname = 'dummy_test_idx';

-- ALTER INDEX .. RESET
ALTER INDEX dummy_test_idx RESET (option_int);
ALTER INDEX dummy_test_idx RESET (option_bool);
ALTER INDEX dummy_test_idx RESET (option_real);
ALTER INDEX dummy_test_idx RESET (option_string_val);
ALTER INDEX dummy_test_idx RESET (option_string_null);
SELECT unnest(reloptions) FROM pg_class WHERE relname = 'dummy_test_idx';

DROP INDEX dummy_test_idx;
