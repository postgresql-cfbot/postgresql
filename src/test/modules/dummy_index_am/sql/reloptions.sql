CREATE EXTENSION dummy_index_am;

SET dummy_index.do_test_reloptions to true;

CREATE TABLE tst (i int4);

-- Test reloptions behavior when no reloption is set
CREATE INDEX test_idx ON tst USING dummy_index_am (i);
DROP INDEX test_idx;

-- Test behavior of int option (default and non default values)
SET dummy_index.do_test_reloption_int to true;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (bool_option = false);
DROP INDEX test_idx;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (int_option = 5);

ALTER INDEX test_idx SET (int_option = 3);
INSERT INTO tst VALUES(1);
ALTER INDEX test_idx SET (bool_option = false);
ALTER INDEX test_idx RESET (int_option);
INSERT INTO tst VALUES(1);

DROP INDEX test_idx;
SET dummy_index.do_test_reloption_int to false;

-- Test behavior of real option (default and non default values)
SET dummy_index.do_test_reloption_real to true;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (bool_option = false);
DROP INDEX test_idx;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (real_option = 5);

ALTER INDEX test_idx SET (real_option = 3);
INSERT INTO tst VALUES(1);
ALTER INDEX test_idx SET (bool_option = false);
ALTER INDEX test_idx RESET (real_option);
INSERT INTO tst VALUES(1);

DROP INDEX test_idx;
SET dummy_index.do_test_reloption_real to false;

-- Test behavior of bool option (default and non default values)
SET dummy_index.do_test_reloption_bool to true;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (int_option = 5);
DROP INDEX test_idx;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (bool_option = false);

ALTER INDEX test_idx SET (bool_option = true);
INSERT INTO tst VALUES(1);
ALTER INDEX test_idx SET (int_option = 5, bool_option = false);
ALTER INDEX test_idx RESET (bool_option);
INSERT INTO tst VALUES(1);

DROP INDEX test_idx;
SET dummy_index.do_test_reloption_bool to false;

-- Test behavior of string option (default and non default values + validate
-- function)
SET dummy_index.do_test_reloption_string to true;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (int_option = 5);
DROP INDEX test_idx;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (string_option =
"Invalid_value");

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (string_option =
"Valid_value");

ALTER INDEX test_idx SET (string_option = "Valid_value_2", int_option = 5);
INSERT INTO tst VALUES(1);
ALTER INDEX test_idx RESET (string_option);
INSERT INTO tst VALUES(1);

DROP INDEX test_idx;
SET dummy_index.do_test_reloption_string to false;

-- Test behavior of second string option
-- Testing default and non default values + _no_ validate function)
SET dummy_index.do_test_reloption_string2 to true;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (string_option =
"Something");
DROP INDEX test_idx;

CREATE INDEX test_idx ON tst USING dummy_index_am (i) WITH (string_option2 =
"Some_value");

ALTER INDEX test_idx SET (string_option2 = "Valid_value_2", int_option = 5);
INSERT INTO tst VALUES(1);
ALTER INDEX test_idx RESET (string_option2);
INSERT INTO tst VALUES(1);

DROP INDEX test_idx;
SET dummy_index.do_test_reloption_string2 to false;
SET dummy_index.do_test_reloptions to false;
