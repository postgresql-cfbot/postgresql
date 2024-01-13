-- Tests for dummy table access method
CREATE EXTENSION dummy_table_am;
CREATE TABLE dummy_table (a int, b text) USING dummy_table_am;
-- Will error out
CREATE INDEX dummy_table_idx ON dummy_table (a);
SELECT * FROM dummy_table;

INSERT INTO dummy_table VALUES (1, 'dummy');
SELECT * FROM dummy_table;

INSERT INTO dummy_table VALUES (1, 'dummy'), (2, 'dummy');
SELECT * FROM dummy_table;

UPDATE dummy_table SET a = 0 WHERE a = 1;
SELECT * FROM dummy_table;

-- Update without WHERE
UPDATE dummy_table SET a = 0, b = NULL;
SELECT * FROM dummy_table;

DELETE FROM dummy_table WHERE a = 1;
SELECT * FROM dummy_table;

TRUNCATE dummy_table;
SELECT * FROM dummy_table;

VACUUM dummy_table;
ANALYZE dummy_table;
VACUUM (FULL) dummy_table;
SELECT * FROM dummy_table;

COPY dummy_table TO STDOUT;
COPY dummy_table (a, b) FROM STDIN;
1	dummy
\.
SELECT * FROM dummy_table;

ALTER TABLE dummy_table ADD COLUMN c BOOLEAN DEFAULT true;
INSERT INTO dummy_table VALUES (1, 'dummy');
SELECT * FROM dummy_table;

-- ALTER TABLE SET ACCESS METHOD
ALTER TABLE dummy_table SET ACCESS METHOD heap;
CREATE INDEX dummy_table_idx ON dummy_table (a);
INSERT INTO dummy_table VALUES (1, 'heap', false);
SELECT * FROM dummy_table;

-- Will error out, the index must be dropped
ALTER TABLE dummy_table SET ACCESS METHOD dummy_table_am;
DROP INDEX dummy_table_idx;
ALTER TABLE dummy_table SET ACCESS METHOD dummy_table_am;
SELECT * FROM dummy_table;

-- Clean up
DROP TABLE dummy_table;
