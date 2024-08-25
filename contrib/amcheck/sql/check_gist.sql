
SELECT setseed(1);

-- Test that index built with bulk load is correct
CREATE TABLE gist_check AS SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
CREATE INDEX gist_check_idx1 ON gist_check USING gist(c);
CREATE INDEX gist_check_idx2 ON gist_check USING gist(c) INCLUDE(p);
SELECT gist_index_check('gist_check_idx1', false);
SELECT gist_index_check('gist_check_idx2', false);
SELECT gist_index_check('gist_check_idx1', true);
SELECT gist_index_check('gist_check_idx2', true);

-- Test that index is correct after inserts
INSERT INTO gist_check SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
SELECT gist_index_check('gist_check_idx1', false);
SELECT gist_index_check('gist_check_idx2', false);
SELECT gist_index_check('gist_check_idx1', true);
SELECT gist_index_check('gist_check_idx2', true);

-- Test that index is correct after vacuuming
DELETE FROM gist_check WHERE c[1] < 5000; -- delete clustered data
DELETE FROM gist_check WHERE c[1]::int % 2 = 0; -- delete scattered data

-- We need two passes through the index and one global vacuum to actually
-- reuse page
VACUUM gist_check;
VACUUM;

SELECT gist_index_check('gist_check_idx1', false);
SELECT gist_index_check('gist_check_idx2', false);
SELECT gist_index_check('gist_check_idx1', true);
SELECT gist_index_check('gist_check_idx2', true);


-- Test that index is correct after reusing pages
INSERT INTO gist_check SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
SELECT gist_index_check('gist_check_idx1', false);
SELECT gist_index_check('gist_check_idx2', false);
SELECT gist_index_check('gist_check_idx1', true);
SELECT gist_index_check('gist_check_idx2', true);
-- cleanup
DROP TABLE gist_check;

--
-- Similar to BUG #15597
--
CREATE TABLE toast_bug(c point,buggy text);
ALTER TABLE toast_bug ALTER COLUMN buggy SET STORAGE extended;
CREATE INDEX toasty ON toast_bug USING gist(c) INCLUDE(buggy);

-- pg_attribute entry for toasty.buggy (the index) will have plain storage:
UPDATE pg_attribute SET attstorage = 'p'
WHERE attrelid = 'toasty'::regclass AND attname = 'buggy';

-- Whereas pg_attribute entry for toast_bug.buggy (the table) still has extended storage:
SELECT attstorage FROM pg_attribute
WHERE attrelid = 'toast_bug'::regclass AND attname = 'buggy';

-- Insert compressible heap tuple (comfortably exceeds TOAST_TUPLE_THRESHOLD):
INSERT INTO toast_bug SELECT point(0,0), repeat('a', 2200);
-- Should not get false positive report of corruption:
SELECT gist_index_check('toasty', true);