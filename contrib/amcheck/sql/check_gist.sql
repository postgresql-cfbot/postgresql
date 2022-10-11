
SELECT setseed(1);

-- Test that index built with bulk load is correct
CREATE TABLE gist_check AS SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
CREATE INDEX gist_check_idx1 ON gist_check USING gist(c);
CREATE INDEX gist_check_idx2 ON gist_check USING gist(c) INCLUDE(p);
SELECT gist_index_parent_check('gist_check_idx1', false);
SELECT gist_index_parent_check('gist_check_idx2', false);
SELECT gist_index_parent_check('gist_check_idx1', true);
SELECT gist_index_parent_check('gist_check_idx2', true);

-- Test that index is correct after inserts
INSERT INTO gist_check SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
SELECT gist_index_parent_check('gist_check_idx1', false);
SELECT gist_index_parent_check('gist_check_idx2', false);
SELECT gist_index_parent_check('gist_check_idx1', true);
SELECT gist_index_parent_check('gist_check_idx2', true);

-- Test that index is correct after vacuuming
DELETE FROM gist_check WHERE c[1] < 5000; -- delete clustered data
DELETE FROM gist_check WHERE c[1]::int % 2 = 0; -- delete scattered data

-- We need two passes through the index and one global vacuum to actually
-- reuse page
VACUUM gist_check;
VACUUM;

SELECT gist_index_parent_check('gist_check_idx1', false);
SELECT gist_index_parent_check('gist_check_idx2', false);
SELECT gist_index_parent_check('gist_check_idx1', true);
SELECT gist_index_parent_check('gist_check_idx2', true);


-- Test that index is correct after reusing pages
INSERT INTO gist_check SELECT point(random(),s) c, random() p FROM generate_series(1,10000) s;
SELECT gist_index_parent_check('gist_check_idx1', false);
SELECT gist_index_parent_check('gist_check_idx2', false);
SELECT gist_index_parent_check('gist_check_idx1', true);
SELECT gist_index_parent_check('gist_check_idx2', true);
-- cleanup
DROP TABLE gist_check;
