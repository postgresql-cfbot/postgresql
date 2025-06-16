-- helper func
CREATE OR REPLACE FUNCTION random_string(int) RETURNS text AS $$
SELECT string_agg(substring('0123456789abcdefghijklmnopqrstuvwxyz', ceil(random() * 36)::integer, 1), '') FROM generate_series(1, $1);
$$ LANGUAGE sql;


-- empty table index should be valid
CREATE TABLE brintest (a bigint) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (a);
SELECT brin_index_check('brintest_idx', true, true);
-- cleanup
DROP TABLE brintest;

-- min_max opclass
CREATE TABLE brintest (a bigint) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (a int8_minmax_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a) SELECT x FROM generate_series(1,100000) x;
-- create some empty ranges
DELETE FROM brintest WHERE a > 20000 AND a < 40000;
SELECT brin_index_check('brintest_idx', true, true);

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (a int8_minmax_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true);
-- cleanup
DROP TABLE brintest;



-- multi_min_max opclass
CREATE TABLE brintest (a bigint) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (a int8_minmax_multi_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a) SELECT x FROM generate_series(1,100000) x;
-- create some empty ranges
DELETE FROM brintest WHERE a > 20000 AND a < 40000;
SELECT brin_index_check('brintest_idx', true, true);

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (a int8_minmax_multi_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true);
-- cleanup
DROP TABLE brintest;



-- bloom opclass
CREATE TABLE brintest (a bigint) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (a int8_bloom_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a) SELECT x FROM generate_series(1,100000) x;
-- create some empty ranges
DELETE FROM brintest WHERE a > 20000 AND a < 40000;
SELECT brin_index_check('brintest_idx', true, true);

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (a int8_bloom_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true);
-- cleanup
DROP TABLE brintest;


-- inclusion opclass
CREATE TABLE brintest (id serial PRIMARY KEY, a box);
CREATE INDEX brintest_idx ON brintest USING brin (a box_inclusion_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a)
SELECT box(point(random() * 1000, random() * 1000), point(random() * 1000, random() * 1000))
FROM generate_series(1, 10000);
-- create some empty ranges
DELETE FROM brintest WHERE id > 2000 AND id < 4000;

SELECT brin_index_check('brintest_idx', true, true, '@>');

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (a box_inclusion_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true, '@>');
-- cleanup
DROP TABLE brintest;


-- multiple attributes
CREATE TABLE brintest (id bigserial, a text) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (id int8_minmax_ops, a text_minmax_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a) SELECT random_string((x % 100)) FROM generate_series(1,3000) x;
-- create some empty ranges
DELETE FROM brintest WHERE id > 1500 AND id < 2500;
SELECT brin_index_check('brintest_idx', true, true);

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (id int8_minmax_ops, a text_minmax_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true);
-- cleanup
DROP TABLE brintest;


-- multiple attributes test with custom operators
CREATE TABLE brintest (id bigserial, a text, b box) WITH (fillfactor = 10);
CREATE INDEX brintest_idx ON brintest USING brin (id int8_minmax_ops, a text_minmax_ops, b box_inclusion_ops) WITH (pages_per_range = 2);
INSERT INTO brintest (a, b) SELECT
                                random_string((x % 100)),
                                box(point(random() * 1000, random() * 1000), point(random() * 1000, random() * 1000))
FROM generate_series(1, 3000) x;
-- create some empty ranges
DELETE FROM brintest WHERE id > 1500 AND id < 2500;
SELECT brin_index_check('brintest_idx', true, true, '=', '=', '@>');

-- rebuild index
DROP INDEX brintest_idx;
CREATE INDEX brintest_idx ON brintest USING brin (id int8_minmax_ops, a text_minmax_ops, b box_inclusion_ops) WITH (pages_per_range = 2);
SELECT brin_index_check('brintest_idx', true, true, '=', '=', '@>');

-- error if it's impossible to use default operator for all index attributes
SELECT brin_index_check('brintest_idx', true, true);

-- error if number of operators in input doesn't match index attributes number
SELECT brin_index_check('brintest_idx', true, true, '=');

-- error if operator name is NULL
SELECT brin_index_check('brintest_idx', true, true, '=', '=', NULL);

-- error if there is no operator for attribute type
SELECT brin_index_check('brintest_idx', true, true, '=', '=', '@@');

-- cleanup
DROP TABLE brintest;


-- cleanup
DROP FUNCTION random_string;