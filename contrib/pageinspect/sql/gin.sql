CREATE TABLE test1 (x int, y int[]);
INSERT INTO test1 VALUES (1, ARRAY[11, 111]);
CREATE INDEX test1_y_idx ON test1 USING gin (y) WITH (fastupdate = off);

\x

SELECT * FROM gin_metapage_info(get_raw_page('test1_y_idx', 0));
SELECT * FROM gin_metapage_info(get_raw_page('test1_y_idx', 1));

SELECT * FROM gin_page_opaque_info(get_raw_page('test1_y_idx', 1));

SELECT * FROM gin_leafpage_items(get_raw_page('test1_y_idx', 1));

INSERT INTO test1 SELECT x, ARRAY[1,10] FROM generate_series(2,10000) x;

SELECT COUNT(*) > 0
FROM gin_leafpage_items(get_raw_page('test1_y_idx',
                        (pg_relation_size('test1_y_idx') /
                         current_setting('block_size')::bigint)::int - 1));

\x

CREATE TABLE test__int(a int[]);
\copy test__int from 'data/test__int.data'

CREATE INDEX gin_idx ON test__int USING gin ( a );

INSERT INTO test__int ( SELECT ARRAY[t] || '{1000}'::_int4 FROM generate_series (1,300) as t );
INSERT INTO test__int ( SELECT ARRAY[t] || '{1001}'::_int4 FROM generate_series (1,300) as t, generate_series(1,12) );
VACUUM ANALYZE test__int;
SELECT * FROM gin_value_count('gin_idx') as t(value int, nrow int);
