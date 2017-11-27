CREATE TABLE spgist_table (t box);
\copy spgist_table from 'data/rect.data'

SELECT center(t) AS p INTO spgist_table_p FROM spgist_table;

CREATE INDEX spgist_idx ON spgist_table_p USING spgist (p);
CREATE INDEX kdspgist_idx ON spgist_table_p USING spgist (p kd_point_ops);

SELECT spgist_stats('spgist_idx');
SELECT * FROM spgist_print('kdspgist_idx') as t(tid tid, allthesame bool, node_n int, level int, tid_pointer tid, prefix float8, node_label int, leaf_value point);
SELECT * FROM spgist_print('spgist_idx') as t(tid tid, allthesame bool, node_n int, level int, tid_pointer tid, prefix point, node_label int, leaf_value point) WHERE level = 1;
