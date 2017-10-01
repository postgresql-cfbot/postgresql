CREATE TABLE gist_table (t box);
\copy gist_table from 'data/rect.data'

CREATE INDEX gist_idx ON gist_table USING gist (t);

SELECT gist_stats('gist_idx');
