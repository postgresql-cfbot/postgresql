\set HIDE_TOAST_COMPRESSION false

-- ensure we get stable results regardless of installation's default
SET default_toast_compression = 'pglz';

-- test creating table with compression method
CREATE TABLE cmdata_zstd(f1 TEXT COMPRESSION zstd);
INSERT INTO cmdata_zstd VALUES(repeat('1234567890', 1004));
\d+ cmdata_zstd

-- verify stored compression method in the data
SELECT pg_column_compression(f1) FROM cmdata_zstd;

-- decompress data slice
SELECT SUBSTR(f1, 2000, 50) FROM cmdata_zstd;

-- test LIKE INCLUDING COMPRESSION
CREATE TABLE cmdata_zstd_2 (LIKE cmdata_zstd INCLUDING COMPRESSION);
\d+ cmdata_zstd_2
DROP TABLE cmdata_zstd_2;

-- test externally stored compressed data
CREATE OR REPLACE FUNCTION large_val() RETURNS TEXT LANGUAGE SQL AS
'select array_agg(md5(g::text))::text from generate_series(1, 256) g';
INSERT INTO cmdata_zstd SELECT large_val() || repeat('a', 4000);
SELECT pg_column_compression(f1) FROM cmdata_zstd;
SELECT SUBSTR(f1, 200, 5) FROM cmdata_zstd;

-- test compression with materialized view
CREATE MATERIALIZED VIEW compressmv_zstd(x) AS SELECT * FROM cmdata_zstd;
\d+ compressmv_zstd
SELECT pg_column_compression(f1) FROM cmdata_zstd;
SELECT pg_column_compression(x) FROM compressmv_zstd;

\set HIDE_TOAST_COMPRESSION true
