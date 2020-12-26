CREATE EXTENSION cmzlib;

-- zlib compression
CREATE TABLE zlibtest(f1 TEXT COMPRESSION lz4);
INSERT INTO zlibtest VALUES(repeat('1234567890',1004));
INSERT INTO zlibtest VALUES(repeat('1234567890 one two three',1004));
SELECT length(f1) FROM zlibtest;

-- alter compression method with rewrite
ALTER TABLE zlibtest ALTER COLUMN f1 SET COMPRESSION lz4;
\d+ zlibtest
ALTER TABLE zlibtest ALTER COLUMN f1 SET COMPRESSION zlib;
\d+ zlibtest

-- preserve old compression method
ALTER TABLE zlibtest ALTER COLUMN f1 SET COMPRESSION lz4 PRESERVE (zlib);
INSERT INTO zlibtest VALUES (repeat('1234567890',1004));
\d+ zlibtest
SELECT pg_column_compression(f1) FROM zlibtest;
SELECT length(f1) FROM zlibtest;

DROP TABLE zlibtest;
