-- test drop
CREATE COMPRESSION METHOD ts1 HANDLER tsvector_compression_handler;
CREATE TABLE droptest(fts tsvector COMPRESSED ts1);
DROP COMPRESSION METHOD ts1;
DROP COMPRESSION METHOD ts1 CASCADE;
\d+ droptest
DROP TABLE droptest;

CREATE COMPRESSION METHOD ts1 HANDLER tsvector_compression_handler;
CREATE TABLE cmtest(fts tsvector COMPRESSED ts1);
SELECT * FROM pg_compression;
SELECT cmhandler, cmoptions FROM pg_compression_opt;

\dCM
\d+ cmtest

INSERT INTO cmtest
	SELECT to_tsvector(string_agg(repeat(substr(i::text,1,1), i), ' '))
	FROM generate_series(1,200) i;

SELECT length(fts) FROM cmtest;
SELECT length(fts) FROM cmtest;

-- check ALTER commands
ALTER TABLE cmtest ALTER COLUMN fts SET NOT COMPRESSED;
\d+ cmtest
ALTER TABLE cmtest ALTER COLUMN fts SET COMPRESSED ts1 WITH (format 'lz');
ALTER TABLE cmtest ALTER COLUMN fts SET COMPRESSED ts1;
\d+ cmtest

-- create different types of tables
SELECT * INTO cmtest2 FROM cmtest;
CREATE TABLE cmtest3 (LIKE cmtest);
CREATE TABLE cmtest4(fts tsvector, a int) INHERITS (cmtest);
CREATE TABLE cmtest5(fts tsvector);
CREATE TABLE cmtest6(fts tsvector);
INSERT INTO cmtest6 SELECT * FROM cmtest;

-- we update usual datum with compressed datum
INSERT INTO cmtest5
	SELECT to_tsvector(string_agg(repeat(substr(i::text,1,1), i), ' '))
	FROM generate_series(1,200) i;
UPDATE cmtest5 SET fts = cmtest.fts FROM cmtest;

\d+ cmtest3
\d+ cmtest4
DROP TABLE cmtest CASCADE;
DROP TABLE cmtest3;

SELECT cmhandler, cmoptions FROM pg_compression_opt;

DROP COMPRESSION METHOD ts1 CASCADE;
SELECT * FROM pg_compression;
SELECT * FROM pg_compression_opt;

-- check that moved tuples still can be decompressed
SELECT length(fts) FROM cmtest2;
SELECT length(fts) FROM cmtest5;
SELECT length(fts) FROM cmtest6;
DROP TABLE cmtest2;
DROP TABLE cmtest5;
DROP TABLE cmtest6;
