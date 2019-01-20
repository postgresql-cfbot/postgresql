-- test drop
DROP ACCESS METHOD pglz; --fail

CREATE ACCESS METHOD pglz1 TYPE COMPRESSION HANDLER pglzhandler;
CREATE TABLE droptest(d1 TEXT COMPRESSION pglz1);
DROP ACCESS METHOD pglz1;
DROP ACCESS METHOD pglz1 CASCADE;
\d+ droptest
DROP TABLE droptest;

CREATE ACCESS METHOD pglz1 TYPE COMPRESSION HANDLER pglzhandler;

-- test auto drop of related attribute compression
CREATE ACCESS METHOD pglz2 TYPE COMPRESSION HANDLER pglzhandler;
CREATE TABLE cmaltertest (f1 TEXT COMPRESSION pglz2);
SELECT acname, acattnum, acoptions FROM pg_attr_compression
	WHERE acrelid = 'cmaltertest'::regclass;
ALTER TABLE cmaltertest DROP COLUMN f1;
SELECT acname, acattnum, acoptions FROM pg_attr_compression
	WHERE acrelid = 'cmaltertest'::regclass;
DROP TABLE cmaltertest;

-- test drop
DROP ACCESS METHOD pglz2;

-- test alter data type
CREATE TABLE cmaltertest(at1 TEXT);
ALTER TABLE cmaltertest ALTER COLUMN at1 SET COMPRESSION pglz1 PRESERVE (pglz);
SELECT pg_column_compression('cmaltertest', 'at1');
ALTER TABLE cmaltertest ALTER COLUMN at1 SET DATA TYPE INT USING at1::INTEGER;
\d+ cmaltertest
SELECT pg_column_compression('cmaltertest', 'at1');
SELECT acname, acattnum, acoptions FROM pg_attr_compression
	WHERE acrelid = 'cmaltertest'::regclass;
ALTER TABLE cmaltertest ALTER COLUMN at1 SET DATA TYPE TEXT;
SELECT pg_column_compression('cmaltertest', 'at1');
DROP TABLE cmaltertest;

-- test storages
CREATE TABLE cmstoragetest(st1 TEXT, st2 INT);
ALTER TABLE cmstoragetest ALTER COLUMN st2
	SET COMPRESSION pglz WITH (min_input_size '100'); -- fail
ALTER TABLE cmstoragetest ALTER COLUMN st1
	SET COMPRESSION pglz WITH (min_input_size '100', min_comp_rate '50');
ALTER TABLE cmstoragetest ALTER COLUMN st1 SET STORAGE EXTERNAL;
\d+ cmstoragetest
ALTER TABLE cmstoragetest ALTER COLUMN st1 SET STORAGE MAIN;
\d+ cmstoragetest
ALTER TABLE cmstoragetest ALTER COLUMN st1 SET STORAGE PLAIN;
\d+ cmstoragetest
ALTER TABLE cmstoragetest ALTER COLUMN st1 SET STORAGE EXTENDED;
\d+ cmstoragetest
DROP TABLE cmstoragetest;

CREATE ACCESS METHOD pglz2 TYPE COMPRESSION HANDLER pglzhandler;

-- test PRESERVE
CREATE TABLE cmtest(f1 TEXT);
\d+ cmtest
-- view to check dependencies
CREATE VIEW cmtest_deps AS
	SELECT classid, objsubid, refclassid, refobjsubid, deptype
	FROM pg_depend
	WHERE (refclassid = 4001 OR classid = 4001) AND
		  (objid = 'cmtest'::regclass OR refobjid = 'cmtest'::regclass)
	ORDER by objid, refobjid;
INSERT INTO cmtest VALUES(repeat('1234567890',1001));

-- one normal dependency
SELECT * FROM cmtest_deps;

-- check decompression
SELECT length(f1) FROM cmtest;
SELECT length(f1) FROM cmtest;

CREATE FUNCTION on_cmtest_rewrite()
RETURNS event_trigger AS $$
BEGIN
	RAISE NOTICE 'cmtest rewrite';
END;
$$  LANGUAGE plpgsql;

CREATE EVENT TRIGGER notice_on_cmtest_rewrite ON table_rewrite
	EXECUTE PROCEDURE on_cmtest_rewrite();

-- no rewrite
ALTER TABLE cmtest ALTER COLUMN f1 SET COMPRESSION pglz1 PRESERVE (pglz);
INSERT INTO cmtest VALUES(repeat('1234567890',1002));
SELECT length(f1) FROM cmtest;
SELECT pg_column_compression('cmtest', 'f1');
-- one normal and one internal dependency
SELECT * FROM cmtest_deps;

-- rewrite
ALTER TABLE cmtest ALTER COLUMN f1 SET COMPRESSION pglz2 PRESERVE (pglz1);
INSERT INTO cmtest VALUES(repeat('1234567890',1003));
SELECT length(f1) FROM cmtest;
SELECT pg_column_compression('cmtest', 'f1');
-- two internal dependencies
SELECT * FROM cmtest_deps;

-- rewrite
ALTER TABLE cmtest ALTER COLUMN f1 SET COMPRESSION pglz;
INSERT INTO cmtest VALUES(repeat('1234567890',1004));
SELECT length(f1) FROM cmtest;
SELECT pg_column_compression('cmtest', 'f1');
-- one nornal dependency
SELECT * FROM cmtest_deps;

-- no rewrites
ALTER TABLE cmtest ALTER COLUMN f1 SET COMPRESSION pglz1 PRESERVE (pglz);
INSERT INTO cmtest VALUES(repeat('1234567890',1005));
ALTER TABLE cmtest ALTER COLUMN f1 SET COMPRESSION pglz
	WITH (min_input_size '1000') PRESERVE (pglz, pglz1);
INSERT INTO cmtest VALUES(repeat('1234567890',1006));
-- one nornal dependency and two internal dependencies
SELECT * FROM cmtest_deps;

-- remove function and related event trigger
DROP FUNCTION on_cmtest_rewrite CASCADE;

-- test moving
CREATE TABLE cmdata(f1 text COMPRESSION pglz WITH (first_success_by '10000'));
INSERT INTO cmdata VALUES(repeat('1234567890',1000));
INSERT INTO cmdata VALUES(repeat('1234567890',1001));

-- copy with table creation
SELECT * INTO cmmove1 FROM cmdata;

-- we update using datum from different table
CREATE TABLE cmmove2(f1 text COMPRESSION pglz WITH (min_input_size '100'));
INSERT INTO cmmove2 VALUES (repeat('1234567890',1004));
UPDATE cmmove2 SET f1 = cmdata.f1 FROM cmdata;

-- copy to existing table
CREATE TABLE cmmove3(f1 text COMPRESSION pglz WITH (min_input_size '100'));
INSERT INTO cmmove3 SELECT * FROM cmdata;

-- drop original compression information
DROP TABLE cmdata;

-- check data is ok
SELECT length(f1) FROM cmmove1;
SELECT length(f1) FROM cmmove2;
SELECT length(f1) FROM cmmove3;

-- create different types of tables
CREATE TABLE cmexample (f1 TEXT COMPRESSION pglz1);
CREATE TABLE cmtestlike1 (LIKE cmexample INCLUDING COMPRESSION);
CREATE TABLE cmtestlike2 (f2 INT) INHERITS (cmexample);

\d+ cmtestlike1
\d+ cmtestlike2

-- test two columns
CREATE TABLE cmaltertest(f1 TEXT, f2 TEXT COMPRESSION pglz1);
\d+ cmaltertest;
-- fail, changing one column twice
ALTER TABLE cmaltertest ALTER COLUMN f1 SET COMPRESSION pglz,
	ALTER COLUMN f1 SET COMPRESSION pglz;

-- with rewrite
ALTER TABLE cmaltertest ALTER COLUMN f1 SET COMPRESSION pglz1,
	ALTER COLUMN f2 SET COMPRESSION pglz PRESERVE (pglz1);
SELECT pg_column_compression('cmaltertest', 'f1');
SELECT pg_column_compression('cmaltertest', 'f2');

-- no rewrite
ALTER TABLE cmaltertest ALTER COLUMN f1 SET COMPRESSION pglz PRESERVE (pglz1),
	ALTER COLUMN f2 SET COMPRESSION pglz2 PRESERVE (pglz1, pglz);
SELECT pg_column_compression('cmaltertest', 'f1');
SELECT pg_column_compression('cmaltertest', 'f2');

-- make pglz2 droppable
ALTER TABLE cmaltertest ALTER COLUMN f2 SET COMPRESSION pglz1;
SELECT pg_column_compression('cmaltertest', 'f1');
SELECT pg_column_compression('cmaltertest', 'f2');

SELECT acname, acattnum, acoptions FROM pg_attr_compression
	WHERE acrelid = 'cmaltertest'::regclass OR acrelid = 'cmtest'::regclass;

-- zlib compression
CREATE TABLE zlibtest(f1 TEXT COMPRESSION zlib WITH (invalid 'param'));
CREATE TABLE zlibtest(f1 TEXT COMPRESSION zlib WITH (level 'best'));
CREATE TABLE zlibtest(f1 TEXT COMPRESSION zlib);
ALTER TABLE zlibtest
	ALTER COLUMN f1 SET COMPRESSION zlib WITH (level 'best_compression');
ALTER TABLE zlibtest
	ALTER COLUMN f1 SET COMPRESSION zlib WITH (level 'best_speed');
ALTER TABLE zlibtest
	ALTER COLUMN f1 SET COMPRESSION zlib WITH (level 'default');
ALTER TABLE zlibtest
	ALTER COLUMN f1 SET COMPRESSION zlib WITH (dict 'one');
INSERT INTO zlibtest VALUES(repeat('1234567890',1004));
ALTER TABLE zlibtest
	ALTER COLUMN f1 SET COMPRESSION zlib WITH (dict 'one two') PRESERVE (zlib);
INSERT INTO zlibtest VALUES(repeat('1234567890 one two three',1004));
SELECT length(f1) FROM zlibtest;

DROP ACCESS METHOD pglz2;
DROP VIEW cmtest_deps;
DROP TABLE cmmove1, cmmove2, cmmove3;
DROP TABLE cmexample, cmtestlike1, cmtestlike2, zlibtest;
