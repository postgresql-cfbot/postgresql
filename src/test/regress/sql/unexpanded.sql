-- sanity check of system catalog
SELECT attrelid, attname, attisunexpanded FROM pg_attribute WHERE attisunexpanded;


CREATE TABLE htest0 (a int PRIMARY KEY, b text NOT NULL);
ALTER TABLE htest0 ALTER COLUMN b SET UNEXPANDED;
INSERT INTO htest0 (a, b) VALUES (1, 'htest0 one');
INSERT INTO htest0 (a, b) VALUES (2, 'htest0 two');
-- we allow that all columns of a relation be unexpanded
ALTER TABLE htest0 ALTER COLUMN a SET UNEXPANDED;
SELECT * FROM htest0;
ALTER TABLE htest0 ALTER COLUMN a DROP UNEXPANDED;

CREATE TABLE htest1 (a bigserial PRIMARY KEY, b text);
ALTER TABLE htest1 ALTER COLUMN a SET UNEXPANDED;
-- Insert without named column must not include the unexpanded column
INSERT INTO htest1 VALUES ('htest1 one');
INSERT INTO htest1 VALUES ('htest1 two');
-- INSERT + SELECT * should handle the unexpanded column
CREATE TABLE htest1_1 (a bigserial PRIMARY KEY, b text);
ALTER TABLE htest1_1 ALTER COLUMN a SET UNEXPANDED;
INSERT INTO htest1_1 VALUES ('htest1 one');
WITH cte AS (
	DELETE FROM htest1_1 RETURNING *
) SELECT * FROM cte;
INSERT INTO htest1_1 SELECT * FROM htest0;
SELECT a, b FROM htest1_1;
DROP TABLE htest1_1;

SELECT attrelid::regclass, attname, attisunexpanded FROM pg_attribute WHERE attisunexpanded;

\d+ htest1

-- DROP/SET unexpanded attribute
ALTER TABLE htest0 ALTER COLUMN b DROP UNEXPANDED;

\d+ htest0

ALTER TABLE htest0 ALTER COLUMN b SET UNEXPANDED;

-- Hidden column are not expandable and must not be returned
SELECT * FROM htest0; -- return only column a
SELECT t.* FROM htest1 t; -- return only column b
-- the whole-row syntax do not take care of the unexpanded attribute
SELECT t FROM htest1 t; -- return column a and b

-- CTEs based on SELECT * only have visible column returned
WITH foo AS (SELECT * FROM htest1) SELECT * FROM foo; -- Only column b is returned here

-- Use of wildcard or whole-row in a function do not apply the unexpanded attribute
SELECT row_to_json(t.*) FROM htest0 t;
SELECT row_to_json(t) FROM htest0 t;

-- inheritance, the unexpanded attribute is inherited
CREATE TABLE htest1_1 () INHERITS (htest1);
SELECT * FROM htest1_1;
\d htest1_1
INSERT INTO htest1_1 VALUES ('htest1 three');
SELECT * FROM htest1_1;
SELECT * FROM htest1;

-- unexpanded column must be explicitely named to be returned
SELECT a,b FROM htest1_1;
SELECT a,b FROM htest1;
DROP TABLE htest1_1;

-- Default CREATE TABLE ... LIKE includes unexpanded columns, and they are not uinexpanded in the new table.
CREATE TABLE htest_like1 (LIKE htest1);
\d+ htest_like1
-- CREATE TABLE ... LIKE includes unexpanded columns, and they are unexpanded if requested
CREATE TABLE htest_like2 (LIKE htest1 INCLUDING UNEXPANDED);
\d+ htest_like2
CREATE TABLE htest_like3 (LIKE htest1 INCLUDING ALL);
\d+ htest_like3
DROP TABLE htest_like1, htest_like2, htest_like3;

-- Insert without named column with and a not null unexpanded column must have a default value
INSERT INTO htest0 VALUES (3); -- error
ALTER TABLE htest0 ALTER COLUMN b SET DEFAULT 'unknown';
INSERT INTO htest0 VALUES (3);
-- Same with COPY
COPY htest0 TO stdout;
COPY htest0 (a, b) TO stdout;
COPY htest0 FROM stdin;
4
5
\.
SELECT a,b FROM htest0;

-- same but with drop/add the column between unexpanded columns (virtual columns can be made unexpanded)
CREATE TABLE htest2 (a serial, b int, c int GENERATED ALWAYS AS (a * 2) STORED);
ALTER TABLE htest2 ALTER COLUMN a SET UNEXPANDED;
ALTER TABLE htest2 ALTER COLUMN c SET UNEXPANDED;
SELECT * FROM htest2;
INSERT INTO htest2 VALUES (2);
SELECT a,b,c FROM htest2;
ALTER TABLE htest2 DROP COLUMN b;
ALTER TABLE htest2 ADD COLUMN b int;
INSERT INTO htest2 VALUES (4);
SELECT a,b,c FROM htest2;
DROP TABLE htest2 CASCADE;

-- a table can NOT have all columns unexpanded
CREATE TABLE htest3 (a serial, b int);
ALTER TABLE htest3
    ALTER COLUMN a SET UNEXPANDED,
    ALTER COLUMN b SET UNEXPANDED; -- error
DROP TABLE htest3;

-- inheritance with an additional single unexpanded column is possible
CREATE TABLE htest3 (a serial, b int);
ALTER TABLE htest3 ALTER COLUMN a SET UNEXPANDED;
SELECT * FROM htest3;
CREATE TABLE htest3_1 (c int) INHERITS (htest3);
ALTER TABLE htest3_1 ALTER COLUMN c SET UNEXPANDED;
SELECT * FROM htest3_1;
\d+ htest3_1
DROP TABLE htest3_1, htest3;

-- Ordering do not include the unexpanded column
CREATE TABLE t1 (col1 integer NOT NULL, col2 integer);
ALTER TABLE t1 ALTER COLUMN col1 SET UNEXPANDED;
INSERT INTO t1 (col1, col2) VALUES (1, 6), (3, 4);
SELECT * FROM t1 ORDER BY 1 DESC;
SELECT col1,col2 FROM t1 ORDER BY 2 DESC;
-- unless it is called explicitly
SELECT * FROM t1 ORDER BY col1 DESC;
DROP TABLE t1;

-- A table can be partitioned by an unexpanded column
CREATE TABLE measurement (
	city_id         int not null,
	logdate         date not null,
	peaktemp        int,
	unitsales       int
) PARTITION BY RANGE (logdate);
ALTER TABLE measurement ALTER COLUMN logdate SET UNEXPANDED;
CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2021-01-01') TO ('2021-03-01');
CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2021-03-01') TO ('2021-05-01');
INSERT INTO measurement (city_id, logdate, peaktemp, unitsales) VALUES (1, '2021-02-28', 34, 4);
INSERT INTO measurement (city_id, logdate, peaktemp, unitsales) VALUES (1, '2021-04-12', 42, 6);
EXPLAIN VERBOSE SELECT * FROM measurement;
SELECT * FROM measurement;
SELECT city_id, logdate, peaktemp, unitsales FROM measurement;
DROP TABLE measurement CASCADE;
-- Same but unitsales is unexpanded instead of the partition key
CREATE TABLE measurement (
	city_id         int not null,
	logdate         date not null,
	peaktemp        int,
	unitsales       int
) PARTITION BY RANGE (logdate);
ALTER TABLE measurement ALTER COLUMN unitsales SET UNEXPANDED;
CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2021-01-01') TO ('2021-03-01');
CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2021-03-01') TO ('2021-05-01');
INSERT INTO measurement (city_id, logdate, peaktemp, unitsales) VALUES (1, '2021-02-28', 34, 4);
INSERT INTO measurement (city_id, logdate, peaktemp, unitsales) VALUES (1, '2021-04-12', 42, 6);
EXPLAIN VERBOSE SELECT * FROM measurement;
SELECT * FROM measurement;
SELECT city_id, logdate, peaktemp, unitsales FROM measurement;
SELECT * FROM measurement_y2006m03;
DROP TABLE measurement CASCADE;

-- Temporary tables can have invisible columns too.
CREATE TEMPORARY TABLE htest_tmp (col1 integer NOT NULL, col2 integer);
ALTER TABLE htest_tmp ALTER COLUMN col1 SET UNEXPANDED;
INSERT INTO htest_tmp (col1, col2) VALUES (1, 6), (3, 4);
SELECT * FROM htest_tmp ORDER BY 1 DESC;
DROP TABLE htest_tmp;

-- A table can use a composite type as an unexpanded column
CREATE TYPE compfoo AS (f1 int, f2 text);
CREATE TABLE htest4 (
    a int,
    b compfoo
);
ALTER TABLE htest4 ALTER COLUMN b SET UNEXPANDED;
SELECT * FROM htest4;
DROP TABLE htest4;
DROP TYPE compfoo;

-- Foreign key constraints can be defined on unexpanded columns, or unexpanded columns can be referenced.
CREATE TABLE t1 (col1 integer UNIQUE, col2 integer);
ALTER TABLE t1 ALTER COLUMN col1 SET UNEXPANDED;
CREATE TABLE t2 (col1 integer PRIMARY KEY, col2 integer);
ALTER TABLE t2 ALTER COLUMN col1 SET UNEXPANDED;
ALTER TABLE t1 ADD CONSTRAINT fk_t1_col1 FOREIGN KEY (col1) REFERENCES t2(col1);
ALTER TABLE t2 ADD CONSTRAINT fk_t2_col1 FOREIGN KEY (col1) REFERENCES t1(col1);
DROP TABLE t1, t2 CASCADE;

-- CHECK constraints can be defined on invisible columns.
CREATE TABLE t1 (col1 integer CHECK (col1 > 2), col2 integer NOT NULL);
ALTER TABLE t1 ALTER COLUMN col1 SET UNEXPANDED;
INSERT INTO t1 (col1, col2) VALUES (1, 6); -- error
INSERT INTO t1 (col1, col2) VALUES (3, 6);
-- An index can reference a unexpanded column
CREATE INDEX ON t1 (col1);
ALTER TABLE t1
  ALTER COLUMN col1 TYPE bigint,
  ALTER COLUMN col1 DROP UNEXPANDED,
  ALTER COLUMN col2 SET UNEXPANDED;
\d+ t1
DROP TABLE t1;

-- View must not include the unexpanded column when not explicitly listed
CREATE VIEW viewt1 AS SELECT * FROM htest1;
\d viewt1
SELECT * FROM viewt1;
-- If the unexpanded attribute on the column is removed the view result must not change
ALTER TABLE htest1 ALTER COLUMN a DROP UNEXPANDED;
SELECT * FROM viewt1;
ALTER TABLE htest1 ALTER COLUMN a SET UNEXPANDED;
DROP VIEW viewt1;
-- Materialized view must include the unexpanded column when explicitly listed
-- but the column is not unexpanded in the materialized view.
CREATE VIEW viewt1 AS SELECT a, b FROM htest1;
\d viewt1
SELECT * FROM viewt1;

-- Materialized view must not include the unexpanded column when not explicitly listed
CREATE MATERIALIZED VIEW mviewt1 AS SELECT * FROM htest1;
\d mviewt1
REFRESH MATERIALIZED VIEW mviewt1;
SELECT * FROM mviewt1;
DROP MATERIALIZED VIEW mviewt1;
-- Materialized view must include the unexpanded column when explicitly listed
-- but the column is not unexpanded in the materialized view.
CREATE MATERIALIZED VIEW mviewt1 AS SELECT a, b FROM htest1;
\d mviewt1
REFRESH MATERIALIZED VIEW mviewt1;
SELECT * FROM mviewt1;

-- typed tables with unexpanded column is not supported
CREATE TYPE htest_type AS (f1 integer, f2 text, f3 bigint);
CREATE TABLE htest28 OF htest_type (f1 WITH OPTIONS DEFAULT 3);
ALTER TABLE htest28 ALTER COLUMN f1 SET UNEXPANDED; -- error
DROP TYPE htest_type CASCADE;

-- Prepared statements
PREPARE q1 AS SELECT * FROM htest1 WHERE a > $1;
EXECUTE q1(0);
ALTER TABLE htest1 ALTER COLUMN a DROP UNEXPANDED;
EXECUTE q1(0); -- error: cached plan change result type
ALTER TABLE htest1 ALTER COLUMN a SET UNEXPANDED;
EXECUTE q1(0);
DEALLOCATE q1;


-- SELECT * INTO and RETURNING * INTO has the same
-- behavior, the unexpanded column is not returned.
CREATE OR REPLACE PROCEDURE test_plpgsq_returning (p_a integer)
AS $$
DECLARE
    v_lbl text;
BEGIN
    SELECT * INTO v_lbl FROM htest1 WHERE a = p_a;
    RAISE NOTICE 'SELECT INTO Col b : %', v_lbl;

    DELETE FROM htest1 WHERE a = p_a
        RETURNING * INTO v_lbl; 
    IF FOUND THEN
	RAISE NOTICE 'RETURNING INTO Col b : %', v_lbl;
    ELSE
        RAISE NOTICE 'Noting found';
    END IF;
END
$$
LANGUAGE plpgsql;

CALL test_plpgsq_returning(1);

-- Cleanup
DROP TABLE htest0, htest1 CASCADE;

