-- sanity check of system catalog
SELECT attrelid, attname, attgenerated FROM pg_attribute WHERE attgenerated NOT IN ('', 's', 'v');


CREATE TABLE gtest0 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (55));
CREATE TABLE gtest1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2));

SELECT table_name, column_name, column_default, is_nullable, is_generated, generation_expression FROM information_schema.columns WHERE table_name LIKE 'gtest_' ORDER BY 1, 2;

SELECT table_name, column_name, dependent_column FROM information_schema.column_column_usage ORDER BY 1, 2, 3;

\d gtest1

-- duplicate generated
CREATE TABLE gtest_err_1 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) GENERATED ALWAYS AS (a * 3));

-- references to other generated columns, including self-references
CREATE TABLE gtest_err_2a (a int PRIMARY KEY, b int GENERATED ALWAYS AS (b * 2));
CREATE TABLE gtest_err_2b (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2), c int GENERATED ALWAYS AS (b * 3));

-- invalid reference
CREATE TABLE gtest_err_3 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (c * 2));

-- functions must be immutable
CREATE TABLE gtest_err_4 (a int PRIMARY KEY, b double precision GENERATED ALWAYS AS (random()));

-- cannot have default/identity and generated
CREATE TABLE gtest_err_5a (a int PRIMARY KEY, b int DEFAULT 5 GENERATED ALWAYS AS (a * 2));
CREATE TABLE gtest_err_5b (a int PRIMARY KEY, b int GENERATED ALWAYS AS identity GENERATED ALWAYS AS (a * 2));

INSERT INTO gtest1 VALUES (1);
INSERT INTO gtest1 VALUES (2, DEFAULT);
INSERT INTO gtest1 VALUES (3, 33);  -- error

SELECT * FROM gtest1 ORDER BY a;

UPDATE gtest1 SET b = DEFAULT WHERE a = 1;
UPDATE gtest1 SET b = 11 WHERE a = 1;  -- error

SELECT * FROM gtest1 ORDER BY a;

SELECT a, b, b * 2 AS b2 FROM gtest1 ORDER BY a;
SELECT a, b FROM gtest1 WHERE b = 4 ORDER BY a;

-- test that overflow error happens on read
INSERT INTO gtest1 VALUES (2000000000);
SELECT * FROM gtest1;
DELETE FROM gtest1 WHERE a = 2000000000;

-- test with joins
CREATE TABLE gtestx (x int, y int);
INSERT INTO gtestx VALUES (11, 1), (22, 2), (33, 3);
SELECT * FROM gtestx, gtest1 WHERE gtestx.y = gtest1.a;
DROP TABLE gtestx;

-- test UPDATE/DELETE quals
SELECT * FROM gtest1 ORDER BY a;
UPDATE gtest1 SET a = 3 WHERE b = 4;
SELECT * FROM gtest1 ORDER BY a;
DELETE FROM gtest1 WHERE b = 2;
SELECT * FROM gtest1 ORDER BY a;

-- views
CREATE VIEW gtest1v AS SELECT * FROM gtest1;
SELECT * FROM gtest1v;
INSERT INTO gtest1v VALUES (4, 8);  -- fails

-- inheritance
CREATE TABLE gtest1_1 () INHERITS (gtest1);
SELECT * FROM gtest1_1;
\d gtest1_1
INSERT INTO gtest1_1 VALUES (4);
SELECT * FROM gtest1_1;
SELECT * FROM gtest1;

-- test inheritance mismatch
CREATE TABLE gtesty (x int, b int);
CREATE TABLE gtest1_2 () INHERITS (gtest1, gtesty);  -- error
DROP TABLE gtesty;

-- stored
CREATE TABLE gtest3 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 3) STORED);
INSERT INTO gtest3 (a) VALUES (1), (2), (3);
SELECT * FROM gtest3 ORDER BY a;
UPDATE gtest3 SET a = 22 WHERE a = 2;
SELECT * FROM gtest3 ORDER BY a;

-- COPY
TRUNCATE gtest1;
INSERT INTO gtest1 (a) VALUES (1), (2);

COPY gtest1 TO stdout;

COPY gtest1 (a, b) TO stdout;

COPY gtest1 FROM stdin;
3
4
\.

COPY gtest1 (a, b) FROM stdin;

SELECT * FROM gtest1 ORDER BY a;

TRUNCATE gtest3;
INSERT INTO gtest3 (a) VALUES (1), (2);

COPY gtest3 TO stdout;

COPY gtest3 (a, b) TO stdout;

COPY gtest3 FROM stdin;
3
4
\.

COPY gtest3 (a, b) FROM stdin;

SELECT * FROM gtest3 ORDER BY a;

-- drop column behavior
CREATE TABLE gtest10 (a int PRIMARY KEY, b int, c int GENERATED ALWAYS AS (b * 2));
ALTER TABLE gtest10 DROP COLUMN b;

\d gtest10

-- privileges
CREATE USER regress_user11;

CREATE TABLE gtest11v (a int PRIMARY KEY, b int, c int GENERATED ALWAYS AS (b * 2) VIRTUAL);
INSERT INTO gtest11v VALUES (1, 10), (2, 20);
GRANT SELECT (a, c) ON gtest11v TO regress_user11;

CREATE TABLE gtest11s (a int PRIMARY KEY, b int, c int GENERATED ALWAYS AS (b * 2) STORED);
INSERT INTO gtest11s VALUES (1, 10), (2, 20);
GRANT SELECT (a, c) ON gtest11s TO regress_user11;

CREATE FUNCTION gf1(a int) RETURNS int AS $$ SELECT a * 3 $$ IMMUTABLE LANGUAGE SQL;
REVOKE ALL ON FUNCTION gf1(int) FROM PUBLIC;

CREATE TABLE gtest12v (a int PRIMARY KEY, b int, c int GENERATED ALWAYS AS (gf1(b)) VIRTUAL);
INSERT INTO gtest12v VALUES (1, 10), (2, 20);
GRANT SELECT (a, c) ON gtest12v TO regress_user11;

CREATE TABLE gtest12s (a int PRIMARY KEY, b int, c int GENERATED ALWAYS AS (gf1(b)) STORED);
INSERT INTO gtest12s VALUES (1, 10), (2, 20);
GRANT SELECT (a, c) ON gtest12s TO regress_user11;

SET ROLE regress_user11;
SELECT a, b FROM gtest11v;  -- not allowed
SELECT a, c FROM gtest11v;  -- allowed
SELECT a, b FROM gtest11s;  -- not allowed
SELECT a, c FROM gtest11s;  -- allowed
SELECT gf1(10);  -- not allowed
SELECT a, c FROM gtest12v;  -- not allowed; TODO: ought to be allowed
SELECT a, c FROM gtest12s;  -- allowed
RESET ROLE;

DROP TABLE gtest11v, gtest11s, gtest12v, gtest12s;
DROP FUNCTION gf1(int);
DROP USER regress_user11;

-- check constraints
CREATE TABLE gtest20 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) CHECK (b < 50));
INSERT INTO gtest20 (a) VALUES (10);  -- ok
INSERT INTO gtest20 (a) VALUES (30);  -- violates constraint

CREATE TABLE gtest20a (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2));
INSERT INTO gtest20a (a) VALUES (10);
INSERT INTO gtest20a (a) VALUES (30);
ALTER TABLE gtest20a ADD CHECK (b < 50);  -- fails on existing row

CREATE TABLE gtest20b (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2));
INSERT INTO gtest20b (a) VALUES (10);
INSERT INTO gtest20b (a) VALUES (30);
ALTER TABLE gtest20b ADD CONSTRAINT chk CHECK (b < 50) NOT VALID;
ALTER TABLE gtest20b VALIDATE CONSTRAINT chk;  -- fails on existing row

-- not-null constraints
CREATE TABLE gtest21a (a int PRIMARY KEY, b int GENERATED ALWAYS AS (nullif(a, 0)) NOT NULL);
INSERT INTO gtest21a (a) VALUES (1);  -- ok
INSERT INTO gtest21a (a) VALUES (0);  -- violates constraint

CREATE TABLE gtest21b (a int PRIMARY KEY, b int GENERATED ALWAYS AS (nullif(a, 0)));
ALTER TABLE gtest21b ALTER COLUMN b SET NOT NULL;  -- error
ALTER TABLE gtest21b ALTER COLUMN b DROP NOT NULL;  -- error

CREATE TABLE gtest21c (a int PRIMARY KEY, b int GENERATED ALWAYS AS (nullif(a, 0)) STORED NOT NULL);
INSERT INTO gtest21c (a) VALUES (1);  -- ok
INSERT INTO gtest21c (a) VALUES (0);  -- violates constraint

CREATE TABLE gtest21d (a int PRIMARY KEY, b int GENERATED ALWAYS AS (nullif(a, 0)) STORED);
ALTER TABLE gtest21d ALTER COLUMN b SET NOT NULL;
INSERT INTO gtest21d (a) VALUES (1);  -- ok
INSERT INTO gtest21d (a) VALUES (0);  -- violates constraint
ALTER TABLE gtest21d ALTER COLUMN b DROP NOT NULL;
INSERT INTO gtest21d (a) VALUES (0);  -- ok now

-- index constraints
CREATE TABLE gtest22a (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) UNIQUE);  -- error
CREATE TABLE gtest22b (a int, b int GENERATED ALWAYS AS (a * 2), PRIMARY KEY (a, b));  -- error

-- indexes
CREATE TABLE gtest22c (a int, b int GENERATED ALWAYS AS (a * 2));
CREATE INDEX gtest22c_b_idx ON gtest22c (b);  -- error
CREATE INDEX gtest22c_expr_idx ON gtest22c ((b * 3));
CREATE INDEX gtest22c_pred_idx ON gtest22c (a) WHERE b > 0;
\d gtest22c

INSERT INTO gtest22c VALUES (1), (2), (3);
SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE b * 3 = 6;
SELECT * FROM gtest22c WHERE b * 3 = 6;
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE a = 1 AND b > 0;
SELECT * FROM gtest22c WHERE a = 1 AND b > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;

CREATE TABLE gtest22d (a int, b int GENERATED ALWAYS AS (a * 2) STORED);
CREATE INDEX gtest22d_b_idx ON gtest22d (b);
CREATE INDEX gtest22d_expr_idx ON gtest22d ((b * 3));
CREATE INDEX gtest22d_pred_idx ON gtest22d (a) WHERE b > 0;
\d gtest22d

INSERT INTO gtest22d VALUES (1), (2), (3);
SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
EXPLAIN (COSTS OFF) SELECT * FROM gtest22d WHERE b = 4;
SELECT * FROM gtest22d WHERE b = 4;
EXPLAIN (COSTS OFF) SELECT * FROM gtest22d WHERE b * 3 = 6;
SELECT * FROM gtest22d WHERE b * 3 = 6;
EXPLAIN (COSTS OFF) SELECT * FROM gtest22d WHERE a = 1 AND b > 0;
SELECT * FROM gtest22d WHERE a = 1 AND b > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- foreign keys
CREATE TABLE gtest23a (x int PRIMARY KEY, y int);
INSERT INTO gtest23a VALUES (1, 11), (2, 22), (3, 33);

CREATE TABLE gtest23x (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) REFERENCES gtest23a (x) ON UPDATE CASCADE);  -- error
CREATE TABLE gtest23x (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) REFERENCES gtest23a (x) ON DELETE SET NULL);  -- error

CREATE TABLE gtest23b (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) REFERENCES gtest23a (x));  -- error

CREATE TABLE gtest23c (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2) STORED REFERENCES gtest23a (x));
\d gtest23c

INSERT INTO gtest23c VALUES (1);  -- ok
INSERT INTO gtest23c VALUES (5);  -- error

DROP TABLE gtest23c;
DROP TABLE gtest23a;

CREATE TABLE gtest23p (x int, y int GENERATED ALWAYS AS (x * 2) STORED, PRIMARY KEY (y));
INSERT INTO gtest23p VALUES (1), (2), (3);

CREATE TABLE gtest23q (a int PRIMARY KEY, b int REFERENCES gtest23p (y));
INSERT INTO gtest23q VALUES (1, 2);  -- ok
INSERT INTO gtest23q VALUES (2, 5);  -- error

-- no test for PK using virtual column, since such an index cannot be created

-- domains
CREATE DOMAIN gtestdomain1 AS int CHECK (VALUE < 10);
CREATE TABLE gtest24 (a int PRIMARY KEY, b gtestdomain1 GENERATED ALWAYS AS (a * 2));  -- prohibited

-- typed tables (currently not supported)
CREATE TYPE gtest_type AS (f1 integer, f2 text, f3 bigint);
CREATE TABLE gtest28 OF gtest_type (f1 WITH OPTIONS GENERATED ALWAYS AS (f2 *2));
DROP TYPE gtest_type CASCADE;

-- table partitions (currently not supported)
CREATE TABLE gtest_parent (f1 date NOT NULL, f2 text, f3 bigint) PARTITION BY RANGE (f1);
CREATE TABLE gtest_child PARTITION OF gtest_parent (
    f3 WITH OPTIONS GENERATED ALWAYS AS (f2 * 2)
) FOR VALUES FROM ('2016-07-01') TO ('2016-08-01'); -- error
DROP TABLE gtest_parent;

-- partitioned table
CREATE TABLE gtest_parent (f1 date NOT NULL, f2 bigint, f3 bigint GENERATED ALWAYS AS (f2 * 2)) PARTITION BY RANGE (f1);
CREATE TABLE gtest_child PARTITION OF gtest_parent FOR VALUES FROM ('2016-07-01') TO ('2016-08-01');
INSERT INTO gtest_parent (f1, f2) VALUES ('2016-07-15', 1);
SELECT * FROM gtest_parent;
SELECT * FROM gtest_child;
DROP TABLE gtest_parent;

CREATE TABLE gtest_parent (f1 date NOT NULL, f2 bigint, f3 bigint GENERATED ALWAYS AS (f2 * 2) STORED) PARTITION BY RANGE (f1);
CREATE TABLE gtest_child PARTITION OF gtest_parent FOR VALUES FROM ('2016-07-01') TO ('2016-08-01');
INSERT INTO gtest_parent (f1, f2) VALUES ('2016-07-15', 1);
SELECT * FROM gtest_parent;
SELECT * FROM gtest_child;
DROP TABLE gtest_parent;

-- generated columns in partition key (currently not supported)
CREATE TABLE gtest_parent (f1 date NOT NULL, f2 bigint, f3 bigint GENERATED ALWAYS AS (f2 * 2)) PARTITION BY RANGE (f3);
CREATE TABLE gtest_parent (f1 date NOT NULL, f2 bigint, f3 bigint GENERATED ALWAYS AS (f2 * 2)) PARTITION BY RANGE ((f3 * 3));
CREATE TABLE gtest_parent (f1 date NOT NULL, f2 bigint, f3 bigint GENERATED ALWAYS AS (f2 * 2) STORED) PARTITION BY RANGE ((f3 * 3));
/*
CREATE TABLE gtest_child1 PARTITION OF gtest_parent FOR VALUES FROM (1) TO (10);
CREATE TABLE gtest_child2 PARTITION OF gtest_parent FOR VALUES FROM (11) TO (20);
INSERT INTO gtest_parent (f1, f2) VALUES ('2016-07-15', 1);
INSERT INTO gtest_parent (f1, f2) VALUES ('2016-07-15', 3);
INSERT INTO gtest_parent (f1, f2) VALUES ('2016-07-15', 10);  -- error
SELECT * FROM gtest_child1;
SELECT * FROM gtest_child2;
DROP TABLE gtest_parent;
*/

-- ALTER TABLE ... ADD COLUMN
CREATE TABLE gtest25 (a int PRIMARY KEY);
INSERT INTO gtest25 VALUES (3), (4);
ALTER TABLE gtest25 ADD COLUMN b int GENERATED ALWAYS AS (a * 3);
SELECT * FROM gtest25 ORDER BY a;
ALTER TABLE gtest25 ADD COLUMN x int GENERATED ALWAYS AS (b * 4);  -- error
ALTER TABLE gtest25 ADD COLUMN x int GENERATED ALWAYS AS (z * 4);  -- error

-- ALTER TABLE ... ALTER COLUMN
CREATE TABLE gtest27 (a int, b int GENERATED ALWAYS AS (a * 2));
INSERT INTO gtest27 (a) VALUES (3), (4);
ALTER TABLE gtest27 ALTER COLUMN a TYPE text;  -- error
ALTER TABLE gtest27 ALTER COLUMN b TYPE numeric;
\d gtest27
SELECT * FROM gtest27;
ALTER TABLE gtest27 ALTER COLUMN b TYPE boolean;  -- error

ALTER TABLE gtest27 ALTER COLUMN b DROP DEFAULT;  -- error
\d gtest27

-- triggers
CREATE TABLE gtest26 (a int PRIMARY KEY, b int GENERATED ALWAYS AS (a * 2));

CREATE FUNCTION gtest_trigger_func() RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
  IF tg_op IN ('DELETE', 'UPDATE') THEN
    RAISE INFO '%: old = %', TG_NAME, OLD;
  END IF;
  IF tg_op IN ('INSERT', 'UPDATE') THEN
    RAISE INFO '%: new = %', TG_NAME, NEW;
  END IF;
  IF tg_op = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END
$$;

CREATE TRIGGER gtest1 BEFORE DELETE OR UPDATE ON gtest26
  FOR EACH ROW
  WHEN (OLD.b < 0)  -- ok
  EXECUTE PROCEDURE gtest_trigger_func();

CREATE TRIGGER gtest2 BEFORE INSERT OR UPDATE ON gtest26
  FOR EACH ROW
  WHEN (NEW.b < 0)  -- error
  EXECUTE PROCEDURE gtest_trigger_func();

CREATE TRIGGER gtest3 AFTER DELETE OR UPDATE ON gtest26
  FOR EACH ROW
  WHEN (OLD.b < 0)  -- ok
  EXECUTE PROCEDURE gtest_trigger_func();

CREATE TRIGGER gtest4 AFTER INSERT OR UPDATE ON gtest26
  FOR EACH ROW
  WHEN (NEW.b < 0)  -- ok
  EXECUTE PROCEDURE gtest_trigger_func();

INSERT INTO gtest26 (a) VALUES (-2), (0), (3);
SELECT * FROM gtest26 ORDER BY a;
UPDATE gtest26 SET a = a * -2;
SELECT * FROM gtest26 ORDER BY a;
DELETE FROM gtest26 WHERE a = -6;
SELECT * FROM gtest26 ORDER BY a;


CREATE FUNCTION gtest_trigger_func2() RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
  NEW.b = 5;
  RETURN NEW;
END
$$;

CREATE TRIGGER gtest10 BEFORE INSERT OR UPDATE ON gtest26
  FOR EACH ROW
  EXECUTE PROCEDURE gtest_trigger_func2();

INSERT INTO gtest26 (a) VALUES (10);
UPDATE gtest26 SET a = 1 WHERE a = 0;

-- LIKE INCLUDING GENERATED and dropped column handling
CREATE TABLE gtest28a (
  a int,
  b int,
  c int,
  x int GENERATED ALWAYS AS (b * 2)
);

ALTER TABLE gtest28a DROP COLUMN a;

CREATE TABLE gtest28b (LIKE gtest28a INCLUDING GENERATED);

\d gtest28*
