--
-- PARTITIONS_MERGE
-- Tests for "ALTER TABLE ... MERGE PARTITIONS ..." command
--

CREATE SCHEMA partitions_merge_schema;
SET search_path = partitions_merge_schema, public;

--
-- BY RANGE partitioning
--

--
-- Test for error codes
--
CREATE TABLE sales_range (salesman_id INT, salesman_name VARCHAR(30), sales_amount INT, sales_date DATE) PARTITION BY RANGE (sales_date);
CREATE TABLE sales_dec2021 PARTITION OF sales_range FOR VALUES FROM ('2021-12-01') TO ('2021-12-31');
CREATE TABLE sales_jan2022 PARTITION OF sales_range FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE sales_feb2022 PARTITION OF sales_range FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE sales_mar2022 PARTITION OF sales_range FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');

CREATE TABLE sales_apr2022 (salesman_id INT, salesman_name VARCHAR(30), sales_amount INT, sales_date DATE) PARTITION BY RANGE (sales_date);
CREATE TABLE sales_apr_1 PARTITION OF sales_apr2022 FOR VALUES FROM ('2022-04-01') TO ('2022-04-15');
CREATE TABLE sales_apr_2 PARTITION OF sales_apr2022 FOR VALUES FROM ('2022-04-15') TO ('2022-05-01');
ALTER TABLE sales_range ATTACH PARTITION sales_apr2022 FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');

CREATE TABLE sales_others PARTITION OF sales_range DEFAULT;

-- ERROR:  partition with name "sales_feb2022" already used
ALTER TABLE sales_range MERGE PARTITIONS (sales_feb2022, sales_mar2022, sales_feb2022) INTO sales_feb_mar_apr2022;
-- ERROR:  cannot merge non-table partition "sales_apr2022"
ALTER TABLE sales_range MERGE PARTITIONS (sales_feb2022, sales_mar2022, sales_apr2022) INTO sales_feb_mar_apr2022;
-- ERROR:  invalid partitions order, partition "sales_mar2022" can not be merged
-- (space between sections sales_jan2022 and sales_mar2022)
ALTER TABLE sales_range MERGE PARTITIONS (sales_jan2022, sales_mar2022) INTO sales_jan_mar2022;
-- ERROR:  invalid partitions order, partition "sales_jan2022" can not be merged
-- (space between sections sales_dec2021 and sales_jan2022)
ALTER TABLE sales_range MERGE PARTITIONS (sales_dec2021, sales_jan2022, sales_feb2022) INTO sales_dec_jan_feb2022;

-- NO ERROR: test for custom partitions order
ALTER TABLE sales_range MERGE PARTITIONS (sales_feb2022, sales_mar2022, sales_jan2022) INTO sales_jan_feb_mar2022;

SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_range'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

DROP TABLE sales_range;

--
-- Add rows into partitioned table then merge partitions
--
CREATE TABLE sales_range (salesman_id INT, salesman_name VARCHAR(30), sales_amount INT, sales_date DATE) PARTITION BY RANGE (sales_date);
CREATE TABLE sales_jan2022 PARTITION OF sales_range FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE sales_feb2022 PARTITION OF sales_range FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE sales_mar2022 PARTITION OF sales_range FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');
CREATE TABLE sales_apr2022 PARTITION OF sales_range FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');
CREATE TABLE sales_others PARTITION OF sales_range DEFAULT;
CREATE INDEX sales_range_sales_date_idx ON sales_range USING btree (sales_date);

INSERT INTO sales_range VALUES (1,  'May',      1000, '2022-01-31');
INSERT INTO sales_range VALUES (2,  'Smirnoff', 500,  '2022-02-10');
INSERT INTO sales_range VALUES (3,  'Ford',     2000, '2022-04-30');
INSERT INTO sales_range VALUES (4,  'Ivanov',   750,  '2022-04-13');
INSERT INTO sales_range VALUES (5,  'Deev',     250,  '2022-04-07');
INSERT INTO sales_range VALUES (6,  'Poirot',   150,  '2022-02-11');
INSERT INTO sales_range VALUES (7,  'Li',       175,  '2022-03-08');
INSERT INTO sales_range VALUES (8,  'Ericsson', 185,  '2022-02-23');
INSERT INTO sales_range VALUES (9,  'Muller',   250,  '2022-03-11');
INSERT INTO sales_range VALUES (10, 'Halder',   350,  '2022-01-28');
INSERT INTO sales_range VALUES (11, 'Trump',    380,  '2022-04-06');
INSERT INTO sales_range VALUES (12, 'Plato',    350,  '2022-03-19');
INSERT INTO sales_range VALUES (13, 'Gandi',    377,  '2022-01-09');
INSERT INTO sales_range VALUES (14, 'Smith',    510,  '2022-05-04');

SELECT pg_catalog.pg_get_partkeydef('sales_range'::regclass);

-- show partitions with conditions:
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_range'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

ALTER TABLE sales_range MERGE PARTITIONS (sales_feb2022, sales_mar2022, sales_apr2022) INTO sales_feb_mar_apr2022;

-- show partitions with conditions:
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_range'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

SELECT * FROM pg_indexes WHERE tablename = 'sales_feb_mar_apr2022' and schemaname = 'partitions_merge_schema';

SELECT * FROM sales_range;
SELECT * FROM sales_jan2022;
SELECT * FROM sales_feb_mar_apr2022;
SELECT * FROM sales_others;

-- Use indexscan for test indexes
SET enable_indexscan = ON;
SET enable_seqscan = OFF;

SELECT * FROM sales_feb_mar_apr2022 where sales_date > '2022-01-01';

SET enable_indexscan = ON;
SET enable_seqscan = ON;

ALTER TABLE sales_range SPLIT PARTITION sales_feb_mar_apr2022 INTO
  (PARTITION sales_feb2022 FOR VALUES FROM ('2022-02-01') TO ('2022-03-01'),
   PARTITION sales_mar2022 FOR VALUES FROM ('2022-03-01') TO ('2022-04-01'),
   PARTITION sales_apr2022 FOR VALUES FROM ('2022-04-01') TO ('2022-05-01'));

-- Merge partitions (include DEFAULT partition) into partition with the same
-- name
ALTER TABLE sales_range MERGE PARTITIONS (sales_jan2022, sales_mar2022, sales_others) INTO sales_others;

select * from sales_others;

-- show partitions with conditions:
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_range'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

DROP TABLE sales_range;

--
-- Test for:
--   * composite partition key;
--   * GENERATED column;
--   * column with DEFAULT value.
--
CREATE TABLE sales_date (salesman_name VARCHAR(30), sales_year INT, sales_month INT, sales_day INT,
  sales_date VARCHAR(10) GENERATED ALWAYS AS
    (LPAD(sales_year::text, 4, '0') || '.' || LPAD(sales_month::text, 2, '0') || '.' || LPAD(sales_day::text, 2, '0')) STORED,
  sales_department VARCHAR(30) DEFAULT 'Sales department')
  PARTITION BY RANGE (sales_year, sales_month, sales_day);

CREATE TABLE sales_dec2022 PARTITION OF sales_date FOR VALUES FROM (2021, 12, 1) TO (2022, 1, 1);
CREATE TABLE sales_jan2022 PARTITION OF sales_date FOR VALUES FROM (2022, 1, 1) TO (2022, 2, 1);
CREATE TABLE sales_feb2022 PARTITION OF sales_date FOR VALUES FROM (2022, 2, 1) TO (2022, 3, 1);
CREATE TABLE sales_other PARTITION OF sales_date FOR VALUES FROM (2022, 3, 1) TO (MAXVALUE, MAXVALUE, MAXVALUE);

INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager1', 2021, 12, 7);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager2', 2021, 12, 8);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager3', 2022, 1, 1);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager1', 2022, 2, 4);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager2', 2022, 1, 2);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager3', 2022, 2, 1);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager1', 2022, 3, 3);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager2', 2022, 3, 4);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager3', 2022, 5, 1);

SELECT * FROM sales_date;
SELECT * FROM sales_dec2022;
SELECT * FROM sales_jan2022;
SELECT * FROM sales_feb2022;
SELECT * FROM sales_other;

ALTER TABLE sales_date MERGE PARTITIONS (sales_jan2022, sales_feb2022) INTO sales_jan_feb2022;

INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager1', 2022, 1, 10);
INSERT INTO sales_date(salesman_name, sales_year, sales_month, sales_day) VALUES ('Manager2', 2022, 2, 10);

SELECT * FROM sales_date;
SELECT * FROM sales_dec2022;
SELECT * FROM sales_jan_feb2022;
SELECT * FROM sales_other;

DROP TABLE sales_date;

--
-- Test: merge partitions of partitioned table with triggers
--
CREATE TABLE salesmans(salesman_id INT PRIMARY KEY, salesman_name VARCHAR(30)) PARTITION BY RANGE (salesman_id);

CREATE TABLE salesmans01_10 PARTITION OF salesmans FOR VALUES FROM (1) TO (10);
CREATE TABLE salesmans10_20 PARTITION OF salesmans FOR VALUES FROM (10) TO (20);
CREATE TABLE salesmans20_30 PARTITION OF salesmans FOR VALUES FROM (20) TO (30);
CREATE TABLE salesmans30_40 PARTITION OF salesmans FOR VALUES FROM (30) TO (40);

INSERT INTO salesmans VALUES (1,  'Poirot');

CREATE OR REPLACE FUNCTION after_insert_row_trigger() RETURNS trigger LANGUAGE 'plpgsql' AS $BODY$
BEGIN
	RAISE NOTICE 'trigger(%) called: action = %, when = %, level = %', TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
    RETURN NULL;
END;
$BODY$;

CREATE TRIGGER salesmans_after_insert_statement_trigger
    AFTER INSERT
    ON salesmans
    FOR EACH STATEMENT
    EXECUTE PROCEDURE after_insert_row_trigger('salesmans');

CREATE TRIGGER salesmans_after_insert_row_trigger
    AFTER INSERT
    ON salesmans
    FOR EACH ROW
    EXECUTE PROCEDURE after_insert_row_trigger('salesmans');

-- 2 triggers should fire here (row + statement):
INSERT INTO salesmans VALUES (10, 'May');
-- 1 trigger should fire here (row):
INSERT INTO salesmans10_20 VALUES (19, 'Ivanov');

ALTER TABLE salesmans MERGE PARTITIONS (salesmans10_20, salesmans20_30, salesmans30_40) INTO salesmans10_40;

-- 2 triggers should fire here (row + statement):
INSERT INTO salesmans VALUES (20, 'Smirnoff');
-- 1 trigger should fire here (row):
INSERT INTO salesmans10_40 VALUES (30, 'Ford');

SELECT * FROM salesmans01_10;
SELECT * FROM salesmans10_40;

DROP TABLE salesmans;
DROP FUNCTION after_insert_row_trigger();

--
-- Test: merge partitions with deleted columns
--
CREATE TABLE salesmans(salesman_id INT PRIMARY KEY, salesman_name VARCHAR(30)) PARTITION BY RANGE (salesman_id);

CREATE TABLE salesmans01_10 PARTITION OF salesmans FOR VALUES FROM (1) TO (10);
-- Create partitions with some deleted columns:
CREATE TABLE salesmans10_20(d1 VARCHAR(30), salesman_id INT PRIMARY KEY, salesman_name VARCHAR(30));
CREATE TABLE salesmans20_30(salesman_id INT PRIMARY KEY, d2 INT,  salesman_name VARCHAR(30));
CREATE TABLE salesmans30_40(salesman_id INT PRIMARY KEY, d3 DATE, salesman_name VARCHAR(30));

INSERT INTO salesmans10_20 VALUES ('dummy value 1', 19, 'Ivanov');
INSERT INTO salesmans20_30 VALUES (20, 101, 'Smirnoff');
INSERT INTO salesmans30_40 VALUES (31, now(), 'Popov');

ALTER TABLE salesmans10_20 DROP COLUMN d1;
ALTER TABLE salesmans20_30 DROP COLUMN d2;
ALTER TABLE salesmans30_40 DROP COLUMN d3;

ALTER TABLE salesmans ATTACH PARTITION salesmans10_20 FOR VALUES FROM (10) TO (20);
ALTER TABLE salesmans ATTACH PARTITION salesmans20_30 FOR VALUES FROM (20) TO (30);
ALTER TABLE salesmans ATTACH PARTITION salesmans30_40 FOR VALUES FROM (30) TO (40);

INSERT INTO salesmans VALUES (1, 'Poirot');
INSERT INTO salesmans VALUES (10, 'May');
INSERT INTO salesmans VALUES (30, 'Ford');

ALTER TABLE salesmans MERGE PARTITIONS (salesmans10_20, salesmans20_30, salesmans30_40) INTO salesmans10_40;

select * from salesmans;
select * from salesmans01_10;
select * from salesmans10_40;

DROP TABLE salesmans;

--
-- Test: merge sub-partitions
--
CREATE TABLE sales_range (salesman_id INT, salesman_name VARCHAR(30), sales_amount INT, sales_date DATE) PARTITION BY RANGE (sales_date);
CREATE TABLE sales_jan2022 PARTITION OF sales_range FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE sales_feb2022 PARTITION OF sales_range FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE sales_mar2022 PARTITION OF sales_range FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');

CREATE TABLE sales_apr2022 (salesman_id INT, salesman_name VARCHAR(30), sales_amount INT, sales_date DATE) PARTITION BY RANGE (sales_date);
CREATE TABLE sales_apr2022_01_10 PARTITION OF sales_apr2022 FOR VALUES FROM ('2022-04-01') TO ('2022-04-10');
CREATE TABLE sales_apr2022_10_20 PARTITION OF sales_apr2022 FOR VALUES FROM ('2022-04-10') TO ('2022-04-20');
CREATE TABLE sales_apr2022_20_30 PARTITION OF sales_apr2022 FOR VALUES FROM ('2022-04-20') TO ('2022-05-01');
ALTER TABLE sales_range ATTACH PARTITION sales_apr2022 FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');

CREATE TABLE sales_others PARTITION OF sales_range DEFAULT;

CREATE INDEX sales_range_sales_date_idx ON sales_range USING btree (sales_date);

INSERT INTO sales_range VALUES (1,  'May',      1000, '2022-01-31');
INSERT INTO sales_range VALUES (2,  'Smirnoff', 500,  '2022-02-10');
INSERT INTO sales_range VALUES (3,  'Ford',     2000, '2022-04-30');
INSERT INTO sales_range VALUES (4,  'Ivanov',   750,  '2022-04-13');
INSERT INTO sales_range VALUES (5,  'Deev',     250,  '2022-04-07');
INSERT INTO sales_range VALUES (6,  'Poirot',   150,  '2022-02-11');
INSERT INTO sales_range VALUES (7,  'Li',       175,  '2022-03-08');
INSERT INTO sales_range VALUES (8,  'Ericsson', 185,  '2022-02-23');
INSERT INTO sales_range VALUES (9,  'Muller',   250,  '2022-03-11');
INSERT INTO sales_range VALUES (10, 'Halder',   350,  '2022-01-28');
INSERT INTO sales_range VALUES (11, 'Trump',    380,  '2022-04-06');
INSERT INTO sales_range VALUES (12, 'Plato',    350,  '2022-03-19');
INSERT INTO sales_range VALUES (13, 'Gandi',    377,  '2022-01-09');
INSERT INTO sales_range VALUES (14, 'Smith',    510,  '2022-05-04');

SELECT * FROM sales_range;
SELECT * FROM sales_apr2022;
SELECT * FROM sales_apr2022_01_10;
SELECT * FROM sales_apr2022_10_20;
SELECT * FROM sales_apr2022_20_30;

ALTER TABLE sales_apr2022 MERGE PARTITIONS (sales_apr2022_01_10, sales_apr2022_10_20, sales_apr2022_20_30) INTO sales_apr_all;

SELECT * FROM sales_range;
SELECT * FROM sales_apr2022;
SELECT * FROM sales_apr_all;

DROP TABLE sales_range;

--
-- BY LIST partitioning
--

--
-- Test: specific errors for BY LIST partitioning
--
CREATE TABLE sales_list
(salesman_id INT GENERATED ALWAYS AS IDENTITY,
 salesman_name VARCHAR(30),
 sales_state VARCHAR(20),
 sales_amount INT,
 sales_date DATE)
PARTITION BY LIST (sales_state);
CREATE TABLE sales_nord PARTITION OF sales_list FOR VALUES IN ('Murmansk', 'St. Petersburg', 'Ukhta');
CREATE TABLE sales_west PARTITION OF sales_list FOR VALUES IN ('Voronezh', 'Smolensk', 'Bryansk');
CREATE TABLE sales_east PARTITION OF sales_list FOR VALUES IN ('Magadan', 'Khabarovsk', 'Vladivostok');
CREATE TABLE sales_central PARTITION OF sales_list FOR VALUES IN ('Moscow', 'Kazan', 'Volgograd');
CREATE TABLE sales_others PARTITION OF sales_list DEFAULT;


CREATE TABLE sales_list2 (LIKE sales_list) PARTITION BY LIST (sales_state);
CREATE TABLE sales_nord2 PARTITION OF sales_list2 FOR VALUES IN ('Murmansk', 'St. Petersburg', 'Ukhta');
CREATE TABLE sales_others2 PARTITION OF sales_list2 DEFAULT;


CREATE TABLE sales_external (LIKE sales_list);
CREATE TABLE sales_external2 (vch VARCHAR(5));

-- ERROR:  partition bound for relation "sales_external" is null
ALTER TABLE sales_list MERGE PARTITIONS (sales_west, sales_east, sales_external) INTO sales_all;
-- ERROR:  partition bound for relation "sales_external2" is null
ALTER TABLE sales_list MERGE PARTITIONS (sales_west, sales_east, sales_external2) INTO sales_all;
-- ERROR:  relation "sales_nord2" is not a partition of relation "sales_list"
ALTER TABLE sales_list MERGE PARTITIONS (sales_west, sales_nord2, sales_east) INTO sales_all;

DROP TABLE sales_external2;
DROP TABLE sales_external;
DROP TABLE sales_list2;
DROP TABLE sales_list;

--
-- Test: BY LIST partitioning, MERGE PARTITIONS with data
--
CREATE TABLE sales_list
(salesman_id INT GENERATED ALWAYS AS IDENTITY,
 salesman_name VARCHAR(30),
 sales_state VARCHAR(20),
 sales_amount INT,
 sales_date DATE)
PARTITION BY LIST (sales_state);

CREATE INDEX sales_list_salesman_name_idx ON sales_list USING btree (salesman_name);
CREATE INDEX sales_list_sales_state_idx ON sales_list USING btree (sales_state);

CREATE TABLE sales_nord PARTITION OF sales_list FOR VALUES IN ('Murmansk', 'St. Petersburg', 'Ukhta');
CREATE TABLE sales_west PARTITION OF sales_list FOR VALUES IN ('Voronezh', 'Smolensk', 'Bryansk');
CREATE TABLE sales_east PARTITION OF sales_list FOR VALUES IN ('Magadan', 'Khabarovsk', 'Vladivostok');
CREATE TABLE sales_central PARTITION OF sales_list FOR VALUES IN ('Moscow', 'Kazan', 'Volgograd');
CREATE TABLE sales_others PARTITION OF sales_list DEFAULT;

INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Trump',    'Magadan',        1000, '2022-03-01');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Smirnoff', 'Smolensk',       500,  '2022-03-03');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Ford',     'St. Petersburg', 2000, '2022-03-05');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Ivanov',   'Moscow',         750,  '2022-03-04');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Deev',     'Voronezh',       250,  '2022-03-07');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Poirot',   'Kazan',          1000, '2022-03-01');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('May',      'Ukhta',          1200, '2022-03-06');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Li',       'Vladivostok',    1150, '2022-03-09');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('May',      'Ukhta',          1200, '2022-03-11');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Halder',   'Murmansk',       800,  '2022-03-02');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Muller',   'Bryansk',        650,  '2022-03-05');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Smith',    'Volgograd',      350,  '2022-03-10');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Gandi',    'Moscow',         150,  '2022-03-08');
INSERT INTO sales_list (salesman_name, sales_state, sales_amount, sales_date) VALUES ('Plato',    'Voronezh',       950,  '2022-03-05');

-- show partitions with conditions:
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_list'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

ALTER TABLE sales_list MERGE PARTITIONS (sales_west, sales_east, sales_central) INTO sales_all;

-- show partitions with conditions:
SELECT c.oid::pg_catalog.regclass, c.relkind, inhdetachpending, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
  FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
  WHERE c.oid = i.inhrelid AND i.inhparent = 'sales_list'::regclass
  ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.oid::pg_catalog.regclass::pg_catalog.text;

SELECT * FROM sales_list;
SELECT * FROM sales_nord;
SELECT * FROM sales_all;

-- Use indexscan for test indexes after merge partitions
SET enable_indexscan = ON;
SET enable_seqscan = OFF;

SELECT * FROM sales_all WHERE sales_state = 'Moscow';
SELECT * FROM sales_list WHERE sales_state = 'Moscow';
SELECT * FROM sales_list WHERE salesman_name = 'Ivanov';

SET enable_indexscan = ON;
SET enable_seqscan = ON;

DROP TABLE sales_list;

--
DROP SCHEMA partitions_merge_schema;
