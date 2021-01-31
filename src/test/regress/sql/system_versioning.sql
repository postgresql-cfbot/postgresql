-- CREATE TABLE
-- invalid datatype
CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp GENERATED ALWAYS AS ROW START,
    end_timestamp integer GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

-- references to other column in period columns
CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS ROW START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (a, end_timestamp)
) WITH SYSTEM VERSIONING;

CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS ROW START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (start_timestamp, a)
) WITH SYSTEM VERSIONING;

CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS ROW START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (end_timestamp, start_timestamp)
) WITH SYSTEM VERSIONING;

-- duplicate system time column
CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS row START,
    start_timestamp1 timestamp with time zone GENERATED ALWAYS AS row START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

CREATE TABLE stest1 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS row START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    end_timestamp1 timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

-- success
CREATE TABLE stest0 (
    a integer PRIMARY KEY,
    start_timestamp timestamp with time zone GENERATED ALWAYS AS ROW START,
    end_timestamp timestamp with time zone GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

-- default system time column usage
CREATE TABLE stest2 (
    a integer
) WITH SYSTEM VERSIONING;

\d stest2

-- ALTER TABLE tbName ADD SYSTEM VERSIONING
CREATE TABLE stest3 (
    a integer
);

\d stest3

ALTER TABLE stest3 ADD SYSTEM VERSIONING;

\d stest3

-- ALTER TABLE tbName DROP SYSTEM VERSIONING
ALTER TABLE stest3 DROP SYSTEM VERSIONING;

\d stest3

-- ALTER TABLE
-- Don't test DROP COLUMN at present because of klugey way we ADD SYSTEM VERSIONING
--ALTER TABLE stest0 DROP COLUMN start_timestamp;
ALTER TABLE stest0 ALTER start_timestamp DROP NOT NULL;
ALTER TABLE stest0 ALTER COLUMN start_timestamp SET DATA TYPE character;

--truncation not allowed
truncate table stest0;

DROP TABLE stest2, stest3;

-- test DML
INSERT INTO stest0 VALUES (1);
INSERT INTO stest0 VALUES (2);
INSERT INTO stest0 VALUES (3);
UPDATE stest0 SET a = 4 WHERE a = 1;
DELETE FROM stest0 WHERE a = 2;
INSERT INTO stest0 VALUES (5);

-- working example
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric
) WITH SYSTEM VERSIONING;
\d products

-- test DML
INSERT INTO products VALUES (100, 'Washing Machine', 300.0);
INSERT INTO products VALUES (200, 'Extended Warranty', 50.0);
INSERT INTO products VALUES (300, 'Laptop', 250.0);
SELECT now() AS ts1 \gset

SELECT product_no, price FROM products ORDER BY product_no;
SELECT product_no FROM products FOR system_time FROM '-infinity' TO 'infinity' ORDER BY product_no;

UPDATE products SET price = 75.0 WHERE product_no = 200;
UPDATE products SET price = 350.0 WHERE product_no = 300;
UPDATE products SET product_no = 400 WHERE product_no = 100;
SELECT now() AS ts2 \gset

SELECT product_no, price FROM products ORDER BY product_no;
SELECT product_no FROM products FOR system_time FROM '-infinity' TO 'infinity' ORDER BY product_no;

DELETE FROM products WHERE product_no = 300;
SELECT now() AS ts3 \gset

SELECT product_no, price FROM products ORDER BY product_no;
SELECT product_no FROM products FOR system_time FROM '-infinity' TO 'infinity' ORDER BY product_no;

INSERT INTO products VALUES (500, 'Spare Parts', 25.0);

SELECT product_no, price FROM products ORDER BY product_no;
SELECT product_no FROM products FOR system_time FROM '-infinity' TO 'infinity' ORDER BY product_no;

-- cannot update system versioning timestamps
UPDATE products SET start_timestamp = now();
-- these should fail... but currently succeed
--UPDATE products SET start_timestamp = default;
--UPDATE products SET end_timestamp = default;

/*
 * Temporal Queries
 */

-- AS OF ...
SELECT product_no, price FROM products
FOR system_time AS OF :'ts1'
ORDER BY product_no, start_timestamp;

SELECT product_no, price FROM products
FOR system_time AS OF :'ts2'
ORDER BY product_no, start_timestamp;

SELECT product_no, price FROM products
FOR system_time AS OF :'ts3'
ORDER BY product_no, start_timestamp;

-- BETWEEN ... AND ...
SELECT product_no, price FROM products
FOR system_time BETWEEN :'ts1' AND :'ts2'
ORDER BY product_no, start_timestamp;

SELECT product_no, price FROM products
FOR system_time BETWEEN :'ts1' AND :'ts3'
ORDER BY product_no, start_timestamp;

SELECT product_no, price FROM products
FOR system_time BETWEEN :'ts1' AND :'ts3'
ORDER BY product_no, start_timestamp;

/*
 * JOINS
 */

CREATE TABLE stestx (x int, y int);
INSERT INTO stestx VALUES (11, 1), (22, 2), (33, 3);

SELECT a, x, y
FROM stestx
INNER JOIN stest0 ON stestx.y = stest0.a;

SELECT a, x, y
FROM stestx
LEFT OUTER JOIN stest0 ON stestx.y = stest0.a;

SELECT a, x, y
FROM stestx
RIGHT OUTER JOIN stest0 ON stestx.y = stest0.a;

SELECT a, x, y
FROM stestx
FULL OUTER JOIN stest0 ON stestx.y = stest0.a;

DROP TABLE stestx;

-- views
CREATE VIEW stest1v AS SELECT a FROM stest0;
CREATE VIEW stest2v AS select a from stest0 for system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a;
SELECT * FROM stest1v;
SELECT * FROM stest2v;

DROP VIEW stest1v;
DROP VIEW stest2v;
-- CTEs
WITH foo AS (SELECT a FROM stest0) SELECT * FROM foo;

WITH foo AS (select a from stest0 for system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a) SELECT * FROM foo;

-- inheritance
CREATE TABLE stest1 () INHERITS (stest0);
SELECT * FROM stest1;

\d stest1

INSERT INTO stest1 VALUES (4);
SELECT a FROM stest1;

DROP TABLE stest0, stest1;
