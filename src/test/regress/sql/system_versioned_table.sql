/*
 * CREATE TABLE
 */

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
ALTER TABLE stest0 ALTER start_timestamp DROP NOT NULL;

ALTER TABLE stest0 ALTER start_timestamp DROP NOT NULL;

ALTER TABLE stest0 ALTER COLUMN start_timestamp SET DATA TYPE character;



--truncation
truncate table stest0;

-- test UPDATE/DELETE
INSERT INTO stest0 VALUES (1);
INSERT INTO stest0 VALUES (2);
INSERT INTO stest0 VALUES (3);
SELECT now() AS ts1 \gset

SELECT a FROM stest0 ORDER BY a;
SELECT a FROM stest0 FOR system_time FROM '-infinity' TO 'infinity' ORDER BY a;

UPDATE stest0 SET a = 4 WHERE a = 1;
SELECT now() AS ts2 \gset

SELECT a FROM stest0 ORDER BY a;
SELECT a FROM stest0 FOR system_time FROM '-infinity' TO 'infinity' ORDER BY a;

DELETE FROM stest0 WHERE a = 2;
SELECT now() AS ts3 \gset

SELECT a FROM stest0 ORDER BY a;
SELECT a FROM stest0 FOR system_time FROM '-infinity' TO 'infinity' ORDER BY a;

INSERT INTO stest0 VALUES (5);

SELECT a FROM stest0 ORDER BY a;
SELECT a FROM stest0 FOR system_time FROM '-infinity' TO 'infinity' ORDER BY a;

/*
 * Temporal Queries
 */

-- AS OF ...
SELECT a FROM stest0 FOR system_time AS OF :'ts1' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time AS OF :'ts2' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time AS OF :'ts3' ORDER BY start_timestamp, a;

-- BETWEEN ... AND ...
SELECT a FROM stest0 FOR system_time BETWEEN :'ts1' AND :'ts2' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN :'ts2' AND :'ts3' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN :'ts1' AND :'ts3' ORDER BY start_timestamp, a;

-- BETWEEN ASYMMETRIC ... AND ...
SELECT a FROM stest0 FOR system_time BETWEEN ASYMMETRIC :'ts1' AND :'ts2' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN ASYMMETRIC :'ts2' AND :'ts3' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN ASYMMETRIC :'ts1' AND :'ts3' ORDER BY start_timestamp, a;

-- BETWEEN SYMMETRIC ... AND ...
SELECT a FROM stest0 FOR system_time BETWEEN SYMMETRIC :'ts2' AND :'ts1' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN SYMMETRIC :'ts3' AND :'ts2' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time BETWEEN SYMMETRIC :'ts3' AND :'ts1' ORDER BY start_timestamp, a;

-- FROM ... TO ...
SELECT a FROM stest0 FOR system_time FROM :'ts1' TO :'ts2' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time FROM :'ts2' TO :'ts3' ORDER BY start_timestamp, a;
SELECT a FROM stest0 FOR system_time FROM :'ts1' TO :'ts3' ORDER BY start_timestamp, a;

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
