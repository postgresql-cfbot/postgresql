
CREATE TABLE stest0(a integer PRIMARY KEY, start_timestamp TIMESTAMP(6) GENERATED ALWAYS AS row START,
   end_timestamp TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
   PERIOD FOR SYSTEM_TIME(start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

--invalid datatype
CREATE TABLE stest1(a integer PRIMARY KEY, start_timestamp TIMESTAMP(6) GENERATED ALWAYS AS row START,
   end_timestamp integer GENERATED ALWAYS AS ROW END,
   PERIOD FOR SYSTEM_TIME(start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

-- references to other column in period columns
CREATE TABLE stest1(a integer PRIMARY KEY, start_timestamp TIMESTAMP(6) GENERATED ALWAYS AS row START,
   end_timestamp TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
   PERIOD FOR SYSTEM_TIME(a, end_timestamp)
) WITH SYSTEM VERSIONING;

-- duplicate system time column
CREATE TABLE stest1(a integer PRIMARY KEY, start_timestamp TIMESTAMP(6) GENERATED ALWAYS AS row START,
   end_timestamp TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
   end_timestamp1 TIMESTAMP(6) GENERATED ALWAYS AS ROW END,
   PERIOD FOR SYSTEM_TIME(start_timestamp, end_timestamp)
) WITH SYSTEM VERSIONING;

-- default system time column usage
CREATE TABLE stest2(a integer
) WITH SYSTEM VERSIONING;

\d stest2

-- ALTER TABLE tbName ADD SYSTEM VERSIONING
CREATE TABLE stest3(a integer
);

\d stest3

ALTER TABLE stest3 ADD SYSTEM VERSIONING;

\d stest3

-- ALTER TABLE tbName DROP SYSTEM VERSIONING
ALTER TABLE stest3 DROP SYSTEM VERSIONING;

\d stest3

-- ALTER TABLE
ALTER TABLE stest0 ALTER start_timestamp drop not null;

ALTER TABLE stest0 ALTER start_timestamp drop not null;

ALTER TABLE stest0 ALTER COLUMN start_timestamp SET DATA TYPE char;



--truncation
truncate table stest0;

-- test UPDATE/DELETE
INSERT INTO stest0 VALUES (1);
INSERT INTO stest0 VALUES (2);
INSERT INTO stest0 VALUES (3);

SELECT a FROM stest0 ORDER BY a;

UPDATE stest0 SET a = 4 where a = 1;

SELECT a FROM stest0 ORDER BY a;

select a from for stest0 system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a;

DELETE FROM stest0 WHERE a = 2;

SELECT a FROM stest0 ORDER BY a;
select a from for stest0 system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a;

-- test with joins
CREATE TABLE stestx (x int, y int);
INSERT INTO stestx VALUES (11, 1), (22, 2), (33, 3);
SELECT a FROM stestx, stest0 WHERE stestx.y = stest0.a;

DROP TABLE stestx;

-- views
CREATE VIEW stest1v AS SELECT a FROM stest0;
CREATE VIEW stest2v AS select a from for stest0 system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a;
SELECT * FROM stest1v;
SELECT * FROM stest2v;

DROP VIEW stest1v;
DROP VIEW stest2v;
-- CTEs
WITH foo AS (SELECT a FROM stest0) SELECT * FROM foo;

WITH foo AS (select a from for stest0 system_time from '2000-01-01 00:00:00.00000' to 'infinity' ORDER BY a) SELECT * FROM foo;

-- inheritance
CREATE TABLE stest1 () INHERITS (stest0);
SELECT * FROM stest1;

\d stest1

INSERT INTO stest1 VALUES (4);
SELECT a FROM stest1;
