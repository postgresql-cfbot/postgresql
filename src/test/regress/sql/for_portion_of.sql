-- Tests for UPDATE/DELETE FOR PORTION OF

SET datestyle TO ISO, YMD;

-- Works on non-PK columns
CREATE TABLE for_portion_of_test (
  id int4range,
  valid_at daterange,
  name text NOT NULL
);
INSERT INTO for_portion_of_test VALUES
('[1,2)', '[2018-01-02,2020-01-01)', 'one');

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO '2019-01-01'
SET name = 'one^1';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2019-01-15' TO '2019-01-20';

-- With a table alias with AS

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2019-02-01' TO '2019-02-03' AS t
SET name = 'one^2';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2019-02-03' TO '2019-02-04' AS t;

-- With a table alias without AS

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2019-02-04' TO '2019-02-05' t
SET name = 'one^3';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2019-02-05' TO '2019-02-06' t;

-- UPDATE with FROM

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2019-03-01' to '2019-03-02'
SET name = 'one^4'
FROM (SELECT '[1,2)'::int4range) AS t2(id)
WHERE for_portion_of_test.id = t2.id;

-- DELETE with USING

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2019-03-02' TO '2019-03-03'
USING (SELECT '[1,2)'::int4range) AS t2(id)
WHERE for_portion_of_test.id = t2.id;

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

-- Works on more than one range
DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range,
  valid1_at daterange,
  valid2_at daterange,
  name text NOT NULL
);
INSERT INTO for_portion_of_test VALUES
('[1,2)', '[2018-01-02,2018-02-03)', '[2015-01-01,2025-01-01)', 'one');

UPDATE for_portion_of_test
FOR PORTION OF valid1_at FROM '2018-01-15' TO NULL
SET name = 'foo';
SELECT * FROM for_portion_of_test ORDER BY id, valid1_at, valid2_at;

UPDATE for_portion_of_test
FOR PORTION OF valid2_at FROM '2018-01-15' TO NULL
SET name = 'bar';
SELECT * FROM for_portion_of_test ORDER BY id, valid1_at, valid2_at;

DELETE FROM for_portion_of_test
FOR PORTION OF valid1_at FROM '2018-01-20' TO NULL;
SELECT * FROM for_portion_of_test ORDER BY id, valid1_at, valid2_at;

DELETE FROM for_portion_of_test
FOR PORTION OF valid2_at FROM '2018-01-20' TO NULL;
SELECT * FROM for_portion_of_test ORDER BY id, valid1_at, valid2_at;

-- Test with NULLs in the scalar/range key columns.
-- This won't happen if there is a PRIMARY KEY or UNIQUE constraint
-- but FOR PORTION OF shouldn't require that.
DROP TABLE for_portion_of_test;
CREATE UNLOGGED TABLE for_portion_of_test (
  id int4range,
  valid_at daterange,
  name text
);
INSERT INTO for_portion_of_test VALUES
  ('[1,2)', NULL, '1 null'),
  ('[1,2)', '(,)', '1 unbounded'),
  ('[1,2)', 'empty', '1 empty'),
  (NULL, NULL, NULL),
  (NULL, daterange('2018-01-01', '2019-01-01'), 'null key');
UPDATE for_portion_of_test
  FOR PORTION OF valid_at FROM NULL TO NULL
  SET name = 'NULL to NULL';
SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range NOT NULL,
  valid_at daterange NOT NULL,
  name text NOT NULL,
  CONSTRAINT for_portion_of_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test
VALUES
('[1,2)', '[2018-01-02,2018-02-03)', 'one'),
('[1,2)', '[2018-02-03,2018-03-03)', 'one'),
('[1,2)', '[2018-03-03,2018-04-04)', 'one'),
('[2,3)', '[2018-01-01,2018-01-05)', 'two'),
('[3,4)', '[2018-01-01,)', 'three'),
('[4,5)', '(,2018-04-01)', 'four'),
('[5,6)', '(,)', 'five')
;

--
-- UPDATE tests
--

-- Setting with a missing column fails
UPDATE for_portion_of_test
FOR PORTION OF invalid_at FROM '2018-06-01' TO NULL
SET name = 'foo'
WHERE id = '[5,6)';

-- Setting the range fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO NULL
SET valid_at = '[1990-01-01,1999-01-01)'
WHERE id = '[5,6)';

-- The wrong type fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM 1 TO 4
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with timestamps reversed fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2018-01-01'
SET name = 'three^1'
WHERE id = '[3,4)';

-- Setting with a subquery fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM (SELECT '2018-01-01') TO '2018-06-01'
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with a column fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM lower(valid_at) TO NULL
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with timestamps equal does nothing
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
SET name = 'three^0'
WHERE id = '[3,4)';

-- Updating a finite/open portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO NULL
SET name = 'three^1'
WHERE id = '[3,4)';

-- Updating a finite/open portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-03-01'
SET name = 'three^2'
WHERE id = '[3,4)';

-- Updating an open/finite portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-02-01'
SET name = 'four^1'
WHERE id = '[4,5)';

-- Updating an open/finite portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO NULL
SET name = 'four^2'
WHERE id = '[4,5)';

-- Updating a finite/finite portion with an exact fit
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2018-02-01'
SET name = 'four^3'
WHERE id = '[4,5)';

-- Updating an enclosed span
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'two^2'
WHERE id = '[2,3)';

-- Updating an open/open portion with a finite/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-01' TO '2019-01-01'
SET name = 'five^1'
WHERE id = '[5,6)';

-- Updating an enclosed span with separate protruding spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2020-01-01'
SET name = 'five^2'
WHERE id = '[5,6)';

-- Updating multiple enclosed spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'one^2'
WHERE id = '[1,2)';

-- With a direct target
UPDATE for_portion_of_test
FOR PORTION OF valid_at (daterange('2018-03-10', '2018-03-17'))
SET name = 'one^3'
WHERE id = '[1,2)';

-- Updating the non-range part of the PK:
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-02-15' TO NULL
SET id = '[6,7)'
WHERE id = '[1,2)';

-- UPDATE with no WHERE clause
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2030-01-01' TO NULL
SET name = name || '*';

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

-- Updating with a shift/reduce conflict
-- (requires a tsrange column)
CREATE UNLOGGED TABLE for_portion_of_test2 (
  id int4range,
  valid_at tsrange,
  name text
);
INSERT INTO for_portion_of_test2 (id, valid_at, name)
  VALUES ('[1,2)', '[2000-01-01,2020-01-01)', 'one');
-- updates [2011-03-01 01:02:00, 2012-01-01) (note 2 minutes)
UPDATE for_portion_of_test2
FOR PORTION OF valid_at
  FROM '2011-03-01'::timestamp + INTERVAL '1:02:03' HOUR TO MINUTE
  TO '2012-01-01'
SET name = 'one^1'
WHERE id = '[1,2)';

-- TO is used for the bound but not the INTERVAL:
-- syntax error
UPDATE for_portion_of_test2
FOR PORTION OF valid_at
  FROM '2013-03-01'::timestamp + INTERVAL '1:02:03' HOUR
  TO '2014-01-01'
SET name = 'one^2'
WHERE id = '[1,2)';

-- adding parens fixes it
-- updates [2015-03-01 01:00:00, 2016-01-01) (no minutes)
UPDATE for_portion_of_test2
FOR PORTION OF valid_at
  FROM ('2015-03-01'::timestamp + INTERVAL '1:02:03' HOUR)
  TO '2016-01-01'
SET name = 'one^3'
WHERE id = '[1,2)';

-- FOR PORTION OF allows params in sql:
CREATE FUNCTION fpo_sql_update(target_from date, target_til date)
RETURNS VOID
AS $$
  UPDATE for_portion_of_test2
  FOR PORTION OF valid_at
    FROM $1 TO $2
  SET name = concat(name, '*')
  WHERE id = '[1,2)';
$$
LANGUAGE sql;
SELECT fpo_sql_update('2018-01-01', '2019-01-01');

-- FOR PORTION OF allows params in plpgsql:
CREATE FUNCTION fpo_plpgsql_update(target_from date, target_til date)
RETURNS VOID
AS $$
BEGIN
  UPDATE for_portion_of_test2
  FOR PORTION OF valid_at
    FROM $1 TO $2
  SET name = concat(name, '+')
  WHERE id = '[1,2)';
END;
$$
LANGUAGE plpgsql;
SELECT fpo_plpgsql_update('2018-06-01', '2019-01-01');

SELECT * FROM for_portion_of_test2 ORDER BY id, valid_at;
DROP TABLE for_portion_of_test2;

--
-- DELETE tests
--

-- Deleting with a missing column fails
DELETE FROM for_portion_of_test
FOR PORTION OF invalid_at FROM '2018-06-01' TO NULL
WHERE id = '[5,6)';

-- Deleting with timestamps reversed fails
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2018-01-01'
WHERE id = '[3,4)';

-- Deleting with timestamps equal does nothing
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
WHERE id = '[3,4)';

-- Deleting with a closed/closed target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2020-06-01'
WHERE id = '[5,6)';

-- Deleting with a closed/open target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO NULL
WHERE id = '[3,4)';

-- Deleting with an open/closed target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-02-08'
WHERE id = '[1,2)';

-- Deleting with an open/open target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
WHERE id = '[6,7)';

-- DELETE with no WHERE clause
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2025-01-01' TO NULL;

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

-- UPDATE ... RETURNING returns only the updated values (not the inserted side values)
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-02-01' TO '2018-02-15'
SET name = 'three^3'
WHERE id = '[3,4)'
RETURNING *;

-- test that we run triggers on the UPDATE/DELETEd row and the INSERTed rows

CREATE FUNCTION for_portion_of_trigger()
RETURNS trigger
AS
$$
BEGIN
  RAISE NOTICE '% % % % of %', TG_WHEN, TG_OP, TG_LEVEL, NEW.valid_at, OLD.valid_at;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_for_portion_of_before
  BEFORE INSERT OR UPDATE OR DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_after
  AFTER INSERT OR UPDATE OR DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_before_stmt
  BEFORE INSERT OR UPDATE OR DELETE ON for_portion_of_test
  FOR EACH STATEMENT
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_after_stmt
  AFTER INSERT OR UPDATE OR DELETE ON for_portion_of_test
  FOR EACH STATEMENT
  EXECUTE FUNCTION for_portion_of_trigger();

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2021-01-01' TO '2022-01-01'
SET name = 'five^3'
WHERE id = '[5,6)';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2023-01-01' TO '2024-01-01'
WHERE id = '[5,6)';

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;
DROP FUNCTION for_portion_of_trigger CASCADE;

-- Triggers with a custom transition table name:

DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range,
  valid_at daterange,
  name text
);
INSERT INTO for_portion_of_test VALUES ('[1,2)', '[2018-01-01,2020-01-01)', 'one');

CREATE FUNCTION dump_trigger()
RETURNS TRIGGER LANGUAGE plpgsql AS
$$
BEGIN
  IF TG_OP = 'INSERT' THEN
    RAISE NOTICE '%: % %, NEW table = %',
      TG_NAME, TG_OP, TG_LEVEL, (SELECT string_agg(new_table::text, ', ' ORDER BY id) FROM new_table);
  ELSIF TG_OP = 'UPDATE' THEN
    RAISE NOTICE '%: % %, OLD table = %, NEW table = %',
      TG_NAME, TG_OP, TG_LEVEL,
      (SELECT string_agg(old_table::text, ', ' ORDER BY id) FROM old_table),
      (SELECT string_agg(new_table::text, ', ' ORDER BY id) FROM new_table);
  ELSIF TG_OP = 'DELETE' THEN
    RAISE NOTICE '%: % %, OLD table = %',
      TG_NAME, TG_OP, TG_LEVEL, (SELECT string_agg(old_table::text, ', ' ORDER BY id) FROM old_table);
  END IF;
  RETURN NULL;
END;
$$;

CREATE TRIGGER for_portion_of_test_insert_trig
AFTER INSERT ON for_portion_of_test
REFERENCING NEW TABLE AS new_table
FOR EACH ROW EXECUTE PROCEDURE dump_trigger();

CREATE TRIGGER for_portion_of_test_insert_trig_stmt
AFTER INSERT ON for_portion_of_test
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE PROCEDURE dump_trigger();

CREATE TRIGGER for_portion_of_test_update_trig
AFTER UPDATE ON for_portion_of_test
REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
FOR EACH ROW EXECUTE PROCEDURE dump_trigger();

CREATE TRIGGER for_portion_of_test_update_trig_stmt
AFTER UPDATE ON for_portion_of_test
REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE PROCEDURE dump_trigger();

CREATE TRIGGER for_portion_of_test_delete_trig
AFTER DELETE ON for_portion_of_test
REFERENCING OLD TABLE AS old_table
FOR EACH ROW EXECUTE PROCEDURE dump_trigger();

CREATE TRIGGER for_portion_of_test_delete_trig_stmt
AFTER DELETE ON for_portion_of_test
REFERENCING OLD TABLE AS old_table
FOR EACH STATEMENT EXECUTE PROCEDURE dump_trigger();

BEGIN;
UPDATE for_portion_of_test
  FOR PORTION OF valid_at FROM '2018-01-15' TO '2019-01-01'
  SET name = '2018-01-15_to_2019-01-01';
ROLLBACK;

BEGIN;
DELETE FROM for_portion_of_test
  FOR PORTION OF valid_at FROM NULL TO '2018-01-21';
ROLLBACK;

BEGIN;
UPDATE for_portion_of_test
  FOR PORTION OF valid_at FROM NULL TO '2018-01-02'
  SET name = 'NULL_to_2018-01-01';
ROLLBACK;

-- Test with multiranges

CREATE TABLE for_portion_of_test2 (
  id int4range NOT NULL,
  valid_at datemultirange NOT NULL,
  name text NOT NULL,
  CONSTRAINT for_portion_of_test2_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test2
VALUES
('[1,2)', datemultirange(daterange('2018-01-02', '2018-02-03)'), daterange('2018-02-04', '2018-03-03')), 'one'),
('[1,2)', datemultirange(daterange('2018-03-03', '2018-04-04)')), 'one'),
('[2,3)', datemultirange(daterange('2018-01-01', '2018-05-01)')), 'two'),
('[3,4)', datemultirange(daterange('2018-01-01', null)), 'three');
;

UPDATE for_portion_of_test2
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-10', '2018-02-10'), daterange('2018-03-05', '2018-05-01')))
SET name = 'one^1'
WHERE id = '[1,2)';

DELETE FROM for_portion_of_test2
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-15', '2018-02-15'), daterange('2018-03-01', '2018-03-15')))
WHERE id = '[2,3)';

SELECT * FROM for_portion_of_test2 ORDER BY id, valid_at;

DROP TABLE for_portion_of_test2;

-- Test with PERIODs

CREATE TABLE for_portion_of_test2 (
  id int4range NOT NULL,
  valid_from date,
  valid_til date,
  name text NOT NULL,
  PERIOD FOR valid_at (valid_from, valid_til),
  CONSTRAINT for_portion_of_test2_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test2
VALUES
('[1,2)', '2018-01-02', '2018-02-03', 'one'),
('[1,2)', '2018-02-04', '2018-03-03', 'one'),
('[1,2)', '2018-03-03', '2018-04-04', 'one'),
('[2,3)', '2018-01-01', '2018-05-01', 'two'),
('[3,4)', '2018-01-01', null, 'three');
;

UPDATE for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-10' TO '2018-02-10'
SET name = 'one^1'
WHERE id = '[1,2)';

DELETE FROM for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-15' TO '2018-02-15'
WHERE id = '[2,3)';

SELECT * FROM for_portion_of_test2 ORDER BY id, valid_at;

DROP TABLE for_portion_of_test2;

-- Test with a custom range type

CREATE TYPE mydaterange AS range(subtype=date);

CREATE TABLE for_portion_of_test2 (
  id int4range NOT NULL,
  valid_at mydaterange NOT NULL,
  name text NOT NULL,
  CONSTRAINT for_portion_of_test2_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test2
VALUES
('[1,2)', '[2018-01-02,2018-02-03)', 'one'),
('[1,2)', '[2018-02-03,2018-03-03)', 'one'),
('[1,2)', '[2018-03-03,2018-04-04)', 'one'),
('[2,3)', '[2018-01-01,2018-05-01)', 'two'),
('[3,4)', '[2018-01-01,)', 'three');
;

UPDATE for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-10' TO '2018-02-10'
SET name = 'one^1'
WHERE id = '[1,2)';

DELETE FROM for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-15' TO '2018-02-15'
WHERE id = '[2,3)';

SELECT * FROM for_portion_of_test2 ORDER BY id, valid_at;

DROP TABLE for_portion_of_test2;
DROP TYPE mydaterange;

-- Test FOR PORTION OF against a partitioned table.
-- temporal_partitioned_1 has the same attnums as the root
-- temporal_partitioned_3 has the different attnums from the root
-- temporal_partitioned_5 has the different attnums too, but reversed

CREATE TABLE temporal_partitioned (
  id int4range,
  valid_at daterange,
  name text,
  CONSTRAINT temporal_paritioned_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE temporal_partitioned_1 PARTITION OF temporal_partitioned FOR VALUES IN ('[1,2)', '[2,3)');
CREATE TABLE temporal_partitioned_3 PARTITION OF temporal_partitioned FOR VALUES IN ('[3,4)', '[4,5)');
CREATE TABLE temporal_partitioned_5 PARTITION OF temporal_partitioned FOR VALUES IN ('[5,6)', '[6,7)');

ALTER TABLE temporal_partitioned DETACH PARTITION temporal_partitioned_3;
ALTER TABLE temporal_partitioned_3 DROP COLUMN id, DROP COLUMN valid_at;
ALTER TABLE temporal_partitioned_3 ADD COLUMN id int4range NOT NULL, ADD COLUMN valid_at daterange NOT NULL;
ALTER TABLE temporal_partitioned ATTACH PARTITION temporal_partitioned_3 FOR VALUES IN ('[3,4)', '[4,5)');

ALTER TABLE temporal_partitioned DETACH PARTITION temporal_partitioned_5;
ALTER TABLE temporal_partitioned_5 DROP COLUMN id, DROP COLUMN valid_at;
ALTER TABLE temporal_partitioned_5 ADD COLUMN valid_at daterange NOT NULL, ADD COLUMN id int4range NOT NULL;
ALTER TABLE temporal_partitioned ATTACH PARTITION temporal_partitioned_5 FOR VALUES IN ('[5,6)', '[6,7)');

INSERT INTO temporal_partitioned VALUES
  ('[1,2)', daterange('2000-01-01', '2010-01-01'), 'one'),
  ('[3,4)', daterange('2000-01-01', '2010-01-01'), 'three'),
  ('[5,6)', daterange('2000-01-01', '2010-01-01'), 'five');

SELECT * FROM temporal_partitioned;

-- Update without moving within partition 1
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-03-01' TO '2000-04-01'
  SET name = 'one^1'
  WHERE id = '[1,2)';

-- Update without moving within partition 3
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-03-01' TO '2000-04-01'
  SET name = 'three^1'
  WHERE id = '[3,4)';

-- Update without moving within partition 5
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-03-01' TO '2000-04-01'
  SET name = 'five^1'
  WHERE id = '[5,6)';

-- Move from partition 1 to partition 3
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-06-01' TO '2000-07-01'
  SET name = 'one^2',
      id = '[4,5)'
  WHERE id = '[1,2)';

-- Move from partition 3 to partition 1
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-06-01' TO '2000-07-01'
  SET name = 'three^2',
      id = '[2,3)'
  WHERE id = '[3,4)';

-- Move from partition 5 to partition 3
UPDATE temporal_partitioned FOR PORTION OF valid_at FROM '2000-06-01' TO '2000-07-01'
  SET name = 'five^2',
      id = '[3,4)'
  WHERE id = '[5,6)';

-- Update all partitions at once (each with leftovers)

SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
SELECT * FROM temporal_partitioned_1 ORDER BY id, valid_at;
SELECT * FROM temporal_partitioned_3 ORDER BY id, valid_at;
SELECT * FROM temporal_partitioned_5 ORDER BY id, valid_at;

DROP TABLE temporal_partitioned;

RESET datestyle;
