-- Tests for UPDATE/DELETE FOR PORTION OF

-- Works on non-PK columns
CREATE TABLE for_portion_of_test (
  id int4range,
  valid_at tsrange,
  name text NOT NULL
);
INSERT INTO for_portion_of_test VALUES
('[1,1]', '[2018-01-02,2020-01-01)', 'one');

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO '2019-01-01'
SET name = 'foo';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2019-01-15' TO NULL;

SELECT * FROM for_portion_of_test;

-- Works on more than one range
DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range,
  valid1_at tsrange,
  valid2_at tsrange,
  name text NOT NULL
);
INSERT INTO for_portion_of_test VALUES
('[1,1]', '[2018-01-02,2018-02-03)', '[2015-01-01,2025-01-01)', 'one');

UPDATE for_portion_of_test
FOR PORTION OF valid1_at FROM '2018-01-15' TO NULL
SET name = 'foo';
SELECT * FROM for_portion_of_test;

UPDATE for_portion_of_test
FOR PORTION OF valid2_at FROM '2018-01-15' TO NULL
SET name = 'bar';
SELECT * FROM for_portion_of_test;

DELETE FROM for_portion_of_test
FOR PORTION OF valid1_at FROM '2018-01-20' TO NULL;
SELECT * FROM for_portion_of_test;

DELETE FROM for_portion_of_test
FOR PORTION OF valid2_at FROM '2018-01-20' TO NULL;
SELECT * FROM for_portion_of_test;

-- Test with NULLs in the scalar/range key columns.
-- This won't happen if there is a PRIMARY KEY or UNIQUE constraint
-- but FOR PORTION OF shouldn't require that.
DROP TABLE for_portion_of_test;
CREATE UNLOGGED TABLE for_portion_of_test (
  id int4range,
  valid_at tsrange,
  name text
);
INSERT INTO for_portion_of_test VALUES
  ('[1,1]', NULL, '1 null'),
  ('[1,1]', '(,)', '1 unbounded'),
  ('[1,1]', 'empty', '1 empty'),
  (NULL, NULL, NULL),
  (NULL, tsrange('2018-01-01', '2019-01-01'), 'null key');
UPDATE for_portion_of_test
  FOR PORTION OF valid_at FROM NULL TO NULL
  SET name = 'NULL to NULL';
SELECT * FROM for_portion_of_test;

DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range NOT NULL,
  valid_at tsrange NOT NULL,
  name text NOT NULL,
	CONSTRAINT for_portion_of_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test
VALUES
('[1,1]', '[2018-01-02,2018-02-03)', 'one'),
('[1,1]', '[2018-02-03,2018-03-03)', 'one'),
('[1,1]', '[2018-03-03,2018-04-04)', 'one'),
('[2,2]', '[2018-01-01,2018-01-05)', 'two'),
('[3,3]', '[2018-01-01,)', 'three'),
('[4,4]', '(,2018-04-01)', 'four'),
('[5,5]', '(,)', 'five')
;

--
-- UPDATE tests
--

-- Setting with a missing column fails
UPDATE for_portion_of_test
FOR PORTION OF invalid_at FROM '2018-06-01' TO NULL
SET name = 'foo'
WHERE id = '[5,5]';

-- Setting the range fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO NULL
SET valid_at = '[1990-01-01,1999-01-01)'
WHERE id = '[5,5]';

-- The wrong type fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM 1 TO 4
SET name = 'nope'
WHERE id = '[3,3]';

-- Setting with timestamps reversed fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2018-01-01'
SET name = 'three^1'
WHERE id = '[3,3]';

-- Setting with a subquery fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM (SELECT '2018-01-01') TO '2018-06-01'
SET name = 'nope'
WHERE id = '[3,3]';

-- Setting with a column fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM lower(valid_at) TO NULL
SET name = 'nope'
WHERE id = '[3,3]';

-- Setting with timestamps equal does nothing
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
SET name = 'three^0'
WHERE id = '[3,3]';

-- Updating a finite/open portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO NULL
SET name = 'three^1'
WHERE id = '[3,3]';

-- Updating a finite/open portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-03-01'
SET name = 'three^2'
WHERE id = '[3,3]';

-- Updating an open/finite portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-02-01'
SET name = 'four^1'
WHERE id = '[4,4]';

-- Updating an open/finite portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO NULL
SET name = 'four^2'
WHERE id = '[4,4]';

-- Updating a finite/finite portion with an exact fit
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2018-02-01'
SET name = 'four^3'
WHERE id = '[4,4]';

-- Updating an enclosed span
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'two^2'
WHERE id = '[2,2]';

-- Updating an open/open portion with a finite/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-01' TO '2019-01-01'
SET name = 'five^2'
WHERE id = '[5,5]';

-- Updating an enclosed span with separate protruding spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2020-01-01'
SET name = 'five^3'
WHERE id = '[5,5]';

-- Updating multiple enclosed spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'one^2'
WHERE id = '[1,1]';

-- Updating with a shift/reduce conflict
UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '1' HOUR TO MINUTE
  TO '2019-01-01'
SET name = 'one^3'
WHERE id = '[1,1]';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '2' HOUR
  TO '2019-01-01'
SET name = 'one^4'
WHERE id = '[1,1]';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM ('2018-03-01' AT TIME ZONE INTERVAL '2' HOUR)
  TO '2019-01-01'
SET name = 'one^4'
WHERE id = '[1,1]';

-- Updating the non-range part of the PK:
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-02-15' TO NULL
SET id = '[6,6]'
WHERE id = '[1,1]';

-- UPDATE with no WHERE clause
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2030-01-01' TO NULL
SET name = name || '*';

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

--
-- DELETE tests
--

-- Deleting with a missing column fails
DELETE FROM for_portion_of_test
FOR PORTION OF invalid_at FROM '2018-06-01' TO NULL
WHERE id = '[5,5]';

-- Deleting with timestamps reversed fails
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2018-01-01'
WHERE id = '[3,3]';

-- Deleting with timestamps equal does nothing
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
WHERE id = '[3,3]';

-- Deleting with a closed/closed target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO '2020-06-01'
WHERE id = '[5,5]';

-- Deleting with a closed/open target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO NULL
WHERE id = '[3,3]';

-- Deleting with an open/closed target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO '2018-02-08'
WHERE id = '[1,1]';

-- Deleting with an open/open target
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
WHERE id = '[6,6]';

-- DELETE with no WHERE clause
DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2025-01-01' TO NULL;

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

-- UPDATE ... RETURNING returns only the updated values (not the inserted side values)
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-02-01' TO '2018-02-15'
SET name = 'three^3'
WHERE id = '[3,3]'
RETURNING *;

-- test that we run triggers on the UPDATE/DELETEd row and the INSERTed rows

CREATE FUNCTION for_portion_of_trigger()
RETURNS trigger
AS
$$
BEGIN
  RAISE NOTICE '% % % of %', TG_WHEN, TG_OP, NEW.valid_at, OLD.valid_at;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_for_portion_of_before_insert
  BEFORE INSERT ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_after_insert
  AFTER INSERT ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_before_update
  BEFORE UPDATE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_after_update
  AFTER UPDATE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_before_delete
  BEFORE DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();
CREATE TRIGGER trg_for_portion_of_after_delete
  AFTER DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger();

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2021-01-01' TO '2022-01-01'
SET name = 'five^4'
WHERE id = '[5,5]';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2023-01-01' TO '2024-01-01'
WHERE id = '[5,5]';

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

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
('[1,1]', '[2018-01-02,2018-02-03)', 'one'),
('[1,1]', '[2018-02-03,2018-03-03)', 'one'),
('[1,1]', '[2018-03-03,2018-04-04)', 'one'),
('[2,2]', '[2018-01-01,2018-05-01)', 'two'),
('[3,3]', '[2018-01-01,)', 'three');
;

UPDATE for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-10' TO '2018-02-10'
SET name = 'one^1'
WHERE id = '[1,1]';

DELETE FROM for_portion_of_test2
FOR PORTION OF valid_at FROM '2018-01-15' TO '2018-02-15'
WHERE id = '[2,2]';

SELECT * FROM for_portion_of_test2 ORDER BY id, valid_at;

DROP TABLE for_portion_of_test2;
DROP TYPE mydaterange;
