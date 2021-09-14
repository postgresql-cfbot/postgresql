-- Tests for UPDATE/DELETE FOR PORTION OF

-- Fails on tables without a temporal PK:
CREATE TABLE for_portion_of_test (
  id int4range PRIMARY KEY,
  valid_at tsrange NOT NULL,
  name text NOT NULL
);

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO MAXVALUE
SET name = 'foo';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO MAXVALUE;

DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range NOT NULL,
  valid_at tsrange NOT NULL,
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
FOR PORTION OF invalid_at FROM '2018-06-01' TO MAXVALUE
SET name = 'foo'
WHERE id = '[5,6)';

-- Setting the range fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO MAXVALUE
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

-- Setting with MAXVALUE lower bound fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MAXVALUE TO NULL
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with MINVALUE upper bound fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO MINVALUE
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with a subquery fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM (SELECT '2018-01-01') TO '2018-06-01'
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with a column fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM lower(valid_at) TO MAXVALUE
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with timestamps equal does nothing
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
SET name = 'three^0'
WHERE id = '[3,4)';

-- Updating a finite/open portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO MAXVALUE
SET name = 'three^1'
WHERE id = '[3,4)';

-- Updating a finite/open portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO '2018-03-01'
SET name = 'three^2'
WHERE id = '[3,4)';

-- Updating an open/finite portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO '2018-02-01'
SET name = 'four^1'
WHERE id = '[4,5)';

-- Updating an open/finite portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO MAXVALUE
SET name = 'four^2'
WHERE id = '[4,5)';

-- Updating a finite/finite portion with an exact fit
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2018-02-01'
SET name = 'four^3'
WHERE id = '[4,5)';

-- Updating an enclosed span
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO MAXVALUE
SET name = 'two^2'
WHERE id = '[2,3)';

-- Updating an open/open portion with a finite/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-01' TO '2019-01-01'
SET name = 'five^2'
WHERE id = '[5,6)';

-- Updating an enclosed span with separate protruding spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2020-01-01'
SET name = 'five^3'
WHERE id = '[5,6)';

-- Updating multiple enclosed spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'one^2'
WHERE id = '[1,2)';

-- Updating with a shift/reduce conflict
UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '1' HOUR TO MINUTE
  TO '2019-01-01'
SET name = 'one^3'
WHERE id = '[1,2)';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '2' HOUR
  TO '2019-01-01'
SET name = 'one^4'
WHERE id = '[1,2)';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM ('2018-03-01' AT TIME ZONE INTERVAL '2' HOUR)
  TO '2019-01-01'
SET name = 'one^4'
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
WHERE id = '[5,6)';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2023-01-01' TO '2024-01-01'
WHERE id = '[5,6)';

SELECT * FROM for_portion_of_test ORDER BY id, valid_at;

--
-- Now re-run the same tests but with a PERIOD instead of a range:
--

DROP TABLE for_portion_of_test;

-- Fails on tables without a temporal PK:
CREATE TABLE for_portion_of_test (
  id int4range PRIMARY KEY,
  valid_from timestamp,
  valid_til timestamp,
  PERIOD FOR valid_at (valid_from, valid_til),
  name text NOT NULL
);

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO MAXVALUE
SET name = 'foo';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-15' TO MAXVALUE;

DROP TABLE for_portion_of_test;
CREATE TABLE for_portion_of_test (
  id int4range NOT NULL,
  valid_from timestamp,
  valid_til timestamp,
  PERIOD FOR valid_at (valid_from, valid_til),
  name text NOT NULL,
	CONSTRAINT for_portion_of_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
INSERT INTO for_portion_of_test
VALUES
('[1,2)', '2018-01-02', '2018-02-03', 'one'),
('[1,2)', '2018-02-03', '2018-03-03', 'one'),
('[1,2)', '2018-03-03', '2018-04-04', 'one'),
('[2,3)', '2018-01-01', '2018-01-05', 'two'),
('[3,4)', '2018-01-01', null, 'three'),
('[4,5)', null, '2018-04-01', 'four'),
('[5,6)', null, null, 'five')
;

--
-- UPDATE tests
--

-- Setting with a missing column fails
UPDATE for_portion_of_test
FOR PORTION OF invalid_at FROM '2018-06-01' TO MAXVALUE
SET name = 'foo'
WHERE id = '[5,6)';

-- Setting the start column fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO MAXVALUE
SET valid_from = '1990-01-01'
WHERE id = '[5,6)';

-- Setting the end column fails
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO MAXVALUE
SET valid_til = '1999-01-01'
WHERE id = '[5,6)';

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
FOR PORTION OF valid_at FROM valid_from TO MAXVALUE
SET name = 'nope'
WHERE id = '[3,4)';

-- Setting with timestamps equal does nothing
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-04-01' TO '2018-04-01'
SET name = 'three^0'
WHERE id = '[3,4)';

-- Updating a finite/open portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-06-01' TO MAXVALUE
SET name = 'three^1'
WHERE id = '[3,4)';

-- Updating a finite/open portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO '2018-03-01'
SET name = 'three^2'
WHERE id = '[3,4)';

-- Updating an open/finite portion with an open/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO '2018-02-01'
SET name = 'four^1'
WHERE id = '[4,5)';

-- Updating an open/finite portion with a finite/open target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO MAXVALUE
SET name = 'four^2'
WHERE id = '[4,5)';

-- Updating a finite/finite portion with an exact fit
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2018-02-01'
SET name = 'four^3'
WHERE id = '[4,5)';

-- Updating an enclosed span
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM MINVALUE TO MAXVALUE
SET name = 'two^2'
WHERE id = '[2,3)';

-- Updating an open/open portion with a finite/finite target
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-01-01' TO '2019-01-01'
SET name = 'five^2'
WHERE id = '[5,6)';

-- Updating an enclosed span with separate protruding spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2017-01-01' TO '2020-01-01'
SET name = 'five^3'
WHERE id = '[5,6)';

-- Updating multiple enclosed spans
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM NULL TO NULL
SET name = 'one^2'
WHERE id = '[1,2)';

-- Updating with a shift/reduce conflict
UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '1' HOUR TO MINUTE
  TO '2019-01-01'
SET name = 'one^3'
WHERE id = '[1,2)';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM '2018-03-01' AT TIME ZONE INTERVAL '2' HOUR
  TO '2019-01-01'
SET name = 'one^4'
WHERE id = '[1,2)';

UPDATE for_portion_of_test
FOR PORTION OF valid_at
  FROM ('2018-03-01' AT TIME ZONE INTERVAL '2' HOUR)
  TO '2019-01-01'
SET name = 'one^4'
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

-- TODO: UPDATE with generated columns too
SELECT * FROM for_portion_of_test ORDER BY id, valid_from NULLS FIRST, valid_til;

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

SELECT * FROM for_portion_of_test ORDER BY id, valid_from NULLS FIRST, valid_til;

-- UPDATE ... RETURNING returns only the updated values (not the inserted side values)
UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2018-02-01' TO '2018-02-15'
SET name = 'three^3'
WHERE id = '[3,4)'
RETURNING *;

-- test that we run triggers on the UPDATE/DELETEd row and the INSERTed rows

CREATE FUNCTION for_portion_of_trigger2()
RETURNS trigger
AS
$$
BEGIN
  RAISE NOTICE '% % [%,%) of [%,%)', TG_WHEN, TG_OP, NEW.valid_from, NEW.valid_til, OLD.valid_from, OLD.valid_til;
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
  EXECUTE FUNCTION for_portion_of_trigger2();
CREATE TRIGGER trg_for_portion_of_after_insert
  AFTER INSERT ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger2();
CREATE TRIGGER trg_for_portion_of_before_update
  BEFORE UPDATE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger2();
CREATE TRIGGER trg_for_portion_of_after_update
  AFTER UPDATE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger2();
CREATE TRIGGER trg_for_portion_of_before_delete
  BEFORE DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger2();
CREATE TRIGGER trg_for_portion_of_after_delete
  AFTER DELETE ON for_portion_of_test
  FOR EACH ROW
  EXECUTE FUNCTION for_portion_of_trigger2();

UPDATE for_portion_of_test
FOR PORTION OF valid_at FROM '2021-01-01' TO '2022-01-01'
SET name = 'five^4'
WHERE id = '[5,6)';

DELETE FROM for_portion_of_test
FOR PORTION OF valid_at FROM '2023-01-01' TO '2024-01-01'
WHERE id = '[5,6)';

SELECT * FROM for_portion_of_test ORDER BY id, valid_from NULLS FIRST, valid_til;
