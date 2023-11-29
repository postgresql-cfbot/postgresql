-- Tests for WITHOUT OVERLAPS.
--
-- We leave behind several tables to test pg_dump etc:
-- temporal_rng, temporal_rng2,
-- temporal_fk_rng2rng.

--
-- test input parser
--

-- PK with no columns just WITHOUT OVERLAPS:

CREATE TABLE temporal_rng (
	valid_at tsrange,
	CONSTRAINT temporal_rng_pk PRIMARY KEY (valid_at WITHOUT OVERLAPS)
);

-- PK with a range column/PERIOD that isn't there:

CREATE TABLE temporal_rng (
	id INTEGER,
	CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

-- PK with a non-range column:

CREATE TABLE temporal_rng (
	id int4range,
	valid_at TEXT,
	CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

-- PK with one column plus a range:

CREATE TABLE temporal_rng (
	-- Since we can't depend on having btree_gist here,
	-- use an int4range instead of an int.
	-- (The rangetypes regression test uses the same trick.)
	id int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng_pk';

-- PK with two columns plus a range:
-- We don't drop this table because tests below also need multiple scalar columns.
CREATE TABLE temporal_rng2 (
	id1 int4range,
	id2 int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';


-- PK with a custom range type:
CREATE TYPE textrange2 AS range (subtype=text, collation="C");
CREATE TABLE temporal_rng3 (
	id int4range,
	valid_at textrange2,
	CONSTRAINT temporal_rng3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_rng3 DROP CONSTRAINT temporal_rng3_pk;
DROP TABLE temporal_rng3;
DROP TYPE textrange2;

-- UNIQUE with no columns just WITHOUT OVERLAPS:

CREATE TABLE temporal_rng3 (
	valid_at tsrange,
	CONSTRAINT temporal_rng3_uq UNIQUE (valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with a range column/PERIOD that isn't there:

CREATE TABLE temporal_rng3 (
	id INTEGER,
	CONSTRAINT temporal_rng3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with a non-range column:

CREATE TABLE temporal_rng3 (
	id int4range,
	valid_at TEXT,
	CONSTRAINT temporal_rng3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with one column plus a range:

CREATE TABLE temporal_rng3 (
	id int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
DROP TABLE temporal_rng3;

-- UNIQUE with two columns plus a range:
CREATE TABLE temporal_rng3 (
	id1 int4range,
	id2 int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng3_uq UNIQUE (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
DROP TABLE temporal_rng3;

-- UNIQUE with a custom range type:
CREATE TYPE textrange2 AS range (subtype=text, collation="C");
CREATE TABLE temporal_rng3 (
	id int4range,
	valid_at textrange2,
	CONSTRAINT temporal_rng3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_rng3 DROP CONSTRAINT temporal_rng3_uq;
DROP TABLE temporal_rng3;
DROP TYPE textrange2;

--
-- test ALTER TABLE ADD CONSTRAINT
--

DROP TABLE temporal_rng;
CREATE TABLE temporal_rng (
	id int4range,
	valid_at tsrange
);
ALTER TABLE temporal_rng
	ADD CONSTRAINT temporal_rng_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);

-- PK with USING INDEX (not possible):
CREATE TABLE temporal3 (
	id int4range,
	valid_at tsrange
);
CREATE INDEX idx_temporal3_uq ON temporal3 USING gist (id, valid_at);
ALTER TABLE temporal3
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY USING INDEX idx_temporal3_uq;
DROP TABLE temporal3;

-- UNIQUE with USING INDEX (not possible):
CREATE TABLE temporal3 (
	id int4range,
	valid_at tsrange
);
CREATE INDEX idx_temporal3_uq ON temporal3 USING gist (id, valid_at);
ALTER TABLE temporal3
	ADD CONSTRAINT temporal3_uq
	UNIQUE USING INDEX idx_temporal3_uq;
DROP TABLE temporal3;

-- UNIQUE with USING [UNIQUE] INDEX (possible but not a temporal constraint):
CREATE TABLE temporal3 (
	id int4range,
	valid_at tsrange
);
CREATE UNIQUE INDEX idx_temporal3_uq ON temporal3 (id, valid_at);
ALTER TABLE temporal3
	ADD CONSTRAINT temporal3_uq
	UNIQUE USING INDEX idx_temporal3_uq;
DROP TABLE temporal3;

-- Add range column and the PK at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_at tsrange,
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- Add range column and UNIQUE constraint at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_at tsrange,
	ADD CONSTRAINT temporal3_uq
	UNIQUE (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

--
-- test PK inserts
--

-- okay:
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng VALUES ('[2,2]', tsrange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng VALUES ('[3,3]', tsrange('2018-01-01', NULL));

-- should fail:
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng VALUES (NULL, tsrange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng VALUES ('[3,3]', NULL);

--
-- test a range with both a PK and a UNIQUE constraint
--

CREATE TABLE temporal3 (
  id int4range,
  valid_at daterange,
  id2 int8range,
  name TEXT,
  CONSTRAINT temporal3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT temporal3_uniq UNIQUE (id2, valid_at WITHOUT OVERLAPS)
);
INSERT INTO temporal3 (id, valid_at, id2, name)
  VALUES
  ('[1,1]', daterange('2000-01-01', '2010-01-01'), '[7,7]', 'foo'),
  ('[2,2]', daterange('2000-01-01', '2010-01-01'), '[9,9]', 'bar')
;
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-05-01' TO '2000-07-01'
  SET name = name || '1';
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-04-01' TO '2000-06-01'
  SET name = name || '2'
  WHERE id = '[2,2]';
SELECT * FROM temporal3 ORDER BY id, valid_at;
-- conflicting id only:
INSERT INTO temporal3 (id, valid_at, id2, name)
  VALUES
  ('[1,1]', daterange('2005-01-01', '2006-01-01'), '[8,8]', 'foo3');
-- conflicting id2 only:
INSERT INTO temporal3 (id, valid_at, id2, name)
  VALUES
  ('[3,3]', daterange('2005-01-01', '2010-01-01'), '[9,9]', 'bar3');
DROP TABLE temporal3;

--
-- test changing the PK's dependencies
--

CREATE TABLE temporal3 (
	id int4range,
	valid_at tsrange,
	CONSTRAINT temporal3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

ALTER TABLE temporal3 ALTER COLUMN valid_at DROP NOT NULL;
ALTER TABLE temporal3 ALTER COLUMN valid_at TYPE tstzrange USING tstzrange(lower(valid_at), upper(valid_at));
ALTER TABLE temporal3 RENAME COLUMN valid_at TO valid_thru;
ALTER TABLE temporal3 DROP COLUMN valid_thru;
DROP TABLE temporal3;

--
-- test PARTITION BY for ranges
--

-- temporal PRIMARY KEY:
CREATE TABLE temporal_partitioned (
	id int4range,
	valid_at daterange,
  name text,
	CONSTRAINT temporal_paritioned_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE tp1 partition OF temporal_partitioned FOR VALUES IN ('[1,1]', '[2,2]');
CREATE TABLE tp2 partition OF temporal_partitioned FOR VALUES IN ('[3,3]', '[4,4]');
INSERT INTO temporal_partitioned VALUES
  ('[1,1]', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,1]', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[3,3]', daterange('2000-01-01', '2010-01-01'), 'three');
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
SELECT * FROM tp1 ORDER BY id, valid_at;
SELECT * FROM tp2 ORDER BY id, valid_at;
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  SET name = 'one2'
  WHERE id = '[1,1]';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-02-20' TO '2000-02-25'
  SET id = '[4,4]'
  WHERE name = 'one';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01'
  SET id = '[2,2]'
  WHERE name = 'three';
DELETE FROM temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  WHERE id = '[3,3]';
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
DROP TABLE temporal_partitioned;

-- temporal UNIQUE:
CREATE TABLE temporal_partitioned (
	id int4range,
	valid_at daterange,
  name text,
	CONSTRAINT temporal_paritioned_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE tp1 partition OF temporal_partitioned FOR VALUES IN ('[1,1]', '[2,2]');
CREATE TABLE tp2 partition OF temporal_partitioned FOR VALUES IN ('[3,3]', '[4,4]');
INSERT INTO temporal_partitioned VALUES
  ('[1,1]', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,1]', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[3,3]', daterange('2000-01-01', '2010-01-01'), 'three');
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
SELECT * FROM tp1 ORDER BY id, valid_at;
SELECT * FROM tp2 ORDER BY id, valid_at;
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  SET name = 'one2'
  WHERE id = '[1,1]';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-02-20' TO '2000-02-25'
  SET id = '[4,4]'
  WHERE name = 'one';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01'
  SET id = '[2,2]'
  WHERE name = 'three';
DELETE FROM temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  WHERE id = '[3,3]';
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
DROP TABLE temporal_partitioned;

--
-- test FK dependencies
--

-- can't drop a range referenced by an FK, unless with CASCADE
CREATE TABLE temporal3 (
	id int4range,
	valid_at tsrange,
	CONSTRAINT temporal3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal3 (id, PERIOD valid_at)
);
ALTER TABLE temporal3 DROP COLUMN valid_at;
ALTER TABLE temporal3 DROP COLUMN valid_at CASCADE;
DROP TABLE temporal_fk_rng2rng;
DROP TABLE temporal3;

--
-- test FOREIGN KEY, range references range
--

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at int4range,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk2 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk2 FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);

CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);
DROP TABLE temporal_fk_rng2rng;

-- with mismatched PERIOD columns:
-- (parent_id, PERIOD valid_at) REFERENCES (id, valid_at)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, PERIOD valid_at)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);
-- (parent_id, PERIOD valid_at) REFERENCES (id)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id)
);
-- (parent_id) REFERENCES (id, PERIOD valid_at)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);

-- with inferred PK on the referenced table:
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
);
DROP TABLE temporal_fk_rng2rng;

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_rng (id, PERIOD id)
);

-- Two scalar columns
CREATE TABLE temporal_fk2_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id1 int4range,
	parent_id2 int4range,
	CONSTRAINT temporal_fk2_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk2_rng2rng_fk FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2 (id1, id2, PERIOD valid_at)
);
\d temporal_fk2_rng2rng
DROP TABLE temporal_fk2_rng2rng;

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng (id, PERIOD valid_at);
-- Two scalar columns:
CREATE TABLE temporal_fk2_rng2rng (
	id int4range,
	valid_at tsrange,
	parent_id1 int4range,
	parent_id2 int4range,
	CONSTRAINT temporal_fk2_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk2_rng2rng
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
	FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
	REFERENCES temporal_rng2 (id1, id2, PERIOD valid_at);
\d temporal_fk2_rng2rng

-- with inferred PK on the referenced table, and wrong column type:
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ALTER COLUMN valid_at TYPE daterange USING daterange(lower(valid_at)::date, upper(valid_at)::date);
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
ALTER TABLE temporal_fk_rng2rng
	ALTER COLUMN valid_at TYPE tsrange USING tsrange(lower(valid_at), upper(valid_at));

-- with inferred PK on the referenced table:
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;

-- should fail because of duplicate referenced columns:
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk2
	FOREIGN KEY (parent_id, PERIOD parent_id)
	REFERENCES temporal_rng (id, PERIOD id);

--
-- test with rows already
--

DELETE FROM temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
INSERT INTO temporal_fk_rng2rng VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-01'), '[1,1]');
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
INSERT INTO temporal_fk_rng2rng VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');
-- should fail:
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
-- okay again:
DELETE FROM temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;

--
-- test pg_get_constraintdef
--

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_rng2rng_fk';

--
-- test FK child inserts
--

INSERT INTO temporal_fk_rng2rng VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-01'), '[1,1]');
-- should fail:
INSERT INTO temporal_fk_rng2rng VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');
-- now it should work:
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-02-03', '2018-03-03'));
INSERT INTO temporal_fk_rng2rng VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');

--
-- test FK child updates
--

UPDATE temporal_fk_rng2rng SET valid_at = tsrange('2018-01-02', '2018-03-01') WHERE id = '[1,1]';
-- should fail:
UPDATE temporal_fk_rng2rng SET valid_at = tsrange('2018-01-02', '2018-05-01') WHERE id = '[1,1]';
UPDATE temporal_fk_rng2rng SET parent_id = '[8,8]' WHERE id = '[1,1]';

-- ALTER FK DEFERRABLE

BEGIN;
  INSERT INTO temporal_rng VALUES
    ('[5,5]', tsrange('2018-01-01', '2018-02-01')),
    ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
  INSERT INTO temporal_fk_rng2rng VALUES
    ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_rng WHERE id = '[5,5]'; --should not fail yet.
COMMIT; -- should fail here.

--
-- test FK parent updates NO ACTION
--

-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01') WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_rng WHERE id = '[5,5]';
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_rng SET valid_at = tsrange('2016-02-01', '2016-03-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced:
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- changing the scalar part fails:
UPDATE temporal_rng SET id = '[7,7]'
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- changing an unreferenced part is okay:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- changing just a part fails:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id IN ('[5,5]', '[7,7]');

--
-- test FK parent updates RESTRICT
--

ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON DELETE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01') WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_rng WHERE id = '[5,5]';
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_rng SET valid_at = tsrange('2016-02-01', '2016-03-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced:
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- changing the scalar part fails:
UPDATE temporal_rng SET id = '[7,7]'
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- changing an unreferenced part is okay:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- changing just a part fails:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id IN ('[5,5]', '[7,7]');
--
-- test FK parent deletes NO ACTION
--
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- deleting an unreferenced part is okay:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,5]';
-- deleting just a part fails:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,5]';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id IN ('[5,5]');

--
-- test FK parent deletes RESTRICT
--
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON DELETE RESTRICT;
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- deleting an unreferenced part is okay:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,5]';
-- deleting just a part fails:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,5]';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id IN ('[5,5]');

--
-- test ON UPDATE/DELETE options
--
-- TOC:
-- parent updates CASCADE
-- parent deletes CASCADE
-- parent updates SET NULL
-- parent deletes SET NULL
-- parent updates SET DEFAULT
-- parent deletes SET DEFAULT
-- parent updates CASCADE (two scalar cols)
-- parent deletes CASCADE (two scalar cols)
-- parent updates SET NULL (two scalar cols)
-- parent deletes SET NULL (two scalar cols)
-- parent deletes SET NULL (two scalar cols, SET NULL subset)
-- parent updates SET DEFAULT (two scalar cols)
-- parent deletes SET DEFAULT (two scalar cols)
-- parent deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)

-- test FK parent updates CASCADE
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
UPDATE temporal_rng SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes CASCADE
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO delete:
DELETE FROM temporal_rng WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_rng WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent updates SET NULL
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
UPDATE temporal_rng SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET NULL
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO delete:
DELETE FROM temporal_rng WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_rng WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent updates SET DEFAULT
INSERT INTO temporal_rng VALUES ('[-1,-1]', tsrange(null, null));
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_fk_rng2rng
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
UPDATE temporal_rng SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET DEFAULT
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
DELETE FROM temporal_rng WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_rng WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng WHERE id IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent updates CASCADE (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
UPDATE temporal_rng2 SET id1 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes CASCADE (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
DELETE FROM temporal_rng2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent updates SET NULL (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
UPDATE temporal_rng2 SET id1 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET NULL (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
DELETE FROM temporal_rng2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET NULL (two scalar cols, SET NULL subset)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET NULL (valid_at) ON UPDATE SET NULL;
-- ok:
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET NULL (parent_id1) ON UPDATE SET NULL;
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
DELETE FROM temporal_rng2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent updates SET DEFAULT (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[-1,-1]', '[-1,-1]', tsrange(null, null));
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_rng2rng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
  ALTER COLUMN parent_id2 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]', id2 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,7]', id2 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
UPDATE temporal_rng2 SET id1 = '[9,9]', id2 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET DEFAULT (two scalar cols)
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
DELETE FROM temporal_rng2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
DELETE FROM temporal_rng2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- test FK parent deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)
INSERT INTO temporal_rng2 VALUES ('[-1,-1]', '[6,6]', tsrange(null, null));
INSERT INTO temporal_rng2 VALUES ('[6,6]', '[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng VALUES ('[100,100]', tsrange('2018-01-01', '2021-01-01'), '[6,6]', '[6,6]');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_rng2rng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET DEFAULT (valid_at) ON UPDATE SET DEFAULT;
-- ok:
ALTER TABLE temporal_fk2_rng2rng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET DEFAULT (parent_id1) ON UPDATE SET DEFAULT;
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- non-FPO update:
DELETE FROM temporal_rng2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,100]';
-- FK across two referenced rows:
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 VALUES ('[8,8]', '[8,8]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_rng2 VALUES ('[-1,-1]', '[8,8]', tsrange(null, null));
INSERT INTO temporal_fk2_rng2rng VALUES ('[200,200]', tsrange('2018-01-01', '2021-01-01'), '[8,8]', '[8,8]');
DELETE FROM temporal_rng2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,200]';
-- clean up
DELETE FROM temporal_fk2_rng2rng WHERE id IN ('[100,100]', '[200,200]');
DELETE FROM temporal_rng2 WHERE id1 IN ('[6,6]', '[7,7]', '[8,8]', '[9,9]');

-- FK with a custom range type

CREATE TYPE mydaterange AS range(subtype=date);

CREATE TABLE temporal_rng3 (
	id int4range,
	valid_at mydaterange,
	CONSTRAINT temporal_rng3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
CREATE TABLE temporal_fk3_rng2rng (
	id int4range,
	valid_at mydaterange,
	parent_id int4range,
	CONSTRAINT temporal_fk3_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk3_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng3 (id, PERIOD valid_at) ON DELETE CASCADE
);
INSERT INTO temporal_rng3 VALUES ('[8,8]', mydaterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk3_rng2rng VALUES ('[5,5]', mydaterange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_rng3 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_fk3_rng2rng WHERE id = '[5,5]';

DROP TABLE temporal_fk3_rng2rng;
DROP TABLE temporal_rng3;
DROP TYPE mydaterange;

-- FK between partitioned tables

CREATE TABLE temporal_partitioned_rng (
	id int4range,
	valid_at daterange,
  name text,
	CONSTRAINT temporal_paritioned_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE tp1 partition OF temporal_partitioned_rng FOR VALUES IN ('[1,1]', '[3,3]', '[5,5]', '[7,7]', '[9,9]', '[11,11]', '[13,13]', '[15,15]', '[17,17]', '[19,19]', '[21,21]', '[23,23]');
CREATE TABLE tp2 partition OF temporal_partitioned_rng FOR VALUES IN ('[0,0]', '[2,2]', '[4,4]', '[6,6]', '[8,8]', '[10,10]', '[12,12]', '[14,14]', '[16,16]', '[18,18]', '[20,20]', '[22,22]', '[24,24]');
INSERT INTO temporal_partitioned_rng VALUES
  ('[1,1]', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,1]', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[2,2]', daterange('2000-01-01', '2010-01-01'), 'two');

CREATE TABLE temporal_partitioned_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_partitioned_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_partitioned_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng (id, PERIOD valid_at)
) PARTITION BY LIST (id);
CREATE TABLE tfkp1 partition OF temporal_partitioned_fk_rng2rng FOR VALUES IN ('[1,1]', '[3,3]', '[5,5]', '[7,7]', '[9,9]', '[11,11]', '[13,13]', '[15,15]', '[17,17]', '[19,19]', '[21,21]', '[23,23]');
CREATE TABLE tfkp2 partition OF temporal_partitioned_fk_rng2rng FOR VALUES IN ('[0,0]', '[2,2]', '[4,4]', '[6,6]', '[8,8]', '[10,10]', '[12,12]', '[14,14]', '[16,16]', '[18,18]', '[20,20]', '[22,22]', '[24,24]');

-- partitioned FK child inserts

INSERT INTO temporal_partitioned_fk_rng2rng VALUES
  ('[1,1]', daterange('2000-01-01', '2000-02-15'), '[1,1]'),
  ('[1,1]', daterange('2001-01-01', '2002-01-01'), '[2,2]'),
  ('[2,2]', daterange('2000-01-01', '2000-02-15'), '[1,1]');
-- should fail:
INSERT INTO temporal_partitioned_fk_rng2rng VALUES
  ('[3,3]', daterange('2010-01-01', '2010-02-15'), '[1,1]');
INSERT INTO temporal_partitioned_fk_rng2rng VALUES
  ('[3,3]', daterange('2000-01-01', '2000-02-15'), '[3,3]');

-- partitioned FK child updates

UPDATE temporal_partitioned_fk_rng2rng SET valid_at = daterange('2000-01-01', '2000-02-13') WHERE id = '[2,2]';
-- move a row from the first partition to the second
UPDATE temporal_partitioned_fk_rng2rng SET id = '[4,4]' WHERE id = '[1,1]';
-- move a row from the second partition to the first
UPDATE temporal_partitioned_fk_rng2rng SET id = '[1,1]' WHERE id = '[4,4]';
-- should fail:
UPDATE temporal_partitioned_fk_rng2rng SET valid_at = daterange('2000-01-01', '2000-04-01') WHERE id = '[1,1]';

-- partitioned FK parent updates NO ACTION

INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2016-01-01', '2016-02-01'));
UPDATE temporal_partitioned_rng SET valid_at = daterange('2018-01-01', '2018-02-01') WHERE id = '[5,5]';
INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[3,3]', daterange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
  WHERE id = '[5,5]' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,5]' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_partitioned_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]';

-- partitioned FK parent deletes NO ACTION

INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[3,3]', daterange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_partitioned_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]';

-- partitioned FK parent updates RESTRICT

ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk;
ALTER TABLE temporal_partitioned_fk_rng2rng
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_partitioned_rng
	ON DELETE RESTRICT;
INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2016-01-01', '2016-02-01'));
UPDATE temporal_partitioned_rng SET valid_at = daterange('2018-01-01', '2018-02-01') WHERE id = '[5,5]';
INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[3,3]', daterange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
  WHERE id = '[5,5]' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,5]' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_partitioned_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]';

-- partitioned FK parent deletes RESTRICT

INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[5,5]', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[3,3]', daterange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
DELETE FROM temporal_partitioned_rng WHERE id = '[5,5]' AND valid_at = daterange('2018-01-01', '2018-02-01');

-- partitioned FK parent updates CASCADE

INSERT INTO temporal_partitioned_rng VALUES ('[6,6]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[4,4]', daterange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[4,4]';
UPDATE temporal_partitioned_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[4,4]';
INSERT INTO temporal_partitioned_rng VALUES ('[15,15]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[15,15]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[10,10]', daterange('2018-01-01', '2021-01-01'), '[15,15]');
UPDATE temporal_partitioned_rng SET id = '[16,16]' WHERE id = '[15,15]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[10,10]';

-- partitioned FK parent deletes CASCADE

INSERT INTO temporal_partitioned_rng VALUES ('[8,8]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[5,5]', daterange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[5,5]';
DELETE FROM temporal_partitioned_rng WHERE id = '[8,8]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[5,5]';
INSERT INTO temporal_partitioned_rng VALUES ('[17,17]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[17,17]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[11,11]', daterange('2018-01-01', '2021-01-01'), '[17,17]');
DELETE FROM temporal_partitioned_rng WHERE id = '[17,17]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[11,11]';

-- partitioned FK parent updates SET NULL

INSERT INTO temporal_partitioned_rng VALUES ('[9,9]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[6,6]', daterange('2018-01-01', '2021-01-01'), '[9,9]');
ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[6,6]';
UPDATE temporal_partitioned_rng SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[6,6]';
INSERT INTO temporal_partitioned_rng VALUES ('[18,18]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[18,18]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[12,12]', daterange('2018-01-01', '2021-01-01'), '[18,18]');
UPDATE temporal_partitioned_rng SET id = '[19,19]' WHERE id = '[18,18]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[12,12]';

-- partitioned FK parent deletes SET NULL

INSERT INTO temporal_partitioned_rng VALUES ('[11,11]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[7,7]', daterange('2018-01-01', '2021-01-01'), '[11,11]');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,11]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[7,7]';
DELETE FROM temporal_partitioned_rng WHERE id = '[11,11]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[7,7]';
INSERT INTO temporal_partitioned_rng VALUES ('[20,20]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[20,20]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[13,13]', daterange('2018-01-01', '2021-01-01'), '[20,20]');
DELETE FROM temporal_partitioned_rng WHERE id = '[20,20]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[13,13]';

-- partitioned FK parent updates SET DEFAULT

INSERT INTO temporal_partitioned_rng VALUES ('[0,0]', daterange(null, null));
INSERT INTO temporal_partitioned_rng VALUES ('[12,12]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[8,8]', daterange('2018-01-01', '2021-01-01'), '[12,12]');
ALTER TABLE temporal_partitioned_fk_rng2rng
  ALTER COLUMN parent_id SET DEFAULT '[0,0]',
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[8,8]';
UPDATE temporal_partitioned_rng SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[8,8]';
INSERT INTO temporal_partitioned_rng VALUES ('[22,22]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[22,22]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[14,14]', daterange('2018-01-01', '2021-01-01'), '[22,22]');
UPDATE temporal_partitioned_rng SET id = '[23,23]' WHERE id = '[22,22]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[14,14]';

-- partitioned FK parent deletes SET DEFAULT

INSERT INTO temporal_partitioned_rng VALUES ('[14,14]', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[9,9]', daterange('2018-01-01', '2021-01-01'), '[14,14]');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,14]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[9,9]';
DELETE FROM temporal_partitioned_rng WHERE id = '[14,14]';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[9,9]';
INSERT INTO temporal_partitioned_rng VALUES ('[24,24]', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng VALUES ('[24,24]', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng VALUES ('[15,15]', daterange('2018-01-01', '2021-01-01'), '[24,24]');
DELETE FROM temporal_partitioned_rng WHERE id = '[24,24]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[15,15]';

DROP TABLE temporal_partitioned_fk_rng2rng;
DROP TABLE temporal_partitioned_rng;
