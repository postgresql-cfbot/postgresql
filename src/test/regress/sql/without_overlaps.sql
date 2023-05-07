-- Tests for WITHOUT OVERLAPS.
--
-- We leave behind several tables to test pg_dump etc:
-- temporal_rng, temporal_per, temporal_rng2, temporal_per2,
-- temporal_fk_{rng,per}2{rng,per}.

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
	id INTEGER,
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
CREATE TABLE temporal_rng2 (
	id1 int4range,
	id2 int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';
DROP TABLE temporal_rng2;


-- PK with one column plus a PERIOD:
CREATE TABLE temporal_per (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_per
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per_pk';

-- PK with two columns plus a PERIOD:
CREATE TABLE temporal_per2 (
	id1 int4range,
	id2 int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_per2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_per2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per2_pk';
DROP TABLE temporal_per2;

-- PK with a custom range type:
CREATE TYPE textrange2 AS range (subtype=text, collation="C");
CREATE TABLE temporal_rng2 (
	id int4range,
	valid_at textrange2,
	CONSTRAINT temporal_rng2_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_rng2 DROP CONSTRAINT temporal_rng2_pk;
DROP TABLE temporal_rng2;
DROP TYPE textrange2;

-- UNIQUE with no columns just WITHOUT OVERLAPS:

CREATE TABLE temporal_rng2 (
	valid_at tsrange,
	CONSTRAINT temporal_rng2_uq UNIQUE (valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with a range column/PERIOD that isn't there:

CREATE TABLE temporal_rng2 (
	id INTEGER,
	CONSTRAINT temporal_rng2_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with a non-range column:

CREATE TABLE temporal_rng2 (
	id INTEGER,
	valid_at TEXT,
	CONSTRAINT temporal_rng2_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);

-- UNIQUE with one column plus a range:

CREATE TABLE temporal_rng2 (
	-- Since we can't depend on having btree_gist here,
	-- use an int4range instead of an int.
	-- (The rangetypes regression test uses the same trick.)
	id int4range,
	valid_at tsrange,
	CONSTRAINT temporal_rng2_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng2_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng2_uq';

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

-- UNIQUE with one column plus a PERIOD:
CREATE TABLE temporal_per2 (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_per2_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_per2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per2_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per2_uq';

-- UNIQUE with two columns plus a PERIOD:
CREATE TABLE temporal_per3 (
	id1 int4range,
	id2 int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_per3_uq UNIQUE (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_per3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
DROP TABLE temporal_per3;

-- UNIQUE with a custom range type:
CREATE TYPE textrange2 AS range (subtype=text, collation="C");
CREATE TABLE temporal_per3 (
	id int4range,
	valid_at textrange2,
	CONSTRAINT temporal_per3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_per3 DROP CONSTRAINT temporal_per3_uq;
DROP TABLE temporal_per3;
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

-- Add range column and the PK at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_at tsrange,
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- Add PERIOD and the PK at the same time
CREATE TABLE temporal3 (
	id int4range,
	valid_from date,
	valid_til date
);
ALTER TABLE temporal3
	ADD PERIOD FOR valid_at (valid_from, valid_til),
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

-- Add PERIOD column and UNIQUE constraint at the same time
CREATE TABLE temporal3 (
	id int4range,
	valid_from date,
	valid_til date
);
ALTER TABLE temporal3
	ADD PERIOD FOR valid_at (valid_from, valid_til),
	ADD CONSTRAINT temporal3_uq
	UNIQUE (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- Add date columns, PERIOD, and the PK at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_from date,
	ADD COLUMN valid_til date,
	ADD PERIOD FOR valid_at (valid_from, valid_til),
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- Add date columns, PERIOD, and UNIQUE constraint at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_from date,
	ADD COLUMN valid_til date,
	ADD PERIOD FOR valid_at (valid_from, valid_til),
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
  ('[3,3]', daterange('2005-01-01', '2010-01-01'), '[9,9]', 'bar3')
;
DROP TABLE temporal3;

--
-- test a PERIOD with both a PK and a UNIQUE constraint
--

CREATE TABLE temporal3 (
  id int4range,
	valid_from date,
	valid_til date,
	PERIOD FOR valid_at (valid_from, valid_til),
  id2 int8range,
  name TEXT,
  CONSTRAINT temporal3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT temporal3_uniq UNIQUE (id2, valid_at WITHOUT OVERLAPS)
);
INSERT INTO temporal3 (id, valid_from, valid_til, id2, name)
  VALUES
  ('[1,1]', '2000-01-01', '2010-01-01', '[7,7]', 'foo'),
  ('[2,2]', '2000-01-01', '2010-01-01', '[9,9]', 'bar')
;
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-05-01' TO '2000-07-01'
  SET name = name || '1';
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-04-01' TO '2000-06-01'
  SET name = name || '2'
  WHERE id = '[2,2]';
SELECT * FROM temporal3 ORDER BY id, valid_from, valid_til;
-- conflicting id only:
INSERT INTO temporal3 (id, valid_from, valid_til, id2, name)
  VALUES
  ('[1,1]', '2005-01-01', '2006-01-01', '[8,8]', 'foo3');
-- conflicting id2 only:
INSERT INTO temporal3 (id, valid_from, valid_til, id2, name)
  VALUES
  ('[3,3]', '2005-01-01', '2010-01-01', '[9,9]', 'bar3')
;
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

CREATE TABLE temporal_partitioned (
	id int4range,
	valid_at daterange,
	CONSTRAINT temporal_paritioned_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
-- TODO: attach some partitions, insert into them, update them with and without FOR PORTION OF, delete them the same way.

--
-- test PARTITION BY for PERIODS
--

CREATE TABLE temporal_partitioned (
  id int4range,
  valid_from TIMESTAMP,
  valid_til TIMESTAMP,
  PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_paritioned_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
-- TODO: attach some partitions, insert into them, update them with and without FOR PORTION OF, delete them the same way.
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

-- can't drop a PERIOD referenced by an FK, unless with CASCADE
CREATE TABLE temporal3 (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
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
ALTER TABLE temporal3 DROP PERIOD FOR valid_at;
ALTER TABLE temporal3 DROP PERIOD FOR valid_at CASCADE;
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
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
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
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[5,5]';

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
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_rng2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[5,5]';
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
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');

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
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');

--
-- test ON UPDATE/DELETE options
--

-- test FK parent updates CASCADE
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[4,4]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[4,4]';
UPDATE temporal_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[4,4]';
INSERT INTO temporal_rng VALUES ('[15,15]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[15,15]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[10,10]', tsrange('2018-01-01', '2021-01-01'), '[15,15]');
UPDATE temporal_rng SET id = '[16,16]' WHERE id = '[15,15]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[10,10]';

-- test FK parent deletes CASCADE
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[5,5]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[8,8]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[5,5]';
INSERT INTO temporal_rng VALUES ('[17,17]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[17,17]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[11,11]', tsrange('2018-01-01', '2021-01-01'), '[17,17]');
DELETE FROM temporal_rng WHERE id = '[17,17]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[11,11]';


-- test FK parent updates SET NULL
INSERT INTO temporal_rng VALUES ('[9,9]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'), '[9,9]');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[6,6]';
UPDATE temporal_rng SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[6,6]';
INSERT INTO temporal_rng VALUES ('[18,18]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[18,18]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[12,12]', tsrange('2018-01-01', '2021-01-01'), '[18,18]');
UPDATE temporal_rng SET id = '[19,19]' WHERE id = '[18,18]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[12,12]';

-- test FK parent deletes SET NULL
INSERT INTO temporal_rng VALUES ('[11,11]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[7,7]', tsrange('2018-01-01', '2021-01-01'), '[11,11]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,11]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[7,7]';
DELETE FROM temporal_rng WHERE id = '[11,11]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[7,7]';
INSERT INTO temporal_rng VALUES ('[20,20]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[20,20]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[13,13]', tsrange('2018-01-01', '2021-01-01'), '[20,20]');
DELETE FROM temporal_rng WHERE id = '[20,20]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[13,13]';

-- test FK parent updates SET DEFAULT
INSERT INTO temporal_rng VALUES ('[-1,-1]', tsrange(null, null));
INSERT INTO temporal_rng VALUES ('[12,12]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[8,8]', tsrange('2018-01-01', '2021-01-01'), '[12,12]');
ALTER TABLE temporal_fk_rng2rng
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[8,8]';
UPDATE temporal_rng SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[8,8]';
INSERT INTO temporal_rng VALUES ('[22,22]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[22,22]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[14,14]', tsrange('2018-01-01', '2021-01-01'), '[22,22]');
UPDATE temporal_rng SET id = '[23,23]' WHERE id = '[22,22]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[14,14]';

-- test FK parent deletes SET DEFAULT
INSERT INTO temporal_rng VALUES ('[14,14]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[9,9]', tsrange('2018-01-01', '2021-01-01'), '[14,14]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,14]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[9,9]';
DELETE FROM temporal_rng WHERE id = '[14,14]';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[9,9]';
INSERT INTO temporal_rng VALUES ('[24,24]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[24,24]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng VALUES ('[15,15]', tsrange('2018-01-01', '2021-01-01'), '[24,24]');
DELETE FROM temporal_rng WHERE id = '[24,24]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[15,15]';



--
-- test FOREIGN KEY, range references PERIOD
--

DELETE FROM temporal_per;
INSERT INTO temporal_per VALUES ('[1,1]', '2018-01-02', '2018-02-03');
INSERT INTO temporal_per VALUES ('[1,1]', '2018-03-03', '2018-04-04');
INSERT INTO temporal_per VALUES ('[2,2]', '2018-01-01', '2018-01-05');
INSERT INTO temporal_per VALUES ('[3,3]', '2018-01-01', NULL);

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_rng2per (
	id int4range,
	valid_at int4range,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2per_pk2 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2per_fk2 FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id, PERIOD valid_at)
);

-- with inferred PK on the referenced table:
CREATE TABLE temporal_fk_rng2per (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
);
DROP TABLE temporal_fk_rng2per;

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_rng2per (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2per_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_per (id, PERIOD id)
);

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_rng2per (
	id int4range,
	valid_at tsrange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per (id, PERIOD valid_at);
ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
-- with inferred PK on the referenced table:
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;

-- should fail because of duplicate referenced columns:
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk2
	FOREIGN KEY (parent_id, PERIOD parent_id)
	REFERENCES temporal_per (id, PERIOD id);

--
-- test with rows already
--

DELETE FROM temporal_fk_rng2per;
ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
INSERT INTO temporal_fk_rng2per VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-01'), '[1,1]');
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
INSERT INTO temporal_fk_rng2per VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');
-- should fail:
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
-- okay again:
DELETE FROM temporal_fk_rng2per;
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;

--
-- test pg_get_constraintdef
--

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_rng2per_fk';

--
-- test FK child inserts
--

INSERT INTO temporal_fk_rng2per VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-01'), '[1,1]');
-- should fail:
INSERT INTO temporal_fk_rng2per VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');
-- now it should work:
INSERT INTO temporal_per VALUES ('[1,1]', '2018-02-03', '2018-03-03');
INSERT INTO temporal_fk_rng2per VALUES ('[2,2]', tsrange('2018-01-02', '2018-04-01'), '[1,1]');

--
-- test FK child updates
--
UPDATE temporal_fk_rng2per SET valid_at = tsrange('2018-01-02', '2018-03-01') WHERE id = '[1,1]';
-- should fail:
UPDATE temporal_fk_rng2per SET valid_at = tsrange('2018-01-02', '2018-05-01') WHERE id = '[1,1]';
UPDATE temporal_fk_rng2per SET parent_id = '[8,8]' WHERE id = '[1,1]';

--
-- test FK parent updates NO ACTION
--

-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_rng2per VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced:
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- clean up:
DELETE FROM temporal_fk_rng2per WHERE parent_id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[5,5]';

--
-- test FK parent updates RESTRICT
--

ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON DELETE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_rng2per VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced:
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- clean up:
DELETE FROM temporal_fk_rng2per WHERE parent_id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[5,5]';

--
-- test FK parent deletes NO ACTION
--

ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_rng2per VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test FK parent deletes RESTRICT
--

ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk;
ALTER TABLE temporal_fk_rng2per
	ADD CONSTRAINT temporal_fk_rng2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON DELETE RESTRICT;
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_rng2per VALUES ('[3,3]', tsrange('2018-01-05', '2018-01-10'), '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test ON UPDATE/DELETE options
--

-- test FK parent updates CASCADE
INSERT INTO temporal_per VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[4,4]', tsrange('2018-01-01', '2021-01-01'), '[6,6]');
ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk,
	ADD CONSTRAINT temporal_fk_rng2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[4,4]';
UPDATE temporal_per SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[4,4]';
INSERT INTO temporal_per VALUES ('[15,15]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[15,15]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[10,10]', tsrange('2018-01-01', '2021-01-01'), '[15,15]');
UPDATE temporal_per SET id = '[16,16]' WHERE id = '[15,15]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[10,10]';

-- test FK parent deletes CASCADE
INSERT INTO temporal_per VALUES ('[8,8]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[5,5]', tsrange('2018-01-01', '2021-01-01'), '[8,8]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[8,8]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[17,17]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[17,17]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[11,11]', tsrange('2018-01-01', '2021-01-01'), '[17,17]');
DELETE FROM temporal_per WHERE id = '[17,17]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[11,11]';

-- test FK parent updates SET NULL
INSERT INTO temporal_per VALUES ('[9,9]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'), '[9,9]');
ALTER TABLE temporal_fk_rng2per
	DROP CONSTRAINT temporal_fk_rng2per_fk,
	ADD CONSTRAINT temporal_fk_rng2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[6,6]';
UPDATE temporal_per SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[6,6]';
INSERT INTO temporal_per VALUES ('[18,18]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[18,18]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[12,12]', tsrange('2018-01-01', '2021-01-01'), '[18,18]');
UPDATE temporal_per SET id = '[19,19]' WHERE id = '[18,18]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[12,12]';

-- test FK parent deletes SET NULL
INSERT INTO temporal_per VALUES ('[11,11]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[7,7]', tsrange('2018-01-01', '2021-01-01'), '[11,11]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,11]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[7,7]';
DELETE FROM temporal_per WHERE id = '[11,11]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[7,7]';
INSERT INTO temporal_per VALUES ('[20,20]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[20,20]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[13,13]', tsrange('2018-01-01', '2021-01-01'), '[20,20]');
DELETE FROM temporal_per WHERE id = '[20,20]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[13,13]';

-- test FK parent updates SET DEFAULT
INSERT INTO temporal_per VALUES ('[-1,-1]', null, null);
INSERT INTO temporal_per VALUES ('[12,12]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[8,8]', tsrange('2018-01-01', '2021-01-01'), '[12,12]');
ALTER TABLE temporal_fk_rng2per
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_rng2per_fk,
	ADD CONSTRAINT temporal_fk_rng2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[8,8]';
UPDATE temporal_per SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[8,8]';
INSERT INTO temporal_per VALUES ('[22,22]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[22,22]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[14,14]', tsrange('2018-01-01', '2021-01-01'), '[22,22]');
UPDATE temporal_per SET id = '[23,23]' WHERE id = '[22,22]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[14,14]';

-- test FK parent deletes SET DEFAULT
INSERT INTO temporal_per VALUES ('[14,14]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[9,9]', tsrange('2018-01-01', '2021-01-01'), '[14,14]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,14]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[9,9]';
DELETE FROM temporal_per WHERE id = '[14,14]';
SELECT * FROM temporal_fk_rng2per WHERE id = '[9,9]';
INSERT INTO temporal_per VALUES ('[24,24]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[24,24]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_rng2per VALUES ('[15,15]', tsrange('2018-01-01', '2021-01-01'), '[24,24]');
DELETE FROM temporal_per WHERE id = '[24,24]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_rng2per WHERE id = '[15,15]';


--
-- test FOREIGN KEY, PERIOD references range
--

DELETE FROM temporal_fk_rng2rng;
DELETE FROM temporal_rng;
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng VALUES ('[2,2]', tsrange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng VALUES ('[3,3]', tsrange('2018-01-01', NULL));

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_per2rng (
	id int4range,
	valid_from int4,
  valid_til int4,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2rng_pk2 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2rng_fk2 FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);

-- with inferred PK on the referenced table:
CREATE TABLE temporal_fk_per2rng (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
);
DROP TABLE temporal_fk_per2rng;

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_per2rng (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2rng_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_rng (id, PERIOD id)
);

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_per2rng (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng (id, PERIOD valid_at);
ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
-- with inferred PK on the referenced table:
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;

-- should fail because of duplicate referenced columns:
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk2
	FOREIGN KEY (parent_id, PERIOD parent_id)
	REFERENCES temporal_rng (id, PERIOD id);

--
-- test with rows already
--

DELETE FROM temporal_fk_per2rng;
ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
INSERT INTO temporal_fk_per2rng VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
INSERT INTO temporal_fk_per2rng VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
-- should fail:
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
-- okay again:
DELETE FROM temporal_fk_per2rng;
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;

--
-- test pg_get_constraintdef
--

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_per2rng_fk';

--
-- test FK child inserts
--

INSERT INTO temporal_fk_per2rng VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
-- should fail:
INSERT INTO temporal_fk_per2rng VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
-- now it should work:
INSERT INTO temporal_rng VALUES ('[1,1]', tsrange('2018-02-03', '2018-03-03'));
INSERT INTO temporal_fk_per2rng VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');

--
-- test FK child updates
--
UPDATE temporal_fk_per2rng SET valid_from = '2018-01-02', valid_til = '2018-03-01' WHERE id = '[1,1]';
-- should fail:
UPDATE temporal_fk_per2rng SET valid_from = '2018-01-02', valid_til = '2018-05-01' WHERE id = '[1,1]';
UPDATE temporal_fk_per2rng SET parent_id = '[8,8]' WHERE id = '[1,1]';

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
INSERT INTO temporal_fk_per2rng VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_rng SET valid_at = tsrange('2016-02-01', '2016-03-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced:
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01','2018-02-01');
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_per2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[5,5]';

--
-- test FK parent updates RESTRICT
--

ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
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
INSERT INTO temporal_fk_per2rng VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_rng SET valid_at = tsrange('2016-02-01', '2016-03-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced:
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2rng WHERE id = '[3,3]';
UPDATE temporal_rng SET valid_at = tsrange('2016-01-01', '2016-02-01')
WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- clean up:
DELETE FROM temporal_fk_per2rng WHERE parent_id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[5,5]';

--
-- test FK parent deletes NO ACTION
--

ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');

--
-- test FK parent deletes RESTRICT
--

ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk;
ALTER TABLE temporal_fk_per2rng
	ADD CONSTRAINT temporal_fk_per2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON DELETE RESTRICT;
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_rng VALUES ('[5,5]', tsrange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2rng WHERE id = '[3,3]';
DELETE FROM temporal_rng WHERE id = '[5,5]' AND valid_at = tsrange('2018-01-01', '2018-02-01');

--
-- test ON UPDATE/DELETE options
--

-- test FK parent updates CASCADE
INSERT INTO temporal_rng VALUES ('[6,6]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[4,4]', '2018-01-01', '2021-01-01', '[6,6]');
ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk,
	ADD CONSTRAINT temporal_fk_per2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[4,4]';
UPDATE temporal_rng SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[4,4]';
INSERT INTO temporal_rng VALUES ('[15,15]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[15,15]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[10,10]', '2018-01-01', '2021-01-01', '[15,15]');
UPDATE temporal_rng SET id = '[16,16]' WHERE id = '[15,15]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[10,10]';

-- test FK parent deletes CASCADE
INSERT INTO temporal_rng VALUES ('[8,8]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[5,5]', '2018-01-01', '2021-01-01', '[8,8]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[5,5]';
DELETE FROM temporal_rng WHERE id = '[8,8]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[5,5]';
INSERT INTO temporal_rng VALUES ('[17,17]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[17,17]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[11,11]', '2018-01-01', '2021-01-01', '[17,17]');
DELETE FROM temporal_rng WHERE id = '[17,17]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[11,11]';

-- test FK parent updates SET NULL
INSERT INTO temporal_rng VALUES ('[9,9]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[6,6]', '2018-01-01', '2021-01-01', '[9,9]');
ALTER TABLE temporal_fk_per2rng
	DROP CONSTRAINT temporal_fk_per2rng_fk,
	ADD CONSTRAINT temporal_fk_per2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[6,6]';
UPDATE temporal_rng SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[6,6]';
INSERT INTO temporal_rng VALUES ('[18,18]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[18,18]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[12,12]', '2018-01-01', '2021-01-01', '[18,18]');
UPDATE temporal_rng SET id = '[19,19]' WHERE id = '[18,18]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[12,12]';

-- test FK parent deletes SET NULL
INSERT INTO temporal_rng VALUES ('[11,11]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[7,7]', '2018-01-01', '2021-01-01', '[11,11]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,11]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[7,7]';
DELETE FROM temporal_rng WHERE id = '[11,11]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[7,7]';
INSERT INTO temporal_rng VALUES ('[20,20]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[20,20]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[13,13]', '2018-01-01', '2021-01-01', '[20,20]');
DELETE FROM temporal_rng WHERE id = '[20,20]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[13,13]';

-- test FK parent updates SET DEFAULT
INSERT INTO temporal_rng VALUES ('[-1,-1]', tsrange(null, null));
INSERT INTO temporal_rng VALUES ('[12,12]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[8,8]', '2018-01-01', '2021-01-01', '[12,12]');
ALTER TABLE temporal_fk_per2rng
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_per2rng_fk,
	ADD CONSTRAINT temporal_fk_per2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[8,8]';
UPDATE temporal_rng SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[8,8]';
INSERT INTO temporal_rng VALUES ('[22,22]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[22,22]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[14,14]', '2018-01-01', '2021-01-01', '[22,22]');
UPDATE temporal_rng SET id = '[23,23]' WHERE id = '[22,22]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[14,14]';

-- test FK parent deletes SET DEFAULT
INSERT INTO temporal_rng VALUES ('[14,14]', tsrange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[9,9]', '2018-01-01', '2021-01-01', '[14,14]');
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,14]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[9,9]';
DELETE FROM temporal_rng WHERE id = '[14,14]';
SELECT * FROM temporal_fk_per2rng WHERE id = '[9,9]';
INSERT INTO temporal_rng VALUES ('[24,24]', tsrange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng VALUES ('[24,24]', tsrange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_per2rng VALUES ('[15,15]', '2018-01-01', '2021-01-01', '[24,24]');
DELETE FROM temporal_rng WHERE id = '[24,24]' AND valid_at @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2rng WHERE id = '[15,15]';


--
-- test FOREIGN KEY, PERIOD references PERIOD
--

DELETE FROM temporal_fk_rng2per;
DELETE FROM temporal_per;
INSERT INTO temporal_per VALUES ('[1,1]', '2018-01-02', '2018-02-03');
INSERT INTO temporal_per VALUES ('[1,1]', '2018-03-03', '2018-04-04');
INSERT INTO temporal_per VALUES ('[2,2]', '2018-01-01', '2018-01-05');
INSERT INTO temporal_per VALUES ('[3,3]', '2018-01-01', NULL);

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from int4,
  valid_til int4,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id, PERIOD valid_at)
);

-- with inferred PK on the referenced table:
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
);
DROP TABLE temporal_fk_per2per;

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_per (id, PERIOD id)
);

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from timestamp,
	valid_til timestamp,
	PERIOD FOR valid_at (valid_from, valid_til),
	parent_id int4range,
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per (id, PERIOD valid_at);
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
-- with inferred PK on the referenced table:
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;

-- should fail because of duplicate referenced columns:
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk2
	FOREIGN KEY (parent_id, PERIOD parent_id)
	REFERENCES temporal_per (id, PERIOD id);

--
-- test with rows already
--

DELETE FROM temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
INSERT INTO temporal_fk_per2per VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
INSERT INTO temporal_fk_per2per VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
-- should fail:
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
-- okay again:
DELETE FROM temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;

--
-- test pg_get_constraintdef
--

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_per2per_fk';

--
-- test FK child inserts
--

INSERT INTO temporal_fk_per2per VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
-- should fail:
INSERT INTO temporal_fk_per2per VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
-- now it should work:
INSERT INTO temporal_per VALUES ('[1,1]', '2018-02-03', '2018-03-03');
INSERT INTO temporal_fk_per2per VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');

--
-- test FK child updates
--
UPDATE temporal_fk_per2per SET valid_from = '2018-01-02', valid_til = '2018-03-01' WHERE id = '[1,1]';
-- should fail:
UPDATE temporal_fk_per2per SET valid_from = '2018-01-02', valid_til = '2018-05-01' WHERE id = '[1,1]';
UPDATE temporal_fk_per2per SET parent_id = '[8,8]' WHERE id = '[1,1]';

--
-- test FK parent updates NO ACTION
--

-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced:
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- clean up:
DELETE FROM temporal_fk_per2per WHERE parent_id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[5,5]';

--
-- test FK parent updates RESTRICT
--

ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON DELETE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced:
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- clean up:
DELETE FROM temporal_fk_per2per WHERE parent_id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[5,5]';

--
-- test FK parent deletes NO ACTION
--

ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test FK parent deletes RESTRICT
--

ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON DELETE RESTRICT;
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per VALUES ('[5,5]', '2018-01-01', '2018-02-01');
INSERT INTO temporal_per VALUES ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test ON UPDATE/DELETE options
--

-- test FK parent updates CASCADE
INSERT INTO temporal_per VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[4,4]', '2018-01-01', '2021-01-01', '[6,6]');
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[4,4]';
UPDATE temporal_per SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[4,4]';
INSERT INTO temporal_per VALUES ('[15,15]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[15,15]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[10,10]', '2018-01-01', '2021-01-01', '[15,15]');
UPDATE temporal_per SET id = '[16,16]' WHERE id = '[15,15]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[10,10]';

-- test FK parent deletes CASCADE
INSERT INTO temporal_per VALUES ('[8,8]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[5,5]', '2018-01-01', '2021-01-01', '[8,8]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,8]';
SELECT * FROM temporal_fk_per2per WHERE id = '[5,5]';
DELETE FROM temporal_per WHERE id = '[8,8]';
SELECT * FROM temporal_fk_per2per WHERE id = '[5,5]';
INSERT INTO temporal_per VALUES ('[17,17]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[17,17]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[11,11]', '2018-01-01', '2021-01-01', '[17,17]');
DELETE FROM temporal_per WHERE id = '[17,17]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[11,11]';

-- test FK parent updates SET NULL
INSERT INTO temporal_per VALUES ('[9,9]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[6,6]', '2018-01-01', '2021-01-01', '[9,9]');
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_per2per WHERE id = '[6,6]';
UPDATE temporal_per SET id = '[10,10]' WHERE id = '[9,9]';
SELECT * FROM temporal_fk_per2per WHERE id = '[6,6]';
INSERT INTO temporal_per VALUES ('[18,18]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[18,18]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[12,12]', '2018-01-01', '2021-01-01', '[18,18]');
UPDATE temporal_per SET id = '[19,19]' WHERE id = '[18,18]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[12,12]';

-- test FK parent deletes SET NULL
INSERT INTO temporal_per VALUES ('[11,11]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[7,7]', '2018-01-01', '2021-01-01', '[11,11]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,11]';
SELECT * FROM temporal_fk_per2per WHERE id = '[7,7]';
DELETE FROM temporal_per WHERE id = '[11,11]';
SELECT * FROM temporal_fk_per2per WHERE id = '[7,7]';
INSERT INTO temporal_per VALUES ('[20,20]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[20,20]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[13,13]', '2018-01-01', '2021-01-01', '[20,20]');
DELETE FROM temporal_per WHERE id = '[20,20]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[13,13]';

-- test FK parent updates SET DEFAULT
INSERT INTO temporal_per VALUES ('[-1,-1]', null, null);
INSERT INTO temporal_per VALUES ('[12,12]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[8,8]', '2018-01-01', '2021-01-01', '[12,12]');
ALTER TABLE temporal_fk_per2per
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_per2per WHERE id = '[8,8]';
UPDATE temporal_per SET id = '[13,13]' WHERE id = '[12,12]';
SELECT * FROM temporal_fk_per2per WHERE id = '[8,8]';
INSERT INTO temporal_per VALUES ('[22,22]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[22,22]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[14,14]', '2018-01-01', '2021-01-01', '[22,22]');
UPDATE temporal_per SET id = '[23,23]' WHERE id = '[22,22]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[14,14]';

-- test FK parent deletes SET DEFAULT
INSERT INTO temporal_per VALUES ('[14,14]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[9,9]', '2018-01-01', '2021-01-01', '[14,14]');
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,14]';
SELECT * FROM temporal_fk_per2per WHERE id = '[9,9]';
DELETE FROM temporal_per WHERE id = '[14,14]';
SELECT * FROM temporal_fk_per2per WHERE id = '[9,9]';
INSERT INTO temporal_per VALUES ('[24,24]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per VALUES ('[24,24]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per VALUES ('[15,15]', '2018-01-01', '2021-01-01', '[24,24]');
DELETE FROM temporal_per WHERE id = '[24,24]' AND tsrange(valid_from, valid_til) @> '2019-01-01'::timestamp;
SELECT * FROM temporal_fk_per2per WHERE id = '[15,15]';
