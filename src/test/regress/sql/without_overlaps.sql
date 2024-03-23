-- Tests for WITHOUT OVERLAPS.
--
-- We leave behind several tables to test pg_dump etc:
-- temporal_rng, temporal_rng2,
-- temporal_fk_rng2rng, temporal_fk2_rng2rng.

SET datestyle TO ISO, YMD;

--
-- test input parser
--

-- PK with no columns just WITHOUT OVERLAPS:

CREATE TABLE temporal_rng (
	valid_at daterange,
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
	valid_at daterange,
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
	valid_at daterange,
	CONSTRAINT temporal_rng2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng2_pk';

-- PK with one column plus a PERIOD:
CREATE TABLE temporal_per (
  id int4range,
  valid_from date,
  valid_til date,
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
  valid_from date,
  valid_til date,
  PERIOD FOR valid_at (valid_from, valid_til),
  CONSTRAINT temporal_per2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_per2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per2_pk';

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

-- PK with one column plus a multirange:
CREATE TABLE temporal_mltrng (
  id int4range,
  valid_at datemultirange,
  CONSTRAINT temporal_mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_mltrng

-- PK with two columns plus a multirange:
-- We don't drop this table because tests below also need multiple scalar columns.
CREATE TABLE temporal_mltrng2 (
	id1 int4range,
	id2 int4range,
	valid_at datemultirange,
	CONSTRAINT temporal_mltrng2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_mltrng2
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_mltrng2_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_mltrng2_pk';

-- UNIQUE with no columns just WITHOUT OVERLAPS:

CREATE TABLE temporal_rng3 (
	valid_at daterange,
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
	valid_at daterange,
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
	valid_at daterange,
	CONSTRAINT temporal_rng3_uq UNIQUE (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng3_uq';
DROP TABLE temporal_rng3;

-- UNIQUE with one column plus a PERIOD:
CREATE TABLE temporal_per3 (
  id int4range,
  valid_from date,
  valid_til date,
  PERIOD FOR valid_at (valid_from, valid_til),
  CONSTRAINT temporal_per3_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_per3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
DROP TABLE temporal_per3;

-- UNIQUE with two columns plus a PERIOD:
CREATE TABLE temporal_per3 (
  id1 int4range,
  id2 int4range,
  valid_from date,
  valid_til date,
  PERIOD FOR valid_at (valid_from, valid_til),
  CONSTRAINT temporal_per3_uq UNIQUE (id1, id2, valid_at WITHOUT OVERLAPS)
);
\d temporal_per3
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_per3_uq';
DROP TABLE temporal_per3;

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
	valid_at daterange
);
ALTER TABLE temporal_rng
	ADD CONSTRAINT temporal_rng_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);

-- PK with USING INDEX (not possible):
CREATE TABLE temporal3 (
	id int4range,
	valid_at daterange
);
CREATE INDEX idx_temporal3_uq ON temporal3 USING gist (id, valid_at);
ALTER TABLE temporal3
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY USING INDEX idx_temporal3_uq;
DROP TABLE temporal3;

-- UNIQUE with USING INDEX (not possible):
CREATE TABLE temporal3 (
	id int4range,
	valid_at daterange
);
CREATE INDEX idx_temporal3_uq ON temporal3 USING gist (id, valid_at);
ALTER TABLE temporal3
	ADD CONSTRAINT temporal3_uq
	UNIQUE USING INDEX idx_temporal3_uq;
DROP TABLE temporal3;

-- UNIQUE with USING [UNIQUE] INDEX (possible but not a temporal constraint):
CREATE TABLE temporal3 (
	id int4range,
	valid_at daterange
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
	ADD COLUMN valid_at daterange,
	ADD CONSTRAINT temporal3_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- Add range column and UNIQUE constraint at the same time
CREATE TABLE temporal3 (
	id int4range
);
ALTER TABLE temporal3
	ADD COLUMN valid_at daterange,
	ADD CONSTRAINT temporal3_uq
	UNIQUE (id, valid_at WITHOUT OVERLAPS);
DROP TABLE temporal3;

-- PRIMARY KEY with PERIOD already there
CREATE TABLE temporal3 (
  id int4range,
  valid_from date,
  valid_til date,
  PERIOD FOR valid_at (valid_from, valid_til)
);
ALTER TABLE temporal3
  ADD CONSTRAINT temporal3_pk
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
\d temporal3
DROP TABLE temporal3;

-- PRIMARY KEY with PERIOD too
CREATE TABLE temporal3 (
  id int4range,
  valid_from date,
  valid_til date
);
ALTER TABLE temporal3
  ADD PERIOD FOR valid_at (valid_from, valid_til),
  ADD CONSTRAINT temporal3_pk
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);
\d temporal3
DROP TABLE temporal3;

-- UNIQUE with PERIOD already there
CREATE TABLE temporal3 (
  id int4range,
  valid_from date,
  valid_til date,
  PERIOD FOR valid_at (valid_from, valid_til)
);
ALTER TABLE temporal3
  ADD CONSTRAINT temporal3_uq
  UNIQUE (id, valid_at WITHOUT OVERLAPS);
\d temporal3
DROP TABLE temporal3;

-- UNIQUE with PERIOD too
CREATE TABLE temporal3 (
  id int4range,
  valid_from date,
  valid_til date
);
ALTER TABLE temporal3
  ADD PERIOD FOR valid_at (valid_from, valid_til),
  ADD CONSTRAINT temporal3_uq
  UNIQUE (id, valid_at WITHOUT OVERLAPS);
\d temporal3
DROP TABLE temporal3;

--
-- test PK inserts
--

-- okay:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[2,3)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', daterange('2018-01-01', NULL));

-- should fail:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES (NULL, daterange('2018-01-01', '2018-01-05'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[3,4)', NULL);

-- okay:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[1,2)', datemultirange(daterange('2018-01-02', '2018-02-03')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[1,2)', datemultirange(daterange('2018-03-03', '2018-04-04')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[2,3)', datemultirange(daterange('2018-01-01', '2018-01-05')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[3,4)', datemultirange(daterange('2018-01-01', NULL)));

-- should fail:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[1,2)', datemultirange(daterange('2018-01-01', '2018-01-05')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES (NULL, datemultirange(daterange('2018-01-01', '2018-01-05')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[3,4)', NULL);

SELECT * FROM temporal_mltrng ORDER BY id, valid_at;

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
  ('[1,2)', daterange('2000-01-01', '2010-01-01'), '[7,8)', 'foo'),
  ('[2,3)', daterange('2000-01-01', '2010-01-01'), '[9,10)', 'bar')
;
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-05-01' TO '2000-07-01'
  SET name = name || '1';
UPDATE temporal3 FOR PORTION OF valid_at FROM '2000-04-01' TO '2000-06-01'
  SET name = name || '2'
  WHERE id = '[2,3)';
SELECT * FROM temporal3 ORDER BY id, valid_at;
-- conflicting id only:
INSERT INTO temporal3 (id, valid_at, id2, name)
  VALUES
  ('[1,2)', daterange('2005-01-01', '2006-01-01'), '[8,9)', 'foo3');
-- conflicting id2 only:
INSERT INTO temporal3 (id, valid_at, id2, name)
  VALUES
  ('[3,4)', daterange('2005-01-01', '2010-01-01'), '[9,10)', 'bar3');
DROP TABLE temporal3;

--
-- test changing the PK's dependencies
--

CREATE TABLE temporal3 (
	id int4range,
	valid_at daterange,
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
CREATE TABLE tp1 PARTITION OF temporal_partitioned FOR VALUES IN ('[1,2)', '[2,3)');
CREATE TABLE tp2 PARTITION OF temporal_partitioned FOR VALUES IN ('[3,4)', '[4,5)');
INSERT INTO temporal_partitioned (id, valid_at, name) VALUES
  ('[1,2)', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,2)', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[3,4)', daterange('2000-01-01', '2010-01-01'), 'three');
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
SELECT * FROM tp1 ORDER BY id, valid_at;
SELECT * FROM tp2 ORDER BY id, valid_at;
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  SET name = 'one2'
  WHERE id = '[1,2)';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-02-20' TO '2000-02-25'
  SET id = '[4,5)'
  WHERE name = 'one';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01'
  SET id = '[2,3)'
  WHERE name = 'three';
DELETE FROM temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  WHERE id = '[3,4)';
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
DROP TABLE temporal_partitioned;

-- temporal UNIQUE:
CREATE TABLE temporal_partitioned (
	id int4range,
	valid_at daterange,
  name text,
	CONSTRAINT temporal_paritioned_uq UNIQUE (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE tp1 PARTITION OF temporal_partitioned FOR VALUES IN ('[1,2)', '[2,3)');
CREATE TABLE tp2 PARTITION OF temporal_partitioned FOR VALUES IN ('[3,4)', '[4,5)');
INSERT INTO temporal_partitioned (id, valid_at, name) VALUES
  ('[1,2)', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,2)', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[3,4)', daterange('2000-01-01', '2010-01-01'), 'three');
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
SELECT * FROM tp1 ORDER BY id, valid_at;
SELECT * FROM tp2 ORDER BY id, valid_at;
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  SET name = 'one2'
  WHERE id = '[1,2)';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-02-20' TO '2000-02-25'
  SET id = '[4,5)'
  WHERE name = 'one';
UPDATE  temporal_partitioned
  FOR PORTION OF valid_at FROM '2002-01-01' TO '2003-01-01'
  SET id = '[2,3)'
  WHERE name = 'three';
DELETE FROM temporal_partitioned
  FOR PORTION OF valid_at FROM '2000-01-15' TO '2000-02-15'
  WHERE id = '[3,4)';
SELECT * FROM temporal_partitioned ORDER BY id, valid_at;
DROP TABLE temporal_partitioned;

--
-- test FK dependencies
--

-- can't drop a range referenced by an FK, unless with CASCADE
CREATE TABLE temporal3 (
	id int4range,
	valid_at daterange,
	CONSTRAINT temporal3_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
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

-- test table setup
DROP TABLE temporal_rng;
CREATE TABLE temporal_rng (id int4range, valid_at daterange);
ALTER TABLE temporal_rng
  ADD CONSTRAINT temporal_rng_pk
  PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at int4range,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk2 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk2 FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);

-- works: PERIOD for both referenced and referencing
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);
\d temporal_fk_rng2rng
DROP TABLE temporal_fk_rng2rng;

-- with mismatched PERIOD columns:

-- (parent_id, PERIOD valid_at) REFERENCES (id, valid_at)
-- REFERENCES part should specify PERIOD
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, valid_at)
-- both should specify PERIOD:
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_rng (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, PERIOD valid_at)
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);
-- (parent_id, valid_at) REFERENCES [implicit]
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_rng
);
-- (parent_id, PERIOD valid_at) REFERENCES (id)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng (id)
);
-- (parent_id) REFERENCES (id, PERIOD valid_at)
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_rng (id, PERIOD valid_at)
);
-- with inferred PK on the referenced table:
-- (parent_id, PERIOD valid_at) REFERENCES [implicit]
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
);
DROP TABLE temporal_fk_rng2rng;
-- (parent_id) REFERENCES [implicit]
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_rng
);

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_rng (id, PERIOD id)
);

-- Two scalar columns
DROP TABLE temporal_rng2;
CREATE TABLE temporal_rng2 (
  id1 int4range,
  id2 int4range,
  valid_at daterange,
  CONSTRAINT temporal_rng2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);

CREATE TABLE temporal_fk2_rng2rng (
	id int4range,
	valid_at daterange,
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
	valid_at daterange,
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
	valid_at daterange,
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
	ALTER COLUMN valid_at TYPE tsrange USING tsrange(lower(valid_at), upper(valid_at));
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
ALTER TABLE temporal_fk_rng2rng
	ALTER COLUMN valid_at TYPE daterange USING daterange(lower(valid_at)::date, upper(valid_at)::date);

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
DELETE FROM temporal_rng;
INSERT INTO temporal_rng (id, valid_at) VALUES
  ('[1,2)', daterange('2018-01-02', '2018-02-03')),
  ('[1,2)', daterange('2018-03-03', '2018-04-04')),
  ('[2,3)', daterange('2018-01-01', '2018-01-05')),
  ('[3,4)', daterange('2018-01-01', NULL));

ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-01'), '[1,2)');
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[2,3)', daterange('2018-01-02', '2018-04-01'), '[1,2)');
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
-- test FK referencing inserts
--

INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[1,2)', daterange('2018-01-02', '2018-02-01'), '[1,2)');
-- should fail:
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[2,3)', daterange('2018-01-02', '2018-04-01'), '[1,2)');
-- now it should work:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[1,2)', daterange('2018-02-03', '2018-03-03'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[2,3)', daterange('2018-01-02', '2018-04-01'), '[1,2)');

--
-- test FK referencing updates
--

UPDATE temporal_fk_rng2rng SET valid_at = daterange('2018-01-02', '2018-03-01') WHERE id = '[1,2)';
-- should fail:
UPDATE temporal_fk_rng2rng SET valid_at = daterange('2018-01-02', '2018-05-01') WHERE id = '[1,2)';
UPDATE temporal_fk_rng2rng SET parent_id = '[8,9)' WHERE id = '[1,2)';

-- ALTER FK DEFERRABLE

BEGIN;
  INSERT INTO temporal_rng (id, valid_at) VALUES
    ('[5,6)', daterange('2018-01-01', '2018-02-01')),
    ('[5,6)', daterange('2018-02-01', '2018-03-01'));
  INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES
    ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_rng WHERE id = '[5,6)'; --should not fail yet.
COMMIT; -- should fail here.

--
-- test FK referenced updates NO ACTION
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON UPDATE NO ACTION;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01') WHERE id = '[5,6)';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_rng WHERE id = '[5,6)';
INSERT INTO temporal_rng (id, valid_at) VALUES
  ('[5,6)', daterange('2018-01-01', '2018-02-01')),
  ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
UPDATE temporal_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced:
UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- a PK update that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;

  UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
COMMIT;
-- changing the scalar part fails:
UPDATE temporal_rng SET id = '[7,8)'
WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- changing an unreferenced part is okay:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,8)'
WHERE id = '[5,6)';
-- changing just a part fails:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,8)'
WHERE id = '[5,6)';
SELECT * FROM temporal_rng WHERE id in ('[5,6)', '[7,8)') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_rng2rng WHERE id in ('[3,4)') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,4)';
UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- test FK referenced updates RESTRICT
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON UPDATE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01') WHERE id = '[5,6)';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_rng WHERE id = '[5,6)';
INSERT INTO temporal_rng (id, valid_at) VALUES
  ('[5,6)', daterange('2018-01-01', '2018-02-01')),
  ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
UPDATE temporal_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- a PK update that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;
  UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
ROLLBACK;
-- changing the scalar part fails:
UPDATE temporal_rng SET id = '[7,8)'
WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- changing an unreferenced part is okay:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,8)'
WHERE id = '[5,6)';
-- changing just a part fails:
UPDATE temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,8)'
WHERE id = '[5,6)';
SELECT * FROM temporal_rng WHERE id in ('[5,6)', '[7,8)') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_rng2rng WHERE id in ('[3,4)') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,4)';
UPDATE temporal_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- test FK referenced deletes NO ACTION
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,6)';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng (id, valid_at) VALUES
  ('[5,6)', daterange('2018-01-01', '2018-02-01')),
  ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
-- a PK delete that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
COMMIT;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,6)';
-- deleting just a part fails:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,6)';
SELECT * FROM temporal_rng WHERE id in ('[5,6)', '[7,8)') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_rng2rng WHERE id in ('[3,4)') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,4)';
DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- test FK referenced deletes RESTRICT
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk;
ALTER TABLE temporal_fk_rng2rng
	ADD CONSTRAINT temporal_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_rng
	ON DELETE RESTRICT;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
DELETE FROM temporal_rng WHERE id = '[5,6)';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_rng (id, valid_at) VALUES
  ('[5,6)', daterange('2018-01-01', '2018-02-01')),
  ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- a PK delete that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_rng2rng
    ALTER CONSTRAINT temporal_fk_rng2rng_fk
    DEFERRABLE INITIALLY DEFERRED;
  DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
ROLLBACK;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,6)';
-- deleting just a part fails:
DELETE FROM temporal_rng
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,6)';
SELECT * FROM temporal_rng WHERE id in ('[5,6)', '[7,8)') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_rng2rng WHERE id in ('[3,4)') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_rng2rng WHERE id = '[3,4)';
DELETE FROM temporal_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- rng2rng test ON UPDATE/DELETE options
--
-- TOC:
-- referenced updates CASCADE
-- referenced deletes CASCADE
-- referenced updates SET NULL
-- referenced deletes SET NULL
-- referenced updates SET DEFAULT
-- referenced deletes SET DEFAULT
-- referenced updates CASCADE (two scalar cols)
-- referenced deletes CASCADE (two scalar cols)
-- referenced updates SET NULL (two scalar cols)
-- referenced deletes SET NULL (two scalar cols)
-- referenced deletes SET NULL (two scalar cols, SET NULL subset)
-- referenced updates SET DEFAULT (two scalar cols)
-- referenced deletes SET DEFAULT (two scalar cols)
-- referenced deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)

--
-- test FK referenced updates CASCADE
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
UPDATE temporal_rng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes CASCADE
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_rng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
DELETE FROM temporal_rng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET NULL
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
ALTER TABLE temporal_fk_rng2rng
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
UPDATE temporal_rng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_rng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
DELETE FROM temporal_rng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET DEFAULT
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[-1,-1]', daterange(null, null));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
ALTER TABLE temporal_fk_rng2rng
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
UPDATE temporal_rng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT
--

TRUNCATE temporal_rng, temporal_fk_rng2rng;
INSERT INTO temporal_rng (id, valid_at) VALUES ('[-1,-1]', daterange(null, null));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_rng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng (id, valid_at) VALUES ('[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk_rng2rng (id, valid_at, parent_id) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
DELETE FROM temporal_rng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates CASCADE (two scalar cols)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
UPDATE temporal_rng2 SET id1 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes CASCADE (two scalar cols)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
DELETE FROM temporal_rng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET NULL (two scalar cols)
--
TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_rng2rng
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
UPDATE temporal_rng2 SET id1 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL (two scalar cols)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
DELETE FROM temporal_rng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL (two scalar cols, SET NULL subset)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
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
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_rng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
DELETE FROM temporal_rng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[-1,-1]', daterange(null, null));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_rng2rng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
  ALTER COLUMN parent_id2 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_rng2rng_fk,
	ADD CONSTRAINT temporal_fk2_rng2rng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_rng2
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,8)', id2 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_rng2 SET id1 = '[7,8)', id2 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
UPDATE temporal_rng2 SET id1 = '[9,10)', id2 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[-1,-1]', daterange(null, null));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_rng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
DELETE FROM temporal_rng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)
--

TRUNCATE temporal_rng2, temporal_fk2_rng2rng;
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[6,7)', daterange(null, null));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', daterange('2018-01-01', '2021-01-01'), '[6,7)', '[6,7)');
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
DELETE FROM temporal_rng2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_rng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_rng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[8,9)', daterange(null, null));
INSERT INTO temporal_fk2_rng2rng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', daterange('2018-01-01', '2021-01-01'), '[8,9)', '[8,9)');
DELETE FROM temporal_rng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_rng2rng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FOREIGN KEY, multirange references multirange
--

-- Can't create a FK with a mismatched multirange type
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at int4multirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk2 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk2 FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng (id, PERIOD valid_at)
);

CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng (id, PERIOD valid_at)
);
DROP TABLE temporal_fk_mltrng2mltrng;

-- with mismatched PERIOD columns:

-- (parent_id, PERIOD valid_at) REFERENCES (id, valid_at)
-- REFERENCES part should specify PERIOD
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, valid_at)
-- both should specify PERIOD:
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_mltrng (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, PERIOD valid_at)
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_mltrng (id, PERIOD valid_at)
);
-- (parent_id, valid_at) REFERENCES [implicit]
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_mltrng
);
-- (parent_id, PERIOD valid_at) REFERENCES (id)
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng (id)
);
-- (parent_id) REFERENCES (id, PERIOD valid_at)
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_mltrng (id, PERIOD valid_at)
);
-- with inferred PK on the referenced table:
-- (parent_id, PERIOD valid_at) REFERENCES [implicit]
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng
);
DROP TABLE temporal_fk_mltrng2mltrng;
-- (parent_id) REFERENCES [implicit]
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_mltrng
);

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_mltrng2mltrng_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_mltrng (id, PERIOD id)
);

-- Two scalar columns
CREATE TABLE temporal_fk2_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id1 int4range,
	parent_id2 int4range,
	CONSTRAINT temporal_fk2_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk2_mltrng2mltrng_fk FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2 (id1, id2, PERIOD valid_at)
);
\d temporal_fk2_mltrng2mltrng
DROP TABLE temporal_fk2_mltrng2mltrng;

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id int4range,
	CONSTRAINT temporal_fk_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at);
-- Two scalar columns:
CREATE TABLE temporal_fk2_mltrng2mltrng (
	id int4range,
	valid_at datemultirange,
	parent_id1 int4range,
	parent_id2 int4range,
	CONSTRAINT temporal_fk2_mltrng2mltrng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk2_mltrng2mltrng
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
	FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
	REFERENCES temporal_mltrng2 (id1, id2, PERIOD valid_at);
\d temporal_fk2_mltrng2mltrng

-- should fail because of duplicate referenced columns:
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk2
	FOREIGN KEY (parent_id, PERIOD parent_id)
	REFERENCES temporal_mltrng (id, PERIOD id);

--
-- test with rows already
--

DELETE FROM temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[1,2)', datemultirange(daterange('2018-01-02', '2018-02-01')), '[1,2)');
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at);
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[2,3)', datemultirange(daterange('2018-01-02', '2018-04-01')), '[1,2)');
-- should fail:
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at);
-- okay again:
DELETE FROM temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at);

--
-- test pg_get_constraintdef
--

SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_mltrng2mltrng_fk';

--
-- test FK referencing inserts
--

INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[1,2)', datemultirange(daterange('2018-01-02', '2018-02-01')), '[1,2)');
-- should fail:
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[2,3)', datemultirange(daterange('2018-01-02', '2018-04-01')), '[1,2)');
-- now it should work:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[1,2)', datemultirange(daterange('2018-02-03', '2018-03-03')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[2,3)', datemultirange(daterange('2018-01-02', '2018-04-01')), '[1,2)');

--
-- test FK referencing updates
--

UPDATE temporal_fk_mltrng2mltrng SET valid_at = datemultirange(daterange('2018-01-02', '2018-03-01')) WHERE id = '[1,2)';
-- should fail:
UPDATE temporal_fk_mltrng2mltrng SET valid_at = datemultirange(daterange('2018-01-02', '2018-05-01')) WHERE id = '[1,2)';
UPDATE temporal_fk_mltrng2mltrng SET parent_id = '[8,9)' WHERE id = '[1,2)';

-- ALTER FK DEFERRABLE

BEGIN;
  INSERT INTO temporal_mltrng (id, valid_at) VALUES
    ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01'))),
    ('[5,6)', datemultirange(daterange('2018-02-01', '2018-03-01')));
  INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES
    ('[3,4)', datemultirange(daterange('2018-01-05', '2018-01-10')), '[5,6)');
  ALTER TABLE temporal_fk_mltrng2mltrng
    ALTER CONSTRAINT temporal_fk_mltrng2mltrng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_mltrng WHERE id = '[5,6)'; --should not fail yet.
COMMIT; -- should fail here.

--
-- test FK referenced updates NO ACTION
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at)
	ON UPDATE NO ACTION;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01')));
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01')) WHERE id = '[5,6)';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_mltrng WHERE id = '[5,6)';
INSERT INTO temporal_mltrng (id, valid_at) VALUES
  ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01'))),
  ('[5,6)', datemultirange(daterange('2018-02-01', '2018-03-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[3,4)', datemultirange(daterange('2018-01-05', '2018-01-10')), '[5,6)');
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-02-01', '2016-03-01'))
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-02-01', '2018-03-01'));
-- a PK update that fails because both are referenced:
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01'))
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
-- a PK update that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_mltrng2mltrng
    ALTER CONSTRAINT temporal_fk_mltrng2mltrng_fk
    DEFERRABLE INITIALLY DEFERRED;

  UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01'))
  WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
COMMIT;
-- changing the scalar part fails:
UPDATE temporal_mltrng SET id = '[7,8)'
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
-- changing an unreferenced part is okay:
UPDATE temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-02', '2018-01-03')))
SET id = '[7,8)'
WHERE id = '[5,6)';
-- changing just a part fails:
UPDATE temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-05', '2018-01-10')))
SET id = '[7,8)'
WHERE id = '[5,6)';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_mltrng2mltrng WHERE id = '[3,4)';
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01'))
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));

--
-- test FK referenced updates RESTRICT
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at)
	ON UPDATE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01')));
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01')) WHERE id = '[5,6)';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_mltrng WHERE id = '[5,6)';
INSERT INTO temporal_mltrng (id, valid_at) VALUES
  ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01'))),
  ('[5,6)', datemultirange(daterange('2018-02-01', '2018-03-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[3,4)', datemultirange(daterange('2018-01-05', '2018-01-10')), '[5,6)');
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-02-01', '2016-03-01'))
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-02-01', '2018-03-01'));
-- a PK update that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_mltrng2mltrng
    ALTER CONSTRAINT temporal_fk_mltrng2mltrng_fk
    DEFERRABLE INITIALLY DEFERRED;

  UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01'))
  WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
ROLLBACK;
-- changing the scalar part fails:
UPDATE temporal_mltrng SET id = '[7,8)'
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
-- changing an unreferenced part is okay:
UPDATE temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-02', '2018-01-03')))
SET id = '[7,8)'
WHERE id = '[5,6)';
-- changing just a part fails:
UPDATE temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-05', '2018-01-10')))
SET id = '[7,8)'
WHERE id = '[5,6)';
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_mltrng2mltrng WHERE id = '[3,4)';
UPDATE temporal_mltrng SET valid_at = datemultirange(daterange('2016-01-01', '2016-02-01'))
WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));

--
-- test FK referenced deletes NO ACTION
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at);
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01')));
DELETE FROM temporal_mltrng WHERE id = '[5,6)';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_mltrng (id, valid_at) VALUES
  ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01'))),
  ('[5,6)', datemultirange(daterange('2018-02-01', '2018-03-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[3,4)', datemultirange(daterange('2018-01-05', '2018-01-10')), '[5,6)');
DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-02-01', '2018-03-01'));
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
-- a PK delete that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_mltrng2mltrng
    ALTER CONSTRAINT temporal_fk_mltrng2mltrng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
COMMIT;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-02', '2018-01-03')))
WHERE id = '[5,6)';
-- deleting just a part fails:
DELETE FROM temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-05', '2018-01-10')))
WHERE id = '[5,6)';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_mltrng2mltrng WHERE id = '[3,4)';
DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));

--
-- test FK referenced deletes RESTRICT
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk;
ALTER TABLE temporal_fk_mltrng2mltrng
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_mltrng (id, PERIOD valid_at)
	ON DELETE RESTRICT;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01')));
DELETE FROM temporal_mltrng WHERE id = '[5,6)';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_mltrng (id, valid_at) VALUES
  ('[5,6)', datemultirange(daterange('2018-01-01', '2018-02-01'))),
  ('[5,6)', datemultirange(daterange('2018-02-01', '2018-03-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[3,4)', datemultirange(daterange('2018-01-05', '2018-01-10')), '[5,6)');
DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-02-01', '2018-03-01'));
-- a PK delete that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_mltrng2mltrng
    ALTER CONSTRAINT temporal_fk_mltrng2mltrng_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));
ROLLBACK;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-02', '2018-01-03')))
WHERE id = '[5,6)';
-- deleting just a part fails:
DELETE FROM temporal_mltrng
FOR PORTION OF valid_at (datemultirange(daterange('2018-01-05', '2018-01-10')))
WHERE id = '[5,6)';
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_mltrng2mltrng WHERE id = '[3,4)';
DELETE FROM temporal_mltrng WHERE id = '[5,6)' AND valid_at = datemultirange(daterange('2018-01-01', '2018-02-01'));

--
-- mltrng2mltrng test ON UPDATE/DELETE options
--
-- TOC:
-- referenced updates CASCADE
-- referenced deletes CASCADE
-- referenced updates SET NULL
-- referenced deletes SET NULL
-- referenced updates SET DEFAULT
-- referenced deletes SET DEFAULT
-- referenced updates CASCADE (two scalar cols)
-- referenced deletes CASCADE (two scalar cols)
-- referenced updates SET NULL (two scalar cols)
-- referenced deletes SET NULL (two scalar cols)
-- referenced deletes SET NULL (two scalar cols, SET NULL subset)
-- referenced updates SET DEFAULT (two scalar cols)
-- referenced deletes SET DEFAULT (two scalar cols)
-- referenced deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)

--
-- test FK referenced updates CASCADE
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
UPDATE temporal_mltrng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes CASCADE
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_mltrng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
DELETE FROM temporal_mltrng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET NULL
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
ALTER TABLE temporal_fk_mltrng2mltrng
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
UPDATE temporal_mltrng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_mltrng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
DELETE FROM temporal_mltrng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET DEFAULT
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[-1,-1]', datemultirange(daterange(null, null)));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
ALTER TABLE temporal_fk_mltrng2mltrng
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk_mltrng2mltrng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_mltrng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
UPDATE temporal_mltrng SET id = '[9,10)' WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT
--

TRUNCATE temporal_mltrng, temporal_fk_mltrng2mltrng;
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[-1,-1]', datemultirange(daterange(null, null)));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_mltrng WHERE id = '[6,7)';
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng (id, valid_at) VALUES ('[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk_mltrng2mltrng (id, valid_at, parent_id) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)');
DELETE FROM temporal_mltrng WHERE id = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates CASCADE (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_mltrng2mltrng
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng2 SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
UPDATE temporal_mltrng2 SET id1 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes CASCADE (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_mltrng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
DELETE FROM temporal_mltrng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET NULL (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_mltrng2mltrng
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng2 SET id1 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
UPDATE temporal_mltrng2 SET id1 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_mltrng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
DELETE FROM temporal_mltrng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET NULL (two scalar cols, SET NULL subset)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_mltrng2mltrng
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET NULL (valid_at) ON UPDATE SET NULL;
-- ok:
ALTER TABLE temporal_fk2_mltrng2mltrng
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET NULL (parent_id1) ON UPDATE SET NULL;
-- leftovers on both sides:
DELETE FROM temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_mltrng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
DELETE FROM temporal_mltrng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced updates SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[-1,-1]', datemultirange(daterange(null, null)));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
ALTER TABLE temporal_fk2_mltrng2mltrng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
  ALTER COLUMN parent_id2 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) SET id1 = '[7,8)', id2 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_mltrng2 SET id1 = '[7,8)', id2 = '[7,8)' WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
UPDATE temporal_mltrng2 SET id1 = '[9,10)', id2 = '[9,10)' WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[-1,-1]', datemultirange(daterange(null, null)));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
-- leftovers on both sides:
DELETE FROM temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_mltrng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
DELETE FROM temporal_mltrng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FK referenced deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)
--

TRUNCATE temporal_mltrng2, temporal_fk2_mltrng2mltrng;
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[6,7)', datemultirange(daterange(null, null)));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[6,7)', '[6,7)', datemultirange(daterange('2018-01-01', '2021-01-01')));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[100,101)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[6,7)', '[6,7)');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_mltrng2mltrng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET DEFAULT (valid_at) ON UPDATE SET DEFAULT;
-- ok:
ALTER TABLE temporal_fk2_mltrng2mltrng
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_mltrng2mltrng_fk,
	ADD CONSTRAINT temporal_fk2_mltrng2mltrng_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_mltrng2
		ON DELETE SET DEFAULT (parent_id1) ON UPDATE SET DEFAULT;
-- leftovers on both sides:
DELETE FROM temporal_mltrng2 FOR PORTION OF valid_at (datemultirange(daterange('2019-01-01', '2020-01-01'))) WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_mltrng2 WHERE id1 = '[6,7)';
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[100,101)' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2018-01-01', '2020-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[8,9)', '[8,9)', datemultirange(daterange('2020-01-01', '2021-01-01')));
INSERT INTO temporal_mltrng2 (id1, id2, valid_at) VALUES ('[-1,-1]', '[8,9)', datemultirange(daterange(null, null)));
INSERT INTO temporal_fk2_mltrng2mltrng (id, valid_at, parent_id1, parent_id2) VALUES ('[200,201)', datemultirange(daterange('2018-01-01', '2021-01-01')), '[8,9)', '[8,9)');
DELETE FROM temporal_mltrng2 WHERE id1 = '[8,9)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_mltrng2mltrng WHERE id = '[200,201)' ORDER BY id, valid_at;

--
-- test FOREIGN KEY, PERIOD references PERIOD
--

-- test table setup
DROP TABLE temporal_per;
CREATE TABLE temporal_per (
	id int4range,
	valid_from date,
	valid_til date,
	PERIOD FOR valid_at (valid_from, valid_til)
);
ALTER TABLE temporal_per
	ADD CONSTRAINT temporal_per_pk
	PRIMARY KEY (id, valid_at WITHOUT OVERLAPS);

-- Can't create a FK with a mismatched range type
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from int,
	valid_til int,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id, PERIOD valid_at)
);

-- works: PERIOD for both referenced and referencing
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id, PERIOD valid_at)
);
\d temporal_fk_per2per
DROP TABLE temporal_fk_per2per;

-- with mismatched PERIOD columns:

-- (parent_id, PERIOD valid_at) REFERENCES (id, valid_at)
-- REFERENCES part should specify PERIOD
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, valid_at)
-- both should specify PERIOD:
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_per (id, valid_at)
);
-- (parent_id, valid_at) REFERENCES (id, PERIOD valid_at)
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_per (id, PERIOD valid_at)
);
-- (parent_id, valid_at) REFERENCES [implicit]
-- FOREIGN KEY part should specify PERIOD
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, valid_at)
		REFERENCES temporal_per
);
-- (parent_id, PERIOD valid_at) REFERENCES (id)
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per (id)
);
-- (parent_id) REFERENCES (id, PERIOD valid_at)
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_per (id, PERIOD valid_at)
);
-- with inferred PK on the referenced table:
-- (parent_id, PERIOD valid_at) REFERENCES [implicit]
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
);
DROP TABLE temporal_fk_per2per;
-- (parent_id) REFERENCES [implicit]
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id)
		REFERENCES temporal_per
);

-- should fail because of duplicate referenced columns:
CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk_per2per_fk FOREIGN KEY (parent_id, PERIOD parent_id)
		REFERENCES temporal_per (id, PERIOD id)
);

-- Two scalar columns
DROP TABLE temporal_per2;
CREATE TABLE temporal_per2 (
	id1 int4range,
	id2 int4range,
	valid_from date,
	valid_til date,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_per2_pk PRIMARY KEY (id1, id2, valid_at WITHOUT OVERLAPS)
);

CREATE TABLE temporal_fk2_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id1 int4range,
	parent_id2 int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk2_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_fk2_per2per_fk FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2 (id1, id2, PERIOD valid_at)
);
\d temporal_fk2_per2per
DROP TABLE temporal_fk2_per2per;

--
-- test ALTER TABLE ADD CONSTRAINT
--

CREATE TABLE temporal_fk_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per (id, PERIOD valid_at);
-- Two scalar columns:
CREATE TABLE temporal_fk2_per2per (
	id int4range,
	valid_from date,
	valid_til date,
	parent_id1 int4range,
	parent_id2 int4range,
	PERIOD FOR valid_at (valid_from, valid_til),
	CONSTRAINT temporal_fk2_per2per_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
ALTER TABLE temporal_fk2_per2per
	ADD CONSTRAINT temporal_fk2_per2per_fk
	FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
	REFERENCES temporal_per2 (id1, id2, PERIOD valid_at);
\d temporal_fk2_per2per

-- with inferred PK on the referenced table, and wrong column type:
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk,
	DROP PERIOD FOR valid_at,
	ALTER COLUMN valid_from TYPE timestamp,
	ALTER COLUMN valid_til TYPE timestamp,
	ADD PERIOD FOR valid_at (valid_from, valid_til);
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
ALTER TABLE temporal_fk_per2per
	DROP PERIOD FOR valid_at,
	ALTER COLUMN valid_from TYPE date,
	ALTER COLUMN valid_til TYPE date,
	ADD PERIOD FOR valid_at (valid_from, valid_til);

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
DELETE FROM temporal_per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
  ('[1,1]', '2018-01-02', '2018-02-03'),
  ('[1,1]', '2018-03-03', '2018-04-04'),
  ('[2,2]', '2018-01-01', '2018-01-05'),
  ('[3,3]', '2018-01-01', NULL);

ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
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
-- test FK referencing inserts
--

INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[1,1]', '2018-01-02', '2018-02-01', '[1,1]');
-- should fail:
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');
-- now it should work:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[1,1]', '2018-02-03', '2018-03-03');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[2,2]', '2018-01-02', '2018-04-01', '[1,1]');

--
-- test FK referencing updates
--

UPDATE temporal_fk_per2per SET valid_from = '2018-01-02', valid_til = '2018-03-01' WHERE id = '[1,1]';
-- should fail:
UPDATE temporal_fk_per2per SET valid_from = '2018-01-02', valid_til = '2018-05-01' WHERE id = '[1,1]';
UPDATE temporal_fk_per2per SET parent_id = '[8,8]' WHERE id = '[1,1]';

-- ALTER FK DEFERRABLE

BEGIN;
  INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
    ('[5,5]', '2018-01-01', '2018-02-01'),
    ('[5,5]', '2018-02-01', '2018-03-01');
  INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES
    ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
  ALTER TABLE temporal_fk_per2per
    ALTER CONSTRAINT temporal_fk_per2per_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_per WHERE id = '[5,5]'; --should not fail yet.
COMMIT; -- should fail here.

--
-- test FK parent updates NO ACTION
--

TRUNCATE temporal_per, temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON UPDATE NO ACTION;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
  ('[5,5]', '2018-01-01', '2018-02-01'),
  ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced:
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- a PK update that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_per2per
    ALTER CONSTRAINT temporal_fk_per2per_fk
    DEFERRABLE INITIALLY DEFERRED;

  UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
  WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
COMMIT;
-- changing the scalar part fails:
UPDATE temporal_per SET id = '[7,7]'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- changing an unreferenced part is okay:
UPDATE temporal_per
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- changing just a part fails:
UPDATE temporal_per
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,7]'
WHERE id = '[5,5]';
SELECT * FROM temporal_per WHERE id in ('[5,5]', '[7,7]') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_per2per WHERE id in ('[3,3]') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test FK parent updates RESTRICT
--

TRUNCATE temporal_per, temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON UPDATE RESTRICT;
-- a PK update that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[5,5]', '2018-01-01', '2018-02-01');
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01' WHERE id = '[5,5]';
-- a PK update that succeeds even though the numeric id is referenced because the range isn't:
DELETE FROM temporal_per WHERE id = '[5,5]';
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
  ('[5,5]', '2018-01-01', '2018-02-01'),
  ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
UPDATE temporal_per SET valid_from = '2016-02-01', valid_til = '2016-03-01'
WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK update that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_per2per
    ALTER CONSTRAINT temporal_fk_per2per_fk
    DEFERRABLE INITIALLY DEFERRED;

  UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
  WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
ROLLBACK;
-- changing the scalar part fails:
UPDATE temporal_per SET id = '[7,7]'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- changing an unreferenced part is okay:
UPDATE temporal_per
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
SET id = '[7,7]'
WHERE id = '[5,5]';
-- changing just a part fails:
UPDATE temporal_per
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
SET id = '[7,7]'
WHERE id = '[5,5]';
SELECT * FROM temporal_per WHERE id in ('[5,5]', '[7,7]') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_per2per WHERE id in ('[3,3]') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK update succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
UPDATE temporal_per SET valid_from = '2016-01-01', valid_til = '2016-02-01'
WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test FK parent deletes NO ACTION
--

TRUNCATE temporal_per, temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per;
-- a PK delete that succeeds because the numeric id isn't referenced:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
  ('[5,5]', '2018-01-01', '2018-02-01'),
  ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced:
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
-- a PK delete that fails because both are referenced, but not 'til commit:
BEGIN;
  ALTER TABLE temporal_fk_per2per
    ALTER CONSTRAINT temporal_fk_per2per_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_per WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');
COMMIT;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_per
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,5]';
-- deleting just a part fails:
DELETE FROM temporal_per
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,5]';
SELECT * FROM temporal_per WHERE id in ('[5,5]', '[7,7]') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_per2per WHERE id in ('[3,3]') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- test FK parent deletes RESTRICT
--

TRUNCATE temporal_per, temporal_fk_per2per;
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk;
ALTER TABLE temporal_fk_per2per
	ADD CONSTRAINT temporal_fk_per2per_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_per
	ON DELETE RESTRICT;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[5,5]', '2018-01-01', '2018-02-01');
DELETE FROM temporal_per WHERE id = '[5,5]';
-- a PK delete that succeeds even though the numeric id is referenced because the range isn't:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES
  ('[5,5]', '2018-01-01', '2018-02-01'),
  ('[5,5]', '2018-02-01', '2018-03-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[3,3]', '2018-01-05', '2018-01-10', '[5,5]');
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-02-01' AND valid_til = '2018-03-01';
-- a PK delete that fails because both are referenced (even before commit):
BEGIN;
  ALTER TABLE temporal_fk_per2per
    ALTER CONSTRAINT temporal_fk_per2per_fk
    DEFERRABLE INITIALLY DEFERRED;

  DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';
ROLLBACK;
-- deleting an unreferenced part is okay:
DELETE FROM temporal_per
FOR PORTION OF valid_at FROM '2018-01-02' TO '2018-01-03'
WHERE id = '[5,5]';
-- deleting just a part fails:
DELETE FROM temporal_per
FOR PORTION OF valid_at FROM '2018-01-05' TO '2018-01-10'
WHERE id = '[5,5]';
SELECT * FROM temporal_per WHERE id in ('[5,5]', '[7,7]') ORDER BY id, valid_at;
SELECT * FROM temporal_fk_per2per WHERE id in ('[3,3]') ORDER BY id, valid_at;
-- then delete the objecting FK record and the same PK delete succeeds:
DELETE FROM temporal_fk_per2per WHERE id = '[3,3]';
DELETE FROM temporal_per WHERE id = '[5,5]' AND valid_from = '2018-01-01' AND valid_til = '2018-02-01';

--
-- per2per test ON UPDATE/DELETE options
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

--
-- test FK parent updates CASCADE
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
UPDATE temporal_per SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes CASCADE
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_per WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
DELETE FROM temporal_per WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent updates SET NULL
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
ALTER TABLE temporal_fk_per2per
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
UPDATE temporal_per SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET NULL
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_per WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
DELETE FROM temporal_per WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent updates SET DEFAULT
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[-1,-1]', null, null);
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
ALTER TABLE temporal_fk_per2per
  ALTER COLUMN parent_id SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk_per2per_fk,
	ADD CONSTRAINT temporal_fk_per2per_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_per
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per SET id = '[7,7]' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
UPDATE temporal_per SET id = '[9,9]' WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET DEFAULT
--

TRUNCATE temporal_per, temporal_fk_per2per;
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[-1,-1]', null, null);
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_per WHERE id = '[6,6]';
SELECT * FROM temporal_fk_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per (id, valid_from, valid_til) VALUES ('[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk_per2per (id, valid_from, valid_til, parent_id) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]');
DELETE FROM temporal_per WHERE id = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent updates CASCADE (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_per2per
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE CASCADE ON UPDATE CASCADE;
-- leftovers on both sides:
UPDATE temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per2 SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
UPDATE temporal_per2 SET id1 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes CASCADE (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_per2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
DELETE FROM temporal_per2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent updates SET NULL (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_per2per
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET NULL ON UPDATE SET NULL;
-- leftovers on both sides:
UPDATE temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per2 SET id1 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
UPDATE temporal_per2 SET id1 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET NULL (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_per2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
DELETE FROM temporal_per2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET NULL (two scalar cols, SET NULL subset)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_per2per
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET NULL (valid_at) ON UPDATE SET NULL;
-- ok:
ALTER TABLE temporal_fk2_per2per
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET NULL (parent_id1) ON UPDATE SET NULL;
-- leftovers on both sides:
DELETE FROM temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO delete:
DELETE FROM temporal_per2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
DELETE FROM temporal_per2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent updates SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[-1,-1]', '[-1,-1]', null, null);
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
ALTER TABLE temporal_fk2_per2per
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
  ALTER COLUMN parent_id2 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
-- leftovers on both sides:
UPDATE temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id1 = '[7,7]', id2 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
UPDATE temporal_per2 SET id1 = '[7,7]', id2 = '[7,7]' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
UPDATE temporal_per2 SET id1 = '[9,9]', id2 = '[9,9]' WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET DEFAULT (two scalar cols)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[-1,-1]', '[-1,-1]', null, null);
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
-- leftovers on both sides:
DELETE FROM temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_per2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
DELETE FROM temporal_per2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

--
-- test FK parent deletes SET DEFAULT (two scalar cols, SET DEFAULT subset)
--

TRUNCATE temporal_per2, temporal_fk2_per2per;
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[-1,-1]', '[6,6]', null, null);
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[6,6]', '[6,6]', '2018-01-01', '2021-01-01');
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[100,100]', '2018-01-01', '2021-01-01', '[6,6]', '[6,6]');
-- fails because you can't set the PERIOD column:
ALTER TABLE temporal_fk2_per2per
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET DEFAULT (valid_at) ON UPDATE SET DEFAULT;
-- ok:
ALTER TABLE temporal_fk2_per2per
  ALTER COLUMN parent_id1 SET DEFAULT '[-1,-1]',
	DROP CONSTRAINT temporal_fk2_per2per_fk,
	ADD CONSTRAINT temporal_fk2_per2per_fk
		FOREIGN KEY (parent_id1, parent_id2, PERIOD valid_at)
		REFERENCES temporal_per2
		ON DELETE SET DEFAULT (parent_id1) ON UPDATE SET DEFAULT;
-- leftovers on both sides:
DELETE FROM temporal_per2 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- non-FPO update:
DELETE FROM temporal_per2 WHERE id1 = '[6,6]';
SELECT * FROM temporal_fk2_per2per WHERE id = '[100,100]' ORDER BY id, valid_at;
-- FK across two referenced rows:
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2018-01-01', '2020-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[8,8]', '[8,8]', '2020-01-01', '2021-01-01');
INSERT INTO temporal_per2 (id1, id2, valid_from, valid_til) VALUES ('[-1,-1]', '[8,8]', null, null);
INSERT INTO temporal_fk2_per2per (id, valid_from, valid_til, parent_id1, parent_id2) VALUES ('[200,200]', '2018-01-01', '2021-01-01', '[8,8]', '[8,8]');
DELETE FROM temporal_per2 WHERE id1 = '[8,8]' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_fk2_per2per WHERE id = '[200,200]' ORDER BY id, valid_at;

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
INSERT INTO temporal_rng3 (id, valid_at) VALUES ('[8,9)', mydaterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_fk3_rng2rng (id, valid_at, parent_id) VALUES ('[5,6)', mydaterange('2018-01-01', '2021-01-01'), '[8,9)');
DELETE FROM temporal_rng3 FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,9)';
SELECT * FROM temporal_fk3_rng2rng WHERE id = '[5,6)';

DROP TABLE temporal_fk3_rng2rng;
DROP TABLE temporal_rng3;
DROP TYPE mydaterange;

--
-- test FOREIGN KEY, box references box
-- (not allowed: PERIOD part must be a range or multirange)
--

CREATE TABLE temporal_box (
  id int4range,
  valid_at box,
  CONSTRAINT temporal_box_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_box

CREATE TABLE temporal_fk_box2box (
  id int4range,
  valid_at box,
  parent_id int4range,
  CONSTRAINT temporal_fk_box2box_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT temporal_fk_box2box_fk FOREIGN KEY (parent_id, PERIOD valid_at)
    REFERENCES temporal_box (id, PERIOD valid_at)
);

--
-- FK between partitioned tables
--

CREATE TABLE temporal_partitioned_rng (
	id int4range,
	valid_at daterange,
  name text,
	CONSTRAINT temporal_paritioned_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
) PARTITION BY LIST (id);
CREATE TABLE tp1 PARTITION OF temporal_partitioned_rng FOR VALUES IN ('[1,2)', '[3,4)', '[5,6)', '[7,8)', '[9,10)', '[11,12)', '[13,14)', '[15,16)', '[17,18)', '[19,20)', '[21,22)', '[23,24)');
CREATE TABLE tp2 PARTITION OF temporal_partitioned_rng FOR VALUES IN ('[0,1)', '[2,3)', '[4,5)', '[6,7)', '[8,9)', '[10,11)', '[12,13)', '[14,15)', '[16,17)', '[18,19)', '[20,21)', '[22,23)', '[24,25)');
INSERT INTO temporal_partitioned_rng (id, valid_at, name) VALUES
  ('[1,2)', daterange('2000-01-01', '2000-02-01'), 'one'),
  ('[1,2)', daterange('2000-02-01', '2000-03-01'), 'one'),
  ('[2,3)', daterange('2000-01-01', '2010-01-01'), 'two');

CREATE TABLE temporal_partitioned_fk_rng2rng (
	id int4range,
	valid_at daterange,
	parent_id int4range,
	CONSTRAINT temporal_partitioned_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
	CONSTRAINT temporal_partitioned_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng (id, PERIOD valid_at)
) PARTITION BY LIST (id);
CREATE TABLE tfkp1 PARTITION OF temporal_partitioned_fk_rng2rng FOR VALUES IN ('[1,2)', '[3,4)', '[5,6)', '[7,8)', '[9,10)', '[11,12)', '[13,14)', '[15,16)', '[17,18)', '[19,20)', '[21,22)', '[23,24)');
CREATE TABLE tfkp2 PARTITION OF temporal_partitioned_fk_rng2rng FOR VALUES IN ('[0,1)', '[2,3)', '[4,5)', '[6,7)', '[8,9)', '[10,11)', '[12,13)', '[14,15)', '[16,17)', '[18,19)', '[20,21)', '[22,23)', '[24,25)');

--
-- partitioned FK referencing inserts
--

INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES
  ('[1,2)', daterange('2000-01-01', '2000-02-15'), '[1,2)'),
  ('[1,2)', daterange('2001-01-01', '2002-01-01'), '[2,3)'),
  ('[2,3)', daterange('2000-01-01', '2000-02-15'), '[1,2)');
-- should fail:
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES
  ('[3,4)', daterange('2010-01-01', '2010-02-15'), '[1,2)');
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES
  ('[3,4)', daterange('2000-01-01', '2000-02-15'), '[3,4)');

--
-- partitioned FK referencing updates
--

UPDATE temporal_partitioned_fk_rng2rng SET valid_at = daterange('2000-01-01', '2000-02-13') WHERE id = '[2,3)';
-- move a row from the first partition to the second
UPDATE temporal_partitioned_fk_rng2rng SET id = '[4,5)' WHERE id = '[1,2)';
-- move a row from the second partition to the first
UPDATE temporal_partitioned_fk_rng2rng SET id = '[1,2)' WHERE id = '[4,5)';
-- should fail:
UPDATE temporal_partitioned_fk_rng2rng SET valid_at = daterange('2000-01-01', '2000-04-01') WHERE id = '[1,2)';

--
-- partitioned FK referenced updates NO ACTION
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2016-01-01', '2016-02-01'));
UPDATE temporal_partitioned_rng SET valid_at = daterange('2018-01-01', '2018-02-01') WHERE id = '[5,6)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- partitioned FK referenced deletes NO ACTION
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
DELETE FROM temporal_partitioned_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
DELETE FROM temporal_partitioned_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- partitioned FK referenced updates RESTRICT
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk;
ALTER TABLE temporal_partitioned_fk_rng2rng
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
	FOREIGN KEY (parent_id, PERIOD valid_at)
	REFERENCES temporal_partitioned_rng
	ON DELETE RESTRICT;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2016-01-01', '2016-02-01'));
UPDATE temporal_partitioned_rng SET valid_at = daterange('2018-01-01', '2018-02-01') WHERE id = '[5,6)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-02-01', '2016-03-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
UPDATE temporal_partitioned_rng SET valid_at = daterange('2016-01-01', '2016-02-01')
  WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- partitioned FK referenced deletes RESTRICT
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-01-01', '2018-02-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[5,6)', daterange('2018-02-01', '2018-03-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[3,4)', daterange('2018-01-05', '2018-01-10'), '[5,6)');
DELETE FROM temporal_partitioned_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-02-01', '2018-03-01');
-- should fail:
DELETE FROM temporal_partitioned_rng WHERE id = '[5,6)' AND valid_at = daterange('2018-01-01', '2018-02-01');

--
-- partitioned FK referenced updates CASCADE
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[4,5)', daterange('2018-01-01', '2021-01-01'), '[6,7)');
ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE CASCADE ON UPDATE CASCADE;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[4,5)';
UPDATE temporal_partitioned_rng SET id = '[7,8)' WHERE id = '[6,7)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[4,5)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[15,16)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[15,16)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[10,11)', daterange('2018-01-01', '2021-01-01'), '[15,16)');
UPDATE temporal_partitioned_rng SET id = '[16,17)' WHERE id = '[15,16)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[10,11)';

--
-- partitioned FK referenced deletes CASCADE
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[8,9)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[5,6)', daterange('2018-01-01', '2021-01-01'), '[8,9)');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[8,9)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[5,6)';
DELETE FROM temporal_partitioned_rng WHERE id = '[8,9)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[5,6)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[17,18)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[17,18)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[11,12)', daterange('2018-01-01', '2021-01-01'), '[17,18)');
DELETE FROM temporal_partitioned_rng WHERE id = '[17,18)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[11,12)';

--
-- partitioned FK referenced updates SET NULL
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[9,10)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[6,7)', daterange('2018-01-01', '2021-01-01'), '[9,10)');
ALTER TABLE temporal_partitioned_fk_rng2rng
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE SET NULL ON UPDATE SET NULL;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[10,11)' WHERE id = '[9,10)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[6,7)';
UPDATE temporal_partitioned_rng SET id = '[10,11)' WHERE id = '[9,10)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[6,7)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[18,19)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[18,19)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[12,13)', daterange('2018-01-01', '2021-01-01'), '[18,19)');
UPDATE temporal_partitioned_rng SET id = '[19,20)' WHERE id = '[18,19)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[12,13)';

--
-- partitioned FK referenced deletes SET NULL
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[11,12)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[7,8)', daterange('2018-01-01', '2021-01-01'), '[11,12)');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[11,12)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[7,8)';
DELETE FROM temporal_partitioned_rng WHERE id = '[11,12)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[7,8)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[20,21)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[20,21)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[13,14)', daterange('2018-01-01', '2021-01-01'), '[20,21)');
DELETE FROM temporal_partitioned_rng WHERE id = '[20,21)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[13,14)';

--
-- partitioned FK referenced updates SET DEFAULT
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[0,1)', daterange(null, null));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[12,13)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[8,9)', daterange('2018-01-01', '2021-01-01'), '[12,13)');
ALTER TABLE temporal_partitioned_fk_rng2rng
  ALTER COLUMN parent_id SET DEFAULT '[0,1)',
	DROP CONSTRAINT temporal_partitioned_fk_rng2rng_fk,
	ADD CONSTRAINT temporal_partitioned_fk_rng2rng_fk
		FOREIGN KEY (parent_id, PERIOD valid_at)
		REFERENCES temporal_partitioned_rng
		ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
UPDATE temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' SET id = '[13,14)' WHERE id = '[12,13)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[8,9)';
UPDATE temporal_partitioned_rng SET id = '[13,14)' WHERE id = '[12,13)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[8,9)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[22,23)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[22,23)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[14,15)', daterange('2018-01-01', '2021-01-01'), '[22,23)');
UPDATE temporal_partitioned_rng SET id = '[23,24)' WHERE id = '[22,23)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[14,15)';

--
-- partitioned FK referenced deletes SET DEFAULT
--

TRUNCATE temporal_partitioned_rng, temporal_partitioned_fk_rng2rng;
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[0,1)', daterange(null, null));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[14,15)', daterange('2018-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[9,10)', daterange('2018-01-01', '2021-01-01'), '[14,15)');
DELETE FROM temporal_partitioned_rng FOR PORTION OF valid_at FROM '2019-01-01' TO '2020-01-01' WHERE id = '[14,15)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[9,10)';
DELETE FROM temporal_partitioned_rng WHERE id = '[14,15)';
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[9,10)';
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[24,25)', daterange('2018-01-01', '2020-01-01'));
INSERT INTO temporal_partitioned_rng (id, valid_at) VALUES ('[24,25)', daterange('2020-01-01', '2021-01-01'));
INSERT INTO temporal_partitioned_fk_rng2rng (id, valid_at, parent_id) VALUES ('[15,16)', daterange('2018-01-01', '2021-01-01'), '[24,25)');
DELETE FROM temporal_partitioned_rng WHERE id = '[24,25)' AND valid_at @> '2019-01-01'::date;
SELECT * FROM temporal_partitioned_fk_rng2rng WHERE id = '[15,16)';

DROP TABLE temporal_partitioned_fk_rng2rng;
DROP TABLE temporal_partitioned_rng;

RESET datestyle;
