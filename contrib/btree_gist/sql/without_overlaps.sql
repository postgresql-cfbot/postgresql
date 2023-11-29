-- Core must test WITHOUT OVERLAPS
-- with an int4range + tsrange,
-- so here we do some simple tests
-- to make sure int + tsrange works too,
-- since that is the expected use-case.
CREATE TABLE temporal_rng (
  id integer,
  valid_at tsrange,
  CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
\d temporal_rng
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_rng_pk';
SELECT pg_get_indexdef(conindid, 0, true) FROM pg_constraint WHERE conname = 'temporal_rng_pk';

-- Foreign key
CREATE TABLE temporal_fk_rng2rng (
  id integer,
  valid_at tsrange,
  parent_id integer,
  CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
  CONSTRAINT temporal_fk_rng2rng_fk FOREIGN KEY (parent_id, PERIOD valid_at)
    REFERENCES temporal_rng (id, PERIOD valid_at)
);
\d temporal_fk_rng2rng
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'temporal_fk_rng2rng_fk';
