--
-- key_join_filter
--
-- Ad-hoc FOR KEY joins with a join-local FILTER (WHERE ...) clause.
-- Self-contained: base fixtures at top, per-case fixtures created and
-- dropped within each case, base fixtures dropped at bottom.
--

-- Base tables shared by the FILTER cases (from the FK-join setup).
CREATE TABLE t1
(
    c1 int not null,
    c2 int not null,
    CONSTRAINT t1_pkey PRIMARY KEY (c1)
);

CREATE TABLE t2
(
    c3 int not null,
    c4 int not null,
    CONSTRAINT t2_pkey PRIMARY KEY (c3),
    CONSTRAINT t2_c3_fkey FOREIGN KEY (c3) REFERENCES t1 (c1)
);

INSERT INTO t1 (c1, c2) VALUES (1, 10);
INSERT INTO t1 (c1, c2) VALUES (2, 20);
INSERT INTO t1 (c1, c2) VALUES (3, 30);
INSERT INTO t2 (c3, c4) VALUES (1, 10);
INSERT INTO t2 (c3, c4) VALUES (3, 30);

-- ============================================================
-- FILTER clause tests
-- ============================================================

-- FILTER1: key join with FILTER clause — restricts result to t2.c4 = 10
SELECT * FROM t1
-- accepted
  JOIN t2 FOR KEY (c3) -> t1 (c1)
  FILTER (WHERE t2.c4 = 10);

-- FILTER2: FK left join with FILTER — outer join rows still present
SELECT * FROM t1
-- accepted
  LEFT JOIN t2 FOR KEY (c3) -> t1 (c1)
  FILTER (WHERE t2.c4 = 10);

-- FILTER3: USING join with FILTER clause is not supported
CREATE TABLE tf1 (id int PRIMARY KEY, val int);
CREATE TABLE tf2 (id int PRIMARY KEY, val int);
INSERT INTO tf1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO tf2 VALUES (1, 100), (2, 200);

-- rejected, reason: join-local FILTER is supported only for FOR KEY joins
SELECT * FROM tf1 JOIN tf2 USING (id) FILTER (WHERE tf1.val > 10);

-- rejected, reason: same restriction after a USING join alias
SELECT * FROM tf1 JOIN tf2 USING (id) AS tfj FILTER (WHERE tf2.val > 100);

-- Ordinary USING joins do not expose key-join facts.
CREATE TABLE tf_child (id int NOT NULL REFERENCES tf1 (id));
INSERT INTO tf_child VALUES (1), (2), (3);

SELECT *
FROM (SELECT id FROM tf2 JOIN tf1 USING (id)) q
-- rejected, reason: the ordinary USING join exposes no surface facts
JOIN tf_child c FOR KEY (id) -> q (id);

DROP TABLE tf_child;

-- Relabeled projected FK columns are not direct key columns.
CREATE TABLE relabel_cast_parent (id text PRIMARY KEY);
CREATE TABLE relabel_cast_child
(
    id text NOT NULL REFERENCES relabel_cast_parent (id)
);
INSERT INTO relabel_cast_parent VALUES ('a');
INSERT INTO relabel_cast_child VALUES ('a');

SELECT *
FROM relabel_cast_parent p
JOIN (
    SELECT id::varchar AS id
    FROM relabel_cast_child
) q FOR KEY (id) -> p (id);

DROP TABLE relabel_cast_child, relabel_cast_parent;

-- Relabeled projected FK columns must preserve exact typmod identity.
CREATE TABLE relabel_typmod_parent (id numeric(10,0) PRIMARY KEY);
CREATE TABLE relabel_typmod_child
(
    id numeric(10,0) NOT NULL REFERENCES relabel_typmod_parent (id)
);
INSERT INTO relabel_typmod_parent VALUES (1);
INSERT INTO relabel_typmod_child VALUES (1);

SELECT *
FROM relabel_typmod_parent p
JOIN (
    SELECT id::numeric AS id
    FROM relabel_typmod_child
) q FOR KEY (id) -> p (id);

DROP TABLE relabel_typmod_child, relabel_typmod_parent;

-- Dropped columns in join inputs are ignored while projecting key facts.
CREATE TABLE dropped_attr_parent (id int PRIMARY KEY, gone int);
CREATE TABLE dropped_attr_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES dropped_attr_parent (id),
    gone int
);
ALTER TABLE dropped_attr_parent DROP COLUMN gone;
ALTER TABLE dropped_attr_child DROP COLUMN gone;
INSERT INTO dropped_attr_parent VALUES (1);
INSERT INTO dropped_attr_child VALUES (10, 1);

SELECT count(*) AS dropped_attr_rows
FROM dropped_attr_parent p
JOIN dropped_attr_child c FOR KEY (parent_id) -> p (id);

DROP TABLE dropped_attr_child, dropped_attr_parent;

-- Scattered ad-hoc FILTER case: recursive-CTE validation must inspect a
-- FOR KEY FILTER before parse analysis merges it into the join quals.
WITH RECURSIVE rec_filter(n) AS (
    SELECT 1
    UNION ALL
    SELECT rec_filter.n + 1
    FROM rec_filter
    CROSS JOIN (
        t1 JOIN t2 FOR KEY (c3) -> t1 (c1)
            FILTER (WHERE EXISTS (SELECT 1 FROM rec_filter r2))
    ) kj
    WHERE rec_filter.n < 3
)
SELECT * FROM rec_filter;

SELECT *
FROM t1 outerp
JOIN LATERAL (
    SELECT *
    FROM t1 p
    JOIN t2 c2
        FOR KEY (c3) -> p (c1) FILTER (WHERE outerp.c1 = 1)
    JOIN t2 c3
        FOR KEY (c3) -> p (c1)
) q ON true; -- rejected, reason: lateral filter outer Vars are not direct key facts

-- Teardown of the base fixtures.
DROP TABLE tf2, tf1;
DROP TABLE t2, t1;
