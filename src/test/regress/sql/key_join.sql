--
-- Test Foreign Key Joins.
--

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

-- Test so we didn't break the parser
SELECT 1<-2; -- accepted, result: false

SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1); -- accepted
SELECT * FROM t1 JOIN t2 FOR KEY (c3) ->/*comment*/ t1 (c1); -- accepted
SELECT * FROM t1 JOIN t2 FOR KEY (c3) /*comment*/-> t1 (c1); -- accepted
SELECT * FROM t1 JOIN t2 FOR KEY (c3) /*comment*/->/*comment*/ t1 (c1); -- accepted
SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t2 (c1); -- rejected, reason: arrow alias must name the left operand
SELECT * FROM t1 JOIN t2 FOR KEY (c3) - > t1 (c2); -- rejected, reason: -> must be a single token
SELECT * FROM t1 JOIN t2 FOR KEY (c3) -/*comment*/> t1 (c2); -- rejected, reason: comments cannot split the -> token
SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c2); -- rejected, reason: referenced column is not the FK target key
SELECT * FROM t1 JOIN t2 FOR KEY (c4) -> t1 (c1); -- rejected, reason: referencing column is not backed by the FK
SELECT * FROM t1 JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2); -- rejected, reason: selected key does not match the FK arity
SELECT * FROM t1 JOIN t2 FOR KEY (c3) <- t1 (c1); -- rejected, reason: wrong direction for this table order
SELECT * FROM t1 JOIN t2 FOR KEY (c1) <- t1 (c3); -- rejected, reason: wrong direction and wrong referencing column
SELECT * FROM t1 JOIN t2 FOR KEY (c3) <- t1 (c2); -- rejected, reason: wrong direction and wrong referenced column
SELECT * FROM t1 JOIN t2 FOR KEY (c4) <- t1 (c1); -- rejected, reason: wrong direction and wrong referencing column
SELECT * FROM t1 JOIN t2 FOR KEY (c3,c4) <- t1 (c1,c2); -- rejected, reason: wrong direction for the composite key
SELECT * FROM t1 AS a JOIN t2 AS b FOR KEY (c3) -> a (c2); -- rejected, reason: alias resolves to a non-referenced key column

SELECT * FROM t2 JOIN t1 FOR KEY (c1) <- t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) <-/*comment*/ t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) /*comment*/<- t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) /*comment*/<-/*comment*/ t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) <- t1 (c3); -- rejected, reason: arrow alias must name the left operand
SELECT * FROM t2 JOIN t1 FOR KEY (c1) < - t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) </*comment*/- t2 (c3); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1) <- t2 (c4); -- rejected, reason: referencing column is not backed by the FK
SELECT * FROM t2 JOIN t1 FOR KEY (c2) <- t2 (c3); -- rejected, reason: referenced column is not the FK target key
SELECT * FROM t2 JOIN t1 FOR KEY (c1,c2) <- t2 (c3,c4); -- rejected, reason: selected key does not match the FK arity
SELECT * FROM t2 JOIN t1 FOR KEY (c1) -> t2 (c3); -- rejected, reason: wrong direction for this table order
SELECT * FROM t2 JOIN t1 FOR KEY (c1) -> t2 (c4); -- rejected, reason: wrong direction and wrong referencing column
SELECT * FROM t2 JOIN t1 FOR KEY (c2) -> t2 (c3); -- rejected, reason: wrong direction and wrong referenced column
SELECT * FROM t2 JOIN t1 FOR KEY (c1,c2) -> t2 (c3,c4); -- rejected, reason: wrong direction for the composite key
SELECT * FROM t2 AS a JOIN t1 AS b FOR KEY (c1) <- a (c4); -- rejected, reason: alias resolves to a non-referencing FK column

-- Self-join elimination must update stored key-join RT indexes when the
-- duplicate relation is the referencing side.
SET enable_self_join_elimination = on;
SELECT c1.c3 AS child_key, p.c1 AS parent_key
FROM t1 p
JOIN t2 c1 FOR KEY (c3) -> p (c1)
JOIN t2 c2 ON c2.c3 = c1.c3
ORDER BY c1.c3;
RESET enable_self_join_elimination;

SELECT *
FROM t1 p
JOIN LATERAL (SELECT p.c1 AS c1) q
  FOR KEY (c1) -> p (c1); -- rejected, reason: lateral outer Vars are not direct key facts

SELECT *
FROM pg_catalog.pg_class pc
-- rejected, reason: catalog NOT NULL columns do not provide proof facts
JOIN t2 FOR KEY (c3) -> pc (relname);

CREATE TABLE no_key_join_fact_parent (id int);
CREATE TABLE no_key_join_fact_child (parent_id int);
SELECT *
FROM ONLY no_key_join_fact_parent p
JOIN ONLY no_key_join_fact_child c
  FOR KEY (parent_id) -> p (id); -- rejected, reason: ONLY scans have no proof facts here
DROP TABLE no_key_join_fact_child, no_key_join_fact_parent;

CREATE TABLE invalid_unique_parent (id int);
CREATE TABLE invalid_unique_child (parent_id int);
INSERT INTO invalid_unique_parent VALUES (1), (1);
INSERT INTO invalid_unique_child VALUES (1);
CREATE UNIQUE INDEX CONCURRENTLY invalid_unique_parent_id_idx
    ON invalid_unique_parent (id);
SELECT *
FROM invalid_unique_parent p
-- rejected, reason: invalid unique indexes do not provide proof facts
JOIN invalid_unique_child c FOR KEY (parent_id) -> p (id);
DROP TABLE invalid_unique_child, invalid_unique_parent;

CREATE TABLE expr_unique_parent (id int);
CREATE TABLE expr_unique_child (parent_id int);
CREATE UNIQUE INDEX expr_unique_parent_idx ON expr_unique_parent ((id + 0));
INSERT INTO expr_unique_parent VALUES (1);
INSERT INTO expr_unique_child VALUES (1);
SELECT *
FROM expr_unique_parent p
-- rejected, reason: expression indexes do not provide proof facts
JOIN expr_unique_child c FOR KEY (parent_id) -> p (id);
DROP TABLE expr_unique_child, expr_unique_parent;

CREATE TABLE partial_unique_parent (id int);
CREATE TABLE partial_unique_child (parent_id int);
CREATE UNIQUE INDEX partial_unique_parent_idx ON partial_unique_parent (id)
    WHERE id > 0;
INSERT INTO partial_unique_parent VALUES (1);
INSERT INTO partial_unique_child VALUES (1);
SELECT *
FROM partial_unique_parent p
-- rejected, reason: partial indexes do not provide proof facts
JOIN partial_unique_child c FOR KEY (parent_id) -> p (id);
DROP TABLE partial_unique_child, partial_unique_parent;

CREATE TABLE collate_unique_parent (id text COLLATE "C");
CREATE TABLE collate_unique_child (parent_id text COLLATE "C");
CREATE UNIQUE INDEX collate_unique_parent_idx
    ON collate_unique_parent (id COLLATE "POSIX");
INSERT INTO collate_unique_parent VALUES ('1');
INSERT INTO collate_unique_child VALUES ('1');
SELECT *
FROM collate_unique_parent p
-- rejected, reason: mismatched index collations do not provide proof facts
JOIN collate_unique_child c FOR KEY (parent_id) -> p (id);
DROP TABLE collate_unique_child, collate_unique_parent;

CREATE OPERATOR CLASS noeq_unique_int4_ops FOR TYPE int USING btree AS
    FUNCTION 1 btint4cmp(int, int);
CREATE TABLE noeq_unique_parent (id int PRIMARY KEY);
CREATE TABLE noeq_unique_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES noeq_unique_parent (id)
);
CREATE UNIQUE INDEX noeq_unique_parent_bad_idx
    ON noeq_unique_parent USING btree (id noeq_unique_int4_ops);
INSERT INTO noeq_unique_parent VALUES (1);
INSERT INTO noeq_unique_child VALUES (10, 1);
PREPARE noeq_unique_plan AS
SELECT *
FROM noeq_unique_parent p
-- accepted, reason: unique index without equality operator is skipped
JOIN noeq_unique_child c FOR KEY (parent_id) -> p (id);
DEALLOCATE noeq_unique_plan;
DROP TABLE noeq_unique_child, noeq_unique_parent;
DROP OPERATOR CLASS noeq_unique_int4_ops USING btree;
DROP OPERATOR FAMILY noeq_unique_int4_ops USING btree;

CREATE DOMAIN domain_key_join_int AS int;
CREATE TABLE domain_key_join_parent (id domain_key_join_int PRIMARY KEY);
CREATE TABLE domain_key_join_child
(
    id int PRIMARY KEY,
    parent_id domain_key_join_int NOT NULL REFERENCES domain_key_join_parent (id)
);
INSERT INTO domain_key_join_parent VALUES (1);
INSERT INTO domain_key_join_child VALUES (1, 1);
SELECT *
FROM domain_key_join_parent p
-- accepted, reason: FK and PK expose the same domain type
JOIN domain_key_join_child c FOR KEY (parent_id) -> p (id);
DROP TABLE domain_key_join_child, domain_key_join_parent;

CREATE TABLE domain_key_join_base_parent (id int PRIMARY KEY);
CREATE TABLE domain_key_join_domain_child
(
    id int PRIMARY KEY,
    parent_id domain_key_join_int NOT NULL
        REFERENCES domain_key_join_base_parent (id)
);
INSERT INTO domain_key_join_base_parent VALUES (1);
INSERT INTO domain_key_join_domain_child VALUES (1, 1);
SELECT *
FROM domain_key_join_base_parent p
-- rejected, reason: domain FK and base PK do not expose the same type
JOIN domain_key_join_domain_child c FOR KEY (parent_id) -> p (id);
DROP TABLE domain_key_join_domain_child, domain_key_join_base_parent;

CREATE TABLE domain_key_join_domain_parent (id domain_key_join_int PRIMARY KEY);
CREATE TABLE domain_key_join_base_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL
        REFERENCES domain_key_join_domain_parent (id)
);
INSERT INTO domain_key_join_domain_parent VALUES (1);
INSERT INTO domain_key_join_base_child VALUES (1, 1);
SELECT *
FROM domain_key_join_domain_parent p
-- rejected, reason: base FK and domain PK do not expose the same type
JOIN domain_key_join_base_child c FOR KEY (parent_id) -> p (id);
DROP TABLE domain_key_join_base_child, domain_key_join_domain_parent;

DROP DOMAIN domain_key_join_int;

CREATE TABLE key_join_self_syntax
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_self_syntax (id)
);

SELECT * FROM key_join_self_syntax AS t1 JOIN key_join_self_syntax AS t2 FOR KEY (parent_id) -> t1 (id); -- accepted
SELECT * FROM key_join_self_syntax AS t1 JOIN key_join_self_syntax AS t2 FOR KEY (parent_id) -> t2 (id); -- rejected, reason: arrow alias must name the left operand
SELECT * FROM key_join_self_syntax AS t1 JOIN key_join_self_syntax AS t2 FOR KEY (id) <- t1 (parent_id); -- accepted
SELECT * FROM key_join_self_syntax AS t1 JOIN key_join_self_syntax AS t2 FOR KEY (id) <- t2 (parent_id); -- rejected, reason: arrow alias must name the left operand

DROP TABLE key_join_self_syntax;

ALTER TABLE t1 ADD UNIQUE (c1,c2);
ALTER TABLE t2 ADD CONSTRAINT t2_c3_c4_fkey FOREIGN KEY (c3,c4) REFERENCES t1 (c1,c2);

--
-- Referenced-partition FK clone rows do not prove containment in one partition
--

CREATE TABLE kj_part_parent (id int PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE kj_part_parent_1 PARTITION OF kj_part_parent
    FOR VALUES FROM (1) TO (10);
CREATE TABLE kj_part_parent_2 PARTITION OF kj_part_parent
    FOR VALUES FROM (10) TO (20);
CREATE TABLE kj_part_child
(
    id  int PRIMARY KEY,
    pid int NOT NULL REFERENCES kj_part_parent (id)
);
INSERT INTO kj_part_parent VALUES (1), (11);
INSERT INTO kj_part_child VALUES (101, 1), (111, 11);

SELECT c.id AS child_id, c.pid, p.id AS parent_id
FROM kj_part_parent p
-- accepted, reason: parent FK proves containment in the whole partitioned table
JOIN kj_part_child c FOR KEY (pid) -> p (id)
ORDER BY c.id;

SELECT c.id AS child_id, c.pid, p1.id AS parent_id
FROM kj_part_parent_1 p1
-- rejected, reason: a cloned FK to one referenced partition is not a global containment proof
JOIN kj_part_child c FOR KEY (pid) -> p1 (id)
ORDER BY c.id;

CREATE TABLE kj_part_ref_parent (id int PRIMARY KEY);
CREATE TABLE kj_part_ref_child
(
    id  int,
    pid int NOT NULL REFERENCES kj_part_ref_parent (id)
) PARTITION BY RANGE (id);
CREATE TABLE kj_part_ref_child_1 PARTITION OF kj_part_ref_child
    FOR VALUES FROM (1) TO (10);
CREATE TABLE kj_part_ref_child_sub PARTITION OF kj_part_ref_child
    FOR VALUES FROM (10) TO (20) PARTITION BY RANGE (id);
CREATE TABLE kj_part_ref_child_sub_1 PARTITION OF kj_part_ref_child_sub
    FOR VALUES FROM (10) TO (15);
INSERT INTO kj_part_ref_parent VALUES (1), (11);
INSERT INTO kj_part_ref_child_1 VALUES (1, 1);
INSERT INTO kj_part_ref_child_sub_1 VALUES (11, 11);

SELECT c.id AS child_id, c.pid, p.id AS parent_id
FROM kj_part_ref_parent p
-- accepted, reason: referencing-side partition FK still targets the same referenced keyspace
JOIN kj_part_ref_child_1 c FOR KEY (pid) -> p (id);

SELECT c.id AS child_id, c.pid, p.id AS parent_id
FROM kj_part_ref_parent p
-- accepted, reason: multi-level referencing partition FK still targets the same referenced keyspace
JOIN kj_part_ref_child_sub_1 c FOR KEY (pid) -> p (id);

DROP TABLE kj_part_ref_child, kj_part_ref_parent;
DROP TABLE kj_part_child, kj_part_parent;

--
-- Test nulls and multiple tables
--

CREATE TABLE t3
(
    c5 int,
    c6 int,
    CONSTRAINT t3_c5_c6_fkey FOREIGN KEY (c5, c6) REFERENCES t1 (c1, c2)
);
INSERT INTO t3 (c5, c6) VALUES (1, 10); -- accepted
INSERT INTO t3 (c5, c6) VALUES (3, 30); -- accepted
INSERT INTO t3 (c5, c6) VALUES (3, NULL); -- accepted
INSERT INTO t3 (c5, c6) VALUES (NULL, 30); -- accepted
INSERT INTO t3 (c5, c6) VALUES (1234, NULL); -- accepted
INSERT INTO t3 (c5, c6) VALUES (NULL, 5678); -- accepted
INSERT INTO t3 (c5, c6) VALUES (NULL, NULL); -- accepted

--
-- Test composite key joins with columns in matching order
--
SELECT *
FROM t1
-- rejected, reason: nullable referencing columns are not preserved by an inner key join
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
JOIN t3 FOR KEY (c5,c6) -> t1 (c1,c2);

SELECT *
FROM t1
-- rejected, reason: nullable referencing columns are on the non-preserved side
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
LEFT JOIN t3 FOR KEY (c5,c6) -> t1 (c1,c2);

SELECT *
FROM t1
-- rejected, reason: referenced t1 surface is unique but not row-covered after the preceding inner key join
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
RIGHT JOIN t3 FOR KEY (c5,c6) -> t1 (c1,c2);

--
-- Test composite key joins with swapped column orders
--
SELECT *
FROM t1
-- rejected, reason: nullable referencing columns are not preserved by an inner key join
JOIN t2 FOR KEY (c4,c3) -> t1 (c2,c1)
JOIN t3 FOR KEY (c6,c5) -> t1 (c2,c1);

--
-- Test mismatched column orders between referencing and referenced sides
--
SELECT *
FROM t1
-- accepted
JOIN t2 FOR KEY (c4,c3) -> t1 (c2,c1)
JOIN t3 FOR KEY (c6,c5) -> t1 (c1,c2); -- rejected, reason: referencing and referenced column orders do not match the FK

--
-- Test defining foreign key constraints with MATCH FULL
--

CREATE TABLE t4
(
    c7 int,
    c8 int,
    CONSTRAINT t4_c7_c8_fkey FOREIGN KEY (c7, c8) REFERENCES t1 (c1, c2) MATCH FULL
);
INSERT INTO t4 (c7, c8) VALUES (1, 10); -- accepted
INSERT INTO t4 (c7, c8) VALUES (3, 30); -- accepted
INSERT INTO t4 (c7, c8) VALUES (3, NULL); -- rejected, reason: MATCH FULL disallows partially NULL composite FK values
INSERT INTO t4 (c7, c8) VALUES (NULL, 30); -- rejected, reason: MATCH FULL disallows partially NULL composite FK values
INSERT INTO t4 (c7, c8) VALUES (1234, NULL); -- rejected, reason: MATCH FULL disallows partially NULL composite FK values
INSERT INTO t4 (c7, c8) VALUES (NULL, 5678); -- rejected, reason: MATCH FULL disallows partially NULL composite FK values
INSERT INTO t4 (c7, c8) VALUES (NULL, NULL); -- accepted

SELECT *
FROM t1
-- rejected, reason: nullable referencing columns are not preserved by an inner key join
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
JOIN t4 FOR KEY (c7,c8) -> t1 (c1,c2);

SELECT *
FROM t1
-- rejected, reason: nullable referencing columns are on the non-preserved side
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
LEFT JOIN t4 FOR KEY (c7,c8) -> t1 (c1,c2);

SELECT *
FROM t1
-- rejected, reason: referenced t1 surface is unique but not row-covered after the preceding inner key join
JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2)
RIGHT JOIN t4 FOR KEY (c7,c8) -> t1 (c1,c2);

CREATE TABLE t5
(
    c9 int not null,
    c10 int not null,
    c11 int not null,
    c12 int not null,
    CONSTRAINT t5_pkey PRIMARY KEY (c9, c10),
    CONSTRAINT t5_c11_c12_fkey FOREIGN KEY (c11, c12) REFERENCES t1 (c1, c2)
);

INSERT INTO t5 (c9, c10, c11, c12) VALUES (1, 2, 1, 10);
INSERT INTO t5 (c9, c10, c11, c12) VALUES (3, 4, 3, 30);

CREATE TABLE t6
(
    c13 int not null,
    c14 int not null,
    CONSTRAINT t6_c13_c14_fkey FOREIGN KEY (c13, c14) REFERENCES t5 (c9, c10)
);

INSERT INTO t6 (c13, c14) VALUES (1, 2);
INSERT INTO t6 (c13, c14) VALUES (3, 4);
INSERT INTO t6 (c13, c14) VALUES (3, 4);

CREATE TABLE t7
(
    c15 int not null,
    c16 int not null,
    CONSTRAINT t7_c15_c16_fkey FOREIGN KEY (c15, c16) REFERENCES t5 (c9, c10)
);

INSERT INTO t7 (c15, c16) VALUES (1, 2);
INSERT INTO t7 (c15, c16) VALUES (1, 2);
INSERT INTO t7 (c15, c16) VALUES (3, 4);

CREATE TABLE t8
(
    c17 int not null,
    c18 int not null,
    c19 int,
    c20 int,
    CONSTRAINT t8_pkey PRIMARY KEY (c17, c18),
    CONSTRAINT t8_c19_c20_fkey FOREIGN KEY (c19, c20) REFERENCES t1 (c1, c2)
);

INSERT INTO t8 (c17, c18, c19, c20) VALUES (1, 2, 1, 10);
INSERT INTO t8 (c17, c18, c19, c20) VALUES (3, 4, 3, 30);

CREATE TABLE t9
(
    c21 int not null,
    c22 int not null,
    CONSTRAINT t9_c21_c22_fkey FOREIGN KEY (c21, c22) REFERENCES t8 (c17, c18)
);

INSERT INTO t9 (c21, c22) VALUES (1, 2);
INSERT INTO t9 (c21, c22) VALUES (3, 4);
INSERT INTO t9 (c21, c22) VALUES (3, 4);

CREATE TABLE t10
(
    c23 INT NOT NULL,
    c24 INT NOT NULL,
    c25 INT NOT NULL,
    c26 INT NOT NULL,
    CONSTRAINT t10_pkey PRIMARY KEY (c23, c24),
    CONSTRAINT t10_c23_c24_fkey FOREIGN KEY (c23, c24) REFERENCES t1 (c1, c2),
    CONSTRAINT t10_c25_c26_fkey FOREIGN KEY (c25, c26) REFERENCES t10 (c23, c24)
);

INSERT INTO t10 (c23, c24, c25, c26) VALUES (1, 10, 1, 10);

CREATE TABLE t11
(
    c27 INT NOT NULL,
    c28 INT NOT NULL,
    CONSTRAINT t11_pkey PRIMARY KEY (c27, c28),
    CONSTRAINT t11_c27_c28_fkey FOREIGN KEY (c27, c28) REFERENCES t10 (c23, c24)
);

INSERT INTO t11 (c27, c28) VALUES (1, 10);

--
-- Test subqueries
--

SELECT
    a.c1,
    a.c2,
    b.c3,
    b.c4
FROM t1 AS a
JOIN
(
    SELECT * FROM t2
-- accepted
) AS b FOR KEY (c3) -> a (c1);

SELECT
    a.c1,
    a.c2,
    b.c3,
    b.c4
FROM
(
    SELECT * FROM t1
) AS a
JOIN
(
    SELECT * FROM t2
-- accepted
) AS b FOR KEY (c3) -> a (c1);

SELECT
    a.t1_c1,
    a.t1_c2,
    b.t2_c3,
    b.t2_c4
FROM
(
    SELECT c1 AS t1_c1, c2 AS t1_c2 FROM t1
) AS a
JOIN
(
    SELECT c3 AS t2_c3, c4 AS t2_c4 FROM t2
-- accepted
) AS b FOR KEY (t2_c3) -> a (t1_c1);

SELECT
    a.outer_c1,
    a.outer_c2,
    b.outer_c3,
    b.outer_c4
FROM
(
    SELECT mid_c1 AS outer_c1, mid_c2 AS outer_c2 FROM
    (
        SELECT c1 AS mid_c1, c2 AS mid_c2 FROM t1
    ) sub1
) AS a
JOIN
(
    SELECT mid_c3 AS outer_c3, mid_c4 AS outer_c4 FROM
    (
        SELECT c3 AS mid_c3, c4 AS mid_c4 FROM t2
    ) sub2
-- accepted
) AS b FOR KEY (outer_c3) -> a (outer_c1);

SELECT *
FROM t1
JOIN
(
    SELECT
        t10.c23,
        t10.c24,
        t10_2.c25,
        t10_2.c26
    FROM t10
-- accepted
    JOIN t10 AS t10_2 FOR KEY (c23, c24) <- t10 (c25, c26)
) AS q1 FOR KEY (c23, c24) -> t1 (c1, c2);

SELECT *
FROM t1
JOIN LATERAL (
    SELECT c3, c4 FROM t2 WHERE c4 = c1 + 9
-- rejected, reason: LATERAL subqueries do not expose surface FK facts
) AS q1 FOR KEY (c3) -> t1 (c1);

SELECT *
FROM t1 AS outer_t,
     LATERAL (SELECT outer_t.c1) AS q1(c1)
-- rejected, reason: LATERAL subqueries do not expose referenced-side surface facts
     JOIN t2 FOR KEY (c3) -> q1 (c1);

SELECT *
FROM t1 AS outer_t,
     LATERAL (SELECT outer_t.c1, g FROM generate_series(1, 2) AS g) AS q1(c1, g)
-- rejected, reason: LATERAL referenced side can duplicate inherited keys
     JOIN t2 FOR KEY (c3) -> q1 (c1);

SELECT *
FROM t1 AS outer_t,
     LATERAL (SELECT outer_t.c1 WHERE false) AS q1(c1)
-- rejected, reason: LATERAL referenced side can suppress inherited keys
     JOIN t2 FOR KEY (c3) -> q1 (c1);

--
-- Test CTEs
--

WITH
q1 (q1_c1, q1_c2) AS
(
    SELECT c1, c2 FROM t1
),
q2 (q2_c1, q2_c2) AS
(
    SELECT q1_c1, q1_c2 FROM q1
),
q3 (q3_c3, q3_c4) AS
(
    SELECT c3, c4 FROM t2
),
q4 (q4_c3, q4_c4) AS
(
    SELECT q3_c3, q3_c4 FROM q3
)
SELECT
    q2_c1,
    q2_c2,
    q4_c3,
    q4_c4
-- accepted
FROM q2 JOIN q4 FOR KEY (q4_c3, q4_c4) -> q2 (q2_c1, q2_c2);

WITH RECURSIVE q1 AS (SELECT c1 FROM t1 UNION SELECT c1 FROM q1)
-- rejected, reason: recursive CTEs do not expose surface FK facts
SELECT * FROM q1 JOIN t2 FOR KEY (c3) -> q1 (c1);

WITH RECURSIVE rec_self(c1) AS (
    SELECT c1 FROM t1
    UNION ALL
    SELECT t2.c3
    FROM rec_self
    -- rejected, reason: recursive self-references do not expose surface FK facts
    JOIN t2 FOR KEY (c3) -> rec_self (c1)
    WHERE false
)
SELECT * FROM rec_self;

-- Regression: a FOR KEY join inside a CTE body of a WITH RECURSIVE
-- block must not raise "unrecognized node type" from the raw
-- parse-tree walker that builds the CTE dependency graph.
WITH RECURSIVE rec_kj AS (
    SELECT t2.c3 FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1)
    UNION ALL
    SELECT c3 FROM rec_kj WHERE false
)
-- rejected, reason: recursive CTEs do not expose surface FK facts
SELECT * FROM rec_kj JOIN t2 t2b FOR KEY (c3) -> rec_kj (c3);

-- Regression: same as above, but the FOR KEY join is in a sibling CTE
-- of the recursive one.  makeDependencyGraph walks every CTE body in
-- the WITH block, so the raw walker must handle KeyJoinClause here too.
WITH RECURSIVE rec_only AS (
    SELECT 1 AS x UNION ALL SELECT x FROM rec_only WHERE false
), kj_sibling AS (
    SELECT t1.c1 FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1)
)
SELECT * FROM rec_only, kj_sibling;

--
-- Test VIEWs
--

CREATE VIEW v1 AS
SELECT c1 AS v1_c1, c2 AS v1_c2 FROM t1;

CREATE VIEW v2 AS
SELECT v1_c1 AS v2_c1, v1_c2 AS v2_c2 FROM v1;

CREATE VIEW v3 AS
SELECT c3 AS v3_c3, c4 AS v3_c4 FROM t2;

CREATE VIEW v4 AS
SELECT v3_c3 AS v4_c3, v3_c4 AS v4_c4 FROM v3;

--
-- Test subqueries, CTEs, and views
--
WITH
q2 (q2_c1, q2_c2) AS
(
    SELECT
        q1_c1,
        q1_c2
    FROM
    (
        SELECT c1 AS q1_c1, c2 AS q1_c2 FROM t1
    ) AS q1
)
SELECT
    q2_c1,
    q2_c2,
    v4_c3,
    v4_c4
-- accepted
FROM q2 JOIN v4 FOR KEY (v4_c3, v4_c4) -> q2 (q2_c1, q2_c2);

DROP VIEW v1, v2, v3, v4;

--
-- Test subqueries, CTEs and VIEWs containing joins
--

SELECT
    q1.c11,
    q1.c12,
    t6.c13,
    t6.c14
FROM
(
    SELECT
        t5.c9,
        t5.c10,
        t5.c11,
        t5.c12
    FROM t5
-- accepted
    JOIN t1 FOR KEY (c1, c2) <- t5 (c11, c12)
    JOIN t1 AS t1_2 FOR KEY (c1, c2) <- t5 (c11, c12)
    JOIN t1 AS t1_3 FOR KEY (c1, c2) <- t5 (c11, c12)
) AS q1
JOIN t6 FOR KEY (c13, c14) -> q1 (c9, c10);

WITH
q1 AS
(
    SELECT
        t5.c9,
        t5.c10,
        t5.c11,
        t5.c12
    FROM t5
-- accepted
    JOIN t1 FOR KEY (c1, c2) <- t5 (c11, c12)
    JOIN t1 AS t1_2 FOR KEY (c1, c2) <- t5 (c11, c12)
    JOIN t1 AS t1_3 FOR KEY (c1, c2) <- t5 (c11, c12)
)
SELECT
    q1.c11,
    q1.c12,
    t6.c13,
    t6.c14
FROM q1
JOIN t6 FOR KEY (c13, c14) -> q1 (c9, c10);

--
-- Test disallowed filtering of referenced table
--

CREATE VIEW v1 AS
SELECT * FROM t1 WHERE c1 > 0;

CREATE VIEW v2 AS
SELECT * FROM t2 WHERE c3 > 0;

-- rejected, reason: a filter on the referenced side (v1) defeats row coverage
SELECT * FROM v1 JOIN t2 FOR KEY (c3) -> v1 (c1);

-- accepted, filtering allowed since v2 is the referencing table
SELECT * FROM t1 JOIN v2 FOR KEY (c3) -> t1 (c1);

-- rejected, reason: a filter on the referenced side (v1) defeats row coverage,
-- even when the referencing side applies an equivalent filter
SELECT * FROM v1 JOIN v2 FOR KEY (c3) -> v1 (c1);

-- rejected, reason: a HAVING filter on the referenced side defeats row coverage
SELECT * FROM
(
    SELECT c1, count(*) FROM t1 GROUP BY c1 HAVING c2 > 100
) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

-- rejected, reason: u is filtered and is the referenced table
SELECT * FROM (SELECT c1 FROM t1 LIMIT 1) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

-- rejected, reason: u is filtered and is the referenced table
SELECT * FROM (SELECT c1 FROM t1 OFFSET 1) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

-- accepted: DISTINCT on the referenced key preserves its row coverage and
-- proves its uniqueness
SELECT * FROM (SELECT DISTINCT c1 FROM t1) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

-- accepted: GROUP BY on the referenced key preserves its row coverage and
-- proves its uniqueness
SELECT * FROM (SELECT c1 FROM t1 GROUP BY c1) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

-- rejected, reason: GROUPING SETS can null the referenced key, so its
-- uniqueness is not proven
SELECT * FROM (SELECT c1 FROM t1 GROUP BY GROUPING SETS ((c1), ())) AS u
JOIN t2 FOR KEY (c3) -> u (c1);

WITH q2 AS
(
    SELECT * FROM t5 WHERE t5.c11 > 0
)
SELECT
    q1.c11,
    q1.c12,
    t7.c15,
    t7.c16
FROM
(
    SELECT
        q2.c9,
        q2.c10,
        q2.c11,
        q2.c12
    FROM q2
-- rejected, reason: referenced-side q1 is filtered — the filter defeats row coverage of c9,c10
    JOIN t1 FOR KEY (c1, c2) <- q2 (c11, c12)
) AS q1
JOIN t7 FOR KEY (c15, c16) -> q1 (c9, c10);

--
-- Test allowed joins not affecting uniqueness
--

SELECT
    q1.c11,
    q1.c12,
    t6.c13,
    t6.c14
FROM
(
    SELECT
        t5.c9,
        t5.c10,
        t5.c11,
        t5.c12
    FROM t5
-- accepted
    JOIN t1 FOR KEY (c1, c2) <- t5 (c11, c12)
) AS q1
JOIN t6 FOR KEY (c13, c14) -> q1 (c9, c10);

--
-- Test disallowed non-unique referenced table
--

SELECT
    q1.c11,
    q1.c12,
    t7.c15,
    t7.c16
FROM
(
    SELECT
        t5.c9,
        t5.c10,
        t5.c11,
        t5.c12
    FROM t5
-- accepted
    JOIN t1 FOR KEY (c1, c2) <- t5 (c11, c12)
    JOIN t6 FOR KEY (c13, c14) -> t5 (c9, c10)
) AS q1
-- rejected, reason: inner key join duplicates the referenced key
JOIN t7 FOR KEY (c15, c16) -> q1 (c9, c10);

CREATE TABLE t1_nn (t1_id INTEGER PRIMARY KEY);
CREATE TABLE t2_nn (
    t2_id INTEGER PRIMARY KEY,
    t2_t1_id_nn INTEGER NOT NULL REFERENCES t1_nn(t1_id),
    t2_t1_id_nullable INTEGER REFERENCES t1_nn(t1_id)
);

INSERT INTO t1_nn VALUES (1), (2), (3);
INSERT INTO t2_nn VALUES (10, 1, 1), (20, 2, NULL);

--
-- Test nullable referencing columns
--

-- N1: Nullable FK, INNER JOIN → error
SELECT * FROM t2_nn
-- rejected, reason: nullable referencing column is not preserved by an inner key join
JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nullable);

-- N3: Nullable FK, LEFT JOIN preserving referencing side → accepted
SELECT * FROM t2_nn
-- accepted
LEFT JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nullable);

-- N4: Nullable FK, RIGHT JOIN preserving referencing side → accepted
SELECT * FROM t1_nn
-- accepted
RIGHT JOIN t2_nn FOR KEY (t2_t1_id_nullable) -> t1_nn (t1_id);

-- N5: Nullable FK, FULL JOIN → accepted
SELECT * FROM t2_nn
-- accepted
FULL JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nullable);

-- N7: Nullable FK, LEFT JOIN but referencing on inner side → error
SELECT * FROM t1_nn
-- rejected, reason: nullable referencing column is on the non-preserved side
LEFT JOIN t2_nn FOR KEY (t2_t1_id_nullable) -> t1_nn (t1_id);

-- NE1: NOT NULL FK col null-extended by prior FK LEFT JOIN → null-extended error
-- t1_nn a LEFT JOIN t2_nn makes t2_t1_id_nn null-extended; the subsequent INNER key join
-- on that column should fire the null-extended branch of the error.
SELECT * FROM
    t1_nn a
    LEFT JOIN t2_nn FOR KEY (t2_t1_id_nn) -> a (t1_id)
-- rejected, reason: FK column was null-extended by the prior outer join
    JOIN t1_nn b FOR KEY (t1_id) <- t2_nn (t2_t1_id_nn);

-- NE2: NOT NULL FK col null-extended through prior outer key joins → error
CREATE TABLE t1_nn_chain (t1_id INTEGER PRIMARY KEY);
CREATE TABLE t2_nn_chain (
    t2_id INTEGER PRIMARY KEY,
    t2_t1_id INTEGER NOT NULL REFERENCES t1_nn_chain(t1_id)
);
CREATE TABLE t3_nn_chain (
    t3_id INTEGER PRIMARY KEY,
    t3_t2_id INTEGER NOT NULL UNIQUE REFERENCES t2_nn_chain(t2_id)
);

INSERT INTO t1_nn_chain VALUES (1), (2);
INSERT INTO t2_nn_chain VALUES (10, 1), (20, 2);
INSERT INTO t3_nn_chain VALUES (100, 10);

SELECT * FROM
    t2_nn_chain m
-- accepted
    FULL JOIN t3_nn_chain l FOR KEY (t3_t2_id) -> m (t2_id)
    LEFT JOIN t2_nn_chain m2 FOR KEY (t2_id) <- l (t3_t2_id)
-- rejected, reason: FK column was null-extended through prior outer key joins
    JOIN t1_nn_chain r FOR KEY (t1_id) <- m2 (t2_t1_id);

DROP TABLE t3_nn_chain, t2_nn_chain, t1_nn_chain;

-- NE3: outer padding null-extends every base table in a nested subtree
CREATE TABLE ne3_a (id INTEGER PRIMARY KEY);
CREATE TABLE ne3_e (id INTEGER PRIMARY KEY);
CREATE TABLE ne3_c (
    id   INTEGER PRIMARY KEY,
    e_id INTEGER NOT NULL REFERENCES ne3_e(id)
);
CREATE TABLE ne3_b (
    id   INTEGER PRIMARY KEY,
    a_id INTEGER NOT NULL REFERENCES ne3_a(id),
    c_id INTEGER NOT NULL REFERENCES ne3_c(id)
);

INSERT INTO ne3_a VALUES (1), (2);
INSERT INTO ne3_e VALUES (10);
INSERT INTO ne3_c VALUES (100, 10);
INSERT INTO ne3_b VALUES (1000, 1, 100);

SELECT * FROM
(
    SELECT ne3_a.id AS a_id, ne3_c.e_id AS c_e_id
    FROM ne3_a
-- accepted
    LEFT JOIN (ne3_c JOIN ne3_b FOR KEY (c_id) -> ne3_c (id))
        FOR KEY (a_id) -> ne3_a (id)
) q
-- rejected, reason: c_e_id is null-extended by the preceding LEFT key join
JOIN ne3_e FOR KEY (id) <- q (c_e_id);

DROP TABLE ne3_b, ne3_c, ne3_e, ne3_a;

DROP TABLE t2_nn, t1_nn;

--
-- Test various error conditions
--

-- rejected, reason: arrow alias is not present in the left join operand
SELECT * FROM t1 JOIN t2 FOR KEY (c3, c4) -> t3 (c1, c2);

CREATE SCHEMA key_join_alias_ambiguous;
CREATE TABLE key_join_alias_ambiguous.t1 (c1 int PRIMARY KEY);

-- rejected, reason: arrow alias matches two visible left-operand relations
SELECT *
FROM public.t1 CROSS JOIN key_join_alias_ambiguous.t1
JOIN t2 FOR KEY (c3) -> t1 (c1);

DROP SCHEMA key_join_alias_ambiguous CASCADE;

-- rejected, reason: referencing and referenced column counts differ
SELECT * FROM t1 JOIN t2 FOR KEY (c3, c4) -> t1 (c1);
-- rejected, reason: referencing and referenced column counts differ
SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1, c2);
-- rejected, reason: referencing and referenced column counts differ
SELECT * FROM t1 JOIN t2 FOR KEY (c3, c4) -> t1 (c1, c2, c3);
-- rejected, reason: referencing column c5 does not exist
SELECT * FROM t1 JOIN t2 FOR KEY (c3, c4, c5) -> t1 (c1, c2);

CREATE FUNCTION t2() RETURNS TABLE (c3 INTEGER, c4 INTEGER)
LANGUAGE sql
RETURN (1, 2);

-- rejected, reason: functions in FROM do not expose surface FK facts
SELECT * FROM t1 JOIN t2() FOR KEY (c3, c4) -> t1 (c1, c2);

-- rejected, reason: referenced column c5 does not exist
SELECT * FROM t1 JOIN t2 FOR KEY (c3, c4) -> t1 (c1, c5);

-- rejected, reason: referenced column name is duplicated in the left operand
SELECT *
FROM (SELECT c1, c2 AS c1 FROM t1) AS dup_ref
JOIN t2 FOR KEY (c3) -> dup_ref (c1);

-- rejected, reason: referencing column is not exposed by the left join operand
SELECT *
FROM t2 AS fk_keep JOIN t2 AS fk_hidden USING (c3)
JOIN t1 FOR KEY (c1) <- fk_hidden (c3);

-- rejected, reason: referenced column is not exposed by the left join operand
SELECT *
FROM t1 AS pk_keep JOIN t1 AS pk_hidden USING (c1)
JOIN t2 FOR KEY (c3) -> pk_hidden (c1);

DROP VIEW v1, v2;
CREATE VIEW v1 AS SELECT c1 AS c1_1, c1 AS c1_2, c2 AS c2_1, c2 AS c2_2 FROM t1;
CREATE VIEW v2 AS SELECT c3 AS c3_1, c3 AS c3_2, c4 AS c4_1, c4 AS c4_2 FROM t2;

SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, c2_1); -- accepted
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_2, c2_1); -- accepted
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, c2_2); -- accepted
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_2, c2_2); -- accepted
SELECT * FROM v1 JOIN v2 FOR KEY (c3_2, c4_1) -> v1 (c1_2, c2_1); -- accepted

-- rejected, reason: selected referenced columns do not map to the composite key
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, c1_2);

-- rejected, reason: referenced column does not exist
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, nonexistent);

/*
 * We don't need to check for duplicate columns,
 * since there is already such a check for foreign key constraints.
 * Let's test it anyway.
 */
-- rejected, reason: duplicate selected columns do not match a foreign key
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c3) -> v1 (c1_1, c1_1);
-- rejected, reason: duplicate referencing aliases do not map to the composite FK
SELECT * FROM v1 JOIN v2 FOR KEY (c3_1, c3_2) -> v1 (c1_1, c2_1);

DROP VIEW v1;
CREATE VIEW v1 AS SELECT c1+0 AS c1_1, c1 AS c1_2, c2 AS c2_1, c2 AS c2_2 FROM t1;
-- rejected, reason: referenced key column is an expression, not a direct Var
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, c2_1);
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_2, c2_1); -- accepted

SELECT * FROM t1 JOIN
(
    SELECT c3, c4 FROM t2
    UNION ALL
    SELECT c3, c4 FROM t2
-- rejected, reason: set operations do not expose surface FK facts
) AS u FOR KEY (c3, c4) -> t1 (c1, c2);

-- rejected, reason: GROUPING SETS can null omitted referencing key columns
SELECT * FROM t1 JOIN
(
    SELECT c3, c4 FROM t2
    GROUP BY GROUPING SETS ((c3), (c4))
) AS sub FOR KEY (c3, c4) -> t1 (c1, c2);

-- rejected, reason: key join on subquery with SRF in target list
SELECT * FROM t1 JOIN
(
    SELECT generate_series(1, 3) AS gs, c3, c4 FROM t2
) AS sub FOR KEY (c3, c4) -> t1 (c1, c2);

SELECT * FROM
(
    SELECT c1, c2 FROM t1 WHERE c2 > 0
) AS u
-- rejected, reason: a filter on the referenced side defeats row coverage
JOIN t2 FOR KEY (c3, c4) -> u (c1, c2);

SELECT *
FROM t1
JOIN
(
    SELECT * FROM t10
    JOIN t10 AS t10_2 FOR KEY (c23, c24) <- t10 (c25, c26)
-- rejected, reason: derived referenced table exposes duplicate key column names
) AS q1 FOR KEY (c23, c24) -> t1 (c1, c2);

SELECT *
FROM t1
JOIN
(
    SELECT
        t10.c23,
        t10.c24,
        t10_2.c25,
        t10_2.c26
    FROM t10
-- accepted
    JOIN t10 AS t10_2 FOR KEY (c23, c24) <- t10 (c25, c26)
-- rejected, reason: referencing column does not exist in the derived table
) AS q1 FOR KEY (nonexistent, c24) -> t1 (c1, c2);

SELECT *
FROM t1
JOIN
(
    SELECT
        t10.c23,
        t10_2.c24
    FROM t10
-- accepted
    JOIN t10 AS t10_2 FOR KEY (c23, c24) <- t10 (c25, c26)
-- rejected, reason: selected derived columns do not preserve the FK mapping
) AS q1 FOR KEY (c23, c24) -> t1 (c1, c2);

--
-- Test materialized views (not supported yet)
--

CREATE MATERIALIZED VIEW mv1 AS
SELECT c1, c2 FROM t1;

-- rejected, reason: materialized views do not expose surface FK facts
SELECT * FROM mv1 JOIN t2 FOR KEY (c3, c4) -> mv1 (c1, c2);

DROP MATERIALIZED VIEW mv1;

--
-- Test PERIOD foreign keys (not key-join proof sources)
--

CREATE TABLE period_fk_parent
(
    id int4range,
    valid_at daterange,
    PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),
    UNIQUE (id, valid_at)
);

CREATE TABLE period_fk_child
(
    id int PRIMARY KEY,
    parent_id int4range NOT NULL,
    valid_at daterange NOT NULL,
    FOREIGN KEY (parent_id, PERIOD valid_at)
        REFERENCES period_fk_parent (id, PERIOD valid_at)
);

INSERT INTO period_fk_parent VALUES
    ('[1,2)', daterange('2020-01-01', '2020-02-01')),
    ('[1,2)', daterange('2020-02-01', '2020-03-01'));
INSERT INTO period_fk_child VALUES
    (1, '[1,2)', daterange('2020-01-15', '2020-02-15'));

SELECT p.id, p.valid_at AS parent_period, c.id, c.valid_at AS child_period
FROM period_fk_parent p
-- rejected, reason: PERIOD FK can match multiple referenced periods
JOIN period_fk_child c FOR KEY (parent_id, valid_at) -> p (id, valid_at);

DROP TABLE period_fk_child, period_fk_parent;

--
-- Test regular inheritance tables
--

CREATE TABLE inh_parent_ref
(
    id int PRIMARY KEY
);

CREATE TABLE inh_child_ref () INHERITS (inh_parent_ref);

CREATE TABLE inh_fk_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES inh_parent_ref(id)
);

INSERT INTO inh_parent_ref VALUES (1);
INSERT INTO inh_child_ref VALUES (1);
INSERT INTO inh_fk_child VALUES (1, 1);

SELECT p.id, c.id
FROM inh_parent_ref p
-- rejected, reason: inherited parent scans are not supported for key joins
JOIN inh_fk_child c FOR KEY (parent_id) -> p (id);

SELECT p.id, c.id
FROM ONLY inh_parent_ref p
-- accepted
JOIN inh_fk_child c FOR KEY (parent_id) -> p (id);

DROP TABLE inh_fk_child, inh_child_ref, inh_parent_ref;

--
-- Test partitioned tables
--

CREATE TABLE pt2
(
    c3 int not null,
    c4 int not null,
    CONSTRAINT pt2_pkey PRIMARY KEY (c3),
    CONSTRAINT pt2_c3_fkey FOREIGN KEY (c3) REFERENCES t1 (c1)
) PARTITION BY RANGE (c3);

CREATE TABLE pt2_1 PARTITION OF pt2 FOR VALUES FROM (1) TO (3);
CREATE TABLE pt2_2 PARTITION OF pt2 FOR VALUES FROM (3) TO (4);

CREATE TABLE pt3
(
    c5 int not null,
    c6 int not null,
    CONSTRAINT pt3_pkey PRIMARY KEY (c5),
    CONSTRAINT pt3_c5_fkey FOREIGN KEY (c5) REFERENCES pt2 (c3)
) PARTITION BY RANGE (c5);

CREATE TABLE pt3_1 PARTITION OF pt3 FOR VALUES FROM (1) TO (3);
CREATE TABLE pt3_2 PARTITION OF pt3 FOR VALUES FROM (3) TO (4);

INSERT INTO pt2 (c3, c4) VALUES (1, 100);
INSERT INTO pt2 (c3, c4) VALUES (3, 300);
INSERT INTO pt3 (c5, c6) VALUES (1, 1000);
INSERT INTO pt3 (c5, c6) VALUES (3, 3000);

-- accepted
SELECT * FROM t1 JOIN pt2 FOR KEY (c3) -> t1 (c1) JOIN pt3 FOR KEY (c5) -> pt2 (c3);
-- accepted
SELECT * FROM t1 JOIN pt2_1 FOR KEY (c3) -> t1 (c1);
-- rejected, reason: ONLY scans on partitioned tables are not supported
SELECT * FROM ONLY pt2 JOIN pt3 FOR KEY (c5) -> pt2 (c3);

DROP TABLE pt3;
DROP TABLE pt2;

SELECT *
FROM (SELECT * FROM (SELECT c1, c2 FROM t1) AS q1(q1_c1, q1_c2)) q
-- accepted
JOIN t2 FOR KEY (c3, c4) -> q (q1_c1, q1_c2);
-- equivalent to:
SELECT *
FROM (WITH q1 (q1_c1, q1_c2) AS (SELECT c1, c2 FROM t1) SELECT * FROM q1) q
-- accepted
JOIN t2 FOR KEY (c3, c4) -> q (q1_c1, q1_c2);

--
-- The below query should raise the error "key join violation",
-- "referenced relation does not preserve uniqueness of keys", since
-- even though there is a UNIQUE constraint on orders.shipment_id,
-- the orders table doesn't preserve the uniqueness of its keys
-- due to the join with order_items.
--

CREATE TABLE shipments
(
    id INTEGER PRIMARY KEY,
    carrier TEXT NOT NULL
);

CREATE TABLE orders
(
    id INTEGER PRIMARY KEY,
    shipment_id INTEGER UNIQUE NULL REFERENCES shipments(id)
);

CREATE TABLE order_items
(
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id)
);

CREATE TABLE packages
(
    id INTEGER PRIMARY KEY,
    shipment_id INTEGER NOT NULL REFERENCES shipments(id)
);

INSERT INTO shipments (id, carrier) VALUES
  (100, 'UPS'),
  (200, 'FedEx');

INSERT INTO orders (id, shipment_id) VALUES
  (10, 100),
  (20, NULL);

INSERT INTO order_items (id, order_id) VALUES
  (1000, 10),
  (1001, 10);

INSERT INTO packages (id, shipment_id) VALUES
  (900, 100);

CREATE TABLE payments
(
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    amount NUMERIC NOT NULL,
    method TEXT NOT NULL
);

INSERT INTO payments (id, order_id, amount, method) VALUES
  (100, 10, 50.00, 'card'),
  (200, 20, 29.99, 'card');

--
-- Inline fan trap: order_items and payments both join to orders via
-- separate key joins.  The first join (order_items <- orders) consumes
-- orders.id uniqueness, so the second join (payments -> orders) must
-- be rejected — orders.id is no longer unique in the intermediate result.
-- Wrapping the same joins in a view correctly rejects this; inline must
-- behave identically.
--
SELECT *
FROM order_items
-- accepted
JOIN orders FOR KEY (id) <- order_items (order_id)
JOIN payments FOR KEY (order_id) -> orders (id);

--
-- Cross-type FK equality semantics are intentionally not supported by
-- key-join proof.  The text UNIQUE constraint distinguishes 'a' and 'a ',
-- but the FK to char(2) treats both as the same referenced key value.
-- If accepted, the first LEFT key join would therefore duplicate p.id, and
-- the outer key join through q.id must not use c.parent_id's text uniqueness
-- to preserve p.id uniqueness.
-- Supporting this would require carrying the distinction between FK
-- equality and uniqueness equality through later output-fact projection.
--
CREATE TABLE eq_sem_parent
(
    id char(2) PRIMARY KEY
);
CREATE TABLE eq_sem_child
(
    id int PRIMARY KEY,
    parent_id text NOT NULL UNIQUE REFERENCES eq_sem_parent (id)
);
CREATE TABLE eq_sem_ref
(
    id int PRIMARY KEY,
    parent_id char(2) NOT NULL REFERENCES eq_sem_parent (id)
);
INSERT INTO eq_sem_parent VALUES ('a');
INSERT INTO eq_sem_child VALUES (1, 'a'), (2, 'a ');
INSERT INTO eq_sem_ref VALUES (1, 'a');

SELECT p.id AS parent_id, c.id AS child_id, c.parent_id AS child_parent_id
FROM eq_sem_parent p
-- rejected, reason: FK and PK key types differ
LEFT JOIN eq_sem_child c FOR KEY (parent_id) -> p (id)
ORDER BY c.id;

SELECT q.id AS parent_id, r.id AS ref_id
FROM (
    SELECT p.id
    FROM eq_sem_parent p
-- rejected, reason: FK and PK key types differ
    LEFT JOIN eq_sem_child c FOR KEY (parent_id) -> p (id)
) q
-- rejected, reason: inner cross-type key join is rejected first
JOIN eq_sem_ref r FOR KEY (parent_id) -> q (id);

DROP TABLE eq_sem_ref, eq_sem_child, eq_sem_parent;

--
-- Key-join proof rejects cross-type FK/PK key identity.  PostgreSQL accepts
-- this FK, but key join deliberately does not compute proof facts from it.
-- Supporting it would require proving FK containment and referenced-side
-- uniqueness under different type-specific equality operators.
--
CREATE TABLE cross_type_key_parent
(
    id numeric PRIMARY KEY
);
CREATE TABLE cross_type_key_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES cross_type_key_parent (id)
);
INSERT INTO cross_type_key_parent VALUES (1);
INSERT INTO cross_type_key_child VALUES (10, 1);

SELECT p.id AS parent_id, c.id AS child_id, c.parent_id AS child_parent_id
FROM cross_type_key_parent p
-- rejected, reason: FK and PK key types differ
JOIN cross_type_key_child c FOR KEY (parent_id) -> p (id);

SELECT c.id AS child_id, p.id AS parent_id
FROM cross_type_key_child c
-- rejected, reason: FK and PK key types differ
JOIN cross_type_key_parent p FOR KEY (id) <- c (parent_id);

DROP TABLE cross_type_key_child, cross_type_key_parent;

--
-- Key-join proof also requires exact typmod identity.
-- Some typmod pairs may be safe, but proof does not reason about typmod
-- coercion or typmod-specific equality behavior.
--
CREATE TABLE typmod_key_parent
(
    id numeric(10,0) PRIMARY KEY
);
CREATE TABLE typmod_key_child
(
    id int PRIMARY KEY,
    parent_id numeric(10,2) NOT NULL REFERENCES typmod_key_parent (id)
);
INSERT INTO typmod_key_parent VALUES (1);
INSERT INTO typmod_key_child VALUES (10, 1.00);

SELECT p.id AS parent_id, c.id AS child_id
FROM typmod_key_parent p
-- rejected, reason: FK and PK typmods differ
JOIN typmod_key_child c FOR KEY (parent_id) -> p (id);

DROP TABLE typmod_key_child, typmod_key_parent;

CREATE TABLE varchar_key_customers
(
    id varchar(128) PRIMARY KEY
);
CREATE TABLE varchar_key_orders
(
    customer_id varchar(128) NOT NULL REFERENCES varchar_key_customers (id)
);
INSERT INTO varchar_key_customers VALUES ('alice'), ('bob');
INSERT INTO varchar_key_orders VALUES ('alice'), ('bob');

SELECT *
FROM varchar_key_customers
JOIN varchar_key_orders FOR KEY (customer_id) -> varchar_key_customers (id)
ORDER BY id, customer_id;

SELECT *
FROM varchar_key_orders
JOIN varchar_key_customers FOR KEY (id) <- varchar_key_orders (customer_id)
ORDER BY customer_id, id;

DROP TABLE varchar_key_orders, varchar_key_customers;

CREATE TABLE varchar_typmod_key_parent
(
    id varchar(128) PRIMARY KEY
);
CREATE TABLE varchar_typmod_key_child
(
    parent_id varchar(64) NOT NULL REFERENCES varchar_typmod_key_parent (id)
);
INSERT INTO varchar_typmod_key_parent VALUES ('alice');
INSERT INTO varchar_typmod_key_child VALUES ('alice');

SELECT *
FROM varchar_typmod_key_parent p
-- rejected, reason: FK and PK typmods differ
JOIN varchar_typmod_key_child c FOR KEY (parent_id) -> p (id);

DROP TABLE varchar_typmod_key_child, varchar_typmod_key_parent;

--
-- Key-join proof requires exact collation identity, even for two
-- deterministic collations with equivalent behavior.
-- Supporting this would require proving equivalence of collation semantics,
-- not just checking that both collations are deterministic.
--
CREATE COLLATION key_join_c_copy FROM "C";
CREATE TABLE collation_key_parent
(
    id text COLLATE "C" PRIMARY KEY
);
CREATE TABLE collation_key_child
(
    id int PRIMARY KEY,
    parent_id text COLLATE key_join_c_copy NOT NULL REFERENCES collation_key_parent (id)
);
INSERT INTO collation_key_parent VALUES ('a');
INSERT INTO collation_key_child VALUES (10, 'a');

SELECT p.id AS parent_id, c.id AS child_id
FROM collation_key_parent p
-- rejected, reason: FK and PK collations differ
JOIN collation_key_child c FOR KEY (parent_id) -> p (id);

DROP TABLE collation_key_child, collation_key_parent;
DROP COLLATION key_join_c_copy;

--
-- FK equality operators used for key-join proof must be immutable.  Temporal
-- cross-type equality against timestamptz depends on TimeZone, so an FK row
-- inserted under one TimeZone might not match the referenced row under
-- another.
--
SET TimeZone = 'UTC';

CREATE TABLE timezone_key_timestamp_parent
(
    id timestamp PRIMARY KEY
);
CREATE TABLE timezone_key_timestamp_child
(
    id int PRIMARY KEY,
    parent_id timestamptz NOT NULL REFERENCES timezone_key_timestamp_parent (id)
);
INSERT INTO timezone_key_timestamp_parent VALUES
    (TIMESTAMP '2020-01-01 00:00:00');
INSERT INTO timezone_key_timestamp_child VALUES
    (1, TIMESTAMPTZ '2020-01-01 00:00:00+00');

CREATE TABLE timezone_key_date_parent
(
    id date PRIMARY KEY
);
CREATE TABLE timezone_key_date_child
(
    id int PRIMARY KEY,
    parent_id timestamptz NOT NULL REFERENCES timezone_key_date_parent (id)
);
INSERT INTO timezone_key_date_parent VALUES (DATE '2020-01-01');
INSERT INTO timezone_key_date_child VALUES
    (1, TIMESTAMPTZ '2020-01-01 00:00:00+00');

SET TimeZone = 'Europe/Stockholm';

SELECT TIMESTAMP '2020-01-01 00:00:00' =
       TIMESTAMPTZ '2020-01-01 00:00:00+00' AS timestamp_equality_now;
SELECT DATE '2020-01-01' =
       TIMESTAMPTZ '2020-01-01 00:00:00+00' AS date_equality_now;

SELECT c.id AS child_id, p.id AS parent_id
FROM timezone_key_timestamp_child c
-- rejected, reason: FK equality operator is not immutable
JOIN timezone_key_timestamp_parent p FOR KEY (id) <- c (parent_id);

SELECT c.id AS child_id, p.id AS parent_id
FROM timezone_key_date_child c
-- rejected, reason: FK equality operator is not immutable
JOIN timezone_key_date_parent p FOR KEY (id) <- c (parent_id);

RESET TimeZone;

DROP TABLE timezone_key_timestamp_child, timezone_key_timestamp_parent;
DROP TABLE timezone_key_date_child, timezone_key_date_parent;

--
-- FK equality operators used for key-join proof must be strict.  Otherwise a
-- nullable referencing row preserved by an outer key join can match multiple
-- referenced rows.
--
CREATE FUNCTION key_join_nonstrict_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE AS
$$ SELECT a IS NULL OR b IS NULL OR a = b $$;

CREATE OPERATOR ===
(
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = key_join_nonstrict_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR CLASS key_join_nonstrict_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 1 < (int, int),
    OPERATOR 2 <= (int, int),
    OPERATOR 3 === (int, int),
    OPERATOR 4 >= (int, int),
    OPERATOR 5 > (int, int),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE key_join_nonstrict_parent
(
    id int
);
CREATE UNIQUE INDEX key_join_nonstrict_parent_idx
    ON key_join_nonstrict_parent
    USING btree (id key_join_nonstrict_int4_ops);

CREATE TABLE key_join_nonstrict_child
(
    id int PRIMARY KEY,
    parent_id int REFERENCES key_join_nonstrict_parent (id)
);

INSERT INTO key_join_nonstrict_parent VALUES (1), (2);
INSERT INTO key_join_nonstrict_child VALUES (10, NULL);

SELECT c.id AS child_id, p.id AS parent_id
FROM key_join_nonstrict_child c
-- rejected, reason: FK equality operator is not strict
LEFT JOIN key_join_nonstrict_parent p FOR KEY (id) <- c (parent_id);

DROP TABLE key_join_nonstrict_child,
           key_join_nonstrict_parent;
DROP OPERATOR CLASS key_join_nonstrict_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_nonstrict_int4_ops USING btree;
DROP OPERATOR === (int, int);
DROP FUNCTION key_join_nonstrict_eq(int, int);

--
-- FK equality operators used for key-join proof must match the normalized
-- referenced-key equality operator.  A domain FK can use a cross-type equality
-- operator that is valid for RI enforcement but not identical to the base-type
-- equality member used for same-domain key joins.
--
CREATE DOMAIN key_join_ri_mismatch_dom AS int;

CREATE FUNCTION key_join_ri_mismatch_dom_cmp(a key_join_ri_mismatch_dom,
                                             b key_join_ri_mismatch_dom)
RETURNS int LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT btint4cmp(a::int, b::int) $$;

CREATE FUNCTION key_join_ri_mismatch_dom_int_cmp(a key_join_ri_mismatch_dom,
                                                 b int)
RETURNS int LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT btint4cmp(a::int, b) $$;

CREATE FUNCTION key_join_ri_mismatch_int_dom_cmp(a int,
                                                 b key_join_ri_mismatch_dom)
RETURNS int LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT btint4cmp(a, b::int) $$;

CREATE FUNCTION key_join_ri_mismatch_dom_eq(a key_join_ri_mismatch_dom,
                                            b key_join_ri_mismatch_dom)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT a::int = b::int $$;

CREATE FUNCTION key_join_ri_mismatch_dom_int_eq(a key_join_ri_mismatch_dom,
                                                b int)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT a::int = b $$;

CREATE FUNCTION key_join_ri_mismatch_int_dom_eq(a int,
                                                b key_join_ri_mismatch_dom)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT a = b::int $$;

CREATE OPERATOR ===
(
    LEFTARG = key_join_ri_mismatch_dom,
    RIGHTARG = key_join_ri_mismatch_dom,
    PROCEDURE = key_join_ri_mismatch_dom_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR ==#
(
    LEFTARG = key_join_ri_mismatch_dom,
    RIGHTARG = int,
    PROCEDURE = key_join_ri_mismatch_dom_int_eq,
    COMMUTATOR = #==,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR #==
(
    LEFTARG = int,
    RIGHTARG = key_join_ri_mismatch_dom,
    PROCEDURE = key_join_ri_mismatch_int_dom_eq,
    COMMUTATOR = ==#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR FAMILY key_join_ri_mismatch_dom_ops USING btree;

CREATE OPERATOR CLASS key_join_ri_mismatch_dom_ops
FOR TYPE key_join_ri_mismatch_dom USING btree
FAMILY key_join_ri_mismatch_dom_ops AS
    OPERATOR 3 === (key_join_ri_mismatch_dom, key_join_ri_mismatch_dom),
    FUNCTION 1 key_join_ri_mismatch_dom_cmp(key_join_ri_mismatch_dom,
                                            key_join_ri_mismatch_dom);

ALTER OPERATOR FAMILY key_join_ri_mismatch_dom_ops USING btree ADD
    OPERATOR 3 ==# (key_join_ri_mismatch_dom, int),
    OPERATOR 3 #== (int, key_join_ri_mismatch_dom),
    OPERATOR 3 = (int, int),
    FUNCTION 1 key_join_ri_mismatch_dom_int_cmp(key_join_ri_mismatch_dom, int),
    FUNCTION 1 key_join_ri_mismatch_int_dom_cmp(int, key_join_ri_mismatch_dom),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE key_join_ri_mismatch_parent
(
    id key_join_ri_mismatch_dom
);
CREATE UNIQUE INDEX key_join_ri_mismatch_parent_idx
    ON key_join_ri_mismatch_parent
    USING btree (id key_join_ri_mismatch_dom_ops);

CREATE TABLE key_join_ri_mismatch_child
(
    id int PRIMARY KEY,
    parent_id key_join_ri_mismatch_dom NOT NULL
        REFERENCES key_join_ri_mismatch_parent (id)
);

INSERT INTO key_join_ri_mismatch_parent VALUES (1);
INSERT INTO key_join_ri_mismatch_child VALUES (10, 1);

SELECT c.id AS child_id, p.id AS parent_id
FROM key_join_ri_mismatch_parent p
-- rejected, reason: FK equality operator differs from referenced-key equality
JOIN key_join_ri_mismatch_child c FOR KEY (parent_id) -> p (id);

DROP TABLE key_join_ri_mismatch_child,
           key_join_ri_mismatch_parent;
DROP OPERATOR CLASS key_join_ri_mismatch_dom_ops USING btree;
DROP OPERATOR FAMILY key_join_ri_mismatch_dom_ops USING btree;
DROP OPERATOR === (key_join_ri_mismatch_dom, key_join_ri_mismatch_dom);
DROP OPERATOR ==# (key_join_ri_mismatch_dom, int);
DROP OPERATOR #== (int, key_join_ri_mismatch_dom);
DROP FUNCTION key_join_ri_mismatch_dom_cmp(key_join_ri_mismatch_dom,
                                           key_join_ri_mismatch_dom);
DROP FUNCTION key_join_ri_mismatch_dom_int_cmp(key_join_ri_mismatch_dom, int);
DROP FUNCTION key_join_ri_mismatch_int_dom_cmp(int, key_join_ri_mismatch_dom);
DROP FUNCTION key_join_ri_mismatch_dom_eq(key_join_ri_mismatch_dom,
                                          key_join_ri_mismatch_dom);
DROP FUNCTION key_join_ri_mismatch_dom_int_eq(key_join_ri_mismatch_dom, int);
DROP FUNCTION key_join_ri_mismatch_int_dom_eq(int, key_join_ri_mismatch_dom);
DROP DOMAIN key_join_ri_mismatch_dom;

--
-- Key join trusts user-defined equality/operator-class contracts exactly as
-- PostgreSQL trusts them for indexes and constraints.  This deliberately
-- incoherent opclass is accepted because its catalog properties satisfy the
-- proof boundary; key join does not attempt to prove operator semantics.
--
CREATE FUNCTION key_join_trusted_contract_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT (a % 10) = (b % 10) $$;

CREATE OPERATOR ===
(
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = key_join_trusted_contract_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR CLASS key_join_trusted_contract_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 1 < (int, int),
    OPERATOR 2 <= (int, int),
    OPERATOR 3 === (int, int),
    OPERATOR 4 >= (int, int),
    OPERATOR 5 > (int, int),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE key_join_trusted_contract_parent
(
    id int NOT NULL
);
CREATE UNIQUE INDEX key_join_trusted_contract_parent_idx
    ON key_join_trusted_contract_parent
    USING btree (id key_join_trusted_contract_int4_ops);

CREATE TABLE key_join_trusted_contract_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_trusted_contract_parent (id)
);

INSERT INTO key_join_trusted_contract_parent VALUES (1), (11);
INSERT INTO key_join_trusted_contract_child VALUES (1, 1);

SELECT c.id AS child_id, p.id AS parent_id
FROM key_join_trusted_contract_child c
JOIN key_join_trusted_contract_parent p FOR KEY (id) <- c (parent_id)
ORDER BY parent_id;

DROP TABLE key_join_trusted_contract_child,
           key_join_trusted_contract_parent;
DROP OPERATOR CLASS key_join_trusted_contract_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_trusted_contract_int4_ops USING btree;
DROP OPERATOR === (int, int);
DROP FUNCTION key_join_trusted_contract_eq(int, int);

--
-- Reused derived relation identities: two references to the same CTE or
-- view body must remain distinct table occurrences.  The inner self-key join
-- duplicates q.pid and drops id = 2 as a referenced key, so the outer key
-- join through refs_to_nodes.ref_id must be rejected.
--
CREATE TABLE self_fk_nodes
(
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL REFERENCES self_fk_nodes(id)
);

CREATE TABLE refs_to_nodes
(
    id INTEGER PRIMARY KEY,
    ref_id INTEGER NOT NULL REFERENCES self_fk_nodes(id)
);

INSERT INTO self_fk_nodes VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO refs_to_nodes VALUES (1, 2);

WITH n AS (SELECT id, parent_id FROM self_fk_nodes)
SELECT *
FROM (
    SELECT p.id AS pid, c.id AS cid
    FROM n p
-- accepted
    JOIN n c FOR KEY (parent_id) -> p (id)
) q
-- rejected, reason: inner self-key join duplicates the referenced key
JOIN refs_to_nodes r FOR KEY (ref_id) -> q (pid);

CREATE VIEW self_fk_nodes_v AS
SELECT id, parent_id FROM self_fk_nodes;

SELECT *
FROM (
    SELECT p.id AS pid, c.id AS cid
    FROM self_fk_nodes_v p
-- accepted
    JOIN self_fk_nodes_v c FOR KEY (parent_id) -> p (id)
) q
-- rejected, reason: inner self-key join duplicates the referenced key
JOIN refs_to_nodes r FOR KEY (ref_id) -> q (pid);

SELECT *
FROM (
    SELECT *
    FROM (
        SELECT p.id AS pid, c.id AS cid
        FROM self_fk_nodes_v p
-- accepted
        JOIN self_fk_nodes_v c FOR KEY (parent_id) -> p (id)
    ) s
) q
-- rejected, reason: inner self-key join duplicates the referenced key
JOIN refs_to_nodes r FOR KEY (ref_id) -> q (pid);

DROP VIEW self_fk_nodes_v;
DROP TABLE refs_to_nodes;
DROP TABLE self_fk_nodes;

CREATE TABLE self_fk_nodes_valid
(
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL UNIQUE REFERENCES self_fk_nodes_valid(id)
);

CREATE TABLE refs_to_nodes_valid
(
    id INTEGER PRIMARY KEY,
    ref_id INTEGER NOT NULL REFERENCES self_fk_nodes_valid(id)
);

INSERT INTO self_fk_nodes_valid VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO refs_to_nodes_valid VALUES (1, 2);

WITH n AS (SELECT id, parent_id FROM self_fk_nodes_valid)
SELECT q.pid, q.cid, r.id AS rid
FROM (
    SELECT p.id AS pid, c.id AS cid
    FROM n p
-- accepted
    LEFT JOIN n c FOR KEY (parent_id) -> p (id)
) q
JOIN refs_to_nodes_valid r FOR KEY (ref_id) -> q (pid)
ORDER BY q.pid, q.cid, rid;

CREATE VIEW self_fk_nodes_valid_v AS
SELECT id, parent_id FROM self_fk_nodes_valid;

SELECT q.pid, q.cid, r.id AS rid
FROM (
    SELECT p.id AS pid, c.id AS cid
    FROM self_fk_nodes_valid_v p
-- accepted
    LEFT JOIN self_fk_nodes_valid_v c FOR KEY (parent_id) -> p (id)
) q
JOIN refs_to_nodes_valid r FOR KEY (ref_id) -> q (pid)
ORDER BY q.pid, q.cid, rid;

DROP VIEW self_fk_nodes_valid_v;
DROP TABLE refs_to_nodes_valid;
DROP TABLE self_fk_nodes_valid;

--
-- Correct pattern: pre-aggregate each child, then join through parent.
-- Each derived table has one row per order_id (GROUP BY guarantees it),
-- so both LEFT JOINs pass validation.
--
SELECT
    o.id,
    oi.item_count,
    p.payment_total
FROM orders o
LEFT JOIN (
    SELECT order_id, count(*) AS item_count
    FROM order_items
    GROUP BY order_id
-- accepted
) oi FOR KEY (order_id) -> o (id)
LEFT JOIN (
    SELECT order_id, SUM(amount) AS payment_total
    FROM payments
    GROUP BY order_id
) p FOR KEY (order_id) -> o (id);

DROP TABLE payments;

--
-- RLS-enabled relations do not expose key-join facts.  This is rejected
-- even for the table owner/superuser because stored key-join proofs must
-- not depend on role-specific RLS bypass behavior.
--
CREATE TABLE rls_key_join_parent
(
    id int PRIMARY KEY
);
CREATE TABLE rls_key_join_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rls_key_join_parent (id)
);

ALTER TABLE rls_key_join_parent ENABLE ROW LEVEL SECURITY;

SELECT *
FROM rls_key_join_parent p
JOIN rls_key_join_child c FOR KEY (parent_id) -> p (id);

ALTER TABLE rls_key_join_parent DISABLE ROW LEVEL SECURITY;
ALTER TABLE rls_key_join_child ENABLE ROW LEVEL SECURITY;

SELECT *
FROM rls_key_join_parent p
JOIN rls_key_join_child c FOR KEY (parent_id) -> p (id);

DROP TABLE rls_key_join_child, rls_key_join_parent;

-- rejected, reason: key join on data-modifying CTE
WITH ins AS (INSERT INTO t2 (c3, c4) VALUES (1, 10) RETURNING c3, c4)
SELECT * FROM t1 JOIN ins FOR KEY (c3, c4) -> t1 (c1, c2);

CREATE TABLE products_gbu (
    id integer PRIMARY KEY
);
CREATE TABLE sales_gbu (
    product_id integer NOT NULL REFERENCES products_gbu(id),
    qty numeric NOT NULL,
    price numeric NOT NULL
);
CREATE TABLE resupplies_gbu (
    product_id integer NOT NULL REFERENCES products_gbu(id),
    qty numeric NOT NULL,
    price numeric NOT NULL
);

INSERT INTO products_gbu VALUES (1), (2), (3);
INSERT INTO sales_gbu VALUES (1, 10, 5.00), (1, 20, 5.00), (2, 5, 10.00);
INSERT INTO resupplies_gbu VALUES (1, 100, 4.00), (2, 50, 8.00);

SELECT *
FROM (
    SELECT DISTINCT abs(id) AS id
    FROM products_gbu
) p
-- rejected, reason: DISTINCT expression does not prove key uniqueness
JOIN resupplies_gbu r FOR KEY (product_id) -> p (id);

-- Nondeterministic DISTINCT collations are exercised in key_join_icu.

-- Query-level DISTINCT equality must use an immutable, strict operator.
SET client_min_messages = warning;
CREATE TYPE query_unique_nonstrict_type;
CREATE FUNCTION query_unique_nonstrict_type_in(cstring)
RETURNS query_unique_nonstrict_type
LANGUAGE internal STRICT IMMUTABLE AS 'int4in';
CREATE FUNCTION query_unique_nonstrict_type_out(query_unique_nonstrict_type)
RETURNS cstring
LANGUAGE internal STRICT IMMUTABLE AS 'int4out';
CREATE TYPE query_unique_nonstrict_type (
    input = query_unique_nonstrict_type_in,
    output = query_unique_nonstrict_type_out,
    like = int4
);
RESET client_min_messages;
CREATE CAST (int4 AS query_unique_nonstrict_type) WITHOUT FUNCTION;
CREATE CAST (query_unique_nonstrict_type AS int4) WITHOUT FUNCTION;
CREATE FUNCTION query_unique_nonstrict_hash(query_unique_nonstrict_type)
RETURNS int LANGUAGE internal STRICT IMMUTABLE AS 'hashint4';
CREATE FUNCTION query_unique_nonstrict_eq(a query_unique_nonstrict_type,
                                          b query_unique_nonstrict_type)
RETURNS boolean LANGUAGE sql IMMUTABLE AS
$$ SELECT a IS NULL OR b IS NULL OR a::int4 = b::int4 $$;
CREATE OPERATOR === (
    LEFTARG = query_unique_nonstrict_type,
    RIGHTARG = query_unique_nonstrict_type,
    PROCEDURE = query_unique_nonstrict_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    HASHES
);
CREATE OPERATOR CLASS query_unique_nonstrict_type_ops
DEFAULT FOR TYPE query_unique_nonstrict_type USING hash AS
    OPERATOR 1 === (query_unique_nonstrict_type,
                    query_unique_nonstrict_type),
    FUNCTION 1 query_unique_nonstrict_hash(query_unique_nonstrict_type);
CREATE TABLE query_unique_nonstrict_parent (id query_unique_nonstrict_type);
CREATE TABLE query_unique_nonstrict_child (parent_id query_unique_nonstrict_type);
SELECT *
FROM (SELECT DISTINCT id FROM query_unique_nonstrict_parent) p
-- rejected, reason: DISTINCT equality operator is not strict
JOIN query_unique_nonstrict_child c FOR KEY (parent_id) -> p (id);
DROP TABLE query_unique_nonstrict_child, query_unique_nonstrict_parent;
DROP OPERATOR CLASS query_unique_nonstrict_type_ops USING hash;
DROP OPERATOR === (query_unique_nonstrict_type, query_unique_nonstrict_type);
DROP FUNCTION query_unique_nonstrict_eq(query_unique_nonstrict_type,
                                        query_unique_nonstrict_type);
DROP FUNCTION query_unique_nonstrict_hash(query_unique_nonstrict_type);
DROP CAST (query_unique_nonstrict_type AS int4);
DROP CAST (int4 AS query_unique_nonstrict_type);
SET client_min_messages = warning;
DROP TYPE query_unique_nonstrict_type CASCADE;
RESET client_min_messages;

-- DISTINCT ON a non-key column can discard referenced keys, so it must not
-- preserve the referenced relation's row set for a later key join.
CREATE TABLE products_do (
    id integer PRIMARY KEY,
    grp integer NOT NULL
);
CREATE TABLE reviews_do (
    id integer PRIMARY KEY,
    product_id integer NOT NULL REFERENCES products_do(id)
);

INSERT INTO products_do VALUES (1, 10), (2, 10);
INSERT INTO reviews_do VALUES (1, 1), (2, 2);

SELECT q.id, r.id
FROM (SELECT DISTINCT ON (grp) id FROM products_do ORDER BY grp, id) q
-- rejected, reason: DISTINCT ON does not cover the referenced key
JOIN reviews_do r FOR KEY (product_id) -> q (id);

DROP TABLE reviews_do, products_do;

-- DISTINCT ON drops sibling rows for each distinct group, so a base
-- rowCoverage on the wider key is no longer valid for the resulting
-- surface.  An outer GROUP BY (or DISTINCT) on the wider key supplies a
-- query-shape unique that, paired with the now-bogus catalog rowCoverage,
-- would let a key join through and silently drop FK-bearing rows whose
-- referenced row was dropped by the inner DISTINCT ON.
CREATE TABLE rowcov_pk (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b));
CREATE TABLE rowcov_fk (fk_a int NOT NULL, fk_b int NOT NULL,
                        FOREIGN KEY (fk_a, fk_b) REFERENCES rowcov_pk (a, b));
INSERT INTO rowcov_pk VALUES (1, 1), (1, 2), (2, 1);
INSERT INTO rowcov_fk VALUES (1, 2);

SELECT *
FROM (
    SELECT a, b
    FROM (SELECT DISTINCT ON (a) a, b FROM rowcov_pk ORDER BY a, b) i
    GROUP BY a, b
) sub
-- rejected, reason: inner DISTINCT ON (a) drops sibling rows so (a, b)
-- row coverage is lost regardless of the outer GROUP BY
JOIN rowcov_fk FOR KEY (fk_a, fk_b) -> sub (a, b);

SELECT *
FROM (
    SELECT DISTINCT a, b
    FROM (SELECT DISTINCT ON (a) a, b FROM rowcov_pk ORDER BY a, b) i
) sub
-- rejected, reason: same anti-pattern using DISTINCT instead of GROUP BY
JOIN rowcov_fk FOR KEY (fk_a, fk_b) -> sub (a, b);

DROP TABLE rowcov_fk, rowcov_pk;

-- GROUP BY/DISTINCT preserve row coverage only when their equality identity
-- exactly matches the key equality identity.
CREATE TABLE rowcov_exact_parent (id int PRIMARY KEY);
CREATE TABLE rowcov_exact_child (
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rowcov_exact_parent (id)
);
INSERT INTO rowcov_exact_parent VALUES (1), (2);
INSERT INTO rowcov_exact_child VALUES (10, 1), (20, 2);

SELECT p.id, c.id AS child_id
FROM (SELECT id FROM rowcov_exact_parent GROUP BY id) p
-- accepted
JOIN rowcov_exact_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

SELECT p.id, c.id AS child_id
FROM (SELECT DISTINCT id FROM rowcov_exact_parent) p
-- accepted
JOIN rowcov_exact_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

SELECT p.id, c.id AS child_id
FROM (SELECT id FROM rowcov_exact_parent GROUP BY ROLLUP(id)) p
-- rejected, reason: ROLLUP preserves row coverage but does not prove uniqueness
JOIN rowcov_exact_child c FOR KEY (parent_id) -> p (id);

SELECT p.id, c.id AS child_id
FROM (
    SELECT id
    FROM (SELECT id FROM rowcov_exact_parent GROUP BY ROLLUP(id)) g
    GROUP BY id
) p
-- accepted, reason: ROLLUP preserves row coverage and outer GROUP BY proves uniqueness
JOIN rowcov_exact_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

SELECT p.id, c.id AS child_id
FROM (SELECT DISTINCT id FROM rowcov_exact_parent GROUP BY ROLLUP(id)) p
-- accepted, reason: ROLLUP preserves row coverage and DISTINCT proves uniqueness
JOIN rowcov_exact_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

DROP TABLE rowcov_exact_child, rowcov_exact_parent;

CREATE TABLE rowcov_gsets_parent (
    a int NOT NULL,
    b int NOT NULL,
    PRIMARY KEY (a, b)
);
CREATE TABLE rowcov_gsets_child (
    id int PRIMARY KEY,
    fk_a int NOT NULL,
    fk_b int NOT NULL,
    FOREIGN KEY (fk_a, fk_b) REFERENCES rowcov_gsets_parent (a, b)
);
INSERT INTO rowcov_gsets_parent VALUES (1, 10), (1, 20), (2, 10);
INSERT INTO rowcov_gsets_child VALUES (10, 1, 10), (20, 1, 20);

SELECT p.a, p.b, c.id AS child_id
FROM (
    SELECT a, b
    FROM (
        SELECT a, b
        FROM rowcov_gsets_parent
        GROUP BY GROUPING SETS ((a, b), (a), ())
    ) g
    GROUP BY a, b
) p
-- accepted, reason: one grouping set contains the full composite key
JOIN rowcov_gsets_child c FOR KEY (fk_a, fk_b) -> p (a, b)
ORDER BY p.a, p.b, c.id;

SELECT p.a, p.b, c.id AS child_id
FROM (
    SELECT a, b
    FROM (
        SELECT a, b
        FROM rowcov_gsets_parent
        GROUP BY GROUPING SETS ((a), (b), ())
    ) g
    GROUP BY a, b
) p
-- rejected, reason: no grouping set contains the full composite key
JOIN rowcov_gsets_child c FOR KEY (fk_a, fk_b) -> p (a, b);

DROP TABLE rowcov_gsets_child, rowcov_gsets_parent;

-- GROUP BY/DISTINCT preserve row coverage for nullable referenced keys when
-- the collapsed key covers the same all-non-null values under the same key
-- identity.  Null-containing referenced rows are outside the referenced
-- multiset that key joins must cover.
CREATE TABLE rowcov_nullable_parent (
    id int PRIMARY KEY,
    code text UNIQUE
);
CREATE TABLE rowcov_nullable_child (
    id int PRIMARY KEY,
    parent_code text NOT NULL REFERENCES rowcov_nullable_parent (code)
);
INSERT INTO rowcov_nullable_parent VALUES
    (1, 'a'), (2, 'b'), (3, NULL), (4, NULL);
INSERT INTO rowcov_nullable_child VALUES (10, 'a'), (20, 'b');

SELECT p.code, c.id AS child_id
FROM (SELECT code FROM rowcov_nullable_parent GROUP BY code) p
-- accepted
JOIN rowcov_nullable_child c FOR KEY (parent_code) -> p (code)
ORDER BY p.code, c.id;

SELECT p.code, c.id AS child_id
FROM (SELECT DISTINCT code FROM rowcov_nullable_parent) p
-- accepted
JOIN rowcov_nullable_child c FOR KEY (parent_code) -> p (code)
ORDER BY p.code, c.id;

SELECT p.code, c.id AS child_id
FROM (SELECT DISTINCT code FROM rowcov_nullable_parent GROUP BY ROLLUP(code)) p
-- accepted, reason: subtotal NULLs do not invalidate all-non-null row coverage
JOIN rowcov_nullable_child c FOR KEY (parent_code) -> p (code)
ORDER BY p.code, c.id;

SELECT p.code, c.id AS child_id
FROM (SELECT code FROM rowcov_nullable_parent GROUP BY code HAVING count(*) > 0) p
-- rejected, reason: HAVING remains a row-removal barrier
JOIN rowcov_nullable_child c FOR KEY (parent_code) -> p (code);

DROP TABLE rowcov_nullable_child, rowcov_nullable_parent;

-- Extra row-collapse columns may have different identity from the key.
-- Row coverage is preserved as long as the grouped key column itself has
-- the exact key identity.
CREATE TABLE rowcov_identity_type_parent (
    label text NOT NULL,
    id int PRIMARY KEY
);
CREATE TABLE rowcov_identity_type_child (
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rowcov_identity_type_parent (id)
);
INSERT INTO rowcov_identity_type_parent VALUES ('a', 1), ('b', 2);
INSERT INTO rowcov_identity_type_child VALUES (10, 1), (20, 2);

SELECT p.id, c.id AS child_id
FROM (
    SELECT label, id
    FROM rowcov_identity_type_parent
    GROUP BY label, id
) p
-- accepted, reason: extra grouped column has different type
JOIN rowcov_identity_type_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

DROP TABLE rowcov_identity_type_child, rowcov_identity_type_parent;

CREATE TABLE rowcov_identity_typmod_parent (
    amount numeric(5,0) NOT NULL,
    id numeric(10,0) PRIMARY KEY
);
CREATE TABLE rowcov_identity_typmod_child (
    id int PRIMARY KEY,
    parent_id numeric(10,0) NOT NULL
        REFERENCES rowcov_identity_typmod_parent (id)
);
INSERT INTO rowcov_identity_typmod_parent VALUES (1, 10), (2, 20);
INSERT INTO rowcov_identity_typmod_child VALUES (10, 10), (20, 20);

SELECT p.id, c.id AS child_id
FROM (
    SELECT amount, id
    FROM rowcov_identity_typmod_parent
    GROUP BY amount, id
) p
-- accepted, reason: extra grouped column has different typmod
JOIN rowcov_identity_typmod_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

DROP TABLE rowcov_identity_typmod_child, rowcov_identity_typmod_parent;

CREATE TABLE rowcov_identity_collation_parent (
    label text COLLATE "POSIX" NOT NULL,
    id text COLLATE "C" PRIMARY KEY
);
CREATE TABLE rowcov_identity_collation_child (
    id int PRIMARY KEY,
    parent_id text COLLATE "C" NOT NULL
        REFERENCES rowcov_identity_collation_parent (id)
);
INSERT INTO rowcov_identity_collation_parent VALUES ('a', 'x'), ('b', 'y');
INSERT INTO rowcov_identity_collation_child VALUES (10, 'x'), (20, 'y');

SELECT p.id, c.id AS child_id
FROM (
    SELECT label, id
    FROM rowcov_identity_collation_parent
    GROUP BY label, id
) p
-- accepted, reason: extra grouped column has different collation
JOIN rowcov_identity_collation_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

DROP TABLE rowcov_identity_collation_child, rowcov_identity_collation_parent;

CREATE FUNCTION rowcov_numeric_scale_cmp(a numeric, b numeric) RETURNS int
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT CASE WHEN a < b THEN -1
               WHEN a > b THEN 1
               WHEN scale(a) < scale(b) THEN -1
               WHEN scale(a) > scale(b) THEN 1
               ELSE 0 END $$;

CREATE FUNCTION rowcov_numeric_scale_eq(a numeric, b numeric) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a = b AND scale(a) = scale(b) $$;

CREATE FUNCTION rowcov_numeric_scale_lt(a numeric, b numeric) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT rowcov_numeric_scale_cmp(a, b) < 0 $$;

CREATE OPERATOR =# (
    LEFTARG = numeric,
    RIGHTARG = numeric,
    FUNCTION = rowcov_numeric_scale_eq,
    COMMUTATOR = =#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR <# (
    LEFTARG = numeric,
    RIGHTARG = numeric,
    FUNCTION = rowcov_numeric_scale_lt
);

CREATE OPERATOR CLASS rowcov_numeric_scale_ops
FOR TYPE numeric USING btree AS
    OPERATOR 1 <# (numeric, numeric),
    OPERATOR 3 =# (numeric, numeric),
    FUNCTION 1 rowcov_numeric_scale_cmp(numeric, numeric);

CREATE TABLE rowcov_numeric_parent (id numeric NOT NULL);
CREATE UNIQUE INDEX rowcov_numeric_parent_id_idx
    ON rowcov_numeric_parent USING btree (id rowcov_numeric_scale_ops);
CREATE TABLE rowcov_numeric_child (
    id int PRIMARY KEY,
    parent_id numeric NOT NULL REFERENCES rowcov_numeric_parent (id)
);
INSERT INTO rowcov_numeric_parent VALUES (1.0), (1.00);
INSERT INTO rowcov_numeric_child VALUES (10, 1.00);

SELECT *
FROM (SELECT id FROM rowcov_numeric_parent GROUP BY id) p
-- rejected, reason: GROUP BY equality collapses keys distinct under FK equality
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);

SELECT *
FROM (
    SELECT id
    FROM (SELECT id FROM rowcov_numeric_parent GROUP BY ROLLUP(id)) g
    GROUP BY id
) p
-- rejected, reason: ROLLUP equality collapses keys distinct under FK equality
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);

SELECT *
FROM (SELECT DISTINCT id FROM rowcov_numeric_parent) p
-- rejected, reason: DISTINCT equality collapses keys distinct under FK equality
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);

SELECT *
FROM (
    SELECT DISTINCT ON (id) id
    FROM rowcov_numeric_parent
    ORDER BY id, id USING <#
) p
-- rejected, reason: duplicate DISTINCT ON ordering adds no key coverage
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);

CREATE VIEW rowcov_numeric_group_v AS
SELECT id FROM rowcov_numeric_parent GROUP BY id;
SELECT *
FROM rowcov_numeric_group_v p
-- rejected, reason: GROUP BY row coverage loss occurred inside the view
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);
DROP VIEW rowcov_numeric_group_v;

CREATE VIEW rowcov_numeric_distinct_v AS
SELECT DISTINCT id FROM rowcov_numeric_parent;
SELECT *
FROM rowcov_numeric_distinct_v p
-- rejected, reason: DISTINCT row coverage loss occurred inside the view
JOIN rowcov_numeric_child c FOR KEY (parent_id) -> p (id);
DROP VIEW rowcov_numeric_distinct_v;

DROP TABLE rowcov_numeric_child, rowcov_numeric_parent;
DROP OPERATOR CLASS rowcov_numeric_scale_ops USING btree;
DROP OPERATOR FAMILY rowcov_numeric_scale_ops USING btree;
DROP OPERATOR <# (numeric, numeric);
DROP OPERATOR =# (numeric, numeric);
DROP FUNCTION rowcov_numeric_scale_cmp(numeric, numeric);
DROP FUNCTION rowcov_numeric_scale_eq(numeric, numeric);
DROP FUNCTION rowcov_numeric_scale_lt(numeric, numeric);

CREATE SEQUENCE volatile_default_seq;
CREATE FUNCTION volatile_default_int(v int DEFAULT nextval('volatile_default_seq')::int)
RETURNS int
LANGUAGE sql STABLE AS $$ SELECT v $$;

CREATE VIEW volatile_target_parent_v AS
SELECT id, set_config('key_join.volatile', id::text, false) AS side_effect
FROM products_gbu;

SELECT *
FROM volatile_target_parent_v p
-- rejected, reason: volatile targetlist blocks surface row coverage
JOIN resupplies_gbu r FOR KEY (product_id) -> p (id);

DROP VIEW volatile_target_parent_v;

CREATE VIEW volatile_default_target_parent_v AS
SELECT id, volatile_default_int() AS side_effect
FROM products_gbu;

SELECT *
FROM volatile_default_target_parent_v p
-- rejected, reason: volatile default targetlist blocks surface row coverage
JOIN resupplies_gbu r FOR KEY (product_id) -> p (id);

DROP VIEW volatile_default_target_parent_v;

DROP FUNCTION volatile_default_int(int);
DROP SEQUENCE volatile_default_seq;

-- FILTER5a: referencing-side uniqueness must exactly match the FK key.
CREATE TABLE filter_unique_len_parent (id int PRIMARY KEY);
CREATE TABLE filter_unique_len_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES filter_unique_len_parent (id),
    tag int NOT NULL,
    UNIQUE (parent_id, tag)
);
INSERT INTO filter_unique_len_parent VALUES (1), (2);
INSERT INTO filter_unique_len_child VALUES (10, 1, 1), (20, 2, 1);

SELECT count(*) AS unique_len_rows
FROM filter_unique_len_parent p
JOIN filter_unique_len_child c FOR KEY (parent_id) -> p (id);

DROP TABLE filter_unique_len_child, filter_unique_len_parent;

CREATE TABLE filter_unique_identity_parent (code text COLLATE "C" PRIMARY KEY);
CREATE TABLE filter_unique_identity_child
(
    code text COLLATE "C" NOT NULL
        REFERENCES filter_unique_identity_parent (code),
    id int PRIMARY KEY
);
CREATE UNIQUE INDEX filter_unique_identity_child_code_idx
    ON filter_unique_identity_child (code COLLATE "POSIX");
INSERT INTO filter_unique_identity_parent VALUES ('a'), ('b');
INSERT INTO filter_unique_identity_child VALUES ('a', 1), ('b', 2);

SELECT count(*) AS unique_identity_rows
FROM filter_unique_identity_parent p
JOIN filter_unique_identity_child c FOR KEY (code) -> p (code);

DROP TABLE filter_unique_identity_child, filter_unique_identity_parent;

-- Referenced-side FK facts survive outer null-extension as nullable FK facts.
CREATE TABLE output_fk_null_ext_gp (id int PRIMARY KEY);
CREATE TABLE output_fk_null_ext_parent
(
    id int PRIMARY KEY,
    gp_id int NOT NULL REFERENCES output_fk_null_ext_gp (id)
);
CREATE TABLE output_fk_null_ext_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES output_fk_null_ext_parent (id)
);
INSERT INTO output_fk_null_ext_gp VALUES (1), (2);
INSERT INTO output_fk_null_ext_parent VALUES (10, 1), (20, 2);
INSERT INTO output_fk_null_ext_child VALUES (100, 10);

-- Control: a non-null-extending referenced side already exported the FK fact.
SELECT q.parent_id, q.gp_id, q.child_id, g.id AS gp_match
FROM (
    SELECT p.id AS parent_id, p.gp_id, c.id AS child_id
    FROM output_fk_null_ext_parent p
    LEFT JOIN output_fk_null_ext_child c FOR KEY (parent_id) -> p (id)
) q
LEFT JOIN output_fk_null_ext_gp g FOR KEY (id) <- q (gp_id)
ORDER BY q.parent_id, q.child_id, g.id;

-- A FULL JOIN can null-extend the referenced side, but non-null FK values
-- projected from that side still have FK-backed containment.
SELECT q.parent_id, q.gp_id, q.child_id, g.id AS gp_match
FROM (
    SELECT p.id AS parent_id, p.gp_id, c.id AS child_id
    FROM output_fk_null_ext_parent p
    FULL JOIN output_fk_null_ext_child c FOR KEY (parent_id) -> p (id)
) q
LEFT JOIN output_fk_null_ext_gp g FOR KEY (id) <- q (gp_id)
ORDER BY q.parent_id, q.child_id, g.id;

-- The FK fact survived, but not the not-null fact, so an inner key join
-- through the same null-extended surface is still rejected.
SELECT q.parent_id, q.gp_id, q.child_id, g.id AS gp_match
FROM (
    SELECT p.id AS parent_id, p.gp_id, c.id AS child_id
    FROM output_fk_null_ext_parent p
    FULL JOIN output_fk_null_ext_child c FOR KEY (parent_id) -> p (id)
) q
JOIN output_fk_null_ext_gp g FOR KEY (id) <- q (gp_id);

DROP TABLE output_fk_null_ext_child,
           output_fk_null_ext_parent,
           output_fk_null_ext_gp;

-- Stronger case: the FULL JOIN can actually produce a row whose referenced
-- side is null-extended.  The later preserved-side key join remains valid
-- because nullable FK values are allowed when the referencing side is preserved.
CREATE TABLE output_fk_nullable_gp (id int PRIMARY KEY);
CREATE TABLE output_fk_nullable_parent
(
    id int PRIMARY KEY,
    gp_id int NOT NULL REFERENCES output_fk_nullable_gp (id)
);
CREATE TABLE output_fk_nullable_child
(
    id int PRIMARY KEY,
    parent_id int REFERENCES output_fk_nullable_parent (id)
);
INSERT INTO output_fk_nullable_gp VALUES (1), (2);
INSERT INTO output_fk_nullable_parent VALUES (10, 1), (20, 2);
INSERT INTO output_fk_nullable_child VALUES (100, 10), (200, NULL);

SELECT q.parent_id, q.gp_id, q.child_id, g.id AS gp_match
FROM (
    SELECT p.id AS parent_id, p.gp_id, c.id AS child_id
    FROM output_fk_nullable_parent p
    FULL JOIN output_fk_nullable_child c FOR KEY (parent_id) -> p (id)
) q
LEFT JOIN output_fk_nullable_gp g FOR KEY (id) <- q (gp_id)
ORDER BY q.parent_id NULLS LAST, q.child_id, g.id;

DROP TABLE output_fk_nullable_child,
           output_fk_nullable_parent,
           output_fk_nullable_gp;

-- Query-level uniqueness on the referenced side must not be paired with row
-- coverage from an unrelated relation that happens to expose the same column
-- number.
CREATE TABLE key_join_wrong_coverage_parent
(
    id int PRIMARY KEY
);
CREATE TABLE key_join_wrong_coverage_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_wrong_coverage_parent (id)
);
CREATE TABLE key_join_wrong_coverage_other
(
    id int PRIMARY KEY
);
INSERT INTO key_join_wrong_coverage_parent VALUES (1), (2);
INSERT INTO key_join_wrong_coverage_child VALUES (10, 2);
INSERT INTO key_join_wrong_coverage_other VALUES (1);

SELECT c.id, c.parent_id, o.id AS other_id
FROM key_join_wrong_coverage_child c
-- rejected, reason: referenced operand does not cover parent_id = 2
JOIN (SELECT id FROM key_join_wrong_coverage_other GROUP BY id) o
    FOR KEY (id) <- c (parent_id);

DROP TABLE key_join_wrong_coverage_child,
           key_join_wrong_coverage_other,
           key_join_wrong_coverage_parent;

-- Referenced-side uniqueness and row coverage must use the same key identity
-- as the FK proof.  When an older alternate unique index appears before the
-- primary-key index, proof selection must skip it and continue to the primary
-- key instead of asserting.
CREATE FUNCTION key_join_refidx_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a = b $$;
CREATE OPERATOR =# (
    LEFTARG = int,
    RIGHTARG = int,
    FUNCTION = key_join_refidx_eq,
    COMMUTATOR = =#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);
CREATE OPERATOR CLASS key_join_refidx_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 3 =# (int, int),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE key_join_refidx_parent
(
    id int NOT NULL
);
CREATE UNIQUE INDEX key_join_refidx_parent_alt_idx
    ON key_join_refidx_parent USING btree (id key_join_refidx_int4_ops);
ALTER TABLE key_join_refidx_parent ADD PRIMARY KEY (id);
CREATE TABLE key_join_refidx_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_refidx_parent
);
INSERT INTO key_join_refidx_parent VALUES (1), (2);
INSERT INTO key_join_refidx_child VALUES (10, 1), (20, 2);

SELECT c.id AS child_id, p.id AS parent_id
FROM key_join_refidx_parent p
JOIN key_join_refidx_child c FOR KEY (parent_id) -> p (id)
ORDER BY c.id;

DROP TABLE key_join_refidx_child,
           key_join_refidx_parent;
DROP OPERATOR CLASS key_join_refidx_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_refidx_int4_ops USING btree;
DROP OPERATOR =# (int, int);
DROP FUNCTION key_join_refidx_eq(int, int);

-- A duplicate-column unique index that appears before the real FK target
-- index must be skipped, not asserted over while matching selected columns.
CREATE TABLE key_join_dupidx_parent
(
    id int NOT NULL,
    other_id int NOT NULL
);
CREATE UNIQUE INDEX key_join_dupidx_parent_dup_idx
    ON key_join_dupidx_parent (id, id);
CREATE UNIQUE INDEX key_join_dupidx_parent_real_idx
    ON key_join_dupidx_parent (id, other_id);
CREATE TABLE key_join_dupidx_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    parent_other_id int NOT NULL,
    FOREIGN KEY (parent_id, parent_other_id)
        REFERENCES key_join_dupidx_parent (id, other_id)
);
INSERT INTO key_join_dupidx_parent VALUES (1, 10), (2, 20);
INSERT INTO key_join_dupidx_child VALUES (101, 1, 10), (202, 2, 20);

SELECT c.id AS child_id, p.id AS parent_id, p.other_id
FROM key_join_dupidx_parent p
JOIN key_join_dupidx_child c
    FOR KEY (parent_id, parent_other_id) -> p (id, other_id)
ORDER BY c.id;

DROP TABLE key_join_dupidx_child,
           key_join_dupidx_parent;

-- Referencing-side uniqueness cannot be projected through a key join unless it
-- uses the same key-position identity as the FK proof.
CREATE FUNCTION key_join_refuniq_bucket_cmp(a int, b int) RETURNS int
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT CASE WHEN a / 10 < b / 10 THEN -1
               WHEN a / 10 > b / 10 THEN 1
               ELSE 0 END $$;
CREATE FUNCTION key_join_refuniq_bucket_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a / 10 = b / 10 $$;
CREATE OPERATOR =# (
    LEFTARG = int,
    RIGHTARG = int,
    FUNCTION = key_join_refuniq_bucket_eq,
    COMMUTATOR = =#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);
CREATE OPERATOR CLASS key_join_refuniq_bucket_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 3 =# (int, int),
    FUNCTION 1 key_join_refuniq_bucket_cmp(int, int);

CREATE TABLE key_join_refuniq_parent
(
    id int NOT NULL
);
CREATE UNIQUE INDEX key_join_refuniq_parent_idx
    ON key_join_refuniq_parent USING btree (id key_join_refuniq_bucket_int4_ops);
CREATE TABLE key_join_refuniq_child
(
    id int PRIMARY KEY,
    parent_id int UNIQUE NOT NULL REFERENCES key_join_refuniq_parent (id)
);
CREATE TABLE key_join_refuniq_grandchild
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_refuniq_parent (id)
);
INSERT INTO key_join_refuniq_parent VALUES (10), (20);
INSERT INTO key_join_refuniq_child VALUES (1, 10), (2, 11), (3, 20);
INSERT INTO key_join_refuniq_grandchild VALUES (101, 10), (102, 20);

SELECT p.id AS parent_key, c.id AS child_id, c.parent_id
FROM key_join_refuniq_parent p
LEFT JOIN key_join_refuniq_child c FOR KEY (parent_id) -> p (id)
ORDER BY 1, 2;

SELECT q.parent_key, q.child_id, g.id AS grandchild_id
FROM (
    SELECT p.id AS parent_key, c.id AS child_id, c.parent_id
    FROM key_join_refuniq_parent p
    LEFT JOIN key_join_refuniq_child c FOR KEY (parent_id) -> p (id)
) q
JOIN key_join_refuniq_grandchild g FOR KEY (parent_id) -> q (parent_key)
ORDER BY q.parent_key, q.child_id, g.id;

DROP TABLE key_join_refuniq_grandchild,
           key_join_refuniq_child,
           key_join_refuniq_parent;
DROP OPERATOR CLASS key_join_refuniq_bucket_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_refuniq_bucket_int4_ops USING btree;
DROP OPERATOR =# (int, int);
DROP FUNCTION key_join_refuniq_bucket_cmp(int, int);
DROP FUNCTION key_join_refuniq_bucket_eq(int, int);

-- Set-returning equality operator functions are not usable proof operators.
CREATE FUNCTION key_join_srf_eq(a int, b int) RETURNS SETOF boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a = b $$;
CREATE FUNCTION key_join_srf_cmp(a int, b int) RETURNS int
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT btint4cmp(a, b) $$;
CREATE OPERATOR =# (
    LEFTARG = int,
    RIGHTARG = int,
    FUNCTION = key_join_srf_eq,
    COMMUTATOR = =#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);
CREATE OPERATOR CLASS key_join_srf_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 1 < (int, int),
    OPERATOR 2 <= (int, int),
    OPERATOR 3 =# (int, int),
    OPERATOR 4 >= (int, int),
    OPERATOR 5 > (int, int),
    FUNCTION 1 key_join_srf_cmp(int, int);

CREATE TABLE key_join_srf_parent
(
    id int NOT NULL
);
CREATE UNIQUE INDEX key_join_srf_parent_idx
    ON key_join_srf_parent USING btree (id key_join_srf_int4_ops);
CREATE TABLE key_join_srf_child
(
    parent_id int NOT NULL REFERENCES key_join_srf_parent (id)
);

SELECT *
FROM key_join_srf_parent p
JOIN key_join_srf_child c FOR KEY (parent_id) -> p (id);

DROP TABLE key_join_srf_child,
           key_join_srf_parent;
DROP OPERATOR CLASS key_join_srf_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_srf_int4_ops USING btree;
DROP OPERATOR =# (int, int);
DROP FUNCTION key_join_srf_eq(int, int);
DROP FUNCTION key_join_srf_cmp(int, int);

-- Generated key-join quals must require SELECT on both key columns.
CREATE ROLE regress_key_join_priv_user;
CREATE SCHEMA key_join_priv;
CREATE TABLE key_join_priv.parent
(
    id int PRIMARY KEY,
    visible text
);
CREATE TABLE key_join_priv.child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_priv.parent (id),
    visible text
);
INSERT INTO key_join_priv.parent VALUES (1, 'parent-one');
INSERT INTO key_join_priv.child VALUES (10, 1, 'child-one');
GRANT USAGE ON SCHEMA key_join_priv TO regress_key_join_priv_user;
GRANT SELECT (visible) ON key_join_priv.parent TO regress_key_join_priv_user;
GRANT SELECT (visible) ON key_join_priv.child TO regress_key_join_priv_user;

SET ROLE regress_key_join_priv_user;
SELECT p.visible AS parent_visible, c.visible AS child_visible
FROM key_join_priv.parent p
JOIN key_join_priv.child c FOR KEY (parent_id) -> p (id);
RESET ROLE;

GRANT SELECT (id) ON key_join_priv.parent TO regress_key_join_priv_user;
SET ROLE regress_key_join_priv_user;
SELECT p.visible AS parent_visible, c.visible AS child_visible
FROM key_join_priv.parent p
JOIN key_join_priv.child c FOR KEY (parent_id) -> p (id);
RESET ROLE;

GRANT SELECT (parent_id) ON key_join_priv.child TO regress_key_join_priv_user;
SET ROLE regress_key_join_priv_user;
SELECT p.visible AS parent_visible, c.visible AS child_visible
FROM key_join_priv.parent p
JOIN key_join_priv.child c FOR KEY (parent_id) -> p (id);
RESET ROLE;

DROP SCHEMA key_join_priv CASCADE;
DROP ROLE regress_key_join_priv_user;

-- Key-join proof metadata must not affect query identifiers.  The same
-- executable query should keep its queryId if an equivalent FK proof is
-- dropped and recreated with new catalog OIDs.
CREATE FUNCTION pg_temp.key_join_query_id(query_text text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    line text;
BEGIN
    FOR line IN EXECUTE 'EXPLAIN (VERBOSE, FORMAT TEXT) ' || query_text
    LOOP
        IF line ~ 'Query Identifier:' THEN
            RETURN regexp_replace(line, '.*Query Identifier:\s*(-?\d+).*', '\1')::bigint;
        END IF;
    END LOOP;
    RAISE EXCEPTION 'Query Identifier not found in EXPLAIN output';
END;
$$;

SET compute_query_id = on;
CREATE TABLE key_join_queryid_parent
(
    id int PRIMARY KEY
);
CREATE TABLE key_join_queryid_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_queryid_parent (id)
);

SELECT pg_temp.key_join_query_id($$
SELECT *
FROM key_join_queryid_parent p
JOIN key_join_queryid_child c FOR KEY (parent_id) -> p (id);
$$) AS qid \gset

ALTER TABLE key_join_queryid_child DROP CONSTRAINT key_join_queryid_child_parent_id_fkey;
ALTER TABLE key_join_queryid_child
    ADD CONSTRAINT key_join_queryid_child_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES key_join_queryid_parent (id);

SELECT pg_temp.key_join_query_id($$
SELECT *
FROM key_join_queryid_parent p
JOIN key_join_queryid_child c FOR KEY (parent_id) -> p (id);
$$) = :'qid'::bigint AS same_query_id_after_fk_recreate;

RESET compute_query_id;
DROP TABLE key_join_queryid_child, key_join_queryid_parent;

-- ============================================================================
-- Precise diagnostics for unprovable key joins: the error states which proof
-- requirement is missing, and when the evidence existed but was discarded
-- mid-query, why (and where, when locatable).
-- ============================================================================
CREATE SCHEMA key_join_diag;
SET search_path = key_join_diag;
CREATE TABLE kd_parent (id int PRIMARY KEY);
CREATE TABLE kd_child (id int, parent_id int REFERENCES kd_parent (id));
CREATE TABLE kd_fanout_child
(
    id int,
    parent_id int NOT NULL REFERENCES kd_parent (id)
);
CREATE TABLE kd_unique_child
(
    id int,
    parent_id int UNIQUE NOT NULL REFERENCES kd_parent (id)
);
CREATE TABLE kd_customer (id int PRIMARY KEY);
CREATE TABLE kd_customs (id int PRIMARY KEY);
CREATE TABLE kd_sale_order
(
    id int PRIMARY KEY,
    cid int NOT NULL REFERENCES kd_customer (id),
    cuid int NOT NULL REFERENCES kd_customs (id)
);

-- Missing: no foreign key matches the referencing columns (id is not an FK).
SELECT * FROM kd_parent p JOIN kd_child c FOR KEY (id) -> p (id);

-- Missing: referencing columns point at a different referenced relation.
SELECT *
FROM kd_sale_order o
JOIN kd_customer c FOR KEY (id) <- o (cuid);

-- Missing through a derived referenced surface, using provenance not base RTEs.
SELECT *
FROM kd_sale_order o
JOIN (SELECT id FROM kd_customer) c FOR KEY (id) <- o (cuid);

-- Accepted: the corrected FK target still succeeds.
SELECT o.id AS order_id, c.id AS customer_id
FROM kd_sale_order o
JOIN kd_customer c FOR KEY (id) <- o (cid);

DROP TABLE kd_sale_order;
DROP TABLE kd_customs, kd_customer;

-- Inactivated: a derived referenced surface drops its row coverage at LIMIT.
SELECT *
FROM (SELECT id FROM kd_parent LIMIT 1) p
JOIN kd_child c FOR KEY (parent_id) -> p (id);

-- Inactivated by HAVING: the row-removing clause is named.
SELECT *
FROM (SELECT id FROM kd_parent GROUP BY id HAVING count(*) > 0) p
JOIN kd_child c FOR KEY (parent_id) -> p (id);

-- Inactivated uniqueness: the referenced surface may fan out.
SELECT *
FROM (
    SELECT p.id
    FROM kd_parent p
    LEFT JOIN kd_fanout_child f FOR KEY (parent_id) -> p (id)
) q
JOIN kd_child c FOR KEY (parent_id) -> q (id);

-- Inactivated row coverage: the referenced surface may lose rows.
SELECT *
FROM (
    SELECT p.id
    FROM kd_parent p
    JOIN kd_unique_child u FOR KEY (parent_id) -> p (id)
) q
JOIN kd_child c FOR KEY (parent_id) -> q (id);

-- Inactivated inside a view: the message names the view to inspect.
CREATE VIEW kd_parent_limited AS SELECT id FROM kd_parent LIMIT 1;
SELECT *
FROM kd_parent_limited p
JOIN kd_child c FOR KEY (parent_id) -> p (id);

DROP VIEW kd_parent_limited;
RESET search_path;
DROP SCHEMA key_join_diag CASCADE;

-- ============================================================================
-- Matched referenced-side filters: key joins whose referenced input is
-- filtered but provable because the referencing side applies the same
-- canonical filter, and canonicalization of the proof-filter terms.
-- ============================================================================
CREATE TABLE scalar_array_filter_parent (id boolean PRIMARY KEY);
CREATE TABLE scalar_array_filter_child
(
    id int PRIMARY KEY,
    parent_id boolean NOT NULL REFERENCES scalar_array_filter_parent (id)
);
INSERT INTO scalar_array_filter_parent VALUES (false), (true);
INSERT INTO scalar_array_filter_child VALUES (1, true), (2, false);

CREATE VIEW scalar_array_filter_parent_v AS
SELECT id
FROM scalar_array_filter_parent
WHERE id = (true = ANY (ARRAY[false, length('x') = 1]));

CREATE VIEW scalar_array_filter_child_v AS
SELECT parent_id
FROM scalar_array_filter_child
WHERE parent_id = (true = ANY (ARRAY[false, length('x') = 1]));

SELECT count(*) AS scalar_array_filter_rows
FROM scalar_array_filter_parent_v p
-- rejected, reason: ScalarArrayOpExpr filter values are not proof-filter terms
JOIN scalar_array_filter_child_v c FOR KEY (parent_id) -> p (id);

DROP VIEW scalar_array_filter_child_v, scalar_array_filter_parent_v;
DROP TABLE scalar_array_filter_child, scalar_array_filter_parent;

-- FILTER1b: strict proof-filter value allowlist accepts constants
CREATE TABLE filter_const_parent (id int PRIMARY KEY);
CREATE TABLE filter_const_child
(
    parent_id int NOT NULL REFERENCES filter_const_parent (id)
);
INSERT INTO filter_const_parent VALUES (1), (2);
INSERT INTO filter_const_child VALUES (1);
CREATE VIEW filter_const_parent_v AS
SELECT id FROM filter_const_parent WHERE id = 1::integer;
CREATE VIEW filter_const_child_v AS
SELECT parent_id FROM filter_const_child WHERE parent_id = 1::integer;
SELECT count(*) AS const_filter_rows
FROM filter_const_parent_v p
JOIN filter_const_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_const_child_v, filter_const_parent_v;
DROP TABLE filter_const_child, filter_const_parent;

-- FILTER1c: strict proof-filter value allowlist accepts SQL value functions
CREATE TABLE filter_sqlvalue_parent (id name PRIMARY KEY);
CREATE TABLE filter_sqlvalue_child
(
    parent_id name NOT NULL REFERENCES filter_sqlvalue_parent (id)
);
INSERT INTO filter_sqlvalue_parent VALUES (CURRENT_USER);
INSERT INTO filter_sqlvalue_child VALUES (CURRENT_USER);
CREATE VIEW filter_sqlvalue_parent_v AS
SELECT id FROM filter_sqlvalue_parent WHERE id = CURRENT_USER;
CREATE VIEW filter_sqlvalue_child_v AS
SELECT parent_id FROM filter_sqlvalue_child WHERE parent_id = CURRENT_USER;
SELECT count(*) AS sqlvalue_filter_rows
FROM filter_sqlvalue_parent_v p
JOIN filter_sqlvalue_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_sqlvalue_child_v, filter_sqlvalue_parent_v;
DROP TABLE filter_sqlvalue_child, filter_sqlvalue_parent;

-- FILTER1c-2: SQL value function filters on varchar keys use text = name
CREATE TABLE filter_varchar_tenant_customers
(
    tenant_id varchar(128),
    id int,
    name varchar(100),
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE filter_varchar_tenant_orders
(
    tenant_id varchar(128),
    id int,
    customer_id int NOT NULL,
    amount numeric(10,2),
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, customer_id)
        REFERENCES filter_varchar_tenant_customers (tenant_id, id)
);
INSERT INTO filter_varchar_tenant_customers
VALUES (CURRENT_USER, 1, 'Alice'), ('other_tenant', 1, 'Other');
INSERT INTO filter_varchar_tenant_orders
VALUES (CURRENT_USER, 10, 1, 5.00), ('other_tenant', 20, 1, 7.00);
CREATE VIEW filter_varchar_tenant_customers_v AS
SELECT * FROM filter_varchar_tenant_customers
WHERE tenant_id = CURRENT_USER;
CREATE VIEW filter_varchar_tenant_orders_v AS
SELECT * FROM filter_varchar_tenant_orders
WHERE tenant_id = CURRENT_USER;
SELECT o.id, c.name
FROM filter_varchar_tenant_orders_v o
JOIN filter_varchar_tenant_customers_v c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id)
ORDER BY o.id, c.name;
SELECT count(*) AS varchar_sqlvalue_filter_unmatched_rows
FROM filter_varchar_tenant_orders o
-- rejected, reason: the referenced-side CURRENT_USER filter is not matched on the FK side
JOIN filter_varchar_tenant_customers_v c FOR KEY (tenant_id, id) <- o (tenant_id, customer_id);
DROP VIEW filter_varchar_tenant_orders_v, filter_varchar_tenant_customers_v;
DROP TABLE filter_varchar_tenant_orders, filter_varchar_tenant_customers;

-- FILTER1d: strict proof-filter value allowlist accepts relabel casts
CREATE TABLE filter_relabel_parent (id text PRIMARY KEY);
CREATE TABLE filter_relabel_child
(
    parent_id text NOT NULL REFERENCES filter_relabel_parent (id)
);
INSERT INTO filter_relabel_parent VALUES ('a'), ('b');
INSERT INTO filter_relabel_child VALUES ('a');
CREATE VIEW filter_relabel_parent_v AS
SELECT id FROM filter_relabel_parent
WHERE id = ('a'::varchar)::text;
CREATE VIEW filter_relabel_child_v AS
SELECT parent_id FROM filter_relabel_child
WHERE parent_id = ('a'::varchar)::text;
SELECT count(*) AS relabel_filter_rows
FROM filter_relabel_parent_v p
JOIN filter_relabel_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_relabel_child_v, filter_relabel_parent_v;
DROP TABLE filter_relabel_child, filter_relabel_parent;

-- FILTER1e: strict proof-filter value allowlist accepts stable functions
CREATE FUNCTION filter_stable_identity(v int) RETURNS int
LANGUAGE sql STABLE AS $$ SELECT v $$;
CREATE TABLE filter_func_parent (id int PRIMARY KEY);
CREATE TABLE filter_func_child
(
    parent_id int NOT NULL REFERENCES filter_func_parent (id)
);
INSERT INTO filter_func_parent VALUES (1), (2);
INSERT INTO filter_func_child VALUES (1);
CREATE VIEW filter_func_parent_v AS
SELECT id FROM filter_func_parent WHERE id = filter_stable_identity(1);
CREATE VIEW filter_func_child_v AS
SELECT parent_id FROM filter_func_child WHERE parent_id = filter_stable_identity(1);
SELECT count(*) AS func_filter_rows
FROM filter_func_parent_v p
JOIN filter_func_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_func_child_v, filter_func_parent_v;

CREATE VIEW filter_func_arg_parent_v AS
SELECT id FROM filter_func_parent
WHERE id = filter_stable_identity(LEAST(1, 2));
CREATE VIEW filter_func_arg_child_v AS
SELECT parent_id FROM filter_func_child
WHERE parent_id = filter_stable_identity(LEAST(1, 2));
SELECT count(*) AS func_arg_filter_rows
FROM filter_func_arg_parent_v p
-- rejected, reason: function arguments must be proof-filter terms
JOIN filter_func_arg_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_func_arg_child_v, filter_func_arg_parent_v;

DROP TABLE filter_func_child, filter_func_parent;
DROP FUNCTION filter_stable_identity(int);

-- FILTER1f: strict proof-filter value allowlist rejects volatile functions
CREATE FUNCTION filter_volatile_identity(v int) RETURNS int
LANGUAGE sql VOLATILE AS $$ SELECT v $$;
CREATE TABLE filter_volatile_parent (id int PRIMARY KEY);
CREATE TABLE filter_volatile_child
(
    parent_id int NOT NULL REFERENCES filter_volatile_parent (id)
);
INSERT INTO filter_volatile_parent VALUES (1), (2);
INSERT INTO filter_volatile_child VALUES (1);
CREATE VIEW filter_volatile_parent_v AS
SELECT id FROM filter_volatile_parent WHERE id = filter_volatile_identity(1);
CREATE VIEW filter_volatile_child_v AS
SELECT parent_id FROM filter_volatile_child WHERE parent_id = filter_volatile_identity(1);
SELECT count(*) AS volatile_filter_rows
FROM filter_volatile_parent_v p
-- rejected, reason: volatile function filter values are not proof-filter terms
JOIN filter_volatile_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_volatile_child_v, filter_volatile_parent_v;
DROP TABLE filter_volatile_child, filter_volatile_parent;
DROP FUNCTION filter_volatile_identity(int);

-- FILTER1f2: strict proof-filter allowlist rejects volatile defaults
CREATE SEQUENCE filter_volatile_default_seq;
CREATE FUNCTION filter_volatile_default_identity(v int DEFAULT nextval('filter_volatile_default_seq')::int)
RETURNS int
LANGUAGE sql STABLE AS $$ SELECT v $$;
CREATE TABLE filter_volatile_default_parent (id int PRIMARY KEY);
CREATE TABLE filter_volatile_default_child
(
    parent_id int NOT NULL REFERENCES filter_volatile_default_parent (id)
);
INSERT INTO filter_volatile_default_parent VALUES (1), (2);
INSERT INTO filter_volatile_default_child VALUES (1);
CREATE VIEW filter_volatile_default_parent_v AS
SELECT id FROM filter_volatile_default_parent
WHERE id = filter_volatile_default_identity();
CREATE VIEW filter_volatile_default_child_v AS
SELECT parent_id FROM filter_volatile_default_child
WHERE parent_id = filter_volatile_default_identity();
SELECT count(*) AS volatile_default_filter_rows
FROM filter_volatile_default_parent_v p
-- rejected, reason: volatile default function filter values are not proof terms
JOIN filter_volatile_default_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_volatile_default_child_v, filter_volatile_default_parent_v;
DROP TABLE filter_volatile_default_child, filter_volatile_default_parent;
DROP FUNCTION filter_volatile_default_identity(int);
DROP SEQUENCE filter_volatile_default_seq;

-- FILTER1g: strict proof-filter value allowlist rejects volatile CoerceViaIO
-- input/output functions before row-coverage propagation.
SET client_min_messages = warning;
CREATE TYPE filter_coerce_io_type;
CREATE FUNCTION filter_coerce_io_type_in(cstring)
RETURNS filter_coerce_io_type
LANGUAGE internal STRICT IMMUTABLE AS 'int4in';
CREATE FUNCTION filter_coerce_io_type_out(filter_coerce_io_type)
RETURNS cstring
LANGUAGE internal STRICT IMMUTABLE AS 'int4out';
CREATE TYPE filter_coerce_io_type (
    input = filter_coerce_io_type_in,
    output = filter_coerce_io_type_out,
    like = int4
);
RESET client_min_messages;
CREATE CAST (filter_coerce_io_type AS integer) WITH INOUT;
CREATE TABLE filter_coerce_io_parent (id int PRIMARY KEY);
CREATE TABLE filter_coerce_io_child
(
    id int NOT NULL UNIQUE REFERENCES filter_coerce_io_parent (id)
);
CREATE TABLE filter_coerce_io_grandchild
(
    id int NOT NULL REFERENCES filter_coerce_io_child (id)
);
INSERT INTO filter_coerce_io_parent VALUES (1), (2);
INSERT INTO filter_coerce_io_child VALUES (1), (2);
INSERT INTO filter_coerce_io_grandchild VALUES (1), (1), (2);
ALTER FUNCTION filter_coerce_io_type_out(filter_coerce_io_type) VOLATILE;
SELECT count(*) AS volatile_coerceviaio_filter_rows
FROM (
    filter_coerce_io_parent p
    JOIN filter_coerce_io_child c FOR KEY (id) -> p (id)
    FILTER (WHERE c.id = ('1'::filter_coerce_io_type)::integer)
)
-- rejected, reason: CoerceViaIO output function is volatile
JOIN filter_coerce_io_grandchild g FOR KEY (id) -> c (id);
DROP TABLE filter_coerce_io_grandchild, filter_coerce_io_child,
           filter_coerce_io_parent;
DROP CAST (filter_coerce_io_type AS integer);
SET client_min_messages = warning;
DROP TYPE filter_coerce_io_type CASCADE;
RESET client_min_messages;

-- ORDER BY collation relabels do not change projected key identity.
CREATE COLLATION relabel_key_join_c_copy FROM "C";
CREATE TABLE relabel_collation_parent (id text COLLATE "C" PRIMARY KEY);
CREATE TABLE relabel_collation_child
(
    id text COLLATE "C" NOT NULL REFERENCES relabel_collation_parent (id)
);
INSERT INTO relabel_collation_parent VALUES ('a');
INSERT INTO relabel_collation_child VALUES ('a');

SELECT *
FROM relabel_collation_parent p
JOIN (
    SELECT id
    FROM relabel_collation_child
    ORDER BY id COLLATE relabel_key_join_c_copy
) q FOR KEY (id) -> p (id);

DROP TABLE relabel_collation_child, relabel_collation_parent;
DROP COLLATION relabel_key_join_c_copy;

-- FILTER5: a filtered key-join result must not export unfiltered rowCoverage
CREATE TABLE filter_parent (id int PRIMARY KEY);
CREATE TABLE filter_bridge
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES filter_parent (id),
    keep boolean NOT NULL
);
CREATE TABLE filter_detail
(
    id int PRIMARY KEY,
    bridge_id int NOT NULL REFERENCES filter_bridge (id)
);

INSERT INTO filter_parent VALUES (1), (2);
INSERT INTO filter_bridge VALUES (10, 1, true), (20, 2, false);
INSERT INTO filter_detail VALUES (100, 10), (200, 20);

SELECT d.id AS detail_id, q.bridge_id
FROM filter_detail d
JOIN (
    SELECT b.id AS bridge_id
    FROM filter_parent p
    JOIN filter_bridge b FOR KEY (parent_id) -> p (id)
      FILTER (WHERE b.keep)
) q FOR KEY (bridge_id) <- d (bridge_id);

SELECT d.id AS detail_id, q.bridge_id
FROM filter_detail d
JOIN (
    SELECT b.id AS bridge_id
    FROM filter_parent p
    JOIN filter_bridge b FOR KEY (parent_id) -> p (id)
      FILTER (WHERE b.id = 10)
) q FOR KEY (bridge_id) <- d (bridge_id);

DROP TABLE filter_detail, filter_bridge, filter_parent;

CREATE TABLE filter_sibling_fk_grand (id int PRIMARY KEY);
CREATE TABLE filter_sibling_fk_parent
(
    id int PRIMARY KEY,
    grand_id int NOT NULL REFERENCES filter_sibling_fk_grand (id)
);
CREATE TABLE filter_sibling_fk_child
(
    parent_id int NOT NULL UNIQUE REFERENCES filter_sibling_fk_parent (id),
    payload int PRIMARY KEY
);
INSERT INTO filter_sibling_fk_grand VALUES (1), (2);
INSERT INTO filter_sibling_fk_parent VALUES (1, 1), (2, 2);
INSERT INTO filter_sibling_fk_child VALUES (1, 10), (2, 20);

SELECT count(*) AS sibling_fk_rows
FROM (
    SELECT c1.parent_id AS c1_parent, c2.parent_id AS c2_parent
    FROM filter_sibling_fk_parent p
    LEFT JOIN filter_sibling_fk_child c1
        FOR KEY (parent_id) -> p (id)
    LEFT JOIN filter_sibling_fk_child c2
        FOR KEY (parent_id) -> p (id)
) q
LEFT JOIN filter_sibling_fk_parent p2
    FOR KEY (id) <- q (c1_parent)
LEFT JOIN filter_sibling_fk_parent p3
    FOR KEY (id) <- q (c2_parent);

SELECT count(*) AS sibling_filtered_fk_rows
FROM (
    SELECT c1.parent_id AS c1_parent, c2.parent_id AS c2_parent
    FROM filter_sibling_fk_parent p
    LEFT JOIN filter_sibling_fk_child c1
        FOR KEY (parent_id) -> p (id)
    LEFT JOIN filter_sibling_fk_child c2
        FOR KEY (parent_id) -> p (id)
        FILTER (WHERE c2.parent_id = 1)
) q
LEFT JOIN filter_sibling_fk_parent p2
    FOR KEY (id) <- q (c1_parent)
LEFT JOIN filter_sibling_fk_parent p3
    FOR KEY (id) <- q (c2_parent);

SELECT count(*) AS sibling_unrelated_target_rows
FROM (
    SELECT c1.parent_id AS c1_parent, c2.parent_id AS c2_parent
    FROM filter_sibling_fk_parent p
    LEFT JOIN filter_sibling_fk_child c1
        FOR KEY (parent_id) -> p (id)
    LEFT JOIN filter_sibling_fk_child c2
        FOR KEY (parent_id) -> p (id)
        FILTER (WHERE c2.parent_id = 1)
) q
LEFT JOIN filter_sibling_fk_parent p2
    FOR KEY (id) <- q (c1_parent)
LEFT JOIN filter_sibling_fk_parent p3
    FOR KEY (id) <- q (c2_parent)
LEFT JOIN filter_sibling_fk_grand g
    FOR KEY (id) <- p3 (grand_id);

DROP TABLE filter_sibling_fk_child, filter_sibling_fk_parent,
    filter_sibling_fk_grand;

-- FILTER5b: matched-filter evidence must not propagate to unrelated FK facts
CREATE TABLE filter_crossprop_a (aid int PRIMARY KEY);
CREATE TABLE filter_crossprop_c (cid int PRIMARY KEY);
CREATE TABLE filter_crossprop_b
(
    c_id int NOT NULL REFERENCES filter_crossprop_c (cid),
    a_id int NOT NULL UNIQUE REFERENCES filter_crossprop_a (aid),
    bid int PRIMARY KEY
);
CREATE TABLE filter_crossprop_r
(
    r_aid int NOT NULL REFERENCES filter_crossprop_a (aid),
    rid int PRIMARY KEY
);

INSERT INTO filter_crossprop_a VALUES (1);
INSERT INTO filter_crossprop_c VALUES (1), (2);
INSERT INTO filter_crossprop_b VALUES (2, 1, 10);
INSERT INTO filter_crossprop_r VALUES (1, 100);

CREATE VIEW filter_crossprop_r_filtered AS
SELECT r_aid, rid FROM filter_crossprop_r WHERE r_aid = 1::int;

CREATE VIEW filter_crossprop_c_filtered AS
SELECT cid FROM filter_crossprop_c WHERE cid = 1::int;

WITH q AS (
    SELECT ab.aid, ab.c_id, rf.rid
    FROM (
        SELECT a.aid, b.c_id, b.bid
        FROM filter_crossprop_a a
        LEFT JOIN filter_crossprop_b b FOR KEY (a_id) -> a (aid)
    ) ab
    JOIN filter_crossprop_r_filtered rf FOR KEY (r_aid) -> ab (aid)
)
SELECT q.aid, q.c_id, cf.cid AS matched_cid
FROM q
LEFT JOIN filter_crossprop_c_filtered cf FOR KEY (cid) <- q (c_id);

DROP VIEW filter_crossprop_c_filtered, filter_crossprop_r_filtered;
DROP TABLE filter_crossprop_r, filter_crossprop_b,
           filter_crossprop_c, filter_crossprop_a;

-- FILTER5c: boolean proof-filter values must be simple expressions
CREATE TABLE filter_bool_parent (id boolean PRIMARY KEY);
CREATE TABLE filter_bool_child
(
    parent_id boolean NOT NULL REFERENCES filter_bool_parent (id)
);
INSERT INTO filter_bool_parent VALUES (true), (false);
INSERT INTO filter_bool_child VALUES (true);

CREATE VIEW filter_bool_parent_or AS
SELECT id FROM filter_bool_parent WHERE id = (true OR false);
CREATE VIEW filter_bool_child_or AS
SELECT parent_id FROM filter_bool_child WHERE parent_id = (true OR false);

SELECT *
FROM filter_bool_parent_or p
-- rejected, reason: BoolExpr filter values are not proof-filter terms
JOIN filter_bool_child_or c FOR KEY (parent_id) -> p (id);

CREATE VIEW filter_bool_parent_test AS
SELECT id FROM filter_bool_parent WHERE id = (true IS TRUE);
CREATE VIEW filter_bool_child_test AS
SELECT parent_id FROM filter_bool_child WHERE parent_id = (true IS TRUE);

SELECT *
FROM filter_bool_parent_test p
-- rejected, reason: BooleanTest filter values are not proof-filter terms
JOIN filter_bool_child_test c FOR KEY (parent_id) -> p (id);

DROP VIEW filter_bool_child_test, filter_bool_parent_test,
          filter_bool_child_or, filter_bool_parent_or;
DROP TABLE filter_bool_child, filter_bool_parent;

-- FILTER5d: pseudo-function filter values are not proof-filter terms
CREATE TABLE filter_pseudo_parent (id int PRIMARY KEY);
CREATE TABLE filter_pseudo_child
(
    parent_id int NOT NULL REFERENCES filter_pseudo_parent (id)
);
INSERT INTO filter_pseudo_parent VALUES (1), (2);
INSERT INTO filter_pseudo_child VALUES (1);

CREATE VIEW filter_case_parent_v AS
SELECT id FROM filter_pseudo_parent
WHERE id = CASE WHEN true THEN 1 ELSE 2 END;
CREATE VIEW filter_case_child_v AS
SELECT parent_id FROM filter_pseudo_child
WHERE parent_id = CASE WHEN true THEN 1 ELSE 2 END;
SELECT *
FROM filter_case_parent_v p
-- rejected, reason: CaseExpr filter values are not proof-filter terms
JOIN filter_case_child_v c FOR KEY (parent_id) -> p (id)
ORDER BY p.id;
DROP VIEW filter_case_child_v, filter_case_parent_v;

CREATE VIEW filter_minmax_parent_v AS
SELECT id FROM filter_pseudo_parent WHERE id = LEAST(1, 2);
CREATE VIEW filter_minmax_child_v AS
SELECT parent_id FROM filter_pseudo_child WHERE parent_id = LEAST(1, 2);
SELECT count(*) AS minmax_filter_rows
FROM filter_minmax_parent_v p
-- rejected, reason: MinMaxExpr filter values are not proof-filter terms
JOIN filter_minmax_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_minmax_child_v, filter_minmax_parent_v;

CREATE VIEW filter_coalesce_parent_v AS
SELECT id FROM filter_pseudo_parent WHERE id = COALESCE(1, 2);
CREATE VIEW filter_coalesce_child_v AS
SELECT parent_id FROM filter_pseudo_child WHERE parent_id = COALESCE(1, 2);
SELECT count(*) AS coalesce_filter_rows
FROM filter_coalesce_parent_v p
-- rejected, reason: CoalesceExpr filter values are not proof-filter terms
JOIN filter_coalesce_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_coalesce_child_v, filter_coalesce_parent_v;

CREATE VIEW filter_nullif_parent_v AS
SELECT id FROM filter_pseudo_parent WHERE id = NULLIF(1, 2);
CREATE VIEW filter_nullif_child_v AS
SELECT parent_id FROM filter_pseudo_child WHERE parent_id = NULLIF(1, 2);
SELECT count(*) AS nullif_filter_rows
FROM filter_nullif_parent_v p
-- rejected, reason: NullIfExpr filter values are not proof-filter terms
JOIN filter_nullif_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW filter_nullif_child_v, filter_nullif_parent_v;

DROP TABLE filter_pseudo_child, filter_pseudo_parent;

-- FILTER5e: matched-filter propagation skips other same-relation keys
CREATE TABLE filter_self_rowcov
(
    id int PRIMARY KEY,
    parent_id int UNIQUE NOT NULL REFERENCES filter_self_rowcov (id)
);
CREATE TABLE filter_self_rowcov_ref
(
    parent_id int NOT NULL REFERENCES filter_self_rowcov (parent_id)
);
INSERT INTO filter_self_rowcov VALUES (1, 1), (2, 2);
INSERT INTO filter_self_rowcov_ref VALUES (1);

CREATE VIEW filter_self_rowcov_ref_v AS
SELECT parent_id FROM filter_self_rowcov_ref WHERE parent_id = 1;

SELECT count(*) AS self_rowcov_rows
FROM (
    SELECT c.parent_id
    FROM filter_self_rowcov p
    JOIN (
        SELECT id, parent_id
        FROM filter_self_rowcov
        WHERE parent_id = 1
    ) c FOR KEY (parent_id) -> p (id)
) q
JOIN filter_self_rowcov_ref_v r FOR KEY (parent_id) -> q (parent_id);

DROP VIEW filter_self_rowcov_ref_v;
DROP TABLE filter_self_rowcov_ref, filter_self_rowcov;

-- FILTER5f: matched-filter propagation skips different key arities
CREATE TABLE filter_len_rowcov
(
    id int UNIQUE NOT NULL,
    a int NOT NULL,
    b int NOT NULL,
    parent_a int NOT NULL,
    parent_b int NOT NULL,
    UNIQUE (a, b),
    UNIQUE (parent_a, parent_b),
    FOREIGN KEY (parent_a, parent_b) REFERENCES filter_len_rowcov (a, b)
);
CREATE TABLE filter_len_ref
(
    parent_a int NOT NULL,
    parent_b int NOT NULL,
    FOREIGN KEY (parent_a, parent_b)
        REFERENCES filter_len_rowcov (parent_a, parent_b)
);
INSERT INTO filter_len_rowcov
VALUES (100, 1, 1, 1, 1), (200, 2, 2, 2, 2);
INSERT INTO filter_len_ref VALUES (1, 1);

CREATE VIEW filter_len_ref_v AS
SELECT parent_a, parent_b
FROM filter_len_ref
WHERE parent_a = 1 AND parent_b = 1;

SELECT count(*) AS length_mismatch_rows
FROM (
    SELECT c.parent_a, c.parent_b
    FROM filter_len_rowcov p
    JOIN (
        SELECT id, a, b, parent_a, parent_b
        FROM filter_len_rowcov
        WHERE parent_a = 1 AND parent_b = 1
    ) c FOR KEY (parent_a, parent_b) -> p (a, b)
) q
JOIN filter_len_ref_v r FOR KEY (parent_a, parent_b) -> q (parent_a, parent_b);

DROP VIEW filter_len_ref_v;
DROP TABLE filter_len_ref, filter_len_rowcov;

-- FILTER5g: matched-filter propagation skips different key identities
CREATE FUNCTION filter_identity_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a = b $$;

CREATE OPERATOR ===
(
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = filter_identity_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR CLASS filter_identity_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 1 < (int, int),
    OPERATOR 2 <= (int, int),
    OPERATOR 3 === (int, int),
    OPERATOR 4 >= (int, int),
    OPERATOR 5 > (int, int),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE filter_identity_grand (id int);
CREATE UNIQUE INDEX filter_identity_grand_idx
    ON filter_identity_grand USING btree (id filter_identity_int4_ops);
CREATE TABLE filter_identity_parent
(
    id int PRIMARY KEY REFERENCES filter_identity_grand (id)
);
CREATE TABLE filter_identity_child
(
    parent_id int NOT NULL REFERENCES filter_identity_parent (id)
);
INSERT INTO filter_identity_grand VALUES (1), (2);
INSERT INTO filter_identity_parent VALUES (1), (2);
INSERT INTO filter_identity_child VALUES (1);

CREATE VIEW filter_identity_child_v AS
SELECT parent_id FROM filter_identity_child WHERE parent_id = 1;

SELECT count(*) AS identity_mismatch_rows
FROM (
    SELECT p.id
    FROM filter_identity_parent p
    JOIN filter_identity_child_v c FOR KEY (parent_id) -> p (id)
) q
JOIN filter_identity_grand g FOR KEY (id) <- q (id);

DROP VIEW filter_identity_child_v;
DROP TABLE filter_identity_child, filter_identity_parent, filter_identity_grand;
DROP OPERATOR CLASS filter_identity_int4_ops USING btree;
DROP OPERATOR FAMILY filter_identity_int4_ops USING btree;
DROP OPERATOR === (int, int);
DROP FUNCTION filter_identity_eq(int, int);

-- FILTER6: key join predicate uses the FK equality operator, not search_path "="
CREATE SCHEMA key_join_shadow;
SET search_path = key_join_shadow, pg_catalog;

CREATE FUNCTION int4_always_true(int4, int4) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT true $$;

CREATE OPERATOR = (
  LEFTARG = int4,
  RIGHTARG = int4,
  FUNCTION = int4_always_true,
  COMMUTATOR = OPERATOR(key_join_shadow.=)
);

CREATE TABLE shadow_parent (id int PRIMARY KEY);
CREATE TABLE shadow_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES shadow_parent (id)
);

INSERT INTO shadow_parent VALUES (1), (2);
INSERT INTO shadow_child VALUES (10, 1);

SELECT p.id AS parent_id, c.id AS child_id, c.parent_id AS child_parent_id
FROM shadow_parent p
-- accepted
JOIN shadow_child c FOR KEY (parent_id) -> p (id)
ORDER BY p.id, c.id;

CREATE VIEW shadow_parent_filter AS
SELECT id FROM shadow_parent WHERE id = 1;

CREATE VIEW shadow_child_filter AS
SELECT parent_id FROM shadow_child WHERE parent_id = 1;

-- rejected, reason: filter "=" is not the exact key equality operator
SELECT *
FROM shadow_parent_filter p
JOIN shadow_child_filter c FOR KEY (parent_id) -> p (id);

DROP VIEW shadow_child_filter, shadow_parent_filter;
DROP TABLE shadow_child, shadow_parent;
DROP OPERATOR = (int4, int4);
DROP FUNCTION int4_always_true(int4, int4);
DROP SCHEMA key_join_shadow;
RESET search_path;

-- FILTER7: matched filters may use compatible cross-type equality
CREATE TABLE filter_opfamily_parent
(
    tenant_id bigint NOT NULL,
    id bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE filter_opfamily_child
(
    tenant_id bigint NOT NULL,
    parent_id bigint NOT NULL,
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES filter_opfamily_parent (tenant_id, id)
);

INSERT INTO filter_opfamily_parent VALUES (1, 10), (2, 20);
INSERT INTO filter_opfamily_child VALUES (1, 10), (2, 20);

CREATE VIEW filter_opfamily_parent_small AS
SELECT tenant_id, id
FROM filter_opfamily_parent
WHERE tenant_id = 1::smallint;

CREATE VIEW filter_opfamily_child_small AS
SELECT tenant_id, parent_id
FROM filter_opfamily_child
WHERE tenant_id = 1::smallint;

CREATE VIEW filter_opfamily_child_big AS
SELECT tenant_id, parent_id
FROM filter_opfamily_child
WHERE tenant_id = 1::bigint;

CREATE VIEW filter_opfamily_parent_big AS
SELECT tenant_id, id
FROM filter_opfamily_parent
WHERE tenant_id = 1::bigint;

CREATE VIEW filter_opfamily_parent_int AS
SELECT tenant_id, id
FROM filter_opfamily_parent
WHERE tenant_id = 1::integer;

CREATE VIEW filter_opfamily_child_int AS
SELECT tenant_id, parent_id
FROM filter_opfamily_child
WHERE tenant_id = 1::integer;

CREATE VIEW filter_opfamily_parent_right_big AS
SELECT tenant_id, id
FROM filter_opfamily_parent
WHERE 1::bigint = tenant_id;

-- accepted, reason: both filters use the same compatible bigint = smallint equality
SELECT *
FROM filter_opfamily_parent_small p
JOIN filter_opfamily_child_small c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id)
ORDER BY p.tenant_id, p.id;

-- accepted, reason: both filters use exact canonical tenant_id = 1::bigint
SELECT *
FROM filter_opfamily_parent_big p
JOIN filter_opfamily_child_big c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id)
ORDER BY p.tenant_id, p.id;

-- rejected, reason: filter value trees do not match exactly
SELECT *
FROM filter_opfamily_parent_small p
JOIN filter_opfamily_child_big c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id);

-- accepted, reason: both filters use the same compatible bigint = integer equality
SELECT *
FROM filter_opfamily_parent_int p
JOIN filter_opfamily_child_int c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id);

-- rejected, reason: key-on-right filters are not canonical proof evidence
SELECT *
FROM filter_opfamily_parent_right_big p
JOIN filter_opfamily_child_big c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id)
ORDER BY p.tenant_id, p.id;

DROP VIEW filter_opfamily_parent_right_big, filter_opfamily_child_int,
          filter_opfamily_parent_int, filter_opfamily_parent_big,
          filter_opfamily_child_big,
          filter_opfamily_child_small, filter_opfamily_parent_small;
DROP TABLE filter_opfamily_child, filter_opfamily_parent;

CREATE TABLE filter_opfamily_cross_parent
(
    tenant_id bigint NOT NULL,
    id bigint NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE filter_opfamily_cross_child
(
    tenant_id integer NOT NULL,
    parent_id bigint NOT NULL,
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES filter_opfamily_cross_parent (tenant_id, id)
);

INSERT INTO filter_opfamily_cross_parent VALUES (1, 10), (2, 20);
INSERT INTO filter_opfamily_cross_child VALUES (1, 10), (2, 20);

CREATE VIEW filter_opfamily_cross_parent_small AS
SELECT tenant_id, id
FROM filter_opfamily_cross_parent
WHERE tenant_id = 1::smallint;

CREATE VIEW filter_opfamily_cross_child_small AS
SELECT tenant_id, parent_id
FROM filter_opfamily_cross_child
WHERE tenant_id = 1::smallint;

-- rejected, reason: FK and PK key types differ
SELECT *
FROM filter_opfamily_cross_parent_small p
JOIN filter_opfamily_cross_child_small c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id)
ORDER BY p.tenant_id, p.id;

DROP VIEW filter_opfamily_cross_child_small,
          filter_opfamily_cross_parent_small;
DROP TABLE filter_opfamily_cross_child, filter_opfamily_cross_parent;

-- FILTER8: matched filters preserve value typmods structurally
CREATE TABLE filter_typmod_parent
(
    amount numeric(10,2) PRIMARY KEY
);
CREATE TABLE filter_typmod_child
(
    amount numeric(10,2) NOT NULL REFERENCES filter_typmod_parent (amount)
);

INSERT INTO filter_typmod_parent VALUES (1.00), (2.00);
INSERT INTO filter_typmod_child VALUES (1.00), (2.00);

CREATE VIEW filter_typmod_parent_short AS
SELECT amount FROM filter_typmod_parent
WHERE amount = 1.00::numeric(5,2);

CREATE VIEW filter_typmod_child_short AS
SELECT amount FROM filter_typmod_child
WHERE amount = 1.00::numeric(5,2);

CREATE VIEW filter_typmod_parent_exact AS
SELECT amount FROM filter_typmod_parent
WHERE amount = 1.00::numeric(10,2);

CREATE VIEW filter_typmod_child_exact AS
SELECT amount FROM filter_typmod_child
WHERE amount = 1.00::numeric(10,2);

-- accepted, reason: both filters use identical value expression typmods
SELECT *
FROM filter_typmod_parent_short p
JOIN filter_typmod_child_short c FOR KEY (amount) -> p (amount);

-- accepted, reason: both filters use identical key value typmods
SELECT *
FROM filter_typmod_parent_exact p
JOIN filter_typmod_child_exact c FOR KEY (amount) -> p (amount)
ORDER BY p.amount;

DROP VIEW filter_typmod_child_exact, filter_typmod_parent_exact,
          filter_typmod_child_short, filter_typmod_parent_short;
DROP TABLE filter_typmod_child, filter_typmod_parent;

-- FILTER9: matched filters may use compatible deterministic collations
CREATE TABLE filter_collation_parent
(
    code text COLLATE "C" PRIMARY KEY
);
CREATE TABLE filter_collation_child
(
    code text COLLATE "C" NOT NULL
        REFERENCES filter_collation_parent (code)
);

INSERT INTO filter_collation_parent VALUES ('a'), ('b');
INSERT INTO filter_collation_child VALUES ('a'), ('b');

CREATE VIEW filter_collation_parent_posix AS
SELECT code FROM filter_collation_parent
WHERE code = 'a'::text COLLATE "POSIX";

CREATE VIEW filter_collation_child_posix AS
SELECT code FROM filter_collation_child
WHERE code = 'a'::text COLLATE "POSIX";

CREATE VIEW filter_collation_parent_exact AS
SELECT code FROM filter_collation_parent
WHERE code = 'a'::text COLLATE "C";

CREATE VIEW filter_collation_child_exact AS
SELECT code FROM filter_collation_child
WHERE code = 'a'::text COLLATE "C";

-- accepted, reason: deterministic collations agree on equality
SELECT *
FROM filter_collation_parent_posix p
JOIN filter_collation_child_posix c FOR KEY (code) -> p (code);

-- accepted, reason: filter value collations exactly match the key collation
SELECT *
FROM filter_collation_parent_exact p
JOIN filter_collation_child_exact c FOR KEY (code) -> p (code)
ORDER BY p.code;

-- Nondeterministic filter collations are exercised in key_join_icu.

DROP VIEW filter_collation_child_exact, filter_collation_parent_exact,
          filter_collation_child_posix, filter_collation_parent_posix;
DROP TABLE filter_collation_child, filter_collation_parent;

-- ============================================================================
-- Stored FOR KEY proofs and dependency revalidation
-- ============================================================================

-- Reset: drop the plain v1/v2 left over by the segments above; the cases below recreate them.
DROP VIEW v1, v2;

--
-- Test renaming tables and columns.
--
CREATE VIEW v1 AS
SELECT *
FROM t1
-- accepted
JOIN t2 FOR KEY (c3) -> t1 (c1);
\d+ v1
SELECT * FROM v1; -- accepted

ALTER TABLE t1 RENAME COLUMN c1 TO c1_renamed;
ALTER TABLE t2 RENAME COLUMN c3 TO c3_renamed;
ALTER TABLE t1 RENAME TO t1_renamed;
ALTER TABLE t2 RENAME TO t2_renamed;
\d+ v1

SELECT * FROM v1; -- accepted

-- Undo the effect of the renames
ALTER TABLE t2_renamed RENAME TO t2;
ALTER TABLE t1_renamed RENAME TO t1;
ALTER TABLE t2 RENAME COLUMN c3_renamed TO c3;
ALTER TABLE t1 RENAME COLUMN c1_renamed TO c1;
\d+ v1

SELECT * FROM v1; -- accepted

ALTER TABLE t2 DROP CONSTRAINT t2_c3_fkey; -- rejected, reason: stored view depends on the FK proof

DROP VIEW v1;
ALTER TABLE t2 DROP CONSTRAINT t2_c3_fkey;

/* Recreate contraint and view to test DROP CASCADE */
ALTER TABLE t2 ADD CONSTRAINT t2_c3_fkey FOREIGN KEY (c3) REFERENCES t1 (c1);
CREATE VIEW v1 AS
SELECT *
FROM t1
-- accepted
JOIN t2 FOR KEY (c3) -> t1 (c1);
ALTER TABLE t2 DROP CONSTRAINT t2_c3_fkey CASCADE; -- accepted

SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1); -- rejected, reason: FK constraint was dropped
SELECT * FROM t2 JOIN t1 FOR KEY (c1) <- t2 (c3); -- rejected, reason: FK constraint was dropped

CREATE VIEW v2 AS
SELECT * FROM t1 JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2); -- accepted
SELECT * FROM t1 JOIN t2 FOR KEY (c3,c4) -> t1 (c1,c2); -- accepted
SELECT * FROM v2; -- accepted
\d+ v2

CREATE VIEW v3 AS
SELECT * FROM t2 JOIN t1 FOR KEY (c1,c2) <- t2 (c3,c4); -- accepted
SELECT * FROM t2 JOIN t1 FOR KEY (c1,c2) <- t2 (c3,c4); -- accepted
\d+ v3

SELECT * FROM v3; -- accepted

SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1); -- rejected, reason: single-column FK proof is no longer available
SELECT * FROM t2 JOIN t1 FOR KEY (c1) <- t2 (c3); -- rejected, reason: single-column FK proof is no longer available
SELECT * FROM t1 JOIN t2 FOR KEY (c3,c4) <- t1 (c1,c2); -- rejected, reason: wrong direction for the composite FK
SELECT * FROM t2 JOIN t1 FOR KEY (c1,c2) -> t2 (c3,c4); -- rejected, reason: wrong direction for the composite FK

-- Recreate the partition fixtures (the ad-hoc partition cases above dropped them).
CREATE TABLE kj_part_parent (id int PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE kj_part_parent_1 PARTITION OF kj_part_parent
    FOR VALUES FROM (1) TO (10);
CREATE TABLE kj_part_parent_2 PARTITION OF kj_part_parent
    FOR VALUES FROM (10) TO (20);
CREATE TABLE kj_part_child
(
    id  int PRIMARY KEY,
    pid int NOT NULL REFERENCES kj_part_parent (id)
);

CREATE VIEW kj_part_bad_view AS
SELECT c.id AS child_id, c.pid, p1.id AS parent_id
FROM kj_part_parent_1 p1
-- rejected, reason: invalid referenced-partition FK proof cannot be stored
JOIN kj_part_child c FOR KEY (pid) -> p1 (id);

DROP TABLE kj_part_child, kj_part_parent;

-- Recreate stuff for pg_dump tests
ALTER TABLE t2
    ADD CONSTRAINT t2_c3_fkey FOREIGN KEY (c3) REFERENCES t1 (c1);
CREATE VIEW v1 AS
SELECT *
FROM t1
-- accepted
JOIN t2 FOR KEY (c3) -> t1 (c1);

DROP VIEW v1, v2, v3;

-- Recreate the plain views v1..v4 (the ad-hoc segment above dropped them) for the stored v5 case.
CREATE VIEW v1 AS
SELECT c1 AS v1_c1, c2 AS v1_c2 FROM t1;

CREATE VIEW v2 AS
SELECT v1_c1 AS v2_c1, v1_c2 AS v2_c2 FROM v1;

CREATE VIEW v3 AS
SELECT c3 AS v3_c3, c4 AS v3_c4 FROM t2;

CREATE VIEW v4 AS
SELECT v3_c3 AS v4_c3, v3_c4 AS v4_c4 FROM v3;

CREATE VIEW v5 AS
SELECT
    v2_c1,
    v2_c2,
    v4_c3,
    v4_c4
-- accepted
FROM v2 JOIN v4 FOR KEY (v4_c3, v4_c4) -> v2 (v2_c1, v2_c2);

SELECT * FROM v5;

DROP VIEW v1, v2, v3, v4, v5;

CREATE VIEW v1 AS
SELECT
    t5.c9,
    t5.c10,
    t5.c11,
    t5.c12
FROM t5
-- accepted
JOIN t1 FOR KEY (c1, c2) <- t5 (c11, c12)
JOIN t1 AS t1_2 FOR KEY (c1, c2) <- t5 (c11, c12)
JOIN t1 AS t1_3 FOR KEY (c1, c2) <- t5 (c11, c12);

SELECT
    v1.c11,
    v1.c12,
    t6.c13,
    t6.c14
FROM v1
-- accepted
JOIN t6 FOR KEY (c13, c14) -> v1 (c9, c10);

DROP VIEW v1;

-- The fan-trap section above left its own table named orders; clear the name
-- (this also strips the order FKs of order_items/packages; the tables survive
-- for the final cleanup).
DROP TABLE orders CASCADE;

--
-- Test revalidation of views
--

CREATE TABLE addresses
(
    id           INTEGER      NOT NULL,
    street       VARCHAR(255) NOT NULL,
    city         VARCHAR(100) NOT NULL,
    state        VARCHAR(100) NOT NULL,
    country_code CHAR(2)      NOT NULL,
    zip_code     VARCHAR(20)  NOT NULL,
    CONSTRAINT addresses_pkey PRIMARY KEY (id)
);

CREATE TABLE customers
(
    id         INTEGER      NOT NULL,
    name       VARCHAR(255) NOT NULL,
    address_id INTEGER      NOT NULL,
    CONSTRAINT customers_pkey            PRIMARY KEY (id),
    CONSTRAINT customers_address_id_fkey FOREIGN KEY (address_id) REFERENCES addresses (id)
);

CREATE TABLE orders
(
    id           BIGINT         NOT NULL,
    order_date   DATE           NOT NULL,
    amount       DECIMAL(10, 2) NOT NULL,
    customer_id  INTEGER        NOT NULL,
    CONSTRAINT orders_pkey             PRIMARY KEY (id),
    CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customers (id)
);

CREATE VIEW customer_details AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    a.street,
    a.city,
    a.state,
    a.country_code,
    a.zip_code
FROM customers AS c
-- accepted
JOIN addresses AS a FOR KEY (id) <- c (address_id);

CREATE VIEW orders_by_country AS
SELECT
    cd.country_code,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount
FROM orders AS o
-- accepted
JOIN customer_details AS cd FOR KEY (customer_id) <- o (customer_id)
GROUP BY ROLLUP (cd.country_code);

-- Test NOT NULL constraint dependency: should error because customer_details
-- (and transitively orders_by_country) depends on address_id being NOT NULL
ALTER TABLE customers ALTER COLUMN address_id DROP NOT NULL;

-- Replace customer_details with LEFT JOIN version.  Revalidating
-- orders_by_country only shrinks its proof dependencies, so the replacement
-- can proceed without rewriting orders_by_country.
CREATE OR REPLACE VIEW customer_details AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    a.street,
    a.city,
    a.state,
    a.country_code,
    a.zip_code
FROM customers AS c
-- accepted
LEFT JOIN addresses AS a FOR KEY (id) <- c (address_id);

-- DROP NOT NULL succeeds because recreating customer_details as a LEFT JOIN
-- removed its stored proof dependency on customers.address_id NOT NULL.
-- orders_by_country was revalidated against the new producer surface; its
-- key join needs customer_id uniqueness and row coverage, not that internal
-- NOT NULL proof.
ALTER TABLE customers ALTER COLUMN address_id DROP NOT NULL;

-- Restore NOT NULL and the inner-join producer for subsequent tests.
DROP VIEW orders_by_country;
ALTER TABLE customers ALTER COLUMN address_id SET NOT NULL;

CREATE OR REPLACE VIEW customer_details AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    a.street,
    a.city,
    a.state,
    a.country_code,
    a.zip_code
FROM customers AS c
-- accepted
JOIN addresses AS a FOR KEY (id) <- c (address_id);

CREATE VIEW orders_by_country AS
SELECT
    cd.country_code,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount
FROM orders AS o
-- accepted
JOIN customer_details AS cd FOR KEY (customer_id) <- o (customer_id)
GROUP BY ROLLUP (cd.country_code);

--
-- Test NOT NULL constraint tracking for various join types
-- Create simple test tables for these tests
--
CREATE TABLE t1_nn (t1_id INTEGER PRIMARY KEY);
CREATE TABLE t2_nn (
    t2_id INTEGER PRIMARY KEY,
    t2_t1_id_nn INTEGER NOT NULL REFERENCES t1_nn(t1_id),
    t2_t1_id_nullable INTEGER REFERENCES t1_nn(t1_id)
);

INSERT INTO t1_nn VALUES (1), (2), (3);
INSERT INTO t2_nn VALUES (10, 1, 1), (20, 2, NULL);

-- Test 1: LEFT JOIN where referencing is on inner side (right)
-- accepted
CREATE VIEW v_left_ref_inner AS
SELECT * FROM t1_nn LEFT JOIN t2_nn FOR KEY (t2_t1_id_nn) -> t1_nn (t1_id);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- rejected, reason: view depends on the NOT NULL proof
DROP VIEW v_left_ref_inner;

-- Test 2: LEFT JOIN where referencing is on outer side (left)
-- Producer facts alone do not create stored dependencies.
CREATE VIEW v_left_ref_outer AS
-- accepted
SELECT * FROM t2_nn LEFT JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nn);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- accepted, reason: no stored proof consumes the NOT NULL fact
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn SET NOT NULL;
DROP VIEW v_left_ref_outer;

-- Test 3: RIGHT JOIN where referencing is on inner side (left)
-- accepted
CREATE VIEW v_right_ref_inner AS
SELECT * FROM t2_nn RIGHT JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nn);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- rejected, reason: view depends on the NOT NULL proof
DROP VIEW v_right_ref_inner;

-- Test 4: RIGHT JOIN where referencing is on outer side (right)
-- Producer facts alone do not create stored dependencies.
CREATE VIEW v_right_ref_outer AS
-- accepted
SELECT * FROM t1_nn RIGHT JOIN t2_nn FOR KEY (t2_t1_id_nn) -> t1_nn (t1_id);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- accepted, reason: no stored proof consumes the NOT NULL fact
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn SET NOT NULL;
DROP VIEW v_right_ref_outer;

-- Test 5: FULL JOIN preserves referencing rows, so the key-join proof does
-- not consume the NOT NULL fact.
CREATE VIEW v_full_1 AS
-- accepted
SELECT * FROM t1_nn FULL JOIN t2_nn FOR KEY (t2_t1_id_nn) -> t1_nn (t1_id);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- accepted, reason: no stored proof consumes the NOT NULL fact
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn SET NOT NULL;
DROP VIEW v_full_1;

CREATE VIEW v_full_2 AS
-- accepted
SELECT * FROM t2_nn FULL JOIN t1_nn FOR KEY (t1_id) <- t2_nn (t2_t1_id_nn);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- accepted, reason: no stored proof consumes the NOT NULL fact
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn SET NOT NULL;
DROP VIEW v_full_2;

-- A second-layer stored key join that consumes a producer view's not-null
-- fact activates the underlying NOT NULL dependency.
CREATE VIEW v_notnull_producer AS
SELECT t2_id, t2_t1_id_nn FROM t2_nn;
CREATE VIEW v_notnull_consumer AS
-- accepted
SELECT *
FROM t1_nn JOIN v_notnull_producer p
    FOR KEY (t2_t1_id_nn) -> t1_nn (t1_id);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- rejected, reason: consumer view depends on the NOT NULL proof
DROP VIEW v_notnull_consumer;
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn DROP NOT NULL; -- accepted, reason: producer view alone does not store the fact
SELECT *
FROM t1_nn JOIN v_notnull_producer p
-- rejected, reason: view facts are recomputed after the NOT NULL constraint was dropped
    FOR KEY (t2_t1_id_nn) -> t1_nn (t1_id);
ALTER TABLE t2_nn ALTER COLUMN t2_t1_id_nn SET NOT NULL;
DROP VIEW v_notnull_producer;

-- Recreate the NE3 fixtures (created and dropped by the ad-hoc nullable-referencing cases).
CREATE TABLE ne3_a (id INTEGER PRIMARY KEY);
CREATE TABLE ne3_e (id INTEGER PRIMARY KEY);
CREATE TABLE ne3_c (
    id   INTEGER PRIMARY KEY,
    e_id INTEGER NOT NULL REFERENCES ne3_e(id)
);
CREATE TABLE ne3_b (
    id   INTEGER PRIMARY KEY,
    a_id INTEGER NOT NULL REFERENCES ne3_a(id),
    c_id INTEGER NOT NULL REFERENCES ne3_c(id)
);

CREATE VIEW ne3_right_nested_v AS
SELECT ne3_a.id AS a_id, ne3_c.e_id AS c_e_id
FROM ne3_a
-- accepted
LEFT JOIN (ne3_c JOIN ne3_b FOR KEY (c_id) -> ne3_c (id))
    FOR KEY (a_id) -> ne3_a (id);

SELECT *
FROM ne3_right_nested_v q
-- rejected, reason: c_e_id is null-extended by the stored right-nested key join
JOIN ne3_e FOR KEY (id) <- q (c_e_id);

DROP VIEW ne3_right_nested_v;

DROP TABLE ne3_b, ne3_c, ne3_e, ne3_a;
DROP TABLE t2_nn, t1_nn;

CREATE TABLE customer_addresses
(
    customer_id INTEGER NOT NULL,
    address_id INTEGER NOT NULL,
    CONSTRAINT customer_addresses_pkey             PRIMARY KEY (customer_id, address_id),
    CONSTRAINT customer_addresses_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customers (id),
    CONSTRAINT customer_addresses_address_id_fkey  FOREIGN KEY (address_id) REFERENCES addresses (id)
);

-- Replacing customer_details to route through customer_addresses would make
-- customer_details.customer_id appear once per customer address.  The
-- dependent orders_by_country view needs customer_details.customer_id to
-- remain unique, so revalidation must reject the producer replacement.
CREATE OR REPLACE VIEW customer_details AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    a.street,
    a.city,
    a.state,
    a.country_code,
    a.zip_code
FROM customers AS c
JOIN customer_addresses AS ca FOR KEY (customer_id) -> c (id)
JOIN addresses AS a FOR KEY (id) <- ca (address_id);

-- Recreate the duplicate-column views v1/v2 (as in the ad-hoc segment); they survive to the final cleanup.
CREATE VIEW v1 AS SELECT c1 AS c1_1, c1 AS c1_2, c2 AS c2_1, c2 AS c2_2 FROM t1;
CREATE VIEW v2 AS SELECT c3 AS c3_1, c3 AS c3_2, c4 AS c4_1, c4 AS c4_2 FROM t2;

CREATE VIEW v_dup_alias_fk AS
SELECT * FROM v1 JOIN v2 FOR KEY (c3_2, c4_1) -> v1 (c1_2, c2_2);
SELECT pg_get_viewdef('v_dup_alias_fk'::regclass, true);
DROP VIEW v_dup_alias_fk;

--
-- Stored key joins must depend directly on FK equality operator functions.
-- Otherwise CREATE OR REPLACE FUNCTION or ALTER FUNCTION could change an
-- operator implementation after view creation without revalidating the
-- stored proof.
--
CREATE FUNCTION key_join_operator_funcdep_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a = b $$;

CREATE OPERATOR ===
(
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = key_join_operator_funcdep_eq,
    COMMUTATOR = ===,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR CLASS key_join_operator_funcdep_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 1 < (int, int),
    OPERATOR 2 <= (int, int),
    OPERATOR 3 === (int, int),
    OPERATOR 4 >= (int, int),
    OPERATOR 5 > (int, int),
    FUNCTION 1 btint4cmp(int, int);

CREATE TABLE key_join_operator_funcdep_parent
(
    id int
);
CREATE UNIQUE INDEX key_join_operator_funcdep_parent_idx
    ON key_join_operator_funcdep_parent
    USING btree (id key_join_operator_funcdep_int4_ops);

CREATE TABLE key_join_operator_funcdep_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES key_join_operator_funcdep_parent (id)
);

INSERT INTO key_join_operator_funcdep_parent VALUES (1);
INSERT INTO key_join_operator_funcdep_child VALUES (1, 1);

CREATE VIEW key_join_operator_funcdep_view AS
SELECT c.id AS child_id, p.id AS parent_id
FROM key_join_operator_funcdep_parent p
JOIN key_join_operator_funcdep_child c FOR KEY (parent_id) -> p (id);

SELECT count(*) AS funcdep_view_rows
FROM key_join_operator_funcdep_view;

CREATE OR REPLACE FUNCTION key_join_operator_funcdep_eq(a int, b int)
RETURNS boolean
LANGUAGE sql STABLE STRICT AS $$ SELECT false $$;

SELECT provolatile AS funcdep_volatility
FROM pg_proc
WHERE oid = 'key_join_operator_funcdep_eq(int,int)'::regprocedure;

SELECT count(*) AS funcdep_view_rows
FROM key_join_operator_funcdep_view;

ALTER FUNCTION key_join_operator_funcdep_eq(int, int) STABLE;

SELECT provolatile AS funcdep_volatility
FROM pg_proc
WHERE oid = 'key_join_operator_funcdep_eq(int,int)'::regprocedure;

SELECT count(*) AS funcdep_view_rows
FROM key_join_operator_funcdep_view;

DROP VIEW key_join_operator_funcdep_view;
DROP TABLE key_join_operator_funcdep_child,
           key_join_operator_funcdep_parent;
DROP OPERATOR CLASS key_join_operator_funcdep_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_operator_funcdep_int4_ops USING btree;
DROP OPERATOR === (int, int);
DROP FUNCTION key_join_operator_funcdep_eq(int, int);

--
-- Test scalar functions alongside key joins.
-- A scalar function (funcretset = false) returns exactly one row,
-- so it has trivial uniqueness and row-preservation properties.
--

-- Scalar function call in SELECT list works fine with key joins
CREATE FUNCTION scalar_meta() RETURNS int LANGUAGE sql STABLE AS $$ SELECT 42 $$;

SELECT t1.c1, scalar_meta() AS meta, t2.c3
FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1); -- accepted

-- View with scalar function in SELECT list and key join in FROM
CREATE VIEW v_fk_with_meta AS
SELECT t1.c1, t1.c2, t2.c3, t2.c4, scalar_meta() AS meta
-- accepted
FROM t1 LEFT JOIN t2 FOR KEY (c3) -> t1 (c1);

SELECT * FROM v_fk_with_meta; -- accepted

-- key join through the view (scalar in SELECT doesn't affect semantics)
CREATE TABLE t_child_meta (
    id int PRIMARY KEY,
    parent_ref int NOT NULL REFERENCES t1 (c1)
);
INSERT INTO t_child_meta VALUES (100, 1), (200, 3);

SELECT v.c1, v.meta, c.id
FROM v_fk_with_meta v
JOIN t_child_meta c FOR KEY (parent_ref) -> v (c1); -- accepted

-- View wrapping a scalar function as sole FROM item cannot be
-- an FK column source (drill_down_to_base_rel has no base table).
CREATE FUNCTION scalar_row(OUT a int, OUT b int) RETURNS record
LANGUAGE sql AS $$ SELECT 1, 42 $$;

-- Scalar function as direct FK column source is rejected with clear error
SELECT t1.c1, s.a
FROM t1 JOIN scalar_row() AS s FOR KEY (a) -> t1 (c1); -- rejected, reason: functions

-- View wrapping scalar function as FK column source
CREATE VIEW v_scalar_only AS SELECT * FROM scalar_row();

SELECT v.a, t2.c3
FROM v_scalar_only v
JOIN t2 FOR KEY (c3) -> v (a); -- rejected, reason: functions

-- SRF as sole FROM item in a view used as FK column source
CREATE FUNCTION srf_rows() RETURNS SETOF int LANGUAGE sql
AS $$ SELECT generate_series(1, 3) $$;

CREATE VIEW v_srf_only AS SELECT * FROM srf_rows() AS s(val);

SELECT v.val, t2.c3
FROM v_srf_only v
JOIN t2 FOR KEY (c3) -> v (val); -- rejected, reason: set-returning functions

-- SRF as direct FK column source
SELECT t1.c1, s.val
FROM t1 JOIN generate_series(1,3) AS s(val) FOR KEY (val) -> t1 (c1); -- rejected, reason: SRF

-- Cleanup
DROP VIEW v_srf_only, v_scalar_only, v_fk_with_meta;
DROP TABLE t_child_meta;
DROP FUNCTION scalar_meta(), scalar_row(), srf_rows();

-- ===================================================================
-- Test: GROUP BY creates BUNC-set uniqueness (not just restores it)
-- ===================================================================
-- GROUP BY on non-unique FK columns should create uniqueness,
-- allowing the result to be used as referencing side in a key join
-- that preserves the referenced table's uniqueness.

-- Basic: GROUP BY on non-unique FK column creates uniqueness in subquery,
-- allowing key join from subquery to base table.
CREATE VIEW v_sales_agg AS
SELECT
    p.id,
    s.total_sold
FROM products_gbu p
LEFT JOIN (
    SELECT product_id, SUM(price * qty) AS total_sold
    FROM sales_gbu
    GROUP BY product_id
-- accepted
) s FOR KEY (product_id) -> p (id);

SELECT * FROM v_sales_agg ORDER BY id;

-- The view preserves products_gbu uniqueness, so outer key join works.
SELECT * FROM v_sales_agg
-- accepted
JOIN resupplies_gbu FOR KEY (product_id) -> v_sales_agg (id)
ORDER BY v_sales_agg.id;

-- GROUP BY with more columns than FK: FK columns are a proper subset
-- of GROUP BY columns, so FK columns are NOT unique.  The view can be
-- created (the inner key join is fine because products_gbu is a base
-- table), but using the view as a referenced side should error because
-- products_gbu uniqueness is not preserved through the join.
CREATE VIEW v_sales_multi_group AS
SELECT
    p.id,
    s.total_qty
FROM products_gbu p
LEFT JOIN (
    SELECT product_id, price, SUM(qty) AS total_qty
    FROM sales_gbu
    GROUP BY product_id, price
-- accepted
) s FOR KEY (product_id) -> p (id);

SELECT * FROM v_sales_multi_group
-- rejected, reason: grouped view does not preserve referenced-side uniqueness
JOIN resupplies_gbu FOR KEY (product_id) -> v_sales_multi_group (id);

-- DISTINCT on non-unique FK column also creates uniqueness.
CREATE VIEW v_sales_distinct AS
SELECT
    p.id,
    s.product_id AS s_product_id
FROM products_gbu p
LEFT JOIN (
    SELECT DISTINCT product_id
    FROM sales_gbu
-- accepted
) s FOR KEY (product_id) -> p (id);

SELECT * FROM v_sales_distinct ORDER BY id;

-- Cleanup
DROP VIEW v_sales_agg, v_sales_multi_group, v_sales_distinct;

-- FILTER4: FILTER in deparsed view (round-trip test)
CREATE VIEW v_filter AS
-- accepted
  SELECT * FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1) FILTER (WHERE t2.c4 = 10);
SELECT pg_get_viewdef('v_filter');
DROP VIEW v_filter;

-- ============================================================
-- Revalidation via stored Query (no text round-trip)
-- ============================================================

-- Set up tables and views for revalidation tests
-- rv_child uses cid (not id) to avoid column name conflicts in SELECT *
CREATE TABLE rv_base (id int PRIMARY KEY, v int);
CREATE TABLE rv_child (cid int PRIMARY KEY, ref int NOT NULL REFERENCES rv_base(id));

-- rv_parent initially exposes all rows of rv_base (rows_preserved = true)
CREATE VIEW rv_parent AS SELECT id FROM rv_base;

-- rv_dep FK-joins rv_child against rv_parent; valid initially
CREATE VIEW rv_dep AS
-- accepted
    SELECT * FROM rv_parent JOIN rv_child FOR KEY (ref) -> rv_parent (id);

-- Replace rv_parent with a filtered version — rv_dep should become invalid
-- because the referenced relation no longer preserves all rows.
-- rejected, reason: replacement would invalidate dependent key-join view
CREATE OR REPLACE VIEW rv_parent AS SELECT id FROM rv_base WHERE v > 0;

-- Replace rv_parent back to unfiltered — rv_dep becomes valid again
CREATE OR REPLACE VIEW rv_parent AS SELECT id FROM rv_base;

DROP VIEW rv_dep, rv_parent;
DROP TABLE rv_child, rv_base;

-- Stored revalidation failure should format multi-column key lists from the
-- stored RTE attnums, not just live parse-time column arrays.
CREATE TABLE rv_multi_parent
(
    tenant_id int NOT NULL,
    id int NOT NULL,
    v int,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE rv_multi_child
(
    cid int PRIMARY KEY,
    tenant_id int NOT NULL,
    ref int NOT NULL,
    FOREIGN KEY (tenant_id, ref)
        REFERENCES rv_multi_parent (tenant_id, id)
);
CREATE VIEW rv_multi_parent_v AS
SELECT tenant_id, id FROM rv_multi_parent;
CREATE VIEW rv_multi_dep AS
-- accepted
    SELECT p.tenant_id, p.id, c.cid
    FROM rv_multi_parent_v p
    JOIN rv_multi_child c FOR KEY (tenant_id, ref) -> p (tenant_id, id);
-- rejected, reason: replacement invalidates a stored composite-key proof
CREATE OR REPLACE VIEW rv_multi_parent_v AS
SELECT tenant_id, id FROM rv_multi_parent WHERE v > 0;
DROP VIEW rv_multi_dep, rv_multi_parent_v;
DROP TABLE rv_multi_child, rv_multi_parent;

-- Regression: no false positive when view has WHERE on referenced base table.
-- A naive revalidation passing the stored query to analyze_join_tree would see
-- jointree->quals != NULL and falsely conclude rows_preserved = false.
CREATE TABLE rv3_ref (id int PRIMARY KEY);
CREATE TABLE rv3_child (ref int NOT NULL REFERENCES rv3_ref(id));
CREATE VIEW rv3_aux AS SELECT 1 AS x;      -- replaced to trigger revalidation

CREATE VIEW rv3_dep AS
    SELECT r.id, c.ref
    FROM rv3_ref r
-- accepted
    JOIN rv3_child c FOR KEY (ref) -> r (id)
    CROSS JOIN rv3_aux
    WHERE r.id > 0;                         -- WHERE on referenced base table

-- Replacing rv3_aux revalidates rv3_dep; must succeed (no false positive).
CREATE OR REPLACE VIEW rv3_aux AS SELECT 2 AS x;

DROP VIEW rv3_dep, rv3_aux;
DROP TABLE rv3_child, rv3_ref;

-- Stored filtered key joins must re-split and re-merge joinFilter quals.
CREATE TABLE rv_filter_parent (id int PRIMARY KEY);
CREATE TABLE rv_filter_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rv_filter_parent (id)
);
INSERT INTO rv_filter_parent VALUES (1), (2);
INSERT INTO rv_filter_child VALUES (10, 1), (20, 2);

CREATE VIEW rv_filter_child_v AS
SELECT c.parent_id
FROM rv_filter_parent p
JOIN rv_filter_child c FOR KEY (parent_id) -> p (id)
  FILTER (WHERE c.parent_id = 1);

SELECT count(*) AS rv_filter_rows
FROM rv_filter_child_v v
JOIN rv_filter_parent p FOR KEY (id) <- v (parent_id);

DROP VIEW rv_filter_child_v;
DROP TABLE rv_filter_child, rv_filter_parent;

-- Stored view revalidation must resolve outer CTE references from nested subqueries.
CREATE TABLE stored_cte_parent (id int PRIMARY KEY);
CREATE TABLE stored_cte_one_child
(
    id int PRIMARY KEY,
    pid int UNIQUE NOT NULL REFERENCES stored_cte_parent(id)
);
CREATE TABLE stored_cte_child
(
    id int PRIMARY KEY,
    pid int NOT NULL REFERENCES stored_cte_parent(id)
);
INSERT INTO stored_cte_parent VALUES (1), (2);
INSERT INTO stored_cte_one_child VALUES (10, 1), (20, 2);
INSERT INTO stored_cte_child VALUES (100, 1), (101, 1), (200, 2);

CREATE VIEW stored_cte_parent_v AS
WITH p AS (SELECT id FROM stored_cte_parent)
SELECT q.id, oc.id AS one_child_id
FROM (SELECT id FROM p) q
LEFT JOIN stored_cte_one_child oc FOR KEY (pid) -> q (id);

SELECT v.id, c.id AS child_id
FROM stored_cte_parent_v v
-- accepted
JOIN stored_cte_child c FOR KEY (pid) -> v (id)
ORDER BY v.id, c.id;
-- CTE lookup must consume ctelevelsup through the CTE owner's query stack, not
-- through a nested RTE reference site.
WITH p AS (SELECT id FROM stored_cte_parent)
SELECT v.id, c.id AS child_id
FROM (
    WITH q AS (SELECT id FROM p),
         p AS (SELECT id FROM stored_cte_parent WHERE id = 1)
    SELECT s.id
    FROM (SELECT id FROM q) s
) v
-- accepted, reason: q sees the outer p, not the later inner p
JOIN stored_cte_child c FOR KEY (pid) -> v (id)
ORDER BY v.id, c.id;

-- Shadowed CTE names must resolve at the RTE's own ctelevelsup.
CREATE VIEW stored_cte_shadow_v AS
WITH p AS (SELECT id FROM stored_cte_parent)
SELECT q.id
FROM (
    WITH p AS (SELECT id FROM stored_cte_parent WHERE id = 1)
    SELECT id FROM p
) q;

SELECT v.id, c.id AS child_id
FROM stored_cte_shadow_v v
-- rejected, reason: inner shadowing CTE filters referenced rows
JOIN stored_cte_child c FOR KEY (pid) -> v (id);

DROP VIEW stored_cte_shadow_v, stored_cte_parent_v;
DROP TABLE stored_cte_child, stored_cte_one_child, stored_cte_parent;

-- Revalidation must not re-store dependent _RETURN rules.  A replacement that
-- would need a different FK proof source is rejected until the dependent view
-- is explicitly recreated.
CREATE TABLE rv_rewrite_parent_a (id int PRIMARY KEY);
CREATE TABLE rv_rewrite_parent_b (id int PRIMARY KEY);
CREATE TABLE rv_rewrite_child
(
    cid int PRIMARY KEY,
    ref int NOT NULL,
    FOREIGN KEY (ref) REFERENCES rv_rewrite_parent_a (id),
    FOREIGN KEY (ref) REFERENCES rv_rewrite_parent_b (id)
);
INSERT INTO rv_rewrite_parent_a VALUES (1);
INSERT INTO rv_rewrite_parent_b VALUES (1);
INSERT INTO rv_rewrite_child VALUES (10, 1);

CREATE VIEW rv_rewrite_parent_v AS SELECT id FROM rv_rewrite_parent_a;
CREATE VIEW rv_rewrite_child_v AS SELECT cid, ref FROM rv_rewrite_child;
CREATE VIEW rv_rewrite_dep AS
SELECT p.id AS parent_id, c.cid AS child_id
FROM rv_rewrite_parent_v p
-- accepted
JOIN rv_rewrite_child_v c FOR KEY (ref) -> p (id);

-- accepted: re-targeting the base view re-pins the dependent proof to
-- parent_b's FK; pg_depend is reconciled to the re-derived evidence
CREATE OR REPLACE VIEW rv_rewrite_parent_v AS SELECT id FROM rv_rewrite_parent_b;

DROP VIEW rv_rewrite_dep;
CREATE OR REPLACE VIEW rv_rewrite_parent_v AS SELECT id FROM rv_rewrite_parent_b;
CREATE VIEW rv_rewrite_dep AS
SELECT p.id AS parent_id, c.cid AS child_id
FROM rv_rewrite_parent_v p
-- accepted
JOIN rv_rewrite_child_v c FOR KEY (ref) -> p (id);

SELECT * FROM rv_rewrite_dep ORDER BY parent_id, child_id;

DROP VIEW rv_rewrite_dep, rv_rewrite_child_v, rv_rewrite_parent_v;
DROP TABLE rv_rewrite_child, rv_rewrite_parent_b, rv_rewrite_parent_a;

-- Revalidation rejects proof switches that would rebuild executable key
-- equality with a different operator.  Explicit recreation stores the new
-- proof normally.
CREATE FUNCTION key_join_bucket_cmp(a int, b int) RETURNS int
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT CASE WHEN a / 10 < b / 10 THEN -1
               WHEN a / 10 > b / 10 THEN 1
               ELSE 0 END $$;

CREATE FUNCTION key_join_bucket_eq(a int, b int) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a / 10 = b / 10 $$;

CREATE OPERATOR =# (
    LEFTARG = int,
    RIGHTARG = int,
    FUNCTION = key_join_bucket_eq,
    COMMUTATOR = =#,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES
);

CREATE OPERATOR CLASS key_join_bucket_int4_ops
FOR TYPE int USING btree AS
    OPERATOR 3 =# (int, int),
    FUNCTION 1 key_join_bucket_cmp(int, int);

CREATE TABLE rv_sound_parent_a (id int PRIMARY KEY);
CREATE TABLE rv_sound_parent_b (id int);
CREATE UNIQUE INDEX rv_sound_parent_b_idx
    ON rv_sound_parent_b USING btree (id key_join_bucket_int4_ops);
CREATE TABLE rv_sound_child
(
    cid int PRIMARY KEY,
    pid int NOT NULL,
    FOREIGN KEY (pid) REFERENCES rv_sound_parent_a (id),
    FOREIGN KEY (pid) REFERENCES rv_sound_parent_b (id)
);
INSERT INTO rv_sound_parent_a VALUES (11);
INSERT INTO rv_sound_parent_b VALUES (10);
INSERT INTO rv_sound_child VALUES (1, 11);

CREATE VIEW rv_sound_parent_v AS SELECT id FROM rv_sound_parent_a;
CREATE VIEW rv_sound_child_v AS SELECT cid, pid FROM rv_sound_child;
CREATE VIEW rv_sound_dep AS
SELECT p.id AS parent_id, c.cid AS child_id, c.pid
FROM rv_sound_parent_v p
-- accepted
JOIN rv_sound_child_v c FOR KEY (pid) -> p (id);

-- rejected, reason: replay would change executable key equality
CREATE OR REPLACE VIEW rv_sound_parent_v AS SELECT id FROM rv_sound_parent_b;

SELECT * FROM rv_sound_dep ORDER BY parent_id, child_id;

DROP VIEW rv_sound_dep;
CREATE OR REPLACE VIEW rv_sound_parent_v AS SELECT id FROM rv_sound_parent_b;
CREATE VIEW rv_sound_dep AS
SELECT p.id AS parent_id, c.cid AS child_id, c.pid
FROM rv_sound_parent_v p
-- accepted
JOIN rv_sound_child_v c FOR KEY (pid) -> p (id);
SELECT * FROM rv_sound_dep ORDER BY parent_id, child_id;

DROP VIEW rv_sound_dep;
CREATE OR REPLACE VIEW rv_sound_parent_v AS SELECT id FROM rv_sound_parent_a;

DROP VIEW rv_sound_child_v, rv_sound_parent_v;
DROP TABLE rv_sound_child, rv_sound_parent_b, rv_sound_parent_a;

DROP OPERATOR CLASS key_join_bucket_int4_ops USING btree;
DROP OPERATOR FAMILY key_join_bucket_int4_ops USING btree;
DROP OPERATOR =# (int, int);
DROP FUNCTION key_join_bucket_cmp(int, int);
DROP FUNCTION key_join_bucket_eq(int, int);

-- Valid replacement changes facts that are recomputed for dependent views.
CREATE TABLE rv_refresh_parent
(
    tenant_id int NOT NULL,
    id int NOT NULL,
    PRIMARY KEY (tenant_id, id)
);
CREATE TABLE rv_refresh_child
(
    tenant_id int NOT NULL,
    parent_id int NOT NULL,
    UNIQUE (tenant_id, parent_id),
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES rv_refresh_parent (tenant_id, id)
);
CREATE TABLE rv_refresh_reader
(
    tenant_id int NOT NULL,
    parent_id int NOT NULL,
    FOREIGN KEY (tenant_id, parent_id)
        REFERENCES rv_refresh_parent (tenant_id, id)
);

CREATE VIEW rv_refresh_parent_v AS
SELECT tenant_id, id FROM rv_refresh_parent;
CREATE VIEW rv_refresh_child_v AS
SELECT tenant_id, parent_id FROM rv_refresh_child WHERE tenant_id = 1;

CREATE VIEW rv_refresh_dep AS
SELECT p.tenant_id, p.id
FROM rv_refresh_parent_v p
-- accepted
LEFT JOIN rv_refresh_child_v c FOR KEY (tenant_id, parent_id) -> p (tenant_id, id);

-- This replacement remains valid for rv_refresh_dep, but recomputing its
-- output facts now yields conditional row coverage.
CREATE OR REPLACE VIEW rv_refresh_parent_v AS
SELECT tenant_id, id FROM rv_refresh_parent WHERE tenant_id = 1;

-- rejected, reason: recomputed dependent facts now require the tenant filter
SELECT *
FROM rv_refresh_reader r
JOIN rv_refresh_dep d FOR KEY (tenant_id, id) <- r (tenant_id, parent_id);

-- accepted, reason: referencing-side tenant filter matches the recomputed fact
SELECT *
FROM (SELECT * FROM rv_refresh_reader WHERE tenant_id = 1) r
JOIN rv_refresh_dep d FOR KEY (tenant_id, id) <- r (tenant_id, parent_id);

DROP VIEW rv_refresh_dep, rv_refresh_child_v, rv_refresh_parent_v;
DROP TABLE rv_refresh_reader, rv_refresh_child, rv_refresh_parent;

-- Stored key joins activate FK provenance from producer views.  The producer
-- view alone does not block dropping the FK.
CREATE TABLE ondemand_fk_parent
(
    id int PRIMARY KEY
);
CREATE TABLE ondemand_fk_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    CONSTRAINT ondemand_fk_child_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES ondemand_fk_parent (id)
);
CREATE VIEW ondemand_fk_child_v AS
SELECT id, parent_id FROM ondemand_fk_child;
ALTER TABLE ondemand_fk_child DROP CONSTRAINT ondemand_fk_child_parent_id_fkey;
ALTER TABLE ondemand_fk_child ADD CONSTRAINT ondemand_fk_child_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES ondemand_fk_parent (id);
CREATE VIEW ondemand_fk_consumer AS
SELECT p.id AS parent_id, c.id AS child_id, c.parent_id AS child_parent_id
FROM ondemand_fk_parent p
-- accepted
JOIN ondemand_fk_child_v c FOR KEY (parent_id) -> p (id);
ALTER TABLE ondemand_fk_child DROP CONSTRAINT ondemand_fk_child_parent_id_fkey; -- rejected, reason: consumer view depends on the FK proof
DROP VIEW ondemand_fk_consumer;
ALTER TABLE ondemand_fk_child DROP CONSTRAINT ondemand_fk_child_parent_id_fkey;
SELECT *
FROM ondemand_fk_parent p
-- rejected, reason: view facts are recomputed after the FK constraint was dropped
JOIN ondemand_fk_child_v c FOR KEY (parent_id) -> p (id);
DROP VIEW ondemand_fk_child_v;
DROP TABLE ondemand_fk_child, ondemand_fk_parent;

-- Revalidation may accept dependency shrinkage without rewriting a stored
-- dependent view.
CREATE TABLE rv_shrink_parent
(
    id int PRIMARY KEY
);
CREATE TABLE rv_shrink_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rv_shrink_parent (id)
);
CREATE TABLE rv_shrink_one_child
(
    id int PRIMARY KEY,
    parent_id int UNIQUE NOT NULL REFERENCES rv_shrink_parent (id)
);
INSERT INTO rv_shrink_parent VALUES (1), (2);
INSERT INTO rv_shrink_child VALUES (10, 1), (20, 2);
INSERT INTO rv_shrink_one_child VALUES (100, 1);

CREATE VIEW rv_shrink_parent_v AS
SELECT p.id
FROM rv_shrink_parent p
LEFT JOIN rv_shrink_one_child oc FOR KEY (parent_id) -> p (id);

CREATE VIEW rv_shrink_dep AS
SELECT p.id AS parent_id
FROM rv_shrink_parent_v p
JOIN rv_shrink_child c FOR KEY (parent_id) -> p (id);

CREATE OR REPLACE VIEW rv_shrink_parent_v AS
SELECT id FROM rv_shrink_parent;

SELECT count(*) AS rv_shrink_rows FROM rv_shrink_dep;

DROP VIEW rv_shrink_dep, rv_shrink_parent_v;
DROP TABLE rv_shrink_one_child, rv_shrink_child, rv_shrink_parent;

-- Revalidation must reject a replay that would require a new not-null
-- proof dependency.
CREATE TABLE rv_new_nn_parent
(
    id int PRIMARY KEY
);
CREATE TABLE rv_new_nn_child_a
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rv_new_nn_parent (id)
);
CREATE TABLE rv_new_nn_child_b
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rv_new_nn_parent (id)
);
INSERT INTO rv_new_nn_parent VALUES (1), (2);
INSERT INTO rv_new_nn_child_a VALUES (10, 1);
INSERT INTO rv_new_nn_child_b VALUES (20, 2);

CREATE VIEW rv_new_nn_child_v AS
SELECT id, parent_id FROM rv_new_nn_child_a;

CREATE VIEW rv_new_nn_dep AS
SELECT c.id AS child_id, c.parent_id
FROM rv_new_nn_parent p
JOIN rv_new_nn_child_v c FOR KEY (parent_id) -> p (id);

-- accepted: the dependent proof re-pins to child_b's NOT NULL and FK
-- evidence; pg_depend is reconciled and the view now reads child_b
CREATE OR REPLACE VIEW rv_new_nn_child_v AS
SELECT id, parent_id FROM rv_new_nn_child_b;

SELECT * FROM rv_new_nn_dep ORDER BY child_id;

DROP VIEW rv_new_nn_dep, rv_new_nn_child_v;
DROP TABLE rv_new_nn_child_b, rv_new_nn_child_a, rv_new_nn_parent;

-- Stored key joins must be revalidated when FK proof flags change.
CREATE TABLE alter_fk_parent
(
    id int PRIMARY KEY
);
CREATE TABLE alter_fk_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    CONSTRAINT alter_fk_child_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES alter_fk_parent (id)
);
INSERT INTO alter_fk_parent VALUES (1);
INSERT INTO alter_fk_child VALUES (10, 1);

CREATE VIEW alter_fk_consumer AS
SELECT p.id AS parent_id, c.id AS child_id
FROM alter_fk_parent p
-- accepted
JOIN alter_fk_child c FOR KEY (parent_id) -> p (id);

-- rejected, reason: stored view depends on the FK proof
ALTER TABLE alter_fk_child
    ALTER CONSTRAINT alter_fk_child_parent_id_fkey NOT ENFORCED;
SELECT * FROM alter_fk_consumer ORDER BY child_id;
-- rejected, reason: stored view depends on the non-deferrable FK proof
ALTER TABLE alter_fk_child
    ALTER CONSTRAINT alter_fk_child_parent_id_fkey
    DEFERRABLE INITIALLY IMMEDIATE;
DROP VIEW alter_fk_consumer;

CREATE VIEW alter_fk_scalar_consumer AS
SELECT (
    SELECT count(*)
    FROM alter_fk_parent p
    -- accepted
    JOIN alter_fk_child c FOR KEY (parent_id) -> p (id)
) AS key_join_rows;
-- rejected, reason: dependency scanning sees the scalar subquery proof
ALTER TABLE alter_fk_child DROP CONSTRAINT alter_fk_child_parent_id_fkey;
-- rejected, reason: stored scalar subquery proof depends on the FK proof
ALTER TABLE alter_fk_child
    ALTER CONSTRAINT alter_fk_child_parent_id_fkey NOT ENFORCED;
DROP VIEW alter_fk_scalar_consumer;

ALTER TABLE alter_fk_child
    ALTER CONSTRAINT alter_fk_child_parent_id_fkey NOT ENFORCED;
SELECT p.id AS parent_id, c.id AS child_id
FROM alter_fk_parent p
-- rejected, reason: FK is now not enforced
JOIN alter_fk_child c FOR KEY (parent_id) -> p (id);
DROP TABLE alter_fk_child, alter_fk_parent;

-- Key join follows PostgreSQL's catalog-level FK validity, not current
-- RI trigger firing state.
CREATE TABLE trigger_bypass_parent (id int PRIMARY KEY);
CREATE TABLE trigger_bypass_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    CONSTRAINT trigger_bypass_child_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES trigger_bypass_parent (id)
);
INSERT INTO trigger_bypass_parent VALUES (1);
INSERT INTO trigger_bypass_child VALUES (10, 1);
ALTER TABLE trigger_bypass_child DISABLE TRIGGER ALL;
ALTER TABLE trigger_bypass_parent DISABLE TRIGGER ALL;
SELECT count(*) AS key_join_rows
FROM trigger_bypass_parent p
JOIN trigger_bypass_child c FOR KEY (parent_id) -> p (id);
ALTER TABLE trigger_bypass_child ENABLE TRIGGER ALL;
ALTER TABLE trigger_bypass_parent ENABLE TRIGGER ALL;
DROP TABLE trigger_bypass_child, trigger_bypass_parent;

-- session_replication_role also does not affect key join proof validity.
CREATE TABLE trigger_bypass_repl_parent (id int PRIMARY KEY);
CREATE TABLE trigger_bypass_repl_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    CONSTRAINT trigger_bypass_repl_child_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES trigger_bypass_repl_parent (id)
);
INSERT INTO trigger_bypass_repl_parent VALUES (1);
INSERT INTO trigger_bypass_repl_child VALUES (10, 1);
SET session_replication_role = replica;
SELECT count(*) AS key_join_rows
FROM trigger_bypass_repl_parent p
JOIN trigger_bypass_repl_child c FOR KEY (parent_id) -> p (id);
RESET session_replication_role;
DROP TABLE trigger_bypass_repl_child, trigger_bypass_repl_parent;

-- Consumed query-shape uniqueness is revalidated on dependent view
-- replacement.
CREATE TABLE rv_unique_parent
(
    id int PRIMARY KEY
);
CREATE TABLE rv_unique_child
(
    parent_id int NOT NULL REFERENCES rv_unique_parent (id)
);
CREATE VIEW rv_unique_parent_v AS
SELECT id FROM rv_unique_parent GROUP BY id;
CREATE VIEW rv_unique_dep AS
SELECT *
FROM rv_unique_parent_v p
-- accepted
JOIN rv_unique_child c FOR KEY (parent_id) -> p (id);
-- rejected, reason: replacement removes consumed query-shape uniqueness and row coverage
CREATE OR REPLACE VIEW rv_unique_parent_v AS
SELECT p.id FROM rv_unique_parent p CROSS JOIN (VALUES (1), (2)) AS g(n);
DROP VIEW rv_unique_dep, rv_unique_parent_v;
DROP TABLE rv_unique_child, rv_unique_parent;

-- Bare unique indexes are uniqueness sources for surface facts, but producer
-- views do not store those facts as DDL-blocking dependencies.
CREATE TABLE bare_unique_parent
(
    id int NOT NULL
);
CREATE UNIQUE INDEX bare_unique_parent_id_idx ON bare_unique_parent (id);
CREATE VIEW bare_unique_parent_v AS
SELECT id FROM bare_unique_parent;
DROP INDEX bare_unique_parent_id_idx;
DROP VIEW bare_unique_parent_v;
DROP TABLE bare_unique_parent;

-- Adding an inheritance child to a parent that backs a stored key-join
-- proof must revalidate dependent views and abort if the proof breaks.
CREATE TABLE inh_kj_parent (id int PRIMARY KEY);
CREATE TABLE inh_kj_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES inh_kj_parent (id)
);
INSERT INTO inh_kj_parent VALUES (1);
INSERT INTO inh_kj_child VALUES (10, 1);

CREATE VIEW inh_kj_v AS
SELECT p.id AS parent_id, c.id AS child_id
FROM inh_kj_parent p
-- accepted
JOIN inh_kj_child c FOR KEY (parent_id) -> p (id);

-- rejected, reason: stored view depends on parent having no subclasses
CREATE TABLE inh_kj_subclass () INHERITS (inh_kj_parent);

CREATE TABLE inh_kj_outsider (id int NOT NULL);
-- rejected, reason: same dependency, ALTER TABLE INHERIT path
ALTER TABLE inh_kj_outsider INHERIT inh_kj_parent;

-- After dropping the dependent view both DDL paths succeed.
DROP VIEW inh_kj_v;
CREATE TABLE inh_kj_subclass () INHERITS (inh_kj_parent);
ALTER TABLE inh_kj_outsider INHERIT inh_kj_parent;
DROP TABLE inh_kj_outsider, inh_kj_subclass, inh_kj_child, inh_kj_parent;

-- Partitioned-table parent: ATTACH PARTITION must NOT trigger a false
-- positive, because the parent's unique constraint is enforced across
-- partitions.
CREATE TABLE inh_kj_pparent (id int PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE inh_kj_ppart0 PARTITION OF inh_kj_pparent
    FOR VALUES FROM (0) TO (100);
CREATE TABLE inh_kj_pchild
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES inh_kj_pparent (id)
);
INSERT INTO inh_kj_pparent VALUES (1);
INSERT INTO inh_kj_pchild VALUES (10, 1);
CREATE VIEW inh_kj_pv AS
SELECT p.id AS parent_id, c.id AS child_id
FROM inh_kj_pparent p
-- accepted
JOIN inh_kj_pchild c FOR KEY (parent_id) -> p (id);
-- accepted: attaching another partition leaves the proof valid because
-- partitioned-table parents enforce uniqueness across partitions.
CREATE TABLE inh_kj_ppart1 PARTITION OF inh_kj_pparent
    FOR VALUES FROM (100) TO (200);
SELECT * FROM inh_kj_pv ORDER BY child_id;
DROP VIEW inh_kj_pv;
DROP TABLE inh_kj_pchild, inh_kj_pparent;

-- ALTER TABLE ENABLE ROW LEVEL SECURITY must revalidate stored key-join
-- proofs.  RLS-protected relations expose no key-join facts, so a stored
-- proof against a relation that gains RLS becomes unprovable.
CREATE TABLE rls_kj_parent (id int PRIMARY KEY);
CREATE TABLE rls_kj_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES rls_kj_parent (id)
);
INSERT INTO rls_kj_parent VALUES (1);
INSERT INTO rls_kj_child VALUES (10, 1);

CREATE VIEW rls_kj_v AS
SELECT p.id AS parent_id, c.id AS child_id
FROM rls_kj_parent p
-- accepted
JOIN rls_kj_child c FOR KEY (parent_id) -> p (id);

-- rejected, reason: stored view depends on RLS-free referenced surface
ALTER TABLE rls_kj_parent ENABLE ROW LEVEL SECURITY;
-- rejected, reason: same dependency, referencing side too
ALTER TABLE rls_kj_child ENABLE ROW LEVEL SECURITY;

-- After dropping the view, ENABLE ROW LEVEL SECURITY succeeds.
DROP VIEW rls_kj_v;
ALTER TABLE rls_kj_parent ENABLE ROW LEVEL SECURITY;
-- Repeating the same setting does not need key-join revalidation.
ALTER TABLE rls_kj_parent ENABLE ROW LEVEL SECURITY;
ALTER TABLE rls_kj_parent DISABLE ROW LEVEL SECURITY;
DROP TABLE rls_kj_child, rls_kj_parent;

-- ALTER FUNCTION and CREATE OR REPLACE FUNCTION on a function used in
-- a stored matched-filter conjunct must revalidate dependent stored
-- key-join proofs.  The proof's contain_volatile_functions check is
-- decided at parse time; a later volatility, strictness, or body change
-- can break the matched-filter contract.
CREATE FUNCTION pf_kj_pick() RETURNS int
    LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN 1; END; $$;
CREATE TABLE pf_kj_p (id int PRIMARY KEY);
CREATE TABLE pf_kj_r (id int NOT NULL REFERENCES pf_kj_p (id));
INSERT INTO pf_kj_p VALUES (1);
INSERT INTO pf_kj_r VALUES (1);
CREATE VIEW pf_kj_vp AS SELECT id FROM pf_kj_p WHERE id = pf_kj_pick();
CREATE VIEW pf_kj_vr AS SELECT id FROM pf_kj_r WHERE id = pf_kj_pick();
CREATE VIEW pf_kj_dep AS
SELECT pf_kj_vp.id FROM pf_kj_vp
-- accepted
JOIN pf_kj_vr FOR KEY (id) -> pf_kj_vp (id);

-- rejected, reason: stored proof depends on the function being non-volatile
ALTER FUNCTION pf_kj_pick() VOLATILE;
-- rejected, reason: CREATE OR REPLACE may change body or volatility
CREATE OR REPLACE FUNCTION pf_kj_pick() RETURNS int
    LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN 2; END; $$;

-- After dropping the consumer view, both DDL paths succeed.
DROP VIEW pf_kj_dep;
ALTER FUNCTION pf_kj_pick() VOLATILE;
CREATE OR REPLACE FUNCTION pf_kj_pick() RETURNS int
    LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN 2; END; $$;
DROP VIEW pf_kj_vp, pf_kj_vr;
DROP TABLE pf_kj_r, pf_kj_p;
DROP FUNCTION pf_kj_pick();

-- CoerceViaIO proof filters must depend on hidden type I/O functions too.
-- Otherwise ALTER FUNCTION can make a stored proof stale without revalidation.
SET client_min_messages = warning;
CREATE TYPE pf_kj_io_type;
CREATE FUNCTION pf_kj_io_type_in(cstring)
RETURNS pf_kj_io_type
LANGUAGE internal STRICT IMMUTABLE AS 'int4in';
CREATE FUNCTION pf_kj_io_type_out(pf_kj_io_type)
RETURNS cstring
LANGUAGE internal STRICT IMMUTABLE AS 'int4out';
CREATE TYPE pf_kj_io_type (
    input = pf_kj_io_type_in,
    output = pf_kj_io_type_out,
    like = int4
);
RESET client_min_messages;
CREATE CAST (pf_kj_io_type AS integer) WITH INOUT;
CREATE TABLE pf_kj_io_p (id int PRIMARY KEY);
CREATE TABLE pf_kj_io_r (id int NOT NULL REFERENCES pf_kj_io_p (id));
INSERT INTO pf_kj_io_p VALUES (1), (2);
INSERT INTO pf_kj_io_r VALUES (1);
CREATE VIEW pf_kj_io_vp AS
SELECT id FROM pf_kj_io_p WHERE id = ('1'::pf_kj_io_type)::integer;
CREATE VIEW pf_kj_io_vr AS
SELECT id FROM pf_kj_io_r WHERE id = ('1'::pf_kj_io_type)::integer;
CREATE VIEW pf_kj_io_dep AS
SELECT pf_kj_io_vp.id FROM pf_kj_io_vp
-- accepted
JOIN pf_kj_io_vr FOR KEY (id) -> pf_kj_io_vp (id);

-- rejected, reason: stored proof depends on CoerceViaIO output being non-volatile
ALTER FUNCTION pf_kj_io_type_out(pf_kj_io_type) VOLATILE;
-- rejected, reason: CREATE OR REPLACE may change CoerceViaIO output volatility
CREATE OR REPLACE FUNCTION pf_kj_io_type_out(pf_kj_io_type)
RETURNS cstring
LANGUAGE internal STRICT VOLATILE AS 'int4out';

-- After dropping the consumer view, both DDL paths succeed.
DROP VIEW pf_kj_io_dep;
ALTER FUNCTION pf_kj_io_type_out(pf_kj_io_type) VOLATILE;
CREATE OR REPLACE FUNCTION pf_kj_io_type_out(pf_kj_io_type)
RETURNS cstring
LANGUAGE internal STRICT VOLATILE AS 'int4out';
DROP VIEW pf_kj_io_vp, pf_kj_io_vr;
DROP TABLE pf_kj_io_r, pf_kj_io_p;
DROP CAST (pf_kj_io_type AS integer);
SET client_min_messages = warning;
DROP TYPE pf_kj_io_type CASCADE;
RESET client_min_messages;

-- Producer-side DDL may affect key-join proofs stored in dependent
-- objects owned by other roles.  If those proofs still validate and
-- require no new dependencies, the producer owner can replace the
-- producer view without owning the dependent view.
CREATE ROLE regress_kj_cross_owner_producer;
CREATE ROLE regress_kj_cross_owner_consumer;
CREATE SCHEMA kj_cross_owner_p AUTHORIZATION regress_kj_cross_owner_producer;
CREATE SCHEMA kj_cross_owner_c AUTHORIZATION regress_kj_cross_owner_consumer;

SET ROLE regress_kj_cross_owner_producer;
CREATE TABLE kj_cross_owner_p.parent (id int PRIMARY KEY);
CREATE TABLE kj_cross_owner_p.child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES kj_cross_owner_p.parent (id)
);
CREATE VIEW kj_cross_owner_p.parent_v AS
SELECT id FROM kj_cross_owner_p.parent;
GRANT USAGE ON SCHEMA kj_cross_owner_p TO regress_kj_cross_owner_consumer;
GRANT SELECT ON kj_cross_owner_p.parent_v TO regress_kj_cross_owner_consumer;
GRANT SELECT ON kj_cross_owner_p.child TO regress_kj_cross_owner_consumer;
RESET ROLE;

SET ROLE regress_kj_cross_owner_consumer;
CREATE VIEW kj_cross_owner_c.dep AS
SELECT p.id AS parent_id, c.id AS child_id
FROM kj_cross_owner_p.parent_v p
-- accepted
JOIN kj_cross_owner_p.child c FOR KEY (parent_id) -> p (id);
RESET ROLE;

SET ROLE regress_kj_cross_owner_producer;
CREATE OR REPLACE VIEW kj_cross_owner_p.parent_v AS
SELECT id FROM kj_cross_owner_p.parent;
RESET ROLE;

DROP SCHEMA kj_cross_owner_c CASCADE;
DROP SCHEMA kj_cross_owner_p CASCADE;
DROP ROLE regress_kj_cross_owner_consumer;
DROP ROLE regress_kj_cross_owner_producer;

-- A key join can preserve referenced-side uniqueness only when the
-- referencing side is unique.  Stored proofs that consume the projected
-- uniqueness must depend on that referencing-side unique proof.
CREATE TABLE output_unique_kj_parent (id int PRIMARY KEY);
CREATE TABLE output_unique_kj_child
(
    id int PRIMARY KEY,
    parent_id int NOT NULL,
    CONSTRAINT output_unique_kj_child_parent_id_key UNIQUE (parent_id),
    CONSTRAINT output_unique_kj_child_parent_id_fkey
        FOREIGN KEY (parent_id) REFERENCES output_unique_kj_parent (id)
);
CREATE TABLE output_unique_kj_grandchild
(
    id int PRIMARY KEY,
    parent_id int NOT NULL REFERENCES output_unique_kj_parent (id)
);
INSERT INTO output_unique_kj_parent VALUES (1), (2);
INSERT INTO output_unique_kj_child VALUES (10, 1);
INSERT INTO output_unique_kj_grandchild VALUES (100, 1), (200, 2);
CREATE VIEW output_unique_kj_view AS
SELECT q.parent_id, q.child_id, gc.id AS grandchild_id
FROM (
    SELECT p.id AS parent_id, c.id AS child_id
    FROM output_unique_kj_parent p
    LEFT JOIN output_unique_kj_child c FOR KEY (parent_id) -> p (id)
) q
-- accepted because output_unique_kj_child.parent_id is unique
JOIN output_unique_kj_grandchild gc FOR KEY (parent_id) -> q (parent_id);

-- rejected, reason: the stored view depends on the referencing-side uniqueness
-- that preserved q.parent_id uniqueness.
ALTER TABLE output_unique_kj_child
    DROP CONSTRAINT output_unique_kj_child_parent_id_key;

-- After dropping the consumer view, the same DDL succeeds and the raw nested
-- key join is no longer provable.
DROP VIEW output_unique_kj_view;
ALTER TABLE output_unique_kj_child
    DROP CONSTRAINT output_unique_kj_child_parent_id_key;
SELECT q.parent_id, q.child_id, gc.id AS grandchild_id
FROM (
    SELECT p.id AS parent_id, c.id AS child_id
    FROM output_unique_kj_parent p
    LEFT JOIN output_unique_kj_child c FOR KEY (parent_id) -> p (id)
) q
JOIN output_unique_kj_grandchild gc
    FOR KEY (parent_id) -> q (parent_id);
DROP TABLE output_unique_kj_grandchild,
           output_unique_kj_child,
           output_unique_kj_parent;

-- Recreate the diagnostics schema and fixtures (the diagnostics section above dropped them).
CREATE SCHEMA key_join_diag;
SET search_path = key_join_diag;
CREATE TABLE kd_parent (id int PRIMARY KEY);
CREATE TABLE kd_child (id int, parent_id int REFERENCES kd_parent (id));
CREATE TABLE kd_unique_child
(
    id int,
    parent_id int UNIQUE NOT NULL REFERENCES kd_parent (id)
);

-- Inactivated row coverage inside a view: name the view with the preceding join.
CREATE VIEW kd_parent_joined AS
SELECT p.id
FROM kd_parent p
JOIN kd_unique_child u FOR KEY (parent_id) -> p (id);

SELECT *
FROM kd_parent_joined p
JOIN kd_child c FOR KEY (parent_id) -> p (id);

DROP VIEW kd_parent_joined;

RESET search_path;
DROP SCHEMA key_join_diag CASCADE;

-- Final cleanup: drop all remaining objects
DROP VIEW v1, v2;
DROP VIEW orders_by_country;
DROP TABLE shipments, orders, order_items, packages CASCADE;
DROP TABLE customer_addresses CASCADE;
DROP TABLE addresses, customers CASCADE;
DROP TABLE resupplies_gbu, sales_gbu, products_gbu CASCADE;
DROP FUNCTION t2();
DROP TABLE t11, t10, t9, t8, t7, t6, t5, t4, t3, t2, t1 CASCADE;

-- Leave a stored FOR KEY proof behind on purpose: the pg_upgrade test dumps
-- and restores the regression database, and the proof's 'k' edges must make
-- pg_dump emit the view after the foreign key and primary key below.
CREATE TABLE keyjoin_dump_parent (
    kdp_id int PRIMARY KEY
);
CREATE TABLE keyjoin_dump_child (
    kdc_id int PRIMARY KEY,
    kdc_parent_id int NOT NULL REFERENCES keyjoin_dump_parent (kdp_id)
);
CREATE VIEW keyjoin_dump_v AS
SELECT c.kdc_id, p.kdp_id
FROM keyjoin_dump_parent p
-- accepted
JOIN keyjoin_dump_child c FOR KEY (kdc_parent_id) -> p (kdp_id);
SELECT * FROM keyjoin_dump_v;
