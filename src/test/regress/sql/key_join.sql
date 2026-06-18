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
-- Derived operands are not supported: FOR KEY joins prove only plain-table
-- operands
--

SELECT *
FROM t1
-- rejected, reason: a subquery operand is not a plain table
JOIN (SELECT c3, c4 FROM t2) AS sub FOR KEY (c3) -> t1 (c1);

SELECT *
FROM t1
JOIN t2 ON c3 = c1
-- rejected, reason: the referenced alias is buried below a preceding join
JOIN t2 AS t2b FOR KEY (c3) -> t1 (c1);

--
-- Test CTEs
--

-- Regression: a FOR KEY join in a sibling CTE of a recursive one.
-- makeDependencyGraph walks every CTE body in the WITH block, so the
-- raw walker must handle KeyJoinClause here too.
WITH RECURSIVE rec_only AS (
    SELECT 1 AS x UNION ALL SELECT x FROM rec_only WHERE false
), kj_sibling AS (
    SELECT t1.c1 FROM t1 JOIN t2 FOR KEY (c3) -> t1 (c1)
)
SELECT * FROM rec_only, kj_sibling;

--
-- Test disallowed filtering of referenced table
--

CREATE VIEW v1 AS
SELECT * FROM t1 WHERE c1 > 0;

CREATE VIEW v2 AS
SELECT * FROM t2 WHERE c3 > 0;

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

-- rejected, reason: referenced column does not exist
SELECT * FROM v1 JOIN t2 FOR KEY (c3, c4) -> v1 (c1_1, nonexistent);

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
SELECT * FROM t1 JOIN pt2_1 FOR KEY (c3) -> t1 (c1);
-- rejected, reason: ONLY scans on partitioned tables are not supported
SELECT * FROM ONLY pt2 JOIN pt3 FOR KEY (c5) -> pt2 (c3);

DROP TABLE pt3;
DROP TABLE pt2;

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

-- Accepted: the corrected FK target still succeeds.
SELECT o.id AS order_id, c.id AS customer_id
FROM kd_sale_order o
JOIN kd_customer c FOR KEY (id) <- o (cid);

DROP TABLE kd_sale_order;
DROP TABLE kd_customs, kd_customer;

RESET search_path;
DROP SCHEMA key_join_diag CASCADE;

-- Final cleanup: drop all remaining objects
DROP VIEW v1, v2;
DROP TABLE shipments, orders, order_items, packages CASCADE;
DROP TABLE resupplies_gbu, sales_gbu, products_gbu CASCADE;
DROP FUNCTION t2();
DROP TABLE t11, t10, t9, t8, t7, t6, t5, t4, t3, t2, t1 CASCADE;
