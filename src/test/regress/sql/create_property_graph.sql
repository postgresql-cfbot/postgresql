CREATE SCHEMA create_property_graph_tests;
GRANT USAGE ON SCHEMA create_property_graph_tests TO PUBLIC;
SET search_path = create_property_graph_tests;

CREATE ROLE regress_graph_user1;
CREATE ROLE regress_graph_user2;

CREATE PROPERTY GRAPH g1;

COMMENT ON PROPERTY GRAPH g1 IS 'a graph';

CREATE PROPERTY GRAPH g1;  -- error: duplicate

CREATE TABLE t1 (a int, b text);
CREATE TABLE t2 (i int PRIMARY KEY, j int, k int);
CREATE TABLE t3 (x int, y text, z text);

CREATE TABLE e1 (a int, i int, t text, PRIMARY KEY (a, i));
CREATE TABLE e2 (a int, x int, t text);

CREATE PROPERTY GRAPH g2
    VERTEX TABLES (t1 KEY (a), t2 DEFAULT LABEL, t3 KEY (x) LABEL t3l1 LABEL t3l2)
    EDGE TABLES (
        e1
            SOURCE KEY (a) REFERENCES t1 (a)
            DESTINATION KEY (i) REFERENCES t2 (i),
        e2 KEY (a, x)
            SOURCE KEY (a) REFERENCES t1 (a)
            DESTINATION KEY (x, t) REFERENCES t3 (x, y)
    );

-- test dependencies/object descriptions

DROP TABLE t1;  -- fail
ALTER TABLE t1 DROP COLUMN b;  -- non-key column; fail
ALTER TABLE t1 DROP COLUMN a;  -- key column; fail

-- like g2 but assembled with ALTER
CREATE PROPERTY GRAPH g3;
ALTER PROPERTY GRAPH g3 ADD VERTEX TABLES (t1 KEY (a), t2 DEFAULT LABEL);
ALTER PROPERTY GRAPH g3
    ADD VERTEX TABLES (t3 KEY (x) LABEL t3l1)
    ADD EDGE TABLES (
        e1 SOURCE KEY (a) REFERENCES t1 (a) DESTINATION KEY (i) REFERENCES t2 (i),
        e2 KEY (a, x) SOURCE KEY (a) REFERENCES t1 (a) DESTINATION KEY (x, t) REFERENCES t3 (x, y)
    );
ALTER PROPERTY GRAPH g3 ALTER VERTEX TABLE t3 ADD LABEL t3l2 PROPERTIES ALL COLUMNS ADD LABEL t3l3 PROPERTIES ALL COLUMNS;
ALTER PROPERTY GRAPH g3 ALTER VERTEX TABLE t3 DROP LABEL t3l3x;  -- error
ALTER PROPERTY GRAPH g3 ALTER VERTEX TABLE t3 DROP LABEL t3l3;
ALTER PROPERTY GRAPH g3 DROP VERTEX TABLES (t2);  -- fail (TODO: dubious error message)
ALTER PROPERTY GRAPH g3 DROP VERTEX TABLES (t2) CASCADE;
ALTER PROPERTY GRAPH g3 DROP EDGE TABLES (e2);

CREATE PROPERTY GRAPH g4
    VERTEX TABLES (
        t1 KEY (a) NO PROPERTIES,
        t2 DEFAULT LABEL PROPERTIES (i + j AS i_j, k),
        t3 KEY (x) LABEL t3l1 PROPERTIES (x, y AS yy) LABEL t3l2 PROPERTIES (x, z AS zz)
    )
    EDGE TABLES (
        e1
            SOURCE KEY (a) REFERENCES t1 (a)
            DESTINATION KEY (i) REFERENCES t2 (i)
            PROPERTIES ALL COLUMNS,
        e2 KEY (a, x)
            SOURCE KEY (a) REFERENCES t1 (a)
            DESTINATION KEY (x, t) REFERENCES t3 (x, y)
            PROPERTIES ALL COLUMNS
    );

ALTER PROPERTY GRAPH g4 ALTER VERTEX TABLE t2 ALTER LABEL t2 ADD PROPERTIES (k * 2 AS kk);
ALTER PROPERTY GRAPH g4 ALTER VERTEX TABLE t2 ALTER LABEL t2 DROP PROPERTIES (k);

CREATE TABLE t11 (a int PRIMARY KEY);
CREATE TABLE t12 (b int PRIMARY KEY);
CREATE TABLE t13 (
    c int PRIMARY KEY,
    d int REFERENCES t11,
    e int REFERENCES t12
);

CREATE PROPERTY GRAPH g5
    VERTEX TABLES (t11, t12)
    EDGE TABLES (t13 SOURCE t11 DESTINATION t12);

SELECT pg_get_propgraphdef('g5'::regclass);

-- error cases
CREATE PROPERTY GRAPH gx VERTEX TABLES (xx, yy);
CREATE PROPERTY GRAPH gx VERTEX TABLES (t1 KEY (a), t2 KEY (i), t1 KEY (a));
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (t1 AS tt KEY (a), t2 KEY (i))
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION t2
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (t1 KEY (a), t2 KEY (i))
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION tx
    );
COMMENT ON PROPERTY GRAPH gx IS 'not a graph';
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (t1 KEY (a), t2)
    EDGE TABLES (
        e1 SOURCE t1 DESTINATION t2  -- no foreign keys
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (
        t1 KEY (a) LABEL foo PROPERTIES (a + 1 AS aa)
                   LABEL bar PROPERTIES (1 + a AS aa)  -- expression mismatch
    );
ALTER PROPERTY GRAPH g2
    ADD VERTEX TABLES (
        t1 AS t1x KEY (a) LABEL foo PROPERTIES (a + 1 AS aa)
                          LABEL bar PROPERTIES (1 + a AS aa)  -- expression mismatch
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (
        t1 KEY (a) PROPERTIES (b AS p1),
        t2 PROPERTIES (k AS p1)  -- type mismatch
    );
ALTER PROPERTY GRAPH g2 ALTER VERTEX TABLE t1 ADD LABEL foo PROPERTIES (b AS k);  -- type mismatch

CREATE PROPERTY GRAPH gx
    VERTEX TABLES (
        t1 KEY (a) LABEL l1 PROPERTIES (a, a AS aa),
        t2 KEY (i) LABEL l1 PROPERTIES (i AS a, j AS b, k)  -- mismatching number of properties on label
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (
        t1 KEY (a) LABEL l1 PROPERTIES (a, b),
        t2 KEY (i) LABEL l1 PROPERTIES (i AS a)  -- mismatching number of properties on label
    );
CREATE PROPERTY GRAPH gx
    VERTEX TABLES (
        t1 KEY (a) LABEL l1 PROPERTIES (a, b),
        t2 KEY (i) LABEL l1 PROPERTIES (i AS a, j AS j)  -- mismatching property names on label
    );
ALTER PROPERTY GRAPH g4 ALTER VERTEX TABLE t1 ADD LABEL t3l1 PROPERTIES (a AS x, b AS yy, b AS zz);  -- mismatching number of properties on label
ALTER PROPERTY GRAPH g4 ALTER VERTEX TABLE t1 ADD LABEL t3l1 PROPERTIES (a AS x, b AS zz);  -- mismatching property names on label
ALTER PROPERTY GRAPH g4 ALTER VERTEX TABLE t1 ADD LABEL t3l1 PROPERTIES (a AS x);  -- mismatching number of properties on label


ALTER PROPERTY GRAPH g1 OWNER TO regress_graph_user1;
SET ROLE regress_graph_user1;
GRANT SELECT ON PROPERTY GRAPH g1 TO regress_graph_user2;
GRANT UPDATE ON PROPERTY GRAPH g1 TO regress_graph_user2;  -- fail
RESET ROLE;


-- information schema

SELECT * FROM information_schema.property_graphs ORDER BY property_graph_name;
SELECT * FROM information_schema.pg_element_tables ORDER BY property_graph_name, element_table_alias;
SELECT * FROM information_schema.pg_element_table_key_columns ORDER BY property_graph_name, element_table_alias, ordinal_position;
SELECT * FROM information_schema.pg_edge_table_components ORDER BY property_graph_name, edge_table_alias, edge_end DESC, ordinal_position;
SELECT * FROM information_schema.pg_element_table_labels ORDER BY property_graph_name, element_table_alias, label_name;
SELECT * FROM information_schema.pg_element_table_properties ORDER BY property_graph_name, element_table_alias, property_name;
SELECT * FROM information_schema.pg_label_properties ORDER BY property_graph_name, label_name, property_name;
SELECT * FROM information_schema.pg_labels ORDER BY property_graph_name, label_name;
SELECT * FROM information_schema.pg_property_data_types ORDER BY property_graph_name, property_name;
SELECT * FROM information_schema.pg_property_graph_privileges WHERE grantee LIKE 'regress%' ORDER BY property_graph_name;


\a\t
SELECT pg_get_propgraphdef('g2'::regclass);
SELECT pg_get_propgraphdef('g3'::regclass);
SELECT pg_get_propgraphdef('g4'::regclass);

SELECT pg_get_propgraphdef('pg_type'::regclass);  -- error
\a\t

\dG g1

-- TODO
\d g1
\d+ g1

DROP TABLE g2;  -- error: wrong object type

DROP PROPERTY GRAPH g1;

DROP PROPERTY GRAPH g1;  -- error: does not exist

DROP PROPERTY GRAPH IF EXISTS g1;

-- leave for pg_upgrade/pg_dump tests
--DROP SCHEMA create_property_graph_tests CASCADE;

DROP ROLE regress_graph_user1, regress_graph_user2;
