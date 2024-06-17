CREATE SCHEMA stats_export_import;

CREATE TYPE stats_export_import.complex_type AS (
    a integer,
    b real,
    c text,
    d date,
    e jsonb);

CREATE TABLE stats_export_import.test(
    id INTEGER PRIMARY KEY,
    name text,
    comp stats_export_import.complex_type,
    arange int4range,
    tags text[]
);

SELECT relpages, reltuples, relallvisible FROM pg_class WHERE oid = 'stats_export_import.test'::regclass;

-- error: regclass not found
SELECT pg_set_relation_stats('stats_export_import.nope'::regclass,
                             150000::integer);

-- error: all three params missing
SELECT pg_set_relation_stats('stats_export_import.test'::regclass,
                             150000::integer);

-- error: reltuples, relallvisible missing
SELECT pg_set_relation_stats('stats_export_import.test'::regclass,
                             150000::integer,
                             'relpages', 4::integer);

-- error: null value
SELECT pg_set_relation_stats('stats_export_import.test'::regclass,
                             150000::integer,
                             'relpages', 'nope'::text,
                             'reltuples', NULL::real,
                             'relallvisible', 4::integer);

-- error: bad relpages type
SELECT pg_set_relation_stats('stats_export_import.test'::regclass,
                             150000::integer,
                             'relpages', 'nope'::text,
                             'reltuples', 400.0::real,
                             'relallvisible', 4::integer);

SELECT pg_set_relation_stats('stats_export_import.test'::regclass,
                             150000::integer,
                             'relpages', 17::integer,
                             'reltuples', 400.0::real,
                             'relallvisible', 4::integer);

SELECT relpages, reltuples, relallvisible
FROM pg_class
WHERE oid = 'stats_export_import.test'::regclass;

-- error: object doesn't exist
SELECT pg_catalog.pg_set_attribute_stats(
    '0'::oid,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

-- error: relation null
SELECT pg_catalog.pg_set_attribute_stats(
    NULL::oid,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

-- error: attname null
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    NULL::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

-- error: inherited null
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    NULL::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

-- error: null_frac null
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', NULL::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

-- error: avg_width null
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', NULL::integer,
    'n_distinct', 0.3::real);

-- error: avg_width null
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', NULL::real);

-- ok: no stakinds
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.1::real,
    'avg_width', 2::integer,
    'n_distinct', 0.3::real);

SELECT stanullfrac, stawidth, stadistinct
FROM pg_statistic
WHERE starelid = 'stats_export_import.test'::regclass;

-- warn: mcv / mcf null mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_freqs', '{0.1,0.2,0.3}'::real[]
    );

-- warn: mcv / mcf null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_vals', '{1,2,3}'::text
    );

-- warn: mcv / mcf type mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_vals', '{2,1,3}'::text,
    'most_common_freqs', '{0.2,0.1}'::double precision[]
    );

-- warning: mcv cast failure
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_vals', '{2,four,3}'::text,
    'most_common_freqs', '{0.3,0.25,0.05}'::real[]
    );

-- ok: mcv+mcf
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_vals', '{2,1,3}'::text,
    'most_common_freqs', '{0.3,0.25,0.05}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- warn: histogram elements null value
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'histogram_bounds', '{1,NULL,3,4}'::text
    );

-- ok: histogram_bounds
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'histogram_bounds', '{1,2,3,4}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- ok: correlation
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'correlation', 0.5::real);

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- warn: scalars can't have mcelem
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_elems', '{1,3}'::text,
    'most_common_elem_freqs', '{0.3,0.2,0.2,0.3,0.0}'::real[]
    );

-- warn: mcelem / mcelem mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'tags'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_elems', '{one,two}'::text
    );

-- warn: mcelem / mcelem null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'tags'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_elem_freqs', '{0.3,0.2,0.2,0.3}'::real[]
    );

-- ok: mcelem
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'tags'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_elems', '{one,three}'::text,
    'most_common_elem_freqs', '{0.3,0.2,0.2,0.3,0.0}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'tags';

-- warn: scalars can't have elem_count_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'elem_count_histogram', '{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );
-- warn: elem_count_histogram null element
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'tags'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'elem_count_histogram', '{1,1,NULL,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );
-- ok: elem_count_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'tags'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'elem_count_histogram', '{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'tags';

-- warn: scalars can't have range stats
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_empty_frac', 0.5::real,
    'range_length_histogram', '{399,499,Infinity}'::text
    );
-- warn: range_empty_frac range_length_hist null mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'arange'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_length_histogram', '{399,499,Infinity}'::text
    );
-- warn: range_empty_frac range_length_hist null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'arange'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_empty_frac', 0.5::real
    );
-- ok: range_empty_frac + range_length_hist
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'arange'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_empty_frac', 0.5::real,
    'range_length_histogram', '{399,499,Infinity}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'arange';

-- warn: scalars can't have range stats
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'id'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_bounds_histogram', '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
    );
-- ok: range_bounds_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'arange'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'range_bounds_histogram', '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'arange';

-- warn: exceed STATISTIC_NUM_SLOTS
SELECT pg_catalog.pg_set_attribute_stats(
    'stats_export_import.test'::regclass,
    'arange'::name,
    false::boolean,
    150000::integer,
    'null_frac', 0.5::real,
    'avg_width', 2::integer,
    'n_distinct', -0.1::real,
    'most_common_vals', '{2,1,3}'::text,
    'most_common_freqs', '{0.3,0.25,0.05}'::real[],
    'histogram_bounds', '{1,2,3,4}'::text,
    'correlation', 1.1::real,
    'most_common_elems', '{3,1}'::text,
    'most_common_elem_freqs', '{0.3,0.2,0.2,0.3,0.0}'::real[],
    'range_empty_frac', -0.5::real,
    'range_length_histogram', '{399,499,Infinity}'::text,
    'range_bounds_histogram', '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
    );
--
-- Test the ability to exactly copy data from one table to an identical table,
-- correctly reconstructing the stakind order as well as the staopN and
-- stacollN values. Because oids are not stable across databases, we can only
-- test this when the source and destination are on the same database
-- instance. For that reason, we borrow and adapt a query found in fe_utils
-- and used by pg_dump/pg_upgrade.
--
INSERT INTO stats_export_import.test
SELECT 1, 'one', (1, 1.1, 'ONE', '2001-01-01', '{ "xkey": "xval" }')::stats_export_import.complex_type, int4range(1,4), array['red','green']
UNION ALL
SELECT 2, 'two', (2, 2.2, 'TWO', '2002-02-02', '[true, 4, "six"]')::stats_export_import.complex_type,  int4range(1,4), array['blue','yellow']
UNION ALL
SELECT 3, 'tre', (3, 3.3, 'TRE', '2003-03-03', NULL)::stats_export_import.complex_type, int4range(-1,1), array['"orange"', 'purple', 'cyan']
UNION ALL
SELECT 4, 'four', NULL, int4range(0,100), NULL;

CREATE INDEX is_odd ON stats_export_import.test(((comp).a % 2 = 1));

-- Generate statistics on table with data
ANALYZE stats_export_import.test;

CREATE TABLE stats_export_import.test_clone ( LIKE stats_export_import.test );

CREATE INDEX is_odd_clone ON stats_export_import.test_clone(((comp).a % 2 = 1));

--
-- Turn off ECHO for the transfer, because the actual stats generated by
-- ANALYZE could change, and we don't care about the actual stats, we care
-- about the ability to transfer them to another relation.
--
\set orig_ECHO :ECHO
\set ECHO none

SELECT
    format('SELECT pg_catalog.pg_set_attribute_stats( '
            || '%L::regclass::oid, '
            || '%L::name, '
            || '%L::boolean, '
            || '%L::integer, '
            || '%L, %L::real, '
            || '%L, %L::integer, '
            || '%L, %L::real %s)',
        'stats_export_import.' || s.tablename || '_clone',
        s.attname,
        s.inherited,
        150000,
        'null_frac', s.null_frac,
        'avg_width', s.avg_width,
        'n_distinct', s.n_distinct,
        CASE
            WHEN s.most_common_vals IS NULL THEN ''
            ELSE format(', %L, %L::text, %L, %L::real[]',
                        'most_common_vals', s.most_common_vals,
                        'most_common_freqs', s.most_common_freqs)
        END ||
        CASE
            WHEN s.histogram_bounds IS NULL THEN ''
            ELSE format(', %L, %L::text',
                        'histogram_bounds', s.histogram_bounds)
        END ||
        CASE
            WHEN s.correlation IS NULL THEN ''
            ELSE format(', %L, %L::real',
                        'correlation', s.correlation)
        END ||
        CASE
            WHEN s.most_common_elems IS NULL THEN ''
            ELSE format(', %L, %L::text, %L, %L::real[]',
                        'most_common_elems', s.most_common_elems,
                        'most_common_elem_freqs', s.most_common_elem_freqs)
        END ||
        CASE
            WHEN s.elem_count_histogram IS NULL THEN ''
            ELSE format(', %L, %L::real[]',
                        'elem_count_histogram', s.elem_count_histogram)
        END ||
        CASE
            WHEN s.range_bounds_histogram IS NULL THEN ''
            ELSE format(', %L, %L::text',
                        'range_bounds_histogram', s.range_bounds_histogram)
        END ||
        CASE
            WHEN s.range_empty_frac IS NULL THEN ''
            ELSE format(', %L, %L::real, %L, %L::text',
                        'range_empty_frac', s.range_empty_frac,
                        'range_length_histogram', s.range_length_histogram)
        END)
FROM pg_catalog.pg_stats AS s
WHERE s.schemaname = 'stats_export_import'
AND s.tablename IN ('test', 'is_odd')
\gexec

-- restore ECHO to original value
\set ECHO :orig_ECHO

SELECT c.relname, COUNT(*) AS num_stats
FROM pg_class AS c
JOIN pg_statistic s ON s.starelid = c.oid
WHERE c.relnamespace = 'stats_export_import'::regnamespace
AND c.relname IN ('test', 'test_clone', 'is_odd', 'is_odd_clone')
GROUP BY c.relname
ORDER BY c.relname;

-- check test minus test_clone
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'test' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.test'::regclass
EXCEPT
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'test' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.test_clone'::regclass;

-- check test_clone minus test
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'test_clone' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.test_clone'::regclass
EXCEPT
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'test_clone' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.test'::regclass;

-- check is_odd minus is_odd_clone
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'is_odd' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.is_odd'::regclass
EXCEPT
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'is_odd' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.is_odd_clone'::regclass;

-- check is_odd_clone minus is_odd
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'is_odd_clone' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.is_odd_clone'::regclass
EXCEPT
SELECT
    a.attname, s.stainherit, s.stanullfrac, s.stawidth, s.stadistinct,
    s.stakind1, s.stakind2, s.stakind3, s.stakind4, s.stakind5,
    s.staop1, s.staop2, s.staop3, s.staop4, s.staop5,
    s.stacoll1, s.stacoll2, s.stacoll3, s.stacoll4, s.stacoll5,
    s.stanumbers1, s.stanumbers2, s.stanumbers3, s.stanumbers4, s.stanumbers5,
    s.stavalues1::text AS sv1, s.stavalues2::text AS sv2,
    s.stavalues3::text AS sv3, s.stavalues4::text AS sv4,
    s.stavalues5::text AS sv5, 'is_odd_clone' AS direction
FROM pg_statistic s
JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum
WHERE s.starelid = 'stats_export_import.is_odd'::regclass;
