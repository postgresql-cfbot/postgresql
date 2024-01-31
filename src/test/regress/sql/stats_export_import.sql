CREATE TYPE stats_import_complex_type AS (
    a integer,
    b float,
    c text,
    d date,
    e jsonb);

CREATE TABLE stats_import_test(
    id INTEGER PRIMARY KEY,
    name text,
    comp stats_import_complex_type,
    tags text[]
);

INSERT INTO stats_import_test
SELECT 1, 'one', (1, 1.1, 'ONE', '2001-01-01', '{ "xkey": "xval" }')::stats_import_complex_type, array['red','green']
UNION ALL
SELECT 2, 'two', (2, 2.2, 'TWO', '2002-02-02', '[true, 4, "six"]')::stats_import_complex_type, array['blue','yellow']
UNION ALL
SELECT 3, 'tre', (3, 3.3, 'TRE', '2003-03-03', NULL)::stats_import_complex_type, array['"orange"', 'purple', 'cyan']
UNION ALL
SELECT 4, 'four', NULL, NULL;

CREATE INDEX is_odd ON stats_import_test(((comp).a % 2 = 1));

CREATE STATISTICS evens_test ON name, ((comp).a % 2 = 0) FROM stats_import_test;

ANALYZE stats_import_test;

-- capture snapshot of source stats
CREATE TABLE stats_export AS
SELECT e.*
FROM pg_catalog.pg_statistic_export AS e
WHERE e.schemaname = 'public'
AND e.relname IN ('stats_import_test', 'is_odd');

SELECT c.reltuples AS before_tuples, c.relpages AS before_pages
FROM pg_class AS c
WHERE oid = 'stats_import_test'::regclass;

-- test settting tuples and pages but no columns
SELECT pg_import_rel_stats(c.oid,
                           current_setting('server_version_num')::integer,
                           1000.0, 200, NULL::jsonb)
FROM pg_class AS c
WHERE oid = 'stats_import_test'::regclass;

SELECT c.reltuples AS after_tuples, c.relpages AS after_pages
FROM pg_class AS c
WHERE oid = 'stats_import_test'::regclass;

-- create a table just like stats_import_test
CREATE TABLE stats_import_clone ( LIKE stats_import_test );

-- create an index just like is_odd
CREATE INDEX is_odd2 ON stats_import_clone(((comp).a % 2 = 0));

-- create a statistics object like evens_test
CREATE STATISTICS evens_clone ON name, ((comp).a % 2 = 0) FROM stats_import_clone;

-- copy table stats to clone table
SELECT pg_import_rel_stats(c.oid, e.server_version_num,
                            e.n_tuples, e.n_pages, e.stats)
FROM pg_class AS c
JOIN pg_namespace AS n
ON n.oid = c.relnamespace
JOIN stats_export AS e
ON e.schemaname = 'public'
AND e.relname = 'stats_import_test'
WHERE c.oid = 'stats_import_clone'::regclass;

-- copy index stats to clone index
SELECT pg_import_rel_stats(c.oid, e.server_version_num,
                            e.n_tuples, e.n_pages, e.stats)
FROM pg_class AS c
JOIN pg_namespace AS n
ON n.oid = c.relnamespace
JOIN stats_export AS e
ON e.schemaname = 'public'
AND e.relname = 'is_odd'
WHERE c.oid = 'is_odd2'::regclass;

-- table stats must match
SELECT staattnum, stainherit, stanullfrac, stawidth, stadistinct,
        stakind1, stakind2, stakind3, stakind4, stakind5,
        staop1, staop2, staop3, staop4, staop5, stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
        stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
        stavalues1::text AS sv1, stavalues2::text AS sv2, stavalues3::text AS sv3,
        stavalues4::text AS sv4, stavalues5::text AS sv5
FROM pg_statistic AS s
WHERE s.starelid = 'stats_import_test'::regclass
EXCEPT
SELECT staattnum, stainherit, stanullfrac, stawidth, stadistinct,
        stakind1, stakind2, stakind3, stakind4, stakind5,
        staop1, staop2, staop3, staop4, staop5, stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
        stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
        stavalues1::text AS sv1, stavalues2::text AS sv2, stavalues3::text AS sv3,
        stavalues4::text AS sv4, stavalues5::text AS sv5
FROM pg_statistic AS s
WHERE s.starelid = 'stats_import_clone'::regclass;

-- index stats must match
SELECT staattnum, stainherit, stanullfrac, stawidth, stadistinct,
        stakind1, stakind2, stakind3, stakind4, stakind5,
        staop1, staop2, staop3, staop4, staop5, stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
        stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
        stavalues1::text AS sv1, stavalues2::text AS sv2, stavalues3::text AS sv3,
        stavalues4::text AS sv4, stavalues5::text AS sv5
FROM pg_statistic AS s
WHERE s.starelid = 'is_odd'::regclass
EXCEPT
SELECT staattnum, stainherit, stanullfrac, stawidth, stadistinct,
        stakind1, stakind2, stakind3, stakind4, stakind5,
        staop1, staop2, staop3, staop4, staop5, stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
        stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
        stavalues1::text AS sv1, stavalues2::text AS sv2, stavalues3::text AS sv3,
        stavalues4::text AS sv4, stavalues5::text AS sv5
FROM pg_statistic AS s
WHERE s.starelid = 'is_odd2'::regclass;

-- copy extended stats to clone table
SELECT pg_import_ext_stats(
        (
            SELECT e.oid as ext_clone_oid
            FROM pg_statistic_ext AS e
            WHERE e.stxname = 'evens_clone'
        ),
        e.server_version_num, e.stats)
FROM pg_catalog.pg_statistic_ext_export AS e
WHERE e.schemaname = 'public'
AND e.tablename = 'stats_import_test'
AND e.ext_stats_name = 'evens_test';

DROP TABLE stats_export;
DROP TABLE stats_import_clone;
DROP TABLE stats_import_test;
DROP TYPE stats_import_complex_type;
