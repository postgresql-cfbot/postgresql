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

SELECT pg_set_relation_stats('stats_export_import.test'::regclass, 999, 3.6::real, 15000);

SELECT relpages, reltuples, relallvisible FROM pg_class WHERE oid = 'stats_export_import.test'::regclass;

-- error: relpages null
SELECT pg_set_relation_stats('stats_export_import.test'::regclass, NULL, 3.6::real, 15000);

SELECT relpages, reltuples, relallvisible FROM pg_class WHERE oid = 'stats_export_import.test'::regclass;

-- error: object doesn't exist
SELECT pg_catalog.pg_set_attribute_stats(
    relation => '0'::oid,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: relation null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => NULL::oid,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: attname null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => NULL::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: inherited null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => NULL::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: inherited true on nonpartitioned table
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => true::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: null_frac null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => NULL::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: avg_width null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => NULL::integer,
    n_distinct => 0.3::real);

-- error: avg_width null
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => NULL::real);

-- ok: no stakinds
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- error: null_frac < 0
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => -0.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: null_frac > 1
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 1.1::real,
    avg_width => 2::integer,
    n_distinct => 0.3::real);

-- error: avg_width < 0
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => -1::integer,
    n_distinct => 0.3::real);

-- error: n_distinct < -1
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -1.1::real);

-- error: mcv / mcf null mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => NULL::text,
    most_common_freqs => '{0.1,0.2,0.3}'::real[]
    );

-- error: mcv / mcf null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => '{1,2,3}'::text,
    most_common_freqs => NULL::real[]
    );

-- error: mcv / mcf length mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => '{2,1,3}'::text,
    most_common_freqs => '{0.2,0.1}'::real[]
    );

-- error: mcf sum bad
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => '{2,1,3}'::text,
    most_common_freqs => '{0.6,0.5,0.3}'::real[]
    );

-- ok: mcv+mcf
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => '{2,1,3}'::text,
    most_common_freqs => '{0.3,0.25,0.05}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- error: histogram elements null value
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    histogram_bounds => '{1,NULL,3,4}'::text
    );
-- error: histogram elements must be in order
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    histogram_bounds => '{1,20,3,4}'::text
    );

-- ok: histogram_bounds
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    histogram_bounds => '{1,2,3,4}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- error: correlation low
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    correlation => -1.1::real);

-- error: correlation high
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    correlation => 1.1::real);

-- ok: correlation
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    correlation => 0.5::real);

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'id';

-- error: scalars can't have mcelem
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{1,3}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.2,0.3,0.0}'::real[]
    );

-- error: mcelem / mcelem null mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{one,two}'::text,
    most_common_elem_freqs => NULL::real[]
    );

-- error: mcelem / mcelem null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => NULL::text,
    most_common_elem_freqs => '{0.3,0.2,0.2,0.3}'::real[]
    );

-- error: mcelem / mcelem length mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2}'::real[]
    );

-- error: mcelem freq element out of bounds
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.1,0.2,0.3,0.0}'::real[]
    );

-- error: mcelem freq low-high mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.4,0.3,0.0}'::real[]
    );

-- error: mcelem freq null pct invalid
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.2,0.3,-0.0001}'::real[]
    );

-- error: mcelem freq bad low bound low
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,-0.15,0.3,0.1}'::real[]
    );

-- error: mcelem freq bad low bound high
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,1.5,0.3,0.1}'::real[]
    );

-- error: mcelem freq bad high bound low
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.4,-0.3,0.1}'::real[]
    );

-- error: mcelem freq bad high bound high
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.4,3.0,0.1}'::real[]
    );

-- error mcelem freq must be non-increasing
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.4,0.2,0.3,0.0}'::real[]
    );

-- ok: mcelem
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_elems => '{three,one}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.2,0.3,0.0}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'tags';

-- error: scalars can't have elem_count_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    elem_count_histogram => '{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );
-- error: elem_count_histogram null element
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    elem_count_histogram => '{1,1,NULL,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );
-- error: elem_count_histogram must be in order
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    elem_count_histogram => '{1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );
-- ok: elem_count_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'tags'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    elem_count_histogram => '{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}'::real[]
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'tags';

-- error: scalars can't have range stats
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => 0.5::real,
    range_length_histogram => '{399,499,Infinity}'::text
    );
-- error: range_empty_frac range_length_hist null mismatch
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_length_histogram => '{399,499,Infinity}'::text
    );
-- error: range_empty_frac range_length_hist null mismatch part 2
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => 0.5::real
    );
-- error: range_empty_frac low
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => -0.5::real,
    range_length_histogram => '{399,499,Infinity}'::text
    );
-- error: range_empty_frac high
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => 1.5::real,
    range_length_histogram => '{399,499,Infinity}'::text
    );
-- error: range_length_histogram not ascending
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => 0.5::real,
    range_length_histogram => '{399,Infinity,499}'::text
    );
-- ok: range_empty_frac + range_length_hist
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_empty_frac => 0.5::real,
    range_length_histogram => '{399,499,Infinity}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'arange';

-- error: scalars can't have range stats
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'id'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_bounds_histogram => '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
    );
-- error: range_bound_hist low bounds must be in order
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_bounds_histogram => '{"[-1,1)","[-2,4)","[1,4)","[1,100)"}'::text
    );
-- error: range_bound_hist high bounds must be in order
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_bounds_histogram => '{"[-1,11)","[0,4)","[1,4)","[1,100)"}'::text
    );
-- ok: range_bounds_histogram
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    range_bounds_histogram => '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
    );

SELECT *
FROM pg_stats
WHERE schemaname = 'stats_export_import'
AND tablename = 'test'
AND inherited = false
AND attname = 'arange';

-- error: exceed STATISTIC_NUM_SLOTS
SELECT pg_catalog.pg_set_attribute_stats(
    relation => 'stats_export_import.test'::regclass,
    attname => 'arange'::name,
    inherited => false::boolean,
    null_frac => 0.5::real,
    avg_width => 2::integer,
    n_distinct => -0.1::real,
    most_common_vals => '{2,1,3}'::text,
    most_common_freqs => '{0.3,0.25,0.05}'::real[],
    histogram_bounds => '{1,2,3,4}'::text,
    correlation => 1.1::real,
    most_common_elems => '{3,1}'::text,
    most_common_elem_freqs => '{0.3,0.2,0.2,0.3,0.0}'::real[],
    range_empty_frac => -0.5::real,
    range_length_histogram => '{399,499,Infinity}'::text,
    range_bounds_histogram => '{"[-1,1)","[0,4)","[1,4)","[1,100)"}'::text
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
            || 'relation => %L::regclass::oid, attname => %L::name, '
            || 'inherited => %L::boolean, null_frac => %L::real, '
            || 'avg_width => %L::integer, n_distinct => %L::real, '
            || 'most_common_vals => %L::text, '
            || 'most_common_freqs => %L::real[], '
            || 'histogram_bounds => %L::text, '
            || 'correlation => %L::real, '
            || 'most_common_elems => %L::text, '
            || 'most_common_elem_freqs => %L::real[], '
            || 'elem_count_histogram => %L::real[], '
            || 'range_length_histogram => %L::text, '
            || 'range_empty_frac => %L::real, '
            || 'range_bounds_histogram => %L::text) ',
        'stats_export_import.' || s.tablename || '_clone', s.attname,
        s.inherited, s.null_frac,
        s.avg_width, s.n_distinct,
        s.most_common_vals, s.most_common_freqs, s.histogram_bounds,
        s.correlation, s.most_common_elems, s.most_common_elem_freqs,
        s.elem_count_histogram, s.range_length_histogram,
        s.range_empty_frac, s.range_bounds_histogram)
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
