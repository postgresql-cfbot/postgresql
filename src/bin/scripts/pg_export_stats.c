/*-------------------------------------------------------------------------
 *
 * pg_export_stats
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/scripts/pg_export_stats.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "common.h"
#include "common/logging.h"
#include "fe_utils/cancel.h"
#include "fe_utils/option_utils.h"
#include "fe_utils/query_utils.h"
#include "fe_utils/simple_list.h"
#include "fe_utils/string_utils.h"

static void help(const char *progname);

/* view definition introduced in 17 */
const char *export_query_v17 =
	"SELECT schemaname, relname, server_version_num, n_tuples, "
	"n_pages, stats FROM pg_statistic_export ";

/* v15-v16 have the same stats layout, but lacks view definition */
const char *export_query_v15 =
	"SELECT "
	"    n.nspname AS schemaname, "
	"    r.relname AS relname, "
	"    current_setting('server_version_num')::integer AS server_version_num, "
	"    r.reltuples::float4 AS n_tuples, "
	"    r.relpages::integer AS n_pages, "
	"    ( "
	"        WITH per_column_stats AS "
	"        ( "
	"            SELECT "
	"                s.stainherit, "
	"                a.attname, "
	"                jsonb_build_object( "
	"                    'stanullfrac', s.stanullfrac::text, "
	"                    'stawidth', s.stawidth::text, "
	"                    'stadistinct', s.stadistinct::text, "
	"                    'stakinds', "
	"                    ( "
	"                        SELECT "
	"                            jsonb_agg( "
	"                                CASE kind.kind "
	"                                    WHEN 0 THEN 'TRIVIAL' "
	"                                    WHEN 1 THEN 'MCV' "
	"                                    WHEN 2 THEN 'HISTOGRAM' "
	"                                    WHEN 3 THEN 'CORRELATION' "
	"                                    WHEN 4 THEN 'MCELEM' "
	"                                    WHEN 5 THEN 'DECHIST' "
	"                                    WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                    WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                END::text "
	"                                ORDER BY kind.ord) "
	"                        FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                    s.stakind3, stakind4, "
	"                                    s.stakind5]) "
	"                             WITH ORDINALITY AS kind(kind, ord) "
	"                    ), "
	"                    'stanumbers', "
	"                    jsonb_build_array( "
	"                        s.stanumbers1::text::text[], "
	"                        s.stanumbers2::text::text[], "
	"                        s.stanumbers3::text::text[], "
	"                        s.stanumbers4::text::text[], "
	"                        s.stanumbers5::text::text[]), "
	"                    'stavalues', "
	"                    jsonb_build_array( "
	"                        s.stavalues1::text::text[], "
	"                        s.stavalues2::text::text[], "
	"                        s.stavalues3::text::text[], "
	"                        s.stavalues4::text::text[], "
	"                        s.stavalues5::text::text[]) "
	"                ) AS stats "
	"            FROM pg_attribute AS a "
	"            JOIN pg_statistic AS s "
	"                ON s.starelid = a.attrelid "
	"                AND s.staattnum = a.attnum "
	"            WHERE a.attrelid = r.oid "
	"            AND NOT a.attisdropped "
	"            AND a.attnum > 0 "
	"            AND has_column_privilege(a.attrelid, a.attnum, 'SELECT') "
	"        ), "
	"        attagg AS "
	"        ( "
	"            SELECT "
	"                pcs.stainherit, "
	"                jsonb_build_object( "
	"                    'columns', "
	"                    jsonb_object_agg( "
	"                        pcs.attname, "
	"                        pcs.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM per_column_stats AS pcs "
	"            GROUP BY pcs.stainherit "
	"        ), "
	"        extended_object_stats AS "
	"        ( "
	"            SELECT "
	"                sd.stxdinherit, "
	"                e.stxname, "
	"                jsonb_build_object( "
	"                    'stxkinds', "
	"                    to_jsonb(e.stxkind), "
	"                    'stxdndistinct', "
	"                    ndist.stxdndistinct, "
	"                    'stxdndependencies', "
	"                    ndep.stxdndependencies, "
	"                    'stxdmcv', "
	"                    mcv.stxdmcv, "
	"                    'stxdexprs', "
	"                    x.stdxdexprs "
	"                ) AS stats "
	"            FROM pg_statistic_ext AS e "
	"            JOIN pg_statistic_ext_data AS sd "
	"                ON sd.stxoid = e.oid "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', string_to_array(nd.attnums, ', '), "
	"                                'ndistinct', nd.ndistinct "
	"                                ) "
	"                            ORDER BY nd.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxdndistinct::text::json) "
	"                         WITH ORDINALITY AS nd(attnums, ndistinct, ord) "
	"                ) AS ndist(stxdndistinct) ON sd.stxdndistinct IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', "
	"                                string_to_array( replace(dep.attrs, ' => ', ', '), ', '), "
	"                                'degree', "
	"                                dep.degree "
	"                                ) "
	"                            ORDER BY dep.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxddependencies::text::json) "
	"                         WITH ORDINALITY AS dep(attrs, degree, ord) "
	"                ) AS ndep(stxdndependencies) ON sd.stxddependencies IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT NULL AS stxdmcv "
	"                ) AS mcv(stxdmcv) ON sd.stxdmcv IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'stanullfrac', s.stanullfrac::text, "
	"                                'stawidth', s.stawidth::text, "
	"                                'stadistinct', s.stadistinct::text, "
	"                                'stakinds', "
	"                                ( "
	"                                    SELECT "
	"                                        jsonb_agg( "
	"                                            CASE kind.kind "
	"                                                WHEN 0 THEN 'TRIVIAL' "
	"                                                WHEN 1 THEN 'MCV' "
	"                                                WHEN 2 THEN 'HISTOGRAM' "
	"                                                WHEN 3 THEN 'CORRELATION' "
	"                                                WHEN 4 THEN 'MCELEM' "
	"                                                WHEN 5 THEN 'DECHIST' "
	"                                                WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                                WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                            END::text "
	"                                            ORDER BY kind.ord) "
	"                                    FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                                s.stakind3, stakind4, "
	"                                                s.stakind5]) WITH ORDINALITY AS kind(kind, ord) "
	"                                ), "
	"                                'stanumbers', "
	"                                jsonb_build_array( "
	"                                    s.stanumbers1::text::text[], "
	"                                    s.stanumbers2::text::text[], "
	"                                    s.stanumbers3::text::text[], "
	"                                    s.stanumbers4::text::text[], "
	"                                    s.stanumbers5::text::text[]), "
	"                                'stavalues', "
	"                                jsonb_build_array( "
	"                                    s.stavalues1::text::text[], "
	"                                    s.stavalues2::text::text[], "
	"                                    s.stavalues3::text::text[], "
	"                                    s.stavalues4::text::text[], "
	"                                    s.stavalues5::text::text[]) "
	"                            ) "
	"                            ORDER BY s.ordinality "
	"                        ) "
	"                    FROM unnest(sd.stxdexpr) WITH ORDINALITY AS s "
	"                ) AS x(stdxdexprs) ON sd.stxdexpr IS NOT NULL "
	"            WHERE e.stxrelid = r.oid "
	"        ), "
	"        extagg AS "
	"        ( "
	"            SELECT "
	"                eos.stxdinherit, "
	"                jsonb_build_object( "
	"                    'extended', "
	"                    jsonb_object_agg( "
	"                        eos.stxname, "
	"                        eos.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM extended_object_stats AS eos "
	"            GROUP BY eos.stxdinherit "
	"        ) "
	"        SELECT "
	"            jsonb_object_agg( "
	"                CASE coalesce(a.stainherit, e.stxdinherit) "
	"                    WHEN TRUE THEN 'inherited' "
	"                    ELSE 'regular' "
	"                END, "
	"                coalesce(a.stats, '{}'::jsonb) || coalesce(e.stats, '{}'::jsonb)  "
	"            ) "
	"        FROM attagg AS a "
	"        FULL OUTER JOIN extagg e ON a.stainherit = e.stxdinherit "
	"    ) AS stats "
	"FROM pg_class AS r "
	"JOIN pg_namespace AS n "
	"    ON n.oid = r.relnamespace "
	"WHERE relkind IN ('r', 'm', 'f', 'p') "
	"AND n.nspname NOT IN ('pg_catalog', 'information_schema')";

/* v14 is like v15, but lacks stxdinherit on ext_data */
const char *export_query_v14 = 
	"SELECT "
	"    n.nspname AS schemaname, "
	"    r.relname AS relname, "
	"    current_setting('server_version_num')::integer AS server_version_num, "
	"    r.reltuples::float4 AS n_tuples, "
	"    r.relpages::integer AS n_pages, "
	"    ( "
	"        WITH per_column_stats AS "
	"        ( "
	"            SELECT "
	"                s.stainherit, "
	"                a.attname, "
	"                jsonb_build_object( "
	"                    'stanullfrac', s.stanullfrac::text, "
	"                    'stawidth', s.stawidth::text, "
	"                    'stadistinct', s.stadistinct::text, "
	"                    'stakinds', "
	"                    ( "
	"                        SELECT "
	"                            jsonb_agg( "
	"                                CASE kind.kind "
	"                                    WHEN 0 THEN 'TRIVIAL' "
	"                                    WHEN 1 THEN 'MCV' "
	"                                    WHEN 2 THEN 'HISTOGRAM' "
	"                                    WHEN 3 THEN 'CORRELATION' "
	"                                    WHEN 4 THEN 'MCELEM' "
	"                                    WHEN 5 THEN 'DECHIST' "
	"                                    WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                    WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                END::text "
	"                                ORDER BY kind.ord) "
	"                        FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                    s.stakind3, stakind4, "
	"                                    s.stakind5]) "
	"                             WITH ORDINALITY AS kind(kind, ord) "
	"                    ), "
	"                    'stanumbers', "
	"                    jsonb_build_array( "
	"                        s.stanumbers1::text::text[], "
	"                        s.stanumbers2::text::text[], "
	"                        s.stanumbers3::text::text[], "
	"                        s.stanumbers4::text::text[], "
	"                        s.stanumbers5::text::text[]), "
	"                    'stavalues', "
	"                    jsonb_build_array( "
	"                        s.stavalues1::text::text[], "
	"                        s.stavalues2::text::text[], "
	"                        s.stavalues3::text::text[], "
	"                        s.stavalues4::text::text[], "
	"                        s.stavalues5::text::text[]) "
	"                ) AS stats "
	"            FROM pg_attribute AS a "
	"            JOIN pg_statistic AS s "
	"                ON s.starelid = a.attrelid "
	"                AND s.staattnum = a.attnum "
	"            WHERE a.attrelid = r.oid "
	"            AND NOT a.attisdropped "
	"            AND a.attnum > 0 "
	"            AND has_column_privilege(a.attrelid, a.attnum, 'SELECT') "
	"        ), "
	"        attagg AS "
	"        ( "
	"            SELECT "
	"                pcs.stainherit, "
	"                jsonb_build_object( "
	"                    'columns', "
	"                    jsonb_object_agg( "
	"                        pcs.attname, "
	"                        pcs.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM per_column_stats AS pcs "
	"            GROUP BY pcs.stainherit "
	"        ), "
	"        extended_object_stats AS "
	"        ( "
	"            SELECT "
	"                false AS stxdinherit, "
	"                e.stxname, "
	"                jsonb_build_object( "
	"                    'stxkinds', "
	"                    to_jsonb(e.stxkind), "
	"                    'stxdndistinct', "
	"                    ndist.stxdndistinct, "
	"                    'stxdndependencies', "
	"                    ndep.stxdndependencies, "
	"                    'stxdmcv', "
	"                    mcv.stxdmcv, "
	"                    'stxdexprs', "
	"                    x.stdxdexprs "
	"                ) AS stats "
	"            FROM pg_statistic_ext AS e "
	"            JOIN pg_statistic_ext_data AS sd "
	"                ON sd.stxoid = e.oid "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', string_to_array(nd.attnums, ', '), "
	"                                'ndistinct', nd.ndistinct "
	"                                ) "
	"                            ORDER BY nd.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxdndistinct::text::json) "
	"                         WITH ORDINALITY AS nd(attnums, ndistinct, ord) "
	"                ) AS ndist(stxdndistinct) ON sd.stxdndistinct IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', "
	"                                string_to_array( replace(dep.attrs, ' => ', ', '), ', '), "
	"                                'degree', "
	"                                dep.degree "
	"                                ) "
	"                            ORDER BY dep.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxddependencies::text::json) "
	"                         WITH ORDINALITY AS dep(attrs, degree, ord) "
	"                ) AS ndep(stxdndependencies) ON sd.stxddependencies IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT NULL AS stxdmcv "
	"                ) AS mcv(stxdmcv) ON sd.stxdmcv IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'stanullfrac', s.stanullfrac::text, "
	"                                'stawidth', s.stawidth::text, "
	"                                'stadistinct', s.stadistinct::text, "
	"                                'stakinds', "
	"                                ( "
	"                                    SELECT "
	"                                        jsonb_agg( "
	"                                            CASE kind.kind "
	"                                                WHEN 0 THEN 'TRIVIAL' "
	"                                                WHEN 1 THEN 'MCV' "
	"                                                WHEN 2 THEN 'HISTOGRAM' "
	"                                                WHEN 3 THEN 'CORRELATION' "
	"                                                WHEN 4 THEN 'MCELEM' "
	"                                                WHEN 5 THEN 'DECHIST' "
	"                                                WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                                WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                            END::text "
	"                                            ORDER BY kind.ord) "
	"                                    FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                                s.stakind3, stakind4, "
	"                                                s.stakind5]) WITH ORDINALITY AS kind(kind, ord) "
	"                                ), "
	"                                'stanumbers', "
	"                                jsonb_build_array( "
	"                                    s.stanumbers1::text::text[], "
	"                                    s.stanumbers2::text::text[], "
	"                                    s.stanumbers3::text::text[], "
	"                                    s.stanumbers4::text::text[], "
	"                                    s.stanumbers5::text::text[]), "
	"                                'stavalues', "
	"                                jsonb_build_array( "
	"                                    s.stavalues1::text::text[], "
	"                                    s.stavalues2::text::text[], "
	"                                    s.stavalues3::text::text[], "
	"                                    s.stavalues4::text::text[], "
	"                                    s.stavalues5::text::text[]) "
	"                            ) "
	"                            ORDER BY s.ordinality "
	"                        ) "
	"                    FROM unnest(sd.stxdexpr) WITH ORDINALITY AS s "
	"                ) AS x(stdxdexprs) ON sd.stxdexpr IS NOT NULL "
	"            WHERE e.stxrelid = r.oid "
	"        ), "
	"        extagg AS "
	"        ( "
	"            SELECT "
	"                eos.stxdinherit, "
	"                jsonb_build_object( "
	"                    'extended', "
	"                    jsonb_object_agg( "
	"                        eos.stxname, "
	"                        eos.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM extended_object_stats AS eos "
	"            GROUP BY eos.stxdinherit "
	"        ) "
	"        SELECT "
	"            jsonb_object_agg( "
	"                CASE coalesce(a.stainherit, e.stxdinherit) "
	"                    WHEN TRUE THEN 'inherited' "
	"                    ELSE 'regular' "
	"                END, "
	"                coalesce(a.stats, '{}'::jsonb) || coalesce(e.stats, '{}'::jsonb)  "
	"            ) "
	"        FROM attagg AS a "
	"        FULL OUTER JOIN extagg e ON a.stainherit = e.stxdinherit "
	"    ) AS stats "
	"FROM pg_class AS r "
	"JOIN pg_namespace AS n "
	"    ON n.oid = r.relnamespace "
	"WHERE relkind IN ('r', 'm', 'f', 'p') "
	"AND n.nspname NOT IN ('pg_catalog', 'information_schema')";

/* v12-v13 are like v14, but lacks stxdexpr on ext_data */
const char *export_query_v12 = 
	"SELECT "
	"    n.nspname AS schemaname, "
	"    r.relname AS relname, "
	"    current_setting('server_version_num')::integer AS server_version_num, "
	"    r.reltuples::float4 AS n_tuples, "
	"    r.relpages::integer AS n_pages, "
	"    ( "
	"        WITH per_column_stats AS "
	"        ( "
	"            SELECT "
	"                s.stainherit, "
	"                a.attname, "
	"                jsonb_build_object( "
	"                    'stanullfrac', s.stanullfrac::text, "
	"                    'stawidth', s.stawidth::text, "
	"                    'stadistinct', s.stadistinct::text, "
	"                    'stakinds', "
	"                    ( "
	"                        SELECT "
	"                            jsonb_agg( "
	"                                CASE kind.kind "
	"                                    WHEN 0 THEN 'TRIVIAL' "
	"                                    WHEN 1 THEN 'MCV' "
	"                                    WHEN 2 THEN 'HISTOGRAM' "
	"                                    WHEN 3 THEN 'CORRELATION' "
	"                                    WHEN 4 THEN 'MCELEM' "
	"                                    WHEN 5 THEN 'DECHIST' "
	"                                    WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                    WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                END::text "
	"                                ORDER BY kind.ord) "
	"                        FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                    s.stakind3, stakind4, "
	"                                    s.stakind5]) "
	"                             WITH ORDINALITY AS kind(kind, ord) "
	"                    ), "
	"                    'stanumbers', "
	"                    jsonb_build_array( "
	"                        s.stanumbers1::text::text[], "
	"                        s.stanumbers2::text::text[], "
	"                        s.stanumbers3::text::text[], "
	"                        s.stanumbers4::text::text[], "
	"                        s.stanumbers5::text::text[]), "
	"                    'stavalues', "
	"                    jsonb_build_array( "
	"                        s.stavalues1::text::text[], "
	"                        s.stavalues2::text::text[], "
	"                        s.stavalues3::text::text[], "
	"                        s.stavalues4::text::text[], "
	"                        s.stavalues5::text::text[]) "
	"                ) AS stats "
	"            FROM pg_attribute AS a "
	"            JOIN pg_statistic AS s "
	"                ON s.starelid = a.attrelid "
	"                AND s.staattnum = a.attnum "
	"            WHERE a.attrelid = r.oid "
	"            AND NOT a.attisdropped "
	"            AND a.attnum > 0 "
	"            AND has_column_privilege(a.attrelid, a.attnum, 'SELECT') "
	"        ), "
	"        attagg AS "
	"        ( "
	"            SELECT "
	"                pcs.stainherit, "
	"                jsonb_build_object( "
	"                    'columns', "
	"                    jsonb_object_agg( "
	"                        pcs.attname, "
	"                        pcs.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM per_column_stats AS pcs "
	"            GROUP BY pcs.stainherit "
	"        ), "
	"        extended_object_stats AS "
	"        ( "
	"            SELECT "
	"                false AS stxdinherit, "
	"                e.stxname, "
	"                jsonb_build_object( "
	"                    'stxkinds', "
	"                    to_jsonb(e.stxkind), "
	"                    'stxdndistinct', "
	"                    ndist.stxdndistinct, "
	"                    'stxdndependencies', "
	"                    ndep.stxdndependencies, "
	"                    'stxdmcv', "
	"                    mcv.stxdmcv, "
	"                ) AS stats "
	"            FROM pg_statistic_ext AS e "
	"            JOIN pg_statistic_ext_data AS sd "
	"                ON sd.stxoid = e.oid "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', string_to_array(nd.attnums, ', '), "
	"                                'ndistinct', nd.ndistinct "
	"                                ) "
	"                            ORDER BY nd.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxdndistinct::text::json) "
	"                         WITH ORDINALITY AS nd(attnums, ndistinct, ord) "
	"                ) AS ndist(stxdndistinct) ON sd.stxdndistinct IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', "
	"                                string_to_array( replace(dep.attrs, ' => ', ', '), ', '), "
	"                                'degree', "
	"                                dep.degree "
	"                                ) "
	"                            ORDER BY dep.ord "
	"                        ) "
	"                    FROM json_each_text(sd.stxddependencies::text::json) "
	"                         WITH ORDINALITY AS dep(attrs, degree, ord) "
	"                ) AS ndep(stxdndependencies) ON sd.stxddependencies IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT NULL AS stxdmcv "
	"                ) AS mcv(stxdmcv) ON sd.stxdmcv IS NOT NULL "
	"            WHERE e.stxrelid = r.oid "
	"        ), "
	"        extagg AS "
	"        ( "
	"            SELECT "
	"                eos.stxdinherit, "
	"                jsonb_build_object( "
	"                    'extended', "
	"                    jsonb_object_agg( "
	"                        eos.stxname, "
	"                        eos.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM extended_object_stats AS eos "
	"            GROUP BY eos.stxdinherit "
	"        ) "
	"        SELECT "
	"            jsonb_object_agg( "
	"                CASE coalesce(a.stainherit, e.stxdinherit) "
	"                    WHEN TRUE THEN 'inherited' "
	"                    ELSE 'regular' "
	"                END, "
	"                coalesce(a.stats, '{}'::jsonb) || coalesce(e.stats, '{}'::jsonb)  "
	"            ) "
	"        FROM attagg AS a "
	"        FULL OUTER JOIN extagg e ON a.stainherit = e.stxdinherit "
	"    ) AS stats "
	"FROM pg_class AS r "
	"JOIN pg_namespace AS n "
	"    ON n.oid = r.relnamespace "
	"WHERE relkind IN ('r', 'm', 'f', 'p') "
	"AND n.nspname NOT IN ('pg_catalog', 'information_schema')";

/* v10-v11 are like v12, but ext_data is gone and ndistinct and dependencies are on ext */
const char *export_query_v10 = 
	"SELECT "
	"    n.nspname AS schemaname, "
	"    r.relname AS relname, "
	"    current_setting('server_version_num')::integer AS server_version_num, "
	"    r.reltuples::float4 AS n_tuples, "
	"    r.relpages::integer AS n_pages, "
	"    ( "
	"        WITH per_column_stats AS "
	"        ( "
	"            SELECT "
	"                s.stainherit, "
	"                a.attname, "
	"                jsonb_build_object( "
	"                    'stanullfrac', s.stanullfrac::text, "
	"                    'stawidth', s.stawidth::text, "
	"                    'stadistinct', s.stadistinct::text, "
	"                    'stakinds', "
	"                    ( "
	"                        SELECT "
	"                            jsonb_agg( "
	"                                CASE kind.kind "
	"                                    WHEN 0 THEN 'TRIVIAL' "
	"                                    WHEN 1 THEN 'MCV' "
	"                                    WHEN 2 THEN 'HISTOGRAM' "
	"                                    WHEN 3 THEN 'CORRELATION' "
	"                                    WHEN 4 THEN 'MCELEM' "
	"                                    WHEN 5 THEN 'DECHIST' "
	"                                    WHEN 6 THEN 'RANGE_LENGTH_HISTOGRAM' "
	"                                    WHEN 7 THEN 'BOUNDS_HISTOGRAM' "
	"                                END::text "
	"                                ORDER BY kind.ord) "
	"                        FROM unnest(ARRAY[s.stakind1, s.stakind2, "
	"                                    s.stakind3, stakind4, "
	"                                    s.stakind5]) "
	"                             WITH ORDINALITY AS kind(kind, ord) "
	"                    ), "
	"                    'stanumbers', "
	"                    jsonb_build_array( "
	"                        s.stanumbers1::text::text[], "
	"                        s.stanumbers2::text::text[], "
	"                        s.stanumbers3::text::text[], "
	"                        s.stanumbers4::text::text[], "
	"                        s.stanumbers5::text::text[]), "
	"                    'stavalues', "
	"                    jsonb_build_array( "
	"                        s.stavalues1::text::text[], "
	"                        s.stavalues2::text::text[], "
	"                        s.stavalues3::text::text[], "
	"                        s.stavalues4::text::text[], "
	"                        s.stavalues5::text::text[]) "
	"                ) AS stats "
	"            FROM pg_attribute AS a "
	"            JOIN pg_statistic AS s "
	"                ON s.starelid = a.attrelid "
	"                AND s.staattnum = a.attnum "
	"            WHERE a.attrelid = r.oid "
	"            AND NOT a.attisdropped "
	"            AND a.attnum > 0 "
	"            AND has_column_privilege(a.attrelid, a.attnum, 'SELECT') "
	"        ), "
	"        attagg AS "
	"        ( "
	"            SELECT "
	"                pcs.stainherit, "
	"                jsonb_build_object( "
	"                    'columns', "
	"                    jsonb_object_agg( "
	"                        pcs.attname, "
	"                        pcs.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM per_column_stats AS pcs "
	"            GROUP BY pcs.stainherit "
	"        ), "
	"        extended_object_stats AS "
	"        ( "
	"            SELECT "
	"                false AS stxdinherit, "
	"                e.stxname, "
	"                jsonb_build_object( "
	"                    'stxkinds', "
	"                    to_jsonb(e.stxkind), "
	"                    'stxdndistinct', "
	"                    ndist.stxdndistinct, "
	"                    'stxdndependencies', "
	"                    ndep.stxdndependencies, "
	"                ) AS stats "
	"            FROM pg_statistic_ext AS e "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', string_to_array(nd.attnums, ', '), "
	"                                'ndistinct', nd.ndistinct "
	"                                ) "
	"                            ORDER BY nd.ord "
	"                        ) "
	"                    FROM json_each_text(e.stxndistinct::text::json) "
	"                         WITH ORDINALITY AS nd(attnums, ndistinct, ord) "
	"                ) AS ndist(stxdndistinct) ON e.stxndistinct IS NOT NULL "
	"            LEFT JOIN LATERAL "
	"                ( "
	"                    SELECT "
	"                        jsonb_agg( "
	"                            jsonb_build_object( "
	"                                'attnums', "
	"                                string_to_array( replace(dep.attrs, ' => ', ', '), ', '), "
	"                                'degree', "
	"                                dep.degree "
	"                                ) "
	"                            ORDER BY dep.ord "
	"                        ) "
	"                    FROM json_each_text(.stxdependencies::text::json) "
	"                         WITH ORDINALITY AS dep(attrs, degree, ord) "
	"                ) AS ndep(stxdndependencies) ON e.stxdependencies IS NOT NULL "
	"            WHERE e.stxrelid = r.oid "
	"        ), "
	"        extagg AS "
	"        ( "
	"            SELECT "
	"                eos.stxdinherit, "
	"                jsonb_build_object( "
	"                    'extended', "
	"                    jsonb_object_agg( "
	"                        eos.stxname, "
	"                        eos.stats "
	"                    ) "
	"                ) AS stats "
	"            FROM extended_object_stats AS eos "
	"            GROUP BY eos.stxdinherit "
	"        ) "
	"        SELECT "
	"            jsonb_object_agg( "
	"                CASE coalesce(a.stainherit, e.stxdinherit) "
	"                    WHEN TRUE THEN 'inherited' "
	"                    ELSE 'regular' "
	"                END, "
	"                coalesce(a.stats, '{}'::jsonb) || coalesce(e.stats, '{}'::jsonb)  "
	"            ) "
	"        FROM attagg AS a "
	"        FULL OUTER JOIN extagg e ON a.stainherit = e.stxdinherit "
	"    ) AS stats "
	"FROM pg_class AS r "
	"JOIN pg_namespace AS n "
	"    ON n.oid = r.relnamespace "
	"WHERE relkind IN ('r', 'm', 'f', 'p') "
	"AND n.nspname NOT IN ('pg_catalog', 'information_schema')";


int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"echo", no_argument, NULL, 'e'},
		{"dbname", required_argument, NULL, 'd'},
		{NULL, 0, NULL, 0}
	};

	const char *progname;
	int			optindex;
	int			c;

	const char *dbname = NULL;
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *username = NULL;
	enum trivalue prompt_password = TRI_DEFAULT;
	ConnParams	cparams;
	bool		echo = false;

	PQExpBufferData sql;
	
	PGconn	   *conn;

	FILE	   *copystream = stdout;

	PGresult   *result;

	ExecStatusType result_status;

	char	   *buf;
	int			ret;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pgscripts"));

	handle_help_version_opts(argc, argv, "clusterdb", help);

	while ((c = getopt_long(argc, argv, "d:eh:p:U:wW", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'e':
				echo = true;
				break;
			case 'h':
				host = pg_strdup(optarg);
				break;
			case 'p':
				port = pg_strdup(optarg);
				break;
			case 'U':
				username = pg_strdup(optarg);
				break;
			case 'w':
				prompt_password = TRI_NO;
				break;
			case 'W':
				prompt_password = TRI_YES;
				break;
			default:
				/* getopt_long already emitted a complaint */
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
				exit(1);
		}
	}

	/*
	 * Non-option argument specifies database name as long as it wasn't
	 * already specified with -d / --dbname
	 */
	if (optind < argc && dbname == NULL)
	{
		dbname = argv[optind];
		optind++;
	}

	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}

	/* fill cparams except for dbname, which is set below */
	cparams.pghost = host;
	cparams.pgport = port;
	cparams.pguser = username;
	cparams.prompt_password = prompt_password;
	cparams.override_dbname = NULL;

	setup_cancel_handler(NULL);

	if (dbname == NULL)
	{
		if (getenv("PGDATABASE"))
			dbname = getenv("PGDATABASE");
		else if (getenv("PGUSER"))
			dbname = getenv("PGUSER");
		else
			dbname = get_user_name_or_exit(progname);
	}

	cparams.dbname = dbname;

	conn = connectDatabase(&cparams, progname, echo, false, true);

	initPQExpBuffer(&sql);

	appendPQExpBufferStr(&sql, "COPY (");

	if (PQserverVersion(conn) >= 170000)
		appendPQExpBufferStr(&sql, export_query_v17);
	else if (PQserverVersion(conn) >= 150000)
		appendPQExpBufferStr(&sql, export_query_v15);
	else if (PQserverVersion(conn) >= 140000)
		appendPQExpBufferStr(&sql, export_query_v14);
	else if (PQserverVersion(conn) >= 120000)
		appendPQExpBufferStr(&sql, export_query_v12);
	else if (PQserverVersion(conn) >= 100000)
		appendPQExpBufferStr(&sql, export_query_v10);
	else
		pg_fatal("exporting statistics from databases prior to version 10 not supported");

	appendPQExpBufferStr(&sql, ") TO STDOUT");

	result = PQexec(conn, sql.data);
	result_status = PQresultStatus(result);

	if (result_status != PGRES_COPY_OUT)
		pg_fatal("malformed copy command");

	for (;;)
	{
		ret = PQgetCopyData(conn, &buf, 0);

		if (ret < 0)
			break;				/* done or server/connection error */

		if (buf)
		{
			if (copystream && fwrite(buf, 1, ret, copystream) != ret)
				pg_fatal("could not write COPY data: %m");
			PQfreemem(buf);
		}
	}

	if (copystream && fflush(copystream))
		pg_fatal("could not write COPY data: %m");

	if (ret == -2)
		pg_fatal("COPY data transfer failed: %s", PQerrorMessage(conn));

	PQfinish(conn);
	termPQExpBuffer(&sql);
	exit(0);
}


static void
help(const char *progname)
{
	printf(_("%s clusters all previously clustered tables in a database.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [DBNAME]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -d, --dbname=DBNAME       database to cluster\n"));
	printf(_("  -e, --echo                show the commands being sent to the server\n"));
	printf(_("  -t, --table=TABLE         cluster specific table(s) only\n"));
	printf(_("  -V, --version             output version information, then exit\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("\nConnection options:\n"));
	printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
	printf(_("  -p, --port=PORT           database server port\n"));
	printf(_("  -U, --username=USERNAME   user name to connect as\n"));
	printf(_("  -w, --no-password         never prompt for password\n"));
	printf(_("  -W, --password            force password prompt\n"));
	printf(_("  --maintenance-db=DBNAME   alternate maintenance database\n"));
	printf(_("\nRead the description of the SQL command CLUSTER for details.\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
