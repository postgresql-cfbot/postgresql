/*-------------------------------------------------------------------------
 *
 * Utility functions for extracting object statistics for frontend code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/fe_utils/stats_export.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "fe_utils/stats_export.h"
/*
#include "libpq/libpq-fs.h"
*/
#include "fe_utils/string_utils.h"

/*
 * No-frills catalog queries that are named according to the statistics they
 * fetch (relation, attribute, extended) and the earliest server version for
 * which they work. These are presented so that if other use cases arise they
 * can share the same base queries but utilize them in their own way.
 *
 * The queries themselves do not filter results, so it is up to the caller
 * to append a WHERE clause filtering either on either c.oid or a combination
 * of c.relname and n.nspname.
 */

const char *export_class_stats_query_v9_2 =
	"SELECT c.oid, n.nspname, c.relname, c.relpages, c.reltuples, c.relallvisible "
	"FROM pg_class AS c "
	"JOIN pg_namespace AS n ON n.oid = c.relnamespace";

const char *export_attribute_stats_query_v17 =
	"SELECT c.oid, n.nspname, c.relname, a.attnum, a.attname, s.inherited, "
	"s.null_frac, s.avg_width, s.n_distinct, "
	"s.most_common_vals::text AS most_common_vals, s.most_common_freqs, "
	"s.histogram_bounds::text AS histogram_bounds, s.correlation, "
	"s.most_common_elems::text AS most_common_elems, "
	"s.most_common_elem_freqs, s.elem_count_histogram, "
	"s.range_length_histogram::text AS range_length_histogram, "
	"s.range_empty_frac, "
	"s.range_bounds_histogram::text AS range_bounds_histogram "
	"FROM pg_class AS c "
	"JOIN pg_namespace AS n ON n.oid = c.relnamespace "
	"JOIN pg_attribute AS a ON a.attrelid = c.oid AND not a.attisdropped "
	"JOIN pg_stats AS s ON s.schemaname = n.nspname AND s.tablename = c.relname";

const char *export_attribute_stats_query_v9_2 =
	"SELECT c.oid, n.nspname, c.relname, a.attnum, a.attname, s.inherited, "
	"s.null_frac, s.avg_width, s.n_distinct, "
	"s.most_common_vals::text AS most_common_vals, s.most_common_freqs, "
	"s.histogram_bounds::text AS histogram_bounds, s.correlation, "
	"s.most_common_elems::text AS most_common_elems, "
	"s.most_common_elem_freqs, s.elem_count_histogram, "
	"NULL::text AS range_length_histogram, NULL::real AS range_empty_frac, "
	"NULL::text AS range_bounds_histogram "
	"FROM pg_class AS c "
	"JOIN pg_namespace AS n ON n.oid = c.relnamespace "
	"JOIN pg_attribute AS a ON a.attrelid = c.oid AND not a.attisdropped "
	"JOIN pg_stats AS s ON s.schemaname = n.nspname AND s.tablename = c.relname";

/*
 * Returns true if the server version number supports exporting regular
 * (e.g. pg_statistic) statistics.
 */
bool
exportStatsSupported(int server_version_num)
{
	return (server_version_num >= MIN_SERVER_NUM);
}

/*
 * Returns true if the server version number supports exporting extended
 * (e.g. pg_statistic_ext, pg_statitic_ext_data) statistics.
 *
 * Currently, none do.
 *
 * XXX Why do we even have this? Nothing is using it, even if it called
 * this function we wouldn't know how to export stats.
 */
bool
exportExtStatsSupported(int server_version_num)
{
	return false;
}

/*
 * Return the query appropriate for extracting relation statistics for the
 * given server version, if one exists.
 */
const char *
exportClassStatsSQL(int server_version_num)
{
	if (server_version_num >= MIN_SERVER_NUM)
		return export_class_stats_query_v9_2;
	return NULL;
}

/*
 * Return the query appropriate for extracting attribute statistics for the
 * given server version, if one exists.
 */
const char *
exportAttributeStatsSQL(int server_version_num)
{
	if (server_version_num >= 170000)
		return export_attribute_stats_query_v17;
	if (server_version_num >= MIN_SERVER_NUM)
		return export_attribute_stats_query_v9_2;
	return NULL;
}

/*
 * Generate a SQL statement that will itself generate a SQL statement to
 * import all regular stats from a given relation into another relation.
 *
 * The query generated takes two parameters.
 *
 * $1 is of type Oid, and represents the oid of the source relation.
 *
 * $2 is is a cstring, and represents the qualified name of the destination
 * relation. If NULL, then the qualified name of the source relation will
 * be used. In either case, the value is casted via ::regclass.
 *
 * The function will return NULL for invalid server version numbers.
 * Otherwise,
 *
 * This function needs to work on databases back to 9.2.
 * The format() function was introduced in 9.1.
 * The string_agg() aggregate was introduced in 9.0.
 *
 */
char *exportRelationStatsSQL(int server_version_num)
{
	const char *relsql = exportClassStatsSQL(server_version_num);
	const char *attrsql = exportAttributeStatsSQL(server_version_num);
	const char *filter = "WHERE c.oid = $1::regclass";
	char	   *s;
	PQExpBuffer sql;

	if ((relsql == NULL) || (attrsql == NULL))
		return NULL;

	/*
	 * Set up the initial CTEs each with the same oid filter
	 */
	sql = createPQExpBuffer();
	appendPQExpBuffer(sql,
					  "WITH r AS (%s %s), a AS (%s %s), ",
					  relsql, filter, attrsql, filter);

	/*
	 * Generate the pg_set_relation_stats function call for the relation
	 * and one pg_set_attribute_stats function call for each attribute with
	 * a pg_statistic entry. Give each row an order value such that the
	 * set relation stats call will be first, followed by the set attribute
	 * stats calls in attnum order (even though the attributes are identified
	 * by attname).
	 *
	 * Then aggregate the function calls into a single SELECT statement that
	 * puts the calls in the order described above.
	 */
	appendPQExpBufferStr(sql,
		"s(ord,sql) AS ( "
			"SELECT 0, format('pg_catalog.pg_set_relation_stats("
			"%L::regclass, %L::integer, %L::real, %L::integer)', "
			"coalesce($2, format('%I.%I', r.nspname, r.relname)), "
			"r.relpages, r.reltuples, r.relallvisible) "
			"FROM r "
			"UNION ALL "
			"SELECT 1, format('pg_catalog.pg_set_attribute_stats( "
			"relation => %L::regclass, attname => %L::name, "
			"inherited => %L::boolean, null_frac => %L::real, "
			"avg_width => %L::integer, n_distinct => %L::real, "
			"most_common_vals => %L::text, "
			"most_common_freqs => %L::real[], "
			"histogram_bounds => %L::text, "
			"correlation => %L::real, "
			"most_common_elems => %L::text, "
			"most_common_elem_freqs => %L::real[], "
			"elem_count_histogram => %L::real[], "
			"range_length_histogram => %L::text, "
			"range_empty_frac => %L::real, "
			"range_bounds_histogram => %L::text)', "
			"coalesce($2, format('%I.%I', a.nspname, a.relname)), "
			"a.attname, a.inherited, a.null_frac, a.avg_width, "
			"a.n_distinct, a.most_common_vals, a.most_common_freqs, "
			"a.histogram_bounds, a.correlation, "
			"a.most_common_elems, a.most_common_elem_freqs, "
			"a.elem_count_histogram, a.range_length_histogram, "
			"a.range_empty_frac, a.range_bounds_histogram ) "
			"FROM a "
		") "
		"SELECT 'SELECT ' || string_agg(s.sql, ', ' ORDER BY s.ord) "
		"FROM s ");

	s = strdup(sql->data);
	destroyPQExpBuffer(sql);
	return s;
}
