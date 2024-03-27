/*-------------------------------------------------------------------------
 *
 * stats_export.h
 *    Queries to export statistics from current and past versions.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1995, Regents of the University of California
 *
 * src/include/varatt.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STATS_EXPORT_H
#define STATS_EXPORT_H

#include "postgres_fe.h"
#include "libpq-fe.h"

/*
 * The minimum supported version number. No attempt is made to get statistics
 * import to work on versions older than this. This version was initially chosen
 * because that was the minimum version supported by pg_dump at the time.
 */
#define MIN_SERVER_NUM 90200

extern bool exportStatsSupported(int server_version_num);
extern bool exportExtStatsSupported(int server_version_num);

extern const char *exportClassStatsSQL(int server_verson_num);
extern const char *exportAttributeStatsSQL(int server_verson_num);

extern char *exportRelationStatsSQL(int server_version_num);

#endif
