/*-------------------------------------------------------------------------
 *
 * pg_statistic_ext.h
 *	  definition of the system "extended statistic" relation (pg_statistic_ext)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_statistic_ext.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_STATISTIC_EXT_H
#define PG_STATISTIC_EXT_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_statistic_ext definition.  cpp turns this into
 *		typedef struct FormData_pg_statistic_ext
 * ----------------
 */
#define StatisticExtRelationId	3381

CATALOG(pg_statistic_ext,3381)
{
	Oid			stxrelid;		/* relation containing attributes */

	/* These two fields form the unique key for the entry: */
	NameData	stxname;		/* statistics object name */
	Oid			stxnamespace;	/* OID of statistics object's namespace */

	Oid			stxowner;		/* statistics object's owner */

	/*
	 * variable-length fields start here, but we allow direct access to
	 * stxkeys
	 */
	int2vector	stxkeys;		/* array of column keys */

#ifdef CATALOG_VARLEN
	char		stxkind[1] BKI_FORCE_NOT_NULL;	/* statistics kinds requested
												 * to build */
	pg_ndistinct stxndistinct;	/* ndistinct coefficients (serialized) */
	pg_dependencies stxdependencies;	/* dependencies (serialized) */
#endif

} FormData_pg_statistic_ext;

DECLARE_TOAST(pg_statistic_ext, 3439, 3440);
DECLARE_UNIQUE_INDEX(pg_statistic_ext_oid_index, 3380, on pg_statistic_ext using btree(oid oid_ops));
#define StatisticExtOidIndexId	3380
DECLARE_UNIQUE_INDEX(pg_statistic_ext_name_index, 3997, on pg_statistic_ext using btree(stxname name_ops, stxnamespace oid_ops));
#define StatisticExtNameIndexId 3997
DECLARE_INDEX(pg_statistic_ext_relid_index, 3379, on pg_statistic_ext using btree(stxrelid oid_ops));
#define StatisticExtRelidIndexId 3379

/* ----------------
 *		Form_pg_statistic_ext corresponds to a pointer to a tuple with
 *		the format of pg_statistic_ext relation.
 * ----------------
 */
typedef FormData_pg_statistic_ext *Form_pg_statistic_ext;

/* ----------------
 *		compiler constants for pg_statistic_ext
 * ----------------
 */
#define Natts_pg_statistic_ext					8
#define Anum_pg_statistic_ext_stxrelid			1
#define Anum_pg_statistic_ext_stxname			2
#define Anum_pg_statistic_ext_stxnamespace		3
#define Anum_pg_statistic_ext_stxowner			4
#define Anum_pg_statistic_ext_stxkeys			5
#define Anum_pg_statistic_ext_stxkind			6
#define Anum_pg_statistic_ext_stxndistinct		7
#define Anum_pg_statistic_ext_stxdependencies	8

#define STATS_EXT_NDISTINCT			'd'
#define STATS_EXT_DEPENDENCIES		'f'

#endif							/* PG_STATISTIC_EXT_H */
