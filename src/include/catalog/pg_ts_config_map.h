/*-------------------------------------------------------------------------
 *
 * pg_ts_config_map.h
 *	definition of token mappings for configurations of tsearch
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_ts_config_map.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TS_CONFIG_MAP_H
#define PG_TS_CONFIG_MAP_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_ts_config_map definition.  cpp turns this into
 *		typedef struct FormData_pg_ts_config_map
 * ----------------
 */
#define TSConfigMapRelationId	3603

CATALOG(pg_ts_config_map,3603) BKI_WITHOUT_OIDS
{
	Oid			mapcfg;			/* OID of configuration owning this entry */
	int32		maptokentype;	/* token type from parser */
	int32		mapseqno;		/* order in which to consult dictionaries */
	Oid			mapdict;		/* dictionary to consult */
} FormData_pg_ts_config_map;

DECLARE_UNIQUE_INDEX(pg_ts_config_map_index, 3609, on pg_ts_config_map using btree(mapcfg oid_ops, maptokentype int4_ops, mapseqno int4_ops));
#define TSConfigMapIndexId	3609

typedef FormData_pg_ts_config_map *Form_pg_ts_config_map;

/* ----------------
 *		compiler constants for pg_ts_config_map
 * ----------------
 */
#define Natts_pg_ts_config_map				4
#define Anum_pg_ts_config_map_mapcfg		1
#define Anum_pg_ts_config_map_maptokentype	2
#define Anum_pg_ts_config_map_mapseqno		3
#define Anum_pg_ts_config_map_mapdict		4

#endif							/* PG_TS_CONFIG_MAP_H */
