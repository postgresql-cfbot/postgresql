/*-------------------------------------------------------------------------
 *
 * pg_db_role_setting.h
 *	definition of configuration settings
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_db_role_setting.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DB_ROLE_SETTING_H
#define PG_DB_ROLE_SETTING_H

#include "utils/guc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/* ----------------
 *		pg_db_role_setting definition.  cpp turns this into
 *		typedef struct FormData_pg_db_role_setting
 * ----------------
 */
#define DbRoleSettingRelationId 2964

CATALOG(pg_db_role_setting,2964) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid			setdatabase;	/* database */
	Oid			setrole;		/* role */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		setconfig[1];	/* GUC settings to apply at login */
#endif
} FormData_pg_db_role_setting;

DECLARE_TOAST(pg_db_role_setting, 2966, 2967);
#define PgDbRoleSettingToastTable 2966
#define PgDbRoleSettingToastIndex 2967
DECLARE_UNIQUE_INDEX(pg_db_role_setting_databaseid_rol_index, 2965, on pg_db_role_setting using btree(setdatabase oid_ops, setrole oid_ops));
#define DbRoleSettingDatidRolidIndexId	2965

typedef FormData_pg_db_role_setting * Form_pg_db_role_setting;

/* ----------------
 *		compiler constants for pg_db_role_setting
 * ----------------
 */
#define Natts_pg_db_role_setting				3
#define Anum_pg_db_role_setting_setdatabase		1
#define Anum_pg_db_role_setting_setrole			2
#define Anum_pg_db_role_setting_setconfig		3

/*
 * prototypes for functions in pg_db_role_setting.h
 */
extern void AlterSetting(Oid databaseid, Oid roleid, VariableSetStmt *setstmt);
extern void DropSetting(Oid databaseid, Oid roleid);
extern void ApplySetting(Snapshot snapshot, Oid databaseid, Oid roleid,
			 Relation relsetting, GucSource source);

#endif							/* PG_DB_ROLE_SETTING_H */
