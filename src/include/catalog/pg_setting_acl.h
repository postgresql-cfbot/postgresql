/*-------------------------------------------------------------------------
 *
 * pg_setting_acl.h
 *	  definition of the "configuration parameter" system catalog
 *	  (pg_setting_acl).
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_setting_acl.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SETTING_ACL_H
#define PG_SETTING_ACL_H

#include "catalog/genbki.h"
#include "catalog/pg_setting_acl_d.h"

/* ----------------
 *		pg_setting_acl definition.  cpp turns this into
 *		typedef struct FormData_pg_setting_acl
 * ----------------
 */
CATALOG(pg_setting_acl,8924,SettingAclRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */
	/*

	 * Variable-length fields start here, but we allow direct access to
	 * setting.
	 */
	text		setting BKI_FORCE_NOT_NULL;

#ifdef CATALOG_VARLEN
	/* Access privileges */
	aclitem		setacl[1] BKI_DEFAULT(_null_);
#endif
} FormData_pg_setting_acl;


/* ----------------
 *		Form_pg_setting_acl corresponds to a pointer to a tuple with
 *		the format of pg_setting_acl relation.
 * ----------------
 */
typedef FormData_pg_setting_acl *Form_pg_setting_acl;

DECLARE_TOAST(pg_setting_acl, 8925, 8926);
#define PgSettingAclToastTable 8925
#define PgSettingAclToastIndex 8926

DECLARE_UNIQUE_INDEX(pg_setting_acl_setting_index, 8927, SettingAclSettingIndexId, on pg_setting_acl using btree(setting text_ops));
DECLARE_UNIQUE_INDEX_PKEY(pg_setting_acl_oid_index, 8928, SettingAclOidIndexId, on pg_setting_acl using btree(oid oid_ops));

extern Oid	SettingAclCreate(const char *setting, bool if_not_exists);

#endif							/* PG_SETTING_ACL_H */
