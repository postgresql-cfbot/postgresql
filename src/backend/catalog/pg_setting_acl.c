/*-------------------------------------------------------------------------
 *
 * pg_setting_acl.c
 *	  routines to support manipulation of the pg_setting_acl relation
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_setting_acl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_setting_acl.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * SettingAclCreate
 *
 * Add a new tuple to pg_setting_acl.
 *
 * setting: the setting name to create.
 * if_not_exists: if true, don't fail on duplicate name, but rather return
 * the existing entry's Oid.
 */
Oid
SettingAclCreate(const char *setting, bool if_not_exists)
{
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	Datum		values[Natts_pg_setting_acl];
	bool		nulls[Natts_pg_setting_acl];
	Oid			settingId;
	const char *canonical;

	/*
	 * Check whether the setting (by the given name or alias)
	 * already exists.
	 */
	settingId = get_setting_oid(setting, true);
	if (OidIsValid(settingId))
	{
		if (if_not_exists)
			return settingId;
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("setting \"%s\" already exists",
						setting)));
	}

	/*
	 * We must not require the setting to be in the list of existent GUCs,
	 * as we may be called at different points during upgrades or the
	 * installation or removal of extensions.  Instead, perform a basic sanity
	 * check of the setting, and translate old forms of known names to their
	 * canonical forms.
	 *
	 * If you deprecate a configuration name in favor of a new spelling, be
	 * sure to consider whether to also upgrade pg_setting_acl entries.
	 */
	if (!valid_variable_name(setting, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("invalid setting name \"%s\"",
						setting)));
	canonical = GetConfigOptionCanonicalName(setting);
	if (!canonical)
		canonical = setting;

	/*
	 * Create and insert a new record, starting with a blank Acl.
	 *
	 * We don't take a strong enough lock to prevent concurrent insertions,
	 * relying instead on the unique index.
	 */
	rel = table_open(SettingAclRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	values[Anum_pg_setting_acl_setting - 1] =
		DirectFunctionCall1(textin, CStringGetDatum(canonical));
	settingId = GetNewOidWithIndex(rel,
									   SettingAclOidIndexId,
									   Anum_pg_setting_acl_oid);
	values[Anum_pg_setting_acl_oid - 1] = ObjectIdGetDatum(settingId);
	nulls[Anum_pg_setting_acl_setacl - 1] = true;
	tuple = heap_form_tuple(tupDesc, values, nulls);
	CatalogTupleInsert(rel, tuple);

	/* Post creation hook for new setting */
	InvokeObjectPostCreateHook(SettingAclRelationId, settingId, 0);

	/*
	 * Close pg_setting_acl, but keep lock till commit.
	 */
	heap_freetuple(tuple);
	table_close(rel, NoLock);

	return settingId;
}
