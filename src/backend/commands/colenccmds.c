/*-------------------------------------------------------------------------
 *
 * colenccmds.c
 *	  column-encryption-related commands support code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/colenccmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_colenckey.h"
#include "catalog/pg_colenckeydata.h"
#include "catalog/pg_colmasterkey.h"
#include "catalog/pg_namespace.h"
#include "commands/colenccmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "common/colenc.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static void
parse_cek_attributes(ParseState *pstate, List *definition, Oid *cmkoid_p, int *alg_p, char **encval_p)
{
	ListCell   *lc;
	DefElem    *cmkEl = NULL;
	DefElem    *algEl = NULL;
	DefElem    *encvalEl = NULL;
	Oid			cmkoid = InvalidOid;
	int			alg = 0;
	char	   *encval = NULL;

	Assert(cmkoid_p);

	foreach(lc, definition)
	{
		DefElem    *defel = lfirst_node(DefElem, lc);
		DefElem   **defelp;

		if (strcmp(defel->defname, "column_master_key") == 0)
			defelp = &cmkEl;
		else if (strcmp(defel->defname, "algorithm") == 0)
			defelp = &algEl;
		else if (strcmp(defel->defname, "encrypted_value") == 0)
			defelp = &encvalEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column encryption key attribute \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
		}
		if (*defelp != NULL)
			errorConflictingDefElem(defel, pstate);
		*defelp = defel;
	}

	if (cmkEl)
	{
		List	   *val = defGetQualifiedName(cmkEl);

		cmkoid = get_cmk_oid(val, false);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("attribute \"%s\" must be specified",
						"column_master_key")));

	if (algEl)
	{
		char	   *val = defGetString(algEl);

		if (!alg_p)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("attribute \"%s\" must not be specified",
							"algorithm")));

		alg = get_cmkalg_num(val);
		if (!alg)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unrecognized encryption algorithm: %s", val));
	}
	else
	{
		if (alg_p)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("attribute \"%s\" must be specified",
							"algorithm")));
	}

	if (encvalEl)
	{
		if (!encval_p)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("attribute \"%s\" must not be specified",
							"encrypted_value")));

		encval = defGetString(encvalEl);
	}
	else
	{
		if (encval_p)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("attribute \"%s\" must be specified",
							"encrypted_value")));
	}

	*cmkoid_p = cmkoid;
	if (alg_p)
		*alg_p = alg;
	if (encval_p)
		*encval_p = encval;
}

static void
insert_cekdata_record(Oid cekoid, Oid cmkoid, int alg, char *encval)
{
	Oid			cekdataoid;
	Relation	rel;
	Datum		values[Natts_pg_colenckeydata] = {0};
	bool		nulls[Natts_pg_colenckeydata] = {0};
	HeapTuple	tup;
	ObjectAddress myself;
	ObjectAddress other;

	rel = table_open(ColumnEncKeyDataRelationId, RowExclusiveLock);

	cekdataoid = GetNewOidWithIndex(rel, ColumnEncKeyDataOidIndexId, Anum_pg_colenckeydata_oid);
	values[Anum_pg_colenckeydata_oid - 1] = ObjectIdGetDatum(cekdataoid);
	values[Anum_pg_colenckeydata_ckdcekid - 1] = ObjectIdGetDatum(cekoid);
	values[Anum_pg_colenckeydata_ckdcmkid - 1] = ObjectIdGetDatum(cmkoid);
	values[Anum_pg_colenckeydata_ckdcmkalg - 1] = Int32GetDatum(alg);
	values[Anum_pg_colenckeydata_ckdencval - 1] = DirectFunctionCall1(byteain, CStringGetDatum(encval));

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, ColumnEncKeyDataRelationId, cekdataoid);

	/* dependency cekdata -> cek */
	ObjectAddressSet(other, ColumnEncKeyRelationId, cekoid);
	recordDependencyOn(&myself, &other, DEPENDENCY_AUTO);

	/* dependency cekdata -> cmk */
	ObjectAddressSet(other, ColumnMasterKeyRelationId, cmkoid);
	recordDependencyOn(&myself, &other, DEPENDENCY_NORMAL);

	table_close(rel, NoLock);
}

ObjectAddress
CreateCEK(ParseState *pstate, DefineStmt *stmt)
{
	Oid			namespaceId;
	char	   *ceknamestr;
	AclResult	aclresult;
	Relation	rel;
	ObjectAddress myself;
	Oid			cekoid;
	ListCell   *lc;
	NameData	cekname;
	Datum		values[Natts_pg_colenckey] = {0};
	bool		nulls[Natts_pg_colenckey] = {0};
	HeapTuple	tup;

	namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames, &ceknamestr);

	aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));

	rel = table_open(ColumnEncKeyRelationId, RowExclusiveLock);

	if (SearchSysCacheExists2(CEKNAMENSP, PointerGetDatum(ceknamestr), ObjectIdGetDatum(namespaceId)))
		ereport(ERROR,
				errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("column encryption key \"%s\" already exists", ceknamestr));

	cekoid = GetNewOidWithIndex(rel, ColumnEncKeyOidIndexId, Anum_pg_colenckey_oid);

	foreach (lc, stmt->definition)
	{
		List	   *definition = lfirst_node(List, lc);
		Oid			cmkoid = 0;
		int			alg;
		char	   *encval;

		parse_cek_attributes(pstate, definition, &cmkoid, &alg, &encval);

		/* pg_colenckeydata */
		insert_cekdata_record(cekoid, cmkoid, alg, encval);
	}

	/* pg_colenckey */
	namestrcpy(&cekname, ceknamestr);
	values[Anum_pg_colenckey_oid - 1] = ObjectIdGetDatum(cekoid);
	values[Anum_pg_colenckey_cekname - 1] = NameGetDatum(&cekname);
	values[Anum_pg_colenckey_ceknamespace - 1] = ObjectIdGetDatum(namespaceId);
	values[Anum_pg_colenckey_cekowner - 1] = ObjectIdGetDatum(GetUserId());

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, ColumnEncKeyRelationId, cekoid);
	recordDependencyOnOwner(ColumnEncKeyRelationId, cekoid, GetUserId());

	table_close(rel, RowExclusiveLock);

	InvokeObjectPostCreateHook(ColumnEncKeyRelationId, cekoid, 0);

	return myself;
}

ObjectAddress
AlterColumnEncryptionKey(ParseState *pstate, AlterColumnEncryptionKeyStmt *stmt)
{
	Oid			cekoid;
	ObjectAddress address;

	cekoid = get_cek_oid(stmt->cekname, false);

	if (!object_ownercheck(ColumnEncKeyRelationId, cekoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_CEK, NameListToString(stmt->cekname));

	if (stmt->isDrop)
	{
		Oid			cmkoid = 0;
		Oid			cekdataoid;
		ObjectAddress obj;

		parse_cek_attributes(pstate, stmt->definition, &cmkoid, NULL, NULL);
		cekdataoid = get_cekdata_oid(cekoid, cmkoid, false);
		ObjectAddressSet(obj, ColumnEncKeyDataRelationId, cekdataoid);
		performDeletion(&obj, DROP_CASCADE, 0);
	}
	else
	{
		Oid			cmkoid = 0;
		int			alg;
		char	   *encval;

		parse_cek_attributes(pstate, stmt->definition, &cmkoid, &alg, &encval);
		if (get_cekdata_oid(cekoid, cmkoid, true))
			ereport(ERROR,
					errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("column encryption key \"%s\" already has data for master key \"%s\"",
						   NameListToString(stmt->cekname), get_cmk_name(cmkoid, false)));
		insert_cekdata_record(cekoid, cmkoid, alg, encval);
	}

	InvokeObjectPostAlterHook(ColumnEncKeyRelationId, cekoid, 0);
	ObjectAddressSet(address, ColumnEncKeyRelationId, cekoid);

	return address;
}

ObjectAddress
CreateCMK(ParseState *pstate, DefineStmt *stmt)
{
	Oid			namespaceId;
	char	   *cmknamestr;
	AclResult	aclresult;
	Relation	rel;
	ObjectAddress myself;
	Oid			cmkoid;
	ListCell   *lc;
	DefElem    *realmEl = NULL;
	char	   *realm;
	NameData	cmkname;
	Datum		values[Natts_pg_colmasterkey] = {0};
	bool		nulls[Natts_pg_colmasterkey] = {0};
	HeapTuple	tup;

	namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames, &cmknamestr);

	aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));

	rel = table_open(ColumnMasterKeyRelationId, RowExclusiveLock);

	if (SearchSysCacheExists2(CMKNAMENSP, PointerGetDatum(cmknamestr), ObjectIdGetDatum(namespaceId)))
		ereport(ERROR,
				errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("column master key \"%s\" already exists", cmknamestr));

	foreach(lc, stmt->definition)
	{
		DefElem    *defel = lfirst_node(DefElem, lc);
		DefElem   **defelp;

		if (strcmp(defel->defname, "realm") == 0)
			defelp = &realmEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column master key attribute \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
		}
		if (*defelp != NULL)
			errorConflictingDefElem(defel, pstate);
		*defelp = defel;
	}

	if (realmEl)
		realm = defGetString(realmEl);
	else
		realm = "";

	cmkoid = GetNewOidWithIndex(rel, ColumnMasterKeyOidIndexId, Anum_pg_colmasterkey_oid);
	namestrcpy(&cmkname, cmknamestr);
	values[Anum_pg_colmasterkey_oid - 1] = ObjectIdGetDatum(cmkoid);
	values[Anum_pg_colmasterkey_cmkname - 1] = NameGetDatum(&cmkname);
	values[Anum_pg_colmasterkey_cmknamespace - 1] = ObjectIdGetDatum(namespaceId);
	values[Anum_pg_colmasterkey_cmkowner - 1] = ObjectIdGetDatum(GetUserId());
	values[Anum_pg_colmasterkey_cmkrealm - 1] = CStringGetTextDatum(realm);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	recordDependencyOnOwner(ColumnMasterKeyRelationId, cmkoid, GetUserId());

	ObjectAddressSet(myself, ColumnMasterKeyRelationId, cmkoid);

	table_close(rel, RowExclusiveLock);

	InvokeObjectPostCreateHook(ColumnMasterKeyRelationId, cmkoid, 0);

	return myself;
}

ObjectAddress
AlterColumnMasterKey(ParseState *pstate, AlterColumnMasterKeyStmt *stmt)
{
	Oid			cmkoid;
	Relation	rel;
	HeapTuple	tup;
	HeapTuple	newtup;
	ObjectAddress address;
	ListCell   *lc;
	DefElem    *realmEl = NULL;
	Datum		values[Natts_pg_colmasterkey] = {0};
	bool		nulls[Natts_pg_colmasterkey] = {0};
	bool		replaces[Natts_pg_colmasterkey] = {0};

	cmkoid = get_cmk_oid(stmt->cmkname, false);

	rel = table_open(ColumnMasterKeyRelationId, RowExclusiveLock);

	tup = SearchSysCache1(CMKOID, ObjectIdGetDatum(cmkoid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for column master key %u", cmkoid);

	if (!object_ownercheck(ColumnMasterKeyRelationId, cmkoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_CMK, NameListToString(stmt->cmkname));

	foreach(lc, stmt->definition)
	{
		DefElem    *defel = lfirst_node(DefElem, lc);
		DefElem   **defelp;

		if (strcmp(defel->defname, "realm") == 0)
			defelp = &realmEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column master key attribute \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
		}
		if (*defelp != NULL)
			errorConflictingDefElem(defel, pstate);
		*defelp = defel;
	}

	if (realmEl)
	{
		values[Anum_pg_colmasterkey_cmkrealm - 1] = CStringGetTextDatum(defGetString(realmEl));
		replaces[Anum_pg_colmasterkey_cmkrealm - 1] = true;
	}

	newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

	CatalogTupleUpdate(rel, &tup->t_self, newtup);

	InvokeObjectPostAlterHook(ColumnMasterKeyRelationId, cmkoid, 0);

	ObjectAddressSet(address, ColumnMasterKeyRelationId, cmkoid);

	heap_freetuple(newtup);
	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);

	return address;
}
