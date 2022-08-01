/*-------------------------------------------------------------------------
 *
 * colenccmds.c
 *	  column-encryption-related commands support code
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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
#include "catalog/objectaccess.h"
#include "catalog/pg_colenckey.h"
#include "catalog/pg_colenckeydata.h"
#include "catalog/pg_colmasterkey.h"
#include "commands/colenccmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

ObjectAddress
CreateCEK(ParseState *pstate, DefineStmt *stmt)
{
	AclResult	aclresult;
	Relation	rel;
	Relation	rel2;
	ObjectAddress myself;
	Oid			cekoid;
	ListCell   *lc;
	DefElem    *cmkEl = NULL;
	DefElem    *algEl = NULL;
	DefElem    *encvalEl = NULL;
	Oid			cmkoid = 0;
	int16		alg;
	char	   *encval;
	Datum		values[Natts_pg_colenckey] = {0};
	bool		nulls[Natts_pg_colenckey] = {0};
	Datum		values2[Natts_pg_colenckeydata] = {0};
	bool		nulls2[Natts_pg_colenckeydata] = {0};
	HeapTuple	tup;

	aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(MyDatabaseId));

	rel = table_open(ColumnEncKeyRelationId, RowExclusiveLock);
	rel2 = table_open(ColumnEncKeyDataRelationId, RowExclusiveLock);

	cekoid = GetSysCacheOid1(CEKNAME, Anum_pg_colenckey_oid,
							 CStringGetDatum(strVal(llast(stmt->defnames))));
	if (OidIsValid(cekoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("column encryption key \"%s\" already exists",
						strVal(llast(stmt->defnames)))));

	foreach(lc, stmt->definition)
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
		char	   *val = defGetString(cmkEl);

		cmkoid = GetSysCacheOid1(CMKNAME, Anum_pg_colmasterkey_oid, PointerGetDatum(val));
		if (!cmkoid)
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("column master key \"%s\" does not exist", val));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("attribute \"%s\" must be specified",
						"column_master_key")));

	if (algEl)
	{
		char	   *val = defGetString(algEl);

		if (strcmp(val, "RSAES_OAEP_SHA_1") == 0)
			alg = PG_CMK_RSAES_OAEP_SHA_1;
		else if (strcmp(val, "PG_CMK_RSAES_OAEP_SHA_256") == 0)
			alg = PG_CMK_RSAES_OAEP_SHA_256;
		else
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unrecognized encryption algorithm: %s", val));
	}
	else
		alg = PG_CMK_RSAES_OAEP_SHA_1;

	if (encvalEl)
		encval = defGetString(encvalEl);
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("attribute \"%s\" must be specified",
						"encrypted_value")));

	/* pg_colenckey */
	cekoid = GetNewOidWithIndex(rel, ColumnEncKeyOidIndexId, Anum_pg_colenckey_oid);
	values[Anum_pg_colenckey_oid - 1] = ObjectIdGetDatum(cekoid);
	values[Anum_pg_colenckey_cekname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(strVal(llast(stmt->defnames))));
	values[Anum_pg_colenckey_cekowner - 1] = ObjectIdGetDatum(GetUserId());

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	/* pg_colenckeydata */
	values2[Anum_pg_colenckeydata_oid - 1] =
		ObjectIdGetDatum(GetNewOidWithIndex(rel, ColumnEncKeyDataOidIndexId, Anum_pg_colenckeydata_oid));
	values2[Anum_pg_colenckeydata_ckdcekid - 1] = ObjectIdGetDatum(cekoid);
	values2[Anum_pg_colenckeydata_ckdcmkid - 1] = ObjectIdGetDatum(cmkoid);
	values2[Anum_pg_colenckeydata_ckdcmkalg - 1] = Int16GetDatum(alg);
	values2[Anum_pg_colenckeydata_ckdencval - 1] = DirectFunctionCall1(byteain, CStringGetDatum(encval));

	tup = heap_form_tuple(RelationGetDescr(rel2), values2, nulls2);
	CatalogTupleInsert(rel2, tup);
	heap_freetuple(tup);

	recordDependencyOnOwner(ColumnEncKeyRelationId, cekoid, GetUserId());

	ObjectAddressSet(myself, ColumnEncKeyRelationId, cekoid);

	table_close(rel2, RowExclusiveLock);
	table_close(rel, RowExclusiveLock);

	InvokeObjectPostCreateHook(ColumnMasterKeyRelationId, cekoid, 0);

	return myself;
}

ObjectAddress
CreateCMK(ParseState *pstate, DefineStmt *stmt)
{
	AclResult	aclresult;
	Relation	rel;
	ObjectAddress myself;
	Oid			cmkoid;
	ListCell   *lc;
	DefElem    *realmEl = NULL;
	char	   *realm;
	Datum		values[Natts_pg_colmasterkey] = {0};
	bool		nulls[Natts_pg_colmasterkey] = {0};
	HeapTuple	tup;

	aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(MyDatabaseId));

	rel = table_open(ColumnMasterKeyRelationId, RowExclusiveLock);

	cmkoid = GetSysCacheOid1(CMKNAME, Anum_pg_colmasterkey_oid,
							 CStringGetDatum(strVal(llast(stmt->defnames))));
	if (OidIsValid(cmkoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("column master key \"%s\" already exists",
						strVal(llast(stmt->defnames)))));

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
	values[Anum_pg_colmasterkey_oid - 1] = ObjectIdGetDatum(cmkoid);
	values[Anum_pg_colmasterkey_cmkname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(strVal(llast(stmt->defnames))));
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
