/*-------------------------------------------------------------------------
 *
 * pg_variable.c
 *		schema variables
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_variable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_variable.h"

#include "nodes/makefuncs.h"

#include "storage/lmgr.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Returns name of schema variable. When variable is not on path,
 * then the name is qualified.
 */
char *
schema_variable_get_name(Oid varid)
{
	HeapTuple	tup;
	Form_pg_variable varform;
	char   *varname;
	char   *nspname;
	char   *result;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	varform = (Form_pg_variable) GETSTRUCT(tup);

	varname = NameStr(varform->varname);

	if (VariableIsVisible(varid))
		nspname = NULL;
	else
		nspname = get_namespace_name(varform->varnamespace);

	result = quote_qualified_identifier(nspname, varname);

	ReleaseSysCache(tup);

	return result;
}

/*
 * Returns varname field of pg_variable
 */
char *
get_schema_variable_name(Oid varid)
{
	HeapTuple	tup;
	Form_pg_variable varform;
	char   *varname;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	varform = (Form_pg_variable) GETSTRUCT(tup);

	varname = NameStr(varform->varname);

	ReleaseSysCache(tup);

	return varname;
}

/*
 * Returns type, typmod of schema variable
 */
void
get_schema_variable_type_typmod(Oid varid, Oid *typid, int32 *typmod)
{
	HeapTuple	tup;
	Form_pg_variable varform;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	varform = (Form_pg_variable) GETSTRUCT(tup);

	*typid = varform->vartype;
	*typmod = varform->vartypmod;

	ReleaseSysCache(tup);

	return;
}

/*
 * Fetch all fields of schema variable from the syscache.
 */
Variable *
GetVariable(Oid varid, bool missing_ok)
{
	HeapTuple	tup;
	Variable *var;
	Form_pg_variable varform;
	Datum		aclDatum;
	Datum		defexprDatum;
	bool		isnull;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
			return NULL;

		elog(ERROR, "cache lookup failed for variable %u", varid);
	}

	varform = (Form_pg_variable) GETSTRUCT(tup);

	var = (Variable *) palloc(sizeof(Variable));
	var->oid = varid;
	var->name = pstrdup(NameStr(varform->varname));
	var->namespace = varform->varnamespace;
	var->typid = varform->vartype;
	var->typmod = varform->vartypmod;
	var->owner = varform->varowner;

	/* Get defexpr */
	defexprDatum = SysCacheGetAttr(VARIABLEOID,
							tup,
							Anum_pg_variable_vardefexpr,
							&isnull);

	if (!isnull)
		var->defexpr = stringToNode(TextDatumGetCString(defexprDatum));
	else
		var->defexpr = NULL;

	/* Get varacl */
	aclDatum = SysCacheGetAttr(VARIABLEOID,
							tup,
							Anum_pg_variable_varacl,
							&isnull);
	if (!isnull)
		var->acl = DatumGetAclPCopy(aclDatum);
	else
		var->acl = NULL;

	ReleaseSysCache(tup);

	return var;
}

ObjectAddress
VariableCreate(const char *varName,
			   Oid varNamespace,
			   Oid varType,
			   int32 varTypmod,
			   Oid varOwner,
			   Node *varDefexpr,
			   bool if_not_exists)
{
	Acl		   *varacl;
	NameData	varname;
	bool		nulls[Natts_pg_variable];
	Datum		values[Natts_pg_variable];
	Relation	rel;
	HeapTuple	tup,
				oldtup;
	TupleDesc	tupdesc;
	ObjectAddress	myself,
					referenced;
	Oid			retval;
	int			i;

	for (i = 0; i < Natts_pg_variable; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	namestrcpy(&varname, varName);
	values[Anum_pg_variable_varname - 1] = NameGetDatum(&varname);
	values[Anum_pg_variable_varnamespace - 1] = ObjectIdGetDatum(varNamespace);
	values[Anum_pg_variable_vartype - 1] = ObjectIdGetDatum(varType);
	values[Anum_pg_variable_vartypmod - 1] = Int32GetDatum(varTypmod);
	values[Anum_pg_variable_varowner - 1] = ObjectIdGetDatum(varOwner);
	/* proacl will be determined later */

	if (varDefexpr)
		values[Anum_pg_variable_vardefexpr - 1] = CStringGetTextDatum(nodeToString(varDefexpr));
	else
		nulls[Anum_pg_variable_vardefexpr - 1] = true;

	rel = heap_open(VariableRelationId, RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	oldtup = SearchSysCache2(VARIABLENAMENSP,
							 PointerGetDatum(varName),
							 ObjectIdGetDatum(varNamespace));

	if (HeapTupleIsValid(oldtup))
	{
		if (if_not_exists)
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("schema variable \"%s\" already exists, skipping",
							varName)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("schema variable \"%s\" already exists",
							varName)));

		heap_freetuple(oldtup);
		heap_close(rel, RowExclusiveLock);

		return InvalidObjectAddress;
	}

	varacl = get_user_default_acl(OBJECT_VARIABLE, varOwner,
								  varNamespace);

	if (varacl != NULL)
		values[Anum_pg_variable_varacl - 1] = PointerGetDatum(varacl);
	else
		nulls[Anum_pg_variable_varacl - 1] = true;

	tup = heap_form_tuple(tupdesc, values, nulls);
	CatalogTupleInsert(rel, tup);

	retval = HeapTupleGetOid(tup);

	myself.classId = VariableRelationId;
	myself.objectId = retval;
	myself.objectSubId = 0;

	/* dependency on namespace */
	referenced.classId = NamespaceRelationId;
	referenced.objectId = varNamespace;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on used type */
	referenced.classId = TypeRelationId;
	referenced.objectId = varType;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on any roles mentioned in ACL */
	if (varacl != NULL)
	{
		int			nnewmembers;
		Oid		   *newmembers;

		nnewmembers = aclmembers(varacl, &newmembers);
		updateAclDependencies(VariableRelationId, retval, 0,
							  varOwner,
							  0, NULL,
							  nnewmembers, newmembers);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(VariableRelationId, retval, 0);

	heap_close(rel, RowExclusiveLock);

	return myself;
}
