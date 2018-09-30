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

#include "access/heapam.h"
#include "access/htup_details.h"

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_variable.h"

#include "commands/schemavariable.h"

#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static VariableEOXActionCodes
to_eoxaction_code(VariableEOXAction action)
{
	switch (action)
	{
		case VARIABLE_EOX_NOOP:
			return VARIABLE_EOX_CODE_NOOP;

		case VARIABLE_EOX_DROP:
			return VARIABLE_EOX_CODE_DROP;

		case VARIABLE_EOX_RESET:
			return VARIABLE_EOX_CODE_RESET;

		default:
			elog(ERROR, "unexpected action");
	}

}

static VariableEOXAction
to_eoxaction(VariableEOXActionCodes code)
{
	switch (code)
	{
		case VARIABLE_EOX_CODE_NOOP:
			return VARIABLE_EOX_NOOP;

		case VARIABLE_EOX_CODE_DROP:
			return VARIABLE_EOX_DROP;

		case VARIABLE_EOX_CODE_RESET:
			return VARIABLE_EOX_RESET;

		default:
			elog(ERROR, "unexpected code");
	}
}

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
get_schema_variable_type_typmod_collid(Oid varid, Oid *typid, int32 *typmod, Oid *collid)
{
	HeapTuple	tup;
	Form_pg_variable varform;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	varform = (Form_pg_variable) GETSTRUCT(tup);

	*typid = varform->vartype;
	*typmod = varform->vartypmod;
	*collid = varform->varcollation;

	ReleaseSysCache(tup);

	return;
}

/*
 * Fetch all fields of schema variable from the syscache.
 */
void
initVariable(Variable *var, Oid varid, bool missing_ok, bool fast_only)
{
	HeapTuple	tup;
	Form_pg_variable varform;
	Datum		defexpr_datum;
	bool		defexpr_isnull;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
	{
		if (missing_ok)
		{
			var->oid = InvalidOid;
			return;
		}

		elog(ERROR, "cache lookup failed for variable %u", varid);
	}

	varform = (Form_pg_variable) GETSTRUCT(tup);

	var->oid = varid;
	var->name = pstrdup(NameStr(varform->varname));
	var->namespace = varform->varnamespace;
	var->typid = varform->vartype;
	var->typmod = varform->vartypmod;
	var->owner = varform->varowner;
	var->collation = varform->varcollation;
	var->eoxaction = to_eoxaction(varform->vareoxaction);
	var->is_not_null = varform->varisnotnull;
	var->is_transact = varform->varistransact;

	/* Get defexpr */
	defexpr_datum = SysCacheGetAttr(VARIABLEOID,
												tup,
												Anum_pg_variable_vardefexpr,
												&defexpr_isnull);

	var->has_defexpr = !defexpr_isnull;

	/*
	 * Sometimes we don't need deserialized defexpr, but we need
	 * info about existence of defexpr.
	 */

	if (!fast_only)
	{
		Datum		aclDatum;
		bool		isnull;

		/* name */
		var->name = pstrdup(NameStr(varform->varname));

		if (!defexpr_isnull)
			var->defexpr = stringToNode(TextDatumGetCString(defexpr_datum));
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
	}
	else
	{
		var->name = NULL;
		var->defexpr = NULL;
		var->acl = NULL;
	}

	ReleaseSysCache(tup);

	return;
}

/*
 * Create entry in pg_variable table
 */
ObjectAddress
VariableCreate(const char *varName,
			   Oid varNamespace,
			   Oid varType,
			   int32 varTypmod,
			   Oid varOwner,
			   Oid varCollation,
			   Node *varDefexpr,
			   VariableEOXAction eoxaction,
			   bool is_not_null,
			   bool is_transact,
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
	values[Anum_pg_variable_varcollation - 1] = ObjectIdGetDatum(varCollation);
	values[Anum_pg_variable_vareoxaction - 1] = CharGetDatum((char) to_eoxaction_code(eoxaction));
	values[Anum_pg_variable_varisnotnull - 1] = BoolGetDatum(is_not_null);
	values[Anum_pg_variable_varistransact - 1] = BoolGetDatum(is_transact);
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

	/*
	 * register on commit action if it is necessary. This moment, the
	 * variable has not any value, so we don't need to solve content
	 * transactionality.
	 */
	register_variable_on_commit_action(myself.objectId, eoxaction);

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(VariableRelationId, retval, 0);

	heap_close(rel, RowExclusiveLock);

	return myself;
}
