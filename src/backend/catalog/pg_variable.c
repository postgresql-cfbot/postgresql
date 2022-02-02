/*-------------------------------------------------------------------------
 *
 * pg_variable.c
 *		session variables
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
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
 * Fetch attributes (without acl) of session variable from the syscache.
 * We don't work with acl directly, so we don't need to read it here.
 * Skip deserialization of defexpr when fast_only is true.
 */
void
initVariable(Variable *var, Oid varid, bool fast_only)
{
	HeapTuple	tup;
	Form_pg_variable varform;
	Datum		defexpr_datum;
	bool		defexpr_isnull;

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for session variable %u", varid);

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
	var->is_immutable = varform->varisimmutable;

	/* Get defexpr */
	defexpr_datum = SysCacheGetAttr(VARIABLEOID,
									tup,
									Anum_pg_variable_vardefexpr,
									&defexpr_isnull);

	var->has_defexpr = !defexpr_isnull;

	/*
	 * Deserialize defexpr only when it is requested.
	 * We need to deserialize Node with default expression,
	 * only when we read from session variable, and this
	 * session variable has not assigned value, and this
	 * session variable has default expression. For other
	 * cases, we skip skip this operation.
	 */
	if (!fast_only && !defexpr_isnull)
		var->defexpr = stringToNode(TextDatumGetCString(defexpr_datum));
	else
		var->defexpr = NULL;

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
			   bool if_not_exists,
			   bool is_immutable)
{
	Acl		   *varacl;
	NameData	varname;
	bool		nulls[Natts_pg_variable];
	Datum		values[Natts_pg_variable];
	Relation	rel;
	HeapTuple	tup,
				oldtup;
	TupleDesc	tupdesc;
	ObjectAddress myself,
				referenced;
	ObjectAddresses *addrs;
	Oid			varid;
	int			i;

	for (i = 0; i < Natts_pg_variable; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	namestrcpy(&varname, varName);

	rel = table_open(VariableRelationId, RowExclusiveLock);

	varid = GetNewOidWithIndex(rel, VariableObjectIndexId, Anum_pg_variable_oid);
	values[Anum_pg_variable_oid - 1] = ObjectIdGetDatum(varid);
	values[Anum_pg_variable_varname - 1] = NameGetDatum(&varname);
	values[Anum_pg_variable_varnamespace - 1] = ObjectIdGetDatum(varNamespace);
	values[Anum_pg_variable_vartype - 1] = ObjectIdGetDatum(varType);
	values[Anum_pg_variable_vartypmod - 1] = Int32GetDatum(varTypmod);
	values[Anum_pg_variable_varowner - 1] = ObjectIdGetDatum(varOwner);
	values[Anum_pg_variable_varcollation - 1] = ObjectIdGetDatum(varCollation);
	values[Anum_pg_variable_vareoxaction - 1] = CharGetDatum((char) to_eoxaction_code(eoxaction));
	values[Anum_pg_variable_varisnotnull - 1] = BoolGetDatum(is_not_null);
	values[Anum_pg_variable_varisimmutable - 1] = BoolGetDatum(is_immutable);
	/* varacl will be determined later */

	if (varDefexpr)
		values[Anum_pg_variable_vardefexpr - 1] = CStringGetTextDatum(nodeToString(varDefexpr));
	else
		nulls[Anum_pg_variable_vardefexpr - 1] = true;

	tupdesc = RelationGetDescr(rel);

	oldtup = SearchSysCache2(VARIABLENAMENSP,
							 PointerGetDatum(varName),
							 ObjectIdGetDatum(varNamespace));

	if (HeapTupleIsValid(oldtup))
	{
		if (if_not_exists)
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("session variable \"%s\" already exists, skipping",
							varName)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("session variable \"%s\" already exists",
							varName)));

		ReleaseSysCache(oldtup);
		table_close(rel, RowExclusiveLock);

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

	addrs = new_object_addresses();

	ObjectAddressSet(myself, VariableRelationId, varid);

	/* dependency on namespace */
	ObjectAddressSet(referenced, NamespaceRelationId, varNamespace);
	add_exact_object_address(&referenced, addrs);

	/* dependency on used type */
	ObjectAddressSet(referenced, TypeRelationId, varType);
	add_exact_object_address(&referenced, addrs);

	/* dependency on collation */
	if (OidIsValid(varCollation) &&
		varCollation != DEFAULT_COLLATION_OID)
	{
		ObjectAddressSet(referenced, CollationRelationId, varCollation);
		add_exact_object_address(&referenced, addrs);
	}

	/* dependency on default expr */
	if (varDefexpr)
		recordDependencyOnExpr(&myself, (Node *) varDefexpr,
							   NIL, DEPENDENCY_NORMAL);

	record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
	free_object_addresses(addrs);

	/* dependency on owner */
	recordDependencyOnOwner(VariableRelationId, varid, varOwner);

	/* dependencies on roles mentioned in default ACL */
	recordDependencyOnNewAcl(VariableRelationId, varid, 0, varOwner, varacl);

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/*
	 * For temporary variables, we need to create a new end of xact action to
	 * ensure deletion from catalog.
	 */
	if (eoxaction == VARIABLE_EOX_DROP)
	{
		Assert(isTempNamespace(varNamespace));

		RegisterOnCommitDropSessionVariable(myself.objectId);
	}

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(VariableRelationId, varid, 0);

	table_close(rel, RowExclusiveLock);

	return myself;
}
