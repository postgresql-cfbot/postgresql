/*-------------------------------------------------------------------------
 *
 * pg_variable.c
 *		session variables
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_variable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"

static ObjectAddress create_variable(const char *varName,
									 Oid varNamespace,
									 Oid varType,
									 int32 varTypmod,
									 Oid varOwner,
									 Oid varCollation,
									 bool if_not_exists,
									 bool is_not_null,
									 bool is_immutable,
									 Node *varDefexpr,
									 VariableEOXAction eoxaction);


/*
 * Creates entry in pg_variable table
 */
static ObjectAddress
create_variable(const char *varName,
				Oid varNamespace,
				Oid varType,
				int32 varTypmod,
				Oid varOwner,
				Oid varCollation,
				bool if_not_exists,
				bool is_not_null,
				bool is_immutable,
				Node *varDefexpr,
				VariableEOXAction eoxaction)
{
	Acl		   *varacl;
	NameData	varname;
	bool		nulls[Natts_pg_variable];
	Datum		values[Natts_pg_variable];
	Relation	rel;
	HeapTuple	tup;
	TupleDesc	tupdesc;
	ObjectAddress myself,
				referenced;
	ObjectAddresses *addrs;
	Oid			varid;

	Assert(varName);
	Assert(OidIsValid(varNamespace));
	Assert(OidIsValid(varType));
	Assert(OidIsValid(varOwner));

	rel = table_open(VariableRelationId, RowExclusiveLock);

	/*
	 * Check for duplicates. Note that this does not really prevent
	 * duplicates, it's here just to provide nicer error message in common
	 * case. The real protection is the unique key on the catalog.
	 */
	if (SearchSysCacheExists2(VARIABLENAMENSP,
							  PointerGetDatum(varName),
							  ObjectIdGetDatum(varNamespace)))
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

		table_close(rel, RowExclusiveLock);

		return InvalidObjectAddress;
	}

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	namestrcpy(&varname, varName);

	varid = GetNewOidWithIndex(rel, VariableObjectIndexId, Anum_pg_variable_oid);

	values[Anum_pg_variable_oid - 1] = ObjectIdGetDatum(varid);
	values[Anum_pg_variable_varcreate_lsn - 1] = LSNGetDatum(GetXLogInsertRecPtr());
	values[Anum_pg_variable_varname - 1] = NameGetDatum(&varname);
	values[Anum_pg_variable_varnamespace - 1] = ObjectIdGetDatum(varNamespace);
	values[Anum_pg_variable_vartype - 1] = ObjectIdGetDatum(varType);
	values[Anum_pg_variable_vartypmod - 1] = Int32GetDatum(varTypmod);
	values[Anum_pg_variable_varowner - 1] = ObjectIdGetDatum(varOwner);
	values[Anum_pg_variable_varcollation - 1] = ObjectIdGetDatum(varCollation);
	values[Anum_pg_variable_varisnotnull - 1] = BoolGetDatum(is_not_null);
	values[Anum_pg_variable_varisimmutable - 1] = BoolGetDatum(is_immutable);
	values[Anum_pg_variable_vareoxaction - 1] = CharGetDatum(eoxaction);

	if (varDefexpr)
		values[Anum_pg_variable_vardefexpr - 1] = CStringGetTextDatum(nodeToString(varDefexpr));
	else
		nulls[Anum_pg_variable_vardefexpr - 1] = true;

	varacl = get_user_default_acl(OBJECT_VARIABLE, varOwner,
								  varNamespace);
	if (varacl != NULL)
		values[Anum_pg_variable_varacl - 1] = PointerGetDatum(varacl);
	else
		nulls[Anum_pg_variable_varacl - 1] = true;

	tupdesc = RelationGetDescr(rel);

	tup = heap_form_tuple(tupdesc, values, nulls);
	CatalogTupleInsert(rel, tup);
	Assert(OidIsValid(varid));

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

	record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
	free_object_addresses(addrs);

	/* dependency on default expr */
	if (varDefexpr)
		recordDependencyOnExpr(&myself, (Node *) varDefexpr,
							   NIL, DEPENDENCY_NORMAL);

	/* dependency on owner */
	recordDependencyOnOwner(VariableRelationId, varid, varOwner);

	/* dependencies on roles mentioned in default ACL */
	recordDependencyOnNewAcl(VariableRelationId, varid, 0, varOwner, varacl);

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	heap_freetuple(tup);

	/* Post creation hook for new function */
	InvokeObjectPostCreateHook(VariableRelationId, varid, 0);

	table_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Creates a new variable
 *
 * Used by CREATE VARIABLE command
 */
ObjectAddress
CreateVariable(ParseState *pstate, CreateSessionVarStmt *stmt)
{
	Oid			namespaceid;
	AclResult	aclresult;
	Oid			typid;
	int32		typmod;
	Oid			varowner = GetUserId();
	Oid			collation;
	Oid			typcollation;
	ObjectAddress variable;
	Node	   *cooked_default = NULL;

	/* Check consistency of arguments */
	if (stmt->eoxaction == VARIABLE_EOX_DROP
		&& stmt->variable->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("ON COMMIT DROP can only be used on temporary variables")));

	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->variable, NoLock, NULL);

	typenameTypeIdAndMod(pstate, stmt->typeName, &typid, &typmod);

	/* Disallow pseudotypes */
	if (get_typtype(typid) == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("session variable cannot be pseudo-type %s",
						format_type_be(typid))));

	aclresult = object_aclcheck(TypeRelationId, typid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typid);

	typcollation = get_typcollation(typid);

	if (stmt->collClause)
		collation = LookupCollation(pstate,
									stmt->collClause->collname,
									stmt->collClause->location);
	else
		collation = typcollation;;

	/* Complain if COLLATE is applied to an uncollatable type */
	if (OidIsValid(collation) && !OidIsValid(typcollation))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						format_type_be(typid)),
				 parser_errposition(pstate, stmt->collClause->location)));

	if (stmt->defexpr)
	{
		cooked_default = transformExpr(pstate, stmt->defexpr,
									   EXPR_KIND_VARIABLE_DEFAULT);

		cooked_default = coerce_to_specific_type(pstate,
												 cooked_default, typid, "DEFAULT");
		assign_expr_collations(pstate, cooked_default);
	}

	variable = create_variable(stmt->variable->relname,
							   namespaceid,
							   typid,
							   typmod,
							   varowner,
							   collation,
							   stmt->if_not_exists,
							   stmt->is_not_null,
							   stmt->is_immutable,
							   cooked_default,
							   stmt->eoxaction);

	elog(DEBUG1, "record for session variable \"%s\" (oid:%d) was created in pg_variable",
		 stmt->variable->relname, variable.objectId);

	/* We want SessionVariableCreatePostprocess to see the catalog changes. */
	CommandCounterIncrement();

	SessionVariableCreatePostprocess(variable.objectId, stmt->eoxaction);

	return variable;
}

/*
 * Drop variable by OID, and register the needed session variable
 * cleanup.
 */
void
DropVariable(Oid varid)
{
	Relation	rel;
	HeapTuple	tup;
	char		eoxaction;

	rel = table_open(VariableRelationId, RowExclusiveLock);

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	eoxaction = ((Form_pg_variable) GETSTRUCT(tup))->vareoxaction;

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);

	/* Do the necessary cleanup if needed in local memory */
	SessionVariableDropPostprocess(varid, eoxaction);
}
