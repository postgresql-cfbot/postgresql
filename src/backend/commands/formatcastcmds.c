/*-------------------------------------------------------------------------
 *
 * formatcastcmds.c
 *	  Routines for SQL commands that manipulate format casts.
 *
 * A format cast associates a function with a (source type, target type) pair.
 * The function implements CAST(expr AS target FORMAT format_expr): it
 * receives the source value and the FORMAT expression (passed as text) and
 * returns the target type.  Format casts are registered in the pg_format_cast
 * catalog and are intentionally separate from pg_cast (see pg_format_cast.h).
 *
 * This file provides the catalog registration machinery (CREATE/DROP
 * FORMAT CAST).  It does not perform expression transformation or execution of
 * formatted casts.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/formatcastcmds.c
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
#include "catalog/objectaddress.h"
#include "catalog/pg_format_cast.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/formatcast.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Validate the signature of a format cast function: it must be a normal
 * function (not a procedure, aggregate or window function), must not return
 * a set, and must have the signature
 *
 *		function(source_type, text) returns target_type
 *
 * NB: the format cast signature is fixed at two arguments and does not include
 * the target type modifier; typmod-aware format cast signatures are not
 * supported.
 */
static void
check_format_cast_function(Form_pg_proc procstruct,
						 Oid sourcetypeid, Oid targettypeid)
{
	if (procstruct->prokind != PROKIND_FUNCTION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("format cast function must be a normal function")));
	if (procstruct->proretset)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("format cast function must not return a set")));
	if (procstruct->pronargs != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("format cast function must take exactly two arguments")));
	if (procstruct->proargtypes.values[0] != sourcetypeid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("first argument of format cast function must be type %s",
						format_type_be(sourcetypeid))));
	if (procstruct->proargtypes.values[1] != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("second argument of format cast function must be type %s",
						"text")));
	if (procstruct->prorettype != targettypeid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("return data type of format cast function must be type %s",
						format_type_be(targettypeid))));
}

/*
 * CREATE FORMAT CAST
 */
ObjectAddress
CreateFormatCast(CreateFormatCastStmt *stmt)
{
	Oid			sourcetypeid;
	Oid			targettypeid;
	char		sourcetyptype;
	char		targettyptype;
	Oid			funcid;
	AclResult	aclresult;
	Form_pg_proc procstruct;
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_format_cast];
	bool		nulls[Natts_pg_format_cast] = {0};
	Oid			formatcastid;
	Relation	relation;
	ObjectAddress myself,
				referenced;
	ObjectAddresses *addrs;

	sourcetypeid = typenameTypeId(NULL, stmt->sourcetype);
	targettypeid = typenameTypeId(NULL, stmt->targettype);
	sourcetyptype = get_typtype(sourcetypeid);
	targettyptype = get_typtype(targettypeid);

	/* No pseudo-types allowed */
	if (sourcetyptype == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("source data type %s is a pseudo-type",
						TypeNameToString(stmt->sourcetype))));
	if (targettyptype == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("target data type %s is a pseudo-type",
						TypeNameToString(stmt->targettype))));

	/*
	 * Permission check.  As for CREATE CAST, the caller must own at least one
	 * of the two types involved; owning a type is what authorizes defining
	 * conversion behavior for it.  In addition, and following CREATE OPERATOR
	 * and CREATE AGGREGATE, we require EXECUTE permission on the format cast
	 * function (this will also be checked when the format cast is used, but it
	 * is a good idea to verify it up front).  We intentionally do not require
	 * ownership of the function, unlike CREATE TRANSFORM, because a format cast
	 * is a data conversion rather than a procedural-language binding.
	 */
	if (!object_ownercheck(TypeRelationId, sourcetypeid, GetUserId())
		&& !object_ownercheck(TypeRelationId, targettypeid, GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of type %s or type %s",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));

	/*
	 * Look up and validate the format cast function.
	 */
	funcid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->func, false);

	aclresult = object_aclcheck(ProcedureRelationId, funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION,
					   NameListToString(stmt->func->objname));

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procstruct = (Form_pg_proc) GETSTRUCT(tuple);
	check_format_cast_function(procstruct, sourcetypeid, targettypeid);
	ReleaseSysCache(tuple);

	/*
	 * Check for a pre-existing format cast for this (source, target) pair.  For
	 * this version only one format cast per pair is allowed.
	 */
	relation = table_open(FormatCastRelationId, RowExclusiveLock);

	if (SearchSysCacheExists2(FORMATCASTSOURCETARGET,
							  ObjectIdGetDatum(sourcetypeid),
							  ObjectIdGetDatum(targettypeid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("format cast from type %s to type %s already exists",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));

	/*
	 * Build and insert the catalog tuple.
	 */
	formatcastid = GetNewOidWithIndex(relation, FormatCastOidIndexId,
									 Anum_pg_format_cast_oid);
	values[Anum_pg_format_cast_oid - 1] = ObjectIdGetDatum(formatcastid);
	values[Anum_pg_format_cast_fmtsource - 1] = ObjectIdGetDatum(sourcetypeid);
	values[Anum_pg_format_cast_fmttarget - 1] = ObjectIdGetDatum(targettypeid);
	values[Anum_pg_format_cast_fmtfunc - 1] = ObjectIdGetDatum(funcid);

	newtuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);
	CatalogTupleInsert(relation, newtuple);

	addrs = new_object_addresses();

	ObjectAddressSet(myself, FormatCastRelationId, formatcastid);

	/* dependency on source type */
	ObjectAddressSet(referenced, TypeRelationId, sourcetypeid);
	add_exact_object_address(&referenced, addrs);

	/* dependency on target type */
	ObjectAddressSet(referenced, TypeRelationId, targettypeid);
	add_exact_object_address(&referenced, addrs);

	/* dependency on the format cast function */
	ObjectAddressSet(referenced, ProcedureRelationId, funcid);
	add_exact_object_address(&referenced, addrs);

	record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
	free_object_addresses(addrs);

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new format cast */
	InvokeObjectPostCreateHook(FormatCastRelationId, formatcastid, 0);

	heap_freetuple(newtuple);

	table_close(relation, RowExclusiveLock);

	return myself;
}

/*
 * get_format_cast_oid - given source and target type OIDs, look up a
 * format cast OID
 */
Oid
get_format_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid2(FORMATCASTSOURCETARGET, Anum_pg_format_cast_oid,
						  ObjectIdGetDatum(sourcetypeid),
						  ObjectIdGetDatum(targettypeid));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("format cast from type %s to type %s does not exist",
						format_type_be(sourcetypeid),
						format_type_be(targettypeid))));
	return oid;
}
