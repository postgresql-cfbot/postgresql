/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands that manipulate compression methods.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/compressioncmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/compression.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_compression.h"
#include "catalog/pg_compression_opt.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static CompressionMethodRoutine *invoke_compression_handler(Oid cmhandler, Oid typeid);

/*
 * Convert a handler function name to an Oid.  If the return type of the
 * function doesn't match the given AM type, an error is raised.
 *
 * This function either return valid function Oid or throw an error.
 */
static Oid
lookup_compression_handler(List *handlerName)
{
	static const Oid funcargtypes[1] = {INTERNALOID};
	Oid			handlerOid;

	if (handlerName == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("handler function is not specified")));

	/* handlers have one argument of type internal */
	handlerOid = LookupFuncName(handlerName, 1, funcargtypes, false);

	/* check that handler has the correct return type */
	if (get_func_rettype(handlerOid) != COMPRESSION_HANDLEROID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return type %s",
						NameListToString(handlerName),
						"compression_handler")));

	return handlerOid;
}

/* Creates a record in pg_compression */
static ObjectAddress
create_compression_method(char *cmName, List *handlerName)
{
	Relation	rel;
	ObjectAddress myself;
	ObjectAddress referenced;
	Oid			cmoid;
	Oid			cmhandler;
	bool		nulls[Natts_pg_compression];
	Datum		values[Natts_pg_compression];
	HeapTuple	tup;

	rel = heap_open(CompressionMethodRelationId, RowExclusiveLock);

	/* Check if name is used */
	cmoid = GetSysCacheOid1(COMPRESSIONMETHODNAME, CStringGetDatum(cmName));
	if (OidIsValid(cmoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("compression method \"%s\" already exists",
						cmName)));

	/*
	 * Get the handler function oid and compression method routine
	 */
	cmhandler = lookup_compression_handler(handlerName);

	/*
	 * Insert tuple into pg_compression.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_compression_cmname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(cmName));
	values[Anum_pg_compression_cmhandler - 1] = ObjectIdGetDatum(cmhandler);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	cmoid = CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, CompressionMethodRelationId, cmoid);

	/* Record dependency on handler function */
	ObjectAddressSet(referenced, ProcedureRelationId, cmhandler);

	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	recordDependencyOnCurrentExtension(&myself, false);

	heap_close(rel, RowExclusiveLock);

	return myself;
}

/*
 * Call the specified compression method handler
 * routine to get its CompressionMethodRoutine struct,
 * which will be palloc'd in the caller's context.
 */
static CompressionMethodRoutine *
invoke_compression_handler(Oid cmhandler, Oid typeid)
{
	Datum		datum;
	CompressionMethodRoutine *routine;
	CompressionMethodOpArgs opargs;

	opargs.typeid = typeid;
	opargs.cmhanderid = cmhandler;

	datum = OidFunctionCall1(cmhandler, PointerGetDatum(&opargs));
	routine = (CompressionMethodRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, CompressionMethodRoutine))
		elog(ERROR, "compression method handler function %u "
			 "did not return an CompressionMethodRoutine struct",
			 cmhandler);

	return routine;
}

/*
 * CREATE COMPRESSION METHOD .. HANDLER ..
 */
ObjectAddress
DefineCompressionMethod(List *names, List *parameters)
{
	char	   *cmName;
	ListCell   *pl;
	DefElem    *handlerEl = NULL;

	if (list_length(names) != 1)
		elog(ERROR, "compression method name cannot be qualified");

	cmName = strVal(linitial(names));

	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create compression method \"%s\"",
						cmName),
				 errhint("Must be superuser to create an compression method.")));

	/* Extract the name of compression handler function */
	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		DefElem   **defelp;

		if (pg_strcasecmp(defel->defname, "handler") == 0)
			defelp = &handlerEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("compression method attribute \"%s\" not recognized",
							defel->defname)));
			break;
		}

		*defelp = defel;
	}

	if (!handlerEl)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("compression method handler is not specified")));

	/* Finally create a compression method */
	return create_compression_method(cmName, (List *) handlerEl->arg);
}

/*
 * RemoveCompressionMethodById
 *
 * Removes a compresssion method by its Oid.
 */
void
RemoveCompressionMethodById(Oid cmOid)
{
	Relation	relation;
	HeapTuple	tup;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop compression methods")));

	relation = heap_open(CompressionMethodRelationId, RowExclusiveLock);

	tup = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for compression method %u", cmOid);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

/*
 * Create new record in pg_compression_opt
 */
Oid
CreateCompressionOptions(Form_pg_attribute attr, ColumnCompression *compression)
{
	Relation	rel;
	HeapTuple	tup,
				newtup;
	Oid			cmoptoid,
				cmid;
	Datum		values[Natts_pg_compression_opt];
	bool		nulls[Natts_pg_compression_opt];

	ObjectAddress myself,
				ref1,
				ref2;

	CompressionMethodRoutine *routine;
	Form_pg_compression cmform;

	/* Initialize buffers for new tuple values */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	cmid = get_compression_method_oid(compression->methodName, false);

	/* Get handler function OID for the compression method */
	tup = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for compression method %u", cmid);
	cmform = (Form_pg_compression) GETSTRUCT(tup);
	routine = invoke_compression_handler(cmform->cmhandler, attr->atttypid);

	if (routine->configure == NULL && compression->options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the compression method \"%s\" does not take any options",
						NameStr(cmform->cmname))));
	else if (routine->configure && compression->options != NIL)
		routine->configure(attr, compression->options);

	rel = heap_open(CompressionOptRelationId, RowExclusiveLock);

	cmoptoid = GetNewOidWithIndex(rel, CompressionOptionsOidIndexId,
								  Anum_pg_compression_opt_cmoptoid);
	values[Anum_pg_compression_opt_cmoptoid - 1] = ObjectIdGetDatum(cmoptoid);
	values[Anum_pg_compression_opt_cmname - 1] = NameGetDatum(&cmform->cmname);
	values[Anum_pg_compression_opt_cmhandler - 1] = ObjectIdGetDatum(cmform->cmhandler);

	if (compression->options)
		values[Anum_pg_compression_opt_cmoptions - 1] =
			optionListToArray(compression->options);
	else
		nulls[Anum_pg_compression_opt_cmoptions - 1] = true;

	newtup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, newtup);
	heap_freetuple(newtup);

	ReleaseSysCache(tup);

	ObjectAddressSet(myself, CompressionOptRelationId, cmoptoid);
	ObjectAddressSet(ref1, ProcedureRelationId, cmform->cmhandler);
	ObjectAddressSet(ref2, CompressionMethodRelationId, cmid);

	recordDependencyOn(&myself, &ref1, DEPENDENCY_NORMAL);
	recordDependencyOn(&myself, &ref2, DEPENDENCY_NORMAL);
	recordDependencyOnCurrentExtension(&myself, false);
	heap_close(rel, RowExclusiveLock);

	return cmoptoid;
}

/*
 * Create pg_depend record between attribute and its compression options
 */
void
CreateColumnCompressionDependency(Form_pg_attribute attr, Oid cmoptoid)
{
	ObjectAddress optref,
				attrref;

	ObjectAddressSet(optref, CompressionOptRelationId, cmoptoid);
	ObjectAddressSubSet(attrref, RelationRelationId, attr->attrelid, attr->attnum);
	recordDependencyOn(&attrref, &optref, DEPENDENCY_NORMAL);
}

/*
 * Remove the compression options record from pg_compression_opt
 */
void
RemoveCompressionOptionsById(Oid cmoptoid)
{
	Relation	relation;
	HeapTuple	tup;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop compression options")));

	tup = SearchSysCache1(COMPRESSIONOPTIONSOID, ObjectIdGetDatum(cmoptoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for compression options %u", cmoptoid);

	relation = heap_open(CompressionOptRelationId, RowExclusiveLock);
	CatalogTupleDelete(relation, &tup->t_self);
	heap_close(relation, RowExclusiveLock);
	ReleaseSysCache(tup);
}

ColumnCompression *
GetColumnCompressionForAttribute(Form_pg_attribute att)
{
	HeapTuple	tuple;
	Form_pg_compression_opt cmoptform;
	ColumnCompression *compression = makeNode(ColumnCompression);

	/* Get handler function OID for the compression method */
	tuple = SearchSysCache1(COMPRESSIONOPTIONSOID,
							ObjectIdGetDatum(att->attcompression));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for compression options %u",
			 att->attcompression);

	cmoptform = (Form_pg_compression_opt) GETSTRUCT(tuple);
	compression->methodName = pstrdup(NameStr(cmoptform->cmname));
	compression->options = GetCompressionOptionsList(att->attcompression);
	ReleaseSysCache(tuple);

	return compression;
}

void
CheckCompressionMismatch(ColumnCompression *c1, ColumnCompression *c2,
						 const char *attributeName)
{
	if (strcmp(c1->methodName, c2->methodName))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression method conflict",
						attributeName),
				 errdetail("%s versus %s", c1->methodName, c2->methodName)));

	if (!equal(c1->options, c2->options))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression options conflict",
						attributeName),
				 errdetail("(%s) versus (%s)",
						   formatRelOptions(c1->options),
						   formatRelOptions(c2->options))));
}

/*
 * get_compression_method_oid
 *
 * If missing_ok is false, throw an error if compression method not found.
 * If missing_ok is true, just return InvalidOid.
 */
Oid
get_compression_method_oid(const char *cmname, bool missing_ok)
{
	HeapTuple	tup;
	Oid			oid = InvalidOid;

	tup = SearchSysCache1(COMPRESSIONMETHODNAME, CStringGetDatum(cmname));
	if (HeapTupleIsValid(tup))
	{
		oid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}

	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("compression method \"%s\" does not exist", cmname)));

	return oid;
}

/*
 * get_compression_method_name
 *
 * given an compression method OID, look up its name.
 */
char *
get_compression_method_name(Oid cmOid)
{
	HeapTuple	tup;
	char	   *result = NULL;

	tup = SearchSysCache1(COMPRESSIONMETHODOID, ObjectIdGetDatum(cmOid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_compression cmform = (Form_pg_compression) GETSTRUCT(tup);

		result = pstrdup(NameStr(cmform->cmname));
		ReleaseSysCache(tup);
	}
	return result;
}

/*
 * get_compression_method_name_for_opt
 *
 * given an compression options OID, look up a name for used compression method.
 */
char *
get_compression_method_name_for_opt(Oid cmoptoid)
{
	HeapTuple	tup;
	char	   *result = NULL;
	Form_pg_compression_opt cmoptform;

	tup = SearchSysCache1(COMPRESSIONOPTIONSOID, ObjectIdGetDatum(cmoptoid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for compression options %u", cmoptoid);

	cmoptform = (Form_pg_compression_opt) GETSTRUCT(tup);
	result = pstrdup(NameStr(cmoptform->cmname));
	ReleaseSysCache(tup);

	return result;
}

/*
 * GetCompressionOptionsList
 *
 * Parse array with compression options and return it as list.
 */
List *
GetCompressionOptionsList(Oid cmoptoid)
{
	HeapTuple	tuple;
	List	   *result = NULL;
	bool		isnull;
	Datum		cmoptions;

	/* Get handler function OID for the compression method */
	tuple = SearchSysCache1(COMPRESSIONOPTIONSOID, ObjectIdGetDatum(cmoptoid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for compression options %u", cmoptoid);

	cmoptions = SysCacheGetAttr(COMPRESSIONOPTIONSOID, tuple,
								Anum_pg_compression_opt_cmoptions, &isnull);

	if (!isnull)
		result = untransformRelOptions(cmoptions);

	ReleaseSysCache(tuple);
	return result;
}

/*
 * GetCompressionMethodRoutine
 *
 * Determine compression handler by compression options Oid and return
 * structure with compression methods
 */
CompressionMethodRoutine *
GetCompressionMethodRoutine(Oid cmoptoid)
{
	HeapTuple	tuple;
	Form_pg_compression_opt cmopt;
	regproc		cmhandler;

	/* Get handler function OID for the compression method */
	tuple = SearchSysCache1(COMPRESSIONOPTIONSOID, ObjectIdGetDatum(cmoptoid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for compression options %u", cmoptoid);

	cmopt = (Form_pg_compression_opt) GETSTRUCT(tuple);
	cmhandler = cmopt->cmhandler;

	/* Complain if handler OID is invalid */
	if (!RegProcedureIsValid(cmhandler))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find compression method handler for compression options %u",
						cmoptoid)));

	ReleaseSysCache(tuple);

	/* And finally, call the handler function to get the API struct. */
	return invoke_compression_handler(cmhandler, InvalidOid);
}
