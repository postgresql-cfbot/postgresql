/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands for compression access methods
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/compressioncmds.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/cmapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attr_compression.h"
#include "catalog/pg_am.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * When conditions of compression satisfies one if builtin attribute
 * compresssion tuples the compressed attribute will be linked to
 * builtin compression without new record in pg_attr_compression.
 * So the fact that the column has a builtin compression we only can find out
 * by its dependency.
 */
static void
lookup_builtin_dependencies(Oid attrelid, AttrNumber attnum,
							List **amoids)
{
	LOCKMODE	lock = AccessShareLock;
	Oid			amoid = InvalidOid;
	HeapTuple	tup;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];

	rel = heap_open(DependRelationId, lock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrelid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(rel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->refclassid == AttrCompressionRelationId)
		{
			Assert(IsBuiltinCompression(depform->refobjid));
			amoid = GetAttrCompressionAmOid(depform->refobjid);
			*amoids = list_append_unique_oid(*amoids, amoid);
		}
	}

	systable_endscan(scan);
	heap_close(rel, lock);
}

/*
 * Find identical attribute compression for reuse and fill the list with
 * used compression access methods.
 */
static Oid
lookup_attribute_compression(Oid attrelid, AttrNumber attnum,
							 Oid amoid, Datum acoptions, List **previous_amoids)
{
	Relation	rel;
	HeapTuple	tuple;
	SysScanDesc scan;
	FmgrInfo	arrayeq_info;
	Oid			result = InvalidOid;
	ScanKeyData key[2];

	/* fill FmgrInfo for array_eq function */
	fmgr_info(F_ARRAY_EQ, &arrayeq_info);

	Assert((attrelid > 0 && attnum > 0) || (attrelid == 0 && attnum == 0));
	if (previous_amoids)
		lookup_builtin_dependencies(attrelid, attnum, previous_amoids);

	rel = heap_open(AttrCompressionRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(attrelid));

	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Oid			acoid,
					tup_amoid;
		Datum		values[Natts_pg_attr_compression];
		bool		nulls[Natts_pg_attr_compression];

		heap_deform_tuple(tuple, RelationGetDescr(rel), values, nulls);
		acoid = DatumGetObjectId(values[Anum_pg_attr_compression_acoid - 1]);
		tup_amoid = get_am_oid(
							   NameStr(*DatumGetName(values[Anum_pg_attr_compression_acname - 1])), false);

		if (previous_amoids)
			*previous_amoids = list_append_unique_oid(*previous_amoids, tup_amoid);

		if (tup_amoid != amoid)
			continue;

		/*
		 * even if we found the match, we still need to acquire all previous
		 * access methods so don't break cycle if previous_amoids is not NULL
		 */
		if (nulls[Anum_pg_attr_compression_acoptions - 1])
		{
			if (DatumGetPointer(acoptions) == NULL)
				result = acoid;
		}
		else
		{
			bool		equal;

			/* check if arrays for WITH options are equal */
			equal = DatumGetBool(CallerFInfoFunctionCall2(
														  array_eq,
														  &arrayeq_info,
														  InvalidOid,
														  acoptions,
														  values[Anum_pg_attr_compression_acoptions - 1]));
			if (equal)
				result = acoid;
		}

		if (previous_amoids == NULL && OidIsValid(result))
			break;
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
	return result;
}

/*
 * Link compression with an attribute. Creates a row in pg_attr_compression
 * if needed.
 *
 * When compression is not specified returns default attribute compression.
 * It is possible case for CREATE TABLE and ADD COLUMN commands
 * where COMPRESSION syntax is optional.
 *
 * If any of builtin attribute compression tuples satisfies conditions
 * returns it.
 *
 * For ALTER command check for previous attribute compression record with
 * identical compression options and reuse it if found any.
 *
 * Note we create attribute compression for EXTERNAL storage too, so when
 * storage is changed we can start compression on future tuples right away.
 */
Oid
CreateAttributeCompression(Form_pg_attribute att,
						   ColumnCompression *compression,
						   bool *need_rewrite, List **preserved_amoids)
{
	Relation	rel;
	HeapTuple	newtup;
	Oid			acoid = InvalidOid,
				amoid;
	regproc		amhandler;
	Datum		arropt;
	Datum		values[Natts_pg_attr_compression];
	bool		nulls[Natts_pg_attr_compression];

	ObjectAddress myself,
				amref;

	CompressionAmRoutine *routine;

	/* No compression for PLAIN storage. */
	if (att->attstorage == 'p')
		return InvalidOid;

	/* Fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionOid;

	amoid = get_compression_am_oid(compression->amname, false);
	if (compression->options)
		arropt = optionListToArray(compression->options, true);
	else
		arropt = PointerGetDatum(NULL);

	/* Try to find builtin compression first */
	acoid = lookup_attribute_compression(0, 0, amoid, arropt, NULL);

	/*
	 * attrelid will be invalid on CREATE TABLE, no need for table rewrite
	 * check.
	 */
	if (OidIsValid(att->attrelid))
	{
		Oid			attacoid;
		List	   *previous_amoids = NIL;

		/*
		 * Try to find identical compression from previous tuples, and fill
		 * the list of previous compresssion methods
		 */
		attacoid = lookup_attribute_compression(att->attrelid, att->attnum,
												amoid, arropt, &previous_amoids);
		if (!OidIsValid(acoid))
			acoid = attacoid;

		/*
		 * Determine if the column needs rewrite or not. Rewrite conditions: -
		 * SET COMPRESSION without PRESERVE - SET COMPRESSION with PRESERVE
		 * but not with full list of previous access methods.
		 */
		if (need_rewrite != NULL)
		{
			/* no rewrite by default */
			*need_rewrite = false;

			Assert(preserved_amoids != NULL);

			if (compression->preserve == NIL)
			{
				Assert(!IsBinaryUpgrade);
				*need_rewrite = true;
			}
			else
			{
				ListCell   *cell;

				foreach(cell, compression->preserve)
				{
					char	   *amname_p = strVal(lfirst(cell));
					Oid			amoid_p = get_am_oid(amname_p, false);

					if (!list_member_oid(previous_amoids, amoid_p))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("\"%s\" compression access method cannot be preserved", amname_p),
								 errhint("use \"pg_column_compression\" function for list of compression methods")
								 ));

					*preserved_amoids = list_append_unique_oid(*preserved_amoids, amoid_p);

					/*
					 * Remove from previous list, also protect from multiple
					 * mentions of one access method in PRESERVE list
					 */
					previous_amoids = list_delete_oid(previous_amoids, amoid_p);
				}

				/*
				 * If the list of previous Oids is not empty after deletions
				 * then we need to rewrite tuples in the table.
				 *
				 * In binary upgrade list will not be free since it contains
				 * Oid of builtin compression access method.
				 */
				if (!IsBinaryUpgrade && list_length(previous_amoids) != 0)
					*need_rewrite = true;
			}
		}

		/* Cleanup */
		list_free(previous_amoids);
	}

	if (IsBinaryUpgrade && !OidIsValid(acoid))
		elog(ERROR, "could not restore attribute compression data");

	/* Return Oid if we already found identical compression on this column */
	if (OidIsValid(acoid))
	{
		if (DatumGetPointer(arropt) != NULL)
			pfree(DatumGetPointer(arropt));

		return acoid;
	}

	/* Initialize buffers for new tuple values */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	amhandler = get_am_handler_oid(amoid, AMTYPE_COMPRESSION, false);

	rel = heap_open(AttrCompressionRelationId, RowExclusiveLock);

	acoid = GetNewOidWithIndex(rel, AttrCompressionIndexId,
							   Anum_pg_attr_compression_acoid);
	if (acoid < FirstNormalObjectId)
	{
		/* this is database initialization */
		heap_close(rel, RowExclusiveLock);
		return DefaultCompressionOid;
	}

	/* we need routine only to call cmcheck function */
	routine = InvokeCompressionAmHandler(amhandler);
	if (routine->cmcheck != NULL)
		routine->cmcheck(att, compression->options);
	pfree(routine);

	values[Anum_pg_attr_compression_acoid - 1] = ObjectIdGetDatum(acoid);
	values[Anum_pg_attr_compression_acname - 1] = CStringGetDatum(compression->amname);
	values[Anum_pg_attr_compression_acrelid - 1] = ObjectIdGetDatum(att->attrelid);
	values[Anum_pg_attr_compression_acattnum - 1] = Int32GetDatum(att->attnum);

	if (compression->options)
		values[Anum_pg_attr_compression_acoptions - 1] = arropt;
	else
		nulls[Anum_pg_attr_compression_acoptions - 1] = true;

	newtup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, newtup);
	heap_freetuple(newtup);
	heap_close(rel, RowExclusiveLock);

	ObjectAddressSet(myself, AttrCompressionRelationId, acoid);
	ObjectAddressSet(amref, AccessMethodRelationId, amoid);

	recordDependencyOn(&myself, &amref, DEPENDENCY_NORMAL);
	recordDependencyOnCurrentExtension(&myself, false);

	/* Make the changes visible */
	CommandCounterIncrement();
	return acoid;
}

/*
 * Remove the attribute compression record from pg_attr_compression and
 * call drop callback from access method routine.
 */
void
RemoveAttributeCompression(Oid acoid)
{
	Relation	relation;
	HeapTuple	tup;
	CompressionAmRoutine *routine;
	Form_pg_attr_compression acform;

	tup = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	acform = (Form_pg_attr_compression) GETSTRUCT(tup);

	/* check we're not trying to remove builtin attribute compression */
	Assert(acform->acrelid != 0);

	/* call drop callback */
	routine = GetCompressionAmRoutine(acoid);
	if (routine->cmdrop)
		routine->cmdrop(acoid);

	/* delete the record from catalogs */
	relation = heap_open(AttrCompressionRelationId, RowExclusiveLock);
	CatalogTupleDelete(relation, &tup->t_self);
	heap_close(relation, RowExclusiveLock);
	ReleaseSysCache(tup);
}

/*
 * CleanupAttributeCompression
 *
 * Remove entries in pg_attr_compression except current attribute compression
 * and related with specified list of access methods.
 */
void
CleanupAttributeCompression(Oid relid, AttrNumber attnum, List *keepAmOids)
{
	Oid			acoid,
				amoid;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];
	HeapTuple	tuple,
				attrtuple;
	Form_pg_attribute attform;
	List	   *removed = NIL;
	ListCell   *lc;

	attrtuple = SearchSysCache2(ATTNUM,
								ObjectIdGetDatum(relid),
								Int16GetDatum(attnum));

	if (!HeapTupleIsValid(attrtuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 attnum, relid);
	attform = (Form_pg_attribute) GETSTRUCT(attrtuple);
	acoid = attform->attcompression;
	ReleaseSysCache(attrtuple);

	Assert(relid > 0 && attnum > 0);

	if (IsBinaryUpgrade)
		goto builtin_removal;

	rel = heap_open(AttrCompressionRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);

	/* Remove attribute compression tuples and collect removed Oids to list */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_attr_compression acform;

		acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
		amoid = get_am_oid(NameStr(acform->acname), false);

		/* skip current compression */
		if (acform->acoid == acoid)
			continue;

		if (!list_member_oid(keepAmOids, amoid))
		{
			CompressionAmRoutine *routine;

			/* call drop callback */
			routine = GetCompressionAmRoutine(acform->acoid);
			if (routine->cmdrop)
				routine->cmdrop(acoid);
			pfree(routine);

			removed = lappend_oid(removed, acform->acoid);
			CatalogTupleDelete(rel, &tuple->t_self);
		}
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	/* Now remove dependencies */
	rel = heap_open(DependRelationId, RowExclusiveLock);
	foreach(lc, removed)
	{
		Oid			tup_acoid = lfirst_oid(lc);

		ScanKeyInit(&key[0],
					Anum_pg_depend_classid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(AttrCompressionRelationId));
		ScanKeyInit(&key[1],
					Anum_pg_depend_objid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(tup_acoid));

		scan = systable_beginscan(rel, DependDependerIndexId, true,
								  NULL, 2, key);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
			CatalogTupleDelete(rel, &tuple->t_self);

		systable_endscan(scan);
	}
	heap_close(rel, RowExclusiveLock);

builtin_removal:
	/* Now remove dependencies with builtin compressions */
	rel = heap_open(DependRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum((int32) attnum));

	scan = systable_beginscan(rel, DependDependerIndexId, true,
							  NULL, 3, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tuple);

		if (depform->refclassid != AttrCompressionRelationId)
			continue;

		/* skip current compression */
		if (depform->refobjid == acoid)
			continue;

		amoid = GetAttrCompressionAmOid(depform->refobjid);
		if (!list_member_oid(keepAmOids, amoid))
			CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Construct ColumnCompression node by attribute compression Oid
 */
ColumnCompression *
MakeColumnCompression(Oid acoid)
{
	HeapTuple	tuple;
	Form_pg_attr_compression acform;
	ColumnCompression *node;

	if (!OidIsValid(acoid))
		return NULL;

	tuple = SearchSysCache1(ATTCOMPRESSIONOID, ObjectIdGetDatum(acoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute compression %u", acoid);

	acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
	node = makeNode(ColumnCompression);
	node->amname = pstrdup(NameStr(acform->acname));
	ReleaseSysCache(tuple);

	/*
	 * fill attribute compression options too. We could've do it above but
	 * it's easier to call this helper.
	 */
	node->options = GetAttrCompressionOptions(acoid);

	return node;
}

void
CheckCompressionMismatch(ColumnCompression *c1, ColumnCompression *c2,
						 const char *attributeName)
{
	if (strcmp(c1->amname, c2->amname))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression method conflict",
						attributeName),
				 errdetail("%s versus %s", c1->amname, c2->amname)));

	if (!equal(c1->options, c2->options))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression options conflict",
						attributeName),
				 errdetail("(%s) versus (%s)",
						   formatRelOptions(c1->options),
						   formatRelOptions(c2->options))));
}

Datum
pg_column_compression(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	char	   *attname = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	Relation	rel;
	HeapTuple	tuple;
	AttrNumber	attnum;
	List	   *amoids = NIL;
	Oid			amoid;
	ListCell   *lc;

	ScanKeyData key[2];
	SysScanDesc scan;
	StringInfoData result;

	attnum = get_attnum(relOid, attname);
	if (attnum == InvalidAttrNumber)
		PG_RETURN_NULL();

	lookup_builtin_dependencies(relOid, attnum, &amoids);

	rel = heap_open(AttrCompressionRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_attr_compression_acrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relOid));
	ScanKeyInit(&key[1],
				Anum_pg_attr_compression_acattnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(rel, AttrCompressionRelidAttnumIndexId,
							  true, NULL, 2, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_attr_compression acform;

		acform = (Form_pg_attr_compression) GETSTRUCT(tuple);
		amoid = get_am_oid(NameStr(acform->acname), false);
		amoids = list_append_unique_oid(amoids, amoid);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	if (!list_length(amoids))
		PG_RETURN_NULL();

	amoid = InvalidOid;
	initStringInfo(&result);
	foreach(lc, amoids)
	{
		if (OidIsValid(amoid))
			appendStringInfoString(&result, ", ");

		amoid = lfirst_oid(lc);
		appendStringInfoString(&result, get_am_name(amoid));
	}

	PG_RETURN_TEXT_P(CStringGetTextDatum(result.data));
}
