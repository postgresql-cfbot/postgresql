/*-------------------------------------------------------------------------
 *
 * compressioncmds.c
 *	  Routines for SQL commands for attribute compression methods
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

#include "access/compressamapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_depend.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * get list of all supported compression methods for the given attribute.
 *
 * If oldcmoids list is passed then it will delete the attribute dependency
 * on the compression methods passed in the oldcmoids, otherwise it will
 * return the list of all the compression method on which the attribute has
 * dependency.
 */
static List *
lookup_attribute_compression(Oid attrelid, AttrNumber attnum, List *oldcmoids)
{
	LOCKMODE	lock = AccessShareLock;
	HeapTuple	tup;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[3];
	List	   *cmoids = NIL;

	rel = table_open(DependRelationId, lock);

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

		if (depform->refclassid == AccessMethodRelationId)
		{
			if (oldcmoids && list_member_oid(oldcmoids, depform->refobjid))
				CatalogTupleDelete(rel, &tup->t_self);
			else if (oldcmoids == NULL)
				cmoids = list_append_unique_oid(cmoids, depform->refobjid);
		}
	}

	systable_endscan(scan);
	table_close(rel, lock);

	return cmoids;
}

/*
 * Remove the attribute dependency on the old compression methods given in the
 * cmoids list.
 */
static void
remove_old_dependencies(Oid attrelid, AttrNumber attnum, List *cmoids)
{
	lookup_attribute_compression(attrelid, attnum, cmoids);
}

/*
 * Check whether the given compression method oid is supported by
 * the target attribue.
 */
bool
IsCompressionSupported(Form_pg_attribute att, Oid cmoid)
{
	List	   *cmoids = NIL;

	/* Check whether it is same as the current compression oid */
	if (cmoid == att->attcompression)
		return true;

	/* Check the oid in all preserved compresion methods */
	cmoids = lookup_attribute_compression(att->attrelid, att->attnum, NULL);
	if (list_member_oid(cmoids, cmoid))
		return true;
	else
		return false;
}

/*
 * Get the compression method oid based on the compression method name.  When
 * compression is not specified returns default attribute compression.  It is
 * possible case for CREATE TABLE and ADD COLUMN commands where COMPRESSION
 * syntax is optional.
 *
 * For ALTER command, check all the supported compression methods for the
 * attribute and if the preserve list is not passed or some of the old
 * compression methods are not given in the preserved list then delete
 * dependency from the old compression methods and force the table rewrite.
 */
Oid
GetAttributeCompression(Form_pg_attribute att, ColumnCompression *compression,
						Datum *acoptions, bool *need_rewrite)
{
	Oid			cmoid;
	char		typstorage = get_typstorage(att->atttypid);
	ListCell   *cell;

	/*
	 * No compression for the plain/external storage, refer comments atop
	 * attcompression parameter in pg_attribute.h
	 */
	if (!IsStorageCompressible(typstorage))
	{
		if (compression != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column data type %s does not support compression",
							format_type_be(att->atttypid))));
		return InvalidOid;
	}

	/* fallback to default compression if it's not specified */
	if (compression == NULL)
		return DefaultCompressionOid;

	cmoid = get_compression_am_oid(compression->cmname, false);

	/* if compression options are given then check them */
	if (compression->options)
	{
		CompressionAmRoutine *routine = GetCompressionAmRoutineByAmId(cmoid);

		/* we need routine only to call cmcheck function */
		if (routine->datum_check != NULL)
			routine->datum_check(compression->options);

		*acoptions = optionListToArray(compression->options);
	}
	else
		*acoptions = PointerGetDatum(NULL);

	/*
	 * Determine if the column needs rewrite or not. Rewrite conditions: SET
	 * COMPRESSION without PRESERVE - SET COMPRESSION with PRESERVE but not
	 * with full list of previous access methods.
	 */
	if (need_rewrite != NULL)
	{
		List	   *previous_cmoids = NIL;

		*need_rewrite = false;

		/* If we have preserved all then rewrite is not required */
		if (compression->preserve_all)
			return cmoid;

		previous_cmoids =
			lookup_attribute_compression(att->attrelid, att->attnum, NULL);

		if (compression->preserve != NIL)
		{
			foreach(cell, compression->preserve)
			{
				char   *cmname_p = strVal(lfirst(cell));
				Oid		cmoid_p = get_compression_am_oid(cmname_p, false);

				if (!list_member_oid(previous_cmoids, cmoid_p))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("\"%s\" compression access method cannot be preserved", cmname_p),
							 errhint("use \"pg_column_compression\" function for list of compression methods")));

				/*
				 * Remove from previous list, also protect from multiple
				 * mentions of one access method in PRESERVE list
				 */
				previous_cmoids = list_delete_oid(previous_cmoids, cmoid_p);
			}
		}

		/* delete the current cmoid from the list */
		previous_cmoids = list_delete_oid(previous_cmoids, cmoid);

		/*
		 * If the list of previous Oids is not empty after deletions then
		 * we need to rewrite tuples in the table.
		 */
		if (list_length(previous_cmoids) != 0)
		{
			remove_old_dependencies(att->attrelid, att->attnum,
									previous_cmoids);
			*need_rewrite = true;
		}

		/* Cleanup */
		list_free(previous_cmoids);
	}

	return cmoid;
}

/*
 * Construct ColumnCompression node from the compression method oid.
 */
ColumnCompression *
MakeColumnCompression(Form_pg_attribute att)
{
	ColumnCompression *node;

	if (!OidIsValid(att->attcompression))
		return NULL;

	node = makeNode(ColumnCompression);
	node->cmname = get_am_name(att->attcompression);
	node->options = GetAttributeCompressionOptions(att);

	return node;
}

/*
 * Fetch atttributes compression options
 */
List *
GetAttributeCompressionOptions(Form_pg_attribute att)
{
	HeapTuple	attr_tuple;
	Datum		attcmoptions;
	List	   *acoptions;
	bool		isNull;

	attr_tuple = SearchSysCache2(ATTNUM,
								 ObjectIdGetDatum(att->attrelid),
								 Int16GetDatum(att->attnum));
	if (!HeapTupleIsValid(attr_tuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 att->attnum, att->attrelid);

	attcmoptions = SysCacheGetAttr(ATTNUM, attr_tuple,
								   Anum_pg_attribute_attcmoptions,
								   &isNull);
	if (isNull)
		acoptions = NULL;
	else
		acoptions = untransformRelOptions(attcmoptions);

	ReleaseSysCache(attr_tuple);

	return acoptions;
}

/*
 * Compare compression options for two columns.
 */
void
CheckCompressionMismatch(ColumnCompression *c1, ColumnCompression *c2,
						 const char *attributeName)
{
	if (strcmp(c1->cmname, c2->cmname))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression method conflict",
						attributeName),
				 errdetail("%s versus %s", c1->cmname, c2->cmname)));

	if (!equal(c1->options, c2->options))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("column \"%s\" has a compression options conflict",
						attributeName),
				 errdetail("(%s) versus (%s)",
						   formatRelOptions(c1->options),
						   formatRelOptions(c2->options))));
}
