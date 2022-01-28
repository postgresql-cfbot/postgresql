/*-------------------------------------------------------------------------
 *
 * pg_publication.c
 *		publication C API manipulation
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/catalog/pg_publication.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_namespace.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_type.h"
#include "commands/publicationcmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static void check_publication_columns(Publication *pub, Relation targetrel,
									  Bitmapset *columns);
static void publication_translate_columns(Relation targetrel, List *columns,
										  int *natts, AttrNumber **attrs,
										  Bitmapset **attset);

/*
 * Check if relation can be in given publication and that the column
 * filter is sensible, and throws appropriate error if not.
 *
 * targetcols is the bitmapset of attribute numbers given in the column list,
 * or NULL if it was not specified.
 */
static void
check_publication_add_relation(Publication *pub, Relation targetrel,
							   Bitmapset *columns)
{
	bool		replidentfull = (targetrel->rd_rel->relreplident == REPLICA_IDENTITY_FULL);

	/* Must be a regular or partitioned table */
	if (RelationGetForm(targetrel)->relkind != RELKIND_RELATION &&
		RelationGetForm(targetrel)->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add relation \"%s\" to publication",
						RelationGetRelationName(targetrel)),
				 errdetail_relkind_not_supported(RelationGetForm(targetrel)->relkind)));

	/* Can't be system table */
	if (IsCatalogRelation(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add relation \"%s\" to publication",
						RelationGetRelationName(targetrel)),
				 errdetail("This operation is not supported for system tables.")));

	/* UNLOGGED and TEMP relations cannot be part of publication. */
	if (targetrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add relation \"%s\" to publication",
						RelationGetRelationName(targetrel)),
				 errdetail("This operation is not supported for temporary tables.")));
	else if (targetrel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add relation \"%s\" to publication",
						RelationGetRelationName(targetrel)),
				 errdetail("This operation is not supported for unlogged tables.")));

	/* Make sure the column list checks out */
	if (columns != NULL)
	{
		/*
		 * Even if the user listed all columns in the column list, we cannot
		 * allow a column list to be specified when REPLICA IDENTITY is FULL;
		 * that would cause problems if a new column is added later, because
		 * the new column would have to be included (because of being part of
		 * the replica identity) but it's technically not allowed (because of
		 * not being in the publication's column list yet).  So reject this
		 * case altogether.
		 */
		if (replidentfull)
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("invalid column list for publishing relation \"%s\"",
						   RelationGetRelationName(targetrel)),
					errdetail("Cannot specify a column list on relations with REPLICA IDENTITY FULL."));

		check_publication_columns(pub, targetrel, columns);
	}
}

/*
 * Enforce that the column list can only leave out columns that aren't
 * forced to be sent.
 *
 * No column can be excluded if REPLICA IDENTITY is FULL (since all the
 * columns need to be sent regardless); and in other cases, the columns in
 * the REPLICA IDENTITY cannot be left out.
 */
static void
check_publication_columns(Publication *pub, Relation targetrel, Bitmapset *columns)
{
	if (targetrel->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot change column set for relation \"%s\"",
					   RelationGetRelationName(targetrel)),
				errdetail("Cannot specify column list on relations with REPLICA IDENTITY FULL."));

	if (pub->pubactions.pubupdate || pub->pubactions.pubdelete)
	{
		Bitmapset  *idattrs;
		int			x;

		idattrs = RelationGetIndexAttrBitmap(targetrel,
											 INDEX_ATTR_BITMAP_IDENTITY_KEY);

		/*
		 * We have to test membership the hard way, because the values returned by
		 * RelationGetIndexAttrBitmap are offset.
		 */
		x = -1;
		while ((x = bms_next_member(idattrs, x)) >= 0)
		{
			if (!bms_is_member(x + FirstLowInvalidHeapAttributeNumber, columns))
				ereport(ERROR,
						errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("invalid column list for publishing relation \"%s\"",
							   RelationGetRelationName(targetrel)),
						errdetail("All columns in REPLICA IDENTITY must be present in the column list."));
		}

		bms_free(idattrs);
	}
}

/*
 * Check if schema can be in given publication and throw appropriate error if
 * not.
 */
static void
check_publication_add_schema(Oid schemaid)
{
	/* Can't be system namespace */
	if (IsCatalogNamespace(schemaid) || IsToastNamespace(schemaid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add schema \"%s\" to publication",
						get_namespace_name(schemaid)),
				 errdetail("This operation is not supported for system schemas.")));

	/* Can't be temporary namespace */
	if (isAnyTempNamespace(schemaid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot add schema \"%s\" to publication",
						get_namespace_name(schemaid)),
				 errdetail("Temporary schemas cannot be replicated.")));
}

/*
 * Returns if relation represented by oid and Form_pg_class entry
 * is publishable.
 *
 * Does same checks as the above, but does not need relation to be opened
 * and also does not throw errors.
 *
 * XXX  This also excludes all tables with relid < FirstNormalObjectId,
 * ie all tables created during initdb.  This mainly affects the preinstalled
 * information_schema.  IsCatalogRelationOid() only excludes tables with
 * relid < FirstUnpinnedObjectId, making that test rather redundant,
 * but really we should get rid of the FirstNormalObjectId test not
 * IsCatalogRelationOid.  We can't do so today because we don't want
 * information_schema tables to be considered publishable; but this test
 * is really inadequate for that, since the information_schema could be
 * dropped and reloaded and then it'll be considered publishable.  The best
 * long-term solution may be to add a "relispublishable" bool to pg_class,
 * and depend on that instead of OID checks.
 */
static bool
is_publishable_class(Oid relid, Form_pg_class reltuple)
{
	return (reltuple->relkind == RELKIND_RELATION ||
			reltuple->relkind == RELKIND_PARTITIONED_TABLE) &&
		!IsCatalogRelationOid(relid) &&
		reltuple->relpersistence == RELPERSISTENCE_PERMANENT &&
		relid >= FirstNormalObjectId;
}

/*
 * Filter out the partitions whose parent tables were also specified in
 * the publication.
 */
static List *
filter_partitions(List *relids)
{
	List	   *result = NIL;
	ListCell   *lc;
	ListCell   *lc2;

	foreach(lc, relids)
	{
		bool		skip = false;
		List	   *ancestors = NIL;
		Oid			relid = lfirst_oid(lc);

		if (get_rel_relispartition(relid))
			ancestors = get_partition_ancestors(relid);

		foreach(lc2, ancestors)
		{
			Oid			ancestor = lfirst_oid(lc2);

			/* Check if the parent table exists in the published table list. */
			if (list_member_oid(relids, ancestor))
			{
				skip = true;
				break;
			}
		}

		if (!skip)
			result = lappend_oid(result, relid);
	}

	return result;
}

/*
 * Another variant of this, taking a Relation.
 */
bool
is_publishable_relation(Relation rel)
{
	return is_publishable_class(RelationGetRelid(rel), rel->rd_rel);
}

/*
 * Returns true if any schema is associated with the publication, false if no
 * schema is associated with the publication.
 */
bool
is_schema_publication(Oid pubid)
{
	Relation	pubschsrel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tup;
	bool		result = false;

	pubschsrel = table_open(PublicationNamespaceRelationId, AccessShareLock);
	ScanKeyInit(&scankey,
				Anum_pg_publication_namespace_pnpubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pubid));

	scan = systable_beginscan(pubschsrel,
							  PublicationNamespacePnnspidPnpubidIndexId,
							  true, NULL, 1, &scankey);
	tup = systable_getnext(scan);
	result = HeapTupleIsValid(tup);

	systable_endscan(scan);
	table_close(pubschsrel, AccessShareLock);

	return result;
}

/*
 * SQL-callable variant of the above
 *
 * This returns null when the relation does not exist.  This is intended to be
 * used for example in psql to avoid gratuitous errors when there are
 * concurrent catalog changes.
 */
Datum
pg_relation_is_publishable(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	bool		result;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	result = is_publishable_class(relid, (Form_pg_class) GETSTRUCT(tuple));
	ReleaseSysCache(tuple);
	PG_RETURN_BOOL(result);
}

/*
 * Gets the relations based on the publication partition option for a specified
 * relation.
 */
List *
GetPubPartitionOptionRelations(List *result, PublicationPartOpt pub_partopt,
							   Oid relid)
{
	if (get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE &&
		pub_partopt != PUBLICATION_PART_ROOT)
	{
		List	   *all_parts = find_all_inheritors(relid, NoLock,
													NULL);

		if (pub_partopt == PUBLICATION_PART_ALL)
			result = list_concat(result, all_parts);
		else if (pub_partopt == PUBLICATION_PART_LEAF)
		{
			ListCell   *lc;

			foreach(lc, all_parts)
			{
				Oid			partOid = lfirst_oid(lc);

				if (get_rel_relkind(partOid) != RELKIND_PARTITIONED_TABLE)
					result = lappend_oid(result, partOid);
			}
		}
		else
			Assert(false);
	}
	else
		result = lappend_oid(result, relid);

	return result;
}

/*
 * Insert new publication / relation mapping.
 */
ObjectAddress
publication_add_relation(Oid pubid, PublicationRelInfo *targetrel,
						 bool if_not_exists)
{
	Relation	rel;
	HeapTuple	tup;
	Datum		values[Natts_pg_publication_rel];
	bool		nulls[Natts_pg_publication_rel];
	Oid			relid = RelationGetRelid(targetrel->relation);
	Oid			pubreloid;
	Publication *pub = GetPublication(pubid);
	Bitmapset  *attset = NULL;
	AttrNumber *attarray;
	int			natts = 0;
	ObjectAddress myself,
				referenced;
	List	   *relids = NIL;

	rel = table_open(PublicationRelRelationId, RowExclusiveLock);

	/*
	 * Check for duplicates. Note that this does not really prevent
	 * duplicates, it's here just to provide nicer error message in common
	 * case. The real protection is the unique key on the catalog.
	 */
	if (SearchSysCacheExists2(PUBLICATIONRELMAP, ObjectIdGetDatum(relid),
							  ObjectIdGetDatum(pubid)))
	{
		table_close(rel, RowExclusiveLock);

		if (if_not_exists)
			return InvalidObjectAddress;

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("relation \"%s\" is already member of publication \"%s\"",
						RelationGetRelationName(targetrel->relation), pub->name)));
	}

	/* Translate column names to numbers and verify suitability */
	publication_translate_columns(targetrel->relation,
								  targetrel->columns,
								  &natts, &attarray, &attset);

	check_publication_add_relation(pub, targetrel->relation, attset);

	bms_free(attset);

	/* Form a tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	pubreloid = GetNewOidWithIndex(rel, PublicationRelObjectIndexId,
								   Anum_pg_publication_rel_oid);
	values[Anum_pg_publication_rel_oid - 1] = ObjectIdGetDatum(pubreloid);
	values[Anum_pg_publication_rel_prpubid - 1] =
		ObjectIdGetDatum(pubid);
	values[Anum_pg_publication_rel_prrelid - 1] =
		ObjectIdGetDatum(relid);
	if (targetrel->columns)
	{
		int2vector *prattrs;

		prattrs = buildint2vector(attarray, natts);
		values[Anum_pg_publication_rel_prattrs - 1] = PointerGetDatum(prattrs);
	}
	else
		nulls[Anum_pg_publication_rel_prattrs - 1] = true;

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	/* Register dependencies as needed */
	ObjectAddressSet(myself, PublicationRelRelationId, pubreloid);

	/* Add dependency on the columns, if any are listed */
	for (int i = 0; i < natts; i++)
	{
		ObjectAddressSubSet(referenced, RelationRelationId, relid, attarray[i]);
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
	}
	pfree(attarray);
	/* Add dependency on the publication */
	ObjectAddressSet(referenced, PublicationRelationId, pubid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Add dependency on the relation */
	ObjectAddressSet(referenced, RelationRelationId, relid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Close the table. */
	table_close(rel, RowExclusiveLock);

	/*
	 * Invalidate relcache so that publication info is rebuilt.
	 *
	 * For the partitioned tables, we must invalidate all partitions contained
	 * in the respective partition hierarchies, not just the one explicitly
	 * mentioned in the publication. This is required because we implicitly
	 * publish the child tables when the parent table is published.
	 */
	relids = GetPubPartitionOptionRelations(relids, PUBLICATION_PART_ALL,
											relid);

	InvalidatePublicationRels(relids);

	return myself;
}

/*
 * Update the column list for a relation in a publication.
 */
void
publication_set_table_columns(Relation pubrel, HeapTuple pubreltup,
							  Relation targetrel, List *columns)
{
	Bitmapset  *attset;
	AttrNumber *attarray;
	HeapTuple	copytup;
	int			natts;
	bool		nulls[Natts_pg_publication_rel];
	bool		replaces[Natts_pg_publication_rel];
	Datum		values[Natts_pg_publication_rel];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	replaces[Anum_pg_publication_rel_prattrs - 1] = true;

	deleteDependencyRecordsForClass(PublicationRelationId,
									((Form_pg_publication_rel) GETSTRUCT(pubreltup))->oid,
									RelationRelationId,
									DEPENDENCY_AUTO);

	if (columns == NULL)
	{
		nulls[Anum_pg_publication_rel_prattrs - 1] = true;
	}
	else
	{
		ObjectAddress myself,
					referenced;
		int2vector *prattrs;
		Publication *pub;

		pub = GetPublication(((Form_pg_publication_rel) GETSTRUCT(pubreltup))->prpubid);

		publication_translate_columns(targetrel, columns,
									  &natts, &attarray, &attset);

		/*
		 * Make sure the column list checks out.  XXX this should occur at
		 * caller in publicationcmds.c, not here.
		 */
		check_publication_columns(pub, targetrel, attset);
		bms_free(attset);
		/* XXX "pub" is leaked here */

		prattrs = buildint2vector(attarray, natts);
		values[Anum_pg_publication_rel_prattrs - 1] = PointerGetDatum(prattrs);

		/* Add dependencies on the new list of columns */
		ObjectAddressSet(myself, PublicationRelRelationId,
						 ((Form_pg_publication_rel) GETSTRUCT(pubreltup))->oid);
		for (int i = 0; i < natts; i++)
		{
			ObjectAddressSubSet(referenced, RelationRelationId,
								RelationGetRelid(targetrel), attarray[i]);
			recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		}
	}

	copytup = heap_modify_tuple(pubreltup, RelationGetDescr(pubrel),
								values, nulls, replaces);

	CatalogTupleUpdate(pubrel, &pubreltup->t_self, copytup);

	heap_freetuple(copytup);
}

/* qsort comparator for attnums */
static int
compare_int16(const void *a, const void *b)
{
	int			av = *(const int16 *) a;
	int			bv = *(const int16 *) b;

	/* this can't overflow if int is wider than int16 */
	return (av - bv);
}

/*
 * Translate a list of column names to an array of attribute numbers
 * and a Bitmapset with them; verify that each attribute is appropriate
 * to have in a publication column list.  Other checks are done later;
 * see check_publication_columns.
 *
 * Note that the attribute numbers are *not* offset by
 * FirstLowInvalidHeapAttributeNumber; system columns are forbidden so this
 * is okay.
 */
static void
publication_translate_columns(Relation targetrel, List *columns, int *natts,
							  AttrNumber **attrs, Bitmapset **attset)
{
	AttrNumber *attarray;
	Bitmapset  *set = NULL;
	ListCell   *lc;
	int			n = 0;
	TupleDesc	tupdesc = RelationGetDescr(targetrel);

	/*
	 * Translate list of columns to attnums. We prohibit system attributes and
	 * make sure there are no duplicate columns.
	 */
	attarray = palloc(sizeof(AttrNumber) * list_length(columns));
	foreach(lc, columns)
	{
		char	   *colname = strVal(lfirst(lc));
		AttrNumber	attnum = get_attnum(RelationGetRelid(targetrel), colname);

		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   colname, RelationGetRelationName(targetrel)));

		if (!AttrNumberIsForUserDefinedAttr(attnum))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("cannot reference system column \"%s\" in publication column list",
						   colname));

		if (TupleDescAttr(tupdesc, attnum - 1)->attgenerated)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("cannot reference generated column \"%s\" in publication column list",
						   colname));

		if (bms_is_member(attnum, set))
			ereport(ERROR,
					errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("duplicate column \"%s\" in publication column list",
						   colname));

		set = bms_add_member(set, attnum);
		attarray[n++] = attnum;
	}

	/* Be tidy, so that the catalog representation is always sorted */
	qsort(attarray, n, sizeof(AttrNumber), compare_int16);

	*natts = n;
	*attrs = attarray;
	*attset = set;
}

/*
 * Insert new publication / schema mapping.
 */
ObjectAddress
publication_add_schema(Oid pubid, Oid schemaid, bool if_not_exists)
{
	Relation	rel;
	HeapTuple	tup;
	Datum		values[Natts_pg_publication_namespace];
	bool		nulls[Natts_pg_publication_namespace];
	Oid			psschid;
	Publication *pub = GetPublication(pubid);
	List	   *schemaRels = NIL;
	ObjectAddress myself,
				referenced;

	rel = table_open(PublicationNamespaceRelationId, RowExclusiveLock);

	/*
	 * Check for duplicates. Note that this does not really prevent
	 * duplicates, it's here just to provide nicer error message in common
	 * case. The real protection is the unique key on the catalog.
	 */
	if (SearchSysCacheExists2(PUBLICATIONNAMESPACEMAP,
							  ObjectIdGetDatum(schemaid),
							  ObjectIdGetDatum(pubid)))
	{
		table_close(rel, RowExclusiveLock);

		if (if_not_exists)
			return InvalidObjectAddress;

		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("schema \"%s\" is already member of publication \"%s\"",
						get_namespace_name(schemaid), pub->name)));
	}

	check_publication_add_schema(schemaid);

	/* Form a tuple */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	psschid = GetNewOidWithIndex(rel, PublicationNamespaceObjectIndexId,
								 Anum_pg_publication_namespace_oid);
	values[Anum_pg_publication_namespace_oid - 1] = ObjectIdGetDatum(psschid);
	values[Anum_pg_publication_namespace_pnpubid - 1] =
		ObjectIdGetDatum(pubid);
	values[Anum_pg_publication_namespace_pnnspid - 1] =
		ObjectIdGetDatum(schemaid);

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog */
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	ObjectAddressSet(myself, PublicationNamespaceRelationId, psschid);

	/* Add dependency on the publication */
	ObjectAddressSet(referenced, PublicationRelationId, pubid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Add dependency on the schema */
	ObjectAddressSet(referenced, NamespaceRelationId, schemaid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

	/* Close the table */
	table_close(rel, RowExclusiveLock);

	/*
	 * Invalidate relcache so that publication info is rebuilt. See
	 * publication_add_relation for why we need to consider all the
	 * partitions.
	 */
	schemaRels = GetSchemaPublicationRelations(schemaid,
											   PUBLICATION_PART_ALL);
	InvalidatePublicationRels(schemaRels);

	return myself;
}

/* Gets list of publication oids for a relation */
List *
GetRelationPublications(Oid relid)
{
	List	   *result = NIL;
	CatCList   *pubrellist;
	int			i;

	/* Find all publications associated with the relation. */
	pubrellist = SearchSysCacheList1(PUBLICATIONRELMAP,
									 ObjectIdGetDatum(relid));
	for (i = 0; i < pubrellist->n_members; i++)
	{
		HeapTuple	tup = &pubrellist->members[i]->tuple;
		Oid			pubid = ((Form_pg_publication_rel) GETSTRUCT(tup))->prpubid;

		result = lappend_oid(result, pubid);
	}

	ReleaseSysCacheList(pubrellist);

	return result;
}

/*
 * Gets a list of OIDs of all partial-column publications of the given
 * relation, that is, those that specify a column list.
 */
List *
GetRelationColumnPartialPublications(Oid relid)
{
	CatCList   *pubrellist;
	List	   *pubs = NIL;

	pubrellist = SearchSysCacheList1(PUBLICATIONRELMAP,
									 ObjectIdGetDatum(relid));
	for (int i = 0; i < pubrellist->n_members; i++)
	{
		HeapTuple	tup = &pubrellist->members[i]->tuple;
		bool		isnull;

		(void) SysCacheGetAttr(PUBLICATIONRELMAP, tup,
							   Anum_pg_publication_rel_prattrs,
							   &isnull);
		if (isnull)
			continue;

		pubs = lappend_oid(pubs,
						   ((Form_pg_publication_rel) GETSTRUCT(tup))->prpubid);
	}

	ReleaseSysCacheList(pubrellist);

	return pubs;
}

/* FIXME maybe these two routines should be in lsyscache.c */
/* Return the set of actions that the given publication includes */
void
GetActionsInPublication(Oid pubid, PublicationActions *actions)
{
	HeapTuple	pub;
	Form_pg_publication	pubForm;

	pub = SearchSysCache1(PUBLICATIONOID,
						  ObjectIdGetDatum(pubid));
	if (!HeapTupleIsValid(pub))
		elog(ERROR, "cache lookup failed for publication %u", pubid);

	pubForm = (Form_pg_publication) GETSTRUCT(pub);
	actions->pubinsert = pubForm->pubinsert;
	actions->pubupdate = pubForm->pubupdate;
	actions->pubdelete = pubForm->pubdelete;
	actions->pubtruncate = pubForm->pubtruncate;

	ReleaseSysCache(pub);
}

/*
 * For a relation in a publication that is known to have a non-null column
 * list, return the list of attribute numbers that are in it.
 */
List *
GetRelationColumnListInPublication(Oid relid, Oid pubid)
{
	HeapTuple	tup;
	Datum		adatum;
	bool		isnull;
	ArrayType  *arr;
	int			nelems;
	int16	   *elems;
	List	   *attnos = NIL;

	tup = SearchSysCache2(PUBLICATIONRELMAP,
						  ObjectIdGetDatum(relid),
						  ObjectIdGetDatum(pubid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for rel %u of publication %u", relid, pubid);
	adatum = SysCacheGetAttr(PUBLICATIONRELMAP, tup,
							 Anum_pg_publication_rel_prattrs, &isnull);
	if (isnull)
		elog(ERROR, "found unexpected null in pg_publication_rel.prattrs");
	arr = DatumGetArrayTypeP(adatum);
	nelems = ARR_DIMS(arr)[0];
	elems = (int16 *) ARR_DATA_PTR(arr);

	for (int i = 0; i < nelems; i++)
		attnos = lappend_oid(attnos, elems[i]);

	ReleaseSysCache(tup);

	return attnos;
}

/*
 * Gets list of relation oids for a publication.
 *
 * This should only be used FOR TABLE publications, the FOR ALL TABLES
 * should use GetAllTablesPublicationRelations().
 */
List *
GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt)
{
	List	   *result;
	Relation	pubrelsrel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tup;

	/* Find all publications associated with the relation. */
	pubrelsrel = table_open(PublicationRelRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_rel_prpubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pubid));

	scan = systable_beginscan(pubrelsrel, PublicationRelPrpubidIndexId,
							  true, NULL, 1, &scankey);

	result = NIL;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_publication_rel pubrel;

		pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);
		result = GetPubPartitionOptionRelations(result, pub_partopt,
												pubrel->prrelid);
	}

	systable_endscan(scan);
	table_close(pubrelsrel, AccessShareLock);

	/* Now sort and de-duplicate the result list */
	list_sort(result, list_oid_cmp);
	list_deduplicate_oid(result);

	return result;
}

/*
 * Gets list of publication oids for publications marked as FOR ALL TABLES.
 */
List *
GetAllTablesPublications(void)
{
	List	   *result;
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tup;

	/* Find all publications that are marked as for all tables. */
	rel = table_open(PublicationRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_puballtables,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 1, &scankey);

	result = NIL;
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Oid			oid = ((Form_pg_publication) GETSTRUCT(tup))->oid;

		result = lappend_oid(result, oid);
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return result;
}

/*
 * Gets list of all relation published by FOR ALL TABLES publication(s).
 *
 * If the publication publishes partition changes via their respective root
 * partitioned tables, we must exclude partitions in favor of including the
 * root partitioned tables.
 */
List *
GetAllTablesPublicationRelations(bool pubviaroot)
{
	Relation	classRel;
	ScanKeyData key[1];
	TableScanDesc scan;
	HeapTuple	tuple;
	List	   *result = NIL;

	classRel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_relkind,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(RELKIND_RELATION));

	scan = table_beginscan_catalog(classRel, 1, key);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = relForm->oid;

		if (is_publishable_class(relid, relForm) &&
			!(relForm->relispartition && pubviaroot))
			result = lappend_oid(result, relid);
	}

	table_endscan(scan);

	if (pubviaroot)
	{
		ScanKeyInit(&key[0],
					Anum_pg_class_relkind,
					BTEqualStrategyNumber, F_CHAREQ,
					CharGetDatum(RELKIND_PARTITIONED_TABLE));

		scan = table_beginscan_catalog(classRel, 1, key);

		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
			Oid			relid = relForm->oid;

			if (is_publishable_class(relid, relForm) &&
				!relForm->relispartition)
				result = lappend_oid(result, relid);
		}

		table_endscan(scan);
	}

	table_close(classRel, AccessShareLock);
	return result;
}

/*
 * Gets the list of schema oids for a publication.
 *
 * This should only be used FOR ALL TABLES IN SCHEMA publications.
 */
List *
GetPublicationSchemas(Oid pubid)
{
	List	   *result = NIL;
	Relation	pubschsrel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tup;

	/* Find all schemas associated with the publication */
	pubschsrel = table_open(PublicationNamespaceRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_publication_namespace_pnpubid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(pubid));

	scan = systable_beginscan(pubschsrel,
							  PublicationNamespacePnnspidPnpubidIndexId,
							  true, NULL, 1, &scankey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_publication_namespace pubsch;

		pubsch = (Form_pg_publication_namespace) GETSTRUCT(tup);

		result = lappend_oid(result, pubsch->pnnspid);
	}

	systable_endscan(scan);
	table_close(pubschsrel, AccessShareLock);

	return result;
}

/*
 * Gets the list of publication oids associated with a specified schema.
 */
List *
GetSchemaPublications(Oid schemaid)
{
	List	   *result = NIL;
	CatCList   *pubschlist;
	int			i;

	/* Find all publications associated with the schema */
	pubschlist = SearchSysCacheList1(PUBLICATIONNAMESPACEMAP,
									 ObjectIdGetDatum(schemaid));
	for (i = 0; i < pubschlist->n_members; i++)
	{
		HeapTuple	tup = &pubschlist->members[i]->tuple;
		Oid			pubid = ((Form_pg_publication_namespace) GETSTRUCT(tup))->pnpubid;

		result = lappend_oid(result, pubid);
	}

	ReleaseSysCacheList(pubschlist);

	return result;
}

/*
 * Get the list of publishable relation oids for a specified schema.
 */
List *
GetSchemaPublicationRelations(Oid schemaid, PublicationPartOpt pub_partopt)
{
	Relation	classRel;
	ScanKeyData key[1];
	TableScanDesc scan;
	HeapTuple	tuple;
	List	   *result = NIL;

	Assert(OidIsValid(schemaid));

	classRel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_class_relnamespace,
				BTEqualStrategyNumber, F_OIDEQ,
				schemaid);

	/* get all the relations present in the specified schema */
	scan = table_beginscan_catalog(classRel, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = relForm->oid;
		char		relkind;

		if (!is_publishable_class(relid, relForm))
			continue;

		relkind = get_rel_relkind(relid);
		if (relkind == RELKIND_RELATION)
			result = lappend_oid(result, relid);
		else if (relkind == RELKIND_PARTITIONED_TABLE)
		{
			List	   *partitionrels = NIL;

			/*
			 * It is quite possible that some of the partitions are in a
			 * different schema than the parent table, so we need to get such
			 * partitions separately.
			 */
			partitionrels = GetPubPartitionOptionRelations(partitionrels,
														   pub_partopt,
														   relForm->oid);
			result = list_concat_unique_oid(result, partitionrels);
		}
	}

	table_endscan(scan);
	table_close(classRel, AccessShareLock);
	return result;
}

/*
 * Gets the list of all relations published by FOR ALL TABLES IN SCHEMA
 * publication.
 */
List *
GetAllSchemaPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt)
{
	List	   *result = NIL;
	List	   *pubschemalist = GetPublicationSchemas(pubid);
	ListCell   *cell;

	foreach(cell, pubschemalist)
	{
		Oid			schemaid = lfirst_oid(cell);
		List	   *schemaRels = NIL;

		schemaRels = GetSchemaPublicationRelations(schemaid, pub_partopt);
		result = list_concat(result, schemaRels);
	}

	return result;
}

/*
 * Get publication using oid
 *
 * The Publication struct and its data are palloc'ed here.
 */
Publication *
GetPublication(Oid pubid)
{
	HeapTuple	tup;
	Publication *pub;
	Form_pg_publication pubform;

	tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication %u", pubid);

	pubform = (Form_pg_publication) GETSTRUCT(tup);

	pub = (Publication *) palloc(sizeof(Publication));
	pub->oid = pubid;
	pub->name = pstrdup(NameStr(pubform->pubname));
	pub->alltables = pubform->puballtables;
	pub->pubactions.pubinsert = pubform->pubinsert;
	pub->pubactions.pubupdate = pubform->pubupdate;
	pub->pubactions.pubdelete = pubform->pubdelete;
	pub->pubactions.pubtruncate = pubform->pubtruncate;
	pub->pubviaroot = pubform->pubviaroot;

	ReleaseSysCache(tup);

	return pub;
}


/*
 * Get Publication using name.
 */
Publication *
GetPublicationByName(const char *pubname, bool missing_ok)
{
	Oid			oid;

	oid = get_publication_oid(pubname, missing_ok);

	return OidIsValid(oid) ? GetPublication(oid) : NULL;
}

/*
 * get_publication_oid - given a publication name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid
get_publication_oid(const char *pubname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(PUBLICATIONNAME, Anum_pg_publication_oid,
						  CStringGetDatum(pubname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist", pubname)));
	return oid;
}

/*
 * get_publication_name - given a publication Oid, look up the name
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return NULL.
 */
char *
get_publication_name(Oid pubid, bool missing_ok)
{
	HeapTuple	tup;
	char	   *pubname;
	Form_pg_publication pubform;

	tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

	if (!HeapTupleIsValid(tup))
	{
		if (!missing_ok)
			elog(ERROR, "cache lookup failed for publication %u", pubid);
		return NULL;
	}

	pubform = (Form_pg_publication) GETSTRUCT(tup);
	pubname = pstrdup(NameStr(pubform->pubname));

	ReleaseSysCache(tup);

	return pubname;
}

/*
 * Returns Oids of tables in a publication.
 */
Datum
pg_get_publication_tables(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	char	   *pubname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Publication *publication;
	List	   *tables;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		publication = GetPublicationByName(pubname, false);

		/*
		 * Publications support partitioned tables, although all changes are
		 * replicated using leaf partition identity and schema, so we only
		 * need those.
		 */
		if (publication->alltables)
		{
			tables = GetAllTablesPublicationRelations(publication->pubviaroot);
		}
		else
		{
			List	   *relids,
					   *schemarelids;

			relids = GetPublicationRelations(publication->oid,
											 publication->pubviaroot ?
											 PUBLICATION_PART_ROOT :
											 PUBLICATION_PART_LEAF);
			schemarelids = GetAllSchemaPublicationRelations(publication->oid,
															publication->pubviaroot ?
															PUBLICATION_PART_ROOT :
															PUBLICATION_PART_LEAF);
			tables = list_concat_unique_oid(relids, schemarelids);

			/*
			 * If the publication publishes partition changes via their
			 * respective root partitioned tables, we must exclude partitions
			 * in favor of including the root partitioned tables. Otherwise,
			 * the function could return both the child and parent tables
			 * which could cause data of the child table to be
			 * double-published on the subscriber side.
			 */
			if (publication->pubviaroot)
				tables = filter_partitions(tables);
		}

		funcctx->user_fctx = (void *) tables;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	tables = (List *) funcctx->user_fctx;

	if (funcctx->call_cntr < list_length(tables))
	{
		Oid			relid = list_nth_oid(tables, funcctx->call_cntr);

		SRF_RETURN_NEXT(funcctx, ObjectIdGetDatum(relid));
	}

	SRF_RETURN_DONE(funcctx);
}
