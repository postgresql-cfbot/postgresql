/*-------------------------------------------------------------------------
 *
 * publicationcmds.c
 *		publication manipulation
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		publicationcmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_publication_schema.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/publicationcmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

/* Same as MAXNUMMESSAGES in sinvaladt.c */
#define MAX_RELCACHE_INVAL_MSGS 4096

static List *OpenTableList(List *tables);
static void CloseTableList(List *rels);
static void PublicationAddTables(Oid pubid, List *rels, bool if_not_exists,
								 AlterPublicationStmt *stmt);
static void PublicationDropTables(Oid pubid, List *rels, bool missing_ok);
static void PublicationAddSchemas(Oid pubid, List *schemas, bool if_not_exists,
								  AlterPublicationStmt *stmt);
static void PublicationDropSchemas(Oid pubid, List *schemas, bool missing_ok);

static void
parse_publication_options(ParseState *pstate,
						  List *options,
						  bool *publish_given,
						  PublicationActions *pubactions,
						  bool *publish_via_partition_root_given,
						  bool *publish_via_partition_root)
{
	ListCell   *lc;

	*publish_given = false;
	*publish_via_partition_root_given = false;

	/* defaults */
	pubactions->pubinsert = true;
	pubactions->pubupdate = true;
	pubactions->pubdelete = true;
	pubactions->pubtruncate = true;
	*publish_via_partition_root = false;

	/* Parse options */
	foreach(lc, options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "publish") == 0)
		{
			char	   *publish;
			List	   *publish_list;
			ListCell   *lc;

			if (*publish_given)
				errorConflictingDefElem(defel, pstate);

			/*
			 * If publish option was given only the explicitly listed actions
			 * should be published.
			 */
			pubactions->pubinsert = false;
			pubactions->pubupdate = false;
			pubactions->pubdelete = false;
			pubactions->pubtruncate = false;

			*publish_given = true;
			publish = defGetString(defel);

			if (!SplitIdentifierString(publish, ',', &publish_list))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid list syntax for \"publish\" option")));

			/* Process the option list. */
			foreach(lc, publish_list)
			{
				char	   *publish_opt = (char *) lfirst(lc);

				if (strcmp(publish_opt, "insert") == 0)
					pubactions->pubinsert = true;
				else if (strcmp(publish_opt, "update") == 0)
					pubactions->pubupdate = true;
				else if (strcmp(publish_opt, "delete") == 0)
					pubactions->pubdelete = true;
				else if (strcmp(publish_opt, "truncate") == 0)
					pubactions->pubtruncate = true;
				else
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized \"publish\" value: \"%s\"", publish_opt)));
			}
		}
		else if (strcmp(defel->defname, "publish_via_partition_root") == 0)
		{
			if (*publish_via_partition_root_given)
				errorConflictingDefElem(defel, pstate);
			*publish_via_partition_root_given = true;
			*publish_via_partition_root = defGetBoolean(defel);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized publication parameter: \"%s\"", defel->defname)));
	}
}

/*
 * Convert the SchemaSpec list into an Oid list.
 */
static List *
ConvertSchemaSpecListToOidList(List *schemas)
{
	List	   *schemaoidlist = NIL;
	ListCell   *cell;

	foreach(cell, schemas)
	{
		SchemaSpec *schema = (SchemaSpec *) lfirst(cell);
		Oid			schemaoid;
		List	   *search_path;
		char	   *nspname;

		if (schema->schematype == SCHEMASPEC_CURRENT_SCHEMA)
		{
			search_path = fetch_search_path(false);
			if (search_path == NIL) /* nothing valid in search_path? */
				ereport(ERROR,
						errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("no schema has been selected"));

			schemaoid = linitial_oid(search_path);
			nspname = get_namespace_name(schemaoid);
			if (nspname == NULL)	/* recently-deleted namespace? */
				ereport(ERROR,
						errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("no schema has been selected"));
		}
		else
			schemaoid = get_namespace_oid(schema->schemaname, false);

		/* Filter out duplicates if user specifies "sch1, sch1" */
		schemaoidlist = list_append_unique_oid(schemaoidlist, schemaoid);
	}

	return schemaoidlist;
}

/*
 * Create new publication.
 */
ObjectAddress
CreatePublication(ParseState *pstate, CreatePublicationStmt *stmt)
{
	Relation	rel;
	ObjectAddress myself;
	Oid			puboid;
	bool		nulls[Natts_pg_publication];
	Datum		values[Natts_pg_publication];
	HeapTuple	tup;
	bool		publish_given;
	PublicationActions pubactions;
	bool		publish_via_partition_root_given;
	bool		publish_via_partition_root;
	AclResult	aclresult;

	/* must have CREATE privilege on database */
	aclresult = pg_database_aclcheck(MyDatabaseId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(MyDatabaseId));

	/* FOR ALL TABLES requires superuser */
	if (stmt->for_all_tables && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create FOR ALL TABLES publication")));

	rel = table_open(PublicationRelationId, RowExclusiveLock);

	/* Check if name is used */
	puboid = GetSysCacheOid1(PUBLICATIONNAME, Anum_pg_publication_oid,
							 CStringGetDatum(stmt->pubname));
	if (OidIsValid(puboid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("publication \"%s\" already exists",
						stmt->pubname)));
	}

	/* Form a tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_publication_pubname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->pubname));
	values[Anum_pg_publication_pubowner - 1] = ObjectIdGetDatum(GetUserId());

	parse_publication_options(pstate,
							  stmt->options,
							  &publish_given, &pubactions,
							  &publish_via_partition_root_given,
							  &publish_via_partition_root);

	puboid = GetNewOidWithIndex(rel, PublicationObjectIndexId,
								Anum_pg_publication_oid);
	values[Anum_pg_publication_oid - 1] = ObjectIdGetDatum(puboid);
	values[Anum_pg_publication_puballtables - 1] =
		BoolGetDatum(stmt->for_all_tables);
	values[Anum_pg_publication_pubinsert - 1] =
		BoolGetDatum(pubactions.pubinsert);
	values[Anum_pg_publication_pubupdate - 1] =
		BoolGetDatum(pubactions.pubupdate);
	values[Anum_pg_publication_pubdelete - 1] =
		BoolGetDatum(pubactions.pubdelete);
	values[Anum_pg_publication_pubtruncate - 1] =
		BoolGetDatum(pubactions.pubtruncate);
	values[Anum_pg_publication_pubviaroot - 1] =
		BoolGetDatum(publish_via_partition_root);

	if (stmt->schemas)
		values[Anum_pg_publication_pubtype - 1] = PUBTYPE_SCHEMA;
	else if (stmt->tables)
		values[Anum_pg_publication_pubtype - 1] = PUBTYPE_TABLE;
	else if (stmt->for_all_tables)
		values[Anum_pg_publication_pubtype - 1] = PUBTYPE_ALLTABLES;
	else
		values[Anum_pg_publication_pubtype - 1] = PUBTYPE_EMPTY;

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	/* Insert tuple into catalog. */
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);

	recordDependencyOnOwner(PublicationRelationId, puboid, GetUserId());

	ObjectAddressSet(myself, PublicationRelationId, puboid);

	/* Make the changes visible. */
	CommandCounterIncrement();

	if (stmt->schemas)
	{
		List	   *schemaoidlist = NIL;
		Relation	nspcrel;

		Assert(list_length(stmt->schemas) > 0);

		schemaoidlist = ConvertSchemaSpecListToOidList(stmt->schemas);

		nspcrel = table_open(NamespaceRelationId, ShareUpdateExclusiveLock);
		PublicationAddSchemas(puboid, schemaoidlist, true, NULL);
		table_close(nspcrel, ShareUpdateExclusiveLock);
	}

	if (stmt->tables)
	{
		List	   *rels;

		Assert(list_length(stmt->tables) > 0);

		rels = OpenTableList(stmt->tables);
		PublicationAddTables(puboid, rels, true, NULL);
		CloseTableList(rels);
	}

	table_close(rel, RowExclusiveLock);

	InvokeObjectPostCreateHook(PublicationRelationId, puboid, 0);

	if (wal_level != WAL_LEVEL_LOGICAL)
	{
		ereport(WARNING,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("wal_level is insufficient to publish logical changes"),
				 errhint("Set wal_level to logical before creating subscriptions.")));
	}

	return myself;
}

/*
 * Update publication type in pg_publication relation.
 */
static void
UpdatePublicationTypeTupleValue(Relation rel, HeapTuple tup, int col,
								char pubtype)
{
	bool		nulls[Natts_pg_publication];
	bool		replaces[Natts_pg_publication];
	Datum		values[Natts_pg_publication];


	/* Everything ok, form a new tuple */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	values[col - 1] = pubtype;
	replaces[col - 1] = true;

	tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
							replaces);

	/* Update the catalog */
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	CommandCounterIncrement();
}

/*
 * Change options of a publication.
 */
static void
AlterPublicationOptions(ParseState *pstate, AlterPublicationStmt *stmt,
						Relation rel, HeapTuple tup)
{
	bool		nulls[Natts_pg_publication];
	bool		replaces[Natts_pg_publication];
	Datum		values[Natts_pg_publication];
	bool		publish_given;
	PublicationActions pubactions;
	bool		publish_via_partition_root_given;
	bool		publish_via_partition_root;
	ObjectAddress obj;
	Form_pg_publication pubform;

	parse_publication_options(pstate,
							  stmt->options,
							  &publish_given, &pubactions,
							  &publish_via_partition_root_given,
							  &publish_via_partition_root);

	/* Everything ok, form a new tuple. */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	if (publish_given)
	{
		values[Anum_pg_publication_pubinsert - 1] = BoolGetDatum(pubactions.pubinsert);
		replaces[Anum_pg_publication_pubinsert - 1] = true;

		values[Anum_pg_publication_pubupdate - 1] = BoolGetDatum(pubactions.pubupdate);
		replaces[Anum_pg_publication_pubupdate - 1] = true;

		values[Anum_pg_publication_pubdelete - 1] = BoolGetDatum(pubactions.pubdelete);
		replaces[Anum_pg_publication_pubdelete - 1] = true;

		values[Anum_pg_publication_pubtruncate - 1] = BoolGetDatum(pubactions.pubtruncate);
		replaces[Anum_pg_publication_pubtruncate - 1] = true;
	}

	if (publish_via_partition_root_given)
	{
		values[Anum_pg_publication_pubviaroot - 1] = BoolGetDatum(publish_via_partition_root);
		replaces[Anum_pg_publication_pubviaroot - 1] = true;
	}

	tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls,
							replaces);

	/* Update the catalog. */
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	CommandCounterIncrement();

	pubform = (Form_pg_publication) GETSTRUCT(tup);

	/* Invalidate the relcache. */
	if (pubform->pubtype == PUBTYPE_ALLTABLES)
	{
		CacheInvalidateRelcacheAll();
	}
	else
	{
		List	   *relids = NIL;

		/*
		 * For any partitioned tables contained in the publication, we must
		 * invalidate all partitions contained in the respective partition
		 * trees, not just those explicitly mentioned in the publication.
		 */
		if (pubform->pubtype == PUBTYPE_TABLE)
			relids = GetPublicationRelations(pubform->oid,
											 PUBLICATION_PART_ALL);
		else if (pubform->pubtype == PUBTYPE_SCHEMA)
			relids = GetAllSchemasPublicationRelations(pubform->pubviaroot,
													   pubform->oid);

		/*
		 * We don't want to send too many individual messages, at some point
		 * it's cheaper to just reset whole relcache.
		 */
		if (list_length(relids) < MAX_RELCACHE_INVAL_MSGS)
		{
			ListCell   *lc;

			foreach(lc, relids)
			{
				Oid			relid = lfirst_oid(lc);

				CacheInvalidateRelcacheByRelid(relid);
			}
		}
		else
			CacheInvalidateRelcacheAll();
	}

	ObjectAddressSet(obj, PublicationRelationId, pubform->oid);
	EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
									 (Node *) stmt);

	InvokeObjectPostAlterHook(PublicationRelationId, pubform->oid, 0);
}

/*
 * Add or remove table to/from publication.
 */
static void
AlterPublicationTables(AlterPublicationStmt *stmt, Relation rel,
					   HeapTuple tup)
{
	List	   *rels = NIL;
	Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);
	Oid			pubid = pubform->oid;

	/* Check that user is allowed to manipulate the publication tables. */
	if (pubform->pubtype == PUBTYPE_ALLTABLES)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publication \"%s\" is defined as FOR ALL TABLES",
						NameStr(pubform->pubname)),
				 errdetail("Tables cannot be added to or dropped from FOR ALL TABLES publications.")));

	if (pubform->pubtype == PUBTYPE_SCHEMA)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publication \"%s\" is defined as FOR SCHEMA",
						NameStr(pubform->pubname)),
				 errdetail("Tables cannot be added to or dropped from FOR SCHEMA publications.")));

	Assert(list_length(stmt->tables) > 0);

	rels = OpenTableList(stmt->tables);

	if (stmt->tableAction == DEFELEM_ADD)
	{
		PublicationAddTables(pubid, rels, false, stmt);
		if (pubform->pubtype == PUBTYPE_EMPTY)
			UpdatePublicationTypeTupleValue(rel, tup, Anum_pg_publication_pubtype,
											PUBTYPE_TABLE);
	}
	else if (stmt->tableAction == DEFELEM_DROP)
		PublicationDropTables(pubid, rels, false);
	else						/* DEFELEM_SET */
	{
		List	   *oldrelids = GetPublicationRelations(pubid,
														PUBLICATION_PART_ROOT);
		List	   *delrels = NIL;
		ListCell   *oldlc;

		/* Calculate which relations to drop. */
		foreach(oldlc, oldrelids)
		{
			Oid			oldrelid = lfirst_oid(oldlc);
			ListCell   *newlc;
			bool		found = false;

			foreach(newlc, rels)
			{
				Relation	newrel = (Relation) lfirst(newlc);

				if (RelationGetRelid(newrel) == oldrelid)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				Relation	oldrel = table_open(oldrelid,
												ShareUpdateExclusiveLock);

				delrels = lappend(delrels, oldrel);
			}
		}

		/* And drop them. */
		PublicationDropTables(pubid, delrels, true);

		/*
		 * Don't bother calculating the difference for adding, we'll catch and
		 * skip existing ones when doing catalog update.
		 */
		PublicationAddTables(pubid, rels, true, stmt);

		CloseTableList(delrels);
		if (pubform->pubtype == PUBTYPE_EMPTY)
			UpdatePublicationTypeTupleValue(rel, tup,
											Anum_pg_publication_pubtype,
											PUBTYPE_TABLE);
	}

	CloseTableList(rels);
}

/*
 * Alter the publication schemas.
 *
 * Add/Remove/Set the schemas to/from publication.
 */
static void
AlterPublicationSchemas(AlterPublicationStmt *stmt, Relation rel,
						HeapTuple tup, Form_pg_publication pubform)
{
	List	   *schemaoidlist = NIL;

	/* Check that user is allowed to manipulate the publication tables */
	if (pubform->pubtype == PUBTYPE_ALLTABLES)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publication \"%s\" is defined as FOR ALL TABLES",
						NameStr(pubform->pubname)),
				 errdetail("Schemas cannot be added to or dropped from FOR ALL TABLES publications.")));

	if (pubform->pubtype == PUBTYPE_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publication \"%s\" is defined as FOR TABLE",
						NameStr(pubform->pubname)),
				 errdetail("Schemas cannot be added to or dropped from FOR TABLE publications.")));

	/* Convert the text list into oid list */
	schemaoidlist = ConvertSchemaSpecListToOidList(stmt->schemas);

	if (stmt->tableAction == DEFELEM_ADD)
	{
		PublicationAddSchemas(pubform->oid, schemaoidlist, false, stmt);
		if (pubform->pubtype == PUBTYPE_EMPTY)
			UpdatePublicationTypeTupleValue(rel, tup,
											Anum_pg_publication_pubtype,
											PUBTYPE_SCHEMA);
	}
	else if (stmt->tableAction == DEFELEM_DROP)
		PublicationDropSchemas(pubform->oid, schemaoidlist, false);
	else
	{
		List	   *oldschemaids = GetPublicationSchemas(pubform->oid);
		List	   *delschemas = NIL;
		ListCell   *oldlc;

		/* Identify which schemas should be dropped */
		delschemas = list_difference_oid(oldschemaids, schemaoidlist);

		/* And drop them */
		PublicationDropSchemas(pubform->oid, delschemas, true);

		/*
		 * Don't bother calculating the difference for adding, we'll catch and
		 * skip existing ones when doing catalog update.
		 */
		PublicationAddSchemas(pubform->oid, schemaoidlist, true, stmt);
		if (pubform->pubtype == PUBTYPE_EMPTY)
			UpdatePublicationTypeTupleValue(rel, tup,
											Anum_pg_publication_pubtype,
											PUBTYPE_SCHEMA);
	}

	return;
}

/*
 * Alter the existing publication.
 *
 * This is dispatcher function for AlterPublicationOptions,
 * AlterPublicationSchemas and AlterPublicationTables.
 */
void
AlterPublication(ParseState *pstate, AlterPublicationStmt *stmt)
{
	Relation	rel;
	HeapTuple	tup;
	Form_pg_publication pubform;

	rel = table_open(PublicationRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PUBLICATIONNAME,
							  CStringGetDatum(stmt->pubname));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist",
						stmt->pubname)));

	pubform = (Form_pg_publication) GETSTRUCT(tup);

	/* must be owner */
	if (!pg_publication_ownercheck(pubform->oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_PUBLICATION,
					   stmt->pubname);

	if (stmt->options)
		AlterPublicationOptions(pstate, stmt, rel, tup);
	else if (stmt->schemas)
		AlterPublicationSchemas(stmt, rel, tup, pubform);
	else
		AlterPublicationTables(stmt, rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);
}

/*
 * Remove relation from publication by mapping OID.
 */
void
RemovePublicationRelById(Oid proid)
{
	Relation	rel;
	HeapTuple	tup;
	Form_pg_publication_rel pubrel;

	rel = table_open(PublicationRelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PUBLICATIONREL, ObjectIdGetDatum(proid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication table %u",
			 proid);

	pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);

	/* Invalidate relcache so that publication info is rebuilt. */
	CacheInvalidateRelcacheByRelid(pubrel->prrelid);

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}

/*
 * Remove schema from publication by mapping OID.
 */
void
RemovePublicationSchemaById(Oid psoid)
{
	Relation	rel;
	HeapTuple	tup;

	rel = table_open(PublicationSchemaRelationId, RowExclusiveLock);

	tup = SearchSysCache1(PUBLICATIONSCHEMA, ObjectIdGetDatum(psoid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for publication schema %u",
			 psoid);

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);
}

/*
 * Open relations specified by a RangeVar list.
 * The returned tables are locked in ShareUpdateExclusiveLock mode in order to
 * add them to a publication.
 */
static List *
OpenTableList(List *tables)
{
	List	   *relids = NIL;
	List	   *rels = NIL;
	ListCell   *lc;

	/*
	 * Open, share-lock, and check all the explicitly-specified relations
	 */
	foreach(lc, tables)
	{
		RangeVar   *rv = lfirst_node(RangeVar, lc);
		bool		recurse = rv->inh;
		Relation	rel;
		Oid			myrelid;

		/* Allow query cancel in case this takes a long time */
		CHECK_FOR_INTERRUPTS();

		rel = table_openrv(rv, ShareUpdateExclusiveLock);
		myrelid = RelationGetRelid(rel);

		/*
		 * Filter out duplicates if user specifies "foo, foo".
		 *
		 * Note that this algorithm is known to not be very efficient (O(N^2))
		 * but given that it only works on list of tables given to us by user
		 * it's deemed acceptable.
		 */
		if (list_member_oid(relids, myrelid))
		{
			table_close(rel, ShareUpdateExclusiveLock);
			continue;
		}

		rels = lappend(rels, rel);
		relids = lappend_oid(relids, myrelid);

		/*
		 * Add children of this rel, if requested, so that they too are added
		 * to the publication.  A partitioned table can't have any inheritance
		 * children other than its partitions, which need not be explicitly
		 * added to the publication.
		 */
		if (recurse && rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		{
			List	   *children;
			ListCell   *child;

			children = find_all_inheritors(myrelid, ShareUpdateExclusiveLock,
										   NULL);

			foreach(child, children)
			{
				Oid			childrelid = lfirst_oid(child);

				/* Allow query cancel in case this takes a long time */
				CHECK_FOR_INTERRUPTS();

				/*
				 * Skip duplicates if user specified both parent and child
				 * tables.
				 */
				if (list_member_oid(relids, childrelid))
					continue;

				/* find_all_inheritors already got lock */
				rel = table_open(childrelid, NoLock);
				rels = lappend(rels, rel);
				relids = lappend_oid(relids, childrelid);
			}
		}
	}

	list_free(relids);

	return rels;
}

/*
 * Close all relations in the list.
 */
static void
CloseTableList(List *rels)
{
	ListCell   *lc;

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);

		table_close(rel, NoLock);
	}
}

/*
 * Add listed tables to the publication.
 */
static void
PublicationAddTables(Oid pubid, List *rels, bool if_not_exists,
					 AlterPublicationStmt *stmt)
{
	ListCell   *lc;

	Assert(!stmt || (!stmt->for_all_tables && !stmt->schemas));

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);
		ObjectAddress obj;

		/* Must be owner of the table or superuser. */
		if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(rel->rd_rel->relkind),
						   RelationGetRelationName(rel));

		obj = publication_add_relation(pubid, rel, if_not_exists);
		if (stmt)
		{
			EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
											 (Node *) stmt);

			InvokeObjectPostCreateHook(PublicationRelRelationId,
									   obj.objectId, 0);
		}
	}
}

/*
 * Add listed schemas to the publication.
 */
static void
PublicationAddSchemas(Oid pubid, List *schemas, bool if_not_exists,
					  AlterPublicationStmt *stmt)
{
	ListCell   *lc;

	Assert(!stmt || (!stmt->for_all_tables && !stmt->tables));

	foreach(lc, schemas)
	{
		Oid			schemaoid = lfirst_oid(lc);
		ObjectAddress obj;

		/* Must be owner of the schema or superuser */
		if (!pg_namespace_ownercheck(schemaoid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA,
						   get_namespace_name(schemaoid));

		obj = publication_add_schema(pubid, schemaoid, if_not_exists);
		if (stmt)
		{
			EventTriggerCollectSimpleCommand(obj, InvalidObjectAddress,
											 (Node *) stmt);

			InvokeObjectPostCreateHook(PublicationSchemaRelationId,
									   obj.objectId, 0);
		}
	}
}

/*
 * Remove listed tables from the publication.
 */
static void
PublicationDropTables(Oid pubid, List *rels, bool missing_ok)
{
	ObjectAddress obj;
	ListCell   *lc;
	Oid			prid;

	foreach(lc, rels)
	{
		Relation	rel = (Relation) lfirst(lc);
		Oid			relid = RelationGetRelid(rel);

		prid = GetSysCacheOid2(PUBLICATIONRELMAP, Anum_pg_publication_rel_oid,
							   ObjectIdGetDatum(relid),
							   ObjectIdGetDatum(pubid));
		if (!OidIsValid(prid))
		{
			if (missing_ok)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation \"%s\" is not part of the publication",
							RelationGetRelationName(rel))));
		}

		ObjectAddressSet(obj, PublicationRelRelationId, prid);
		performDeletion(&obj, DROP_CASCADE, 0);
	}
}

/*
 * Remove listed schemas from the publication.
 */
static void
PublicationDropSchemas(Oid pubid, List *schemas, bool missing_ok)
{
	ObjectAddress obj;
	ListCell   *lc;
	Oid			psid;

	foreach(lc, schemas)
	{
		Oid			schemaoid = lfirst_oid(lc);

		psid = GetSysCacheOid2(PUBLICATIONSCHEMAMAP,
							   Anum_pg_publication_schema_oid,
							   ObjectIdGetDatum(schemaoid),
							   ObjectIdGetDatum(pubid));
		if (!OidIsValid(psid))
		{
			if (missing_ok)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("schema \"%s\" is not part of the publication",
							get_namespace_name(schemaoid))));
		}

		ObjectAddressSet(obj, PublicationSchemaRelationId, psid);
		performDeletion(&obj, DROP_CASCADE, 0);
	}
}

/*
 * Internal workhorse for changing a publication owner
 */
static void
AlterPublicationOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
	Form_pg_publication form;

	form = (Form_pg_publication) GETSTRUCT(tup);

	if (form->pubowner == newOwnerId)
		return;

	if (!superuser())
	{
		AclResult	aclresult;

		/* Must be owner */
		if (!pg_publication_ownercheck(form->oid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_PUBLICATION,
						   NameStr(form->pubname));

		/* Must be able to become new owner */
		check_is_member_of_role(GetUserId(), newOwnerId);

		/* New owner must have CREATE privilege on database */
		aclresult = pg_database_aclcheck(MyDatabaseId, newOwnerId, ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_DATABASE,
						   get_database_name(MyDatabaseId));

		if (form->pubtype == PUBTYPE_ALLTABLES && !superuser_arg(newOwnerId))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to change owner of publication \"%s\"",
							NameStr(form->pubname)),
					 errhint("The owner of a FOR ALL TABLES publication must be a superuser.")));
	}

	form->pubowner = newOwnerId;
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	/* Update owner dependency reference */
	changeDependencyOnOwner(PublicationRelationId,
							form->oid,
							newOwnerId);

	InvokeObjectPostAlterHook(PublicationRelationId,
							  form->oid, 0);
}

/*
 * Change publication owner -- by name
 */
ObjectAddress
AlterPublicationOwner(const char *name, Oid newOwnerId)
{
	Oid			subid;
	HeapTuple	tup;
	Relation	rel;
	ObjectAddress address;
	Form_pg_publication pubform;

	rel = table_open(PublicationRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PUBLICATIONNAME, CStringGetDatum(name));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication \"%s\" does not exist", name)));

	pubform = (Form_pg_publication) GETSTRUCT(tup);
	subid = pubform->oid;

	AlterPublicationOwner_internal(rel, tup, newOwnerId);

	ObjectAddressSet(address, PublicationRelationId, subid);

	heap_freetuple(tup);

	table_close(rel, RowExclusiveLock);

	return address;
}

/*
 * Change publication owner -- by OID
 */
void
AlterPublicationOwner_oid(Oid subid, Oid newOwnerId)
{
	HeapTuple	tup;
	Relation	rel;

	rel = table_open(PublicationRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(PUBLICATIONOID, ObjectIdGetDatum(subid));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("publication with OID %u does not exist", subid)));

	AlterPublicationOwner_internal(rel, tup, newOwnerId);

	heap_freetuple(tup);

	table_close(rel, RowExclusiveLock);
}
