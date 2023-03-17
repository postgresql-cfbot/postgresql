/*-------------------------------------------------------------------------
 *
 * pg_dict.c
 *	  routines to support manipulation of the pg_dict relation
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_dict.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/genam.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_dict.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Creates an entry in pg_dict for each of the supplied values.
 * vals is a list of String values.
 */
void
DictEntriesCreate(Oid dictTypeOid, List *vals)
{
	Relation	pg_dict;
	NameData	dictentry;
	Datum		values[Natts_pg_dict];
	bool		nulls[Natts_pg_dict];
	ListCell   *lc;
	HeapTuple	tup;

	if (vals == NIL)
	{
		/* The list is empty; do nothing. */
		return;
	}

	memset(nulls, false, sizeof(nulls));

	/*
	 * We don't check the list of values for duplicates here. If there are
	 * any, the user will get an unique-index violation.
	 */

	pg_dict = table_open(DictRelationId, RowExclusiveLock);
	foreach(lc, vals)
	{
		Oid			oid = GetNewOidWithIndex(pg_dict, DictOidIndexId,
											 Anum_pg_dict_oid);
		char	   *entry = strVal(lfirst(lc));

		/*
		 * Entries are stored in a name field, for easier syscache lookup, so
		 * check the length to make sure it's within range.
		 */
		if (strlen(entry) > (NAMEDATALEN - 1))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("invalid dict entry \"%s\"", entry),
					 errdetail("Entries must be %d bytes or less.",
							   NAMEDATALEN - 1)));

		/* AALEKSEEV TODO use bytea instead */
		namestrcpy(&dictentry, entry);

		values[Anum_pg_dict_oid - 1] = ObjectIdGetDatum(oid);
		values[Anum_pg_dict_dicttypid - 1] = ObjectIdGetDatum(dictTypeOid);
		values[Anum_pg_dict_dictentry - 1] = NameGetDatum(&dictentry);

		tup = heap_form_tuple(RelationGetDescr(pg_dict), values, nulls);

		CatalogTupleInsert(pg_dict, tup);
		heap_freetuple(tup);
	}

	/* clean up */
	table_close(pg_dict, RowExclusiveLock);
}

/*
 * Returns all the entries for the dictinary with given Oid. Entries are sorted
 * by dictentry. Note that shorter entries are considered smaller, i.e. 'abc'
 * goes before 'abcdef'. The memory is allocated in a child memory context of
 * the caller's memory context. It can be freed with DictEntriesFree().
 *
 * If there are no entries a valid but empty dictionary is returned.
 */
Dictionary
DictEntriesRead(Oid dictTypeOid)
{
	Relation	pg_dict;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tup;
	uint32      entries_allocated;
	Dictionary  dict;
	MemoryContext myctx, oldctx;

	myctx = AllocSetContextCreate(CurrentMemoryContext, "DictEntriesCtx", ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(myctx);

	dict = (Dictionary)palloc(sizeof(DictionaryData));

	entries_allocated = 8;
	dict->nentries = 0;
	dict->entries = (DictEntry*)palloc(entries_allocated*sizeof(DictEntry));

	pg_dict = table_open(DictRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_dict_dicttypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dictTypeOid));

	scan = systable_beginscan(pg_dict, DictTypIdEntryIndexId, true,
							  NULL, 1, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		if(dict->nentries == entries_allocated)
		{
			entries_allocated = entries_allocated * 2;
			dict->entries = (DictEntry*)repalloc(dict->entries, entries_allocated*sizeof(DictEntry));
		}

		dict->entries[dict->nentries].oid = ((Form_pg_dict) GETSTRUCT(tup))->oid;
		/*
		 * This is arguably not the fastest way to determine the length of
		 * the entry. Alternatively, we could store a precalculated value in
		 * the catalog. However, usually it's a good idea to keep things simple
		 * until somebody discovers a bottleneck in this exact place and
		 * proposes a concrete fix.
		 */
		dict->entries[dict->nentries].length = (uint32)strlen(((Form_pg_dict) GETSTRUCT(tup))->dictentry.data);
		dict->entries[dict->nentries].data = palloc(dict->entries[dict->nentries].length);
		memcpy(dict->entries[dict->nentries].data, ((Form_pg_dict) GETSTRUCT(tup))->dictentry.data, dict->entries[dict->nentries].length);
		dict->nentries++;
	}

	systable_endscan(scan);
	table_close(pg_dict, RowExclusiveLock);

	MemoryContextSwitchTo(oldctx);

	return dict;
}

/*
 * Frees the memory allocated for the dictionary.
 */
void
DictEntriesFree(Dictionary dict)
{
	MemoryContextDelete(GetMemoryChunkContext(dict));
}

/*
 * Deletes all the entries for the dictinary with given Oid.
 */
void
DictEntriesDelete(Oid dictTypeOid)
{
	Relation	pg_dict;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple	tup;

	pg_dict = table_open(DictRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_dict_dicttypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dictTypeOid));

	scan = systable_beginscan(pg_dict, DictTypIdEntryIndexId, true,
							  NULL, 1, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		CatalogTupleDelete(pg_dict, &tup->t_self);
	}

	systable_endscan(scan);
	table_close(pg_dict, RowExclusiveLock);
}
