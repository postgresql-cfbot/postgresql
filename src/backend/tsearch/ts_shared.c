/*-------------------------------------------------------------------------
 *
 * ts_shared.c
 *	  tsearch shared memory management
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/ts_shared.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/pg_ts_dict.h"
#include "lib/dshash.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tsearch/ts_shared.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


/*
 * Hash table structures
 */
typedef struct
{
	Oid			dict_id;
	dsm_handle	dict_dsm;
	Size		dict_size;

	/* How many backends have DSM mapping */
	uint32		refcnt;
} TsearchDictEntry;

static dshash_table *dict_table = NULL;

/*
 * Shared struct for locking
 */
typedef struct
{
	dsa_handle	area;
	dshash_table_handle dict_table_handle;

	/* Total size of loaded dictionaries into shared memory in bytes */
	Size		loaded_size;

	LWLock		lock;
} TsearchCtlData;

static TsearchCtlData *tsearch_ctl;

/*
 * GUC variable for maximum number of shared dictionaries. Default value is
 * 100MB.
 */
int			max_shared_dictionaries_size = 100 * 1024;

static void init_dict_table(void);

/* Parameters for dict_table */
static const dshash_parameters dict_table_params ={
	sizeof(Oid),
	sizeof(TsearchDictEntry),
	dshash_memcmp,
	dshash_memhash,
	LWTRANCHE_TSEARCH_TABLE
};

/*
 * Build the dictionary using allocate_cb callback. If there is a space in
 * shared memory and max_shared_dictionaries_size is greater than 0 copy the
 * dictionary into DSM.
 *
 * If max_shared_dictionaries_size is greater than 0 then try to find the
 * dictionary in shared hash table first. If it was built by someone earlier
 * just return its location in DSM.
 *
 * dictid: Oid of the dictionary.
 * arg: an argument to the callback function.
 * allocate_cb: function to build the dictionary, if it wasn't found in DSM.
 *
 * Returns address in the dynamic shared memory segment or in backend memory.
 */
void *
ts_dict_shmem_location(Oid dictid, List *dictoptions,
					   ispell_build_callback allocate_cb)
{
	TsearchDictEntry *entry;
	bool		found;
	dsm_segment *seg;
	void	   *dict,
			   *dict_location;

#define CHECK_SHARED_SPACE() \
	if (entry->dict_size + tsearch_ctl->loaded_size >	\
		max_shared_dictionaries_size * 1024L)			\
	{													\
		LWLockRelease(&tsearch_ctl->lock);				\
		ereport(LOG, \
				(errmsg("there is no space in shared memory for text search " \
						"dictionary %u, it will be loaded into backend's memory", \
						dictid))); \
		dshash_delete_entry(dict_table, entry);		\
		return dict; \
	} \

	init_dict_table();

	/*
	 * Build the dictionary in backend's memory if a hash table wasn't created
	 * or dictid is invalid (it may happen if the dicionary's init method was
	 * called within verify_dictoptions()).
	 */
	if (!DsaPointerIsValid(tsearch_ctl->dict_table_handle) ||
		!OidIsValid(dictid))
	{
		Size		dict_size;

		dict = allocate_cb(dictoptions, &dict_size);

		return dict;
	}

	/* Try to find an entry in the hash table */
	entry = (TsearchDictEntry *) dshash_find(dict_table, &dictid, false);

	if (entry)
	{
		seg = dsm_find_mapping(entry->dict_dsm);
		if (!seg)
		{
			seg = dsm_attach(entry->dict_dsm);
			/* Remain attached until end of session */
			dsm_pin_mapping(seg);
		}

		entry->refcnt++;
		dshash_release_lock(dict_table, entry);

		return dsm_segment_address(seg);
	}

	/* Dictionary haven't been loaded into memory yet */
	entry = (TsearchDictEntry *) dshash_find_or_insert(dict_table, &dictid,
													   &found);

	if (found)
	{
		/*
		 * Someone concurrently inserted a dictionary entry since the first time
		 * we checked.
		 */
		seg = dsm_attach(entry->dict_dsm);

		/* Remain attached until end of session */
		dsm_pin_mapping(seg);

		entry->refcnt++;
		dshash_release_lock(dict_table, entry);

		return dsm_segment_address(seg);
	}

	/* Build the dictionary */
	dict = allocate_cb(dictoptions, &entry->dict_size);

	LWLockAcquire(&tsearch_ctl->lock, LW_SHARED);

	/* Before allocating a DSM segment check remaining shared space */
	Assert(max_shared_dictionaries_size);

	CHECK_SHARED_SPACE();

	LWLockRelease(&tsearch_ctl->lock);
	/* If we come here, we need an exclusive lock */
	while (!LWLockAcquireOrWait(&tsearch_ctl->lock, LW_EXCLUSIVE))
	{
		/*
		 * Check again in case if there are no space anymore while we were
		 * waiting for exclusive lock.
		 */
		CHECK_SHARED_SPACE();
	}

	tsearch_ctl->loaded_size += entry->dict_size;

	LWLockRelease(&tsearch_ctl->lock);

	/* At least, allocate a DSM segment for the compiled dictionary */
	seg = dsm_create(entry->dict_size, 0);
	dict_location = dsm_segment_address(seg);
	memcpy(dict_location, dict, entry->dict_size);

	pfree(dict);

	entry->dict_id = dictid;
	entry->dict_dsm = dsm_segment_handle(seg);
	entry->refcnt++;

	/* Remain attached until end of postmaster */
	dsm_pin_segment(seg);
	/* Remain attached until end of session */
	dsm_pin_mapping(seg);

	dshash_release_lock(dict_table, entry);

	return dsm_segment_address(seg);
}

/*
 * Release memory occupied by the dictionary. Function just unpins DSM mapping.
 * If nobody else hasn't mapping to this DSM then unping DSM segment.
 *
 * dictid: Oid of the dictionary.
 */
void
ts_dict_shmem_release(Oid dictid)
{
	TsearchDictEntry *entry;

	/*
	 * If we didn't attach to a hash table then do nothing.
	 */
	if (!dict_table)
		return;

	/* Try to find an entry in the hash table */
	entry = (TsearchDictEntry *) dshash_find(dict_table, &dictid, true);

	if (entry)
	{
		dsm_segment *seg;

		seg = dsm_find_mapping(entry->dict_dsm);
		/*
		 * If current backend didn't pin a mapping then we don't need to do
		 * unpinning.
		 */
		if (!seg)
		{
			dshash_release_lock(dict_table, entry);
			return;
		}

		dsm_unpin_mapping(seg);
		dsm_detach(seg);

		entry->refcnt--;

		if (entry->refcnt == 0)
		{
			dsm_unpin_segment(entry->dict_dsm);
			dshash_delete_entry(dict_table, entry);
		}
		else
			dshash_release_lock(dict_table, entry);
	}
}

/*
 * Allocate and initialize tsearch-related shared memory.
 */
void
TsearchShmemInit(void)
{
	bool		found;

	tsearch_ctl = (TsearchCtlData *)
		ShmemInitStruct("Full Text Search Ctl", sizeof(TsearchCtlData), &found);

	if (!found)
	{
		LWLockRegisterTranche(LWTRANCHE_TSEARCH_DSA, "tsearch_dsa");
		LWLockRegisterTranche(LWTRANCHE_TSEARCH_TABLE, "tsearch_table");

		LWLockInitialize(&tsearch_ctl->lock, LWTRANCHE_TSEARCH_DSA);

		tsearch_ctl->area = DSM_HANDLE_INVALID;
		tsearch_ctl->dict_table_handle = InvalidDsaPointer;
		tsearch_ctl->loaded_size = 0;
	}
}

/*
 * Report shared memory space needed by TsearchShmemInit.
 */
Size
TsearchShmemSize(void)
{
	Size		size = 0;

	/* size of service structure */
	size = add_size(size, MAXALIGN(sizeof(TsearchCtlData)));

	return size;
}

/*
 * Initialize hash table located in DSM.
 *
 * The hash table should be created and initialized iff
 * max_shared_dictionaries_size GUC is greater than zero and it doesn't exist
 * yet.
 */
static void
init_dict_table(void)
{
	MemoryContext old_context;
	dsa_area   *dsa;

	if (max_shared_dictionaries_size == 0)
		return;

	if (dict_table)
		return;

	old_context = MemoryContextSwitchTo(TopMemoryContext);

recheck_table:
	LWLockAcquire(&tsearch_ctl->lock, LW_SHARED);

	/* Hash table have been created already by someone */
	if (DsaPointerIsValid(tsearch_ctl->dict_table_handle))
	{
		Assert(tsearch_ctl->area != DSM_HANDLE_INVALID);

		dsa = dsa_attach(tsearch_ctl->area);

		dict_table = dshash_attach(dsa,
								   &dict_table_params,
								   tsearch_ctl->dict_table_handle,
								   NULL);
	}
	else
	{
		/* Try to get exclusive lock */
		LWLockRelease(&tsearch_ctl->lock);
		if (!LWLockAcquireOrWait(&tsearch_ctl->lock, LW_EXCLUSIVE))
		{
			/*
			 * The lock was released by another backend and other backend
			 * has concurrently created the hash table already.
			 */
			goto recheck_table;
		}

		dsa = dsa_create(LWTRANCHE_TSEARCH_DSA);
		tsearch_ctl->area = dsa_get_handle(dsa);

		dict_table = dshash_create(dsa, &dict_table_params, NULL);
		tsearch_ctl->dict_table_handle = dshash_get_hash_table_handle(dict_table);

		/* Remain attached until end of postmaster */
		dsa_pin(dsa);
	}

	LWLockRelease(&tsearch_ctl->lock);

	/* Remain attached until end of session */
	dsa_pin_mapping(dsa);

	MemoryContextSwitchTo(old_context);
}

/*
 * pg_ts_shared_dictionaries - SQL SRF showing dictionaries currently in
 * shared memory.
 */
Datum
pg_ts_shared_dictionaries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Relation	rel;
	HeapTuple	tuple;
	SysScanDesc scan;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	init_dict_table();

	/*
	 * If a hash table wasn't created return zero records.
	 */
	if (!DsaPointerIsValid(tsearch_ctl->dict_table_handle))
	{
		tuplestore_donestoring(tupstore);

		PG_RETURN_VOID();
	}

	/* Start to scan pg_ts_dict */
	rel = heap_open(TSDictionaryRelationId, AccessShareLock);
	scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		values[4];
		bool		nulls[4];
		Form_pg_ts_dict dict = (Form_pg_ts_dict) GETSTRUCT(tuple);
		Oid			dictid = HeapTupleGetOid(tuple);
		TsearchDictEntry *entry;
		NameData	dict_name;

		/* If dictionary isn't located in shared memory try following */
		entry = (TsearchDictEntry *) dshash_find(dict_table, &dictid, false);
		if (!entry)
			continue;

		namecpy(&dict_name, &dict->dictname);

		memset(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(dictid);

		if (OidIsValid(dict->dictnamespace))
			values[1] = CStringGetDatum(get_namespace_name(dict->dictnamespace));
		else
			nulls[1] = true;

		values[2] = NameGetDatum(&dict_name);
		values[3] = Int64GetDatum(entry->dict_size);

		dshash_release_lock(dict_table, entry);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}
