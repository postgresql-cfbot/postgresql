/*-------------------------------------------------------------------------
 *
 * ts_shared.c
 *	  tsearch shared memory management
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/ts_shared.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tsearch/ts_shared.h"
#include "utils/hashutils.h"
#include "utils/memutils.h"


/*
 * Hash table entries key.
 */
typedef struct
{
	Oid			db_id;
	DictEntryData dict;
} TsearchDictKey;

/*
 * Hash table entries representing shared dictionaries.
 */
typedef struct
{
	TsearchDictKey key;
	dsm_handle	dict_dsm;

	/*
	 * We need a flag that the DSM segment is pinned/unpinned.  Otherwise we can
	 * face double dsm_unpin_segment().
	 */
	bool		segment_ispinned;
} TsearchDictEntry;

static dshash_table *dict_table = NULL;

/*
 * Information about the main shmem segment, used to coordinate
 * access to the hash table and dictionaries.
 */
typedef struct
{
	dsa_handle	area;
	dshash_table_handle dict_table_handle;

	LWLock		lock;
} TsearchCtlData;

static TsearchCtlData *tsearch_ctl;

static int tsearch_dict_cmp(const void *a, const void *b, size_t size,
							void *arg);
static uint32 tsearch_dict_hash(const void *a, size_t size, void *arg);
static void init_dict_table(void);

/* Parameters for dict_table */
static const dshash_parameters dict_table_params ={
	sizeof(TsearchDictKey),
	sizeof(TsearchDictEntry),
	tsearch_dict_cmp,
	tsearch_dict_hash,
	LWTRANCHE_TSEARCH_TABLE
};

/*
 * Build the dictionary using allocate_cb callback.
 *
 * Firstly try to find the dictionary in shared hash table. If it was built by
 * someone earlier just return its location in DSM.
 *
 * init_data: an argument used within a template's init method.
 * allocate_cb: function to build the dictionary, if it wasn't found in DSM.
 *
 * Returns address in the dynamic shared memory segment or in backend memory.
 */
void *
ts_dict_shmem_location(DictInitData *init_data,
					   ts_dict_build_callback allocate_cb)
{
	TsearchDictKey key;
	TsearchDictEntry *entry;
	bool		found;
	dsm_segment *seg;
	void	   *dict,
			   *dict_location;
	Size		dict_size;

	init_dict_table();

	/*
	 * Build the dictionary in backend's memory if dictid is invalid (it may
	 * happen if the dicionary's init method was called within
	 * verify_dictoptions()).
	 */
	if (!OidIsValid(init_data->dict.id))
	{
		dict = allocate_cb(init_data->dict_options, &dict_size);

		return dict;
	}

	/* Set up key for hashtable search */
	key.db_id = MyDatabaseId;
	key.dict = init_data->dict;

	/* Try to find an entry in the hash table */
	entry = (TsearchDictEntry *) dshash_find(dict_table, &key, false);

	if (entry)
	{
		seg = dsm_find_mapping(entry->dict_dsm);
		if (!seg)
		{
			seg = dsm_attach(entry->dict_dsm);
			/* Remain attached until end of session */
			dsm_pin_mapping(seg);
		}

		dshash_release_lock(dict_table, entry);

		return dsm_segment_address(seg);
	}

	/* Dictionary haven't been loaded into memory yet */
	entry = (TsearchDictEntry *) dshash_find_or_insert(dict_table, &key,
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

		dshash_release_lock(dict_table, entry);

		return dsm_segment_address(seg);
	}

	/* Build the dictionary */
	dict = allocate_cb(init_data->dict_options, &dict_size);

	/* At least, allocate a DSM segment for the compiled dictionary */
	seg = dsm_create(dict_size, 0);
	dict_location = dsm_segment_address(seg);
	memcpy(dict_location, dict, dict_size);

	pfree(dict);

	entry->key = key;
	entry->dict_dsm = dsm_segment_handle(seg);
	entry->segment_ispinned = true;

	/* Remain attached until end of postmaster */
	dsm_pin_segment(seg);
	/* Remain attached until end of session */
	dsm_pin_mapping(seg);

	dshash_release_lock(dict_table, entry);

	return dsm_segment_address(seg);
}

/*
 * Release memory occupied by the dictionary.  Function just unpins DSM mapping.
 * If nobody else hasn't mapping to this DSM or the dictionary was dropped or
 * altered then unpin the DSM segment.
 *
 * The segment still may leak.  It may happen if some backend used the
 * dictionary before dropping, the backend will hold its DSM segment till
 * disconnecting or calling lookup_ts_dictionary_cache().
 *
 * id, xmin, xmax, tid: information to search the dictionary's DSM segment.
 * unpin_segment: true if we need to unpin the segment in case if the dictionary
 *				  was dropped or altered.
 */
void
ts_dict_shmem_release(Oid id, TransactionId xmin, TransactionId xmax,
					  ItemPointerData tid, bool unpin_segment)
{
	TsearchDictKey key;
	TsearchDictEntry *entry;

	/*
	 * If we didn't attach to a hash table then do nothing.
	 */
	if (!dict_table && !unpin_segment)
		return;
	/*
	 * But if we need to unpin the DSM segment to get of rid of the segment when
	 * the last interested process disconnects we need the hash table to find
	 * the dictionary's entry.
	 */
	else if (unpin_segment)
		init_dict_table();

	/* Set up key for hashtable search */
	key.db_id = MyDatabaseId;
	key.dict.id = id;
	key.dict.xmin = xmin;
	key.dict.xmax = xmax;
	key.dict.tid = tid;

	/* Try to find an entry in the hash table */
	entry = (TsearchDictEntry *) dshash_find(dict_table, &key, true);

	if (entry)
	{
		dsm_segment *seg;

		seg = dsm_find_mapping(entry->dict_dsm);

		if (seg)
		{
			dsm_unpin_mapping(seg);
			dsm_detach(seg);
		}

		if (unpin_segment && entry->segment_ispinned)
		{
			dsm_unpin_segment(entry->dict_dsm);
			entry->segment_ispinned = false;
		}

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
 * A comparator function for TsearchDictKey.
 *
 * Returns 1 if keys are equal.
 */
static int
tsearch_dict_cmp(const void *a, const void *b, size_t size, void *arg)
{
	TsearchDictKey *k1 = (TsearchDictKey *) a;
	TsearchDictKey *k2 = (TsearchDictKey *) b;

	if (k1->db_id == k2->db_id && k1->dict.id == k2->dict.id &&
		k1->dict.xmin == k2->dict.xmin && k1->dict.xmax == k2->dict.xmax &&
		ItemPointerEquals(&k1->dict.tid, &k2->dict.tid))
		return 0;
	else
		return 1;
}

/*
 * A hash function for TsearchDictKey.
 */
static uint32
tsearch_dict_hash(const void *a, size_t size, void *arg)
{
	TsearchDictKey *k = (TsearchDictKey *) a;
	uint32		s;

	s = hash_combine(0, hash_uint32(k->db_id));
	s = hash_combine(s, hash_uint32(k->dict.id));
	s = hash_combine(s, hash_uint32(k->dict.xmin));
	s = hash_combine(s, hash_uint32(k->dict.xmax));
	s = hash_combine(s,
					 hash_uint32(BlockIdGetBlockNumber(&k->dict.tid.ip_blkid)));
	s = hash_combine(s, hash_uint32(k->dict.tid.ip_posid));

	return s;
}

/*
 * Initialize hash table located in DSM.
 *
 * The hash table should be created and initialized if it doesn't exist yet.
 */
static void
init_dict_table(void)
{
	MemoryContext old_context;
	dsa_area   *dsa;

	/* Exit if hash table was initialized alread */
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
