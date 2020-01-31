/*-------------------------------------------------------------------------
 *
 * storage_gtt.c
 *	  code to create and destroy physical storage for global temparary table
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_gtt.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/storage_gtt.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_statistic.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "nodes/execnodes.h"
#include "miscadmin.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include <utils/relcache.h>
#include "utils/inval.h"
#include "utils/guc.h"


/* Copy from bitmapset.c, because gtt used the function in bitmapset.c */
#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords)	\
	(offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

static bool gtt_cleaner_exit_registered = false;
static HTAB *gtt_storage_local_hash = NULL;
static HTAB *active_gtt_shared_hash = NULL;
static MemoryContext gtt_relstats_context = NULL;

/* relfrozenxid of all gtts in the current session */
static List *gtt_session_relfrozenxid_list = NIL;
static TransactionId gtt_session_frozenxid = InvalidTransactionId;

typedef struct gtt_ctl_data
{
	LWLock			lock;
	int			max_entry;
	int			entry_size;
}gtt_ctl_data;

static gtt_ctl_data *gtt_shared_ctl = NULL;

typedef struct
{
	RelFileNode	rnode;
	Bitmapset	*map;
	/* bitmap data */
} gtt_shared_hash_entry;

typedef struct
{
	Oid			relid;

	Oid			spcnode;
	/* pg_class stat */
	int32		relpages;
	float4		reltuples;
	int32		relallvisible;
	TransactionId relfrozenxid;
	TransactionId relminmxid;
	char		relkind;
	bool		on_commit_delete;

	/* pg_statistic */
	int			natts;
	int			*attnum;
	HeapTuple	*att_stat_tups;
} gtt_local_hash_entry;

static Size action_gtt_shared_hash_entry_size(void);
static void gtt_storage_checkin(RelFileNode rnode);
static void gtt_storage_checkout(RelFileNode rnode, bool skiplock);
static void gtt_storage_removeall(int code, Datum arg);
static void insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid);
static void remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid);
static void set_gtt_session_relfrozenxid(void);

Datum pg_get_gtt_statistics(PG_FUNCTION_ARGS);
Datum pg_get_gtt_relstats(PG_FUNCTION_ARGS);
Datum pg_gtt_attached_pid(PG_FUNCTION_ARGS);
Datum pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS);


static Size
action_gtt_shared_hash_entry_size(void)
{
	int 	wordnum;
	Size	hash_entry_size = 0;

	if (max_active_gtt <= 0)
		return 0;

	wordnum = WORDNUM(MaxBackends + 1);
	hash_entry_size += MAXALIGN(sizeof(gtt_shared_hash_entry));
	hash_entry_size += MAXALIGN(BITMAPSET_SIZE(wordnum + 1));

	return hash_entry_size;
}

Size
active_gtt_shared_hash_size(void)
{
	Size	size = 0;
	Size	hash_entry_size = 0;

	if (max_active_gtt <= 0)
		return 0;

	size = MAXALIGN(sizeof(gtt_ctl_data));
	hash_entry_size = action_gtt_shared_hash_entry_size();
	size += hash_estimate_size(max_active_gtt, hash_entry_size);

	return size;
}

void
active_gtt_shared_hash_init(void)
{
	HASHCTL info;
	bool	found;

	if (max_active_gtt <= 0)
		return;

	gtt_shared_ctl =
		ShmemInitStruct("gtt_shared_ctl",
						sizeof(gtt_ctl_data),
						&found);

	if (!found)
	{
		LWLockRegisterTranche(LWTRANCHE_GTT_CTL, "gtt_shared_ctl");
		LWLockInitialize(&gtt_shared_ctl->lock, LWTRANCHE_GTT_CTL);
		gtt_shared_ctl->max_entry = max_active_gtt;
		gtt_shared_ctl->entry_size = action_gtt_shared_hash_entry_size();
	}

	info.keysize = sizeof(RelFileNode);
	info.entrysize = gtt_shared_ctl->entry_size;
	active_gtt_shared_hash =
		ShmemInitHash("active gtt shared hash",
						gtt_shared_ctl->max_entry,
						gtt_shared_ctl->max_entry,
						&info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
}

static void
gtt_storage_checkin(RelFileNode rnode)
{
	gtt_shared_hash_entry	*entry;
	bool			found;

	if (max_active_gtt <= 0)
		return;

	LWLockAcquire(&gtt_shared_ctl->lock, LW_EXCLUSIVE);
	entry = (gtt_shared_hash_entry *) hash_search(active_gtt_shared_hash,
												&rnode, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory"),
				 errhint("You might need to increase max_active_gtt.")));
	}

	if (found == false)
	{
		int			wordnum;

		entry->map = (Bitmapset *)((char *)entry + MAXALIGN(sizeof(gtt_shared_hash_entry)));
		wordnum = WORDNUM(MaxBackends + 1);
		memset(entry->map, 0, BITMAPSET_SIZE(wordnum + 1));
		entry->map->nwords = wordnum + 1;
	}

	bms_add_member(entry->map, MyBackendId);
	LWLockRelease(&gtt_shared_ctl->lock);
}

static void
gtt_storage_checkout(RelFileNode rnode, bool skiplock)
{
	gtt_shared_hash_entry	*entry;

	if (max_active_gtt <= 0)
		return;

	if (!skiplock)
		LWLockAcquire(&gtt_shared_ctl->lock, LW_EXCLUSIVE);

	entry = hash_search(active_gtt_shared_hash,
					(void *) &(rnode), HASH_FIND, NULL);

	if (entry == NULL)
	{
		if (!skiplock)
			LWLockRelease(&gtt_shared_ctl->lock);

		elog(WARNING, "relfilenode %u/%u/%u not exist in gtt shared hash when forget",
						rnode.dbNode, rnode.spcNode, rnode.relNode);
		return;
	}

	Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
	bms_del_member(entry->map, MyBackendId);

	if (bms_is_empty(entry->map))
	{
		if (!hash_search(active_gtt_shared_hash, &rnode, HASH_REMOVE, NULL))
			elog(PANIC, "gtt shared hash table corrupted");
	}

	if (!skiplock)
		LWLockRelease(&gtt_shared_ctl->lock);

	return;
}

Bitmapset *
copy_active_gtt_bitmap(RelFileNode node)
{
	gtt_shared_hash_entry	*entry;
	Bitmapset	*map_copy = NULL;

	if (max_active_gtt <= 0)
		return NULL;

	LWLockAcquire(&gtt_shared_ctl->lock, LW_SHARED);
	entry = hash_search(active_gtt_shared_hash,
					(void *) &(node), HASH_FIND, NULL);

	if (entry == NULL)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		return NULL;
	}

	Assert(entry->map);
	if (!bms_is_empty(entry->map))
		map_copy = bms_copy(entry->map);

	LWLockRelease(&gtt_shared_ctl->lock);

	return map_copy;
}

bool
is_other_backend_use_gtt(RelFileNode node)
{
	gtt_shared_hash_entry	*entry;
	bool		in_use = false;
	int			num_use = 0;

	if (max_active_gtt <= 0)
		return false;

	LWLockAcquire(&gtt_shared_ctl->lock, LW_SHARED);
	entry = hash_search(active_gtt_shared_hash,
					(void *) &(node), HASH_FIND, NULL);

	if (entry == NULL)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		return false;
	}

	Assert(entry->map);
	Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);

	num_use = bms_num_members(entry->map);
	if (num_use == 0)
		in_use = false;
	else if (num_use == 1)
	{
		if(bms_is_member(MyBackendId, entry->map))
			in_use = false;
		else
			in_use = true;
	}
	else
		in_use = true;

	LWLockRelease(&gtt_shared_ctl->lock);

	return in_use;
}

void
remember_gtt_storage_info(RelFileNode rnode, Relation rel)
{
	gtt_local_hash_entry	*entry;
	MemoryContext			oldcontext;
	Oid			relid = rnode.relNode;
	bool		found;
	int			natts = 0;

	if (max_active_gtt <= 0)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Global temporary table feature is disable"),
			 errhint("You might need to increase max_active_gtt to enable this feature.")));

	if (RecoveryInProgress())
		elog(ERROR, "readonly mode not support access global temp table yet");

	if (rel->rd_rel->relkind == RELKIND_INDEX &&
		(!rel->rd_index->indisvalid ||
		 !rel->rd_index->indisready ||
		 !rel->rd_index->indislive))
		 elog(ERROR, "invalid gtt index %s not allow to create stroage file", RelationGetRelationName(rel));

	if (gtt_storage_local_hash == NULL)
	{
#define GTT_LOCAL_HASH_SIZE		1024
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(gtt_local_hash_entry);
		gtt_storage_local_hash =
			hash_create("global temp relation table",
						GTT_LOCAL_HASH_SIZE,
						&ctl, HASH_ELEM | HASH_BLOBS);

		if (!CacheMemoryContext)
			CreateCacheMemoryContext();

		gtt_relstats_context =
			AllocSetContextCreate(CacheMemoryContext,
							"gtt relstats context",
							ALLOCSET_DEFAULT_SIZES);
	}

	/* Look up or create an entry */
	entry = hash_search(gtt_storage_local_hash,
					(void *) &relid, HASH_ENTER, &found);

	if (found)
	{
		elog(ERROR, "backend %d relid %u already exists in global temp table local hash",
					MyBackendId, relid);
	}

	oldcontext = MemoryContextSwitchTo(gtt_relstats_context);

	entry->spcnode = rnode.spcNode;
	entry->relpages = 0;
	entry->reltuples = 0;
	entry->relallvisible = 0;
	entry->relkind = rel->rd_rel->relkind;
	natts = RelationGetNumberOfAttributes(rel);
	entry->natts = natts;
	entry->attnum = palloc0(sizeof(int) * natts);
	entry->att_stat_tups = palloc0(sizeof(HeapTuple) * natts);
	/* only heap contain transaction information */
	if (entry->relkind == RELKIND_RELATION ||
		entry->relkind == RELKIND_PARTITIONED_TABLE)
	{
		if (RELATION_GTT_ON_COMMIT_DELETE(rel))
		{
			entry->on_commit_delete = true;
			register_on_commit_action(rel->rd_node.relNode, ONCOMMIT_DELETE_ROWS);
		}

		entry->relfrozenxid = RecentXmin;
		entry->relminmxid = GetOldestMultiXactId();
		insert_gtt_relfrozenxid_to_ordered_list(entry->relfrozenxid);
		set_gtt_session_relfrozenxid();
		gtt_storage_checkin(rnode);
	}
	else
	{
		entry->on_commit_delete = false;
		entry->relfrozenxid = 0;
		entry->relminmxid = 0;
	}

	MemoryContextSwitchTo(oldcontext);

	if (!gtt_cleaner_exit_registered)
	{
		before_shmem_exit(gtt_storage_removeall, 0);
		gtt_cleaner_exit_registered = true;
	}

	return;
}

void
forget_gtt_storage_info(Oid relid)
{
	gtt_local_hash_entry	*entry = NULL;

	if (max_active_gtt <= 0)
		return;

	if (gtt_storage_local_hash == NULL)
		return;

	entry = hash_search(gtt_storage_local_hash,
				(void *) &(relid), HASH_FIND, NULL);

	if (entry)
	{
		int		i;

		if (entry->relkind == RELKIND_RELATION ||
			entry->relkind == RELKIND_PARTITIONED_TABLE)
		{
			RelFileNode rnode;

			rnode.spcNode = entry->spcnode;
			rnode.dbNode = MyDatabaseId;
			rnode.relNode = entry->relid;

			gtt_storage_checkout(rnode, false);

			Assert(TransactionIdIsNormal(entry->relfrozenxid));
			remove_gtt_relfrozenxid_from_ordered_list(entry->relfrozenxid);
			set_gtt_session_relfrozenxid();
		}

		for (i = 0; i < entry->natts; i++)
		{
			if (entry->att_stat_tups[i])
			{
				heap_freetuple(entry->att_stat_tups[i]);
				entry->att_stat_tups[i] = NULL;
			}
		}

		pfree(entry->attnum);
		pfree(entry->att_stat_tups);
	}

	hash_search(gtt_storage_local_hash,
			(void *) &(relid), HASH_REMOVE, NULL);

	return;
}

/* is the storage file was created in this backend */
bool
gtt_storage_attached(Oid relid)
{
	bool found;

	if (max_active_gtt <= 0)
		return false;

	if (gtt_storage_local_hash == NULL)
		return false;

	hash_search(gtt_storage_local_hash,
			(void *) &(relid), HASH_FIND, &found);

	return found;
}

static void
gtt_storage_removeall(int code, Datum arg)
{
	HASH_SEQ_STATUS				status;
	gtt_local_hash_entry		*entry;
	int			nrels = 0,
				maxrels = 0;
	SMgrRelation	*srels = NULL;
	RelFileNode		*rnodes = NULL;
	char			*relkinds = NULL;

	if (gtt_storage_local_hash == NULL)
		return;

	hash_seq_init(&status, gtt_storage_local_hash);
	while ((entry = (gtt_local_hash_entry *) hash_seq_search(&status)) != NULL)
	{
		SMgrRelation srel;
		RelFileNode rnode;

		rnode.spcNode = entry->spcnode;
		rnode.dbNode = MyDatabaseId;
		rnode.relNode = entry->relid;

		srel = smgropen(rnode, MyBackendId);

		/* allocate the initial array, or extend it, if needed */
		if (maxrels == 0)
		{
			maxrels = 32;
			srels = palloc(sizeof(SMgrRelation) * maxrels);
			rnodes = palloc(sizeof(RelFileNode) * maxrels);
			relkinds = palloc(sizeof(char) * maxrels);
		}
		else if (maxrels <= nrels)
		{
			maxrels *= 2;
			srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
			rnodes = repalloc(rnodes, sizeof(RelFileNode) * maxrels);
			relkinds = repalloc(relkinds, sizeof(char) * maxrels);
		}

		srels[nrels] = srel;
		rnodes[nrels] = rnode;
		relkinds[nrels] = entry->relkind;
		nrels++;
	}

	if (nrels > 0)
	{
		int i;

		smgrdounlinkall(srels, nrels, false);

		LWLockAcquire(&gtt_shared_ctl->lock, LW_EXCLUSIVE);
		for (i = 0; i < nrels; i++)
		{
			smgrclose(srels[i]);
			if (relkinds[i] == RELKIND_RELATION)
				gtt_storage_checkout(rnodes[i], true);
		}
		LWLockRelease(&gtt_shared_ctl->lock);

		pfree(srels);
		pfree(rnodes);
		pfree(relkinds);
	}

	MyProc->session_gtt_frozenxid = InvalidTransactionId;

	return;
}

/*
 * Update global temp table relstats(relpage/reltuple/relallvisible) 
 * to local hashtable
 */
void
up_gtt_relstats(Relation relation,
					BlockNumber num_pages,
					double num_tuples,
					BlockNumber num_all_visible_pages,
					TransactionId relfrozenxid,
					TransactionId relminmxid)
{
	Oid		relid = RelationGetRelid(relation);
	gtt_local_hash_entry	*entry;
	bool					found;

	if (max_active_gtt <= 0)
		return;

	if (gtt_storage_local_hash == NULL)
		return;

	entry = hash_search(gtt_storage_local_hash,
					(void *) &relid, HASH_FIND, &found);

	if (!found)
		return;

	Assert(entry->spcnode);

	if (num_pages >= 0 &&
		entry->relpages != (int32)num_pages)
		entry->relpages = (int32)num_pages;

	if (num_tuples >= 0 &&
		num_tuples != (float4)entry->reltuples)
		entry->reltuples = (float4)num_tuples;

	/* only heap contain transaction information and relallvisible */
	if (entry->relkind == RELKIND_RELATION)
	{
		if (entry->relallvisible >= 0 &&
			entry->relallvisible != (int32)num_all_visible_pages)
		{
			entry->relallvisible = (int32)num_all_visible_pages;
		}

		if (TransactionIdIsNormal(relfrozenxid) &&
			entry->relfrozenxid != relfrozenxid &&
			(TransactionIdPrecedes(entry->relfrozenxid, relfrozenxid) ||
			 TransactionIdPrecedes(ReadNewTransactionId(), entry->relfrozenxid)))
		{
			remove_gtt_relfrozenxid_from_ordered_list(entry->relfrozenxid);
			entry->relfrozenxid = relfrozenxid;
			insert_gtt_relfrozenxid_to_ordered_list(relfrozenxid);
			set_gtt_session_relfrozenxid();
		}

		if (MultiXactIdIsValid(relminmxid) &&
			entry->relminmxid != relminmxid &&
			(MultiXactIdPrecedes(entry->relminmxid, relminmxid) ||
			 MultiXactIdPrecedes(ReadNextMultiXactId(), entry->relminmxid)))
		{
			entry->relminmxid = relminmxid;
		}
	}

	return;
}

/*
 * Search global temp table relstats(relpage/reltuple/relallvisible) 
 * from local hashtable.
 */
bool
get_gtt_relstats(Oid relid, BlockNumber *relpages, double *reltuples,
				BlockNumber *relallvisible, TransactionId *relfrozenxid,
				TransactionId *relminmxid)
{
	gtt_local_hash_entry	*entry;
	bool					found;

	if (max_active_gtt <= 0)
		return false;

	if (gtt_storage_local_hash == NULL)
		return false;

	entry = hash_search(gtt_storage_local_hash,
					(void *) &relid, HASH_FIND, &found);

	if (!found)
		return false;

	Assert(entry->relid == relid);

	if (relpages)
		*relpages = entry->relpages;

	if (reltuples)
		*reltuples = entry->reltuples;

	if (relallvisible)
		*relallvisible = entry->relallvisible;

	if (relfrozenxid)
		*relfrozenxid = entry->relfrozenxid;

	if (relminmxid)
		*relminmxid = entry->relminmxid;

	return true;
}

/*
 * Update global temp table statistic info(definition is same as pg_statistic)
 * to local hashtable where ananyze global temp table
 */
void
up_gtt_att_statistic(Oid reloid, int attnum, bool inh, int natts,
					TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
	gtt_local_hash_entry	*entry;
	bool					found;
	MemoryContext			oldcontext;
	int						i = 0;

	if (max_active_gtt <= 0)
		return;

	if (gtt_storage_local_hash == NULL)
		return;

	entry = hash_search(gtt_storage_local_hash,
						(void *) &reloid, HASH_FIND, &found);

	if (!found)
		return;

	/* todo */
	if (entry->natts < natts)
	{
		elog(WARNING, "reloid %u not support update attstat after add colunm", reloid);
		return;
	}

	oldcontext = MemoryContextSwitchTo(gtt_relstats_context);
	Assert(entry->relid == reloid);
	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] == 0)
		{
			entry->attnum[i] = attnum;
			break;
		}
		else if (entry->attnum[i] == attnum)
		{
			Assert(entry->att_stat_tups[i]);
			heap_freetuple(entry->att_stat_tups[i]);
			entry->att_stat_tups[i] = NULL;
			break;
		}
	}

	Assert(i < entry->natts);
	Assert(entry->att_stat_tups[i] == NULL);
	entry->att_stat_tups[i] = heap_form_tuple(tupleDescriptor, values, isnull);
	MemoryContextSwitchTo(oldcontext);

	return;
}

/*
 * Search global temp table statistic info(definition is same as pg_statistic)
 * from local hashtable.
 */
HeapTuple
get_gtt_att_statistic(Oid reloid, int attnum, bool inh)
{
	gtt_local_hash_entry	*entry;
	bool					found;
	int						i = 0;

	if (max_active_gtt <= 0)
		return NULL;

	if (gtt_storage_local_hash == NULL)
		return NULL;

	entry = hash_search(gtt_storage_local_hash,
						(void *) &reloid, HASH_FIND, &found);

	if (!found)
		return NULL;

	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] == attnum)
		{
			Assert(entry->att_stat_tups[i]);
			return entry->att_stat_tups[i];
		}
	}

	return NULL;
}

void
release_gtt_statistic_cache(HeapTuple tup)
{
	/* do nothing */
	return;
}

static void
insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid)
{
	MemoryContext	oldcontext;
	ListCell	*cell;
	int			i;

	Assert(TransactionIdIsNormal(relfrozenxid));

	oldcontext = MemoryContextSwitchTo(gtt_relstats_context);
	/* Does the datum belong at the front? */
	if (gtt_session_relfrozenxid_list == NIL ||
		TransactionIdFollowsOrEquals(relfrozenxid,
			linitial_oid(gtt_session_relfrozenxid_list)))
	{
		gtt_session_relfrozenxid_list =
			lcons_oid(relfrozenxid, gtt_session_relfrozenxid_list);
		MemoryContextSwitchTo(oldcontext);

		return;
	}

	/* No, so find the entry it belongs after */
	i = 0;
	foreach (cell, gtt_session_relfrozenxid_list)
	{
		if (TransactionIdFollowsOrEquals(relfrozenxid, lfirst_oid(cell)))
			break;

		i++;
	}
	gtt_session_relfrozenxid_list =
		list_insert_nth_oid(gtt_session_relfrozenxid_list, i, relfrozenxid);
	MemoryContextSwitchTo(oldcontext);

	return;
}

static void
remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid)
{
	gtt_session_relfrozenxid_list =
		list_delete_oid(gtt_session_relfrozenxid_list, relfrozenxid);
}

static void
set_gtt_session_relfrozenxid(void)
{
	TransactionId gtt_frozenxid = InvalidTransactionId;

	if (gtt_session_relfrozenxid_list)
		gtt_frozenxid = llast_oid(gtt_session_relfrozenxid_list);

	gtt_session_frozenxid = gtt_frozenxid;
	if (MyProc->session_gtt_frozenxid != gtt_frozenxid)
		MyProc->session_gtt_frozenxid = gtt_frozenxid;
}

Datum
pg_get_gtt_statistics(PG_FUNCTION_ARGS)
{
	HeapTuple	tuple;
	Relation	rel = NULL;
	int		attnum = PG_GETARG_INT32(1);
	Oid		reloid = PG_GETARG_OID(0);
	char		rel_persistence;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	  tupdesc;
	MemoryContext oldcontext;
	Tuplestorestate *tupstore;
	Relation		pg_tatistic = NULL;
	TupleDesc		sd;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("materialize mode required, but it is not " \
				"allowed in this context")));

	oldcontext = MemoryContextSwitchTo(
				rsinfo->econtext->ecxt_per_query_memory);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	rel = table_open(reloid, AccessShareLock);
	rel_persistence = get_rel_persistence(reloid);
	if (rel_persistence != RELPERSISTENCE_GLOBAL_TEMP)
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		table_close(rel, NoLock);
		return (Datum) 0;
	}

	pg_tatistic = table_open(StatisticRelationId, AccessShareLock);
	sd = RelationGetDescr(pg_tatistic);

	tuple = get_gtt_att_statistic(reloid, attnum, false);
	if (tuple)
	{
		Datum	values[31];
		bool	isnull[31];
		HeapTuple	res = NULL;

		memset(&values, 0, sizeof(values));
		memset(&isnull, 0, sizeof(isnull));
		heap_deform_tuple(tuple, sd, values, isnull);
		res = heap_form_tuple(tupdesc, values, isnull);
		tuplestore_puttuple(tupstore, res);
	}

	table_close(rel, NoLock);
	table_close(pg_tatistic, AccessShareLock);
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
pg_get_gtt_relstats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	MemoryContext	oldcontext;
	HeapTuple	tuple;
	Oid		reloid = PG_GETARG_OID(0);
	char	rel_persistence;
	BlockNumber	relpages = 0;
	double		reltuples = 0;
	BlockNumber	relallvisible = 0;
	uint32		relfrozenxid = 0;
	uint32		relminmxid = 0;
	Relation	rel = NULL;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	oldcontext = MemoryContextSwitchTo(
			rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	rel = table_open(reloid, AccessShareLock);
	rel_persistence = get_rel_persistence(reloid);
	if (rel_persistence != RELPERSISTENCE_GLOBAL_TEMP)
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		table_close(rel, NoLock);
		return (Datum) 0;
	}

	if (get_gtt_relstats(reloid,
						&relpages, &reltuples, &relallvisible,
						&relfrozenxid, &relminmxid))
	{
		Datum	values[5];
		bool	isnull[5];

		memset(isnull, 0, sizeof(isnull));
		memset(values, 0, sizeof(values));
		values[0] = Int32GetDatum(relpages);
		values[1] = Float4GetDatum((float4)reltuples);
		values[2] = Int32GetDatum(relallvisible);
		values[3] = UInt32GetDatum(relfrozenxid);
		values[4] = UInt32GetDatum(relminmxid);
		tuple = heap_form_tuple(tupdesc, values, isnull);
		tuplestore_puttuple(tupstore, tuple);
	}

	tuplestore_donestoring(tupstore);
	table_close(rel, NoLock);

	return (Datum) 0;
}

Datum
pg_gtt_attached_pid(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	oldcontext;
	HeapTuple	tuple;
	Oid		reloid = PG_GETARG_OID(0);
	char	rel_persistence;
	Relation	rel = NULL;
	PGPROC		*proc = NULL;
	Bitmapset	*map = NULL;
	pid_t		pid = 0;
	int			backendid = 0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	oldcontext = MemoryContextSwitchTo(
			rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	rel = table_open(reloid, AccessShareLock);
	rel_persistence = get_rel_persistence(reloid);
	if (rel_persistence != RELPERSISTENCE_GLOBAL_TEMP)
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		table_close(rel, NoLock);
		return (Datum) 0;
	}

	map = copy_active_gtt_bitmap(rel->rd_node);
	if (map)
	{
		backendid = bms_first_member(map);

		do
		{
			proc = BackendIdGetProc(backendid);
			pid = proc->pid;
			if (pid > 0)
			{
				Datum	values[2];
				bool	isnull[2];

				memset(isnull, false, sizeof(isnull));
				memset(values, 0, sizeof(values));
				values[0] = UInt32GetDatum(reloid);
				values[1] = Int32GetDatum(pid);
				tuple = heap_form_tuple(tupdesc, values, isnull);
				tuplestore_puttuple(tupstore, tuple);
			}
			backendid = bms_next_member(map, backendid);
		} while (backendid > 0);

		pfree(map);
	}

	tuplestore_donestoring(tupstore);
	table_close(rel, NoLock);

	return (Datum) 0;
}

Datum
pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	oldcontext;
	HeapTuple	tuple;
	int			num_xid = MaxBackends + 1;
	int			*pids = NULL;
	uint32		*xids = NULL;
	int			i = 0;
	int			j = 0;
	uint32		oldest = 0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	oldcontext = MemoryContextSwitchTo(
			rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	if (max_active_gtt <= 0)
		return (Datum) 0;

	if (RecoveryInProgress())
		return (Datum) 0;

	pids = palloc0(sizeof(int) * num_xid);
	xids = palloc0(sizeof(int) * num_xid);
	oldest = list_all_session_gtt_frozenxids(num_xid, pids, xids, &i);
	if (i > 0)
	{
		if (i > 0)
		{
			pids[i] = 0;
			xids[i] = oldest;
			i++;
		}

		for(j = 0; j < i; j++)
		{
			Datum	values[2];
			bool	isnull[2];

			memset(isnull, false, sizeof(isnull));
			memset(values, 0, sizeof(values));
			values[0] = Int32GetDatum(pids[j]);
			values[1] = UInt32GetDatum(xids[j]);
			tuple = heap_form_tuple(tupdesc, values, isnull);
			tuplestore_puttuple(tupstore, tuple);
		}
	}
	tuplestore_donestoring(tupstore);
	pfree(pids);
	pfree(xids);

	return (Datum) 0;
}

void
gtt_force_enable_index(Relation index)
{
	Oid indexOid = RelationGetRelid(index);

	if (!RELATION_IS_GLOBAL_TEMP(index))
		return;

	Assert(index->rd_rel->relkind == RELKIND_INDEX);
	Assert(OidIsValid(indexOid));

	index->rd_index->indisvalid = true;
	index->rd_index->indislive = true;
	index->rd_index->indisready = true;
}

void
gtt_fix_index_state(Relation index)
{
	Oid indexOid = RelationGetRelid(index);
	Oid relOid = index->rd_index->indrelid;

	if (!RELATION_IS_GLOBAL_TEMP(index))
		return;

	if (!index->rd_index->indisvalid)
		return;

	if (gtt_storage_attached(relOid) &&
		!gtt_storage_attached(indexOid))
	{
		index->rd_index->indisvalid = false;
		index->rd_index->indislive = false;
		index->rd_index->indisready = false;
	}

	return;
}

