/*-------------------------------------------------------------------------
 *
 * storage_gtt.c
 *	  The body implementation of Global temparary table.
 *
 * IDENTIFICATION
 *	  src/backend/catalog/storage_gtt.c
 *
 *	  See src/backend/catalog/GTT_README for Global temparary table's
 *	  requirements and design.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_statistic.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/syscache.h"

/*
 * WORDNUM/BITNUM/BITMAPSET_SIZE copy from bitmapset.c, and use BITS_PER_BITMAPWORD
 * and typedef bitmapword from nodes/bitmapset.h. GTT records the status of global
 * temporary tables in each session using bitmaps, which are stored in shared memory.
 */
#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)
#define BITMAPSET_SIZE(nwords)	\
	(offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

static bool gtt_cleaner_exit_registered = false;
static HTAB *gtt_storage_local_hash = NULL;
static HTAB *active_gtt_shared_hash = NULL;
static MemoryContext gtt_info_context = NULL;

/* relfrozenxid of all gtts in the current session */
static List *gtt_session_relfrozenxid_list = NIL;

int	vacuum_gtt_defer_check_age = 0;

/*
 * Global temp table or senquence has separate state in each session,
 * record in a shared memory bitmap.
 * However, index and toast relation for global temp table is not,
 * because they depend on tables and don't exist independently.
 */
#define GLOBAL_TEMP_RELKIND_STATE_IS_SHARED(relkind) \
	((relkind) == RELKIND_RELATION || \
	 (relkind) == RELKIND_SEQUENCE)

/*
 * The Global temporary table's shared hash table data structure
 */
typedef struct gtt_ctl_data
{
	LWLock		lock;
	int			max_entry;
	int			entry_size;
}gtt_ctl_data;

static gtt_ctl_data *gtt_shared_ctl = NULL;

typedef struct gtt_fnode
{
	Oid			dbNode;
	Oid			relNode;
} gtt_fnode;

/* record this global temporary table in which backends are being used */
typedef struct
{
	gtt_fnode	rnode;
	Bitmapset	*map;
	/* bitmap data */
} gtt_shared_hash_entry;

/*
 * The Global temporary table's local hash table data structure
 */
/* Record the storage information and statistical information of the global temporary table */
typedef struct
{
	Oid			relfilenode;	/* relation */
	Oid			spcnode;		/* tablespace */

	/* pg_class relation statistics */
	int32		relpages;
	float4		reltuples;
	int32		relallvisible;
	TransactionId relfrozenxid;
	TransactionId relminmxid;

	/* pg_statistic column statistics */
	int			natts;
	int			*attnum;
	HeapTuple	*att_stat_tups;
} gtt_relfilenode;

typedef struct
{
	Oid			relid;

	List		*relfilenode_list;

	char		relkind;
	bool		on_commit_delete;
} gtt_local_hash_entry;

static Size action_gtt_shared_hash_entry_size(void);
static void gtt_storage_checkin(Oid relid, char relkind);
static void gtt_storage_checkout(Oid relid, bool isCommit, char relkind);
static void gtt_storage_removeall(int code, Datum arg);
static void insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid);
static void remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid);
static void set_gtt_session_relfrozenxid(void);
static void gtt_free_statistics(gtt_relfilenode *rnode);
static gtt_relfilenode *gtt_search_relfilenode(gtt_local_hash_entry	*entry, Oid relfilenode, Oid spcnode, bool missing_ok);
static gtt_local_hash_entry *gtt_search_by_relid(Oid relid, bool missing_ok);
static Bitmapset *copy_active_gtt_bitmap(Oid relid);

Datum pg_get_gtt_statistics(PG_FUNCTION_ARGS);
Datum pg_get_gtt_relstats(PG_FUNCTION_ARGS);
Datum pg_gtt_attached_pid(PG_FUNCTION_ARGS);
Datum pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS);

/*
 * Calculate shared hash table entry size for GTT.
 */
static Size
action_gtt_shared_hash_entry_size(void)
{
	int	wordnum;
	Size	hash_entry_size = 0;

	if (max_active_gtt <= 0)
		return 0;

	wordnum = WORDNUM(GetMaxBackends() + 1);

	/* hash entry header size */
	hash_entry_size = MAXALIGN(sizeof(gtt_shared_hash_entry));

	/*
	 * hash entry data size
	 * this is a bitmap in shared memory, each backend have a bit.
	 * ensure we have enough words to store the upper bit.
	 */
	hash_entry_size += MAXALIGN(BITMAPSET_SIZE(wordnum + 1));

	return hash_entry_size;
}

/*
 * Calculate shared hash table max size for GTT.
 */
Size
active_gtt_shared_hash_size(void)
{
	Size	size = 0;
	Size	hash_entry_size = 0;

	if (max_active_gtt <= 0)
		return 0;

	/* shared hash header size */
	size = MAXALIGN(sizeof(gtt_ctl_data));
	/* hash entry size */
	hash_entry_size = action_gtt_shared_hash_entry_size();
	/* max size */
	size += hash_estimate_size(max_active_gtt, hash_entry_size);

	return size;
}

/*
 * Initialization shared hash table for GTT.
 */
void
active_gtt_shared_hash_init(void)
{
	HASHCTL info;
	bool	found;

	if (max_active_gtt <= 0)
		return;

	gtt_shared_ctl = ShmemInitStruct("gtt_shared_ctl",
									 sizeof(gtt_ctl_data),
									 &found);

	if (!found)
	{
		LWLockRegisterTranche(LWTRANCHE_GLOBAL_TEMP_TABLE_CTL, "gtt_shared_ctl");
		LWLockInitialize(&gtt_shared_ctl->lock, LWTRANCHE_GLOBAL_TEMP_TABLE_CTL);
		gtt_shared_ctl->max_entry = max_active_gtt;
		gtt_shared_ctl->entry_size = action_gtt_shared_hash_entry_size();
	}

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(gtt_fnode);
	info.entrysize = action_gtt_shared_hash_entry_size();
	active_gtt_shared_hash = ShmemInitHash("active gtt shared hash",
										   gtt_shared_ctl->max_entry,
										   gtt_shared_ctl->max_entry,
										   &info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
}

/*
 * Record GTT relid to shared hash table, which means that current session is using this GTT.
 */
static void
gtt_storage_checkin(Oid relid, char relkind)
{
	gtt_shared_hash_entry	*entry;
	bool			found;
	gtt_fnode		fnode;

	if (max_active_gtt <= 0)
		return;

	if (!GLOBAL_TEMP_RELKIND_STATE_IS_SHARED(relkind))
		return;

	fnode.dbNode = MyDatabaseId;
	fnode.relNode = relid;
	LWLockAcquire(&gtt_shared_ctl->lock, LW_EXCLUSIVE);
	entry = hash_search(active_gtt_shared_hash, (void *)&(fnode), HASH_ENTER_NULL, &found);
	if (!entry)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory"),
				 errhint("You might need to increase max_active_global_temporary_table.")));
	}

	if (!found)
	{
		int		wordnum;

		/* init bitmap in shared memory */
		entry->map = (Bitmapset *)((char *)entry + MAXALIGN(sizeof(gtt_shared_hash_entry)));
		wordnum = WORDNUM(GetMaxBackends() + 1);
		memset(entry->map, 0, BITMAPSET_SIZE(wordnum + 1));
		entry->map->nwords = wordnum + 1;
	}

	/* record current backendid in shared bitmap */
	bms_add_member(entry->map, MyBackendId);
	LWLockRelease(&gtt_shared_ctl->lock);
}

/*
 * Remove the GTT relid record from the shared hash table which means that current session is
 * not use this GTT.
 */
static void
gtt_storage_checkout(Oid relid, bool isCommit, char relkind)
{
	gtt_shared_hash_entry	*entry;
	gtt_fnode		fnode;

	if (max_active_gtt <= 0)
		return;

	if (!GLOBAL_TEMP_RELKIND_STATE_IS_SHARED(relkind))
		return;

	fnode.dbNode = MyDatabaseId;
	fnode.relNode = relid;
	LWLockAcquire(&gtt_shared_ctl->lock, LW_EXCLUSIVE);
	entry = hash_search(active_gtt_shared_hash, (void *) &(fnode), HASH_FIND, NULL);
	if (!entry)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		if (isCommit)
			elog(WARNING, "relid %u not exist in gtt shared hash when drop local storage", relid);

		return;
	}

	Assert(MyBackendId >= 1 && MyBackendId <= GetMaxBackends());

	/* remove current backendid from shared bitmap */
	bms_del_member(entry->map, MyBackendId);
	if (bms_is_empty(entry->map))
	{
		if (!hash_search(active_gtt_shared_hash, &fnode, HASH_REMOVE, NULL))
			elog(PANIC, "gtt shared hash table corrupted");
	}
	LWLockRelease(&gtt_shared_ctl->lock);

	return;
}

/*
 * Gets usage information for a GTT from shared hash table.
 * The information is in the form of bitmap.
 * Quickly copy the entire bitmap from shared memory and return it.
 * that to avoid holding locks for a long time.
 */
static Bitmapset *
copy_active_gtt_bitmap(Oid relid)
{
	gtt_shared_hash_entry	*entry;
	Bitmapset		*map_copy = NULL;
	gtt_fnode		fnode;

	if (max_active_gtt <= 0)
		return NULL;

	fnode.dbNode = MyDatabaseId;
	fnode.relNode = relid;
	LWLockAcquire(&gtt_shared_ctl->lock, LW_SHARED);
	entry = hash_search(active_gtt_shared_hash, (void *) &(fnode), HASH_FIND, NULL);
	if (entry)
	{
		Assert(entry->map);
		/* copy the entire bitmap */
		if (!bms_is_empty(entry->map))
			map_copy = bms_copy(entry->map);
	}

	LWLockRelease(&gtt_shared_ctl->lock);

	return map_copy;
}

/*
 * Check if there are other sessions using this GTT besides the current session.
 */
bool
is_other_backend_use_gtt(Oid relid)
{
	gtt_shared_hash_entry	*entry;
	bool			in_use = false;
	gtt_fnode		fnode;
	int			num_use = 0;

	if (max_active_gtt <= 0)
		return false;

	fnode.dbNode = MyDatabaseId;
	fnode.relNode = relid;
	LWLockAcquire(&gtt_shared_ctl->lock, LW_SHARED);
	entry = hash_search(active_gtt_shared_hash, (void *) &(fnode), HASH_FIND, NULL);
	if (!entry)
	{
		LWLockRelease(&gtt_shared_ctl->lock);
		return false;
	}

	Assert(entry->map);
	Assert(MyBackendId >= 1 && MyBackendId <= GetMaxBackends());

	/* how many backend are using this GTT */
	num_use = bms_num_members(entry->map);
	if (num_use == 0)
		in_use = false;
	else if (num_use == 1)
	{
		/* check if this is itself */
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

/*
 * Record GTT information to local hash.
 * They include GTT storage info, transaction info and statistical info.
 */
void
remember_gtt_storage_info(RelFileNode rnode, Relation rel)
{
	gtt_local_hash_entry		*entry;
	MemoryContext			oldcontext;
	gtt_relfilenode			*new_node = NULL;
	Oid				relid = RelationGetRelid(rel);
	int				natts = 0;

	if (max_active_gtt <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Global temporary table is disabled"),
				 errhint("You might need to increase max_active_global_temporary_table to enable this feature.")));

	if (RecoveryInProgress())
		elog(ERROR, "readonly mode not support access global temporary table");

	if (rel->rd_rel->relkind == RELKIND_INDEX &&
		rel->rd_index &&
		(!rel->rd_index->indisvalid ||
		 !rel->rd_index->indisready ||
		 !rel->rd_index->indislive))
		 elog(ERROR, "index \"%s\" is invalid that cannot create storage", RelationGetRelationName(rel));

	/* First time through: initialize the hash table */
	if (!gtt_storage_local_hash)
	{
#define GTT_LOCAL_HASH_SIZE		1024
		HASHCTL		ctl;

		if (!CacheMemoryContext)
			CreateCacheMemoryContext();

		gtt_info_context = AllocSetContextCreate(CacheMemoryContext,
												 "gtt info context",
												 ALLOCSET_DEFAULT_SIZES);

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(gtt_local_hash_entry);
		ctl.hcxt = gtt_info_context;
		gtt_storage_local_hash = hash_create("global temporary table info",
											 GTT_LOCAL_HASH_SIZE,
											 &ctl, HASH_ELEM | HASH_BLOBS);
	}

	Assert(CacheMemoryContext);
	Assert(gtt_info_context);
	oldcontext = MemoryContextSwitchTo(gtt_info_context);

	entry = gtt_search_by_relid(relid, true);
	if (!entry)
	{
		bool	found = false;

		/* Look up or create an entry */
		entry = hash_search(gtt_storage_local_hash, (void *) &relid, HASH_ENTER, &found);
		if (found)
		{
			MemoryContextSwitchTo(oldcontext);
			elog(ERROR, "backend %d relid %u already exists in gtt local hash",
						MyBackendId, relid);
		}

		entry->relfilenode_list = NIL;
		entry->relkind = rel->rd_rel->relkind;
		entry->on_commit_delete = false;

		if (entry->relkind == RELKIND_RELATION)
		{
			/* record the on commit clause */
			if (RELATION_GTT_ON_COMMIT_DELETE(rel))
			{
				entry->on_commit_delete = true;
				register_on_commit_action(RelationGetRelid(rel), ONCOMMIT_DELETE_ROWS, true);
			}
		}

		gtt_storage_checkin(relid, entry->relkind);
	}

	/* record storage info relstat columnstats and transaction info to relfilenode list */
	new_node = palloc0(sizeof(gtt_relfilenode));
	new_node->relfilenode = rnode.relNode;
	new_node->spcnode = rnode.spcNode;
	new_node->relpages = 0;
	new_node->reltuples = 0;
	new_node->relallvisible = 0;
	new_node->relfrozenxid = InvalidTransactionId;
	new_node->relminmxid = InvalidMultiXactId;
	new_node->natts = 0;
	new_node->attnum = NULL;
	new_node->att_stat_tups = NULL;
	entry->relfilenode_list = lappend(entry->relfilenode_list, new_node);

	/* init structure for column statistics */
	natts = RelationGetNumberOfAttributes(rel);
	new_node->attnum = palloc0(sizeof(int) * natts);
	new_node->att_stat_tups = palloc0(sizeof(HeapTuple) * natts);
	new_node->natts = natts;

	/* remember transaction info */
	if (RELKIND_HAS_TABLE_AM(entry->relkind))
	{
		new_node->relfrozenxid = RecentXmin;
		new_node->relminmxid = GetOldestMultiXactId();

		insert_gtt_relfrozenxid_to_ordered_list(new_node->relfrozenxid);
		set_gtt_session_relfrozenxid();
	}

	MemoryContextSwitchTo(oldcontext);

	/* Registration callbacks are used to trigger cleanup during process exit */
	if (!gtt_cleaner_exit_registered)
	{
		before_shmem_exit(gtt_storage_removeall, 0);
		gtt_cleaner_exit_registered = true;
	}

	return;
}

/*
 * Remove GTT information from local hash when transaction commit/rollback.
 */
void
forget_gtt_storage_info(Oid relid, RelFileNode rnode, bool isCommit)
{
	gtt_local_hash_entry	*entry = NULL;
	gtt_relfilenode		*d_rnode = NULL;

	if (max_active_gtt <= 0)
		return;

	entry = gtt_search_by_relid(relid, true);
	if (entry == NULL)
	{
		if (isCommit)
			elog(ERROR,"global temp relid %u not found in local hash", relid);

		return;
	}

	d_rnode = gtt_search_relfilenode(entry, rnode.relNode, rnode.spcNode, true);
	if (d_rnode == NULL)
	{
		if (isCommit)
			elog(ERROR,"global temp relfilenode %u not found in rel %u", rnode.relNode, relid);
		else
		{
			/* rollback transaction */
			if (entry->relfilenode_list == NIL)
			{
				gtt_storage_checkout(relid, isCommit, entry->relkind);

				hash_search(gtt_storage_local_hash, (void *) &(relid), HASH_REMOVE, NULL);
			}

			return;
		}
	}

	/* Clean up transaction info from Local order list and MyProc */
	if (RELKIND_HAS_TABLE_AM(entry->relkind))
	{
		Assert(TransactionIdIsNormal(d_rnode->relfrozenxid) || !isCommit);

		/* this is valid relfrozenxid */
		if (TransactionIdIsValid(d_rnode->relfrozenxid))
		{
			remove_gtt_relfrozenxid_from_ordered_list(d_rnode->relfrozenxid);
			set_gtt_session_relfrozenxid();
		}
	}

	/* delete relfilenode from rel entry */
	entry->relfilenode_list = list_delete_ptr(entry->relfilenode_list, d_rnode);
	gtt_free_statistics(d_rnode);

	if (entry->relfilenode_list == NIL)
	{
		/* tell shared hash that current session will no longer use this GTT */
		gtt_storage_checkout(relid, isCommit, entry->relkind);

		hash_search(gtt_storage_local_hash, (void *) &(relid), HASH_REMOVE, NULL);
	}

	return;
}

/*
 * Check if current session is using this GTT.
 */
bool
gtt_storage_attached(Oid relid)
{
	bool			found = false;
	gtt_local_hash_entry	*entry = NULL;

	if (max_active_gtt <= 0)
		return false;

	if (!OidIsValid(relid))
		return false;

	entry = gtt_search_by_relid(relid, true);
	if (entry)
		found = true;

	return found;
}

/*
 * When backend exit, bulk cleaning all GTT storage and local buffer of this backend.
 */
static void
gtt_storage_removeall(int code, Datum arg)
{
	HASH_SEQ_STATUS			status;
	gtt_local_hash_entry	*entry;

	if (!gtt_storage_local_hash)
		return;

	/* Need to ensure we have a usable transaction. */
	AbortOutOfAnyTransaction();

	/* Search all relfilenode for GTT in current session */
	hash_seq_init(&status, gtt_storage_local_hash);
	while ((entry = (gtt_local_hash_entry *) hash_seq_search(&status)) != NULL)
	{
		ListCell *lc;

		foreach(lc, entry->relfilenode_list)
		{
			SMgrRelation	srel[1];
			RelFileNode		rnode;
			gtt_relfilenode *gtt_rnode = lfirst(lc);

			rnode.spcNode = gtt_rnode->spcnode;
			rnode.dbNode = MyDatabaseId;
			rnode.relNode = gtt_rnode->relfilenode;
			srel[0] = smgropen(rnode, MyBackendId);
			smgrdounlinkall(srel, 1, false);
			smgrclose(srel[0]);
		}

		gtt_storage_checkout(entry->relid, false, entry->relkind);

		hash_search(gtt_storage_local_hash, (void *) &(entry->relid), HASH_REMOVE, NULL);
	}

	/* set to global area */
	MyProc->gtt_frozenxid = InvalidTransactionId;

	return;
}

/*
 * Update GTT relstats(relpage/reltuple/relallvisible)
 * to local hash.
 */
void
gtt_update_relstats(Relation relation, BlockNumber relpages, double reltuples,
						BlockNumber relallvisible, TransactionId relfrozenxid,
						TransactionId relminmxid)
{
	Oid						relid = RelationGetRelid(relation);
	gtt_relfilenode			*gtt_rnode = NULL;
	gtt_local_hash_entry	*entry = NULL;

	if (max_active_gtt <= 0)
		return;

	if (!OidIsValid(relid))
		return;

	entry = gtt_search_by_relid(relid, true);
	if (entry == NULL)
		return;

	gtt_rnode = lfirst(list_tail(entry->relfilenode_list));
	if (gtt_rnode == NULL)
		return;

	if (relpages >= 0 &&
		gtt_rnode->relpages != (int32)relpages)
	{
		gtt_rnode->relpages = (int32)relpages;
		relation->rd_rel->relpages = (int32) relpages;
	}

	if (reltuples >= 0 &&
		gtt_rnode->reltuples != (float4)reltuples)
	{
		gtt_rnode->reltuples = (float4)reltuples;
		relation->rd_rel->reltuples = (float4)reltuples;
	}

	if (relallvisible >= 0 &&
		gtt_rnode->relallvisible != (int32)relallvisible)
	{
		gtt_rnode->relallvisible = (int32)relallvisible;
		relation->rd_rel->relallvisible = (int32)relallvisible;
	}

	/* only heap contain transaction information and relallvisible */
	if (RELKIND_HAS_TABLE_AM(entry->relkind))
	{
		if (TransactionIdIsNormal(relfrozenxid) &&
			gtt_rnode->relfrozenxid != relfrozenxid &&
			(TransactionIdPrecedes(gtt_rnode->relfrozenxid, relfrozenxid) ||
			 TransactionIdPrecedes(ReadNextTransactionId(), gtt_rnode->relfrozenxid)))
		{
			/* set to local order list */
			remove_gtt_relfrozenxid_from_ordered_list(gtt_rnode->relfrozenxid);
			gtt_rnode->relfrozenxid = relfrozenxid;
			insert_gtt_relfrozenxid_to_ordered_list(relfrozenxid);
			/* set to global area */
			set_gtt_session_relfrozenxid();
			relation->rd_rel->relfrozenxid = relfrozenxid;
		}

		if (MultiXactIdIsValid(relminmxid) &&
			gtt_rnode->relminmxid != relminmxid &&
			(MultiXactIdPrecedes(gtt_rnode->relminmxid, relminmxid) ||
			 MultiXactIdPrecedes(ReadNextMultiXactId(), gtt_rnode->relminmxid)))
		{
			gtt_rnode->relminmxid = relminmxid;
			relation->rd_rel->relminmxid = relminmxid;
		}
	}

	return;
}

/*
 * Search GTT relstats(relpage/reltuple/relallvisible)
 * from local has.
 */
bool
get_gtt_relstats(Oid relid, BlockNumber *relpages, double *reltuples,
				BlockNumber *relallvisible, TransactionId *relfrozenxid,
				TransactionId *relminmxid)
{
	gtt_local_hash_entry	*entry;
	gtt_relfilenode		*gtt_rnode = NULL;

	if (max_active_gtt <= 0)
		return false;

	entry = gtt_search_by_relid(relid, true);
	if (entry == NULL)
		return false;

	Assert(entry->relid == relid);

	gtt_rnode = lfirst(list_tail(entry->relfilenode_list));
	if (gtt_rnode == NULL)
		return false;

	if (relpages)
		*relpages = gtt_rnode->relpages;

	if (reltuples)
		*reltuples = gtt_rnode->reltuples;

	if (relallvisible)
		*relallvisible = gtt_rnode->relallvisible;

	if (relfrozenxid)
		*relfrozenxid = gtt_rnode->relfrozenxid;

	if (relminmxid)
		*relminmxid = gtt_rnode->relminmxid;

	return true;
}

/*
 * Update GTT info(definition is same as pg_statistic)
 * to local hash.
 */
void
up_gtt_att_statistic(Oid reloid, int attnum, bool inh, int natts,
					TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
	gtt_local_hash_entry	*entry;
	gtt_relfilenode		*gtt_rnode = NULL;
	MemoryContext		oldcontext;
	bool		found = false;
	int			i = 0;

	/* not support whole row or system column */
	if (attnum <= 0)
		return;

	if (max_active_gtt <= 0)
		return;

	entry = gtt_search_by_relid(reloid, true);
	if (entry == NULL)
		return;

	Assert(entry->relid == reloid);

	gtt_rnode = lfirst(list_tail(entry->relfilenode_list));
	if (gtt_rnode == NULL)
		return;

	/* switch context to gtt_info_context for store tuple at heap_form_tuple */
	oldcontext = MemoryContextSwitchTo(gtt_info_context);
	for (i = 0; i < gtt_rnode->natts; i++)
	{
		if (gtt_rnode->attnum[i] == 0)
		{
			Assert(gtt_rnode->att_stat_tups[i] == NULL);
			gtt_rnode->attnum[i] = attnum;
			gtt_rnode->att_stat_tups[i] = heap_form_tuple(tupleDescriptor, values, isnull);
			found = true;
			break;
		}
		else if (gtt_rnode->attnum[i] == attnum)
		{
			Assert(gtt_rnode->att_stat_tups[i]);
			heap_freetuple(gtt_rnode->att_stat_tups[i]);
			gtt_rnode->att_stat_tups[i] = heap_form_tuple(tupleDescriptor, values, isnull);
			found = true;
			break;
		}
	}
	MemoryContextSwitchTo(oldcontext);

	if (!found)
		elog(WARNING, "analyze can not update relid %u column %d statistics after add or drop column, try truncate table first", reloid, attnum);

	return;
}

/*
 * Search GTT statistic info(definition is same as pg_statistic)
 * from local hash.
 */
HeapTuple
get_gtt_att_statistic(Oid reloid, int attnum, bool inh)
{
	gtt_local_hash_entry	*entry;
	int			i = 0;
	gtt_relfilenode		*gtt_rnode = NULL;

	/* not support whole row or system column */
	if (attnum <= 0)
		return NULL;

	if (max_active_gtt <= 0)
		return NULL;

	entry = gtt_search_by_relid(reloid, true);
	if (entry == NULL)
		return NULL;

	gtt_rnode = lfirst(list_tail(entry->relfilenode_list));
	if (gtt_rnode == NULL)
		return NULL;

	for (i = 0; i < gtt_rnode->natts; i++)
	{
		if (gtt_rnode->attnum[i] == attnum)
		{
			Assert(gtt_rnode->att_stat_tups[i]);
			return gtt_rnode->att_stat_tups[i];
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

/*
 * Maintain a order relfrozenxid list of backend Level for GTT.
 * Insert a RelfrozenXID into the list and keep the list in order.
 */
static void
insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid)
{
	MemoryContext	oldcontext;
	ListCell	*cell;
	int		i;

	Assert(TransactionIdIsNormal(relfrozenxid));

	oldcontext = MemoryContextSwitchTo(gtt_info_context);

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

/*
 * Maintain a order relfrozenxid list of backend Level for GTT.
 * Remove a RelfrozenXID from order list gtt_session_relfrozenxid_list.
 */
static void
remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid)
{
	gtt_session_relfrozenxid_list =
		list_delete_oid(gtt_session_relfrozenxid_list, relfrozenxid);
}

/*
 * Update of backend Level oldest relfrozenxid to MyProc.
 * This makes each backend's oldest RelFrozenxID globally visible.
 */
static void
set_gtt_session_relfrozenxid(void)
{
	TransactionId gtt_frozenxid = InvalidTransactionId;

	if (gtt_session_relfrozenxid_list != NIL)
		gtt_frozenxid = llast_oid(gtt_session_relfrozenxid_list);

	if (MyProc->gtt_frozenxid != gtt_frozenxid)
		MyProc->gtt_frozenxid = gtt_frozenxid;
}

/*
 * Get GTT column level data statistics.
 */
Datum
pg_get_gtt_statistics(PG_FUNCTION_ARGS)
{
	ReturnSetInfo		*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate		*tupstore;
	HeapTuple		tuple;
	Relation		rel = NULL;
	Oid			reloid = PG_GETARG_OID(0);
	int			attnum = PG_GETARG_INT32(1);
	TupleDesc		tupdesc;
	MemoryContext		oldcontext;
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
			 errmsg("materialize mode required, but it is not allowed in this context")));

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	rel = relation_open(reloid, AccessShareLock);
	if (!RELATION_IS_GLOBAL_TEMP(rel))
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		relation_close(rel, NoLock);
		return (Datum) 0;
	}

	pg_tatistic = relation_open(StatisticRelationId, AccessShareLock);
	sd = RelationGetDescr(pg_tatistic);

	/* get data from local hash */
	tuple = get_gtt_att_statistic(reloid, attnum, false);
	if (tuple)
	{
		Datum		values[Natts_pg_statistic];
		bool		isnull[Natts_pg_statistic];
		HeapTuple	res = NULL;

		memset(&values, 0, sizeof(values));
		memset(&isnull, 0, sizeof(isnull));
		heap_deform_tuple(tuple, sd, values, isnull);
		res = heap_form_tuple(tupdesc, values, isnull);
		tuplestore_puttuple(tupstore, res);
	}
	tuplestore_donestoring(tupstore);

	relation_close(rel, AccessShareLock);
	relation_close(pg_tatistic, AccessShareLock);

	return (Datum) 0;
}

/*
 * Get GTT table level data statistics.
 */
Datum
pg_get_gtt_relstats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate	*tupstore;
	TupleDesc	tupdesc;
	MemoryContext	oldcontext;
	HeapTuple	tuple;
	Oid			reloid = PG_GETARG_OID(0);
	Oid			relnode = 0;
	BlockNumber		relpages = 0;
	BlockNumber		relallvisible = 0;
	uint32			relfrozenxid = 0;
	uint32			relminmxid = 0;
	double			reltuples = 0;
	Relation		rel = NULL;

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

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	rel = relation_open(reloid, AccessShareLock);
	if (!RELATION_IS_GLOBAL_TEMP(rel))
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		relation_close(rel, NoLock);
		return (Datum) 0;
	}

	get_gtt_relstats(reloid,
					&relpages, &reltuples, &relallvisible,
					&relfrozenxid, &relminmxid);
	relnode = gtt_fetch_current_relfilenode(reloid);
	if (relnode != InvalidOid)
	{
		Datum	values[6];
		bool	isnull[6];

		memset(isnull, 0, sizeof(isnull));
		memset(values, 0, sizeof(values));
		values[0] = UInt32GetDatum(relnode);
		values[1] = Int32GetDatum(relpages);
		values[2] = Float4GetDatum((float4)reltuples);
		values[3] = Int32GetDatum(relallvisible);
		values[4] = UInt32GetDatum(relfrozenxid);
		values[5] = UInt32GetDatum(relminmxid);
		tuple = heap_form_tuple(tupdesc, values, isnull);
		tuplestore_puttuple(tupstore, tuple);
	}
	tuplestore_donestoring(tupstore);

	relation_close(rel, NoLock);

	return (Datum) 0;
}

/*
 * Get a list of backend pids that are currently using this GTT.
 */
Datum
pg_gtt_attached_pid(PG_FUNCTION_ARGS)
{
	ReturnSetInfo		*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	PGPROC			*proc = NULL;
	Bitmapset		*map = NULL;
	Tuplestorestate		*tupstore;
	TupleDesc		tupdesc;
	MemoryContext		oldcontext;
	HeapTuple		tuple;
	Oid			reloid = PG_GETARG_OID(0);
	Relation		rel = NULL;
	int				backendid = 0;

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

	rel = relation_open(reloid, AccessShareLock);
	if (!RELATION_IS_GLOBAL_TEMP(rel))
	{
		elog(WARNING, "relation OID %u is not a global temporary table", reloid);
		relation_close(rel, NoLock);
		return (Datum) 0;
	}

	/* get data from share hash */
	map = copy_active_gtt_bitmap(reloid);
	if (map)
	{
		backendid = bms_first_member(map);

		do
		{
			/* backendid map to process pid */
			proc = BackendIdGetProc(backendid);
			if (proc && proc->pid > 0)
			{
				Datum	values[2];
				bool	isnull[2];
				pid_t	pid = proc->pid;

				memset(isnull, 0, sizeof(isnull));
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
	relation_close(rel, AccessShareLock);

	return (Datum) 0;
}

/*
 * Get backend level oldest relfrozenxid of each backend using GTT in current database.
 */
Datum
pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo		*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate		*tupstore;
	TupleDesc		tupdesc;
	MemoryContext	oldcontext;
	TransactionId	oldest = InvalidTransactionId;
	List			*pids = NULL;
	List			*xids = NULL;

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

	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	if (max_active_gtt <= 0)
		return (Datum) 0;

	if (RecoveryInProgress())
		return (Datum) 0;

	/* Get all session level oldest relfrozenxid that in current database use global temp table */
	oldest = gtt_get_oldest_frozenxids_in_current_database(&pids, &xids);
	if (TransactionIdIsValid(oldest))
	{
		HeapTuple		tuple;
		ListCell		*lc1 = NULL;
		ListCell		*lc2 = NULL;

		Assert(list_length(pids) == list_length(xids));
		forboth(lc1, pids, lc2, xids)
		{
			Datum	values[2];
			bool	isnull[2];

			memset(isnull, 0, sizeof(isnull));
			memset(values, 0, sizeof(values));
			values[0] = Int32GetDatum(lfirst_int(lc1));
			values[1] = UInt32GetDatum(lfirst_oid(lc2));
			tuple = heap_form_tuple(tupdesc, values, isnull);
			tuplestore_puttuple(tupstore, tuple);
		}
	}
	tuplestore_donestoring(tupstore);
	list_free(pids);
	list_free(xids);

	return (Datum) 0;
}

/*
 * In order to build the GTT index, force enable GTT'index.
 */
void
gtt_index_force_enable(Relation index)
{
	if (!RELATION_IS_GLOBAL_TEMP(index))
		return;

	Assert(index->rd_rel->relkind == RELKIND_INDEX);
	Assert(OidIsValid(RelationGetRelid(index)));

	index->rd_index->indisvalid = true;
	index->rd_index->indislive = true;
	index->rd_index->indisready = true;
}

/*
 * Fix the local state of the GTT's index.
 */
void
gtt_correct_index_session_state(Relation index)
{
	Oid indexOid = RelationGetRelid(index);
	Oid heapOid = index->rd_index->indrelid;

	/* Must be GTT */
	if (!RELATION_IS_GLOBAL_TEMP(index))
		return;

	if (!index->rd_index->indisvalid)
		return;

	/*
	 * If this GTT is not initialized in the current session,
	 * its index status is temporarily set to invalid(local relcache).
	 */
	if (gtt_storage_attached(heapOid) &&
		!gtt_storage_attached(indexOid))
	{
		index->rd_index->indisvalid = false;
		index->rd_index->indislive = false;
		index->rd_index->indisready = false;
	}

	return;
}

/*
 * Initialize storage of GTT and build empty index in this session.
 */
void
gtt_init_storage(CmdType operation, Relation relation)
{
	Oid			toastrelid;
	List		*indexoidlist = NIL;
	ListCell	*l;

	if (!(operation == CMD_INSERT))
		return;

	if (!RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
		return;

	if (!RELATION_IS_GLOBAL_TEMP(relation))
		return;

	/* Each GTT is initialized once in each backend */
	if (gtt_storage_attached(RelationGetRelid(relation)))
		return;

	/* init heap storage */
	RelationCreateStorage(relation->rd_node, RELPERSISTENCE_GLOBAL_TEMP, relation);

	indexoidlist = RelationGetIndexList(relation);
	foreach(l, indexoidlist)
	{
		Oid			indexOid = lfirst_oid(l);
		Relation	index = index_open(indexOid, RowExclusiveLock);
		IndexInfo	*info = BuildDummyIndexInfo(index);

		index_build(relation, index, info, true, false);
		/* after build index, index re-enabled */
		Assert(index->rd_index->indisvalid);
		Assert(index->rd_index->indislive);
		Assert(index->rd_index->indisready);
		index_close(index, NoLock);
	}
	list_free(indexoidlist);

	/* rebuild index for global temp toast table */
	toastrelid = relation->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		Relation	toastrel;
		ListCell	*indlist;

		toastrel = table_open(toastrelid, RowExclusiveLock);

		/* init index storage */
		RelationCreateStorage(toastrel->rd_node, RELPERSISTENCE_GLOBAL_TEMP, toastrel);

		foreach(indlist, RelationGetIndexList(toastrel))
		{
			Oid			indexId = lfirst_oid(indlist);
			Relation	index = index_open(indexId, RowExclusiveLock);
			IndexInfo	*info = BuildDummyIndexInfo(index);

			/* build empty index */
			index_build(toastrel, index, info, true, false);
			Assert(index->rd_index->indisvalid);
			Assert(index->rd_index->indislive);
			Assert(index->rd_index->indisready);
			index_close(index, NoLock);
		}

		table_close(toastrel, NoLock);
	}

	return;
}

/*
 * Release the data structure memory used to store GTT storage info.
 */
static void
gtt_free_statistics(gtt_relfilenode *rnode)
{
	int i;

	Assert(rnode);

	for (i = 0; i < rnode->natts; i++)
	{
		if (rnode->att_stat_tups[i])
		{
			heap_freetuple(rnode->att_stat_tups[i]);
			rnode->att_stat_tups[i] = NULL;
		}
	}

	if (rnode->attnum)
		pfree(rnode->attnum);

	if (rnode->att_stat_tups)
		pfree(rnode->att_stat_tups);

	pfree(rnode);

	return;
}

/*
 * Get the current relfilenode of this GTT.
 */
Oid
gtt_fetch_current_relfilenode(Oid relid)
{
	gtt_local_hash_entry	*entry;
	gtt_relfilenode			*gtt_rnode = NULL;

	if (max_active_gtt <= 0)
		return InvalidOid;

	entry = gtt_search_by_relid(relid, true);
	if (entry == NULL)
		return InvalidOid;

	Assert(entry->relid == relid);
	gtt_rnode = lfirst(list_tail(entry->relfilenode_list));
	if (gtt_rnode == NULL)
		return InvalidOid;

	return gtt_rnode->relfilenode;
}

/*
 * Get a relfilenode used by this GTT during the transaction life cycle.
 */
static gtt_relfilenode *
gtt_search_relfilenode(gtt_local_hash_entry	*entry, Oid relfilenode, Oid spcnode, bool missing_ok)
{
	gtt_relfilenode		*rnode = NULL;
	ListCell		*lc;

	Assert(entry);
	foreach(lc, entry->relfilenode_list)
	{
		gtt_relfilenode	*gtt_rnode = lfirst(lc);
		if (gtt_rnode->relfilenode == relfilenode &&
			gtt_rnode->spcnode == spcnode)
		{
			rnode = gtt_rnode;
			break;
		}
	}

	if (!missing_ok && rnode == NULL)
		elog(ERROR, "search relfilenode %u relfilenodelist from relid %u fail", relfilenode, entry->relid);

	return rnode;
}

/*
 * Get one GTT info from local hash.
 */
static gtt_local_hash_entry *
gtt_search_by_relid(Oid relid, bool missing_ok)
{
	gtt_local_hash_entry	*entry = NULL;

	if (!gtt_storage_local_hash)
		return NULL;

	entry = hash_search(gtt_storage_local_hash, (void *) &(relid), HASH_FIND, NULL);
	if (!entry && !missing_ok)
		elog(ERROR, "relid %u not found in local hash", relid);

	return entry;
}

/*
 * update pg_class entry after CREATE INDEX or REINDEX for global temp table
 */
void
index_update_gtt_relstats(Relation rel, bool hasindex, double reltuples, bool isreindex)
{
	Oid			relid = RelationGetRelid(rel);

	Assert(RELATION_IS_GLOBAL_TEMP(rel));

	/* see index_update_stats() */
	if (reltuples == 0 && rel->rd_rel->reltuples < 0)
		reltuples = -1;

	/* update reltuples relpages relallvisible to localhash */
	if (reltuples >= 0)
	{
		BlockNumber relpages = RelationGetNumberOfBlocks(rel);
		BlockNumber relallvisible = 0;

		if (rel->rd_rel->relkind != RELKIND_INDEX)
			visibilitymap_count(rel, &relallvisible, NULL);
		else
			relallvisible = 0;

		gtt_update_relstats(rel, relpages, reltuples, relallvisible,
							InvalidTransactionId, InvalidMultiXactId);
	}

	/* update relhasindex to pg_class */
	if (hasindex != rel->rd_rel->relhasindex)
	{
		Relation		pg_class = table_open(RelationRelationId, RowExclusiveLock);
		Form_pg_class	rd_rel;
		HeapTuple		tuple;

		Assert(rel->rd_rel->relkind != RELKIND_INDEX);
		tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "could not find tuple for relation %u", relid);

		rd_rel = (Form_pg_class) GETSTRUCT(tuple);
		rd_rel->relhasindex = hasindex;
		heap_inplace_update(pg_class, tuple);
		heap_freetuple(tuple);
		table_close(pg_class, RowExclusiveLock);
	}
	else if (!isreindex)
	{
		/*
		 * For global temp table
		 * Even if pg_class does not change, relcache needs to be rebuilt
		 * for flush rd_indexlist list (for a table) at create index process.
		 *
		 * Each session index has an independent data and cache(rd_amcache)
		 * so relcache of the table and index do not need to be refreshed at reindex process.
		 * This is different from the reindex of a regular table.
		 */
		CacheInvalidateRelcache(rel);
	}
}

/*
 * update statistics for one global temp relation
 */
void
vac_update_gtt_relstats(Relation relation,
					BlockNumber num_pages, double num_tuples,
					BlockNumber num_all_visible_pages,
					bool hasindex, TransactionId frozenxid,
					MultiXactId minmulti, bool in_outer_xact)
{
	Oid			relid = RelationGetRelid(relation);
	Relation	pg_class;
	HeapTuple	ctup;
	Form_pg_class pgcform;
	bool		dirty = false;
	List		*idxs = NIL;

	Assert(RELATION_IS_GLOBAL_TEMP(relation));

	/* For global temporary table, store relstats and transaction info to the localhash */
	gtt_update_relstats(relation, num_pages, num_tuples,
						num_all_visible_pages, frozenxid, minmulti);

	if (relation->rd_rel->relkind == RELKIND_RELATION)
		idxs = RelationGetIndexList(relation);

	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u vanished during vacuuming", relid);
	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	/* Apply DDL updates, but not inside an outer transaction (see above) */
	if (!in_outer_xact)
	{
		/*
		 * If we didn't find any indexes, reset relhasindex.
		 *
		 * Global temporary table may contain indexes that are not valid locally.
		 * The catalog should not be updated based on local invalid index.
		 */
		if (pgcform->relhasindex && !hasindex && idxs == NIL)
		{
			pgcform->relhasindex = false;
			dirty = true;
		}

		/* We also clear relhasrules and relhastriggers if needed */
		if (pgcform->relhasrules && relation->rd_rules == NULL)
		{
			pgcform->relhasrules = false;
			dirty = true;
		}
		if (pgcform->relhastriggers && relation->trigdesc == NULL)
		{
			pgcform->relhastriggers = false;
			dirty = true;
		}
	}

	/* If anything changed, write out the tuple. */
	if (dirty)
		heap_inplace_update(pg_class, ctup);

	table_close(pg_class, RowExclusiveLock);

	list_free(idxs);
}

void
GlobalTempRelationSetNewRelfilenode(Relation relation)
{
	Oid			newrelfilenode;
	MultiXactId minmulti = InvalidMultiXactId;
	TransactionId freezeXid = InvalidTransactionId;
	RelFileNode newrnode;

	Assert(RELATION_IS_GLOBAL_TEMP(relation));
	Assert(!RelationIsMapped(relation));

	/* Allocate a new relfilenode */
	newrelfilenode = GetNewRelFileNode(relation->rd_rel->reltablespace, NULL,
									   RELPERSISTENCE_GLOBAL_TEMP);

	/*
	 * Schedule unlinking of the old storage at transaction commit.
	 */
	RelationDropStorage(relation);

	newrnode = relation->rd_node;
	newrnode.relNode = newrelfilenode;

	if (RELKIND_HAS_TABLE_AM(relation->rd_rel->relkind))
	{
		table_relation_set_new_filenode(relation, &newrnode,
										RELPERSISTENCE_GLOBAL_TEMP,
										&freezeXid, &minmulti);
	}
	else if (RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
	{
		/* handle these directly, at least for now */
		SMgrRelation srel;

		srel = RelationCreateStorage(newrnode, RELPERSISTENCE_GLOBAL_TEMP, relation);
		smgrclose(srel);
	}
	else
	{
		/* we shouldn't be called for anything else */
		elog(ERROR, "relation \"%s\" does not have storage",
			 RelationGetRelationName(relation));
	}

	RelationAssumeNewRelfilenode(relation);

	/* The local relcache and hashtable have been updated */
	Assert(gtt_fetch_current_relfilenode(RelationGetRelid(relation)) == newrelfilenode);
	Assert(relation->rd_node.relNode == newrelfilenode);
}
