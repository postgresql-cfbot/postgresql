/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.c
 *		Prefetching support for recovery.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/backend/access/transam/xlogprefetcher.c
 *
 * This module provides a drop-in replacement for an XLogReader that tries to
 * minimize I/O stalls by looking up future blocks in the buffer cache, and
 * initiating I/Os that might complete before the caller eventually needs the
 * data.  XLogRecBufferForRedo() cooperates uses information stored in the
 * decoded record to find buffers efficiently.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "funcapi.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

/* Every time we process this much WAL, we update dynamic values in shm. */
#define XLOGPREFETCHER_STATS_SHM_DISTANCE BLCKSZ

/* GUCs */
bool		recovery_prefetch = false;

static int	XLogPrefetchReconfigureCount = 0;

/*
 * Enum used to report whether an IO should be started.
 */
typedef enum
{
	LRQ_NEXT_NO_IO,
	LRQ_NEXT_IO,
	LRQ_NEXT_AGAIN
} LsnReadQueueNextStatus;

/*
 * Type of callback that can decide which block to prefetch next.  For now
 * there is only one.
 */
typedef LsnReadQueueNextStatus (*LsnReadQueueNextFun) (uintptr_t lrq_private,
													   XLogRecPtr *lsn);

/*
 * A simple circular queue of LSNs, using to control the number of
 * (potentially) inflight IOs.  This stands in for a later more general IO
 * control mechanism, which is why it has the apparently unnecessary
 * indirection through a function pointer.
 */
typedef struct LsnReadQueue
{
	LsnReadQueueNextFun next;
	uintptr_t	lrq_private;
	uint32		max_inflight;
	uint32		inflight;
	uint32		completed;
	uint32		head;
	uint32		tail;
	uint32		size;
	struct
	{
		bool		io;
		XLogRecPtr	lsn;
	}			queue[FLEXIBLE_ARRAY_MEMBER];
} LsnReadQueue;

/*
 * A prefetcher.  This is a mechanism that wraps an XLogReader, prefetching
 * blocks that will be soon be referenced, to try to avoid IO stalls.
 */
struct XLogPrefetcher
{
	/* WAL reader and current reading state. */
	XLogReaderState *reader;
	DecodedXLogRecord *record;
	int			next_block_id;

	/* When to publish stats. */
	XLogRecPtr	next_stats_shm_lsn;

	/* Book-keeping required to avoid accessing non-existing blocks. */
	HTAB	   *filter_table;
	dlist_head	filter_queue;

	/* IO depth manager. */
	LsnReadQueue *streaming_read;

	XLogRecPtr	begin_ptr;

	int			reconfigure_count;
};

/*
 * A temporary filter used to track block ranges that haven't been created
 * yet, whole relations that haven't been created yet, and whole relations
 * that (we assume) have already been dropped, or will be created by bulk WAL
 * operators.
 */
typedef struct XLogPrefetcherFilter
{
	RelFileNode rnode;
	XLogRecPtr	filter_until_replayed;
	BlockNumber filter_from_block;
	dlist_node	link;
} XLogPrefetcherFilter;

/*
 * Counters exposed in shared memory for pg_stat_prefetch_recovery.
 */
typedef struct XLogPrefetchStats
{
	pg_atomic_uint64 reset_time;	/* Time of last reset. */
	pg_atomic_uint64 prefetch;	/* Prefetches initiated. */
	pg_atomic_uint64 hit;		/* Blocks already in cache. */
	pg_atomic_uint64 skip_init; /* Zero-inited blocks skipped. */
	pg_atomic_uint64 skip_new;	/* New/missing blocks filtered. */
	pg_atomic_uint64 skip_fpw;	/* FPWs skipped. */

	/* Reset counters */
	pg_atomic_uint32 reset_request;
	uint32		reset_handled;

	/* Dynamic values */
	int			wal_distance;	/* Number of WAL bytes ahead. */
	int			block_distance; /* Number of block references ahead. */
	int			io_depth;		/* Number of I/Os in progress. */
} XLogPrefetchStats;

static inline void XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher,
										   RelFileNode rnode,
										   BlockNumber blockno,
										   XLogRecPtr lsn);
static inline bool XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher,
											RelFileNode rnode,
											BlockNumber blockno);
static inline void XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher,
												 XLogRecPtr replaying_lsn);
static LsnReadQueueNextStatus XLogPrefetcherNextBlock(uintptr_t pgsr_private,
													  XLogRecPtr *lsn);

static XLogPrefetchStats *SharedStats;

static inline LsnReadQueue *
lrq_alloc(uint32 max_distance,
		  uint32 max_inflight,
		  uintptr_t lrq_private,
		  LsnReadQueueNextFun next)
{
	LsnReadQueue *lrq;
	uint32		size;

	Assert(max_distance >= max_inflight);

	size = max_distance + 1;	/* full ring buffer has a gap */
	lrq = palloc(offsetof(LsnReadQueue, queue) + sizeof(lrq->queue[0]) * size);
	lrq->lrq_private = lrq_private;
	lrq->max_inflight = max_inflight;
	lrq->size = size;
	lrq->next = next;
	lrq->head = 0;
	lrq->tail = 0;
	lrq->inflight = 0;
	lrq->completed = 0;

	return lrq;
}

static inline void
lrq_free(LsnReadQueue *lrq)
{
	pfree(lrq);
}

static inline uint32
lrq_inflight(LsnReadQueue *lrq)
{
	return lrq->inflight;
}

static inline uint32
lrq_completed(LsnReadQueue *lrq)
{
	return lrq->completed;
}

static inline void
lrq_prefetch(LsnReadQueue *lrq)
{
	/* Try to start as many IOs as we can within our limits. */
	while (lrq->inflight < lrq->max_inflight &&
		   lrq->inflight + lrq->completed < lrq->size - 1)
	{
		Assert(((lrq->head + 1) % lrq->size) != lrq->tail);
		switch (lrq->next(lrq->lrq_private, &lrq->queue[lrq->head].lsn))
		{
			case LRQ_NEXT_AGAIN:
				return;
			case LRQ_NEXT_IO:
				lrq->queue[lrq->head].io = true;
				lrq->inflight++;
				break;
			case LRQ_NEXT_NO_IO:
				lrq->queue[lrq->head].io = false;
				lrq->completed++;
				break;
		}
		lrq->head++;
		if (lrq->head == lrq->size)
			lrq->head = 0;
	}
}

static inline void
lrq_complete_lsn(LsnReadQueue *lrq, XLogRecPtr lsn)
{
	/*
	 * We know that LSNs before 'lsn' have been replayed, so we can now assume
	 * that any IOs that were started before then have finished.
	 */
	while (lrq->tail != lrq->head &&
		   lrq->queue[lrq->tail].lsn < lsn)
	{
		if (lrq->queue[lrq->tail].io)
			lrq->inflight--;
		else
			lrq->completed--;
		lrq->tail++;
		if (lrq->tail == lrq->size)
			lrq->tail = 0;
	}
	if (recovery_prefetch)
		lrq_prefetch(lrq);
}

size_t
XLogPrefetchShmemSize(void)
{
	return sizeof(XLogPrefetchStats);
}

static void
XLogPrefetchResetStats(void)
{
	pg_atomic_write_u64(&SharedStats->reset_time, GetCurrentTimestamp());
	pg_atomic_write_u64(&SharedStats->prefetch, 0);
	pg_atomic_write_u64(&SharedStats->hit, 0);
	pg_atomic_write_u64(&SharedStats->skip_init, 0);
	pg_atomic_write_u64(&SharedStats->skip_new, 0);
	pg_atomic_write_u64(&SharedStats->skip_fpw, 0);
}

void
XLogPrefetchShmemInit(void)
{
	bool		found;

	SharedStats = (XLogPrefetchStats *)
		ShmemInitStruct("XLogPrefetchStats",
						sizeof(XLogPrefetchStats),
						&found);

	if (!found)
	{
		pg_atomic_init_u32(&SharedStats->reset_request, 0);
		SharedStats->reset_handled = 0;

		pg_atomic_init_u64(&SharedStats->reset_time, GetCurrentTimestamp());
		pg_atomic_init_u64(&SharedStats->prefetch, 0);
		pg_atomic_init_u64(&SharedStats->hit, 0);
		pg_atomic_init_u64(&SharedStats->skip_init, 0);
		pg_atomic_init_u64(&SharedStats->skip_new, 0);
		pg_atomic_init_u64(&SharedStats->skip_fpw, 0);
	}
}

/*
 * Called when any GUC is changed that affects prefetching.
 */
void
XLogPrefetchReconfigure(void)
{
	XLogPrefetchReconfigureCount++;
}

/*
 * Called by any backend to request that the stats be reset.
 */
void
XLogPrefetchRequestResetStats(void)
{
	pg_atomic_fetch_add_u32(&SharedStats->reset_request, 1);
}

/*
 * Increment a counter in shared memory.  This is equivalent to *counter++ on a
 * plain uint64 without any memory barrier or locking, except on platforms
 * where readers can't read uint64 without possibly observing a torn value.
 */
static inline void
XLogPrefetchIncrement(pg_atomic_uint64 *counter)
{
	Assert(AmStartupProcess() || !IsUnderPostmaster);
	pg_atomic_write_u64(counter, pg_atomic_read_u64(counter) + 1);
}

/*
 * Create a prefetcher that is ready to begin prefetching blocks referenced by
 * WAL records.
 */
XLogPrefetcher *
XLogPrefetcherAllocate(XLogReaderState *reader)
{
	XLogPrefetcher *prefetcher;
	static HASHCTL hash_table_ctl = {
		.keysize = sizeof(RelFileNode),
		.entrysize = sizeof(XLogPrefetcherFilter)
	};

	prefetcher = palloc0(sizeof(XLogPrefetcher));

	prefetcher->reader = reader;
	prefetcher->filter_table = hash_create("XLogPrefetcherFilterTable", 1024,
										   &hash_table_ctl,
										   HASH_ELEM | HASH_BLOBS);
	dlist_init(&prefetcher->filter_queue);

	SharedStats->wal_distance = 0;
	SharedStats->block_distance = 0;
	SharedStats->io_depth = 0;

	/* First usage will cause streaming_read to be allocated. */
	prefetcher->reconfigure_count = XLogPrefetchReconfigureCount - 1;

	return prefetcher;
}

/*
 * Destroy a prefetcher and release all resources.
 */
void
XLogPrefetcherFree(XLogPrefetcher *prefetcher)
{
	lrq_free(prefetcher->streaming_read);
	hash_destroy(prefetcher->filter_table);
	pfree(prefetcher);
}

/*
 * Provide access to the reader.
 */
XLogReaderState *
XLogPrefetcherReader(XLogPrefetcher *prefetcher)
{
	return prefetcher->reader;
}

static void
XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher, XLogRecPtr lsn)
{
	uint32		io_depth;
	uint32		completed;
	uint32		reset_request;
	int64		wal_distance;


	/* How far ahead of replay are we now? */
	if (prefetcher->record)
		wal_distance = prefetcher->record->lsn - prefetcher->reader->record->lsn;
	else
		wal_distance = 0;

	/* How many IOs are currently in flight and completed? */
	io_depth = lrq_inflight(prefetcher->streaming_read);
	completed = lrq_completed(prefetcher->streaming_read);

	/* Update the instantaneous stats visible in pg_stat_prefetch_recovery. */
	SharedStats->io_depth = io_depth;
	SharedStats->block_distance = io_depth + completed;
	SharedStats->wal_distance = wal_distance;

	/*
	 * Have we been asked to reset our stats counters?  This is checked with
	 * an unsynchronized memory read, but we'll see it eventually and we'll be
	 * accessing that cache line anyway.
	 */
	reset_request = pg_atomic_read_u32(&SharedStats->reset_request);
	if (reset_request != SharedStats->reset_handled)
	{
		XLogPrefetchResetStats();
		SharedStats->reset_handled = reset_request;
	}

	prefetcher->next_stats_shm_lsn = lsn + XLOGPREFETCHER_STATS_SHM_DISTANCE;
}

/*
 * A callback that reads ahead in the WAL and tries to initiate one IO.
 */
static LsnReadQueueNextStatus
XLogPrefetcherNextBlock(uintptr_t pgsr_private, XLogRecPtr *lsn)
{
	XLogPrefetcher *prefetcher = (XLogPrefetcher *) pgsr_private;
	XLogReaderState *reader = prefetcher->reader;
	XLogRecPtr	replaying_lsn = reader->ReadRecPtr;

	/*
	 * We keep track of the record and block we're up to between calls with
	 * prefetcher->record and prefetcher->next_block_id.
	 */
	for (;;)
	{
		DecodedXLogRecord *record;

		/* Try to read a new future record, if we don't already have one. */
		if (prefetcher->record == NULL)
		{
			bool		nonblocking;

			/*
			 * If there are already records or an error queued up that could
			 * be replayed, we don't want to block here.  Otherwise, it's OK
			 * to block waiting for more data: presumably the caller has
			 * nothing else to do.
			 */
			nonblocking = XLogReaderHasQueuedRecordOrError(reader);

			record = XLogReadAhead(prefetcher->reader, nonblocking);
			if (record == NULL)
			{
				/*
				 * We can't read any more, due to an error or lack of data in
				 * nonblocking mode.
				 */
				return LRQ_NEXT_AGAIN;
			}

			/*
			 * If prefetching is disabled, we don't need to analyze the record
			 * or issue any prefetches.  We just need to cause one record to
			 * be decoded.
			 */
			if (!recovery_prefetch)
			{
				*lsn = InvalidXLogRecPtr;
				return LRQ_NEXT_NO_IO;
			}

			/* We have a new record to process. */
			prefetcher->record = record;
			prefetcher->next_block_id = 0;
		}
		else
		{
			/* Continue to process from last call, or last loop. */
			record = prefetcher->record;
		}

		/*
		 * Check for operations that change the identity of buffer tags. These
		 * must be treated as barriers that prevent prefetching for certain
		 * ranges of buffer tags, so that we can't be confused by OID
		 * wraparound (and later we might pin buffers).
		 *
		 * XXX Perhaps this information could be derived automatically if we
		 * had some standardized header flags and fields for these fields,
		 * instead of special logic.
		 *
		 * XXX Are there other operations that need this treatment?
		 */
		if (replaying_lsn < record->lsn)
		{
			uint8		rmid = record->header.xl_rmid;
			uint8		record_type = record->header.xl_info & ~XLR_INFO_MASK;

			if (rmid == RM_DBASE_ID)
			{
				if (record_type == XLOG_DBASE_CREATE)
				{
					xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *)
					record->main_data;
					RelFileNode rnode = {InvalidOid, xlrec->db_id, InvalidOid};

					/*
					 * Don't try to prefetch anything in this database until
					 * it has been created, or we might confuse blocks on OID
					 * wraparound.  (We could use XLOG_DBASE_DROP instead, but
					 * there shouldn't be any reference to blocks in a
					 * database between DROP and CREATE for the same OID, and
					 * doing it on CREATE avoids the more expensive
					 * ENOENT-handling if we didn't treat CREATE as a
					 * barrier).
					 */
					XLogPrefetcherAddFilter(prefetcher, rnode, 0, record->lsn);
				}
			}
			else if (rmid == RM_SMGR_ID)
			{
				if (record_type == XLOG_SMGR_CREATE)
				{
					xl_smgr_create *xlrec = (xl_smgr_create *)
					record->main_data;

					/*
					 * Don't prefetch anything for this whole relation until
					 * it has been created, or we might confuse blocks on OID
					 * wraparound.
					 */
					XLogPrefetcherAddFilter(prefetcher, xlrec->rnode, 0,
											record->lsn);
				}
				else if (record_type == XLOG_SMGR_TRUNCATE)
				{
					xl_smgr_truncate *xlrec = (xl_smgr_truncate *)
					record->main_data;

					/*
					 * Don't prefetch anything in the truncated range until
					 * the truncation has been performed.
					 */
					XLogPrefetcherAddFilter(prefetcher, xlrec->rnode,
											xlrec->blkno,
											record->lsn);
				}
			}
		}

		/* Scan the block references, starting where we left off last time. */
		while (prefetcher->next_block_id <= record->max_block_id)
		{
			int			block_id = prefetcher->next_block_id++;
			DecodedBkpBlock *block = &record->blocks[block_id];
			SMgrRelation reln;
			PrefetchBufferResult result;

			if (!block->in_use)
				continue;

			Assert(!BufferIsValid(block->prefetch_buffer));;

			/*
			 * Record the LSN of this record.  When it's replayed,
			 * LsnReadQueue will consider any IOs submitted for earlier LSNs
			 * to be finished.
			 */
			*lsn = record->lsn;

			/* We don't try to prefetch anything but the main fork for now. */
			if (block->forknum != MAIN_FORKNUM)
			{
				return LRQ_NEXT_NO_IO;
			}

			/*
			 * If there is a full page image attached, we won't be reading the
			 * page, so don't both trying to prefetch.
			 */
			if (block->has_image)
			{
				XLogPrefetchIncrement(&SharedStats->skip_fpw);
				return LRQ_NEXT_NO_IO;
			}

			/* There is no point in reading a page that will be zeroed. */
			if (block->flags & BKPBLOCK_WILL_INIT)
			{
				XLogPrefetchIncrement(&SharedStats->skip_init);
				return LRQ_NEXT_NO_IO;
			}

			/* Should we skip prefetching this block due to a filter? */
			if (XLogPrefetcherIsFiltered(prefetcher, block->rnode, block->blkno))
			{
				XLogPrefetchIncrement(&SharedStats->skip_new);
				return LRQ_NEXT_NO_IO;
			}

			/*
			 * We could try to have a fast path for repeated references to the
			 * same relation (with some scheme to handle invalidations
			 * safely), but for now we'll call smgropen() every time.
			 */
			reln = smgropen(block->rnode, InvalidBackendId);

			/*
			 * If the block is past the end of the relation, filter out
			 * further accesses until this record is replayed.
			 */
			if (block->blkno >= smgrnblocks(reln, block->forknum))
			{
				XLogPrefetcherAddFilter(prefetcher, block->rnode, block->blkno,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				return LRQ_NEXT_NO_IO;
			}

			/* Try to initiate prefetching. */
			result = PrefetchSharedBuffer(reln, block->forknum, block->blkno);
			if (BufferIsValid(result.recent_buffer))
			{
				/* Cache hit, nothing to do. */
				XLogPrefetchIncrement(&SharedStats->hit);
				block->prefetch_buffer = result.recent_buffer;
				return LRQ_NEXT_NO_IO;
			}
			else if (result.initiated_io)
			{
				/* Cache miss, I/O (presumably) started. */
				XLogPrefetchIncrement(&SharedStats->prefetch);
				block->prefetch_buffer = InvalidBuffer;
				return LRQ_NEXT_IO;
			}
			else
			{
				/*
				 * Neither cached nor initiated.  The underlying segment file
				 * doesn't exist. (ENOENT)
				 *
				 * It might be missing becaused it was unlinked, we crashed,
				 * and now we're replaying WAL.  Recovery will use correct
				 * this problem or complain if something is wrong.
				 */
				XLogPrefetcherAddFilter(prefetcher, block->rnode, 0,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				return LRQ_NEXT_NO_IO;
			}
		}

		/*
		 * Several callsites need to be able to read exactly one record
		 * without any internal readahead.  Examples: xlog.c reading
		 * checkpoint records with emode set to PANIC, which might otherwise
		 * cause XLogPageRead() to panic on some future page, and xlog.c
		 * determining where to start writing WAL next, which depends on the
		 * contents of the reader's internal buffer after reading one record.
		 * Therefore, don't even think about prefetching until the first
		 * record after XLogPrefetcherBeginRead() has been consumed.
		 */
#if 1
		if (prefetcher->reader->decode_queue_tail &&
			prefetcher->reader->decode_queue_tail->lsn == prefetcher->begin_ptr)
			return LRQ_NEXT_AGAIN;
#endif

		/* Advance to the next record. */
		prefetcher->record = NULL;
	}
	pg_unreachable();
}

/*
 * Expose statistics about recovery prefetching.
 */
Datum
pg_stat_get_prefetch_recovery(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_PREFETCH_RECOVERY_COLS 10
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[PG_STAT_GET_PREFETCH_RECOVERY_COLS];
	bool		nulls[PG_STAT_GET_PREFETCH_RECOVERY_COLS];

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mod required, but it is not allowed in this context")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (pg_atomic_read_u32(&SharedStats->reset_request) != SharedStats->reset_handled)
	{
		/* There's an unhandled reset request, so just show NULLs */
		for (int i = 0; i < PG_STAT_GET_PREFETCH_RECOVERY_COLS; ++i)
			nulls[i] = true;
	}
	else
	{
		for (int i = 0; i < PG_STAT_GET_PREFETCH_RECOVERY_COLS; ++i)
			nulls[i] = false;
	}

	values[0] = TimestampTzGetDatum(pg_atomic_read_u64(&SharedStats->reset_time));
	values[1] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->prefetch));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->hit));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_init));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_new));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_fpw));
	values[6] = Int32GetDatum(SharedStats->wal_distance);
	values[7] = Int32GetDatum(SharedStats->block_distance);
	values[8] = Int32GetDatum(SharedStats->io_depth);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Don't prefetch any blocks >= 'blockno' from a given 'rnode', until 'lsn'
 * has been replayed.
 */
static inline void
XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher, RelFileNode rnode,
						BlockNumber blockno, XLogRecPtr lsn)
{
	XLogPrefetcherFilter *filter;
	bool		found;

	filter = hash_search(prefetcher->filter_table, &rnode, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * Don't allow any prefetching of this block or higher until replayed.
		 */
		filter->filter_until_replayed = lsn;
		filter->filter_from_block = blockno;
		dlist_push_head(&prefetcher->filter_queue, &filter->link);
	}
	else
	{
		/*
		 * We were already filtering this rnode.  Extend the filter's lifetime
		 * to cover this WAL record, but leave the lower of the block numbers
		 * there because we don't want to have to track individual blocks.
		 */
		filter->filter_until_replayed = lsn;
		dlist_delete(&filter->link);
		dlist_push_head(&prefetcher->filter_queue, &filter->link);
		filter->filter_from_block = Min(filter->filter_from_block, blockno);
	}
}

/*
 * Have we replayed any records that caused us to begin filtering a block
 * range?  That means that relations should have been created, extended or
 * dropped as required, so we can stop filtering out accesses to a given
 * relfilenode.
 */
static inline void
XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher, XLogRecPtr replaying_lsn)
{
	while (unlikely(!dlist_is_empty(&prefetcher->filter_queue)))
	{
		XLogPrefetcherFilter *filter = dlist_tail_element(XLogPrefetcherFilter,
														  link,
														  &prefetcher->filter_queue);

		if (filter->filter_until_replayed >= replaying_lsn)
			break;

		dlist_delete(&filter->link);
		hash_search(prefetcher->filter_table, filter, HASH_REMOVE, NULL);
	}
}

/*
 * Check if a given block should be skipped due to a filter.
 */
static inline bool
XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher, RelFileNode rnode,
						 BlockNumber blockno)
{
	/*
	 * Test for empty queue first, because we expect it to be empty most of
	 * the time and we can avoid the hash table lookup in that case.
	 */
	if (unlikely(!dlist_is_empty(&prefetcher->filter_queue)))
	{
		XLogPrefetcherFilter *filter;

		/* See if the block range is filtered. */
		filter = hash_search(prefetcher->filter_table, &rnode, HASH_FIND, NULL);
		if (filter && filter->filter_from_block <= blockno)
			return true;

		/* See if the whole database is filtered. */
		rnode.relNode = InvalidOid;
		filter = hash_search(prefetcher->filter_table, &rnode, HASH_FIND, NULL);
		if (filter && filter->filter_from_block <= blockno)
			return true;
	}

	return false;
}

/*
 * A wrapper for XLogBeginRead() that also resets the prefetcher.
 */
void
XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher,
						XLogRecPtr recPtr)
{
	/* This will forget about any in-flight IO. */
	prefetcher->reconfigure_count--;

	/* Book-keeping to avoid readahead on first read. */
	prefetcher->begin_ptr = recPtr;

	/* This will forget about any queued up records in the decoder. */
	XLogBeginRead(prefetcher->reader, recPtr);
}

/*
 * A wrapper for XLogReadRecord() that provides the same interface, but also
 * tries to initiate IO ahead of time unless asked not to.
 */
XLogRecord *
XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
						 char **errmsg)
{
	DecodedXLogRecord *record;

	/*
	 * See if it's time to reset the prefetching machinery, because a relevant
	 * GUC was changed.
	 */
	if (unlikely(XLogPrefetchReconfigureCount != prefetcher->reconfigure_count))
	{
		if (prefetcher->streaming_read)
			lrq_free(prefetcher->streaming_read);

		/*
		 * Arbitrarily look up to 4 times further ahead than the number of IOs
		 * we're allowed to run concurrently.
		 */
		prefetcher->streaming_read =
			lrq_alloc(recovery_prefetch ? maintenance_io_concurrency * 4 : 1,
					  recovery_prefetch ? maintenance_io_concurrency : 1,
					  (uintptr_t) prefetcher,
					  XLogPrefetcherNextBlock);

		prefetcher->reconfigure_count = XLogPrefetchReconfigureCount;
	}

	/*
	 * Release last returned record, if there is one.  We need to do this so
	 * that we can check for empty decode queue accurately.
	 */
	XLogReleasePreviousRecord(prefetcher->reader);

	/* If there's nothing queued yet, then start prefetching. */
	if (!XLogReaderHasQueuedRecordOrError(prefetcher->reader))
		lrq_prefetch(prefetcher->streaming_read);

	/* Read the next record. */
	record = XLogNextRecord(prefetcher->reader, errmsg);
	if (!record)
		return NULL;

	/*
	 * The record we just got is the "current" one, for the benefit of the
	 * XLogRecXXX() macros.
	 */
	Assert(record == prefetcher->reader->record);

	/*
	 * Can we drop any prefetch filters yet, given the record we're about to
	 * return?  This assumes that any records with earlier LSNs have been
	 * replayed, so if we were waiting for a relation to be created or
	 * extended, it is now OK to access blocks in the covered range.
	 */
	XLogPrefetcherCompleteFilters(prefetcher, record->lsn);

	/*
	 * See if it's time to compute some statistics, because enough WAL has
	 * been processed.
	 */
	if (unlikely(record->lsn >= prefetcher->next_stats_shm_lsn))
		XLogPrefetcherComputeStats(prefetcher, record->lsn);

	/*
	 * The caller is about to replay this record, so we can now report that
	 * all IO initiated because of early WAL must be finished. This may
	 * trigger more readahead.
	 */
	lrq_complete_lsn(prefetcher->streaming_read, record->lsn);

	Assert(record == prefetcher->reader->record);

	return &record->header;
}

bool
check_recovery_prefetch(bool *new_value, void **extra, GucSource source)
{
#ifndef USE_PREFETCH
	if (*new_value)
	{
		GUC_check_errdetail("recovery_prefetch must be set to off on platforms that lack posix_fadvise().");
		return false;
	}
#endif

	return true;
}

void
assign_recovery_prefetch(bool new_value, void *extra)
{
	/* Reconfigure prefetching, because a setting it depends on changed. */
	recovery_prefetch = new_value;
	if (AmStartupProcess())
		XLogPrefetchReconfigure();
}
