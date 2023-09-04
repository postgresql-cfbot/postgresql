/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.c
 *		Prefetching support for recovery.
 *
 * Portions Copyright (c) 2022-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/backend/access/transam/xlogprefetcher.c
 *
 * This module provides a drop-in replacement for an XLogReader that tries to
 * minimize I/O stalls by looking ahead in the WAL.  If blocks that will be
 * accessed in the near future are not already in the buffer pool, it initiates
 * I/Os that might complete before the caller eventually needs the data.  When
 * referenced blocks are found in the buffer pool already, the buffer is
 * recorded in the decoded record so that XLogReadBufferForRedo() can try to
 * avoid a second buffer mapping table lookup.
 *
 * Currently, only the main fork is considered for prefetching.  Currently,
 * prefetching is only effective on systems where PrefetchBuffer() does
 * something useful (mainly Linux).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
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
#include "storage/streaming_read.h"
#include "utils/guc_hooks.h"
#include "utils/hsearch.h"

/*
 * Every time we process this much WAL, we'll update the values in
 * pg_stat_recovery_prefetch.
 */
#define XLOGPREFETCHER_STATS_DISTANCE BLCKSZ

/*
 * When maintenance_io_concurrency is not saturated, we're prepared to look
 * ahead up to N times that number of block references.
 */
#define XLOGPREFETCHER_DISTANCE_MULTIPLIER 4

/* Define to log internal debugging messages. */
/* #define XLOGPREFETCHER_DEBUG_LEVEL LOG */

/* GUCs */
int			recovery_prefetch = RECOVERY_PREFETCH_ON;

#define RecoveryPrefetchEnabled() \
		(recovery_prefetch != RECOVERY_PREFETCH_OFF)

static int	XLogPrefetchReconfigureCount = 0;

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

	/* Book-keeping to avoid accessing blocks that don't exist yet. */
	HTAB	   *filter_table;
	dlist_head	filter_queue;

	/* Book-keeping to disable prefetching temporarily. */
	XLogRecPtr	no_readahead_until;

	/* IO depth manager. */
	PgStreamingRead *streaming_read;

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
	RelFileLocator rlocator;
	XLogRecPtr	filter_until_replayed;
	BlockNumber filter_from_block;
	dlist_node	link;
} XLogPrefetcherFilter;

/*
 * Counters exposed in shared memory for pg_stat_recovery_prefetch.
 */
typedef struct XLogPrefetchStats
{
	pg_atomic_uint64 reset_time;	/* Time of last reset. */
	pg_atomic_uint64 prefetch;	/* Prefetches initiated. */
	pg_atomic_uint64 hit;		/* Blocks already in cache. */
	pg_atomic_uint64 skip_init; /* Zero-inited blocks skipped. */
	pg_atomic_uint64 skip_new;	/* New/missing blocks filtered. */
	pg_atomic_uint64 skip_fpw;	/* FPWs skipped. */
	pg_atomic_uint64 skip_rep;	/* Repeat accesses skipped. */

	/* Dynamic values */
	int			wal_distance;	/* Number of WAL bytes ahead. */
	int			block_distance; /* Number of block references ahead. */
	int			io_depth;		/* Number of I/Os in progress. */
} XLogPrefetchStats;

static inline void XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher,
										   RelFileLocator rlocator,
										   BlockNumber blockno,
										   XLogRecPtr lsn);
static inline bool XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher,
											RelFileLocator rlocator,
											BlockNumber blockno);
static inline void XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher,
												 XLogRecPtr replaying_lsn);
static bool XLogPrefetcherNextBlock(PgStreamingRead *pgsr,
									uintptr_t pgsr_prviate,
									void *per_io_data,
									BufferManagerRelation *bmr,
									ForkNumber *forknum,
									BlockNumber *blocknum,
									ReadBufferMode *mode);

static XLogPrefetchStats *SharedStats;

size_t
XLogPrefetchShmemSize(void)
{
	return sizeof(XLogPrefetchStats);
}

/*
 * Reset all counters to zero.
 */
void
XLogPrefetchResetStats(void)
{
	pg_atomic_write_u64(&SharedStats->reset_time, GetCurrentTimestamp());
	pg_atomic_write_u64(&SharedStats->prefetch, 0);
	pg_atomic_write_u64(&SharedStats->hit, 0);
	pg_atomic_write_u64(&SharedStats->skip_init, 0);
	pg_atomic_write_u64(&SharedStats->skip_new, 0);
	pg_atomic_write_u64(&SharedStats->skip_fpw, 0);
	pg_atomic_write_u64(&SharedStats->skip_rep, 0);
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
		pg_atomic_init_u64(&SharedStats->reset_time, GetCurrentTimestamp());
		pg_atomic_init_u64(&SharedStats->prefetch, 0);
		pg_atomic_init_u64(&SharedStats->hit, 0);
		pg_atomic_init_u64(&SharedStats->skip_init, 0);
		pg_atomic_init_u64(&SharedStats->skip_new, 0);
		pg_atomic_init_u64(&SharedStats->skip_fpw, 0);
		pg_atomic_init_u64(&SharedStats->skip_rep, 0);
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
		.keysize = sizeof(RelFileLocator),
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
	/*
	 * Redo records are normally expected to read and then release any buffers
	 * referenced by a WAL record, but we may need to release buffers for the
	 * final block if recovery ends without replaying a record.
	 */
	if (prefetcher->reader->record)
	{
		DecodedXLogRecord *last_record = prefetcher->reader->record;

		for (int i = 0; i <= last_record->max_block_id; ++i)
		{
			if (last_record->blocks[i].in_use &&
				BufferIsValid(last_record->blocks[i].prefetch_buffer))
				ReleaseBuffer(last_record->blocks[i].prefetch_buffer);
		}
	}
	pg_streaming_read_free(prefetcher->streaming_read);
	hash_destroy(prefetcher->filter_table);
	pfree(prefetcher);
}

/*
 * Provide access to the reader.
 */
XLogReaderState *
XLogPrefetcherGetReader(XLogPrefetcher *prefetcher)
{
	return prefetcher->reader;
}

/*
 * Update the statistics visible in the pg_stat_recovery_prefetch view.
 */
void
XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher)
{
	int			ios;
	int			pins;
	int64		wal_distance;


	/* How far ahead of replay are we now? */
	if (prefetcher->reader->decode_queue_tail)
	{
		wal_distance =
			prefetcher->reader->decode_queue_tail->lsn -
			prefetcher->reader->decode_queue_head->lsn;
	}
	else
	{
		wal_distance = 0;
	}

	/* How many IOs are currently in progress, and how many pins do we have? */
	ios = pg_streaming_read_ios(prefetcher->streaming_read);
	pins = pg_streaming_read_pins(prefetcher->streaming_read);

	/* Update the instantaneous stats visible in pg_stat_recovery_prefetch. */
	SharedStats->io_depth = ios;
	SharedStats->block_distance = pins;
	SharedStats->wal_distance = wal_distance;

	prefetcher->next_stats_shm_lsn =
		prefetcher->reader->ReadRecPtr + XLOGPREFETCHER_STATS_DISTANCE;
}

/*
 * A PgStreamingRead callback that generates a stream of block references by
 * looking ahead in the WAL, which XLogPrefetcherReadRecord() will later
 * retrieve.
 */
static bool
XLogPrefetcherNextBlock(PgStreamingRead *pgsr,
						uintptr_t pgsr_private,
						void *per_io_data,
						BufferManagerRelation *bmr,
						ForkNumber *forknum,
						BlockNumber *blocknum,
						ReadBufferMode *mode)
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
		uint8		rmid;
		uint8		record_type;

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

			/* Readahead is disabled until we replay past a certain point. */
			if (nonblocking && replaying_lsn <= prefetcher->no_readahead_until)
				return false;

			record = XLogReadAhead(prefetcher->reader, nonblocking);
			if (record == NULL)
			{
				/*
				 * We can't read any more, due to an error or lack of data in
				 * nonblocking mode.  Don't try to read ahead again until
				 * we've replayed everything already decoded.
				 */
				if (nonblocking && prefetcher->reader->decode_queue_tail)
					prefetcher->no_readahead_until =
						prefetcher->reader->decode_queue_tail->lsn;

				return false;
			}

			/* We have a new record to process. */
			prefetcher->record = record;
			prefetcher->next_block_id = 0;

			/*
			 * If prefetching is disabled, we don't need to analyze the
			 * record.  We just needed to cause one record to be decoded, so
			 * we can give up now.
			 */
			if (!RecoveryPrefetchEnabled())
				return false;
		}
		else
		{
			/* Continue to process from last call, or last loop. */
			record = prefetcher->record;
		}

		rmid = record->header.xl_rmid;
		record_type = record->header.xl_info & ~XLR_INFO_MASK;

		/*
		 * Check for operations that require us to filter out block ranges, or
		 * pause readahead completely.
		 */
		if (replaying_lsn < record->lsn)
		{
			if (rmid == RM_XLOG_ID)
			{
				if (record_type == XLOG_CHECKPOINT_SHUTDOWN ||
					record_type == XLOG_END_OF_RECOVERY)
				{
					/*
					 * These records might change the TLI.  Avoid potential
					 * bugs if we were to allow "read TLI" and "replay TLI" to
					 * differ without more analysis.
					 */
					prefetcher->no_readahead_until = record->lsn;

#ifdef XLOGPREFETCHER_DEBUG_LEVEL
					elog(XLOGPREFETCHER_DEBUG_LEVEL,
						 "suppressing all readahead until %X/%X is replayed due to possible TLI change",
						 LSN_FORMAT_ARGS(record->lsn));
#endif

					/* Fall through so we move past this record. */
				}
			}
			else if (rmid == RM_DBASE_ID)
			{
				/*
				 * When databases are created with the file-copy strategy,
				 * there are no WAL records to tell us about the creation of
				 * individual relations.
				 */
				if (record_type == XLOG_DBASE_CREATE_FILE_COPY)
				{
					xl_dbase_create_file_copy_rec *xlrec =
						(xl_dbase_create_file_copy_rec *) record->main_data;
					RelFileLocator rlocator =
					{InvalidOid, xlrec->db_id, InvalidRelFileNumber};

					/*
					 * Don't try to prefetch anything in this database until
					 * it has been created, or we might confuse the blocks of
					 * different generations, if a database OID or
					 * relfilenumber is reused.  It's also more efficient than
					 * discovering that relations don't exist on disk yet with
					 * ENOENT errors.
					 */
					XLogPrefetcherAddFilter(prefetcher, rlocator, 0, record->lsn);

#ifdef XLOGPREFETCHER_DEBUG_LEVEL
					elog(XLOGPREFETCHER_DEBUG_LEVEL,
						 "suppressing prefetch in database %u until %X/%X is replayed due to raw file copy",
						 rlocator.dbOid,
						 LSN_FORMAT_ARGS(record->lsn));
#endif

					/*
					 * Don't prefetch anything in the source database either,
					 * because FlushDatabaseBuffers() doesn't like to see any
					 * pinned buffers.
					 */
					rlocator.dbOid = xlrec->src_db_id;
					XLogPrefetcherAddFilter(prefetcher, rlocator, 0, record->lsn);

#ifdef XLOGPREFETCHER_DEBUG_LEVEL
					elog(XLOGPREFETCHER_DEBUG_LEVEL,
						 "suppressing prefetch in database %u until %X/%X is replayed due to raw file copy",
						 rlocator.dbOid,
						 LSN_FORMAT_ARGS(record->lsn));
#endif
				}
				else if (record_type == XLOG_DBASE_CREATE_WAL_LOG)
				{
					xl_dbase_create_wal_log_rec *xlrec =
						(xl_dbase_create_wal_log_rec *) record->main_data;
					RelFileLocator rlocator =
					{InvalidOid, xlrec->db_id, InvalidRelFileNumber};

					/*
					 * As above, we don't want to pin buffers on the other
					 * side of a DROP, CREATE DATABASE sequence that recycles
					 * database OIDs.
					 */
					XLogPrefetcherAddFilter(prefetcher, rlocator, 0, record->lsn);
				}
				else if (record_type == XLOG_DBASE_DROP)
				{
					xl_dbase_drop_rec *xlrec =
						(xl_dbase_drop_rec *) record->main_data;
					RelFileLocator rlocator =
					{InvalidOid, xlrec->db_id, InvalidRelFileNumber};

					/*
					 * XLOG_DBASE_DROP can be used while moving between
					 * tablespaces, and in that case it invalidates buffers
					 * from all tablespaces.  That means that we also have to
					 * wait for the drop before prefetching anything in this
					 * whole DB.
					 */
					XLogPrefetcherAddFilter(prefetcher, rlocator, 0, record->lsn);
				}
			}
			else if (rmid == RM_SMGR_ID)
			{
				if (record_type == XLOG_SMGR_CREATE)
				{
					xl_smgr_create *xlrec = (xl_smgr_create *)
						record->main_data;

					if (xlrec->forkNum == MAIN_FORKNUM)
					{
						/*
						 * Don't prefetch anything for this whole relation
						 * until it has been created.  Otherwise we might
						 * confuse the blocks of different generations, if a
						 * relfilenumber is reused.  This also avoids the need
						 * to discover the problem via extra syscalls that
						 * report ENOENT.
						 */
						XLogPrefetcherAddFilter(prefetcher, xlrec->rlocator, 0,
												record->lsn);

#ifdef XLOGPREFETCHER_DEBUG_LEVEL
						elog(XLOGPREFETCHER_DEBUG_LEVEL,
							 "suppressing prefetch in relation %u/%u/%u until %X/%X is replayed, which creates the relation",
							 xlrec->rlocator.spcOid,
							 xlrec->rlocator.dbOid,
							 xlrec->rlocator.relNumber,
							 LSN_FORMAT_ARGS(record->lsn));
#endif
					}
				}
				else if (record_type == XLOG_SMGR_TRUNCATE)
				{
					xl_smgr_truncate *xlrec = (xl_smgr_truncate *)
						record->main_data;

					/*
					 * Don't consider prefetching anything in the truncated
					 * range until the truncation has been performed.
					 */
					XLogPrefetcherAddFilter(prefetcher, xlrec->rlocator,
											xlrec->blkno,
											record->lsn);

#ifdef XLOGPREFETCHER_DEBUG_LEVEL
					elog(XLOGPREFETCHER_DEBUG_LEVEL,
						 "suppressing prefetch in relation %u/%u/%u from block %u until %X/%X is replayed, which truncates the relation",
						 xlrec->rlocator.spcOid,
						 xlrec->rlocator.dbOid,
						 xlrec->rlocator.relNumber,
						 xlrec->blkno,
						 LSN_FORMAT_ARGS(record->lsn));
#endif
				}
			}
			else if (rmid == RM_XACT_ID)
			{
				/*
				 * If this is a COMMIT/ABORT that drops relations, the
				 * SMgrRelation pointers to those relations that we have
				 * already queued for streaming read will become dangling
				 * pointers after this is replayed due to smgrclose(), so
				 * pause prefetching here until that's done.
				 */
				if (unlikely(record->header.xl_info & XACT_XINFO_HAS_RELFILELOCATORS))
				{
					RelFileLocator *xlocators;
					int			nrels = 0;

					if (record_type == XLOG_XACT_COMMIT ||
						record_type == XLOG_XACT_COMMIT_PREPARED)
					{
						xl_xact_parsed_commit parsed;

						ParseCommitRecord(record->header.xl_info,
										  (xl_xact_commit *) record->main_data,
										  &parsed);

						xlocators = parsed.xlocators;
						nrels = parsed.nrels;
					}
					else if (record_type == XLOG_XACT_ABORT)
					{
						xl_xact_parsed_abort parsed;

						ParseAbortRecord(record->header.xl_info,
										 (xl_xact_abort *) record->main_data,
										 &parsed);

						xlocators = parsed.xlocators;
						nrels = parsed.nrels;
					}

					/*
					 * Filter out these relations until the record is
					 * replayed.
					 */
					for (int i = 0; i < nrels; ++i)
						XLogPrefetcherAddFilter(prefetcher, xlocators[i], 0,
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

			if (!block->in_use)
				continue;

			Assert(!BufferIsValid(block->prefetch_buffer));
			Assert(!block->prefetch_buffer_streamed);

			/*
			 * We only stream the main fork only, for now.  Some of the other
			 * forks require RBM_ZERO_ON_ERROR and have unusual logging
			 * protocols.
			 */
			if (block->forknum != MAIN_FORKNUM)
				continue;

			/*
			 * FPI_FOR_HINT is special, with full_page_writes = off and
			 * wal_log_hints = true, the record is solely used to include
			 * knowledge about modified blocks in the WAL.
			 */
			if (rmid == RM_XLOG_ID && record_type == XLOG_FPI_FOR_HINT &&
				!block->has_image)
			{
				XLogPrefetchIncrement(&SharedStats->skip_fpw);
				continue;
			}

			/* Should we skip prefetching this block due to a filter? */
			if (XLogPrefetcherIsFiltered(prefetcher, block->rlocator, block->blkno))
			{
				XLogPrefetchIncrement(&SharedStats->skip_new);
				continue;
			}

			/*
			 * We could try to have a fast path for repeated references to the
			 * same relation (with some scheme to handle invalidations
			 * safely), but for now we'll call smgropen() every time.
			 */
			reln = smgropen(block->rlocator, InvalidBackendId);

			/*
			 * If the relation file doesn't exist on disk, for example because
			 * we're replaying after a crash and the file will be created and
			 * then unlinked by WAL that hasn't been replayed yet, suppress
			 * further prefetching in the relation until this record is
			 * replayed.
			 */
			if (!smgrexists(reln, MAIN_FORKNUM))
			{
#ifdef XLOGPREFETCHER_DEBUG_LEVEL
				elog(XLOGPREFETCHER_DEBUG_LEVEL,
					 "suppressing all prefetch in relation %u/%u/%u until %X/%X is replayed, because the relation does not exist on disk",
					 reln->smgr_rlocator.locator.spcOid,
					 reln->smgr_rlocator.locator.dbOid,
					 reln->smgr_rlocator.locator.relNumber,
					 LSN_FORMAT_ARGS(record->lsn));
#endif
				XLogPrefetcherAddFilter(prefetcher, block->rlocator, 0,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				continue;
			}

			/*
			 * If the relation isn't big enough to contain the referenced
			 * block yet, suppress prefetching of this block and higher until
			 * this record is replayed.
			 */
			if (block->blkno >= smgrnblocks(reln, block->forknum))
			{
#ifdef XLOGPREFETCHER_DEBUG_LEVEL
				elog(XLOGPREFETCHER_DEBUG_LEVEL,
					 "suppressing prefetch in relation %u/%u/%u from block %u until %X/%X is replayed, because the relation is too small",
					 reln->smgr_rlocator.locator.spcOid,
					 reln->smgr_rlocator.locator.dbOid,
					 reln->smgr_rlocator.locator.relNumber,
					 block->blkno,
					 LSN_FORMAT_ARGS(record->lsn));
#endif
				XLogPrefetcherAddFilter(prefetcher, block->rlocator, block->blkno,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				continue;
			}

			/*
			 * Stream this block!  It will be looked up and pinned by the
			 * streaming read (possibly combining I/Os with nearby blocks),
			 * and XLogPrefetcherReadRecord() will consume it from the
			 * streaming read object, and make it available for
			 * XLogReadBufferForRedo() to provide to a redo routine.
			 */
			block->prefetch_buffer_streamed = true;

			/*
			 * If there's an FPI or redo is just going to zero it, we can skip
			 * a useless I/O, and stream a pinned buffer that
			 * XLogReadBufferForRedo() will pass to ZeroBuffer().
			 */
			if (block->apply_image ||
				(block->flags & BKPBLOCK_WILL_INIT))
				*mode = RBM_WILL_ZERO;
			else
				*mode = RBM_NORMAL;
			*bmr = BMR_SMGR(reln, RELPERSISTENCE_PERMANENT);
			*forknum = block->forknum;
			*blocknum = block->blkno;

			return true;
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
		if (prefetcher->reader->decode_queue_tail &&
			prefetcher->reader->decode_queue_tail->lsn == prefetcher->begin_ptr)
			return false;

		/* Advance to the next record. */
		prefetcher->record = NULL;
	}
	pg_unreachable();
}

/*
 * Expose statistics about recovery prefetching.
 */
Datum
pg_stat_get_recovery_prefetch(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_RECOVERY_PREFETCH_COLS 10
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[PG_STAT_GET_RECOVERY_PREFETCH_COLS];
	bool		nulls[PG_STAT_GET_RECOVERY_PREFETCH_COLS];

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < PG_STAT_GET_RECOVERY_PREFETCH_COLS; ++i)
		nulls[i] = false;

	values[0] = TimestampTzGetDatum(pg_atomic_read_u64(&SharedStats->reset_time));
	values[1] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->prefetch));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->hit));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_init));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_new));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_fpw));
	values[6] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_rep));
	values[7] = Int32GetDatum(SharedStats->wal_distance);
	values[8] = Int32GetDatum(SharedStats->block_distance);
	values[9] = Int32GetDatum(SharedStats->io_depth);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum) 0;
}

/*
 * Don't prefetch any blocks >= 'blockno' from a given 'rlocator', until 'lsn'
 * has been replayed.
 */
static inline void
XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher, RelFileLocator rlocator,
						BlockNumber blockno, XLogRecPtr lsn)
{
	XLogPrefetcherFilter *filter;
	bool		found;

	filter = hash_search(prefetcher->filter_table, &rlocator, HASH_ENTER, &found);
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
		 * We were already filtering this rlocator.  Extend the filter's
		 * lifetime to cover this WAL record, but leave the lower of the block
		 * numbers there because we don't want to have to track individual
		 * blocks.
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
 * relfilenumber.
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
XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher, RelFileLocator rlocator,
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
		filter = hash_search(prefetcher->filter_table, &rlocator, HASH_FIND, NULL);
		if (filter && filter->filter_from_block <= blockno)
		{
#ifdef XLOGPREFETCHER_DEBUG_LEVEL
			elog(XLOGPREFETCHER_DEBUG_LEVEL,
				 "prefetch of %u/%u/%u block %u suppressed; filtering until LSN %X/%X is replayed (blocks >= %u filtered)",
				 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, blockno,
				 LSN_FORMAT_ARGS(filter->filter_until_replayed),
				 filter->filter_from_block);
#endif
			return true;
		}

		/* See if the whole database is filtered. */
		rlocator.relNumber = InvalidRelFileNumber;
		rlocator.spcOid = InvalidOid;
		filter = hash_search(prefetcher->filter_table, &rlocator, HASH_FIND, NULL);
		if (filter)
		{
#ifdef XLOGPREFETCHER_DEBUG_LEVEL
			elog(XLOGPREFETCHER_DEBUG_LEVEL,
				 "prefetch of %u/%u/%u block %u suppressed; filtering until LSN %X/%X is replayed (whole database)",
				 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, blockno,
				 LSN_FORMAT_ARGS(filter->filter_until_replayed));
#endif
			return true;
		}
	}

	return false;
}

/*
 * A wrapper for XLogBeginRead() that also resets the prefetcher.
 */
void
XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher, XLogRecPtr recPtr)
{
	/* This will forget about any in-flight IO. */
	prefetcher->reconfigure_count--;

	/* Book-keeping to avoid readahead on first read. */
	prefetcher->begin_ptr = recPtr;

	prefetcher->no_readahead_until = 0;

	/* This will forget about any queued up records in the decoder. */
	XLogBeginRead(prefetcher->reader, recPtr);
}

/*
 * A wrapper for XLogReadRecord() that provides the same interface, but also
 * tries to initiate I/O for blocks referenced in future WAL records.
 */
XLogRecord *
XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher, char **errmsg)
{
	DecodedXLogRecord *record;
	XLogRecPtr	replayed_up_to;

#ifdef USE_ASSERT_CHECKING

	/*
	 * The recovery code (ie individual redo routines) should have called
	 * XLogReadBufferForRedo() for all registered buffers.  Here we'll assert
	 * that that's the case.
	 */
	if (prefetcher->reader->record)
	{
		DecodedXLogRecord *last_record = prefetcher->reader->record;

		for (int i = 0; i <= last_record->max_block_id; ++i)
		{
			if (last_record->blocks[i].in_use &&
				BufferIsValid(last_record->blocks[i].prefetch_buffer))
				elog(ERROR,
					 "redo routine did not read buffer pinned by prefetcher, LSN %X/%X",
					 LSN_FORMAT_ARGS(last_record->lsn));
		}
	}
#endif

	/*
	 * See if it's time to reset the prefetching machinery, because a relevant
	 * GUC was changed.
	 */
	if (unlikely(XLogPrefetchReconfigureCount != prefetcher->reconfigure_count))
	{
		int			max_ios;

		if (prefetcher->streaming_read)
			pg_streaming_read_free(prefetcher->streaming_read);

		if (RecoveryPrefetchEnabled())
		{
			/*
			 * maintenance_io_concurrency might be 0 on some systems, but we
			 * need at least 1 to function.
			 */
			max_ios = Max(maintenance_io_concurrency, 1);
		}
		else
		{
			/*
			 * Non-zero values are needed, but the NextBlock callback will
			 * never actually do any prefetching, so the streaming read will
			 * not be used except as a way to force records to be decoded one
			 * by one.
			 */
			max_ios = 1;
		}

		prefetcher->streaming_read =
			pg_streaming_read_buffer_alloc(max_ios,
										   0,
										   (uintptr_t) prefetcher,
										   NULL,
										   XLogPrefetcherNextBlock);

		prefetcher->reconfigure_count = XLogPrefetchReconfigureCount;
	}

	/*
	 * Release last returned record, if there is one, as it's now been
	 * replayed.
	 */
	replayed_up_to = XLogReleasePreviousRecord(prefetcher->reader);

	/*
	 * Can we drop any filters yet?  If we were waiting for a relation to be
	 * created or extended, it is now OK to access blocks in the covered
	 * range.
	 */
	XLogPrefetcherCompleteFilters(prefetcher, replayed_up_to);

	/*
	 * If there's nothing queued yet, then start prefetching.  Normally this
	 * happens automatically when we call streaming_read_get_next() below to
	 * complete earlier IOs, but if we didn't have a special case for an empty
	 * queue we'd never be able to get started.
	 */
	if (!XLogReaderHasQueuedRecordOrError(prefetcher->reader))
	{
		pg_streaming_read_reset(prefetcher->streaming_read);
		pg_streaming_read_prefetch(prefetcher->streaming_read);
	}

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
	 * If maintenance_io_concurrency is set very low, we might have started
	 * prefetching some but not all of the blocks referenced in the record
	 * we're about to return.  Forget about the rest of the blocks in this
	 * record by dropping the prefetcher's reference to it.
	 */
	if (record == prefetcher->record)
		prefetcher->record = NULL;

	/*
	 * See if it's time to compute some statistics, because enough WAL has
	 * been processed.
	 */
	if (unlikely(record->lsn >= prefetcher->next_stats_shm_lsn))
		XLogPrefetcherComputeStats(prefetcher);

	/*
	 * Make sure that any IOs we initiated earlier for this record are
	 * completed, by pulling the buffers out of the StreamingRead.
	 */
	for (int block_id = 0; block_id <= record->max_block_id; ++block_id)
	{
		DecodedBkpBlock *block = &record->blocks[block_id];
		Buffer		buffer;

		if (!block->in_use)
			continue;

		/*
		 * If we haven't streamed this buffer, then
		 * XLogReadBufferForRedoExtended() will just have to read it the
		 * traditional way.
		 */
		if (!block->prefetch_buffer_streamed)
			continue;

		Assert(!BufferIsValid(block->prefetch_buffer));

		/*
		 * Otherwise we already have a pinned buffer waiting for us in the
		 * streaming read.
		 */
		buffer = pg_streaming_read_buffer_get_next(prefetcher->streaming_read, NULL);

		if (buffer == InvalidBuffer)
			elog(PANIC, "unexpectedly ran out of buffers in streaming read");

		block->prefetch_buffer = buffer;
		block->prefetch_buffer_streamed = false;

		/*
		 * Assert that we're in sync with XLogPrefetcherNextBlock(), which is
		 * feeding blocks into the far end of the pipe.  For every decoded
		 * block that has the prefetch_buffer_streamed flag set, in order, we
		 * expect the corresponding already-pinned buffer to be the next to
		 * come out of streaming_read.
		 */
#ifdef USE_ASSERT_CHECKING
		{
			RelFileLocator rlocator;
			ForkNumber	forknum;
			BlockNumber blocknum;

			BufferGetTag(buffer, &rlocator, &forknum, &blocknum);
			Assert(RelFileLocatorEquals(rlocator, block->rlocator));
			Assert(forknum == block->forknum);
			Assert(blocknum == block->blkno);
		}
#endif
	}

	Assert(record == prefetcher->reader->record);

	return &record->header;
}

void
assign_recovery_prefetch(int new_value, void *extra)
{
	/* Reconfigure prefetching, because a setting it depends on changed. */
	recovery_prefetch = new_value;
	if (AmStartupProcess())
		XLogPrefetchReconfigure();
}
