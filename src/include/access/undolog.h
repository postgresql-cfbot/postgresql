/*-------------------------------------------------------------------------
 *
 * undolog.h
 *
 * PostgreSQL undo log manager.  This module is responsible for lifecycle
 * management of undo logs and backing files, associating undo logs with
 * backends, allocating and managing space within undo logs.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undolog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOLOG_H
#define UNDOLOG_H

#include "access/undodefs.h"
#include "lib/ilist.h"

#ifndef FRONTEND
#include "storage/lwlock.h"
#endif

/* printf-family format string for UndoRecPtr. */
#define UndoRecPtrFormat "%016" INT64_MODIFIER "X"

/* how large a buffer do we need for that format string? */
#define UndoRecPtrFormatBufferLength	17

/* printf-family format string for UndoLogOffset. */
#define UndoLogOffsetFormat UINT64_FORMAT

/*
 * UNDO_DEBUG can be defined to make the log file and its segments much smaller
 * so that it's easier to simulate what happens when a transaction needs
 * multiple segments or even logs.
 */

/* Number of blocks of BLCKSZ in an undo log segment file.  128 = 1MB. */
#ifndef UNDO_DEBUG
#define UNDOSEG_SIZE 128
#else
#define UNDOSEG_SIZE 4			/* 32 kB */
#endif

/* Size of an undo log segment file in bytes. */
#define UndoLogSegmentSize ((size_t) BLCKSZ * UNDOSEG_SIZE)

/*
 * The width of an undo log number in bits.  24 allows for 16.7m logs.
 *
 * It's probably not necessary to define higher value for the UNDO_DEBUG build
 * as that should not be used in production systems. To cope with the smaller
 * logs, the log number would need more than 32 bits and so we'd need specific
 * formatting for the UNDO_DEBUG build.
*/
#define UndoLogNumberBits 24

/* The maximum valid undo log number. */
#define MaxUndoLogNumber ((1 << UndoLogNumberBits) - 1)

/* The width of an undo log offset in bits.  40 allows for 1TB per log.*/
#ifndef UNDO_DEBUG
#define UndoLogOffsetBits (64 - UndoLogNumberBits)
#else
/*
 * We want maximum offset 2^17 = 128 kB (64 kB would be too little because
 * only two segments would fit into it, so we would only test boundaries of
 * the log and segment recycling wouldn't work), however we do not increase
 * the size of the log number so it still fits into integer type.
*/
#define UndoLogOffsetBits (41 - UndoLogNumberBits)
#endif

/* End-of-list value when building linked lists of undo logs. */
#define InvalidUndoLogNumber -1

/*
 * The maximum amount of data that can be stored in an undo log.  Can be set
 * artificially low to test full log behavior.
 */
#define UndoLogMaxSize ((UndoLogOffset) 1 << UndoLogOffsetBits)

/* Extract the undo log number from an UndoRecPtr. */
#define UndoRecPtrGetLogNo(urp)					\
	((urp) >> UndoLogOffsetBits)

/* Extract the offset from an UndoRecPtr. */
#define UndoRecPtrGetOffset(urp)				\
	((urp) & ((UINT64CONST(1) << UndoLogOffsetBits) - 1))

/* Make an UndoRecPtr from an log number and offset. */
#define MakeUndoRecPtr(logno, offset)			\
	(((uint64) (logno) << UndoLogOffsetBits) | (offset))

/* Length of undo checkpoint filename */
#define UNDO_CHECKPOINT_FILENAME_LENGTH	16

/* Extract the relnode for an undo log. */
#define UndoRecPtrGetRelNode(urp)				\
	UndoRecPtrGetLogNo(urp)

/* The only valid fork number for undo log buffers. */
#define UndoLogForkNum MAIN_FORKNUM

/* Compute the block number that holds a given UndoRecPtr. */
#define UndoRecPtrGetBlockNum(urp)				\
	(UndoRecPtrGetOffset(urp) / BLCKSZ)

/* Compute the offset of a given UndoRecPtr in the page that holds it. */
#define UndoRecPtrGetPageOffset(urp)			\
	(UndoRecPtrGetOffset(urp) % BLCKSZ)

/* Compare two undo checkpoint files to find the oldest file. */
#define UndoCheckPointFilenamePrecedes(file1, file2)	\
	(strcmp(file1, file2) < 0)

/*
 * Populate a RelFileNode from an UndoRecPtr.
 *
 * dbNode is invalid, the relation file accessed has to be distinguished by
 * SmgrId.
 */
#define UndoRecPtrAssignRelFileNode(rfn, urp)								 \
	do																		 \
	{																		 \
		(rfn).spcNode = UndoLogNumberGetTablespace(UndoRecPtrGetLogNo(urp)); \
		(rfn).dbNode = InvalidOid;											 \
		(rfn).relNode = UndoRecPtrGetRelNode(urp);							 \
	} while (false);

/*
 * Control metadata for an active undo log.  Lives in shared memory inside an
 * UndoLogSlot object, but also written to disk during checkpoints.
 */
typedef struct UndoLogMetaData
{
	UndoLogNumber logno;
	Oid			tablespace;
	char		persistence;

	UndoLogOffset discard;		/* oldest data needed (tail) */
	UndoLogOffset insert;		/* location of next insert (head) */
	UndoLogOffset size;			/* If insert == size, the log is full. */
} UndoLogMetaData;

#ifndef FRONTEND

/*
 * The state of an UndoLogSlot determines what types of concurrent access are
 * permitted.
 */
typedef enum UndoLogSlotState
{
	/* This is a spare slot, not currently managing an undo log. */
	UNDOLOGSLOT_FREE,

	/*
	 * This slot is managing an undo log, and is available for any backend to
	 * acquire.  In this state, a slot can be moved to UNDOLOGSLOT_FREE state
	 * by CheckPointUndoLogs() or DropUndoLogsInTablespace().
	 */
	UNDOLOGSLOT_ON_SHARED_FREE_LIST,

	/*
	 * This slot is managing an undo log, but currently on one backend's
	 * private free list.  In this state, the undo log can be moved to
	 * UNDOLOGSLOT_FREE state by CheckPointUndoLogs() or
	 * DropUndoLogsInTablespace(), but no other backend can acquire it.  That
	 * can be detected by a change in associated pid.  This state exists to
	 * give backends a fast way to keep reusing the same undo log for
	 * sequential transactions without having to take a more heavily contended
	 * lock.
	 */
	UNDOLOGSLOT_ON_PRIVATE_FREE_LIST,

	/*
	 * This slot is managing an undo log that has been acquired for use in an
	 * UndoRecordSet.  In this state, the undo log is "pinned", so it can't be
	 * freed by another backend, and the backend that owns the UndoRecordSet
	 * can read all struct members without having to acquire a lock, because
	 * no one else can write to it.
	 */
	UNDOLOGSLOT_IN_UNDO_RECORD_SET
} UndoLogSlotState;

/*
 * The in-memory control object for an undo log.  We have a fixed-sized array
 * of these.
 *
 * The following locks protect different aspects of UndoLogSlot objects, and
 * if more than one these is taken they must be taken in the order listed
 * here:
 *
 * * file_lock -- used to prevent concurrent modification of begin, end
 * * meta_lock -- used to update or read meta, begin, end
 *
 * Note that begin and end can be read while holding only file_lock or
 * meta_lock, but can only be updated while holding both.
 */
typedef struct UndoLogSlot
{
	/*
	 * Protected by UndoLogLock, file_lock and meta_lock.  All must be held to
	 * steal this slot for another undolog.  Any one may be held to prevent
	 * that from happening.
	 */
	UndoLogNumber logno;		/* InvalidUndoLogNumber for unused slots */

	slist_node	next;			/* link node for shared free lists */

	LWLock		file_lock;		/* prevents concurrent file operations */

	LWLock		meta_lock;		/* protects following members */
	UndoLogSlotState state;		/* free, in use etc */
	UndoLogMetaData meta;		/* current meta-data */
	bool		force_truncate; /* for testing only */
	pid_t		pid;			/* InvalidPid for unattached */
	TransactionId xid;
	UndoLogOffset begin;		/* beginning of lowest segment file */
	UndoLogOffset end;			/* one past end of highest segment */
} UndoLogSlot;

extern UndoLogSlot *UndoLogGetSlot(UndoLogNumber logno, bool missing_ok);
extern UndoLogSlot *UndoLogGetNextSlot(UndoLogSlot *slot);
extern UndoRecPtr UndoLogGetOldestRecord(UndoLogNumber logno, bool *full);

/*
 * Each backend maintains a small hash table mapping undo log numbers to
 * UndoLogSlot objects in shared memory.
 *
 * We also cache the tablespace, category and a recently observed discard
 * pointer here, since we need fast access to those.  We could also reach them
 * via slot->meta, but they can't be accessed without locking (since the
 * UndoLogSlot object might be recycled if the log is entirely discard).
 * Since tablespace and category are constant for lifetime of the undo log
 * number, and the discard pointer only travels in one direction, there is no
 * cache invalidation problem to worry about.
 */
typedef struct UndoLogTableEntry
{
	UndoLogNumber number;
	UndoLogSlot *slot;
	Oid			tablespace;
	char		persistence;
	UndoRecPtr	recent_discard;
	char		status;			/* used by simplehash */
} UndoLogTableEntry;

/*
 * Instantiate fast inline hash table access functions.  We use an identity
 * hash function for speed, since we already have integers and don't expect
 * many collisions.
 */
#define SH_PREFIX undologtable
#define SH_ELEMENT_TYPE UndoLogTableEntry
#define SH_KEY_TYPE UndoLogNumber
#define SH_KEY number
#define SH_HASH_KEY(tb, key) (key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

extern PGDLLIMPORT undologtable_hash * undologtable_cache;
extern UndoLogNumber undologtable_low_logno;

/*
 * Find or create an UndoLogTableGetEntry for this log number.  This is used
 * only for fast look-ups of tablespace and persistence.
 */
static pg_attribute_always_inline UndoLogTableEntry *
UndoLogGetTableEntry(UndoLogNumber logno)
{
	UndoLogTableEntry *entry;

	/* Fast path. */
	entry = undologtable_lookup(undologtable_cache, logno);
	if (likely(entry))
		return entry;

	/* Avoid a slow look up for ancient discarded undo logs. */
	if (logno < undologtable_low_logno)
		return NULL;

	/* Slow path: force cache entry to be created. */
	UndoLogGetSlot(logno, false);
	entry = undologtable_lookup(undologtable_cache, logno);

	return entry;
}

/*
 * Look up the tablespace for an undo log in our cache.
 */
static inline Oid
UndoLogNumberGetTablespace(UndoLogNumber logno)
{
	return UndoLogGetTableEntry(logno)->tablespace;
}

static inline Oid
UndoRecPtrGetTablespace(UndoRecPtr urp)
{
	return UndoLogNumberGetTablespace(UndoRecPtrGetLogNo(urp));
}

/*
 * Look up the category for an undo log in our cache.
 */
static inline char
UndoLogNumberGetPersistence(UndoLogNumber logno)
{
	return UndoLogGetTableEntry(logno)->persistence;
}

static inline char
UndoRecPtrGetPersistence(UndoRecPtr urp)
{
	return UndoLogNumberGetPersistence(UndoRecPtrGetLogNo(urp));
}

#endif

/* Discarding data. */
extern void UndoDiscard(UndoRecPtr location);
extern bool UndoLogRecPtrIsDiscardedSlowPath(UndoRecPtr pointer);

#ifndef FRONTEND

/*
 * Check if an undo log pointer is discarded.
 */
static inline bool
UndoRecPtrIsDiscarded(UndoRecPtr pointer)
{
	UndoLogNumber logno = UndoRecPtrGetLogNo(pointer);
	UndoRecPtr	recent_discard;

	/* See if we can answer the question without acquiring any locks. */
	recent_discard = UndoLogGetTableEntry(logno)->recent_discard;
	if (likely(recent_discard > pointer))
		return true;

	/*
	 * It might be discarded or not, but we'll need to do a bit more work to
	 * find out.
	 */
	return UndoLogRecPtrIsDiscardedSlowPath(pointer);
}

/* Interfaces used by undorecordset.c. */
extern UndoLogSlot *UndoLogAcquire(char persistence, UndoLogNumber min_logno);
extern void UndoLogRelease(UndoLogSlot *slot);
extern void UndoLogAdjustPhysicalRange(UndoLogNumber logno,
									   UndoLogOffset new_discard,
									   UndoLogOffset new_isnert);
extern void UndoLogTruncate(UndoLogSlot *uls, UndoLogOffset size);

extern UndoPersistenceLevel GetUndoPersistenceLevel(char persistence);

#endif

/* Initialization/cleanup interfaces. */
extern void UndoLogShmemInit(void);
extern Size UndoLogShmemSize(void);
extern void AtProcExit_UndoLog(void);

/* Startup/checkpoint interfaces. */
extern void StartupUndoLogs(UndoCheckpointContext *ctx);
extern void CheckPointUndoLogs(UndoCheckpointContext *ctx);

/* Interfaces exported for undo_file.c. */
extern void UndoLogNewSegment(UndoLogNumber logno, Oid tablespace, int segno);
extern void UndoLogDirectory(Oid tablespace, char *path);
extern void UndoLogSegmentPath(UndoLogNumber logno, int segno, Oid tablespace,
							   char *path);
extern void ResetUndoLogs(char persistence);

/* Interface use by tablespace.c. */
extern bool DropUndoLogsInTablespace(Oid tablespace);

/* GUC interfaces. */
extern void assign_undo_tablespaces(const char *newval, void *extra);

extern void TempUndoDiscard(UndoLogNumber);

#endif
