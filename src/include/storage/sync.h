/*-------------------------------------------------------------------------
 *
 * sync.h
 *	  File synchronization management code.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sync.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYNC_H
#define SYNC_H

#include "storage/relfilenode.h"
#include "lib/ilist.h"

/*
 * Type of sync request.  These are used to manage the set of pending
 * requests to call a sync handler's sync or unlink functions at the next
 * checkpoint.
 */
typedef enum SyncRequestType
{
	SYNC_REQUEST,				/* schedule a call of sync function */
	SYNC_UNLINK_REQUEST,		/* schedule a call of unlink function */
	SYNC_FORGET_REQUEST,		/* forget all calls for a tag */
	SYNC_FILTER_REQUEST			/* forget all calls satisfying match fn */
} SyncRequestType;

/*
 * Which set of functions to use to handle a given request.  The values of
 * the enumerators must match the indexes of the function table in sync.c.
 */
typedef enum SyncRequestHandler
{
	SYNC_HANDLER_MD = 0,
	SYNC_HANDLER_CLOG,
	SYNC_HANDLER_COMMIT_TS,
	SYNC_HANDLER_MULTIXACT_OFFSET,
	SYNC_HANDLER_MULTIXACT_MEMBER,
	SYNC_HANDLER_NONE
} SyncRequestHandler;

/*
 * A tag identifying a file.  Currently it has the members required for md.c's
 * usage, but sync.c has no knowledge of the internal structure, and it is
 * liable to change as required by future handlers.
 */
typedef struct FileTag
{
	int16		handler;		/* SyncRequestHandler value, saving space */
	int16		forknum;		/* ForkNumber, saving space */
	RelFileNode rnode;
	uint32		segno;
} FileTag;

struct PendingFsyncEntry;

typedef struct InflightSyncEntry
{
	FileTag		tag;			/* identifies handler and file */

	/* to be filled by handler, for error messages */
	char		path[MAXPGPATH];

	/* for use of handler, to pass data to completion */
	uintptr_t	handler_data;

	/* associated hash entry, so it can be deleted */
	struct PendingFsyncEntry *hash_entry;

	int			retry_count;

	/* stats */
	uint64		start_time;
	uint64		end_time;

	/* membership in inflightSyncs, retrySyncs */
	dlist_node	node;
} InflightSyncEntry;

struct PgStreamingWrite;

extern void InitSync(void);
extern void SyncPreCheckpoint(void);
extern void SyncPostCheckpoint(void);
extern void ProcessSyncRequests(void);
extern void RememberSyncRequest(const FileTag *ftag, SyncRequestType type);
extern bool RegisterSyncRequest(const FileTag *ftag, SyncRequestType type,
								bool retryOnError);
extern void SyncRequestCompleted(InflightSyncEntry *inflight_entry, bool success, int err);

#endif							/* SYNC_H */
