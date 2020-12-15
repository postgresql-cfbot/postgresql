/*
 * fdwxact.h
 *
 * PostgreSQL global transaction manager
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "access/fdwxact_xlog.h"
#include "foreign/foreign.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/s_lock.h"

/* Flag passed to FDW transaction management APIs */
#define FDWXACT_FLAG_ONEPHASE		0x01	/* transaction can commit/rollback
											 * without preparation */

/* Enum for foreign_twophase_commit parameter */
typedef enum
{
	FOREIGN_TWOPHASE_COMMIT_DISABLED,	/* disable foreign twophase commit */
	FOREIGN_TWOPHASE_COMMIT_REQUIRED	/* all foreign servers have to support
										 * twophase commit */
}			ForeignTwophaseCommitLevel;

/* Enum to track the status of foreign transaction */
typedef enum
{
	FDWXACT_STATUS_INVALID = 0,
	FDWXACT_STATUS_PREPARING,	/* foreign transaction is being prepared */
	FDWXACT_STATUS_PREPARED,	/* foreign transaction is prepared */
	FDWXACT_STATUS_COMMITTING,	/* foreign prepared transaction is committed */
	FDWXACT_STATUS_ABORTING		/* foreign prepared transaction is aborted */
} FdwXactStatus;

/*
 * Shared memory state of a single foreign transaction.
 */
typedef struct FdwXactData *FdwXact;
typedef struct FdwXactData
{
	FdwXact		fdwxact_free_next;	/* Next free FdwXact entry */

	TransactionId local_xid;	/* XID of local transaction */

	/* Information relevant with foreign transaction */
	Oid			dbid;
	Oid			serverid;
	Oid			userid;
	Oid			umid;

	/* Foreign transaction status */
	FdwXactStatus status;
	slock_t		mutex;			/* protect the above field */

	/*
	 * Note that we need to keep track of two LSNs for each FdwXact. We keep
	 * track of the start LSN because this is the address we must use to read
	 * state data back from WAL when committing a FdwXact. We keep track of
	 * the end LSN because that is the LSN we need to wait for prior to
	 * commit.
	 */
	XLogRecPtr	insert_start_lsn;	/* XLOG offset of inserting this entry
									 * start */
	XLogRecPtr	insert_end_lsn; /* XLOG offset of inserting this entry end */

	bool		valid;			/* has the entry been complete and written to
								 * file? */
	BackendId	locking_backend;	/* backend currently working on the fdw xact */
	bool		ondisk;			/* true if prepare state file is on disk */
	bool		inredo;			/* true if entry was added via xlog_redo */

	char		fdwxact_id[FDWXACT_ID_MAX_LEN]; /* prepared transaction
												 * identifier */
}			FdwXactData;

/*
 * Shared memory layout for maintaining foreign prepared transaction entries.
 * Adding or removing FdwXact entry needs to hold FdwXactLock in exclusive mode,
 * and iterating fdwXacts needs that in shared mode.
 */
typedef struct
{
	/* Head of linked list of free FdwXactData structs */
	FdwXact		free_fdwxacts;

	/* Number of valid foreign transaction entries */
	int			num_fdwxacts;

	/* Upto max_prepared_foreign_xacts entries in the array */
	FdwXact		fdwxacts[FLEXIBLE_ARRAY_MEMBER];	/* Variable length array */
} FdwXactCtlData;

/* Pointer to the shared memory holding the foreign transactions data */
FdwXactCtlData *FdwXactCtl;

/* State data for foreign transaction resolution, passed to FDW callbacks */
typedef struct FdwXactRslvState
{
	/* Foreign transaction information */
	char		   *fdwxact_id;
	ForeignServer *server;
	UserMapping *usermapping;

	int			flags;			/* OR of FDWXACT_FLAG_xx flags */
} FdwXactRslvState;

/* GUC parameters */
extern int	max_prepared_foreign_xacts;
extern int	max_foreign_xact_resolvers;
extern int	foreign_xact_resolution_retry_interval;
extern int	foreign_xact_resolver_timeout;
extern int	foreign_twophase_commit;

/* Function declarations */
extern Size FdwXactShmemSize(void);
extern void FdwXactShmemInit(void);
extern void PreCommit_FdwXact(void);
extern void AtEOXact_FdwXact(bool is_commit);
extern void PrePrepare_FdwXact(void);
extern bool FdwXactIsForeignTwophaseCommitRequired(void);
extern void FdwXactResolveFdwXacts(int *fdwxact_idxs, int nfdwxacts);
extern bool FdwXactExists(Oid dbid, Oid serverid, Oid userid);
extern bool FdwXactExistsXid(TransactionId xid);
extern void CheckPointFdwXacts(XLogRecPtr redo_horizon);
extern void RecreateFdwXactFile(Oid dbid, TransactionId xid, Oid serverid,
								Oid userid, void *content, int len);
extern void RestoreFdwXactData(void);
extern void RecoverFdwXacts(void);
extern TransactionId PrescanFdwXacts(TransactionId oldestActiveXid);

#endif /* FDWXACT_H */
