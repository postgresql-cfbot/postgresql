/*
 * fdwxact.h
 *
 * PostgreSQL global transaction manager
 *
 * Portions Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * src/include/access/fdwxact.h
 */
#ifndef FDWXACT_H
#define FDWXACT_H

#include "access/fdwxact_xlog.h"
#include "access/xlogreader.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/execnodes.h"
#include "storage/backendid.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* fdwXactState */
#define	FDWXACT_NOT_WAITING		0
#define	FDWXACT_WAITING			1
#define	FDWXACT_WAIT_COMPLETE	2

/* Flag passed to FDW transaction management APIs */
#define FDWXACT_FLAG_ONEPHASE		0x01	/* transaction can commit/rollback
											   without preparation */

/* Enum for foreign_twophase_commit parameter */
typedef enum
{
	FOREIGN_TWOPHASE_COMMIT_DISABLED,	/* disable foreign twophase commit */
	FOREIGN_TWOPHASE_COMMIT_PREFER,		/* use twophase commit where available */
	FOREIGN_TWOPHASE_COMMIT_REQUIRED	/* all foreign servers have to support
										   twophase commit */
} ForeignTwophaseCommitLevel;

/* Enum to track the status of foreign transaction */
typedef enum
{
	FDWXACT_STATUS_INVALID,
	FDWXACT_STATUS_INITIAL,
	FDWXACT_STATUS_PREPARING,		/* foreign transaction is being prepared */
	FDWXACT_STATUS_PREPARED,		/* foreign transaction is prepared */
	FDWXACT_STATUS_COMMITTING,		/* foreign prepared transaction is to
									 * be committed */
	FDWXACT_STATUS_ABORTING,		/* foreign prepared transaction is to be
									 * aborted */
	FDWXACT_STATUS_RESOLVED
} FdwXactStatus;

typedef struct FdwXactData *FdwXact;

/*
 * Shared memory state of a single foreign transaction.
 */
typedef struct FdwXactData
{
	FdwXact			fdwxact_free_next;	/* Next free FdwXact entry */

	Oid				dbid;			/* database oid where to find foreign server
									 * and user mapping */
	TransactionId	local_xid;		/* XID of local transaction */
	Oid				serverid;		/* foreign server where transaction takes
									 * place */
	Oid				userid;			/* user who initiated the foreign
									 * transaction */
	Oid				umid;
	bool			indoubt;		/* Is an in-doubt transaction? */
	slock_t			mutex;			/* Protect the above fields */

	/* The status of the foreign transaction, protected by FdwXactLock */
	FdwXactStatus 	status;
	/*
	 * Note that we need to keep track of two LSNs for each FdwXact. We keep
	 * track of the start LSN because this is the address we must use to read
	 * state data back from WAL when committing a FdwXact. We keep track of
	 * the end LSN because that is the LSN we need to wait for prior to
	 * commit.
	 */
	XLogRecPtr	insert_start_lsn;		/* XLOG offset of inserting this entry start */
	XLogRecPtr	insert_end_lsn;		/* XLOG offset of inserting this entry end */

	bool		valid;			/* has the entry been complete and written to file? */
	BackendId	held_by;		/* backend who are holding */
	bool		ondisk;			/* true if prepare state file is on disk */
	bool		inredo;			/* true if entry was added via xlog_redo */

	char		fdwxact_id[FDWXACT_ID_MAX_LEN];		/* prepared transaction identifier */
} FdwXactData;

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
	FdwXact		fdwxacts[FLEXIBLE_ARRAY_MEMBER];		/* Variable length array */
} FdwXactCtlData;

/* Pointer to the shared memory holding the foreign transactions data */
FdwXactCtlData *FdwXactCtl;

/* State data for foreign transaction resolution, passed to FDW callbacks */
typedef struct FdwXactRslvState
{
	/* Foreign transaction information */
	char	*fdwxact_id;

	ForeignServer	*server;
	UserMapping		*usermapping;

	int		flags;			/* OR of FDWXACT_FLAG_xx flags */
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
extern void restoreFdwXactData(void);
extern TransactionId PrescanFdwXacts(TransactionId oldestActiveXid);
extern void RecoverFdwXacts(void);
extern void AtEOXact_FdwXacts(bool is_commit);
extern void AtPrepare_FdwXacts(void);
extern bool fdwxact_exists(Oid dboid, Oid serverid, Oid userid);
extern void CheckPointFdwXacts(XLogRecPtr redo_horizon);
extern bool FdwTwoPhaseNeeded(void);
extern void PreCommit_FdwXacts(void);
extern void KnownFdwXactRecreateFiles(XLogRecPtr redo_horizon);
extern void FdwXactWaitToBeResolved(TransactionId wait_xid, bool commit);
extern bool FdwXactIsForeignTwophaseCommitRequired(void);
extern void FdwXactResolveTransactionAndReleaseWaiter(Oid dbid, TransactionId xid,
													  PGPROC *waiter);
extern bool FdwXactResolveInDoubtTransactions(Oid dbid);
extern PGPROC *FdwXactGetWaiter(TimestampTz *nextResolutionTs_p, TransactionId *waitXid_p);
extern void FdwXactCleanupAtProcExit(void);
extern void RegisterFdwXactByRelId(Oid relid, bool modified);
extern void RegisterFdwXactByServerId(Oid serverid, bool modified);
extern void FdwXactMarkForeignServerAccessed(Oid relid, bool modified);
extern bool check_foreign_twophase_commit(int *newval, void **extra,
										  GucSource source);
extern bool FdwXactWaiterExists(Oid dbid);

#endif   /* FDWXACT_H */
