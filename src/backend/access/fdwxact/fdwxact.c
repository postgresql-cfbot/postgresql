/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL global transaction manager for foreign servers.
 *
 * To achieve commit among all foreign servers automically, we employee
 * two-phase commit protocol, which is a type of atomic commitment
 * protocol(ACP). The basic strategy is that we prepare all of the remote
 * transactions before committing locally and commit them after committing
 * locally.
 *
 * During executor node initialization, they can register the foreign server
 * by calling either RegisterFdwXactByRelId() or RegisterFdwXactByServerId()
 * to participate it to a group for global commit. The foreign servers are
 * registered if FDW has both CommitForeignTransaciton API and
 * RollbackForeignTransactionAPI. Registered participant servers are identified
 * by OIDs of foreign server and user.
 *
 * During pre-commit of local transaction, we prepare the transaction on
 * foreign server everywhere. And after committing or rolling back locally,
 * we notify the resolver process and tell it to commit or rollback those
 * transactions. If we ask it to commit, we also tell it to notify us when
 * it's done, so that we can wait interruptibly for it to finish, and so
 * that we're not trying to locally do work that might fail after foreign
 * transaction are committed.
 *
 * The best performing way to manage the waiting backends is to have a
 * queue of waiting backends, so that we can avoid searching the through all
 * foreign transactions each time we receive a request. We have one queue
 * of which elements are ordered by the timestamp that they expect to be
 * processed at. Before waiting for foreign transactions being resolved the
 * backend enqueues with the timestamp that they expects to be processed.
 * Similary if failed to resolve them, it enqueues again with new timestamp
 * (its timestamp + foreign_xact_resolution_interval).
 *
 * If any network failure, server crash occurs or user stopped waiting
 * prepared foreign transactions are left in in-doubt state (aka. in-doubt
 * transaction). Foreign transactions in in-doubt state are not resolved
 * automatically so must be processed manually using by pg_resovle_fdwxact()
 * function.
 *
 * Two-phase commit protocol is required if the transaction modified two or
 * more servers including itself. In other case, all foreign transactions are
 * committed or rolled back during pre-commit.
 *
 * LOCKING
 *
 * Whenever a foreign transaction is processed by FDW, the corresponding
 * FdwXact entry is update. In order to protect the entry from concurrent
 * removing we need to hold a lock on the entry or a lock for entire global
 * array. However, we don't want to hold the lock during FDW is processing the
 * foreign transaction that may take a unpredictable time. To avoid this, the
 * in-memory data of foreign transaction follows a locking model based on
 * four linked concepts:
 *
 * * A foreign transaction's status variable is switched using the LWLock
 *   FdwXactLock, which need to be hold in exclusive mode when updating the
 *   status, while readers need to hold it in shared mode when looking at the
 *   status.
 * * A process who is going to update FdwXact entry cannot process foreign
 *   transaction that is being resolved.
 * * So setting the status to FDWACT_STATUS_PREPARING,
 *   FDWXACT_STATUS_COMMITTING or FDWXACT_STATUS_ABORTING, which makes foreign
 *   transaction in-progress states, means to own the FdwXact entry, which
 *   protect it from updating/removing by concurrent writers.
 * * Individual fields are protected by mutex where only the backend owning
 *   the foreign transaction is authorized to update the fields from its own
 *   one.

 * Therefore, before doing PREPARE, COMMIT PREPARED or ROLLBACK PREPARED a
 * process who is going to call transaction callback functions needs to change
 * the status to the corresponding status above while holding FdwXactLock in
 * exclusive mode, and call callback function after releasing the lock.
 *
 * RECOVERY
 *
 * During replay WAL and replication FdwXactCtl also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdwxact records happens by the following rules:
 *
 * * At the beginning of recovery, pg_fdwxacts is scanned once, filling FdwXact
 *   with entries marked with fdwxact->inredo and fdwxact->ondisk. FdwXact file
 *   data older than the XID horizon of the redo position are discarded.
 * * On PREPARE redo, the foreign transaction is added to FdwXactCtl->fdwxacts.
 *   We set fdwxact->inredo to true for such entries.
 * * On Checkpoint we iterate through FdwXactCtl->fdwxacts entries that
 *   have fdwxact->inredo set and are behind the redo_horizon. We save
 *   them to disk and then set fdwxact->ondisk to true.
 * * On resolution we delete the entry from FdwXactCtl->fdwxacts. If
 *   fdwxact->ondisk is true, the corresponding entry from the disk is
 *   additionally deleted.
 * * RecoverFdwXacts() and PrescanFdwXacts() have been modified to go through
 *   fdwxact->inredo entries that have not made it to dink.
 *
 * These replay rules are borrowed from twophase.c
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/access/fdwxact/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/fdwxact_xlog.h"
#include "access/resolver_internal.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Atomic commit is enabled by configuration */
#define IsForeignTwophaseCommitEnabled() \
	(max_prepared_foreign_xacts > 0 && \
	 max_foreign_xact_resolvers > 0)

/* Foreign twophase commit is enabled and requested by user */
#define IsForeignTwophaseCommitRequested() \
	(IsForeignTwophaseCommitEnabled() && \
	 (foreign_twophase_commit > FOREIGN_TWOPHASE_COMMIT_DISABLED))

/* Check the FdwXactParticipant is capable of two-phase commit  */
#define IsSeverCapableOfTwophaseCommit(fdw_part) \
	(((FdwXactParticipant *)(fdw_part))->prepare_foreign_xact_fn != NULL)

/* Check the FdwXact is begin resolved */
#define FdwXactIsBeingResolved(fx) \
	(((((FdwXact)(fx))->status) == FDWXACT_STATUS_PREPARING) || \
	 ((((FdwXact)(fx))->status) == FDWXACT_STATUS_COMMITTING) || \
	 ((((FdwXact)(fx))->status) == FDWXACT_STATUS_ABORTING))

/*
 * Structure to bundle the foreign transaction participant. This struct
 * is created at the beginning of execution for each foreign servers and
 * is used until the end of transaction where we cannot look at syscaches.
 * Therefore, this is allocated in the TopTransactionContext.
 */
typedef struct FdwXactParticipant
{
	/*
	 * Pointer to a FdwXact entry in the global array. NULL if the entry
	 * is not inserted yet but this is registered as a participant.
	 */
	FdwXact		fdwxact;

	/* Foreign server and user mapping info, passed to callback routines */
	ForeignServer	*server;
	UserMapping		*usermapping;

	/* Transaction identifier used for PREPARE */
	char			*fdwxact_id;

	/* true if modified the data on the server */
	bool			modified;

	/* Callbacks for foreign transaction */
	PrepareForeignTransaction_function	prepare_foreign_xact_fn;
	CommitForeignTransaction_function	commit_foreign_xact_fn;
	RollbackForeignTransaction_function	rollback_foreign_xact_fn;
	GetPrepareId_function				get_prepareid_fn;
} FdwXactParticipant;

/*
 * List of foreign transaction participants for atomic commit. This list
 * has only foreign servers that provides transaction management callbacks,
 * that is CommitForeignTransaction and RollbackForeignTransaction.
 */
static List *FdwXactParticipants = NIL;
static bool ForeignTwophaseCommitIsRequired = false;

/* Directory where the foreign prepared transaction files will reside */
#define FDWXACTS_DIR "pg_fdwxact"

/*
 * Name of foreign prepared transaction file is 8 bytes database oid,
 * xid, foreign server oid and user oid separated by '_'.
 *
 * Since FdwXact stat file is created per foreign transaction in a
 * distributed transaction and the xid of unresolved distributed
 * transaction never reused, the name is fairly enough to ensure
 * uniqueness.
 */
#define FDWXACT_FILE_NAME_LEN (8 + 1 + 8 + 1 + 8 + 1 + 8)
#define FdwXactFilePath(path, dbid, xid, serverid, userid)	\
	snprintf(path, MAXPGPATH, FDWXACTS_DIR "/%08X_%08X_%08X_%08X", \
			 dbid, xid, serverid, userid)

/* Guc parameters */
int	max_prepared_foreign_xacts = 0;
int	max_foreign_xact_resolvers = 0;
int foreign_twophase_commit = FOREIGN_TWOPHASE_COMMIT_DISABLED;

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;

static FdwXact FdwXactInsertFdwXactEntry(TransactionId xid,
										 FdwXactParticipant *fdw_part);
static void FdwXactPrepareForeignTransactions(void);
static void FdwXactOnePhaseEndForeignTransaction(FdwXactParticipant *fdw_part,
												 bool for_commit);
static void FdwXactResolveForeignTransaction(FdwXact fdwxact,
											 FdwXactRslvState *state,
											 FdwXactStatus fallback_status);
static void FdwXactComputeRequiredXmin(void);
static void FdwXactCancelWait(void);
static void FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void FdwXactRedoRemove(Oid dbid, TransactionId xid, Oid serverid,
							  Oid userid, bool give_warnings);
static void FdwXactQueueInsert(PGPROC *waiter);
static void AtProcExit_FdwXact(int code, Datum arg);
static void ForgetAllFdwXactParticipants(void);
static char *ReadFdwXactFile(Oid dbid, TransactionId xid, Oid serverid,
							 Oid userid);
static void RemoveFdwXactFile(Oid dbid, TransactionId xid, Oid serverid,
							  Oid userid, bool giveWarning);
static void RecreateFdwXactFile(Oid dbid, TransactionId xid, Oid serverid,
								Oid userid,	void *content, int len);
static void XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len);
static char *ProcessFdwXactBuffer(Oid dbid, TransactionId local_xid,
								  Oid serverid, Oid userid,
								  XLogRecPtr insert_start_lsn,
								  bool from_disk);
static void FdwXactDetermineTransactionFate(FdwXact fdwxact, bool need_lock);
static bool is_foreign_twophase_commit_required(void);
static void register_fdwxact(Oid serverid, Oid userid, bool modified);
static List *get_fdwxacts(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
						  bool including_indoubts, bool include_in_progress,
						  bool need_lock);
static FdwXact get_all_fdwxacts(int *num_p);
static FdwXact insert_fdwxact(Oid dbid, TransactionId xid, Oid serverid,
							  Oid userid, Oid umid, char *fdwxact_id);
static char *get_fdwxact_identifier(FdwXactParticipant *fdw_part,
									TransactionId xid);
static void remove_fdwxact(FdwXact fdwxact);
static FdwXact get_fdwxact_to_resolve(Oid dbid, TransactionId xid);
static FdwXactRslvState *create_fdwxact_state(void);

#ifdef USE_ASSERT_CHECKING
static bool FdwXactQueueIsOrderedByTimestamp(void);
#endif

/*
 * Remember accessed foreign transaction. Both RegisterFdwXactByRelId and
 * RegisterFdwXactByServerId are called by executor during initialization.
 */
void
RegisterFdwXactByRelId(Oid relid, bool modified)
{
	Relation		rel;
	Oid				serverid;
	Oid				userid;

	rel = relation_open(relid, NoLock);
	serverid = GetForeignServerIdByRelId(relid);
	userid = rel->rd_rel->relowner ? rel->rd_rel->relowner : GetUserId();
	relation_close(rel, NoLock);

	register_fdwxact(serverid, userid, modified);
}

void
RegisterFdwXactByServerId(Oid serverid, bool modified)
{
	register_fdwxact(serverid, GetUserId(), modified);
}

/*
 * Register given foreign transaction identified by given arguments as
 * a participant of the transaction.
 *
 * The foreign transaction identified by given server id and user id.
 * Registered foreign transactions are managed by the global transaction
 * manager until the end of the transaction.
 */
static void
register_fdwxact(Oid serverid, Oid userid, bool modified)
{
	FdwXactParticipant	*fdw_part;
	ForeignServer 		*foreign_server;
	ForeignDataWrapper	*fdw;
	UserMapping			*user_mapping;
	MemoryContext		old_ctx;
	FdwRoutine			*routine;
	ListCell	   		*lc;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant	*fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->server->serverid == serverid &&
			fdw_part->usermapping->userid == userid)
		{
			/* The foreign server is already registered, return */
			fdw_part->modified |= modified;
			return;
		}
	}

	/*
	 * Participant's information is also needed at the end of a transaction,
	 * where system cache are not available. Save it in TopTransactionContext
	 * so that these can live until the end of transaction.
	 */
	old_ctx = MemoryContextSwitchTo(TopTransactionContext);
	routine = GetFdwRoutineByServerId(serverid);

	/*
	 * Don't register foreign server if it doesn't provide both commit and
	 * rollback transaction management callbacks.
	 */
	if (!routine->CommitForeignTransaction ||
		!routine->RollbackForeignTransaction)
	{
		MyXactFlags |= XACT_FLAGS_FDWNOPREPARE;
		pfree(routine);
		return;
	}

	/*
	 * Remember we touched the foreign server that is not capable of two-phase
	 * commit.
	 */
	if (!routine->PrepareForeignTransaction)
		MyXactFlags |= XACT_FLAGS_FDWNOPREPARE;

	foreign_server = GetForeignServer(serverid);
	fdw = GetForeignDataWrapper(foreign_server->fdwid);
	user_mapping = GetUserMapping(userid, serverid);


	fdw_part = (FdwXactParticipant *) palloc(sizeof(FdwXactParticipant));

	fdw_part->fdwxact_id = NULL;
	fdw_part->server = foreign_server;
	fdw_part->usermapping = user_mapping;
	fdw_part->fdwxact = NULL;
	fdw_part->modified = modified;
	fdw_part->prepare_foreign_xact_fn = routine->PrepareForeignTransaction;
	fdw_part->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdw_part->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;
	fdw_part->get_prepareid_fn = routine->GetPrepareId;

	/* Add to the participants list */
	FdwXactParticipants = lappend(FdwXactParticipants, fdw_part);

	/* Revert back the context */
	MemoryContextSwitchTo(old_ctx);
}

/*
 * Calculates the size of shared memory allocated for maintaining foreign
 * prepared transaction entries.
 */
Size
FdwXactShmemSize(void)
{
	Size		size;

	/* Size for foreign transaction information array */
	size = offsetof(FdwXactCtlData, fdwxacts);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXact)));
	size = MAXALIGN(size);
	size = add_size(size, mul_size(max_prepared_foreign_xacts,
								   sizeof(FdwXactData)));

	return size;
}

/*
 * Initialization of shared memory for maintaining foreign prepared transaction
 * entries. The shared memory layout is defined in definition of FdwXactCtlData
 * structure.
 */
void
FdwXactShmemInit(void)
{
	bool		found;

	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FdwXact, 0);
		fdwXactExitRegistered = true;
	}

	FdwXactCtl = ShmemInitStruct("Foreign transactions table",
								 FdwXactShmemSize(),
								 &found);
	if (!IsUnderPostmaster)
	{
		FdwXact		fdwxacts;
		int			cnt;

		Assert(!found);
		FdwXactCtl->free_fdwxacts = NULL;
		FdwXactCtl->num_fdwxacts = 0;

		/* Initialize the linked list of free FDW transactions */
		fdwxacts = (FdwXact)
			((char *) FdwXactCtl +
			 MAXALIGN(offsetof(FdwXactCtlData, fdwxacts) +
					  sizeof(FdwXact) * max_prepared_foreign_xacts));
		for (cnt = 0; cnt < max_prepared_foreign_xacts; cnt++)
		{
			fdwxacts[cnt].status = FDWXACT_STATUS_INVALID;
			fdwxacts[cnt].fdwxact_free_next = FdwXactCtl->free_fdwxacts;
			FdwXactCtl->free_fdwxacts = &fdwxacts[cnt];
			SpinLockInit(&(fdwxacts[cnt].mutex));
		}
	}
	else
	{
		Assert(FdwXactCtl);
		Assert(found);
	}
}

/*
 * Prepare all foreign transactions if foreign twophase commit is required.
 * If foreign twophase commit is required, the behavior depends on the value
 * of foreign_twophase_commit; when 'required' we strictly require for all
 * foreign server's FDWs to support two-phase commit protocol and ask them to
 *  prepare foreign transactions, when 'prefer' we ask only foreign servers
 * that are capable of two-phase commit to prepare foreign transactions and ask
 * for other servers to commit, and for 'disabled' we ask all foreign servers
 * to commit foreign transaction in one-phase. If we failed to commit any of
 * them we change to aborting.
 *
 * Note that non-modified foreign servers always can be committed without
 * preparation.
 */
void
PreCommit_FdwXacts(void)
{
	bool		need_twophase_commit;
	ListCell	*lc = NULL;

	/* If there are no foreign servers involved, we have no business here */
	if (FdwXactParticipants == NIL)
		return;

	/*
	 * we require all modified server have to be capable of two-phase
	 * commit protocol.
	 */
	if (foreign_twophase_commit == FOREIGN_TWOPHASE_COMMIT_REQUIRED &&
		(MyXactFlags & XACT_FLAGS_FDWNOPREPARE) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot COMMIT a distributed transaction that has operated on foreign server that doesn't support atomic commit")));

	/*
	 * Check if we need to use foreign twophase commit. It's always false
	 * if foreign twophase commit is disabled.
	 */
	need_twophase_commit = is_foreign_twophase_commit_required();

	/*
	 * Firstly, we consider to commit foreign transactions in one-phase.
	 */
	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);
		bool	commit = false;

		/* Can commit in one-phase if two-phase commit is not requried */
		if (!need_twophase_commit)
			commit = true;

		/* Non-modified foreign transaction always can be committed in one-phase */
		if (!fdw_part->modified)
			commit = true;

		/*
		 * In 'prefer' case, non-twophase-commit capable server can be
		 * committed in one-phase.
		 */
		if (foreign_twophase_commit == FOREIGN_TWOPHASE_COMMIT_PREFER &&
			!IsSeverCapableOfTwophaseCommit(fdw_part))
			commit = true;

		if (commit)
		{
			/* Commit the foreign transaction in one-phase */
			FdwXactOnePhaseEndForeignTransaction(fdw_part, true);

			/* Delete it from the participant list */
			FdwXactParticipants = foreach_delete_current(FdwXactParticipants,
														 lc);
			continue;
		}
	}

	/* All done if we committed all foreign transactions */
	if (FdwXactParticipants == NIL)
		return;

	/*
	 * Secondary, if only one transaction is remained in the participant list
	 * and we didn't modified the local data we can commit it without
	 * preparation.
	 */
	if (list_length(FdwXactParticipants) == 1 &&
		(MyXactFlags & XACT_FLAGS_WROTENONTEMPREL) == 0)
	{
		/* Commit the foreign transaction in one-phase */
		FdwXactOnePhaseEndForeignTransaction(linitial(FdwXactParticipants),
											 true);

		/* All foreign transaction must be committed */
		list_free(FdwXactParticipants);
		return;
	}

	/*
	 * Finally, prepare foreign transactions. Note that we keep
	 * FdwXactParticipants until the end of transaction.
	 */
	FdwXactPrepareForeignTransactions();
}

/*
 * Insert FdwXact entries and prepare foreign transactions. Before inserting
 * FdwXact entry we call get_preparedid callback to get a transaction
 * identifier from FDW.
 *
 * We still can change to rollback here. If any error occurs, we rollback
 * non-prepared foreign trasactions and leave others to the resolver.
 */
static void
FdwXactPrepareForeignTransactions(void)
{
	ListCell		*lcell;
	TransactionId	xid;

	if (FdwXactParticipants == NIL)
		return;

	/* Parameter check */
	if (max_prepared_foreign_xacts == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("prepread foreign transactions are disabled"),
				 errhint("Set max_prepared_foreign_transactions to a nonzero value.")));

	if (max_foreign_xact_resolvers == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("prepread foreign transactions are disabled"),
				 errhint("Set max_foreign_transaction_resolvers to a nonzero value.")));

	xid = GetTopTransactionId();

	/* Loop over the foreign connections */
	foreach(lcell, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lcell);
		FdwXactRslvState 	*state;
		FdwXact		fdwxact;

		fdw_part->fdwxact_id = get_fdwxact_identifier(fdw_part, xid);

		Assert(fdw_part->fdwxact_id);

		/*
		 * Insert the foreign transaction entry with the FDWXACT_STATUS_PREPARING
		 * status. Registration persists this information to the disk and logs
		 * (that way relaying it on standby). Thus in case we loose connectivity
		 * to the foreign server or crash ourselves, we will remember that we
		 * might have prepared transaction on the foreign server and try to
		 * resolve it when connectivity is restored or after crash recovery.
		 *
		 * If we prepare the transaction on the foreign server before persisting
		 * the information to the disk and crash in-between these two steps,
		 * we will forget that we prepared the transaction on the foreign server
		 * and will not be able to resolve it after the crash. Hence persist
		 * first then prepare.
		 */
		fdwxact = FdwXactInsertFdwXactEntry(xid, fdw_part);

		state = create_fdwxact_state();
		state->server = fdw_part->server;
		state->usermapping = fdw_part->usermapping;
		state->fdwxact_id = pstrdup(fdw_part->fdwxact_id);

		/* Update the status */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		Assert(fdwxact->status == FDWXACT_STATUS_INITIAL);
		fdwxact->status = FDWXACT_STATUS_PREPARING;
		LWLockRelease(FdwXactLock);

		/*
		 * Prepare the foreign transaction.
		 *
		 * Between FdwXactInsertFdwXactEntry call till this backend hears
		 * acknowledge from foreign server, the backend may abort the local
		 * transaction (say, because of a signal).
		 *
		 * During abort processing, we might try to resolve a never-preapred
		 * transaction, and get an error. This is fine as long as the FDW
		 * provides us unique prepared transaction identifiers.
		 */
		PG_TRY();
		{
			fdw_part->prepare_foreign_xact_fn(state);
		}
		PG_CATCH();
		{
			/* failed, back to the initial state */
			LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
			fdwxact->status = FDWXACT_STATUS_INITIAL;
			LWLockRelease(FdwXactLock);

			PG_RE_THROW();
		}
		PG_END_TRY();

		/* succeeded, update status */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		fdwxact->status = FDWXACT_STATUS_PREPARED;
		LWLockRelease(FdwXactLock);
	}
}

/*
 * One-phase commit or rollback the given foreign transaction participant.
 */
static void
FdwXactOnePhaseEndForeignTransaction(FdwXactParticipant *fdw_part,
									 bool for_commit)
{
	FdwXactRslvState *state;

	Assert(fdw_part->commit_foreign_xact_fn);
	Assert(fdw_part->rollback_foreign_xact_fn);

	state = create_fdwxact_state();
	state->server = fdw_part->server;
	state->usermapping = fdw_part->usermapping;
	state->flags = FDWXACT_FLAG_ONEPHASE;

	/*
	 * Commit or rollback foreign transaction in one-phase. Since we didn't
	 * insert FdwXact entry for this transaction we don't need to care
	 * failures. On failure we change to rollback.
	 */
	if (for_commit)
		fdw_part->commit_foreign_xact_fn(state);
	else
		fdw_part->rollback_foreign_xact_fn(state);
}

/*
 * This function is used to create new foreign transaction entry before an FDW
 * prepares and commit/rollback. The function adds the entry to WAL and it will
 * be persisted to the disk under pg_fdwxact directory when checkpoint.
 */
static FdwXact
FdwXactInsertFdwXactEntry(TransactionId xid, FdwXactParticipant *fdw_part)
{
	FdwXact				fdwxact;
	FdwXactOnDiskData	*fdwxact_file_data;
	MemoryContext		old_context;
	int					data_len;

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	/*
	 * Enter the foreign transaction in the shared memory structure.
	 */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdwxact = insert_fdwxact(MyDatabaseId, xid, fdw_part->server->serverid,
							fdw_part->usermapping->userid,
							fdw_part->usermapping->umid, fdw_part->fdwxact_id);
	fdwxact->status = FDWXACT_STATUS_INITIAL;
	fdwxact->held_by = MyBackendId;
	LWLockRelease(FdwXactLock);

	fdw_part->fdwxact = fdwxact;
	MemoryContextSwitchTo(old_context);

	/*
	 * Prepare to write the entry to a file. Also add xlog entry. The contents
	 * of the xlog record are same as what is written to the file.
	 */
	data_len = offsetof(FdwXactOnDiskData, fdwxact_id);
	data_len = data_len + strlen(fdw_part->fdwxact_id) + 1;
	data_len = MAXALIGN(data_len);
	fdwxact_file_data = (FdwXactOnDiskData *) palloc0(data_len);
	fdwxact_file_data->dbid = MyDatabaseId;
	fdwxact_file_data->local_xid = xid;
	fdwxact_file_data->serverid = fdw_part->server->serverid;
	fdwxact_file_data->userid = fdw_part->usermapping->userid;
	fdwxact_file_data->umid = fdw_part->usermapping->umid;
	memcpy(fdwxact_file_data->fdwxact_id, fdw_part->fdwxact_id,
		   strlen(fdw_part->fdwxact_id) + 1);

	/* See note in RecordTransactionCommit */
	MyPgXact->delayChkpt = true;

	START_CRIT_SECTION();

	/* Add the entry in the xlog and save LSN for checkpointer */
	XLogBeginInsert();
	XLogRegisterData((char *) fdwxact_file_data, data_len);
	fdwxact->insert_end_lsn = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_INSERT);
	XLogFlush(fdwxact->insert_end_lsn);

	/* If we crash now, we have prepared: WAL replay will fix things */

	/* Store record's start location to read that later on CheckPoint */
	fdwxact->insert_start_lsn = ProcLastRecPtr;

	/* File is written completely, checkpoint can proceed with syncing */
	fdwxact->valid = true;

	/* Checkpoint can process now */
	MyPgXact->delayChkpt = false;

	END_CRIT_SECTION();

	pfree(fdwxact_file_data);
	return fdwxact;
}

/*
 * Insert a new entry for a given foreign transaction identified by transaction
 * id, foreign server and user mapping, into the shared memory array. Caller
 * must hold FdwXactLock in exclusive mode.
 *
 * If the entry already exists, the function raises an error.
 */
static FdwXact
insert_fdwxact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
				Oid umid, char *fdwxact_id)
{
	int i;
	FdwXact fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Check for duplicated foreign transaction entry */
	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		fdwxact = FdwXactCtl->fdwxacts[i];
		if (fdwxact->dbid == dbid &&
			fdwxact->local_xid == xid &&
			fdwxact->serverid == serverid &&
			fdwxact->userid == userid)
			ereport(ERROR, (errmsg("could not insert a foreign transaction entry"),
							errdetail("duplicate entry with transaction id %u, serverid %u, userid %u",
								   xid, serverid, userid)));
	}

	/*
	 * Get a next free foreign transaction entry. Raise error if there are
	 * none left.
	 */
	if (!FdwXactCtl->free_fdwxacts)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of foreign transactions reached"),
				 errhint("Increase max_prepared_foreign_transactions: \"%d\".",
						 max_prepared_foreign_xacts)));
	}
	fdwxact = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact->fdwxact_free_next;

	/* Insert the entry to shared memory array */
	Assert(FdwXactCtl->num_fdwxacts < max_prepared_foreign_xacts);
	FdwXactCtl->fdwxacts[FdwXactCtl->num_fdwxacts++] = fdwxact;

	fdwxact->held_by = InvalidBackendId;
	fdwxact->dbid = dbid;
	fdwxact->local_xid = xid;
	fdwxact->serverid = serverid;
	fdwxact->userid = userid;
	fdwxact->umid = umid;
	fdwxact->insert_start_lsn = InvalidXLogRecPtr;
	fdwxact->insert_end_lsn = InvalidXLogRecPtr;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;
	fdwxact->indoubt = false;
	memcpy(fdwxact->fdwxact_id, fdwxact_id, strlen(fdwxact_id) + 1);

	return fdwxact;
}

/*
 * Remove the foreign prepared transaction entry from shared memory.
 * Caller must hold FdwXactLock in exclusive mode.
 */
static void
remove_fdwxact(FdwXact fdwxact)
{
	int i;

	Assert(fdwxact != NULL);
	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	if (FdwXactIsBeingResolved(fdwxact))
		elog(ERROR, "cannot remove fdwxact entry that is beging resolved");

	/* Search the slot where this entry resided */
	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		if (FdwXactCtl->fdwxacts[i] == fdwxact)
			break;
	}

	/* We did not find the given entry in the array */
	if (i >= FdwXactCtl->num_fdwxacts)
		ereport(ERROR,
				(errmsg("could not remove a foreign transaction entry"),
				 errdetail("failed to find entry for xid %u, foreign server %u, and user %u",
						   fdwxact->local_xid, fdwxact->serverid, fdwxact->userid)));

	elog(DEBUG2, "remove fdwxact entry id %s, xid %u db %d user %d",
		 fdwxact->fdwxact_id, fdwxact->local_xid, fdwxact->dbid,
		 fdwxact->userid);

	/* Remove the entry from active array */
	FdwXactCtl->num_fdwxacts--;
	FdwXactCtl->fdwxacts[i] = FdwXactCtl->fdwxacts[FdwXactCtl->num_fdwxacts];

	/* Put it back into free list */
	fdwxact->fdwxact_free_next = FdwXactCtl->free_fdwxacts;
	FdwXactCtl->free_fdwxacts = fdwxact;

	/* Reset informations */
	fdwxact->status = FDWXACT_STATUS_INVALID;
	fdwxact->held_by = InvalidBackendId;
	fdwxact->indoubt = false;

	if (!RecoveryInProgress())
	{
		xl_fdwxact_remove record;
		XLogRecPtr	recptr;

		/* Fill up the log record before releasing the entry */
		record.serverid = fdwxact->serverid;
		record.dbid = fdwxact->dbid;
		record.xid = fdwxact->local_xid;
		record.userid = fdwxact->userid;

		/*
		 * Now writing FdwXact state data to WAL. We have to set delayChkpt
		 * here, otherwise a checkpoint starting immediately after the
		 * WAL record is inserted could complete without fsync'ing our
		 * state file.  (This is essentially the same kind of race condition
		 * as the COMMIT-to-clog-write case that RecordTransactionCommit
		 * uses delayChkpt for; see notes there.)
		 */
		START_CRIT_SECTION();

		MyPgXact->delayChkpt = true;

		/*
		 * Log that we are removing the foreign transaction entry and
		 * remove the file from the disk as well.
		 */
		XLogBeginInsert();
		XLogRegisterData((char *) &record, sizeof(xl_fdwxact_remove));
		recptr = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_REMOVE);
		XLogFlush(recptr);

		/*
		 * Now we can mark ourselves as out of the commit critical section: a
		 * checkpoint starting after this will certainly see the gxact as a
		 * candidate for fsyncing.
		 */
		MyPgXact->delayChkpt = false;

		END_CRIT_SECTION();
	}
}

/*
 * Return true and set FdwXactAtomicCommitReady to true if the current transaction
 * modified data on two or more servers in FdwXactParticipants and
 * local server itself.
 */
static bool
is_foreign_twophase_commit_required(void)
{
	ListCell*	lc;
	int			nserverswritten = 0;

	if (!IsForeignTwophaseCommitRequested())
		return false;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->modified)
			nserverswritten++;
	}

	if ((MyXactFlags & XACT_FLAGS_WROTENONTEMPREL) != 0)
		++nserverswritten;

	/*
	 * Atomic commit is required if we modified data on two or more
	 * participants.
	 */
	if (nserverswritten <= 1)
		return false;

	ForeignTwophaseCommitIsRequired = true;
	return true;
}

bool
FdwXactIsForeignTwophaseCommitRequired(void)
{
	return ForeignTwophaseCommitIsRequired;
}

/*
 * Compute the oldest xmin across all unresolved foreign transactions
 * and store it in the ProcArray.
 */
static void
FdwXactComputeRequiredXmin(void)
{
	int	i;
	TransactionId agg_xmin = InvalidTransactionId;

	Assert(FdwXactCtl != NULL);

	LWLockAcquire(FdwXactLock, LW_SHARED);

	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact fdwxact = FdwXactCtl->fdwxacts[i];

		if (!fdwxact->valid)
			continue;

		Assert(TransactionIdIsValid(fdwxact->local_xid));

		if (!TransactionIdIsValid(agg_xmin) ||
			TransactionIdPrecedes(fdwxact->local_xid, agg_xmin))
			agg_xmin = fdwxact->local_xid;
	}

	LWLockRelease(FdwXactLock);

	ProcArraySetFdwXactUnresolvedXmin(agg_xmin);
}

/*
 * Mark my foreign transaction participants as in-doubt and clear
 * the FdwXactParticipants list.
 *
 * If we leave any foreign transaction, update the oldest xmin of unresolved
 * transaction so that local transaction id of in-doubt transaction is not
 * truncated.
 */
static void
ForgetAllFdwXactParticipants(void)
{
	ListCell *cell;
	int		n_lefts = 0;

	if (FdwXactParticipants == NIL)
		return;

	foreach(cell, FdwXactParticipants)
	{
		FdwXactParticipant	*fdw_part = (FdwXactParticipant *) lfirst(cell);
		FdwXact fdwxact = fdw_part->fdwxact;

		/* Nothing to do if didn't register FdwXact entry yet */
		if (!fdw_part->fdwxact)
			continue;

		/*
		 * There is a race condition; the FdwXact entries in FdwXactParticipants
		 * could be used by other backend before we forget in case where the
		 * resolver process removes the FdwXact entry and other backend reuses
		 * it before we forget. So we need to check if the entries are still
		 * associated with the transaction.
		 */
		SpinLockAcquire(&fdwxact->mutex);
		if (fdwxact->held_by == MyBackendId)
		{
			fdwxact->held_by = InvalidBackendId;
			fdwxact->indoubt = true;
			n_lefts++;
		}
		SpinLockRelease(&fdwxact->mutex);
	}

	/*
	 * If we left any FdwXact entries, update the oldest local transaction of
	 * unresolved distributed transaction and take over them to the foreign
	 * transaction resolver.
	 */
	if (n_lefts > 0)
	{
		elog(DEBUG1, "left %u foreign transactions in in-doubt status", n_lefts);
		FdwXactComputeRequiredXmin();
	}

	FdwXactParticipants = NIL;
}

/*
 * When the process exits, forget all the entries.
 */
static void
AtProcExit_FdwXact(int code, Datum arg)
{
	ForgetAllFdwXactParticipants();
}

void
FdwXactCleanupAtProcExit(void)
{
	if (!SHMQueueIsDetached(&(MyProc->fdwXactLinks)))
	{
		LWLockAcquire(FdwXactResolutionLock, LW_EXCLUSIVE);
		SHMQueueDelete(&(MyProc->fdwXactLinks));
		LWLockRelease(FdwXactResolutionLock);
	}
}

/*
 * Wait for the foreign transaction to be resolved.
 *
 * Initially backends start in state FDWXACT_NOT_WAITING and then change
 * that state to FDWXACT_WAITING before adding ourselves to the wait queue.
 * During FdwXactResolveForeignTransaction a fdwxact resolver changes the
 * state to FDWXACT_WAIT_COMPLETE once all foreign transactions are resolved.
 * This backend then resets its state to FDWXACT_NOT_WAITING.
 * If a resolver fails to resolve the waiting transaction it moves us to
 * the retry queue.
 *
 * This function is inspired by SyncRepWaitForLSN.
 */
void
FdwXactWaitToBeResolved(TransactionId wait_xid, bool is_commit)
{
	char		*new_status = NULL;
	const char	*old_status;

	Assert(FdwXactCtl != NULL);
	Assert(TransactionIdIsValid(wait_xid));
	Assert(SHMQueueIsDetached(&(MyProc->fdwXactLinks)));
	Assert(MyProc->fdwXactState == FDWXACT_NOT_WAITING);

	/* Quick exit if atomic commit is not requested */
	if (!IsForeignTwophaseCommitRequested())
		return;

	/*
	 * Also, exit if the transaction itself has no foreign transaction
	 * participants.
	 */
	if (FdwXactParticipants == NIL && wait_xid == MyPgXact->xid)
		return;

	/* Set backend status and enqueue itself to the active queue */
	LWLockAcquire(FdwXactResolutionLock, LW_EXCLUSIVE);
	MyProc->fdwXactState = FDWXACT_WAITING;
	MyProc->fdwXactWaitXid = wait_xid;
	MyProc->fdwXactNextResolutionTs = GetCurrentTransactionStopTimestamp();
	FdwXactQueueInsert(MyProc);
	Assert(FdwXactQueueIsOrderedByTimestamp());
	LWLockRelease(FdwXactResolutionLock);

	/* Launch a resolver process if not yet, or wake up */
	FdwXactLaunchOrWakeupResolver();

	/*
	 * Alter ps display to show waiting for foreign transaction
	 * resolution.
	 */
	if (update_process_title)
	{
		int len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 31 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for resolution %d", wait_xid);
		set_ps_display(new_status, false);
		new_status[len] = '\0';	/* truncate off "waiting ..." */
	}

	/* Wait for all foreign transactions to be resolved */
	for (;;)
	{
		/* Must reset the latch before testing state */
		ResetLatch(MyLatch);

		/*
		 * Acquiring the lock is not needed, the latch ensures proper
		 * barriers. If it looks like we're done, we must really be done,
		 * because once walsender changes the state to FDWXACT_WAIT_COMPLETE,
		 * it will never update it again, so we can't be seeing a stale value
		 * in that case.
		 */
		if (MyProc->fdwXactState == FDWXACT_WAIT_COMPLETE)
			break;

		/*
		 * If a wait for foreign transaction resolution is pending, we can
		 * neither acknowledge the commit nor raise ERROR or FATAL.  The latter
		 * would lead the client to believe that the distributed transaction
		 * aborted, which is not true: it's already committed locally. The
		 * former is no good either: the client has requested committing a
		 * distributed transaction, and is entitled to assume that a acknowledged
		 * commit is also commit on all foreign servers, which might not be
		 * true. So in this case we issue a WARNING (which some clients may
		 * be able to interpret) and shut off further output. We do NOT reset
		 * PorcDiePending, so that the process will die after the commit is
		 * cleaned up.
		 */
		if (ProcDiePending)
		{
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for resolving foreign transaction and terminating connection due to administrator command"),
					 errdetail("The transaction has already committed locally, but might not have been committed on the foreign server.")));
			whereToSendOutput = DestNone;
			FdwXactCancelWait();
			break;
		}

		/*
		 * If a query cancel interrupt arrives we just terminate the wait with
		 * a suitable warning. The foreign transactions can be orphaned but
		 * the foreign xact resolver can pick up them and tries to resolve them
		 * later.
		 */
		if (QueryCancelPending)
		{
			QueryCancelPending = false;
			ereport(WARNING,
					(errmsg("canceling wait for resolving foreign transaction due to user request"),
					 errdetail("The transaction has already committed locally, but might not have been committed on the foreign server.")));
			FdwXactCancelWait();
			break;
		}

		/*
		 * If the postmaster dies, we'll probably never get an
		 * acknowledgement, because all the wal sender processes will exit. So
		 * just bail out.
		 */
		if (!PostmasterIsAlive())
		{
			ProcDiePending = true;
			whereToSendOutput = DestNone;
			FdwXactCancelWait();
			break;
		}

		/*
		 * Wait on latch.  Any condition that should wake us up will set the
		 * latch, so no need for timeout.
		 */
		WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
				  WAIT_EVENT_FDWXACT_RESOLUTION);
	}

	pg_read_barrier();

	Assert(SHMQueueIsDetached(&(MyProc->fdwXactLinks)));
	MyProc->fdwXactState = FDWXACT_NOT_WAITING;

	if (new_status)
	{
		set_ps_display(new_status, false);
		pfree(new_status);
	}
}

/*
 * Return true if there are at least one backend in the wait queue. The caller
 * must hold FdwXactResolutionLock.
 */
bool
FdwXactWaiterExists(Oid dbid)
{
	PGPROC *proc;

	Assert(LWLockHeldByMeInMode(FdwXactResolutionLock, LW_SHARED));

	proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
								   &(FdwXactRslvCtl->fdwxact_queue),
								   offsetof(PGPROC, fdwXactLinks));

	while (proc)
	{
		if (proc->databaseId == dbid)
			return true;

		proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
									   &(proc->fdwXactLinks),
									   offsetof(PGPROC, fdwXactLinks));
	}

	return false;
}

/*
 * Insert the waiter to the wait queue in fdwXactNextResolutoinTs order.
 */
static void
FdwXactQueueInsert(PGPROC *waiter)
{
	PGPROC *proc;

	Assert(LWLockHeldByMeInMode(FdwXactResolutionLock, LW_EXCLUSIVE));

	proc = (PGPROC *) SHMQueuePrev(&(FdwXactRslvCtl->fdwxact_queue),
								   &(FdwXactRslvCtl->fdwxact_queue),
								   offsetof(PGPROC, fdwXactLinks));

	while (proc)
	{
		if (proc->fdwXactNextResolutionTs < waiter->fdwXactNextResolutionTs)
			break;

		proc = (PGPROC *) SHMQueuePrev(&(FdwXactRslvCtl->fdwxact_queue),
									   &(proc->fdwXactLinks),
									   offsetof(PGPROC, fdwXactLinks));
	}

	if (proc)
		SHMQueueInsertAfter(&(proc->fdwXactLinks), &(waiter->fdwXactLinks));
	else
		SHMQueueInsertAfter(&(FdwXactRslvCtl->fdwxact_queue), &(waiter->fdwXactLinks));
}

#ifdef USE_ASSERT_CHECKING
static bool
FdwXactQueueIsOrderedByTimestamp(void)
{
	PGPROC *proc;
	TimestampTz lastTs;

	proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
								   &(FdwXactRslvCtl->fdwxact_queue),
								   offsetof(PGPROC, fdwXactLinks));
	lastTs = 0;

	while (proc)
	{

		if (proc->fdwXactNextResolutionTs < lastTs)
			return false;

		lastTs = proc->fdwXactNextResolutionTs;

		proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
									   &(proc->fdwXactLinks),
									   offsetof(PGPROC, fdwXactLinks));
	}

	return true;
}
#endif

/*
 * Acquire FdwXactResolutionLock and cancel any wait currently in progress.
 */
static void
FdwXactCancelWait(void)
{
	LWLockAcquire(FdwXactResolutionLock, LW_EXCLUSIVE);
	if (!SHMQueueIsDetached(&(MyProc->fdwXactLinks)))
		SHMQueueDelete(&(MyProc->fdwXactLinks));
	MyProc->fdwXactState = FDWXACT_NOT_WAITING;
	LWLockRelease(FdwXactResolutionLock);
}

/*
 * AtEOXact_FdwXacts
 */
extern void
AtEOXact_FdwXacts(bool is_commit)
{
	ListCell   *lcell;

	if (!is_commit)
	{
		foreach (lcell, FdwXactParticipants)
		{
			FdwXactParticipant	*fdw_part = lfirst(lcell);

			/*
			 * If the foreign transaction has FdwXact entry we might have
			 * prepared it. Skip already-prepared foreign transaction because
			 * it has closed its transaction. But we are not sure that foreign
			 * transaction with status == FDWXACT_STATUS_PREPARING has been
			 * prepared or not. So we call the rollback API to close its
			 * transaction for safety. The prepared foreign transaction that
			 * we might have will be resolved by the foreign transaction
			 * resolver.
			 */
			if (fdw_part->fdwxact)
			{
				bool is_prepared;

				LWLockAcquire(FdwXactLock, LW_SHARED);
				is_prepared = fdw_part->fdwxact &&
					fdw_part->fdwxact->status == FDWXACT_STATUS_PREPARED;
				LWLockRelease(FdwXactLock);

				if (is_prepared)
					continue;
			}

			/* One-phase rollback foreign transaction */
			FdwXactOnePhaseEndForeignTransaction(fdw_part, false);
		}
	}

	/*
	 * In commit cases, we have already prepared foreign transactions during
	 * pre-commit phase. And these prepared transactions will be resolved by
	 * the resolver process.
	 */

	ForgetAllFdwXactParticipants();
	ForeignTwophaseCommitIsRequired = false;
}

/*
 * Prepare foreign transactions.
 *
 * Note that it's possible that the transaction aborts after we prepared some
 * of participants. In this case we change to rollback and rollback all foreign
 * transactions.
 */
void
AtPrepare_FdwXacts(void)
{
	if (FdwXactParticipants == NIL)
		return;

	/* Check for an invalid condition */
	if (!IsForeignTwophaseCommitRequested())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a distributed transaction when foreign_twophase_commit is \'disabled\'")));

	/*
	 * We cannot prepare if any foreign server of participants isn't capable
	 * of two-phase commit.
	 */
	if (is_foreign_twophase_commit_required() &&
		(MyXactFlags & XACT_FLAGS_FDWNOPREPARE) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot prepare the transaction because some foreign servers involved in transaction can not prepare the transaction")));

	/* Prepare transactions on participating foreign servers. */
	FdwXactPrepareForeignTransactions();

	FdwXactParticipants = NIL;
}

/*
 * Return one backend that connects to my database and is waiting for
 * resolution.
 */
PGPROC *
FdwXactGetWaiter(TimestampTz *nextResolutionTs_p, TransactionId *waitXid_p)
{
	PGPROC *proc;

	LWLockAcquire(FdwXactResolutionLock, LW_SHARED);
	Assert(FdwXactQueueIsOrderedByTimestamp());

	proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
								   &(FdwXactRslvCtl->fdwxact_queue),
								   offsetof(PGPROC, fdwXactLinks));

	while (proc)
	{
		if (proc->databaseId == MyDatabaseId)
			break;

		proc = (PGPROC *) SHMQueueNext(&(FdwXactRslvCtl->fdwxact_queue),
									   &(proc->fdwXactLinks),
									   offsetof(PGPROC, fdwXactLinks));
	}

	if (proc)
	{
		*nextResolutionTs_p = proc->fdwXactNextResolutionTs;
		*waitXid_p = proc->fdwXactWaitXid;
	}
	else
	{
		*nextResolutionTs_p = -1;
		*waitXid_p = InvalidTransactionId;
	}

	LWLockRelease(FdwXactResolutionLock);

	return proc;
}

/*
 * Get one FdwXact entry to resolve. This function intended to be used when
 * a resolver process get FdwXact entries to resolve. So we search entries
 * while not including in-doubt transactions and in-progress transactions.
 */
static FdwXact
get_fdwxact_to_resolve(Oid dbid, TransactionId xid)
{
	List *fdwxacts = NIL;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Don't include both in-doubt transactions and in-progress transactions */
	fdwxacts = get_fdwxacts(dbid, xid, InvalidOid, InvalidOid,
							false, false, false);

	return fdwxacts == NIL ? NULL : (FdwXact) linitial(fdwxacts);
}

/*
 * Resolve one distributed transaction on the given database . The target
 * distributed transaction is fetched from the waiting queue and its transaction
 * participants are fetched from the global array.
 *
 * Release the waiter and return true after we resolved the all of the foreign
 * transaction participants. On failure, we re-enqueue the waiting backend after
 * incremented the next resolution time.
 */
void
FdwXactResolveTransactionAndReleaseWaiter(Oid dbid, TransactionId xid,
										  PGPROC *waiter)
{
	FdwXact	fdwxact;

	Assert(TransactionIdIsValid(xid));

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	while ((fdwxact = get_fdwxact_to_resolve(MyDatabaseId, xid)) != NULL)
	{
		FdwXactRslvState *state;
		ForeignServer *server;
		UserMapping	*usermapping;

		CHECK_FOR_INTERRUPTS();

		server = GetForeignServer(fdwxact->serverid);
		usermapping = GetUserMapping(fdwxact->userid, fdwxact->serverid);

		state = create_fdwxact_state();
		SpinLockAcquire(&fdwxact->mutex);
		state->server = server;
		state->usermapping = usermapping;
		state->fdwxact_id = pstrdup(fdwxact->fdwxact_id);
		SpinLockRelease(&fdwxact->mutex);

		FdwXactDetermineTransactionFate(fdwxact, false);

		/* Do not hold during foreign transaction resolution */
		LWLockRelease(FdwXactLock);

		PG_TRY();
		{
			/*
			 * Resolve the foreign transaction. When committing or aborting
			 * prepared foreign transactions the previous status is always
			 * FDWXACT_STATUS_PREPARED.
			 */
			FdwXactResolveForeignTransaction(fdwxact, state,
											 FDWXACT_STATUS_PREPARED);
		}
		PG_CATCH();
		{
			/*
			 * Failed to resolve. Re-insert the waiter to the tail of retry
			 * queue if the waiter is still waiting.
			 */
			LWLockAcquire(FdwXactResolutionLock, LW_EXCLUSIVE);
			if (waiter->fdwXactState == FDWXACT_WAITING)
			{
				SHMQueueDelete(&(waiter->fdwXactLinks));
				pg_write_barrier();
				waiter->fdwXactNextResolutionTs =
					TimestampTzPlusMilliseconds(waiter->fdwXactNextResolutionTs,
												foreign_xact_resolution_retry_interval);
				FdwXactQueueInsert(waiter);
			}
			LWLockRelease(FdwXactResolutionLock);

			PG_RE_THROW();
		}
		PG_END_TRY();

		elog(DEBUG2, "resolved one foreign transaction xid %u, serverid %d, userid %d",
			 fdwxact->local_xid, fdwxact->serverid, fdwxact->userid);

		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	}

	LWLockRelease(FdwXactLock);

	LWLockAcquire(FdwXactResolutionLock, LW_EXCLUSIVE);

	/*
	 * Remove waiter from shmem queue, if not detached yet. The waiter
	 * could already be detached if user cancelled to wait before
	 * resolution.
	 */
	if (!SHMQueueIsDetached(&(waiter->fdwXactLinks)))
	{
		TransactionId	wait_xid = waiter->fdwXactWaitXid;

		SHMQueueDelete(&(waiter->fdwXactLinks));
		pg_write_barrier();

		/* Set state to complete */
		waiter->fdwXactState = FDWXACT_WAIT_COMPLETE;

		/* Wake up the waiter only when we have set state and removed from queue */
		SetLatch(&(waiter->procLatch));

		elog(DEBUG2, "released the proc with xid %u", wait_xid);
	}
	else
		elog(DEBUG2, "the waiter backend had been already detached");

	LWLockRelease(FdwXactResolutionLock);
}

/*
 * Determine whether the given foreign transaction should be committed or
 * rolled back according to the result of the local transaction. This function
 * changes fdwxact->status so the caller must hold FdwXactLock in exclusive
 * mode or passing need_lock with true.
 */
static void
FdwXactDetermineTransactionFate(FdwXact fdwxact, bool need_lock)
{
	bool			is_commit = false;

	if (need_lock)
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	/*
	 * The being resolved transaction must be either that has been cancelled
	 *  and marked as in-doubt or that has been prepared.
	 */
	Assert(fdwxact->indoubt ||
		   fdwxact->status == FDWXACT_STATUS_PREPARED);

	/*
	 * If the local transaction is already committed, commit prepared
	 * foreign transaction.
	 */
	if (TransactionIdDidCommit(fdwxact->local_xid))
	{
		fdwxact->status = FDWXACT_STATUS_COMMITTING;
		is_commit = true;
	}

	/*
	 * If the local transaction is already aborted, abort prepared
	 * foreign transactions.
	 */
	else if (TransactionIdDidAbort(fdwxact->local_xid))
	{
		fdwxact->status = FDWXACT_STATUS_ABORTING;
		is_commit = false;
	}

	/*
	 * The local transaction is not in progress but the foreign
	 * transaction is not prepared on the foreign server. This
	 * can happen when transaction failed after registered this
	 * entry but before actual preparing on the foreign server.
	 * So let's assume it aborted.
	 */
	else if (!TransactionIdIsInProgress(fdwxact->local_xid))
	{
		fdwxact->status = FDWXACT_STATUS_ABORTING;
		is_commit = false;
	}

	/*
	 * The Local transaction is in progress and foreign transaction is
	 * about to be committed or aborted. This should not happen except for one
	 * case where the local transaction is prepared and this foreign transaction
	 * is being resolved manually using by pg_resolve_foreign_xact(). Raise an
	 * error anyway since we cannot determine the fate of this foreign
	 * transaction according to the local transaction whose fate is also not
	 * determined.
	 */
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot resolve the foreign transaction associated with in-progress transaction %u on server %u",
						fdwxact->local_xid, fdwxact->serverid),
				 errhint("The local transaction with xid %u might be prepared",
						 fdwxact->local_xid)));

	if (need_lock)
		LWLockRelease(FdwXactLock);
}

/*
 * Resolve the foreign transaction using the foreign data wrapper's transaction
 * callback function. The 'state' is passed to the callback function. The fate of
 * foreign transaction must be determined. If foreign transaction is resolved
 * successfully, remove the FdwXact entry from the shared memory and also
 * remove the corresponding on-disk file. If failed, the status of FdwXact
 * entry changes to 'fallback_status' before erroring out.
 */
static void
FdwXactResolveForeignTransaction(FdwXact fdwxact, FdwXactRslvState *state,
								 FdwXactStatus fallback_status)
{
	ForeignServer		*server;
	ForeignDataWrapper	*fdw;
	FdwRoutine			*fdw_routine;
	bool				is_commit;

	Assert(state != NULL);
	Assert(state->server && state->usermapping && state->fdwxact_id);
	Assert(fdwxact != NULL);

	LWLockAcquire(FdwXactLock, LW_SHARED);

	if (fdwxact->status != FDWXACT_STATUS_COMMITTING &&
		fdwxact->status != FDWXACT_STATUS_ABORTING)
		elog(ERROR, "cannot resolve foreign transaction whose fate is not determined");

	is_commit = fdwxact->status == FDWXACT_STATUS_COMMITTING;
	LWLockRelease(FdwXactLock);

	server = GetForeignServer(fdwxact->serverid);
	fdw = GetForeignDataWrapper(server->fdwid);
	fdw_routine = GetFdwRoutine(fdw->fdwhandler);

	PG_TRY();
	{
		if (is_commit)
			fdw_routine->CommitForeignTransaction(state);
		else
			fdw_routine->RollbackForeignTransaction(state);
	}
	PG_CATCH();
	{
		/* Back to the fallback status */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		fdwxact->status = fallback_status;
		LWLockRelease(FdwXactLock);

		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Resolution was a success, remove the entry */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	elog(DEBUG1, "successfully %s the foreign transaction with xid %u db %u server %u user %u",
		 is_commit ? "committed" : "rolled back",
		 fdwxact->local_xid, fdwxact->dbid, fdwxact->serverid,
		 fdwxact->userid);

	fdwxact->status = FDWXACT_STATUS_RESOLVED;
	if (fdwxact->ondisk)
		RemoveFdwXactFile(fdwxact->dbid, fdwxact->local_xid,
						  fdwxact->serverid, fdwxact->userid,
						  true);
	remove_fdwxact(fdwxact);
	LWLockRelease(FdwXactLock);
}

/*
 * Return palloc'd and initialized FdwXactRslvState.
 */
static FdwXactRslvState *
create_fdwxact_state(void)
{
	FdwXactRslvState *state;

	state = palloc(sizeof(FdwXactRslvState));
	state->server = NULL;
	state->usermapping = NULL;
	state->fdwxact_id = NULL;
	state->flags = 0;

	return state;
}

/*
 * Return at least one FdwXact entry that matches to given argument,
 * otherwise return NULL. All arguments must be valid values so that it can
 * search exactly one (or none) entry. Note that this function intended to be
 * used for modifying the returned FdwXact entry, so the caller must hold
 * FdwXactLock in exclusive mode and it doesn't include the in-progress
 * FdwXact entries.
 */
static FdwXact
get_one_fdwxact(Oid dbid, TransactionId xid, Oid serverid, Oid userid)
{
	List	*fdwxact_list;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* All search conditions must be valid values */
	Assert(TransactionIdIsValid(xid));
	Assert(OidIsValid(serverid));
	Assert(OidIsValid(userid));
	Assert(OidIsValid(dbid));

	/* Include in-dbout transactions but don't include in-progress ones */
	fdwxact_list = get_fdwxacts(dbid, xid, serverid, userid,
								true, false, false);

	/* Must be one entry since we search it by the unique key */
	Assert(list_length(fdwxact_list) <= 1);

	/* Could not find entry */
	if (fdwxact_list == NIL)
		return NULL;

	return (FdwXact) linitial(fdwxact_list);
}

/*
 * Return true if there is at least one prepared foreign transaction
 * which matches given arguments.
 */
bool
fdwxact_exists(Oid dbid, Oid serverid, Oid userid)
{
	List	*fdwxact_list;

	/* Find entries from all FdwXact entries */
	fdwxact_list = get_fdwxacts(dbid, InvalidTransactionId, serverid,
								userid, true, true, true);

	return fdwxact_list != NIL;
}

/*
 * Returns an array of all foreign prepared transactions for the user-level
 * function pg_foreign_xacts, and the number of entries to num_p.
 *
 * WARNING -- we return even those transactions whose information is not
 * completely filled yet. The caller should filter them out if he doesn't
 * want them.
 *
 * The returned array is palloc'd.
 */
static FdwXact
get_all_fdwxacts(int *num_p)
{
	List		*all_fdwxacts;
	ListCell	*lc;
	FdwXact		fdwxacts;
	int			num_fdwxacts = 0;

	Assert(num_p != NULL);

	/* Get all entries */
	all_fdwxacts = get_fdwxacts(InvalidOid, InvalidTransactionId,
								InvalidOid, InvalidOid, true,
								true, true);

	if (all_fdwxacts == NIL)
	{
		*num_p = 0;
		return NULL;
	}

	fdwxacts = (FdwXact)
		palloc(sizeof(FdwXactData) * list_length(all_fdwxacts));
	*num_p = list_length(all_fdwxacts);

	/* Convert list to array of FdwXact */
	foreach(lc, all_fdwxacts)
	{
		FdwXact fx = (FdwXact) lfirst(lc);

		memcpy(fdwxacts + num_fdwxacts, fx,
			   sizeof(FdwXactData));
		num_fdwxacts++;
	}

	list_free(all_fdwxacts);

	return fdwxacts;
}

/*
 * Return a list of FdwXact matched to given arguments. Otherwise return NIL.
 * The search condition is defined by arguments with valid values for
 * respective datatypes. 'include_indoubt' and 'include_in_progress' are the
 * option for that the result includes in-doubt transactions and in-progress
 * transactions respecitively.
 */
static List*
get_fdwxacts(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
			 bool include_indoubt, bool include_in_progress, bool need_lock)
{
	int i;
	List	*fdwxact_list = NIL;

	if (need_lock)
		LWLockAcquire(FdwXactLock, LW_SHARED);

	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact	fdwxact = FdwXactCtl->fdwxacts[i];

		/* dbid */
		if (OidIsValid(dbid) && fdwxact->dbid != dbid)
			continue;

		/* xid */
		if (TransactionIdIsValid(xid) && xid != fdwxact->local_xid)
			continue;

		/* serverid */
		if (OidIsValid(serverid) && serverid != fdwxact->serverid)
			continue;

		/* userid */
		if (OidIsValid(userid) && fdwxact->userid != userid)
			continue;

		/* include in-doubt transaction? */
		if (!include_indoubt && fdwxact->indoubt)
			continue;

		/* include in-progress transaction? */
		if (!include_in_progress && FdwXactIsBeingResolved(fdwxact))
			continue;

		/* Append it if matched */
		fdwxact_list = lappend(fdwxact_list, fdwxact);
	}

	if (need_lock)
		LWLockRelease(FdwXactLock);

	return fdwxact_list;
}

/* Apply the redo log for a foreign transaction */
void
fdwxact_redo(XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_FDWXACT_INSERT)
	{
		/*
		 * Add fdwxact entry and set start/end lsn of the WAL record
		 * in FdwXact entry.
		 */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoAdd(XLogRecGetData(record),
					   record->ReadRecPtr,
					   record->EndRecPtr);
		LWLockRelease(FdwXactLock);
	}
	else if (info == XLOG_FDWXACT_REMOVE)
	{
		xl_fdwxact_remove *record = (xl_fdwxact_remove *) rec;

		/* Delete FdwXact entry and file if exists */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactRedoRemove(record->dbid, record->xid, record->serverid,
						  record->userid, false);
		LWLockRelease(FdwXactLock);
	}
	else
		elog(ERROR, "invalid log type %d in foreign transction log record", info);

	return;
}

/*
 * Return a null-terminated foreign transaction identifier. If the given
 * foreign server's FDW provides getPrepareId callback we return the identifier
 * returned from it. Otherwise we generate an unique identifier with in the
 * form of "fx_<random number>_<xid>_<serverid>_<userid> whose length is
 * less than FDWXACT_ID_MAX_LEN.
 *
 * Returned string value is used to identify foreign transaction. The
 * identifier should not be same as any other concurrent prepared transaction
 * identifier.
 *
 * To make the foreign transactionid unique, we should ideally use something
 * like UUID, which gives unique ids with high probability, but that may be
 * expensive here and UUID extension which provides the function to generate
 * UUID is not part of the core code.
 */
static char *
get_fdwxact_identifier(FdwXactParticipant *fdw_part, TransactionId xid)
{
	char	*id;
	int		id_len = 0;

	if (!fdw_part->get_prepareid_fn)
	{
		char buf[FDWXACT_ID_MAX_LEN] = {0};

		/*
		 * FDW doesn't provide the callback function, generate an unique
		 * idenetifier.
		 */
		snprintf(buf, FDWXACT_ID_MAX_LEN, "fx_%ld_%u_%d_%d",
			 Abs(random()), xid, fdw_part->server->serverid,
				 fdw_part->usermapping->userid);

		return pstrdup(buf);
	}

	/* Get an unique identifier from callback function */
	id = fdw_part->get_prepareid_fn(xid, fdw_part->server->serverid,
									fdw_part->usermapping->userid,
									&id_len);

	if (id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 (errmsg("foreign transaction identifier is not provided"))));

	/* Check length of foreign transaction identifier */
	if (id_len > FDWXACT_ID_MAX_LEN)
	{
		id[FDWXACT_ID_MAX_LEN] = '\0';
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("foreign transaction identifer \"%s\" is too long",
						id),
				 errdetail("foreign transaction identifier must be less than %d characters.",
						   FDWXACT_ID_MAX_LEN)));
	}

	id[id_len] = '\0';
	return pstrdup(id);
}

/*
 * We must fsync the foreign transaction state file that is valid or generated
 * during redo and has a inserted LSN <= the checkpoint'S redo horizon.
 * The foreign transaction entries and hence the corresponding files are expected
 * to be very short-lived. By executing this function at the end, we might have
 * lesser files to fsync, thus reducing some I/O. This is similar to
 * CheckPointTwoPhase().
 *
 * This is deliberately run as late as possible in the checkpoint sequence,
 * because FdwXacts ordinarily have short lifespans, and so it is quite
 * possible that FdwXacts that were valid at checkpoint start will no longer
 * exist if we wait a little bit. With typical checkpoint settings this
 * will be about 3 minutes for an online checkpoint, so as a result we
 * expect that there will be no FdwXacts that need to be copied to disk.
 *
 * If a FdwXact remains valid across multiple checkpoints, it will already
 * be on disk so we don't bother to repeat that write.
 */
void
CheckPointFdwXacts(XLogRecPtr redo_horizon)
{
	int			cnt;
	int			serialized_fdwxacts = 0;

	if (max_prepared_foreign_xacts <= 0)
		return;						/* nothing to do */

	TRACE_POSTGRESQL_FDWXACT_CHECKPOINT_START();

	/*
	 * We are expecting there to be zero FdwXact that need to be copied to
	 * disk, so we perform all I/O while holding FdwXactLock for simplicity.
	 * This presents any new foreign xacts from preparing while this occurs,
	 * which shouldn't be a problem since the presence fo long-lived prepared
	 * foreign xacts indicated the transaction manager isn't active.
	 *
	 * It's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FdwXact with a
	 * insert_end_lsn set prior to the last checkpoint yet is marked
	 * invalid, because of the efforts with delayChkpt.
	 */
	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (cnt = 0; cnt < FdwXactCtl->num_fdwxacts; cnt++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[cnt];

		if ((fdwxact->valid || fdwxact->inredo) &&
			!fdwxact->ondisk &&
			fdwxact->insert_end_lsn <= redo_horizon)
		{
			char	   *buf;
			int			len;

			XlogReadFdwXactData(fdwxact->insert_start_lsn, &buf, &len);
			RecreateFdwXactFile(fdwxact->dbid, fdwxact->local_xid,
								fdwxact->serverid, fdwxact->userid,
								buf, len);
			fdwxact->ondisk = true;
			fdwxact->insert_start_lsn = InvalidXLogRecPtr;
			fdwxact->insert_end_lsn = InvalidXLogRecPtr;
			pfree(buf);
			serialized_fdwxacts++;
		}
	}

	LWLockRelease(FdwXactLock);

	/*
	 * Flush unconditionally the parent directory to make any information
	 * durable on disk.  FdwXact files could have been removed and those
	 * removals need to be made persistent as well as any files newly created.
	 */
	fsync_fname(FDWXACTS_DIR, true);

	TRACE_POSTGRESQL_FDWXACT_CHECKPOINT_DONE();

	if (log_checkpoints && serialized_fdwxacts > 0)
		ereport(LOG,
			  (errmsg_plural("%u foreign transaction state file was written "
							 "for long-running prepared transactions",
							 "%u foreign transaction state files were written "
							 "for long-running prepared transactions",
							 serialized_fdwxacts,
							 serialized_fdwxacts)));
}

/*
 * Reads foreign transaction data from xlog. During checkpoint this data will
 * be moved to fdwxact files and ReadFdwXactFile should be used instead.
 *
 * Note clearly that this function accesses WAL during normal operation, similarly
 * to the way WALSender or Logical Decoding would do. It does not run during
 * crash recovery or standby processing.
 */
static void
XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;

	xlogreader = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page, NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
		   errdetail("Failed while allocating an XLog reading processor.")));

	record = XLogReadRecord(xlogreader, lsn, &errormsg);
	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
		errmsg("could not read foreign transaction state from xlog at %X/%X",
			   (uint32) (lsn >> 32),
			   (uint32) lsn)));

	if (XLogRecGetRmid(xlogreader) != RM_FDWXACT_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != XLOG_FDWXACT_INSERT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected foreign transaction state data is not present in xlog at %X/%X",
						(uint32) (lsn >> 32),
						(uint32) lsn)));

	if (len != NULL)
		*len = XLogRecGetDataLen(xlogreader);

	*buf = palloc(sizeof(char) * XLogRecGetDataLen(xlogreader));
	memcpy(*buf, XLogRecGetData(xlogreader), sizeof(char) * XLogRecGetDataLen(xlogreader));

	XLogReaderFree(xlogreader);
}

/*
 * Recreates a foreign transaction state file. This is used in WAL replay
 * and during checkpoint creation.
 *
 * Note: content and len don't include CRC.
 */
void
RecreateFdwXactFile(Oid dbid, TransactionId xid, Oid serverid,
					Oid userid, void *content, int len)
{
	char		path[MAXPGPATH];
	pg_crc32c	statefile_crc;
	int			fd;

	/* Recompute CRC */
	INIT_CRC32C(statefile_crc);
	COMP_CRC32C(statefile_crc, content, len);
	FIN_CRC32C(statefile_crc);

	FdwXactFilePath(path, dbid, xid, serverid, userid);

	fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
		errmsg("could not recreate foreign transaction state file \"%s\": %m",
			   path)));

	/* Write content and CRC */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_WRITE);
	if (write(fd, content, len) != len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not write foreign transcation state file: %m")));
	}
	if (write(fd, &statefile_crc, sizeof(pg_crc32c)) != sizeof(pg_crc32c))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not write foreign transcation state file: %m")));
	}
	pgstat_report_wait_end();

	/*
	 * We must fsync the file because the end-of-replay checkpoint will not do
	 * so, there being no FDWXACT in shared memory yet to tell it to.
	 */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_SYNC);
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
			  errmsg("could not fsync foreign transaction state file: %m")));
	pgstat_report_wait_end();

	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close foreign transaction file: %m")));
}

/*
 * Given a transaction id, userid and serverid read it either from disk
 * or read it directly via shmem xlog record pointer using the provided
 * "insert_start_lsn".
 */
static char *
ProcessFdwXactBuffer(Oid dbid, TransactionId xid, Oid serverid,
					 Oid userid, XLogRecPtr insert_start_lsn, bool fromdisk)
{
	TransactionId	origNextXid =
		XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
	char	*buf;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	if (!fromdisk)
		Assert(!XLogRecPtrIsInvalid(insert_start_lsn));

	/* Reject XID if too new */
	if (TransactionIdFollowsOrEquals(xid, origNextXid))
	{
		if (fromdisk)
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state file for xid %u, server %u and user %u",
							xid, serverid, userid)));
			RemoveFdwXactFile(dbid, xid, serverid, userid, true);
		}
		else
		{
			ereport(WARNING,
					(errmsg("removing future fdwxact state from memory for xid %u, server %u and user %u",
							xid, serverid, userid)));
			FdwXactRedoRemove(dbid, xid, serverid, userid, true);
		}
		return NULL;
	}

	if (fromdisk)
	{
		/* Read and validate file */
		buf = ReadFdwXactFile(dbid, xid, serverid, userid);
	}
	else
	{
		/* Read xlog data */
		XlogReadFdwXactData(insert_start_lsn, &buf, NULL);
	}

	return buf;
}

/*
 * Read and validate the foreign transaction state file.
 *
 * If it looks OK (has a valid magic number and CRC), return the palloc'd
 * contents of the file, issuing an error when finding corrupted data.
 * This state can be reached when doing recovery.
 */
static char *
ReadFdwXactFile(Oid dbid, TransactionId xid, Oid serverid, Oid userid)
{
	char		path[MAXPGPATH];
	int			fd;
	FdwXactOnDiskData *fdwxact_file_data;
	struct stat stat;
	uint32		crc_offset;
	pg_crc32c	calc_crc;
	pg_crc32c	file_crc;
	char	   *buf;
	int			r;

	FdwXactFilePath(path, dbid, xid, serverid, userid);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
			   errmsg("could not open FDW transaction state file \"%s\": %m",
					  path)));

	/*
	 * Check file length.  We can determine a lower bound pretty easily. We
	 * set an upper bound to avoid palloc() failure on a corrupt file, though
	 * we can't guarantee that we won't get an out of memory error anyway,
	 * even on a valid file.
	 */
	if (fstat(fd, &stat))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat FDW transaction state file \"%s\": %m",
						path)));

	if (stat.st_size < (offsetof(FdwXactOnDiskData, fdwxact_id) +
						sizeof(pg_crc32c)) ||
		stat.st_size > MaxAllocSize)

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("too large FDW transaction state file \"%s\": %m",
						path)));

	crc_offset = stat.st_size - sizeof(pg_crc32c);
	if (crc_offset != MAXALIGN(crc_offset))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incorrect alignment of CRC offset for file \"%s\"",
						path)));

	/*
	 * Ok, slurp in the file.
	 */
	buf = (char *) palloc(stat.st_size);
	fdwxact_file_data = (FdwXactOnDiskData *) buf;

	/* Slurp the file */
	pgstat_report_wait_start(WAIT_EVENT_FDWXACT_FILE_READ);
	r = read(fd, buf, stat.st_size);
	if (r != stat.st_size)
	{
		if (r < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
		else
			ereport(ERROR,
					(errmsg("could not read file \"%s\": read %d of %zu",
							path, r, (Size) stat.st_size)));
	}
	pgstat_report_wait_end();

	if (CloseTransientFile(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	/*
	 * Check the CRC.
	 */
	INIT_CRC32C(calc_crc);
	COMP_CRC32C(calc_crc, buf, crc_offset);
	FIN_CRC32C(calc_crc);

	file_crc = *((pg_crc32c *) (buf + crc_offset));

	if (!EQ_CRC32C(calc_crc, file_crc))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						path)));

	/* Check if the contents is an expected data */
	fdwxact_file_data = (FdwXactOnDiskData *) buf;
	if (fdwxact_file_data->dbid  != dbid ||
		fdwxact_file_data->serverid != serverid ||
		fdwxact_file_data->userid != userid ||
		fdwxact_file_data->local_xid != xid)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid foreign transaction state file \"%s\"",
						path)));

	return buf;
}

/*
 * Scan the shared memory entries of FdwXact and determine the range of valid
 * XIDs present.  This is run during database startup, after we have completed
 * reading WAL.  ShmemVariableCache->nextFullXid has been set to one more than
 * the highest XID for which evidence exists in WAL.

 * On corrupted two-phase files, fail immediately.  Keeping around broken
 * entries and let replay continue causes harm on the system, and a new
 * backup should be rolled in.

 * Our other responsibility is to update and return the oldest valid XID
 * among the distributed transactions. This is needed to synchronize pg_subtrans
 * startup properly.
 */
TransactionId
PrescanFdwXacts(TransactionId oldestActiveXid)
{
	FullTransactionId nextFullXid = ShmemVariableCache->nextFullXid;
	TransactionId origNextXid = XidFromFullTransactionId(nextFullXid);
	TransactionId result = origNextXid;
	int i;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact fdwxact = FdwXactCtl->fdwxacts[i];
		char *buf;

		buf = ProcessFdwXactBuffer(fdwxact->dbid, fdwxact->local_xid,
								   fdwxact->serverid, fdwxact->userid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		if (TransactionIdPrecedes(fdwxact->local_xid, result))
			result = fdwxact->local_xid;

		pfree(buf);
	}
	LWLockRelease(FdwXactLock);

	return result;
}

/*
 * Scan pg_fdwxact and fill FdwXact depending on the on-disk data.
 * This is called once at the beginning of recovery, saving any extra
 * lookups in the future.  FdwXact files that are newer than the
 * minimum XID horizon are discarded on the way.
 */
void
restoreFdwXactData(void)
{
	DIR		   *cldir;
	struct dirent *clde;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	cldir = AllocateDir(FDWXACTS_DIR);
	while ((clde = ReadDir(cldir, FDWXACTS_DIR)) != NULL)
	{
		if (strlen(clde->d_name) == FDWXACT_FILE_NAME_LEN &&
			strspn(clde->d_name, "0123456789ABCDEF_") == FDWXACT_FILE_NAME_LEN)
		{
			TransactionId local_xid;
			Oid			dbid;
			Oid			serverid;
			Oid			userid;
			char		*buf;

			sscanf(clde->d_name, "%08x_%08x_%08x_%08x",
				   &dbid, &local_xid, &serverid, &userid);

			/* Read fdwxact data from disk */
			buf = ProcessFdwXactBuffer(dbid, local_xid, serverid, userid,
									   InvalidXLogRecPtr, true);

			if (buf == NULL)
				continue;

			/* Add this entry into the table of foreign transactions */
			FdwXactRedoAdd(buf, InvalidXLogRecPtr, InvalidXLogRecPtr);
		}
	}

	LWLockRelease(FdwXactLock);
	FreeDir(cldir);
}

/*
 * Remove the foreign transaction file for given entry.
 *
 * If giveWarning is false, do not complain about file-not-present;
 * this is an expected case during WAL replay.
 */
static void
RemoveFdwXactFile(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
				  bool giveWarning)
{
	char		path[MAXPGPATH];

	FdwXactFilePath(path, dbid, xid, serverid, userid);
	if (unlink(path) < 0 && (errno != ENOENT || giveWarning))
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove foreign transaction state file \"%s\": %m",
						path)));
}

/*
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdwxact entry in shared memory FdwXactData structure.
 */
static void
FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	FdwXactOnDiskData *fdwxact_data = (FdwXactOnDiskData *) buf;
	FdwXact fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	/*
	 * Add this entry into the table of foreign transactions. The
	 * status of the transaction is set as preparing, since we do not
	 * know the exact status right now. Resolver will set it later
	 * based on the status of local transaction which prepared this
	 * foreign transaction.
	 */
	fdwxact = insert_fdwxact(fdwxact_data->dbid, fdwxact_data->local_xid,
							  fdwxact_data->serverid, fdwxact_data->userid,
							  fdwxact_data->umid, fdwxact_data->fdwxact_id);

	elog(DEBUG2, "added fdwxact entry in shared memory for foreign transaction, db %u xid %u server %u user %u id %s",
		 fdwxact_data->dbid, fdwxact_data->local_xid,
		 fdwxact_data->serverid, fdwxact_data->userid,
		 fdwxact_data->fdwxact_id);

	/*
	 * Set status as PREPARED and as in-doubt, since we do not know
	 * the xact status right now. Resolver will set it later based on
	 * the status of local transaction that prepared this fdwxact entry.
	 */
	fdwxact->status = FDWXACT_STATUS_PREPARED;
	fdwxact->insert_start_lsn = start_lsn;
	fdwxact->insert_end_lsn = end_lsn;
	fdwxact->inredo = true;	/* added in redo */
	fdwxact->indoubt = true;
	fdwxact->valid = false;
	fdwxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
}

/*
 * Remove the corresponding fdwxact entry from FdwXactCtl. Also remove
 * FdwXact file if a foreign transaction was saved via an earlier checkpoint.
 * We could not found the FdwXact entry in the case where a crash recovery
 * starts from the point where is after added but before removed the entry.
 */
void
FdwXactRedoRemove(Oid dbid, TransactionId xid, Oid serverid,
				  Oid userid, bool givewarning)
{
	FdwXact	fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	fdwxact = get_one_fdwxact(dbid, xid, serverid, userid);

	if (fdwxact == NULL)
		return;

	elog(DEBUG2, "removed fdwxact entry from shared memory for foreign transaction, db %u xid %u server %u user %u id %s",
		 fdwxact->dbid, fdwxact->local_xid, fdwxact->serverid,
		 fdwxact->userid, fdwxact->fdwxact_id);

	/* Clean up entry and any files we may have left */
	if (fdwxact->ondisk)
		RemoveFdwXactFile(fdwxact->dbid, fdwxact->local_xid,
						  fdwxact->serverid, fdwxact->userid,
						  givewarning);
	remove_fdwxact(fdwxact);
}

/*
 * Scan the shared memory entries of FdwXact and valid them.
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 */
void
RecoverFdwXacts(void)
{
	int i;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact fdwxact = FdwXactCtl->fdwxacts[i];
		char	*buf;

		buf = ProcessFdwXactBuffer(fdwxact->dbid, fdwxact->local_xid,
								   fdwxact->serverid, fdwxact->userid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		ereport(LOG,
				(errmsg("recovering foreign transaction %u for server %u and user %u from shared memory",
						fdwxact->local_xid, fdwxact->serverid, fdwxact->userid)));

		/* recovered, so reset the flag for entries generated by redo */
		fdwxact->inredo = false;
		fdwxact->valid = true;

		/*
		 * If the foreign transaction is part of the prepared local
		 * transaction, it's not in in-doubt. The future COMMIT/ROLLBACK
		 * PREPARED can determine the fate of this foreign transaction.
		 */
		if (TwoPhaseExists(fdwxact->local_xid))
		{
			ereport(DEBUG2,
					(errmsg("clear in-doubt flag from foreign transaction %u, server %u, user %u as found the corresponding local prepared transaction",
							fdwxact->local_xid, fdwxact->serverid,
							fdwxact->userid)));
			fdwxact->indoubt = false;
		}

		pfree(buf);
	}
	LWLockRelease(FdwXactLock);
}

bool
check_foreign_twophase_commit(int *newval, void **extra, GucSource source)
{
	ForeignTwophaseCommitLevel newForeignTwophaseCommitLevel = *newval;

	/* Parameter check */
	if (newForeignTwophaseCommitLevel > FOREIGN_TWOPHASE_COMMIT_DISABLED &&
		(max_prepared_foreign_xacts == 0 || max_foreign_xact_resolvers == 0))
	{
		GUC_check_errdetail("Cannot enable \"foreign_twophase_commit\" when "
							"\"max_prepared_foreign_transactions\" or \"max_foreign_transaction_resolvers\""
							"is zero value");
		return false;
	}

	return true;
}

/* Built in functions */

/*
 * Structure to hold and iterate over the foreign transactions to be displayed
 * by the built-in functions.
 */
typedef struct
{
	FdwXact		fdwxacts;
	int			num_xacts;
	int			cur_xact;
}	WorkingStatus;

Datum
pg_foreign_xacts(PG_FUNCTION_ARGS)
{
#define PG_PREPARED_FDWXACTS_COLS	7
	FuncCallContext *funcctx;
	WorkingStatus *status;
	char	   *xact_status;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		int			num_fdwxacts = 0;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		/* this had better match pg_fdwxacts view in system_views.sql */
		tupdesc = CreateTemplateTupleDesc(PG_PREPARED_FDWXACTS_COLS);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dbid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "transaction",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "serverid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "userid",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "indoubt",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "identifier",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/*
		 * Collect status information that we will format and send out as a
		 * result set.
		 */
		status = (WorkingStatus *) palloc(sizeof(WorkingStatus));
		funcctx->user_fctx = (void *) status;

		status->fdwxacts = get_all_fdwxacts(&num_fdwxacts);
		status->num_xacts = num_fdwxacts;
		status->cur_xact = 0;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status = funcctx->user_fctx;

	while (status->cur_xact < status->num_xacts)
	{
		FdwXact		fdwxact = &status->fdwxacts[status->cur_xact++];
		Datum		values[PG_PREPARED_FDWXACTS_COLS];
		bool		nulls[PG_PREPARED_FDWXACTS_COLS];
		HeapTuple	tuple;
		Datum		result;

		if (!fdwxact->valid)
			continue;

		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(fdwxact->dbid);
		values[1] = TransactionIdGetDatum(fdwxact->local_xid);
		values[2] = ObjectIdGetDatum(fdwxact->serverid);
		values[3] = ObjectIdGetDatum(fdwxact->userid);

		switch (fdwxact->status)
		{
			case FDWXACT_STATUS_INITIAL:
				xact_status = "initial";
				break;
			case FDWXACT_STATUS_PREPARING:
				xact_status = "preparing";
				break;
			case FDWXACT_STATUS_PREPARED:
				xact_status = "prepared";
				break;
			case FDWXACT_STATUS_COMMITTING:
				xact_status = "committing";
				break;
			case FDWXACT_STATUS_ABORTING:
				xact_status = "aborting";
				break;
			case FDWXACT_STATUS_RESOLVED:
				xact_status = "resolved";
				break;
			default:
				xact_status = "unknown";
				break;
		}
		values[4] = CStringGetTextDatum(xact_status);
		values[5] = BoolGetDatum(fdwxact->indoubt);
		values[6] = PointerGetDatum(cstring_to_text_with_len(fdwxact->fdwxact_id,
															 strlen(fdwxact->fdwxact_id)));

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Built-in function to resolve a prepared foreign transaction manually.
 */
Datum
pg_resolve_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId	xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid				serverid = PG_GETARG_OID(1);
	Oid				userid = PG_GETARG_OID(2);
	ForeignServer	*server;
	UserMapping		*usermapping;
	FdwXact			fdwxact;
	FdwXactRslvState	*state;
	FdwXactStatus		prev_status;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to resolve foreign transactions"))));

	server = GetForeignServer(serverid);
	usermapping = GetUserMapping(userid, serverid);
	state = create_fdwxact_state();

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_one_fdwxact(MyDatabaseId, xid, serverid, userid);

	if (fdwxact == NULL)
	{
		LWLockRelease(FdwXactLock);
		PG_RETURN_BOOL(false);
	}

	state->server = server;
	state->usermapping = usermapping;
	state->fdwxact_id = pstrdup(fdwxact->fdwxact_id);

	SpinLockAcquire(&fdwxact->mutex);
	prev_status = fdwxact->status;
	SpinLockRelease(&fdwxact->mutex);

	FdwXactDetermineTransactionFate(fdwxact, false);

	LWLockRelease(FdwXactLock);

	FdwXactResolveForeignTransaction(fdwxact, state, prev_status);

	PG_RETURN_BOOL(true);
}

/*
 * Built-in function to remove a prepared foreign transaction entry without
 * resolution. The function gives a way to forget about such prepared
 * transaction in case: the foreign server where it is prepared is no longer
 * available, the user which prepared this transaction needs to be dropped.
 */
Datum
pg_remove_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId	xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid				serverid = PG_GETARG_OID(1);
	Oid				userid = PG_GETARG_OID(2);
	FdwXact			fdwxact;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to remove foreign transactions"))));

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	fdwxact = get_one_fdwxact(MyDatabaseId, xid, serverid, userid);

	if (fdwxact == NULL)
		PG_RETURN_BOOL(false);

	remove_fdwxact(fdwxact);

	LWLockRelease(FdwXactLock);

	PG_RETURN_BOOL(true);
}
