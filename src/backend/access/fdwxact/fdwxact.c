/*-------------------------------------------------------------------------
 *
 * fdwxact.c
 *		PostgreSQL global transaction manager for foreign servers.
 *
 * This module contains the code for managing transactions started on foreign
 * servers.
 *
 * FDW who implements both commit and rollback APIs can request to register the
 * foreign transaction by FdwXactRegisterXact() to participate it to a
 * group of distributed tranasction.  The registered foreign transactions are
 * identified by OIDs of server and user.  On commit, rollback and prepare, the
 * global transaction manager calls corresponding FDW API to end the tranasctions.
 *
 * To achieve commit among all foreign servers atomically, the global transaction
 * manager supports two-phase commit protocol, which is a type of atomic commitment
 * protocol(ACP). Two-phase commit protocol is crash-safe.  We WAL logs the foreign
 * transaction information.
 *
 * FOREIGN TRANSACTION RESOLUTION
 *
 * The transaction involving multiple foreign transactions uses two-phase commit
 * protocol to commit the distributed transaction if enabled.  The basic strategy
 * is that we prepare all of the remote transactions before committing locally and
 * commit them after committing locally.
 *
 * At pre-commit of local transaction, we prepare the transactions on all foreign
 * servers after logging the information of foreign transaction.  The result of
 * distributed transaction is determined by the result of the corresponding local
 * transaction.  Once the local transaction is successfully committed, all
 * transactions on foreign servers must be committed.  In case where an error occurred
 * before the local transaction commit all transactions must be aborted.  After
 * committing or rolling back locally, we leave foreign transactions as in-doubt
 * transactions and then notify the resolver process. The resolver process asynchronously
 * resolves these foreign transactions according to the result of the corresponding local
 * transaction.  Also, the user can use pg_resolve_foreign_xact() SQL function to
 * resolve a foreign transaction manually.
 *
 * At PREPARE TRANSACTION, we prepare all transactions on foreign servers by executing
 * PrepareForeignTransaction() API regardless of data on the foreign server having been
 * modified.  At COMMIT PREPARED and ROLLBACK PREPARED, we commit or rollback only the
 * local transaction but not do anything for involved foreign transactions.
 *
 * LOCKING
 *
 * Whenever a foreign transaction is processed, the corresponding FdwXact
 * entry is update.	 To avoid holding the lock during transaction processing
 * which may take an unpredicatable time the in-memory data of foreign
 * transaction follows a locking model based on the following linked concepts:
 *
 * * All FdwXact fields except for status are protected by FdwXactLock. The
 *	 status is protected by its mutex.
 * * A process who is going to process foreign transaction needs to set
 *   locking_backend of the FdwXact entry to lock the entry, which prevents the entry from
 *	 being updated and removed by concurrent processes.
 *
 * RECOVERY
 *
 * During replay WAL and replication FdwXactCtl also holds information about
 * active prepared foreign transaction that haven't been moved to disk yet.
 *
 * Replay of fdwxact records happens by the following rules:
 *
 * * At the beginning of recovery, pg_fdwxacts is scanned once, filling FdwXact
 *	 with entries marked with fdwxact->inredo and fdwxact->ondisk.	FdwXact file
 *	 data older than the XID horizon of the redo position are discarded.
 * * On PREPARE redo, the foreign transaction is added to FdwXactCtl->fdwxacts.
 *	 We set fdwxact->inredo to true for such entries.
 * * On Checkpoint we iterate through FdwXactCtl->fdwxacts entries that
 *	 have fdwxact->inredo set and are behind the redo_horizon.	We save
 *	 them to disk and then set fdwxact->ondisk to true.
 * * On resolution we delete the entry from FdwXactCtl->fdwxacts.  If
 *	 fdwxact->ondisk is true, the corresponding entry from the disk is
 *	 additionally deleted.
 * * RecoverFdwXacts() and PrescanFdwXacts() have been modified to go through
 *	 fdwxact->inredo entries that have not made it to disk.
 *
 * These replay rules are borrowed from twophase.c
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/fdwxact/fdwxact.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/fdwxact.h"
#include "access/fdwxact_resolver.h"
#include "access/fdwxact_launcher.h"
#include "access/twophase.h"
#include "access/resolver_internal.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Check the FdwXactParticipant is capable of two-phase commit  */
#define ServerSupportTransactionCallback(fdw_part) \
	(((FdwXactParticipant *)(fdw_part))->commit_foreign_xact_fn != NULL)
#define ServerSupportTwophaseCommit(fdw_part) \
	(((FdwXactParticipant *)(fdw_part))->prepare_foreign_xact_fn != NULL)

/* Foreign twophase commit is enabled and requested by user */
#define IsForeignTwophaseCommitRequested() \
	 (foreign_twophase_commit > FOREIGN_TWOPHASE_COMMIT_DISABLED)

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

/*
 * Structure to bundle the foreign transaction participant.	 This struct
 * needs to live until the end of transaction where we cannot look at
 * syscaches. Therefore, this is allocated in the TopTransactionContext.
 */
typedef struct FdwXactParticipant
{
	/*
	 * Pointer to a FdwXact entry in the global array. NULL if the entry is
	 * not inserted yet but this is registered as a participant.
	 */
	FdwXact		fdwxact;

	/* Foreign server and user mapping info, passed to callback routines */
	ForeignServer *server;
	UserMapping *usermapping;

	/* Transaction identifier used for PREPARE */
	char	   *fdwxact_id;

	/* true if modified the data on the server */
	bool		modified;

	/* Callbacks for foreign transaction */
	CommitForeignTransaction_function commit_foreign_xact_fn;
	RollbackForeignTransaction_function rollback_foreign_xact_fn;
	PrepareForeignTransaction_function prepare_foreign_xact_fn;
	GetPrepareId_function get_prepareid_fn;
} FdwXactParticipant;

/*
 * List of foreign transactions involved in the transaction.  A member of
 * participants must support both commit and rollback APIs.
 *
 * ForeignTwophaseCommitIsRequired is true if the current transaction needs to
 * be committed using two-phase commit protocol.
 */
static List *FdwXactParticipants = NIL;
static bool ForeignTwophaseCommitIsRequired = false;

/* Keep track of registering process exit call back. */
static bool fdwXactExitRegistered = false;


/* Guc parameter */
int			max_prepared_foreign_xacts = 0;
int			max_foreign_xact_resolvers = 0;
int			foreign_twophase_commit = FOREIGN_TWOPHASE_COMMIT_DISABLED;

static void AtProcExit_FdwXact(int code, Datum arg);
static void FdwXactPrepareForeignTransactions(TransactionId xid, bool prepare_all);
static void ForgetAllFdwXactParticipants(void);
static void FdwXactParticipantEndTransaction(FdwXactParticipant *fdw_part,
											 bool commit);
static FdwXact FdwXactInsertFdwXactEntry(TransactionId xid,
										 FdwXactParticipant *fdw_part);
static void FdwXactComputeRequiredXmin(void);
static FdwXactStatus FdwXactGetTransactionFate(TransactionId xid);
static void FdwXactResolveOneFdwXact(FdwXact fdwxact);
static void FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn);
static void FdwXactRedoRemove(Oid dbid, TransactionId xid, Oid serverid,
							  Oid userid, bool givewarning);
static void XlogReadFdwXactData(XLogRecPtr lsn, char **buf, int *len);
static char *ProcessFdwXactBuffer(Oid dbid, TransactionId xid, Oid serverid,
								  Oid userid, XLogRecPtr insert_start_lsn,
								  bool fromdisk);
static char *ReadFdwXactFile(Oid dbid, TransactionId xid, Oid serverid, Oid userid);
static void RemoveFdwXactFile(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
							  bool giveWarning);
static bool checkForeignTwophaseCommitRequired(bool local_modified);
static FdwXact insert_fdwxact(Oid dbid, TransactionId xid, Oid serverid, Oid userid,
							  Oid umid, char *fdwxact_id);
static void remove_fdwxact(FdwXact fdwxact);
static FdwXactParticipant *create_fdwxact_participant(Oid serverid, Oid userid,
													  FdwRoutine *routine);
static char *get_fdwxact_identifier(FdwXactParticipant *fdw_part,
									TransactionId xid);
static int	get_fdwxact(Oid dbid, TransactionId xid, Oid serverid, Oid userid);

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
			SpinLockInit(&fdwxacts[cnt].mutex);
		}
	}
	else
	{
		Assert(FdwXactCtl);
		Assert(found);
	}
}

/*
 * Register the given foreign transaction identified by the given arguments
 * as a participant of the transaction.
 */
void
FdwXactRegisterXact(Oid serverid, Oid userid, bool modified)
{
	FdwXactParticipant *fdw_part;
	MemoryContext old_ctx;
	FdwRoutine *routine;
	ListCell   *lc;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->server->serverid == serverid &&
			fdw_part->usermapping->userid == userid)
		{
			/* Already registered */
			fdw_part->modified |= modified;
			return;
		}
	}

	/* on first call, register the exit hook */
	if (!fdwXactExitRegistered)
	{
		before_shmem_exit(AtProcExit_FdwXact, 0);
		fdwXactExitRegistered = true;
	}

	routine = GetFdwRoutineByServerId(serverid);

	/*
	 * Foreign server managed by the transaction manager must implement
	 * transaction callbacks.
	 */
	if (!routine->CommitForeignTransaction)
		ereport(ERROR,
				(errmsg("cannot register foreign server not supporting transaction callback")));

	/*
	 * Participant's information is also used at the end of a transaction,
	 * where system cache are not available. Save it in TopTransactionContext
	 * so that these can live until the end of transaction.
	 */
	old_ctx = MemoryContextSwitchTo(TopTransactionContext);

	fdw_part = create_fdwxact_participant(serverid, userid, routine);
	fdw_part->modified = modified;

	/* Add to the participants list */
	FdwXactParticipants = lappend(FdwXactParticipants, fdw_part);

	/* Revert back the context */
	MemoryContextSwitchTo(old_ctx);
}

/* Remove the given foreign server from FdwXactParticipants */
void
FdwXactUnregisterXact(Oid serverid, Oid userid)
{
	ListCell   *lc;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (fdw_part->server->serverid == serverid &&
			fdw_part->usermapping->userid == userid)
		{
			/* Remove the entry */
			FdwXactParticipants =
				foreach_delete_current(FdwXactParticipants, lc);
			break;
		}
	}
}

/* Return palloc'd FdwXactParticipant variable */
static FdwXactParticipant *
create_fdwxact_participant(Oid serverid, Oid userid, FdwRoutine *routine)
{
	FdwXactParticipant *fdw_part;
	ForeignServer *foreign_server;
	UserMapping *user_mapping;

	foreign_server = GetForeignServer(serverid);
	user_mapping = GetUserMapping(userid, serverid);

	fdw_part = (FdwXactParticipant *) palloc(sizeof(FdwXactParticipant));

	fdw_part->fdwxact = NULL;
	fdw_part->server = foreign_server;
	fdw_part->usermapping = user_mapping;
	fdw_part->fdwxact_id = NULL;
	fdw_part->modified = false;
	fdw_part->commit_foreign_xact_fn = routine->CommitForeignTransaction;
	fdw_part->rollback_foreign_xact_fn = routine->RollbackForeignTransaction;
	fdw_part->prepare_foreign_xact_fn = routine->PrepareForeignTransaction;
	fdw_part->get_prepareid_fn = routine->GetPrepareId;

	return fdw_part;
}

 /*
 * Prepare all foreign transactions if foreign twophase commit is required.
 * When foreign twophase commit is enabled, the behavior depends on the value
 * of foreign_twophase_commit; when 'required' we strictly require for all
 * foreign servers' FDW to support two-phase commit protocol and ask them to
 * prepare foreign transactions, and when 'disabled' since we use one-phase
 * commit these foreign transactions are committed at the transaction end.
 * If we failed to prepare any of them we change to aborting.
 */
void
PreCommit_FdwXact(void)
{
	TransactionId xid;
	bool		local_modified;

	/* If there are no foreign servers involved, we have no business here */
	if (FdwXactParticipants == NIL)
		return;

	Assert(!RecoveryInProgress());

	/*
	 * Check if the current transaction did writes.	 We need to include the
	 * local node to the distributed transaction participant and to regard it
	 * as modified, if the current transaction has performed WAL logging and
	 * has assigned an xid.	 The transaction can end up not writing any WAL,
	 * even if it has an xid, if it only wrote to temporary and/or unlogged
	 * tables.	It can end up having written WAL without an xid if did HOT
	 * pruning.
	 */
	xid = GetTopTransactionIdIfAny();
	local_modified = (TransactionIdIsValid(xid) && (XactLastRecEnd != 0));

	/*
	 * Check if we need to use foreign twophase commit. Note that we don't
	 * support foreign twophase commit in single user mode.
	 */
	if (IsUnderPostmaster && checkForeignTwophaseCommitRequired(local_modified))
	{
		/*
		 * Two-phase commit is required.  Assign a transaction id to the
		 * current transaction if not yet because the local transaction is
		 * necessary to determine the result of the distributed transaction.
		 * Then we prepare foreign transactions on foreign servers that support
		 * two-phase commit.  Note that we keep FdwXactParticipants until the
		 * end of the transaction.
		 */
		if (!TransactionIdIsValid(xid))
			xid = GetTopTransactionId();
		FdwXactPrepareForeignTransactions(xid, false);
		ForeignTwophaseCommitIsRequired = true;
	}
}

/* Return true if the current transaction needs to use two-phase commit */
bool
FdwXactIsForeignTwophaseCommitRequired(void)
{
	return ForeignTwophaseCommitIsRequired;
}

/*
 * Return true if the current transaction modifies data on two or more servers
 * in FdwXactParticipants and local server itself.
 */
static bool
checkForeignTwophaseCommitRequired(bool local_modified)
{
	ListCell   *lc;
	bool		have_notwophase = false;
	int			nserverswritten = 0;

	if (!IsForeignTwophaseCommitRequested())
		return false;

	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (!fdw_part->modified)
			continue;

		if (!ServerSupportTwophaseCommit(fdw_part))
			have_notwophase = true;

		nserverswritten++;
	}

	/* Did we modify the local non-temporary data? */
	if (local_modified)
		nserverswritten++;

	/*
	 * Two-phase commit is not required if the number of servers performed
	 * writes is less than 2.
	 */
	if (nserverswritten < 2)
		return false;

	Assert(foreign_twophase_commit == FOREIGN_TWOPHASE_COMMIT_REQUIRED);

	/* Two-phase commit is required. Check parameters */
	if (max_prepared_foreign_xacts == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign two-phase commit is required but prepared foreign transactions are disabled"),
				 errhint("Set max_prepared_foreign_transactions to a nonzero value.")));

	if (max_foreign_xact_resolvers == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("foreign two-phase commit is required but prepared foreign transactions are disabled"),
				 errhint("Set max_foreign_transaction_resolvers to a nonzero value.")));

	if (have_notwophase)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot process a distributed transaction that has operated on a foreign server that does not support two-phase commit protocol"),
				 errdetail("foreign_twophase_commit is \'required\' but the transaction has some foreign servers which are not capable of two-phase commit")));

	return true;
}

/*
 * Insert FdwXact entries and prepare foreign transactions.  If prepare_all is
 * true, we prepare all foreign transaction regardless of writes having happened
 * on the server.
 *
 * We still can change to rollback here on failure. If any error occurs, we
 * rollback non-prepared foreign transactions.
 */
static void
FdwXactPrepareForeignTransactions(TransactionId xid, bool prepare_all)
{
	ListCell   *lc;

	Assert(FdwXactParticipants != NIL);
	Assert(TransactionIdIsValid(xid));

	/* Loop over the foreign connections */
	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);
		FdwXactRslvState state;
		FdwXact		fdwxact;

		Assert(ServerSupportTwophaseCommit(fdw_part));

		CHECK_FOR_INTERRUPTS();

		if (!prepare_all && !fdw_part->modified)
			continue;

		/* Get prepared transaction identifier */
		fdw_part->fdwxact_id = get_fdwxact_identifier(fdw_part, xid);
		Assert(fdw_part->fdwxact_id);

		/*
		 * Insert the foreign transaction entry with the
		 * FDWXACT_STATUS_PREPARING status. Registration persists this
		 * information to the disk and logs (that way relaying it on standby).
		 * Thus in case we loose connectivity to the foreign server or crash
		 * ourselves, we will remember that we might have prepared transaction
		 * on the foreign server and try to resolve it when connectivity is
		 * restored or after crash recovery.
		 *
		 * If we prepare the transaction on the foreign server before
		 * persisting the information to the disk and crash in-between these
		 * two steps, we will lost the prepared transaction on the foreign
		 * server and will not be able to resolve it after the crash recovery.
		 * Hence persist first then prepare.
		 */
		fdwxact = FdwXactInsertFdwXactEntry(xid, fdw_part);

		/*
		 * Prepare the foreign transaction.
		 *
		 * Between FdwXactInsertFdwXactEntry call till this backend hears
		 * acknowledge from foreign server, the backend may abort the local
		 * transaction (say, because of a signal).
		 */
		state.server = fdw_part->server;
		state.usermapping = fdw_part->usermapping;
		state.fdwxact_id = fdw_part->fdwxact_id;
		fdw_part->prepare_foreign_xact_fn(&state);

		/* succeeded, update status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwxact->status = FDWXACT_STATUS_PREPARED;
		SpinLockRelease(&fdwxact->mutex);
	}
}

/*
 * Return a null-terminated foreign transaction identifier.  If the given
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
	char *id;
	int	id_len;

	/*
	 * If FDW doesn't provide the callback function, generate an unique
	 * identifier.
	 */
	if (!fdw_part->get_prepareid_fn)
	{
		char		buf[FDWXACT_ID_MAX_LEN] = {0};

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
				 errmsg("foreign transaction identifier \"%s\" is too long",
						id),
				 errdetail("Foreign transaction identifier must be less than %d characters.",
						   FDWXACT_ID_MAX_LEN)));
	}

	id[id_len] = '\0';
	return pstrdup(id);
}

/*
 * This function is used to create new foreign transaction entry before an FDW
 * prepares and commit/rollback. The function adds the entry to WAL and it will
 * be persisted to the disk under pg_fdwxact directory when checkpoint.
 */
static FdwXact
FdwXactInsertFdwXactEntry(TransactionId xid, FdwXactParticipant *fdw_part)
{
	FdwXact		fdwxact;
	FdwXactOnDiskData *fdwxact_file_data;
	MemoryContext old_context;
	int			data_len;

	old_context = MemoryContextSwitchTo(TopTransactionContext);

	/*
	 * Enter the foreign transaction in the shared memory structure.
	 */
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	fdwxact = insert_fdwxact(MyDatabaseId, xid, fdw_part->server->serverid,
							 fdw_part->usermapping->userid,
							 fdw_part->usermapping->umid, fdw_part->fdwxact_id);
	fdwxact->locking_backend = MyBackendId;
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
	MyProc->delayChkpt = true;

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
	MyProc->delayChkpt = false;

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
	FdwXact		fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

	/* Check for duplicated foreign transaction entry */
	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		fdwxact = FdwXactCtl->fdwxacts[i];
		if (fdwxact->valid &&
			fdwxact->dbid == dbid &&
			fdwxact->local_xid == xid &&
			fdwxact->serverid == serverid &&
			fdwxact->userid == userid)
			ereport(ERROR, (errmsg("could not insert a foreign transaction entry"),
							errdetail("Duplicate entry with transaction id %u, serverid %u, userid %u exists.",
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

	fdwxact->status = FDWXACT_STATUS_PREPARING;
	fdwxact->local_xid = xid;
	fdwxact->dbid = dbid;
	fdwxact->serverid = serverid;
	fdwxact->userid = userid;
	fdwxact->umid = umid;
	fdwxact->insert_start_lsn = InvalidXLogRecPtr;
	fdwxact->insert_end_lsn = InvalidXLogRecPtr;
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;
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
	int			i;

	Assert(fdwxact != NULL);
	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));

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
				 errdetail("Failed to find entry for xid %u, foreign server %u, and user %u.",
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
	fdwxact->locking_backend = InvalidBackendId;
	fdwxact->valid = false;
	fdwxact->ondisk = false;
	fdwxact->inredo = false;

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
		 * here, otherwise a checkpoint starting immediately after the WAL
		 * record is inserted could complete without fsync'ing our state file.
		 * (This is essentially the same kind of race condition as the
		 * COMMIT-to-clog-write case that RecordTransactionCommit uses
		 * delayChkpt for; see notes there.)
		 */
		START_CRIT_SECTION();

		MyProc->delayChkpt = true;

		/*
		 * Log that we are removing the foreign transaction entry and remove
		 * the file from the disk as well.
		 */
		XLogBeginInsert();
		XLogRegisterData((char *) &record, sizeof(xl_fdwxact_remove));
		recptr = XLogInsert(RM_FDWXACT_ID, XLOG_FDWXACT_REMOVE);
		XLogFlush(recptr);

		/* Now we can mark ourselves as out of the commit critical section */
		MyProc->delayChkpt = false;

		END_CRIT_SECTION();
	}
}

/*
 * When the process exits, forget all the entries.
 */
static void
AtProcExit_FdwXact(int code, Datum arg)
{
	ForgetAllFdwXactParticipants();
}

/*
 * The routine for committing or rolling back the given transaction participant.
 */
static void
FdwXactParticipantEndTransaction(FdwXactParticipant *fdw_part, bool commit)
{
	FdwXactRslvState state;

	Assert(ServerSupportTransactionCallback(fdw_part));

	state.server = fdw_part->server;
	state.usermapping = fdw_part->usermapping;
	state.fdwxact_id = NULL;
	state.flags = FDWXACT_FLAG_ONEPHASE;

	if (commit)
	{
		fdw_part->commit_foreign_xact_fn(&state);
		elog(DEBUG1, "successfully committed the foreign transaction for server %u user %u",
			 fdw_part->usermapping->serverid,
			 fdw_part->usermapping->userid);
	}
	else
	{
		fdw_part->rollback_foreign_xact_fn(&state);
		elog(DEBUG1, "successfully rolled back the foreign transaction for server %u user %u",
			 fdw_part->usermapping->serverid,
			 fdw_part->usermapping->userid);
	}
}

/*
 * Unlock foreign transaction participants and clear the FdwXactParticipants
 * list.  If we left foreign transaction, update the oldest xmin of unresolved
 * transaction so that local transaction id of such unresolved foreign transaction
 * is not truncated.
 */
static void
ForgetAllFdwXactParticipants(void)
{
	ListCell   *cell;
	int			nlefts = 0;

	if (FdwXactParticipants == NIL)
	{
		Assert(!ForeignTwophaseCommitIsRequired);
		return;
	}

	foreach(cell, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(cell);
		FdwXact		fdwxact = fdw_part->fdwxact;

		/* Nothing to do if didn't register FdwXact entry yet */
		if (!fdwxact)
			continue;

		/* Unlock the foreign transaction entry */
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		fdwxact->locking_backend = InvalidBackendId;
		LWLockRelease(FdwXactLock);
		nlefts++;
	}

	/*
	 * If we leave any FdwXact entries, update the oldest local transaction of
	 * unresolved distributed transaction and notify the launcher.
	 */
	if (nlefts > 0)
	{
		elog(DEBUG1, "left %u foreign transactions", nlefts);
		FdwXactComputeRequiredXmin();
		FdwXactLaunchOrWakeupResolver();
	}

	list_free_deep(FdwXactParticipants);
	FdwXactParticipants = NIL;
}

/*
 * Close in-progress involved foreign transactions.  We don't perform the second
 * phase of two-phase commit protocol here.  All prepared foreign transactions
 * enter in-doubt state and a resolver process will process them.
 */
void
AtEOXact_FdwXact(bool is_commit)
{
	ListCell   *lc;

	/* If there are no foreign servers involved, we have no business here */
	if (FdwXactParticipants == NIL)
		return;

	Assert(!RecoveryInProgress());

	/* Commit or rollback foreign transactions in the participant list */
	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);
		FdwXact		fdwxact = fdw_part->fdwxact;

		if (!fdwxact)
		{
			/*
			 * If this participant doesn't have an FdwXact entry, it's not
			 * prepared yet. Therefore we can commit or rollback it in one-phase.
			 */
			Assert(ServerSupportTransactionCallback(fdw_part));
			FdwXactParticipantEndTransaction(fdw_part, is_commit);
			continue;
		}

		/*
		 * This foreign transaction might have been prepared.  In commit case,
		 * we don't need to anything for this participant because all foreign
		 * transactions should have already been prepared and therefore the
		 * transaction already closed. These will be resolved manually.  On the
		 * other hand in abort case, we need to close the transaction if
		 * preparing might be in-progress, since an error might have occurred
		 * on preparing a foreign transaction.
		 */
		if (!is_commit)
		{
			int					   status;

			SpinLockAcquire(&(fdwxact->mutex));
			status = fdwxact->status;
			fdwxact->status = FDWXACT_STATUS_ABORTING;
			SpinLockRelease(&(fdwxact->mutex));

			if (status == FDWXACT_STATUS_PREPARING)
				FdwXactParticipantEndTransaction(fdw_part, false);
		}
	}

	ForgetAllFdwXactParticipants();
	ForeignTwophaseCommitIsRequired = false;
}

/*
 * Prepare foreign transactions by PREPARE TRANSACTION command.
 *
 * Note that it's possible that the transaction aborts after we prepared some
 * of participants. In this case we change to rollback and rollback all foreign
 * transactions.
 */
void
PrePrepare_FdwXact(void)
{
	ListCell   *lc;
	TransactionId xid;

	if (FdwXactParticipants == NIL)
		return;

	/*
	 * Check if there is a server that doesn't support two-phase commit. All
	 * involved servers need to support two-phase commit as we're going to
	 *  prepare all of them.
	 */
	foreach(lc, FdwXactParticipants)
	{
		FdwXactParticipant *fdw_part = (FdwXactParticipant *) lfirst(lc);

		if (!ServerSupportTwophaseCommit(fdw_part))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot PREPARE a distributed transaction which has operated on a foreign server not supporting two-phase commit protocol")));
	}

	/*
	 * Assign a transaction id if not yet because the local transaction id
	 * is used to determine the result of the distributed transaction. And
	 * prepare all foreign transactions.
	 */
	xid = GetTopTransactionId();
	FdwXactPrepareForeignTransactions(xid, true);

	/*
	 * We keep FdwXactParticipants until the transaction end so that we change
	 * the involved foreign transactions to ABORTING in case of failure.
	 */
}

/*
 * Resolve foreign transactions at the give indexes.
 *
 * The caller must hold the given foreign transactions in advance to prevent
 * concurrent update.
 */
void
FdwXactResolveFdwXacts(int *fdwxact_idxs, int nfdwxacts)
{
	for (int i = 0; i < nfdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[fdwxact_idxs[i]];

		CHECK_FOR_INTERRUPTS();

		FdwXactResolveOneFdwXact(fdwxact);

		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		if (fdwxact->ondisk)
			RemoveFdwXactFile(fdwxact->dbid, fdwxact->local_xid, fdwxact->serverid,
							  fdwxact->userid, true);
		remove_fdwxact(fdwxact);
		LWLockRelease(FdwXactLock);
	}
}

/*
 * Return true if there is at least one prepared foreign transaction
 * which matches given arguments.
 */
bool
FdwXactExists(Oid dbid, Oid serverid, Oid userid)
{
	int			idx;

	LWLockAcquire(FdwXactLock, LW_SHARED);
	idx = get_fdwxact(dbid, InvalidTransactionId, serverid, userid);
	LWLockRelease(FdwXactLock);

	return (idx >= 0);
}
bool
FdwXactExistsXid(TransactionId xid)
{
	int			idx;

	LWLockAcquire(FdwXactLock, LW_SHARED);
	idx = get_fdwxact(InvalidOid, xid, InvalidOid, InvalidOid);
	LWLockRelease(FdwXactLock);

	return (idx >= 0);
}

/*
 * Return the index of first found FdwXact entry that matched to given arguments.
 * Otherwise return -1.	 The search condition is defined by arguments with valid
 * values for respective datatypes.
 */
static int
get_fdwxact(Oid dbid, TransactionId xid, Oid serverid, Oid userid)
{
	bool		found = false;
	int			i;

	Assert(LWLockHeldByMe(FdwXactLock));

	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];

		if (!fdwxact->valid)
			continue;

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

		/* This entry matches the condition */
		found = true;
		break;
	}

	return found ? i : -1;
}

/*
 * Compute the oldest xmin across all unresolved foreign transactions
 * and store it in the ProcArray.
 *
 * XXX: we can exclude FdwXact entries whose status is already committing
 * or aborting.
 */
static void
FdwXactComputeRequiredXmin(void)
{
	TransactionId agg_xmin = InvalidTransactionId;

	Assert(FdwXactCtl != NULL);

	LWLockAcquire(FdwXactLock, LW_SHARED);

	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];

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
 * Return whether the foreign transaction associated with the given transaction
 * id should be committed or rolled back according to the result of the local
 * transaction.
 */
static FdwXactStatus
FdwXactGetTransactionFate(TransactionId xid)
{
	/*
	 * If the local transaction is already committed, commit prepared foreign
	 * transaction.
	 */
	if (TransactionIdDidCommit(xid))
		return FDWXACT_STATUS_COMMITTING;

	/*
	 * If the local transaction is already aborted, abort prepared foreign
	 * transactions.
	 */
	else if (TransactionIdDidAbort(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The local transaction is not in progress but the foreign transaction is
	 * not prepared on the foreign server. This can happen when transaction
	 * failed after registered this entry but before actual preparing on the
	 * foreign server. So let's assume it aborted.
	 */
	else if (!TransactionIdIsInProgress(xid))
		return FDWXACT_STATUS_ABORTING;

	/*
	 * The Local transaction is in progress and foreign transaction is about
	 * to be committed or aborted.	Raise an error anyway since we cannot
	 * determine the fate of this foreign transaction according to the local
	 * transaction whose fate is also not determined.
	 */
	else
		elog(ERROR,
			 "cannot resolve the foreign transaction associated with in-process transaction");

	pg_unreachable();
}

/* Commit or rollback one prepared foreign transaction */
static void
FdwXactResolveOneFdwXact(FdwXact fdwxact)
{
	FdwXactRslvState state;
	ForeignServer *server;
	ForeignDataWrapper *fdw;
	FdwRoutine *routine;

	/* The FdwXact entry must be held by me */
	Assert(fdwxact != NULL);
	Assert(fdwxact->locking_backend == MyBackendId);

	if (fdwxact->status != FDWXACT_STATUS_COMMITTING &&
		fdwxact->status != FDWXACT_STATUS_ABORTING)
	{
		FdwXactStatus new_status;

		new_status = FdwXactGetTransactionFate(fdwxact->local_xid);
		Assert(new_status == FDWXACT_STATUS_COMMITTING ||
			   new_status == FDWXACT_STATUS_ABORTING);

		/* Update the status */
		SpinLockAcquire(&fdwxact->mutex);
		fdwxact->status = new_status;
		SpinLockRelease(&fdwxact->mutex);
	}

	server = GetForeignServer(fdwxact->serverid);
	fdw = GetForeignDataWrapper(server->fdwid);
	routine = GetFdwRoutine(fdw->fdwhandler);

	/* Prepare the resolution state to pass to API */
	state.server = server;
	state.usermapping = GetUserMapping(fdwxact->userid, fdwxact->serverid);
	state.fdwxact_id = fdwxact->fdwxact_id;
	state.flags = 0;

	if (fdwxact->status == FDWXACT_STATUS_COMMITTING)
	{
		routine->CommitForeignTransaction(&state);
		elog(DEBUG1, "successfully committed the prepared foreign transaction for server %u user %u",
			 fdwxact->serverid, fdwxact->userid);
	}
	else
	{
		routine->RollbackForeignTransaction(&state);
		elog(DEBUG1, "successfully rolled back the prepared foreign transaction for server %u user %u",
			 fdwxact->serverid, fdwxact->userid);
	}
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
		 * Add fdwxact entry and set start/end lsn of the WAL record in
		 * FdwXact entry.
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
		elog(ERROR, "invalid log type %d in foreign transaction log record", info);

	return;
}


/*
 * Store pointer to the start/end of the WAL record along with the xid in
 * a fdwxact entry in shared memory FdwXactData structure.
 */
static void
FdwXactRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn)
{
	FdwXactOnDiskData *fdwxact_data = (FdwXactOnDiskData *) buf;
	FdwXact		fdwxact;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	/*
	 * Add this entry into the table of foreign transactions. The status of
	 * the transaction is set as preparing, since we do not know the exact
	 * status right now. Resolver will set it later based on the status of
	 * local transaction which prepared this foreign transaction.
	 */
	fdwxact = insert_fdwxact(fdwxact_data->dbid, fdwxact_data->local_xid,
							 fdwxact_data->serverid, fdwxact_data->userid,
							 fdwxact_data->umid, fdwxact_data->fdwxact_id);

	elog(DEBUG2, "added fdwxact entry in shared memory for foreign transaction, db %u xid %u server %u user %u id %s",
		 fdwxact_data->dbid, fdwxact_data->local_xid,
		 fdwxact_data->serverid, fdwxact_data->userid,
		 fdwxact_data->fdwxact_id);

	/*
	 * Set status as PREPARED, since we do not know the xact status right now.
	 * We will set it later based on the status of local transaction that
	 * prepared this fdwxact entry.
	 */
	fdwxact->status = FDWXACT_STATUS_PREPARED;
	fdwxact->insert_start_lsn = start_lsn;
	fdwxact->insert_end_lsn = end_lsn;
	fdwxact->inredo = true;		/* added in redo */
	fdwxact->valid = false;
	fdwxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
}

/*
 * Remove the corresponding fdwxact entry from FdwXactCtl. Also remove
 * FdwXact file if a foreign transaction was saved via an earlier checkpoint.
 * We could not found the FdwXact entry in the case where a crash recovery
 * starts from the point where is after added but before removed the entry.
 */
static void
FdwXactRedoRemove(Oid dbid, TransactionId xid, Oid serverid,
				  Oid userid, bool givewarning)
{
	FdwXact		fdwxact;
	int			i;

	Assert(LWLockHeldByMeInMode(FdwXactLock, LW_EXCLUSIVE));
	Assert(RecoveryInProgress());

	for (i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		fdwxact = FdwXactCtl->fdwxacts[i];

		if (fdwxact->dbid == dbid && fdwxact->local_xid == xid &&
			fdwxact->serverid == serverid && fdwxact->userid == userid)
			break;
	}

	if (i >= FdwXactCtl->num_fdwxacts)
		return;

	/* Clean up entry and any files we may have left */
	if (fdwxact->ondisk)
		RemoveFdwXactFile(fdwxact->dbid, fdwxact->local_xid,
						  fdwxact->serverid, fdwxact->userid,
						  givewarning);
	remove_fdwxact(fdwxact);

	elog(DEBUG2, "removed fdwxact entry from shared memory for foreign transaction, db %u xid %u server %u user %u id %s",
		 fdwxact->dbid, fdwxact->local_xid, fdwxact->serverid,
		 fdwxact->userid, fdwxact->fdwxact_id);
}

/*
 * We must fsync the foreign transaction state file that is valid or generated
 * during redo and has a inserted LSN <= the checkpoint's redo horizon.
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
		return;					/* nothing to do */

	/*
	 * We are expecting there to be zero FdwXact that need to be copied to
	 * disk, so we perform all I/O while holding FdwXactLock for simplicity.
	 * This presents any new foreign xacts from preparing while this occurs,
	 * which shouldn't be a problem since the presence of long-lived prepared
	 * foreign xacts indicated the transaction manager isn't active.
	 *
	 * It's also possible to move I/O out of the lock, but on every error we
	 * should check whether somebody committed our transaction in different
	 * backend. Let's leave this optimisation for future, if somebody will
	 * spot that this place cause bottleneck.
	 *
	 * Note that it isn't possible for there to be a FdwXact with a
	 * insert_end_lsn set prior to the last checkpoint yet is marked invalid,
	 * because of the efforts with delayChkpt.
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
	 * durable on disk.	 FdwXact files could have been removed and those
	 * removals need to be made persistent as well as any files newly created.
	 */
	fsync_fname(FDWXACTS_DIR, true);

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

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating an XLog reading processor.")));

	XLogBeginRead(xlogreader, lsn);
	record = XLogReadRecord(xlogreader, &errormsg);
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
				 errmsg("could not write foreign transaction state file: %m")));
	}
	if (write(fd, &statefile_crc, sizeof(pg_crc32c)) != sizeof(pg_crc32c))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write foreign transaction state file: %m")));
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
	TransactionId origNextXid =
	XidFromFullTransactionId(ShmemVariableCache->nextXid);
	char	   *buf;

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
	if (fdwxact_file_data->dbid != dbid ||
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
 * reading WAL.	 ShmemVariableCache->nextXid has been set to one more than
 * the highest XID for which evidence exists in WAL.

 * On corrupted two-phase files, fail immediately.	Keeping around broken
 * entries and let replay continue causes harm on the system, and a new
 * backup should be rolled in.

 * Our other responsibility is to update and return the oldest valid XID
 * among the distributed transactions. This is needed to synchronize pg_subtrans
 * startup properly.
 */
TransactionId
PrescanFdwXacts(TransactionId oldestActiveXid)
{
	FullTransactionId nextXid = ShmemVariableCache->nextXid;
	TransactionId origNextXid = XidFromFullTransactionId(nextXid);
	TransactionId result = origNextXid;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];
		char	   *buf;

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
RestoreFdwXactData(void)
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
			char	   *buf;

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
 * Scan the shared memory entries of FdwXact and valid them.
 *
 * This is run at the end of recovery, but before we allow backends to write
 * WAL.
 */
void
RecoverFdwXacts(void)
{
	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];
		char	   *buf;

		buf = ProcessFdwXactBuffer(fdwxact->dbid, fdwxact->local_xid,
								   fdwxact->serverid, fdwxact->userid,
								   fdwxact->insert_start_lsn, fdwxact->ondisk);

		if (buf == NULL)
			continue;

		ereport(LOG,
				(errmsg("recovering foreign prepared transaction %u for server %u and user %u from shared memory",
						fdwxact->local_xid, fdwxact->serverid, fdwxact->userid)));

		/* recovered, so reset the flag for entries generated by redo */
		fdwxact->inredo = false;
		fdwxact->valid = true;
		pfree(buf);
	}
	LWLockRelease(FdwXactLock);
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
}			WorkingStatus;

Datum
pg_foreign_xacts(PG_FUNCTION_ARGS)
{
#define PG_PREPARED_FDWXACTS_COLS	6
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(FdwXactLock, LW_SHARED);
	for (int i = 0; i < FdwXactCtl->num_fdwxacts; i++)
	{
		FdwXact		fdwxact = FdwXactCtl->fdwxacts[i];
		FdwXactStatus status;
		char	   *xact_status;
		Datum		values[PG_PREPARED_FDWXACTS_COLS];
		bool		nulls[PG_PREPARED_FDWXACTS_COLS];

		if (!fdwxact->valid)
			continue;

		memset(nulls, 0, sizeof(nulls));

		SpinLockAcquire(&fdwxact->mutex);
		status = fdwxact->status;
		SpinLockRelease(&fdwxact->mutex);

		values[0] = TransactionIdGetDatum(fdwxact->local_xid);
		values[1] = ObjectIdGetDatum(fdwxact->serverid);
		values[2] = ObjectIdGetDatum(fdwxact->userid);

		switch (status)
		{
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
			default:
				xact_status = "unknown";
				break;
		}

		values[3] = CStringGetTextDatum(xact_status);
		values[4] = CStringGetTextDatum(fdwxact->fdwxact_id);

		if (fdwxact->locking_backend != InvalidBackendId)
		{
			PGPROC *locker = BackendIdGetProc(fdwxact->locking_backend);
			values[5] = Int32GetDatum(locker->pid);
		}
		else
			nulls[5] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(FdwXactLock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Built-in SQL function to resolve a prepared foreign transaction.
 */
Datum
pg_resolve_foreign_xact(PG_FUNCTION_ARGS)
{
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid			serverid = PG_GETARG_OID(1);
	Oid			userid = PG_GETARG_OID(2);
	Oid			myuserid;
	FdwXact		fdwxact;
	int			idx;

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	idx = get_fdwxact(MyDatabaseId, xid, serverid, userid);

	if (idx < 0)
	{
		/* not found */
		LWLockRelease(FdwXactLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("does not exist foreign transaction")));
	}

	fdwxact = FdwXactCtl->fdwxacts[idx];

	myuserid = GetUserId();
	if (myuserid != fdwxact->userid && !superuser_arg(myuserid))
		ereport(ERROR,
				 (errmsg("permission denied to resolve prepared foreign transaction"),
				  errhint("Must be superuser or the user that prepared the transaction")));

	if (fdwxact->locking_backend != InvalidBackendId)
	{
		/* the entry is being processed by someone */
		LWLockRelease(FdwXactLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction with transaction identifier \"%s\" is busy",
						fdwxact->fdwxact_id)));
	}

	if (TwoPhaseExists(fdwxact->local_xid))
	{
		/*
		 * the entry's local transaction is prepared. Since we cannot know the
		 * fate of the local transaction, we cannot resolve this foreign
		 * transaction.
		 */
		LWLockRelease(FdwXactLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot resolve foreign transaction with identifier \"%s\" whose local transaction is in-progress",
						fdwxact->fdwxact_id),
				 errhint("Do COMMIT PREPARED or ROLLBACK PREPARED")));
	}

	/* Hold the entry */
	fdwxact->locking_backend = MyBackendId;

	LWLockRelease(FdwXactLock);

	PG_TRY();
	{
		FdwXactResolveFdwXacts(&idx, 1);
	}
	PG_CATCH();
	{
		LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);
		FdwXactCtl->fdwxacts[idx]->locking_backend = InvalidBackendId;
		LWLockRelease(FdwXactLock);

		PG_RE_THROW();
	}
	PG_END_TRY();

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
	TransactionId xid = DatumGetTransactionId(PG_GETARG_DATUM(0));
	Oid			serverid = PG_GETARG_OID(1);
	Oid			userid = PG_GETARG_OID(2);
	Oid			myuserid;
	FdwXact		fdwxact;
	int			idx;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to remove foreign transactions"))));

	LWLockAcquire(FdwXactLock, LW_EXCLUSIVE);

	idx = get_fdwxact(MyDatabaseId, xid, serverid, userid);

	if (idx < 0)
	{
		/* not found */
		LWLockRelease(FdwXactLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("does not exist foreign transaction on server %u",
						serverid)));
	}

	fdwxact = FdwXactCtl->fdwxacts[idx];

	myuserid = GetUserId();
	if (myuserid != fdwxact->userid && !superuser_arg(myuserid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("permission denied to remove prepared foreign transaction"),
				  errhint("Must be superuser or the user that prepared the transaction"))));

	if (fdwxact->locking_backend != InvalidBackendId)
	{
		/* the entry is being held by someone */
		LWLockRelease(FdwXactLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign transaction with transaction id %u, server %u, and user %u is busy",
						xid, serverid, userid)));
	}

	/* Hold the entry */
	fdwxact->locking_backend = MyBackendId;

	PG_TRY();
	{
		/* Clean up entry and any files we may have left */
		if (fdwxact->ondisk)
			RemoveFdwXactFile(fdwxact->dbid, fdwxact->local_xid,
							  fdwxact->serverid, fdwxact->userid,
							  true);
		remove_fdwxact(fdwxact);
	}
	PG_CATCH();
	{
		if (fdwxact->valid)
		{
			Assert(fdwxact->locking_backend == MyBackendId);
			fdwxact->locking_backend = InvalidBackendId;
		}
		LWLockRelease(FdwXactLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LWLockRelease(FdwXactLock);

	PG_RETURN_BOOL(true);
}
