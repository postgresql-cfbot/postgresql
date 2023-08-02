/*-------------------------------------------------------------------------
 * auxprocess.c
 *	  functions related to auxiliary processes.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/auxprocess.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <signal.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/auxprocess.h"
#include "postmaster/bgwriter.h"
#include "postmaster/startup.h"
#include "postmaster/walwriter.h"
#include "replication/walreceiver.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"


static void ShutdownAuxiliaryProcess(int code, Datum arg);


/* ----------------
 *		global variables
 * ----------------
 */

AuxProcType MyAuxProcType = NotAnAuxProcess;	/* declared in miscadmin.h */


/*
 *	 AuxiliaryProcessMain XXX
 *
 *	 The main entry point for auxiliary processes, such as the bgwriter,
 *	 walwriter, walreceiver, bootstrapper and the shared memory checker code.
 *
 *	 This code is here just because of historical reasons.
 */
void
AuxiliaryProcessInit(void)
{
	/* Release postmaster's working memory context */
	if (PostmasterContext)
	{
		MemoryContextDelete(PostmasterContext);
		PostmasterContext = NULL;
	}

	Assert(IsUnderPostmaster);

	init_ps_display(NULL);

	SetProcessingMode(BootstrapProcessing);
	IgnoreSystemIndexes = true;

	/*
	 * As an auxiliary process, we aren't going to do the full InitPostgres
	 * pushups, but there are a couple of things that need to get lit up even
	 * in an auxiliary process.
	 */

	/*
	 * Create a PGPROC so we can use LWLocks.
	 */
	InitAuxiliaryProcess();

	/* Attach process to shared data structures */
	AttachSharedMemoryAndSemaphores();

	BaseInit();

	/*
	 * Assign the ProcSignalSlot for an auxiliary process.  Since it doesn't
	 * have a BackendId, the slot is statically allocated based on the
	 * auxiliary process type (MyAuxProcType).  Backends use slots indexed in
	 * the range from 1 to MaxBackends (inclusive), so we use MaxBackends +
	 * AuxProcType + 1 as the index of the slot for an auxiliary process.
	 *
	 * This will need rethinking if we ever want more than one of a particular
	 * auxiliary process type.
	 */
	ProcSignalInit(MaxBackends + MyAuxProcType + 1);

	/*
	 * Auxiliary processes don't run transactions, but they may need a
	 * resource owner anyway to manage buffer pins acquired outside
	 * transactions (and, perhaps, other things in future).
	 */
	CreateAuxProcessResourceOwner();


	/* Initialize backend status information */
	pgstat_beinit();
	pgstat_bestart();

	/* register a before-shutdown callback for LWLock cleanup */
	before_shmem_exit(ShutdownAuxiliaryProcess, 0);

	SetProcessingMode(NormalProcessing);
}

/*
 * Begin shutdown of an auxiliary process.  This is approximately the equivalent
 * of ShutdownPostgres() in postinit.c.  We can't run transactions in an
 * auxiliary process, so most of the work of AbortTransaction() is not needed,
 * but we do need to make sure we've released any LWLocks we are holding.
 * (This is only critical during an error exit.)
 */
static void
ShutdownAuxiliaryProcess(int code, Datum arg)
{
	LWLockReleaseAll();
	ConditionVariableCancelSleep();
	pgstat_report_wait_end();
}
