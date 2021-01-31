/*-------------------------------------------------------------------------
 *
 * mcxtfuncs.c
 *	  Functions to show backend memory context.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/mcxtfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "common/logging.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/mcxtfuncs.h"

/* The max bytes for showing names and identifiers of MemoryContext. */
#define MEMORY_CONTEXT_DISPLAY_SIZE	1024

/* Number of columns in pg_backend_memory_contexts view */
#define PG_GET_BACKEND_MEMORY_CONTEXTS_COLS	9

/* Shared memory struct for managing the status of memory context dump. */
static mcxtdumpShmemStruct *mcxtdumpShmem =  NULL;

/*
 * McxtReqCleanup
 *		Error cleanup callback for memory context requestor.
 */
static void
McxtReqCleanup(int code, Datum arg)
{
	int		dst_pid = DatumGetInt32(arg);

	LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

	if (mcxtdumpShmem->src_pid != MyProcPid)
	{
		/*
		 * If the requestor is not me, simply exit.
		 */
		LWLockRelease(McxtDumpLock);
		return;
	}

	if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_REQUESTING)
	{
		/*
		 * Since the dumper has not received the dump order yet, clean up
		 * things by myself.
		 */
		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;
	}
	else if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_DUMPING)
	{
		/*
		 * Since the dumper has received the request already,
		 * requestor just change the status and the dumper cleans up
		 * things later.
		 */
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_CANCELING;
	}
	else if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_DONE)
	{
		/*
		 * Since the dumper has already finished dumping, clean up things
		 * by myself.
		 */
		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

		RemoveMemcxtFile(dst_pid);
	}
	LWLockRelease(McxtDumpLock);
}

/*
 * PutMemoryContextsStatsTupleStore
 *		One recursion level for pg_get_backend_memory_contexts.
 *
 * Note: When fpout is not NULL, ferror() check must be done by the caller.
 */
static void
PutMemoryContextsStatsTupleStore(Tuplestorestate *tupstore,
								TupleDesc tupdesc, MemoryContext context,
								const char *parent, int level, FILE *fpout)
{
	Datum		values[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
	bool		nulls[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
	char		clipped_ident[MEMORY_CONTEXT_DISPLAY_SIZE];
	MemoryContextCounters stat;
	MemoryContext child;
	const char *name;
	const char *ident;

	AssertArg(MemoryContextIsValid(context));

	name = context->name;
	ident = context->ident;

	/*
	 * To be consistent with logging output, we label dynahash contexts
	 * with just the hash table name as with MemoryContextStatsPrint().
	 */
	if (ident && strcmp(name, "dynahash") == 0)
	{
		name = ident;
		ident = NULL;
	}

	/* Examine the context itself */
	memset(&stat, 0, sizeof(stat));
	(*context->methods->stats) (context, NULL, (void *) &level, &stat);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	if (name)
		values[0] = CStringGetTextDatum(name);
	else
		nulls[0] = true;

	if (ident)
	{
		int		idlen = strlen(ident);
		/*
		 * Some identifiers such as SQL query string can be very long,
		 * truncate oversize identifiers.
		 */
		if (idlen >= MEMORY_CONTEXT_DISPLAY_SIZE)
			idlen = pg_mbcliplen(ident, idlen, MEMORY_CONTEXT_DISPLAY_SIZE - 1);

		memcpy(clipped_ident, ident, idlen);
		clipped_ident[idlen] = '\0';
		values[1] = CStringGetTextDatum(clipped_ident);
	}
	else
		nulls[1] = true;

	if (parent)
		values[2] = CStringGetTextDatum(parent);
	else
		nulls[2] = true;

	values[3] = Int32GetDatum(level);
	values[4] = Int64GetDatum(stat.totalspace);
	values[5] = Int64GetDatum(stat.nblocks);
	values[6] = Int64GetDatum(stat.freespace);
	values[7] = Int64GetDatum(stat.freechunks);
	values[8] = Int64GetDatum(stat.totalspace - stat.freespace);

	/*
	 * Since pg_get_backend_memory_contexts() is called from local process,
	 * simply put tuples.
	 */
	if(fpout == NULL)
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/*
	 * Write out the current memory context information in the form of
	 * "key: value" pairs to the file specified by the requestor.
	 */
	else
	{
		/*
		 * Make each memory context information starts with 'D'.
		 * This is checked by the requestor when reading the file.
		 */
		fputc('D', fpout);

		fprintf(fpout,
			"name: %s, ident: %s, parent: %s, level: %d, total_bytes: %lu, \
			total_nblocks: %lu, free_bytes: %lu, free_chunks: %lu, used_bytes: %lu,\n",
			name,
			ident ? clipped_ident : "none",
			parent ? parent : "none", level,
			stat.totalspace,
			stat.nblocks,
			stat.freespace,
			stat.freechunks,
			stat.totalspace - stat.freespace);
	}

	for (child = context->firstchild; child != NULL; child = child->nextchild)
	{
		PutMemoryContextsStatsTupleStore(tupstore, tupdesc,
								  child, name, level + 1, fpout);
	}
}

/*
 * SetupMcxtdumpShem
 * 		Setup shared memory struct for dumping specified PID.
 */
static bool
SetupMcxtdumpShmem(int pid)
{
	/*
	 * We only allow one session per target process to request memory
	 * contexts dump at a time.
	 */
	LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

	if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_ACCEPTABLE)
	{
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_REQUESTING;
		mcxtdumpShmem->src_pid = MyProcPid;
		mcxtdumpShmem->dst_pid = pid;

		/*
		 * Dump files should not exist now, but delete any of
		 * them just in case.
		 *
		 * Note: This is possible because only one session can
		 * request memory contexts per instance.
		 */
		RemoveMemcxtFile(0);

		LWLockRelease(McxtDumpLock);

		return true;
	}
	else
	{
		LWLockRelease(McxtDumpLock);

		ereport(WARNING,
				(errmsg("Only one session can dump at a time and another session is dumping currently.")));

		return false;
	}
}

/*
 * PutDumpedValuesOnTuplestore
 *		Read specified memory context dump file and put its values
 *		on the tuplestore.
 */
static void
PutDumpedValuesOnTuplestore(char *dumpfile, Tuplestorestate *tupstore,
								TupleDesc tupdesc, int pid)
{
	FILE	 	*fpin;
	int		format_id;

	if ((fpin = AllocateFile(dumpfile, "r")) == NULL)
	{
		if (errno != ENOENT)
			ereport(LOG, (errcode_for_file_access(),
				errmsg("could not open memory context dump file \"%s\": %m",
					dumpfile)));
	}

	/* Verify it's of the expected format. */
	if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) ||
		format_id != PG_MCXT_FILE_FORMAT_ID)
	{
		ereport(WARNING,
				(errmsg("corrupted memory context dump file \"%s\"", dumpfile)));
		goto done;
	}

	/* Read dump file and put values on tuple store. */
	while (true)
	{
		Datum		values[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
		bool		nulls[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
		char 		name[MEMORY_CONTEXT_DISPLAY_SIZE];
		char 		parent[MEMORY_CONTEXT_DISPLAY_SIZE];
		char 		clipped_ident[MEMORY_CONTEXT_DISPLAY_SIZE];
		int 		level;
		Size	total_bytes;
		Size	total_nblocks;
		Size	free_bytes;
		Size	free_chunks;
		Size	used_bytes;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		switch (fgetc(fpin))
		{
			/* 'D'	A memory context information follows */
			case 'D':
				if (fscanf(fpin, "name: %1023[^,], ident: %1023[^,], parent: %1023[^,], \
					level: %d, total_bytes: %lu, total_nblocks: %lu, \
					free_bytes: %lu, free_chunks: %lu, used_bytes: %lu,\n",
					name, clipped_ident, parent, &level, &total_bytes, &total_nblocks,
						&free_bytes, &free_chunks, &used_bytes)
					!= PG_GET_BACKEND_MEMORY_CONTEXTS_COLS)
				{
					ereport(WARNING,
						(errmsg("corrupted memory context dump file \"%s\"",
							dumpfile)));
					goto done;
				}

				values[0] = CStringGetTextDatum(name);

				if (strcmp(clipped_ident, "none"))
					values[1] = CStringGetTextDatum(clipped_ident);
				else
					nulls[1] = true;

				if (strcmp(parent, "none"))
					values[2] = CStringGetTextDatum(parent);
				else
					nulls[2] = true;

				values[3] = Int32GetDatum(level);
				values[4] = Int64GetDatum(total_bytes);
				values[5] = Int64GetDatum(total_nblocks);
				values[6] = Int64GetDatum(free_bytes);
				values[7] = Int64GetDatum(free_chunks);
				values[8] = Int64GetDatum(used_bytes);

				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				break;

			case 'E':
				goto done;

			default:
				ereport(WARNING,
						(errmsg("corrupted memory context dump file \"%s\"",
							dumpfile)));
				goto done;
		}
	}
done:
	FreeFile(fpin);
	unlink(dumpfile);

	LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

	Assert(mcxtdumpShmem->src_pid == MyProcPid);

	mcxtdumpShmem->dst_pid = 0;
	mcxtdumpShmem->src_pid = 0;
	mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

	LWLockRelease(McxtDumpLock);
}

/*
 * pg_get_backend_memory_contexts
 *		SQL SRF showing local backend memory context.
 */
Datum
pg_get_backend_memory_contexts(PG_FUNCTION_ARGS)
{
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

	PutMemoryContextsStatsTupleStore(tupstore, tupdesc,
							TopMemoryContext, "", 0, NULL);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * pg_get_target_backend_memory_contexts
 *		SQL SRF showing specified process's backend memory context.
 */
Datum
pg_get_target_backend_memory_contexts(PG_FUNCTION_ARGS)
{
	int		dst_pid = PG_GETARG_INT32(0);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	char		dumpfile[MAXPGPATH];
	PGPROC	 	*proc;
	PgBackendStatus *beentry;

	snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, dst_pid);

	/*
	 * Prohibit calling this function inside a transaction since
	 * it can cause dead lock.
	 */
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("pg_get_target_backend_memory_contexts cannot run inside a transaction block")));

	/* If the target process is itself, call a dedicated function. */
	if (dst_pid == MyProcPid)
	{
		pg_get_backend_memory_contexts(fcinfo);
		return (Datum) 0;
	}

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

	/*
	 * Check whether the target process is PostgreSQL backend process.
	 */
	proc = BackendPidGetProc(dst_pid);

	if (proc == NULL)
	{
		proc = AuxiliaryPidGetProc(dst_pid);

		if (proc == NULL)
			ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process.", dst_pid)));
		else
			ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL backend process but an auxiliary process.", dst_pid)));

		tuplestore_donestoring(tupstore);
		return (Datum) 1;
	}

	beentry = pgstat_fetch_stat_beentry(proc->backendId);

	if (beentry->st_backendType != B_BACKEND)
	{
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL backend process", dst_pid)));

		tuplestore_donestoring(tupstore);
		return (Datum) 1;
	}

	/*
	 * The ENSURE stuff ensures we clean up the shared memory struct and files
	 * on failure.
	 */
	PG_ENSURE_ERROR_CLEANUP(McxtReqCleanup, (Datum) Int32GetDatum(dst_pid));
	{
		if(!SetupMcxtdumpShmem(dst_pid))
		{
			/* Someone uses mcxtdumpShmem, simply exit. */
			tuplestore_donestoring(tupstore);
			return (Datum) 1;
		}

		SendProcSignal(dst_pid, PROCSIG_DUMP_MEMCXT, InvalidBackendId);

		/* Wait until target process finishes dumping file. */
		for (;;)
		{
			/* Check for dump cancel request. */
			CHECK_FOR_INTERRUPTS();

			/* Must reset the latch before testing state. */
			ResetLatch(MyLatch);

			LWLockAcquire(McxtDumpLock, LW_SHARED);

			if (mcxtdumpShmem->src_pid != MyProcPid)
			{
				/*
				 * It seems the dumper exited and subsequently another
				 * process is requesting dumping.
				 */
				LWLockRelease(McxtDumpLock);

				ereport(INFO,
					(errmsg("The request has failed and now PID %d is requsting dumping.",
						mcxtdumpShmem->src_pid)));

				tuplestore_donestoring(tupstore);

				return (Datum) 0;
			}
			else if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_ACCEPTABLE)
			{
				/*
				 * Dumper seems to have cleaned up things already because
				 * of failures or cancellation.
				 * Since the dumper has already removed the dump file,
				 * simply exit.
				 */
				LWLockRelease(McxtDumpLock);
				tuplestore_donestoring(tupstore);

				return (Datum) 0;
			}
			else if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_CANCELING)
			{
				/*
				 * Request has been canceled. Exit without dumping.
				 */
				LWLockRelease(McxtDumpLock);
				tuplestore_donestoring(tupstore);

				return (Datum) 0;
			}

			else if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_DONE)
			{
				/* Dumping has completed. */
				LWLockRelease(McxtDumpLock);
				break;
			}
			/*
			 * The dumper must be in the middle of a dumping or the request
			 * hasn't been reached yet.
			 */
			Assert(mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_REQUESTING ||
				mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_DUMPING);

			/*
			 * Although we have checked the target process,
			 * it might have been terminated after the check.
			 * Ensure it again.
			 */
			proc = BackendPidGetProc(dst_pid);

			if (proc == NULL)
			{
				ereport(WARNING,
						(errmsg("PID %d seems to exit before dumping.", dst_pid)));

				/* Initialize the shared memory and exit. */
				LWLockRelease(McxtDumpLock);
				LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

				mcxtdumpShmem->dst_pid = 0;
				mcxtdumpShmem->src_pid = 0;
				mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

				LWLockRelease(McxtDumpLock);

				tuplestore_donestoring(tupstore);
				return (Datum) 1;
			}
			LWLockRelease(McxtDumpLock);

			/*
			 * Wait. We expect to get a latch signal back from the dumper.
			 * Use a timeout to enable cancellation.
			 */
			(void) WaitLatch(MyLatch,
					WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					1000L, WAIT_EVENT_DUMP_MEMORY_CONTEXT);
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(McxtReqCleanup, (Datum) Int32GetDatum(dst_pid));

	/* Read values from the dump file and put them on tuplestore. */
	PutDumpedValuesOnTuplestore(dumpfile, tupstore, tupdesc, dst_pid);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * dump_memory_contexts
 * 		Dump local memory contexts to a file.
 */
static void
dump_memory_contexts(void)
{
	FILE		*fpout;
	char		dumpfile[MAXPGPATH];
	int		format_id;
	pid_t		src_pid;
	PGPROC	 	*src_proc;

	snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, MyProcPid);

	LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

	if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_ACCEPTABLE)
	{
		/*
		 * The requestor canceled the request and initialized
		 * the shared memory. Simply exit.
		 */
		LWLockRelease(McxtDumpLock);

		return;
	}

	if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_CANCELING)
	{
		/*
		 * The requestor canceled the request.
		 * Initialize the shared memory and exit.
		 */
		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

		LWLockRelease(McxtDumpLock);

		return;
	}

	Assert(mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_REQUESTING);

	mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_DUMPING;
	src_pid = mcxtdumpShmem->src_pid;

	LWLockRelease(McxtDumpLock);

	fpout = AllocateFile(dumpfile, "w");

	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write dump file \"%s\": %m",
						dumpfile)));
		FreeFile(fpout);

		LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

		LWLockRelease(McxtDumpLock);

		return;
	}
	format_id = PG_MCXT_FILE_FORMAT_ID;
	fwrite(&format_id, sizeof(format_id), 1, fpout);

	/* Look into each memory context from TopMemoryContext recursively. */
	PutMemoryContextsStatsTupleStore(NULL, NULL,
							TopMemoryContext, NULL, 0, fpout);

	/*
	 * Make dump file ends with 'E'.
	 * This is checked by the requestor later.
	 */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write dump file \"%s\": %m",
						dumpfile)));
		FreeFile(fpout);
		unlink(dumpfile);

		LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

		LWLockRelease(McxtDumpLock);

		return;
	}

	/* No more output to be done. Close file. */
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close dump file \"%s\": %m",
						dumpfile)));
	}

	LWLockAcquire(McxtDumpLock, LW_EXCLUSIVE);

	if (mcxtdumpShmem->dump_status == MCXTDUMPSTATUS_CANCELING)
	{
		/* During dumping, the requestor canceled the request. */
		unlink(dumpfile);

		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;

		LWLockRelease(McxtDumpLock);

		return;
	}

	/* Dumping has succeeded, notify it to the requestor. */
	mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_DONE;
	LWLockRelease(McxtDumpLock);
	src_proc = BackendPidGetProc(src_pid);
	SetLatch(&(src_proc->procLatch));

	return;
}

/*
 * ProcessDumpMemoryContextInterrupt
 *		The portion of memory context dump interrupt handling that runs
 *		outside of the signal handler.
 */
void
ProcessDumpMemoryContextInterrupt(void)
{
	ProcSignalDumpMemoryContextPending = false;
	dump_memory_contexts();
}

/*
 * HandleProcSignalDumpMemoryContext
 * 		Handle receipt of an interrupt indicating a memory context dump.
 *		Signal handler portion of interrupt handling.
 */
void
HandleProcSignalDumpMemoryContext(void)
{
	ProcSignalDumpMemoryContextPending = true;
}

/*
 * McxtDumpShmemInit
 * 		Initialize mcxtdump shared memory struct.
 */
void
McxtDumpShmemInit(void)
{
	bool		found;

	mcxtdumpShmem = (mcxtdumpShmemStruct *)
		ShmemInitStruct("Memory Context Dump Data",
						sizeof(mcxtdumpShmemStruct),
						&found);
	if (!found)
	{
		mcxtdumpShmem->dst_pid = 0;
		mcxtdumpShmem->src_pid = 0;
		mcxtdumpShmem->dump_status = MCXTDUMPSTATUS_ACCEPTABLE;
	}
}

/*
 * RemoveMemcxtFile
 *	 	Remove dump files.
 */
void
RemoveMemcxtFile(int pid)
{
	DIR		*dir;
	struct 	dirent *dumpfile;

	if (pid == 0)
	{
		/* delete all dump files */
		dir = AllocateDir(PG_MEMUSAGE_DIR);
		while ((dumpfile = ReadDir(dir, PG_MEMUSAGE_DIR)) != NULL)
		{
			char		dumpfilepath[32];

			if (strcmp(dumpfile->d_name, ".") == 0 || strcmp(dumpfile->d_name, "..") == 0)
				continue;

			sprintf(dumpfilepath, "%s/%s", PG_MEMUSAGE_DIR, dumpfile->d_name);

			ereport(DEBUG2,
					(errmsg("removing file \"%s\"", dumpfilepath)));

			if (unlink(dumpfilepath) < 0)
			{
				ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", dumpfilepath)));
			}
		}
		FreeDir(dir);
	}
	else
	{
		/* delete specified dump file */
		char		str_pid[12];
		char		dumpfilepath[32];
		struct 		stat stat_tmp;

		pg_ltoa(pid, str_pid);
		sprintf(dumpfilepath, "%s/%s", PG_MEMUSAGE_DIR, str_pid);

		ereport(DEBUG2,
				(errmsg("removing file \"%s\"", dumpfilepath)));

		if (stat(dumpfilepath, &stat_tmp) == 0)
		{
			if (unlink(dumpfilepath) < 0)
			{
				ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m", dumpfilepath)));
			}
		}
	}
}
