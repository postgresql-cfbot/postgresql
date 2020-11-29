/*-------------------------------------------------------------------------
 *
 * mcxtfuncs.c
 *	  Functions to show backend memory context.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
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

/* Hash for managing the status of memory context dump. */
static HTAB *mcxtdumpHash = NULL;


/*
 * McxtReqKill
 *		Cleanup function for memory context dump requestor.
 *
 * Called when the caller of pg_get_backend_memory_contexts()
 * exits.
 */
static void
McxtReqKill(int code, Datum arg)
{
	mcxtdumpEntry	*entry;
	int		dump_status;
	int		dst_pid = DatumGetInt32(arg);;

	LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

	entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &dst_pid, HASH_FIND, NULL);

	if (entry == NULL)
		elog(ERROR, "hash table corrupted");

	dump_status = entry->dump_status;

	if (dump_status == MCXTDUMPSTATUS_REQUESTING)
	{
		elog(DEBUG2, "removing %d entry at MCXTDUMPSTATUS_REQUESTING." , dst_pid);
		hash_search(mcxtdumpHash, &dst_pid, HASH_REMOVE, NULL);
	}

	else if (dump_status == MCXTDUMPSTATUS_DUMPING)
	{
		entry->dump_status = MCXTDUMPSTATUS_CANCELING;
		elog(DEBUG2, "status changed from MCXTDUMPSTATUS_DUMPING to MCXTDUMPSTATUS_CANCELING.");
	}
	else if (dump_status == MCXTDUMPSTATUS_DONE)
	{
		hash_search(mcxtdumpHash, &dst_pid, HASH_REMOVE, NULL);

		/* for debug */
		elog(DEBUG2, "removing dump file of PID %d at MCXTDUMPSTATUS_DONE.", dst_pid);
		RemoveMemcxtFile(dst_pid);
	}
	LWLockRelease(McxtDumpHashLock);
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
 * AddEntryToMcxtdumpHash
 * 		Add an entry to McxtdumpHash for specified PID.
 */
static mcxtdumpEntry *
AddEntryToMcxtdumpHash(int pid)
{
	mcxtdumpEntry	*entry;
	bool		found;

	/*
	 * We only allow one session per target process to request a memory
	 * dump at a time.
	 * If mcxtdumpHash has corresponding entry, wait until it has removed.
	 */
	while (true)
	{
		LWLockAcquire(McxtDumpHashLock, LW_SHARED);
		entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &pid,
			HASH_ENTER, &found);

		if (!found)
		{
			/* Need exclusive lock to make a new hashtable entry */
			LWLockRelease(McxtDumpHashLock);
			LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

			entry->dump_status = MCXTDUMPSTATUS_REQUESTING;
			entry->src_pid = MyProcPid;

			LWLockRelease(McxtDumpHashLock);

			return entry;
		}
		else
		{
			ereport(INFO,
					(errmsg("PID %d is looked up by another process", pid)));

			LWLockRelease(McxtDumpHashLock);

			pg_usleep(5000000L);
		}
	}
}

/*
 * PutDumpedValuesOnTuplestore
 *		Read specified memory context dump file and put its values
 *		on the tuple store.
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
		format_id != PG_MEMCONTEXT_FILE_FORMAT_ID)
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
			/* 'D'	A memory context information follows. */
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

	LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

	if (hash_search(mcxtdumpHash, &pid, HASH_REMOVE, NULL) == NULL)
		elog(ERROR, "hash table corrupted");

	LWLockRelease(McxtDumpHashLock);
}

/*
 * pg_get_backend_memory_contexts
 *		SQL SRF showing backend memory context.
 */
Datum
pg_get_backend_memory_contexts(PG_FUNCTION_ARGS)
{
	int			dst_pid = PG_ARGISNULL(0) ? -1 : PG_GETARG_INT32(0);

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

	/*
	 * If the target is local process, simply look into memory contexts
	 * recursively.
	 */
	if (dst_pid == -1 || dst_pid == MyProcPid)
		PutMemoryContextsStatsTupleStore(tupstore, tupdesc,
								TopMemoryContext, "", 0, NULL);

	/*
	 * Target process is not local.
	 * Send signal for dumping memory contexts to the target process,
	 * and read the dump file.
	 */
	else
	{
		char		dumpfile[MAXPGPATH];
		mcxtdumpEntry	*entry;
		PGPROC	 	*proc;

		snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, dst_pid);

		/*
		 * Check whether the target process is PostgreSQL backend process.
		 *
		 * If the target process dies after this point and before sending signal,
		 * users are expected to cancel the request.
		 */

		/* TODO: Check also whether backend or not. */
		proc = BackendPidGetProc(dst_pid);

		if (proc == NULL)
		{
			ereport(WARNING,
					(errmsg("PID %d is not a PostgreSQL server process", dst_pid)));

			return (Datum) 1;
		}

		/*
		 * The ENSURE stuff ensures we clean up the shared memory entry and files
		 * on failure.
		 */
		PG_ENSURE_ERROR_CLEANUP(McxtReqKill, (Datum) Int32GetDatum(dst_pid));
		{
			entry = AddEntryToMcxtdumpHash(dst_pid);

			SendProcSignal(dst_pid, PROCSIG_DUMP_MEMCXT, InvalidBackendId);

			/* Wait until target process finishes dumping file. */
			for (;;)
			{
				/* Check for dump cancel request. */
				CHECK_FOR_INTERRUPTS();

				/* Must reset the latch before testing state. */
				ResetLatch(MyLatch);

				/* Check whether the dump has completed. */
				LWLockAcquire(McxtDumpHashLock, LW_SHARED);
				entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &dst_pid, HASH_FIND, NULL);

				if (entry == NULL)
				{
					/*
					 * Dumper seems to cleanup the enry because of failures or
					 * cancellation.
					 * Since the dumper has already removed the dump file, the
					 * requestor can simply exit.
					 */
					LWLockRelease(McxtDumpHashLock);
					tuplestore_donestoring(tupstore);

					return (Datum) 0;
				}

				if (entry->dump_status == MCXTDUMPSTATUS_CANCELING)
				{
					/* Request has canceled. Exit without dumping. */
					LWLockRelease(McxtDumpHashLock);
					tuplestore_donestoring(tupstore);

					return (Datum) 0;
				}

				else if (entry->dump_status == MCXTDUMPSTATUS_DONE)
				{
					/* Dumping has completed. */
					LWLockRelease(McxtDumpHashLock);
					break;
				}

				LWLockRelease(McxtDumpHashLock);

				/*
				 * The state is either the dumper is in the middle of a dump,
				 * or the request hasn't been reached yet.
				 */
				Assert(entry->dump_status == MCXTDUMPSTATUS_REQUESTING ||
					entry->dump_status == MCXTDUMPSTATUS_DUMPING);

				/*
				 * Wait. We expect to get a latch signal back from the dumper,
				 * but use a timeout to enable cancellation.
				 */
				(void) WaitLatch(MyLatch,
						WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						1000L, WAIT_EVENT_MEMORY_CONTEXT_DUMP);
			}
		}
		PG_END_ENSURE_ERROR_CLEANUP(McxtReqKill, (Datum) Int32GetDatum(dst_pid));

		/* Read values from the dump file and put them on tuplestore. */
		PutDumpedValuesOnTuplestore(dumpfile, tupstore, tupdesc, dst_pid);
	}

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
	mcxtdumpEntry	*entry;

	snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, MyProcPid);

	LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);
	entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &MyProcPid, HASH_FIND, NULL);

	if (entry == NULL)
	{
		/*
		 * The dump request seems to have canceled already.
		 * Exit without dumping.
		 */
		LWLockRelease(McxtDumpHashLock);
		return;
	}

	entry->dump_status = MCXTDUMPSTATUS_DUMPING;
	src_pid = entry->src_pid;

	LWLockRelease(McxtDumpHashLock);

	fpout = AllocateFile(dumpfile, "w");

	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write memory context file \"%s\": %m",
						dumpfile)));
		FreeFile(fpout);

		LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

		if (hash_search(mcxtdumpHash, &MyProcPid, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");

		LWLockRelease(McxtDumpHashLock);

		return;
	}

	format_id = PG_MEMCONTEXT_FILE_FORMAT_ID;
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

		LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

		if (hash_search(mcxtdumpHash, &MyProcPid, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");

		LWLockRelease(McxtDumpHashLock);

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

	LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);
	entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &MyProcPid, HASH_FIND, NULL);

	/* During dumping, the requestor canceled the request. */
	if (entry->dump_status == MCXTDUMPSTATUS_CANCELING)
	{
		unlink(dumpfile);

		if (hash_search(mcxtdumpHash, &MyProcPid, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");

		LWLockRelease(McxtDumpHashLock);

		return;
	}

	/*  Dump has succeeded, notify it to the request. */
	entry->dump_status = MCXTDUMPSTATUS_DONE;
	LWLockRelease(McxtDumpHashLock);
	src_proc = BackendPidGetProc(src_pid);
	SetLatch(&(src_proc->procLatch));

	return;
}

/*
 * ProcessDumpMemoryInterrupt
 *		The portion of memory context dump interrupt handling that runs
 *		outside of the signal handler.
 */
void
ProcessDumpMemoryInterrupt(void)
{
	ProcSignalDumpMemoryPending = false;
	dump_memory_contexts();
}

/*
 * HandleProcSignalDumpMemory
 * 		Handle receipt of an interrupt indicating a memory context dump.
 *		Signal handler portion of interrupt handling.
 */
void
HandleProcSignalDumpMemory(void)
{
	ProcSignalDumpMemoryPending = true;
}

/*
 * McxtDumpShmemInit
 * 		Initialize mcxtdump hash table.
 */
void
McxtDumpShmemInit(void)
{
	HASHCTL		info;

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(pid_t);
	info.entrysize = sizeof(mcxtdumpEntry);

	LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);

	mcxtdumpHash = ShmemInitHash("mcxtdump hash",
							   SHMEM_MEMCONTEXT_SIZE,
							   SHMEM_MEMCONTEXT_SIZE,
							   &info,
							   HASH_ELEM | HASH_BLOBS);

	LWLockRelease(McxtDumpHashLock);
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
