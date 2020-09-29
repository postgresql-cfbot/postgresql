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

	if(fpout == NULL)
		/*
		 * Since pg_get_backend_memory_contexts() is called from local process,
		 * simply put tuples.
		 */
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	else
	{
		/*
		 * Write out the current memory context information in the form of
		 * "key: value" pairs to the file specified by the caller of
		 * pg_get_backend_memory_contexts().
		 */

		/*
		 * Make each memory context information starts with 'D'.
		 * This is checked by the caller when reading the file.
		 */
		fputc('D', fpout);

		fprintf(fpout,
			"name: %s, ident: %s, parent: %s, level: %d, total_bytes: %lu, total_nblocks: %lu, free_bytes: %lu, free_chunks: %lu, used_bytes: %lu,\n",
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
	 * Send signal for dumping memory contexts to the target process,
	 * and read the dumped file.
	 */
	else
	{
		FILE	   	*fpin;
		char		tmpfile[MAXPGPATH];
		char		dumpfile[MAXPGPATH];
		bool		found;
		mcxtdumpEntry  *entry;
		struct 		stat stat_tmp;
		PGPROC	   	*proc;
		int		format_id;

		snprintf(tmpfile, sizeof(tmpfile), "%s/%d.tmp", PG_MEMUSAGE_DIR, dst_pid);
		snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, dst_pid);

		/*
		 * Since we allow only one session can request to dump  memory context at
		 * the same time, check whether the dump files already exist.
		 */
		while (stat(dumpfile, &stat_tmp) == 0 || stat(tmpfile, &stat_tmp) == 0)
		{
			pg_usleep(1000000L);
		}

		entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &dst_pid, HASH_ENTER, &found);

		if (!found)
		{
			entry->is_dumped = false;
			entry->src_pid = MyProcPid;
		}

		/* Check whether the target process is PostgreSQL backend process */
		/* TODO: Check also whether backend or not. */
		proc = BackendPidGetProc(dst_pid);

		if (proc == NULL)
		{
			ereport(WARNING,
					(errmsg("PID %d is not a PostgreSQL server process", dst_pid)));
			return (Datum) 1;
		}

		SendProcSignal(dst_pid, PROCSIG_DUMP_MEMORY, InvalidBackendId);

		/* Wait until target process finished dumping file. */
		while (!entry->is_dumped)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(10000L);
		}

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
					if (fscanf(fpin, "name: %1023[^,], ident: %1023[^,], parent: %1023[^,], level: %d, total_bytes: %lu, total_nblocks: %lu, free_bytes: %lu, free_chunks: %lu, used_bytes: %lu,\n",
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

		if (hash_search(mcxtdumpHash, &dst_pid, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");
		else
			elog(INFO, "successed deleting hash entry");
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * dump_memory_contexts
 * 		Dumping local memory contexts to a file.
 * 		This function does not delete dumped file, as it is intended to be read
 * 		by another process.
 */
static void
dump_memory_contexts(void)
{
	FILE		*fpout;
	char		tmpfile[MAXPGPATH];
	char		dumpfile[MAXPGPATH];
	mcxtdumpEntry	*entry;
	int		format_id;

	snprintf(tmpfile, sizeof(tmpfile), "%s/%d.tmp", PG_MEMUSAGE_DIR, MyProcPid);
	snprintf(dumpfile, sizeof(dumpfile), "%s/%d", PG_MEMUSAGE_DIR, MyProcPid);

	entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &MyProcPid, HASH_FIND, NULL);

	Assert(entry);

	fpout = AllocateFile(tmpfile, "w");

	if (fpout == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary memory context file \"%s\": %m",
						tmpfile)));
		return;
	}

	format_id = PG_MEMCONTEXT_FILE_FORMAT_ID;
	fwrite(&format_id, sizeof(format_id), 1, fpout);

	/* Look into each memory context from TopMemoryContext recursively. */
	PutMemoryContextsStatsTupleStore(NULL, NULL,
							TopMemoryContext, NULL, 0, fpout);

	/* No more output to be done. Close the tmp file and rename it. */
	fputc('E', fpout);

	if (ferror(fpout))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write temporary memory context dump file \"%s\": %m",
						tmpfile)));
		FreeFile(fpout);
		unlink(tmpfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not close temporary memory context dump file \"%s\": %m",
						tmpfile)));
		unlink(tmpfile);
	}
	else if (rename(tmpfile, dumpfile) < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename dump file \"%s\" to \"%s\": %m",
						tmpfile, dumpfile)));
		unlink(tmpfile);
	}

	entry->is_dumped = true;
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

	/* make sure the event is processed in due course */
	SetLatch(MyLatch);
}

/*
 * McxtDumpShmemInit
 * 		Initialize mcxtdump hash table
 */
void
McxtDumpShmemInit(void)
{
	HASHCTL		info;

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(pid_t);
	info.entrysize = sizeof(mcxtdumpEntry);

	mcxtdumpHash = ShmemInitHash("mcxtdump hash",
								   SHMEM_MEMCONTEXT_SIZE,
								   SHMEM_MEMCONTEXT_SIZE,
								   &info,
								   HASH_ELEM | HASH_BLOBS);
}

/*
 * pg_memusage_reset
 *	 	Remove the memory context dump files.
 */
void
pg_memusage_reset(void)
{
	DIR		*dir;
	struct 	dirent *dumpfile;

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
