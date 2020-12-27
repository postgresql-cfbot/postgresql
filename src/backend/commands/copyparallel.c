/*-------------------------------------------------------------------------
 *
 * copyparallel.c
 *              Implements the Parallel COPY utility command
 *
 * Parallel copy allows the copy from to leverage multiple CPUs in order to copy
 * data from file/STDIN to a table. This adds a PARALLEL option to COPY FROM
 * command where the user can specify the number of workers that can be used
 * to perform the COPY FROM command.
 * The backend, to which the "COPY FROM" query is submitted acts as leader with
 * the responsibility of reading data from the file/stdin, launching at most n
 * number of workers as specified with PARALLEL 'n' option in the "COPY FROM"
 * query. The leader populates the common data using
 * PARALLEL_COPY_KEY_SHARED_INFO, PARALLEL_COPY_KEY_CSTATE,
 * PARALLEL_COPY_KEY_WAL_USAGE & PARALLEL_COPY_KEY_BUFFER_USAGE	 required for
 * the workers execution in the DSM and shares it with the workers. The leader
 * then executes before statement triggers if there exists any. Leader populates
 * DSM lines (ParallelCopyDataBlock & ParallelCopyLineBoundaries data
 * structures) which includes the start offset and line size, while populating
 * the lines it reads as many blocks as required into the DSM data blocks from
 * the file. Each block is of 64K size. The leader parses the data to identify a
 * line, the existing logic from CopyReadLineText which identifies the lines
 * with some changes was used for this. Leader checks if a free line is
 * available (ParallelCopyLineBoundary->line_size will be -1) to copy the
 * information, if there is no free line it waits till the required line is
 * freed up by the worker and then copies the identified lines information
 * (offset & line size) into the DSM lines. Leader will set the state of the
 * line to LINE_LEADER_POPULATED to indicate the line is populated by the leader
 * and the worker can start processing the line. This process is repeated till
 * the complete file is processed. Simultaneously, each worker caches
 * WORKER_CHUNK_COUNT lines locally into the local memory and release the lines
 * to the leader for further populating. Each worker processes the lines as it
 * was done in sequential copy. Refer comments above ParallelCopyLineBoundary
 * for more details on leader/workers syncronization.
 * The leader does not participate in the insertion of data, leaders only
 * responsibility will be to identify the lines as fast as possible for the
 * workers to do the actual copy operation. The leader waits till all the lines
 * populated are processed by the workers and exits. We have chosen this design
 * based on the reason "that everything stalls if the leader doesn't accept
 * further input data, as well as when there are no available splitted chunks so
 * it doesn't seem like a good idea to have the leader do other work.  This is
 * backed by the performance data where we have seen that with 1 worker there is
 * just a 5-10% performance difference".
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/commands/copyparallel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/parallel.h"
#include "catalog/pg_proc_d.h"
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "utils/lsyscache.h"

/* DSM keys for parallel copy.  */
#define PARALLEL_COPY_KEY_SHARED_INFO				1
#define PARALLEL_COPY_KEY_CSTATE					2
#define PARALLEL_COPY_KEY_WAL_USAGE					3
#define PARALLEL_COPY_KEY_BUFFER_USAGE				4

/*
 * COPY_WAIT_TO_PROCESS - Wait before continuing to process.
 */
#define COPY_WAIT_TO_PROCESS() \
{ \
	CHECK_FOR_INTERRUPTS(); \
	(void) WaitLatch(MyLatch, \
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, \
					 1L, WAIT_EVENT_PG_SLEEP); \
	ResetLatch(MyLatch); \
}

/*
 * Estimate1ByteStrSize
 *
 * Estimate the size required for  1Byte strings in shared memory.
 */
static inline uint32
Estimate1ByteStrSize(char *src)
{
	return (src) ? sizeof(uint8) + 1 : sizeof(uint8);
}

/*
 * EstimateNodeSize
 *
 * Estimate the size required for  node type in shared memory.
 */
static uint32
EstimateNodeSize(List *list, char **listStr)
{
	uint32		estsize = sizeof(uint32);

	if ((List *) list != NIL)
	{
		*listStr = nodeToString(list);
		estsize += strlen(*listStr) + 1;
	}

	return estsize;
}

/*
 * EstimateCStateSize
 *
 * Estimate the size of the required cstate variables in the shared memory.
 */
static uint32
EstimateCStateSize(ParallelContext *pcxt, CopyFromState cstate, List *attnamelist,
				   SerializedListToStrCState *list_converted_str)
{
	Size		estsize = 0;
	uint32		estnodesize;

	estsize = add_size(estsize, sizeof(cstate->copy_src));
	estsize = add_size(estsize, sizeof(cstate->opts.file_encoding));
	estsize = add_size(estsize, sizeof(cstate->need_transcoding));
	estsize = add_size(estsize, sizeof(cstate->encoding_embeds_ascii));
	estsize = add_size(estsize, sizeof(cstate->opts.csv_mode));
	estsize = add_size(estsize, sizeof(cstate->opts.header_line));
	estsize = add_size(estsize, sizeof(cstate->opts.null_print_len));
	estsize = add_size(estsize, sizeof(cstate->opts.force_quote_all));
	estsize = add_size(estsize, sizeof(cstate->opts.convert_selectively));
	estsize = add_size(estsize, sizeof(cstate->num_defaults));
	estsize = add_size(estsize, sizeof(cstate->pcdata->relid));
	estsize = add_size(estsize, sizeof(cstate->opts.binary));

	/* Size of null_print is available in null_print_len. */
	estsize = add_size(estsize, sizeof(uint32) + cstate->opts.null_print_len);

	/* delim, quote & escape all uses 1 byte string to store the information. */
	estsize = add_size(estsize, Estimate1ByteStrSize(cstate->opts.delim));
	estsize = add_size(estsize, Estimate1ByteStrSize(cstate->opts.quote));
	estsize = add_size(estsize, Estimate1ByteStrSize(cstate->opts.escape));

	estnodesize = EstimateNodeSize(attnamelist, &list_converted_str->attnameListStr);
	estsize = add_size(estsize, estnodesize);
	estnodesize = EstimateNodeSize(cstate->opts.force_notnull, &list_converted_str->notnullListStr);
	estsize = add_size(estsize, estnodesize);
	estnodesize = EstimateNodeSize(cstate->opts.force_null, &list_converted_str->nullListStr);
	estsize = add_size(estsize, estnodesize);
	estnodesize = EstimateNodeSize(cstate->opts.convert_select, &list_converted_str->convertListStr);
	estsize = add_size(estsize, estnodesize);
	estnodesize = EstimateNodeSize((List *) cstate->whereClause, &list_converted_str->whereClauseStr);
	estsize = add_size(estsize, estnodesize);
	estnodesize = EstimateNodeSize(cstate->range_table, &list_converted_str->rangeTableStr);
	estsize = add_size(estsize, estnodesize);

	shm_toc_estimate_chunk(&pcxt->estimator, estsize);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	return estsize;
}

/*
 * SerializeStringToSharedMemory
 *
 * Copy the string to shared memory.
 */
static void
SerializeStringToSharedMemory(char *destptr, char *srcPtr, Size *copiedsize)
{
	uint32		len = srcPtr ? strlen(srcPtr) + 1 : 0;

	memcpy(destptr + *copiedsize, (uint32 *) &len, sizeof(uint32));
	*copiedsize += sizeof(uint32);
	if (len)
	{
		memcpy(destptr + *copiedsize, srcPtr, len);
		*copiedsize += len;
	}
}

/*
 * Serialize1ByteStr
 *
 * Copy 1Byte strings to shared memory.
 */
static void
Serialize1ByteStr(char *dest, char *src, Size *copiedsize)
{
	uint8		len = (src) ? 1 : 0;

	memcpy(dest + (*copiedsize), (uint8 *) &len, sizeof(uint8));
	*copiedsize += sizeof(uint8);
	if (src)
		dest[(*copiedsize)++] = src[0];
}

/*
 * SerializeParallelCopyState
 *
 * Serialize the cstate members required by the workers into shared memory.
 */
static void
SerializeParallelCopyState(ParallelContext *pcxt, CopyFromState cstate,
						   uint32 estimatedSize,
						   SerializedListToStrCState *list_converted_str)
{
	char	   *shmptr = (char *) shm_toc_allocate(pcxt->toc, estimatedSize + 1);
	Size		copiedsize = 0;

	memcpy(shmptr + copiedsize, (char *) &cstate->copy_src, sizeof(cstate->copy_src));
	copiedsize += sizeof(cstate->copy_src);
	memcpy(shmptr + copiedsize, (char *) &cstate->file_encoding, sizeof(cstate->opts.file_encoding));
	copiedsize += sizeof(cstate->opts.file_encoding);
	memcpy(shmptr + copiedsize, (char *) &cstate->need_transcoding, sizeof(cstate->need_transcoding));
	copiedsize += sizeof(cstate->need_transcoding);
	memcpy(shmptr + copiedsize, (char *) &cstate->encoding_embeds_ascii, sizeof(cstate->encoding_embeds_ascii));
	copiedsize += sizeof(cstate->encoding_embeds_ascii);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.csv_mode, sizeof(cstate->opts.csv_mode));
	copiedsize += sizeof(cstate->opts.csv_mode);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.header_line, sizeof(cstate->opts.header_line));
	copiedsize += sizeof(cstate->opts.header_line);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.null_print_len, sizeof(cstate->opts.null_print_len));
	copiedsize += sizeof(cstate->opts.null_print_len);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.force_quote_all, sizeof(cstate->opts.force_quote_all));
	copiedsize += sizeof(cstate->opts.force_quote_all);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.convert_selectively, sizeof(cstate->opts.convert_selectively));
	copiedsize += sizeof(cstate->opts.convert_selectively);
	memcpy(shmptr + copiedsize, (char *) &cstate->num_defaults, sizeof(cstate->num_defaults));
	copiedsize += sizeof(cstate->num_defaults);
	memcpy(shmptr + copiedsize, (char *) &cstate->pcdata->relid, sizeof(cstate->pcdata->relid));
	copiedsize += sizeof(cstate->pcdata->relid);
	memcpy(shmptr + copiedsize, (char *) &cstate->opts.binary, sizeof(cstate->opts.binary));
	copiedsize += sizeof(cstate->opts.binary);

	memcpy(shmptr + copiedsize, (uint32 *) &cstate->opts.null_print_len, sizeof(uint32));
	copiedsize += sizeof(uint32);
	if (cstate->opts.null_print_len)
	{
		memcpy(shmptr + copiedsize, cstate->opts.null_print, cstate->opts.null_print_len);
		copiedsize += cstate->opts.null_print_len;
	}

	Serialize1ByteStr(shmptr, cstate->opts.delim, &copiedsize);
	Serialize1ByteStr(shmptr, cstate->opts.quote, &copiedsize);
	Serialize1ByteStr(shmptr, cstate->opts.escape, &copiedsize);

	SerializeStringToSharedMemory(shmptr, list_converted_str->attnameListStr, &copiedsize);
	SerializeStringToSharedMemory(shmptr, list_converted_str->notnullListStr, &copiedsize);
	SerializeStringToSharedMemory(shmptr, list_converted_str->nullListStr, &copiedsize);
	SerializeStringToSharedMemory(shmptr, list_converted_str->convertListStr, &copiedsize);
	SerializeStringToSharedMemory(shmptr, list_converted_str->whereClauseStr, &copiedsize);
	SerializeStringToSharedMemory(shmptr, list_converted_str->rangeTableStr, &copiedsize);

	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_CSTATE, shmptr);
}

/*
 * RestoreNodeFromSharedMemory
 *
 * Copy the node contents which was stored as string format in shared memory &
 * convert it into node type.
 */
static void *
RestoreNodeFromSharedMemory(char *srcPtr, Size *copiedsize, bool isSrcStr)
{
	char	   *destptr = NULL;
	List	   *destList = NIL;
	uint32		len;

	memcpy((uint32 *) (&len), srcPtr + *copiedsize, sizeof(uint32));
	*copiedsize += sizeof(uint32);
	if (len)
	{
		destptr = (char *) palloc(len);
		memcpy(destptr, srcPtr + *copiedsize, len);
		*copiedsize += len;
		if (!isSrcStr)
		{
			destList = (List *) stringToNode(destptr);
			pfree(destptr);
			return destList;
		}

		return destptr;
	}

	return NULL;
}

/*
 * Restore1ByteStr
 *
 * Restore 1Byte strings from shared memory to worker local memory.
 */
static void
Restore1ByteStr(char **dest, char *src, Size *copiedsize)
{
	uint8		len;

	memcpy((uint8 *) (&len), src + (*copiedsize), sizeof(uint8));
	(*copiedsize) += sizeof(uint8);
	if (len)
	{
		*dest = palloc0(sizeof(char) + 1);
		(*dest)[0] = src[(*copiedsize)++];
	}
}

/*
 * RestoreParallelCopyState
 *
 * Retrieve the cstate members which was populated by the leader in the shared
 * memory.
 */
static void
RestoreParallelCopyState(shm_toc *toc, CopyFromState cstate, List **attlist)
{
	char	   *shared_str_val = (char *) shm_toc_lookup(toc, PARALLEL_COPY_KEY_CSTATE, true);
	Size		copiedsize = 0;

	memcpy((char *) &cstate->copy_src, shared_str_val + copiedsize, sizeof(cstate->copy_src));
	copiedsize += sizeof(cstate->copy_src);
	memcpy((char *) &cstate->opts.file_encoding, shared_str_val + copiedsize, sizeof(cstate->opts.file_encoding));
	copiedsize += sizeof(cstate->opts.file_encoding);
	memcpy((char *) &cstate->need_transcoding, shared_str_val + copiedsize, sizeof(cstate->need_transcoding));
	copiedsize += sizeof(cstate->need_transcoding);
	memcpy((char *) &cstate->encoding_embeds_ascii, shared_str_val + copiedsize, sizeof(cstate->encoding_embeds_ascii));
	copiedsize += sizeof(cstate->encoding_embeds_ascii);
	memcpy((char *) &cstate->opts.csv_mode, shared_str_val + copiedsize, sizeof(cstate->opts.csv_mode));
	copiedsize += sizeof(cstate->opts.csv_mode);
	memcpy((char *) &cstate->opts.header_line, shared_str_val + copiedsize, sizeof(cstate->opts.header_line));
	copiedsize += sizeof(cstate->opts.header_line);
	memcpy((char *) &cstate->opts.null_print_len, shared_str_val + copiedsize, sizeof(cstate->opts.null_print_len));
	copiedsize += sizeof(cstate->opts.null_print_len);
	memcpy((char *) &cstate->opts.force_quote_all, shared_str_val + copiedsize, sizeof(cstate->opts.force_quote_all));
	copiedsize += sizeof(cstate->opts.force_quote_all);
	memcpy((char *) &cstate->opts.convert_selectively, shared_str_val + copiedsize, sizeof(cstate->opts.convert_selectively));
	copiedsize += sizeof(cstate->opts.convert_selectively);
	memcpy((char *) &cstate->num_defaults, shared_str_val + copiedsize, sizeof(cstate->num_defaults));
	copiedsize += sizeof(cstate->num_defaults);
	memcpy((char *) &cstate->pcdata->relid, shared_str_val + copiedsize, sizeof(cstate->pcdata->relid));
	copiedsize += sizeof(cstate->pcdata->relid);
	memcpy((char *) &cstate->opts.binary, shared_str_val + copiedsize, sizeof(cstate->opts.binary));
	copiedsize += sizeof(cstate->opts.binary);

	cstate->opts.null_print = (char *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, true);
	if (!cstate->opts.null_print)
		cstate->opts.null_print = "";

	Restore1ByteStr(&cstate->opts.delim, shared_str_val, &copiedsize);
	Restore1ByteStr(&cstate->opts.quote, shared_str_val, &copiedsize);
	Restore1ByteStr(&cstate->opts.escape, shared_str_val, &copiedsize);

	*attlist = (List *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
	cstate->opts.force_notnull = (List *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
	cstate->opts.force_null = (List *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
	cstate->opts.convert_select = (List *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
	cstate->whereClause = (Node *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
	cstate->range_table = (List *) RestoreNodeFromSharedMemory(shared_str_val, &copiedsize, false);
}

/*
 * PopulateParallelCopyShmInfo
 *
 * Sets ParallelCopyShmInfo structure members.
 */
static void
PopulateParallelCopyShmInfo(ParallelCopyShmInfo *shared_info_ptr)
{
	uint32		count;

	MemSet(shared_info_ptr, 0, sizeof(ParallelCopyShmInfo));
	shared_info_ptr->is_read_in_progress = true;
	shared_info_ptr->cur_block_pos = -1;
	SpinLockInit(&shared_info_ptr->line_boundaries.worker_pos_lock);
	for (count = 0; count < RINGSIZE; count++)
	{
		ParallelCopyLineBoundary *lineInfo = &shared_info_ptr->line_boundaries.ring[count];

		pg_atomic_init_u32(&(lineInfo->line_size), -1);
	}
}

/*
 * CheckExprParallelSafety
 *
 * Determine if where clause and default expressions are parallel safe & do not
 * have volatile expressions, return true if condition satisfies else return
 * false.
 */
static bool
CheckExprParallelSafety(CopyFromState cstate)
{
	if (max_parallel_hazard((Query *) cstate->whereClause) != PROPARALLEL_SAFE)
		return false;

	/*
	 * Can't support parallel copy if there are any volatile function
	 * expressions in WHERE clause as such expressions may query the table
	 * we're inserting into.
	 */
	if (contain_volatile_functions(cstate->whereClause))
		return false;

	/*
	 * Check if any of the column has default expression. if yes, and they are
	 * not parallel safe, then parallelism is not allowed. For instance, if
	 * there are any serial/bigserial columns for which nextval() default
	 * expression which is parallel unsafe is associated, parallelism should
	 * not be allowed.
	 */
	if (cstate->defexprs != NULL && cstate->num_defaults != 0)
	{
		int			i;

		for (i = 0; i < cstate->num_defaults; i++)
		{
			if (max_parallel_hazard((Query *) cstate->defexprs[i]->expr) !=
				PROPARALLEL_SAFE)
				return false;

			/*
			 * Can't support parallel copy if there are any volatile function
			 * expressions in default expressions as such expressions may
			 * query the table we're inserting into.
			 */
			if (contain_volatile_functions((Node *) cstate->defexprs[i]->expr))
				return false;
		}
	}

	return true;
}

/*
 * IsParallelCopyAllowed
 *
 * Check if parallel copy can be allowed.
 */
bool
IsParallelCopyAllowed(CopyFromState cstate, Oid relid)
{
	/*
	 * Check if parallel operation can be performed based on local
	 * table/foreign table/index/check constraints/triggers present for the
	 * relation and also by doing similar checks recursively for each of the
	 * associated parrtitions if exists.
	 */
	if (MaxParallelHazardForModify(relid) != PROPARALLEL_SAFE)
		return false;

	/*
	 * If there are volatile default expressions or where clause contain
	 * volatile expressions, allow parallelism if they are parallel safe,
	 * otherwise not.
	 */
	if (!CheckExprParallelSafety(cstate))
		return false;

	return true;
}

/*
 * BeginParallelCopy - Start parallel copy tasks.
 *
 * Get the number of workers required to perform the parallel copy. The data
 * structures that are required by the parallel workers will be initialized, the
 * size required in DSM will be calculated and the necessary keys will be loaded
 * in the DSM. The specified number of workers will then be launched.
 */
ParallelContext *
BeginParallelCopy(CopyFromState cstate, List *attnamelist, Oid relid)
{
	ParallelContext *pcxt;
	ParallelCopyShmInfo *shared_info_ptr;
	int			parallel_workers = 0;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	ParallelCopyData *pcdata;
	MemoryContext oldcontext;
	uint32		estsize;
	SerializedListToStrCState list_converted_str = {0};

	CheckTargetRelValidity(cstate);
	parallel_workers = Min(cstate->opts.nworkers, max_worker_processes);

	/* Can't perform copy in parallel */
	if (parallel_workers == 0)
		return NULL;

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);
	pcdata = (ParallelCopyData *) palloc0(sizeof(ParallelCopyData));
	MemoryContextSwitchTo(oldcontext);

	cstate->pcdata = pcdata;
	pcdata->relid = relid;
	(void) GetCurrentFullTransactionId();
	(void) GetCurrentCommandId(true);

	EnterParallelMode();
	pcxt = CreateParallelContext("postgres", "ParallelCopyMain",
								 parallel_workers);
	Assert(pcxt->nworkers > 0);

	/*
	 * Estimate size for shared information for PARALLEL_COPY_KEY_SHARED_INFO
	 */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelCopyShmInfo));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate the size for shared information for PARALLEL_COPY_KEY_CSTATE */
	estsize = EstimateCStateSize(pcxt, cstate, attnamelist, &list_converted_str);

	/*
	 * Estimate space for WalUsage and BufferUsage --
	 * PARALLEL_COPY_KEY_WAL_USAGE and PARALLEL_COPY_KEY_BUFFER_USAGE.
	 */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, back out (do serial copy) */
	if (pcxt->seg == NULL)
	{
		EndParallelCopy(pcxt);
		pfree(pcdata);
		cstate->pcdata = NULL;
		return NULL;
	}

	/* Allocate shared memory for PARALLEL_COPY_KEY_SHARED_INFO */
	shared_info_ptr = (ParallelCopyShmInfo *) shm_toc_allocate(pcxt->toc, sizeof(ParallelCopyShmInfo));
	PopulateParallelCopyShmInfo(shared_info_ptr);

	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_SHARED_INFO, shared_info_ptr);
	pcdata->pcshared_info = shared_info_ptr;

	SerializeParallelCopyState(pcxt, cstate, estsize, &list_converted_str);

	/*
	 * Allocate space for each worker's WalUsage and BufferUsage; no need to
	 * initialize.
	 */
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_WAL_USAGE, walusage);
	pcdata->walusage = walusage;
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_COPY_KEY_BUFFER_USAGE, bufferusage);
	pcdata->bufferusage = bufferusage;

	LaunchParallelWorkers(pcxt);
	if (pcxt->nworkers_launched == 0)
	{
		EndParallelCopy(pcxt);
		pfree(pcdata);
		cstate->pcdata = NULL;
		return NULL;
	}

	/*
	 * Caller needs to wait for all launched workers when we return.  Make
	 * sure that the failure-to-start case will not hang forever.
	 */
	WaitForParallelWorkersToAttach(pcxt);

	pcdata->is_leader = true;
	cstate->is_parallel = true;
	return pcxt;
}

/*
 * EndParallelCopy
 *
 * End the parallel copy tasks.
 */
pg_attribute_always_inline void
EndParallelCopy(ParallelContext *pcxt)
{
	Assert(!IsParallelWorker());

	DestroyParallelContext(pcxt);
	ExitParallelMode();
}

/*
 * InitializeParallelCopyInfo
 *
 * Initialize parallel worker.
 */
static void
InitializeParallelCopyInfo(CopyFromState cstate, List *attnamelist)
{
	uint32		count;
	ParallelCopyData *pcdata = cstate->pcdata;
	TupleDesc	tup_desc = RelationGetDescr(cstate->rel);

	PopulateCommonCStateInfo(cstate, tup_desc, attnamelist);

	/* Initialize state variables. */
	cstate->reached_eof = false;
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->pcdata->curr_data_block = NULL;

	/* Set up variables to avoid per-attribute overhead. */
	initStringInfo(&cstate->attribute_buf);

	initStringInfo(&cstate->line_buf);
	for (count = 0; count < WORKER_CHUNK_COUNT; count++)
		initStringInfo(&pcdata->worker_line_buf[count].line_buf);

	cstate->line_buf_converted = false;
	cstate->raw_buf = NULL;
	cstate->raw_buf_index = cstate->raw_buf_len = 0;

	PopulateCStateCatalogInfo(cstate);

	/* Create workspace for CopyReadAttributes results. */
	if (!cstate->opts.binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}
}

/*
 * UpdateLineInfo
 *
 * Update line information & return.
 */
static bool
UpdateLineInfo(ParallelCopyShmInfo *pcshared_info,
			   ParallelCopyLineBoundary *lineInfo, uint32 write_pos)
{
	elog(DEBUG1, "[Worker] Completed processing line:%u", write_pos);
	pg_atomic_write_u32(&lineInfo->line_state, LINE_WORKER_PROCESSED);
	pg_atomic_write_u32(&lineInfo->line_size, -1);
	pg_atomic_add_fetch_u64(&pcshared_info->total_worker_processed, 1);
	return false;
}

/*
 * CacheLineInfo
 *
 * Cache the line information to local memory.
 */
static bool
CacheLineInfo(CopyFromState cstate, uint32 buff_count, uint32 line_pos)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ParallelCopyData *pcdata = cstate->pcdata;
	uint32		write_pos;
	ParallelCopyDataBlock *data_blk_ptr;
	ParallelCopyLineBoundary *lineInfo;
	uint32		offset;
	uint32		dataSize;
	uint32		copiedSize = 0;

	resetStringInfo(&pcdata->worker_line_buf[buff_count].line_buf);
	write_pos = GetLinePosition(cstate, line_pos);
	if (-1 == write_pos)
		return true;

	/* Get the current line information. */
	lineInfo = &pcshared_info->line_boundaries.ring[write_pos];
	if (pg_atomic_read_u32(&lineInfo->line_size) == 0)
		return UpdateLineInfo(pcshared_info, lineInfo, write_pos);

	/* Get the block information. */
	data_blk_ptr = &pcshared_info->data_blocks[lineInfo->first_block];

	/* Get the offset information from where the data must be copied. */
	offset = lineInfo->start_offset;
	pcdata->worker_line_buf[buff_count].cur_lineno = lineInfo->cur_lineno;

	elog(DEBUG1, "[Worker] Processing - line position:%u, block:%u, unprocessed lines:%u, offset:%u, line size:%u",
		 write_pos, lineInfo->first_block,
		 pg_atomic_read_u32(&data_blk_ptr->unprocessed_line_parts),
		 offset, pg_atomic_read_u32(&lineInfo->line_size));

	for (;;)
	{
		uint8		skip_bytes = data_blk_ptr->skip_bytes;

		/*
		 * There is a possibility that the loop embedded at the bottom of the
		 * current loop has come out because data_blk_ptr->curr_blk_completed
		 * is set, but dataSize read might be an old value, if
		 * data_blk_ptr->curr_blk_completed and the line is completed,
		 * line_size will be set. Read the line_size again to be sure if it is
		 * completed or partial block.
		 */
		dataSize = pg_atomic_read_u32(&lineInfo->line_size);
		if (dataSize != -1)
		{
			uint32		remainingSize = dataSize - copiedSize;

			if (!remainingSize)
				break;

			/* Whole line is in current block. */
			if (remainingSize + offset + skip_bytes < DATA_BLOCK_SIZE)
			{
				appendBinaryStringInfo(&pcdata->worker_line_buf[buff_count].line_buf,
									   &data_blk_ptr->data[offset],
									   remainingSize);
				pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts,
										1);
				break;
			}
			else
			{
				/* Line is spread across the blocks. */
				uint32		lineInCurrentBlock = (DATA_BLOCK_SIZE - skip_bytes) - offset;

				appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
										 &data_blk_ptr->data[offset],
										 lineInCurrentBlock);
				pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts, 1);
				copiedSize += lineInCurrentBlock;
				while (copiedSize < dataSize)
				{
					uint32		currentBlockCopySize;
					ParallelCopyDataBlock *currBlkPtr = &pcshared_info->data_blocks[data_blk_ptr->following_block];

					skip_bytes = currBlkPtr->skip_bytes;

					/*
					 * If complete data is present in current block use
					 * dataSize - copiedSize, or copy the whole block from
					 * current block.
					 */
					currentBlockCopySize = Min(dataSize - copiedSize, DATA_BLOCK_SIZE - skip_bytes);
					appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
											 &currBlkPtr->data[0],
											 currentBlockCopySize);
					pg_atomic_sub_fetch_u32(&currBlkPtr->unprocessed_line_parts, 1);
					copiedSize += currentBlockCopySize;
					data_blk_ptr = currBlkPtr;
				}

				break;
			}
		}
		else if (data_blk_ptr->curr_blk_completed)
		{
			/* Copy this complete block from the current offset. */
			uint32		lineInCurrentBlock = (DATA_BLOCK_SIZE - skip_bytes) - offset;

			appendBinaryStringInfoNT(&pcdata->worker_line_buf[buff_count].line_buf,
									 &data_blk_ptr->data[offset],
									 lineInCurrentBlock);
			pg_atomic_sub_fetch_u32(&data_blk_ptr->unprocessed_line_parts, 1);
			copiedSize += lineInCurrentBlock;

			/*
			 * Reset the offset. For the first copy, copy from the offset. For
			 * the subsequent copy the complete block.
			 */
			offset = 0;

			/* Set data_blk_ptr to the following block. */
			data_blk_ptr = &pcshared_info->data_blocks[data_blk_ptr->following_block];
		}

		for (;;)
		{
			/* Get the size of this line */
			dataSize = pg_atomic_read_u32(&lineInfo->line_size);

			/*
			 * If the data is present in current block lineInfo->line_size
			 * will be updated. If the data is spread across the blocks either
			 * of lineInfo->line_size or data_blk_ptr->curr_blk_completed can
			 * be updated. lineInfo->line_size will be updated if the complete
			 * read is finished. data_blk_ptr->curr_blk_completed will be
			 * updated if processing of current block is finished and data
			 * processing is not finished.
			 */
			if (data_blk_ptr->curr_blk_completed || (dataSize != -1))
				break;

			COPY_WAIT_TO_PROCESS()
		}
	}

	return UpdateLineInfo(pcshared_info, lineInfo, write_pos);
}

/*
 * GetCachedLine
 *
 * Return a previously cached line to be processed by the worker.
 */
static bool
GetCachedLine(CopyFromState cstate, ParallelCopyData *pcdata)
{
	cstate->line_buf = pcdata->worker_line_buf[pcdata->worker_line_buf_pos].line_buf;
	cstate->cur_lineno = pcdata->worker_line_buf[pcdata->worker_line_buf_pos].cur_lineno;
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet. */
	cstate->line_buf_converted = false;

	/* For binary format data, we don't need conversion. */
	if (!cstate->opts.binary)
		ConvertToServerEncoding(cstate);

	pcdata->worker_line_buf_pos++;
	return false;
}

/*
 * GetWorkerLine
 *
 * Returns a line for worker to process.
 */
bool
GetWorkerLine(CopyFromState cstate)
{
	uint32		buff_count;
	ParallelCopyData *pcdata = cstate->pcdata;
	ParallelCopyLineBoundaries *line_boundaries = &pcdata->pcshared_info->line_boundaries;
	uint32 worker_pos;

	/*
	 * Copy the line data to line_buf and release the line position so that
	 * the worker can continue loading data.
	 */
	if (pcdata->worker_line_buf_pos < pcdata->worker_line_buf_count)
		return GetCachedLine(cstate, pcdata);

	pcdata->worker_line_buf_pos = 0;
	pcdata->worker_line_buf_count = 0;

	SpinLockAcquire(&line_boundaries->worker_pos_lock);
	worker_pos = line_boundaries->worker_pos;
	line_boundaries->worker_pos = (line_boundaries->worker_pos +
								   WORKER_CHUNK_COUNT) % RINGSIZE;
	SpinLockRelease(&line_boundaries->worker_pos_lock);

	for (buff_count = 0; buff_count < WORKER_CHUNK_COUNT; buff_count++)
	{
		bool result = CacheLineInfo(cstate, buff_count,
									worker_pos + buff_count);
		if (result)
			break;

		pcdata->worker_line_buf_count++;
	}

	if (pcdata->worker_line_buf_count > 0)
		return GetCachedLine(cstate, pcdata);
	else
		resetStringInfo(&cstate->line_buf);

	return true;
}

/*
 * ParallelCopyMain - Parallel copy worker's code.
 *
 * Where clause handling, convert tuple to columns, add default null values for
 * the missing columns that are not present in that record. Find the partition
 * if it is partitioned table, invoke before row insert Triggers, handle
 * constraints and insert the tuples.
 */
void
ParallelCopyMain(dsm_segment *seg, shm_toc *toc)
{
	CopyFromState cstate;
	ParallelCopyData *pcdata;
	ParallelCopyShmInfo *pcshared_info;
	Relation	rel = NULL;
	MemoryContext oldcontext;
	List	   *attlist = NIL;
	WalUsage   *walusage;
	BufferUsage *bufferusage;

	/* Allocate workspace and zero all fields. */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	pcdata = (ParallelCopyData *) palloc0(sizeof(ParallelCopyData));
	cstate->pcdata = pcdata;
	pcdata->is_leader = false;
	pcdata->worker_processed_pos = -1;
	cstate->is_parallel = true;
	pcshared_info = (ParallelCopyShmInfo *) shm_toc_lookup(toc, PARALLEL_COPY_KEY_SHARED_INFO, false);

	ereport(DEBUG1, (errmsg("Starting parallel copy worker")));

	pcdata->pcshared_info = pcshared_info;
	RestoreParallelCopyState(toc, cstate, &attlist);

	/* Open and lock the relation, using the appropriate lock type. */
	rel = table_open(cstate->pcdata->relid, RowExclusiveLock);
	cstate->rel = rel;
	InitializeParallelCopyInfo(cstate, attlist);

	/* Prepare to track buffer usage during parallel execution */
	InstrStartParallelQuery();

	CopyFrom(cstate);

	if (rel != NULL)
		table_close(rel, RowExclusiveLock);

	/* Report WAL/buffer usage during parallel execution */
	bufferusage = shm_toc_lookup(toc, PARALLEL_COPY_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_COPY_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
	return;
}

/*
 * UpdateSharedLineInfo
 *
 * Update the line information.
 */
uint32
UpdateSharedLineInfo(CopyFromState cstate, uint32 blk_pos, uint32 offset,
					 uint32 line_size, uint32 line_state, uint32 blk_line_pos)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ParallelCopyLineBoundaries *lineBoundaryPtr = &pcshared_info->line_boundaries;
	ParallelCopyLineBoundary *lineInfo;
	uint32		line_pos;

	/* blk_line_pos will be valid in case line_pos was blocked earlier. */
	if (blk_line_pos == -1)
	{
		line_pos = lineBoundaryPtr->pos;

		/* Update the line information for the worker to pick and process. */
		lineInfo = &lineBoundaryPtr->ring[line_pos];
		while (pg_atomic_read_u32(&lineInfo->line_size) != -1)
			COPY_WAIT_TO_PROCESS()

		lineInfo->first_block = blk_pos;
		lineInfo->start_offset = offset;
		lineInfo->cur_lineno = cstate->cur_lineno;
		lineBoundaryPtr->pos = (lineBoundaryPtr->pos + 1) % RINGSIZE;
	}
	else
	{
		line_pos = blk_line_pos;
		lineInfo = &lineBoundaryPtr->ring[line_pos];
	}

	if (line_state == LINE_LEADER_POPULATED)
	{
		elog(DEBUG1, "[Leader] Added line with block:%u, offset:%u, line position:%u, line size:%u",
			 lineInfo->first_block, lineInfo->start_offset, line_pos,
			 line_size);
		pcshared_info->populated++;
		if (line_size == 0 || cstate->opts.binary)
			pg_atomic_write_u32(&lineInfo->line_state, line_state);
		else
		{
			uint32		current_line_state = LINE_LEADER_POPULATING;

			/*
			 * Make sure that no worker has consumed this element, if this
			 * line is spread across multiple data blocks, worker would have
			 * started processing, no need to change the state to
			 * LINE_LEADER_POPULATED in this case.
			 */
			(void) pg_atomic_compare_exchange_u32(&lineInfo->line_state,
												  &current_line_state,
												  LINE_LEADER_POPULATED);
		}
	}
	else
	{
		elog(DEBUG1, "[Leader] Adding - block:%u, offset:%u, line position:%u",
			 lineInfo->first_block, lineInfo->start_offset, line_pos);
		pg_atomic_write_u32(&lineInfo->line_state, line_state);
	}

	pg_atomic_write_u32(&lineInfo->line_size, line_size);

	return line_pos;
}

/*
 * ParallelCopyFrom - parallel copy leader's functionality.
 *
 * Leader executes the before statement for before statement trigger, if before
 * statement trigger is present. It will read the table data from the file and
 * copy the contents to DSM data blocks. It will then read the input contents
 * from the DSM data block and identify the records based on line breaks. This
 * information is called line or a record that need to be inserted into a
 * relation. The line information will be stored in ParallelCopyLineBoundary DSM
 * data structure. Workers will then process this information and insert the
 * data in to table. It will repeat this process until the all data is read from
 * the file and all the DSM data blocks are processed. While processing if
 * leader identifies that DSM Data blocks or DSM ParallelCopyLineBoundary data
 * structures is full, leader will wait till the worker frees up some entries
 * and repeat the process. It will wait till all the lines populated are
 * processed by the workers and exits.
 */
void
ParallelCopyFrom(CopyFromState cstate)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	ErrorContextCallback errcallback;

	ereport(DEBUG1, (errmsg("Running parallel copy leader")));

	/* raw_buf is not used in parallel copy, instead data blocks are used. */
	pfree(cstate->raw_buf);
	cstate->raw_buf = NULL;

	/* Execute the before statement triggers from the leader */
	ExecBeforeStmtTrigger(cstate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	if (!cstate->opts.binary)
	{
		/* On input just throw the header line away. */
		if (cstate->cur_lineno == 0 && cstate->opts.header_line)
		{
			cstate->cur_lineno++;
			if (CopyReadLine(cstate))
			{
				pcshared_info->is_read_in_progress = false;
				return;				/* done */
			}
		}

		for (;;)
		{
			bool		done;

			CHECK_FOR_INTERRUPTS();

			cstate->cur_lineno++;

			/* Actually read the line into memory here. */
			done = CopyReadLine(cstate);

			/*
			 * EOF at start of line means we're done.  If we see EOF after
			 * some characters, we act as though it was newline followed by
			 * EOF, ie, process the line and then exit loop on next iteration.
			 */
			if (done && cstate->line_buf.len == 0)
				break;
		}
	}
	else
	{
		cstate->pcdata->curr_data_block = NULL;
		cstate->raw_buf_index = 0;
		pcshared_info->populated = 0;
		cstate->cur_lineno = 0;
		cstate->max_fields = list_length(cstate->attnumlist);

		for (;;)
		{
			bool		eof = false;

			CHECK_FOR_INTERRUPTS();

			cstate->cur_lineno++;

			eof = CopyReadBinaryTupleLeader(cstate);

			if (eof)
				break;
		}
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	/*
	 * In the old protocol, tell pqcomm that we can process normal protocol
	 * messages again.
	 */
	if (cstate->copy_src == COPY_OLD_FE)
		pq_endmsgread();

	pcshared_info->is_read_in_progress = false;
	cstate->cur_lineno = 0;
}

/*
 * CopyReadBinaryGetDataBlock
 *
 * Gets a new block, updates the current offset, calculates the skip bytes.
 */
void
CopyReadBinaryGetDataBlock(CopyFromState cstate, FieldInfoType field_info)
{
	ParallelCopyDataBlock *data_block = NULL;
	ParallelCopyDataBlock *curr_data_block = cstate->pcdata->curr_data_block;
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	uint8		move_bytes = 0;
	uint32		block_pos;
	uint32		prev_block_pos;
	int			read_bytes = 0;

	prev_block_pos = pcshared_info->cur_block_pos;

	block_pos = WaitGetFreeCopyBlock(pcshared_info);

	if (field_info == FIELD_SIZE || field_info == FIELD_COUNT)
		move_bytes = (DATA_BLOCK_SIZE - cstate->raw_buf_index);

	if (curr_data_block != NULL)
		curr_data_block->skip_bytes = move_bytes;

	data_block = &pcshared_info->data_blocks[block_pos];

	if (move_bytes > 0 && curr_data_block != NULL)
		memmove(&data_block->data[0], &curr_data_block->data[cstate->raw_buf_index], move_bytes);

	elog(DEBUG1, "LEADER - field info %d is spread across data blocks - moved %d bytes from current block %u to %u block",
		 field_info, move_bytes, prev_block_pos, block_pos);

	read_bytes = CopyGetData(cstate, &data_block->data[move_bytes], 1, (DATA_BLOCK_SIZE - move_bytes));

	if (field_info == FIELD_NONE && cstate->reached_eof)
		return;

	if (cstate->reached_eof)
		EOF_ERROR;

	elog(DEBUG1, "LEADER - bytes read from file %d", read_bytes);

	if (field_info == FIELD_SIZE || field_info == FIELD_DATA)
	{
		ParallelCopyDataBlock *prev_data_block = NULL;

		prev_data_block = curr_data_block;
		prev_data_block->following_block = block_pos;

		if (prev_data_block->curr_blk_completed == false)
			prev_data_block->curr_blk_completed = true;

		pg_atomic_add_fetch_u32(&prev_data_block->unprocessed_line_parts, 1);
	}

	cstate->pcdata->curr_data_block = data_block;
	cstate->raw_buf_index = 0;
}

/*
 * CopyReadBinaryTupleLeader
 *
 * Leader reads data from binary formatted file to data blocks and identifies
 * tuple boundaries/offsets so that workers can work on the data blocks data.
 */
bool
CopyReadBinaryTupleLeader(CopyFromState cstate)
{
	ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;
	int16		fld_count;
	uint32		line_size = 0;
	uint32		start_block_pos;
	uint32		start_offset;

	if (cstate->pcdata->curr_data_block == NULL)
	{
		CopyReadBinaryGetDataBlock(cstate, FIELD_NONE);

		/*
		 * no data is read from file here. one possibility to be here could be
		 * that the binary file just has a valid signature but nothing else.
		 */
		if (cstate->reached_eof)
			return true;
	}

	if ((cstate->raw_buf_index + sizeof(fld_count)) >= DATA_BLOCK_SIZE)
		CopyReadBinaryGetDataBlock(cstate, FIELD_COUNT);

	memcpy(&fld_count, &cstate->pcdata->curr_data_block->data[cstate->raw_buf_index], sizeof(fld_count));
	fld_count = (int16) pg_ntoh16(fld_count);

	CHECK_FIELD_COUNT;

	start_offset = cstate->raw_buf_index;
	cstate->raw_buf_index += sizeof(fld_count);
	line_size += sizeof(fld_count);
	start_block_pos = pcshared_info->cur_block_pos;

	CopyReadBinaryFindTupleSize(cstate, &line_size);

	pg_atomic_add_fetch_u32(&cstate->pcdata->curr_data_block->unprocessed_line_parts, 1);

	if (line_size > 0)
		(void) UpdateSharedLineInfo(cstate, start_block_pos, start_offset,
									line_size, LINE_LEADER_POPULATED, -1);

	return false;
}

/*
 * CopyReadBinaryFindTupleSize
 *
 * Leader identifies boundaries/offsets for each attribute/column and finally
 * results in the tuple/row size. It moves on to next data block if the
 * attribute/column is spread across data blocks.
 */
void
CopyReadBinaryFindTupleSize(CopyFromState cstate, uint32 *line_size)
{
	int32		fld_size;
	ListCell   *cur;
	TupleDesc	tup_desc = RelationGetDescr(cstate->rel);

	foreach(cur, cstate->attnumlist)
	{
		int			att_num = lfirst_int(cur);
		Form_pg_attribute att = TupleDescAttr(tup_desc, (att_num - 1));

		cstate->cur_attname = NameStr(att->attname);
		fld_size = 0;

		if ((cstate->raw_buf_index + sizeof(fld_size)) >= DATA_BLOCK_SIZE)
			CopyReadBinaryGetDataBlock(cstate, FIELD_SIZE);

		memcpy(&fld_size, &cstate->pcdata->curr_data_block->data[cstate->raw_buf_index], sizeof(fld_size));
		cstate->raw_buf_index += sizeof(fld_size);
		*line_size += sizeof(fld_size);
		fld_size = (int32) pg_ntoh32(fld_size);

		/* fld_size -1 represents the null value for the field. */
		if (fld_size == -1)
			continue;

		CHECK_FIELD_SIZE(fld_size);

		*line_size += fld_size;

		if ((DATA_BLOCK_SIZE - cstate->raw_buf_index) >= fld_size)
		{
			cstate->raw_buf_index += fld_size;
			elog(DEBUG1, "LEADER - tuple lies in he same data block");
		}
		else
		{
			int32		required_blks = 0;
			int32		curr_blk_bytes = (DATA_BLOCK_SIZE - cstate->raw_buf_index);
			int			i = 0;

			GET_REQUIRED_BLOCKS(required_blks, fld_size, curr_blk_bytes);

			i = required_blks;

			while (i > 0)
			{
				CopyReadBinaryGetDataBlock(cstate, FIELD_DATA);
				i--;
			}

			GET_RAW_BUF_INDEX(cstate->raw_buf_index, fld_size, required_blks, curr_blk_bytes);

			/*
			 * raw_buf_index should never cross data block size, as the
			 * required number of data blocks would have been obtained in the
			 * above while loop.
			 */
			Assert(cstate->raw_buf_index <= DATA_BLOCK_SIZE);
		}
		cstate->cur_attname = NULL;
	}
}

/*
 * CopyReadBinaryTupleWorker
 *
 * Each worker reads data from data blocks caches the tuple data into local
 * memory.
 */
bool
CopyReadBinaryTupleWorker(CopyFromState cstate, Datum *values, bool *nulls)
{
	int16		fld_count;
	ListCell   *cur;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	TupleDesc	tup_desc = RelationGetDescr(cstate->rel);
	bool		done = false;

	done = GetWorkerLine(cstate);
	cstate->raw_buf_index = 0;

	if (done && cstate->line_buf.len == 0)
		return true;

	memcpy(&fld_count, &cstate->line_buf.data[cstate->raw_buf_index], sizeof(fld_count));
	fld_count = (int16) pg_ntoh16(fld_count);

	CHECK_FIELD_COUNT;

	cstate->raw_buf_index += sizeof(fld_count);

	foreach(cur, cstate->attnumlist)
	{
		int			att_num = lfirst_int(cur);
		int			m = att_num - 1;
		Form_pg_attribute att = TupleDescAttr(tup_desc, m);

		cstate->cur_attname = NameStr(att->attname);

		values[m] = CopyReadBinaryAttributeWorker(cstate,
												  &in_functions[m],
												  typioparams[m],
												  att->atttypmod,
												  &nulls[m]);
		cstate->cur_attname = NULL;
	}

	return false;
}

/*
 * CopyReadBinaryAttributeWorker
 *
 * Worker identifies and converts each attribute/column data from binary to
 * the data type of attribute/column.
 */
Datum
CopyReadBinaryAttributeWorker(CopyFromState cstate, FmgrInfo *flinfo,
							  Oid typioparam, int32 typmod, bool *isnull)
{
	int32		fld_size;
	Datum		result;

	memcpy(&fld_size, &cstate->line_buf.data[cstate->raw_buf_index], sizeof(fld_size));
	cstate->raw_buf_index += sizeof(fld_size);
	fld_size = (int32) pg_ntoh32(fld_size);

	/* fld_size -1 represents the null value for the field. */
	if (fld_size == -1)
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}

	CHECK_FIELD_SIZE(fld_size);

	/* Reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);

	memcpy(&cstate->attribute_buf.data[0], &cstate->line_buf.data[cstate->raw_buf_index], fld_size);
	cstate->raw_buf_index += fld_size;

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';

	/* Call the column type's binary input converter */
	result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
								 typioparam, typmod);

	/* Trouble if it didn't eat the whole buffer */
	if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("incorrect binary data format")));

	*isnull = false;
	return result;
}

/*
 * GetLinePosition
 *
 * Return the line position once the leader has populated the data.
 */
uint32
GetLinePosition(CopyFromState cstate, uint32 line_pos)
{
	ParallelCopyData *pcdata = cstate->pcdata;
	ParallelCopyShmInfo *pcshared_info = pcdata->pcshared_info;
	uint32		write_pos = line_pos;

	for (;;)
	{
		uint32		dataSize;
		bool		is_read_in_progress = pcshared_info->is_read_in_progress;
		ParallelCopyLineBoundary *lineInfo;
		ParallelCopyDataBlock *data_blk_ptr;
		uint32		line_state = LINE_LEADER_POPULATED;

		CHECK_FOR_INTERRUPTS();

		/* File read completed & no elements to process. */
		if (!is_read_in_progress &&
			(pcshared_info->populated ==
			 pg_atomic_read_u64(&pcshared_info->total_worker_processed)))
		{
			write_pos = -1;
			break;
		}

		/* Get the current line information. */
		lineInfo = &pcshared_info->line_boundaries.ring[write_pos];

		/* Get the size of this line. */
		dataSize = pg_atomic_read_u32(&lineInfo->line_size);

		if (dataSize != 0)		/* If not an empty line. */
		{
			/* Get the block information. */
			data_blk_ptr = &pcshared_info->data_blocks[lineInfo->first_block];

			if (!data_blk_ptr->curr_blk_completed && (dataSize == -1))
			{
				/* Wait till the current line or block is added. */
				COPY_WAIT_TO_PROCESS()
					continue;
			}
		}

		/* Make sure that no worker has consumed this element. */
		if (pg_atomic_compare_exchange_u32(&lineInfo->line_state,
										   &line_state, LINE_WORKER_PROCESSING))
			break;

		line_state = LINE_LEADER_POPULATING;
		/* Make sure that no worker has consumed this element. */
		if (pg_atomic_compare_exchange_u32(&lineInfo->line_state,
										   &line_state, LINE_WORKER_PROCESSING))
			break;
	}

	pcdata->worker_processed_pos = write_pos;
	return write_pos;
}

/*
 * GetFreeCopyBlock
 *
 * Get a free block for data to be copied.
 */
static pg_attribute_always_inline uint32
GetFreeCopyBlock(ParallelCopyShmInfo *pcshared_info)
{
	int			count = 0;
	uint32		last_free_block = pcshared_info->cur_block_pos;
	uint32		block_pos = (last_free_block != -1) ? ((last_free_block + 1) % MAX_BLOCKS_COUNT) : 0;

	/*
	 * Get a new block for copying data, don't check current block, current
	 * block will have some unprocessed data.
	 */
	while (count < (MAX_BLOCKS_COUNT - 1))
	{
		ParallelCopyDataBlock *dataBlkPtr = &pcshared_info->data_blocks[block_pos];
		uint32		unprocessed_line_parts = pg_atomic_read_u32(&dataBlkPtr->unprocessed_line_parts);

		if (unprocessed_line_parts == 0)
		{
			dataBlkPtr->curr_blk_completed = false;
			dataBlkPtr->skip_bytes = 0;
			dataBlkPtr->following_block = -1;
			pcshared_info->cur_block_pos = block_pos;
			MemSet(&dataBlkPtr->data[0], 0, DATA_BLOCK_SIZE);
			return block_pos;
		}

		block_pos = (block_pos + 1) % MAX_BLOCKS_COUNT;
		count++;
	}

	return -1;
}

/*
 * WaitGetFreeCopyBlock
 *
 * If there are no blocks available, wait and get a block for copying data.
 */
uint32
WaitGetFreeCopyBlock(ParallelCopyShmInfo *pcshared_info)
{
	uint32		new_free_pos = -1;

	for (;;)
	{
		new_free_pos = GetFreeCopyBlock(pcshared_info);
		if (new_free_pos != -1) /* We have got one block, break now. */
			break;

		COPY_WAIT_TO_PROCESS()
	}

	return new_free_pos;
}

/*
 * SetRawBufForLoad
 *
 * Set raw_buf to the shared memory where the file data must be read.
 */
void
SetRawBufForLoad(CopyFromState cstate, uint32 line_size, uint32 copy_buf_len,
				 uint32 raw_buf_ptr, char **copy_raw_buf)
{
	ParallelCopyShmInfo *pcshared_info;
	uint32		cur_block_pos;
	uint32		next_block_pos;
	ParallelCopyDataBlock *cur_data_blk_ptr = NULL;
	ParallelCopyDataBlock *next_data_blk_ptr = NULL;

	Assert(IsParallelCopy());

	pcshared_info = cstate->pcdata->pcshared_info;
	cur_block_pos = pcshared_info->cur_block_pos;
	cur_data_blk_ptr = (cstate->raw_buf) ? &pcshared_info->data_blocks[cur_block_pos] : NULL;
	next_block_pos = WaitGetFreeCopyBlock(pcshared_info);
	next_data_blk_ptr = &pcshared_info->data_blocks[next_block_pos];

	/* set raw_buf to the data block in shared memory */
	cstate->raw_buf = next_data_blk_ptr->data;
	*copy_raw_buf = cstate->raw_buf;
	if (cur_data_blk_ptr)
	{
		if (line_size)
		{
			/*
			 * Mark the previous block as completed, worker can start copying
			 * this data.
			 */
			cur_data_blk_ptr->following_block = next_block_pos;
			pg_atomic_add_fetch_u32(&cur_data_blk_ptr->unprocessed_line_parts, 1);
			cur_data_blk_ptr->curr_blk_completed = true;
		}

		cur_data_blk_ptr->skip_bytes = copy_buf_len - raw_buf_ptr;
		cstate->raw_buf_len = cur_data_blk_ptr->skip_bytes;

		/* Copy the skip bytes to the next block to be processed. */
		if (cur_data_blk_ptr->skip_bytes)
			memcpy(cstate->raw_buf, cur_data_blk_ptr->data + raw_buf_ptr,
				   cur_data_blk_ptr->skip_bytes);
	}
	else
		cstate->raw_buf_len = 0;

	cstate->raw_buf_index = 0;
}

/*
 * EndLineParallelCopy
 *
 * Update the line information in shared memory.
 */
void
EndLineParallelCopy(CopyFromState cstate, uint32 line_pos, uint32 line_size,
					uint32 raw_buf_ptr)
{
	uint8		new_line_size;

	if (!IsParallelCopy())
		return;

	if (!IsHeaderLine())
	{
		ParallelCopyShmInfo *pcshared_info = cstate->pcdata->pcshared_info;

		/* Set the newline size. */
		if (cstate->eol_type == EOL_NL || cstate->eol_type == EOL_CR)
			new_line_size = 1;
		else if (cstate->eol_type == EOL_CRNL)
			new_line_size = 2;
		else
			new_line_size = 0;

		if (line_size)
		{
			/*
			 * If the new_line_size > raw_buf_ptr, then the new block has only
			 * new line char content. The unprocessed count should not be
			 * increased in this case.
			 */
			if (raw_buf_ptr > new_line_size)
			{
				uint32		cur_block_pos = pcshared_info->cur_block_pos;
				ParallelCopyDataBlock *curr_data_blk_ptr = &pcshared_info->data_blocks[cur_block_pos];

				pg_atomic_add_fetch_u32(&curr_data_blk_ptr->unprocessed_line_parts, 1);
			}

			/*
			 * Update line size & line state, other members are already
			 * updated.
			 */
			(void) UpdateSharedLineInfo(cstate, -1, -1, line_size,
										LINE_LEADER_POPULATED, line_pos);
		}
		else if (new_line_size)
			/* This means only new line char, empty record should be inserted. */
			(void) UpdateSharedLineInfo(cstate, -1, -1, 0,
										LINE_LEADER_POPULATED, -1);
	}
}

/*
 * ExecBeforeStmtTrigger
 *
 * Execute the before statement trigger, this will be executed for parallel copy
 * by the leader process. This function code changes has been taken from
 * CopyFrom function. Refer to comments section of respective code in CopyFrom
 * function for more detailed information.
 */
void
ExecBeforeStmtTrigger(CopyFromState cstate)
{
	EState	   *estate = CreateExecutorState();
	ResultRelInfo *resultRelInfo;

	Assert(IsLeader());
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);
	CheckValidResultRel(resultRelInfo, CMD_INSERT);
	AfterTriggerBeginQuery();
	ExecBSInsertTriggers(estate, resultRelInfo);
	AfterTriggerEndQuery(estate);
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);
	FreeExecutorState(estate);
}
