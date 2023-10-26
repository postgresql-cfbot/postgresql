/*--------------------------------------------------------------------------
 *
 * test_memstack.c
 *		Test harness code
 *
 * Copyright (c) 2013-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_memtrack/test_memtrack.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "varatt.h"

#include "storage/dsm.h"
#include "utils/backend_status.h"
#include "utils/memutils_internal.h"

#include "worker_pool.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_memtrack);
PG_FUNCTION_INFO_V1(test_allocation);

typedef enum {ALLOCATE, RELEASE} Action;
typedef struct AllocateRequest
{
	Action action;
	pg_allocator_type type;
	int32     nWorkers;
	int32     nBlocks;
	int64     blockSize;
} AllocateRequest;

typedef struct ResponseData
{
	int32		errorCode;
	PgBackendMemoryStatus  memory;
	PgBackendMemoryStatus  startingMemory;
} ResponseData;

/* Forward references */
static bool reserveBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static bool releaseBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static void validateArgs(int nWorkers, pg_allocator_type type, int nBlocks, int blockSize);
static void sendRequest(WorkerPool *pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize);
static void processReply(WorkerPool *pool, int worker, Action actions, pg_allocator_type type, int nBlocks, int blockSize);
static Datum test_nonDSM(FunctionCallInfo fcinfo, char *workerFunction);

/* Test the memory tracking features standalone */
PGDLLEXPORT Datum test_memtrack(PG_FUNCTION_ARGS);
PGDLLEXPORT void test_memtrack_worker(Datum arg);

/* Test the memory tracking features with actual allocations */
PGDLLEXPORT Datum test_allocation(PG_FUNCTION_ARGS);
PGDLLEXPORT void test_allocation_worker(Datum arg);
/*
 * Test the memory tracking features.
 */
Datum
test_memtrack(PG_FUNCTION_ARGS)
{
	return test_nonDSM(fcinfo, "test_memtrack_worker");
}


Datum
test_allocation(PG_FUNCTION_ARGS)
{
	return test_nonDSM(fcinfo, "test_allocation_worker");
}



/****************
 * Parent task to test the memory tracking features.
 * Schedules a pool of workers and verifies the results.
 * This version tests non-shared memory allocations.
 */
static
Datum test_nonDSM(FunctionCallInfo fcinfo, char *workerFunction)
{
	WorkerPool *pool;
	int64 delta, starting_bkend_bytes;
	int64 expected, fudge;

	/* Get  the SQL function arguments */
	int32 nWorkers = PG_GETARG_INT32(0);
	pg_allocator_type type = PG_GETARG_INT32(1);
	int32 nBlocks = PG_GETARG_INT32(2);
	int32 blockSize = PG_GETARG_INT32(3);

	debug("nWorkers=%d type=%d nBlocks=%d blockSize=%d\n", nWorkers, type, nBlocks, blockSize);
	validateArgs(nWorkers, type, nBlocks, blockSize);

	/* Our global totals may be off by an allocation allowance per worker */
	fudge = nWorkers * allocation_allowance_refill_qty * nWorkers;
	expected = nWorkers * nBlocks * blockSize;

	/* Set up pool of background workers */
	pool = createWorkerPool(nWorkers, sizeof(AllocateRequest) * 10, sizeof(ResponseData) * 10,
							"test_memtrack", workerFunction);

	/* Remember the total memory before we start allocations */
	starting_bkend_bytes = pg_atomic_read_u32((void *)&ProcGlobal->total_bkend_mem_bytes);

	/* Tell the workers to start their first batch of allocations */
	for (int w = 0; w < nWorkers; w++)
	    sendRequest(pool, w, ALLOCATE, type, nBlocks, blockSize);

	/* Get the workers response and verify all is good */
	for (int w = 0; w < nWorkers; w++)
	    processReply(pool, w, ALLOCATE, type, nBlocks, blockSize);

	/* Confirm the total backend memory is greater than what we just allocated */
	delta = pg_atomic_read_u32((void *)&ProcGlobal->total_bkend_mem_bytes) - starting_bkend_bytes;
	debug("starting_bkend_bytes=%zd  delta=%zd\n", starting_bkend_bytes, delta);
	if (delta < expected - fudge || delta > expected + fudge)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("Total allocated memory %zd doesn't match expected %zd  fudge=%zd\n", delta, expected, fudge)));

	/* Tell the workers to release their memory */
	for (int w=0; w < nWorkers; w++)
		sendRequest(pool, w, RELEASE, type, nBlocks, blockSize);

	/* Get the workers response and verify all is good */
	for (int w = 0; w < nWorkers; w++)
		processReply(pool, w, RELEASE, type, nBlocks, blockSize);

	/* Verify the new total is reasonable */
	delta = pg_atomic_read_u32((void *)&ProcGlobal->total_bkend_mem_bytes) - starting_bkend_bytes;
	debug("After release: delta=%zd  expected=%d\n", delta, 0);
	if (delta < -fudge || delta > fudge)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("Total allocated memory %zd is less than expected %zd\n", delta, 0l)));

	/* Clean up */
	freeWorkerPool(pool);
	debug("done\n");
	PG_RETURN_VOID();
}


static
void validateArgs(int nWorkers, pg_allocator_type type, int nBlocks, int blockSize)
{
	/* A negative blockcount is nonsensical. */
	if (nBlocks < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("repeat count size must be an integer value greater than or equal to zero")));

	/* a minimum of 1 worker is required. */
	if (nWorkers <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("number of workers must be an integer value greater than zero")));

	/* block size must be > 0 */
	if (blockSize <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("block size must be an integer value greater than zero")));

	/* Valid type? */
	if (type < 0 || type >= PG_ALLOC_TYPE_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid allocation type")));
}

static
void sendRequest(WorkerPool *pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize)
{
	AllocateRequest req;
	int result;
    debug("worker=%d action=%d type=%d nBlocks=%d blockSize=%d\n", worker, action, type, nBlocks, blockSize);

	req = (AllocateRequest) {.nBlocks = nBlocks, .action = action, .type = type, .blockSize = blockSize};

	result = sendToWorker(pool, worker, &req, sizeof(req));
	if (result != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("could not send message")));
}


void processReply(WorkerPool *pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize)
{
        int64 delta;
		ResponseData *resp;
		Size len;
		int result;
		debug("worker=%d action=%d type=%d nBlocks=%d blockSize=%d\n", worker, action, type, nBlocks, blockSize);

		/* Receive a message. Returns a pointer to the message and a length */
		result = recvFromWorker(pool, worker, (void *)&resp, &len);
		if (result != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not receive message")));

		if (resp->errorCode != 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("worker ran out of memory")));

		/* Verify the totals */
		delta = resp->memory.allocated_bytes_by_type[type] - resp->startingMemory.allocated_bytes_by_type[type];
		debug("delta=%ld expected=%ld\n", delta, (int64)nBlocks*blockSize);
		if (action == ALLOCATE && delta < nBlocks * blockSize)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("worker reported %ld bytes, expected %d", resp->memory.allocated_bytes_by_type[type], nBlocks * blockSize)));

		if (action == RELEASE && delta != 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("worker reported %ld bytes, expected 0", resp->memory.allocated_bytes_by_type[type])));
}



void test_memtrack_worker(Datum arg)
{
	AllocateRequest *req;
	Size reqSize;

	ResponseData resp[1];
	shm_mq_result result;

	PgBackendMemoryStatus startingMemory;

	debug("");
	workerInit(arg);

	/* Now that we're running, make note of how much memory has been already allocated */
	startingMemory = my_memory;

	do
	{
		result = workerRecv((void *)&req, &reqSize);
		debug("Received  result=%d  action=%d type=%d nBlocks=%d  blockSize=%ld\n", result, req->action, req->type, req->nBlocks, req->blockSize);
		if (result != SHM_MQ_SUCCESS)
			break;

		/* Allocate or release the blocks */
		if (req->action == ALLOCATE && reserveBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else if (req->action == RELEASE && releaseBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else
			resp->errorCode = 1;

		/* Get the current memory totals */
		resp->memory = my_memory;
		resp->startingMemory = startingMemory;
		debug("MyBEEntry=%p  allocated_bytes=%ld  type=%d\n", MyBEEntry, my_memory.allocated_bytes_by_type[req->type], req->type);

		/* Send the response */
		result = workerSend(resp, sizeof(resp[1]));
		debug("Send errorCode=%d  result=%d\n", resp->errorCode, result);

	} while (result == SHM_MQ_SUCCESS);

	workerExit(0);
}


bool reserveBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
    debug("type=%d nBlocks=%d blockSize=%d  prev=%zd\n", type, nBlocks, blockSize, my_memory.allocated_bytes_by_type[type]);
	/* Allocate the requested number of blocks */
	for (int i = 0; i < nBlocks; i++)
		if (!reserve_backend_memory(blockSize, type))
			return false;

	debug("success: allocated_bytes=%zd\n", my_memory.allocated_bytes_by_type[type]);
	return true;
}


bool releaseBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	debug("type=%d nBlocks=%d blockSize=%d\n", type, nBlocks, blockSize);
    for (int i = 0; i < nBlocks; i++)
	    release_backend_memory(blockSize, type);

	return true;
}



/***************
 * Worker which allocates and frees blocks,
 * but this one does actual allocations.
 */

/* Forward references for the allocation worker */
static bool freeBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static MemoryContext createTestContext(pg_allocator_type type);
static bool allocateBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static bool allocateDSMBlocks(int nBlocks, int blockSize);
static bool freeDSMBlocks(int nBlocks, int blockSize);
static bool allocateContextBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static bool freeContextBlocks(pg_allocator_type type, int nBlocks, int blockSize);


/* An array of pointers to the allocated blocks */
static void **allocations;
static MemoryContext testContext;


void test_allocation_worker(Datum arg)
{
	AllocateRequest *req;
	Size reqSize;

	ResponseData resp[1];
	shm_mq_result result;

	PgBackendMemoryStatus startingMemory;

	debug("\n");
	workerInit(arg);

	/* Now that we're running, make note of how much memory has been already allocated */
	startingMemory = my_memory;

	do
	{
		result = workerRecv((void *)&req, &reqSize);
		debug("Received  result=%d  action=%d type=%d nBlocks=%d  blockSize=%ld\n", result, req->action, req->type, req->nBlocks, req->blockSize);
		if (result != SHM_MQ_SUCCESS)
			break;

		/* Allocate or release the blocks */
		if (req->action == ALLOCATE && allocateBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else if (req->action == RELEASE && freeBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else
			resp->errorCode = 1;

		/* Get the current memory totals */
		resp->memory = my_memory;
		resp->startingMemory = startingMemory;
		debug("MyBEEntry=%p  allocated_bytes=%ld  type=%d\n", MyBEEntry, my_memory.allocated_bytes_by_type[req->type], req->type);

		/* Send the response */
		result = workerSend(resp, sizeof(resp[1]));

	} while (result == SHM_MQ_SUCCESS);

	workerExit(0);
}



static bool allocateBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	debug("type=%d nBlocks=%d blockSize=%d  prev=%zd\n", type, nBlocks, blockSize, my_memory.allocated_bytes_by_type[type]);
	if (type == PG_ALLOC_DSM)
		return allocateDSMBlocks(nBlocks, blockSize);
	else
		return allocateContextBlocks(type, nBlocks, blockSize);
}

static bool freeBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	debug("type=%d nBlocks=%d blockSize=%d\n", type, nBlocks, blockSize);
	if (type == PG_ALLOC_DSM)
		return freeDSMBlocks(nBlocks, blockSize);
	else
		return freeContextBlocks(type, nBlocks, blockSize);
}




static
MemoryContext createTestContext(pg_allocator_type type)
{
	switch (type)
	{
		case PG_ALLOC_ASET:
			return AllocSetContextCreate(TopMemoryContext, "test_aset", 64, 1024, 16 * 1024);
		case PG_ALLOC_SLAB:
			return SlabContextCreate(TopMemoryContext, "test_slab", 16 * 1024, 1024);
		case PG_ALLOC_GENERATION:
			return GenerationContextCreate(TopMemoryContext, "test_gen", 64, 1024, 16 * 1024);
		default: return NULL;

	}
}


static
bool allocateContextBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	MemoryContext old;

	debug("type=%d nBlocks=%d blockSize=%d  prev=%zd\n", type, nBlocks, blockSize, my_memory.allocated_bytes_by_type[type]);
	if (type == PG_ALLOC_DSM || type == PG_ALLOC_OTHER)
	    return false;

	/* Create a memory context for the allocations */
    testContext = createTestContext(type);
	if (testContext == NULL)
		return false;

	/* Switch to the new context */
	old = MemoryContextSwitchTo(testContext);

	/* Create a list of block pointers - not in the context */
	allocations = malloc(sizeof(void *) * nBlocks);
	if (allocations == NULL)
		return false;

	/* Allocate the requested number of blocks */
	for (int i = 0; i < nBlocks; i++)
	{
		allocations[i] = palloc(blockSize);
		if (allocations[i] == NULL)
			return false;
	}

	/* Switch back to the old context */
	MemoryContextSwitchTo(old);

	debug("success: allocated_bytes=%zd\n", my_memory.allocated_bytes_by_type[type]);
	return true;
	}


static
bool freeContextBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	debug("type=%d nBlocks=%d blockSize=%d\n", type, nBlocks, blockSize);
	for (int i = 0; i < nBlocks; i++)
		pfree(allocations[i]);

	MemoryContextDelete(testContext);
	free(allocations);
	allocations = NULL;

	return true;
}




static bool allocateDSMBlocks(int nBlocks, int blockSize)
{
	debug("nBlocks=%d blockSize=%d\n", nBlocks, blockSize);

	/* Create a list of block pointers - not in the context */
	allocations = malloc(sizeof(void *) * nBlocks);
	if (allocations == NULL)
		return false;

	for (int i = 0; i < nBlocks; i++)
	{
		allocations[i] = dsm_create(blockSize, 0);
		if (allocations[i] == NULL)
			return false;
		debug("segment=%p size=%ld\n", allocations[i], dsm_segment_map_length(allocations[i]));
	}

	return true;
}


static bool freeDSMBlocks(int nBlocks, int blockSize)
{
	debug("nBlocks=%d blockSize=%d\n", nBlocks, blockSize);

	for (int i = 0; i < nBlocks; i++)
		dsm_detach(allocations[i]);

	free(allocations);
	allocations = NULL;


	return true;
}
