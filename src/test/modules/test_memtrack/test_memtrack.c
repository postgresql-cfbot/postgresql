/*--------------------------------------------------------------------------
 *
 * test_memstack.c
 *	Testing the memory tracking features.
 *	Creates a pool of workers which allocate/free memory,
 *	and verifies the totals are as expected.
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
#include "utils/memtrack.h"

#include "worker_pool.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_memtrack);
PG_FUNCTION_INFO_V1(test_allocation);

/* Message sent to the worker */
typedef enum
{
	ALLOCATE, RELEASE
}			Action;
typedef struct AllocateRequest
{
	Action		action;
	pg_allocator_type type;
	int32		nWorkers;
	int32		nBlocks;
	int64		blockSize;
}			AllocateRequest;

/* Message received back from the worker */
typedef struct ResponseData
{
	int32		errorCode;
	PgStat_Memory memory;
	PgStat_Memory startingMemory;
}			ResponseData;

/* Forward references */
static bool reserveBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static bool releaseBlocks(pg_allocator_type type, int nBlocks, int blockSize);
static void validateArgs(int nWorkers, pg_allocator_type type, int nBlocks, int blockSize);
static void sendRequest(WorkerPool * pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize);
static void processReply(WorkerPool * pool, int worker, Action actions, pg_allocator_type type, int nBlocks, int blockSize);
static Datum exercise_worker(FunctionCallInfo fcinfo, char *workerFunction);
static void checkAllocations();

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
	return exercise_worker(fcinfo, "test_memtrack_worker");
}


Datum
test_allocation(PG_FUNCTION_ARGS)
{
	return exercise_worker(fcinfo, "test_allocation_worker");
}



/*
 * Test the memory tracking features
 * Creates a pool of workers, issues requests, and verifies the results.
 */
static
Datum
exercise_worker(FunctionCallInfo fcinfo, char *workerFunction)
{
	WorkerPool	pool[1];
	int64		delta,
				starting_bkend_bytes;
	int64		expected,
				fudge;
	PgStatShared_Memtrack *global = &pgStatLocal.shmem->memtrack;

	/* Get  the SQL function arguments */
	int32		nWorkers = PG_GETARG_INT32(0);
	pg_allocator_type type = PG_GETARG_INT32(1);
	int32		nBlocks = PG_GETARG_INT32(2);
	int32		blockSize = PG_GETARG_INT32(3);

	validateArgs(nWorkers, type, nBlocks, blockSize);

	/* Our global totals may be off by an allocation allowance per worker */
	fudge = nWorkers * allocation_allowance_refill_qty * nWorkers;
	expected = nWorkers * nBlocks * blockSize;

	/* Set up pool of background workers */
	initWorkerPool(pool, nWorkers, sizeof(AllocateRequest) * 10, sizeof(ResponseData) * 10,
							"test_memtrack", workerFunction);

	/* Remember the total memory before we start allocations */
	starting_bkend_bytes = pg_atomic_read_u32((void *) &global->total_memory_reserved);

	/* Tell the workers to start their first batch of allocations */
	for (int w = 0; w < nWorkers; w++)
		sendRequest(pool, w, ALLOCATE, type, nBlocks, blockSize);

	/* Get the workers response and verify all is good */
	for (int w = 0; w < nWorkers; w++)
		processReply(pool, w, ALLOCATE, type, nBlocks, blockSize);

	/* Confirm the total backend memory is greater than what we just allocated */
	delta = pg_atomic_read_u32((void *) &global->total_memory_reserved) - starting_bkend_bytes;
	if (delta < expected - fudge || delta > expected + fudge)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Total allocated memory %zd doesn't match expected %zd  fudge=%zd\n", delta, expected, fudge)));

	/* Tell the workers to release their memory */
	for (int w = 0; w < nWorkers; w++)
		sendRequest(pool, w, RELEASE, type, nBlocks, blockSize);

	/* Get the workers response and verify all is good */
	for (int w = 0; w < nWorkers; w++)
		processReply(pool, w, RELEASE, type, nBlocks, blockSize);

	/* Verify the new total is reasonable */
	delta = pg_atomic_read_u32((void *) &global->total_memory_reserved) - starting_bkend_bytes;
	if (delta < -fudge || delta > fudge)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Total allocated memory %zd is less than expected %d\n", delta, 0)));

	/* Clean up */
	freeWorkerPool(pool);
	PG_RETURN_VOID();
}


/*
 * Verify the arguments passed to the SQL function are valid.
 */
static
void
validateArgs(int nWorkers, pg_allocator_type type, int nBlocks, int blockSize)
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

/*
 * Send a request message to a worker.
 */
static
void
sendRequest(WorkerPool * pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize)
{
	AllocateRequest req;
	int			result;

	req = (AllocateRequest)
	{
		.nBlocks = nBlocks,.action = action,.type = type,.blockSize = blockSize
	};

	result = sendToWorker(pool, worker, &req, sizeof(req));
	if (result != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not send message")));
}


/*
 * Receive a reply from a worker and verify the results make sense.
 */
void
processReply(WorkerPool * pool, int worker, Action action, pg_allocator_type type, int nBlocks, int blockSize)
{
	int64		delta;
	ResponseData *rp;
	ResponseData resp[1];
	Size		len;
	int			result;

	/* Receive a message. Returns a pointer to the message and a length */
	result = recvFromWorker(pool, worker, (void *) &rp, &len);
	if (result != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not receive message")));

	/* Copy response message in case the buffer isn't aligned */
	memcpy(resp, rp, sizeof(*resp));

	/* Check for outof memory error */
	if (resp->errorCode != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("worker ran out of memory")));

	/* Verify the totals */
	delta = resp->memory.subTotal[type] - resp->startingMemory.subTotal[type];
	if (action == ALLOCATE && delta < nBlocks * blockSize / 2)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("worker(allocate) increased %zd bytes, expected %d", delta, nBlocks * blockSize)));

	if (action == RELEASE && delta > -nBlocks*blockSize / 2)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("worker(release) decreased %zd bytes, expected %d", -delta, -nBlocks * blockSize)));
}


/*
 * Worker which bumps the memtrack counters without actually allocating anything.
 */
void
test_memtrack_worker(Datum arg)
{
	AllocateRequest *req;
	Size		reqSize;

	ResponseData resp[1];
	shm_mq_result result;

	workerInit(arg);

	/*
	 * Now that we're running, make note of how much memory has been already
	 * allocated
	 */

	do
	{

		result = workerRecv((void *) &req, &reqSize);
		if (result != SHM_MQ_SUCCESS)
			break;

		resp->startingMemory = my_memory;

		/* Allocate or release the blocks */
		if (req->action == ALLOCATE && reserveBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else if (req->action == RELEASE && releaseBlocks(req->type, req->nBlocks, req->blockSize))
			resp->errorCode = 0;
		else
			resp->errorCode = 1;

		/* Get the current memory totals */
		resp->memory = my_memory;

		/* Send the response */
		result = workerSend(resp, sizeof(resp[1]));

	} while (result == SHM_MQ_SUCCESS);

	workerExit(0);
}


bool
reserveBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	/* Allocate the requested number of blocks */
	for (int i = 0; i < nBlocks; i++)
		if (!reserve_tracked_memory(blockSize, type))
			return false;

	return true;
}


bool
releaseBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	for (int i = 0; i < nBlocks; i++)
		release_tracked_memory(blockSize, type);

	return true;
}

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


/*
 * Worker which actually allocates and releases memory.
 */
void
test_allocation_worker(Datum arg)
{
	AllocateRequest *req;
	Size		reqSize;

	ResponseData resp[1];
	shm_mq_result result;

	PgStat_Memory startingMemory;

	workerInit(arg);

	do
	{
		result = workerRecv((void *) &req, &reqSize);
		if (result != SHM_MQ_SUCCESS)
			break;

		startingMemory = my_memory;

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

		/* Send the response */
		result = workerSend(resp, sizeof(resp[1]));

		checkAllocations();

	} while (result == SHM_MQ_SUCCESS);

	workerExit(0);
}



static bool
allocateBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	if (type == PG_ALLOC_DSM)
		return allocateDSMBlocks(nBlocks, blockSize);
	else
		return allocateContextBlocks(type, nBlocks, blockSize);
}

static bool
freeBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	if (type == PG_ALLOC_DSM)
		return freeDSMBlocks(nBlocks, blockSize);
	else
		return freeContextBlocks(type, nBlocks, blockSize);
}



/*
 * Create a memory context for the non-DSM memory allocations.
 */
static
MemoryContext
createTestContext(pg_allocator_type type)
{
	switch (type)
	{
		case PG_ALLOC_ASET:
			return AllocSetContextCreate(TopMemoryContext, "test_aset", 0, 1024, 16 * 1024);
		case PG_ALLOC_SLAB:
			return SlabContextCreate(TopMemoryContext, "test_slab", 16 * 1024, 1024);
		case PG_ALLOC_GENERATION:
			return GenerationContextCreate(TopMemoryContext, "test_gen", 0, 1024, 16 * 1024);
		default:
			return NULL;

	}
}


/*
 * Allocate blocks of memory from a non-DSM context.
 */
static
bool
allocateContextBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	MemoryContext old;

	if (type == PG_ALLOC_DSM || type == PG_ALLOC_INIT)
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

	return true;
}


/*
 * Free blocks of memory allocated earlier
 */
static
bool
freeContextBlocks(pg_allocator_type type, int nBlocks, int blockSize)
{
	for (int i = 0; i < nBlocks; i++)
		pfree(allocations[i]);

	MemoryContextDelete(testContext);
	free(allocations);
	allocations = NULL;

	return true;
}


/*
 * Allocate blocks of memory from DSM
 */
static bool
allocateDSMBlocks(int nBlocks, int blockSize)
{

	/* Create a list of block pointers - not in the context */
	allocations = malloc(sizeof(void *) * nBlocks);
	if (allocations == NULL)
		return false;

	for (int i = 0; i < nBlocks; i++)
	{
		allocations[i] = dsm_create(blockSize, 0);
		if (allocations[i] == NULL)
			return false;
	}

	return true;
}


/*
 * Free blocks of DSM memory allocated earlier
 */
static bool
freeDSMBlocks(int nBlocks, int blockSize)
{
	for (int i = 0; i < nBlocks; i++)
		dsm_detach(allocations[i]);

	free(allocations);
	allocations = NULL;


	return true;
}


/*
 * Compare top context allocations with our private memory numbers.
 * They should be the same.
 */
static void
checkAllocations()
{
	int64 my_private = my_memory.total - my_memory.subTotal[PG_ALLOC_INIT] - my_memory.subTotal[PG_ALLOC_DSM];
	int64 context = getContextMemoryTotal();
	Assert(my_private == context);
}
