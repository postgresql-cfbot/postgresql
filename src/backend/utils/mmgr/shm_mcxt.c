/*-------------------------------------------------------------------------
 *
 * shm_mcxt.c
 *	  ShmContext allocator definitions.
 *
 * ShmContext is a MemoryContext implementation designed for cases where you
 * want to allocate and free objects in the shared memory by MemoryContext
 * API (palloc/pfree).
 *
 * Portions Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/shm_mcxt.c
 *
 *
 * NOTE:
 *
 * ShmContext allows allocation in the shared memory via palloc().
 * This is intended to migrate locally allocated objects into shared memory.
 * These objects could be plancache, syscache or somthing else. They usually
 * allocate memory in local heap by palloc().  * To take advantage of exisiting
 * code, ShmContext uses dsa_allocate()/dsa_free() as palloc()/pfree().
 * However, dsa_allocate() returns dsa_pointer while palloc() returns native
 * pointer. And also an object may be a graph structure with pointers.
 * It needs to remember either native pointer or dsa_pointer.
 *
 * So allow the creation of DSA areas inside the traditional fixed memory
 * segment (instead of DSM), in a fixed-sized space reserved by the postmaster.
 * In this case, dsa_pointer is directly castable to a raw pointer, which is
 * common to every process. This fits to regular MemoryContext interface. But
 * note that the total size is fixed at start up time.
 *
 * Now we can put objects into shared memory via palloc(). But without
 * some garbage collection mechanism, memory leaks will be cluster-wide
 * and cluster-life-time. Leaked object won't go away even if one backend
 * exits.
 *
 * To address this issue, ShmContext has two types of context: "temporary" and
 * "permanent" one. "Temporary" context is located in local regular MemoryContext
 * and has buffer of dsa_pointers to dsa_allocated objects in order to free them
 * all at once at transaction rollback. Once developers think memory leak won't
 * happen, you can re-parent these temp objects to permanent context. Permanent
 * context exists in the shared memory.
 *
 * API to manipulate "temporary" and "permanent" context.
 * - CreatePermShmContext()
 * - CreateTempShmContext()
 * - ChangeToPermShmContext()
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/ilist.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"


/*
 * ShmContext is a specialized memory context supporting dsa area created
 * above Postmaster-initialized shared memory
 */
typedef struct ShmContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	/* ShmContext parameters */
	void	 *base;	/* raw address of Postmaster-initialized shared memory */
	dsa_area   *area; /* dsa area created-in-place  */
} ShmContext;



#define isTempShmContext(shm_context) (shm_context->temp_buffer != NULL)
#define isBufferFull(buf) (buf->idx == NUM_CHUNKS)
#define dsaptr_to_rawptr(dsa_p, base_p)	\
	((char *)(base_p) + dsa_p)
#define rawptr_to_dsaptr(raw_p, base_p)	\
	((dsa_pointer) ((char *)raw_p - (char *)base_p))



/*
 * These functions implement the MemoryContext API for ShmContext.
 */
static void *ShmContextAlloc(MemoryContext context, Size size);
static void ShmContextFree(MemoryContext context, void *pointer);
static void *ShmContextRealloc(MemoryContext context, void *pointer, Size size);
static void ShmContextReset(MemoryContext context);
static void ShmContextDelete(MemoryContext context);
static Size ShmContextGetChunkSpace(MemoryContext context, void *pointer);
static bool ShmContextIsEmpty(MemoryContext context);
static void ShmContextStats(MemoryContext context,
					  MemoryStatsPrintFunc printfunc, void *passthru,
					  MemoryContextCounters *totals);
#ifdef MEMORY_CONTEXT_CHECKING
static void ShmContextCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for ShmContext contexts.
 */
static const MemoryContextMethods ShmContextMethods = {
	ShmContextAlloc,
	ShmContextFree,
	ShmContextRealloc,
	ShmContextReset,
	ShmContextDelete,
	ShmContextGetChunkSpace,
	ShmContextIsEmpty,
	ShmContextStats
#ifdef MEMORY_CONTEXT_CHECKING
	,ShmContextCheck
#endif
};


/*
 * CreatePermShmContext
 *		Create a new permanent ShmContext context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * area: dsa_area created in place of Postmaster-initialized shared memory
 * base: address of Postmaster-initialized shared memory
 *
 * This context itself is allocated on shared memory.
 */
MemoryContext
CreatePermShmContext(MemoryContext parent,
					 const char *name,
					 dsa_area *area,
					 void *base)
{
	ShmContext *shmContext;
	bool found;

	shmContext = (ShmContext *)
		ShmemInitStruct(name, ShmContextSize(), &found);

	if (found)
		return &shmContext->header;

	MemoryContextCreate(&shmContext->header,
						T_ShmContext,
						&ShmContextMethods,
						parent,
						name);

	shmContext->base = base;
	shmContext->area = area;

	return &shmContext->header;
}

/*
 * ShmContextSize
 *		Size of ShmContext
 * If you use CreatePermShmContext, this fucntion should be called
 * to reserve the size of ShmContext at postgres start-up time
 */
Size
ShmContextSize(void)
{
	return sizeof(ShmContext);
}



/*
 * ShmContextReset
 * 		Free all the memory registered in temp_buffer and buffer
 *
 * The chunks registered in temp_buffer are dsa_freed.
 * This does not affect permanent context.
 *
 */
static void
ShmContextReset(MemoryContext context)
{
	ShmContext  *shmContext = (ShmContext *) context;

	Assert(shmContext);

	elog(ERROR,
		 "reset is not supported at permanent DSA MemoryContext");
}

/*
 * ShmContextDelete
 *  	Free all the memory registered in temp_buffer and context.
 *		See ShmContextReset.
 */
static void
ShmContextDelete(MemoryContext context)
{
	/* Reset to release all the temp_buffers */
	ShmContextReset(context);
	/* And free the context header */
	pfree(context);
}

/*
 * ShmContextAlloc
 *		Returns native pointer to allocated memory
 */
static void *
ShmContextAlloc(MemoryContext context, Size size)
{
	ShmContext *shmContext = (ShmContext *) context;
	dsa_pointer chunk_dp;
	char *chunk_backp;

	chunk_dp = dsa_allocate(shmContext->area, sizeof(void *) + size);

	chunk_backp = dsaptr_to_rawptr(chunk_dp, shmContext->base);

	*(void **) chunk_backp = context;

	return chunk_backp + sizeof(void *);
}


/*
 * ShmContextFree
 *		Frees allocated memory
 */
static void
ShmContextFree(MemoryContext context, void *pointer)
{
	ShmContext *shmContext = (ShmContext *) context;
	char *chunk_backp;
	dsa_pointer dp;

	/* Rewind to the secret start of the chunk */
	chunk_backp = (char *) pointer - sizeof(void *);
	dp = rawptr_to_dsaptr(chunk_backp, shmContext->base);

	dsa_free(shmContext->area, dp);
}

/*
 * ShmContextRealloc
 *
 *	realloc() is not supported
 */
static void *
ShmContextRealloc(MemoryContext context, void *pointer, Size size)
{
	elog(ERROR, "ShmContext does not support realloc()");
	return NULL;				/* keep compiler quiet */
}

/*
 * ShmContextGetChunkSpace
 */
static Size
ShmContextGetChunkSpace(MemoryContext context, void *pointer)
{
	elog(ERROR, "ShmContext does not support get_chunk_space()");
	return 0;				/* keep compiler quiet */
}

/*
 * ShmContextIsEmpty
 */
static bool
ShmContextIsEmpty(MemoryContext context)
{
	elog(ERROR, "ShmContext does not support is_empty()");
	return false;				/* keep compiler quiet */
}

/*
 * ShmContextStats
 *		Compute stats about memory consumption of a ShmContext context.
 */
static void
ShmContextStats(MemoryContext context,
		  MemoryStatsPrintFunc printfunc, void *passthru,
		  MemoryContextCounters *totals)
{
	elog(ERROR, "ShmContext does not support stats()");
}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * ShmContextCheck
 *
 * XXX: for now, do nothing
 */
static void
ShmContextCheck(MemoryContext context)
{

}

#endif							/* MEMORY_CONTEXT_CHECKING */
