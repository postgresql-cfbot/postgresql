/*-------------------------------------------------------------------------
 *
 * shm_retail.c
 *	  ShmRetailContext allocator definitions.
 *
 * ShmRetailContext is a MemoryContext implementation designed for cases where
 * you want to allocate and free objects in the shared memory by MemoryContext
 * API (palloc/pfree). The main use case is global system catalog cache.
 *
 * Portions Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/shm_retail.c
 *
 *
 * NOTE:
 *
 * ShmRetailContext allows allocation in the shared memory via palloc().
 * This is intended to migrate locally allocated objects into shared memory.
 * These objects could be syscache or somthing else. They usually
 * allocate memory in local heap by palloc(). To take advantage of exisiting
 * code, ShmRetailContext uses dsa_allocate()/dsa_free() as palloc()/pfree().
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
 * To address this issue, ShmRetailContext has two types of context: "local" and
 * "global" one. Local context itself is located in local memory but chunks
 * are allocated in shared memory. These chunks are pointed by chunk-list via
 * dsa_pointer in order to free them all at once at transaction rollback.
 * Once chunks are registerd to some index strucure in shared memory so that
 * memory leak won't happen, you can move chunks to global ShmRetailContext,
 * which is located in shared memory.
 *
 * Example of usage is found in:
 *	 src/test/modules/test_shm_retail
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/ilist.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"


/*
 * An arbitrary number for how many chunks is pointed by one chunk list
 * Use case like global system catalog cache calls palloc() in couple of
 * times in one memory context. So 16 would be enough.
 */
#define NUM_CHUNKS 16

typedef struct chunk_list chunk_list;

/*
 * ShmRetailContext is a specialized memory context supporting dsa area created
 * above Postmaster-initialized shared memory
 */
typedef struct ShmRetailContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	/* ShmRetailContext parameters */
	void	 *base;	/* raw address of Postmaster-initialized shared memory */
	dsa_area   *area; /* dsa area created-in-place	*/
	/* array of pointers to chunks. If ShmRetailContext is permanent, NULL */
	chunk_list *chunk_list;
} ShmRetailContext;

/*
 * chunk_list
 *		keeping dsa_pointer to chunks to free them at rollback
 *
 * Local ShmRetailContext have this list while permanent one does not.
 * If list becomes full, next list is pushed to head.
 */
struct chunk_list
{
	chunk_list *next; /* single linked list */
	int tail;			/* index of array to be allocated */
	dsa_pointer chunks[NUM_CHUNKS]; /* relative address of chunks */
};


#define isLocalShmRetailContext(shm_context) (shm_context->chunk_list != NULL)
#define isListFull(list) (list->tail == NUM_CHUNKS)
#define dsaptr_to_rawptr(dsa_p, base_p)	\
	((char *)(base_p) + dsa_p)
#define rawptr_to_dsaptr(raw_p, base_p)	\
	((dsa_pointer) ((char *)raw_p - (char *)base_p))


 /* Helper function for ShmRetailContext API */
static inline MemoryContext
ShmRetailContextCreateInternal(MemoryContext parent,
							   const char *name,
							   dsa_area *area,
							   void *base,
							   bool isLocal);
static void push_chunk_list(chunk_list *list);


/*
 * These functions implement the MemoryContext API for ShmRetailContext.
 */
static void *ShmRetailContextAlloc(MemoryContext context, Size size);
static void ShmRetailContextFree(MemoryContext context, void *pointer);
static void *ShmRetailContextRealloc(MemoryContext context, void *pointer, Size size);
static void ShmRetailContextReset(MemoryContext context);
static void ShmRetailContextDelete(MemoryContext context);
static Size ShmRetailContextGetChunkSpace(MemoryContext context, void *pointer);
static bool ShmRetailContextIsEmpty(MemoryContext context);
static void ShmRetailContextStats(MemoryContext context,
					  MemoryStatsPrintFunc printfunc, void *passthru,
					  MemoryContextCounters *totals);
#ifdef MEMORY_CONTEXT_CHECKING
static void ShmRetailContextCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for ShmRetailContext contexts.
 */
static const MemoryContextMethods ShmRetailContextMethods = {
	ShmRetailContextAlloc,
	ShmRetailContextFree,
	ShmRetailContextRealloc,
	ShmRetailContextReset,
	ShmRetailContextDelete,
	ShmRetailContextGetChunkSpace,
	ShmRetailContextIsEmpty,
	ShmRetailContextStats
#ifdef MEMORY_CONTEXT_CHECKING
	,ShmRetailContextCheck
#endif
};


/*
 * ShmRetailContextCreateGlobal
 *		Create a new permanent ShmRetailContext context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * area: dsa_area created in place of Postmaster-initialized shared memory
 * base: address of Postmaster-initialized shared memory
 *
 * This context itself is allocated on shared memory.
 */
MemoryContext
ShmRetailContextCreateGlobal(MemoryContext parent,
					 const char *name,
					 dsa_area *area,
					 void *base)
{
	return ShmRetailContextCreateInternal(parent, name, area, base, false);
}

/*
 * CreateTempShmRetailContext
 *		Create local ShmRetailContext in local heap by ShmRetailContextCreate
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * global_context: permanent shared MemoryContext
 *
 * Temp context inherits dsa_area and base address of permanent ShmRetailContext.
 * This context itself is allocated on the parent context, which must not
 * be permanent ShmRetailContext.
 *
 */
MemoryContext
ShmRetailContextCreateLocal(MemoryContext parent,
						   const char *name,
						   MemoryContext global_context)
{
	ShmRetailContext *shmContext;

	AssertArg(MemoryContextIsValid(global_context));

	shmContext = (ShmRetailContext *) global_context;

	/* global_context should be permanent one */
	Assert(!isLocalShmRetailContext(shmContext));

	return ShmRetailContextCreateInternal(parent, name,
				 shmContext->area, shmContext->base, true);
}


/*
 * ShmRetailContextCreateInternal
 *		Work-horse for ShmRetailContextCreateGlobal/ShmRetailContextCreateLocal
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * area: dsa_area created in place of Postmaster-initialized shared memory
 * base: address of Postmaster-initialized shared memory
 * isLocal: context is local?
 *
 */
static inline MemoryContext
ShmRetailContextCreateInternal(MemoryContext parent,
							   const char *name,
							   dsa_area *area,
							   void *base,
							   bool isLocal)
{
	ShmRetailContext *shmContext;
	bool found;

	/*
	 * If context is temp, allocate it and its list in parent context.
	 * If it is permanent, chunk_list is not used.
	 */
	if (isLocal)
	{
		MemoryContext old_context;

		if (!parent)
			elog(ERROR, "Parent context of local shared context"
				 "is not specified");

		old_context = MemoryContextSwitchTo(parent);

		shmContext = palloc0(sizeof(ShmRetailContext));
		shmContext->chunk_list = (chunk_list *)
			palloc0(sizeof(chunk_list));

		MemoryContextSwitchTo(old_context);
	}
	else
	{
		shmContext = (ShmRetailContext *)
			ShmemInitStruct(name, ShmRetailContextSize(), &found);
		shmContext->chunk_list = NULL;

		if (found)
			return &shmContext->header;
	}

	MemoryContextCreate(&shmContext->header,
						T_ShmRetailContext,
						&ShmRetailContextMethods,
						parent,
						name);

	shmContext->base = base;
	shmContext->area = area;

	return &shmContext->header;
}

/*
 * ShmRetailContextSize
 *		Size of ShmRetailContext
 * If you use ShmRetailContextCreateGlobal, this fucntion should be called
 * to reserve the size of ShmRetailContext at postgres start-up time
 */
Size
ShmRetailContextSize(void)
{
	return sizeof(ShmRetailContext);
}


/*
 * ShmRetailContextMoveChunk
 *
 * We don't want to leak memory in shared memory. Unlike local process,
 * memory leak still exists even after local process is terminated.
 * If error occurs in transaction, we free all dsa_allocated chunks linked
 * from local ShmRetailContext. When you make sure that the memory leak does
 * not happen, ShmRetailContextMoveChunk should be called.
 *
 */
void
ShmRetailContextMoveChunk(MemoryContext local_context, MemoryContext global_context)
{
	ShmRetailContext *localShmRetailContext = (ShmRetailContext *) local_context;
	chunk_list *list = localShmRetailContext->chunk_list;
	int idx;

	/* change backpointer to shared MemoryContext */
	while (list && list->chunks)
	{
		chunk_list *next_list = list->next;

		for (idx = 0; idx < list->tail; idx++)
		{
			/* Rewind to the secret start of the chunk */
			*(void **)(list->chunks[idx] + (char *)localShmRetailContext->base)
						   = global_context;
		}
		/* initialize tail of list */
		list->tail = 0;
		list = next_list;
	}
}

/*
 * push_chunk_list
 *		If chunk_list becomes full, add new list
 */
static void
push_chunk_list(chunk_list *list)
{
	MemoryContext old_context;
	chunk_list *new_list;

	/* choose the same context as current list to make lifetime consistent */
	old_context = MemoryContextSwitchTo(GetMemoryChunkContext(list));

	/* insert a new list into head position */
	new_list = (chunk_list *) palloc0(sizeof(chunk_list));
	new_list->next = list;
	list = new_list;

	MemoryContextSwitchTo(old_context);
}


/*
 * ShmRetailContextReset
 *		Free all the memory registered in chunk_list and list
 *
 * The chunks registered in chunk_list are dsa_freed.
 * This does not affect permanent context.
 *
 */
static void
ShmRetailContextReset(MemoryContext context)
{
	int			idx;
	chunk_list *list;
	ShmRetailContext  *shmContext = (ShmRetailContext *) context;

	Assert(shmContext);

	/* We don't support reset if context is permanent */
	if (!isLocalShmRetailContext(shmContext))
	   elog(ERROR,
			"reset is not supported at global ShmRetailContext");


	list = shmContext->chunk_list;

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	ShmRetailContextCheck(context);
#endif

	/* free all chunks and lists */
	while (list && list->chunks)
	{
		chunk_list *next_list = list->next;

		for (idx = 0; idx < list->tail; idx++)
			dsa_free(shmContext->area, list->chunks[idx]);

		pfree(list);
		list = next_list;
	}
}

/*
 * ShmRetailContextDelete
 *		Free all the memory registered in chunk_list and context.
 *		See ShmRetailContextReset.
 */
static void
ShmRetailContextDelete(MemoryContext context)
{
	/* Reset to release all the chunk_lists */
	ShmRetailContextReset(context);
	/* And free the context header */
	pfree(context);
}

/*
 * ShmRetailContextAlloc
 *		Returns native pointer to allocated memory
 */
static void *
ShmRetailContextAlloc(MemoryContext context, Size size)
{
	ShmRetailContext *shmContext = (ShmRetailContext *) context;

	char *chunk_backp;
	chunk_list *list;

	/* we only allow palloc in local ShmRetailContext */
	if (!isLocalShmRetailContext(shmContext))
	{
		elog(ERROR, "ShmRetailContextAlloc should be run in "
			 "local ShmRetailContext");
		return NULL;
	}

	/* if list is full, allocate a new list */
	if (isListFull(shmContext->chunk_list))
		push_chunk_list(shmContext->chunk_list);

	/* Add space for the secret context pointer. */
	list = shmContext->chunk_list;
	list->chunks[list->tail] = dsa_allocate(shmContext->area,
											sizeof(void *) + size);

	chunk_backp = dsaptr_to_rawptr(list->chunks[list->tail],
								   shmContext->base);

	*(void **) chunk_backp = context;
	list->tail++;

	return chunk_backp + sizeof(void *);
}


/*
 * ShmRetailContextFree
 *		Frees allocated memory
 */
static void
ShmRetailContextFree(MemoryContext context, void *pointer)
{
	ShmRetailContext *shmContext = (ShmRetailContext *) context;
	chunk_list *list;
	char *chunk_backp;
	dsa_pointer dp;
	int idx;

	/* Rewind to the secret start of the chunk */
	chunk_backp = (char *) pointer - sizeof(void *);

	dp = rawptr_to_dsaptr(chunk_backp, shmContext->base);

	dsa_free(shmContext->area, dp);

	/* If global, no need to delete its reference from chunk_list */
	if (!isLocalShmRetailContext(shmContext))
		return;

	/* To avoid double free by ShmRetailContextDelete, remove its reference */
	for (list = shmContext->chunk_list; list != NULL; list = list->next)
	{
		for (idx = 0; idx < list->tail; idx++)
		{
			if (list->chunks[idx] == dp)
			{
				if (idx != list->tail - 1)
					list->chunks[idx] = list->chunks[list->tail - 1];

				list->tail--;
				break;
			}
		}
	}
}

/*
 * ShmRetailContextRealloc
 *
 *	realloc() is not supported
 */
static void *
ShmRetailContextRealloc(MemoryContext context, void *pointer, Size size)
{
	elog(ERROR, "ShmRetailContext does not support realloc()");
	return NULL;				/* keep compiler quiet */
}

/*
 * ShmRetailContextGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
ShmRetailContextGetChunkSpace(MemoryContext context, void *pointer)
{
	elog(ERROR, "ShmRetailContext does not support get_chunk_space()");
	return 0;				/* keep compiler quiet */
}

/*
 * ShmRetailContextIsEmpty
 *		Is an ShmRetailContext empty of any allocated space?
 */
static bool
ShmRetailContextIsEmpty(MemoryContext context)
{
	elog(ERROR, "ShmRetailContext does not support is_empty()");
	return false;				/* keep compiler quiet */
}

/*
 * ShmRetailContextStats
 *		Compute stats about memory consumption of a ShmRetailContext context.
 *
 * XXX: can dsa_dump be used?
 * printfunc: if not NULL, pass a human-readable stats string to this.
 * passthru: pass this pointer through to printfunc.
 * totals: if not NULL, add stats about this context into *totals.
 */
static void
ShmRetailContextStats(MemoryContext context,
		  MemoryStatsPrintFunc printfunc, void *passthru,
		  MemoryContextCounters *totals)
{
	elog(ERROR, "ShmRetailContext does not support stats()");
}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * ShmRetailContextCheck
 *
 * XXX: for now, do nothing
 */
static void
ShmRetailContextCheck(MemoryContext context)
{

}

#endif							/* MEMORY_CONTEXT_CHECKING */
