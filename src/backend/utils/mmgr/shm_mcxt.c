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


#define NUM_CHUNKS 128 /* an arbitrary number */

typedef struct dsa_temp_buffer dsa_temp_buffer;

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
	/* array of pointers to chunks. If ShmContext is permanent, NULL */
	dsa_temp_buffer *temp_buffer;
} ShmContext;

/*
 * dsa_temp_buffer
 * 		keeping dsa_pointer to chunks to free them at rollback 
 * 
 * Temporary ShmContext have this buffer while permanent one does not.
 * If buffer becomes full, next buffer is pushed to head.
 */
struct dsa_temp_buffer
{
	dsa_temp_buffer *next; /* single linked list */
	int idx;			/* index of array to be allocated */
	dsa_pointer chunks[NUM_CHUNKS]; /* relative address of chunks */
};


#define isTempShmContext(shm_context) (shm_context->temp_buffer != NULL)
#define isBufferFull(buf) (buf->idx == NUM_CHUNKS)
#define dsaptr_to_rawptr(dsa_p, base_p)	\
	((char *)(base_p) + dsa_p)
#define rawptr_to_dsaptr(raw_p, base_p)	\
	((dsa_pointer) ((char *)raw_p - (char *)base_p))

 
 /* Helper function for ShmContext API */
static inline MemoryContext
CreateShmContextInternal(MemoryContext parent,
						 const char *name,
						 dsa_area *area, 
						 void *base,
						 bool isTemp);
static Size ShmContextSize(void);
static void push_temp_buffer(dsa_temp_buffer *buffer);


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
	return CreateShmContextInternal(parent, name, area, base, false);
}

/* 
 * CreateTempShmContext
 *		Create temporary ShmContext in local heap by ShmContextCreate 
 * 
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * perm_context: permanent shared MemoryContext 
 *
 * Temp context inherits dsa_area and base address of permanent ShmContext.
 * This context itself is allocated on the parent context, which must not
 * be permanent ShmContext.
 * 
 */
MemoryContext
CreateTempShmContext(MemoryContext parent,
					 const char *name,
					 MemoryContext perm_context)
{
	ShmContext *shmContext;

	AssertArg(MemoryContextIsValid(perm_context));

	shmContext = (ShmContext *) perm_context;
	
	/* perm_context should be permanent one */
	Assert(!isTempShmContext(shmContext));

	return CreateShmContextInternal(parent, name,
				 shmContext->area, shmContext->base, true);
}


/*
 * CreateShmContextInternal
 *		Work-horse for CreatePermShmContext/CreateTempShmContext
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * area: dsa_area created in place of Postmaster-initialized shared memory
 * base: address of Postmaster-initialized shared memory
 * isTemp: context is temporary?
 *
 */
static inline MemoryContext
CreateShmContextInternal(MemoryContext parent,
						 const char *name,
						 dsa_area *area, 
						 void *base,
						 bool isTemp)
{
	ShmContext *shmContext;
	bool found;

	/* 
	 * If context is temp, allocate it and its buffer in parent context.
	 * If it is permanent, temp_buffer is not used.
	 */
	if (isTemp)
	{
   		MemoryContext old_context;
		
		if (!parent) 
			elog(ERROR, "Parent context of temporary shared context"
				 "is not specified");

   		old_context = MemoryContextSwitchTo(parent);

   		shmContext = palloc0(sizeof(ShmContext));
   		shmContext->temp_buffer = (dsa_temp_buffer *)
			palloc0(sizeof(dsa_temp_buffer));
		
		MemoryContextSwitchTo(old_context);
	}
	else
	{
		shmContext = (ShmContext *) 
			ShmemInitStruct(name, ShmContextSize(), &found);
	   	shmContext->temp_buffer = NULL;
	
		if (found)
			return &shmContext->header;
	}

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
 */
static Size
ShmContextSize(void)
{
	return sizeof(ShmContext);
}


/* 
 * ChangeToPermShmContext
 *
 * We don't want to leak memory in shared memory. Unlike local process, 
 * memory leak still exists even after local process is terminated.
 * If error occurs in transaction, we free all dsa_allocated chunks linked
 * from temporary ShmContext. When you make sure that the memory leak does
 * not happen, ChangeToPermShmContext should be called.
 *
 */
void
ChangeToPermShmContext(MemoryContext temp_context, MemoryContext perm_context)
{	
	ShmContext *temp_shm_context = (ShmContext *) temp_context;
	dsa_temp_buffer *buf = temp_shm_context->temp_buffer;
	int idx;

	/* change backpointer to shared MemoryContext */	
	while (buf && buf->chunks)
   	{
		dsa_temp_buffer *next_buf = buf->next;

		for (idx = 0; idx < buf->idx; idx++) 
		{
			/* Rewind to the secret start of the chunk */
			if (buf->chunks[idx] != 0)
				*(void **)(buf->chunks[idx] + (char *)temp_shm_context->base)
						   = perm_context;
		}
		/* initialize head of buffer */
		buf->idx = 0;
		buf = next_buf;
	}
}

/* 
 * push_temp_buffer
 * 		If temp_buffer becomes full, add new buffer
 */
static void
push_temp_buffer(dsa_temp_buffer *buffer)
{
	MemoryContext old_context;
	dsa_temp_buffer *new_buffer;

	/* choose the same context as current buffer to make lifetime consistent */
	old_context = MemoryContextSwitchTo(GetMemoryChunkContext(buffer));
	
	/* insert a new buffer into head position */
	new_buffer = (dsa_temp_buffer *) palloc0(sizeof(dsa_temp_buffer));
	new_buffer->next = buffer;
	buffer = new_buffer;
	
	MemoryContextSwitchTo(old_context);
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
	int			idx;
 	dsa_temp_buffer *buf;
	ShmContext  *shmContext = (ShmContext *) context;

	Assert(shmContext);

   	/* We don't support reset if context is permanent */
	if (!isTempShmContext(shmContext))
	   elog(ERROR, 
			"reset is not supported at permanent DSA MemoryContext");


	buf = shmContext->temp_buffer;

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	ShmContextCheck(context);
#endif


	/* free all chunks and buffers */
	while (buf && buf->chunks)
   	{
		dsa_temp_buffer *next_buf = buf->next;
		
		for (idx = 0; idx < buf->idx; idx++) 
		{
			/* chunks may be already freed */
			if (buf->chunks[idx] != 0)
				dsa_free(shmContext->area, buf->chunks[idx]);
		}

		pfree(buf);
		buf = next_buf;
	}
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
	
	char *chunk_backp;	
	dsa_temp_buffer *buf;

	/* we only allow palloc in temporary ShmContext */
	if (!isTempShmContext(shmContext))
	{
		elog(ERROR, "ShmContextAlloc should be run in "
			 "temporary ShmContext");
		return NULL;
	}

	/* if buffer is full, allocate a new buffer */
	if (isBufferFull(shmContext->temp_buffer))
		push_temp_buffer(shmContext->temp_buffer);

	/* Add space for the secret context pointer. */
	buf = shmContext->temp_buffer;
	buf->chunks[buf->idx] = dsa_allocate(shmContext->area, sizeof(void *) + size);
	
	chunk_backp = dsaptr_to_rawptr(buf->chunks[buf->idx], shmContext->base);
	buf->idx++;
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
	dsa_temp_buffer *buf;
	char *chunk_backp;
	dsa_pointer dp;
	int idx;

	/* Rewind to the secret start of the chunk */
	chunk_backp = (char *) pointer - sizeof(void *);
	dp = rawptr_to_dsaptr(chunk_backp, shmContext->base);

	dsa_free(shmContext->area, dp);

	/* if permananet, no need to delete its reference from temp_buffer */
	if (!isTempShmContext(shmContext))
		return;

	/* To avoid double free by ShmContextDelete, remove its reference */
	for (buf = shmContext->temp_buffer; buf != NULL; buf = buf->next)
	{
		for (idx = 0; idx < buf->idx; idx++) 
		{
			if (buf->chunks[idx] == dp)
			{
				buf->chunks[idx] = 0;
				break;
			}
		}
	}	
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
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
ShmContextGetChunkSpace(MemoryContext context, void *pointer)
{
	elog(ERROR, "ShmContext does not support get_chunk_space()");
	return 0;				/* keep compiler quiet */
}

/*
 * ShmContextIsEmpty
 *		Is an ShmContext empty of any allocated space?
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
 *
 * XXX: can dsa_dump be used?
 * printfunc: if not NULL, pass a human-readable stats string to this.
 * passthru: pass this pointer through to printfunc.
 * totals: if not NULL, add stats about this context into *totals.
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
