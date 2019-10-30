/*-------------------------------------------------------------------------
 *
 * shm_zone.c
 *	  ShmZoneContext allocator definitions.
 *
 * ShmZoneContext is a MemoryContext implementation designed for cases where
 * you want to allocate and free objects in the shared memory by MemoryContext
 * API (palloc/pfree). The main use case is global relation cache/plan cache.
 *
 * Portions Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/shm_zone.c
 *
 */

#include "postgres.h"

#include "lib/ilist.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"


#define ShmZone_BLOCKHDRSZ  MAXALIGN(sizeof(ShmZoneBlock))
#define ShmZone_CHUNKHDRSZ	sizeof(ShmZoneChunk)

typedef struct ShmZoneBlock ShmZoneBlock; /* forward reference */
typedef struct ShmZoneChunk ShmZoneChunk;


/*
 * ShmZoneContext is a specialized memory context supporting dsa area created
 * above Postmaster-initialized shared memory.
 * This context doesn't reuse allocated chunks and blocks, free chunks and
 * blocks all at once when context is destroyed.
 */
typedef struct ShmZoneContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	/* ShmZoneContext parameters */
	void	 *base;	/* raw address of Postmaster-initialized shared memory */
	dsa_area   *area; /* dsa area created-in-place	*/
	LWLock *lock; /* protect parent-child graph path */

	Size		blockSize;		/* standard block size */
	ShmZoneBlock *block; /* current (most recently allocated) block */
	dlist_head blocks; /* list of blocks */
} ShmZoneContext;


struct ShmZoneBlock
{
	dlist_node	node;			/* doubly-linked list of blocks */
	Size		blksize;		/* allocated size of this block */
	char	   *freeptr;		/* start of free space in this block */
	char	   *endptr;			/* end of space in this block */
};


struct ShmZoneChunk
{
	/* size is always the size of the usable space in the chunk */
	Size		size;
#define GENERATIONCHUNK_RAWSIZE  (SIZEOF_SIZE_T + SIZEOF_VOID_P)
	/* ensure proper alignment by adding padding if needed */
#if (GENERATIONCHUNK_RAWSIZE % MAXIMUM_ALIGNOF) != 0
	char		padding[MAXIMUM_ALIGNOF - GENERATIONCHUNK_RAWSIZE % MAXIMUM_ALIGNOF];
#endif

	ShmZoneContext *context; /* owning context, or NULL if freed chunk */
	/* there must not be any padding to reach a MAXALIGN boundary here! */
};



#define dsaptr_to_rawptr(dsa_p, base_p)	\
	((char *)(base_p) + dsa_p)
#define rawptr_to_dsaptr(raw_p, base_p)	\
	((dsa_pointer) ((char *)raw_p - (char *)base_p))


/*
 * ShmZoneIsValid
 *		True iff set is valid allocation set.
 */
#define ShmZoneIsValid(context) PointerIsValid(context)

#define ShmZonePointerGetChunk(ptr) \
	((ShmZoneChunk *)(((char *)(ptr)) - ShmZone_CHUNKHDRSZ))
#define ShmZoneChunkGetPointer(chk) \
	((ShmZoneChunk *)(((char *)(chk)) + ShmZone_CHUNKHDRSZ))


static inline MemoryContext
ShmZoneContextCreateInternal(MemoryContext parent, const char *name,
							 dsa_area *area, void *base, Size blockSize,
							 LWLock *lock, bool startup);

/*
 * These functions implement the MemoryContext API for ShmZoneContext.
 */
static void *ShmZoneContextAlloc(MemoryContext context, Size size);
static void ShmZoneContextFree(MemoryContext context, void *pointer);
static void *ShmZoneContextRealloc(MemoryContext context, void *pointer, Size size);
static void ShmZoneContextReset(MemoryContext context);
static void ShmZoneContextDelete(MemoryContext context);
static Size ShmZoneContextGetChunkSpace(MemoryContext context, void *pointer);
static bool ShmZoneContextIsEmpty(MemoryContext context);
static void ShmZoneContextStats(MemoryContext context,
					  MemoryStatsPrintFunc printfunc, void *passthru,
					  MemoryContextCounters *totals);
#ifdef MEMORY_CONTEXT_CHECKING
static void ShmZoneContextCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for ShmZoneContext contexts.
 */
static const MemoryContextMethods ShmZoneContextMethods = {
	ShmZoneContextAlloc,
	ShmZoneContextFree,
	ShmZoneContextRealloc,
	ShmZoneContextReset,
	ShmZoneContextDelete,
	ShmZoneContextGetChunkSpace,
	ShmZoneContextIsEmpty,
	ShmZoneContextStats
#ifdef MEMORY_CONTEXT_CHECKING
	,ShmZoneContextCheck
#endif
};


/*
 * ShmZoneContextCreateOrigin
 *		Create a new permanent ShmZoneContext context.
 *
 */
MemoryContext
ShmZoneContextCreateOrigin(MemoryContext parent,
						   const char *name,
						   dsa_area *area,
						   void *base,
						   LWLock *lock)
{
	/* XXX: use more appropriate size */
	return ShmZoneContextCreateInternal(parent, name, area, base,
										ShmZone_DEFAULT_BLOCK_SIZE,
										lock, true);
}

MemoryContext
ShmZoneContextCreateClone(MemoryContext parent,
						   const char *name,
						   MemoryContext origin)
{
	ShmZoneContext *shmContext = (ShmZoneContext *) origin;

	return ShmZoneContextCreateInternal(parent, name,
										shmContext->area, shmContext->base,
										ShmZone_DEFAULT_BLOCK_SIZE,
										shmContext->lock, false);
}

LWLock *
ShmZoneContextGetLock(MemoryContext context)
{
	ShmZoneContext *shmContext = (ShmZoneContext *) context;

	return shmContext->lock;
}

/*
 * ShmZoneContextCreateInternal
 *		Work-horse for ShmZoneContextCreateOrigin/ShmZoneContextCreateClone
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (must be statically allocated)
 * area: dsa_area created in place of Postmaster-initialized shared memory
 * base: address of Postmaster-initialized shared memory
 * isLocal: context is local?
 *
 */
static inline MemoryContext
ShmZoneContextCreateInternal(MemoryContext parent, const char *name,
							 dsa_area *area, void *base, Size blockSize,
							 LWLock *lock, bool startup)
{
	ShmZoneContext *shmContext;

	/*
	 * If it's Postgres start up time, allocate context object itself
	 * by ShmemInitstruct. Otherwise, use  ShmZoneContextAlloc via palloc.
	 */
	if (startup)
	{
		bool found;

		AssertArg(!parent);

		shmContext = (ShmZoneContext *)
			ShmemInitStruct(name, ShmZoneContextSize(), &found);

		if (found)
			return &shmContext->header;
	}
	else
	{
		/* Caller should switch to T_ShmZoneContext context */
		Assert(nodeTag(CurrentMemoryContext) == T_ShmZoneContext);
		AssertArg(parent);

		shmContext = palloc0(sizeof(ShmZoneContext));
	}

	shmContext->base = base;
	shmContext->area = area;
	shmContext->blockSize = blockSize;
	shmContext->block = NULL;
	dlist_init(&shmContext->blocks);
	shmContext->lock = lock;

	MemoryContextCreate(&shmContext->header,
						T_ShmZoneContext,
						&ShmZoneContextMethods,
						parent,
						name);

	return &shmContext->header;
}

/*
 * ShmZoneContextSize
 *		Size of ShmZoneContext
 * If you use ShmZoneContextCreateGlobal, this fucntion should be called
 * to reserve the size of ShmZoneContext at postgres start-up time
 */
Size
ShmZoneContextSize(void)
{
	return sizeof(ShmZoneContext);
}



/*
 * ShmZoneContextReset
 *		Free all the memory registered in chunk_list and list
 *
 * The chunks registered in chunk_list are dsa_freed.
 * This does not affect permanent context.
 *
 */
static void
ShmZoneContextReset(MemoryContext context)
{
	ShmZoneContext *shmContext = (ShmZoneContext *) context;
	dsa_pointer dsa_blockptr;
	dlist_mutable_iter miter;

	AssertArg(ShmZoneIsValid(shmContext));

	dlist_foreach_modify(miter, &shmContext->blocks)
	{
		ShmZoneBlock *block = dlist_container(ShmZoneBlock, node, miter.cur);
		dlist_delete(miter.cur);

		context->mem_allocated -= block->blksize;

		dsa_blockptr = rawptr_to_dsaptr(block, shmContext->base);

		dsa_free(shmContext->area, dsa_blockptr);
	}

	shmContext->block = NULL;

	Assert(dlist_is_empty(&shmContext->blocks));
}

/*
 * ShmZoneContextDelete
 *		Free all the memory registered in chunk_list and context.
 *		See ShmZoneContextReset.
 */
static void
ShmZoneContextDelete(MemoryContext context)
{
	/* Reset to release all the chunk_lists */
	ShmZoneContextReset(context);
	/* And free the context header */
	pfree(context);
}

/*
 * ShmZoneContextAlloc
 *		Returns native pointer to allocated memory
 * This function assumes that the parent of this context is located in
 * local memory. Since only the process which created the context can
 * allocate memory, acquiring a lock is not necessary here. After this
 * context is re-parent to shared context, no allocation is allowed.
 */
static void *
ShmZoneContextAlloc(MemoryContext context, Size size)
{
	ShmZoneContext *shmContext = (ShmZoneContext *) context;
	ShmZoneBlock *block;
	ShmZoneChunk *chunk;
	Size		chunk_size = MAXALIGN(size);

	if (chunk_size > shmContext->blockSize / 8)
	{
		dsa_pointer dsa_blockptr;
		Size blksize = chunk_size + ShmZone_BLOCKHDRSZ + ShmZone_CHUNKHDRSZ;

		dsa_blockptr = dsa_allocate(shmContext->area,
									sizeof(void *) + size);

		if (DsaPointerIsValid(dsa_blockptr))
			return NULL;

		context->mem_allocated += blksize;

		block = (ShmZoneBlock *) dsaptr_to_rawptr(dsa_blockptr, shmContext->base);

		block->blksize = blksize;

		block->freeptr = block->endptr = ((char *) block) + blksize;

		chunk = (ShmZoneChunk *) (((char *) block) + ShmZone_BLOCKHDRSZ);
		chunk->context = shmContext;
		chunk->size = chunk_size;

		/* add the block to the list of allocated blocks */
		dlist_push_head(&shmContext->blocks, &block->node);

		return ShmZoneChunkGetPointer(chunk);
	}

	block = shmContext->block;

	if ((block == NULL) ||
		(block->endptr - block->freeptr) < ShmZone_CHUNKHDRSZ + chunk_size)
	{
		dsa_pointer dsa_blockptr;
		Size blksize = shmContext->blockSize;

		dsa_blockptr = dsa_allocate(shmContext->area,
									sizeof(void *) + size);

		if (DsaPointerIsValid(dsa_blockptr))
			return NULL;

		context->mem_allocated += blksize;

		block = (ShmZoneBlock *) dsaptr_to_rawptr(dsa_blockptr, shmContext->base);

		block->blksize = blksize;

		block->freeptr = ((char *) block) + ShmZone_BLOCKHDRSZ;
		block->endptr = ((char *) block) + blksize;

		/* add it to the doubly-linked list of blocks */
		dlist_push_head(&shmContext->blocks, &block->node);

		/* and also use it as the current allocation block */
		shmContext->block = block;

	}

	Assert(block != NULL);
	Assert(block->endptr - block->freeptr >= ShmZone_CHUNKHDRSZ + chunk_size);

	chunk = (ShmZoneChunk *)block->freeptr;

	block->freeptr += (ShmZone_CHUNKHDRSZ + chunk_size);

	chunk->size = chunk_size;
	chunk->context = shmContext;

	return ShmZoneChunkGetPointer(chunk);
}


/*
 * ShmZoneContextFree
 *		Do nothing. Free chunks and blocks all at once when context
 *		is destroyed.
 */
static void
ShmZoneContextFree(MemoryContext context, void *pointer)
{
    exit(0);
}

/*
 * ShmZoneContextRealloc
 *
 *	realloc() is not supported
 */
static void *
ShmZoneContextRealloc(MemoryContext context, void *pointer, Size size)
{
	elog(ERROR, "ShmZoneContext does not support realloc()");
	return NULL;				/* keep compiler quiet */
}

/*
 * ShmZoneContextGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
ShmZoneContextGetChunkSpace(MemoryContext context, void *pointer)
{
	ShmZoneChunk *chunk = ShmZonePointerGetChunk(pointer);

	return chunk->size + ShmZone_CHUNKHDRSZ;
}

/*
 * ShmZoneContextIsEmpty
 *		Is an ShmZoneContext empty of any allocated space?
 */
static bool
ShmZoneContextIsEmpty(MemoryContext context)
{
	ShmZoneContext *shmContext = (ShmZoneContext *) context;

	return dlist_is_empty(&shmContext->blocks);
}

/*
 * ShmZoneContextStats
 *		Compute stats about memory consumption of a ShmZoneContext context.
 *
 * XXX: can dsa_dump be used?
 * printfunc: if not NULL, pass a human-readable stats string to this.
 * passthru: pass this pointer through to printfunc.
 * totals: if not NULL, add stats about this context into *totals.
 */
static void
ShmZoneContextStats(MemoryContext context,
		  MemoryStatsPrintFunc printfunc, void *passthru,
		  MemoryContextCounters *totals)
{
	elog(ERROR, "ShmZoneContext does not support stats()");
}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * ShmZoneContextCheck
 *
 * XXX: for now, do nothing
 */
static void
ShmZoneContextCheck(MemoryContext context)
{

}

#endif							/* MEMORY_CONTEXT_CHECKING */
