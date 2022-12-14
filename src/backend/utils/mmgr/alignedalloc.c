/*-------------------------------------------------------------------------
 *
 * alignedalloc.c
 *	  Allocator functions to implement palloc_aligned
 *
 * This is not a fully fledged MemoryContext type as there is no means to
 * create a MemoryContext of this type.  The code here only serves to allow
 * operations such as pfree() and repalloc() to work correctly on a memory
 * chunk that was allocated by palloc_aligned().
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/alignedalloc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memdebug.h"
#include "utils/memutils_memorychunk.h"

void
AlignedAllocFree(void *pointer)
{
	MemoryChunk *chunk = PointerGetMemoryChunk(pointer);
	void *unaligned;

#ifdef MEMORY_CONTEXT_CHECKING
	/*
	 * Test for someone scribbling on unused space in chunk.  We don't have
	 * the ability to include the context name here, so just mention that it's
	 * an aligned chunk.
	 */
	if (!sentinel_ok(pointer, chunk->requested_size))
		elog(WARNING, "detected write past %zu-byte aligned chunk end at %p",
			 MemoryChunkGetValue(chunk), chunk);
#endif

	Assert(!MemoryChunkIsExternal(chunk));

	/* obtain the original (unaligned) allocated pointer */
	unaligned = MemoryChunkGetBlock(chunk);

	pfree(unaligned);
}

void *
AlignedAllocRealloc(void *pointer, Size size)
{
	MemoryChunk	   *redirchunk = PointerGetMemoryChunk(pointer);
	Size		alignto = MemoryChunkGetValue(redirchunk);
	void	   *unaligned = MemoryChunkGetBlock(redirchunk);
	MemoryContext	ctx;
	Size			old_size;
	void		   *newptr;

	/* sanity check this is a power of 2 value */
	Assert((alignto & (alignto - 1)) == 0);

	/*
	 * Determine the size of the original allocation.  We can't determine this
	 * exactly as GetMemoryChunkSpace() returns the total space used for the
	 * allocation, which for contexts like aset includes rounding up to the
	 * next power of 2.  However, this value is just used to memcpy() the old
	 * data into the new allocation, so we only need to concern ourselves with
	 * not reading beyond the end of the original allocation's memory.  The
	 * drawback here is that we may copy more bytes than we need to, which
	 * amounts only to wasted effort.
	 */
#ifndef MEMORY_CONTEXT_CHECKING
	old_size = GetMemoryChunkSpace(unaligned) -
		((char *) pointer - (char *) PointerGetMemoryChunk(unaligned));
#else
	old_size = redirchunk->requested_size;
#endif

	ctx = GetMemoryChunkContext(unaligned);
	newptr = MemoryContextAllocAligned(ctx, size, alignto, 0);

	/*
	 * We may memcpy beyond the end of the orignal allocation request size, so
	 * we must mark the entire allocation as defined.
	 */
	VALGRIND_MAKE_MEM_DEFINED(pointer, old_size);
	memcpy(newptr, pointer, Min(size, old_size));
	pfree(unaligned);

	return newptr;
}

MemoryContext
AlignedAllocGetChunkContext(void *pointer)
{
	MemoryChunk *chunk = PointerGetMemoryChunk(pointer);

	Assert(!MemoryChunkIsExternal(chunk));

	return GetMemoryChunkContext(MemoryChunkGetBlock(chunk));
}

Size
AlignedGetChunkSpace(void *pointer)
{
	MemoryChunk	   *redirchunk = PointerGetMemoryChunk(pointer);
	void	   *unaligned = MemoryChunkGetBlock(redirchunk);

	return GetMemoryChunkSpace(unaligned);
}
