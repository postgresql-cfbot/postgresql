/*-------------------------------------------------------------------------
 *
 * blocksize.c
 *	  This file contains methods to calculate various size constants for variable-sized blocks.
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/clustersizes.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/nbtree_int.h"
#include "common/blocksize.h"
#ifndef FRONTEND
#include "storage/freespace.h"
#endif

PGDLLIMPORT uint32 calculated_block_sizes[BS_NUM_SIZES];

/*
 * This routine will calculate and cache the necessary constants. This should
 * be called once very very early in the process (as soon as the native block
 * size is known, so after reading ControlFile).
 */

void
BlockSizeInit(Size rawblocksize)
{
//	Assert(IsValidBlockSize(rawblocksize));

	calculated_block_sizes[BS_BLOCK_SIZE] = rawblocksize;
//	Assert(rawblocksize <= MAX_BLOCK_SIZE);
	calculated_block_sizes[BS_MAX_HEAP_TUPLES] = CalcMaxHeapTupleSize(rawblocksize);
//    Assert(calculated_block_sizes[BS_MAX_HEAP_TUPLES] <= MaxHeapTupleSizeLimit);
	calculated_block_sizes[BS_MAX_HEAP_TUPLES_PER_PAGE] = CalcMaxHeapTuplesPerPage(rawblocksize);
//    Assert(calculated_block_sizes[BS_MAX_HEAP_TUPLES_PER_PAGE] <= MaxHeapTuplesPerPageLimit);
	calculated_block_sizes[BS_MAX_INDEX_TUPLES_PER_PAGE] = CalcMaxIndexTuplesPerPage(rawblocksize);
//    Assert(calculated_block_sizes[BS_MAX_INDEX_TUPLES_PER_PAGE] <= MaxIndexTuplesPerPageLimit);
	calculated_block_sizes[BS_MAX_TIDS_PER_BTREE_PAGE] = CalcMaxTIDsPerBTreePage(rawblocksize);
//    Assert(calculated_block_sizes[BS_MAX_TIDS_PER_BTREE_PAGE] <= MaxTIDsPerBTreePageLimit);
	calculated_block_sizes[BS_TOAST_MAX_CHUNK_SIZE] = CalcToastMaxChunkSize(rawblocksize);
//    Assert(calculated_block_sizes[BS_TOAST_MAX_CHUNK_SIZE] <= TOAST_MAX_CHUNK_SIZE_LIMIT);

	#ifndef FRONTEND
	/* also setup the FreeSpaceMap internal sizing */
	FreeSpaceMapInit();
	#endif
}
