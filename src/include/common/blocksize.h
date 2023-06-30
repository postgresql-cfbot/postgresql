/*-------------------------------------------------------------------------
 *
 * blocksize.h
 *	  defintions for cluster-specific limits/structure defs
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION: src/include/clustersizes.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BLOCKSIZE_H
#define BLOCKSIZE_H

#ifndef DEFAULT_BLOCK_SIZE
#define DEFAULT_BLOCK_SIZE 8192
#endif

#ifndef MIN_BLOCK_SIZE
#define MIN_BLOCK_SIZE 1024
#endif

#ifndef MAX_BLOCK_SIZE
#define MAX_BLOCK_SIZE 32*1024
#endif

#define IsValidBlockSize(size) ((size) >= MIN_BLOCK_SIZE && \
								(size) <= MAX_BLOCK_SIZE && \
								((size)&((size)-1)) == 0)

typedef enum ClusterSize {
    BS_BLOCK_SIZE = 0,
	BS_MAX_HEAP_TUPLES,
	BS_MAX_HEAP_TUPLES_PER_PAGE,
	BS_MAX_INDEX_TUPLES_PER_PAGE,
	BS_MAX_TIDS_PER_BTREE_PAGE,
	BS_TOAST_MAX_CHUNK_SIZE,
	BS_NUM_SIZES
} ClusterSize;

extern PGDLLIMPORT uint32 calculated_block_sizes[BS_NUM_SIZES];

void BlockSizeInit(Size rawblocksize);
#define GetBlockSize(theSize) (calculated_block_sizes[theSize])
#define CLUSTER_BLOCK_SIZE GetBlockSize(BS_BLOCK_SIZE)
#define CLUSTER_RELSEG_SIZE (RELSEG_SIZE * DEFAULT_BLOCK_SIZE / CLUSTER_BLOCK_SIZE)

/* Specific calculations' now parameterized sources */

/* originally in heaptoast.h */

#define CalcMaximumBytesPerTuple(blocksize,tuplesPerPage)	\
	MAXALIGN_DOWN((blocksize - \
				   MAXALIGN(SizeOfPageHeaderData + (tuplesPerPage) * sizeof(ItemIdData))) \
				  / (tuplesPerPage))

#define CalcToastMaxChunkSize(blocksize) \
	(CalcMaximumBytesPerTuple(blocksize,EXTERN_TUPLES_PER_PAGE) -	\
	 MAXALIGN(SizeofHeapTupleHeader) -					\
	 sizeof(Oid) -										\
	 sizeof(int32) -									\
	 VARHDRSZ)

/* originally in htup_details.h */

#define CalcMaxHeapTupleSize(size)  (size - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))

#define CalcMaxHeapTuplesPerPage(size)									\
	((int) (((size) - SizeOfPageHeaderData) /							\
			(MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))))

/* originally in itup.h */

#define CalcMaxIndexTuplesPerPage(size)		\
	((int) ((size - SizeOfPageHeaderData) / \
			(MAXALIGN(sizeof(IndexTupleData) + 1) + sizeof(ItemIdData))))

/* originally in nbtree_int.h */

#define CalcMaxTIDsPerBTreePage(size)									\
	(int) ((size - SizeOfPageHeaderData - sizeof(BTPageOpaqueData)) / \
		   sizeof(ItemPointerData))

/* originally in bloom.h */
#define CalcFreeBlockNumberElems(size) (MAXALIGN_DOWN(size - SizeOfPageHeaderData - MAXALIGN(sizeof(BloomPageOpaqueData)) \
													   - MAXALIGN(sizeof(uint16) * 2 + sizeof(uint32) + sizeof(BloomOptions)) \
									 ) / sizeof(BlockNumber))

#endif
