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

/* identifiers only */
typedef enum {
	BLOCK_SIZE_UNSET = 0,
	BLOCK_SIZE_1K,
	BLOCK_SIZE_2K,
	BLOCK_SIZE_4K,
	BLOCK_SIZE_8K,
	BLOCK_SIZE_16K,
	BLOCK_SIZE_32K,
} BlockSizeIdent;

extern PGDLLIMPORT BlockSizeIdent cluster_block_setting;

void BlockSizeInit(Size rawblocksize);
#define cluster_block_bits (cluster_block_setting+9)
#define cluster_block_size (1<<cluster_block_bits)
// TODO: make this calculate using use DEFAULT_BLOCK_SIZE instead?
#define DEFAULT_BLOCK_SIZE_BITS 13 
#define cluster_relseg_size (RELSEG_SIZE << DEFAULT_BLOCK_SIZE_BITS >> cluster_block_bits)

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

#define BlockSizeDecl(calc) \
	static inline unsigned int _block_size_##calc(BlockSizeIdent bsi) {	\
	switch(bsi){														\
	case BLOCK_SIZE_1K: return calc(1024); break;						\
	case BLOCK_SIZE_2K: return calc(2048); break;						\
	case BLOCK_SIZE_4K: return calc(4096); break;						\
	case BLOCK_SIZE_8K: return calc(8192); break;						\
	case BLOCK_SIZE_16K: return calc(16384); break;						\
	case BLOCK_SIZE_32K: return calc(32768); break;						\
	default: return 0;}}

#define BlockSizeDecl2(calc)											\
	static inline unsigned int _block_size_##calc(BlockSizeIdent bsi, unsigned int arg) { \
	switch(bsi){														\
	case BLOCK_SIZE_1K: return calc(1024,arg); break;						\
	case BLOCK_SIZE_2K: return calc(2048,arg); break;						\
	case BLOCK_SIZE_4K: return calc(4096,arg); break;						\
	case BLOCK_SIZE_8K: return calc(8192,arg); break;						\
	case BLOCK_SIZE_16K: return calc(16384,arg); break;					\
	case BLOCK_SIZE_32K: return calc(32768,arg); break;					\
	default: return 0;}}

#define BlockSizeCalc(bsi,calc) _block_size_##calc(bsi)


#endif
