/*
 * page_compression.h
 *		internal declarations for page compression
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/storage/page_compression.h
 */

#ifndef PAGE_COMPRESSION_H
#define PAGE_COMPRESSION_H

#include <sys/mman.h>

#include "storage/bufpage.h"
#include "datatype/timestamp.h"

#ifdef FRONTEND
typedef uint32 pg_atomic_uint32;
#else
#include "port/atomics.h"

/* The page compression feature relies on native atomic operation support.
 * On platforms that do not support native atomic operations, the members
 * of pg_atomic_uint32 contain semaphore objects, which will affect the
 * persistence of compressed page address files.
 */
#define SUPPORT_PAGE_COMPRESSION (sizeof(pg_atomic_uint32) == sizeof(uint32))
#endif

/* In order to avoid the inconsistency of address metadata when the server
 * is crash, it is necessary to prevent the address metadata of one data block
 * from crossing two storage device blocks. The block size of ordinary storage
 * devices is a multiple of 512, so 512 is used as the block size of the
 * page compression address file.
 */
#define COMPRESS_ADDR_BLCKSZ 512

typedef uint32 pc_chunk_number_t;

/* Compression algorithms for page compression */
typedef enum PageCompression
{
	PAGE_COMPRESSION_NONE = 0,
	PAGE_COMPRESSION_PGLZ,
	PAGE_COMPRESSION_LZ4,
	PAGE_COMPRESSION_ZSTD
} PageCompression;

/*
 * Layout of files for page compression:
 *
 * 1. page compression address file(_pca)
 * - PageCompressHeader
 * - PageCompressAddr[0]
 * - PageCompressAddr[1]
 * - ...
 *
 * 2. page compression data file(_pcd)
 * - PageCompressChunk[0]
 *   - PageCompressData(in whole or in part)
 * - PageCompressChunk[1]
 *   - PageCompressData(in whole or in part)
 * - ...
 */

typedef struct PageCompressHeader
{
	pg_atomic_uint32 nblocks;	/* number of total blocks in this segment */
	pg_atomic_uint32 allocated_chunks;	/* number of total allocated chunks in
										 * data area */
	uint16		chunk_size;		/* size of chunk, must be 1/2, 1/4, 1/8 or
								 * 1/16 of BLCKSZ */
	uint8		algorithm;		/* compress algorithm, 1=pglz, 2=lz4 3=zstd */
	pg_atomic_uint32 last_synced_nblocks;	/* last synced nblocks */
	pg_atomic_uint32 last_synced_allocated_chunks;	/* last synced
													 * allocated_chunks */
	TimestampTz last_recovery_start_time;	/* postmaster start time of last
											 * recovery */
} PageCompressHeader;

typedef struct PageCompressAddr
{
	volatile uint8 nchunks;		/* number of chunks for this block */
	volatile uint8 allocated_chunks;	/* number of allocated chunks for this
										 * block */

	/*
	 * variable-length fields, 1 based chunk number array for this block, size
	 * of the array is "BLCKSZ/chunk_size + 1"
	 */
	pc_chunk_number_t chunknos[FLEXIBLE_ARRAY_MEMBER];
} PageCompressAddr;

typedef struct PageCompressChunk
{
	uint32		blockno;		/* block number of the block stored in this
								 * chunk */
	uint8		chunkseq;		/* index of this chunk, from 1 to
								 * allocated_chunks */
	uint8		withdata:1,		/* if has data */
				unused:7;
	uint16		checksum;		/* checksum of this chunk */
	char		data[FLEXIBLE_ARRAY_MEMBER];	/* 1/nchunks of
												 * PageCompressData , and at
												 * least 4-byte aligned */
} PageCompressChunk;

typedef struct PageCompressData
{
	char		page_header[SizeOfPageHeaderData];	/* page header */
	uint16		size;			/* size of data */
	uint16		unused;
	char		data[FLEXIBLE_ARRAY_MEMBER];	/* data of compressed page,
												 * except for the page header */
} PageCompressData;

/*
 * Use this, not "char buf[BLCKSZ + BLCKSZ/2]", to declare a field or local
 * variable holding a buffer for all chunks for one compressed page, if that buffer
 * need to be aligned, such as when calculating checksum. (In some places, we use
 * this to declare buffers even though we only pass them to read() and
 * write(), because copying to/from aligned buffers is usually faster than
 * using unaligned buffers.)  We include both "double" and "int64" in the
 * union to ensure that the compiler knows the value must be MAXALIGN'ed
 * (cf. configure's computation of MAXIMUM_ALIGNOF).
 */
typedef union PGAlignedCompressChunks
{
	char		data[BLCKSZ + BLCKSZ / 2];
	double		force_align_d;
	int64		force_align_i64;
}			PGAlignedCompressChunks;

#define IsValidPageCompressChunkSize(chunk_size) \
	((chunk_size) == BLCKSZ / 2 || \
	 (chunk_size) == BLCKSZ / 4 || \
	 (chunk_size) == BLCKSZ / 8 || \
	 (chunk_size) == BLCKSZ / 16)

#define SizeOfPageCompressHeaderData sizeof(PageCompressHeader)
#define SizeOfPageCompressAddrHeaderData offsetof(PageCompressAddr, chunknos)
#define SizeOfPageCompressChunkHeaderData offsetof(PageCompressChunk, data)
#define SizeOfPageCompressDataHeaderData offsetof(PageCompressData, data)

#define MaxChunksPreCompressedPage(chunk_size) \
	(BLCKSZ / (chunk_size) + 1)

#define SizeOfPageCompressAddr(chunk_size) \
	(SizeOfPageCompressAddrHeaderData + sizeof(pc_chunk_number_t) * MaxChunksPreCompressedPage(chunk_size))

#define NumberOfPageCompressAddrPerBlock(chunk_size) \
	(COMPRESS_ADDR_BLCKSZ / SizeOfPageCompressAddr(chunk_size))

#define OffsetOfPageCompressAddr(chunk_size, blockno) \
	(COMPRESS_ADDR_BLCKSZ * (1 + (blockno) / NumberOfPageCompressAddrPerBlock(chunk_size)) \
	+ SizeOfPageCompressAddr(chunk_size) * ((blockno) % NumberOfPageCompressAddrPerBlock(chunk_size)))

#define GetPageCompressAddr(pcbuffer, chunk_size, blockno) \
	(PageCompressAddr *)((char *)(pcbuffer) + OffsetOfPageCompressAddr((chunk_size),(blockno) % RELSEG_SIZE))

#define SizeofPageCompressAddrFile(chunk_size) \
	OffsetOfPageCompressAddr((chunk_size), RELSEG_SIZE)

#define OffsetOfPageCompressChunk(chunk_size, chunkno) \
	((chunk_size) * ((chunkno) - 1))

#define StoreCapacityPerPageCompressChunk(chunk_size) \
	(chunk_size - SizeOfPageCompressChunkHeaderData)

#define NeedPageCompressChunksToStoreData(chunk_size, data_size) \
	((data_size + StoreCapacityPerPageCompressChunk(chunk_size) - 1) / StoreCapacityPerPageCompressChunk(chunk_size))


#define MAX_PAGE_COMPRESS_ADDRESS_FILE_SIZE SizeofPageCompressAddrFile(BLCKSZ / 16)

#define MAX_PAGE_COMPRESS_CHUNK_NUMBER(chunk_size) \
	(RELSEG_SIZE * MaxChunksPreCompressedPage(chunk_size))

/*
 * After allocated 32MB data space, sync the page compression address file.
 */
#define COMPRESS_ADDRESS_SYNC_THRESHOLD(chunk_size) (32 * 1024 * 1024 / (chunk_size))

/* Compress function */
extern int	compress_page_buffer_bound(uint8 algorithm);
extern int	compress_page(const char *src, char *dst, int dst_size, uint8 algorithm, int8 level);
extern int	decompress_page(const char *src, char *dst, uint8 algorithm);

/* Memory mapping function */
extern PageCompressHeader *pc_mmap(int fd, int chunk_size, bool readonly);
extern int	pc_munmap(PageCompressHeader *map);
extern int	pc_msync(PageCompressHeader *map);


#ifndef FRONTEND
int			errcode_for_dynamic_shared_memory(void);
void		check_and_repair_compress_address(PageCompressHeader *pcmap, uint16 chunk_size, uint8 algorithm,
											  const char *path, int fd_pcd, const char *path_pcd);
#endif

#endif							/* PAGE_COMPRESSION_H */
