#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/mman.h>

#include "common/pg_lzcompress.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#ifdef USE_ZSTD
#include <zstd.h>

#define DEFAULT_ZSTD_COMPRESSION_LEVEL 1
#define MIN_ZSTD_COMPRESSION_LEVEL ZSTD_minCLevel()
#define MAX_ZSTD_COMPRESSION_LEVEL ZSTD_maxCLevel()
#endif


/**
 * compress_page_buffer_bound()
 *  Get the destination buffer boundary to compress one page.
 *
 *  Return needed destination buffer size for compress one page or
 *     -1 for unrecognized compression algorithm
 *
 */
int
compress_page_buffer_bound(uint8 algorithm)
{
	int			result = 0;

	switch (algorithm)
	{
		case PAGE_COMPRESSION_PGLZ:
			result = PGLZ_MAX_OUTPUT(BLCKSZ - SizeOfPageHeaderData);
			break;
#ifdef USE_LZ4
		case PAGE_COMPRESSION_LZ4:
			result = LZ4_compressBound(BLCKSZ - SizeOfPageHeaderData);
			break;
#endif
#ifdef USE_ZSTD
		case PAGE_COMPRESSION_ZSTD:
			result = ZSTD_compressBound(BLCKSZ - SizeOfPageHeaderData);
			break;
#endif
		default:
			return -1;
			break;
	}
	if (result < 0)
		return -1;

	return result + SizeOfPageCompressDataHeaderData;
}

/**
 * compress_page() -- Compress one page.
 *
 *	Only the parts other than the page header will be compressed.
 *	This function returns the size of compressed data or
 *	    -1 for compression fail
 *	    -2 for unrecognized compression algorithm
* 	note:The size of dst must be greater than or equal to "BLCKSZ + chunck_size".
 */
int
compress_page(const char *src, char *dst, int dst_size, uint8 algorithm, int8 level)
{
	int			compressed_size;
	PageCompressData *pcdata;

	pcdata = (PageCompressData *) dst;

	switch (algorithm)
	{
		case PAGE_COMPRESSION_PGLZ:
			compressed_size = pglz_compress(src + SizeOfPageHeaderData,
											BLCKSZ - SizeOfPageHeaderData,
											pcdata->data,
											PGLZ_strategy_always);
			break;

#ifdef USE_LZ4
		case PAGE_COMPRESSION_LZ4:
			{
				compressed_size = LZ4_compress_default(src + SizeOfPageHeaderData,
													   pcdata->data,
													   BLCKSZ - SizeOfPageHeaderData,
													   dst_size);
				if (compressed_size <= 0)
				{
					return -1;
				}
				break;
			}
#endif

#ifdef USE_ZSTD
		case PAGE_COMPRESSION_ZSTD:
			{
				int			real_level = level;

				if (level == 0)
					real_level = DEFAULT_ZSTD_COMPRESSION_LEVEL;
				else if (level < MIN_ZSTD_COMPRESSION_LEVEL)
					real_level = MIN_ZSTD_COMPRESSION_LEVEL;
				else if (level > MAX_ZSTD_COMPRESSION_LEVEL)
					real_level = MAX_ZSTD_COMPRESSION_LEVEL;

				compressed_size = ZSTD_compress(pcdata->data,
												dst_size,
												src + SizeOfPageHeaderData,
												BLCKSZ - SizeOfPageHeaderData,
												real_level);

				if (ZSTD_isError(compressed_size))
				{
					return -1;
				}
				break;
			}
#endif
		default:
			return -2;
			break;
	}

	if (compressed_size < 0)
		return -1;

	memcpy(pcdata->page_header, src, SizeOfPageHeaderData);
	pcdata->size = compressed_size;

	return SizeOfPageCompressDataHeaderData + compressed_size;
}

/**
 * decompress_page() -- Decompress one compressed page.
 *  Returns size of decompressed page which should be BLCKSZ or
 *         -1 for decompress error
 *         -2 for unrecognized compression algorithm
 *
 * 	note:The size of dst must be greater than or equal to BLCKSZ.
 */
int
decompress_page(const char *src, char *dst, uint8 algorithm)
{
	int			decompressed_size;
	PageCompressData *pcdata;

	pcdata = (PageCompressData *) src;

	memcpy(dst, src, SizeOfPageHeaderData);

	switch (algorithm)
	{
		case PAGE_COMPRESSION_PGLZ:
			decompressed_size = pglz_decompress(pcdata->data,
												pcdata->size,
												dst + SizeOfPageHeaderData,
												BLCKSZ - SizeOfPageHeaderData,
												false);
			if (decompressed_size < 0)
			{
				return -1;
			}
			break;

#ifdef USE_LZ4
		case PAGE_COMPRESSION_LZ4:
			decompressed_size = LZ4_decompress_safe(pcdata->data,
													dst + SizeOfPageHeaderData,
													pcdata->size,
													BLCKSZ - SizeOfPageHeaderData);

			if (decompressed_size < 0)
			{
				return -1;
			}

			break;
#endif

#ifdef USE_ZSTD
		case PAGE_COMPRESSION_ZSTD:
			decompressed_size = ZSTD_decompress(dst + SizeOfPageHeaderData,
												BLCKSZ - SizeOfPageHeaderData,
												pcdata->data,
												pcdata->size);

			if (ZSTD_isError(decompressed_size))
			{
				return -1;
			}

			break;
#endif

		default:
			return -2;
			break;

	}

	return SizeOfPageHeaderData + decompressed_size;
}


/**
 * pc_mmap() -- create memory map for address file of comressed relation.
 *
 */
PageCompressHeader *
pc_mmap(int fd, int chunk_size, bool readonly)
{
	PageCompressHeader *map;
	int			file_size,
				pc_memory_map_size;

	pc_memory_map_size = SizeofPageCompressAddrFile(chunk_size);

	file_size = lseek(fd, 0, SEEK_END);
	if (file_size != pc_memory_map_size)
	{
		if (ftruncate(fd, pc_memory_map_size) != 0)
			return (PageCompressHeader *) MAP_FAILED;
	}

#ifdef WIN32
	{
		HANDLE		mh;

		if (readonly)
			mh = CreateSnapshotMapping((HANDLE) _get_osfhandle(fd), NULL, PAGE_READONLY,
									   0, (DWORD) pc_memory_map_size, NULL);
		else
			mh = CreateSnapshotMapping((HANDLE) _get_osfhandle(fd), NULL, PAGE_READWRITE,
									   0, (DWORD) pc_memory_map_size, NULL);

		if (mh == NULL)
			return (PageCompressHeader *) MAP_FAILED;

		map = (PageCompressHeader *) MapViewOfFile(mh, FILE_MAP_ALL_ACCESS, 0, 0, 0);
		CloseHandle(mh);
	}
	if (map == NULL)
		return (PageCompressHeader *) MAP_FAILED;

#else
	if (readonly)
		map = (PageCompressHeader *) mmap(NULL, pc_memory_map_size, PROT_READ, MAP_SHARED, fd, 0);
	else
		map = (PageCompressHeader *) mmap(NULL, pc_memory_map_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
#endif
	return map;
}

/**
 * pc_munmap() -- release memory map of page compression address file.
 *
 */
int
pc_munmap(PageCompressHeader *map)
{
#ifdef WIN32
	return UnmapViewOfFile(map) ? 0 : -1;
#else
	return munmap(map, SizeofPageCompressAddrFile(map->chunk_size));
#endif
}

/**
 * pc_msync() -- sync memory map of page compression address file.
 *
 */
int
pc_msync(PageCompressHeader *map)
{
#ifndef FRONTEND
	if (!enableFsync)
		return 0;
#endif

#ifdef WIN32
	return FlushViewOfFile(map, SizeofPageCompressAddrFile(map->chunk_size)) ? 0 : -1;
#else
	return msync(map, SizeofPageCompressAddrFile(map->chunk_size), MS_SYNC);
#endif
}
