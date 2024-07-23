/*-------------------------------------------------------------------------
 *
 * pg_crc32c_sse42_choose.c
 *	  Choose between Intel SSE 4.2 and software CRC-32C implementation.
 *
 * On first call, checks if the CPU we're running on supports Intel SSE
 * 4.2. If it does, use the special SSE instructions for CRC-32C
 * computation. Otherwise, fall back to the pure software implementation
 * (slicing-by-8).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_crc32c_sse42_choose.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/pg_crc32c.h"
#include "port/pg_hw_feat_check.h"

/*
 * This gets called on the first call. It replaces the function pointer
 * so that subsequent calls are routed directly to the chosen implementation.
 */
static pg_crc32c
pg_comp_crc32c_choose(pg_crc32c crc, const void *data, size_t len)
{
	if (pg_crc32c_sse42_available())
		pg_comp_crc32c = pg_comp_crc32c_sse42;
	else
		pg_comp_crc32c = pg_comp_crc32c_sb8;

	return pg_comp_crc32c(crc, data, len);
}

pg_crc32c (*pg_comp_crc32c) (pg_crc32c crc, const void *data, size_t len) = pg_comp_crc32c_choose;
