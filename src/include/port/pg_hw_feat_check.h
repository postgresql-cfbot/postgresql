/*-------------------------------------------------------------------------
 *
 * pg_hw_feat_check.h
 *	  Miscellaneous functions for cheing for hardware features at runtime.
 *
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * src/include/port/pg_hw_feat_check.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_HW_FEAT_CHECK_H
#define PG_HW_FEAT_CHECK_H

/*
 * Test to see if all hardware features required by SSE 4.2 crc32c (64 bit)
 * are available.
 */
extern PGDLLIMPORT bool pg_crc32c_sse42_available(void);

/*
 * Test to see if all hardware features required by SSE 4.1 POPCNT (64 bit)
 * are available.
 */
extern PGDLLIMPORT bool pg_popcount_available(void);

/*
 * Test to see if all hardware features required by AVX-512 POPCNT are
 * available.
 */
extern PGDLLIMPORT bool pg_popcount_avx512_available(void);
#endif							/* PG_HW_FEAT_CHECK_H */
