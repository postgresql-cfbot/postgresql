/*-------------------------------------------------------------------------
 *
 * pg_popcount_avx512.c
 *	  Holds the pg_popcount() implementation that uses AVX512 instructions.
 *
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/pg_popcount_avx512.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include "port/pg_bitutils.h"

/*
 * XXX: Someday we should figure out how to determine whether this file needs
 * to comiled at configure-time instead of relying on macros that are
 * determined at compile-time.
 */
#ifdef TRY_POPCOUNT_AVX512

/*
 * Return true if CPUID indicates that the AVX512 POPCNT instruction is
 * available.
 */
bool
pg_popcount_avx512_available(void)
{
	unsigned int exx[4] = {0, 0, 0, 0};

#if defined(HAVE__GET_CPUID_COUNT)
	__get_cpuid_count(7, 0, &exx[0], &exx[1], &exx[2], &exx[3]);
#elif defined(HAVE__CPUIDEX)
	__cpuidex(exx, 7, 0);
#else
#error cpuid instruction not available
#endif

	if ((exx[1] & (1 << 16)) != 0 &&
		(exx[2] & (1 << 14)) != 0)
	{
		/*
		 * We also need to check that the OS has enabled support for the ZMM
		 * registers.
		 */
#ifdef _MSC_VER
		return (_xgetbv(0) & 0xe0) != 0;
#else
		uint64		xcr = 0;
		uint32		high;
		uint32		low;

__asm__ __volatile__(" xgetbv\n":"=a"(low), "=d"(high):"c"(xcr));
		return (low & 0xe0) != 0;
#endif
	}

	return false;
}

/*
 * pg_popcount_avx512
 *		Returns the number of 1-bits in buf
 */
uint64
pg_popcount_avx512(const char *buf, int bytes)
{
	uint64		popcnt;
	__m512i		accum = _mm512_setzero_si512();

	for (; bytes >= sizeof(__m512i); bytes -= sizeof(__m512i))
	{
		const		__m512i val = _mm512_loadu_si512((const __m512i *) buf);
		const		__m512i cnt = _mm512_popcnt_epi64(val);

		accum = _mm512_add_epi64(accum, cnt);

		buf += sizeof(__m512i);
	}

	popcnt = _mm512_reduce_add_epi64(accum);

	return popcnt + pg_popcount_fast(buf, bytes);
}

#endif							/* TRY_POPCOUNT_AVX512 */
