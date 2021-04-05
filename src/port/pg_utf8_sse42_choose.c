/*-------------------------------------------------------------------------
 *
 * pg_utf8_sse42_choose.c
 *	  Choose between Intel SSE 4.2 and fallback implementation.
 *
 * On first call, checks if the CPU we're running on supports Intel SSE
 * 4.2. If it does, use SSE instructions for UTF-8 validation. Otherwise,
 * fall back to the pure C implementation.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_utf8_choose.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#ifdef HAVE__GET_CPUID
#include <cpuid.h>
#endif

#ifdef HAVE__CPUID
#include <intrin.h>
#endif

#include "port/pg_utf8.h"

static bool
pg_utf8_sse42_available(void)
{
	/* To save from checking every SSE2 intrinsic, insist on 64-bit. */
#ifdef __x86_64__
	unsigned int exx[4] = {0, 0, 0, 0};

#if defined(HAVE__GET_CPUID)
	__get_cpuid(1, &exx[0], &exx[1], &exx[2], &exx[3]);
#elif defined(HAVE__CPUID)
	__cpuid(exx, 1);
#else
#error cpuid instruction not available
#endif							/* HAVE__GET_CPUID */
	return (exx[2] & (1 << 20)) != 0;	/* SSE 4.2 */

#else
	return false;
#endif							/* __x86_64__ */
}

/*
 * This gets called on the first call. It replaces the function pointer
 * so that subsequent calls are routed directly to the chosen implementation.
 */
static int
pg_validate_utf8_choose(const unsigned char *s, int len)
{
	if (pg_utf8_sse42_available())
		pg_validate_utf8 = pg_validate_utf8_sse42;
	else
		pg_validate_utf8 = pg_validate_utf8_fallback;

	return pg_validate_utf8(s, len);
}

int	(*pg_validate_utf8) (const unsigned char *s, int len) = pg_validate_utf8_choose;
