/*-------------------------------------------------------------------------
 *
 * pg_utf8.h
 *	  Routines for fast validation of UTF-8 text.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_utf8.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UTF8_H
#define PG_UTF8_H


#if defined(USE_SSE42_UTF8)
/* Use Intel SSE4.2 instructions. */
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8_sse42((s), (len))

extern int	pg_validate_utf8_sse42(const unsigned char *s, int len);

#elif defined(USE_SSE42_UTF8_WITH_RUNTIME_CHECK)
/*
 * Use Intel SSE 4.2 instructions, but perform a runtime check first
 * to check that they are available.
 */
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8((s), (len))

extern int	(*pg_validate_utf8) (const unsigned char *s, int len);
extern int	pg_validate_utf8_sse42(const unsigned char *s, int len);

#else
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8_fallback((s), (len))

#endif							/* USE_SSE42_UTF8 */

/* The following need to be visible everywhere. */

extern int	pg_validate_utf8_fallback(const unsigned char *s, int len);

#define IS_CONTINUATION_BYTE(c)	(((c) & 0xC0) == 0x80)
#define IS_TWO_BYTE_LEAD(c)		(((c) & 0xE0) == 0xC0)
#define IS_THREE_BYTE_LEAD(c)	(((c) & 0xF0) == 0xE0)
#define IS_FOUR_BYTE_LEAD(c)	(((c) & 0xF8) == 0xF0)

/* from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord */
#define HAS_ZERO(chunk) ( \
	((chunk) - UINT64CONST(0x0101010101010101)) & \
	 ~(chunk) & \
	 UINT64CONST(0x8080808080808080))

/* Verify a chunk of bytes for valid ASCII including a zero-byte check. */
static inline int
check_ascii(const unsigned char *s, int len)
{
	uint64		half1,
				half2,
				highbits_set;

	if (len >= 2 * sizeof(uint64))
	{
		memcpy(&half1, s, sizeof(uint64));
		memcpy(&half2, s + sizeof(uint64), sizeof(uint64));

		/* If there are zero bytes, bail and let the slow path handle it. */
		if (HAS_ZERO(half1) || HAS_ZERO(half2))
			return 0;

		/* Check if any bytes in this chunk have the high bit set. */
		highbits_set = ((half1 | half2) & UINT64CONST(0x8080808080808080));

		if (!highbits_set)
			return 2 * sizeof(uint64);
		else
			return 0;
	}
	else
		return 0;
}

#endif							/* PG_UTF8_H */
