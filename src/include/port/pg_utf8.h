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
/* Use SSE 4.2 instructions. */
#define UTF8_VERIFYSTR_FAST(s, len) \
	pg_validate_utf8_sse42((s), (len))

extern int	pg_validate_utf8_sse42(const unsigned char *s, int len);

#elif defined(USE_SSE42_UTF8_WITH_RUNTIME_CHECK)
/* Use SSE 4.2 instructions, but perform a runtime check first. */
#define UTF8_VERIFYSTR_FAST(s, len) \
	pg_validate_utf8((s), (len))

extern int	pg_validate_utf8_fallback(const unsigned char *s, int len);
extern int	(*pg_validate_utf8) (const unsigned char *s, int len);
extern int	pg_validate_utf8_sse42(const unsigned char *s, int len);

#else
/* Use a portable implementation */
#define UTF8_VERIFYSTR_FAST(s, len) \
	pg_validate_utf8_fallback((s), (len))

extern int	pg_validate_utf8_fallback(const unsigned char *s, int len);

#endif							/* USE_SSE42_UTF8 */

/* The following are visible everywhere. */

/*
 * Verify a chunk of bytes for valid ASCII including a zero-byte check.
 * This is here in case non-UTF8 encodings want to use it.
 * WIP: Is there a better place for it?
 */
static inline bool
is_valid_ascii(const unsigned char *s, int len)
{
	uint64		chunk,
				highbit_cum = UINT64CONST(0),
				zero_cum = UINT64CONST(0x8080808080808080);

	Assert(len % sizeof(chunk) == 0);

	while (len >= sizeof(chunk))
	{
		memcpy(&chunk, s, sizeof(chunk));

		/* Capture any set bits in this chunk. */
		highbit_cum |= chunk;

		/*
		 * Capture any zero bytes in this chunk.
		 *
		 * First, add 0x7f to each byte. This sets the high bit in each byte,
		 * unless it was a zero. We will check later that none of the bytes in
		 * the chunk had the high bit set, in which case the max value each
		 * byte can have after the addition is 0x7f + 0x7f = 0xfe, and we
		 * don't need to worry about carrying over to the next byte.
		 *
		 * If any resulting high bits are zero, the corresponding high bits in
		 * the zero accumulator will be cleared.
		 */
		zero_cum &= (chunk + UINT64CONST(0x7f7f7f7f7f7f7f7f));

		s += sizeof(chunk);
		len -= sizeof(chunk);
	}

	/* Check for any set high bits in the high bit accumulator. */
	if (highbit_cum & UINT64CONST(0x8080808080808080))
		return false;

	/*
	 * Check if all bytes in the zero accumulator still have the high bit set.
	 * XXX: This check is only valid after checking the high bit accumulator,
	 * as noted above.
	 */
	if (zero_cum == UINT64CONST(0x8080808080808080))
		return true;
	else
		return false;
}

#endif							/* PG_UTF8_H */
