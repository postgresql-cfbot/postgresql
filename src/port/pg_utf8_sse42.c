/*-------------------------------------------------------------------------
 *
 * pg_utf8_sse42.c
 *	  Validate UTF-8 using Intel SSE 4.2 instructions.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_utf8_sse42.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <nmmintrin.h>

#include "port/pg_utf8.h"

/*
 * This module is based on the paper "Validating UTF-8 In Less Than One
 * Instruction Per Byte" by John Keiser and Daniel Lemire, arXiv:2010.03090
 * [cs.DB], 10 Oct 2020.
 *
 * The authors provide an implementation of this algorithm
 * in the simdjson library (Apache 2.0 license) found at
 * https://github.com/simdjson/simdjson. Even if it were practical to
 * use this library directly, we cannot because it simply returns valid
 * or not valid, and we need to return the number of valid bytes found
 * before the first invalid one.
 *
 * Therefore, the PG code was written from scratch, but with some idioms
 * and naming conventions adapted from the Westmere implementation of
 * simdjson. The constants and lookup tables were taken directly from
 * simdjson with some cosmetic rearrangements.
 *
 * The core of the lookup algorithm is a two-part process:
 *
 * 1. Classify 2-byte sequences. All 2-byte errors can be found by looking
 * at the first three nibbles of each overlapping 2-byte sequence,
 * using three separate lookup tables. The interesting bytes are either
 * definite errors or two continuation bytes in a row. The latter may
 * be valid depending on what came before.
 *
 * 2. Find starts of possible 3- and 4-byte sequences.
 *
 * Combining the above results allows us to verify any UTF-8 sequence.
 */


/* constants for comparing bytes */
#define MAX_CONTINUATION 0xBF
#define MAX_TWO_BYTE_LEAD 0xDF
#define MAX_THREE_BYTE_LEAD 0xEF

/* lookup tables for classifying two-byte sequences */

/*
 * 11______ 0_______
 * 11______ 11______
 */
#define TOO_SHORT		(1 << 0)

/* 0_______ 10______ */
#define TOO_LONG		(1 << 1)

/* 1100000_ 10______ */
#define OVERLONG_2		(1 << 2)

/* 11100000 100_____ */
#define OVERLONG_3		(1 << 3)

/* The following two symbols intentionally share the same value. */

/* 11110000 1000____ */
#define OVERLONG_4		(1 << 4)

/*
 * 11110101 1000____
 * 1111011_ 1000____
 * 11111___ 1000____
 */
#define TOO_LARGE_1000	(1 << 4)

/*
 * 11110100 1001____
 * 11110100 101_____
 * 11110101 1001____
 * 11110101 101_____
 * 1111011_ 1001____
 * 1111011_ 101_____
 * 11111___ 1001____
 * 11111___ 101_____
 */
#define TOO_LARGE		(1 << 5)

/* 11101101 101_____ */
#define SURROGATE		(1 << 6)

/*
 * 10______ 10______
 *
 * The cast here is to silence warnings about implicit conversion
 * from 'int' to 'char'. It's fine that this is a negative value,
 * because we only care about the pattern of bits.
 */
#define TWO_CONTS ((char) (1 << 7))

/* These all have ____ in byte 1 */
#define CARRY (TOO_SHORT | TOO_LONG | TWO_CONTS)

/*
 * table for categorizing bits in the high nibble of
 * the first byte of a 2-byte sequence
 */
#define BYTE_1_HIGH_TABLE \
	/* 0_______ ________ <ASCII in byte 1> */ \
	TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, \
	TOO_LONG, TOO_LONG, TOO_LONG, TOO_LONG, \
	/* 10______ ________ <continuation in byte 1> */ \
	TWO_CONTS, TWO_CONTS, TWO_CONTS, TWO_CONTS, \
	/* 1100____ ________ <two byte lead in byte 1> */ \
	TOO_SHORT | OVERLONG_2, \
	/* 1101____ ________ <two byte lead in byte 1> */ \
	TOO_SHORT, \
	/* 1110____ ________ <three byte lead in byte 1> */ \
	TOO_SHORT | OVERLONG_3 | SURROGATE, \
	/* 1111____ ________ <four+ byte lead in byte 1> */ \
	TOO_SHORT | TOO_LARGE | TOO_LARGE_1000 | OVERLONG_4

/*
 * table for categorizing bits in the low nibble of
 * the first byte of a 2-byte sequence
 */
#define BYTE_1_LOW_TABLE \
	/* ____0000 ________ */ \
	CARRY | OVERLONG_2 | OVERLONG_3 | OVERLONG_4, \
	/* ____0001 ________ */ \
	CARRY | OVERLONG_2, \
	/* ____001_ ________ */ \
	CARRY, \
	CARRY, \
	/* ____0100 ________ */ \
	CARRY | TOO_LARGE, \
	/* ____0101 ________ */ \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	/* ____011_ ________ */ \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	/* ____1___ ________ */ \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	/* ____1101 ________ */ \
	CARRY | TOO_LARGE | TOO_LARGE_1000 | SURROGATE, \
	CARRY | TOO_LARGE | TOO_LARGE_1000, \
	CARRY | TOO_LARGE | TOO_LARGE_1000

/*
 * table for categorizing bits in the high nibble of
 * the second byte of a 2-byte sequence
 */
#define BYTE_2_HIGH_TABLE \
	/* ________ 0_______ <ASCII in byte 2> */ \
	TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, \
	TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT, \
	/* ________ 1000____ */ \
	TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE_1000 | OVERLONG_4, \
	/* ________ 1001____ */ \
	TOO_LONG | OVERLONG_2 | TWO_CONTS | OVERLONG_3 | TOO_LARGE, \
	/* ________ 101_____ */ \
	TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE, \
	TOO_LONG | OVERLONG_2 | TWO_CONTS | SURROGATE | TOO_LARGE, \
	/* ________ 11______ */ \
	TOO_SHORT, TOO_SHORT, TOO_SHORT, TOO_SHORT \


/* helper functions to wrap intrinsics */

#define vset(...)		_mm_setr_epi8(__VA_ARGS__)

/* return a zeroed register */
static inline const __m128i
vzero()
{
	return _mm_setzero_si128();
}

/* perform an unaligned load from memory into a register */
static inline const __m128i
vload(const unsigned char *raw_input)
{
	return _mm_loadu_si128((const __m128i *) raw_input);
}

/* return a vector with each 8-bit lane populated with the input scalar */
static inline __m128i
splat(char byte)
{
	return _mm_set1_epi8(byte);
}

/* perform signed greater-than on all 8-bit lanes */
static inline __m128i
greater_than(const __m128i v1, const __m128i v2)
{
	return _mm_cmpgt_epi8(v1, v2);
}

/* bitwise vector operations */
static inline __m128i
bitwise_and(const __m128i v1, const __m128i v2)
{
	return _mm_and_si128(v1, v2);
}

static inline __m128i
bitwise_or(const __m128i v1, const __m128i v2)
{
	return _mm_or_si128(v1, v2);
}

static inline __m128i
bitwise_xor(const __m128i v1, const __m128i v2)
{
	return _mm_xor_si128(v1, v2);
}

/*
 * Do unsigned subtraction, but instead of wrapping around
 * on overflow, stop at zero. Useful for emulating unsigned
 * comparison.
 */
static inline __m128i
saturating_sub(const __m128i v1, const __m128i v2)
{
	return _mm_subs_epu8(v1, v2);
}

/*
 * Shift right each 8-bit lane
 *
 * There is no intrinsic to do this on 8-bit lanes, so shift right in each
 * 16-bit lane then apply a mask in each 8-bit lane shifted the same amount.
 */
static inline __m128i
shift_right(const __m128i v, const int n)
{
	const		__m128i shift16 = _mm_srli_epi16(v, n);
	const		__m128i mask = splat(0xFF >> n);

	return bitwise_and(shift16, mask);
}

/*
 * Shift entire 'input' register right by N 8-bit lanes, and
 * replace the first N lanes with the last N lanes from the
 * 'prev' register. Could be stated in C thusly:
 *
 * ((prev << 128) | input) >> (N * 8)
 *
 * The third argument to the intrinsic must be a numeric constant, so
 * we must have separate functions for different shift amounts.
 */
static inline __m128i
prev1(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 1);
}

static inline __m128i
prev2(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 2);
}

static inline __m128i
prev3(__m128i prev, __m128i input)
{
	return _mm_alignr_epi8(input, prev, sizeof(__m128i) - 3);
}

/*
 * For each 8-bit lane in the input, use that value as an index
 * into the lookup vector as if it were a 16-element byte array.
 */
static inline __m128i
lookup(const __m128i input, const __m128i lookup)
{
	return _mm_shuffle_epi8(lookup, input);
}

/*
 * Return a vector with lanes non-zero where we have either errors, or
 * two or more continuations in a row.
 */
static inline __m128i
check_special_cases(const __m128i prev, const __m128i input)
{
	const		__m128i byte_1_high_table = vset(BYTE_1_HIGH_TABLE);
	const		__m128i byte_1_low_table = vset(BYTE_1_LOW_TABLE);
	const		__m128i byte_2_high_table = vset(BYTE_2_HIGH_TABLE);

	/*
	 * To classify the first byte in each chunk we need to have the last byte
	 * from the previous chunk.
	 */
	const		__m128i input_shift1 = prev1(prev, input);

	/* put the relevant nibbles into their own bytes in their own registers */
	const		__m128i byte_1_high = shift_right(input_shift1, 4);
	const		__m128i byte_1_low = bitwise_and(input_shift1, splat(0x0F));
	const		__m128i byte_2_high = shift_right(input, 4);

	/* lookup the possible errors for each set of nibbles */
	const		__m128i lookup_1_high = lookup(byte_1_high, byte_1_high_table);
	const		__m128i lookup_1_low = lookup(byte_1_low, byte_1_low_table);
	const		__m128i lookup_2_high = lookup(byte_2_high, byte_2_high_table);

	/*
	 * AND all the lookups together. At this point, non-zero lanes in the
	 * returned vector represent:
	 *
	 * 1. invalid 2-byte sequences
	 *
	 * 2. the second continuation byte of a 3- or 4-byte character
	 *
	 * 3. the third continuation byte of a 4-byte character
	 */
	const		__m128i temp = bitwise_and(lookup_1_high, lookup_1_low);

	return bitwise_and(temp, lookup_2_high);
}

/*
 * Return a vector with lanes set to TWO_CONTS where we expect to find two
 * continuations in a row. These are valid only within 3- and 4-byte sequences.
 */
static inline __m128i
check_multibyte_lengths(const __m128i prev, const __m128i input)
{
	/*
	 * Populate registers that contain the input shifted right by 2 and 3
	 * bytes, filling in the left lanes from the previous input.
	 */
	const		__m128i input_shift2 = prev2(prev, input);
	const		__m128i input_shift3 = prev3(prev, input);

	/*
	 * Constants for comparison. Any 3-byte lead is greater than
	 * MAX_TWO_BYTE_LEAD, etc.
	 */
	const		__m128i max_lead2 = splat(MAX_TWO_BYTE_LEAD);
	const		__m128i max_lead3 = splat(MAX_THREE_BYTE_LEAD);

	/*
	 * Look in the shifted registers for 3- or 4-byte leads. There is no
	 * unsigned comparison, so we use saturating subtraction followed by
	 * signed comparison with zero. Any non-zero bytes in the result represent
	 * valid leads.
	 */
	const		__m128i is_third_byte = saturating_sub(input_shift2, max_lead2);
	const		__m128i is_fourth_byte = saturating_sub(input_shift3, max_lead3);

	/* OR them together for easier comparison */
	const		__m128i temp = bitwise_or(is_third_byte, is_fourth_byte);

	/*
	 * Set all bits in each 8-bit lane if the result is greater than zero.
	 * Signed arithmetic is okay because the values are small.
	 */
	const		__m128i must23 = greater_than(temp, vzero());

	/*
	 * We want to compare with the result of check_special_cases() so apply a
	 * mask to return only the set bits corresponding to the "two
	 * continuations" case.
	 */
	return bitwise_and(must23, splat(TWO_CONTS));
}

/* set bits in the error vector where we find invalid UTF-8 input */
static inline void
check_utf8_bytes(const __m128i prev, const __m128i input, __m128i * error)
{
	const		__m128i special_cases = check_special_cases(prev, input);
	const		__m128i expect_two_conts = check_multibyte_lengths(prev, input);

	/* If the two cases are identical, this will be zero. */
	const		__m128i result = bitwise_xor(expect_two_conts, special_cases);

	*error = bitwise_or(*error, result);
}

/* return false if a register is zero, true otherwise */
static inline bool
to_bool(const __m128i v)
{
	/*
	 * _mm_testz_si128 returns 1 if the bitwise AND of the two arguments is
	 * zero. Zero is the only value whose bitwise AND with itself is zero.
	 */
	return !_mm_testz_si128(v, v);
}

/* set bits in the error vector where bytes in the input are zero */
static inline void
check_for_zeros(const __m128i v, __m128i * error)
{
	const		__m128i cmp = _mm_cmpeq_epi8(v, vzero());

	*error = bitwise_or(*error, cmp);
}

/* vector version of IS_HIGHBIT_SET() */
static inline bool
is_highbit_set(const __m128i v)
{
	return _mm_movemask_epi8(v) != 0;
}

/* return non-zero if the input terminates with an incomplete code point */
static inline __m128i
is_incomplete(const __m128i v)
{
	const		__m128i max_array =
	vset(0xFF, 0xFF, 0xFF, 0xFF,
		 0xFF, 0xFF, 0xFF, 0xFF,
		 0xFF, 0xFF, 0xFF, 0xFF,
		 0xFF, MAX_THREE_BYTE_LEAD, MAX_TWO_BYTE_LEAD, MAX_CONTINUATION);

	return saturating_sub(v, max_array);
}

/*
 * See the comment in common/wchar.c under "multibyte sequence validators".
 */
int
pg_validate_utf8_sse42(const unsigned char *s, int len)
{
	const unsigned char *start = s;
	const int	orig_len = len;
	__m128i		error = vzero();
	__m128i		prev = vzero();
	__m128i		prev_incomplete = vzero();
	__m128i		input;

	while (len > sizeof(__m128i))
	{
		input = vload(s);

		check_for_zeros(input, &error);

		/*
		 * If the chunk is all ASCII, we can skip the full UTF-8 check, but we
		 * must still check the previous chunk for incomplete multibyte
		 * sequences at the end. We only update prev_incomplete if the chunk
		 * contains non-ASCII, since the error is cumulative.
		 */
		if (!is_highbit_set(input))
			error = bitwise_or(error, prev_incomplete);
		else
		{
			check_utf8_bytes(prev, input, &error);
			prev_incomplete = is_incomplete(input);
		}

		prev = input;
		s += sizeof(__m128i);
		len -= sizeof(__m128i);
	}

	/*
	 * If we saw an error any time during the loop, start over with the
	 * fallback so we can return the number of valid bytes.
	 */
	if (to_bool(error))
		return pg_validate_utf8_fallback(start, orig_len);
	else
	{
		unsigned char inbuf[sizeof(__m128i)];

		/*
		 * Back-fill the remainder with some kind of ASCII so that we have a
		 * whole register. Normally we memset buffers with zero, but if we did
		 * that, we couldn't reuse our check for zero bytes using vector
		 * operations.
		 */
		memset(inbuf, 0x20, sizeof(__m128i));
		memcpy(inbuf, s, len);

		input = vload(inbuf);

		check_for_zeros(input, &error);
		check_utf8_bytes(prev, input, &error);

		/*
		 * We must also check that the remainder does not end with an
		 * incomplete code point. This would only slip past check_utf8_bytes()
		 * if the remainder is 16 bytes in length, but it's not worth adding a
		 * branch for that.
		 */
		error = bitwise_or(error, is_incomplete(input));

		if (to_bool(error))
		{
			/*
			 * If we encounter errors in the remainder, we need to be a bit
			 * more careful, since it's possible that the end of the input
			 * falls within a multibyte sequence, and we don't want to repeat
			 * the work we've already done. In that case, we just walk
			 * backwards into the previous chunk, if any, to find the last
			 * byte that could have been the start of a character. For short
			 * strings, this will start over from the beginning, but that's
			 * fine.
			 */
			while (s > start)
			{
				s--;
				len++;

				if ((!IS_HIGHBIT_SET(*s) && *s != '\0') ||
					IS_TWO_BYTE_LEAD(*s) ||
					IS_THREE_BYTE_LEAD(*s) ||
					IS_FOUR_BYTE_LEAD(*s))
					break;
			}
			return orig_len - len + pg_validate_utf8_fallback(s, len);
		}
		else
			return orig_len;
	}
}
