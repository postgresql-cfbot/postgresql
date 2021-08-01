/*-------------------------------------------------------------------------
 *
 * pg_utf8_sse42.c
 *	  Validate UTF-8 using SSE 4.2 instructions.
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

#include "port/pg_sse42_utils.h"
#include "port/pg_utf8.h"

/*
 * This module is based on the paper "Validating UTF-8 In Less Than One
 * Instruction Per Byte" by John Keiser and Daniel Lemire:
 *
 * https://arxiv.org/pdf/2010.03090.pdf
 *
 * The authors provide an implementation of this algorithm in the simdjson
 * library (Apache 2.0 license):
 *
 * https://github.com/simdjson/simdjson
 *
 * The PG code was written from scratch, but with some naming conventions
 * adapted from the Westmere implementation of simdjson. The constants and
 * lookup tables were taken directly from simdjson with some cosmetic
 * rearrangements.
 *
 * The core of the lookup algorithm is a two-part process:
 *
 * 1. Classify 2-byte sequences. All 2-byte errors can be found by looking at
 * the first three nibbles of each overlapping 2-byte sequence, using three
 * separate lookup tables. The interesting bytes are either definite errors
 * or two continuation bytes in a row. The latter may be valid depending on
 * what came before.
 *
 * 2. Find starts of possible 3- and 4-byte sequences.
 *
 * Combining the above results allows us to verify any UTF-8 sequence.
 */

/* constants for comparing bytes at the end of a vector */
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


/*
 * Return a vector with lanes non-zero where we have either errors, or
 * two or more continuations in a row.
 */
static inline __m128i
check_special_cases(const __m128i prev, const __m128i input)
{
	const		__m128i byte_1_hi_table = vset(BYTE_1_HIGH_TABLE);
	const		__m128i byte_1_lo_table = vset(BYTE_1_LOW_TABLE);
	const		__m128i byte_2_hi_table = vset(BYTE_2_HIGH_TABLE);

	/*
	 * To classify the first byte in each chunk we need to have the last byte
	 * from the previous chunk.
	 */
	const		__m128i input_shift1 = prev1(prev, input);

	/* put the relevant nibbles into their own bytes in their own registers */
	const		__m128i byte_1_hi = shift_right(input_shift1, 4);
	const		__m128i byte_1_lo = bitwise_and(input_shift1, splat(0x0F));
	const		__m128i byte_2_hi = shift_right(input, 4);

	/* lookup the possible errors for each set of nibbles */
	const		__m128i lookup_1_hi = lookup(byte_1_hi, byte_1_hi_table);
	const		__m128i lookup_1_lo = lookup(byte_1_lo, byte_1_lo_table);
	const		__m128i lookup_2_hi = lookup(byte_2_hi, byte_2_hi_table);

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
	const		__m128i temp = bitwise_and(lookup_1_hi, lookup_1_lo);

	return bitwise_and(temp, lookup_2_hi);
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
	 * bytes, filling in the left lanes with the previous input.
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
	const		__m128i set_two_conts = check_multibyte_lengths(prev, input);

	/* If the two cases are identical, this will be zero. */
	const		__m128i result = bitwise_xor(special_cases, set_two_conts);

	*error = bitwise_or(*error, result);
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
 * Returns zero on error, or the number of bytes processed if no errors were
 * detected.
 *
 * In the error case, the caller must start over at the beginning and verify
 * one byte at a time.
 *
 * In the non-error case, it's still possible we ended in the middle of an
 * incomplete multibyte sequence, so the caller is responsible for adjusting
 * the returned result to make sure it represents the end of the last valid
 * byte sequence.
 *
 * See also the comment in common/wchar.c under "multibyte sequence
 * validators".
 */
int
pg_validate_utf8_sse42(const unsigned char *s, int len)
{
	const unsigned char *start = s;
	__m128i		error = vzero();
	__m128i		prev = vzero();
	__m128i		prev_incomplete = vzero();
	__m128i		input;

	while (len >= sizeof(input))
	{
		input = vload(s);
		check_for_zeros(input, &error);

		/*
		 * If the chunk is all ASCII, we can skip the full UTF-8 check, but we
		 * must still check the previous chunk for incomplete multibyte
		 * sequences at the end. We only update prev_incomplete if the chunk
		 * contains non-ASCII.
		 */
		if (is_highbit_set(input))
		{
			check_utf8_bytes(prev, input, &error);
			prev_incomplete = is_incomplete(input);
		}
		else
			error = bitwise_or(error, prev_incomplete);

		prev = input;
		s += sizeof(input);
		len -= sizeof(input);
	}

	/* If we saw an error during the loop, let the caller handle it. */
	if (to_bool(error))
		return 0;
	else
		return s - start;
}
