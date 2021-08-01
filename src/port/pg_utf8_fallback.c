/*-------------------------------------------------------------------------
 *
 * pg_utf8_fallback.c
 *	  Validate UTF-8 using plain C.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_utf8_fallback.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/pg_utf8.h"


/*
 * This determines how much to advance the string pointer each time per loop.
 * Sixteen seems to give the best balance of performance across different
 * byte distributions.
 */
#define STRIDE_LENGTH 16

/*
 * With six bits per state, the mask is 63, whose importance is described
 * later.
 */
#define DFA_BITS_PER_STATE 6
#define DFA_MASK ((1 << DFA_BITS_PER_STATE) - 1)


/*
 * The fallback UTF-8 validator uses a "shift-based" DFA as described by Per
 * Vognsen:
 *
 * https://gist.github.com/pervognsen/218ea17743e1442e59bb60d29b1aa725
 *
 * In a traditional table-driven DFA, the input byte and current state are
 * used to compute the index into an array of state transitions. Since the
 * load address is dependent on earlier work, the CPU is not kept busy.
 *
 * Now, in a shift-based DFA, the input byte is an index into array of
 * integers that encode the state transitions. To retrieve the current state,
 * you simply shift the integer by the current state and apply a mask. In
 * this scheme, loads only depend on the input byte, so there is better
 * pipelining.
 *
 * The naming conventions, but not code, in this file are adopted from the
 * following UTF-8 to UTF-16/32 transcoder, which uses a traditional DFA:
 *
 * https://github.com/BobSteagall/utf_utils/blob/master/src/utf_utils.cpp
 *
 * ILL  ASC  CR1  CR2  CR3  L2A  L3A  L3B  L3C  L4A  L4B  L4C CLASS / STATE
 * =========================================================================
 * err, END, bct, bct, bct, CS1, P3A, CS2, P3B, P4A, CS3, P4B,      | BGN/END
 * err, err, err, err, err, err, err, err, err, err, err, err,      | ERR
 *
 * err, err, END, END, END, err, err, err, err, err, err, err,      | CS1
 * err, err, CS1, CS1, CS1, err, err, err, err, err, err, err,      | CS2
 * err, err, CS2, CS2, CS2, err, err, err, err, err, err, err,      | CS3
 *
 * err, err, tol, tol, CS1, err, err, err, err, err, err, err,      | P3A
 * err, err, CS1, CS1, sur, err, err, err, err, err, err, err,      | P3B
 *
 * err, err, fol, CS2, CS2, err, err, err, err, err, err, err,      | P4A
 * err, err, CS2, ftl, ftl, err, err, err, err, err, err, err,      | P4B
 *
 * Error states are lower case in the table for readability. The table as
 * copied here adds more detail on some of the error states, but this is
 * just for clarity -- it makes no difference in the coding:
 *
 * bct = bare continuation
 * tol = three-byte overlong
 * sur = surrogate
 * fol = four-byte overlong
 * ftl = four-byte too large
 */

/* Invalid state */
#define	ERR  UINT64CONST(0)
/* Begin */
#define	BGN (UINT64CONST(1) * DFA_BITS_PER_STATE)
/* Continuation states */
#define	CS1 (UINT64CONST(2) * DFA_BITS_PER_STATE)	/* expect 1 cont. byte */
#define	CS2 (UINT64CONST(3) * DFA_BITS_PER_STATE)	/* expect 2 cont. bytes */
#define	CS3 (UINT64CONST(4) * DFA_BITS_PER_STATE)	/* expect 3 cont. bytes */
/* Partial 3-byte sequence states */
#define	P3A (UINT64CONST(5) * DFA_BITS_PER_STATE)	/* leading byte was E0 */
#define	P3B (UINT64CONST(6) * DFA_BITS_PER_STATE)	/* leading byte was ED */
/* Partial 4-byte sequence states */
#define	P4A (UINT64CONST(7) * DFA_BITS_PER_STATE)	/* leading byte was F0 */
#define	P4B (UINT64CONST(8) * DFA_BITS_PER_STATE)	/* leading byte was F4 */
/* Begin and End are the same state */
#define	END BGN

/*
 * The byte categories are 64-bit integers that encode within them the state
 * transitions. Shifting by the current state gives the next state.
 */

/* invalid byte */
#define ILL ERR
/* ASCII */
#define ASC (END << BGN)
/* continuation byte */
#define CR1 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4B)
#define CR2 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4A)
#define CR3 (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3A) | (CS2 << P4A)
/* 2-byte lead */
#define L2A (CS1 << BGN)
/* 3-byte lead */
#define L3A (P3A << BGN)
#define L3B (CS2 << BGN)
#define L3C (P3B << BGN)
/* 4-byte lead */
#define L4A (P4A << BGN)
#define L4B (CS3 << BGN)
#define L4C (P4B << BGN)

/* map an input byte to its byte category */
const uint64 ByteCategory[256] =
{
	/* ASCII */

	ILL, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,
	ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC,

	/* continuation bytes */

	/* 80..8F */
	CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1,
	CR1, CR1, CR1, CR1, CR1, CR1, CR1, CR1,

	/* 90..9F */
	CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2,
	CR2, CR2, CR2, CR2, CR2, CR2, CR2, CR2,

	/* A0..BF */
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,
	CR3, CR3, CR3, CR3, CR3, CR3, CR3, CR3,

	/* leading bytes */

	/* C0..DF */
	ILL, ILL, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,
	L2A, L2A, L2A, L2A, L2A, L2A, L2A, L2A,

	/* E0..EF */
	L3A, L3B, L3B, L3B, L3B, L3B, L3B, L3B,
	L3B, L3B, L3B, L3B, L3B, L3C, L3B, L3B,

	/* F0..FF */
	L4A, L4B, L4B, L4B, L4C, ILL, ILL, ILL,
	ILL, ILL, ILL, ILL, ILL, ILL, ILL, ILL
};

static inline void
utf8_advance(const unsigned char *s, uint64 *state, int len)
{
	/* Note: We deliberately don't check the state within the loop. */
	while (len > 0)
	{
		/*
		 * It's important that the mask value is 63: In most instruction sets,
		 * a shift by a 64-bit operand is understood to be a shift by its mod
		 * 64, so the compiler should elide the mask operation.
		 */
		*state = ByteCategory[*s++] >> (*state & DFA_MASK);
		len--;
	}

	*state &= DFA_MASK;
}

/*
 * Returns zero on error, or the string length if no errors were detected.
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
pg_validate_utf8_fallback(const unsigned char *s, int len)
{
	const int	orig_len = len;
	uint64		state = BGN;

	while (len >= STRIDE_LENGTH)
	{
		/*
		 * If the chunk is all ASCII, we can skip the full UTF-8 check, but we
		 * must first check for a non-END state, which means the previous
		 * chunk ended in the middle of a multibyte sequence.
		 */
		if (state != END || !is_valid_ascii(s, STRIDE_LENGTH))
			utf8_advance(s, &state, STRIDE_LENGTH);

		s += STRIDE_LENGTH;
		len -= STRIDE_LENGTH;
	}

	/* check remaining bytes */
	utf8_advance(s, &state, len);

	/*
	 * If we saw an error during the loop, let the caller handle it. We treat
	 * all other states as success.
	 */
	if (state == ERR)
		return 0;
	else
		return orig_len;
}
