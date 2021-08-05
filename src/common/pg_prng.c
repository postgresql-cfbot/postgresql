/*
 * Pseudo-Random Number Generator
 *
 * Copyright (c) 2021-2021, PostgreSQL Global Development Group
 *
 * Previous version was the 1988 (?) rand48 standard with 48 bits state LCG,
 * designed for running on 32 bits or even 16 bits architectures, and to produce
 * 32 bit ints and floats (i.e. *not* doubles).
 *
 * The implied 16-bits unpacking/packing may have a detrimental performance impact
 * on modern 64 bits architectures.
 *
 * We (probably) want:
 * - one reasonable default PRNG for all pg internal uses.
 * - NOT to invent a new design!
 * - something fast, close to rand48 (which basically does 2 arithmetic ops)
 *   no need for something cryptographic though, which would imply slow
 * - to produce 64 bits integers & doubles with a 52 bits mantissa
 *   so state size > 64 bits.
 * - a small state though, because we might generate quite a few of them
 *   for different purposes so state size <= 256 or even <= 128 bits
 * - the state to be aligned to whatever, so state size = 128 bits
 * - 64 bits operations for efficiency on modern architectures,
 *   but NOT 128 bits operations.
 * - not to depend on special hardware for speed (eg MMX/SSE/AES).
 * - not something with obvious known and relevant defects.
 * - not something with "rights" attached.
 *
 * These constraints reduce a lot the available options from
 * https://en.wikipedia.org/wiki/List_of_random_number_generators
 *
 * LCG
 * - POSIX rand48: m = 2^48, a = 25214903917, c = 11, extract 47...16
 * - MMIX by Donald Knuth: m = 2^64, a = 6364136223846793005, c = 1442695040888963407
 * - very bad on low bits, which MUST NOT BE USED
 *   a 128 bits would do, but no clear standards, and it would imply 128-bit ops,
 *   see PCG below.
 *
 * PCG Family (https://www.pcg-random.org/)
 * - this is basically an LCG with an improved outpout function
 *   so as to avoid typical LCG issues and pass stats tests.
 * - recommands state size to be twice the output size,
 *   which seems a reasonable requirement.
 * - this implies 128 bit ops for our target parameters,
 *   otherwise it is a fairly good candidate for simplicity,
 *   efficiency and quality.
 *
 * Xor-Shift-Rotate PRNGs:
 * - a generic annoying feature of this generator class is that there is one
 *   special all-zero state which must not be used, inducing a constraint on
 *   initialization, and when the generator get close to it (i.e. many zeros
 *   in the state) it takes some iterations to recover, with small lumps
 *   of close/related/bad numbers generated in the process.
 * Xoroshiro128+
 * - fails statistical linearity tests in the low bits.
 * Xoroshiro128**
 *   https://prng.di.unimi.it/xoroshiro128starstar.c
 * - mild and irrelevant issues: close to zero state, stats issues after some transforms
 * Xoshiro256**
 * - it looks good, but some issues (no big deal for pg usage IMO)
 *   https://www.pcg-random.org/posts/a-quick-look-at-xoshiro256.html
 *   mild problems getting out of closes-to-zero state
 *
 * MT19937-64
 * + default in many places, standard
 * - huge 2.5 KiB state (but TinyMT, 127 bits state space)
 * - slow (but SFMT, twice as fast)
 * - vulnerabilities on tinyMT?
 *
 * WELL
 * - large state, 32-bit oriented
 *
 * ACORN Family (https://en.wikipedia.org/wiki/ACORN_(PRNG))
 * - Fortran / double oriented, no C implementation found?!
 * - medium large state "In practice we recommend using k > 10, M = 2^60 (for general application)
 *   or M = 2^120 (for demanding applications requiring high-quality pseudo-random numbers
 *   that will consistently pass all the tests in standard test packages such as TestU01)
 *   and choose any odd value less than M for the seed."
 *   k = 11 => 88 bytes/704-bits state, too large:-(
 *
 * Splitmix?
 * Others?
 *
 * The conclusion is that we use xor-shift-rotate generator below.
 */

#include "c.h"
#include "common/pg_prng.h"
#include "port/pg_bitutils.h"
#include <math.h>

#define FIRST_BIT_MASK UINT64CONST(0x8000000000000000)
#define RIGHT_HALF_MASK UINT64CONST(0x00000000FFFFFFFF)
#define DMANTISSA_MASK UINT64CONST(0x000FFFFFFFFFFFFF)

/* 64-bits rotate left */
static inline uint64
rotl(const uint64 x, const int bits)
{
	return (x << bits) | (x >> (64 - bits));
}

/* another 64-bits generator is used for state initialization or iterations */
static uint64
splitmix64(uint64 * state)
{
	/* state update */
	uint64	val = (*state += UINT64CONST(0x9E3779B97f4A7C15));

	/* value extraction */
	val = (val ^ (val >> 30)) * UINT64CONST(0xBF58476D1CE4E5B9);
	val = (val ^ (val >> 27)) * UINT64CONST(0x94D049BB133111EB);

	return val ^ (val >> 31);
}

/* seed prng state from a 64 bits integer, ensuring non zero */
void
pg_prng_seed(pg_prng_state *state, uint64 seed)
{
	state->s0 = splitmix64(&seed);
	state->s1 = splitmix64(&seed);
}

/* seed with 53 bits (mantissa & sign) from a float */
void
pg_prng_fseed(pg_prng_state *state, double fseed)
{
	uint64 seed = (int64) (((double) DMANTISSA_MASK) * fseed);
	pg_prng_seed(state, seed);
}

/* strong random seeding */
void
pg_prng_strong_seed(pg_prng_state *state)
{
	pg_strong_random((void *) state, sizeof(pg_prng_state));

	/* avoid zero with Donald Knuth's LCG parameters */
	if (unlikely(state->s0 == 0 && state->s1 == 0))
	{
		/* should it warn that something is amiss if we get there? */
		pg_prng_state def = PG_PRNG_DEFAULT_STATE;
		*state = def;
	}
}

/* generator & state update */
static uint64
xoroshiro128ss(pg_prng_state *state)
{
	const uint64	s0 = state->s0,
					sx = state->s1 ^ s0,
					val = rotl(s0 * 5, 7) * 9;

	/* update state */
	state->s0 = rotl(s0, 24) ^ sx ^ (sx << 16);
	state->s1 = rotl(sx, 37);

	return val;
}

/* u64 generator */
uint64
pg_prng_u64(pg_prng_state *state)
{
	return xoroshiro128ss(state);
}

/*
 * select in a range with bitmask rejection.
 *
 * if range is empty, rmin is returned.
 *
 * the prng may advance by several states or none,
 * depending on the range value.
 */
uint64
pg_prng_u64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
	uint64 val;

	if (likely(rmax > rmin))
	{
		uint64 range = rmax - rmin;

		/* bit position is between 0 and 63, so rshift >= 0 */
		uint32 rshift = 63 - pg_leftmost_one_pos64(range);

		do
		{
			val = xoroshiro128ss(state) >> rshift;
		}
		while (unlikely(val > range));
	}
	else
		val = 0;

	return rmin + val;
}

/* i64 generator */
int64
pg_prng_i64(pg_prng_state *state)
{
	return (int64) xoroshiro128ss(state);
}

/* u32 generator */
uint32
pg_prng_u32(pg_prng_state *state)
{
	const uint64 v = xoroshiro128ss(state);
	return (uint32) (((v >> 32) ^ v) & RIGHT_HALF_MASK);
}

/* i32 generator */
int32
pg_prng_i32(pg_prng_state *state)
{
	const uint64 v = xoroshiro128ss(state);
	return (int32) (((v >> 32) ^ v) & RIGHT_HALF_MASK);
}

/* double generator */
double
pg_prng_f64(pg_prng_state *state)
{
	uint64 v = xoroshiro128ss(state);
	return ldexp((double) (v & DMANTISSA_MASK), -52);
}

/* bool generator */
bool
pg_prng_bool(pg_prng_state *state)
{
	uint64 v = xoroshiro128ss(state);
	return (v & FIRST_BIT_MASK) == FIRST_BIT_MASK;
}
