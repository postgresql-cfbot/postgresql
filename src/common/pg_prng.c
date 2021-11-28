/*
 * Pseudo-Random Number Generator
 *
 * Copyright (c) 2021-2021, PostgreSQL Global Development Group
 *
 * https://en.wikipedia.org/wiki/List_of_random_number_generators
 *
 * We chose a xoshiro 128 bit state to have a small, fast PRNG suitable
 * for generating reasonably good quality uniform 64 bit data.
 *
 * About these generators: https://prng.di.unimi.it/
 */

#include "c.h"
#include "common/pg_prng.h"
#include "port/pg_bitutils.h"
#include <math.h>

/* global state */
pg_prng_state	pg_global_prng_state;

#define LAST_32BIT_MASK UINT64CONST(0x00000000FFFFFFFF)
#define LAST_31BIT_MASK UINT64CONST(0x000000007FFFFFFF)
#define LAST_63BIT_MASK UINT64CONST(0x7FFFFFFFFFFFFFFF)
#define DMANTISSA_MASK  UINT64CONST(0x000FFFFFFFFFFFFF)

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
bool
pg_prng_strong_seed(pg_prng_state *state)
{
	bool ok = pg_strong_random((void *) state, sizeof(pg_prng_state));

	/* avoid zero with Donald Knuth's LCG parameters */
	if (unlikely(state->s0 == 0 && state->s1 == 0))
	{
		/* should it warn that something is amiss if we get there? */
		pg_prng_state def = PG_PRNG_DEFAULT_STATE;
		*state = def;
	}

	return ok;
}

/* basic generator & state update used for all types */
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

/* return a uniform uint64 */
uint64
pg_prng_uint64(pg_prng_state *state)
{
	return xoroshiro128ss(state);
}

/* return a uniform uint64 in range [rmin, rmax], or rmin if range is empty */
uint64
pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax)
{
	uint64 val;

	if (likely(rmax > rmin))
	{
		uint64 range = rmax - rmin;

		/* bit position is between 0 and 63, so rshift >= 0 */
		uint32 rshift = 63 - pg_leftmost_one_pos64(range);

		/*
		 * iterate with a bitmask rejection method.
		 * the prng may advance by several states or none,
		 * depending on the range value.
		 */
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

/* return a uniform int64 in [-2^63, 2^63) */
int64
pg_prng_int64(pg_prng_state *state)
{
	return (int64) xoroshiro128ss(state);
}

/* return a uniform int64 in [0, 2^63) */
int64
pg_prng_int64p(pg_prng_state *state)
{
	return (int64) xoroshiro128ss(state) & LAST_63BIT_MASK;
}

/* return a uniform uint32 */
uint32
pg_prng_uint32(pg_prng_state *state)
{
	const uint64 v = xoroshiro128ss(state);
	return (uint32) (((v >> 32) ^ v) & LAST_32BIT_MASK);
}

/* return a uniform int32 in [-2^31, 2^31) */
int32
pg_prng_int32(pg_prng_state *state)
{
	const uint64 v = xoroshiro128ss(state);
	return (int32) (((v >> 32) ^ v) & LAST_32BIT_MASK);
}

/* return a uniform int32 in [0, 2^31) */
int32
pg_prng_int32p(pg_prng_state *state)
{
	const uint64 v = xoroshiro128ss(state);
	return (int32) (((v >> 32) ^ v) & LAST_31BIT_MASK);
}

/* return a uniform double in [0.0, 1.0) */
double
pg_prng_double(pg_prng_state *state)
{
	uint64 v = xoroshiro128ss(state);
	return ldexp((double) (v & DMANTISSA_MASK), -52);
}

/* return a uniform bool */
bool
pg_prng_bool(pg_prng_state *state)
{
	uint64 v = xoroshiro128ss(state);
	return (v & 1) == 1;
}
