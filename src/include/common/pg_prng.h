/*-------------------------------------------------------------------------
 *
 * PRNG: internal Pseudo-Random Number Generator
 *
 * Copyright (c) 2021-2021, PostgreSQL Global Development Group
 *
 * src/include/common/pg_prng.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PRNG_H
#define PG_PRNG_H

/* 128 bits state */
typedef struct pg_prng_state {
	uint64	s0, s1;
} pg_prng_state;

/* global pseudo-random state */
extern PGDLLIMPORT pg_prng_state	pg_global_prng_state;

/* arbitrarily use Donald Knuth's LCG constants for default state */
#define PG_PRNG_DEFAULT_STATE \
	{ UINT64CONST(0x5851F42D4C957F2D), UINT64CONST(0x14057B7EF767814F) }

extern void pg_prng_seed(pg_prng_state *state, uint64 seed);
extern void pg_prng_fseed(pg_prng_state *state, double fseed);
extern bool pg_prng_strong_seed(pg_prng_state *state);
extern uint64 pg_prng_uint64(pg_prng_state *state);
extern uint64 pg_prng_uint64_range(pg_prng_state *state, uint64 rmin, uint64 rmax);
extern int64 pg_prng_int64(pg_prng_state *state);
extern int64 pg_prng_int64p(pg_prng_state *state);
extern uint32 pg_prng_uint32(pg_prng_state *state);
extern int32 pg_prng_int32(pg_prng_state *state);
extern int32 pg_prng_int32p(pg_prng_state *state);
extern double pg_prng_double(pg_prng_state *state);
extern bool pg_prng_bool(pg_prng_state *state);

#endif							/* PG_PRNG_H */
