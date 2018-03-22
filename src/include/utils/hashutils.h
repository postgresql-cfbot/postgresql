/*
 * Utilities for working with hash values.
 *
 * Portions Copyright (c) 2017-2018, PostgreSQL Global Development Group
 */

#ifndef HASHUTILS_H
#define HASHUTILS_H

/*
 * Combine two 32-bit hash values, resulting in another hash value, with
 * decent bit mixing.
 *
 * Similar to boost's hash_combine().
 */
static inline uint32
hash_combine(uint32 a, uint32 b)
{
	a ^= b + 0x9e3779b9 + (a << 6) + (a >> 2);
	return a;
}

/*
 * Combine two 64-bit hash values, resulting in another hash value, using the
 * same kind of technique as hash_combine().  Testing shows that this also
 * produces good bit mixing.
 */
static inline uint64
hash_combine64(uint64 a, uint64 b)
{
	/* 0x49a0f4dd15e5a8e3 is 64bit random data */
	a ^= b + UINT64CONST(0x49a0f4dd15e5a8e3) + (a << 54) + (a >> 7);
	return a;
}

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer, for
 * performance.
 */
static inline uint32
murmurhash32(uint32 data)
{
	uint32		h = data;

	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

#define	ROTL32(x,r)	((x << r) | (x >> (32 - r)))

/*
 * Simple inline murmur hash implementation hashing a 32 bit integer and
 * 32 bit seed, for performance.
 *
 * XXX Check this actually produces same results as MurmurHash3_x86_32.
 */
static inline uint32
murmurhash32_seed(uint32 seed, uint32 data)
{
	uint32	h = seed;
	uint32	k = data;
	uint32	c1 = 0xcc9e2d51;
	uint32	c2 = 0x1b873593;

	k *= c1;
	k = ROTL32(k,15);
	k *= c2;

	h ^= k;
	h = ROTL32(h,13); 
	h = h * 5 + 0xe6546b64;

	h ^= sizeof(uint32);

	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;
}

#endif							/* HASHUTILS_H */
