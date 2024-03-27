/*-------------------------------------------------------------------------
 *
 * hashfn_unstable.c
 *		Convenience hashing functions based on hashfn_unstable.h.
 *		As described in that header, they must not be used in indexes
 *		or other on-disk structures.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/hashfn_unstable.c
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include "common/hashfn_unstable.h"


uint32
hash_string(const char *s)
{
	fasthash_state hs;
	size_t		s_len;

	fasthash_init(&hs, 0);

	/* Hash string and save the length for tweaking the final mix. */
	s_len = fasthash_accum_cstring(&hs, s);

	return fasthash_final32(&hs, s_len);
}
