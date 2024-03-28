/*-------------------------------------------------------------------------
 *
 * pg_crc32c_armv8.c
 *	  Compute CRC-32C checksum using ARMv8 CRC Extension instructions
 *	  with ARMv8 VMULL Extentsion instructions or not
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_crc32c_armv8.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <arm_acle.h>
#include <arm_neon.h>

#include "port/pg_crc32c.h"

static inline pg_crc32c
pg_comp_crc32c_helper(pg_crc32c crc, const void *data, size_t len, bool use_vmull)
{
	const unsigned char *p = data;
	const unsigned char *pend = p + len;

	/*
	 * ARMv8 doesn't require alignment, but aligned memory access is
	 * significantly faster. Process leading bytes so that the loop below
	 * starts with a pointer aligned to eight bytes.
	 */
	if (!PointerIsAligned(p, uint16) &&
		p + 1 <= pend)
	{
		crc = __crc32cb(crc, *p);
		p += 1;
	}
	if (!PointerIsAligned(p, uint32) &&
		p + 2 <= pend)
	{
		crc = __crc32ch(crc, *(uint16 *) p);
		p += 2;
	}
	if (!PointerIsAligned(p, uint64) &&
		p + 4 <= pend)
	{
		crc = __crc32cw(crc, *(uint32 *) p);
		p += 4;
	}

	if (use_vmull)
	{
#if defined(USE_ARMV8_CRYPTO) || defined(USE_ARMV8_CRYPTO_WITH_RUNTIME_CHECK)
/*
 * Crc32c parallel computation Input data is divided into three
 * equal-sized blocks. Block length : 42 words(42 * 8 bytes).
 * CRC0: 0 ~ 41 * 8,
 * CRC1: 42 * 8 ~ (42 * 2 - 1) * 8,
 * CRC2: 42 * 2 * 8 ~ (42 * 3 - 1) * 8.
 */
		while (p + 1024 <= pend)
		{
#define BLOCK_LEN 42
			const uint64_t *in64 = (const uint64_t *) (p);
			uint32_t	crc0 = crc,
						crc1 = 0,
						crc2 = 0;

			for (int i = 0; i < BLOCK_LEN; i++, in64++)
			{
				crc0 = __crc32cd(crc0, *(in64));
				crc1 = __crc32cd(crc1, *(in64 + BLOCK_LEN));
				crc2 = __crc32cd(crc2, *(in64 + BLOCK_LEN * 2));
			}
			in64 += BLOCK_LEN * 2;
			crc0 = __crc32cd(0, vmull_p64(crc0, 0xcec3662e));
			crc1 = __crc32cd(0, vmull_p64(crc1, 0xa60ce07b));
			crc = crc0 ^ crc1 ^ crc2;

			crc = __crc32cd(crc, *in64++);
			crc = __crc32cd(crc, *in64++);

			p += 1024;
#undef BLOCK_LEN
		}
#endif
	}

	/* Process eight bytes at a time, as far as we can. */
	while (p + 8 <= pend)
	{
		crc = __crc32cd(crc, *(uint64 *) p);
		p += 8;
	}

	/* Process remaining 0-7 bytes. */
	if (p + 4 <= pend)
	{
		crc = __crc32cw(crc, *(uint32 *) p);
		p += 4;
	}
	if (p + 2 <= pend)
	{
		crc = __crc32ch(crc, *(uint16 *) p);
		p += 2;
	}
	if (p < pend)
	{
		crc = __crc32cb(crc, *p);
	}

	return crc;
}

#if (defined(USE_ARMV8_CRC32C) && defined(USE_ARMV8_CRYPTO)) || defined(USE_ARMV8_CRC32C_WITH_RUNTIME_CHECK)
pg_crc32c
pg_comp_crc32c_armv8_parallel(pg_crc32c crc, const void *data, size_t len)
{
	return pg_comp_crc32c_helper(crc, data, len, true);
}
#endif

#if (defined(USE_ARMV8_CRC32C) && !defined(USE_ARMV8_CRYPTO)) || defined(USE_ARMV8_CRC32C_WITH_RUNTIME_CHECK)
pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len)
{
	return pg_comp_crc32c_helper(crc, data, len, false);
}
#endif
