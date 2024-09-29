/*-------------------------------------------------------------------------
 *
 * pg_crc32c_avx512.c
 *	  Compute CRC-32C checksum using Intel AVX-512 instructions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/port/pg_crc32c_avx512.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <immintrin.h>

#include "port/pg_crc32c.h"


/*******************************************************************
 * pg_crc32c_avx512(): compute the crc32c of the buffer, where the
 * buffer length must be at least 256, and a multiple of 64. Based
 * on:
 *
 * "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ
 * Instruction"
 *  V. Gopal, E. Ozturk, et al., 2009
 *
 * For This Function:
 * Copyright 2015 The Chromium Authors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
pg_attribute_no_sanitize_alignment()
inline pg_crc32c
pg_comp_crc32c_avx512(pg_crc32c crc, const void *data, size_t length)
{
	static const uint64 k1k2[8] = {
		0xdcb17aa4, 0xb9e02b86, 0xdcb17aa4, 0xb9e02b86, 0xdcb17aa4,
		0xb9e02b86, 0xdcb17aa4, 0xb9e02b86};
	static const uint64 k3k4[8] = {
		0x740eef02, 0x9e4addf8, 0x740eef02, 0x9e4addf8, 0x740eef02,
		0x9e4addf8, 0x740eef02, 0x9e4addf8};
	static const uint64 k9k10[8] = {
		0x6992cea2, 0x0d3b6092, 0x6992cea2, 0x0d3b6092, 0x6992cea2,
		0x0d3b6092, 0x6992cea2, 0x0d3b6092};
	static const uint64 k1k4[8] = {
		0x1c291d04, 0xddc0152b, 0x3da6d0cb, 0xba4fc28e, 0xf20c0dfe,
		0x493c7d27, 0x00000000, 0x00000000};

	const uint8 *input = (const uint8 *)data;
	if (length >= 256)
	{
		uint64 val;
		__m512i x0, x1, x2, x3, x4, x5, x6, x7, x8, y5, y6, y7, y8;
		__m128i a1, a2;

		/*
		 * AVX-512 Optimized crc32c algorithm with mimimum of 256 bytes aligned
		 * to 32 bytes.
		 * >>> BEGIN
		 */

		/*
		* There's at least one block of 256.
		*/
		x1 = _mm512_loadu_si512((__m512i *)(input + 0x00));
		x2 = _mm512_loadu_si512((__m512i *)(input + 0x40));
		x3 = _mm512_loadu_si512((__m512i *)(input + 0x80));
		x4 = _mm512_loadu_si512((__m512i *)(input + 0xC0));

		x1 = _mm512_xor_si512(x1, _mm512_castsi128_si512(_mm_cvtsi32_si128(crc)));

		x0 = _mm512_load_si512((__m512i *)k1k2);

		input += 256;
		length -= 256;

		/*
		* Parallel fold blocks of 256, if any.
		*/
		while (length >= 256)
		{
			x5 = _mm512_clmulepi64_epi128(x1, x0, 0x00);
			x6 = _mm512_clmulepi64_epi128(x2, x0, 0x00);
			x7 = _mm512_clmulepi64_epi128(x3, x0, 0x00);
			x8 = _mm512_clmulepi64_epi128(x4, x0, 0x00);

			x1 = _mm512_clmulepi64_epi128(x1, x0, 0x11);
			x2 = _mm512_clmulepi64_epi128(x2, x0, 0x11);
			x3 = _mm512_clmulepi64_epi128(x3, x0, 0x11);
			x4 = _mm512_clmulepi64_epi128(x4, x0, 0x11);

			y5 = _mm512_loadu_si512((__m512i *)(input + 0x00));
			y6 = _mm512_loadu_si512((__m512i *)(input + 0x40));
			y7 = _mm512_loadu_si512((__m512i *)(input + 0x80));
			y8 = _mm512_loadu_si512((__m512i *)(input + 0xC0));

			x1 = _mm512_ternarylogic_epi64(x1, x5, y5, 0x96);
			x2 = _mm512_ternarylogic_epi64(x2, x6, y6, 0x96);
			x3 = _mm512_ternarylogic_epi64(x3, x7, y7, 0x96);
			x4 = _mm512_ternarylogic_epi64(x4, x8, y8, 0x96);

			input += 256;
			length -= 256;
				}

		/*
		 * Fold 256 bytes into 64 bytes.
		 */
		x0 = _mm512_load_si512((__m512i *)k9k10);
		x5 = _mm512_clmulepi64_epi128(x1, x0, 0x00);
		x6 = _mm512_clmulepi64_epi128(x1, x0, 0x11);
		x3 = _mm512_ternarylogic_epi64(x3, x5, x6, 0x96);

		x7 = _mm512_clmulepi64_epi128(x2, x0, 0x00);
		x8 = _mm512_clmulepi64_epi128(x2, x0, 0x11);
		x4 = _mm512_ternarylogic_epi64(x4, x7, x8, 0x96);

		x0 = _mm512_load_si512((__m512i *)k3k4);
		y5 = _mm512_clmulepi64_epi128(x3, x0, 0x00);
		y6 = _mm512_clmulepi64_epi128(x3, x0, 0x11);
		x1 = _mm512_ternarylogic_epi64(x4, y5, y6, 0x96);

		/*
		 * Single fold blocks of 64, if any.
		 */
		while (length >= 64)
		{
			x2 = _mm512_loadu_si512((__m512i *)input);

			x5 = _mm512_clmulepi64_epi128(x1, x0, 0x00);
			x1 = _mm512_clmulepi64_epi128(x1, x0, 0x11);
			x1 = _mm512_ternarylogic_epi64(x1, x2, x5, 0x96);

			input += 64;
			length -= 64;
		}

		/*
		 * Fold 512-bits to 128-bits.
		 */
		x0 = _mm512_loadu_si512((__m512i *)k1k4);

		a2 = _mm512_extracti32x4_epi32(x1, 3);
		x5 = _mm512_clmulepi64_epi128(x1, x0, 0x00);
		x1 = _mm512_clmulepi64_epi128(x1, x0, 0x11);
		x1 = _mm512_ternarylogic_epi64(x1, x5, _mm512_castsi128_si512(a2), 0x96);

		x0 = _mm512_shuffle_i64x2(x1, x1, 0x4E);
		x0 = _mm512_xor_epi64(x1, x0);
		a1 = _mm512_extracti32x4_epi32(x0, 1);
		a1 = _mm_xor_epi64(a1, _mm512_castsi512_si128(x0));

		/*
		 * Fold 128-bits to 32-bits.
		 */
		val = _mm_crc32_u64(0, _mm_extract_epi64(a1, 0));
		crc = (uint32_t)_mm_crc32_u64(val, _mm_extract_epi64(a1, 1));
		/*
		 * AVX-512 Optimized crc32c algorithm with mimimum of 256 bytes aligned
		 * to 32 bytes.
		 * <<< END
		 ******************************************************************/
	}

	/*
	 * Finish any remaining bytes with legacy AVX algorithm.
	 */
	return pg_comp_crc32c_sse42(crc, input, length);
}
