/* ----------
 * pg_lzcompress.h -
 *
 *	Definitions for the builtin LZ compressor
 *
 * src/include/common/pg_lzcompress.h
 * ----------
 */

#ifndef _PG_LZCOMPRESS_H_
#define _PG_LZCOMPRESS_H_

#ifdef HAVE_LZ4
#include "lz4.h"

#define SIZEOF_PG_COMPRESS_HEADER	1
/*
 * Macro version of pg_compress_bound, less precise, usable in places where
 * we need compile time size information.
 * We add +1 compared to what algorithms need because that's the size of
 * pg_compress header.
 */
#define PGLZ_MAX_OUTPUT(_dlen)	(Max((_dlen) + 4, LZ4_COMPRESSBOUND(_dlen)) + \
								 SIZEOF_PG_COMPRESS_HEADER)
#else
#define PGLZ_MAX_OUTPUT(_dlen)	((_dlen) + 4 + SIZEOF_PG_COMPRESS_HEADER)
#endif

/*
 * PGLZCompressionAlgo
 *
 * Which algorithm to use for TOAST and WAL compression.
 *
 * COMPRESS_ALGO_PGLZ - use the builtin pglz algorithm
 * COMPRESS_ALGO_LZ4 - use the LZ4 library
 */
typedef enum
{
	COMPRESS_ALGO_PGLZ = 0,
	COMPRESS_ALGO_LZ4
}	PGLZCompressAlgo;


/* ----------
 * PGLZ_Strategy -
 *
 *		Some values that control the compression algorithm.
 *
 *		min_input_size		Minimum input data size to consider compression.
 *
 *		max_input_size		Maximum input data size to consider compression.
 *
 *		min_comp_rate		Minimum compression rate (0-99%) to require.
 *							Regardless of min_comp_rate, the output must be
 *							smaller than the input, else we don't store
 *							compressed.
 *
 *		first_success_by	Abandon compression if we find no compressible
 *							data within the first this-many bytes.
 *
 *		match_size_good		The initial GOOD match size when starting history
 *							lookup. When looking up the history to find a
 *							match that could be expressed as a tag, the
 *							algorithm does not always walk back entirely.
 *							A good match fast is usually better than the
 *							best possible one very late. For each iteration
 *							in the lookup, this value is lowered so the
 *							longer the lookup takes, the smaller matches
 *							are considered good.
 *
 *		match_size_drop		The percentage by which match_size_good is lowered
 *							after each history check. Allowed values are
 *							0 (no change until end) to 100 (only check
 *							latest history entry at all).
 * ----------
 */
typedef struct PGLZ_Strategy
{
	int32		min_input_size;
	int32		max_input_size;
	int32		min_comp_rate;
	int32		first_success_by;
	int32		match_size_good;
	int32		match_size_drop;
} PGLZ_Strategy;


/* ----------
 * The standard strategies
 *
 *		PGLZ_strategy_default		Recommended default strategy for TOAST.
 *
 *		PGLZ_strategy_always		Try to compress inputs of any length.
 *									Fallback to uncompressed storage only if
 *									output would be larger than input.
 * ----------
 */
extern const PGLZ_Strategy *const PGLZ_strategy_default;
extern const PGLZ_Strategy *const PGLZ_strategy_always;

/*
 * Compression algorithm.
 */

extern int	compression_algorithm;

/* ----------
 * Global function declarations
 * ----------
 */
extern int32 pglz_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize, bool check_complete);
extern int32 lz4_decompress(const char *source, int32 slen, char *dest,
				int32 rawsize, bool check_complete);
extern int32 pg_compress(const char *source, int32 slen, char *dest, int32 capacity,
			const PGLZ_Strategy *strategy);
extern int32 pg_decompress(const char *source, int32 slen, char *dest,
			  int32 rawsize, bool check_complete);

extern int32 pg_compress_bound(int32 slen);

#endif							/* _PG_LZCOMPRESS_H_ */
