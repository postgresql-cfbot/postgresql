/* ----------
 * pg_lzcompress.c -
 *
 *		This is an implementation of LZ compression for PostgreSQL.
 *		It uses a simple history table and generates 2-3 byte tags
 *		capable of backward copy information for 3-273 bytes with
 *		a max offset of 4095.
 *
 *		Entry routines:
 *
 *			int32
 *			pglz_compress(const char *source, int32 slen, char *dest,
 *						  const PGLZ_Strategy *strategy);
 *
 *				source is the input data to be compressed.
 *
 *				slen is the length of the input data.
 *
 *				dest is the output area for the compressed result.
 *					It must be at least as big as PGLZ_MAX_OUTPUT(slen).
 *
 *				strategy is a pointer to some information controlling
 *					the compression algorithm. If NULL, the compiled
 *					in default strategy is used.
 *
 *				The return value is the number of bytes written in the
 *				buffer dest, or -1 if compression fails; in the latter
 *				case the contents of dest are undefined.
 *
 *			int32
 *			pglz_decompress(const char *source, int32 slen, char *dest,
 *							int32 rawsize, bool check_complete)
 *
 *				source is the compressed input.
 *
 *				slen is the length of the compressed input.
 *
 *				dest is the area where the uncompressed data will be
 *					written to. It is the callers responsibility to
 *					provide enough space.
 *
 *					The data is written to buff exactly as it was handed
 *					to pglz_compress(). No terminating zero byte is added.
 *
 *				rawsize is the length of the uncompressed data.
 *
 *				check_complete is a flag to let us know if -1 should be
 *					returned in cases where we don't reach the end of the
 *					source or dest buffers, or not.  This should be false
 *					if the caller is asking for only a partial result and
 *					true otherwise.
 *
 *				The return value is the number of bytes written in the
 *				buffer dest, or -1 if decompression fails.
 *
 *		The decompression algorithm and internal data format:
 *
 *			It is made with the compressed data itself.
 *
 *			The data representation is easiest explained by describing
 *			the process of decompression.
 *
 *			If compressed_size == rawsize, then the data
 *			is stored uncompressed as plain bytes. Thus, the decompressor
 *			simply copies rawsize bytes to the destination.
 *
 *			Otherwise the first byte tells what to do the next 8 times.
 *			We call this the control byte.
 *
 *			An unset bit in the control byte means, that one uncompressed
 *			byte follows, which is copied from input to output.
 *
 *			A set bit in the control byte means, that a tag of 2-3 bytes
 *			follows. A tag contains information to copy some bytes, that
 *			are already in the output buffer, to the current location in
 *			the output. Let's call the three tag bytes T1, T2 and T3. The
 *			position of the data to copy is coded as an offset from the
 *			actual output position.
 *
 *			The offset is in the upper nibble of T1 and in T2.
 *			The length is in the lower nibble of T1.
 *
 *			So the 16 bits of a 2 byte tag are coded as
 *
 *				7---T1--0  7---T2--0
 *				OOOO LLLL  OOOO OOOO
 *
 *			This limits the offset to 1-4095 (12 bits) and the length
 *			to 3-18 (4 bits) because 3 is always added to it. To emit
 *			a tag of 2 bytes with a length of 2 only saves one control
 *			bit. But we lose one byte in the possible length of a tag.
 *
 *			In the actual implementation, the 2 byte tag's length is
 *			limited to 3-17, because the value 0xF in the length nibble
 *			has special meaning. It means, that the next following
 *			byte (T3) has to be added to the length value of 18. That
 *			makes total limits of 1-4095 for offset and 3-273 for length.
 *
 *			Now that we have successfully decoded a tag. We simply copy
 *			the output that occurred <offset> bytes back to the current
 *			output location in the specified <length>. Thus, a
 *			sequence of 200 spaces (think about bpchar fields) could be
 *			coded in 4 bytes. One literal space and a three byte tag to
 *			copy 199 bytes with a -1 offset. Whow - that's a compression
 *			rate of 98%! Well, the implementation needs to save the
 *			original data size too, so we need another 4 bytes for it
 *			and end up with a total compression rate of 96%, what's still
 *			worth a Whow.
 *
 *		The compression algorithm
 *
 *			The following uses numbers used in the default strategy.
 *
 *			The compressor works best for attributes of a size between
 *			1K and 1M. For smaller items there's not that much chance of
 *			redundancy in the character sequence (except for large areas
 *			of identical bytes like trailing spaces) and for bigger ones
 *			our 4K maximum look-back distance is too small.
 *
 *			The compressor creates a table for lists of positions.
 *			For each input position (except the last 4), a hash key is
 *			built from the 4 next input bytes and the position remembered
 *			in the appropriate list. Thus, the table points to linked
 *			lists of likely to be at least in the first 4 characters
 *			matching strings. This is done on the fly while the input
 *			is compressed into the output area.  Table entries are only
 *			kept for the last 4094 input positions, since we cannot use
 *			back-pointers larger than that anyway.  The size of the hash
 *			table is chosen based on the size of the input - a larger table
 *			has a larger startup cost, as it needs to be initialized to
 *			zero, but reduces the number of hash collisions on long inputs.
 *
 *			For each byte in the input, its hash key (built from this
 *			byte and the next 3) is used to find the appropriate list
 *			in the table. The lists remember the positions of all bytes
 *			that had the same hash key in the past in increasing backward
 *			offset order. Now for all entries in the used lists, the
 *			match length is computed by comparing the characters from the
 *			entries position with the characters from the actual input
 *			position.
 *
 *			The compressor starts with a so called "good_match" of 128.
 *			It is a "prefer speed against compression ratio" optimizer.
 *			So if the first entry looked at already has 128 or more
 *			matching characters, the lookup stops and that position is
 *			used for the next tag in the output.
 *
 *			For each subsequent entry in the history list, the "good_match"
 *			is lowered roughly by 10%. So the compressor will be more happy
 *			with short matches the farer it has to go back in the history.
 *			Another "speed against ratio" preference characteristic of
 *			the algorithm.
 *
 *			Thus there are 2 stop conditions for the lookup of matches:
 *
 *				- a match >= good_match is found
 *				- there are no more history entries to look at
 *
 *			Finally the match algorithm checks that at least a match
 *			of 3 or more bytes has been found, because that is the smallest
 *			amount of copy information to code into a tag. If so, a tag
 *			is omitted and all the input bytes covered by that are just
 *			scanned for the history add's, otherwise a literal character
 *			is omitted and only his history entry added.
 *
 *		Acknowledgments:
 *
 *			Many thanks to Adisak Pochanayon, who's article about SLZ
 *			inspired me to write the PostgreSQL compression this way.
 *
 *			Jan Wieck
 *
 *			Many thanks to Yann Collet for his article about unaligned
 *			memory access.
 *
 *			Leskov Vladimir
 *
 * Copyright (c) 1999-2019, PostgreSQL Global Development Group
 *
 * src/common/pg_lzcompress.c
 * ----------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <limits.h>

#include "common/pg_lzcompress.h"


/**************************************
 *  CPU Feature Detection			  *
 **************************************/
/* PGLZ_FORCE_MEMORY_ACCESS
 * By default, access to unaligned memory is controlled by `memcpy()`, which is safe and portable.
 * Unfortunately, on some target/compiler combinations, the generated assembly is sub-optimal.
 * The below switch allow to select different access method for improved performance.
 * Method 0 (default) : use `memcpy()`. Safe and portable.
 * Method 1 : direct access. This method is portable but violate C standard.
 *			It can generate buggy code on targets which assembly generation depends on alignment.
 *			But in some circumstances, it's the only known way to get the most performance (ie GCC + ARMv6)
 * See https://fastcompression.blogspot.fr/2015/08/accessing-unaligned-memory.html for details.
 * Prefer these methods in priority order (0 > 1)
 */
#ifndef PGLZ_FORCE_MEMORY_ACCESS   /* can be defined externally */
#  if defined(__GNUC__) && \
  ( defined(__ARM_ARCH_6__) || defined(__ARM_ARCH_6J__) || defined(__ARM_ARCH_6K__) \
  || defined(__ARM_ARCH_6Z__) || defined(__ARM_ARCH_6ZK__) || defined(__ARM_ARCH_6T2__) )
#	define PGLZ_FORCE_MEMORY_ACCESS 1
#  endif
#endif


#if defined(__x86_64__)
  typedef uint64 reg_t;   /* 64-bits in x32 mode */
#else
  typedef size_t reg_t;   /* 32-bits in x32 mode */
#endif


#if defined(PGLZ_FORCE_MEMORY_ACCESS) && (PGLZ_FORCE_MEMORY_ACCESS==1)
/* lie to the compiler about data alignment; use with caution */

static uint32 pglz_read32(const void* ptr) { return *(const uint32*) ptr; }

#else  /* safe and portable access using memcpy() */

static uint32 pglz_read32(const void* ptr)
{
	uint32 val; memcpy(&val, ptr, sizeof(val)); return val;
}

#endif /* PGLZ_FORCE_MEMORY_ACCESS */


/* ----------
 * Local definitions
 * ----------
 */
#define PGLZ_MAX_HISTORY_LISTS	8192	/* must be power of 2 */
#define PGLZ_HISTORY_SIZE		0x0fff - 1	/* to avoid compare in iteration */
#define PGLZ_MAX_MATCH			273


/* ----------
 * PGLZ_HistEntry -
 *
 *		Linked list for the backward history lookup
 *
 * All the entries sharing a hash key are linked in a singly linked list.
 * Links are not changed during insertion in order to speed it up.
 * Instead more complicated stop condition is used during list iteration. 
 * ----------
 */
typedef struct PGLZ_HistEntry
{
	int16 next_id;					  /* links for my hash key's list */
	uint16 hist_idx;					/* my current hash key */
	const unsigned char *pos;			/* my input position */
} PGLZ_HistEntry;


/* ----------
 * The provided standard strategies
 * ----------
 */
static const PGLZ_Strategy strategy_default_data = {
	32,							/* Data chunks less than 32 bytes are not
								 * compressed */
	INT_MAX,					/* No upper limit on what we'll try to
								 * compress */
	25,							/* Require 25% compression rate, or not worth
								 * it */
	1024,						/* Give up if no compression in the first 1KB */
	128,						/* Stop history lookup if a match of 128 bytes
								 * is found */
	10							/* Lower good match size by 10% at every loop
								 * iteration */
};
const PGLZ_Strategy *const PGLZ_strategy_default = &strategy_default_data;


static const PGLZ_Strategy strategy_always_data = {
	0,							/* Chunks of any size are compressed */
	INT_MAX,
	0,							/* It's enough to save one single byte */
	INT_MAX,					/* Never give up early */
	128,						/* Stop history lookup if a match of 128 bytes
								 * is found */
	6							/* Look harder for a good match */
};
const PGLZ_Strategy *const PGLZ_strategy_always = &strategy_always_data;


/* ----------
 * Statically allocated work arrays for history
 * ----------
 */
static int16 hist_start[PGLZ_MAX_HISTORY_LISTS];
static PGLZ_HistEntry hist_entries[PGLZ_HISTORY_SIZE + 1];

/*
 * Element 0 in hist_entries is unused, and means 'invalid'. Likewise,
 * INVALID_ENTRY_PTR in next/prev pointers mean 'invalid'.
 */
#define INVALID_ENTRY			0
#define INVALID_ENTRY_PTR		(&hist_entries[INVALID_ENTRY])

/* ----------
 * pglz_hist_idx -
 *
 *		Computes the history table slot for the lookup by the next 4
 *		characters in the input.
 *
 * NB: because we use the next 4 characters, we are not guaranteed to
 * find 3-character matches; they very possibly will be in the wrong
 * hash list.  This seems an acceptable tradeoff for spreading out the
 * hash keys more.
 * ----------
 */
static inline uint16 pglz_hist_idx(const unsigned char* s, uint16 mask)
{
	return ((s[0] << 6) ^ (s[1] << 4) ^ (s[2] << 2) ^ s[3]) & mask;
}


/* ----------
 * pglz_hist_add -
 *
 *		Adds a new entry to the history table
 *		and recalculates hash value.
 * ----------
 */
static inline int16
pglz_hist_add(int16 hist_next, uint16 *hist_idx, const unsigned char* s, uint16 mask)
{
	int16* my_hist_start = &hist_start[*hist_idx];
	PGLZ_HistEntry* entry = &(hist_entries)[hist_next];

	/*
	 * Initialize entry with a new value.
	 */ 
	entry->next_id = *my_hist_start;
	entry->hist_idx = *hist_idx;
	entry->pos = s;

	/*
	 * Update linked list head pointer.
	 */
	*my_hist_start = hist_next;

	/*
	 * Recalculate hash value.
	 */
	*hist_idx = ((((*hist_idx) ^ (s[0] << 6)) << 2) ^ s[4]) & mask;

	/*
	 * Shift history pointer.
	 */
	hist_next++;
	if (hist_next == PGLZ_HISTORY_SIZE + 1)
	{
		hist_next = 1;
	}
	return hist_next;
}


/* ----------
 * pglz_out_tag -
 *
 *		Outputs a backward reference tag of 2-4 bytes (depending on
 *		offset and length) to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
static inline unsigned char*
pglz_out_tag(unsigned char* dest_ptr, int32 match_len, int32 match_offset)
{
	if (match_len > 17)
	{
		*(dest_ptr++) = (unsigned char)((((match_offset) & 0xf00) >> 4) | 0x0f);
		*(dest_ptr++) = (unsigned char)(((match_offset) & 0xff));
		*(dest_ptr++) = (unsigned char)((match_len) - 18);
	}
	else
	{
		*(dest_ptr++) = (unsigned char)((((match_offset) & 0xf00) >> 4) | ((match_len) - 3));
		*(dest_ptr++) = (unsigned char)((match_offset) & 0xff);
	}
	return dest_ptr;
}


/* ----------
 * pglz_compare -
 *
 *		Compares two sequences of bytes
 *		and returns positiion of first mismatch.
 * ----------
 */
static inline int32
pglz_compare(int32 len, int32 len_bound, const unsigned char* input_pos,
					   const unsigned char* hist_pos)
{
	while(len <= len_bound - 4 && pglz_read32(input_pos) == pglz_read32(hist_pos))
	{
		len += 4;
		input_pos += 4;
		hist_pos += 4;
	}
	while (len < len_bound && *input_pos == *hist_pos)
	{
		len++;
		input_pos++;
		hist_pos++;
	}
	return len;
}


/* ----------
 * pglz_find_match -
 *
 *		Lookup the history table if the actual input stream matches
 *		another sequence of characters, starting somewhere earlier
 *		in the input buffer.
 * ----------
 */
static inline int
pglz_find_match(uint16 hist_idx, const unsigned char *input, const unsigned char *end,
				int *len_ptr, int *offset_ptr, int good_match, int good_drop)
{
	PGLZ_HistEntry *hist_entry;
	int16		*hist_entry_number;
	int32		len = 0;
	int32		offset = 0;
	int32		cur_len = 0;
	int32	   len_bound = Min(end - input, PGLZ_MAX_MATCH);

	/*
	 * Traverse the linked history list until a good enough match is found.
	 */
	hist_entry_number = &hist_start[hist_idx];
	if (*hist_entry_number == INVALID_ENTRY)
		return 0;

	hist_entry = &hist_entries[*hist_entry_number];
	if (hist_idx != hist_entry->hist_idx)
	{
		/*
		 * If current linked list head points to invalid entry
		 * then clear it to reduce a number of comparisons in future.
		 */
		*hist_entry_number = INVALID_ENTRY;
		return 0;
	}

	while (true)
	{
		const unsigned char *input_pos = input;
		const unsigned char *hist_pos = hist_entry->pos;
		const unsigned char *my_pos;
		int32		cur_offset = input_pos - hist_pos;

		/*
		 * Determine length of match. A better match must be larger than the
		 * best so far. And if we already have a match of 16 or more bytes,
		 * it's worth the call overhead to use memcmp() to check if this match
		 * is equal for the same size. After that we must fallback to
		 * character by character comparison to know the exact position where
		 * the diff occurred.
		 */
		if (len >= 16)
		{
			if (memcmp(input_pos, hist_pos, len) == 0)
			{
				offset = cur_offset;
				len = pglz_compare(len, len_bound, input_pos + len, hist_pos + len);
			}
		}
		else
		{
			if (pglz_read32(input_pos) == pglz_read32(hist_pos))
			{
				cur_len = pglz_compare(4, len_bound, input_pos + 4, hist_pos + 4);
				if (cur_len > len)
				{
					len = cur_len;
					offset = cur_offset;
				}
			}
		}

		/*
		 * Advance to the next history entry
		 */
		my_pos = hist_entry->pos;
		hist_entry = &hist_entries[hist_entry->next_id];

		/*
		 * If current match length is ok then stop iteration.
		 * As outdated list links are not updated during insertion process
		 * then additional stop condition should be introduced to avoid following them.
		 * If recycled entry has another hash, then iteration stops.
		 * If it has the same hash then recycled cell would break input stream
		 * position monotonicity what is checked after.
		 */
		if (len >= good_match || hist_idx != hist_entry->hist_idx || my_pos <= hist_entry->pos)
		{
			break;
		}
		/*
		 * Be happy with lesser good matches the more entries we visited.
		 */
		good_match -= (good_match * good_drop) >> 7;
	}

	/*
	 * As initial compare for short matches compares 4 bytes
	 * then for the end of stream length of match should be cut
	 */
	len = Min(len, len_bound);

	/*
	 * Return match information only if it results at least in one byte
	 * reduction.
	 */
	if (len > 2)
	{
		*len_ptr = len;
		*offset_ptr = offset;
		return 1;
	}

	return 0;
}


/* ----------
 * pglz_compress -
 *
 *		Compresses source into dest using strategy. Returns the number of
 *		bytes written in buffer dest, or -1 if compression fails.
 * ----------
 */
int32
pglz_compress(const char *source, int32 src_len, char *dest,
			  const PGLZ_Strategy *strategy)
{
	unsigned char *dest_ptr = (unsigned char *) dest;
	unsigned char *dest_start = dest_ptr;
	uint16			hist_next = 1;
	uint16		  hist_idx;
	const unsigned char *src_ptr = (const unsigned char*)source;
	const unsigned char *src_end = (const unsigned char*)source + src_len;
	const unsigned char *compress_src_end = src_end - 4;
	unsigned char control_dummy = 0;
	unsigned char *control_ptr = &control_dummy;
	unsigned char control_byte = 0;
	unsigned char control_pos = 0;
	bool		found_match = false;
	int32		match_len;
	int32		match_offset;
	int32		good_match;
	int32		good_drop;
	int32		result_size;
	int32		result_max;
	int32		need_rate;
	int			hash_size;
	uint16		mask;


	/*
	 * Our fallback strategy is the default.
	 */
	if (strategy == NULL)
	{
		strategy = PGLZ_strategy_default;
	}

	/*
	 * If the strategy forbids compression (at all or if source chunk size out
	 * of range), fail.
	 */
	if (strategy->match_size_good <= 0 ||
		src_len < strategy->min_input_size ||
		src_len > strategy->max_input_size)
		return -1;

	/*
	 * Limit the match parameters to the supported range.
	 */
	good_match = strategy->match_size_good;
	if (good_match > PGLZ_MAX_MATCH)
		good_match = PGLZ_MAX_MATCH;
	else if (good_match < 17)
		good_match = 17;

	good_drop = strategy->match_size_drop;
	if (good_drop < 0)
		good_drop = 0;
	else if (good_drop > 100)
		good_drop = 100;
	good_drop = good_drop * 128 / 100;

	need_rate = strategy->min_comp_rate;
	if (need_rate < 0)
		need_rate = 0;
	else if (need_rate > 99)
		need_rate = 99;

	/*
	 * Compute the maximum result size allowed by the strategy, namely the
	 * input size minus the minimum wanted compression rate.  This had better
	 * be <= src_len, else we might overrun the provided output buffer.
	 */
	if (src_len > (INT_MAX / 100))
	{
		/* Approximate to avoid overflow */
		result_max = (src_len / 100) * (100 - need_rate);
	}
	else
	{
		result_max = (src_len * (100 - need_rate)) / 100;
	}

	/*
	 * Experiments suggest that these hash sizes work pretty well. A large
	 * hash table minimizes collision, but has a higher startup cost. For a
	 * small input, the startup cost dominates. The table size must be a power
	 * of two.
	 */
	if (src_len < 128)
		hash_size = 512;
	else if (src_len < 256)
		hash_size = 1024;
	else if (src_len < 512)
		hash_size = 2048;
	else if (src_len < 1024)
		hash_size = 4096;
	else
		hash_size = 8192;
	mask = hash_size - 1;

	/*
	 * Initialize the history lists to empty.  We do not need to zero the
	 * hist_entries[] array; its entries are initialized as they are used.
	 */
	memset(hist_start, 0, hash_size * sizeof(int16));

	/*
	 * Initialize INVALID_ENTRY for stopping during lookup.
	 */
	hist_entries[INVALID_ENTRY].pos = src_end;
	hist_entries[INVALID_ENTRY].hist_idx = hash_size;

	/*
	 * Calculate initial hash value.
	 */
	hist_idx = pglz_hist_idx(src_ptr, mask);

	/*
	 * Compress the source directly into the output buffer.
	 */
	while (src_ptr < compress_src_end)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (dest_ptr - dest_start >= result_max)
			return -1;

		/*
		 * If we've emitted more than first_success_by bytes without finding
		 * anything compressible at all, fail.  This lets us fall out
		 * reasonably quickly when looking at incompressible input (such as
		 * pre-compressed data).
		 */
		if (!found_match && dest_ptr - dest_start >= strategy->first_success_by)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
		if ((control_pos & 0xff) == 0)												
		{																		
			*(control_ptr) = control_byte;												
			control_ptr = (dest_ptr)++;												
			control_byte = 0;														
			control_pos = 1;															
		}																		
		/*
		 * Try to find a match in the history
		 */
		if (pglz_find_match(hist_idx, src_ptr, compress_src_end, &match_len,
							&match_offset, good_match, good_drop))
		{
			/*
			 * Create the tag and add history entries for all matched
			 * characters.
			 */
			control_byte |= control_pos;														
			dest_ptr = pglz_out_tag(dest_ptr, match_len, match_offset);
			while (match_len--)
			{
				hist_next = pglz_hist_add(hist_next, &hist_idx, src_ptr, mask);
				src_ptr++;			/* Do not do this ++ in the line above! */
				/* The macro would do it four times - Jan.  */
			}
			found_match = true;
		}
		else
		{
			/*
			 * No match found. Copy one literal byte.
			 */
			hist_next = pglz_hist_add(hist_next, &hist_idx, src_ptr, mask);
			*(dest_ptr)++ = (unsigned char)(*src_ptr);
			src_ptr++;				/* Do not do this ++ in the line above! */
			/* The macro would do it four times - Jan.  */
		}
		control_pos <<= 1;
	}


	while (src_ptr < src_end)
	{
		/*
		 * If we already exceeded the maximum result size, fail.
		 *
		 * We check once per loop; since the loop body could emit as many as 4
		 * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
		 * allow 4 slop bytes.
		 */
		if (dest_ptr - dest_start >= result_max)
			return -1;

		/*
		 * Refresh control byte if needed.
		 */
		if ((control_pos & 0xff) == 0)
		{
			*(control_ptr) = control_byte;
			control_ptr = (dest_ptr)++;
			control_byte = 0;
			control_pos = 1;
		}
		*(dest_ptr)++ = (unsigned char)(*src_ptr);
		src_ptr++;
		control_pos <<= 1;
	}


	/*
	 * Write out the last control byte and check that we haven't overrun the
	 * output size allowed by the strategy.
	 */
	*control_ptr = control_byte;
	result_size = dest_ptr - dest_start;
	if (result_size >= result_max)
		return -1;

	/* success */
	return result_size;
}

/* ----------
 * pglz_decompress -
 *
 *		Decompresses source into dest. Returns the number of bytes
 *		decompressed in the destination buffer, and *optionally*
 *		checks that both the source and dest buffers have been
 *		fully read and written to, respectively.
 * ----------
 */
int32
pglz_decompress(const char *source, int32 slen, char *dest,
						 int32 rawsize, bool check_complete)
{
	const unsigned char *sp;
	const unsigned char *srcend;
	unsigned char *dp;
	unsigned char *destend;

	sp = (const unsigned char *) source;
	srcend = ((const unsigned char *) source) + slen;
	dp = (unsigned char *) dest;
	destend = dp + rawsize;

	while (sp < srcend && dp < destend)
	{
		/*
		 * Read one control byte and process the next 8 items (or as many as
		 * remain in the compressed input).
		 */
		unsigned char ctrl = *sp++;
		int			ctrlc;

		for (ctrlc = 0; ctrlc < 8 && sp < srcend && dp < destend; ctrlc++)
		{

			if (ctrl & 1)
			{
				/*
				 * Otherwise it contains the match length minus 3 and the
				 * upper 4 bits of the offset. The next following byte
				 * contains the lower 8 bits of the offset. If the length is
				 * coded as 18, another extension tag byte tells how much
				 * longer the match really was (0-255).
				 */
				int32		len;
				int32		off;

				len = (sp[0] & 0x0f) + 3;
				off = ((sp[0] & 0xf0) << 4) | sp[1];
				sp += 2;
				if (len == 18)
					len += *sp++;

				/*
				 * Now we copy the bytes specified by the tag from OUTPUT to
				 * OUTPUT. It is dangerous and platform dependent to use
				 * memcpy() here, because the copied areas could overlap
				 * extremely!
				 */
				len = Min(len, destend - dp);
				while (len--)
				{
					*dp = dp[-off];
					dp++;
				}
			}
			else
			{
				/*
				 * An unset control bit means LITERAL BYTE. So we just copy
				 * one from INPUT to OUTPUT.
				 */
				*dp++ = *sp++;
			}

			/*
			 * Advance the control bit
			 */
			ctrl >>= 1;
		}
	}

	/*
	 * Check we decompressed the right amount.
	 * If we are slicing, then we won't necessarily
	 * be at the end of the source or dest buffers
	 * when we hit a stop, so we don't test them.
	 */
	if (check_complete && (dp != destend || sp != srcend))
		return -1;

	/*
	 * That's it.
	 */
	return (char*)dp - dest;
}
