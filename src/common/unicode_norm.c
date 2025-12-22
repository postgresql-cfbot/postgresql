/*-------------------------------------------------------------------------
 * unicode_norm.c
 *		Normalize a Unicode string
 *
 * This implements Unicode normalization, per the documentation at
 * https://www.unicode.org/reports/tr15/.
 *
 * Portions Copyright (c) 2017-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/unicode_norm.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/unicode_norm.h"
#include "common/unicode_norm_table.h"
#ifndef FRONTEND
#include "common/unicode_normprops_table.h"
#include "port/pg_bswap.h"
#include "utils/memutils.h"
#endif

#ifndef FRONTEND
#define ALLOC(size) palloc(size)
#define FREE(size) pfree(size)
#else
#define ALLOC(size) malloc(size)
#define FREE(size) free(size)
#endif

/* Constants for calculations with Hangul characters */
#define SBASE		0xAC00		/* U+AC00 */
#define LBASE		0x1100		/* U+1100 */
#define VBASE		0x1161		/* U+1161 */
#define TBASE		0x11A7		/* U+11A7 */
#define LCOUNT		19
#define VCOUNT		21
#define TCOUNT		28
#define NCOUNT		VCOUNT * TCOUNT
#define SCOUNT		LCOUNT * NCOUNT

/*
 * get_code_entry
 *
 * Get the entry corresponding to code in the decomposition lookup table.
 * The backend version of this code uses a perfect hash function for the
 * lookup, while the frontend version uses a binary search.
 */
static const pg_unicode_decomposition *
get_code_entry(char32_t code)
{
	uint16		idx = normalization_index(code);

	return idx != 0 ? &UnicodeDecompMain[idx] : NULL;
}

/*
 * Get the combining class of the given codepoint.
 */
static uint8
get_canonical_class(char32_t code)
{
	const pg_unicode_decomposition *entry = get_code_entry(code);

	/*
	 * If no entries are found, the character used is either a Hangul
	 * character or a character with a class of 0 and no decompositions.
	 */
	if (!entry)
		return 0;
	else
		return entry->comb_class;
}

/*
 * Given a decomposition entry looked up earlier, get the decomposed
 * characters.
 *
 * Note: the returned pointer can point to statically allocated buffer, and
 * is only valid until next call to this function!
 */
static const char32_t *
get_code_decomposition(const pg_unicode_decomposition *entry, int *dec_size)
{
	static char32_t x;

	if (DECOMPOSITION_IS_INLINE(entry))
	{
		Assert(DECOMPOSITION_SIZE(entry) == 1);
		x = (char32_t) entry->dec_index;
		*dec_size = 1;
		return &x;
	}

	*dec_size = DECOMPOSITION_SIZE(entry);
	return &UnicodeDecompCodepoints[entry->dec_index];

}

static const char32_t *
get_code_compat_decomposition(const pg_unicode_decomposition *entry,
							  int *dec_size)
{
	static char32_t x;

	if (DECOMPOSITION_IS_INLINE(entry))
	{
		x = (char32_t) entry->dec_index;
		*dec_size = 1;
		return &x;
	}

	*dec_size = DECOMPOSITION_COMPAT_SIZE(entry);
	if (*dec_size > 0)
		return &UnicodeDecompCodepoints[entry->dec_index
										+ DECOMPOSITION_SIZE(entry)];

	*dec_size = DECOMPOSITION_SIZE(entry);
	return &UnicodeDecompCodepoints[entry->dec_index];
}

/*
 * Given a decomposition entry looked up earlier, get the decomposed size.
 */
static inline int
get_code_size(const pg_unicode_decomposition *entry)
{
	if (DECOMPOSITION_IS_INLINE(entry))
		return 1;

	return DECOMPOSITION_SIZE(entry);

}

static inline int
get_code_compat_size(const pg_unicode_decomposition *entry)
{
	int			size;

	if (DECOMPOSITION_IS_INLINE(entry))
		return 1;

	size = DECOMPOSITION_COMPAT_SIZE(entry);
	if (size > 0)
		return size;

	return DECOMPOSITION_SIZE(entry);
}

/*
 * Calculate how many characters long the decomposed version will be.
 */
static bool
get_decomposed_size(const char32_t *p, bool compat, int *decomp_size)
{
	char32_t	code;
	const pg_unicode_decomposition *entry;
	Size		size = 0;

	while ((code = *p++))
	{
		int			code_size;

		/*
		 * Fast path for Hangul characters not stored in tables to save memory
		 * as decomposition is algorithmic. See
		 * https://www.unicode.org/reports/tr15/tr15-18.html, annex 10 for
		 * details on the matter.
		 */
		if (code >= SBASE && code < SBASE + SCOUNT)
		{
			uint32		tindex,
						sindex;

			sindex = code - SBASE;
			tindex = sindex % TCOUNT;

			if (tindex != 0)
				code_size = 3;
			else
				code_size = 2;
		}
		else
		{
			entry = get_code_entry(code);

			/*
			 * Just count current code if no other decompositions.  A NULL
			 * entry is equivalent to a character with class 0 and no
			 * decompositions.
			 */
			if (entry == NULL || entry->dec_index == 0 ||
				(!compat && DECOMPOSITION_IS_COMPAT(entry)))
				code_size = 1;

			/*
			 * If this entry has other decomposition codes look at them as
			 * well. First get its decomposition in the list of tables
			 * available.
			 */
			else if (!compat)
				code_size = get_code_size(entry);
			else
				code_size = get_code_compat_size(entry);
		}

		size += code_size;
		if (unlikely(size > MaxAllocSize / sizeof(char32_t)))
		{
			*decomp_size = (int) size;
			return false;
		}
	}

	*decomp_size = (int) size;
	return true;
}

/*
 * Recompose a set of characters. For hangul characters, the calculation
 * is algorithmic. For others, an inverse lookup at the decomposition
 * table is necessary. Returns true if a recomposition can be done, and
 * false otherwise.
 */
static inline bool
recompose_code(uint32 start, uint32 code, uint32 *result)
{
	/*
	 * Handle Hangul characters algorithmically, per the Unicode spec.
	 *
	 * Check if two current characters are L and V.
	 */
	if (start >= LBASE && start < LBASE + LCOUNT &&
		code >= VBASE && code < VBASE + VCOUNT)
	{
		/* make syllable of form LV */
		uint32		lindex = start - LBASE;
		uint32		vindex = code - VBASE;

		*result = SBASE + (lindex * VCOUNT + vindex) * TCOUNT;
		return true;
	}
	/* Check if two current characters are LV and T */
	else if (start >= SBASE && start < (SBASE + SCOUNT) &&
			 ((start - SBASE) % TCOUNT) == 0 &&
			 code > TBASE && code < (TBASE + TCOUNT))
	{
		/* make syllable of form LVT */
		uint32		tindex = code - TBASE;

		*result = start + tindex;
		return true;
	}

	*result = normalization_inverse(start, code);

	return *result != 0;
}

/*
 * Canonical ordering.
 */
static void
unicode_canonical_reorder(char32_t *decomps, char32_t *last_starter,
						  uint8 *cur_class)
{
	int			i,
				length;
	uint8		ccc;
	char32_t	tmp;

	/*
	 * Reordering occurs from starter to starter. There cannot be another
	 * starter between starters. Therefore, there is no need to perform a
	 * combining class check on 0.
	 */

	length = (int) (last_starter - decomps);

	for (i = 1; i < length; i++)
	{
		/*
		 * Per Unicode (https://www.unicode.org/reports/tr15/tr15-18.html)
		 * annex 4, a sequence of two adjacent characters in a string is an
		 * exchangeable pair if the combining class (from the Unicode
		 * Character Database) for the first character is greater than the
		 * combining class for the second, and the second is not a starter.  A
		 * character is a starter if its combining class is 0.
		 */
		if (cur_class[i - 1] <= cur_class[i])
			continue;

		/* exchange can happen */
		tmp = decomps[i - 1];
		decomps[i - 1] = decomps[i];
		decomps[i] = tmp;

		ccc = cur_class[i - 1];
		cur_class[i - 1] = cur_class[i];
		cur_class[i] = ccc;

		/* backtrack to check again */
		if (i > 1)
			i -= 2;
	}
}

static char32_t *
decomposition(const char32_t *p, char32_t *decomps,
			  uint8 *cur_class, bool compat)
{
	int			length;
	uint32		l,
				v,
				tindex,
				sindex;
	char32_t	cp,
			   *next_after_starter;
	const char32_t *cps;
	const pg_unicode_decomposition *entry;
	uint8	   *next_after_class;

	next_after_starter = decomps;
	next_after_class = cur_class;

	while ((cp = *p++))
	{
		/*
		 * Fast path for Hangul characters not stored in tables to save memory
		 * as decomposition is algorithmic. See
		 * https://www.unicode.org/reports/tr15/tr15-18.html, annex 10 for
		 * details on the matter.
		 */
		if (cp >= SBASE && cp < SBASE + SCOUNT)
		{
			sindex = cp - SBASE;
			l = LBASE + sindex / (VCOUNT * TCOUNT);
			v = VBASE + (sindex % (VCOUNT * TCOUNT)) / TCOUNT;
			tindex = sindex % TCOUNT;

			if (decomps - next_after_starter > 1)
				unicode_canonical_reorder(next_after_starter, decomps,
										  next_after_class);

			*decomps++ = l;
			*cur_class++ = 0;

			*decomps++ = v;
			*cur_class++ = 0;

			if (tindex != 0)
			{
				*decomps++ = TBASE + tindex;
				*cur_class++ = 0;
			}

			next_after_class = cur_class;
			next_after_starter = decomps;

			continue;
		}

		entry = get_code_entry(cp);

		/*
		 * Just fill in with the current decomposition if there are no
		 * decomposition codes.  A NULL entry is equivalent to a character
		 * with class 0 and no decompositions, so just leave also in this
		 * case.
		 */
		if (entry == NULL || entry->dec_index == 0
			|| (!compat && DECOMPOSITION_IS_COMPAT(entry)))
		{
			*decomps++ = cp;

			if (entry != NULL && entry->comb_class > 0)
			{
				*cur_class++ = entry->comb_class;
				continue;
			}

			*cur_class++ = 0;

			if (decomps - next_after_starter > 1)
				unicode_canonical_reorder(next_after_starter, decomps - 1,
										  next_after_class);

			next_after_class = cur_class;
			next_after_starter = decomps;

			continue;
		}

		/*
		 * Recursion is not required in the decomposition; the data was
		 * expanded in advance during the formation of the decomposition
		 * tables.
		 */
		if (!compat)
			cps = get_code_decomposition(entry, &length);
		else
			cps = get_code_compat_decomposition(entry, &length);

		for (int i = 0; i < length; i++)
		{
			char32_t	ccc;
			const char32_t lcode = cps[i];

			ccc = get_canonical_class(lcode);

			*decomps++ = lcode;
			*cur_class++ = ccc;

			if (ccc == 0)
			{
				char32_t   *starter = decomps - 1;

				if (starter - next_after_starter > 1)
					unicode_canonical_reorder(next_after_starter, starter,
											  next_after_class);

				next_after_starter = decomps;
				next_after_class = cur_class;
			}
		}
	}

	if (decomps - next_after_starter > 1)
		unicode_canonical_reorder(next_after_starter, decomps,
								  next_after_class);

	*decomps = '\0';

	return decomps;
}

/*
 * unicode_normalize - Normalize a Unicode string to the specified form.
 *
 * The input is a 0-terminated array of codepoints.
 *
 * In frontend, returns a 0-terminated array of codepoints, allocated with
 * malloc. Or NULL if we run out of memory. In backend, the returned
 * string is palloc'd instead, and OOM is reported with ereport().
 */
char32_t *
unicode_normalize(UnicodeNormalizationForm form, const char32_t *input)
{
	bool		compat = (form == UNICODE_NFKC || form == UNICODE_NFKD);
	bool		recompose = (form == UNICODE_NFC || form == UNICODE_NFKC);
	int			decomp_len,
				i,
				w;
	char32_t   *decomps,
			   *decomps_end,
			   *starter,
				composed;
	uint8	   *classes,
				prev_ccc;
	uint8		class_buf[512];

	/*
	 * Calculate how many characters long the decomposed version will be.
	 *
	 * Some characters decompose to quite a few code points, so that the
	 * decomposed version's size could overrun MaxAllocSize, and even 32-bit
	 * size_t, even though the input string presumably fits in that.  In
	 * frontend we want to just return NULL in that case, so monitor the sum
	 * and exit early once we'd need more than MaxAllocSize bytes.
	 */
	if (!get_decomposed_size(input, compat, &decomp_len))
	{
#ifndef FRONTEND
		/* Let palloc() throw the appropriate error below. */
#else
		/* Just return NULL with no explicit error. */
		return NULL;
#endif
	}

	decomps = (char32_t *) ALLOC((decomp_len + 1) * sizeof(char32_t));
	if (decomps == NULL)
		return NULL;

	/*
	 * We will cache all combining classes to reduce the number of visits to
	 * data tables.
	 */
	if (decomp_len <= sizeof(class_buf))
	{
		classes = class_buf;
	}
	else
	{
		classes = (uint8 *) ALLOC(decomp_len * sizeof(uint8));
		if (classes == NULL)
		{
			FREE(decomps);
			return NULL;
		}
	}

	decomps_end = decomposition(input, decomps, classes, compat);

	if (!recompose)
		goto done;

	starter = NULL;
	decomp_len = (int) (decomps_end - decomps);

	/*
	 * Find the first starter. This is necessary in order to avoid checking
	 * for the presence of a starter in the main recomposition cycle.
	 */
	for (i = 0; i < decomp_len; i++)
	{
		if (classes[i] == 0)
		{
			starter = &decomps[i];
			i += 1;
			break;
		}
	}

	if (starter == NULL)
		goto done;

	prev_ccc = 0;

	/*
	 * The last phase of NFC and NFKC is the recomposition of the reordered
	 * Unicode string using combining classes. The recomposed string cannot be
	 * longer than the decomposed one, so make the allocation of the output
	 * string based on that assumption.
	 */
	for (w = i; i < decomp_len; i++)
	{
		char32_t	ch = decomps[i];
		uint8		ccc = classes[i];

		if (prev_ccc != 0 && prev_ccc >= ccc)
		{
			if (ccc == 0)
			{
				starter = &decomps[w];
				prev_ccc = 0;
			}
			else
				prev_ccc = ccc;

			decomps[w++] = ch;
			continue;
		}

		if (recompose_code(*starter, ch, &composed))
		{
			*starter = composed;
			continue;
		}

		if (ccc == 0)
			starter = &decomps[w];

		decomps[w++] = ch;
		prev_ccc = ccc;
	}

	decomps[w] = '\0';

done:

	if (classes != class_buf)
		FREE(classes);

	return decomps;
}

/*
 * Normalization "quick check" algorithm; see
 * <http://www.unicode.org/reports/tr15/#Detecting_Normalization_Forms>
 */

/* We only need this in the backend. */
#ifndef FRONTEND

static const pg_unicode_normprops *
qc_hash_lookup(char32_t ch, const pg_unicode_norminfo *norminfo)
{
	int			h;
	uint32		hashkey;

	/*
	 * Compute the hash function. The hash key is the codepoint with the bytes
	 * in network order.
	 */
	hashkey = pg_hton32(ch);
	h = norminfo->hash(&hashkey);

	/* An out-of-range result implies no match */
	if (h < 0 || h >= norminfo->num_normprops)
		return NULL;

	/*
	 * Since it's a perfect hash, we need only match to the specific codepoint
	 * it identifies.
	 */
	if (ch != norminfo->normprops[h].codepoint)
		return NULL;

	/* Success! */
	return &norminfo->normprops[h];
}

/*
 * Look up the normalization quick check character property
 */
static UnicodeNormalizationQC
qc_is_allowed(UnicodeNormalizationForm form, char32_t ch)
{
	const pg_unicode_normprops *found = NULL;

	switch (form)
	{
		case UNICODE_NFC:
			found = qc_hash_lookup(ch, &UnicodeNormInfo_NFC_QC);
			break;
		case UNICODE_NFKC:
			found = qc_hash_lookup(ch, &UnicodeNormInfo_NFKC_QC);
			break;
		default:
			Assert(false);
			break;
	}

	if (found)
		return found->quickcheck;
	else
		return UNICODE_NORM_QC_YES;
}

UnicodeNormalizationQC
unicode_is_normalized_quickcheck(UnicodeNormalizationForm form, const char32_t *input)
{
	uint8		lastCanonicalClass = 0;
	UnicodeNormalizationQC result = UNICODE_NORM_QC_YES;

	/*
	 * For the "D" forms, we don't run the quickcheck.  We don't include the
	 * lookup tables for those because they are huge, checking for these
	 * particular forms is less common, and running the slow path is faster
	 * for the "D" forms than the "C" forms because you don't need to
	 * recompose, which is slow.
	 */
	if (form == UNICODE_NFD || form == UNICODE_NFKD)
		return UNICODE_NORM_QC_MAYBE;

	for (const char32_t *p = input; *p; p++)
	{
		char32_t	ch = *p;
		uint8		canonicalClass;
		UnicodeNormalizationQC check;

		canonicalClass = get_canonical_class(ch);
		if (lastCanonicalClass > canonicalClass && canonicalClass != 0)
			return UNICODE_NORM_QC_NO;

		check = qc_is_allowed(form, ch);
		if (check == UNICODE_NORM_QC_NO)
			return UNICODE_NORM_QC_NO;
		else if (check == UNICODE_NORM_QC_MAYBE)
			result = UNICODE_NORM_QC_MAYBE;

		lastCanonicalClass = canonicalClass;
	}
	return result;
}

#endif							/* !FRONTEND */
