/*-------------------------------------------------------------------------
 *
 * generic_xlog.c
 *	 Implementation of generic xlog records.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/generic_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/generic_xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/*-------------------------------------------------------------------------
 * Internally, a delta between pages consists of a set of fragments.  Each
 * fragment represents changes made in a given region of a page.  A fragment
 * is made up as follows:
 *
 * - offset of page region (OffsetNumber)
 * - length of page region (OffsetNumber)
 * - data - the data to place into the region ('length' number of bytes)
 *
 * Unchanged regions of a page are not represented in its delta.  As a result,
 * a delta can be more compact than the full page image.  But having an
 * unchanged region between two fragments that is smaller than the fragment
 * header (offset+length) does not pay off in terms of the overall size of
 * the delta.  For this reason, we merge adjacent fragments if the unchanged
 * region between them is <= MATCH_THRESHOLD bytes.
 *
 * We do not bother to merge fragments across the "lower" and "upper" parts
 * of a page; it's very seldom the case that pd_lower and pd_upper are within
 * MATCH_THRESHOLD bytes of each other, and handling that infrequent case
 * would complicate and slow down the delta-computation code unduly.
 * Therefore, the worst-case delta size includes two fragment headers plus
 * a full page's worth of data.
 *-------------------------------------------------------------------------
 */
#define FRAGMENT_HEADER_SIZE	(2 * (sizeof(OffsetNumber) + \
									  sizeof(char) + sizeof(int)))
#define MATCH_THRESHOLD			FRAGMENT_HEADER_SIZE
#define MAX_DELTA_SIZE			(BLCKSZ + 2 * FRAGMENT_HEADER_SIZE) + sizeof(bool)

#define MAX_ALIGN_MISMATCHES	64
/* MAX_ALIGN_MISMATCHES is supposed to be not greater than PG_UINT8_MAX */
#define MIN_DELTA_DIFFERECE		12
#define ALIGN_GAP				256

#define writeToPtr(ptr, x)		memcpy(ptr, &(x), sizeof(x)), ptr += sizeof(x)
#define readFromPtr(ptr, x)		memcpy(&(x), ptr, sizeof(x)), ptr += sizeof(x)

/* Struct of generic xlog data for single page */
typedef struct
{
	Buffer		buffer;			/* registered buffer */
	int			flags;			/* flags for this buffer */
	int			deltaLen;		/* space consumed in delta field */
	char	   *image;			/* copy of page image for modification, do not
								 * do it in-place to have aligned memory chunk */
	char		delta[MAX_DELTA_SIZE];	/* delta between page images */
} PageData;

/* State of generic xlog record construction */
struct GenericXLogState
{
	/*
	 * page's images. Should be first in this struct to have MAXALIGN'ed
	 * images addresses, because some code working with pages directly aligns
	 * addresses, not offsets from beginning of page
	 */
	char		images[MAX_GENERIC_XLOG_PAGES * BLCKSZ];
	PageData	pages[MAX_GENERIC_XLOG_PAGES];
	bool		isLogged;
};

/* Describes for the region which type of delta is used in it */
typedef enum
{
	DIFF_DELTA,					/* diff delta with insert, remove and replace
								 * operations */
	BASE_DELTA,					/* base delta with update operations only */
}	DeltaType;

/* Diff delta operations for transforming current page to target page */
typedef enum
{
	DIFF_INSERT,
	DIFF_REMOVE,
	DIFF_REPLACE,
}	DiffDeltaOperations;

static void writeFragment(PageData *pageData, OffsetNumber offset,
			  OffsetNumber len, const char *data);
static void computeRegionDelta(PageData *pageData,
				   const char *curpage, const char *targetpage,
				   int targetStart, int targetEnd,
				   int validStart, int validEnd);
static void computeDelta(PageData *pageData, Page curpage, Page targetpage);
static void applyPageRedo(Page page, const char *delta, Size deltaSize);

static int alignRegions(const char *curRegion, const char *targetRegion,
			 int curRegionLen, int targetRegionLen);

static bool computeRegionDiffDelta(PageData *pageData,
					   const char *curpage, const char *targetpage,
					   int targetStart, int targetEnd,
					   int validStart, int validEnd);
static const char *applyPageDiffRedo(Page page, const char *delta, Size deltaSize);

static void computeRegionBaseDelta(PageData *pageData,
					   const char *curpage, const char *targetpage,
					   int targetStart, int targetEnd,
					   int validStart, int validEnd);
static const char *applyPageBaseRedo(Page page, const char *delta, Size deltaSize);

static bool containsDiffDelta(PageData *pageData);
static void downgradeDeltaToBaseFormat(PageData *pageData);

/* Arrays for the alignment building and for the resulting alignments */
static int	V[MAX_ALIGN_MISMATCHES + 1][2 * MAX_ALIGN_MISMATCHES + 1];
static int16 curRegionAligned[BLCKSZ + MAX_ALIGN_MISMATCHES];
static int16 targetRegionAligned[BLCKSZ + MAX_ALIGN_MISMATCHES];

/* Array for diff delta application */
static char localPage[BLCKSZ];


/*
 * Write next fragment into pageData's delta.
 *
 * The fragment has the given offset and length, and data points to the
 * actual data (of length length).
 */
static void
writeFragment(PageData *pageData, OffsetNumber offset, OffsetNumber length,
			  const char *data)
{
	char	   *ptr = pageData->delta + pageData->deltaLen;

	/* Verify we have enough space */
	Assert(pageData->deltaLen + sizeof(offset) +
		   sizeof(length) + length <= MAX_DELTA_SIZE);

	/* Write fragment data */
	writeToPtr(ptr, offset);
	writeToPtr(ptr, length);

	memcpy(ptr, data, length);
	ptr += length;

	pageData->deltaLen = ptr - pageData->delta;
}

/*
 * Compute the XLOG fragments needed to transform a region of curpage into the
 * corresponding region of targetpage, and append them to pageData's delta
 * field. The region to transform runs from targetStart to targetEnd-1.
 * Bytes in curpage outside the range validStart to validEnd-1 should be
 * considered invalid, and always overwritten with target data.
 *
 * This function tries to build diff delta first and, if it fails, uses
 * the base delta. It also provides the header before the delta in which
 * the type and the length of the delta are contained.
 */
static void
computeRegionDelta(PageData *pageData,
				   const char *curpage, const char *targetpage,
				   int targetStart, int targetEnd,
				   int validStart, int validEnd)
{
	int			length;
	char		header;
	bool		diff;
	int			prevDeltaLen;
	char	   *ptr = pageData->delta + pageData->deltaLen;

	/* Verify we have enough space */
	Assert(pageData->deltaLen + sizeof(header) +
		   sizeof(length) <= MAX_DELTA_SIZE);

	pageData->deltaLen += sizeof(header) + sizeof(length);
	prevDeltaLen = pageData->deltaLen;
	diff = computeRegionDiffDelta(pageData,
								  curpage, targetpage,
								  targetStart, targetEnd,
								  validStart, validEnd);
	/*
	 * If we succeeded to make diff delta, just set the header.
	 * Otherwise, make base delta.
	 */
	if (diff)
	{
		header = DIFF_DELTA;
	}
	else
	{
		header = BASE_DELTA;
		computeRegionBaseDelta(pageData,
							   curpage, targetpage,
							   targetStart, targetEnd,
							   validStart, validEnd);
	}
	length = pageData->deltaLen - prevDeltaLen;

	writeToPtr(ptr, header);
	writeToPtr(ptr, length);
}

/*
 * Compute the XLOG fragments needed to transform a region of curpage into the
 * corresponding region of targetpage, and append them to pageData's delta
 * field. The region to transform runs from targetStart to targetEnd-1.
 * Bytes in curpage outside the range validStart to validEnd-1 should be
 * considered invalid, and always overwritten with target data.
 *
 * This function is a hot spot, so it's worth being as tense as possible
 * about the data-matching loops.
 */
static void
computeRegionBaseDelta(PageData *pageData,
					   const char *curpage, const char *targetpage,
					   int targetStart, int targetEnd,
					   int validStart, int validEnd)
{
	int			i,
				loopEnd,
				fragmentBegin = -1,
				fragmentEnd = -1;

	/* Deal with any invalid start region by including it in first fragment */
	if (validStart > targetStart)
	{
		fragmentBegin = targetStart;
		targetStart = validStart;
	}

	/* We'll deal with any invalid end region after the main loop */
	loopEnd = Min(targetEnd, validEnd);

	/* Examine all the potentially matchable bytes */
	i = targetStart;
	while (i < loopEnd)
	{
		if (curpage[i] != targetpage[i])
		{
			/* On unmatched byte, start new fragment if not already in one */
			if (fragmentBegin < 0)
				fragmentBegin = i;
			/* Mark unmatched-data endpoint as uncertain */
			fragmentEnd = -1;
			/* Extend the fragment as far as possible in a tight loop */
			i++;
			while (i < loopEnd && curpage[i] != targetpage[i])
				i++;
			if (i >= loopEnd)
				break;
		}

		/* Found a matched byte, so remember end of unmatched fragment */
		fragmentEnd = i;

		/*
		 * Extend the match as far as possible in a tight loop.  (On typical
		 * workloads, this inner loop is the bulk of this function's runtime.)
		 */
		i++;
		while (i < loopEnd && curpage[i] == targetpage[i])
			i++;

		/*
		 * There are several possible cases at this point:
		 *
		 * 1. We have no unwritten fragment (fragmentBegin < 0).  There's
		 * nothing to write; and it doesn't matter what fragmentEnd is.
		 *
		 * 2. We found more than MATCH_THRESHOLD consecutive matching bytes.
		 * Dump out the unwritten fragment, stopping at fragmentEnd.
		 *
		 * 3. The match extends to loopEnd.  We'll do nothing here, exit the
		 * loop, and then dump the unwritten fragment, after merging it with
		 * the invalid end region if any.  If we don't so merge, fragmentEnd
		 * establishes how much the final writeFragment call needs to write.
		 *
		 * 4. We found an unmatched byte before loopEnd.  The loop will repeat
		 * and will enter the unmatched-byte stanza above.  So in this case
		 * also, it doesn't matter what fragmentEnd is.  The matched bytes
		 * will get merged into the continuing unmatched fragment.
		 *
		 * Only in case 3 do we reach the bottom of the loop with a meaningful
		 * fragmentEnd value, which is why it's OK that we unconditionally
		 * assign "fragmentEnd = i" above.
		 */
		if (fragmentBegin >= 0 && i - fragmentEnd > MATCH_THRESHOLD)
		{
			writeFragment(pageData, fragmentBegin,
						  fragmentEnd - fragmentBegin,
						  targetpage + fragmentBegin);
			fragmentBegin = -1;
			fragmentEnd = -1;	/* not really necessary */
		}
	}

	/* Deal with any invalid end region by including it in final fragment */
	if (loopEnd < targetEnd)
	{
		if (fragmentBegin < 0)
			fragmentBegin = loopEnd;
		fragmentEnd = targetEnd;
	}

	/* Write final fragment if any */
	if (fragmentBegin >= 0)
	{
		if (fragmentEnd < 0)
			fragmentEnd = targetEnd;
		writeFragment(pageData, fragmentBegin,
					  fragmentEnd - fragmentBegin,
					  targetpage + fragmentBegin);
	}
}

/*
 * Align curRegion and targetRegion and return the length of the alignment
 * or -1 if the alignment with number of mismathing positions less than
 * or equal to MAX_ALIGN_MISMATCHES does not exist.
 * The alignment is stored in curRegionAligned and targetRegionAligned.
 * The algorithm guarantees to find the alignment with the least possible
 * number of mismathing positions or return that such least number is greater
 * than MAX_ALIGN_MISMATCHES.
 *
 * The implemented algorithm is a modification of that was described in
 * the paper "An O(ND) Difference Algorithm and Its Variations". We chose
 * the algorithm which requires O(N + D^2) memory with time complexity O(ND),
 * because it is faster than another one which requires O(N + D) memory and
 * O(ND) time. The only modification we made is the introduction of REPLACE
 * operations, while in the original algorithm only INSERT and REMOVE
 * are considered.
 */
static int
alignRegions(const char *curRegion, const char *targetRegion,
			 int curRegionLen, int targetRegionLen)
{
	/* Number of mismatches */
	int			m;

	/* Difference between curRegion and targetRegion prefix lengths */
	int			k;

	/* Curbuf and targetRegion prefix lengths */
	int			i,
				j;
	int			resLen;
	int			numMismatches = -1;

	/*
	 * V is an array to store the values of dynamic programming. The first
	 * dimension corresponds to m, and the second one is for k + m.
	 */
	V[0][0] = 0;

	/* Find the longest path with the given number of mismatches */
	for (m = 0; m <= MAX_ALIGN_MISMATCHES; ++m)
	{
		/*
		 * Find the largest prefix alignment with the given number of
		 * mismatches and the given difference between curRegion and
		 * targetRegion prefix lengths.
		 */
		for (k = -m; k <= m; ++k)
		{
			/* Dynamic programming recurrent step */
			if (m > 0)
			{
				if (k == 0 && m == 1)
					i = 1;
				else if (k == -m || (k != m &&
						  V[m - 1][m - 1 + k - 1] < V[m - 1][m - 1 + k + 1]))
					i = V[m - 1][m - 1 + k + 1];
				else
					i = V[m - 1][m - 1 + k - 1] + 1;
				if (k != -m && k != m && V[m - 1][m - 1 + k] + 1 > i)
					i = V[m - 1][m - 1 + k] + 1;
			}
			else
				i = 0;
			j = i - k;

			/* Boundary checks */
			Assert(i >= 0);
			Assert(j >= 0);

			/* Increase the prefixes while the bytes are equal */
			while (i < curRegionLen && j < targetRegionLen &&
				   curRegion[i] == targetRegion[j])
				i++, j++;

			/*
			 * Save the largest curRegion prefix that was aligned with given
			 * number of mismatches and difference between curRegion and
			 * targetRegion prefix lengths.
			 */
			V[m][m + k] = i;

			/* If we find the alignment, save its length and break */
			if (i == curRegionLen && j == targetRegionLen)
			{
				numMismatches = m;
				break;
			}
		}
		/* Break if we find an alignment */
		if (numMismatches != -1)
			break;
	}
	/* No alignment was found */
	if (numMismatches == -1)
		return -1;

	/* Restore the reversed alignment */
	resLen = 0;
	while (i != 0 || j != 0)
	{
		/* Check cycle invariants */
		Assert(i >= 0 && j >= 0);
		Assert(m >= 0);
		Assert(-m <= k && k <= m);

		/* Rollback the equal bytes */
		while (i != 0 && j != 0 && curRegion[i - 1] == targetRegion[j - 1])
		{
			curRegionAligned[resLen] = curRegion[--i];
			targetRegionAligned[resLen] = targetRegion[--j];
			resLen++;
		}
		Assert(i >= 0 && j >= 0);

		/* Break if we reach the start point */
		if (i == 0 && j == 0)
			break;

		/* Do the backward dynamic programming step */
		if ((k == 0 && m == 1) ||
			(k != -m && k != m && V[m - 1][m - 1 + k] + 1 == i))
		{
			curRegionAligned[resLen] = curRegion[--i];
			targetRegionAligned[resLen] = targetRegion[--j];
			resLen++;
			m -= 1;
		}
		else if (k == -m || (k != m &&
						  V[m - 1][m - 1 + k - 1] < V[m - 1][m - 1 + k + 1]))
		{
			curRegionAligned[resLen] = ALIGN_GAP;
			targetRegionAligned[resLen] = targetRegion[--j];
			resLen++;
			m -= 1, k += 1;
		}
		else
		{
			curRegionAligned[resLen] = curRegion[--i];
			targetRegionAligned[resLen] = ALIGN_GAP;
			resLen++;
			m -= 1, k -= 1;
		}
	}
	Assert(m == 0 && k == 0);

	/* Reverse alignment */
	for (i = 0, j = resLen - 1; i < j; ++i, --j)
	{
		int16		t;

		t = curRegionAligned[i];
		curRegionAligned[i] = curRegionAligned[j];
		curRegionAligned[j] = t;
		t = targetRegionAligned[i];
		targetRegionAligned[i] = targetRegionAligned[j];
		targetRegionAligned[j] = t;
	}

	return resLen;
}

/*
 * Try to build a short alignment in order to produce a short diff delta.
 * If fails, return false, otherwise return true and write the delta to
 * pageData->delta.
 * Also return false if the produced alignment is not much better than
 * the alignment with DIFF_REPLACE operations only (the minimal difference
 * is set in MIN_DELTA_DIFFERECE in order to avoid cases when diff delta
 * is larger than base delta because of the greater header size).
 */
static bool
computeRegionDiffDelta(PageData *pageData,
					   const char *curpage, const char *targetpage,
					   int targetStart, int targetEnd,
					   int validStart, int validEnd)
{
	char	   *ptr = pageData->delta + pageData->deltaLen;
	int			i,
				j;
	char		type;
	char		len;
	OffsetNumber start;
	OffsetNumber tmp;

	int			baseAlignmentCost = 0;
	int			diffAlignmentCost = 0;
	int			alignmentLength;

	alignmentLength = alignRegions(&curpage[validStart],
								   &targetpage[targetStart],
								   validEnd - validStart,
								   targetEnd - targetStart);

	/* If no proper alignment was found return false */
	if (alignmentLength < 0)
		return false;

	/* Compute the cost of found alignment */
	for (i = 0; i < alignmentLength; ++i)
		diffAlignmentCost += (curRegionAligned[i] != targetRegionAligned[i]);

	/*
	 * The following cycle computes the cost of alignment with DIFF_REPLACE
	 * operations only (similar to as if the previous delta was used). The
	 * position is match if both regions don't contain it, or if both regions
	 * contain that position and the bytes on it are equal. Otherwise the
	 * position is mismatch with cost 1.
	 */
	for (i = Min(validStart, targetStart); i < validEnd || i < targetEnd; ++i)
		baseAlignmentCost += (i < validStart || i < targetStart ||
							  i >= validEnd || i >= targetEnd ||
							  curpage[i] != targetpage[i]);

	/*
	 * Check whether the found alignment is much better than the one with
	 * DIFF_PERLACE operations only.
	 */
	if (baseAlignmentCost - MIN_DELTA_DIFFERECE < diffAlignmentCost)
		return false;

	/*
	 * Translate the alignment into the set of instructions for transformation
	 * from curRegion into targetRegion, and write these instructions into
	 * pageData->delta.
	 */

	/* Verify we have enough space */
	Assert(pageData->deltaLen + 4 * sizeof(tmp) <= MAX_DELTA_SIZE);

	/* Write start and end indexes of the buffers */
	tmp = validStart;
	writeToPtr(ptr, tmp);
	tmp = validEnd;
	writeToPtr(ptr, tmp);
	tmp = targetStart;
	writeToPtr(ptr, tmp);
	tmp = targetEnd;
	writeToPtr(ptr, tmp);

	/* Transform the alignment into the set of instructions */
	start = 0;
	for (i = 0; i < alignmentLength; ++i)
	{
		/* Verify the alignment */
		Assert(curRegionAligned[i] != ALIGN_GAP ||
			   targetRegionAligned[i] != ALIGN_GAP);

		/* If the values are equal, write no instructions */
		if (curRegionAligned[i] == targetRegionAligned[i])
		{
			start++;
			continue;
		}

		if (curRegionAligned[i] == ALIGN_GAP)
			type = DIFF_INSERT;
		else if (targetRegionAligned[i] == ALIGN_GAP)
			type = DIFF_REMOVE;
		else
			type = DIFF_REPLACE;

		/* Find the end of the block of the same instructions */
		j = i + 1;
		while (j < alignmentLength)
		{
			bool		sameType = false;

			switch (type)
			{
				case DIFF_INSERT:
					sameType = (curRegionAligned[j] == ALIGN_GAP);
					break;
				case DIFF_REMOVE:
					sameType = (targetRegionAligned[j] == ALIGN_GAP);
					break;
				case DIFF_REPLACE:
					sameType = (curRegionAligned[j] != ALIGN_GAP &&
								targetRegionAligned[j] != ALIGN_GAP &&
							  curRegionAligned[j] != targetRegionAligned[j]);
					break;
				default:
					elog(ERROR, "unrecognized delta operation type: %d", type);
					break;
			}
			if (sameType)
				j++;
			else
				break;
		}
		len = j - i;

		/* Verify we have enough space */
		Assert(pageData->deltaLen + sizeof(type) +
			   sizeof(len) + sizeof(start) <= MAX_DELTA_SIZE);
		/* Write the header for instruction */
		writeToPtr(ptr, type);
		writeToPtr(ptr, len);
		writeToPtr(ptr, start);

		/* Write instruction data and go to the end of the block */
		if (type != DIFF_REMOVE)
		{
			/* Verify we have enough space */
			Assert(pageData->deltaLen + len <= MAX_DELTA_SIZE);
			while (i < j)
			{
				char		c = targetRegionAligned[i++];

				writeToPtr(ptr, c);
			}
		}
		else
			i = j;
		i--;

		/* Shift start position which shows where to apply instruction */
		if (type != DIFF_INSERT)
			start += len;
	}

	pageData->deltaLen = ptr - pageData->delta;

	return true;
}

/*
 * Return whether pageData->delta contains diff deltas or not.
 */
static bool
containsDiffDelta(PageData *pageData)
{
	char	   *ptr = pageData->delta + sizeof(bool);
	char	   *end = pageData->delta + pageData->deltaLen;
	char		header;
	int			length;

	while (ptr < end)
	{
		readFromPtr(ptr, header);
		readFromPtr(ptr, length);

		if (header == DIFF_DELTA)
			return true;
		ptr += length;
	}
	return false;
}

/*
 * Downgrade pageData->delta to base delta format.
 *
 * Only base diffs are allowed to perform the transformation.
 */
static void
downgradeDeltaToBaseFormat(PageData *pageData)
{
	char	   *ptr = pageData->delta + sizeof(bool);
	char	   *cur = pageData->delta + sizeof(bool);
	char	   *end = pageData->delta + pageData->deltaLen;
	char		header;
	int			length;
	int			newDeltaLength = 0;

	/* Check whether the first byte is false */
	Assert(!*((bool *) pageData->delta));

	while (ptr < end)
	{
		readFromPtr(ptr, header);
		readFromPtr(ptr, length);

		/* Check whether the region delta is base delta */
		Assert(header == BASE_DELTA);
		newDeltaLength += length;

		memmove(cur, ptr, length);
		cur += length;
		ptr += length;
	}
	pageData->deltaLen = newDeltaLength;
}

/*
 * Compute the XLOG delta record needed to transform curpage into targetpage,
 * and store it in pageData's delta field.
 */
static void
computeDelta(PageData *pageData, Page curpage, Page targetpage)
{
	int			targetLower = ((PageHeader) targetpage)->pd_lower,
				targetUpper = ((PageHeader) targetpage)->pd_upper,
				curLower = ((PageHeader) curpage)->pd_lower,
				curUpper = ((PageHeader) curpage)->pd_upper;

	pageData->deltaLen = sizeof(bool);

	/* Compute delta records for lower part of page ... */
	computeRegionDelta(pageData, curpage, targetpage,
					   0, targetLower,
					   0, curLower);
	/* ... and for upper part, ignoring what's between */
	computeRegionDelta(pageData, curpage, targetpage,
					   targetUpper, BLCKSZ,
					   curUpper, BLCKSZ);

	/*
	 * Set first byte to true if at least one of the region deltas
	 * is diff delta. Otherwise set first byte to false and downgrade all
	 * regions to base format without extra headers.
	 */
	*((bool *) pageData->delta) = containsDiffDelta(pageData);
	if (!*((bool *) pageData->delta))
		downgradeDeltaToBaseFormat(pageData);

	/*
	 * If xlog debug is enabled, then check produced delta.  Result of delta
	 * application to curpage should be equivalent to targetpage.
	 */
#ifdef WAL_DEBUG
	if (XLOG_DEBUG)
	{
		char		tmp[BLCKSZ];

		memcpy(tmp, curpage, BLCKSZ);
		applyPageRedo(tmp, pageData->delta, pageData->deltaLen);
		if (memcmp(tmp, targetpage, targetLower) != 0 ||
			memcmp(tmp + targetUpper, targetpage + targetUpper,
				   BLCKSZ - targetUpper) != 0)
			elog(ERROR, "result of generic xlog apply does not match");
	}
#endif
}

/*
 * Start new generic xlog record for modifications to specified relation.
 */
GenericXLogState *
GenericXLogStart(Relation relation)
{
	GenericXLogState *state;
	int			i;

	state = (GenericXLogState *) palloc(sizeof(GenericXLogState));
	state->isLogged = RelationNeedsWAL(relation);

	for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
	{
		state->pages[i].image = state->images + BLCKSZ * i;
		state->pages[i].buffer = InvalidBuffer;
	}

	return state;
}

/*
 * Register new buffer for generic xlog record.
 *
 * Returns pointer to the page's image in the GenericXLogState, which
 * is what the caller should modify.
 *
 * If the buffer is already registered, just return its existing entry.
 * (It's not very clear what to do with the flags in such a case, but
 * for now we stay with the original flags.)
 */
Page
GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer, int flags)
{
	int			block_id;

	/* Search array for existing entry or first unused slot */
	for (block_id = 0; block_id < MAX_GENERIC_XLOG_PAGES; block_id++)
	{
		PageData   *page = &state->pages[block_id];

		if (BufferIsInvalid(page->buffer))
		{
			/* Empty slot, so use it (there cannot be a match later) */
			page->buffer = buffer;
			page->flags = flags;
			memcpy(page->image, BufferGetPage(buffer), BLCKSZ);
			return (Page) page->image;
		}
		else if (page->buffer == buffer)
		{
			/*
			 * Buffer is already registered.  Just return the image, which is
			 * already prepared.
			 */
			return (Page) page->image;
		}
	}

	elog(ERROR, "maximum number %d of generic xlog buffers is exceeded",
		 MAX_GENERIC_XLOG_PAGES);
	/* keep compiler quiet */
	return NULL;
}

/*
 * Apply changes represented by GenericXLogState to the actual buffers,
 * and emit a generic xlog record.
 */
XLogRecPtr
GenericXLogFinish(GenericXLogState *state)
{
	XLogRecPtr	lsn;
	int			i;

	if (state->isLogged)
	{
		/* Logged relation: make xlog record in critical section. */
		XLogBeginInsert();

		START_CRIT_SECTION();

		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];
			Page		page;
			PageHeader	pageHeader;

			if (BufferIsInvalid(pageData->buffer))
				continue;

			page = BufferGetPage(pageData->buffer);
			pageHeader = (PageHeader) pageData->image;

			if (pageData->flags & GENERIC_XLOG_FULL_IMAGE)
			{
				/*
				 * A full-page image does not require us to supply any xlog
				 * data.  Just apply the image, being careful to zero the
				 * "hole" between pd_lower and pd_upper in order to avoid
				 * divergence between actual page state and what replay would
				 * produce.
				 */
				memcpy(page, pageData->image, pageHeader->pd_lower);
				memset(page + pageHeader->pd_lower, 0,
					   pageHeader->pd_upper - pageHeader->pd_lower);
				memcpy(page + pageHeader->pd_upper,
					   pageData->image + pageHeader->pd_upper,
					   BLCKSZ - pageHeader->pd_upper);

				XLogRegisterBuffer(i, pageData->buffer,
								   REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			}
			else
			{
				/*
				 * In normal mode, calculate delta and write it as xlog data
				 * associated with this page.
				 */
				computeDelta(pageData, page, (Page) pageData->image);

				/* Apply the image, with zeroed "hole" as above */
				memcpy(page, pageData->image, pageHeader->pd_lower);
				memset(page + pageHeader->pd_lower, 0,
					   pageHeader->pd_upper - pageHeader->pd_lower);
				memcpy(page + pageHeader->pd_upper,
					   pageData->image + pageHeader->pd_upper,
					   BLCKSZ - pageHeader->pd_upper);

				XLogRegisterBuffer(i, pageData->buffer, REGBUF_STANDARD);
				XLogRegisterBufData(i, pageData->delta, pageData->deltaLen);
			}
		}

		/* Insert xlog record */
		lsn = XLogInsert(RM_GENERIC_ID, 0);

		/* Set LSN and mark buffers dirty */
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];

			if (BufferIsInvalid(pageData->buffer))
				continue;
			PageSetLSN(BufferGetPage(pageData->buffer), lsn);
			MarkBufferDirty(pageData->buffer);
		}
		END_CRIT_SECTION();
	}
	else
	{
		/* Unlogged relation: skip xlog-related stuff */
		START_CRIT_SECTION();
		for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
		{
			PageData   *pageData = &state->pages[i];

			if (BufferIsInvalid(pageData->buffer))
				continue;
			memcpy(BufferGetPage(pageData->buffer),
				   pageData->image,
				   BLCKSZ);
			/* We don't worry about zeroing the "hole" in this case */
			MarkBufferDirty(pageData->buffer);
		}
		END_CRIT_SECTION();
		/* We don't have a LSN to return, in this case */
		lsn = InvalidXLogRecPtr;
	}

	pfree(state);

	return lsn;
}

/*
 * Abort generic xlog record construction.  No changes are applied to buffers.
 *
 * Note: caller is responsible for releasing locks/pins on buffers, if needed.
 */
void
GenericXLogAbort(GenericXLogState *state)
{
	pfree(state);
}

/*
 * Apply delta to given page image.
 *
 * Read blocks of instructions and apply them based on their type:
 * BASE_DELTA or DIFF_DELTA.
 */
static void
applyPageRedo(Page page, const char *delta, Size deltaSize)
{
	const char *ptr = delta;
	const char *end = delta + deltaSize;
	char		header;
	int			length;
	bool		diff_delta;

	/* If page delta is base delta, apply it. */
	readFromPtr(ptr, diff_delta);
	if (!diff_delta)
	{
		applyPageBaseRedo(page, ptr, end - ptr);
		return;
	}

	/* Otherwise apply each region delta. */
	while (ptr < end)
	{
		readFromPtr(ptr, header);
		readFromPtr(ptr, length);

		switch (header)
		{
			case DIFF_DELTA:
				ptr = applyPageDiffRedo(page, ptr, length);
				break;
			case BASE_DELTA:
				ptr = applyPageBaseRedo(page, ptr, length);
				break;
			default:
				elog(ERROR,
					 "unrecognized delta type: %d",
					 header);
				break;
		}
	}
}

/*
 * Apply base delta to given page image.
 */
static const char *
applyPageBaseRedo(Page page, const char *delta, Size deltaSize)
{
	const char *ptr = delta;
	const char *end = delta + deltaSize;

	while (ptr < end)
	{
		OffsetNumber offset,
					length;

		readFromPtr(ptr, offset);
		readFromPtr(ptr, length);

		memcpy(page + offset, ptr, length);

		ptr += length;
	}
	return ptr;
}

/*
 * Apply diff delta to given page image.
 */
static const char *
applyPageDiffRedo(Page page, const char *delta, Size deltaSize)
{
	const char *ptr = delta;
	const char *end = delta + deltaSize;
	OffsetNumber targetStart,
				targetEnd;
	OffsetNumber validStart,
				validEnd;
	int			i,
				j;
	OffsetNumber start;

	/* Read start and end indexes of the buffers */
	readFromPtr(ptr, validStart);
	readFromPtr(ptr, validEnd);
	readFromPtr(ptr, targetStart);
	readFromPtr(ptr, targetEnd);

	/* Read and apply the instructions */
	i = 0, j = 0;
	while (ptr < end)
	{
		char		type;
		char		len;

		/* Read the header of the current instruction */
		readFromPtr(ptr, type);
		readFromPtr(ptr, len);
		readFromPtr(ptr, start);

		/* Copy the data before current instruction to buffer */
		memcpy(&localPage[j], page + validStart + i, start - i);
		j += start - i;
		i = start;

		/* Apply the instruction */
		switch (type)
		{
			case DIFF_INSERT:
				memcpy(&localPage[j], ptr, len);
				ptr += len;
				j += len;
				break;
			case DIFF_REMOVE:
				i += len;
				break;
			case DIFF_REPLACE:
				memcpy(&localPage[j], ptr, len);
				i += len;
				j += len;
				ptr += len;
				break;
			default:
				elog(ERROR,
					 "unrecognized delta instruction type: %d",
					 type);
				break;
		}
	}

	/* Copy the data after the last instruction */
	memcpy(&localPage[j], page + validStart + i, validEnd - validStart - i);
	j += validEnd - validStart - i;
	i = validEnd - validStart;

	memcpy(page + targetStart, localPage, j);
	return ptr;
}

/*
 * Redo function for generic xlog record.
 */
void
generic_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffers[MAX_GENERIC_XLOG_PAGES];
	uint8		block_id;

	/* Protect limited size of buffers[] array */
	Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

	/* Iterate over blocks */
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		XLogRedoAction action;

		if (!XLogRecHasBlockRef(record, block_id))
		{
			buffers[block_id] = InvalidBuffer;
			continue;
		}

		action = XLogReadBufferForRedo(record, block_id, &buffers[block_id]);

		/* Apply redo to given block if needed */
		if (action == BLK_NEEDS_REDO)
		{
			Page		page;
			PageHeader	pageHeader;
			char	   *blockDelta;
			Size		blockDeltaSize;

			page = BufferGetPage(buffers[block_id]);
			blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize);
			applyPageRedo(page, blockDelta, blockDeltaSize);

			/*
			 * Since the delta contains no information about what's in the
			 * "hole" between pd_lower and pd_upper, set that to zero to
			 * ensure we produce the same page state that application of the
			 * logged action by GenericXLogFinish did.
			 */
			pageHeader = (PageHeader) page;
			memset(page + pageHeader->pd_lower, 0,
				   pageHeader->pd_upper - pageHeader->pd_lower);

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffers[block_id]);
		}
	}

	/* Changes are done: unlock and release all buffers */
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (BufferIsValid(buffers[block_id]))
			UnlockReleaseBuffer(buffers[block_id]);
	}
}

/*
 * Mask a generic page before performing consistency checks on it.
 */
void
generic_mask(char *page, BlockNumber blkno)
{
	mask_page_lsn_and_checksum(page);

	mask_unused_space(page);
}
