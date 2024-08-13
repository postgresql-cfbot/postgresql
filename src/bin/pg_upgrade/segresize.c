/*
 *	segresize.c
 *
 *	SLRU segment resize utility
 *
 *	Copyright (c) 2024, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/segresize.c
 */

#include "postgres_fe.h"

#include "pg_upgrade.h"
#include "access/multixact.h"

/* See slru.h */
#define SLRU_PAGES_PER_SEGMENT		32

/*
 * Some kind of iterator associated with a particular SLRU segment.  The idea is
 * to specify the segment and page number and then move through the pages.
 */
typedef struct SlruSegState
{
	char	   *dir;
	char	   *fn;
	FILE	   *file;
	int64		segno;
	uint64		pageno;
	bool		leading_gap;
	bool		long_segment_names;
} SlruSegState;

/*
 * Get SLRU segmen file name from state.
 *
 * NOTE: this function should mirror SlruFileName call.
 */
static inline char *
SlruFileName(SlruSegState *state)
{
	if (state->long_segment_names)
	{
		Assert(state->segno >= 0 &&
			   state->segno <= INT64CONST(0xFFFFFFFFFFFFFFF));
		return psprintf("%s/%015llX", state->dir, (long long) state->segno);
	}
	else
	{
		Assert(state->segno >= 0 &&
			   state->segno <= INT64CONST(0xFFFFFF));
		return psprintf("%s/%04X", state->dir, (unsigned int) state->segno);
	}
}

/*
 * Create SLRU segment file.
 */
static void
create_segment(SlruSegState *state)
{
	Assert(state->fn == NULL);
	Assert(state->file == NULL);

	state->fn = SlruFileName(state);
	state->file = fopen(state->fn, "wb");
	if (!state->file)
		pg_fatal("could not create file \"%s\": %m", state->fn);
}

/*
 * Open existing SLRU segment file.
 */
static void
open_segment(SlruSegState *state)
{
	Assert(state->fn == NULL);
	Assert(state->file == NULL);

	state->fn = SlruFileName(state);
	state->file = fopen(state->fn, "rb");
	if (!state->file)
		pg_fatal("could not open file \"%s\": %m", state->fn);
}

/*
 * Close SLRU segment file.
 */
static void
close_segment(SlruSegState *state)
{
	if (state->file)
	{
		fclose(state->file);
		state->file = NULL;
	}

	if (state->fn)
	{
		pfree(state->fn);
		state->fn = NULL;
	}
}

/*
 * Read next page from the old 32-bit offset segment file.
 */
static int
read_old_segment_page(SlruSegState *state, void *buf, bool *empty)
{
	int		len;

	/* Open next segment file, if needed. */
	if (!state->fn)
	{
		if (!state->segno)
			state->leading_gap = true;

		open_segment(state);

		/* Set position to the needed page. */
		if (state->pageno > 0 &&
			fseek(state->file, state->pageno * BLCKSZ, SEEK_SET))
		{
			close_segment(state);
		}
	}

	if (state->file)
	{
		/* Segment file do exists, read page from it. */
		state->leading_gap = false;

		len = fread(buf, sizeof(char), BLCKSZ, state->file);

		/* Are we done or was there an error? */
		if (len <= 0)
		{
			if (ferror(state->file))
				pg_fatal("error reading file \"%s\": %m", state->fn);

			if (feof(state->file))
			{
				*empty = true;
				len = -1;

				close_segment(state);
			}
		}
		else
			*empty = false;
	}
	else if (!state->leading_gap)
	{
		/* We reached the last segment. */
		len = -1;
		*empty = true;
	}
	else
	{
		/* Skip few first segments if they were frozen and removed. */
		len = BLCKSZ;
		*empty = true;
	}

	if (++state->pageno >= SLRU_PAGES_PER_SEGMENT)
	{
		/* Start a new segment. */
		state->segno++;
		state->pageno = 0;

		close_segment(state);
	}

	return len;
}

/*
 * Write next page to the new 64-bit offset segment file.
 */
static void
write_new_segment_page(SlruSegState *state, void *buf)
{
	/*
	 * Create a new segment file if we still didn't.  Creation is
	 * postponed until the first non-empty page is found.  This helps
	 * not to create completely empty segments.
	 */
	if (!state->file)
	{
		create_segment(state);

		/* Write zeroes to the previously skipped prefix. */
		if (state->pageno > 0)
		{
			char		zerobuf[BLCKSZ] = {0};

			for (int64 i = 0; i < state->pageno; i++)
			{
				if (fwrite(zerobuf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
					pg_fatal("could not write file \"%s\": %m", state->fn);
			}
		}
	}

	/* Write page to the new segment (if it was created). */
	if (state->file)
	{
		if (fwrite(buf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
			pg_fatal("could not write file \"%s\": %m", state->fn);
	}

	state->pageno++;

	/*
	 * Did we reach the maximum page number?  Then close segment file
	 * and create a new one on the next iteration.
	 */
	if (state->pageno >= SLRU_PAGES_PER_SEGMENT)
	{
		state->segno++;
		state->pageno = 0;
		close_segment(state);
	}
}

/*
 * Convert pg_multixact/offsets segments and return oldest multi offset.
 */
uint64
convert_multixact_offsets(void)
{
	/* See multixact.c */
#define MULTIXACT_OFFSETS_PER_PAGE_OLD	(BLCKSZ / sizeof(uint32))
#define MULTIXACT_OFFSETS_PER_PAGE		(BLCKSZ / sizeof(MultiXactOffset))

	SlruSegState	oldseg = {0},
					newseg = {0};
	uint32			oldbuf[MULTIXACT_OFFSETS_PER_PAGE_OLD] = {0};
	MultiXactOffset	newbuf[MULTIXACT_OFFSETS_PER_PAGE] = {0};
	/*
	 * It is much easier to deal with multi wraparound in 64 bitd format.  Thus
	 * we use 64 bits for multi-transactions, although they remain 32 bits.
	 */
	uint64			oldest_multi = old_cluster.controldata.chkpnt_oldstMulti,
					next_multi = old_cluster.controldata.chkpnt_nxtmulti,
					multi,
					old_entry,
					new_entry;
	bool			found = false;
	uint64			oldest_offset = 0;

	prep_status("Converting pg_multixact/offsets to 64-bit");

	oldseg.pageno = oldest_multi / MULTIXACT_OFFSETS_PER_PAGE_OLD;
	oldseg.segno = oldseg.pageno / SLRU_PAGES_PER_SEGMENT;
	oldseg.pageno %= SLRU_PAGES_PER_SEGMENT;
	oldseg.dir = psprintf("%s/pg_multixact/offsets", old_cluster.pgdata);
	oldseg.long_segment_names = false;		/* old format XXXX */

	newseg.pageno = oldest_multi / MULTIXACT_OFFSETS_PER_PAGE;
	newseg.segno = newseg.pageno / SLRU_PAGES_PER_SEGMENT;
	newseg.pageno %= SLRU_PAGES_PER_SEGMENT;
	newseg.dir = psprintf("%s/pg_multixact/offsets", new_cluster.pgdata);
	newseg.long_segment_names = true;

	old_entry = oldest_multi % MULTIXACT_OFFSETS_PER_PAGE_OLD;
	new_entry = oldest_multi % MULTIXACT_OFFSETS_PER_PAGE;

	if (next_multi < oldest_multi)
		next_multi += (uint64) 1 << 32;		/* wraparound */

	for (multi = oldest_multi; multi < next_multi; old_entry = 0)
	{
		int			oldlen;
		bool		empty;

		/* Handle possible segment wraparound. */
		if (oldseg.segno > MaxMultiXactId /
								MULTIXACT_OFFSETS_PER_PAGE_OLD /
								SLRU_PAGES_PER_SEGMENT)
			oldseg.segno = 0;

		/* Read old offset segment. */
		oldlen = read_old_segment_page(&oldseg, oldbuf, &empty);
		if (oldlen <= 0 || empty)
			pg_fatal("cannot read page %llu from file \"%s\": %m",
					 (unsigned long long) oldseg.pageno, oldseg.fn);

		/* Fill possible gap. */
		if (oldlen < BLCKSZ)
			memset((char *) oldbuf + oldlen, 0, BLCKSZ - oldlen);

		/* Save oldest multi offset */
		if (!found)
		{
			oldest_offset = oldbuf[old_entry];
			found = true;
		}

		/* ... skip wrapped-around invalid multi */
		if (multi == (uint64) 1 << 32)
		{
			Assert(oldseg.segno == 0);
			Assert(oldseg.pageno == 1);
			Assert(old_entry == 0);

			multi += FirstMultiXactId;
			old_entry = FirstMultiXactId;
		}

		/* Copy entries to the new page. */
		for (; multi < next_multi && old_entry < MULTIXACT_OFFSETS_PER_PAGE_OLD;
			 multi++, old_entry++)
		{
			MultiXactOffset offset = oldbuf[old_entry];

			/* Handle possible offset wraparound. */
			if (offset < oldest_offset)
				offset += ((uint64) 1 << 32) - 1;

			/* Subtract oldest_offset, so new offsets will start from 1. */
			newbuf[new_entry++] = offset - oldest_offset + 1;
			if (new_entry >= MULTIXACT_OFFSETS_PER_PAGE)
			{
				/* Write a new page. */
				write_new_segment_page(&newseg, newbuf);
				new_entry = 0;
			}
		}
	}

	/* Write the last incomplete page. */
	if (new_entry > 0 || oldest_multi == next_multi)
	{
		memset(&newbuf[new_entry], 0,
			   sizeof(newbuf[0]) * (MULTIXACT_OFFSETS_PER_PAGE - new_entry));
		write_new_segment_page(&newseg, newbuf);
	}

	/* Release resources. */
	close_segment(&oldseg);
	close_segment(&newseg);

	pfree(oldseg.dir);
	pfree(newseg.dir);

	check_ok();

	return oldest_offset;
}
