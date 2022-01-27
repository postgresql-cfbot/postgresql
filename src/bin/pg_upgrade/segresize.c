/*
 *	segresize.c
 *		Segment resize utility
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *	src/bin/pg_upgrade/segresize.c
 */


#include "postgres_fe.h"

#include "pg_upgrade.h"
#include "access/multixact.h"
#include "access/transam.h"


#define OldFileName(path, seg) \
	psprintf("%s/%04X", path, seg)

#define NewFileName(path, seg) \
	psprintf("%s/%04X%08X", path, \
		(uint32) ((seg) >> 32), (uint32) ((seg) & (int64)0xFFFFFFFF))

#define SLRU_PAGES_PER_SEGMENT_OLD 32
#define SLRU_PAGES_PER_SEGMENT_NEW 2048	/* XXX SLRU_PAGES_PER_SEGMENT */


static FILE*
create_target_file(const char *dir, int64 segno, char **pfn)
{
	char *fn;
	FILE *filedesc;

	fn = NewFileName(dir, segno);
	filedesc = fopen(fn, "wb");

	if (!filedesc)
		pg_fatal("Cannot create file: %s", fn);

	if (pfn)
	{
		if (*pfn)
			pfree(*pfn);
		*pfn = fn;
	}
	else
		pfree(fn);

	return filedesc;
}

typedef struct SLRUSegmentState
{
	const char *dir;
	char	   *fn;
	FILE	   *file;
	uint64		segno;
	uint64		pageno;
	bool		leading_gap;
} SLRUSegmentState;

static void
close_segment(SLRUSegmentState *state)
{
	if (state->file != NULL)
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

static int
read_old_segment_page(SLRUSegmentState *state, void *buf, bool *is_empty)
{
	size_t		len;

	/* Open next segment file, if needed */
	if (!state->fn)
	{
		if (!state->segno)
			state->leading_gap = true;
		state->fn = OldFileName(state->dir, (uint32) state->segno);
		state->file = fopen(state->fn, "rb");

		/* Set position to the needed page */
		if (state->file && state->pageno > 0)
		{
			if (fseek(state->file, state->pageno * BLCKSZ, SEEK_SET))
			{
				fclose(state->file);
				state->file = NULL;
			}
		}
	}

	if (state->file)
	{
		/* Segment file do exists, read page from it */
		state->leading_gap = false;

		len = fread(buf, sizeof(char), BLCKSZ, state->file);

		/* Are we done or was there an error? */
		if (len <= 0)
		{
			if (ferror(state->file))
				pg_fatal("Error reading file: %s", state->fn);

			if (feof(state->file))
			{
				*is_empty = true;
				len = -1;
				fclose(state->file);
				state->file = NULL;
			}
		}
		else
			*is_empty = false;
	}
	else if (!state->leading_gap)
	{
		/* We reached the last segment */
		len = -1;
		*is_empty = true;
	}
	else
	{
		/* Skip few first segments if they were frozen and removed */
		len = BLCKSZ;
		*is_empty = true;
	}

	state->pageno++;

	if (state->pageno >= SLRU_PAGES_PER_SEGMENT_OLD)
	{
		/* Start new segment */
		state->segno++;
		state->pageno = 0;
		close_segment(state);
	}

	return (int) len;
}

static void
write_new_segment_page(SLRUSegmentState *state, void *buf, bool is_empty)
{
	/*
	 * Create a new segment file if we still didn't.  Creation is
	 * postponed until the first non-empty page is found.  This helps
	 * not to create completely empty segments.
	 */
	if (!state->file && !is_empty)
	{
		state->file = create_target_file(state->dir, state->segno, &state->fn);

		/* Write zeroes to the previously skipped prefix */
		if (state->pageno > 0)
		{
			char		zerobuf[BLCKSZ] = {0};

			for (int64 i = 0; i < state->pageno; i++)
			{
				if (fwrite(zerobuf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
					pg_fatal("Could not write file: %s", state->fn);
			}
		}
	}

	/* Write page to the new segment (if it was created) */
	if (state->file)
	{
		if (is_empty)
			memset(buf, 0, BLCKSZ);

		if (fwrite(buf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
			pg_fatal("Could not write file: %s", state->fn);
	}

	state->pageno++;

	/*
	 * Did we reach the maximum page number? Then close segment file
	 * and create a new one on the next iteration
	 */
	if (state->pageno >= SLRU_PAGES_PER_SEGMENT_NEW)
	{
		state->segno++;
		state->pageno = 0;
		close_segment(state);
	}
}

#define CLOG_BITS_PER_XACT	2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)

#define MaxTransactionIdOld	((TransactionId) 0xFFFFFFFF)

/*
 * Convert pg_xact segments.
 */
void
convert_clog(const char *old_subdir, const char *new_subdir)
{
	SLRUSegmentState oldseg = {0};
	SLRUSegmentState newseg = {0};
	TransactionId oldest_xid = old_cluster.controldata.chkpnt_oldstxid;
	TransactionId next_xid = old_cluster.controldata.chkpnt_nxtxid;
	TransactionId xid;
	uint64		pageno;
	char		buf[BLCKSZ] = {0};

	oldseg.dir = old_subdir;
	newseg.dir = new_subdir;

	pageno = oldest_xid / CLOG_XACTS_PER_PAGE;

	oldseg.segno = pageno / SLRU_PAGES_PER_SEGMENT_OLD;
	oldseg.pageno = pageno % SLRU_PAGES_PER_SEGMENT_OLD;

	newseg.segno = pageno / SLRU_PAGES_PER_SEGMENT_NEW;
	newseg.pageno = pageno % SLRU_PAGES_PER_SEGMENT_NEW;

	if (next_xid < oldest_xid)
		next_xid += FirstUpgradedTransactionId;	/* wraparound */

	/* Copy xid flags reading only needed segment pages */
	for (xid = oldest_xid & ~(CLOG_XACTS_PER_PAGE - 1);
		 xid <= ((next_xid - 1) & ~(CLOG_XACTS_PER_PAGE - 1));
		 xid += CLOG_XACTS_PER_PAGE)
	{
		bool		is_empty;
		int			len;

		/* Handle possible segment wraparound */
		if (oldseg.segno > MaxTransactionIdOld / CLOG_XACTS_PER_PAGE / SLRU_PAGES_PER_SEGMENT_OLD) {
			pageno = (MaxTransactionIdOld + 1) / CLOG_XACTS_PER_PAGE;

			Assert(oldseg.segno == pageno / SLRU_PAGES_PER_SEGMENT_OLD);
			Assert(!oldseg.pageno);
			Assert(!oldseg.file && !oldseg.fn);
			oldseg.segno = 0;

			Assert(newseg.segno == pageno / SLRU_PAGES_PER_SEGMENT_NEW);
			Assert(!newseg.pageno);
			Assert(!newseg.file);
			newseg.segno = 0;
		}

		len = read_old_segment_page(&oldseg, buf, &is_empty);

		/*
		 * Ignore read errors, copy all existing segment pages in the
		 * interesting xid range.
		 */
		is_empty |= len <= 0;

		if (!is_empty && len < BLCKSZ)
			memset(&buf[len], 0, BLCKSZ - len);

		write_new_segment_page(&newseg, buf, is_empty);
	}

	/* Release resources */
	close_segment(&oldseg);
	close_segment(&newseg);
}

typedef uint32 MultiXactIdOld;
typedef uint64 MultiXactIdNew;

typedef uint32 MultiXactOffsetOld;
typedef uint64 MultiXactOffsetNew;

#define MaxMultiXactIdOld		((MultiXactIdOld) 0xFFFFFFFF)
#define MaxMultiXactOffsetOld	((MultiXactOffsetOld) 0xFFFFFFFF)

#define MXACT_OFFSETS_PER_BLOCK_OLD (BLCKSZ / sizeof(MultiXactOffsetOld))
#define MXACT_OFFSETS_PER_BLOCK_NEW (BLCKSZ / sizeof(MultiXactOffsetNew))

/*
 * Convert pg_multixact/offsets segments and return oldest mxid offset.
 */
MultiXactOffsetNew
convert_multixact_offsets(const char *old_subdir, const char *new_subdir)
{
	SLRUSegmentState oldseg = {0};
	SLRUSegmentState newseg = {0};
	MultiXactOffsetOld oldbuf[MXACT_OFFSETS_PER_BLOCK_OLD] = {0};
	MultiXactOffsetNew newbuf[MXACT_OFFSETS_PER_BLOCK_NEW] = {0};
	MultiXactOffsetOld oldest_mxoff = 0;
	MultiXactId oldest_mxid = old_cluster.controldata.chkpnt_oldstMulti;
	MultiXactId next_mxid = old_cluster.controldata.chkpnt_nxtmulti;
	MultiXactId mxid;
	uint64		old_entry;
	uint64		new_entry;
	bool		oldest_mxoff_known = false;

	oldseg.dir = psprintf("%s/%s", old_cluster.pgdata, old_subdir);
	newseg.dir = psprintf("%s/%s", new_cluster.pgdata, new_subdir);

	old_entry = oldest_mxid % MXACT_OFFSETS_PER_BLOCK_OLD;
	oldseg.pageno = oldest_mxid / MXACT_OFFSETS_PER_BLOCK_OLD;
	oldseg.segno = oldseg.pageno / SLRU_PAGES_PER_SEGMENT_OLD;
	oldseg.pageno %= SLRU_PAGES_PER_SEGMENT_OLD;

	new_entry = oldest_mxid % MXACT_OFFSETS_PER_BLOCK_NEW;
	newseg.pageno = oldest_mxid / MXACT_OFFSETS_PER_BLOCK_NEW;
	newseg.segno = newseg.pageno / SLRU_PAGES_PER_SEGMENT_NEW;
	newseg.pageno %= SLRU_PAGES_PER_SEGMENT_NEW;

	if (next_mxid < oldest_mxid)
		next_mxid += (uint64) 1 << 32;	/* wraparound */

	prep_status("Converting old %s to new format", old_subdir);

	/* Copy mxid offsets reading only needed segment pages */
	for (mxid = oldest_mxid; mxid < next_mxid; old_entry = 0)
	{
		int			oldlen;
		bool		is_empty;

		/* Handle possible segment wraparound */
		if (oldseg.segno > MaxMultiXactIdOld / MXACT_OFFSETS_PER_BLOCK_OLD / SLRU_PAGES_PER_SEGMENT_OLD) /* 0xFFFF */
			oldseg.segno = 0;

		oldlen = read_old_segment_page(&oldseg, oldbuf, &is_empty);

		if (oldlen <= 0 || is_empty)
		{
			char pageno_str[32];

			snprintf(pageno_str, sizeof(pageno_str), UINT64_FORMAT, oldseg.pageno);
			pg_fatal("Cannot read page %s from segment: %s\n",
					 pageno_str, oldseg.fn);
		}

		if (oldlen < BLCKSZ)
			memset((char *) oldbuf + oldlen, 0, BLCKSZ - oldlen);

		/* Save oldest mxid offset */
		if (!oldest_mxoff_known)
		{
			oldest_mxoff = oldbuf[old_entry];
			oldest_mxoff_known = true;
		}

		/* Skip wrapped-around invalid MultiXactIds */
		if (mxid == (MultiXactId) 1 << 32)
		{
			Assert(oldseg.segno == 0);
			Assert(oldseg.pageno == 1);
			Assert(old_entry == 0);
			mxid += FirstMultiXactId;
			old_entry = FirstMultiXactId;
		}

		/* Copy entries to the new page */
		for (; mxid < next_mxid && old_entry < MXACT_OFFSETS_PER_BLOCK_OLD;
			 mxid++, old_entry++)
		{
			MultiXactOffsetNew mxoff = oldbuf[old_entry];

			/* Handle possible offset wraparound (1 becomes 2^32) */
			if (mxoff < oldest_mxoff)
				mxoff += ((uint64) 1 << 32) - 1;

			/* Subtract oldest_mxoff, so new offsets will start from 1 */
			newbuf[new_entry++] = mxoff - oldest_mxoff + 1;

			if (new_entry >= MXACT_OFFSETS_PER_BLOCK_NEW)
			{
				/* Write new page */
				write_new_segment_page(&newseg, newbuf, false);
				new_entry = 0;
			}
		}
	}

	/* Write the last incomplete page */
	if (new_entry > 0 || oldest_mxid == next_mxid)
	{
		memset(&newbuf[new_entry], 0,
			   sizeof(newbuf[0]) * (MXACT_OFFSETS_PER_BLOCK_NEW - new_entry));
		write_new_segment_page(&newseg, newbuf, false);
	}

	/* Use next_mxoff as oldest_mxoff, if oldest_mxid == next_mxid */
	if (!oldest_mxoff_known)
	{
		Assert(oldest_mxid == next_mxid);
		oldest_mxoff = (MultiXactOffsetNew) old_cluster.controldata.chkpnt_nxtmxoff;
	}

	/* Release resources */
	close_segment(&oldseg);
	close_segment(&newseg);

	pfree((char *) oldseg.dir);
	pfree((char *) newseg.dir);

	check_ok();

	return oldest_mxoff;
}

typedef uint32 TransactionIdOld;
typedef uint64 TransactionIdNew;

#define MXACT_MEMBERS_FLAG_BYTES			1

#define MXACT_MEMBERS_PER_GROUP_OLD			4
#define MXACT_MEMBERS_GROUP_SIZE_OLD		(MXACT_MEMBERS_PER_GROUP_OLD * (sizeof(TransactionIdOld) + MXACT_MEMBERS_FLAG_BYTES))
#define MXACT_MEMBER_GROUPS_PER_PAGE_OLD	(BLCKSZ / MXACT_MEMBERS_GROUP_SIZE_OLD)
#define MXACT_MEMBERS_PER_PAGE_OLD			(MXACT_MEMBERS_PER_GROUP_OLD * MXACT_MEMBER_GROUPS_PER_PAGE_OLD)
#define MXACT_MEMBER_FLAG_BYTES_PER_GROUP_OLD MXACT_MEMBERS_FLAG_BYTES * MXACT_MEMBERS_PER_GROUP_OLD

#define MXACT_MEMBERS_PER_GROUP_NEW			8
#define MXACT_MEMBERS_GROUP_SIZE_NEW		(MXACT_MEMBERS_PER_GROUP_NEW * (sizeof(TransactionIdNew) + MXACT_MEMBERS_FLAG_BYTES))
#define MXACT_MEMBER_GROUPS_PER_PAGE_NEW	(BLCKSZ / MXACT_MEMBERS_GROUP_SIZE_NEW)

/*
 * Convert pg_multixact/members segments, offsets will start from 1.
 */
void
convert_multixact_members(const char *old_subdir, const char *new_subdir,
						  MultiXactOffset oldest_mxoff)
{
	MultiXactOffsetNew next_mxoff = (MultiXactOffsetNew) old_cluster.controldata.chkpnt_nxtmxoff;
	MultiXactOffsetNew mxoff;
	SLRUSegmentState oldseg = { 0 };
	SLRUSegmentState newseg = { 0 };
	char		oldbuf[BLCKSZ] = { 0 };
	char		newbuf[BLCKSZ] = { 0 };
	int			newgroup;
	int			newmember;
	char	   *newflag = newbuf;
	TransactionIdNew *newxid = (TransactionIdNew *)(newflag + MXACT_MEMBERS_FLAG_BYTES * MXACT_MEMBERS_PER_GROUP_NEW);
	int			newidx;
	int			oldidx;

	oldseg.dir = psprintf("%s/%s", old_cluster.pgdata, old_subdir);
	newseg.dir = psprintf("%s/%s", new_cluster.pgdata, new_subdir);

	prep_status("Converting old %s to new format", old_subdir);

	if (next_mxoff < oldest_mxoff)
		next_mxoff += (uint64) 1 << 32;

	/* Initialize old starting position */
	oldidx = oldest_mxoff % MXACT_MEMBERS_PER_PAGE_OLD;
	oldseg.pageno = oldest_mxoff / MXACT_MEMBERS_PER_PAGE_OLD;
	oldseg.segno = oldseg.pageno / SLRU_PAGES_PER_SEGMENT_OLD;
	oldseg.pageno %= SLRU_PAGES_PER_SEGMENT_OLD;

	/* Initialize new starting position (skip invalid zero offset) */
	newgroup = 0;
	newidx = 1;
	newmember = 1;
	newflag++;
	newxid++;

	/* Iterate through the original directory */
	for (mxoff = oldest_mxoff; mxoff < next_mxoff; oldidx = 0)
	{
		bool		old_is_empty;
		int			oldlen = read_old_segment_page(&oldseg, oldbuf, &old_is_empty);
		int			ngroups;
		int			oldgroup;
		int			oldmember;

		if (old_is_empty || oldlen <= 0)
		{
			char pageno_str[32];

			snprintf(pageno_str, sizeof(pageno_str), UINT64_FORMAT, oldseg.pageno);
			pg_fatal("Cannot read page %s from segment: %s\n",
					 pageno_str, oldseg.fn);
		}

		if (oldlen < BLCKSZ)
		{
			memset(oldbuf + oldlen, 0, BLCKSZ - oldlen);
			oldlen = BLCKSZ;
		}

		ngroups = oldlen / MXACT_MEMBERS_GROUP_SIZE_OLD;

		/* Iterate through old member groups */
		for (oldgroup = oldidx / MXACT_MEMBERS_PER_GROUP_OLD,
			 oldmember = oldidx % MXACT_MEMBERS_PER_GROUP_OLD;
			 oldgroup < ngroups && mxoff < next_mxoff;
			 oldgroup++, oldmember = 0)
		{
			char		*oldflag = (char *) oldbuf + oldgroup * MXACT_MEMBERS_GROUP_SIZE_OLD;
			TransactionIdOld *oldxid = (TransactionIdOld *)(oldflag + MXACT_MEMBER_FLAG_BYTES_PER_GROUP_OLD);

			oldxid += oldmember;
			oldflag += oldmember;

			/* Iterate through old members */
			for (int j = 0;
				 j < MXACT_MEMBERS_PER_GROUP_OLD && mxoff < next_mxoff;
				 j++)
			{
				/* Copy member's xid and flags to the new page */
				*newflag++ = *oldflag++;
				*newxid++ = (TransactionIdNew) *oldxid++;

				newidx++;
				oldidx++;
				mxoff++;

				if (++newmember >= MXACT_MEMBERS_PER_GROUP_NEW)
				{
					/* Start next member group */
					newmember = 0;

					if (++newgroup >= MXACT_MEMBER_GROUPS_PER_PAGE_NEW)
					{
						/* Write current page and start new */
						newgroup = 0;
						newidx = 0;
						write_new_segment_page(&newseg, newbuf, false);
						memset(newbuf, 0, BLCKSZ);
					}

					newflag = (char *) newbuf + newgroup * MXACT_MEMBERS_GROUP_SIZE_NEW;
					newxid = (TransactionIdNew *)(newflag + MXACT_MEMBERS_FLAG_BYTES * MXACT_MEMBERS_PER_GROUP_NEW);
				}

				/* Handle offset wraparound */
				if (mxoff > MaxMultiXactOffsetOld)
				{
					Assert(mxoff == (uint64) 1 << 32);
					Assert(oldseg.segno == MaxMultiXactOffsetOld / MXACT_MEMBERS_PER_PAGE_OLD / SLRU_PAGES_PER_SEGMENT_OLD);
					Assert(oldseg.pageno == MaxMultiXactOffsetOld / MXACT_MEMBERS_PER_PAGE_OLD % SLRU_PAGES_PER_SEGMENT_OLD);
					Assert(oldmember == MaxMultiXactOffsetOld % MXACT_MEMBERS_PER_PAGE_OLD);

					/* Switch to segment 0000 */
					close_segment(&oldseg);
					oldseg.segno = 0;
					oldseg.pageno = 0;

					oldidx = 1;		/* skip invalid zero mxid offset */
				}
			}
		}
	}

	/* Write last page, unless it is empty */
	if (newflag > (char *) newbuf || oldest_mxoff == next_mxoff)
		write_new_segment_page(&newseg, newbuf, false);

	/* Release resources */
	close_segment(&oldseg);
	close_segment(&newseg);

	pfree((char *) oldseg.dir);
	pfree((char *) newseg.dir);

	check_ok();
}
