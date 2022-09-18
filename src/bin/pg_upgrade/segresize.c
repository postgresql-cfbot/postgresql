/*-------------------------------------------------------------------------
 *
 * segresize.c
 *	  SLRU segment resize utility from 32bit to 64bit xid format
 *
 * Copyright (c) 2015-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  src/bin/pg_upgrade/segresize.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "pg_upgrade.h"
#include "access/multixact.h"
#include "access/transam.h"

#define SLRU_PAGES_PER_SEGMENT_OLD	32
#define SLRU_PAGES_PER_SEGMENT		32 /* Should be equal to value from slru.h */

#define CLOG_BITS_PER_XACT		2
#define CLOG_XACTS_PER_BYTE		4
#define CLOG_XACTS_PER_PAGE		(BLCKSZ * CLOG_XACTS_PER_BYTE)

typedef uint32 MultiXactId32;
typedef uint32 MultiXactOffset32;
typedef uint32 TransactionId32;

#define MaxTransactionId32					((TransactionId32) 0xFFFFFFFF)
#define MaxMultiXactId32					((MultiXactId32) 0xFFFFFFFF)
#define MaxMultiXactOffset32				((MultiXactOffset32) 0xFFFFFFFF)

#define MULTIXACT_OFFSETS_PER_PAGE_OLD		(BLCKSZ / sizeof(MultiXactOffset32))
#define MULTIXACT_OFFSETS_PER_PAGE			(BLCKSZ / sizeof(MultiXactOffset))

#define MXACT_MEMBER_FLAGS_PER_BYTE			1

/* 64xid */
#define MULTIXACT_FLAGBYTES_PER_GROUP		8
#define MULTIXACT_MEMBERS_PER_MEMBERGROUP	\
	(MULTIXACT_FLAGBYTES_PER_GROUP * MXACT_MEMBER_FLAGS_PER_BYTE)
/* size in bytes of a complete group */
#define MULTIXACT_MEMBERGROUP_SIZE \
	(sizeof(TransactionId) * MULTIXACT_MEMBERS_PER_MEMBERGROUP + MULTIXACT_FLAGBYTES_PER_GROUP)
#define MULTIXACT_MEMBERGROUPS_PER_PAGE (BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE)

/* 32xid */
#define MULTIXACT_FLAGBYTES_PER_GROUP_OLD	4
#define MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD	\
	(MULTIXACT_FLAGBYTES_PER_GROUP_OLD * MXACT_MEMBER_FLAGS_PER_BYTE)
/* size in bytes of a complete group */
#define MULTIXACT_MEMBERGROUP_SIZE_OLD \
	(sizeof(TransactionId32) * MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD + MULTIXACT_FLAGBYTES_PER_GROUP_OLD)
#define MULTIXACT_MEMBERGROUPS_PER_PAGE_OLD (BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE_OLD)
#define MULTIXACT_MEMBERS_PER_PAGE_OLD	\
	(MULTIXACT_MEMBERGROUPS_PER_PAGE_OLD * MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD)

typedef struct SLRUSegmentState
{
	const char	   *dir;
	FILE		   *file;
	int64			segno;
	int64			pageno;
	bool			is_empty_segment;
}			SLRUSegmentState;

static char *
slru_filename_old(const char *path, int64 segno)
{
	Assert(segno <= PG_INT32_MAX);
	return psprintf("%s/%04X", path, (int) segno);
}

static char *
slru_filename_new(const char *path, int64 segno)
{
	return psprintf("%s/%012llX", path, (long long) segno);
}

static inline FILE *
open_file(SLRUSegmentState *state,
		  char * (filename_fn)(const char *path, int64 segno),
		  char *mode, char *fatal_msg)
{
	char	*filename = filename_fn(state->dir, state->segno);
	FILE	*fd = fopen(filename, mode);

	if (!fd)
		pg_fatal(fatal_msg, filename);

	pfree(filename);

	return fd;
}

static void
close_file(SLRUSegmentState *state,
		   char * (filename_fn)(const char *path, int64 segno))
{
	if (state->file != NULL)
	{
		if (fclose(state->file) != 0)
			pg_fatal("could not close file \"%s\": %m",
					 filename_fn(state->dir, state->segno));
		state->file = NULL;
	}
}

static inline int
read_file(SLRUSegmentState *state, void *buf)
{
	size_t		n = fread(buf, sizeof(char), BLCKSZ, state->file);

	if (n != 0)
		return n;

	if (ferror(state->file))
		pg_fatal("could not read file \"%s\": %m",
				 slru_filename_old(state->dir, state->segno));

	if (!feof(state->file))
		pg_fatal("unknown file read state \"%s\": %m",
				 slru_filename_old(state->dir, state->segno));

	close_file(state, slru_filename_old);

	return 0;
}

static int
read_old_segment_page(SLRUSegmentState *state, void *buf, bool *is_empty)
{
	int		n;

	/* Open next segment file, if needed */
	if (!state->file)
	{
		state->file = open_file(state, slru_filename_old, "rb",
								"could not open source file \"%s\": %m");

		/* Set position to the needed page */
		if (fseek(state->file, state->pageno * BLCKSZ, SEEK_SET))
			close_file(state, slru_filename_old);

		/*
		 * Skip segment conversion if segment file doesn't exist.
		 * First segment file should exist in any case.
		 */
		if (state->segno != 0)
			state->is_empty_segment = true;
	}

	if (state->file)
	{
		/* Segment file does exist, read page from it */
		state->is_empty_segment = false;

		/* Try to read BLCKSZ bytes */
		n = read_file(state, buf);
		*is_empty = (n == 0);

		/* Zeroing buf tail if needed */
		if (n)
			memset((char *) buf + n, 0, BLCKSZ - n);
	}
	else
	{
		n = state->is_empty_segment ?
				BLCKSZ :	/* Skip empty block at the end of segment */
				0;			/* We reached the last segment */
		*is_empty = true;

		if (n)
			memset((char *) buf, 0, BLCKSZ);
	}

	state->pageno++;

	if (state->pageno >= SLRU_PAGES_PER_SEGMENT_OLD)
	{
		/* Start new segment */
		state->segno++;
		state->pageno = 0;
		close_file(state, slru_filename_old);
	}

	return n;
}

static void
write_new_segment_page(SLRUSegmentState *state, void *buf, bool is_empty)
{
	/*
	 * Create a new segment file if we still didn't.  Creation is postponed
	 * until the first non-empty page is found.  This helps not to create
	 * completely empty segments.
	 */
	if (!state->file && !is_empty)
	{
		state->file = open_file(state, slru_filename_new, "wb",
								"could not open target file \"%s\": %m");

		/* Write zeroes to the previously skipped prefix */
		if (state->pageno > 0)
		{
			char	zerobuf[BLCKSZ] = {0};

			for (int64 i = 0; i < state->pageno; i++)
			{
				if (fwrite(zerobuf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
					pg_fatal("could not write file \"%s\": %m",
							 slru_filename_new(state->dir, state->segno));
			}
		}

	}

	/* Write page to the new segment (if it was created) */
	if (state->file)
	{
		if (fwrite(buf, sizeof(char), BLCKSZ, state->file) != BLCKSZ)
			pg_fatal("could not write file \"%s\": %m",
					 slru_filename_new(state->dir, state->segno));
	}

	state->pageno++;

	/*
	 * Did we reach the maximum page number? Then close segment file and
	 * create a new one on the next iteration
	 */
	if (state->pageno >= SLRU_PAGES_PER_SEGMENT)
	{
		state->segno++;
		state->pageno = 0;
		close_file(state, slru_filename_new);
	}
}

/*
 * Convert pg_xact segments.
 */
void
convert_xact(const char *old_subdir, const char *new_subdir)
{
	SLRUSegmentState	oldseg = {0};
	SLRUSegmentState	newseg = {0};
	TransactionId		oldest_xid = old_cluster.controldata.chkpnt_oldstxid;
	TransactionId		next_xid = old_cluster.controldata.chkpnt_nxtxid;
	TransactionId		xid;
	int64				pageno;
	char				buf[BLCKSZ] = {0};

	oldseg.dir = old_subdir;
	newseg.dir = new_subdir;

	pageno = oldest_xid / CLOG_XACTS_PER_PAGE;

	oldseg.segno = pageno / SLRU_PAGES_PER_SEGMENT_OLD;
	oldseg.pageno = pageno % SLRU_PAGES_PER_SEGMENT_OLD;

	newseg.segno = pageno / SLRU_PAGES_PER_SEGMENT;
	newseg.pageno = pageno % SLRU_PAGES_PER_SEGMENT;

	if (next_xid < oldest_xid)
		next_xid += (TransactionId) 1 << 32; /* wraparound */

	/* Copy xid flags reading only needed segment pages */
	for (xid = oldest_xid & ~(CLOG_XACTS_PER_PAGE - 1);
		 xid <= ((next_xid - 1) & ~(CLOG_XACTS_PER_PAGE - 1));
		 xid += CLOG_XACTS_PER_PAGE)
	{
		bool		is_empty;

		/* Handle possible segment wraparound */
		if (oldseg.segno > MaxTransactionId32 / CLOG_XACTS_PER_PAGE / SLRU_PAGES_PER_SEGMENT_OLD)
		{
			pageno = (MaxTransactionId32 + 1) / CLOG_XACTS_PER_PAGE;

			Assert(oldseg.segno == pageno / SLRU_PAGES_PER_SEGMENT_OLD);
			Assert(!oldseg.pageno);
			Assert(!oldseg.file);
			oldseg.segno = 0;

			Assert(newseg.segno == pageno / SLRU_PAGES_PER_SEGMENT);
			Assert(!newseg.pageno);
			Assert(!newseg.file);
			newseg.segno = 0;
		}

		read_old_segment_page(&oldseg, buf, &is_empty);
		write_new_segment_page(&newseg, buf, is_empty);
	}

	/* Release resources */
	close_file(&oldseg, slru_filename_old);
	close_file(&newseg, slru_filename_new);
}

static inline SLRUSegmentState
create_slru_segment_state(MultiXactId mxid,
						  int offsets_per_page,
						  int pages_per_segment,
						  char *dir)
{
	SLRUSegmentState	seg = {0};
	int64				n;

	n = mxid / offsets_per_page;
	seg.pageno = n % pages_per_segment;
	seg.segno = n / pages_per_segment;
	seg.dir = dir;

	return seg;
}

/*
 * Convert pg_multixact/offsets segments and return oldest mxid offset.
 */
MultiXactOffset
convert_multixact_offsets(const char *old_subdir, const char *new_subdir)
{
	SLRUSegmentState		oldseg,
							newseg;
	MultiXactOffset32		oldbuf[MULTIXACT_OFFSETS_PER_PAGE_OLD] = {0};
	MultiXactOffset			newbuf[MULTIXACT_OFFSETS_PER_PAGE] = {0};
	MultiXactOffset32		oldest_mxoff = 0;
	MultiXactId				oldest_mxid,
							next_mxid,
							mxid;
	uint64					old_entry,
							new_entry;
	bool					oldest_mxoff_known = false;

	StaticAssertStmt((sizeof(oldbuf) == BLCKSZ && sizeof(newbuf) == BLCKSZ),
					 "buf should be BLCKSZ");

	oldest_mxid = old_cluster.controldata.chkpnt_oldstMulti;

	oldseg = create_slru_segment_state(oldest_mxid,
									   MULTIXACT_OFFSETS_PER_PAGE_OLD,
									   SLRU_PAGES_PER_SEGMENT_OLD,
									   psprintf("%s/%s", old_cluster.pgdata,
												old_subdir));

	newseg = create_slru_segment_state(oldest_mxid,
									   MULTIXACT_OFFSETS_PER_PAGE,
									   SLRU_PAGES_PER_SEGMENT,
									   psprintf("%s/%s", new_cluster.pgdata,
												new_subdir));

	old_entry = oldest_mxid % MULTIXACT_OFFSETS_PER_PAGE_OLD;
	new_entry = oldest_mxid % MULTIXACT_OFFSETS_PER_PAGE;

	next_mxid = old_cluster.controldata.chkpnt_nxtmulti;
	if (next_mxid < oldest_mxid)
		next_mxid += (MultiXactId) 1 << 32;	/* wraparound */

	prep_status("Converting old %s to new format", old_subdir);

	/* Copy mxid offsets reading only needed segment pages */
	for (mxid = oldest_mxid; mxid < next_mxid; old_entry = 0)
	{
		int			oldlen;
		bool		is_empty;

		/* Handle possible segment wraparound */
		if (oldseg.segno > MaxMultiXactId32 / MULTIXACT_OFFSETS_PER_PAGE_OLD / SLRU_PAGES_PER_SEGMENT_OLD)	/* 0xFFFF */
			oldseg.segno = 0;

		oldlen = read_old_segment_page(&oldseg, oldbuf, &is_empty);

		if (oldlen == 0 || is_empty)
			pg_fatal("cannot read page %lld from segment: %s\n",
					 (long long) oldseg.pageno,
					 slru_filename_old(oldseg.dir, oldseg.segno));

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
		for (; mxid < next_mxid && old_entry < MULTIXACT_OFFSETS_PER_PAGE_OLD;
			 mxid++, old_entry++)
		{
			MultiXactOffset mxoff = oldbuf[old_entry];

			/* Handle possible offset wraparound (1 becomes 2^32) */
			if (mxoff < oldest_mxoff)
				mxoff += ((MultiXactOffset) 1 << 32) - 1;

			/* Subtract oldest_mxoff, so new offsets will start from 1 */
			newbuf[new_entry++] = mxoff - oldest_mxoff + 1;

			if (new_entry >= MULTIXACT_OFFSETS_PER_PAGE)
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
			   sizeof(newbuf[0]) * (MULTIXACT_OFFSETS_PER_PAGE - new_entry));
		write_new_segment_page(&newseg, newbuf, false);
	}

	/* Use next_mxoff as oldest_mxoff, if oldest_mxid == next_mxid */
	if (!oldest_mxoff_known)
	{
		Assert(oldest_mxid == next_mxid);
		oldest_mxoff = (MultiXactOffset) old_cluster.controldata.chkpnt_nxtmxoff;
	}

	/* Release resources */
	close_file(&oldseg, slru_filename_old);
	close_file(&newseg, slru_filename_new);

	pfree((char *) oldseg.dir);
	pfree((char *) newseg.dir);

	check_ok();

	return oldest_mxoff;
}

/*
 * Convert pg_multixact/members segments, offsets will start from 1.
 */
void
convert_multixact_members(const char *old_subdir, const char *new_subdir,
						  MultiXactOffset oldest_mxoff)
{
	MultiXactOffset		next_mxoff,
						mxoff;
	SLRUSegmentState	oldseg,
						newseg;
	char				oldbuf[BLCKSZ] = {0},
						newbuf[BLCKSZ] = {0};
	int					newgroup,
						newmember;
	char			   *newflag = newbuf;
	TransactionId	   *newxid;
	int					oldidx,
						newidx;

	prep_status("Converting old %s to new format", old_subdir);

	next_mxoff = (MultiXactOffset) old_cluster.controldata.chkpnt_nxtmxoff;
	if (next_mxoff < oldest_mxoff)
		next_mxoff += (MultiXactOffset) 1 << 32;

	newxid = (TransactionId *) (newflag + MXACT_MEMBER_FLAGS_PER_BYTE * MULTIXACT_MEMBERS_PER_MEMBERGROUP);

	/* Initialize old starting position */
	oldidx = oldest_mxoff % MULTIXACT_MEMBERS_PER_PAGE_OLD;
	oldseg = create_slru_segment_state(oldest_mxoff,
									   MULTIXACT_MEMBERS_PER_PAGE_OLD,
									   SLRU_PAGES_PER_SEGMENT_OLD,
									   psprintf("%s/%s", old_cluster.pgdata,
												old_subdir));

	/* Initialize empty new segment */
	newseg = create_slru_segment_state(0, 1, 1,
									   psprintf("%s/%s", new_cluster.pgdata,
									   new_subdir));

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
		int			oldlen;
		int			ngroups;
		int			oldgroup;
		int			oldmember;

		oldlen = read_old_segment_page(&oldseg, oldbuf, &old_is_empty);

		if (oldlen == 0 || old_is_empty)
			pg_fatal("cannot read page %lld from segment: %s\n",
					 (long long) oldseg.pageno,
					 slru_filename_old(oldseg.dir, oldseg.segno));

		ngroups = oldlen / MULTIXACT_MEMBERGROUP_SIZE_OLD;

		/* Iterate through old member groups */
		for (oldgroup = oldidx / MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD,
			 oldmember = oldidx % MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD;
			 oldgroup < ngroups && mxoff < next_mxoff;
			 oldgroup++, oldmember = 0)
		{
			char	   *oldflag = (char *) oldbuf + oldgroup * MULTIXACT_MEMBERGROUP_SIZE_OLD;
			TransactionId32 *oldxid = (TransactionId32 *) (oldflag + MULTIXACT_FLAGBYTES_PER_GROUP_OLD);

			oldxid += oldmember;
			oldflag += oldmember;

			/* Iterate through old members */
			for (int i = 0;
				 i < MULTIXACT_MEMBERS_PER_MEMBERGROUP_OLD && mxoff < next_mxoff;
				 i++)
			{
				/* Copy member's xid and flags to the new page */
				*newflag++ = *oldflag++;
				*newxid++ = (TransactionId) * oldxid++;

				newidx++;
				oldidx++;
				mxoff++;

				if (++newmember >= MULTIXACT_MEMBERS_PER_MEMBERGROUP)
				{
					/* Start next member group */
					newmember = 0;

					if (++newgroup >= MULTIXACT_MEMBERGROUPS_PER_PAGE)
					{
						/* Write current page and start new */
						newgroup = 0;
						newidx = 0;
						write_new_segment_page(&newseg, newbuf, false);
						memset(newbuf, 0, BLCKSZ);
					}

					newflag = (char *) newbuf + newgroup * MULTIXACT_MEMBERGROUP_SIZE;
					newxid = (TransactionId *) (newflag + MXACT_MEMBER_FLAGS_PER_BYTE * MULTIXACT_MEMBERS_PER_MEMBERGROUP);
				}

				/* Handle offset wraparound */
				if (mxoff > MaxMultiXactOffset32)
				{
					Assert(mxoff == (MultiXactOffset) 1 << 32);
					Assert(oldseg.segno == MaxMultiXactOffset32 / MULTIXACT_MEMBERS_PER_PAGE_OLD / SLRU_PAGES_PER_SEGMENT_OLD);
					Assert(oldseg.pageno == MaxMultiXactOffset32 / MULTIXACT_MEMBERS_PER_PAGE_OLD % SLRU_PAGES_PER_SEGMENT_OLD);
					Assert(oldmember == MaxMultiXactOffset32 % MULTIXACT_MEMBERS_PER_PAGE_OLD);

					/* Switch to segment 0000 */
					close_file(&oldseg, slru_filename_old);
					oldseg.segno = 0;
					oldseg.pageno = 0;

					oldidx = 1; /* skip invalid zero mxid offset */
				}
			}
		}
	}

	/* Write last page, unless it is empty */
	if (newflag > (char *) newbuf || oldest_mxoff == next_mxoff)
		write_new_segment_page(&newseg, newbuf, false);

	/* Release resources */
	close_file(&oldseg, slru_filename_old);
	close_file(&newseg, slru_filename_new);

	pfree((char *) oldseg.dir);
	pfree((char *) newseg.dir);

	check_ok();
}
