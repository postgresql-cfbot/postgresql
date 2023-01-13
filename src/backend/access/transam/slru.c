/*-------------------------------------------------------------------------
 *
 * slru.c
 *		Simple buffering for transaction status logfiles
 *
 * XXX write me
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/slru.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/shmem.h"

/*
 * SLRU ID to path mapping
 */
#define PG_SLRU(symname,name,path,synchronize) \
	path,

static char *slru_dirs[] =
{
#include "access/slrulist.h"
};

/*
 * We'll maintain a little cache of recently seen buffers, to try to avoid the
 * buffer mapping table on repeat access (ie the busy end of the CLOG).  One
 * entry per SLRU.
 */
struct SlruRecentBuffer {
	int			pageno;
	Buffer		recent_buffer;
};

static struct SlruRecentBuffer slru_recent_buffers[SLRU_NEXT_ID];

static bool SlruScanDirCbDeleteCutoff(int slru_id,
									  SlruPagePrecedesFunction PagePrecedes,
									  char *filename,
									  int segpage, void *data);
static void SlruInternalDeleteSegment(int slru_id, int segno);

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
SimpleLruDoesPhysicalPageExist(int slru_id, int pageno)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	off_t		offset = rpageno * BLCKSZ;
	off_t		size;
	RelFileLocator rlocator = SlruRelFileLocator(slru_id, segno);
	SMgrFileHandle sfile = smgropen(rlocator, InvalidBackendId, MAIN_FORKNUM);

	/* update the stats counter of checked pages */
	pgstat_count_slru_page_exists(slru_id);

	if (smgrexists(sfile))
		size = smgrnblocks(sfile);
	else
		size = 0;

	return size >= offset + BLCKSZ;
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * All SLRUs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the SLRU is quite full, SimpleLruTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
void
SimpleLruTruncate(int slru_id, SlruPagePrecedesFunction PagePrecedes, int cutoffPage)
{
	/* update the stats counter of truncates */
	pgstat_count_slru_truncate(slru_id);

	/* Now we can remove the old segment(s) */
	(void) SlruScanDirectory(slru_id, PagePrecedes, SlruScanDirCbDeleteCutoff,
							 &cutoffPage);
}

/*
 * Delete an individual SLRU segment.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
SlruInternalDeleteSegment(int slru_id, int segno)
{
	RelFileLocator rlocator = SlruRelFileLocator(slru_id, segno);
	SMgrFileHandle sfile = smgropen(rlocator, InvalidBackendId, MAIN_FORKNUM);

	/* Unlink the file. */
	smgrunlink(sfile, false);
}

/*
 * Delete an individual SLRU segment, identified by the segment number.
 */
void
SlruDeleteSegment(int slru_id, int segno)
{
	SlruInternalDeleteSegment(slru_id, segno);
}

/*
 * Determine whether a segment is okay to delete.
 *
 * segpage is the first page of the segment, and cutoffPage is the oldest (in
 * PagePrecedes order) page in the SLRU containing still-useful data.  Since
 * every core PagePrecedes callback implements "wrap around", check the
 * segment's first and last pages:
 *
 * first<cutoff  && last<cutoff:  yes
 * first<cutoff  && last>=cutoff: no; cutoff falls inside this segment
 * first>=cutoff && last<cutoff:  no; wrap point falls inside this segment
 * first>=cutoff && last>=cutoff: no; every page of this segment is too young
 */
static bool
SlruMayDeleteSegment(SlruPagePrecedesFunction PagePrecedes,
					 int segpage, int cutoffPage)
{
	int			seg_last_page = segpage + SLRU_PAGES_PER_SEGMENT - 1;

	Assert(segpage % SLRU_PAGES_PER_SEGMENT == 0);

	return (PagePrecedes(segpage, cutoffPage) &&
			PagePrecedes(seg_last_page, cutoffPage));
}

#ifdef USE_ASSERT_CHECKING
static void
SlruPagePrecedesTestOffset(SlruPagePrecedesFunction PagePrecedes,
						   int per_page, uint32 offset)
{
	TransactionId lhs,
				rhs;
	int			newestPage,
				oldestPage;
	TransactionId newestXact,
				oldestXact;

	/*
	 * Compare an XID pair having undefined order (see RFC 1982), a pair at
	 * "opposite ends" of the XID space.  TransactionIdPrecedes() treats each
	 * as preceding the other.  If RHS is oldestXact, LHS is the first XID we
	 * must not assign.
	 */
	lhs = per_page + offset;	/* skip first page to avoid non-normal XIDs */
	rhs = lhs + (1U << 31);
	Assert(TransactionIdPrecedes(lhs, rhs));
	Assert(TransactionIdPrecedes(rhs, lhs));
	Assert(!TransactionIdPrecedes(lhs - 1, rhs));
	Assert(TransactionIdPrecedes(rhs, lhs - 1));
	Assert(TransactionIdPrecedes(lhs + 1, rhs));
	Assert(!TransactionIdPrecedes(rhs, lhs + 1));
	Assert(!TransactionIdFollowsOrEquals(lhs, rhs));
	Assert(!TransactionIdFollowsOrEquals(rhs, lhs));
	Assert(!PagePrecedes(lhs / per_page, lhs / per_page));
	Assert(!PagePrecedes(lhs / per_page, rhs / per_page));
	Assert(!PagePrecedes(rhs / per_page, lhs / per_page));
	Assert(!PagePrecedes((lhs - per_page) / per_page, rhs / per_page));
	Assert(PagePrecedes(rhs / per_page, (lhs - 3 * per_page) / per_page));
	Assert(PagePrecedes(rhs / per_page, (lhs - 2 * per_page) / per_page));
	Assert(PagePrecedes(rhs / per_page, (lhs - 1 * per_page) / per_page)
		   || (1U << 31) % per_page != 0);	/* See CommitTsPagePrecedes() */
	Assert(PagePrecedes((lhs + 1 * per_page) / per_page, rhs / per_page)
		   || (1U << 31) % per_page != 0);
	Assert(PagePrecedes((lhs + 2 * per_page) / per_page, rhs / per_page));
	Assert(PagePrecedes((lhs + 3 * per_page) / per_page, rhs / per_page));
	Assert(!PagePrecedes(rhs / per_page, (lhs + per_page) / per_page));

	/*
	 * GetNewTransactionId() has assigned the last XID it can safely use, and
	 * that XID is in the *LAST* page of the second segment.  We must not
	 * delete that segment.
	 */
	newestPage = 2 * SLRU_PAGES_PER_SEGMENT - 1;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!SlruMayDeleteSegment(PagePrecedes,
								 (newestPage -
								  newestPage % SLRU_PAGES_PER_SEGMENT),
								 oldestPage));

	/*
	 * GetNewTransactionId() has assigned the last XID it can safely use, and
	 * that XID is in the *FIRST* page of the second segment.  We must not
	 * delete that segment.
	 */
	newestPage = SLRU_PAGES_PER_SEGMENT;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!SlruMayDeleteSegment(PagePrecedes,
								 (newestPage -
								  newestPage % SLRU_PAGES_PER_SEGMENT),
								 oldestPage));
}

/*
 * Unit-test a PagePrecedes function.
 *
 * This assumes every uint32 >= FirstNormalTransactionId is a valid key.  It
 * assumes each value occupies a contiguous, fixed-size region of SLRU bytes.
 * (MultiXactMemberCtl separates flags from XIDs.  AsyncCtl has
 * variable-length entries, no keys, and no random access.  These unit tests
 * do not apply to them.)
 */
void
SlruPagePrecedesUnitTests(SlruPagePrecedesFunction PagePrecedes, int per_page)
{
	/* Test first, middle and last entries of a page. */
	SlruPagePrecedesTestOffset(PagePrecedes, per_page, 0);
	SlruPagePrecedesTestOffset(PagePrecedes, per_page, per_page / 2);
	SlruPagePrecedesTestOffset(PagePrecedes, per_page, per_page - 1);
}
#endif

/*
 * SlruScanDirectory callback
 *		This callback reports true if there's any segment wholly prior to the
 *		one containing the page passed as "data".
 */
bool
SlruScanDirCbReportPresence(int slru_id, SlruPagePrecedesFunction PagePrecedes,
							char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (SlruMayDeleteSegment(PagePrecedes, segpage, cutoffPage))
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
static bool
SlruScanDirCbDeleteCutoff(int slru_id, SlruPagePrecedesFunction PagePrecedes,
						  char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (SlruMayDeleteSegment(PagePrecedes, segpage, cutoffPage))
	{
		SlruDeleteSegment(slru_id, segpage / SLRU_PAGES_PER_SEGMENT);
	}

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
bool
SlruScanDirCbDeleteAll(int slru_id, SlruPagePrecedesFunction PagePrecedes,
					   char *filename, int segpage, void *data)
{
	SlruInternalDeleteSegment(slru_id, segpage / SLRU_PAGES_PER_SEGMENT);

	return false;				/* keep going */
}

/*
 * Scan the SimpleLru directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the SlruCtl struct for the
 * slru being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
SlruScanDirectory(int slru_id, SlruPagePrecedesFunction PagePrecedes,
				  SlruScanCallback callback, void *data)
{
	bool		retval = false;
	DIR		   *cldir;
	struct dirent *clde;
	int			segno;
	int			segpage;
	const char *path;

	path = slru_dirs[slru_id];

	cldir = AllocateDir(path);
	while ((clde = ReadDir(cldir, path)) != NULL)
	{
		size_t		len;

		len = strlen(clde->d_name);

		if ((len == 4 || len == 5 || len == 6) &&
			strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			segno = (int) strtol(clde->d_name, NULL, 16);
			segpage = segno * SLRU_PAGES_PER_SEGMENT;

			elog(DEBUG2, "SlruScanDirectory invoking callback on %s/%s",
				 path, clde->d_name);
			retval = callback(slru_id, PagePrecedes, clde->d_name, segpage, data);
			if (retval)
				break;
		}
	}
	FreeDir(cldir);

	return retval;
}

/*
 * Read a buffer.  Buffer is pinned on return.
 */
Buffer
ReadSlruBuffer(int slru_id, int pageno, ReadBufferMode mode)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	RelFileLocator rlocator = SlruRelFileLocator(slru_id, segno);
	Buffer		buffer;
	bool		hit;

	/* Try to avoid doing a buffer mapping table lookup for repeated access. */
	buffer = slru_recent_buffers[slru_id].recent_buffer;
	if (slru_recent_buffers[slru_id].pageno == pageno &&
		BufferIsValid(buffer) &&
		ReadRecentBuffer(rlocator, MAIN_FORKNUM, pageno, buffer))
	{
		pgstat_count_slru_page_hit(slru_id);
		return buffer;
	}

	/* Regular lookup. */
	buffer = ReadBufferWithoutRelcacheWithHit(rlocator, MAIN_FORKNUM, rpageno,
											  mode, NULL, true, &hit);

	/* Remember where this page is for next time. */
	slru_recent_buffers[slru_id].pageno = pageno;
	slru_recent_buffers[slru_id].recent_buffer = buffer;

	if (hit)
		pgstat_count_slru_page_hit(slru_id);

	return buffer;
}

/*
 * Zero-initialize a buffer.  Buffer is pinned and exclusively locked on return.
 */
Buffer
ZeroSlruBuffer(int slru_id, int pageno)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	RelFileLocator rlocator = SlruRelFileLocator(slru_id, segno);
	Buffer		buffer;
	SMgrFileHandle sfile;

	sfile = smgropen(rlocator, InvalidBackendId, MAIN_FORKNUM);
	if (!smgrexists(sfile))
		smgrcreate(sfile, false);
	
	buffer = ReadBufferWithoutRelcache(rlocator, MAIN_FORKNUM, rpageno, RBM_ZERO_AND_LOCK, NULL, true);

	/* Remember where this page is for next time. */
	slru_recent_buffers[slru_id].pageno = pageno;
	slru_recent_buffers[slru_id].recent_buffer = buffer;

	pgstat_count_slru_page_zeroed(slru_id);

	return buffer;
}

bool
ProbeSlruBuffer(int slru_id, int pageno)
{
	int			segno = pageno / SLRU_PAGES_PER_SEGMENT;
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	RelFileLocator rlocator = SlruRelFileLocator(slru_id, segno);

	return BufferProbe(rlocator, MAIN_FORKNUM, rpageno);
}
