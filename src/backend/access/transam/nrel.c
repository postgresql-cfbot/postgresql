/*-------------------------------------------------------------------------
 *
 * nrel.c
 *		
 * interface for non relational data to be buffered through
 * buffercache.
 * 
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/nrel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/nrel.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/shmem.h"

#define NrelFileName(rel_id, path, seg)							\
	snprintf(path, MAXPGPATH, "%s/%04X", defs[(rel_id)].path, seg)

struct NrelDef {
	const char *name;
	const char *path;
	bool synchronize;
};

static const struct NrelDef defs[] = {
	[NREL_CLOG_REL_ID] = {
		.name = "Xact",
		.path = "pg_xact",
		.synchronize = true,
	},
	[NREL_SUBTRANS_REL_ID] = {
		.name = "Subtrans",
		.path = "pg_subtrans",
	},
	[NREL_MULTIXACT_OFFSET_REL_ID] = {
		.name = "MultiXactOffset",
		.path = "pg_multixact/offsets",
		.synchronize = true,
	},
	[NREL_MULTIXACT_MEMBER_REL_ID] = {
		.name = "MultiXactMember",
		.path = "pg_multixact/members",
		.synchronize = true,
	},
	[NREL_COMMITTS_REL_ID] = {
		.name = "CommitTs",
		.path = "pg_commit_ts",
		.synchronize = true,
	},
	[NREL_SERIAL_REL_ID] = {
		.name = "Serial",
		.path = "pg_serial",
	},
	[NREL_NOTIFY_REL_ID] = {
		.name = "Notify",
		.path = "pg_notify",
	},
};

/*
 * We'll maintain a little cache of recently seen buffers, to try to avoid the
 * buffer mapping table on repeat access (ie the busy end of the CLOG).  One
 * entry per NREL relation.
 */
struct NrelRecentBuffer {
	int			pageno;
	Buffer		recent_buffer;
};

static struct NrelRecentBuffer nrel_recent_buffers[lengthof(defs)];

/*
 * Populate a file tag identifying an NREL segment file.
 */
#define INIT_NRELFILETAG(a,xx_rel_id,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_NREL, \
	(a).rlocator = NrelRelFileLocator(xx_rel_id), \
	(a).segno = (xx_segno) \
)

static bool NrelScanDirCbDeleteCutoff(Oid rel_id,
									  NrelPagePrecedesFunction PagePrecedes,
									  char *filename,
 									  int segpage, void *data);

static void NrelInternalDeleteSegment(Oid rel_id, int segno);

static File nrelfile(SMgrRelation reln, BlockNumber blocknum, int mode,
					 bool missing_ok);

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
NonRelDoesPhysicalPageExist(Oid rel_id, int pageno)
{
	int			rpageno = pageno % NREL_PAGES_PER_SEGMENT;
	off_t		offset = rpageno * BLCKSZ;
	off_t		size;
	File		file;
	RelFileLocator	rlocator = NrelRelFileLocator(rel_id);
	SMgrRelation reln = smgropen(rlocator, InvalidBackendId);

	/* update the stats counter of checked pages */
	pgstat_count_slru_page_exists(rel_id);

	file = nrelfile(reln, pageno, O_RDWR, true);
	if (file < 0)
	{
		Assert(errno == ENOENT);
		return false;
	}
	size = FileSize(file);
	if (size < 0)
		elog(ERROR, "could not get size of file \"%s\": %m",
			 FilePathName(file));

	return size >= offset + BLCKSZ;
}

/*
 * Remove all segments before the one holding the passed page number
 *
 * All NRELs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the NREL is quite full, NonRelTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
void
NonRelTruncate(Oid rel_id, NrelPagePrecedesFunction PagePrecedes, int cutoffPage)
{

	/* update the stats counter of truncates */
	pgstat_count_slru_truncate(rel_id);
	

	/* Now we can remove the old segment(s) */
	(void) NrelScanDirectory(rel_id, PagePrecedes, NrelScanDirCbDeleteCutoff,
							 &cutoffPage);
}

/*
 * Delete an individual NREL segment.
 *
 * NB: This does not touch the NREL buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
NrelInternalDeleteSegment(Oid rel_id, int segno)
{
	char		path[MAXPGPATH];

	/* Forget any fsync requests queued for this segment. */
	if (defs[rel_id].synchronize)
	{
		FileTag		tag;

		INIT_NRELFILETAG(tag, rel_id, segno);
		RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
	}

	/* Unlink the file. */
	NrelFileName(rel_id, path, segno);
	ereport(DEBUG2, (errmsg_internal("removing file \"%s\"", path)));
	unlink(path);
}

/*
 * Delete an individual NREL segment, identified by the segment number.
 */
void
NrelDeleteSegment(Oid rel_id, int segno)
{
	RelFileLocator rlocator = NrelRelFileLocator(rel_id);

	/* Clean out any possibly existing references to the segment. */
	for (int i = 0; i < NREL_PAGES_PER_SEGMENT; ++i)
		DiscardBuffer(rlocator, MAIN_FORKNUM, segno * NREL_PAGES_PER_SEGMENT + i);

	NrelInternalDeleteSegment(rel_id, segno);
}

/*
 * Determine whether a segment is okay to delete.
 *
 * segpage is the first page of the segment, and cutoffPage is the oldest (in
 * PagePrecedes order) page in the NREL containing still-useful data.  Since
 * every core PagePrecedes callback implements "wrap around", check the
 * segment's first and last pages:
 *
 * first<cutoff  && last<cutoff:  yes
 * first<cutoff  && last>=cutoff: no; cutoff falls inside this segment
 * first>=cutoff && last<cutoff:  no; wrap point falls inside this segment
 * first>=cutoff && last>=cutoff: no; every page of this segment is too young
 */
static bool
NrelMayDeleteSegment(NrelPagePrecedesFunction PagePrecedes,
					 int segpage, int cutoffPage)
{
	int			seg_last_page = segpage + NREL_PAGES_PER_SEGMENT - 1;

	Assert(segpage % NREL_PAGES_PER_SEGMENT == 0);

	return (PagePrecedes(segpage, cutoffPage) &&
			PagePrecedes(seg_last_page, cutoffPage));
}

#ifdef USE_ASSERT_CHECKING
static void
NrelPagePrecedesTestOffset(NrelPagePrecedesFunction PagePrecedes,
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
	newestPage = 2 * NREL_PAGES_PER_SEGMENT - 1;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!NrelMayDeleteSegment(PagePrecedes,
								 (newestPage -
								  newestPage % NREL_PAGES_PER_SEGMENT),
								 oldestPage));

	/*
	 * GetNewTransactionId() has assigned the last XID it can safely use, and
	 * that XID is in the *FIRST* page of the second segment.  We must not
	 * delete that segment.
	 */
	newestPage = NREL_PAGES_PER_SEGMENT;
	newestXact = newestPage * per_page + offset;
	Assert(newestXact / per_page == newestPage);
	oldestXact = newestXact + 1;
	oldestXact -= 1U << 31;
	oldestPage = oldestXact / per_page;
	Assert(!NrelMayDeleteSegment(PagePrecedes,
								 (newestPage -
								  newestPage % NREL_PAGES_PER_SEGMENT),
								 oldestPage));
}

/*
 * Unit-test a PagePrecedes function.
 *
 * This assumes every uint32 >= FirstNormalTransactionId is a valid key.  It
 * assumes each value occupies a contiguous, fixed-size region of NREL bytes.
 * (MultiXactMemberCtl separates flags from XIDs.  NotifyCtl has
 * variable-length entries, no keys, and no random access.  These unit tests
 * do not apply to them.)
 */
void
NrelPagePrecedesUnitTests(NrelPagePrecedesFunction PagePrecedes, int per_page)
{
	/* Test first, middle and last entries of a page. */
	NrelPagePrecedesTestOffset(PagePrecedes, per_page, 0);
	NrelPagePrecedesTestOffset(PagePrecedes, per_page, per_page / 2);
	NrelPagePrecedesTestOffset(PagePrecedes, per_page, per_page - 1);
}
#endif

/*
 * NrelScanDirectory callback
 *		This callback reports true if there's any segment wholly prior to the
 *		one containing the page passed as "data".
 */
bool
NrelScanDirCbReportPresence(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
							char *filename, int segpage, void *data)
{
	int			cutoffPage = *(int *) data;

	if (NrelMayDeleteSegment(PagePrecedes, segpage, cutoffPage))
		return true;			/* found one; don't iterate any more */

	return false;				/* keep going */
}

/*
 * NrelScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
static bool
NrelScanDirCbDeleteCutoff(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
						  char *filename, int segpage, void *data)
{
	RelFileLocator	rlocator = NrelRelFileLocator(rel_id);
	int			cutoffPage = *(int *) data;

	if (NrelMayDeleteSegment(PagePrecedes, segpage, cutoffPage))
	{
		for (int i = 0; i < NREL_PAGES_PER_SEGMENT; ++i)
			DiscardBuffer(rlocator, MAIN_FORKNUM, segpage + i);
		NrelInternalDeleteSegment(rel_id, segpage / NREL_PAGES_PER_SEGMENT);
	}

	return false;				/* keep going */
}

/*
 * NrelScanDirectory callback.
 *		This callback deletes all segments.
 */
bool
NrelScanDirCbDeleteAll(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
					   char *filename, int segpage, void *data)
{
	NrelInternalDeleteSegment(rel_id, segpage / NREL_PAGES_PER_SEGMENT);

	return false;				/* keep going */
}

/*
 * Scan the NonRel directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * The callback receives the following arguments: 1. the NrelCtl struct for the
 * nrel being truncated; 2. the filename being considered; 3. the page number
 * for the first page of that file; 4. a pointer to the opaque data given to us
 * by the caller.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool
NrelScanDirectory(Oid rel_id, NrelPagePrecedesFunction PagePrecedes,
				  NrelScanCallback callback, void *data)
{
	bool		retval = false;
	DIR		   *cldir;
	struct dirent *clde;
	int			segno;
	int			segpage;
	const char *path;

	path = defs[rel_id].path;

	cldir = AllocateDir(path);
	while ((clde = ReadDir(cldir, path)) != NULL)
	{
		size_t		len;

		len = strlen(clde->d_name);

		if ((len == 4 || len == 5 || len == 6) &&
			strspn(clde->d_name, "0123456789ABCDEF") == len)
		{
			segno = (int) strtol(clde->d_name, NULL, 16);
			segpage = segno * NREL_PAGES_PER_SEGMENT;

			elog(DEBUG2, "NrelScanDirectory invoking callback on %s/%s",
				 path, clde->d_name);
			retval = callback(rel_id, PagePrecedes, clde->d_name, segpage, data);
			if (retval)
				break;
		}
	}
	FreeDir(cldir);

	return retval;
}


void
CheckPointNREL(void)
{
	/* Ensure that directory entries for new files are on disk. */
	for (int i = 0; i < lengthof(defs); ++i)
	{
		if (defs[i].synchronize)
			fsync_fname(defs[i].path, true);
	}
}

/*
 * Read a buffer.  Buffer is pinned on return.
 */
Buffer
ReadNrelBuffer(Oid rel_id, int pageno)
{
	RelFileLocator rlocator = NrelRelFileLocator(rel_id);
	Buffer buffer;
	bool hit;

	/* Try to avoid doing a buffer mapping table lookup for repeated access. */
	buffer = nrel_recent_buffers[rel_id].recent_buffer;
	if (nrel_recent_buffers[rel_id].pageno == pageno &&
		BufferIsValid(buffer) &&
		ReadRecentBuffer(rlocator, MAIN_FORKNUM, pageno, buffer))
	{
		pgstat_count_slru_page_hit(rel_id);
		return buffer;
	}

	/* Regular lookup. */
	buffer = ReadBufferWithoutRelcacheWithHit(rlocator, MAIN_FORKNUM, pageno,
											  RBM_NORMAL, &hit);

	/* Remember where this page is for next time. */
	nrel_recent_buffers[rel_id].pageno = pageno;
	nrel_recent_buffers[rel_id].recent_buffer = buffer;

	if (hit)
		pgstat_count_slru_page_hit(rel_id);

	return buffer;
}

/*
 * Zero-initialize a buffer.  Buffer is pinned and exclusively locked on return.
 */
Buffer
ZeroNrelBuffer(Oid rel_id, int pageno)
{
	RelFileLocator rlocator = NrelRelFileLocator(rel_id);
	Buffer buffer;

	buffer = ReadBufferWithoutRelcache(rlocator, MAIN_FORKNUM, pageno,
									   RBM_ZERO_AND_LOCK, NULL, true);

	/* Remember where this page is for next time. */
	nrel_recent_buffers[rel_id].pageno = pageno;
	nrel_recent_buffers[rel_id].recent_buffer = buffer;

	pgstat_count_slru_page_zeroed(rel_id);

	return buffer;
}

Oid
NrelRelIdByName(const char *name)
{
	for (int i = 0; i < lengthof(defs); ++i)
		if (strcmp(defs[i].name, name) == 0)
			return i;
	
	/*return max index + 1 to denote "other" type component, using SLRU */
	return lengthof(defs);
}

const char *
NrelName(Oid rel_id)
{
	if (rel_id > NREL_NUM_RELS)
		elog(ERROR, "invalid NREL rel ID %u", rel_id);

	return defs[rel_id].name;
}

int
nrelsyncfiletag(const FileTag *ftag, char *path)
{
	SMgrRelation reln;
	File		file;

	reln = smgropen(ftag->rlocator, InvalidBackendId);
	file = nrelfile(reln, ftag->segno * NREL_PAGES_PER_SEGMENT, O_RDWR, true);
	if (file < 0)
	{
		/* Path is reported here so the caller can make an error message */
		NrelFileName(ftag->rlocator.relNumber, path, ftag->segno);
		return -1;
	}

	pgstat_count_slru_flush(reln->smgr_rlocator.locator.relNumber);

	return FileSync(file, WAIT_EVENT_NREL_SYNC);
}

static File
nrelfile(SMgrRelation reln, BlockNumber blocknum, int mode, bool missing_ok)
{
	int segment = blocknum / NREL_PAGES_PER_SEGMENT;
	char path[MAXPGPATH];

	Assert(reln->smgr_rlocator.locator.dbOid == NREL_DB_ID);
	Assert(reln->smgr_rlocator.locator.relNumber < lengthof(defs));
	Assert(defs[reln->smgr_rlocator.locator.relNumber].path != NULL);

	/* Do we have the right file open already? */
	if (reln->nrel_file_segment == segment)
	{
		/* XXX How can we invalidate this if the NREL wraps around?! */
		Assert(reln->nrel_file != -1);
		return reln->nrel_file;
	}

	/* Close the current file, if we have one open. */
	if (reln->nrel_file_segment != -1)
	{
		Assert(reln->nrel_file != -1);
		FileClose(reln->nrel_file);
		reln->nrel_file = -1;
		reln->nrel_file_segment = -1;
	}

	/* Open the file we want. */
	NrelFileName(reln->smgr_rlocator.locator.relNumber, path, segment);
	reln->nrel_file = PathNameOpenFile(path, mode);
	if (reln->nrel_file >= 0)
		reln->nrel_file_segment = segment;
	else if (!(missing_ok && errno == ENOENT))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						path)));

	/*
	 * XXX That error message doesn't contain the xid; then again, the
	 * previous NREL error codes were all weird about xids anyway; maybe
	 * clog.c, notify.c et al should set a context that has NREL-specific
	 * context in a more natural format
	 */

	return reln->nrel_file;
}

void
nrelopen(SMgrRelation reln)
{
	reln->nrel_file = -1;
	reln->nrel_file_segment = -1;

	/*
	 * We don't want this to be closed at end of transaction, which would
	 * otherwise happen, because it isn't owned by a Relation.
	 */
	dlist_delete(&reln->node);
}

void
nrelclose(SMgrRelation reln, ForkNumber forknum)
{
	if (reln->nrel_file != -1)
		FileClose(reln->nrel_file);
	reln->nrel_file = -1;
	reln->nrel_file_segment = -1;
}

void
nrelwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	File file;
	Oid	rel_id;
	off_t offset;
	int rc;

	file = nrelfile(reln, blocknum, O_RDWR | O_CREAT, false);
	offset = (blocknum % NREL_PAGES_PER_SEGMENT) * BLCKSZ;

	rc = FileWrite(file, buffer, BLCKSZ, offset, WAIT_EVENT_NREL_WRITE);
	if (rc < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write %d bytes to file \"%s\" at offset %d: %m",
						BLCKSZ,
						FilePathName(file),
						(int) offset)));
	if (rc < BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write %d bytes to file \"%s\" at offset %d, only %d bytes written",
						BLCKSZ,
						FilePathName(file),
						(int) offset,
						rc)));

	rel_id = reln->smgr_rlocator.locator.relNumber;
	if (defs[rel_id].synchronize)
	{
		FileTag			tag;

		/* Tell checkpointer to synchronize this file. */
		INIT_NRELFILETAG(tag, rel_id, blocknum / NREL_PAGES_PER_SEGMENT);
		if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false))
		{
			/* Queue full.  Do it synchronously. */
			if (FileSync(file, WAIT_EVENT_NREL_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						 errmsg("could not synchronize file \"%s\": %m",
								FilePathName(file))));
		}
	}

	pgstat_count_slru_page_written(reln->smgr_rlocator.locator.relNumber);
}

void
nrelread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer)
{
	File file;
	off_t offset;
	int rc;

	file = nrelfile(reln, blocknum, O_RDWR, false);
	offset = (blocknum % NREL_PAGES_PER_SEGMENT) * BLCKSZ;

	rc = FileRead(file, buffer, BLCKSZ, offset, WAIT_EVENT_NREL_READ);
	if (rc < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read %d bytes from file \"%s\" at offset %d: %m",
						BLCKSZ,
						FilePathName(file),
						(int) offset)));
	if (rc < BLCKSZ)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read %d bytes from file \"%s\" at offset %d, only %d bytes read",
						BLCKSZ,
						FilePathName(file),
						(int) offset,
						rc)));

	pgstat_count_slru_page_read(reln->smgr_rlocator.locator.relNumber);
}

void
nrelwriteback(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber blocknum, BlockNumber nblocks)
{
	Oid rel_id;

	/* No point in flushing data we won't be fsyncing. */
	rel_id = reln->smgr_rlocator.locator.relNumber;
	if (!defs[rel_id].synchronize)
		return;

	while (nblocks > 0)
	{
		File		file;
		BlockNumber blocknum_in_this_file;
		BlockNumber nflush;

		/* File range of blocks to flush in one file. */
		blocknum_in_this_file = blocknum % NREL_PAGES_PER_SEGMENT;
		nflush = Min(nblocks, NREL_PAGES_PER_SEGMENT - blocknum_in_this_file);

		/* The file might have been unlinked already, so tolerate missing. */
		file = nrelfile(reln, blocknum, O_RDWR, true);
		if (file < 0)
			return;

		FileWriteback(file,
					  BLCKSZ * blocknum_in_this_file,
					  BLCKSZ * nflush,
					  WAIT_EVENT_NREL_FLUSH);

		nblocks -= nflush;
		blocknum += nflush;
	}
}
