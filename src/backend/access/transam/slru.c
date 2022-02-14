/*-------------------------------------------------------------------------
 *
 * slru.c
 *		Simple buffering for transaction status logfiles
 *
 * XXX write me
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

#define SlruFileName(rel_id, path, seg)							\
	snprintf(path, MAXPGPATH, "%s/%04X", defs[(rel_id)].path, seg)

struct SlruDef {
	const char *name;
	const char *path;
	bool synchronize;
};

static const struct SlruDef defs[] = {
	[SLRU_CLOG_REL_ID] = {
		.name = "Xact",
		.path = "pg_xact",
		.synchronize = true,
	},
	[SLRU_SUBTRANS_REL_ID] = {
		.name = "Subtrans",
		.path = "pg_subtrans",
	},
	[SLRU_MULTIXACT_OFFSET_REL_ID] = {
		.name = "MultiXactOffset",
		.path = "pg_multixact/offsets",
		.synchronize = true,
	},
	[SLRU_MULTIXACT_MEMBER_REL_ID] = {
		.name = "MultiXactMember",
		.path = "pg_multixact/members",
		.synchronize = true,
	},
	[SLRU_COMMITTS_REL_ID] = {
		.name = "CommitTs",
		.path = "pg_commit_ts",
		.synchronize = true,
	},
	[SLRU_SERIAL_REL_ID] = {
		.name = "Serial",
		.path = "pg_serial",
	},
	[SLRU_NOTIFY_REL_ID] = {
		.name = "Notify",
		.path = "pg_notify",
	},
};

/*
 * We'll maintain a little cache of recently seen buffers, to try to avoid the
 * buffer mapping table on repeat access (ie the busy end of the CLOG).  One
 * entry per SLRU relation.
 */
struct SlruRecentBuffer {
	int			pageno;
	Buffer		recent_buffer;
};

static struct SlruRecentBuffer slru_recent_buffers[lengthof(defs)];

/*
 * Populate a file tag identifying an SLRU segment file.
 */
#define INIT_SLRUFILETAG(a,xx_rel_id,xx_segno) \
( \
	memset(&(a), 0, sizeof(FileTag)), \
	(a).handler = SYNC_HANDLER_SLRU, \
	(a).rnode = SlruRelFileNode(xx_rel_id), \
	(a).segno = (xx_segno) \
)

static bool SlruScanDirCbDeleteCutoff(Oid rel_id,
									  SlruPagePrecedesFunction PagePrecedes,
									  char *filename,
									  int segpage, void *data);
static void SlruInternalDeleteSegment(Oid rel_id, int segno);

static File slrufile(SMgrRelation reln, BlockNumber blocknum, int mode,
					 bool missing_ok);

/*
 * Return whether the given page exists on disk.
 *
 * A false return means that either the file does not exist, or that it's not
 * large enough to contain the given page.
 */
bool
SimpleLruDoesPhysicalPageExist(Oid rel_id, int pageno)
{
	int			rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
	off_t		offset = rpageno * BLCKSZ;
	off_t		size;
	File		file;
	RelFileNode	rnode = SlruRelFileNode(rel_id);
	SMgrRelation reln = smgropen(rnode, InvalidBackendId);

	/* update the stats counter of checked pages */
	pgstat_count_slru_page_exists(rel_id);

	file = slrufile(reln, pageno, O_RDWR, true);
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
 * All SLRUs prevent concurrent calls to this function, either with an LWLock
 * or by calling it only as part of a checkpoint.  Mutual exclusion must begin
 * before computing cutoffPage.  Mutual exclusion must end after any limit
 * update that would permit other backends to write fresh data into the
 * segment immediately preceding the one containing cutoffPage.  Otherwise,
 * when the SLRU is quite full, SimpleLruTruncate() might delete that segment
 * after it has accrued freshly-written data.
 */
void
SimpleLruTruncate(Oid rel_id, SlruPagePrecedesFunction PagePrecedes, int cutoffPage)
{
	/* update the stats counter of truncates */
	pgstat_count_slru_truncate(rel_id);

	/* Now we can remove the old segment(s) */
	(void) SlruScanDirectory(rel_id, PagePrecedes, SlruScanDirCbDeleteCutoff,
							 &cutoffPage);
}

/*
 * Delete an individual SLRU segment.
 *
 * NB: This does not touch the SLRU buffers themselves, callers have to ensure
 * they either can't yet contain anything, or have already been cleaned out.
 */
static void
SlruInternalDeleteSegment(Oid rel_id, int segno)
{
	char		path[MAXPGPATH];

	/* Forget any fsync requests queued for this segment. */
	if (defs[rel_id].synchronize)
	{
		FileTag		tag;

		INIT_SLRUFILETAG(tag, rel_id, segno);
		RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true);
	}

	/* Unlink the file. */
	SlruFileName(rel_id, path, segno);
	ereport(DEBUG2, (errmsg_internal("removing file \"%s\"", path)));
	unlink(path);
}

/*
 * Delete an individual SLRU segment, identified by the segment number.
 */
void
SlruDeleteSegment(Oid rel_id, int segno)
{
	RelFileNode rnode = SlruRelFileNode(rel_id);

	/* Clean out any possibly existing references to the segment. */
	for (int i = 0; i < SLRU_PAGES_PER_SEGMENT; ++i)
		DiscardBuffer(rnode, MAIN_FORKNUM, segno * SLRU_PAGES_PER_SEGMENT + i);

	SlruInternalDeleteSegment(rel_id, segno);
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
SlruScanDirCbReportPresence(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
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
SlruScanDirCbDeleteCutoff(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
						  char *filename, int segpage, void *data)
{
	RelFileNode	rnode = SlruRelFileNode(rel_id);
	int			cutoffPage = *(int *) data;

	if (SlruMayDeleteSegment(PagePrecedes, segpage, cutoffPage))
	{
		for (int i = 0; i < SLRU_PAGES_PER_SEGMENT; ++i)
			DiscardBuffer(rnode, MAIN_FORKNUM, segpage + i);
		SlruInternalDeleteSegment(rel_id, segpage / SLRU_PAGES_PER_SEGMENT);
	}

	return false;				/* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
bool
SlruScanDirCbDeleteAll(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
					   char *filename, int segpage, void *data)
{
	SlruInternalDeleteSegment(rel_id, segpage / SLRU_PAGES_PER_SEGMENT);

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
SlruScanDirectory(Oid rel_id, SlruPagePrecedesFunction PagePrecedes,
				  SlruScanCallback callback, void *data)
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
			segpage = segno * SLRU_PAGES_PER_SEGMENT;

			elog(DEBUG2, "SlruScanDirectory invoking callback on %s/%s",
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
CheckPointSLRU(void)
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
ReadSlruBuffer(Oid rel_id, int pageno)
{
	RelFileNode rnode = SlruRelFileNode(rel_id);
	Buffer buffer;
	bool hit;

	/* Try to avoid doing a buffer mapping table lookup for repeated access. */
	buffer = slru_recent_buffers[rel_id].recent_buffer;
	if (slru_recent_buffers[rel_id].pageno == pageno &&
		BufferIsValid(buffer) &&
		ReadRecentBuffer(rnode, MAIN_FORKNUM, pageno, buffer))
	{
		pgstat_count_slru_page_hit(rel_id);
		return buffer;
	}

	/* Regular lookup. */
	buffer = ReadBufferWithoutRelcacheWithHit(rnode, MAIN_FORKNUM, pageno,
											  RBM_NORMAL, &hit);

	/* Remember where this page is for next time. */
	slru_recent_buffers[rel_id].pageno = pageno;
	slru_recent_buffers[rel_id].recent_buffer = buffer;

	if (hit)
		pgstat_count_slru_page_hit(rel_id);

	return buffer;
}

/*
 * Zero-initialize a buffer.  Buffer is pinned and exclusively locked on return.
 */
Buffer
ZeroSlruBuffer(Oid rel_id, int pageno)
{
	RelFileNode rnode = SlruRelFileNode(rel_id);
	Buffer buffer;

	buffer = ReadBufferWithoutRelcache(rnode, MAIN_FORKNUM, pageno,
									   RBM_ZERO_AND_LOCK, NULL);

	/* Remember where this page is for next time. */
	slru_recent_buffers[rel_id].pageno = pageno;
	slru_recent_buffers[rel_id].recent_buffer = buffer;

	pgstat_count_slru_page_zeroed(rel_id);

	return buffer;
}

Oid
SlruRelIdByName(const char *name)
{
	for (int i = 0; i < lengthof(defs); ++i)
		if (strcmp(defs[i].name, name) == 0)
			return i;

	elog(ERROR, "unknown SLRU \"%s\"", name);
}

const char *
SlruName(Oid rel_id)
{
	if (rel_id >= SLRU_NUM_RELS)
		elog(ERROR, "invalid SLRU rel ID %u", rel_id);

	return defs[rel_id].name;
}

int
slrusyncfiletag(const FileTag *ftag, char *path)
{
	SMgrRelation reln;
	File		file;

	reln = smgropen(ftag->rnode, InvalidBackendId);
	file = slrufile(reln, ftag->segno * SLRU_PAGES_PER_SEGMENT, O_RDWR, true);
	if (file < 0)
	{
		/* Path is reported here so the caller can make an error message */
		SlruFileName(ftag->rnode.relNode, path, ftag->segno);
		return -1;
	}

	pgstat_count_slru_flush(reln->smgr_rnode.node.relNode);

	return FileSync(file, WAIT_EVENT_SLRU_SYNC);
}

static File
slrufile(SMgrRelation reln, BlockNumber blocknum, int mode, bool missing_ok)
{
	int segment = blocknum / SLRU_PAGES_PER_SEGMENT;
	char path[MAXPGPATH];

	Assert(reln->smgr_rnode.node.dbNode == SLRU_DB_ID);
	Assert(reln->smgr_rnode.node.relNode < lengthof(defs));
	Assert(defs[reln->smgr_rnode.node.relNode].path != NULL);

	/* Do we have the right file open already? */
	if (reln->slru_file_segment == segment)
	{
		/* XXX How can we invalidate this if the SLRU wraps around?! */
		Assert(reln->slru_file != -1);
		return reln->slru_file;
	}

	/* Close the current file, if we have one open. */
	if (reln->slru_file_segment != -1)
	{
		Assert(reln->slru_file != -1);
		FileClose(reln->slru_file);
		reln->slru_file = -1;
		reln->slru_file_segment = -1;
	}

	/* Open the file we want. */
	SlruFileName(reln->smgr_rnode.node.relNode, path, segment);
	reln->slru_file = PathNameOpenFile(path, mode);
	if (reln->slru_file >= 0)
		reln->slru_file_segment = segment;
	else if (!(missing_ok && errno == ENOENT))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						path)));

	/*
	 * XXX That error message doesn't contain the xid; then again, the
	 * previous SLRU error codes were all weird about xids anyway; maybe
	 * clog.c, notify.c et al should set a context that has SLRU-specific
	 * context in a more natural format
	 */

	return reln->slru_file;
}

void
slruopen(SMgrRelation reln)
{
	reln->slru_file = -1;
	reln->slru_file_segment = -1;

	/*
	 * We don't want this to be closed at end of transaction, which would
	 * otherwise happen, because it isn't owned by a Relation.
	 */
	dlist_delete(&reln->node);
}

void
slruclose(SMgrRelation reln, ForkNumber forknum)
{
	if (reln->slru_file != -1)
		FileClose(reln->slru_file);
	reln->slru_file = -1;
	reln->slru_file_segment = -1;
}

void
slruwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  char *buffer, bool skipFsync)
{
	File file;
	Oid	rel_id;
	off_t offset;
	int rc;

	file = slrufile(reln, blocknum, O_RDWR | O_CREAT, false);
	offset = (blocknum % SLRU_PAGES_PER_SEGMENT) * BLCKSZ;

	rc = FileWrite(file, buffer, BLCKSZ, offset, WAIT_EVENT_SLRU_WRITE);
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

	rel_id = reln->smgr_rnode.node.relNode;
	if (defs[rel_id].synchronize)
	{
		FileTag			tag;

		/* Tell checkpointer to synchronize this file. */
		INIT_SLRUFILETAG(tag, rel_id, blocknum / SLRU_PAGES_PER_SEGMENT);
		if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false))
		{
			/* Queue full.  Do it synchronously. */
			if (FileSync(file, WAIT_EVENT_SLRU_SYNC) < 0)
				ereport(data_sync_elevel(ERROR),
						(errcode_for_file_access(),
						 errmsg("could not synchronize file \"%s\": %m",
								FilePathName(file))));
		}
	}

	pgstat_count_slru_page_written(reln->smgr_rnode.node.relNode);
}

void
slruread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		 char *buffer)
{
	File file;
	off_t offset;
	int rc;

	file = slrufile(reln, blocknum, O_RDWR, false);
	offset = (blocknum % SLRU_PAGES_PER_SEGMENT) * BLCKSZ;

	rc = FileRead(file, buffer, BLCKSZ, offset, WAIT_EVENT_SLRU_READ);
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

	pgstat_count_slru_page_read(reln->smgr_rnode.node.relNode);
}

void
slruwriteback(SMgrRelation reln, ForkNumber forknum,
			  BlockNumber blocknum, BlockNumber nblocks)
{
	/* XXX TODO */
}
