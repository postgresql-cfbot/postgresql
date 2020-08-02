/*-------------------------------------------------------------------------
 *
 * checksum.c
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/page/checksum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/checksum.h"
/*
 * The actual checksum computation code is in storage/checksum_impl.h.  This
 * is done so that external programs can incorporate the checksum code by
 * #include'ing that file from the exported Postgres headers.  (Compare our
 * CRC code.)
 */
#include "storage/checksum_impl.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/lmgr.h"

/* ----------------
 * The rest of this module provides a set of functions that can be used to
 * safely check all checksums on a running cluster.
 *
 * Please note that those only perform standard buffered reads, and don't try
 * to bypass or discard the operating system cache.  If you want to check the
 * actual storage, you have to discard the operating system cache before
 * running those functions.
 *
 * To avoid torn pages and possible false positives when reading data, and to
 * keep overhead as low as possible, the following heuristics are used:
 *
 * - a shared LWLock is taken on the target buffer pool partition mapping, and
 *   we detect if a block is in shared_buffers or not.  See check_get_buffer()
 *   comments for more details about the locking strategy.
 *
 * - if a block is dirty in shared_buffers, it's ignored as it'll be flushed to
 *   disk either before the end of the next checkpoint or during recovery in
 *   case of unsafe shutdown
 *
 * - if a block is otherwise found in shared_buffers, an IO lock is taken on
 *   the block and the block is then read from storage, ignoring the block in
 *   shared_buffers
 *
 * - if a block is not found in shared_buffers, the LWLock is released and the
 *   block is read from disk without taking any lock.  If an error is detected,
 *   the read block will be discarded and retrieved again while holding the
 *   LWLock.  This is because an error due to concurrent write is possible but
 *   very unlikely, so it's better to have an optimistic approach to limit
 *   locking overhead
 *
 * The check can be performed using an SQL function, returning the list of
 * problematic blocks.
 * ----------------
 */

static bool check_buffer(char *buffer, uint32 blkno, uint16 *chk_expected,
							   uint16 *chk_found);
static void check_delay_point(void);
static bool check_get_buffer(Relation relation, ForkNumber forknum,
							 BlockNumber blkno, char *buffer, bool needlock,
							 bool *found_in_sb);

/*
 * Check data sanity for a specific block in the given fork of the given
 * relation, always retrieved locally with smgrread even if a version exists in
 * shared_buffers.  Returns false if the block appears to be corrupted, true
 * otherwise.  Note that dirty and invalid blocks won't be checked.  Caller
 * must hold at least an AccessShareLock on the relation.
 */
bool
check_one_block(Relation relation, ForkNumber forknum, BlockNumber blkno,
				uint16 *chk_expected, uint16 *chk_found)
{
	char		buffer[BLCKSZ];
	bool		force_lock = false;
	bool		found_in_sb;

	Assert(CheckRelationLockedByMe(relation, AccessShareLock, true));
	Assert(blkno < RelationGetNumberOfBlocksInFork(relation, forknum));
	Assert(smgrexists(relation->rd_smgr, forknum));

	*chk_expected = *chk_found = NoComputedChecksum;

	/*
	 * To avoid excessive overhead, the buffer will be first read without
	 * the locks that would prevent false positives, as such
	 * events should be quite rare.
	 */
Retry:
	if (!check_get_buffer(relation, forknum, blkno, buffer, force_lock,
						  &found_in_sb))
		return true;

	if (check_buffer(buffer, blkno, chk_expected, chk_found))
		return true;

	/*
	 * If we get a failure and the buffer wasn't found in shared buffers,
	 * reread the buffer with suitable lock to avoid false positive.  See
	 * check_get_buffer for more details.
	 */
	if (!found_in_sb && !force_lock)
	{
		force_lock = true;
		goto Retry;
	}

	/* A corruption is detected. */
	return false;
}

/*
 * Perform a checksum check on the passed page.  Return True iff the page is
 * valid or not, and assign the expected and found checksum in chk_expected and
 * chk_found, respectively.  Note that a page can look like new but could be
 * the result of corruption.  We still check for this case, but we can't
 * compute its checksum as pg_checksum_page() is explicitly checking for
 * non-new pages, so NoComputedChecksum will be set in chk_found.
 */
static bool
check_buffer(char *buffer, uint32 blkno, uint16 *chk_expected,
				   uint16 *chk_found)
{
	Page		page = (Page) buffer;
	PageHeader	hdr = (PageHeader) page;

	Assert(chk_expected && chk_found);

	if (PageIsNew(page))
	{
		/*
		 * Check if the page is really new or if there's corruption that
		 * affected PageIsNew detection.  Note that PageIsVerified won't try to
		 * detect checksum corruption in this case, so there's no risk of
		 * duplicated corruption report.
		 */
		if (PageIsVerified(page, blkno))
		{
			/* No corruption. */
			return true;
		}

		/*
		 * There's corruption, but since this affects PageIsNew, we
		 * can't compute a checksum, so set NoComputedChecksum for the
		 * expected checksum.
		 */
		*chk_expected = NoComputedChecksum;
		*chk_found = hdr->pd_checksum;
		return false;
	}

	*chk_expected = pg_checksum_page(buffer, blkno);
	*chk_found = hdr->pd_checksum;

	return (*chk_expected == *chk_found);
}

/*
 * Check for interrupts and cost-based delay.
 */
static void
check_delay_point(void)
{
	/* Always check for interrupts */
	CHECK_FOR_INTERRUPTS();

	if (!ChecksumCostActive || InterruptPending)
		return;

	/* Nap if appropriate */
	if (ChecksumCostBalance >= ChecksumCostLimit)
	{
		int			msec;

		msec = ChecksumCostDelay * ChecksumCostBalance / ChecksumCostLimit;
		if (msec > ChecksumCostDelay * 4)
			msec = ChecksumCostDelay * 4;

		pg_usleep(msec * 1000L);

		ChecksumCostBalance = 0;

		/* Might have gotten an interrupt while sleeping */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 *-------------------------
 * Safely read the wanted buffer from disk, dealing with possible concurrency
 * issue.  Note that if a buffer is found dirty in shared_buffers, no read will
 * be performed and the caller will be informed that no check should be done.
 * We can safely ignore such buffers as they'll be written before next
 * checkpoint's completion..
 *
 * The following locks can be used in this function:
 *
 *   - shared LWLock on the target buffer pool partition mapping.
 *   - IOLock on the buffer
 *
 * The IOLock is taken when reading the buffer from disk if it exists in
 * shared_buffers, to avoid torn pages.
 *
 * If the buffer isn't in shared_buffers, it'll be read from disk without any
 * lock unless caller asked otherwise, setting needlock.  In this case, the
 * read will be done while the buffer mapping partition LWLock is still being
 * held.  Reading with this lock is to avoid the unlikely but possible case
 * that a buffer wasn't present in shared buffers when we checked but it then
 * alloc'ed in shared_buffers, modified and flushed concurrently when we
 * later try to read it, leading to false positives due to a torn page.  Caller
 * can first read the buffer without holding the target buffer mapping
 * partition LWLock to have an optimistic approach, and reread the buffer
 * from disk in case of error.
 *
 * Caller should hold an AccessShareLock on the Relation
 *-------------------------
 */
static bool
check_get_buffer(Relation relation, ForkNumber forknum,
				 BlockNumber blkno, char *buffer, bool needlock,
				 bool *found_in_sb)
{
	bool		checkit = true;
	BufferTag	buf_tag;		/* identity of requested block */
	uint32		buf_hash;		/* hash value for buf_tag */
	LWLock	   *partLock;		/* buffer partition lock for the buffer */
	BufferDesc *bufdesc;
	int			buf_id;

	*found_in_sb = false;

	/* Check for interrupts and take throttling into account. */
	check_delay_point();

	/* create a tag so we can lookup the buffer */
	INIT_BUFFERTAG(buf_tag, relation->rd_smgr->smgr_rnode.node, forknum, blkno);

	/* determine its hash code and partition lock ID */
	buf_hash = BufTableHashCode(&buf_tag);
	partLock = BufMappingPartitionLock(buf_hash);

	/* see if the block is in the buffer pool already */
	LWLockAcquire(partLock, LW_SHARED);
	buf_id = BufTableLookup(&buf_tag, buf_hash);
	if (buf_id >= 0)
	{
		uint32		buf_state;

		*found_in_sb = true;

		/*
		 * Found it.  Now, retrieve its state to know what to do with it, and
		 * release the pin immediately.  We do so to limit overhead as much
		 * as possible.  We'll keep the shared lightweight lock on the target
		 * buffer mapping partition, so this buffer can't be evicted, and
		 * we'll acquire an IOLock on the buffer if we need to read the
		 * content on disk.
		 */
		bufdesc = GetBufferDescriptor(buf_id);

		buf_state = LockBufHdr(bufdesc);
		UnlockBufHdr(bufdesc, buf_state);

		/*
		 * Dirty pages are ignored as they'll be flushed soon. Invalid buffers
		 * are also skipped.
		 */
		if ((buf_state & BM_DIRTY) || !(buf_state & BM_TAG_VALID))
			checkit = false;

		/*
		 * Read the buffer from disk, taking an IO lock to prevent torn-page
		 * reads, in the unlikely event that it was concurrently dirtied and
		 * flushed.
		 */
		if (checkit)
		{
			LWLockAcquire(BufferDescriptorGetIOLock(bufdesc), LW_SHARED);
			smgrread(relation->rd_smgr, forknum, blkno, buffer);
			LWLockRelease(BufferDescriptorGetIOLock(bufdesc));

			/* Add a page cost. */
			ChecksumCostBalance += ChecksumCostPage;
		}
	}
	else if (needlock)
	{
		/*
		 * Caller asked to read the buffer while we have a lock on the target
		 * partition.
		 */
		smgrread(relation->rd_smgr, forknum, blkno, buffer);

		/* The buffer will have to be checked. */
		Assert(checkit);

		/* Add a page cost. */
		ChecksumCostBalance += ChecksumCostPage;
	}

	LWLockRelease(partLock);

	if (*found_in_sb || needlock)
		return checkit;

	/* After this point the buffer will always be checked. */
	Assert(checkit);

	/*
	 * Didn't find it in the buffer pool and didn't read it while holding the
	 * buffer mapping partition lock.  We'll have to try to read it from
	 * disk, after releasing the target partition lock to avoid excessive
	 * overhead.  It means that it's possible to get a torn page later, so
	 * we'll have to retry with a suitable lock in case of error to avoid
	 * false positive.
	 */
	smgrread(relation->rd_smgr, forknum, blkno, buffer);

	/* Add a page cost. */
	ChecksumCostBalance += ChecksumCostPage;

	return checkit;
}
