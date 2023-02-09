/*-------------------------------------------------------------------------
 *
 * subtrans.c
 *		PostgreSQL subtransaction-log manager
 *
 * The pg_subtrans manager is a pg_xact-like manager that stores the parent
 * transaction Id for each transaction.  It is a fundamental part of the
 * nested transactions implementation.  A main transaction has a parent
 * of InvalidTransactionId, and each subtransaction has its immediate parent.
 * The tree can easily be walked from child to parent, but not in the
 * opposite direction.
 *
 * This code is based on xact.c, but the robustness requirements
 * are completely different from pg_xact, because we only need to remember
 * pg_subtrans information for currently-open transactions.  Thus, there is
 * no need to preserve data over a crash and restart.
 *
 * There are no XLOG interactions since we do not care about preserving
 * data across crashes.  During database startup, we simply force the
 * currently-active page of SUBTRANS to zeroes.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/subtrans.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "pg_trace.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"


/*
 * Defines for SubTrans page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * SubTrans page numbering also wraps around at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE, and segment numbering at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateSUBTRANS (see SubTransPagePrecedes) and zeroing
 * them in StartupSUBTRANS.
 */

/* We need four bytes per xact */
#define SUBTRANS_XACTS_PER_PAGE ((BLCKSZ - SizeOfPageHeaderData) / sizeof(TransactionId))

#define TransactionIdToPage(xid) ((xid) / (TransactionId) SUBTRANS_XACTS_PER_PAGE)
#define TransactionIdToEntry(xid) ((xid) % (TransactionId) SUBTRANS_XACTS_PER_PAGE)


static Buffer ZeroSUBTRANSPage(int pageno);
static bool SubTransPagePrecedes(int page1, int page2);


/*
 * Record the parent of a subtransaction in the subtrans log.
 */
void
SubTransSetParent(TransactionId xid, TransactionId parent)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToEntry(xid);
	TransactionId *ptr;
	Buffer		buffer;

	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

	buffer = ReadSlruBuffer(SLRU_SUBTRANS_ID, pageno, RBM_NORMAL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	ptr = (TransactionId *) PageGetContents(BufferGetPage(buffer));
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != parent)
	{
		Assert(*ptr == InvalidTransactionId);
		*ptr = parent;
		MarkBufferDirty(buffer);
	}

	UnlockReleaseBuffer(buffer);
}

/*
 * Interrogate the parent of a transaction in the subtrans log.
 */
TransactionId
SubTransGetParent(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			entryno = TransactionIdToEntry(xid);
	TransactionId *ptr;
	TransactionId parent;
	Buffer		buffer;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	/* Bootstrap and frozen XIDs have no parent */
	if (!TransactionIdIsNormal(xid))
		return InvalidTransactionId;

	buffer = ReadSlruBuffer(SLRU_SUBTRANS_ID, pageno, RBM_NORMAL);

	ptr = (TransactionId *) PageGetContents(BufferGetPage(buffer));
	ptr += entryno;

	parent = *ptr;

	ReleaseBuffer(buffer);

	return parent;
}

/*
 * SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
TransactionId
SubTransGetTopmostTransaction(TransactionId xid)
{
	TransactionId parentXid = xid,
				previousXid = xid;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	while (TransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;
		if (TransactionIdPrecedes(parentXid, TransactionXmin))
			break;
		parentXid = SubTransGetParent(parentXid);

		/*
		 * By convention the parent xid gets allocated first, so should always
		 * precede the child xid. Anything else points to a corrupted data
		 * structure that could lead to an infinite loop, so exit.
		 */
		if (!TransactionIdPrecedes(parentXid, previousXid))
			elog(ERROR, "pg_subtrans contains invalid entry: xid %u points to parent xid %u",
				 previousXid, parentXid);
	}

	Assert(TransactionIdIsValid(previousXid));

	return previousXid;
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial SUBTRANS segment.  (The SUBTRANS directory is assumed to
 * have been created by the initdb shell script, and SUBTRANSShmemInit
 * must have been called already.)
 *
 * Note: it's not really necessary to create the initial segment now,
 * since slru.c would create it on first write anyway.  But we may as well
 * do it to be sure the directory is set up correctly.
 */
void
BootStrapSUBTRANS(void)
{
	Buffer		buffer;

	SlruPagePrecedesUnitTests(SubTransPagePrecedes, SUBTRANS_XACTS_PER_PAGE);

	/* Create and zero the first page of the subtrans log */
	buffer = ZeroSUBTRANSPage(0);

	/* Make sure it's written out */
	FlushOneBuffer(buffer);
	UnlockReleaseBuffer(buffer);
}

/*
 * Initialize (or reinitialize) a page of SUBTRANS to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static Buffer
ZeroSUBTRANSPage(int pageno)
{
	Buffer		buffer;
	Page		page;

	buffer = ZeroSlruBuffer(SLRU_SUBTRANS_ID, pageno);
	page = BufferGetPage(buffer);
	PageInitSLRU(page, BLCKSZ, 0);

	MarkBufferDirty(buffer);

	return buffer;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupSUBTRANS(TransactionId oldestActiveXID)
{
	FullTransactionId nextXid;
	int			startPage;
	int			endPage;

	/*
	 * Since we don't expect pg_subtrans to be valid across crashes, we
	 * initialize the currently-active page(s) to zeroes during startup.
	 * Whenever we advance into a new page, ExtendSUBTRANS will likewise zero
	 * the new page without regard to whatever was previously on disk.
	 */

	startPage = TransactionIdToPage(oldestActiveXID);
	nextXid = ShmemVariableCache->nextXid;
	endPage = TransactionIdToPage(XidFromFullTransactionId(nextXid));

	while (startPage != endPage)
	{
		UnlockReleaseBuffer(ZeroSUBTRANSPage(startPage));
		startPage++;
		/* must account for wraparound */
		if (startPage > TransactionIdToPage(MaxTransactionId))
			startPage = 0;
	}
	UnlockReleaseBuffer(ZeroSUBTRANSPage(startPage));
}

/*
 * Make sure that SUBTRANS has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty subtrans page to make room
 * in shared memory.
 */
void
ExtendSUBTRANS(TransactionId newestXact)
{
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToEntry(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	/* Zero the page */
	UnlockReleaseBuffer(ZeroSUBTRANSPage(pageno));
}


/*
 * Remove all SUBTRANS segments before the one holding the passed transaction ID
 *
 * oldestXact is the oldest TransactionXmin of any running transaction.  This
 * is called only during checkpoint.
 */
void
TruncateSUBTRANS(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to SimpleLruTruncate.  We step
	 * back one transaction to avoid passing a cutoff page that hasn't been
	 * created yet in the rare case that oldestXact would be the first item on
	 * a page and oldestXact == next XID.  In that case, if we didn't subtract
	 * one, we'd trigger SimpleLruTruncate's wraparound detection.
	 */
	TransactionIdRetreat(oldestXact);
	cutoffPage = TransactionIdToPage(oldestXact);

	SimpleLruTruncate(SLRU_SUBTRANS_ID, SubTransPagePrecedes, cutoffPage);
}


/*
 * Decide whether a SUBTRANS page number is "older" for truncation purposes.
 * Analogous to CLOGPagePrecedes().
 */
static bool
SubTransPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * SUBTRANS_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId + 1;
	xid2 = ((TransactionId) page2) * SUBTRANS_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId + 1;

	return (TransactionIdPrecedes(xid1, xid2) &&
			TransactionIdPrecedes(xid1, xid2 + SUBTRANS_XACTS_PER_PAGE - 1));
}
