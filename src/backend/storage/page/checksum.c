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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_authid_d.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/lockdefs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * A zero checksum can never be computed, see pg_checksum_page() */
#define NoComputedChecksum	0

/* The rest of this module provides a set of functions that can be used to
 * safely check all checksums on a running cluster.
 *
 * Please note that those only perform standard buffered reads, and don't try
 * to bypass or discard the operating system cache.  If you want to check the
 * actual storage, you have to discard the operating system cache before
 * running those functions.
 *
 * To avoid torn page and possible false postives when reading data, and
 * keeping overhead as low as possible, the following heuristics are used:
 *
 * - a shared LWLock is taken on the target buffer pool partition mapping, and
 *   we detect if a block is in shared_buffers or not.  See get_buffer()
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
 * - if a block is not found in shared_buffers, the LWLock is relased and the
 *   block is read from disk without taking any lock.  If an error is detected,
 *   the read block will be discarded and retrieved again while holding the
 *   LWLock.  This is because an error due to concurrent write is possible but
 *   very unlikely, so it's better to have an optimistic approach to limit
 *   locking overhead
 *
 * The check can be performed using an SQL function, returning the list of
 * problematic blocks.
 *
 * Vacuum's GUCs are used to avoid consuming too much resources while running
 * this tool.
 */

static void check_all_relations(TupleDesc tupdesc, Tuplestorestate *tupstore,
									  ForkNumber forknum);
static bool check_buffer(char *buffer, uint32 blkno, uint16 *chk_expected,
							   uint16 *chk_found);
static void check_one_relation(TupleDesc tupdesc, Tuplestorestate *tupstore,
								 Oid relid, ForkNumber single_forknum);
static void check_relation_fork(TupleDesc tupdesc, Tuplestorestate *tupstore,
									  Relation relation, ForkNumber forknum);
static void check_delay_point(void);
static void check_get_buffer(Relation relation, ForkNumber forknum,
							 BlockNumber blkno, char *buffer, bool needlock,
							 bool *checkit, bool *found_in_sb);

/*
 * Iterate over all relation having a physical storage in the current database
 * and perform a check for all of them.  Note that this function can run for
 * a very long time, and as such can cause bloat or other issues.  Client
 * should iterate over a local list of oid using the per-table mode if
 * keeping an open transaction for very long can be a concern.
 */
static void
check_all_relations(TupleDesc tupdesc, Tuplestorestate *tupstore,
						  ForkNumber forknum)
{
	List	   *relids = NIL;
	ListCell   *lc;
	Relation	relation;
	TableScanDesc scan;
	HeapTuple	tuple;

	relation = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(relation, 0, NULL);

	while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_class class = (Form_pg_class) GETSTRUCT(tuple);

		if (!RELKIND_HAS_STORAGE(class->relkind))
			continue;

		relids = lappend_oid(relids, class->oid);
	}

	table_endscan(scan);
	table_close(relation, AccessShareLock);	/* release lock */

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);

		check_one_relation(tupdesc, tupstore, relid, forknum);
	}
}

/*
 * Perform a checksum check on the passed page.  Returns whether the page is
 * valid or not, and assign the expected and found checksum in chk_expected and
 * chk_found, respectively.  Note that a page can look like new but could be
 * the result of a corruption.  We still check for this case, but we can't
 * compute its checksum as pg_checksum_page() is explictly checking for non-new
 * pages, so NoComputedChecksum will be set in chk_found.
 */
static bool
check_buffer(char *buffer, uint32 blkno, uint16 *chk_expected,
				   uint16 *chk_found)
{
	PageHeader	hdr = (PageHeader) buffer;

	Assert(chk_expected && chk_found);

	if (PageIsNew(hdr))
	{
		if (PageIsVerified(buffer, blkno))
		{
			/*
			 * If the page is really new, there won't by any checksum to be
			 * computed or expected.
			 */
			*chk_expected = *chk_found = NoComputedChecksum;
			return true;
		}
		else
		{
			/*
			 * There's a corruption, but since this affect PageIsNew, we
			 * can't compute a checksum, so set NoComputedChecksum for the
			 * expected checksum.
			 */
			*chk_expected = NoComputedChecksum;
			*chk_found = hdr->pd_checksum;
		}
		return false;
	}

	*chk_expected = pg_checksum_page(buffer, blkno);
	*chk_found = hdr->pd_checksum;

	return (*chk_expected == *chk_found);
}

/*
 * Perform the check on a single relation, possibly filtered with a single
 * fork.  This function will check if the given relation exists or not, as
 * a relation could be dropped after checking for the list of relations and
 * before getting here, and we don't want to error out in this case.
 */
static void
check_one_relation(TupleDesc tupdesc, Tuplestorestate *tupstore,
					 Oid relid, ForkNumber single_forknum)
{
	Relation	relation = NULL;
	ForkNumber	forknum;

	/* Check if the relation (still) exists */
	if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
	{
		/*
		 * We use a standard relation_open() to acquire the initial lock.  It
		 * means that this will block until the lock is acquired, or will
		 * raise an ERROR if lock_timeout has been set.  If caller wants to
		 * check multiple tables while relying on a maximum wait time, it
		 * should process tables one by one instead of relying on a global
		 * processing with the main SRF.
		 */
		relation = relation_open(relid, AccessShareLock);
	}

	if (!RelationIsValid(relation))
	{
		elog(WARNING, "relation %u does not exists in database \"%s\"",
			 relid, get_database_name(MyDatabaseId));
		return;
	}

	if (!RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("Relation \"%s\" does not have storage to be checked.",
				 quote_qualified_identifier(
					 get_namespace_name(get_rel_namespace(relid)),
					 get_rel_name(relid)))));

	RelationOpenSmgr(relation);

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
	{
		if (single_forknum != InvalidForkNumber && single_forknum != forknum)
			continue;

		if (smgrexists(relation->rd_smgr, forknum))
			check_relation_fork(tupdesc, tupstore, relation, forknum);
	}

	relation_close(relation, AccessShareLock);	/* release the lock */
}

/*
 * For a given relation and fork, Do the real work of iterating over all pages
 * and doing the check.  Caller must hold an AccessShareLock lock on the given
 * relation.
 */
static void
check_relation_fork(TupleDesc tupdesc, Tuplestorestate *tupstore,
						  Relation relation, ForkNumber forknum)
{
	BlockNumber blkno,
				nblocks;
	char		buffer[BLCKSZ];
	bool		checkit,
				found_in_sb;

	/*
	 * We remember the number of blocks here.  Since caller must hold a lock on
	 * the relation, we know that it won't be truncated while we're iterating
	 * over the blocks.  Any block added after this function started won't be
	 * checked, but this is out of scope as such pages will be flushed before
	 * the next checkpoint's completion.
	 */
	nblocks = RelationGetNumberOfBlocksInFork(relation, forknum);

#define CRF_COLS			5	/* Number of output arguments in the SRF */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		bool		force_lock = false;
		uint16		chk_expected,
					chk_found;
		Datum		values[CRF_COLS];
		bool		nulls[CRF_COLS];
		int			i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/*
		 * To avoid too much overhead, the buffer will be first read without
		 * the locks that would guarantee the lack of false positive, as such
		 * events should be quite rare.
		 */
Retry:
		check_get_buffer(relation, forknum, blkno, buffer, force_lock,
						 &checkit, &found_in_sb);

		if (!checkit)
			continue;

		if (check_buffer(buffer, blkno, &chk_expected, &chk_found))
			continue;

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

		values[i++] = ObjectIdGetDatum(relation->rd_id);
		values[i++] = Int32GetDatum(forknum);
		values[i++] = UInt32GetDatum(blkno);
		/*
		 * This can happen if a corruption makes the block appears as
		 * PageIsNew() but isn't a new page.
		 */
		if (chk_expected == NoComputedChecksum)
			nulls[i++] = true;
		else
			values[i++] = UInt16GetDatum(chk_expected);
		values[i++] = UInt16GetDatum(chk_found);

		Assert(i == CRF_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
}

/*
 * Check for interrupts and cost-based delay.
 */
static void
check_delay_point(void)
{
	/* Always check for interrupts */
	CHECK_FOR_INTERRUPTS();

	/* Nap if appropriate */
	if (!InterruptPending && VacuumCostBalance >= VacuumCostLimit)
	{
		int			msec;

		msec = VacuumCostDelay * VacuumCostBalance / VacuumCostLimit;
		if (msec > VacuumCostDelay * 4)
			msec = VacuumCostDelay * 4;

		pg_usleep(msec * 1000L);

		VacuumCostBalance = 0;

		/* Might have gotten an interrupt while sleeping */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
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
 * later try to read it, leading to false positive due to torn page.  Caller
 * can read first the buffer without holding the target buffer mapping
 * partition LWLock to have an optimistic approach, and reread the buffer
 * from disk in case of error.
 *
 * Caller should hold an AccessShareLock on the Relation
 */
static void
check_get_buffer(Relation relation, ForkNumber forknum,
				 BlockNumber blkno, char *buffer, bool needlock, bool *checkit,
				 bool *found_in_sb)
{
	BufferTag	buf_tag;		/* identity of requested block */
	uint32		buf_hash;		/* hash value for buf_tag */
	LWLock	   *partLock;		/* buffer partition lock for the buffer */
	BufferDesc *bufdesc;
	int			buf_id;

	*checkit = true;
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

		/* Dirty pages are ignored as they'll be flushed soon. */
		if (buf_state & BM_DIRTY)
			*checkit = false;

		/*
		 * Read the buffer from disk, taking on IO lock to prevent torn-page
		 * reads, in the unlikely event that it was concurrently dirted and
		 * flushed.
		 */
		if (*checkit)
		{
			LWLockAcquire(BufferDescriptorGetIOLock(bufdesc), LW_SHARED);
			smgrread(relation->rd_smgr, forknum, blkno, buffer);
			LWLockRelease(BufferDescriptorGetIOLock(bufdesc));

			/*
			 * Add a page miss cost, as we're always reading outside the
			 * shared buffers.
			 */
			VacuumCostBalance += VacuumCostPageMiss;
		}
	}
	else if (needlock)
	{
		/*
		 * Caller asked to read the buffer while we have a lock on the target
		 * partition.
		 */
		smgrread(relation->rd_smgr, forknum, blkno, buffer);

		/*
		 * Add a page miss cost, as we're always reading outside the shared
		 * buffers.
		 */
		VacuumCostBalance += VacuumCostPageMiss;
	}

	LWLockRelease(partLock);

	if (*found_in_sb || needlock)
		return;

	/*
	 * Didn't find it in the buffer pool and didn't read it while holding the
	 * buffer mapping partition lock.  We'll have to try to read it from
	 * disk, after releasing the target partition lock to avoid too much
	 * overhead.  It means that it's possible to get a torn page later, so
	 * we'll have to retry with a suitable lock in case of error to avoid
	 * false positive.
	 */
	smgrread(relation->rd_smgr, forknum, blkno, buffer);

	/*
	 * Add a page miss cost, as we're always reading outside the shared
	 * buffers.
	 */
	VacuumCostBalance += VacuumCostPageMiss;
}

Datum
pg_check_relation(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ForkNumber	forknum = InvalidForkNumber;
	Oid			relid = InvalidOid;
	const char *forkname;

	if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_SERVER_FILES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superuser or a member of the pg_read_server_files role may use this function")));

	if (!DataChecksumsEnabled())
		elog(ERROR, "Data checksums are not enabled");

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (PG_NARGS() >= 1 && !PG_ARGISNULL(0))
		relid = PG_GETARG_OID(0);

	if (PG_NARGS() == 2 && !PG_ARGISNULL(1))
	{
		forkname = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
		forknum = forkname_to_number(forkname);
	}

	VacuumCostBalance = 0;

	if (OidIsValid(relid))
		check_one_relation(tupdesc, tupstore, relid, forknum);
	else
		check_all_relations(tupdesc, tupstore, forknum);

	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
