/*-------------------------------------------------------------------------
 *
 * seqlocalam.c
 *	  Local sequence access manager
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/access/sequence/seqlocalam.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/multixact.h"
#include "access/seqlocalam.h"
#include "access/sequenceam.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage_xlog.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"


/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOCAL_LOG_VALS	32

/* Format of tuples stored in heap table associated to local sequences */
typedef struct FormData_pg_seq_local_data
{
	int64		last_value;
	int64		log_cnt;
	bool		is_called;
} FormData_pg_seq_local_data;

typedef FormData_pg_seq_local_data *Form_pg_seq_local_data;

/*
 * Columns of a local sequence relation
 */
#define SEQ_LOCAL_COL_LASTVAL			1
#define SEQ_LOCAL_COL_LOG				2
#define SEQ_LOCAL_COL_CALLED			3

#define SEQ_LOCAL_COL_FIRSTCOL		SEQ_LOCAL_COL_LASTVAL
#define SEQ_LOCAL_COL_LASTCOL		SEQ_LOCAL_COL_CALLED


/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOCAL_LOG_VALS		32

static Form_pg_seq_local_data read_seq_tuple(Relation rel,
											 Buffer *buf,
											 HeapTuple seqdatatuple);
static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static void fill_seq_fork_with_data(Relation rel, HeapTuple tuple,
									ForkNumber forkNum);

/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqdatatuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
static Form_pg_seq_local_data
read_seq_tuple(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
	Page		page;
	ItemId		lp;
	seq_local_magic *sm;
	Form_pg_seq_local_data seq;

	*buf = ReadBuffer(rel, 0);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	sm = (seq_local_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SEQ_LOCAL_MAGIC)
		elog(ERROR, "bad magic number in sequence \"%s\": %08X",
			 RelationGetRelationName(rel), sm->magic);

	lp = PageGetItemId(page, FirstOffsetNumber);
	Assert(ItemIdIsNormal(lp));

	/* Note we currently only bother to set these two fields of *seqdatatuple */
	seqdatatuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	seqdatatuple->t_len = ItemIdGetLength(lp);

	/*
	 * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
	 * a sequence, which would leave a non-frozen XID in the sequence tuple's
	 * xmax, which eventually leads to clog access failures or worse. If we
	 * see this has happened, clean up after it.  We treat this like a hint
	 * bit update, ie, don't bother to WAL-log it, since we can certainly do
	 * this again if the update gets lost.
	 */
	Assert(!(seqdatatuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI));
	if (HeapTupleHeaderGetRawXmax(seqdatatuple->t_data) != InvalidTransactionId)
	{
		HeapTupleHeaderSetXmax(seqdatatuple->t_data, InvalidTransactionId);
		seqdatatuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
		seqdatatuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
		MarkBufferDirtyHint(*buf, true);
	}

	seq = (Form_pg_seq_local_data) GETSTRUCT(seqdatatuple);

	return seq;
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 *
 * This handles unlogged sequences by writing to both the main and the init
 * fork as necessary.
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
	fill_seq_fork_with_data(rel, tuple, MAIN_FORKNUM);

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		SMgrRelation srel;

		srel = smgropen(rel->rd_locator, INVALID_PROC_NUMBER);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(&rel->rd_locator, INIT_FORKNUM);
		fill_seq_fork_with_data(rel, tuple, INIT_FORKNUM);
		FlushRelationBuffers(rel);
		smgrclose(srel);
	}
}

/*
 * Initialize a sequence's relation fork with the specified tuple as content
 */
static void
fill_seq_fork_with_data(Relation rel, HeapTuple tuple, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	seq_local_magic *sm;
	OffsetNumber offnum;

	/* Initialize first page of relation with special magic number */

	buf = ExtendBufferedRel(BMR_REL(rel), forkNum, NULL,
							EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(seq_local_magic));
	sm = (seq_local_magic *) PageGetSpecialPointer(page);
	sm->magic = SEQ_LOCAL_MAGIC;

	/* Now insert sequence tuple */

	/*
	 * Since VACUUM does not process sequences, we have to force the tuple to
	 * have xmin = FrozenTransactionId now.  Otherwise it would become
	 * invisible to SELECTs after 2G transactions.  It is okay to do this
	 * because if the current transaction aborts, no other xact will ever
	 * examine the sequence tuple anyway.
	 */
	HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
	HeapTupleHeaderSetXminFrozen(tuple->t_data);
	HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

	/* check the comment above nextval_internal()'s equivalent call. */
	if (RelationNeedsWAL(rel))
		GetTopTransactionId();

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
						 InvalidOffsetNumber, false, false);
	if (offnum != FirstOffsetNumber)
		elog(ERROR, "failed to add sequence tuple to page");

	/* XLOG stuff */
	if (RelationNeedsWAL(rel) || forkNum == INIT_FORKNUM)
	{
		xl_seq_local_rec xlrec;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		xlrec.locator = rel->rd_locator;

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_local_rec));
		XLogRegisterData((char *) tuple->t_data, tuple->t_len);

		recptr = XLogInsert(RM_SEQ_LOCAL_ID, XLOG_SEQ_LOCAL_LOG);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * seq_local_nextval()
 *
 * Allocate a new value for a local sequence, based on the sequence
 * configuration.
 */
static int64
seq_local_nextval(Relation rel, int64 incby, int64 maxv,
			  int64 minv, int64 cache, bool cycle,
			  int64 *last)
{
	int64		result;
	int64		fetch;
	int64		next;
	int64		rescnt = 0;
	int64		log;
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_pg_seq_local_data seq;
	Page		page;
	bool		logit = false;

	/* lock page buffer and read tuple */
	seq = read_seq_tuple(rel, &buf, &seqdatatuple);
	page = BufferGetPage(buf);

	*last = next = result = seq->last_value;
	fetch = cache;
	log = seq->log_cnt;

	if (!seq->is_called)
	{
		rescnt++;				/* return last_value if not is_called */
		fetch--;
	}

	/*
	 * Decide whether we should emit a WAL log record.  If so, force up the
	 * fetch count to grab SEQ_LOCAL_LOG_VALS more values than we actually
	 * need to cache.  (These will then be usable without logging.)
	 *
	 * If this is the first nextval after a checkpoint, we must force a new
	 * WAL record to be written anyway, else replay starting from the
	 * checkpoint would fail to advance the sequence past the logged values.
	 * In this case we may as well fetch extra values.
	 */
	if (log < fetch || !seq->is_called)
	{
		/* forced log to satisfy local demand for values */
		fetch = log = fetch + SEQ_LOCAL_LOG_VALS;
		logit = true;
	}
	else
	{
		XLogRecPtr	redoptr = GetRedoRecPtr();

		if (PageGetLSN(page) <= redoptr)
		{
			/* last update of seq was before checkpoint */
			fetch = log = fetch + SEQ_LOCAL_LOG_VALS;
			logit = true;
		}
	}

	while (fetch)				/* try to fetch cache [+ log ] numbers */
	{
		/*
		 * Check MAXVALUE for ascending sequences and MINVALUE for descending
		 * sequences
		 */
		if (incby > 0)
		{
			/* ascending sequence */
			if ((maxv >= 0 && next > maxv - incby) ||
				(maxv < 0 && next + incby > maxv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					ereport(ERROR,
							(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
							 errmsg("nextval: reached maximum value of sequence \"%s\" (%lld)",
									RelationGetRelationName(rel),
									(long long) maxv)));
				next = minv;
			}
			else
				next += incby;
		}
		else
		{
			/* descending sequence */
			if ((minv < 0 && next < minv - incby) ||
				(minv >= 0 && next + incby < minv))
			{
				if (rescnt > 0)
					break;		/* stop fetching */
				if (!cycle)
					ereport(ERROR,
							(errcode(ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED),
							 errmsg("nextval: reached minimum value of sequence \"%s\" (%lld)",
									RelationGetRelationName(rel),
									(long long) minv)));
				next = maxv;
			}
			else
				next += incby;
		}
		fetch--;
		if (rescnt < cache)
		{
			log--;
			rescnt++;
			*last = next;
			if (rescnt == 1)	/* if it's first result - */
				result = next;	/* it's what to return */
		}
	}

	log -= fetch;				/* adjust for any unfetched numbers */
	Assert(log >= 0);

	/*
	 * If something needs to be WAL logged, acquire an xid, so this
	 * transaction's commit will trigger a WAL flush and wait for syncrep.
	 * It's sufficient to ensure the toplevel transaction has an xid, no need
	 * to assign xids subxacts, that'll already trigger an appropriate wait.
	 * (Have to do that here, so we're outside the critical section)
	 */
	if (logit && RelationNeedsWAL(rel))
		GetTopTransactionId();

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();

	/*
	 * We must mark the buffer dirty before doing XLogInsert(); see notes in
	 * SyncOneBuffer().  However, we don't apply the desired changes just yet.
	 * This looks like a violation of the buffer update protocol, but it is in
	 * fact safe because we hold exclusive lock on the buffer.  Any other
	 * process, including a checkpoint, that tries to examine the buffer
	 * contents will block until we release the lock, and then will see the
	 * final state that we install below.
	 */
	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (logit && RelationNeedsWAL(rel))
	{
		xl_seq_local_rec xlrec;
		XLogRecPtr	recptr;

		/*
		 * We don't log the current state of the tuple, but rather the state
		 * as it would appear after "log" more fetches.  This lets us skip
		 * that many future WAL records, at the cost that we lose those
		 * sequence values if we crash.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		/* set values that will be saved in xlog */
		seq->last_value = next;
		seq->is_called = true;
		seq->log_cnt = 0;

		xlrec.locator = rel->rd_locator;

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_local_rec));
		XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

		recptr = XLogInsert(RM_SEQ_LOCAL_ID, XLOG_SEQ_LOCAL_LOG);

		PageSetLSN(page, recptr);
	}

	/* Now update sequence tuple to the intended final state */
	seq->last_value = *last;	/* last fetched number */
	seq->is_called = true;
	seq->log_cnt = log;			/* how much is logged */

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);

	return result;
}

/*
 * seq_local_get_table_am()
 *
 * Return the table access method used by this sequence.
 */
static const char *
seq_local_get_table_am(void)
{
	return "heap";
}

/*
 * seq_local_init()
 *
 * Add the sequence attributes to the relation created for this sequence
 * AM and insert a tuple of metadata into the sequence relation, based on
 * the information guessed from pg_sequences.  This is the first tuple
 * inserted after the relation has been created, filling in its heap
 * table.
 */
static void
seq_local_init(Relation rel, int64 last_value, bool is_called)
{
	Datum		value[SEQ_LOCAL_COL_LASTCOL];
	bool		null[SEQ_LOCAL_COL_LASTCOL];
	List	   *elts = NIL;
	List	   *atcmds = NIL;
	ListCell   *lc;
	TupleDesc	tupdesc;
	HeapTuple	tuple;

	/*
	 * Create relation (and fill value[] and null[] for the initial tuple).
	 */
	for (int i = SEQ_LOCAL_COL_FIRSTCOL; i <= SEQ_LOCAL_COL_LASTCOL; i++)
	{
		ColumnDef  *coldef = NULL;

		switch (i)
		{
			case SEQ_LOCAL_COL_LASTVAL:
				coldef = makeColumnDef("last_value", INT8OID, -1, InvalidOid);
				value[i - 1] = Int64GetDatumFast(last_value);
				break;
			case SEQ_LOCAL_COL_LOG:
				coldef = makeColumnDef("log_cnt", INT8OID, -1, InvalidOid);
				value[i - 1] = Int64GetDatum(0);
				break;
			case SEQ_LOCAL_COL_CALLED:
				coldef = makeColumnDef("is_called", BOOLOID, -1, InvalidOid);
				value[i - 1] = BoolGetDatum(is_called);
				break;
		}

		coldef->is_not_null = true;
		null[i - 1] = false;
		elts = lappend(elts, coldef);
	}

	/* Add all the attributes to the sequence */
	foreach(lc, elts)
	{
		AlterTableCmd *atcmd;

		atcmd = makeNode(AlterTableCmd);
		atcmd->subtype = AT_AddColumnToSequence;
		atcmd->def = (Node *) lfirst(lc);
		atcmds = lappend(atcmds, atcmd);
	}

	/*
	 * No recursion needed.  Note that EventTriggerAlterTableStart() should
	 * have been called.
	 */
	AlterTableInternal(RelationGetRelid(rel), atcmds, false);
	CommandCounterIncrement();

	tupdesc = RelationGetDescr(rel);
	tuple = heap_form_tuple(tupdesc, value, null);
	fill_seq_with_data(rel, tuple);
}

/*
 * seq_local_setval()
 *
 * Callback for setval().
 */
static void
seq_local_setval(Relation rel, int64 next, bool iscalled)
{
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_pg_seq_local_data seq;

	/* lock page buffer and read tuple */
	seq = read_seq_tuple(rel, &buf, &seqdatatuple);

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();
	seq->last_value = next;		/* last fetched number */
	seq->is_called = iscalled;
	seq->log_cnt = 0;

	MarkBufferDirty(buf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_seq_local_rec xlrec;
		XLogRecPtr	recptr;
		Page		page = BufferGetPage(buf);

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

		xlrec.locator = rel->rd_locator;
		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_local_rec));
		XLogRegisterData((char *) seqdatatuple.t_data, seqdatatuple.t_len);

		recptr = XLogInsert(RM_SEQ_LOCAL_ID, XLOG_SEQ_LOCAL_LOG);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * seq_local_reset()
 *
 * Perform a hard reset on the local sequence, rewriting its heap data
 * entirely.
 */
static void
seq_local_reset(Relation rel, int64 startv, bool is_called, bool reset_state)
{
	Form_pg_seq_local_data seq;
	Buffer		buf;
	HeapTupleData seqdatatuple;
	HeapTuple	tuple;

	/* lock buffer page and read tuple */
	(void) read_seq_tuple(rel, &buf, &seqdatatuple);

	/*
	 * Copy the existing sequence tuple.
	 */
	tuple = heap_copytuple(&seqdatatuple);

	/* Now we're done with the old page */
	UnlockReleaseBuffer(buf);

	/*
	 * Modify the copied tuple to execute the restart (compare the RESTART
	 * action in AlterSequence)
	 */
	seq = (Form_pg_seq_local_data) GETSTRUCT(tuple);
	seq->last_value = startv;
	seq->is_called = is_called;
	if (reset_state)
		seq->log_cnt = 0;

	/*
	 * Create a new storage file for the sequence.
	 */
	RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);

	/*
	 * Ensure sequence's relfrozenxid is at 0, since it won't contain any
	 * unfrozen XIDs.  Same with relminmxid, since a sequence will never
	 * contain multixacts.
	 */
	Assert(rel->rd_rel->relfrozenxid == InvalidTransactionId);
	Assert(rel->rd_rel->relminmxid == InvalidMultiXactId);

	/*
	 * Insert the modified tuple into the new storage file.
	 */
	fill_seq_with_data(rel, tuple);
}

/*
 * seq_local_get_state()
 *
 * Retrieve the state of a local sequence.
 */
static void
seq_local_get_state(Relation rel, int64 *last_value, bool *is_called)
{
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_pg_seq_local_data seq;

	/* lock page buffer and read tuple */
	seq = read_seq_tuple(rel, &buf, &seqdatatuple);

	*last_value = seq->last_value;
	*is_called = seq->is_called;

	UnlockReleaseBuffer(buf);
}

/*
 * seq_local_change_persistence()
 *
 * Persistence change for the local sequence Relation.
 */
static void
seq_local_change_persistence(Relation rel, char newrelpersistence)
{
	Buffer		buf;
	HeapTupleData seqdatatuple;

	(void) read_seq_tuple(rel, &buf, &seqdatatuple);
	RelationSetNewRelfilenumber(rel, newrelpersistence);
	fill_seq_with_data(rel, &seqdatatuple);
	UnlockReleaseBuffer(buf);
}

/* ------------------------------------------------------------------------
 * Definition of the local sequence access method.
 * ------------------------------------------------------------------------
 */
static const SequenceAmRoutine seq_local_methods = {
	.type = T_SequenceAmRoutine,
	.get_table_am = seq_local_get_table_am,
	.init = seq_local_init,
	.nextval = seq_local_nextval,
	.setval = seq_local_setval,
	.reset = seq_local_reset,
	.get_state = seq_local_get_state,
	.change_persistence = seq_local_change_persistence
};

Datum
seq_local_sequenceam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&seq_local_methods);
}
