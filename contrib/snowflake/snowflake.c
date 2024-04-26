/*-------------------------------------------------------------------------
 *
 * snowflake.c
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/snowflake/snowflake.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/time.h>

#include "access/generic_xlog.h"
#include "access/sequenceam.h"
#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "commands/tablecmds.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;

/* "special area" of a snowflake's buffer page. */
#define SNOWFLAKE_MAGIC	0x01

typedef struct snowflake_magic
{
	uint32		magic;
} snowflake_magic;

/* -----------------------------------------------------------------------
 * Snowflake ID are 64-bit based, with the following structure:
 * - 41 bits for an epoch-based timestamp, in milli-seconds.
 * - 10 bits for a machine ID.
 * - 12 bits for a sequence counter.
 *
 * The timestamp can be cut to an offset.  The machine ID is controlled
 * by a superuser GUC.  Sequence properties apply to the sequence counter,
 * as the other two are environment-dependent.
 * -----------------------------------------------------------------------
 */

/*
 * Helper routines to convert a snowflake ID from/to an int64.
 */
#define SNOWFLAKE_COUNTER_MASK		0x0000000000000FFF	/* 12 bits */
#define SNOWFLAKE_COUNTER_SHIFT		0
#define SNOWFLAKE_MACHINE_ID_MASK	0x00000000000003FF	/* 10 bits */
#define SNOWFLAKE_MACHINE_ID_SHIFT	12	/* counter */
#define SNOWFLAKE_TIMESTAMP_MASK	0x000001FFFFFFFFFF	/* 41 bits */
#define SNOWFLAKE_TIMESTAMP_SHIFT	22	/* machine ID + counter sizes */

typedef struct snowflake_id
{
	uint32		count;
	uint32		machine;
	uint64		time_ms;
} snowflake_id;

#define snowflake_id_to_int64(id, raw) { \
	raw = (((id).count) & SNOWFLAKE_COUNTER_MASK) << SNOWFLAKE_COUNTER_SHIFT |	\
		(((id).machine) & SNOWFLAKE_MACHINE_ID_MASK) << SNOWFLAKE_MACHINE_ID_SHIFT | \
		(((id).time_ms) & SNOWFLAKE_TIMESTAMP_MASK) << SNOWFLAKE_TIMESTAMP_SHIFT;	\
}

#define int64_to_snowflake_id(raw, id) { \
	(id).count = ((raw) >> SNOWFLAKE_COUNTER_SHIFT) & SNOWFLAKE_COUNTER_MASK;		\
	(id).machine = ((raw) >> SNOWFLAKE_MACHINE_ID_SHIFT) & SNOWFLAKE_MACHINE_ID_MASK; \
	(id).time_ms = ((raw) >> SNOWFLAKE_TIMESTAMP_SHIFT) & SNOWFLAKE_TIMESTAMP_MASK;	\
}

/*
 * Format of tuples stored in heap table associated to snowflake sequence.
 */
typedef struct FormData_snowflake_data
{
	/* enough to cover 12 bits of the internal counter */
	int16	count;
	bool	is_called;
} FormData_snowflake_data;

typedef FormData_snowflake_data *Form_snowflake_data;

/*
 * Columns of a snowflake sequence relation.
 */
#define SNOWFLAKE_COL_COUNT		1
#define SNOWFLAKE_COL_CALLED	2

#define	SNOWFLAKE_COLS			2

/* GUC parameter */
static int snowflake_machine_id = 1;

PG_FUNCTION_INFO_V1(snowflake_sequenceam_handler);

/* -----------------------------------------------------------------------
 * Interfaces for relation manipulation.
 * -----------------------------------------------------------------------
 */

/*
 * Initialize snowflake relation's fork with some data.
 */
static void
fill_snowflake_fork(Relation rel, HeapTuple tuple, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	snowflake_magic *sm;
	OffsetNumber offnum;
	GenericXLogState *state = NULL;

	/* Initialize first page of relation with special magic number */

	buf = ExtendBufferedRel(BMR_REL(rel), forkNum, NULL,
							EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK);
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(snowflake_magic));
	sm = (snowflake_magic *) PageGetSpecialPointer(page);
	sm->magic = SNOWFLAKE_MAGIC;

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

	/*
	 * Initialize before entering in the critical section, as this does
	 * allocations.
	 */
	if (forkNum == INIT_FORKNUM)
		state = GenericXLogStart(rel);

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
						 InvalidOffsetNumber, false, false);
	if (offnum != FirstOffsetNumber)
		elog(ERROR, "failed to add sequence tuple to page");

	/*
	 * Init forks have to be logged.  These go through generic WAL records
	 * for simplicity's sake to save from the need of a custom RMGR.
	 */
	if (forkNum == INIT_FORKNUM)
	{
		(void) GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		GenericXLogFinish(state);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * Initialize snowflake relation.
 *
 * This needs to handle both the initial and main forks.
 */
static void
fill_snowflake(Relation rel, HeapTuple tuple)
{
	SMgrRelation srel;

	Assert(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED);

	fill_snowflake_fork(rel, tuple, MAIN_FORKNUM);

	/* init fork */
	srel = smgropen(rel->rd_locator, INVALID_PROC_NUMBER);
	smgrcreate(srel, INIT_FORKNUM, false);
	log_smgrcreate(&rel->rd_locator, INIT_FORKNUM);
	fill_snowflake_fork(rel, tuple, INIT_FORKNUM);
	FlushRelationBuffers(rel);
	smgrclose(srel);
}

/*
 * Read the current state of a snowflake sequence
 *
 * Given an opened sequence relation, lock the page buffer and find the tuple.
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer.
 * *seqdatatuple receives the reference to the sequence tuple proper.
 *
 * Returns value points to the data payload of the tuple.
 */
static Form_snowflake_data
read_snowflake(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
{
	Page		page;
	ItemId		lp;
	snowflake_magic *sm;
	Form_snowflake_data	seq;

	*buf = ReadBuffer(rel, 0);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buf);
	sm = (snowflake_magic *) PageGetSpecialPointer(page);

	if (sm->magic != SNOWFLAKE_MAGIC)
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

	seq = (Form_snowflake_data) GETSTRUCT(seqdatatuple);

	return seq;
}


/* ------------------------------------------------------------------------
 * Callbacks for the snowflake sequence access method.
 * ------------------------------------------------------------------------
 */

/*
 * Return the table access method used by this sequence.
 *
 * This is just an on-memory sequence, so anything is fine.
 */
static const char *
snowflake_sequenceam_get_table_am(void)
{
	return "heap";
}

/*
 * snowflake_sequenceam_init
 *
 * Initialize relation of a snowflake sequence.  This stores the sequence
 * counter in an unlogged relation as timestamps ensure value unicity.
 */
static void
snowflake_sequenceam_init(Relation rel, int64 last_value, bool is_called)
{
	Datum	values[SNOWFLAKE_COLS];
	bool	nulls[SNOWFLAKE_COLS];
	int16	counter;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	List	   *elts = NIL;
	ListCell   *lc;
	ColumnDef  *coldef = NULL;
	AlterTableCmd *atcmd;
	List		  *atcmds = NIL;

	/* Adjust last_value, depending on the defaults given */
	counter = ((int16) last_value) & SNOWFLAKE_COUNTER_MASK;

	/*
	 * Create unlogged relation with its attributes.
	 */
	coldef = makeColumnDef("count", INT2OID, -1, InvalidOid);
	coldef->is_not_null = true;
	elts = lappend(elts, coldef);
	coldef = makeColumnDef("is_called", BOOLOID, -1, InvalidOid);
	coldef->is_not_null = true;
	elts = lappend(elts, coldef);

	foreach(lc, elts)
	{
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

	/*
	 * Switch the relation to be unlogged.  This forces a rewrite, but
	 * the relation is empty so that's OK.
	 */
	RelationSetNewRelfilenumber(rel, RELPERSISTENCE_UNLOGGED);

	/* And insert its first tuple */
	values[0] = Int16GetDatum(counter);
	nulls[0] = false;
	values[1] = BoolGetDatum(is_called);
	nulls[1] = false;

	tupdesc = RelationGetDescr(rel);
	tuple = heap_form_tuple(tupdesc, values, nulls);
	fill_snowflake(rel, tuple);
}

/*
 * snowflake_sequenceam_nextval
 *
 * Return the next value for a snowflake sequence.
 */
static int64
snowflake_sequenceam_nextval(Relation rel, int64 incby, int64 maxv,
							 int64 minv, int64 cache, bool cycle,
							 int64 *last)
{
	Buffer		buf;
	Form_snowflake_data seq;
	HeapTupleData seqdatatuple;
	int64	result = 0;
	snowflake_id	id = {0};
	struct timeval tp;

	/* lock page buffer and read tuple */
	seq = read_snowflake(rel, &buf, &seqdatatuple);

	/*
	 * The logic here is quite simple, increment the counter until its
	 * threshold is reached and get back to the start.  If the threshold
	 * is reached, wait 1ms to ensure a unique timestamp.  There is no
	 * need to do a retry as the buffer is already locked.
	 */
	id.count = seq->count;
	id.count++;

	if (id.count > (PG_INT16_MAX & SNOWFLAKE_COUNTER_MASK))
	{
		/*
		 * Threshold reached, so wait a bit for force clock to a new
		 * timestamp.
		 */
		id.count = 1;
		pg_usleep(1000L);	/* 1ms */
	}

	/* Compute timestamp, with buffer locked */
	gettimeofday(&tp, NULL);
	id.time_ms = (uint64) tp.tv_sec * 1000 +
		tp.tv_usec / 1000;

	/* Machine ID */
	id.machine = snowflake_machine_id;

	/* ready to change the on-disk (or really, in-buffer) tuple */
	START_CRIT_SECTION();
	seq->count = id.count;
	seq->is_called = true;
	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);

	/* Store the last value computed for lastval() */
	snowflake_id_to_int64(id, result);
	*last = result;
	return result;
}

/*
 * snowflake_sequenceam_setval
 *
 * Set the sequence value, manipulating only the sequence counter.
 */
static void
snowflake_sequenceam_setval(Relation rel, int64 next, bool iscalled)
{
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_snowflake_data seq;

	/* lock page buffer and read tuple */
	seq = read_snowflake(rel, &buf, &seqdatatuple);

	/* Change the in-buffer tuple */
	START_CRIT_SECTION();
	seq->count = (next & SNOWFLAKE_COUNTER_MASK);
	seq->is_called = iscalled;
	MarkBufferDirty(buf);
	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * snowflake_sequenceam_get_state
 *
 * Return the last sequence counter value.
 */
static void
snowflake_sequenceam_get_state(Relation rel, int64 *last_value, bool *is_called)
{
	Buffer		buf;
	HeapTupleData seqdatatuple;
	Form_snowflake_data seq;

	seq = read_snowflake(rel, &buf, &seqdatatuple);
	*last_value = seq->count;
	*is_called = seq->is_called;
	UnlockReleaseBuffer(buf);
}

/*
 * snowflake_sequenceam_reset
 *
 * Reset the sequence, coming down to resetting its counter.
 */
static void
snowflake_sequenceam_reset(Relation rel, int64 startv, bool is_called,
						   bool reset_state)
{
	HeapTupleData seqdatatuple;
	HeapTuple	tuple;
	Form_snowflake_data seq;
	Buffer		buf;

	/* lock buffer page and read tuple */
	(void) read_snowflake(rel, &buf, &seqdatatuple);

	/* copy the existing tuple */
	tuple = heap_copytuple(&seqdatatuple);

	/* Now we're done with the old page */
	UnlockReleaseBuffer(buf);

	/*
	 * Modify the copied tuple to execute the restart (compare the RESTART
	 * action in AlterSequence)
	 */
	seq = (Form_snowflake_data) GETSTRUCT(tuple);
	seq->count = (startv & SNOWFLAKE_COUNTER_MASK);
	seq->is_called = is_called;

	/* create new storage */
	RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);

	/* insert the modified tuple into the page */
	fill_snowflake(rel, tuple);
}

/*
 * snowflake_sequenceam_change_persistence
 *
 * There is nothing to do here, the underneath relation has to remain
 * unlogged and is set as such when creating the sequence.
 */
static void
snowflake_sequenceam_change_persistence(Relation rel, char newrelpersistence)
{
	/* Nothing to do here */
}

/* ------------------------------------------------------------------------
 * Definition of the snowflake sequence access method.
 * ------------------------------------------------------------------------
 */

static const SequenceAmRoutine snowflake_sequenceam_methods = {
	.type = T_SequenceAmRoutine,
	.get_table_am = snowflake_sequenceam_get_table_am,
	.init = snowflake_sequenceam_init,
	.nextval = snowflake_sequenceam_nextval,
	.setval = snowflake_sequenceam_setval,
	.get_state = snowflake_sequenceam_get_state,
	.reset = snowflake_sequenceam_reset,
	.change_persistence = snowflake_sequenceam_change_persistence
};

Datum
snowflake_sequenceam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&snowflake_sequenceam_methods);
}

/* Utility functions */

/*
 * snowflake_get
 *
 * Return a tuple worth of snowflake ID data, in a readable shape.
 */
PG_FUNCTION_INFO_V1(snowflake_get);
Datum
snowflake_get(PG_FUNCTION_ARGS)
{
#define	SNOWFLAKE_GET_COLS	3
	int64	raw = PG_GETARG_INT64(0);
	Datum  *values;
	bool   *nulls;
	TupleDesc		tupdesc;
	snowflake_id	id;

	/* determine result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	int64_to_snowflake_id(raw, id);

	nulls = palloc0(sizeof(bool) * tupdesc->natts);
	values = palloc0(sizeof(Datum) * tupdesc->natts);

	values[0] = Int64GetDatum(id.time_ms);
	values[1] = Int32GetDatum(id.machine);
	values[2] = Int32GetDatum(id.count);

	/* Returns the record as Datum */
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Entry point when loading extension.
 */
void
_PG_init(void)
{
	DefineCustomIntVariable("snowflake.machine_id",
							"Machine ID to use with snowflake sequence.",
							"Default value is 1.",
							&snowflake_machine_id,
							1, 0, 1023, /* 10 bits as max */
							PGC_SUSET,
							0, NULL, NULL, NULL);
}
