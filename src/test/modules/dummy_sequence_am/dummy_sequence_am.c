/*-------------------------------------------------------------------------
 *
 * dummy_sequence_am.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/test/modules/dummy_sequence_am/dummy_sequence_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sequenceam.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

/* this sequence is fully on-memory */
static int	dummy_seqam_last_value = 1;
static bool dummy_seqam_is_called = false;

PG_FUNCTION_INFO_V1(dummy_sequenceam_handler);


/* ------------------------------------------------------------------------
 * Callbacks for the dummy sequence access method.
 * ------------------------------------------------------------------------
 */

/*
 * Return the table access method used by this sequence.
 *
 * This is just an on-memory sequence, so anything is fine.
 */
static const char *
dummy_sequenceam_get_table_am(void)
{
	return "heap";
}

static void
dummy_sequenceam_init(Relation rel, int64 last_value, bool is_called)
{
	dummy_seqam_last_value = last_value;
	dummy_seqam_is_called = is_called;
}

static int64
dummy_sequenceam_nextval(Relation rel, int64 incby, int64 maxv,
						 int64 minv, int64 cache, bool cycle,
						 int64 *last)
{
	dummy_seqam_last_value += incby;
	dummy_seqam_is_called = true;

	return dummy_seqam_last_value;
}

static void
dummy_sequenceam_setval(Relation rel, int64 next, bool iscalled)
{
	dummy_seqam_last_value = next;
	dummy_seqam_is_called = iscalled;
}

static void
dummy_sequenceam_get_state(Relation rel, int64 *last_value, bool *is_called)
{
	*last_value = dummy_seqam_last_value;
	*is_called = dummy_seqam_is_called;
}

static void
dummy_sequenceam_reset(Relation rel, int64 startv, bool is_called,
					   bool reset_state)
{
	dummy_seqam_last_value = startv;
	dummy_seqam_is_called = is_called;
}

static void
dummy_sequenceam_change_persistence(Relation rel, char newrelpersistence)
{
	/* nothing to do, really */
}

/* ------------------------------------------------------------------------
 * Definition of the dummy sequence access method.
 * ------------------------------------------------------------------------
 */

static const SequenceAmRoutine dummy_sequenceam_methods = {
	.type = T_SequenceAmRoutine,
	.get_table_am = dummy_sequenceam_get_table_am,
	.init = dummy_sequenceam_init,
	.nextval = dummy_sequenceam_nextval,
	.setval = dummy_sequenceam_setval,
	.get_state = dummy_sequenceam_get_state,
	.reset = dummy_sequenceam_reset,
	.change_persistence = dummy_sequenceam_change_persistence
};

Datum
dummy_sequenceam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&dummy_sequenceam_methods);
}
