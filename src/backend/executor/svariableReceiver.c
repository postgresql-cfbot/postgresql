/*-------------------------------------------------------------------------
 *
 * svariableReceiver.c
 *	  An implementation of DestReceiver that stores the result value in
 *	  a session variable.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/svariableReceiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "commands/session_variable.h"
#include "executor/svariableReceiver.h"

/*
 * This DestReceiver is used by the LET command for storing the result to a
 * session variable.  The result has to have only one tuple with only one
 * non-deleted attribute.  The row counter (field "rows") is incremented
 * after receiving a row, and an error is raised when there are no rows or
 * there are more than one received rows.  A received tuple cannot to have
 * deleted attributes.
 *
 * The assignment to session variable have to be postponed until we are
 * sure so only one row was received.
 */
typedef struct
{
	DestReceiver pub;
	char	   *varname;
	int			rows;			/* row counter */
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	MemoryContext tuple_cxt;	/* holds a value before storing to variable */
} SVariableState;

/*
 * Prepare to receive tuples from executor.
 */
static void
svariableStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	SVariableState *myState = (SVariableState *) self;

	Assert(myState->pub.mydest == DestVariable);
	Assert(typeinfo->natts == 1);

	myState->rows = 0;
	myState->tupdesc = typeinfo;
	myState->tuple = NULL;
	myState->tuple_cxt = CurrentMemoryContext;
}

/*
 * Receive a tuple from the executor and store it in the buffer
 */
static bool
svariableReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	SVariableState *myState = (SVariableState *) self;
	MemoryContext oldcxt;

	if (++myState->rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ROWS),
				 errmsg("expression returned more than one row")));

	/*
	 * We cannot to assign received value directly, so we should to
	 * save received value in the buffer.
	 */
	oldcxt = MemoryContextSwitchTo(myState->tuple_cxt);
	myState->tuple = ExecCopySlotHeapTuple(slot);
	MemoryContextSwitchTo(oldcxt);

	return true;
}

/*
 * Clean up at end of the executor run
 */
static void
svariableShutdownReceiver(DestReceiver *self)
{
	SVariableState *myState = (SVariableState *) self;
	Form_pg_attribute attr;
	Datum		value;
	bool		isnull;

	if (myState->rows == 0)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("expression returned no rows")));

	attr = TupleDescAttr(myState->tupdesc, 0);
	Assert(!attr->attisdropped);

	value = heap_getattr(myState->tuple, 1, myState->tupdesc, &isnull);

	SetSessionVariableWithTypecheck(myState->varname,
									attr->atttypid, attr->atttypmod,
									value, isnull);

	heap_freetuple(myState->tuple);
}

/*
 * Destroy the receiver when we are done with it
 */
static void
svariableDestroyReceiver(DestReceiver *self)
{
	pfree(self);
}

/*
 * Initially create a DestReceiver object.
 */
DestReceiver *
CreateVariableDestReceiver(char *varname)
{
	SVariableState *self = (SVariableState *) palloc0(sizeof(SVariableState));

	self->pub.receiveSlot = svariableReceiveSlot;
	self->pub.rStartup = svariableStartupReceiver;
	self->pub.rShutdown = svariableShutdownReceiver;
	self->pub.rDestroy = svariableDestroyReceiver;
	self->pub.mydest = DestVariable;

	self->varname = varname;

	return (DestReceiver *) self;
}
