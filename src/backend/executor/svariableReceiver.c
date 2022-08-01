/*-------------------------------------------------------------------------
 *
 * svariableReceiver.c
 *	  An implementation of DestReceiver that stores the result value in
 *	  a session variable.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/svariableReceiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/detoast.h"
#include "executor/svariableReceiver.h"
#include "commands/session_variable.h"

typedef struct
{
	DestReceiver pub;
	Oid			varid;
	Oid			typid;
	int32		typmod;
	int			typlen;
	int			slot_offset;
	int			rows;
}			svariableState;


/*
 * Prepare to receive tuples from executor.
 */
static void
svariableStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	svariableState *myState = (svariableState *) self;
	int			natts = typeinfo->natts;
	int			outcols = 0;
	int			i;

	/* Receiver should be initialized by SetVariableDestReceiverParams */
	Assert(OidIsValid(myState->varid));

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(typeinfo, i);

		if (attr->attisdropped)
			continue;

		if (++outcols > 1)
			elog(ERROR, "svariable DestReceiver can take only one attribute");

		myState->typid = attr->atttypid;
		myState->typmod = attr->atttypmod;
		myState->typlen = attr->attlen;
		myState->slot_offset = i;
	}

	if (outcols == 0)
		elog(ERROR, "svariable DestReceiver requires one attribute");

	myState->rows = 0;
}

/*
 * Receive a tuple from the executor and store it in session variable.
 */
static bool
svariableReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	svariableState *myState = (svariableState *) self;
	Datum		value;
	bool		isnull;
	bool		freeval = false;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	value = slot->tts_values[myState->slot_offset];
	isnull = slot->tts_isnull[myState->slot_offset];

	if (myState->typlen == -1 && !isnull && VARATT_IS_EXTERNAL(DatumGetPointer(value)))
	{
		value = PointerGetDatum(detoast_external_attr((struct varlena *)
													  DatumGetPointer(value)));
		freeval = true;
	}

	SetSessionVariable(myState->varid, value, isnull, myState->typid);

	if (freeval)
		pfree(DatumGetPointer(value));

	return true;
}

/*
 * Clean up at end of an executor run
 */
static void
svariableShutdownReceiver(DestReceiver *self)
{
	/* Nothing to do. */
}

/*
 * Destroy receiver when done with it
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
CreateVariableDestReceiver(void)
{
	svariableState *self = (svariableState *) palloc0(sizeof(svariableState));

	self->pub.receiveSlot = svariableReceiveSlot;
	self->pub.rStartup = svariableStartupReceiver;
	self->pub.rShutdown = svariableShutdownReceiver;
	self->pub.rDestroy = svariableDestroyReceiver;
	self->pub.mydest = DestVariable;

	/*
	 * Private fields will be set by SetVariableDestReceiverParams and
	 * svariableStartupReceiver.
	 */
	return (DestReceiver *) self;
}

/*
 * Set parameters for a VariableDestReceiver.
 * Should be called right after creating the DestReceiver.
 */
void
SetVariableDestReceiverParams(DestReceiver *self, Oid varid)
{
	svariableState *myState = (svariableState *) self;

	Assert(myState->pub.mydest == DestVariable);
	Assert(OidIsValid(varid));

	myState->varid = varid;
}
