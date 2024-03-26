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
#include "miscadmin.h"

#include "access/detoast.h"
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
#include "executor/svariableReceiver.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * This DestReceiver is used by the LET command for storing the result to
 * session variable.  The result has to have only one tuple with only one
 * not deleted attribute.  The row counter (field "rows") is incremented
 * after receiving of any row, and the error is raised when there are no rows
 * or there are more than one received rows.  Because received tuple can have
 * deleted attributes, we need to find wirst not deleted attribute
 * (field "slot_offset"). The value is detoasted before storing to session
 * variable.
 */
typedef struct
{
	DestReceiver pub;
	Oid			varid;
	bool		need_detoast;		/* we need detoast attr? */
	int			slot_offset;		/* position of not deleted attr */
	int			rows;				/* row counter */
} SVariableState;

/*
 * Prepare to receive tuples from executor.
 */
static void
svariableStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	SVariableState *myState = (SVariableState *) self;
	int			natts = typeinfo->natts;
	int			outcols = 0;
	int			i;
	LOCKTAG		locktag PG_USED_FOR_ASSERTS_ONLY;

	Assert(myState->pub.mydest == DestVariable);
	Assert(OidIsValid(myState->varid));
	Assert(SearchSysCacheExists1(VARIABLEOID, myState->varid));

#ifdef USE_ASSERT_CHECKING

	SET_LOCKTAG_OBJECT(locktag,
					   MyDatabaseId,
					   VariableRelationId,
					   myState->varid,
					   0);

	Assert(LockHeldByMe(&locktag, AccessShareLock));

#endif

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(typeinfo, i);
		Oid			typid;
		Oid			collid;
		int32		typmod;

		if (attr->attisdropped)
			continue;

		if (++outcols > 1)
			continue;

		get_session_variable_type_typmod_collid(myState->varid,
												&typid,
												&typmod,
												&collid);

		/*
		 * double check - the type and typmod of target variable should be
		 * same as type and typmod of assignment expression. It should be, the
		 * expression is wrapped by cast to target type and typmod.
		 */
		if (attr->atttypid != typid ||
			(attr->atttypmod >= 0 &&
			 attr->atttypmod != typmod))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("target session variable is of type %s"
							" but expression is of type %s",
							format_type_with_typemod(typid, typmod),
							format_type_with_typemod(attr->atttypid,
													 attr->atttypmod))));

		myState->need_detoast = attr->attlen == -1;
		myState->slot_offset = i;
	}

	if (outcols != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("assignment expression returned %d column",
							   "assignment expression returned %d columns",
							   outcols,
							   outcols)));

	myState->rows = 0;
}

/*
 * Receive a tuple from the executor and store it in session variable.
 */
static bool
svariableReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
	SVariableState *myState = (SVariableState *) self;
	Datum		value;
	bool		isnull;
	bool		freeval = false;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	value = slot->tts_values[myState->slot_offset];
	isnull = slot->tts_isnull[myState->slot_offset];

	if (myState->need_detoast && !isnull && VARATT_IS_EXTERNAL(DatumGetPointer(value)))
	{
		value = PointerGetDatum(detoast_external_attr((struct varlena *)
													  DatumGetPointer(value)));
		freeval = true;
	}

	myState->rows += 1;

	if (myState->rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ROWS),
				 errmsg("expression returned more than one row")));

	SetSessionVariable(myState->varid, value, isnull);

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
	if (((SVariableState *) self)->rows == 0)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("expression returned no rows")));
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
CreateVariableDestReceiver(Oid varid)
{
	SVariableState *self = (SVariableState *) palloc0(sizeof(SVariableState));

	self->pub.receiveSlot = svariableReceiveSlot;
	self->pub.rStartup = svariableStartupReceiver;
	self->pub.rShutdown = svariableShutdownReceiver;
	self->pub.rDestroy = svariableDestroyReceiver;
	self->pub.mydest = DestVariable;

	self->varid = varid;

	return (DestReceiver *) self;
}
