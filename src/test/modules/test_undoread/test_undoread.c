/*--------------------------------------------------------------------------
 *
 * test_undoread.c
 *		Test code for src/backend/access/undo/undoread.c
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_undoread/test_undoread.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/session.h"
#include "access/undolog.h"
#include "access/undopage.h"
#include "access/undoread.h"
#include "access/undorecordset.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "catalog/pg_class.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

static UndoRecordSet *current_urs = NULL;

PG_MODULE_MAGIC;

static void
check_debug_build(void)
{
#ifndef UNDO_DEBUG
	elog(ERROR, "UNDO_DEBUG must be defined at build time");
#endif
}

static void
xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PREPARE)
		current_urs = NULL;
}

static bool xact_callbacks_registered = false;

PG_FUNCTION_INFO_V1(test_undoread_create);
Datum
test_undoread_create(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;
	XactUndoRecordSetHeader hdr;

	check_debug_build();

	if (!xact_callbacks_registered)
	{
		RegisterXactCallback(xact_callback, NULL);
		xact_callbacks_registered = true;
	}

	if (current_urs != NULL)
		elog(ERROR, "an UndoRecordSet is already active");

	hdr.fxid = GetTopFullTransactionId();

	old_context = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Relation persistence does not matter, we're only trying to generate any
	 * kind UNDO log.
	 */
	current_urs = UndoCreate(URST_TRANSACTION,
							 RELPERSISTENCE_PERMANENT,
							 GetCurrentTransactionNestLevel(),
							 SizeOfXactUndoRecordSetHeader,
							 (char *) &hdr);
	MemoryContextSwitchTo(old_context);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_undoread_close);
Datum
test_undoread_close(PG_FUNCTION_ARGS)
{
	check_debug_build();

	if (current_urs == NULL)
		elog(ERROR, "no active UndoRecordSet");

	if (UndoPrepareToMarkClosed(current_urs))
	{
		START_CRIT_SECTION();
		UndoMarkClosed(current_urs);
		END_CRIT_SECTION();
	}

	UndoDestroy(current_urs);

	current_urs = NULL;

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_undoread_insert);
Datum
test_undoread_insert(PG_FUNCTION_ARGS)
{
	char	   *string = text_to_cstring(PG_GETARG_TEXT_PP(0));
	UndoRecData rdata;
	XactUndoContext undo_context;
	UndoRecPtr	urp;

	check_debug_build();

	if (current_urs == NULL)
		elog(ERROR, "no active UndoRecordSet");

	rdata.len = strlen(string) + 1;
	rdata.data = string;
	rdata.next = NULL;

	/*
	 * Neither relation persistence nor record type nor rmid is important
	 * here.
	 */
	urp = PrepareXactUndoData(&undo_context, RELPERSISTENCE_PERMANENT,
							  GetUndoDataSize(&rdata));
	SerializeUndoData(&undo_context.data, 0, 0, &rdata);

	/* Since we do not modify relation pages, no WAL needs to be written. */

	START_CRIT_SECTION();
	InsertXactUndoData(&undo_context, 0);
	END_CRIT_SECTION();

	CleanupXactUndoInsertion(&undo_context);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(UndoRecPtrFormat, urp)));
}

/* Parse the textual value of UNDO ponter. */
static UndoRecPtr
urp_from_string(char *str)
{
	uint32		high,
				low;
	UndoRecPtr	result;

	if (sscanf(str, "%08X%08X", &high, &low) != 2)
		elog(ERROR, "could not recognize UNDO pointer value \"%s\"", str);

	result = (((UndoRecPtr) high) << 32) + low;

	return result;
}

/* Read records in the given range and return them as a set. */
PG_FUNCTION_INFO_V1(test_undoread_read);
Datum
test_undoread_read(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	UndoRSReaderState *state;
	bool		backward = PG_GETARG_BOOL(2);
	bool		have_next;

	check_debug_build();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		UndoRecPtr	start,
					end;

		start = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(0)));
		end = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(1)));

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		state = (UndoRSReaderState *) palloc(sizeof(UndoRSReaderState));
		UndoRSReaderInit(state, start, end,
						 RELPERSISTENCE_PERMANENT,
						 GetCurrentTransactionNestLevel() == 1);
		MemoryContextSwitchTo(oldcontext);
		funcctx->user_fctx = (void *) state;
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (UndoRSReaderState *) funcctx->user_fctx;

	if (!backward)
		have_next = UndoRSReaderReadOneForward(state, false);
	else
		have_next = UndoRSReaderReadOneBackward(state);

	if (have_next)
	{
		char	   *res_str;
		Datum		result;

		res_str = pstrdup(state->node.n.data);
		result = PointerGetDatum(cstring_to_text(res_str));
		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		UndoRSReaderClose(state);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * Arrange things so that next time this backend acquires a new slot, it's one
 * to which no data has been written yet. It's useful to make the reader tests
 * reproducible.
 *
 * Note that no other backend should be trying to acquire UNDO slot
 * concurrently, since the cached slot is not locked. This is ok for tests.
 */
PG_FUNCTION_INFO_V1(test_undoread_cache_empty_log);
Datum
test_undoread_cache_empty_log(PG_FUNCTION_ARGS)
{
	UndoLogSlot *slot;
	bool		accept = false;
	List	   *slots = NIL;
	char		persistence = RELPERSISTENCE_PERMANENT;
	UndoPersistenceLevel plevel = GetUndoPersistenceLevel(persistence);
	ListCell   *lc;

	/* Should not happen. */
	if (!CurrentSession)
		elog(ERROR, "backend session is not initialized");

	check_debug_build();

	slot = CurrentSession->private_undolog_free_lists[plevel];
	/* Is the suitable slot already cached in the session? */
	if (slot)
	{
		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		/* See UndoLogAcquire() for comments on PID. */
		if (slot->pid == MyProcPid)
		{
			Assert(slot->state == UNDOLOGSLOT_ON_PRIVATE_FREE_LIST);
			/* Is this slot a fresh one? */
			if (slot->meta.insert == SizeOfUndoPageHeaderData)
				accept = true;
		}
		LWLockRelease(&slot->meta_lock);

		if (accept)
			PG_RETURN_VOID();
	}

	while (true)
	{
		/* By acquiring the slot we ensure that the session cache is empty. */
		slot = UndoLogAcquire(persistence, InvalidUndoLogNumber);
		/* The slot is already ours, so no need to lock meta_lock. */
		if (slot->meta.insert == SizeOfUndoPageHeaderData)
		{
			/*
			 * By releasing this slot first we ensure that it gets added to
			 * the session cache.
			 */
			UndoLogRelease(slot);
			break;
		}

		/*
		 * Keep the slot acquired so that the next call of UndoLogAcquire()
		 * returns another one.
		 */
		slots = lappend(slots, slot);
	}

	/* Release the slots that did not match our requirements. */
	foreach(lc, slots)
	{
		slot = (UndoLogSlot *) lfirst(lc);
		UndoLogRelease(slot);
	}

	list_free(slots);

	PG_RETURN_VOID();
}
