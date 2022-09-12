/*-------------------------------------------------------------------------
 *
 * stopevent.c
 *	  Auxiliary infrastructure for automated testing of concurrency issues
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/stopevent.c
*/
#include "postgres.h"

#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "storage/condition_variable.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "storage/stopevent.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#define QUERY_BUFFER_SIZE 1024

typedef struct
{
	char		condition[QUERY_BUFFER_SIZE];
	bool		enabled;
	slock_t		lock;
	ConditionVariable cv;
} StopEvent;

bool		enable_stopevents = false;
bool		trace_stopevents = false;
StopEvent  *stopevents = NULL;
MemoryContext stopevents_cxt = NULL;

Size
StopEventShmemSize(void)
{
#ifdef USE_STOP_EVENTS
	Size		size;

	size = mul_size(NUM_BUILTIN_STOPEVENTS, sizeof(StopEvent));
	return size;
#else
	return 0;
#endif
}

void
StopEventShmemInit(void)
{
#ifndef USE_STOP_EVENTS
	Size		size = StopEventShmemSize();
	bool		found;

	stopevents = (StopEvent *) ShmemInitStruct("Stop events Data",
											   size,
											   &found);

	if (!found)
	{
		int			i;

		for (i = 0; i < NUM_BUILTIN_STOPEVENTS; i++)
		{
			SpinLockInit(&stopevents[i].lock);
			stopevents[i].enabled = false;
			ConditionVariableInit(&stopevents[i].cv);
		}
	}
#else
	return;
#endif
}

static StopEvent *
find_stop_event(text *name)
{
	int			i;
	char	   *name_data = VARDATA_ANY(name);
	int			len = VARSIZE_ANY_EXHDR(name);

	for (i = 0; i < NUM_BUILTIN_STOPEVENTS; i++)
	{
		if (strlen(stopeventnames[i]) == len &&
			memcmp(name_data, stopeventnames[i], len) == 0)
			return &stopevents[i];
	}

	elog(ERROR, "unknown stop event: \"%s\"", text_to_cstring(name));
	return NULL;
}

#ifdef USE_STOP_EVENTS
#define CHECK_FOR_STOPEVENTS()
#else
#define CHECK_FOR_STOPEVENTS() \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
				 errmsg("stop events are not supported"), \
				 errhint("Try to rebuild PostgreSQL with --enable-stop-events flag.")))
#endif

Datum
pg_stopevent_set(PG_FUNCTION_ARGS)
{
	text	   *event_name = PG_GETARG_TEXT_PP(0);
	JsonPath   *condition = PG_GETARG_JSONPATH_P(1);
	StopEvent  *event;

	CHECK_FOR_STOPEVENTS();

	event = find_stop_event(event_name);

	if (VARSIZE_ANY(condition) > QUERY_BUFFER_SIZE)
		elog(ERROR, "jsonpath condition is too long");

	SpinLockAcquire(&event->lock);
	event->enabled = true;
	memcpy(&event->condition, condition, VARSIZE_ANY(condition));
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_FREE_IF_COPY(condition, 1);
	PG_RETURN_VOID();
}

Datum
pg_stopevent_reset(PG_FUNCTION_ARGS)
{
	text	   *event_name = PG_GETARG_TEXT_PP(0);
	StopEvent  *event;

	CHECK_FOR_STOPEVENTS();

	event = find_stop_event(event_name);

	SpinLockAcquire(&event->lock);
	event->enabled = false;
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_RETURN_VOID();
}

Datum
pg_stopevents(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		randomAccess;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;
	AttrNumber	attnum;
	int			i;

	CHECK_FOR_STOPEVENTS();

	/* The tupdesc and tuplestore must be created in ecxt_per_query_memory */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(3);
	attnum = (AttrNumber) 1;
	TupleDescInitEntry(tupdesc, attnum, "stopevent", TEXTOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "condition", JSONPATHOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "waiters", INT4ARRAYOID, -1, 0);

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < NUM_BUILTIN_STOPEVENTS; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};
		StopEvent  *event = &stopevents[i];
		proclist_mutable_iter iter;
		List	   *waiters = NIL;
		Datum	   *elems;
		ListCell   *lc;
		int			j;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}
		values[0] = PointerGetDatum(cstring_to_text(stopeventnames[i]));
		values[1] = PointerGetDatum(&event->condition);

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

			waiters = lappend_int(waiters, waiter->pid);
		}
		SpinLockRelease(&event->cv.mutex);

		elems = (Datum *) palloc(sizeof(Datum) * list_length(waiters));
		j = 0;
		foreach(lc, waiters)
		{
			elems[j] = Int32GetDatum(lfirst_int(lc));
			j++;
		}
		values[2] = PointerGetDatum(construct_array(elems, list_length(waiters), INT4OID, 4, true, 'i'));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		SpinLockRelease(&event->lock);
	}
	PG_RETURN_VOID();
}

bool
pid_is_waiting_for_stopevent(int pid)
{
	int			i;

	for (i = 0; i < NUM_BUILTIN_STOPEVENTS; i++)
	{
		StopEvent  *event = &stopevents[i];
		proclist_mutable_iter iter;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

			if (waiter->pid == pid)
			{
				SpinLockRelease(&event->cv.mutex);
				SpinLockRelease(&event->lock);
				return true;
			}
		}
		SpinLockRelease(&event->cv.mutex);
		SpinLockRelease(&event->lock);
	}
	return false;
}

static bool
check_stopevent_condition(StopEvent *event, Jsonb *params)
{
	Datum		res;

	SpinLockAcquire(&event->lock);
	if (!event->enabled)
	{
		SpinLockRelease(&event->lock);
		return false;
	}

	res = DirectFunctionCall2(jsonb_path_match,
							  PointerGetDatum(params),
							  PointerGetDatum(&event->condition));

	SpinLockRelease(&event->lock);

	return DatumGetBool(res);
}

void
handle_stopevent(int event_id, Jsonb *params)
{
	StopEvent  *event = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < NUM_BUILTIN_STOPEVENTS);

	if (event->enabled && check_stopevent_condition(event, params))
	{
		ConditionVariablePrepareToSleep(&event->cv);
		for (;;)
		{
			if (!check_stopevent_condition(event, params))
				break;
			ConditionVariableSleep(&event->cv, WAIT_EVENT_STOPEVENT);
		}
		ConditionVariableCancelSleep();
	}

	if (trace_stopevents)
	{
		char	   *params_string;

		params_string = DatumGetCString(DirectFunctionCall1(jsonb_out, PointerGetDatum(params)));
		elog(DEBUG2, "stop event \"%s\", params \"%s\"",
			 stopeventnames[event_id],
			 params_string);
		pfree(params_string);
	}

	MemoryContextReset(stopevents_cxt);
}

void
stopevents_make_cxt(void)
{
	if (!stopevents_cxt)
		stopevents_cxt = AllocSetContextCreate(TopMemoryContext,
											   "CacheMemoryContext",
											   ALLOCSET_DEFAULT_SIZES);
}

void
jsonb_push_key(JsonbParseState **state, char *key)
{
	JsonbValue	jval;

	jval.type = jbvString;
	jval.val.string.len = strlen(key);
	jval.val.string.val = key;
	(void) pushJsonbValue(state, WJB_KEY, &jval);
}

void
jsonb_push_int8_key(JsonbParseState **state, char *key, int64 value)
{
	JsonbValue	jval;

	jsonb_push_key(state, key);

	jval.type = jbvNumeric;
	jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(value)));
	(void) pushJsonbValue(state, WJB_VALUE, &jval);

}

void
jsonb_push_string_key(JsonbParseState **state, const char *key,
					  const char *value)
{
	JsonbValue	jval;

	jsonb_push_key(state, (char *) key);

	jval.type = jbvString;
	jval.val.string.len = strlen(value);
	jval.val.string.val = (char *) value;
	(void) pushJsonbValue(state, WJB_VALUE, &jval);
}

void
relation_stopevent_params(JsonbParseState **state, Relation relation)
{
	jsonb_push_int8_key(state, "datoid", MyDatabaseId);
	jsonb_push_string_key(state, "datname", get_database_name(MyDatabaseId));
	jsonb_push_int8_key(state, "reloid", relation->rd_id);
	jsonb_push_string_key(state, "relname", NameStr(relation->rd_rel->relname));
}
