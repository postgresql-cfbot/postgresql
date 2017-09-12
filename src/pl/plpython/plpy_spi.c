/*
 * interface to SPI functions
 *
 * src/pl/plpython/plpy_spi.c
 */

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "parser/parse_type.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#include "plpython.h"

#include "plpy_spi.h"

#include "plpy_elog.h"
#include "plpy_main.h"
#include "plpy_planobject.h"
#include "plpy_plpymodule.h"
#include "plpy_procedure.h"
#include "plpy_resultobject.h"

typedef struct
{
	DestReceiver pub;
	PLyExecutionContext *exec_ctx;
	MemoryContext parent_ctx;
	MemoryContext cb_ctx;
	TupleDesc	desc;
	PLyTypeInfo *args;

	PyObject	*result;
} CallbackState;

static void PLy_CSStartup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void PLy_CSDestroy(DestReceiver *self);
static bool PLy_CSreceive(TupleTableSlot *slot, DestReceiver *self);
static CallbackState *PLy_Callback_New(PLyExecutionContext *exec_ctx);
static CallbackState *PLy_Callback_Free(CallbackState *callback);
static PLyResultObject *PLyCSNewResult(CallbackState *myState);

static PyObject *PLy_spi_execute_query(char *query, long limit);
static PyObject *PLy_spi_execute_fetch_result(CallbackState *callback,
							 uint64 rows, int status);
static void PLy_spi_exception_set(PyObject *excclass, ErrorData *edata);


/* prepare(query="select * from foo")
 * prepare(query="select * from foo where bar = $1", params=["text"])
 * prepare(query="select * from foo where bar = $1", params=["text"], limit=5)
 */
PyObject *
PLy_spi_prepare(PyObject *self, PyObject *args)
{
	PLyPlanObject *plan;
	PyObject   *list = NULL;
	PyObject   *volatile optr = NULL;
	char	   *query;
	volatile MemoryContext oldcontext;
	volatile ResourceOwner oldowner;
	volatile int nargs;

	if (!PyArg_ParseTuple(args, "s|O:prepare", &query, &list))
		return NULL;

	if (list && (!PySequence_Check(list)))
	{
		PLy_exception_set(PyExc_TypeError,
						  "second argument of plpy.prepare must be a sequence");
		return NULL;
	}

	if ((plan = (PLyPlanObject *) PLy_plan_new()) == NULL)
		return NULL;

	plan->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "PL/Python plan context",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(plan->mcxt);

	nargs = list ? PySequence_Length(list) : 0;

	plan->nargs = nargs;
	plan->types = nargs ? palloc(sizeof(Oid) * nargs) : NULL;
	plan->values = nargs ? palloc(sizeof(Datum) * nargs) : NULL;
	plan->args = nargs ? palloc(sizeof(PLyTypeInfo) * nargs) : NULL;

	MemoryContextSwitchTo(oldcontext);

	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	PLy_spi_subtransaction_begin(oldcontext, oldowner);

	PG_TRY();
	{
		int			i;
		PLyExecutionContext *exec_ctx = PLy_current_execution_context();

		/*
		 * the other loop might throw an exception, if PLyTypeInfo member
		 * isn't properly initialized the Py_DECREF(plan) will go boom
		 */
		for (i = 0; i < nargs; i++)
		{
			PLy_typeinfo_init(&plan->args[i], plan->mcxt);
			plan->values[i] = PointerGetDatum(NULL);
		}

		for (i = 0; i < nargs; i++)
		{
			char	   *sptr;
			HeapTuple	typeTup;
			Oid			typeId;
			int32		typmod;

			optr = PySequence_GetItem(list, i);
			if (PyString_Check(optr))
				sptr = PyString_AsString(optr);
			else if (PyUnicode_Check(optr))
				sptr = PLyUnicode_AsString(optr);
			else
			{
				ereport(ERROR,
						(errmsg("plpy.prepare: type name at ordinal position %d is not a string", i)));
				sptr = NULL;	/* keep compiler quiet */
			}

			/********************************************************
			 * Resolve argument type names and then look them up by
			 * oid in the system cache, and remember the required
			 *information for input conversion.
			 ********************************************************/

			parseTypeString(sptr, &typeId, &typmod, false);

			typeTup = SearchSysCache1(TYPEOID,
									  ObjectIdGetDatum(typeId));
			if (!HeapTupleIsValid(typeTup))
				elog(ERROR, "cache lookup failed for type %u", typeId);

			Py_DECREF(optr);

			/*
			 * set optr to NULL, so we won't try to unref it again in case of
			 * an error
			 */
			optr = NULL;

			plan->types[i] = typeId;
			PLy_output_datum_func(&plan->args[i], typeTup, exec_ctx->curr_proc->langid, exec_ctx->curr_proc->trftypes);
			ReleaseSysCache(typeTup);
		}

		pg_verifymbstr(query, strlen(query), false);
		plan->plan = SPI_prepare(query, plan->nargs, plan->types);
		if (plan->plan == NULL)
			elog(ERROR, "SPI_prepare failed: %s",
				 SPI_result_code_string(SPI_result));

		/* transfer plan from procCxt to topCxt */
		if (SPI_keepplan(plan->plan))
			elog(ERROR, "SPI_keepplan failed");

		PLy_spi_subtransaction_commit(oldcontext, oldowner);
	}
	PG_CATCH();
	{
		Py_DECREF(plan);
		Py_XDECREF(optr);

		PLy_spi_subtransaction_abort(oldcontext, oldowner);
		return NULL;
	}
	PG_END_TRY();

	Assert(plan->plan != NULL);
	return (PyObject *) plan;
}

/* execute(query="select * from foo", limit=5)
 * execute(plan=plan, values=(foo, bar), limit=5)
 */
PyObject *
PLy_spi_execute(PyObject *self, PyObject *args)
{
	char	   *query;
	PyObject   *plan;
	PyObject   *list = NULL;
	long		limit = 0;

	if (PyArg_ParseTuple(args, "s|l", &query, &limit))
		return PLy_spi_execute_query(query, limit);

	PyErr_Clear();

	if (PyArg_ParseTuple(args, "O|Ol", &plan, &list, &limit) &&
		is_PLyPlanObject(plan))
		return PLy_spi_execute_plan(plan, list, limit);

	PLy_exception_set(PLy_exc_error, "plpy.execute expected a query or a plan");
	return NULL;
}

PyObject *
PLy_spi_execute_plan(PyObject *ob, PyObject *list, long limit)
{
	PLyExecutionContext *exec_ctx = PLy_current_execution_context();
	CallbackState	*callback;
	volatile int nargs;
	int			i,
				rv;
	PLyPlanObject *plan;
	volatile MemoryContext oldcontext;
	volatile ResourceOwner oldowner;
	PyObject   *ret;

	if (list != NULL)
	{
		if (!PySequence_Check(list) || PyString_Check(list) || PyUnicode_Check(list))
		{
			PLy_exception_set(PyExc_TypeError, "plpy.execute takes a sequence as its second argument");
			return NULL;
		}
		nargs = PySequence_Length(list);
	}
	else
		nargs = 0;

	plan = (PLyPlanObject *) ob;

	if (nargs != plan->nargs)
	{
		char	   *sv;
		PyObject   *so = PyObject_Str(list);

		if (!so)
			PLy_elog(ERROR, "could not execute plan");
		sv = PyString_AsString(so);
		PLy_exception_set_plural(PyExc_TypeError,
								 "Expected sequence of %d argument, got %d: %s",
								 "Expected sequence of %d arguments, got %d: %s",
								 plan->nargs,
								 plan->nargs, nargs, sv);
		Py_DECREF(so);

		return NULL;
	}

	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;
	callback = PLy_Callback_New(exec_ctx);

	PLy_spi_subtransaction_begin(oldcontext, oldowner);

	PG_TRY();
	{
		char	   *volatile nulls;
		volatile int j;

		if (nargs > 0)
			nulls = palloc(nargs * sizeof(char));
		else
			nulls = NULL;

		for (j = 0; j < nargs; j++)
		{
			PyObject   *elem;

			elem = PySequence_GetItem(list, j);
			if (elem != Py_None)
			{
				PG_TRY();
				{
					plan->values[j] =
						plan->args[j].out.d.func(&(plan->args[j].out.d),
												 -1,
												 elem,
												 false);
				}
				PG_CATCH();
				{
					Py_DECREF(elem);
					PG_RE_THROW();
				}
				PG_END_TRY();

				Py_DECREF(elem);
				nulls[j] = ' ';
			}
			else
			{
				Py_DECREF(elem);
				plan->values[j] =
					InputFunctionCall(&(plan->args[j].out.d.typfunc),
									  NULL,
									  plan->args[j].out.d.typioparam,
									  -1);
				nulls[j] = 'n';
			}
		}

		rv = SPI_execute_plan_callback(plan->plan, plan->values, nulls,
							exec_ctx->curr_proc->fn_readonly, limit,
							(DestReceiver *) callback);
		ret = PLy_spi_execute_fetch_result(callback, SPI_processed, rv);

		if (nargs > 0)
			pfree(nulls);

		PLy_spi_subtransaction_commit(oldcontext, oldowner);
	}
	PG_CATCH();
	{
		int			k;

		/*
		 * cleanup plan->values array
		 */
		for (k = 0; k < nargs; k++)
		{
			if (!plan->args[k].out.d.typbyval &&
				(plan->values[k] != PointerGetDatum(NULL)))
			{
				pfree(DatumGetPointer(plan->values[k]));
				plan->values[k] = PointerGetDatum(NULL);
			}
		}

		PLy_spi_subtransaction_abort(oldcontext, oldowner);
		PLy_Callback_Free(callback);
		return NULL;
	}
	PG_END_TRY();
	callback = PLy_Callback_Free(callback);

	for (i = 0; i < nargs; i++)
	{
		if (!plan->args[i].out.d.typbyval &&
			(plan->values[i] != PointerGetDatum(NULL)))
		{
			pfree(DatumGetPointer(plan->values[i]));
			plan->values[i] = PointerGetDatum(NULL);
		}
	}

	if (rv < 0)
	{
		PLy_exception_set(PLy_exc_spi_error,
						  "SPI_execute_plan failed: %s",
						  SPI_result_code_string(rv));
		return NULL;
	}

	return ret;
}

static PyObject *
PLy_spi_execute_query(char *query, long limit)
{
	PLyExecutionContext *exec_ctx = PLy_current_execution_context();
	CallbackState	*callback = PLy_Callback_New(exec_ctx);
	volatile MemoryContext oldcontext;
	volatile ResourceOwner oldowner;
	int			rv;
	PyObject   *ret = NULL;

	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	PLy_spi_subtransaction_begin(oldcontext, oldowner);

	PG_TRY();
	{
		pg_verifymbstr(query, strlen(query), false);
		rv = SPI_execute_callback(query, exec_ctx->curr_proc->fn_readonly, limit,
				(DestReceiver *) callback);

		ret = PLy_spi_execute_fetch_result(callback, SPI_processed, rv);

		PLy_spi_subtransaction_commit(oldcontext, oldowner);
	}
	PG_CATCH();
	{
		PLy_spi_subtransaction_abort(oldcontext, oldowner);
		PLy_Callback_Free(callback);
		return NULL;
	}
	PG_END_TRY();
	callback = PLy_Callback_Free(callback);

	if (rv < 0)
	{
		Py_XDECREF(ret);
		PLy_exception_set(PLy_exc_spi_error,
						  "SPI_execute failed: %s",
						  SPI_result_code_string(rv));
		return NULL;
	}

	return ret;
}

static CallbackState *
PLy_Callback_New(PLyExecutionContext *exec_ctx)
{
	MemoryContext oldcontext, cb_ctx;
	CallbackState *callback;

	/* XXX does this really need palloc0? */
	callback = palloc0(sizeof(CallbackState));

	/*
	 * Use a new context to make cleanup easier. Allocate it in the current
	 * context so we don't have to worry about cleaning it up if there's an
	 * error.
	 */
	cb_ctx = AllocSetContextCreate(CurrentMemoryContext,
								"PL/Python callback context",
								ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cb_ctx);
	callback->parent_ctx = oldcontext;
	callback->cb_ctx = cb_ctx;
	memcpy(&(callback->pub), CreateDestReceiver(DestSPICallback), sizeof(DestReceiver));
	callback->pub.receiveSlot = PLy_CSreceive;
	callback->pub.rStartup = PLy_CSStartup;
	callback->pub.rDestroy = PLy_CSDestroy;
	callback->exec_ctx = exec_ctx;

	MemoryContextSwitchTo(oldcontext);

	return callback;
}

static CallbackState *
PLy_Callback_Free(CallbackState *callback)
{
	if (callback)
	{
		if (callback->cb_ctx)
			(callback->pub.rDestroy) ((DestReceiver *) callback);

		pfree(callback);
	}

	return (CallbackState *) NULL;
}

static PLyResultObject *
PLyCSNewResult(CallbackState *myState)
{
	MemoryContext oldctx;

	/* The result info needs to be in the parent context */
	oldctx = MemoryContextSwitchTo(myState->parent_ctx);
	myState->result = PLy_result_new();
	if (myState->result == NULL)
		PLy_elog(ERROR, "could not create new result object");

	MemoryContextSwitchTo(oldctx);
	return (PLyResultObject *) myState->result;
}

void
PLy_CSStartup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	PLyExecutionContext *old_exec_ctx;
	CallbackState *myState = (CallbackState *) self;
	PLyResultObject *result;
	PLyTypeInfo *args;
	MemoryContext mctx, old_mctx;

	/*
	 * We may be in a different execution context when we're called, so switch
	 * back to our original one.
	 */
	mctx = myState->cb_ctx;
	old_exec_ctx = PLy_switch_execution_context(myState->exec_ctx);
	old_mctx = MemoryContextSwitchTo(mctx);

	/*
	 * We need to store this because the TupleDesc that the receive function
	 * gets has no names.
	 */
	myState->desc = typeinfo;

	/* Setup type conversion info */
	myState->args = args = palloc0(sizeof(PLyTypeInfo));
	PLy_typeinfo_init(args, mctx);
	PLy_input_tuple_funcs(args, typeinfo);

	/* result is actually myState.result */
	result = PLyCSNewResult(myState);

	/*
	 * Save tuple descriptor for later use by result set metadata
	 * functions.  Save it in TopMemoryContext so that it survives outside of
	 * an SPI context.  We trust that PLy_result_dealloc() will clean it up
	 * when the time is right. The difference between result and everything
	 * else is that result needs to survive after the portal is destroyed,
	 * because result is what's handed back to the plpython function. While
	 * it's tempting to use something other than TopMemoryContext, that won't
	 * work: the user could potentially put result into the global dictionary,
	 * which means it could survive as long as the session does.  This might
	 * result in a leak if an error happens and the result doesn't get
	 * dereferenced, but if that happens it means the python GC has failed us,
	 * at which point we probably have bigger problems.
	 *
	 * This still isn't perfect though; if something the result tupledesc
	 * references has it's OID changed then the tupledesc will be invalid. I'm
	 * not sure it's worth worrying about that though.
	 */
	MemoryContextSwitchTo(TopMemoryContext);
	result->tupdesc = CreateTupleDescCopy(typeinfo);

	MemoryContextSwitchTo(old_mctx);
	PLy_switch_execution_context(old_exec_ctx);
}

void
PLy_CSDestroy(DestReceiver *self)
{
	CallbackState *myState = (CallbackState *) self;
	MemoryContext cb_ctx = myState->cb_ctx;

	MemoryContextDelete(cb_ctx);
	myState->cb_ctx = 0;
}

static bool
PLy_CSreceive(TupleTableSlot *slot, DestReceiver *self)
{
	CallbackState 	*myState = (CallbackState *) self;
	TupleDesc		desc = myState->desc;
	PLyTypeInfo 	*args = myState->args;
	PLyResultObject *result = (PLyResultObject *) myState->result;
	PLyExecutionContext *old_exec_ctx = PLy_switch_execution_context(myState->exec_ctx);
	MemoryContext 	scratch_context = PLy_get_scratch_context(myState->exec_ctx);
	MemoryContext 	oldcontext = CurrentMemoryContext;
	int			rv = 1;
	PyObject   *row;

	/* Verify saved state matches incoming slot */
	Assert(desc->tdtypeid == slot->tts_tupleDescriptor->tdtypeid);
	Assert(args->in.r.natts == slot->tts_tupleDescriptor->natts);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/*
	 * Do the work in the scratch context to avoid leaking memory from the
	 * datatype output function calls.
	 */
	MemoryContextSwitchTo(scratch_context);

	PG_TRY();
	{
		row = PLyDict_FromTuple(args, ExecFetchSlotTuple(slot), desc);
	}
	PG_CATCH();
	{
		Py_XDECREF(row);
		MemoryContextSwitchTo(oldcontext);
		PLy_switch_execution_context(old_exec_ctx);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * If we tried to do this in the PG_CATCH we'd have to mark row
	 * as volatile, but that won't work with PyList_Append, so just
	 * test the error code after doing Py_DECREF().
	 */
	if (row)
	{
		rv = PyList_Append(result->rows, row);
		Py_DECREF(row);
	}

	if (rv != 0)
		ereport(ERROR,
				(errmsg("unable to append value to list")));

	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(scratch_context);
	PLy_switch_execution_context(old_exec_ctx);

	return true;
}


static PyObject *
PLy_spi_execute_fetch_result(CallbackState *callback, uint64 rows, int status)
{
	PLyResultObject *result = (PLyResultObject *) callback->result;

	/* If status < 0 this stuff would just get thrown away anyway. */
	if (status > 0)
	{
		if (!result)
		{
			/*
			 * This happens if the command returned no results. Create a dummy result set.
			 */
			result = PLyCSNewResult(callback);
			callback->result = (PyObject *) result;
		}

		Py_DECREF(result->status);
		result->status = PyInt_FromLong(status);
		Py_DECREF(result->nrows);
		result->nrows = (rows > (uint64) LONG_MAX) ?
			PyFloat_FromDouble((double) rows) :
			PyInt_FromLong((long) rows);
	}

	return (PyObject *) result;
}

/*
 * Utilities for running SPI functions in subtransactions.
 *
 * Usage:
 *
 *	MemoryContext oldcontext = CurrentMemoryContext;
 *	ResourceOwner oldowner = CurrentResourceOwner;
 *
 *	PLy_spi_subtransaction_begin(oldcontext, oldowner);
 *	PG_TRY();
 *	{
 *		<call SPI functions>
 *		PLy_spi_subtransaction_commit(oldcontext, oldowner);
 *	}
 *	PG_CATCH();
 *	{
 *		<do cleanup>
 *		PLy_spi_subtransaction_abort(oldcontext, oldowner);
 *		return NULL;
 *	}
 *	PG_END_TRY();
 *
 * These utilities take care of restoring connection to the SPI manager and
 * setting a Python exception in case of an abort.
 */
void
PLy_spi_subtransaction_begin(MemoryContext oldcontext, ResourceOwner oldowner)
{
	BeginInternalSubTransaction(NULL);
	/* Want to run inside function's memory context */
	MemoryContextSwitchTo(oldcontext);
}

void
PLy_spi_subtransaction_commit(MemoryContext oldcontext, ResourceOwner oldowner)
{
	/* Commit the inner transaction, return to outer xact context */
	ReleaseCurrentSubTransaction();
	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;
}

void
PLy_spi_subtransaction_abort(MemoryContext oldcontext, ResourceOwner oldowner)
{
	ErrorData  *edata;
	PLyExceptionEntry *entry;
	PyObject   *exc;

	/* Save error info */
	MemoryContextSwitchTo(oldcontext);
	edata = CopyErrorData();
	FlushErrorState();

	/* Abort the inner transaction */
	RollbackAndReleaseCurrentSubTransaction();
	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	/* Look up the correct exception */
	entry = hash_search(PLy_spi_exceptions, &(edata->sqlerrcode),
						HASH_FIND, NULL);

	/*
	 * This could be a custom error code, if that's the case fallback to
	 * SPIError
	 */
	exc = entry ? entry->exc : PLy_exc_spi_error;
	/* Make Python raise the exception */
	PLy_spi_exception_set(exc, edata);
	FreeErrorData(edata);
}

/*
 * Raise a SPIError, passing in it more error details, like the
 * internal query and error position.
 */
static void
PLy_spi_exception_set(PyObject *excclass, ErrorData *edata)
{
	PyObject   *args = NULL;
	PyObject   *spierror = NULL;
	PyObject   *spidata = NULL;

	args = Py_BuildValue("(s)", edata->message);
	if (!args)
		goto failure;

	/* create a new SPI exception with the error message as the parameter */
	spierror = PyObject_CallObject(excclass, args);
	if (!spierror)
		goto failure;

	spidata = Py_BuildValue("(izzzizzzzz)", edata->sqlerrcode, edata->detail, edata->hint,
							edata->internalquery, edata->internalpos,
							edata->schema_name, edata->table_name, edata->column_name,
							edata->datatype_name, edata->constraint_name);
	if (!spidata)
		goto failure;

	if (PyObject_SetAttrString(spierror, "spidata", spidata) == -1)
		goto failure;

	PyErr_SetObject(excclass, spierror);

	Py_DECREF(args);
	Py_DECREF(spierror);
	Py_DECREF(spidata);
	return;

failure:
	Py_XDECREF(args);
	Py_XDECREF(spierror);
	Py_XDECREF(spidata);
	elog(ERROR, "could not convert SPI error to Python exception");
}
