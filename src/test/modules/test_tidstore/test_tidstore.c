/*--------------------------------------------------------------------------
 *
 * test_tidstore.c
 *		Test TidStore data structure.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_tidstore/test_tidstore.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tidstore.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* #define TEST_SHARED_TIDSTORE 1 */

#define TEST_TIDSTORE_MAX_BYTES (2 * 1024 * 1024L) /* 2MB */

PG_FUNCTION_INFO_V1(tidstore_create);
PG_FUNCTION_INFO_V1(tidstore_set_block_offsets);
PG_FUNCTION_INFO_V1(tidstore_dump_tids);
PG_FUNCTION_INFO_V1(tidstore_lookup_tids);
PG_FUNCTION_INFO_V1(tidstore_get_state);
PG_FUNCTION_INFO_V1(tidstore_reset);
PG_FUNCTION_INFO_V1(tidstore_destroy);

static TidStore *tidstore = NULL;

/* Create a TidStore on TopMemoryContext */
Datum
tidstore_create(PG_FUNCTION_ARGS)
{
	bool	shared = PG_GETARG_BOOL(0);
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	if (shared)
	{
		int tranche_id;
		dsa_area *dsa;

		tranche_id = LWLockNewTrancheId();
		LWLockRegisterTranche(tranche_id, "test_tidstore");
		dsa = dsa_create(tranche_id);
		dsa_pin_mapping(dsa);

		tidstore = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, MaxHeapTuplesPerPage, dsa);
	}
	else
		tidstore = TidStoreCreate(TEST_TIDSTORE_MAX_BYTES, MaxHeapTuplesPerPage, NULL);

	MemoryContextSwitchTo(old_ctx);

	PG_RETURN_VOID();
}

static void
sanity_check_array(ArrayType *ta)
{
	if (ARR_HASNULL(ta) && array_contains_nulls(ta))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("array must not contain nulls")));

	if (ARR_NDIM(ta) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("argument must be empty or one-dimensional array")));
}

static void
check_tidstore_available(void)
{
	if (tidstore == NULL)
		elog(ERROR, "tidstore is not initialized");
}

/* Set the given block and offsets pairs */
Datum
tidstore_set_block_offsets(PG_FUNCTION_ARGS)
{
	BlockNumber blkno = PG_GETARG_INT64(0);
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P_COPY(1);
	OffsetNumber *offs;
	int	noffs;

	check_tidstore_available();
	sanity_check_array(ta);

	noffs = ArrayGetNItems(ARR_NDIM(ta), ARR_DIMS(ta));
	offs = ((OffsetNumber *) ARR_DATA_PTR(ta));

	TidStoreSetBlockOffsets(tidstore, blkno, offs, noffs);

	PG_RETURN_VOID();
}

/* Dump tids in the tidstore. The output should be sorted by tid */
Datum
tidstore_dump_tids(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ItemPointerData	*tids;

	check_tidstore_available();

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TidStoreIter	*iter;
		TidStoreIterResult	*iter_result;
		int64			ntids = 0;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		tids = (ItemPointerData *)
			palloc0(sizeof(ItemPointerData) * TidStoreNumTids(tidstore));

		iter = TidStoreBeginIterate(tidstore);
		while ((iter_result = TidStoreIterateNext(iter)) != NULL)
		{
			for (int i = 0; i < iter_result->num_offsets; i++)
				ItemPointerSet(&(tids[ntids++]), iter_result->blkno,
							   iter_result->offsets[i]);
		}

		Assert(ntids == TidStoreNumTids(tidstore));

		funcctx->user_fctx = tids;
		funcctx->max_calls = TidStoreNumTids(tidstore);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	tids = (ItemPointerData *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		int i = funcctx->call_cntr;
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(&(tids[i])));
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
tidstore_lookup_tids(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P_COPY(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	ItemPointer	tids;
	int		ntids;
	Datum	values[2];
	bool	nulls[2] = {false};

	check_tidstore_available();

	sanity_check_array(ta);

	InitMaterializedSRF(fcinfo, 0);

	ntids = ArrayGetNItems(ARR_NDIM(ta), ARR_DIMS(ta));
	tids = ((ItemPointer) ARR_DATA_PTR(ta));

	for (int i = 0; i < ntids; i++)
	{
		bool found;
		ItemPointerData tid = tids[i];

		found = TidStoreIsMember(tidstore, &tid);

		values[0] = ItemPointerGetDatum(&tid);
		values[1] = BoolGetDatum(found);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

Datum
tidstore_get_state(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum	values[2];
	bool	nulls[2] = {false};

	check_tidstore_available();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* num_tids */
	values[0] = Int64GetDatum(TidStoreNumTids(tidstore));

	/* is_full */
	values[1] = BoolGetDatum(TidStoreIsFull(tidstore));

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

Datum
tidstore_reset(PG_FUNCTION_ARGS)
{
	check_tidstore_available();

	TidStoreReset(tidstore);

	PG_RETURN_VOID();
}

Datum
tidstore_destroy(PG_FUNCTION_ARGS)
{
	check_tidstore_available();

	TidStoreDestroy(tidstore);
	tidstore = NULL;
	/* DSA for tidstore will be detached at the end of session */

	PG_RETURN_VOID();
}
