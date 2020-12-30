/*
 * catcachebench: test code for cache pruning feature
 */
/* #define CATCACHE_STATS */
#include "postgres.h"
#include "catalog/pg_type.h"
#include "catalog/pg_statistic.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

Oid		tableoids[10000];
int		ntables = 0;
int16	attnums[1000];
int		natts = 0;

PG_MODULE_MAGIC;

double catcachebench1(void);
double catcachebench2(void);
double catcachebench3(void);
void collectinfo(void);
void catcachewarmup(void);

PG_FUNCTION_INFO_V1(catcachebench);
PG_FUNCTION_INFO_V1(catcachereadstats);

extern void CatalogCacheFlushCatalog2(Oid catId);
extern int64 catcache_called;
extern CatCache *SysCache[];

typedef struct catcachestatsstate
{
	TupleDesc tupd;
	int		  catId;
} catcachestatsstate;

Datum
catcachereadstats(PG_FUNCTION_ARGS)
{
	catcachestatsstate *state_data = NULL;
	FuncCallContext *fctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext mctx;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		state_data = palloc(sizeof(catcachestatsstate));

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		state_data->tupd = tupdesc;
		state_data->catId = 0;

		fctx->user_fctx = state_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	state_data = fctx->user_fctx;

	if (state_data->catId < SysCacheSize)
	{
		Datum	values[5];
		bool	nulls[5];
		HeapTuple	resulttup;
		Datum	result;
		int		catId = state_data->catId++;

		memset(nulls, 0, sizeof(nulls));
		memset(values, 0, sizeof(values));
		values[0] = Int16GetDatum(catId);
		values[1] = ObjectIdGetDatum(SysCache[catId]->cc_reloid);
#ifdef CATCACHE_STATS		
		values[2] = Int64GetDatum(SysCache[catId]->cc_searches);
		values[3] = Int64GetDatum(SysCache[catId]->cc_hits);
		values[4] = Int64GetDatum(SysCache[catId]->cc_neg_hits);
#endif
		resulttup = heap_form_tuple(state_data->tupd, values, nulls);
		result = HeapTupleGetDatum(resulttup);

		SRF_RETURN_NEXT(fctx, result);
	}

	SRF_RETURN_DONE(fctx);
}

Datum
catcachebench(PG_FUNCTION_ARGS)
{
	int		testtype = PG_GETARG_INT32(0);
	double	ms;

	collectinfo();

	/* flush the catalog -- safe? don't mind. */
	CatalogCacheFlushCatalog2(StatisticRelationId);

	switch (testtype)
	{
	case 0:
		catcachewarmup(); /* prewarm of syscatalog */
		PG_RETURN_NULL();
	case 1:
		ms = catcachebench1(); break;
	case 2:
		ms = catcachebench2(); break;
	case 3:
		ms = catcachebench3(); break;
	default:
		elog(ERROR, "Invalid test type: %d", testtype);
	}

	PG_RETURN_DATUM(Float8GetDatum(ms));
}

/*
 * fetch all attribute entires of all tables.
 */
double
catcachebench1(void)
{
	int t, a;
	instr_time	start,
				duration;

	PG_SETMASK(&BlockSig);
	INSTR_TIME_SET_CURRENT(start);
	for (t = 0 ; t < ntables ; t++)
	{
		for (a = 0 ; a < natts ; a++)
		{
			HeapTuple tup;

			tup = SearchSysCache3(STATRELATTINH,
								  ObjectIdGetDatum(tableoids[t]),
								  Int16GetDatum(attnums[a]),
								  BoolGetDatum(false));
			/* should be null, but.. */
			if (HeapTupleIsValid(tup))
				ReleaseSysCache(tup);
		}
	}
	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
	PG_SETMASK(&UnBlockSig);

	return INSTR_TIME_GET_MILLISEC(duration);
};

/*
 * fetch all attribute entires of a table 6000 times.
 */
double
catcachebench2(void)
{
	int t, a;
	instr_time	start,
				duration;

	PG_SETMASK(&BlockSig);
	INSTR_TIME_SET_CURRENT(start);
	for (t = 0 ; t < 240000 ; t++)
	{
		for (a = 0 ; a < natts ; a++)
		{
			HeapTuple tup;

			tup = SearchSysCache3(STATRELATTINH,
								  ObjectIdGetDatum(tableoids[0]),
								  Int16GetDatum(attnums[a]),
								  BoolGetDatum(false));
			/* should be null, but.. */
			if (HeapTupleIsValid(tup))
				ReleaseSysCache(tup);
		}
	}
	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
	PG_SETMASK(&UnBlockSig);

	return INSTR_TIME_GET_MILLISEC(duration);
};

/*
 * fetch all attribute entires of all tables twice with having expiration
 * happen.
 */
double
catcachebench3(void)
{
	const int clock_step = 1000;
	int i, t, a;
	instr_time	start,
				duration;

	PG_SETMASK(&BlockSig);
	INSTR_TIME_SET_CURRENT(start);
	for (i = 0 ; i < 4 ; i++)
	{
		int ct = clock_step;

		for (t = 0 ; t < ntables ; t++)
		{
			/*
			 * catcacheclock is updated by transaction timestamp, so needs to
			 * be updated by other means for this test to work. Here I choosed
			 * to update the clock every 1000 tables scan.
			 */
			if (--ct < 0)
			{
				SetCatCacheClock(GetCurrentTimestamp());
				ct = clock_step;
			}
			for (a = 0 ; a < natts ; a++)
			{
				HeapTuple tup;

				tup = SearchSysCache3(STATRELATTINH,
									  ObjectIdGetDatum(tableoids[t]),
									  Int16GetDatum(attnums[a]),
									  BoolGetDatum(false));
				/* should be null, but.. */
				if (HeapTupleIsValid(tup))
					ReleaseSysCache(tup);
			}
		}
	}
	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
	PG_SETMASK(&UnBlockSig);

	return INSTR_TIME_GET_MILLISEC(duration);
};

void
catcachewarmup(void)
{
	int t, a;

	/* load up catalog tables */
	for (t = 0 ; t < ntables ; t++)
	{
		for (a = 0 ; a < natts ; a++)
		{
			HeapTuple tup;

			tup = SearchSysCache3(STATRELATTINH,
								  ObjectIdGetDatum(tableoids[t]),
								  Int16GetDatum(attnums[a]),
								  BoolGetDatum(false));
			/* should be null, but.. */
			if (HeapTupleIsValid(tup))
				ReleaseSysCache(tup);
		}
	}
}

void
collectinfo(void)
{
	int ret;
	Datum	values[10000];
	bool	nulls[10000];
	Oid		types0[] = {OIDOID};
	int i;

	ntables = 0;
	natts = 0;

	SPI_connect();
	/* collect target tables */
	ret = SPI_execute("select oid from pg_class where relnamespace = (select oid from pg_namespace where nspname = \'test\')",
					  true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "Failed 1");
	if (SPI_processed == 0)
		elog(ERROR, "no relation found in schema \"test\"");
	if (SPI_processed > 10000)
		elog(ERROR, "too many relation found in schema \"test\"");

	for (i = 0 ; i < SPI_processed ; i++)
	{
		heap_deform_tuple(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
						  values, nulls);
		if (nulls[0])
			elog(ERROR, "Failed 2");

		tableoids[ntables++] = DatumGetObjectId(values[0]);
	}
	SPI_finish();
	elog(DEBUG1, "%d tables found", ntables);

	values[0] = ObjectIdGetDatum(tableoids[0]);
	nulls[0] = false;
	SPI_connect();
	ret = SPI_execute_with_args("select attnum from pg_attribute where attrelid = (select oid from pg_class where oid = $1)",
								1, types0, values, NULL, true, 0);
	if (SPI_processed == 0)
		elog(ERROR, "no attribute found in table %d", tableoids[0]);
	if (SPI_processed > 10000)
		elog(ERROR, "too many relation found in table %d", tableoids[0]);
	
	/* collect target attributes. assuming all tables have the same attnums */
	for (i = 0 ; i < SPI_processed ; i++)
	{
		int16 attnum;

		heap_deform_tuple(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
						  values, nulls);
		if (nulls[0])
			elog(ERROR, "Failed 3");
		attnum = DatumGetInt16(values[0]);

		if (attnum > 0)
			attnums[natts++] = attnum;
	}
	SPI_finish();
	elog(DEBUG1, "%d attributes found", natts);
}
