/*-------------------------------------------------------------------------
 *
 * instrument.c
 *	 functions for instrumentation of plan execution
 *
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/executor/instrument.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"
#include "utils/memutils.h"

BufferUsage pgBufferUsage;
static BufferUsage save_pgBufferUsage;
WalUsage	pgWalUsage;
static WalUsage save_pgWalUsage;

List* pgCustInstr; /* description of custom instriumentations */
Size pgCustUsageSize;
static CustomInstrumentationData save_pgCustUsage; /* saved custom instrumentation state */


static void BufferUsageAdd(BufferUsage *dst, const BufferUsage *add);
static void WalUsageAdd(WalUsage *dst, WalUsage *add);

void
RegisterCustomInsrumentation(CustomInstrumentation* inst)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	pgCustInstr = lappend(pgCustInstr, inst);
	pgCustUsageSize += inst->size;
	MemoryContextSwitchTo(oldcontext);
	if (pgCustUsageSize > MAX_CUSTOM_INSTR_SIZE)
		elog(ERROR, "Total size of custom instrumentations exceed limit %d",  MAX_CUSTOM_INSTR_SIZE);
}

void
GetCustomInstrumentationState(char* dst)
{
	ListCell* lc;

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);	
		memcpy(dst, ci->usage, ci->size);
		dst += ci->size;
	}
}

void
AccumulateCustomInstrumentationState(char* dst, char const* before)
{
	ListCell* lc;

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);	
		if (ci->selected)
		{
			memset(dst, 0, ci->size);
			ci->accum(dst, ci->usage, before);
		}
		dst += ci->size;
		before += ci->size;
	}
}

/* Allocate new instrumentation structure(s) */
Instrumentation *
InstrAlloc(int n, int instrument_options, bool async_mode)
{
	Instrumentation *instr;

	/* initialize all fields to zeroes, then modify as needed */
	instr = palloc0(n * sizeof(Instrumentation));
	if (instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER | INSTRUMENT_WAL))
	{
		bool		need_buffers = (instrument_options & INSTRUMENT_BUFFERS) != 0;
		bool		need_wal = (instrument_options & INSTRUMENT_WAL) != 0;
		bool		need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;
		int			i;

		for (i = 0; i < n; i++)
		{
			instr[i].need_bufusage = need_buffers;
			instr[i].need_walusage = need_wal;
			instr[i].need_timer = need_timer;
			instr[i].async_mode = async_mode;
		}
	}
	return instr;
}

/* Initialize a pre-allocated instrumentation structure. */
void
InstrInit(Instrumentation *instr, int instrument_options)
{
	memset(instr, 0, sizeof(Instrumentation));
	instr->need_bufusage = (instrument_options & INSTRUMENT_BUFFERS) != 0;
	instr->need_walusage = (instrument_options & INSTRUMENT_WAL) != 0;
	instr->need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;
}

/* Entry to a plan node */
void
InstrStartNode(Instrumentation *instr)
{
	ListCell *lc;
	char* cust_start = instr->cust_usage_start.data;
	if (instr->need_timer &&
		!INSTR_TIME_SET_CURRENT_LAZY(instr->starttime))
		elog(ERROR, "InstrStartNode called twice in a row");

	/* save buffer usage totals at node entry, if needed */
	if (instr->need_bufusage)
		instr->bufusage_start = pgBufferUsage;

	if (instr->need_walusage)
		instr->walusage_start = pgWalUsage;

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		memcpy(cust_start, ci->usage, ci->size);
		cust_start += ci->size;
	}
}

/* Exit from a plan node */
void
InstrStopNode(Instrumentation *instr, double nTuples)
{
	double		save_tuplecount = instr->tuplecount;
	instr_time	endtime;
	ListCell *lc;
	char *cust_start = instr->cust_usage_start.data;
	char *cust_usage = instr->cust_usage.data;

	/* count the returned tuples */
	instr->tuplecount += nTuples;

	/* let's update the time only if the timer was requested */
	if (instr->need_timer)
	{
		if (INSTR_TIME_IS_ZERO(instr->starttime))
			elog(ERROR, "InstrStopNode called without start");

		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_ACCUM_DIFF(instr->counter, endtime, instr->starttime);

		INSTR_TIME_SET_ZERO(instr->starttime);
	}

	/* Add delta of buffer usage since entry to node's totals */
	if (instr->need_bufusage)
		BufferUsageAccumDiff(&instr->bufusage,
							 &pgBufferUsage, &instr->bufusage_start);

	if (instr->need_walusage)
		WalUsageAccumDiff(&instr->walusage,
						  &pgWalUsage, &instr->walusage_start);

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		ci->accum(cust_usage, ci->usage, cust_start);
		cust_start += ci->size;
		cust_usage += ci->size;
	}

    /* Is this the first tuple of this cycle? */
	if (!instr->running)
	{
		instr->running = true;
		instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
	}
	else
	{
		/*
		 * In async mode, if the plan node hadn't emitted any tuples before,
		 * this might be the first tuple
		 */
		if (instr->async_mode && save_tuplecount < 1.0)
			instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
	}
}

/* Update tuple count */
void
InstrUpdateTupleCount(Instrumentation *instr, double nTuples)
{
	/* count the returned tuples */
	instr->tuplecount += nTuples;
}

/* Finish a run cycle for a plan node */
void
InstrEndLoop(Instrumentation *instr)
{
	double		totaltime;

	/* Skip if nothing has happened, or already shut down */
	if (!instr->running)
		return;

	if (!INSTR_TIME_IS_ZERO(instr->starttime))
		elog(ERROR, "InstrEndLoop called on running node");

	/* Accumulate per-cycle statistics into totals */
	totaltime = INSTR_TIME_GET_DOUBLE(instr->counter);

	instr->startup += instr->firsttuple;
	instr->total += totaltime;
	instr->ntuples += instr->tuplecount;
	instr->nloops += 1;

	/* Reset for next cycle (if any) */
	instr->running = false;
	INSTR_TIME_SET_ZERO(instr->starttime);
	INSTR_TIME_SET_ZERO(instr->counter);
	instr->firsttuple = 0;
	instr->tuplecount = 0;
}

/* aggregate instrumentation information */
void
InstrAggNode(Instrumentation *dst, Instrumentation *add)
{
	ListCell *lc;
	char *cust_dst = dst->cust_usage.data;
	char *cust_add = add->cust_usage.data;

	if (!dst->running && add->running)
	{
		dst->running = true;
		dst->firsttuple = add->firsttuple;
	}
	else if (dst->running && add->running && dst->firsttuple > add->firsttuple)
		dst->firsttuple = add->firsttuple;

	INSTR_TIME_ADD(dst->counter, add->counter);

	dst->tuplecount += add->tuplecount;
	dst->startup += add->startup;
	dst->total += add->total;
	dst->ntuples += add->ntuples;
	dst->ntuples2 += add->ntuples2;
	dst->nloops += add->nloops;
	dst->nfiltered1 += add->nfiltered1;
	dst->nfiltered2 += add->nfiltered2;

	/* Add delta of buffer usage since entry to node's totals */
	if (dst->need_bufusage)
		BufferUsageAdd(&dst->bufusage, &add->bufusage);

	if (dst->need_walusage)
		WalUsageAdd(&dst->walusage, &add->walusage);

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		ci->add(cust_dst, cust_add);
		cust_dst += ci->size;
		cust_add += ci->size;
	}
}

/* note current values during parallel executor startup */
void
InstrStartParallelQuery(void)
{
	ListCell* lc;
	char* cust_dst = save_pgCustUsage.data;

	save_pgBufferUsage = pgBufferUsage;
	save_pgWalUsage = pgWalUsage;

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		memcpy(cust_dst, ci->usage, ci->size);
		cust_dst += ci->size;
	}
}

/* report usage after parallel executor shutdown */
void
InstrEndParallelQuery(BufferUsage *bufusage, WalUsage *walusage, char* cust_usage)
{
	ListCell *lc;
	char* cust_save = save_pgCustUsage.data;

	memset(bufusage, 0, sizeof(BufferUsage));
	BufferUsageAccumDiff(bufusage, &pgBufferUsage, &save_pgBufferUsage);
	memset(walusage, 0, sizeof(WalUsage));
	WalUsageAccumDiff(walusage, &pgWalUsage, &save_pgWalUsage);

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		ci->accum(cust_usage, ci->usage, cust_save);
		cust_usage += ci->size;
		cust_save += ci->size;
	}
}

/* accumulate work done by workers in leader's stats */
void
InstrAccumParallelQuery(BufferUsage *bufusage, WalUsage *walusage, char* cust_usage)
{
	ListCell *lc;
	BufferUsageAdd(&pgBufferUsage, bufusage);
	WalUsageAdd(&pgWalUsage, walusage);

	foreach (lc, pgCustInstr)
	{
		CustomInstrumentation *ci = (CustomInstrumentation*)lfirst(lc);
		ci->add(ci->usage, cust_usage);
		cust_usage += ci->size;
	}
}

/* dst += add */
static void
BufferUsageAdd(BufferUsage *dst, const BufferUsage *add)
{
	dst->shared_blks_hit += add->shared_blks_hit;
	dst->shared_blks_read += add->shared_blks_read;
	dst->shared_blks_dirtied += add->shared_blks_dirtied;
	dst->shared_blks_written += add->shared_blks_written;
	dst->local_blks_hit += add->local_blks_hit;
	dst->local_blks_read += add->local_blks_read;
	dst->local_blks_dirtied += add->local_blks_dirtied;
	dst->local_blks_written += add->local_blks_written;
	dst->temp_blks_read += add->temp_blks_read;
	dst->temp_blks_written += add->temp_blks_written;
	INSTR_TIME_ADD(dst->shared_blk_read_time, add->shared_blk_read_time);
	INSTR_TIME_ADD(dst->shared_blk_write_time, add->shared_blk_write_time);
	INSTR_TIME_ADD(dst->local_blk_read_time, add->local_blk_read_time);
	INSTR_TIME_ADD(dst->local_blk_write_time, add->local_blk_write_time);
	INSTR_TIME_ADD(dst->temp_blk_read_time, add->temp_blk_read_time);
	INSTR_TIME_ADD(dst->temp_blk_write_time, add->temp_blk_write_time);
}

/* dst += add - sub */
void
BufferUsageAccumDiff(BufferUsage *dst,
					 const BufferUsage *add,
					 const BufferUsage *sub)
{
	dst->shared_blks_hit += add->shared_blks_hit - sub->shared_blks_hit;
	dst->shared_blks_read += add->shared_blks_read - sub->shared_blks_read;
	dst->shared_blks_dirtied += add->shared_blks_dirtied - sub->shared_blks_dirtied;
	dst->shared_blks_written += add->shared_blks_written - sub->shared_blks_written;
	dst->local_blks_hit += add->local_blks_hit - sub->local_blks_hit;
	dst->local_blks_read += add->local_blks_read - sub->local_blks_read;
	dst->local_blks_dirtied += add->local_blks_dirtied - sub->local_blks_dirtied;
	dst->local_blks_written += add->local_blks_written - sub->local_blks_written;
	dst->temp_blks_read += add->temp_blks_read - sub->temp_blks_read;
	dst->temp_blks_written += add->temp_blks_written - sub->temp_blks_written;
	INSTR_TIME_ACCUM_DIFF(dst->shared_blk_read_time,
						  add->shared_blk_read_time, sub->shared_blk_read_time);
	INSTR_TIME_ACCUM_DIFF(dst->shared_blk_write_time,
						  add->shared_blk_write_time, sub->shared_blk_write_time);
	INSTR_TIME_ACCUM_DIFF(dst->local_blk_read_time,
						  add->local_blk_read_time, sub->local_blk_read_time);
	INSTR_TIME_ACCUM_DIFF(dst->local_blk_write_time,
						  add->local_blk_write_time, sub->local_blk_write_time);
	INSTR_TIME_ACCUM_DIFF(dst->temp_blk_read_time,
						  add->temp_blk_read_time, sub->temp_blk_read_time);
	INSTR_TIME_ACCUM_DIFF(dst->temp_blk_write_time,
						  add->temp_blk_write_time, sub->temp_blk_write_time);
}

/* helper functions for WAL usage accumulation */
static void
WalUsageAdd(WalUsage *dst, WalUsage *add)
{
	dst->wal_bytes += add->wal_bytes;
	dst->wal_records += add->wal_records;
	dst->wal_fpi += add->wal_fpi;
}

void
WalUsageAccumDiff(WalUsage *dst, const WalUsage *add, const WalUsage *sub)
{
	dst->wal_bytes += add->wal_bytes - sub->wal_bytes;
	dst->wal_records += add->wal_records - sub->wal_records;
	dst->wal_fpi += add->wal_fpi - sub->wal_fpi;
}
