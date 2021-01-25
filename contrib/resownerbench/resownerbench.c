#include "postgres.h"

#include "catalog/pg_type.h"
#include "catalog/pg_statistic.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(snapshotbench_lifo);
PG_FUNCTION_INFO_V1(snapshotbench_fifo);

/*
 * ResourceOwner Performance test, using RegisterSnapshot().
 *
 * This takes three parameters: numkeep, numsnaps, numiters.
 *
 * First, we register 'numkeep' snapshots. They are kept registed
 * until the end of the test. Then, we repeatedly register and
 * unregister 'numsnaps - numkeep' additional snapshots, repeating
 * 'numiters' times. All the register/unregister calls are made in
 * LIFO order.
 *
 * Returns the time spent, in milliseconds.
 *
 * The idea is to test the performance of ResourceOwnerRemember()
 * and ReourceOwnerForget() operations, under different regimes.
 *
 * In the old implementation, if 'numsnaps' is small enough, all
 * the entries fit in the resource owner's small array (it can
 * hold 64 entries).
 *
 * In the new implementation, the array is much smaller, only 8
 * entries, but it's used together with the hash table so that
 * we stay in the "array regime" as long as 'numsnaps - numkeep'
 * is smaller than 8 entries.
 *
 * 'numiters' can be adjusted to adjust the overall runtime to be
 * suitable long.
 */
Datum
snapshotbench_lifo(PG_FUNCTION_ARGS)
{
	int			numkeep = PG_GETARG_INT32(0);
	int			numsnaps = PG_GETARG_INT32(1);
	int			numiters = PG_GETARG_INT32(2);
	int			i;
	instr_time	start,
				duration;
	Snapshot	lsnap;
	Snapshot   *rs;
	int			numregistered = 0;

	rs = palloc(Max(numsnaps, numkeep) * sizeof(Snapshot));

	lsnap = GetLatestSnapshot();

	PG_SETMASK(&BlockSig);
	INSTR_TIME_SET_CURRENT(start);

	while (numregistered < numkeep)
	{
		rs[numregistered] = RegisterSnapshot(lsnap);
		numregistered++;
	}

	for (i = 0 ; i < numiters; i++)
	{
		while (numregistered < numsnaps)
		{
			rs[numregistered] = RegisterSnapshot(lsnap);
			numregistered++;
		}

		while (numregistered > numkeep)
		{
			numregistered--;
			UnregisterSnapshot(rs[numregistered]);
		}
	}

	while (numregistered > 0)
	{
		numregistered--;
		UnregisterSnapshot(rs[numregistered]);
	}

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
	PG_SETMASK(&UnBlockSig);

	PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(duration));
};


/*
 * Same, but do the register/unregister operations in
 * FIFO order.
 */
Datum
snapshotbench_fifo(PG_FUNCTION_ARGS)
{
	int			numkeep = PG_GETARG_INT32(0);
	int			numsnaps = PG_GETARG_INT32(1);
	int			numiters = PG_GETARG_INT32(2);
	int			i,
				j;
	instr_time	start,
				duration;
	Snapshot	lsnap;
	Snapshot   *rs;
	int			numregistered = 0;

	rs = palloc(Max(numsnaps, numkeep) * sizeof(Snapshot));

	lsnap = GetLatestSnapshot();

	PG_SETMASK(&BlockSig);
	INSTR_TIME_SET_CURRENT(start);

	while (numregistered < numkeep)
	{
		rs[numregistered] = RegisterSnapshot(lsnap);
		numregistered++;
	}

	for (i = 0 ; i < numiters; i++)
	{
		while (numregistered < numsnaps)
		{
			rs[numregistered] = RegisterSnapshot(lsnap);
			numregistered++;
		}

		for (j = numkeep; j < numregistered; j++)
			UnregisterSnapshot(rs[j]);
		numregistered = numkeep;
	}

	for (j = 0; j < numregistered; j++)
		UnregisterSnapshot(rs[j]);
	numregistered = numkeep;

	INSTR_TIME_SET_CURRENT(duration);
	INSTR_TIME_SUBTRACT(duration, start);
	PG_SETMASK(&UnBlockSig);

	PG_RETURN_FLOAT8(INSTR_TIME_GET_MILLISEC(duration));
};
