/*-------------------------------------------------------------------------
 *
 * s_lock.c
 *	   Implementation of spinlocks.
 *
 * When waiting for a contended spinlock we loop tightly for awhile, then
 * delay using pg_usleep() and try again.  Preferably, "awhile" should be a
 * small multiple of the maximum time we expect a spinlock to be held.  1000
 * iterations has historically worked well for multiprocessors, where it's
 * usually better to spin a bit longer than to call the kernel.  We used to
 * try to adapt the spin loop count depending on whether we seem to be in a
 * uniprocessor or multiprocessor (since the loop is a waste of cycles on a
 * uniprocessor) but we no longer anticipate any serious usage on
 * uniprocessor systems.
 *
 * Once we do decide to block, we use randomly increasing pg_usleep()
 * delays. The first delay is 1 msec, then the delay randomly increases to
 * about one second, after which we reset to 1 msec and start again.  The
 * idea here is that in the presence of heavy contention we need to
 * increase the delay, else the spinlock holder may never get to run and
 * release the lock.  (Consider situation where spinlock holder has been
 * nice'd down in priority by the scheduler --- it will not get scheduled
 * until all would-be acquirers are sleeping, so if we always use a 1-msec
 * sleep, there is a real possibility of starvation.)  But we can't just
 * clamp the delay to an upper bound, else it would take a long time to
 * make a reasonable number of tries.
 *
 * We time out and declare error after NUM_DELAYS delays (thus, exactly
 * that many tries).  With the given settings, this will usually take 2 or
 * so minutes.  It seems better to fix the total number of tries (and thus
 * the probability of unintended failure) than to fix the total time
 * spent.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/s_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>
#include <unistd.h>

#include "common/pg_prng.h"
#include "port/atomics.h"
#include "storage/s_lock.h"
#include "utils/wait_event.h"

#define SPINS_PER_DELAY		1000
#define NUM_DELAYS			1000
#define MIN_DELAY_USEC		1000L
#define MAX_DELAY_USEC		1000000L

#ifdef S_LOCK_TEST
/*
 * These are needed by pgstat_report_wait_start in the standalone compile of
 * s_lock_test.
 */
static uint32 local_my_wait_event_info;
uint32	   *my_wait_event_info = &local_my_wait_event_info;
#endif


/*
 * s_lock_stuck() - complain about a stuck spinlock
 */
static void
s_lock_stuck(const char *file, int line, const char *func)
{
	if (!func)
		func = "(unknown)";
#if defined(S_LOCK_TEST)
	fprintf(stderr,
			"\nStuck spinlock detected at %s, %s:%d.\n",
			func, file, line);
	exit(1);
#else
	elog(PANIC, "stuck spinlock detected at %s, %s:%d",
		 func, file, line);
#endif
}

/*
 * s_lock(lock) - platform-independent portion of waiting for a spinlock.
 */
int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	SpinDelayStatus delayStatus;

	init_spin_delay(&delayStatus, file, line, func);

	while (TAS_SPIN(lock))
	{
		perform_spin_delay(&delayStatus);
	}

	return delayStatus.delays;
}

#ifdef USE_DEFAULT_S_UNLOCK
void
s_unlock(volatile slock_t *lock)
{
	*lock = 0;
}
#endif

/*
 * Wait while spinning on a contended spinlock.
 */
void
perform_spin_delay(SpinDelayStatus *status)
{
	/* CPU-specific delay each time through the loop */
	SPIN_DELAY();

	/* Block the process every SPINS_PER_DELAY tries */
	if (++(status->spins) >= SPINS_PER_DELAY)
	{
		if (++(status->delays) > NUM_DELAYS)
			s_lock_stuck(status->file, status->line, status->func);

		if (status->cur_delay == 0) /* first time to delay? */
			status->cur_delay = MIN_DELAY_USEC;

		/*
		 * Once we start sleeping, the overhead of reporting a wait event is
		 * justified. Actively spinning easily stands out in profilers, but
		 * sleeping with an exponential backoff is harder to spot...
		 *
		 * We might want to report something more granular at some point, but
		 * this is better than nothing.
		 */
		pgstat_report_wait_start(WAIT_EVENT_SPIN_DELAY);
		pg_usleep(status->cur_delay);
		pgstat_report_wait_end();

#if defined(S_LOCK_TEST)
		fprintf(stdout, "*");
		fflush(stdout);
#endif

		/* increase delay by a random fraction between 1X and 2X */
		status->cur_delay += (int) (status->cur_delay *
									pg_prng_double(&pg_global_prng_state) + 0.5);
		/* wrap back to minimum delay when max is exceeded */
		if (status->cur_delay > MAX_DELAY_USEC)
			status->cur_delay = MIN_DELAY_USEC;

		status->spins = 0;
	}
}


/*****************************************************************************/
#if defined(S_LOCK_TEST)

/*
 * test program for verifying a port's spinlock support.
 */

struct test_lock_struct
{
	char		pad1;
	slock_t		lock;
	char		pad2;
};

volatile struct test_lock_struct test_lock;

int
main()
{
	pg_prng_seed(&pg_global_prng_state, (uint64) time(NULL));

	test_lock.pad1 = test_lock.pad2 = 0x44;

	S_INIT_LOCK(&test_lock.lock);

	if (test_lock.pad1 != 0x44 || test_lock.pad2 != 0x44)
	{
		printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
		return 1;
	}

	if (!S_LOCK_FREE(&test_lock.lock))
	{
		printf("S_LOCK_TEST: failed, lock not initialized\n");
		return 1;
	}

	S_LOCK(&test_lock.lock);

	if (test_lock.pad1 != 0x44 || test_lock.pad2 != 0x44)
	{
		printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
		return 1;
	}

	if (S_LOCK_FREE(&test_lock.lock))
	{
		printf("S_LOCK_TEST: failed, lock not locked\n");
		return 1;
	}

	S_UNLOCK(&test_lock.lock);

	if (test_lock.pad1 != 0x44 || test_lock.pad2 != 0x44)
	{
		printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
		return 1;
	}

	if (!S_LOCK_FREE(&test_lock.lock))
	{
		printf("S_LOCK_TEST: failed, lock not unlocked\n");
		return 1;
	}

	S_LOCK(&test_lock.lock);

	if (test_lock.pad1 != 0x44 || test_lock.pad2 != 0x44)
	{
		printf("S_LOCK_TEST: failed, declared datatype is wrong size\n");
		return 1;
	}

	if (S_LOCK_FREE(&test_lock.lock))
	{
		printf("S_LOCK_TEST: failed, lock not re-locked\n");
		return 1;
	}

	printf("S_LOCK_TEST: this will print %d stars and then\n", NUM_DELAYS);
	printf("             exit with a 'stuck spinlock' message\n");
	printf("             if S_LOCK() and TAS() are working.\n");
	fflush(stdout);

	s_lock(&test_lock.lock, __FILE__, __LINE__, __func__);

	printf("S_LOCK_TEST: failed, lock not locked\n");
	return 1;
}

#endif							/* S_LOCK_TEST */
