/*-------------------------------------------------------------------------
 *
 * pgsleep.c
 *	   Portable delay handling.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * src/port/pgsleep.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include <time.h>

#include "portability/instr_time.h"

/*
 * In a Windows backend, we don't use this implementation, but rather
 * the signal-aware version in src/backend/port/win32/signal.c.
 */
#if defined(FRONTEND) || !defined(WIN32)

/*
 * pg_usleep --- delay the specified number of microseconds.
 *
 * NOTE: Although the delay is specified in microseconds, older Unixen and
 * Windows use periodic kernel ticks to wake up, which might increase the delay
 * time significantly.  We've observed delay increases as large as 20
 * milliseconds on supported platforms.
 *
 * On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
 *
 * CAUTION: It's not a good idea to use long sleeps in the backend.  They will
 * silently return early if a signal is caught, but that doesn't include
 * latches being set on most OSes, and even signal handlers that set MyLatch
 * might happen to run before the sleep begins, allowing the full delay.
 * Better practice is to use WaitLatch() with a timeout, so that backends
 * respond to latches and signals promptly.
 */
void
pg_usleep(long microsec)
{
	if (microsec > 0)
	{
#ifndef WIN32
		struct timespec delay;

		delay.tv_sec = microsec / 1000000L;
		delay.tv_nsec = (microsec % 1000000L) * 1000;
		(void) nanosleep(&delay, NULL);
#else
		SleepEx((microsec < 500 ? 1 : (microsec + 500) / 1000), FALSE);
#endif
	}
}

/*
 * pg_usleep_non_interruptible --- delay the specified number of microseconds.
 *
 * Unlike pg_usleep, this function continues the delay in case of an
 * interrupt.
 */
void
pg_usleep_non_interruptible(long microsec)
{
	/*
	 * We allow nanosleep to handle interrupts and retry with the remaining
	 * time. However, frequent interruptions and restarts of the nanosleep
	 * calls can substantially lead to drift in the time when the sleep
	 * finally completes. To deal with this, we break out of the loop whenever
	 * the current time is past the expected end time of the sleep.
	 */
	if (microsec > 0)
	{
#ifndef WIN32
		struct timespec delay;
		struct timespec remain;
		instr_time	end_time;

		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_ADD_MICROSEC(end_time, microsec);

		delay.tv_sec = microsec / 1000000L;
		delay.tv_nsec = (microsec % 1000000L) * 1000;

		while (nanosleep(&delay, &remain) == -1 && errno == EINTR)
		{
			instr_time	current_time;

			INSTR_TIME_SET_CURRENT(current_time);

			if (INSTR_TIME_IS_GREATER(current_time, end_time))
				break;

			delay = remain;
		}
#else
		SleepEx((microsec < 500 ? 1 : (microsec + 500) / 1000), FALSE);
#endif
	}
}


#endif							/* defined(FRONTEND) || !defined(WIN32) */
