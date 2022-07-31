/*-------------------------------------------------------------------------
 *
 * instr_time.c
 *	   Non-inline parts of the portable high-precision interval timing
 *	 implementation
 *
 * Portions Copyright (c) 2022, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/port/instr_time.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "portability/instr_time.h"

#ifdef HAVE_CLOCK_GETTIME

/*
 * Stores what the number of cycles needs to be multiplied with to end up with
 * seconds. This indirection exists to support the rtdsc instruction.
 *
 * As a default, assume we are using clock_gettime() as a fallback and treat it
 * as 1 "cycle" per nanosecond (aka 1 GHz).
 *
 * When using the rtdsc instruction directly this is filled in during
 * initialization based on the relevant cpuid fields.
 */
double cycles_to_sec = 1.0 / NS_PER_S;

/*
 * Determines whether rdtsc is used (Linux/x86 only, when OS uses tsc clocksource)
 */
bool use_rdtsc = false;

#if defined(__x86_64__) && defined(__linux__)
/*
 * Decide whether we use the rdtsc instruction at runtime, for Linux/x86,
 * instead of incurring the overhead of a full clock_gettime() call.
 *
 * This can't be reliably determined at compile time, since the
 * availability of an "invariant" TSC (that is not affected by CPU
 * frequency changes) is dependent on the CPU architecture. Additionally,
 * there are cases where TSC availability is impacted by virtualization,
 * where a simple cpuid feature check would not be enough.
 *
 * Since Linux already does a significant amount of work to determine
 * whether TSC is a viable clock source, decide based on that.
 */
void pg_clock_gettime_initialize_rdtsc(void)
{
	FILE *fp = fopen("/sys/devices/system/clocksource/clocksource0/current_clocksource", "r");
	char buf[128];

	if (fp)
	{
		fgets(buf, sizeof(buf), fp);
		if (strcmp(buf, "tsc\n") == 0)
		{
			use_rdtsc = true;
		}
		fclose(fp);
	}

	/*
	 * Compute baseline cpu peformance, determines speed at which rdtsc advances
	 */
	if (use_rdtsc)
	{
		uint32 cpuinfo[4] = {0};

		/*
		 * FIXME: We should probably not unnecessarily use floating point math
		 * here. And it's likely that the numbers are small enough that we are running
		 * into floating point inaccuracies already. Probably worthwhile to be a good
		 * bit smarter.
		 */

		__get_cpuid(0x16, cpuinfo, cpuinfo + 1, cpuinfo + 2, cpuinfo + 3);

		if (cpuinfo[0] != 0) {
			cycles_to_sec = 1 / ((double) cpuinfo[0] * 1000 * 1000);
		} else {
			FILE       *fp = fopen("/proc/cpuinfo", "r");
			char            buf[128];
			float cpu_mhz;

			if (fp)
			{
				while (fgets(buf, sizeof(buf), fp))
				{
					if (sscanf(buf, "cpu MHz                : %f", &cpu_mhz) == 1)
					{
						cycles_to_sec = 1 / ((double) cpu_mhz * 1000 * 1000);
						break;
					}
				}
			}
			fclose(fp);
		}
	}
}
#endif							/* defined(__x86_64__) && defined(__linux__) */

#endif						    /* HAVE_CLOCK_GETTIME */
