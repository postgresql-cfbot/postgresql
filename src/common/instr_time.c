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

#ifndef WIN32
/*
 * Stores what the number of cycles needs to be multiplied with to end up
 * with nanoseconds using integer math. See comment in pg_initialize_rdtsc()
 * for more details.
 *
 * By default assume we are using clock_gettime() as a fallback which uses
 * nanoseconds as ticks. Hence, we set the multiplier to the precision scalar
 * so that the division in INSTR_TIME_GET_NANOSEC() won't change the nanoseconds.
 *
 * When using the RDTSC instruction directly this is filled in during initialization
 * based on the relevant CPUID fields.
 */
int64 ticks_per_ns_scaled = TICKS_TO_NS_PRECISION;
int64 ticks_per_sec = NS_PER_S;
int64 max_ticks_no_overflow = PG_INT64_MAX / TICKS_TO_NS_PRECISION;

#if defined(__x86_64__) && defined(__linux__)
/*
 * Indicates if RDTSC can be used (Linux/x86 only, when OS uses TSC clocksource)
 */
bool has_rdtsc = false;

/*
 * Indicates if RDTSCP can be used. True if RDTSC can be used and RDTSCP is available.
 */
bool has_rdtscp = false;

/*
 * This fails on hypervisors. On hypervisors a different CPUID leaf could be used
 * to obtain the TSC frequency. We don't do that because the TSC on hypervisors
 * is usually emulated and the performance of emulated an TSC is usually as bad as
 * the performance of the hypervisor's clock_gettime() implementation.
 */
static bool get_tsc_frequency_khz(uint32 *tsc_freq)
{
	uint32 r0, r1, r2;
	int result = __get_cpuid(0x16, tsc_freq, &r0, &r1, &r2);
	*tsc_freq *= 1000; // Convert from MHz to KHz
	return result > 0 && *tsc_freq > 0;
}

static bool is_rdtscp_available()
{
	uint32 r0, r1, r2, r3;
	return __get_cpuid(0x80000001, &r0, &r1, &r2, &r3) > 0 && (r3 & (1 << 27)) != 0;
}

/*
 * Decide whether we use the RDTSC instruction at runtime, for Linux/x86,
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
void pg_initialize_rdtsc(void)
{
	FILE *fp = fopen("/sys/devices/system/clocksource/clocksource0/current_clocksource", "r");

	if (fp)
	{
		char buf[128];

		if (fgets(buf, sizeof(buf), fp) != NULL && strcmp(buf, "tsc\n") == 0)
		{
			/*
			* Compute baseline CPU peformance, determines speed at which RDTSC advances.
			*/
			uint32 tsc_freq;

			if (get_tsc_frequency_khz(&tsc_freq))
			{
				/*
				* Ticks to nanoseconds conversion requires floating point math
				* because because:
				* 
				* sec = ticks / frequency_hz
				* ns  = ticks / frequency_hz * 1,000,000,000
				* ns  = ticks * (1,000,000,000 / frequency_hz)
				* ns  = ticks * (1,000,000 / frequency_khz) <-- now in kilohertz
				*
				* Here, 'ns' is usually a floating number. For example for a 2.5 GHz
				* CPU the scaling factor becomes 1,000,000 / 2,500,000 = 1.2.
				*
				* To be able to use integer math we work around the lack of precision.
				* We first scale the integer up and after the multiplication by the
				* number of ticks in INSTR_TIME_GET_NANOSEC() we divide again by the
				* same value. We picked the scaler such that it provides enough precision
				* and is a power-of-two which allows for shifting instead of doing an
				* integer division.
				*/
				ticks_per_ns_scaled = INT64CONST(1000000) * TICKS_TO_NS_PRECISION / tsc_freq;
				ticks_per_sec = tsc_freq * 1000; // KHz -> Hz
				max_ticks_no_overflow = PG_INT64_MAX / ticks_per_ns_scaled;

				has_rdtsc = true;
				has_rdtscp = is_rdtscp_available();
			}
		}

		fclose(fp);
	}
}
#endif							/* defined(__x86_64__) && defined(__linux__) */

#endif							/* WIN32 */
