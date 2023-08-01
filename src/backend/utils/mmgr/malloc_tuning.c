#include "postgres.h"

#include "utils/guc_hooks.h"
#include "utils/memutils.h"
#include "miscadmin.h"


/*
 * Implementation speficic GUCs. Those are defined even if we use another implementation, but will have
 * no effect in that case.
 */

int glibc_malloc_max_trim_threshold;

/*
 * Depending on the malloc implementation used, we may want to
 * tune it.
 * In this first version, the only tunable library is glibc's malloc
 * implementation.
 */
/* GLIBC implementation */

void
assign_work_mem(int newval, void* extra)
{
	work_mem = newval;
	MallocAdjustSettings();
}

void
assign_glibc_trim_threshold(int newval, void* extra)
{
	glibc_malloc_max_trim_threshold = newval;
	MallocAdjustSettings();
}

#if defined(__GLIBC__)
#include <malloc.h>
#include <limits.h>
#include <bits/wordsize.h>

int previous_mmap_threshold = -1;
int previous_trim_threshold = -1;

/* For GLIBC's malloc, we want to avoid having too many mmap'd memory regions,
 * and also to avoid repeatingly allocating / releasing memory from the system.
 *
 * The default configuration of malloc adapt's its M_MMAP_THRESHOLD and M_TRIM_THRESHOLD
 * dynamically when previoulsy mmaped blocks are freed.
 *
 * This isn't really sufficient for our use case, as we may end up with a trim
 * threshold which repeatedly releases work_mem memory to the system.
 *
 * Instead of letting malloc dynamically tune itself, for values up to 32MB we
 * ensure that work_mem will fit both bellow M_MMAP_THRESHOLD and
 * M_TRIM_THRESHOLD. The rationale for this is that once a backend has allocated
 * this much memory, it is likely to use it again.
 *
 * To keep up with malloc's default behaviour, we set M_TRIM_THRESHOLD to
 * M_MMAP_THRESHOLD * 2 so that work_mem blocks can avoid being released too
 * early.
 *
 * Newer versions of malloc got rid of the MALLOC_MMAP_THRESHOLD upper limit,
 * but we still enforce the values it sets to avoid wasting too much memory if we have a huge
 * work_mem which is used only once.
 */

# if __WORDSIZE == 32
#  define MMAP_THRESHOLD_MAX (512 * 1024)
# else
#  define MMAP_THRESHOLD_MAX (4 * 1024 * 1024 * sizeof(long))
# endif

void
MallocAdjustSettings()
{
	int uncapped_mmap_threshold,
		mmap_threshold,
		trim_threshold;
	long base_target;
	/* If static malloc tuning is disabled, bail out. */
	if (glibc_malloc_max_trim_threshold == -1)
		return;
	/* We don't want to adjust anything in the postmaster process, as that would
	 * disable dynamic adjustment for any child process*/
	if ((MyProcPid == PostmasterPid) ||
		((MyBackendType != B_BACKEND) &&
		 (MyBackendType != B_BG_WORKER)))
		return;
	base_target = Min((long) work_mem * 1024, (long) glibc_malloc_max_trim_threshold / 2 * 1024);
	/* To account for overhead, add one more memory page to that. */
	base_target += 4096;
	uncapped_mmap_threshold = Min(INT_MAX, base_target);
	/* Cap mmap_threshold to MMAP_THRESHOLD_MAX */
	mmap_threshold = Min(MMAP_THRESHOLD_MAX, uncapped_mmap_threshold);
	/* Set trim treshold to two times the uncapped value */
	trim_threshold = Min(INT_MAX, (long) uncapped_mmap_threshold * 2);
	if (mmap_threshold != previous_mmap_threshold)
	{
		mallopt(M_MMAP_THRESHOLD, mmap_threshold);
		previous_mmap_threshold = mmap_threshold;
	}

	if (trim_threshold != previous_trim_threshold)
	{
		mallopt(M_TRIM_THRESHOLD, trim_threshold);
		/* If we reduce the trim_threshold, ask malloc to actually trim it.
		 * This allows us to recover from a bigger work_mem set up once, then
		 * reset back to a smaller value.
		 */
		if (trim_threshold < previous_trim_threshold)
			malloc_trim(trim_threshold);
		previous_trim_threshold = trim_threshold;
	}
}

/* Default no-op implementation for others malloc providers. */
#else
void MallocAdjustSettings()
{
}
#endif
