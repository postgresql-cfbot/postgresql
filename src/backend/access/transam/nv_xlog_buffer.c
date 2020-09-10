/*-------------------------------------------------------------------------
 *
 * nv_xlog_buffer.c
 *		PostgreSQL non-volatile WAL buffer
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/nv_xlog_buffer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_NVWAL

#include <libpmem.h>
#include "access/nv_xlog_buffer.h"

#include "miscadmin.h" /* IsBootstrapProcessingMode */
#include "common/file_perm.h" /* pg_file_create_mode */

/*
 * Maps non-volatile WAL buffer on shared memory.
 *
 * Returns a mapped address if success; PANICs and never return otherwise.
 */
void *
MapNonVolatileXLogBuffer(const char *fname, Size fsize)
{
	void	   *addr;
	size_t		map_len = 0;
	int			is_pmem = 0;

	Assert(fname != NULL);
	Assert(fsize > 0);

	if (IsBootstrapProcessingMode())
	{
		/*
		 * Create and map a new file if we are in bootstrap mode (typically
		 * executed by initdb).
		 */
		addr = pmem_map_file(fname, fsize, PMEM_FILE_CREATE|PMEM_FILE_EXCL,
							 pg_file_create_mode, &map_len, &is_pmem);
	}
	else
	{
		/*
		 * Map an existing file.  The second argument (len) should be zero,
		 * the third argument (flags) should have neither PMEM_FILE_CREATE nor
		 * PMEM_FILE_EXCL, and the fourth argument (mode) will be ignored.
		 */
		addr = pmem_map_file(fname, 0, 0, 0, &map_len, &is_pmem);
	}

	if (addr == NULL)
		elog(PANIC, "could not map non-volatile WAL buffer '%s': %m", fname);

	if (map_len != fsize)
		elog(PANIC, "size of non-volatile WAL buffer '%s' is invalid; "
					"expected %zu; actual %zu",
			 fname, fsize, map_len);

	if (!is_pmem)
		elog(PANIC, "non-volatile WAL buffer '%s' is not on persistent memory",
			 fname);

	/*
	 * Assert page boundary alignment (8KiB as default).  It should pass because
	 * PMDK considers hugepage boundary alignment (2MiB or 1GiB on x64).
	 */
	Assert((uint64) addr % XLOG_BLCKSZ == 0);

	elog(LOG, "non-volatile WAL buffer '%s' is mapped on [%p-%p)",
		 fname, addr, (char *) addr + map_len);
	return addr;
}

void
UnmapNonVolatileXLogBuffer(void *addr, Size fsize)
{
	Assert(addr != NULL);

	if (pmem_unmap(addr, fsize) < 0)
	{
		elog(WARNING, "could not unmap non-volatile WAL buffer: %m");
		return;
	}

	elog(LOG, "non-volatile WAL buffer unmapped");
}

#endif
