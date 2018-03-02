/*-------------------------------------------------------------------------
 *
 * pmem.c
 *	  Virtual file descriptor code.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/pmem.c
 *
 * NOTES:
 *
 * This code manages an memory-mapped file on a filesystem mounted with DAX on
 * persistent memory device using the Persistent Memory Development Kit
 * (http://pmem.io/pmdk/).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/pmem.h"
#include "storage/fd.h"

#ifdef USE_LIBPMEM
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <libpmem.h>
#include <sys/mman.h>
#include <string.h>

#define PmemFileSize 32

/*
 * This function returns true, only if the file is stored on persistent memory.
 */
bool
CheckPmem(const char *path)
{
	int    is_pmem = 0; /* false */
	size_t mapped_len = 0;
	bool   ret = true;
	void   *tmpaddr;

	/*
	 * The value of is_pmem is 0, if the file(path) isn't stored on
	 * persistent memory.
	 */
	tmpaddr = pmem_map_file(path, PmemFileSize, PMEM_FILE_CREATE,
			PG_FILE_MODE_DEFAULT, &mapped_len, &is_pmem);

	if (tmpaddr)
	{
		pmem_unmap(tmpaddr, mapped_len);
		unlink(path);
	}

	if (is_pmem)
		elog(LOG, "%s is stored on persistent memory.", path);
	else
		ret = false;

	return ret;
}

int
PmemFileOpen(const char *pathname, int flags, size_t fsize, void **addr)
{
	return PmemFileOpenPerm(pathname, flags, PG_FILE_MODE_DEFAULT, fsize, addr);
}

int
PmemFileOpenPerm(const char *pathname, int flags, int mode, size_t fsize,
		void **addr)
{
	int mapped_flag = 0;
	size_t mapped_len = 0, size = 0;
	void *ret_addr;

	if (addr == NULL)
		return BasicOpenFile(pathname, flags);

	/* non-zero 'len' not allowed without PMEM_FILE_CREATE */
	if (flags & O_CREAT)
	{
		mapped_flag = PMEM_FILE_CREATE;
		size = fsize;
	}

	if (flags & O_EXCL)
		mapped_flag |= PMEM_FILE_EXCL;

	ret_addr = pmem_map_file(pathname, size, mapped_flag, mode, &mapped_len,
			NULL);

	if (fsize != mapped_len)
	{
		if (ret_addr != NULL)
			pmem_unmap(ret_addr, mapped_len);

		return -1;
	}

	if (mapped_flag & PMEM_FILE_CREATE)
		if (msync(ret_addr, mapped_len, MS_SYNC))
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not msync log file %s: %m", pathname)));

	*addr = ret_addr;

	return NO_FD_FOR_MAPPED_FILE;
}

void
PmemFileWrite(void *dest, void *src, size_t len)
{
	pmem_memcpy_nodrain((void *)dest, src, len);
}

void
PmemFileRead(void *map_addr, void *buf, size_t len)
{
	memcpy(buf, (void *)map_addr, len);
}

void
PmemFileSync(void)
{
	return pmem_drain();
}

int
PmemFileClose(void *addr, size_t fsize)
{
	return pmem_unmap((void *)addr, fsize);
}


#else
bool
CheckPmem(const char *path)
{
	return true;
}

int
PmemFileOpen(const char *pathname, int flags, size_t fsize, void **addr)
{
	return BasicOpenFile(pathname, flags);
}

int
PmemFileOpenPerm(const char *pathname, int flags, int mode, size_t fsize,
		void **addr)
{
	return BasicOpenFilePerm(pathname, flags, mode);
}

void
PmemFileWrite(void *dest, void *src, size_t len)
{
	ereport(PANIC, (errmsg("don't have the pmem device")));
}

void
PmemFileRead(void *map_addr, void *buf, size_t len)
{
	ereport(PANIC, (errmsg("don't have the pmem device")));
}

void
PmemFileSync(void)
{
	ereport(PANIC, (errmsg("don't have the pmem device")));
}

int
PmemFileClose(void *addr, size_t fsize)
{
	ereport(PANIC, (errmsg("don't have the pmem device")));
	return -1;
}
#endif

