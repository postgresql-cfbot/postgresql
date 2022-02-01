#include "postgres.h"

#ifdef USE_LIBPMEM

#include <errno.h>
#include <limits.h>		/* INT_MAX */
#include <stddef.h>		/* size_t */
#include <stdint.h>		/* uintptr_t */
#include <unistd.h>		/* getpid, unlink */

/*
 * On Windows, we will have two ported but conflicting mode_t:
 *
 * mode_t in libpmem:
 *     libpmem.h -> pmemcompat.h -> typedef int mode_t
 * mode_t in PostgreSQL:
 *     c.h -> port.h -> win32_port.h -> typedef unsigned short mode_t
 *
 * We want to use PostgreSQL's one, so conseal libpmem's one.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
#define mode_t unused_libpmem_mode_t
#include <libpmem.h>
#undef mode_t
/* On other platforms, simply include libpmem.h */
#else
#include <libpmem.h>
#endif

#include "c.h"						/* bool, Size */
#include "access/xlog.h"
#include "access/xlog_internal.h"	/* XLogFilePath, XLByteToSeg */
#include "access/xlogpmem.h"
#include "common/file_perm.h"		/* pg_file_create_mode */
#include "miscadmin.h"				/* enableFsync */
#include "pgstat.h"

static char *mappedPages = NULL;
static XLogSegNo mappedSegNo = 0;

#define PG_DAX_HUGEPAGE_SIZE (((uintptr_t) 1) << 21)
#define PG_DAX_HUGEPAGE_MASK (~(PG_DAX_HUGEPAGE_SIZE - 1))

static XLogSegNo PmemXLogMap(XLogSegNo segno, TimeLineID tli);
static void PmemXLogCreate(XLogSegNo segno, TimeLineID tli);
static void PmemXLogUnmap(void);

static void *PmemCreateMapFile(const char *path, size_t len);
static void *PmemOpenMapFile(const char *path, size_t expected_len);
static void *PmemTryOpenMapFile(const char *path, size_t expected_len);
static void *PmemMapFile(const char *path, size_t expected_len, int flags,
						 bool try_open);
static void PmemUnmapForError(void *addr, size_t len);

/*
 * Ensures the WAL segment containg {ptr-1} to be mapped.
 *
 * Returns mapped XLogSegNo.
 */
XLogSegNo
PmemXLogEnsurePrevMapped(XLogRecPtr ptr, TimeLineID tli)
{
	XLogSegNo	segno;

	Assert(wal_pmem_map);

	XLByteToPrevSeg(ptr, segno, wal_segment_size);

	if (mappedPages != NULL)
	{
		/* Fast return: The segment we need is already mapped */
		if (mappedSegNo == segno)
			return mappedSegNo;

		/* Unmap the current segment we don't need */
		PmemXLogUnmap();
	}

	return PmemXLogMap(segno, tli);
}

/*
 * Creates a new XLOG file segment, or open a pre-existing one, for WAL buffers.
 *
 * Returns mapped XLogSegNo.
 *
 * See also XLogFileInit in xlog.c.
 */
static XLogSegNo
PmemXLogMap(XLogSegNo segno, TimeLineID tli)
{
	char		path[MAXPGPATH];

	Assert(mappedPages == NULL);

	XLogFilePath(path, tli, segno, wal_segment_size);

	/* PmemTryOpenMapFile will handle error except ENOENT */
	mappedPages = PmemTryOpenMapFile(path, wal_segment_size);

	/* Fast return if already exists */
	if (mappedPages != NULL)
	{
		mappedSegNo = segno;
		return mappedSegNo;
	}

	elog(DEBUG2, "creating and filling new WAL file");
	PmemXLogCreate(segno, tli);

	/* PmemCreateMapFile will handle error */
	mappedPages = PmemOpenMapFile(path, wal_segment_size);
	mappedSegNo = segno;

	elog(DEBUG2, "done creating and filling new WAL file");
	return mappedSegNo;
}

/*
 * Creates a new XLOG file segment.
 *
 * See also XLogFileInit in xlog.c.
 */
static void
PmemXLogCreate(XLogSegNo segno, TimeLineID tli)
{
	char	   *addr;
	char		tmppath[MAXPGPATH];
	XLogSegNo	inst_segno;
	XLogSegNo	max_segno;

	snprintf(tmppath, MAXPGPATH, XLOGDIR "/xlogtemp.%d", (int) getpid());
	unlink(tmppath);

	/* PmemCreateMapFile will handle error */
	addr = PmemCreateMapFile(tmppath, wal_segment_size);

	/*
	 * Initialize whole the buffers.
	 *
	 * Note that we don't put any single byte if not wal_init_zero. It's okay
	 * because we already have a new segment file truncated to the proper size.
	 */
	pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_WRITE);
	if (wal_init_zero)
		pmem_memset_nodrain(addr, 0, wal_segment_size);
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_SYNC);
	if (enableFsync)
		pmem_drain();
	pgstat_report_wait_end();

	if (pmem_unmap(addr, wal_segment_size) < 0)
		elog(ERROR, "could not pmem_unmap temporal WAL buffers: %m");

	inst_segno = segno;
	max_segno = segno + CheckPointSegments;
	if (!InstallXLogFileSegment(&inst_segno, tmppath, true, max_segno, tli))
		unlink(tmppath);
}

/*
 * Unmaps the current WAL segment file if mapped.
 */
static void
PmemXLogUnmap(void)
{
	/* Fast return if not mapped */
	if (mappedPages == NULL)
		return;

	if (pmem_unmap(mappedPages, wal_segment_size) < 0)
		elog(ERROR, "could not pmem_unmap WAL buffers: %m");

	mappedPages = NULL;
}

/*
 * Gets the head address of the WAL buffers.
 */
char *
PmemXLogGetBufferPages(void)
{
	Assert(wal_pmem_map);
	Assert(mappedPages != NULL);

	return mappedPages;
}

/*
 * Flushes records in the given range [start, end) within a single segment.
 */
void
PmemXLogFlush(XLogRecPtr start, XLogRecPtr end)
{
	Size		off;
	instr_time	start_time;

	Assert(wal_pmem_map);
	Assert(start < end);
	Assert(mappedPages != NULL);
	Assert(XLByteInSeg(start, mappedSegNo, wal_segment_size));
	Assert(XLByteInPrevSeg(end, mappedSegNo, wal_segment_size));

	off = XLogSegmentOffset(start, wal_segment_size);

	/* Measure I/O timing to write WAL data */
	if (track_wal_io_timing)
		INSTR_TIME_SET_CURRENT(start_time);

	pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
	pmem_flush(mappedPages + off, end - start);
	pgstat_report_wait_end();

	/*
	 * Increment the I/O timing and the number of times WAL data
	 * were written out to disk.
	 */
	if (track_wal_io_timing)
	{
		instr_time	duration;

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start_time);
		WalStats.m_wal_write_time += INSTR_TIME_GET_MICROSEC(duration);
	}

	WalStats.m_wal_write++;
}

/*
 * Wait for cache-flush to finish.
 *
 * See also issue_xlog_fsync in xlog.c.
 */
void
PmemXLogSync(void)
{
	instr_time	start;

	Assert(wal_pmem_map);

	/* Fast return */
	if (!enableFsync)
		return;

	/* Measure I/O timing to sync the WAL file */
	if (track_wal_io_timing)
		INSTR_TIME_SET_CURRENT(start);

	pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
	pmem_drain();
	pgstat_report_wait_end();

	/*
	 * Increment the I/O timing and the number of times WAL files were synced.
	 */
	if (track_wal_io_timing)
	{
		instr_time	duration;

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);
		WalStats.m_wal_sync_time += INSTR_TIME_GET_MICROSEC(duration);
	}

	WalStats.m_wal_sync++;
}

/*
 * Wrappers for pmem_map_file.
 */
static void *
PmemCreateMapFile(const char *path, size_t len)
{
	return PmemMapFile(path, len, PMEM_FILE_CREATE | PMEM_FILE_EXCL, false);
}

static void *
PmemOpenMapFile(const char *path, size_t expected_len)
{
	return PmemMapFile(path, expected_len, 0, false);
}

static void *
PmemTryOpenMapFile(const char *path, size_t expected_len)
{
	return PmemMapFile(path, expected_len, 0, true);
}

static void *
PmemMapFile(const char *path, size_t expected_len, int flags, bool try_open)
{
	size_t		param_len;
	int			mode;
	size_t		mapped_len;
	int			is_pmem;
	void	   *addr;

	Assert(expected_len > 0);
	Assert(expected_len <= INT_MAX);

	param_len = (flags & PMEM_FILE_CREATE) ? expected_len : 0;
	mode = (flags & PMEM_FILE_CREATE) ? pg_file_create_mode : 0;

	mapped_len = 0;
	is_pmem = 0;
#if defined(WIN32) && !defined(__CYGWIN__)
	addr = pmem_map_fileU(path, param_len, flags, mode, &mapped_len, &is_pmem);
#else
	addr = pmem_map_file(path, param_len, flags, mode, &mapped_len, &is_pmem);
#endif

	if (addr == NULL)
	{
		if (try_open && errno == ENOENT)
			return NULL;

		ereport(ERROR,
				errcode_for_file_access(),
				errmsg("could not pmem_map_file \"%s\": %m", path));
	}

	if (mapped_len > INT_MAX)
	{
		PmemUnmapForError(addr, mapped_len);
		elog(ERROR,
			 "unexpected file size: path \"%s\" actual (greater than %d) expected %d",
			 path, INT_MAX, (int) expected_len);
	}

	if (mapped_len != expected_len)
	{
		PmemUnmapForError(addr, mapped_len);
		elog(ERROR,
			 "unexpected file size: path \"%s\" actual %d expected %d",
			 path, (int) mapped_len, (int) expected_len);
	}

	if (!is_pmem)
	{
		PmemUnmapForError(addr, mapped_len);
		elog(ERROR, "file not on PMEM: path \"%s\"", path);
	}

	if ((uintptr_t) addr & ~PG_DAX_HUGEPAGE_MASK)
		elog(WARNING,
			 "file not mapped on DAX hugepage boundary: path \"%s\" addr %p",
			 path, addr);

	return addr;
}

static void
PmemUnmapForError(void *addr, size_t len)
{
	int		saved_errno;

	saved_errno = errno;
	(void) pmem_unmap(addr, len);
	errno = saved_errno;
}

#endif /* USE_LIBPMEM */
