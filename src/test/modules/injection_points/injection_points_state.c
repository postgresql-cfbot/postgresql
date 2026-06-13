/*--------------------------------------------------------------------------
 *
 * injection_points_state.c
 *		Standalone client for the injection point shared state.
 *
 * This small program maps the injection point state files from a data
 * directory and lets a test harness drive injection points without going
 * through a backend connection or any SQL.  It can:
 *
 *	  - attach/detach a "wait" injection point by writing the core registry file
 *		(injection_points.shm, see src/backend/utils/misc/injection_point.c),
 *	  - detect that a process has reached a wait point and release it by
 *		writing this module's wait file (injection_points_wait.shm, see
 *		injection_points.c).
 *
 * That is useful when the cooperating process has no PGPROC or no wait-event
 * visibility (for example the postmaster, or early startup before the SQL
 * machinery is up), where SQL-driven attach/wakeup is not an option and a
 * fixed sleep would be unreliable.
 *
 * Both files are mapped exactly like the backend does -- POSIX mmap() or, on
 * Windows, a file-backed CreateFileMapping() -- because plain file reads are
 * not guaranteed to be coherent with a mapped view on Windows.
 *
 * NB: this is prototype/test tooling.  attach/detach update the registry
 * without holding the backend's InjectionPointLock, so they assume no
 * concurrent SQL attach/detach of the same array.  That holds for the
 * intended use (attaching points out of band, before or alongside controlled
 * test sessions).
 *
 * TODO: if this outgrows test tooling, the registry should get a real
 * publication protocol that is safe against concurrent writers - an
 * out-of-process equivalent of InjectionPointLock, or a CAS-based claim on
 * the generation counter - instead of this single-writer assumption.
 *
 * Usage:
 *	  injection_points_state DATADIR attach NAME
 *	  injection_points_state DATADIR detach NAME
 *	  injection_points_state DATADIR wait   NAME [TIMEOUT_SEC]
 *	  injection_points_state DATADIR wakeup NAME [TIMEOUT_SEC]
 *
 * Exit status: 0 success, 1 usage/IO error, 2 timeout.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points_state.c
 *
 * -------------------------------------------------------------------------
 */

#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef WIN32
#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#else
#include <windows.h>
#endif

#include "injection_points.h"

#define INJ_DEFAULT_TIMEOUT_SEC		180
#define INJ_POLL_INTERVAL_MS		10

/*
 * Mirror of the core active injection points array (InjectionPointsCtl /
 * InjectionPointEntry in src/backend/utils/misc/injection_point.c).
 *
 * The backend stores that array in INJ_POINTS_FILE using pg_atomic_uint{32,64}
 * for "max_inuse" and "generation".  Those atomic types are layout-compatible
 * with the plain integers used here (the backend static-asserts the widths),
 * so we can read and write the same bytes to attach and detach points.  The
 * field sizes below must match the backend's INJ_*_MAXLEN /
 * MAX_INJECTION_POINTS.
 *
 * "generation" must be alignas(8): the backend's pg_atomic_uint64 forces
 * 8-byte alignment, so the entries array starts 8 bytes into the control
 * struct.  On an ILP32 platform a plain uint64_t is only 4-byte aligned, which
 * would push entries to offset 4 and make us read and write the wrong slots.
 * The _Static_assert below pins the offset so any future drift fails the build
 * instead of hanging a test.
 */
#define INJ_POINTS_FILE			"injection_points.shm"
#define INJ_REG_MAXPOINTS		128
#define INJ_LIB_MAXLEN			128
#define INJ_FUNC_MAXLEN			128
#define INJ_PRIVATE_MAXLEN		1024

typedef struct InjectionRegEntry
{
	alignas(8) uint64_t generation; /* even: free, odd: in use */
	char		name[INJ_NAME_MAXLEN];
	char		library[INJ_LIB_MAXLEN];
	char		function[INJ_FUNC_MAXLEN];
	char		private_data[INJ_PRIVATE_MAXLEN];
} InjectionRegEntry;

typedef struct InjectionRegCtl
{
	uint32_t	max_inuse;
	InjectionRegEntry entries[INJ_REG_MAXPOINTS];
} InjectionRegCtl;

_Static_assert(offsetof(InjectionRegCtl, entries) == 8,
			   "registry entries must start at offset 8 to match the backend");

static const char *progname = "injection_points_state";

static void
usage(void)
{
	fprintf(stderr,
			"usage: %s DATADIR {attach|detach|wait|wakeup} NAME [TIMEOUT_SEC]\n",
			progname);
}

/* Sleep for the given number of milliseconds. */
static void
sleep_ms(int ms)
{
#ifndef WIN32
	struct timespec ts;

	ts.tv_sec = ms / 1000;
	ts.tv_nsec = (long) (ms % 1000) * 1000000L;
	nanosleep(&ts, NULL);
#else
	Sleep(ms);
#endif
}

/* Atomically bump a 32-bit counter shared with the backend. */
static void
atomic_inc_u32(volatile uint32_t *counter)
{
#if defined(_MSC_VER)
	_InterlockedIncrement((volatile long *) counter);
#else
	__atomic_add_fetch(counter, 1, __ATOMIC_SEQ_CST);
#endif
}

/* Loads/stores matching the backend's generation protocol. */
static uint32_t
load_u32(volatile uint32_t *p)
{
#if defined(_MSC_VER)
	return (uint32_t) _InterlockedOr((volatile long *) p, 0);
#else
	return __atomic_load_n(p, __ATOMIC_ACQUIRE);
#endif
}

static void
store_u32(volatile uint32_t *p, uint32_t v)
{
#if defined(_MSC_VER)
	_InterlockedExchange((volatile long *) p, (long) v);
#else
	__atomic_store_n(p, v, __ATOMIC_RELEASE);
#endif
}

static uint64_t
load_u64(volatile uint64_t *p)
{
#if defined(_MSC_VER)
	return (uint64_t) _InterlockedOr64((volatile __int64 *) p, 0);
#else
	return __atomic_load_n(p, __ATOMIC_ACQUIRE);
#endif
}

/* Publish "generation" after the other fields are in place (release store). */
static void
store_u64_release(volatile uint64_t *p, uint64_t v)
{
#if defined(_MSC_VER)
	_InterlockedExchange64((volatile __int64 *) p, (__int64) v);
#else
	__atomic_store_n(p, v, __ATOMIC_RELEASE);
#endif
}

/*
 * Map a file of the given size read-write, the same way the backend does.
 * Returns the mapped base, or NULL on failure (with a message on stderr).
 */
static void *
map_file(const char *path, size_t size)
{
#ifndef WIN32
	int			fd;
	void	   *base;

	fd = open(path, O_RDWR, 0);
	if (fd < 0)
	{
		fprintf(stderr, "%s: could not open \"%s\": %s\n",
				progname, path, strerror(errno));
		return NULL;
	}

	base = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	if (base == MAP_FAILED)
	{
		fprintf(stderr, "%s: could not map \"%s\": %s\n",
				progname, path, strerror(errno));
		return NULL;
	}
	return base;
#else
	HANDLE		hfile;
	HANDLE		hmap;
	void	   *base;

	hfile = CreateFile(path, GENERIC_READ | GENERIC_WRITE,
					   FILE_SHARE_READ | FILE_SHARE_WRITE,
					   NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (hfile == INVALID_HANDLE_VALUE)
	{
		fprintf(stderr, "%s: could not open \"%s\": error code %lu\n",
				progname, path, GetLastError());
		return NULL;
	}

	hmap = CreateFileMapping(hfile, NULL, PAGE_READWRITE, 0,
							 (DWORD) size, NULL);
	if (hmap == NULL)
	{
		fprintf(stderr, "%s: could not create mapping for \"%s\": error code %lu\n",
				progname, path, GetLastError());
		CloseHandle(hfile);
		return NULL;
	}

	base = MapViewOfFile(hmap, FILE_MAP_ALL_ACCESS, 0, 0, size);
	CloseHandle(hmap);
	CloseHandle(hfile);

	if (base == NULL)
	{
		fprintf(stderr, "%s: could not map \"%s\": error code %lu\n",
				progname, path, GetLastError());
		return NULL;
	}
	return base;
#endif
}

/*
 * Attach a "wait" injection point by adding it to the registry, mimicking
 * InjectionPointAttach(): find a free slot, fill the entry, then publish it by
 * flipping the generation to odd with a release store.  The point fires the
 * module's injection_wait() callback (private_data left zeroed, i.e. an
 * unconditional INJ_CONDITION_ALWAYS condition).
 */
static int
do_attach(InjectionRegCtl *ctl, const char *name)
{
	uint32_t	max_inuse = load_u32(&ctl->max_inuse);
	int			free_idx = -1;
	InjectionRegEntry *e;
	uint64_t	generation;

	for (uint32_t i = 0; i < max_inuse; i++)
	{
		uint64_t	g = load_u64(&ctl->entries[i].generation);

		if (g % 2 == 0)
		{
			if (free_idx < 0)
				free_idx = (int) i;
		}
		else if (strncmp(ctl->entries[i].name, name, INJ_NAME_MAXLEN) == 0)
		{
			fprintf(stderr, "%s: injection point \"%s\" already attached\n",
					progname, name);
			return 1;
		}
	}

	if (free_idx < 0)
	{
		if (max_inuse >= INJ_REG_MAXPOINTS)
		{
			fprintf(stderr, "%s: too many injection points\n", progname);
			return 1;
		}
		free_idx = (int) max_inuse;
	}

	e = &ctl->entries[free_idx];
	generation = load_u64(&e->generation);	/* even (free) */

	memset(e->name, 0, INJ_NAME_MAXLEN);
	strncpy(e->name, name, INJ_NAME_MAXLEN - 1);
	memset(e->library, 0, INJ_LIB_MAXLEN);
	strncpy(e->library, "injection_points", INJ_LIB_MAXLEN - 1);
	memset(e->function, 0, INJ_FUNC_MAXLEN);
	strncpy(e->function, "injection_wait", INJ_FUNC_MAXLEN - 1);
	memset(e->private_data, 0, INJ_PRIVATE_MAXLEN);

	store_u64_release(&e->generation, generation + 1);	/* publish (odd) */

	if ((uint32_t) (free_idx + 1) > max_inuse)
		store_u32(&ctl->max_inuse, (uint32_t) (free_idx + 1));

	return 0;
}

/* Detach an injection point by flipping its generation back to even. */
static int
do_detach(InjectionRegCtl *ctl, const char *name)
{
	uint32_t	max_inuse = load_u32(&ctl->max_inuse);

	for (uint32_t i = 0; i < max_inuse; i++)
	{
		uint64_t	g = load_u64(&ctl->entries[i].generation);

		if (g % 2 == 0)
			continue;
		if (strncmp(ctl->entries[i].name, name, INJ_NAME_MAXLEN) == 0)
		{
			store_u64_release(&ctl->entries[i].generation, g + 1);
			return 0;
		}
	}

	fprintf(stderr, "%s: injection point \"%s\" not attached\n",
			progname, name);
	return 1;
}

/*
 * Return the wait slot currently registered for "name", or -1 if none.
 *
 * The backend writes the name under a spinlock before it starts waiting, so a
 * stable match means the wait point has been reached.  A torn read simply
 * fails to match and the caller retries.
 */
static int
find_wait_slot(InjectionPointPublicState *state, const char *name)
{
	for (int i = 0; i < INJ_MAX_WAIT; i++)
	{
		if (strncmp(state->name[i], name, INJ_NAME_MAXLEN) == 0)
			return i;
	}
	return -1;
}

/*
 * Poll the wait file until "name" appears (a process has reached the point),
 * up to timeout_sec.  Returns the slot, or -1 on timeout.
 */
static int
wait_for_slot(InjectionPointPublicState *state, const char *name,
			  int timeout_sec)
{
	int			max_polls = (timeout_sec * 1000) / INJ_POLL_INTERVAL_MS;

	for (int polls = 0;; polls++)
	{
		int			slot = find_wait_slot(state, name);

		if (slot >= 0)
			return slot;
		if (polls >= max_polls)
			return -1;
		sleep_ms(INJ_POLL_INTERVAL_MS);
	}
}

int
main(int argc, char **argv)
{
	const char *datadir;
	const char *mode;
	const char *name;
	int			timeout_sec = INJ_DEFAULT_TIMEOUT_SEC;
	char		path[1024];

	if (argc < 4 || argc > 5)
	{
		usage();
		return 1;
	}

	datadir = argv[1];
	mode = argv[2];
	name = argv[3];
	if (argc == 5)
		timeout_sec = atoi(argv[4]);

	if (strlen(name) >= INJ_NAME_MAXLEN)
	{
		fprintf(stderr, "%s: injection point name too long\n", progname);
		return 1;
	}

	/* attach/detach operate on the core registry file. */
	if (strcmp(mode, "attach") == 0 || strcmp(mode, "detach") == 0)
	{
		InjectionRegCtl *ctl;

		snprintf(path, sizeof(path), "%s/%s", datadir, INJ_POINTS_FILE);
		ctl = (InjectionRegCtl *) map_file(path, sizeof(InjectionRegCtl));
		if (ctl == NULL)
			return 1;

		if (strcmp(mode, "attach") == 0)
			return do_attach(ctl, name);
		else
			return do_detach(ctl, name);
	}

	/* wait/wakeup operate on this module's wait file. */
	if (strcmp(mode, "wait") == 0 || strcmp(mode, "wakeup") == 0)
	{
		InjectionPointPublicState *state;
		int			slot;

		snprintf(path, sizeof(path), "%s/%s", datadir, INJ_STATE_FILE);
		state = (InjectionPointPublicState *) map_file(path,
													   sizeof(InjectionPointPublicState));
		if (state == NULL)
			return 1;

		slot = wait_for_slot(state, name, timeout_sec);
		if (slot < 0)
		{
			fprintf(stderr, "%s: timed out waiting for injection point \"%s\"\n",
					progname, name);
			return 2;
		}

		if (strcmp(mode, "wakeup") == 0)
			atomic_inc_u32(&state->wait_counts[slot]);

		return 0;
	}

	usage();
	return 1;
}
