/*-------------------------------------------------------------------------
 *
 * ipc.h
 *	  POSTGRES inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.  The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_H
#define IPC_H

typedef void (*pg_on_exit_callback) (int code, Datum arg);
typedef void (*shmem_startup_hook_type) (void);

/*----------
 * API for handling cleanup that must occur during either ereport(ERROR)
 * or ereport(FATAL) exits from a block of code.  (Typical examples are
 * undoing transient changes to shared-memory state.)
 *
 *		PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *		{
 *			... code that might throw ereport(ERROR) or ereport(FATAL) ...
 *		}
 *		PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *
 * where the cleanup code is in a function declared per pg_on_exit_callback.
 * The Datum value "arg" can carry any information the cleanup function
 * needs.
 *
 * This construct ensures that cleanup_function() will be called during
 * either ERROR or FATAL exits.  It will not be called on successful
 * exit from the controlled code.  (If you want it to happen then too,
 * call the function yourself from just after the construct.)
 *
 * Note: the macro arguments are multiply evaluated, so avoid side-effects.
 *----------
 */
#define PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
	do { \
		before_shmem_exit(cleanup_function, arg); \
		PG_TRY()

#define PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
		cancel_before_shmem_exit(cleanup_function, arg); \
		PG_CATCH(); \
		{ \
			cancel_before_shmem_exit(cleanup_function, arg); \
			cleanup_function (0, arg); \
			PG_RE_THROW(); \
		} \
		PG_END_TRY(); \
	} while (0)


/* ipc.c */
extern PGDLLIMPORT bool proc_exit_inprogress;
extern PGDLLIMPORT bool shmem_exit_inprogress;
extern int AcquireNBuffersLock(void);
extern void ReleaseNBuffersLock(void);

/*----------
 * BEGIN_NBUFFERS_ACCESS / END_NBUFFERS_ACCESS
 *
 * The lock is released at scope exit via __attribute__((cleanup)), so:
 *	- early `return`, `break`, or `goto` between BEGIN and END does NOT leak.
 *	- END_NBUFFERS_ACCESS(name) is idempotent: it releases the lock and sets
 *	  a sentinel so the cleanup at scope exit skips a second release. Use it
 *	  when you want to drop the lock before the enclosing block ends.
 *	- ereport(ERROR) bypasses the cleanup attribute, but LWLockReleaseAll()
 *	  during transaction abort still releases AccessNBuffersLock, so no leak.
 *
 * Usage:
 *		BEGIN_NBUFFERS_ACCESS(localNBuffers);
 *		for (int i = 0; i < localNBuffers; i++)
 *			... use buffer i ...
 *		END_NBUFFERS_ACCESS(localNBuffers);
 *
 *----------
 */
static inline void
nbuffers_lock_auto_release(const bool *released)
{
	if (!*released)
		ReleaseNBuffersLock();
}

#define BEGIN_NBUFFERS_ACCESS(name) \
	bool name##_released __attribute__((cleanup(nbuffers_lock_auto_release))) = false; \
	int name = AcquireNBuffersLock()

#define END_NBUFFERS_ACCESS(name) \
	do { \
		if (!name##_released) \
		{ \
			ReleaseNBuffersLock(); \
			name##_released = true; \
		} \
	} while (0)

pg_noreturn extern void proc_exit(int code);
extern void shmem_exit(int code);
extern void on_proc_exit(pg_on_exit_callback function, Datum arg);
extern void on_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void on_exit_reset(void);
extern void check_on_shmem_exit_lists_are_empty(void);

/* ipci.c */
extern PGDLLIMPORT shmem_startup_hook_type shmem_startup_hook;

extern void RegisterBuiltinShmemCallbacks(void);
extern Size CalculateShmemSize(void);
extern void CreateSharedMemoryAndSemaphores(void);
#ifdef EXEC_BACKEND
extern void AttachSharedMemoryStructs(void);
#endif
extern void InitializeShmemGUCs(void);

#endif							/* IPC_H */
