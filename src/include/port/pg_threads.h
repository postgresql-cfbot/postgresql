/*-------------------------------------------------------------------------
 *
 * pg_threads.h
 *    Portable multi-threading API.
 *
 * A multi-threading API abstraction based on a subset of C11 <threads.h>,
 * with some extensions.  The identifiers have a pg_ prefix but otherwise
 * follow C11 naming.
 *
 * Extensions not present in C11:
 *
 * - pg_mtx_t has initialization value PG_MTX_INIT
 * - pg_rwlock_t for read/write locks
 * - pg_barrier_t for synchronization (like pthread_barrier_t)
 * - pg_thrd_atexit_t
 *
 * On POSIX systems, clients can assume that this includes <pthread.h>, that
 * pthread_t is the same type as pg_thrd_t (mirroring POSIX's requirement that
 * thrd_t is pthread_t), and that pthread_sigmask() is available.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/port/pg_threads.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_THREADS_H
#define PG_THREADS_H

#if defined(WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif


/*-------------------------------------------------------------------------
 *
 * Thread-local storage.
 *
 *-------------------------------------------------------------------------
 */

/*
 * In C23 and C++11, thread_local is a storage class keyword so we don't need
 * to do anything to use it.
 *
 * In C11, _Thread_local is the keyword and <thread.h> is supposed to define
 * thread_local as a macro.  Do the same here if necessary, since some C11
 * systems don't have that header yet.
 */
#if !(defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L) && !defined(__cplusplus)
#ifndef thread_local
#define thread_local _Thread_local
#endif
#endif


/*-------------------------------------------------------------------------
 *
 * Return values.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 thrd_error_t. */
typedef enum pg_thrd_error_t
{
	pg_thrd_success = 0,
	pg_thrd_nomem = 1,
	pg_thrd_timedout = 2,
	pg_thrd_busy = 3,
	pg_thrd_error = 4,
} pg_thrd_error_t;

/* Convert native error to pg_thrd_error_t. */
static inline int
pg_thrd_maperror(int error)
{
#ifdef WIN32
	return error ? pg_thrd_success : pg_thrd_error;
#else
	return error == 0 ? pg_thrd_success : pg_thrd_error;
#endif
}


/*-------------------------------------------------------------------------
 *
 * Threads.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 thrd_t.  Uses native thread handle directly. */
#ifdef WIN32
typedef HANDLE pg_thrd_t;
#else
typedef pthread_t pg_thrd_t;
#endif

/* Like C11 thrd_start_t. */
typedef int (*pg_thrd_start_t) (void *);

/* Like C11 thrd_create(). */
extern int	pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument);

/* Like C11 thrd_join(). */
extern int	pg_thrd_join(pg_thrd_t thread, int *result);

#ifdef WIN32
/* Helper for Windows, whose threads don't know their own handle. */
extern pg_thrd_t pg_thrd_current_win32(void);
#endif

/* Like C11 thrd_current(). */
static inline pg_thrd_t
pg_thrd_current(void)
{
#ifdef WIN32
	return pg_thrd_current_win32();
#else
	return pthread_self();
#endif
}

/* Like C11 thrd_equal().  Return value zero means not equal. */
static inline int
pg_thrd_equal(pg_thrd_t lhs, pg_thrd_t rhs)
{
#ifdef WIN32
	return lhs == rhs;
#else
	return pthread_equal(lhs, rhs);
#endif
}

/* Like C11 thrd_exit(). */
static inline void
pg_thrd_exit(int result)
{
#ifdef WIN32
	ExitThread((DWORD) result);
#else
	pthread_exit((void *) (intptr_t) result);
#endif
}


/*-------------------------------------------------------------------------
 *
 * User-supplied callback on thread exit.  Not in C11.
 *
 * This is built on top of OS-supplied "explicit" thread local storage APIs,
 * and works even for threads not created by pg_threads.h.
 *
 * XXX We could also take a user data pointer, but we'd have to allocate a
 * small object to hold the function pointer + data.  Not currently needed.
 *
 * XXX We could provide a more general tss_t API with per-thread-local-slot
 * destructor support if there is a real need, but language-supported
 * "thread_local" is generally much easier to use, and this narrow abstraction
 * avoids some portability complications.
 *
 *-------------------------------------------------------------------------
 */

/* A thread-exit callback ID. */
#ifdef WIN32
typedef DWORD pg_thrd_atexit_t;
#else
typedef pthread_key_t pg_thrd_atexit_t;
#endif

/* The type of user-supplied thread-exit functions. */
typedef void (*pg_thrd_atexit_function_t) (void);

/* Windows' FLS callback needs non-default calling convention. */
#ifdef WIN32
#define PG_THRD_ATEXIT_INVOKE_CC CALLBACK
#else
#define PG_THRD_ATEXIT_INVOKE_CC
#endif

/* Function called by OS on thread exit with per-thread user function as value. */
extern void PG_THRD_ATEXIT_INVOKE_CC pg_thrd_atexit_invoke(void *value);

/*
 * Create a process-wide thread-exit callback ID.
 *
 * XXX In theory we might also want pg_thrd_atexit_destroy(), but we'd have to
 * come up with a way to stop Windows from calling pg_thrd_atexit_invoke()
 * once for each thread, from the calling thread.
 */
static inline pg_thrd_error_t
pg_thrd_atexit_create(pg_thrd_atexit_t *atexit_id)
{
#ifdef WIN32
	if ((*atexit_id = FlsAlloc(pg_thrd_atexit_invoke)) == FLS_OUT_OF_INDEXES)
		return pg_thrd_error;
#else
	if (pthread_key_create(atexit_id, pg_thrd_atexit_invoke) != 0)
		return pg_thrd_error;
#endif
	return pg_thrd_success;
}

/*
 * Set the thread-exit callback function for a given ID in the calling thread,
 * replacing any previously installed callback for the same ID in this thread.
 * The ID must have been created by any thread in the same process with
 * pg_thrd_atexit_create() first.
 */
static inline void
pg_thrd_atexit_enable(pg_thrd_atexit_t atexit_id,
					  pg_thrd_atexit_function_t function)
{
#ifdef WIN32
	FlsSetValue(atexit_id, function);
#else
	pthread_setspecific(atexit_id, (const void *) function);
#endif
}

/*
 * Remove the thread-exit callback function for a given ID in the calling
 * thread.
 */
static inline void
pg_thrd_atexit_disable(pg_thrd_atexit_t atexit_id)
{
	pg_thrd_atexit_enable(atexit_id, NULL);
}


/*-------------------------------------------------------------------------
 *
 * "Once"-style initialization functions.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 once_flag and ONCE_FLAG_INIT. */
#ifdef WIN32
typedef INIT_ONCE pg_once_flag;
#define PG_ONCE_FLAG_INIT INIT_ONCE_STATIC_INIT
#else
typedef pthread_once_t pg_once_flag;
#define PG_ONCE_FLAG_INIT PTHREAD_ONCE_INIT
#endif

/* Like C11 once_function_t. */
typedef void (*pg_call_once_function_t) (void);

#ifdef WIN32
/* Windows helper that deals with function type mismatch. */
extern BOOL CALLBACK pg_call_once_invoke(pg_once_flag *flag,
										 void *parameter,
										 void **context);
#endif

/* Like C11 call_once(). */
static inline void
pg_call_once(pg_once_flag *flag, pg_call_once_function_t function)
{
#ifdef WIN32
	InitOnceExecuteOnce(flag, pg_call_once_invoke, function, NULL);
#else
	pthread_once(flag, function);
#endif
}


/*-------------------------------------------------------------------------
 *
 * Read/write locks.  Not in C11.
 *
 * Unfortunately Windows makes you say whether you're unlocking a read lock or
 * a write lock, so we have to expose that here too.  POSIX already knows.
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
typedef SRWLOCK pg_rwlock_t;
#define PG_RWLOCK_INIT SRWLOCK_INIT
#else
typedef pthread_rwlock_t pg_rwlock_t;
#define PG_RWLOCK_INIT PTHREAD_RWLOCK_INITIALIZER
#endif

static inline int
pg_rwlock_init(pg_rwlock_t *lock)
{
#ifdef WIN32
	InitializeSRWLock(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_init(lock, NULL));
#endif
}

static inline int
pg_rwlock_rlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	AcquireSRWLockShared(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_rdlock(lock));
#endif
}

static inline int
pg_rwlock_wlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	AcquireSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_wrlock(lock));
#endif
}

static inline int
pg_rwlock_wunlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	ReleaseSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_unlock(lock));
#endif
}

static inline int
pg_rwlock_runlock(pg_rwlock_t *lock)
{
#ifdef WIN32
	ReleaseSRWLockShared(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_unlock(lock));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Simple mutexes.
 *
 *-------------------------------------------------------------------------
 */

/*
 * C11 doesn't define a static initializer, but it is very convenient to have
 * one.
 */
#ifdef WIN32
/*
 * CRITICAL_SECTION might be the most obvious Windows mechanism for pg_mtx_t,
 * but SRWLock is reported to be at least as fast when used only in exclusive
 * mode, and has the advantage of a static initializer (CRITICAL_SECTION must
 * be initialized and destroyed explicitly because it allocates kernel
 * resource).  So we'll just point pg_mtx_t to pg_rwlock_t on Windows.  (Visual
 * Studio 2022's <threads.h> takes the same approach.)
 */
typedef pg_rwlock_t pg_mtx_t;
#define PG_MTX_INIT PG_RWLOCK_INIT
#else
typedef pthread_mutex_t pg_mtx_t;
#define PG_MTX_INIT PTHREAD_MUTEX_INITIALIZER
#endif

/* Like C11 mtx_type_t. */
typedef enum pg_mtx_type_t
{
	pg_mtx_plain = 0
} pg_mtx_type_t;

/* Like C11 mtx_init(). */
static inline int
pg_mtx_init(pg_mtx_t *mutex, int type)
{
#ifdef WIN32
	return pg_rwlock_init(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_init(mutex, NULL));
#endif
}

/* Like C11 mtx_lock(). */
static inline int
pg_mtx_lock(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_rwlock_wlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_lock(mutex));
#endif
}

/* Like C11 mtx_unlock(). */
static inline int
pg_mtx_unlock(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_rwlock_wunlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_unlock(mutex));
#endif
}

/* Like C11 mtx_destroy(). */
static inline int
pg_mtx_destroy(pg_mtx_t *mutex)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_mutex_destroy(mutex));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Condition variables.
 *
 *-------------------------------------------------------------------------
 */

/* Like C11 cnd_t. */
#ifdef WIN32
typedef CONDITION_VARIABLE pg_cnd_t;
#else
typedef pthread_cond_t pg_cnd_t;
#endif

/* Like C11 cnd_init(). */
static inline int
pg_cnd_init(pg_cnd_t *condvar)
{
#ifdef WIN32
	InitializeConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_init(condvar, NULL));
#endif
}

/* Like C11 cnd_broadcast(). */
static inline int
pg_cnd_broadcast(pg_cnd_t *condvar)
{
#ifdef WIN32
	WakeAllConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_broadcast(condvar));
#endif
}

/* Like C11 cnd_wait(). */
static inline int
pg_cnd_wait(pg_cnd_t *condvar, pg_mtx_t *mutex)
{
#ifdef WIN32
	SleepConditionVariableSRW(condvar, mutex, INFINITE, 0);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_wait(condvar, mutex));
#endif
}

/* Like C11 cnd_destroy(). */
static inline int
pg_cnd_destroy(pg_cnd_t *condvar)
{
#ifdef WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_destroy(condvar));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Barriers.  Not in C11.
 *
 *-------------------------------------------------------------------------
 */

/*
 * Define USE_PG_BARRIER_FALLBACK manually to test the fallback code.  It is
 * normally only used on macOS.
 */
#if !defined(WIN32) && !defined(HAVE_PTHREAD_BARRIER_WAIT)
#define USE_PG_BARRIER_FALLBACK
#endif

/* A thread synchronization barrier. */
#ifdef USE_PG_BARRIER_FALLBACK
typedef struct pg_barrier_t
{
	bool		sense;
	int			expected;
	int			arrived;
	pg_mtx_t	mutex;
	pg_cnd_t	cond;
} pg_barrier_t;
#elif defined(WIN32)
typedef SYNCHRONIZATION_BARRIER pg_barrier_t;
#else
typedef pthread_barrier_t pg_barrier_t;
#endif

/*
 * Initialize a thread synchronization barrier that waits for 'count' threads
 * when pg_barrier_wait() is called.
 */
static inline int
pg_barrier_init(pg_barrier_t *barrier, int count)
{
#ifdef USE_PG_BARRIER_FALLBACK
	barrier->sense = false;
	barrier->expected = count;
	barrier->arrived = 0;
	if (pg_cnd_init(&barrier->cond) != pg_thrd_success)
		return pg_thrd_error;
	if (pg_mtx_init(&barrier->mutex, pg_mtx_plain) != pg_thrd_success)
	{
		pg_cnd_destroy(&barrier->cond);
		return pg_thrd_error;
	}
	return pg_thrd_success;
#elif defined(WIN32)
	return pg_thrd_maperror(InitializeSynchronizationBarrier(barrier, count, 0));
#else
	return pg_thrd_maperror(pthread_barrier_init(barrier, NULL, count));
#endif
}

/*
 * Wait for all expected threads to arrive at the barrier, and elect one
 * arbitrary thread to perform a phase of computation serially.  Sets
 * *elected_thread to true in the elected thread, and false in all others.
 */
static inline int
pg_barrier_wait_and_elect(pg_barrier_t *barrier, bool *elected_thread)
{
#ifdef USE_PG_BARRIER_FALLBACK
	bool		initial_sense;

	pg_mtx_lock(&barrier->mutex);
	barrier->arrived++;
	if (barrier->arrived == barrier->expected)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pg_mtx_unlock(&barrier->mutex);
		pg_cnd_broadcast(&barrier->cond);
		*elected_thread = true;
		return pg_thrd_success;
	}
	initial_sense = barrier->sense;
	do
	{
		pg_cnd_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);
	pg_mtx_unlock(&barrier->mutex);
	*elected_thread = false;
	return pg_thrd_success;
#elif defined(WIN32)
	if (EnterSynchronizationBarrier(barrier,
									SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY))
		*elected_thread = true;
	else
		*elected_thread = false;
	return pg_thrd_success;
#else
	int			error = pthread_barrier_wait(barrier);

	if (error == 0)
	{
		*elected_thread = false;
		return pg_thrd_success;
	}
	else if (error == PTHREAD_BARRIER_SERIAL_THREAD)
	{
		*elected_thread = true;
		return pg_thrd_success;
	}
	else
	{
		return pg_thrd_error;
	}
#endif
}

/* Wait for all threads to arrive at the barrier. */
static inline int
pg_barrier_wait(pg_barrier_t *barrier)
{
	bool		elected_thread;

	return pg_barrier_wait_and_elect(barrier, &elected_thread);
}

/* Destroy a barrier. */
static inline int
pg_barrier_destroy(pg_barrier_t *barrier)
{
#ifdef USE_PG_BARRIER_FALLBACK
	pg_mtx_destroy(&barrier->mutex);
	pg_cnd_destroy(&barrier->cond);
	return pg_thrd_success;
#elif defined(WIN32)
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_barrier_destroy(barrier));
#endif
}

#endif
