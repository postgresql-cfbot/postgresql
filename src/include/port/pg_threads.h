/*-------------------------------------------------------------------------
 *
 * pg_threads.h
 *    Portable multi-threading API.
 *
 * A multi-threading API abstraction loosely based on a subset C11
 * standard's <threads.h> header.  The identifiers have a pg_ prefix.
 *
 * We have some extensions of our own, not present in C11:
 *
 * - pg_rwlock_t for read/write locks
 * - pg_mtx_t static initializer PG_MTX_STATIC_INIT
 * - pg_barrier_t
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/port/pg_threads.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_THREADS_H
#define PG_THREADS_H

#ifdef WIN32
/*
 * We use the macro PG_THREADS_WIN32 rather than WIN32 directly, because we
 * might want to use the C11 APIs in Visual Studio 2022+ at some point.
 * While using Windows native APIs, need an in-house implementation of TSS
 * destructors, which we also gate separately so that it can be
 * tested/maintained on other OSes too.
 */
#define PG_THREADS_WIN32
#define PG_THREADS_NEED_DESTRUCTOR_TABLE
#endif

/*
 * To test our own destructor mechanism on POSIX systems, for the
 * benefit of developers maintaining it, define this macro.
 */
/* #define PG_THREADS_NEED_DESTRUCTOR_TABLE */

#if defined(PG_THREADS_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif


/*-------------------------------------------------------------------------
 *
 * Thread-local storage class.  This is a C11 language feature, not a
 * library feature.  We don't require C11, but we expect compilers to
 * provide some way to request thread-local storage.  (See also
 * pg_tss_t, which is similar but uses explicit set/get functions and
 * supports destructor function that are called at thread exit.)
 *
 *-------------------------------------------------------------------------
 */

#if defined(_MSC_VER)
/* MSVC */
#define pg_thread_local __declspec(thread)
#elif defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(__SUNPRO_C)
/* GCC, Clang, Intel C, XLC, Solaris Studio */
#define pg_thread_local __thread
#else
#error "no known thread_local storage class for this compiler"
#endif


/*-------------------------------------------------------------------------
 *
 * Return values.
 *
 *-------------------------------------------------------------------------
 */

typedef enum pg_thrd_error_t
{
	pg_thrd_success = 0,
	pg_thrd_nomem = 1,
	pg_thrd_timedout = 2,
	pg_thrd_busy = 3,
	pg_thrd_error = 4,

	/* Not from C11.  Needed by our pg_barrier_wait(). */
	pg_thrd_success_last = 5,
} pg_thrd_error_t;

static inline int
pg_thrd_maperror(int error)
{
#ifdef PG_THREADS_WIN32
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

#ifdef PG_THREADS_WIN32
typedef HANDLE pg_thrd_t;
#else
typedef pthread_t pg_thrd_t;
#endif

typedef int (*pg_thrd_start_t) (void *);

extern int	pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument);
extern int	pg_thrd_join(pg_thrd_t thread, int *result);
extern void pg_thrd_exit(int result);

static inline pg_thrd_t
pg_thrd_current(void)
{
#ifdef PG_THREADS_WIN32
	return GetCurrentThreadId();
#else
	return pthread_self();
#endif
}

static inline int
pg_thrd_equal(pg_thrd_t lhs, pg_thrd_t rhs)
{
#ifdef PG_THREADS_WIN32
	return lhs == rhs;
#else
	return pthread_equal(lhs, rhs);
#endif
}


/*-------------------------------------------------------------------------
 *
 * Initialization functions.
 *
 *-------------------------------------------------------------------------
 */

#ifdef PG_THREADS_WIN32
typedef INIT_ONCE pg_once_flag;
#define PG_ONCE_FLAG_INIT INIT_ONCE_STATIC_INIT
#else
typedef pthread_once_t pg_once_flag;
#define PG_ONCE_FLAG_INIT PTHREAD_ONCE_INIT
#endif

typedef void (*pg_call_once_function_t) (void);

#ifdef PG_THREADS_WIN32
extern BOOL CALLBACK pg_call_once_trampoline(pg_once_flag *flag,
											 void *parameter,
											 void **context);
#endif

static inline void
pg_call_once(pg_once_flag *flag, pg_call_once_function_t function)
{
#ifdef PG_THREADS_WIN32
	InitOnceExecuteOnce(flag, pg_call_once_trampoline, (void *) function, NULL);
#else
	pthread_once(flag, function);
#endif
}


/*-------------------------------------------------------------------------
 *
 * Thread-specific storage.  This mechanism is an alternative to using
 * the pg_thread_local storage class, which should be preferred where
 * possible.  The only advantage is that the TSS interface allows a
 * destructor functions to be run for non-NULL values when each thread
 * exits.
 *
 *-------------------------------------------------------------------------
 */

#ifdef PG_THREADS_WIN32
typedef DWORD pg_tss_t;
#else
typedef pthread_key_t pg_tss_t;
#endif

typedef void (*pg_tss_dtor_t) (void *);

/*
 * How long before we give up trying to call all the registered
 * destructors, if the destructors themselves are calling pg_tss_set()
 * to befuddle us by storing new non-NULL values?
 */
#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
#define PG_TSS_DTOR_ITERATIONS 8
#else
#define PG_TSS_DTOR_ITERATIONS PTHREAD_DESTRUCTOR_ITERATIONS
#endif

extern int	pg_tss_create(pg_tss_t *tss_id, pg_tss_dtor_t destructor);
extern void pg_tss_dtor_delete(pg_tss_t tss_id);
#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
extern void pg_tss_ensure_destructors_in_this_thread(void);
#endif

static inline void *
pg_tss_get(pg_tss_t key)
{
#ifdef PG_THREADS_WIN32
	return TlsGetValue(key);
#else
	return pthread_getspecific(key);
#endif
}

static inline int
pg_tss_set(pg_tss_t tss_id, void *value)
{
#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
	if (value)
		pg_tss_ensure_destructors_in_this_thread();
#endif

#ifdef PG_THREADS_WIN32
	return pg_thrd_maperror(TlsSetValue(tss_id, value));
#else
	return pg_thrd_maperror(pthread_setspecific(tss_id, value));
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

#ifdef PG_THREADS_WIN32
typedef SRWLOCK pg_rwlock_t;
#define PG_RWLOCK_STATIC_INIT SRWLOCK_INIT
#else
typedef pthread_rwlock_t pg_rwlock_t;
#define PG_RWLOCK_STATIC_INIT PTHREAD_RWLOCK_INITIALIZER
#endif

static inline int
pg_rwlock_init(pg_rwlock_t * lock)
{
#ifdef PG_THREADS_WIN32
	InitializeSRWLock(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_init(lock, NULL));
#endif
}

static inline int
pg_rwlock_rdlock(pg_rwlock_t * lock)
{
#ifdef PG_THREADS_WIN32
	AcquireSRWLockShared(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_rdlock(lock));
#endif
}

static inline int
pg_rwlock_wrlock(pg_rwlock_t * lock)
{
#ifdef PG_THREADS_WIN32
	AcquireSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_wrlock(lock));
#endif
}

static inline int
pg_wrlock_unlock(pg_rwlock_t * lock)
{
#ifdef PG_THREADS_WIN32
	ReleaseSRWLockExclusive(lock);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_rwlock_unlock(lock));
#endif
}

static inline int
pg_rdlock_unlock(pg_rwlock_t * lock)
{
#ifdef PG_THREADS_WIN32
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

#ifdef PG_THREADS_WIN32
/*
 * CRITICAL_SECTION might be the most obvious Windows mechanism for
 * pg_mtx_t, but SRWLock is reported to be at least as fast when used
 * only in exclusive mode, and has the advantage of a static
 * initializer (CRITICAL_SECTION must be initialized and destroyed
 * explicitly because it allocates resources other than the space it
 * occupies.)  C11 doesn't define a static initializer (possibly
 * because CRITICAL_SECTION doesn't?), but we want one anyway.  So
 * we'll just point pg_mtx_t to pg_rwlock_t.
 */
typedef pg_rwlock_t pg_mtx_t;
#define PG_MTX_STATIC_INIT PG_RWLOCK_STATIC_INIT
#else
typedef pthread_mutex_t pg_mtx_t;
#define PG_MTX_STATIC_INIT PTHREAD_MUTEX_INITIALIZER
#endif

typedef enum pg_mtx_type_t
{
	pg_mtx_plain = 0
} pg_mtx_type_t;


static inline int
pg_mtx_init(pg_mtx_t *mutex, int type)
{
#ifdef PG_THREADS_WIN32
	return pg_rwlock_init(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_init(mutex, NULL));
#endif
}

static inline int
pg_mtx_lock(pg_mtx_t *mutex)
{
#ifdef PG_THREADS_WIN32
	return pg_rwlock_wrlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_lock(mutex));
#endif
}

static inline int
pg_mtx_unlock(pg_mtx_t *mutex)
{
#ifdef PG_THREADS_WIN32
	return pg_wrlock_unlock(mutex);
#else
	return pg_thrd_maperror(pthread_mutex_unlock(mutex));
#endif
}

static inline int
pg_mtx_destroy(pg_mtx_t *mutex)
{
#ifdef PG_THREADS_WIN32
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

#ifdef PG_THREADS_WIN32
typedef CONDITION_VARIABLE pg_cnd_t;
#else
typedef pthread_cond_t pg_cnd_t;
#endif

static inline int
pg_cnd_init(pg_cnd_t *condvar)
{
#ifdef PG_THREADS_WIN32
	InitializeConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_init(condvar, NULL));
#endif
}

static inline int
pg_cnd_broadcast(pg_cnd_t *condvar)
{
#ifdef PG_THREADS_WIN32
	WakeAllConditionVariable(condvar);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_broadcast(condvar));
#endif
}

static inline int
pg_cnd_wait(pg_cnd_t *condvar, pg_mtx_t *mutex)
{
#ifdef PG_THREADS_WIN32
	SleepConditionVariableSRW(condvar, mutex, INFINITE, 0);
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_wait(condvar, mutex));
#endif
}

static inline int
pg_cnd_destroy(pg_cnd_t *condvar)
{
#ifdef PG_THREADS_WIN32
	return pg_thrd_success;
#else
	return pg_thrd_maperror(pthread_cond_destroy(condvar));
#endif
}


/*-------------------------------------------------------------------------
 *
 * Barriers.  Not in C11.  Apple currently lacks the POSIX version.
 * We assume that the OS might know a better way to implement it that
 * we do, so we only provide our own if we have to.
 *
 *-------------------------------------------------------------------------
 */

#ifdef PG_THREADS_WIN32
typedef SYNCHRONIZATION_BARRIER pg_barrier_t;
#elif defined(HAVE_PTHREAD_BARRIER)
typedef pthread_barrier_t pg_barrier_t;
#else
typedef struct pg_barrier_t
{
	bool		sense;
	int			expected;
	int			arrived;
	pg_mtx_t	mutex;
	pg_cnd_t	cond;
} pg_barrier_t;
#endif

static inline int
pg_barrier_init(pg_barrier_t *barrier, int count)
{
#ifdef PG_THREADS_WIN32
	return pg_thrd_maperror(InitializeSynchronizationBarrier(barrier, count, 0));
#elif defined(HAVE_PTHREAD_BARRIER)
	return pg_thrd_maperror(pthread_barrier_init(barrier, NULL, count));
#else
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
#endif
}

static inline int
pg_barrier_wait(pg_barrier_t *barrier)
{
#ifdef PG_THREADS_WIN32
	if (EnterSynchronizationBarrier(barrier, SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY))
		return pg_thrd_success_last;
	else
		return pg_thrd_success;
#elif defined(HAVE_PTHREAD_BARRIER)
	int			error = pthread_barrier_wait(barrier);

	if (error == 0)
		return pg_thrd_success;
	else if (error == PTHREAD_BARRIER_SERIAL_THREAD)
		return pg_thrd_success_last;
	else
		return pg_thrd_error;
#else
	bool		initial_sense;

	pg_mtx_lock(&barrier->mutex);
	barrier->arrived++;
	if (barrier->arrived == barrier->expected)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pg_mtx_unlock(&barrier->mutex);
		pg_cnd_broadcast(&barrier->cond);
		return pg_thrd_success_last;
	}
	initial_sense = barrier->sense;
	do
	{
		pg_cnd_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);
	pg_mtx_unlock(&barrier->mutex);
	return pg_thrd_success;
#endif
}

static inline int
pg_barrier_destroy(pg_barrier_t *barrier)
{
#ifdef PG_THREADS_WIN32
	return pg_thrd_success;
#elif defined(HAVE_PTHREAD_BARRIER)
	return pg_thrd_maperror(pthread_barrier_destroy(barrier));
#else
	pg_mtx_destroy(&barrier->mutex);
	pg_cnd_destroy(&barrier->cond);
	return pg_thrd_success;
#endif
}

#endif
