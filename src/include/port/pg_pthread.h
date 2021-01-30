/*-------------------------------------------------------------------------
 *
 * Declarations for missing POSIX thread components.
 *
 *	  Currently this supplies an implementation of pthread_barrier_t for the
 *	  benefit of macOS, which lacks it as of release 11.  These declarations
 *	  are not in port.h, because that'd require <pthread.h> to be included by
 *	  every translation unit.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_PTHREAD_H
#define PG_PTHREAD_H

/*
 * Multi-platform pthread implementations
 */
#ifdef WIN32

/* Use native win32 threads on Windows */

typedef struct win32_pthread *pthread_t;
typedef int pthread_attr_t;
typedef SYNCHRONIZATION_BARRIER pthread_barrier_t;

extern int	pthread_create(pthread_t *thread, pthread_attr_t *attr, void *(*start_routine) (void *), void *arg);
extern int	pthread_join(pthread_t th, void **thread_return);

extern int	pthread_barrier_init(pthread_barrier_t *barrier, void *unused, int nthreads);
extern int	pthread_barrier_wait(pthread_barrier_t *barrier);
extern int	pthread_barrier_destroy(pthread_barrier_t *barrier);

#define PTHREAD_BARRIER_SERIAL_THREAD (-1)

#elif defined(ENABLE_THREAD_SAFETY)

/* Use platform-dependent pthread capability */
#include <pthread.h>

#ifndef PTHREAD_BARRIER_SERIAL_THREAD
#define PTHREAD_BARRIER_SERIAL_THREAD (-1)
#endif

/* MacOS is missing pthread barriers */

#if !defined(HAVE_PTHREAD_BARRIER_WAIT)

typedef struct pg_pthread_barrier
{
	bool		sense;			/* we only need a one bit phase */
	int			count;			/* number of threads expected */
	int			arrived;		/* number of threads that have arrived */
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} pthread_barrier_t;

extern int pthread_barrier_init(pthread_barrier_t *barrier,
								const void *attr,
								int count);
extern int pthread_barrier_wait(pthread_barrier_t *barrier);
extern int pthread_barrier_destroy(pthread_barrier_t *barrier);

#endif /* HAVE_PTHREAD_BARRIER_WAIT */

#else

/* No threads implementation, use none */
#define pthread_t void *
#define pthread_barrier_t void *
#define pthread_barrier_init(a, b, c) /* ignore */
#define pthread_barrier_wait(a) /* ignore */
#define pthread_barrier_destroy(a) /* ignore */

#endif /* WIN32 / ENABLE_THREAD_SAFETY */

#endif /* PG_PTHREAD_H */
