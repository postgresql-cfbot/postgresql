#include "postgres_fe.h"

#include "port/pg_pthread.h"

/*
 * partial pthread and pthread_barrier implementation for Windows
 */

#ifdef WIN32

typedef struct win32_pthread
{
	HANDLE		handle;
	void	   *(*routine) (void *);
	void	   *arg;
	void	   *result;
} win32_pthread;

unsigned __stdcall
win32_pthread_run(void *arg)
{
	win32_pthread *th = (win32_pthread *) arg;

	th->result = th->routine(th->arg);

	return 0;
}

int
pthread_create(pthread_t *thread,
			   pthread_attr_t *attr,
			   void *(*start_routine) (void *),
			   void *arg)
{
	int			save_errno;
	win32_pthread *th;

	th = (win32_pthread *) pg_malloc(sizeof(win32_pthread));
	th->routine = start_routine;
	th->arg = arg;
	th->result = NULL;

	th->handle = (HANDLE) _beginthreadex(NULL, 0, win32_pthread_run, th, 0, NULL);
	if (th->handle == NULL)
	{
		save_errno = errno;
		free(th);
		return save_errno;
	}

	*thread = th;
	return 0;
}

int
pthread_join(pthread_t th, void **thread_return)
{
	if (th == NULL || th->handle == NULL)
		return errno = EINVAL;

	if (WaitForSingleObject(th->handle, INFINITE) != WAIT_OBJECT_0)
	{
		_dosmaperr(GetLastError());
		return errno;
	}

	if (thread_return)
		*thread_return = th->result;

	CloseHandle(th->handle);
	free(th);
	return 0;
}

int
pthread_barrier_init(pthread_barrier_t *barrier, void *unused, int nthreads)
{
	/* no spinning: threads are not expected to arrive at the barrier together */
	bool ok = InitializeSynchronizationBarrier(barrier, nthreads, 0);
	return 0;
}

int
pthread_barrier_wait(pthread_barrier_t *barrier)
{
	bool last = EnterSynchronizationBarrier(barrier, SYNCHRONIZATION_BARRIER_FLAGS_BLOCK_ONLY);
	return last ? PTHREAD_BARRIER_SERIAL_THREAD : 0;
}

int
pthread_barrier_destroy(pthread_barrier_t *barrier)
{
	/*
	 * The following is coldly ignored because it requires Windows 8
	 * or Windows Server 2012, which is a little too much.
	 *
	 * Also, there is a SYNCHRONIZATION_BARRIER_FLAGS_NO_DELETE flag
	 * but it probably requires the same versions.
	 *
	 *  (void) DeleteSynchronizationBarrier(barrier);
	 */
	return 0;
}

#elif !defined(HAVE_PTHREAD_BARRIER_WAIT)

/*
 * pthread barrier implementation for MacOS
 */

int
pthread_barrier_init(pthread_barrier_t *barrier, const void *attr, int count)
{
	barrier->sense = false;
	barrier->count = count;
	barrier->arrived = 0;
	if (pthread_cond_init(&barrier->cond, NULL) < 0)
		return -1;
	if (pthread_mutex_init(&barrier->mutex, NULL) < 0)
	{
		int save_errno = errno;

		pthread_cond_destroy(&barrier->cond);
		errno = save_errno;

		return -1;
	}

	return 0;
}

int
pthread_barrier_wait(pthread_barrier_t *barrier)
{
	bool		initial_sense;

	pthread_mutex_lock(&barrier->mutex);

	/* We have arrived at the barrier. */
	barrier->arrived++;
	Assert(barrier->arrived <= barrier->count);

	/* If we were the last to arrive, release the others and return. */
	if (barrier->arrived == barrier->count)
	{
		barrier->arrived = 0;
		barrier->sense = !barrier->sense;
		pthread_mutex_unlock(&barrier->mutex);
		pthread_cond_broadcast(&barrier->cond);

		return PTHREAD_BARRIER_SERIAL_THREAD;
	}

	/* Wait for someone else to flip the sense. */
	initial_sense = barrier->sense;
	do
	{
		pthread_cond_wait(&barrier->cond, &barrier->mutex);
	} while (barrier->sense == initial_sense);

	pthread_mutex_unlock(&barrier->mutex);

	return 0;
}

int
pthread_barrier_destroy(pthread_barrier_t *barrier)
{
	pthread_cond_destroy(&barrier->cond);
	pthread_mutex_destroy(&barrier->mutex);
	return 0;
}

#endif							/* HAVE_PTHREAD_BARRIER_WAIT / WIN32 */
