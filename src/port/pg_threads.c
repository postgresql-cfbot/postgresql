/*-------------------------------------------------------------------------
 *
 * pg_threads.c
 *    Out-of-line parts of portable multi-threading API.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/port/pg_threads.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "port/pg_threads.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>


/* XXX TODO: make atomics avialable in frontend so we can use these! */
#define pg_read_barrier()
#define pg_write_barrier()


/*-------------------------------------------------------------------------
 *
 * Threads.
 *
 * There are small differences between the function types in C11,
 * POSIX (return type) and Windows (return type signedness, calling
 * convention).  The int return value will survive casting to/from
 * void * and DWORD respectively, but we still need a small trampoline
 * function to deal with the different function pointer type.
 *
 *-------------------------------------------------------------------------
 */

typedef struct pg_thrd_thunk
{
	pg_thrd_start_t function;
	void	   *argument;
} pg_thrd_thunk;

/*
 * A trampoline function, to handle calling convention and parameter
 * variations in the native APIs.
 */
#ifdef PG_THREADS_WIN32
static DWORD __stdcall
pg_thrd_body(void *vthunk)
#else
static void *
pg_thrd_body(void *vthunk)
#endif
{
	pg_thrd_thunk *thunk = (pg_thrd_thunk *) vthunk;
	void	   *argument = thunk->argument;
	pg_thrd_start_t function = thunk->function;
	int			result;

	free(vthunk);

	result = function(argument);

#ifdef PG_THREADS_WIN32
	return (DWORD) result;
#else
	return (void *) (intptr_t) result;
#endif
}

int
pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument)
{
	pg_thrd_thunk *thunk;

	thunk = malloc(sizeof(*thunk));
	if (thunk == NULL)
		return pg_thrd_nomem;
	thunk->function = function;
	thunk->argument = argument;

#ifdef WIN32
	*thread = CreateThread(NULL, 0, pg_thrd_body, thunk, 0, 0);
	if (*thread != NULL)
		return pg_thrd_success;
#else
	if (pthread_create(thread, NULL, pg_thrd_body, thunk) == 0)
		return pg_thrd_success;
#endif

	free(thunk);
	return pg_thrd_error;
}

int
pg_thrd_join(pg_thrd_t thread, int *result)
{
#ifdef WIN32
	DWORD		dword_result;

	if (WaitForSingleObject(thread, INFINITE) == WAIT_OBJECT_0)
	{
		if (result)
		{
			if (!GetExitCodeThread(thread, &dword_result))
				return pg_thrd_error;
			*result = (int) dword_result;
		}
		CloseHandle(thread);
		return pg_thrd_success;
	}
#else
	void	   *void_star_result;

	if (pthread_join(thread, &void_star_result) == 0)
	{
		if (result)
			*result = (int) (intptr_t) void_star_result;
		return pg_thrd_success;
	}
#endif
	return pg_thrd_error;
}

void
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
 * Initialization functions.
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
BOOL		CALLBACK
pg_call_once_trampoline(pg_once_flag *flag, void *parameter, void **context)
{
	pg_call_once_function_t function = (pg_call_once_function_t) parameter;

	function();
	return TRUE;
}
#endif


/*-------------------------------------------------------------------------
 *
 * Thread-specific storage.
 *
 * This extra support code is only needed when we can't use the native
 * support for thread-local storage destructors.  Normally that is
 * Windows, due to incompatible calling conventions, but this code
 * path can be activated on POSIX systems too for testing.
 *
 *-------------------------------------------------------------------------
 */

#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE

struct dtor_table_entry
{
	pg_tss_t	tss_id;
	pg_tss_dtor_t function;
};

static pg_rwlock_t dtor_table_lock = PG_RWLOCK_STATIC_INIT;
static size_t dtor_table_count = 0;
static size_t dtor_table_capacity = 0;
static struct dtor_table_entry *dtor_table;

/* One native TLS key with a native destructor, which drives all others. */
static pg_tss_t pg_tss_destructor_hook;
static bool pg_tss_run_destructors_installed;

/*
 * Helper function for recording the destructor for a given tss_id.
 * Returns true on success, or false if the table is full.
 */
static bool
pg_tss_dtor_set(pg_tss_t tss_id, pg_tss_dtor_t destructor)
{
	bool		have_space = true;

	pg_rwlock_wrlock(&dtor_table_lock);

	/* Make sure we have space, or fail. */
	if (dtor_table_count == dtor_table_capacity)
	{
		struct dtor_table_entry *new_dtor_table;
		size_t		new_dtor_table_capacity;

		new_dtor_table_capacity = Max(1, dtor_table_capacity * 2);
		new_dtor_table = malloc(sizeof(dtor_table[0]) * new_dtor_table_capacity);
		if (new_dtor_table == NULL)
		{
			/* Out of memory. */
			have_space = false;
		}
		else
		{
			if (dtor_table_count > 0)
			{
				memcpy(new_dtor_table,
					   dtor_table,
					   sizeof(dtor_table[0]) * dtor_table_count);
				free(dtor_table);
			}
			dtor_table = new_dtor_table;
			dtor_table_capacity = new_dtor_table_capacity;
		}
	}

#ifdef USE_ASSERTION_CHECKING
	/* We don't expect to see this ID already in the table. */
	for (size_t i = 0; i < dtor_table_count; i++)
		Assert(dtor_table[i].tss_id != tss_id);
#endif

	/* Store it. */
	if (have_space)
	{
		Assert(dtor_table_count < dtor_table_capacity);
		dtor_table[dtor_table_count].tss_id = tss_id;
		dtor_table[dtor_table_count].function = destructor;

		dtor_table_count++;
	}

	pg_wrlock_unlock(&dtor_table_lock);

	return have_space;
}

/*
 * The destructor installed for the single special FLS value that will
 * be called by Windows (or POSIX if we are using the special test
 * mode).  This must have CALLBACK calling convention on Windows,
 * which is the reason we can't just use its FlsAlloc() destructors
 * directly for pg_tss_create().
 */
static void
#ifdef WIN32
			CALLBACK
#endif
pg_tss_run_destructors(void *data)
{
	pg_rwlock_rdlock(&dtor_table_lock);

	for (int i = 0; i < PG_TSS_DTOR_ITERATIONS; ++i)
	{
		bool		seen_non_null_value = false;

		for (size_t slot = 0; slot < dtor_table_count; ++slot)
		{
			pg_tss_t	tss_id = dtor_table[slot].tss_id;
			void	   *value = pg_tss_get(tss_id);

			if (value)
			{
				pg_tss_dtor_t function = dtor_table[slot].function;

				Assert(function);

				/* Clear value. */
				pg_tss_set(tss_id, NULL);

				/*
				 * We'll need to go around again to make sure that a
				 * destructor called in this iteration didn't set something.
				 */
				seen_non_null_value = true;

				/* Unlock while running the destructor. */
				pg_rdlock_unlock(&dtor_table_lock);
				function(value);
				pg_rwlock_rdlock(&dtor_table_lock);
			}
		}

		/* If we didn't see any values, we're finished. */
		if (!seen_non_null_value)
			break;
	}
	pg_rdlock_unlock(&dtor_table_lock);
}

static void
pg_tss_install_run_destructors(void)
{
	/*
	 * We need a way to make sure our TSS destructors run at thread exit, even
	 * if the thread exits via native calls instead of our own pg_thrd_exit()
	 * or trampoline function.  So we register one real native 'hook'
	 * destructor that will then call all the destructors in our own
	 * destructor table.
	 *
	 * On Windows, we use FlsAlloc(), not TlsAlloc(), because that supports
	 * destructors.  Unforunately they have the wrong calling convention, or
	 * we could simply use them directly instead of doing all this extra work.
	 */
#ifdef PG_THREADS_WIN32
	pg_tss_destructor_hook = FlsAlloc(pg_tss_run_destructors);
	if (pg_tss_destructor_hook == FLS_OUT_OF_INDEXES)
		return;
#else
	if (pthread_key_create(&pg_tss_destructor_hook, pg_tss_run_destructors) != 0)
		return;
#endif
	pg_write_barrier();
	pg_tss_run_destructors_installed = true;

	/*
	 * Make sure that any thread that receives a pg_tss_t and might store a
	 * value can see that there is now potentially a registered destructor.
	 */
	pg_write_barrier();
}

/*
 * Called every time pg_tss_set() installs a non-NULL value.
 */
void
pg_tss_ensure_destructors_in_this_thread(void)
{
	/*
	 * Pairs with pg_tss_install_run_destructors(), called by pg_tss_create().
	 * This makes sure that we know if the tss_id being set could possibly
	 * have a destructor.  We don't want to pay the cost of checking, but we
	 * can check with a simple load if *any* tss_id has a destructor.  If so,
	 * we make sure that pg_tss_destructor_hook has a non-NULL value in *this*
	 * thread, because both Windows and POSIX will only call a destructor for
	 * a non-NULL value.
	 */
	pg_read_barrier();
	if (pg_tss_run_destructors_installed)
	{
#ifdef PG_THREADS_WIN32
		if (FlsGetValue(pg_tss_destructor_hook) == NULL)
			FlsSetValue(pg_tss_destructor_hook, (void *) 1);
#else
		if (pthread_getspecific(pg_tss_destructor_hook) == NULL)
			pthread_setspecific(pg_tss_destructor_hook, (void *) 1);
#endif
	}
}
#endif

int
pg_tss_create(pg_tss_t *tss_id, pg_tss_dtor_t destructor)
{
#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
	static pg_once_flag destructor_cleanup_once;

	/*
	 * Make sure our destructor hook is registered with the operating system
	 * in this process. This happens only once in the whole process.  Making
	 * sure it will run actually in each thread happens in
	 * pg_tss_ensure_destructors_will_run().
	 */
	pg_call_once(&destructor_cleanup_once, pg_tss_install_run_destructors);
	if (!pg_tss_run_destructors_installed)
		return pg_thrd_error;
#endif

#ifdef PG_THREADS_WIN32
	/* Windows native TSL, our own destructors machinery. */
	*tss_id = TlsAlloc();
	if (*tss_id == TLS_OUT_OF_INDEXES)
		return pg_thrd_error;
#elif defined(PG_THREADS_NEED_DESTRUCTOR_TABLE)
	/* POSIX, but testing our own destructor machinery. */
	if (pthread_key_create(tss_id, NULL) != 0)
		return pg_thrd_error;
#else
	/* POSIX handles destructors. */
	return pg_thrd_maperror(pthread_key_create(tss_id, destructor));
#endif

#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
	/* Allocate destructor table entry, or fail and clean up. */
	if (destructor &&!pg_tss_dtor_set(*tss_id, destructor))
	{
#ifdef PG_THREADS_WIN32
		TlsFree(*tss_id);
#else
		pthread_key_delete(*tss_id);
#endif
		return pg_thrd_error;
	}
#endif

	return pg_thrd_success;
}

void
pg_tss_dtor_delete(pg_tss_t tss_id)
{
#ifdef PG_THREADS_NEED_DESTRUCTOR_TABLE
	/*
	 * We have to search the destructor table linearly, but deleting IDs is
	 * probably very rare so that's OK.
	 */
	pg_rwlock_wrlock(&dtor_table_lock);
	for (size_t i = 0; i < dtor_table_count; ++i)
	{
		if (dtor_table[i].tss_id == tss_id)
		{
			/* Move the other values to compact the table. */
			if (i < dtor_table_count - 1)
				memmove(&dtor_table[i],
						&dtor_table[i + 1],
						sizeof(dtor_table[i]) * (dtor_table_count - i - 1));
			dtor_table_count--;
			break;
		}
	}
	pg_wrlock_unlock(&dtor_table_lock);
#endif

#ifdef PG_THREADS_WIN32
	TlsFree(tss_id);
#else
	pthread_key_delete(tss_id);
#endif
}
