/*-------------------------------------------------------------------------
 *
 * pg_threads.c
 *    Out-of-line parts of portable multi-threading API.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
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

typedef struct pg_thrd_start_info
{
	pg_thrd_start_t function;
	void	   *argument;

#ifdef WIN32
	/* Space to pass the thread's handle, for use by pg_thrd_current(). */
	pg_thrd_t	self;
#endif
} pg_thrd_start_info;

#ifdef WIN32
static thread_local pg_thrd_t my_thrd_handle;
#endif

/*
 * A trampoline function, to handle calling convention and parameter
 * variations in the native APIs.
 */
#ifdef WIN32
static DWORD __stdcall
pg_thrd_invoke(void *start_info_v)
#else
static void *
pg_thrd_invoke(void *start_info_v)
#endif
{
	pg_thrd_start_info *start_info = start_info_v;
	pg_thrd_start_t function = start_info->function;
	void	   *argument = start_info->argument;
	int			result;

#ifdef WIN32

	/*
	 * Retrieve handle passed here by pg_thrd_create() before allowing this
	 * thread to run.  (pg_thrd_current() can't use CurrentThread(), because
	 * that returns a pseudo-handle with the same value in all threads.)
	 */
	Assert(start_info->self);
	my_thrd_handle = start_info->self;
#endif

	free(start_info);

	result = function(argument);

#ifdef WIN32
	return (DWORD) result;
#else
	return (void *) (intptr_t) result;
#endif
}

int
pg_thrd_create(pg_thrd_t *thread, pg_thrd_start_t function, void *argument)
{
	pg_thrd_start_info *start_info;

	start_info = malloc(sizeof(*start_info));
	if (start_info == NULL)
		return pg_thrd_nomem;
	start_info->function = function;
	start_info->argument = argument;

#ifdef WIN32
	*thread = CreateThread(NULL, 0, pg_thrd_invoke, start_info,
						   CREATE_SUSPENDED, 0);
	if (*thread != NULL)
	{
		/*
		 * Give the thread its own handle so that pg_thrd_current() works,
		 * before it is allowed to start running.
		 */
		start_info->self = *thread;
		ResumeThread(*thread);

		return pg_thrd_success;
	}
#else
	if (pthread_create(thread, NULL, pg_thrd_invoke, start_info) == 0)
		return pg_thrd_success;
#endif

	free(start_info);
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

#ifdef WIN32
pg_thrd_t
pg_thrd_current_win32(void)
{
	/*
	 * This function is in .c file to avoid potential cross-DLL complications
	 * if a load from thread_local is inlined.
	 */
	return my_thrd_handle;
}
#endif


/*-------------------------------------------------------------------------
 *
 * User-supplied callback on thread exit.
 *
 *-------------------------------------------------------------------------
 */

void		PG_THRD_ATEXIT_INVOKE_CC
pg_thrd_atexit_invoke(void *value)
{
	pg_thrd_atexit_function_t function = value;

	/*
	 * Shouldn't be called with a NULL value on POSIX, but it's not well
	 * documented on Windows so let's be paranoid.
	 */
	if (function)
		function();
}


/*-------------------------------------------------------------------------
 *
 * "Once"-style initialization functions.
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32
/* Only needed on Windows, because POSIX has the right prototype already. */
BOOL		CALLBACK
pg_call_once_invoke(pg_once_flag *flag, void *parameter, void **context)
{
	pg_call_once_function_t function = parameter;

	function();
	return true;
}
#endif
