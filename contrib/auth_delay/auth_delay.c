/* -------------------------------------------------------------------------
 *
 * auth_delay.c
 *
 * Copyright (c) 2010-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/auth_delay/auth_delay.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "libpq/auth.h"
#include "miscadmin.h"
#include "port.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define MAX_CONN_RECORDS 50

/* GUC Variables */
static int	auth_delay_milliseconds = 0;
static bool auth_delay_exp_backoff = false;
static int	auth_delay_max_seconds = 0;

/* Original Hook */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;

typedef struct AuthConnRecord
{
	char		remote_host[NI_MAXHOST];
	double		sleep_time;		/* in milliseconds */
} AuthConnRecord;

static shmem_startup_hook_type shmem_startup_next = NULL;
static shmem_request_hook_type shmem_request_next = NULL;
static AuthConnRecord *acr_array = NULL;

static AuthConnRecord *find_acr_for_host(char *remote_host);
static AuthConnRecord *find_free_acr(void);
static double increase_delay_after_failed_conn_auth(Port *port);
static void cleanup_conn_record(Port *port);

/*
 * Check authentication
 */
static void
auth_delay_checks(Port *port, int status)
{
	double		delay = auth_delay_milliseconds;

	/*
	 * Any other plugins which use ClientAuthentication_hook.
	 */
	if (original_client_auth_hook)
		original_client_auth_hook(port, status);

	/*
	 * Inject a short delay if authentication failed.
	 */
	if (status == STATUS_ERROR)
	{
		if (auth_delay_exp_backoff)
		{
			/*
			 * Delay by 2^n seconds after each authentication failure from a
			 * particular host, where n is the number of consecutive
			 * authentication failures.
			 */
			delay = increase_delay_after_failed_conn_auth(port);

			/*
			 * Clamp delay to a maximum of auth_delay_max_seconds.
			 */
			if (auth_delay_max_seconds > 0) {
				delay = Min(delay, 1000L * auth_delay_max_seconds);
			}
		}

		if (delay > 0)
		{
			elog(DEBUG1, "Authentication delayed for %g seconds due to auth_delay", delay / 1000.0);
			pg_usleep(1000L * (long) delay);
		}
	}

	/*
	 * Remove host-specific delay if authentication succeeded.
	 */
	if (status == STATUS_OK)
		cleanup_conn_record(port);
}

static double
increase_delay_after_failed_conn_auth(Port *port)
{
	AuthConnRecord *acr = NULL;

	acr = find_acr_for_host(port->remote_host);

	if (!acr)
	{
		acr = find_free_acr();

		if (!acr)
		{
			/*
			 * No free space, MAX_CONN_RECORDS reached. Wait for the
			 * configured maximum amount.
			 */
			return 1000L * auth_delay_max_seconds;
		}
		strcpy(acr->remote_host, port->remote_host);
	}
	if (acr->sleep_time == 0)
		acr->sleep_time = (double) auth_delay_milliseconds;
	else
		acr->sleep_time *= 2;

	return acr->sleep_time;
}

static AuthConnRecord *
find_acr_for_host(char *remote_host)
{
	int			i;

	for (i = 0; i < MAX_CONN_RECORDS; i++)
	{
		if (strcmp(acr_array[i].remote_host, remote_host) == 0)
			return &acr_array[i];
	}

	return NULL;
}

static AuthConnRecord *
find_free_acr(void)
{
	int			i;

	for (i = 0; i < MAX_CONN_RECORDS; i++)
	{
		if (!acr_array[i].remote_host[0])
			return &acr_array[i];
	}

	return 0;
}

static void
cleanup_conn_record(Port *port)
{
	AuthConnRecord *acr = NULL;

	acr = find_acr_for_host(port->remote_host);
	if (acr == NULL)
		return;

	port->remote_host[0] = '\0';

	acr->sleep_time = 0.0;
}

/*
 * Set up shared memory
 */

static void
auth_delay_shmem_request(void)
{
	Size		shm_size;

	if (shmem_request_next)
		shmem_request_next();

	shm_size = sizeof(AuthConnRecord) * MAX_CONN_RECORDS;
	shm_size += sizeof(int);
	RequestAddinShmemSpace(shm_size);
}

static void
auth_delay_shmem_startup(void)
{
	bool		found;
	Size		shm_size;

	if (shmem_startup_next)
		shmem_startup_next();

	shm_size = sizeof(AuthConnRecord) * MAX_CONN_RECORDS;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	acr_array = ShmemInitStruct("Array of AuthConnRecord", shm_size, &found);
	if (!found)
	{
		/* First time through ... */
		memset(acr_array, 0, shm_size);
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("auth_delay must be loaded via shared_preload_libraries")));

	/* Define custom GUC variables */
	DefineCustomIntVariable("auth_delay.milliseconds",
							"Milliseconds to delay before reporting authentication failure",
							NULL,
							&auth_delay_milliseconds,
							0,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("auth_delay.exponential_backoff",
							 "Double the delay after each authentication failure from a particular host",
							 NULL,
							 &auth_delay_exp_backoff,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("auth_delay.max_seconds",
							"Maximum delay when exponential backoff is enabled",
							NULL,
							&auth_delay_max_seconds,
							10,
							0, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("auth_delay");

	/* Install Hooks */
	original_client_auth_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = auth_delay_checks;

	/* Set up shared memory */
	shmem_request_next = shmem_request_hook;
	shmem_request_hook = auth_delay_shmem_request;
	shmem_startup_next = shmem_startup_hook;
	shmem_startup_hook = auth_delay_shmem_startup;
}
