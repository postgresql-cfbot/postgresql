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
#include "storage/dsm_registry.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define MAX_CONN_RECORDS 100

/* GUC Variables */
static int	auth_delay_milliseconds = 0;
static int	auth_delay_max_milliseconds = 0;

/* Original Hook */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;

typedef struct AuthConnRecord
{
	char		remote_host[NI_MAXHOST];
	double		sleep_time;		/* in milliseconds */
	TimestampTz last_failed_auth;
} AuthConnRecord;

static shmem_startup_hook_type shmem_startup_next = NULL;
static AuthConnRecord *acr_array = NULL;

static AuthConnRecord *auth_delay_find_acr_for_host(char *remote_host);
static AuthConnRecord *auth_delay_find_free_acr(void);
static double auth_delay_increase_delay_after_failed_conn_auth(Port *port);
static void auth_delay_cleanup_conn_record(Port *port);
static void auth_delay_expire_conn_records(Port *port);

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
	 * We handle both STATUS_ERROR and STATUS_OK - the third option
	 * (STATUS_EOF) is disregarded.
	 *
	 * In case of STATUS_ERROR we inject a short delay, optionally with
	 * exponential backoff.
	 */
	if (status == STATUS_ERROR)
	{
		if (auth_delay_max_milliseconds > 0)
		{
			/*
			 * Delay by 2^n seconds after each authentication failure from a
			 * particular host, where n is the number of consecutive
			 * authentication failures.
			 */
			delay = auth_delay_increase_delay_after_failed_conn_auth(port);

			/*
			 * Clamp delay to a maximum of auth_delay_max_milliseconds.
			 */
			delay = Min(delay, auth_delay_max_milliseconds);
		}

		if (delay > 0)
		{
			elog(DEBUG1, "Authentication delayed for %g seconds due to auth_delay", delay / 1000.0);
			pg_usleep(1000L * (long) delay);
		}

		/*
		 * Expire delays from other hosts after auth_delay_max_milliseconds *
		 * 5.
		 */
		auth_delay_expire_conn_records(port);
	}

	/*
	 * Remove host-specific delay if authentication succeeded.
	 */
	if (status == STATUS_OK)
		auth_delay_cleanup_conn_record(port);
}

static double
auth_delay_increase_delay_after_failed_conn_auth(Port *port)
{
	AuthConnRecord *acr = NULL;

	acr = auth_delay_find_acr_for_host(port->remote_host);

	if (!acr)
	{
		acr = auth_delay_find_free_acr();

		if (!acr)
		{
			/*
			 * No free space, MAX_CONN_RECORDS reached. Wait for the
			 * configured maximum amount.
			 */
			elog(LOG, "auth_delay: host connection list full, waiting maximum amount");
			return auth_delay_max_milliseconds;
		}
		strcpy(acr->remote_host, port->remote_host);
	}
	if (acr->sleep_time == 0)
		acr->sleep_time = (double) auth_delay_milliseconds;
	else
		acr->sleep_time *= 2;

	/*
	 * Set current timestamp for later expiry.
	 */
	acr->last_failed_auth = GetCurrentTimestamp();

	return acr->sleep_time;
}

static AuthConnRecord *
auth_delay_find_acr_for_host(char *remote_host)
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
auth_delay_find_free_acr(void)
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
auth_delay_cleanup_conn_record(Port *port)
{
	AuthConnRecord *acr = NULL;

	acr = auth_delay_find_acr_for_host(port->remote_host);
	if (acr == NULL)
		return;

	port->remote_host[0] = '\0';

	acr->sleep_time = 0.0;
	acr->last_failed_auth = 0.0;
}

static void
auth_delay_expire_conn_records(Port *port)
{
	int			i;
	TimestampTz now = GetCurrentTimestamp();

	for (i = 0; i < MAX_CONN_RECORDS; i++)
	{
		/*
		 * Do not expire the host from which the current authentication
		 * failure originated.
		 */
		if (strcmp(acr_array[i].remote_host, port->remote_host) == 0)
			continue;

		if (acr_array[i].last_failed_auth > 0 && (long) ((now - acr_array[i].last_failed_auth) / 1000) > 5 * auth_delay_max_milliseconds)
		{
			acr_array[i].remote_host[0] = '\0';
			acr_array[i].sleep_time = 0.0;
			acr_array[i].last_failed_auth = 0.0;
		}
	}
}

/*
 * Set up shared memory
 */

static void
auth_delay_init_state(void *ptr)
{
	Size		shm_size;
	AuthConnRecord *array = (AuthConnRecord *) ptr;

	shm_size = sizeof(AuthConnRecord) * MAX_CONN_RECORDS;

	memset(array, 0, shm_size);
}

static void
auth_delay_shmem_startup(void)
{
	bool		found;
	Size		shm_size;

	if (shmem_startup_next)
		shmem_startup_next();

	shm_size = sizeof(AuthConnRecord) * MAX_CONN_RECORDS;
	acr_array = GetNamedDSMSegment("auth_delay", shm_size, auth_delay_init_state, &found);
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

	DefineCustomIntVariable("auth_delay.max_milliseconds",
							"Maximum delay for exponential backoff",
							NULL,
							&auth_delay_max_milliseconds,
							0,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("auth_delay");

	/* Install Hooks */
	original_client_auth_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = auth_delay_checks;

	/* Set up shared memory */
	shmem_startup_next = shmem_startup_hook;
	shmem_startup_hook = auth_delay_shmem_startup;
}
