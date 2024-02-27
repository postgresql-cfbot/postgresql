/*--------------------------------------------------------------------------
 *
 * injection_points.c
 *		Code for testing injection points.
 *
 * Injection points are able to trigger user-defined callbacks in pre-defined
 * code paths.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_points.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

extern PGDLLEXPORT void injection_error(const char *name);
extern PGDLLEXPORT void injection_notice(const char *name);
extern PGDLLEXPORT void injection_wait(const char *name);


/* Set of callbacks available to be attached to an injection point. */
void
injection_error(const char *name)
{
	elog(ERROR, "error triggered for injection point %s", name);
}

void
injection_notice(const char *name)
{
	elog(NOTICE, "notice triggered for injection point %s", name);
}

void
injection_wait(const char *name)
{
	elog(NOTICE, "waiting triggered for injection point %s", name);
	do
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
	} while (InjectionPointIsAttach(name));
	elog(NOTICE, "waiting done for injection point %s", name);
}

/*
 * SQL function for creating an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_attach);
Datum
injection_points_attach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *function;

	if (strcmp(action, "error") == 0)
		function = "injection_error";
	else if (strcmp(action, "notice") == 0)
		function = "injection_notice";
	else if (strcmp(action, "wait") == 0)
		function = "injection_wait";
	else
		elog(ERROR, "incorrect action \"%s\" for injection point creation", action);

	InjectionPointAttach(name, "injection_points", function);

	PG_RETURN_VOID();
}

/*
 * SQL function for triggering an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_run);
Datum
injection_points_run(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	INJECTION_POINT(name);

	PG_RETURN_VOID();
}

/*
 * SQL function for dropping an injection point.
 */
PG_FUNCTION_INFO_V1(injection_points_detach);
Datum
injection_points_detach(PG_FUNCTION_ARGS)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	InjectionPointDetach(name);

	PG_RETURN_VOID();
}

#include "access/multixact.h"
#include "access/xact.h"

PG_FUNCTION_INFO_V1(create_test_multixact);
Datum
create_test_multixact(PG_FUNCTION_ARGS)
{
	MultiXactId id;
	MultiXactIdSetOldestMember();
	id = MultiXactIdCreate(GetCurrentTransactionId(), MultiXactStatusUpdate,
						GetCurrentTransactionId(), MultiXactStatusForShare);
	PG_RETURN_TRANSACTIONID(id);
}

PG_FUNCTION_INFO_V1(read_test_multixact);
Datum
read_test_multixact(PG_FUNCTION_ARGS)
{
	MultiXactId id = PG_GETARG_TRANSACTIONID(0);
	MultiXactMember *members;
	INJECTION_POINT("read_test_multixact");
	/* discard caches */
	AtEOXact_MultiXact();

	if (GetMultiXactIdMembers(id,&members,false, false) == -1)
		elog(ERROR, "MultiXactId not found");
	PG_RETURN_VOID();
}