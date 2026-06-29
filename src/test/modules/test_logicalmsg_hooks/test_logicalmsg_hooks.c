/*--------------------------------------------------------------------------
 *
 * test_logicalmsg_hooks.c
 *		Code for testing LogicalMessageHandle_hook
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/test_logicalmsg_hooks/test_logicalmsg_hooks.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "replication/worker_internal.h"

PG_MODULE_MAGIC;

static void test_logical_message_handler(LogicalRepMessageData *msg);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	LogicalRepMessageHandle_hook = &test_logical_message_handler;
}

void
test_logical_message_handler(LogicalRepMessageData *msg)
{
	ereport(LOG,
			(errmsg("received message: LSN %X/%08X, prefix: %s, message: %s",
					LSN_FORMAT_ARGS(msg->lsn),
					msg->prefix,
					msg->message)));
}
