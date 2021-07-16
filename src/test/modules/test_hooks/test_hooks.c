/*--------------------------------------------------------------------------
 *
 * test_hooks.c
 *		Code for testing RLS hooks.
 *
 * Copyright (c) 2015-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_hooks/test_hooks.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "test_hooks.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

/* Install hooks */
void
_PG_init(void)
{
	install_test_process_interrupts_hook();
}

/* Uninstall hooks */
void
_PG_fini(void)
{
	uninstall_test_process_interrupts_hook();
}
