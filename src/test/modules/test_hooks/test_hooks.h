/*--------------------------------------------------------------------------
 *
 * test_hooks.h
 *		Definitions for various PostgreSQL hooks
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_hooks/test_hooks.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TEST_HOOKS_H
#define TEST_HOOKS_H

extern void install_test_process_interrupts_hook(void);
extern void uninstall_test_process_interrupts_hook(void);

#endif
