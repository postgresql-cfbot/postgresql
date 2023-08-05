/*-------------------------------------------------------------------------
 *
 * shell_restore.h
 *		Exports for restoring via shell.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/restore/shell_restore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SHELL_RESTORE_H
#define _SHELL_RESTORE_H

#include "restore/restore_module.h"

/*
 * Since the logic for restoring via shell commands is in the core server and
 * does not need to be loaded via a shared library, it has a special
 * initialization function.
 */
extern const RestoreModuleCallbacks *shell_restore_init(void);

#endif							/* _SHELL_RESTORE_H */
