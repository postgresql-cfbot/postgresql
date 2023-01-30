/*-------------------------------------------------------------------------
 *
 * archive_module.h
 *		Exports for archive modules.
 *
 * Copyright (c) 2022-2023, PostgreSQL Global Development Group
 *
 * src/include/postmaster/archive_module.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _ARCHIVE_MODULE_H
#define _ARCHIVE_MODULE_H

/*
 * The value of the archive_library GUC.
 */
extern PGDLLIMPORT char *XLogArchiveLibrary;

/*
 * API struct for an archive module.  This should be allocated as a static
 * const struct and returned via _PG_archive_module_init.  archive_file_cb is
 * the only required callback.  For more information about the purpose of each
 * callback, refer to the archive modules documentation.
 */
typedef struct ArchiveModuleCallbacks
{
	bool (*check_configured_cb) (void *arg);
	bool (*archive_file_cb) (const char *file, const char *path, void *arg);
	void (*shutdown_cb) (void *arg);
} ArchiveModuleCallbacks;

/*
 * Type of the shared library symbol _PG_archive_module_init that is looked
 * up when loading an archive library.
 */
typedef const ArchiveModuleCallbacks *(*ArchiveModuleInit) (void **arg);

extern PGDLLEXPORT const ArchiveModuleCallbacks *_PG_archive_module_init(void **arg);

/*
 * Since the logic for archiving via a shell command is in the core server
 * and does not need to be loaded via a shared library, it has a special
 * initialization function.
 */
extern const ArchiveModuleCallbacks *shell_archive_init(void **arg);

#endif							/* _ARCHIVE_MODULE_H */
