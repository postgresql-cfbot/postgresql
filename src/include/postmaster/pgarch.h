/*-------------------------------------------------------------------------
 *
 * pgarch.h
 *	  Exports from postmaster/pgarch.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/pgarch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PGARCH_H
#define _PGARCH_H

/* ----------
 * Archiver control info.
 *
 * We expect that archivable files within pg_wal will have names between
 * MIN_XFN_CHARS and MAX_XFN_CHARS in length, consisting only of characters
 * appearing in VALID_XFN_CHARS.  The status files in archive_status have
 * corresponding names with ".ready" or ".done" appended.
 * ----------
 */
#define MIN_XFN_CHARS	16
#define MAX_XFN_CHARS	40
#define VALID_XFN_CHARS "0123456789ABCDEF.history.backup.partial"

extern Size PgArchShmemSize(void);
extern void PgArchShmemInit(void);
extern bool PgArchCanRestart(void);
extern void PgArchiverMain(void) pg_attribute_noreturn();
extern void PgArchWakeup(void);
extern void PgArchForceDirScan(void);

/*
 * The value of the archive_library GUC.
 */
extern char *XLogArchiveLibrary;

/*
 * Callback that gets called to determine if the archive module is
 * configured.
 */
typedef bool (*ArchiveCheckConfiguredCB) (void);

/*
 * Callback called to archive a single WAL file.
 */
typedef bool (*ArchiveFileCB) (const char *file, const char *path);

/*
 * Archive module callbacks
 */
typedef struct ArchiveModuleCallbacks
{
	ArchiveCheckConfiguredCB check_configured_cb;
	ArchiveFileCB archive_file_cb;
} ArchiveModuleCallbacks;

/*
 * Type of the shared library symbol _PG_archive_module_init that is looked
 * up when loading an archive library.
 */
typedef void (*ArchiveModuleInit) (ArchiveModuleCallbacks *cb);

/*
 * Since the logic for archiving via a shell command is in the core server
 * and does not need to be loaded via a shared library, it has a special
 * initialization function.
 */
extern void shell_archive_init(ArchiveModuleCallbacks *cb);

/*
 * We consider archiving via shell to be enabled if archive_library is
 * empty or if archive_library is set to "shell".
 */
#define ShellArchivingEnabled() \
	(XLogArchiveLibrary[0] == '\0' || strcmp(XLogArchiveLibrary, "shell") == 0)

#endif							/* _PGARCH_H */
