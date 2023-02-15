/*-------------------------------------------------------------------------
 *
 * shell_restore.h
 *		Exports for restoring via shell.
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * src/include/restore/shell_restore.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SHELL_RESTORE_H
#define _SHELL_RESTORE_H

extern bool shell_restore_file_configured(void);
extern bool shell_restore_wal_segment(const char *file, const char *path,
									  const char *lastRestartPointFileName);
extern bool shell_restore_timeline_history(const char *file, const char *path);

extern bool shell_archive_cleanup_configured(void);
extern void shell_archive_cleanup(const char *lastRestartPointFileName);

extern bool shell_recovery_end_configured(void);
extern void shell_recovery_end(const char *lastRestartPointFileName);

#endif							/* _SHELL_RESTORE_H */
