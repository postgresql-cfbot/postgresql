/*-------------------------------------------------------------------------
 *
 * xlogbackup.h
 *		Definitions for internals of base backups.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogbackup.h
 *-------------------------------------------------------------------------
 */

#ifndef XLOG_BACKUP_H
#define XLOG_BACKUP_H

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "pgtime.h"

/* Structure to hold backup state. */
typedef struct BackupState
{
	/* Fields saved at backup start */
	/* Backup label name one extra byte for null-termination */
	char		name[MAXPGPATH + 1];
	XLogRecPtr	startpoint;		/* backup start WAL location */
	TimeLineID	starttli;		/* backup start TLI */
	XLogRecPtr	checkpointloc;	/* last checkpoint location */
	pg_time_t	starttime;		/* backup start time */
	bool		started_in_recovery;	/* backup started in recovery? */

	/* Fields saved at the end of backup */
	XLogRecPtr	stoppoint;		/* backup stop WAL location */
	TimeLineID	stoptli;		/* backup stop TLI */
	pg_time_t	stoptime;		/* backup stop time */
} BackupState;

/*
 * Session-level status of base backups
 *
 * This is used in parallel with the shared memory status to control parallel
 * execution of base backup functions for a given session, be it a backend
 * dedicated to replication or a normal backend connected to a database. The
 * update of the session-level status happens at the same time as the shared
 * memory counters to keep a consistent global and local state of the backups
 * running.
 */
typedef enum SessionBackupState
{
	SESSION_BACKUP_NONE,
	SESSION_BACKUP_RUNNING,
} SessionBackupState;

extern char *build_backup_content(BackupState *state,
								  bool ishistoryfile);

/*
 * Routines to start, stop, and get status of a base backup.
 */
extern void do_pg_backup_start(const char *backupidstr, bool fast,
							   List **tablespaces, BackupState *state,
							   StringInfo tblspcmapfile);
extern void do_pg_backup_stop(BackupState *state, bool waitforarchive);
extern void do_pg_abort_backup(int code, Datum arg);
extern void register_persistent_abort_backup_handler(void);
extern SessionBackupState get_backup_status(void);
extern void reset_backup_status(void);

#endif							/* XLOG_BACKUP_H */
