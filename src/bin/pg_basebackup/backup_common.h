/*-------------------------------------------------------------------------
 *
 * backup_common.h - common code that is used in both pg_basebackup and pg_rewind.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKUP_COMMON_H
#define BACKUP_COMMON_H

#include "libpq-fe.h"
#include "pqexpbuffer.h"

/* Contents of configuration file to be generated */
extern PQExpBuffer recoveryconfcontents;

extern bool writerecoveryconf;
extern char *replication_slot;
PGconn	   *conn;

extern void disconnect_atexit(void);
extern void GenerateRecoveryConf(void);
extern void WriteRecoveryConf(char *target_dir);

/*
 * recovery.conf is integrated into postgresql.conf from version 12.
 */
#define MINIMUM_VERSION_FOR_RECOVERY_GUC 120000


#endif							/* BACKUP_COMMON_H */
