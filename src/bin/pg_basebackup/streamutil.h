/*-------------------------------------------------------------------------
 *
 * streamutil.h
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.h
 *-------------------------------------------------------------------------
 */

#ifndef STREAMUTIL_H
#define STREAMUTIL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "libpq-fe.h"

extern const char *progname;
extern char *connection_string;
extern char *dbhost;
extern char *dbuser;
extern char *dbport;
extern char *dbname;
extern int	dbgetpassword;
extern uint32 WalSegSz;

/* Connection kept global so we can disconnect easily */
extern PGconn *conn;

typedef struct SlotInformation {
	bool	   is_logical;
	XLogRecPtr restart_lsn;
	XLogRecPtr confirmed_flush_lsn;
	TimeLineID restart_tli;
	TimeLineID confirmed_flush_tli;
} SlotInformation;

extern PGconn *GetConnection(void);

/* Replication commands */
extern bool CreateReplicationSlot(PGconn *conn, const char *slot_name,
								  const char *plugin, bool is_temporary,
								  bool is_physical, bool reserve_wal,
								  bool slot_exists_ok, bool two_phase);
extern bool DropReplicationSlot(PGconn *conn, const char *slot_name);
extern bool RunIdentifySystem(PGconn *conn, char **sysid,
							  TimeLineID *starttli,
							  XLogRecPtr *startpos,
							  char **db_name);
typedef enum {
	READ_REPLICATION_SLOT_OK,
	READ_REPLICATION_SLOT_UNSUPPORTED,
	READ_REPLICATION_SLOT_ERROR,
	READ_REPLICATION_SLOT_NONEXISTENT
} ReadReplicationSlotStatus;

extern ReadReplicationSlotStatus GetSlotInformation(PGconn *conn, const char *slot_name, SlotInformation *slot_info);
extern bool RetrieveWalSegSize(PGconn *conn);
extern TimestampTz feGetCurrentTimestamp(void);
extern void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
								  long *secs, int *microsecs);

extern bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time,
										 int msec);
extern void fe_sendint64(int64 i, char *buf);
extern int64 fe_recvint64(char *buf);

#endif							/* STREAMUTIL_H */
