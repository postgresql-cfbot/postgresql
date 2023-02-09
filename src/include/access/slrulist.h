/*---------------------------------------------------------------------------
 * slrulist.h
 *
 * The SLRU list is kept in its own source file for possible
 * use by automatic tools.  The exact representation of a rmgr is determined
 * by the PG_SLRU macro, which is not defined in this file; it can be
 * defined by the caller for special purposes.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slrulist.h
 *---------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef SLRULIST_H here */

/*
 * List of SLRU entries.  Note that order of entries defines the
 * numerical values of each SLRU's ID, which is used in in-memory structus.
 */

/* symbol name, textual name, path, synchronize */
PG_SLRU(SLRU_CLOG_ID, "Xact", "pg_xact", true)
PG_SLRU(SLRU_SUBTRANS_ID, "Subtrans", "pg_subtrans", false)
PG_SLRU(SLRU_MULTIXACT_OFFSET_ID, "MultiXactOffset", "pg_multixact/offsets", true)
PG_SLRU(SLRU_MULTIXACT_MEMBER_ID, "MultiXactMember", "pg_multixact/members", true)
PG_SLRU(SLRU_COMMIT_TS_ID, "CommitTs", "pg_commit_ts", true)
PG_SLRU(SLRU_SERIAL_ID, "Serial", "pg_serial", false)
PG_SLRU(SLRU_NOTIFY_ID, "Notify", "pg_notify", false)
