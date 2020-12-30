/*
 * clogdefs.h
 *
 * PostgreSQL transaction-commit-log manager
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/clogdefs.h
 */
#ifndef CLOGDEFS_H
#define CLOGDEFS_H

/*
 * Possible transaction statuses --- note that all-zeroes is the initial
 * state.
 *
 * A "subcommitted" transaction is a committed subtransaction whose parent
 * hasn't committed or aborted yet.
 *
 * An "unknown" status indicates an error condition, such as when the clog has
 * been erroneously truncated and the commit status of a transaction cannot be
 * determined.
 */
typedef enum XidStatus {
	TRANSACTION_STATUS_IN_PROGRESS = 0x00,
	TRANSACTION_STATUS_COMMITTED = 0x01,
	TRANSACTION_STATUS_ABORTED = 0x02,
	TRANSACTION_STATUS_SUB_COMMITTED = 0x03,
	TRANSACTION_STATUS_UNKNOWN = 0x04	/* error condition */
} XidStatus;

#endif							/* CLOG_H */
