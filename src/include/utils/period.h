/*-------------------------------------------------------------------------
 *
 * period.h
 *	  support for Postgres periods.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/period.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PERIOD_H
#define PERIOD_H

extern Datum period_to_range(TupleTableSlot *slot, int startattno, int endattno, Oid rangetype);

#endif							/* PERIOD_H */
