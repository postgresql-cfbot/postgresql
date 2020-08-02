/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "postgres.h"

#include "access/tupdesc.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"

/*
 * A zero checksum can never be computed, see pg_checksum_page() */
#define NoComputedChecksum	0

extern bool check_one_block(Relation relation, ForkNumber forknum,
							BlockNumber blkno, uint16 *chk_expected,
							uint16 *chk_found);
/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);

#endif							/* CHECKSUM_H */
