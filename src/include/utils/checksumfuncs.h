/*-------------------------------------------------------------------------
 *
 * checksumfunc.h
 *	  Checksum verification implementation for data pages.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksumfunc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUMFUNC_H
#define CHECKSUMFUNC_H

#include "postgres.h"

#include "access/tupdesc.h"
#include "common/relpath.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"

/*
 * A zero checksum can never be computed, see pg_checksum_page() */
#define NoComputedChecksum	0

extern bool check_one_block(Relation relation, ForkNumber forknum,
							BlockNumber blkno, uint16 *chk_expected,
							uint16 *chk_found);

#endif							/* CHECKSUMFUNC_H */
