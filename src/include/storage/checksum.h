/*-------------------------------------------------------------------------
 *
 * checksum.h
 *	  Checksum implementation for data pages.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_H
#define CHECKSUM_H

#include "storage/block.h"

/*
 * Compute the checksum for a Postgres page.  The page must be aligned on a
 * 4-byte boundary.
 */
extern uint16 pg_checksum_page(char *page, BlockNumber blkno);
extern uint32 pg_checksum32_page(char *page, BlockNumber blkno, char*offset);
extern uint64 pg_checksum64_page(char *page, BlockNumber blkno, uint64*offset);
extern uint64 pg_checksum56_page(char *page, BlockNumber blkno, uint64*offset);
extern void pg_set_checksum56_page(char *page, uint64 checksum, uint64 *cksumloc);
extern uint64 pg_get_checksum56_page(char *page, uint64 *cksumloc);

#endif							/* CHECKSUM_H */
