/*-------------------------------------------------------------------------
 *
 * blocksize.h
 *	  definitions for cluster-specific limits/structure defs
 *
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION: src/include/common/blocksize.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BLOCKSIZE_H
#define BLOCKSIZE_H

#include "catalog/pg_control.h"

void BlockSizeInit(Size rawblocksize, Size reservedsize);
void BlockSizeInitControl(ControlFileData *ControlFile, const char *DataDir);

/* These constants are initialized at runtime but are effective constants for callers */

const extern PGDLLIMPORT Size ReservedPageSize;
const extern PGDLLIMPORT Size ClusterMaxTIDsPerBTreePage;
const extern PGDLLIMPORT Size ClusterLargeObjectBlockSize;
const extern PGDLLIMPORT Size ClusterMaxHeapTupleSize;
const extern PGDLLIMPORT Size ClusterMaxHeapTuplesPerPage;
const extern PGDLLIMPORT Size ClusterMaxIndexTuplesPerPage;
const extern PGDLLIMPORT Size ClusterToastMaxChunkSize;

#define MaxReservedPageSize 256

/* between 0 and MaxReservedPageSize and multiple of 8 */
#define IsValidReservedPageSize(s) ((s) >= 0 && (s) <= MaxReservedPageSize && (((s)&0x7) == 0))

#endif
