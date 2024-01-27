/*-------------------------------------------------------------------------
 *
 * blocksize_int.h
 *	  internal defintions for cluster-specific limits/structure defs
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION: src/include/common/blocksize_int.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Note: We use the same identifier here as in blocksize.h, due to blocksize.c
 * (the only consumer of this header file) needing to see these definitions;
 * other subsequent header files will pull in blocksize.h, so without using
 * the same symbol you get conflicting defintion errors.
 */

#ifndef BLOCKSIZE_H
#define BLOCKSIZE_H

/* forward declaration */
typedef struct ControlFileData ControlFileData;

void BlockSizeInit(Size rawblocksize, Size reservedsize);
void BlockSizeInitControl(ControlFileData *ControlFile, const char *DataDir);

extern PGDLLIMPORT Size ReservedPageSize;
extern PGDLLIMPORT Size ClusterMaxTIDsPerBTreePage;
extern PGDLLIMPORT Size ClusterLargeObjectBlockSize;
extern PGDLLIMPORT Size ClusterMaxHeapTupleSize;
extern PGDLLIMPORT Size ClusterMaxHeapTuplesPerPage;
extern PGDLLIMPORT Size ClusterMaxIndexTuplesPerPage;
extern PGDLLIMPORT Size ClusterToastMaxChunkSize;

#define MaxReservedPageSize 256

/* between 0 and MaxReservedPageSize and multiple of 8 */
#define IsValidReservedPageSize(s) ((s) >= 0 && (s) <= MaxReservedPageSize && (((s)&0x7) == 0))

#endif
