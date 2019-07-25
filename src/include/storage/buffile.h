/*-------------------------------------------------------------------------
 *
 * buffile.h
 *	  Management of large buffered temporary files.
 *
 * The BufFile routines provide a partial replacement for stdio atop
 * virtual file descriptors managed by fd.c.  Currently they only support
 * buffered access to a virtual file, without any of stdio's formatting
 * features.  That's enough for immediate needs, but the set of facilities
 * could be expanded if necessary.
 *
 * BufFile also supports working with temporary files that exceed the OS
 * file size limit and/or the largest offset representable in an int.
 * It might be better to split that out as a separately accessible module,
 * but currently we have no need for oversize temp files without buffered
 * access.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buffile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BUFFILE_H
#define BUFFILE_H

#include "storage/sharedfileset.h"

/*
 * BufFile and TransientBufFile are opaque types whose details are not known
 * outside buffile.c.
 */

typedef struct BufFile BufFile;
typedef struct TransientBufFile TransientBufFile;

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large BufFiles to be spread across multiple
 * tablespaces when available.
 *
 * An integer value indicating the number of useful bytes in the segment is
 * appended to each segment of if the file is both shared and encrypted, see
 * BufFile.useful.
 */
#define MAX_PHYSICAL_FILESIZE		0x40000000

/* Express segment size in the number of blocks. */
#define BUFFILE_SEG_BLOCKS(phys)	((phys) / BLCKSZ)

/* GUC to control size of the file segment. */
extern int buffile_max_filesize;
/* Segment size in blocks, derived from the above. */
extern int buffile_seg_blocks;

/*
 * prototypes for functions in buffile.c
 */

extern BufFile *BufFileCreateTemp(bool interXact);
extern void BufFileClose(BufFile *file);
extern size_t BufFileRead(BufFile *file, void *ptr, size_t size);
extern size_t BufFileWrite(BufFile *file, void *ptr, size_t size);
extern int	BufFileSeek(BufFile *file, int fileno, off_t offset, int whence);
extern void BufFileTell(BufFile *file, int *fileno, off_t *offset);
extern int	BufFileSeekBlock(BufFile *file, long blknum);
extern int64 BufFileSize(BufFile *file);
extern long BufFileAppend(BufFile *target, BufFile *source);

extern BufFile *BufFileCreateShared(SharedFileSet *fileset, const char *name);
extern void BufFileExportShared(BufFile *file);
extern BufFile *BufFileOpenShared(SharedFileSet *fileset, const char *name);
extern void BufFileDeleteShared(SharedFileSet *fileset, const char *name);

extern TransientBufFile *BufFileOpenTransient(const char *path, int fileFlags);
extern void BufFileCloseTransient(TransientBufFile *file);
extern size_t BufFileReadTransient(TransientBufFile *file, void *ptr,
					 size_t size);
extern size_t BufFileWriteTransient(TransientBufFile *file, void *ptr,
					  size_t size);

#endif							/* BUFFILE_H */
