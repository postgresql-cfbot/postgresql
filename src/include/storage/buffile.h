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
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buffile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BUFFILE_H
#define BUFFILE_H

#include "storage/fileset.h"

/* BufFile is an opaque type whose details are not known outside buffile.c. */
typedef struct BufFile BufFile;

/* BufFileStream needs to know how to treat the file descriptor. */
typedef enum BufFileStreamFileKind
{
	/* A "raw descriptor", typically obtained by BasicOpenFile(). */
	BFS_FILE_FD,

	/* A "raw descriptor", obtained by OpenTransientFile() */
	BFS_FILE_FD_TRANSIENT,

	/* A "virtual descriptor". */
	BFS_FILE_VFD
} BufFileStreamFileKind;

/* The file information for external users of BufFileStream. */
typedef struct BufFileStreamFile
{
	BufFileStreamFileKind kind;

	union
	{
		int			fd;
		File		vfd;
	}			f;

	char		name[MAXPGPATH];

	uint32		wait_event_info;
} BufFileStreamFile;

/*
 * This is our counterpart of the FILE structure, as defined in
 * <stdio.h>. Users of buffile.c are supposed to perform reads/writes via this
 * stream.
 */
typedef struct BufFileStream
{
	/*
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	/* TODO Rename to "offset"? Or "bufOffset"? */
	off_t		curOffset;		/* offset of the buffer in the file */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */
	int			bufsize;		/* maximum # of valid bytes in buffer */
	/* TODO Reconsider what the reasonable minimum size is. */
#define		BUFSIZE_MIN			64
#define		BUFSIZE_MAX			BLCKSZ
	PGAlignedBlock buffer;
	bool		dirty;			/* does buffer need to be written? */

	int			elevel;			/* error level to raise */

	/* the underlying file - not used if the stream is inside BufFile. */
	BufFileStreamFile file;
} BufFileStream;

/*
 * prototypes for functions in buffile.c
 */

#define BufFileStreamIsOpen(s) (((s).file.kind == BFS_FILE_FD && (s).file.f.fd != -1) || \
								((s).file.kind == BFS_FILE_VFD && (s).file.f.vfd != -1))

extern void BufFileStreamInitFD(BufFileStream *stream, int fd, bool transient,
								const char *name, uint32 wait_event_info,
								int bufsize, int elevel);
extern void BufFileStreamInitVFD(BufFileStream *stream, File vfd, char *name,
								 uint32 wait_event_info, int bufsize,
								 int elevel);
extern int	BufFileStreamClose(BufFileStream *stream, uint32 sync_event);
extern BufFile *BufFileCreateTemp(bool interXact);
extern void BufFileClose(BufFile *file);
extern size_t BufFileRead(BufFile *file, void *ptr, size_t size);
extern size_t BufFileStreamRead(BufFileStream *stream, void *ptr,
								size_t size);
extern void BufFileWrite(BufFile *file, void *ptr, size_t size);
extern size_t BufFileStreamWrite(BufFileStream *stream, void *ptr, size_t size);
extern int	BufFileSeek(BufFile *file, int fileno, off_t offset, int whence);
extern int	BufFileStreamSeek(BufFileStream *stream, off_t offset, int whence);
extern void BufFileTell(BufFile *file, int *fileno, off_t *offset);
extern int	BufFileSeekBlock(BufFile *file, long blknum);
extern int64 BufFileSize(BufFile *file);
extern long BufFileAppend(BufFile *target, BufFile *source);

extern BufFile *BufFileCreateFileSet(FileSet *fileset, const char *name);
extern void BufFileExportFileSet(BufFile *file);
extern BufFile *BufFileOpenFileSet(FileSet *fileset, const char *name,
								   int mode, bool missing_ok);
extern void BufFileDeleteFileSet(FileSet *fileset, const char *name,
								 bool missing_ok);
extern void BufFileTruncateFileSet(BufFile *file, int fileno, off_t offset);

/*
 * Information on a source or destination file to be processed by
 * BufFileCopyFD().
 */
typedef struct BufFileCopyFDArg
{
	int			fd;				/* file descriptor */
	char	   *path;			/* file path */
	int			wait_event_info;	/* event reporting info (see wait_event.h_ */
} BufFileCopyFDArg;

extern size_t BufFileCopyFD(BufFileCopyFDArg *dst, BufFileCopyFDArg *src,
							int bufsize, size_t upto, int flush_distance,
							bool unlink_dst);
extern void BufFileWriteFD(BufFileCopyFDArg *dst, char *buffer, int nbytes,
						   bool unlink_dst);

#endif							/* BUFFILE_H */
