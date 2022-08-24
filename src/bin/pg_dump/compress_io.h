/*-------------------------------------------------------------------------
 *
 * compress_io.h
 *	 Interface to compress_io.c routines
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_io.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __COMPRESS_IO__
#define __COMPRESS_IO__

#include "pg_backup_archiver.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#define GZCLOSE(fh) gzclose(fh)
#define GZWRITE(p, s, n, fh) gzwrite(fh, p, (n) * (s))
#define GZREAD(p, s, n, fh) gzread(fh, p, (n) * (s))
#define GZEOF(fh)	gzeof(fh)
#else
#define GZCLOSE(fh) fclose(fh)
#define GZWRITE(p, s, n, fh) (fwrite(p, s, n, fh) * (s))
#define GZREAD(p, s, n, fh) fread(p, s, n, fh)
#define GZEOF(fh)	feof(fh)
/* this is just the redefinition of a libz constant */
#define Z_DEFAULT_COMPRESSION (-1)
#endif

/* Initial buffer sizes used in zlib compression. */
#define ZLIB_OUT_SIZE	4096
#define ZLIB_IN_SIZE	4096

/* Prototype for callback function to WriteDataToArchive() */
typedef void (*WriteFunc) (ArchiveHandle *AH, const char *buf, size_t len);

/*
 * Prototype for callback function to ReadDataFromArchive()
 *
 * ReadDataFromArchive will call the read function repeatedly, until it
 * returns 0 to signal EOF. ReadDataFromArchive passes a buffer to read the
 * data into in *buf, of length *buflen. If that's not big enough for the
 * callback function, it can free() it and malloc() a new one, returning the
 * new buffer and its size in *buf and *buflen.
 *
 * Returns the number of bytes read into *buf, or 0 on EOF.
 */
typedef size_t (*ReadFunc) (ArchiveHandle *AH, char **buf, size_t *buflen);

typedef struct CompressorState CompressorState;
struct CompressorState
{
	/*
	 * Read all compressed data from the input stream (via readF) and print it
	 * out with ahwrite().
	 */
	void		(*readData) (ArchiveHandle *AH, CompressorState *cs);

	/*
	 * Compress and write data to the output stream (via writeF).
	 */
	void		(*writeData) (ArchiveHandle *AH, CompressorState *cs,
							  const void *data, size_t dLen);
	void		(*end) (ArchiveHandle *AH, CompressorState *cs);

	ReadFunc	readF;
	WriteFunc	writeF;

	void	   *private;
};

extern CompressorState *AllocateCompressor(const pg_compress_specification compress_spec,
										   ReadFunc readF,
										   WriteFunc writeF);
extern void EndCompressor(ArchiveHandle *AH, CompressorState *cs);

/*
 * Compress File Handle
 */
typedef struct CompressFileHandle CompressFileHandle;

struct CompressFileHandle
{
	int			(*open) (const char *path, int fd, const char *mode,
						 CompressFileHandle * CFH);
	int			(*open_write) (const char *path, const char *mode,
							   CompressFileHandle * cxt);
	size_t		(*read) (void *ptr, size_t size, CompressFileHandle * CFH);
	size_t		(*write) (const void *ptr, size_t size,
						  struct CompressFileHandle *CFH);
	char	   *(*gets) (char *s, int size, CompressFileHandle * CFH);
	int			(*getc) (CompressFileHandle * CFH);
	int			(*eof) (CompressFileHandle * CFH);
	int			(*close) (CompressFileHandle * CFH);
	const char *(*get_error) (CompressFileHandle * CFH);

	void	   *private;
};


extern CompressFileHandle * InitCompressFileHandle(const pg_compress_specification compress_spec);

extern CompressFileHandle * InitDiscoverCompressFileHandle(const char *path,
														   const char *mode);

extern int	DestroyCompressFileHandle(CompressFileHandle * CFH);
#endif
