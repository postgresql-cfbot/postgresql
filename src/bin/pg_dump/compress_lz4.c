#include "postgres_fe.h"
#include "pg_backup_utils.h"

#include "compress_lz4.h"

#ifdef USE_LZ4
#include <lz4.h>
#include <lz4frame.h>

#define LZ4_OUT_SIZE	(4 * 1024)
#define LZ4_IN_SIZE		(16 * 1024)

/*----------------------
 * Compressor API
 *----------------------
 */

typedef struct LZ4CompressorState
{
	char	   *outbuf;
	size_t		outsize;
}			LZ4CompressorState;

/* Private routines that support LZ4 compressed data I/O */
static void ReadDataFromArchiveLZ4(ArchiveHandle *AH, CompressorState *cs);
static void WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs,
								  const void *data, size_t dLen);
static void EndCompressorLZ4(ArchiveHandle *AH, CompressorState *cs);

static void
ReadDataFromArchiveLZ4(ArchiveHandle *AH, CompressorState *cs)
{
	LZ4_streamDecode_t lz4StreamDecode;
	char	   *buf;
	char	   *decbuf;
	size_t		buflen;
	size_t		cnt;

	buflen = LZ4_IN_SIZE;
	buf = pg_malloc(buflen);
	decbuf = pg_malloc(buflen);

	LZ4_setStreamDecode(&lz4StreamDecode, NULL, 0);

	while ((cnt = cs->readF(AH, &buf, &buflen)))
	{
		int			decBytes = LZ4_decompress_safe_continue(&lz4StreamDecode,
															buf, decbuf,
															cnt, buflen);

		ahwrite(decbuf, 1, decBytes, AH);
	}

	pg_free(buf);
	pg_free(decbuf);
}

static void
WriteDataToArchiveLZ4(ArchiveHandle *AH, CompressorState *cs,
					  const void *data, size_t dLen)
{
	LZ4CompressorState *LZ4cs = (LZ4CompressorState *) cs->private;
	size_t		compressed;
	size_t		requiredsize = LZ4_compressBound(dLen);

	if (requiredsize > LZ4cs->outsize)
	{
		LZ4cs->outbuf = pg_realloc(LZ4cs->outbuf, requiredsize);
		LZ4cs->outsize = requiredsize;
	}

	compressed = LZ4_compress_default(data, LZ4cs->outbuf,
									  dLen, LZ4cs->outsize);

	if (compressed <= 0)
		pg_fatal("failed to LZ4 compress data");

	cs->writeF(AH, LZ4cs->outbuf, compressed);
}

static void
EndCompressorLZ4(ArchiveHandle *AH, CompressorState *cs)
{
	LZ4CompressorState *LZ4cs;

	LZ4cs = (LZ4CompressorState *) cs->private;
	if (LZ4cs)
	{
		pg_free(LZ4cs->outbuf);
		pg_free(LZ4cs);
		cs->private = NULL;
	}
}


/* Public routines that support LZ4 compressed data I/O */
void
InitCompressorLZ4(CompressorState *cs, int compressionLevel)
{
	cs->readData = ReadDataFromArchiveLZ4;
	cs->writeData = WriteDataToArchiveLZ4;
	cs->end = EndCompressorLZ4;

	/* Will be lazy init'd */
	cs->private = pg_malloc0(sizeof(LZ4CompressorState));
}

/*----------------------
 * Compress File API
 *----------------------
 */

/*
 * State needed for LZ4 (de)compression using the CompressFileHandle API.
 */
typedef struct LZ4File
{
	FILE	   *fp;

	LZ4F_preferences_t prefs;

	LZ4F_compressionContext_t ctx;
	LZ4F_decompressionContext_t dtx;

	bool		inited;
	bool		compressing;

	size_t		buflen;
	char	   *buffer;

	size_t		overflowalloclen;
	size_t		overflowlen;
	char	   *overflowbuf;

	size_t		errcode;
}			LZ4File;

/*
 * LZ4 equivalent to feof() or gzeof(). The end of file
 * is reached if there is no decompressed output in the
 * overflow buffer and the end of the file is reached.
 */
static int
LZ4File_eof(CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;

	return fs->overflowlen == 0 && feof(fs->fp);
}

static const char *
LZ4File_get_error(CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;
	const char *errmsg;

	if (LZ4F_isError(fs->errcode))
		errmsg = LZ4F_getErrorName(fs->errcode);
	else
		errmsg = strerror(errno);

	return errmsg;
}

/*
 * Prepare an already alloc'ed LZ4File struct for subsequent calls.
 *
 * It creates the nessary contexts for the operations. When compressing,
 * it additionally writes the LZ4 header in the output stream.
 */
static int
LZ4File_init(LZ4File * fs, int size, bool compressing)
{
	size_t		status;

	if (fs->inited)
		return 0;

	fs->compressing = compressing;
	fs->inited = true;

	if (fs->compressing)
	{
		fs->buflen = LZ4F_compressBound(LZ4_IN_SIZE, &fs->prefs);
		if (fs->buflen < LZ4F_HEADER_SIZE_MAX)
			fs->buflen = LZ4F_HEADER_SIZE_MAX;

		status = LZ4F_createCompressionContext(&fs->ctx, LZ4F_VERSION);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		fs->buffer = pg_malloc(fs->buflen);
		status = LZ4F_compressBegin(fs->ctx, fs->buffer, fs->buflen,
									&fs->prefs);

		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		if (fwrite(fs->buffer, 1, status, fs->fp) != status)
		{
			errno = errno ? : ENOSPC;
			return 1;
		}
	}
	else
	{
		status = LZ4F_createDecompressionContext(&fs->dtx, LZ4F_VERSION);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return 1;
		}

		fs->buflen = size > LZ4_OUT_SIZE ? size : LZ4_OUT_SIZE;
		fs->buffer = pg_malloc(fs->buflen);

		fs->overflowalloclen = fs->buflen;
		fs->overflowbuf = pg_malloc(fs->overflowalloclen);
		fs->overflowlen = 0;
	}

	return 0;
}

/*
 * Read already decompressed content from the overflow buffer into 'ptr' up to
 * 'size' bytes, if available. If the eol_flag is set, then stop at the first
 * occurance of the new line char prior to 'size' bytes.
 *
 * Any unread content in the overflow buffer, is moved to the beginning.
 */
static int
LZ4File_read_overflow(LZ4File * fs, void *ptr, int size, bool eol_flag)
{
	char	   *p;
	int			readlen = 0;

	if (fs->overflowlen == 0)
		return 0;

	if (fs->overflowlen >= size)
		readlen = size;
	else
		readlen = fs->overflowlen;

	if (eol_flag && (p = memchr(fs->overflowbuf, '\n', readlen)))
		/* Include the line terminating char */
		readlen = p - fs->overflowbuf + 1;

	memcpy(ptr, fs->overflowbuf, readlen);
	fs->overflowlen -= readlen;

	if (fs->overflowlen > 0)
		memmove(fs->overflowbuf, fs->overflowbuf + readlen, fs->overflowlen);

	return readlen;
}

/*
 * The workhorse for reading decompressed content out of an LZ4 compressed
 * stream.
 *
 * It will read up to 'ptrsize' decompressed content, or up to the new line char
 * if found first when the eol_flag is set. It is possible that the decompressed
 * output generated by reading any compressed input via the LZ4F API, exceeds
 * 'ptrsize'. Any exceeding decompressed content is stored at an overflow
 * buffer within LZ4File. Of course, when the function is called, it will first
 * try to consume any decompressed content already present in the overflow
 * buffer, before decompressing new content.
 */
static int
LZ4File_read_internal(LZ4File * fs, void *ptr, int ptrsize, bool eol_flag)
{
	size_t		dsize = 0;
	size_t		rsize;
	size_t		size = ptrsize;
	bool		eol_found = false;

	void	   *readbuf;

	/* Lazy init */
	if (!fs->inited && LZ4File_init(fs, size, false /* decompressing */ ))
		return -1;

	/* Verfiy that there is enough space in the outbuf */
	if (size > fs->buflen)
	{
		fs->buflen = size;
		fs->buffer = pg_realloc(fs->buffer, size);
	}

	/* use already decompressed content if available */
	dsize = LZ4File_read_overflow(fs, ptr, size, eol_flag);
	if (dsize == size || (eol_flag && memchr(ptr, '\n', dsize)))
		return dsize;

	readbuf = pg_malloc(size);

	do
	{
		char	   *rp;
		char	   *rend;

		rsize = fread(readbuf, 1, size, fs->fp);
		if (rsize < size && !feof(fs->fp))
			return -1;

		rp = (char *) readbuf;
		rend = (char *) readbuf + rsize;

		while (rp < rend)
		{
			size_t		status;
			size_t		outlen = fs->buflen;
			size_t		read_remain = rend - rp;

			memset(fs->buffer, 0, outlen);
			status = LZ4F_decompress(fs->dtx, fs->buffer, &outlen,
									 rp, &read_remain, NULL);
			if (LZ4F_isError(status))
			{
				fs->errcode = status;
				return -1;
			}

			rp += read_remain;

			/*
			 * fill in what space is available in ptr if the eol flag is set,
			 * either skip if one already found or fill up to EOL if present
			 * in the outbuf
			 */
			if (outlen > 0 && dsize < size && eol_found == false)
			{
				char	   *p;
				size_t		lib = (eol_flag == 0) ? size - dsize : size - 1 - dsize;
				size_t		len = outlen < lib ? outlen : lib;

				if (eol_flag == true &&
					(p = memchr(fs->buffer, '\n', outlen)) &&
					(size_t) (p - fs->buffer + 1) <= len)
				{
					len = p - fs->buffer + 1;
					eol_found = true;
				}

				memcpy((char *) ptr + dsize, fs->buffer, len);
				dsize += len;

				/* move what did not fit, if any, at the begining of the buf */
				if (len < outlen)
					memmove(fs->buffer, fs->buffer + len, outlen - len);
				outlen -= len;
			}

			/* if there is available output, save it */
			if (outlen > 0)
			{
				while (fs->overflowlen + outlen > fs->overflowalloclen)
				{
					fs->overflowalloclen *= 2;
					fs->overflowbuf = pg_realloc(fs->overflowbuf,
												 fs->overflowalloclen);
				}

				memcpy(fs->overflowbuf + fs->overflowlen, fs->buffer, outlen);
				fs->overflowlen += outlen;
			}
		}
	} while (rsize == size && dsize < size && eol_found == 0);

	pg_free(readbuf);

	return (int) dsize;
}

/*
 * Compress size bytes from ptr and write them to the stream.
 */
static size_t
LZ4File_write(const void *ptr, size_t size, CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;
	size_t		status;
	int			remaining = size;

	if (!fs->inited && LZ4File_init(fs, size, true))
		return -1;

	while (remaining > 0)
	{
		int			chunk = remaining < LZ4_IN_SIZE ? remaining : LZ4_IN_SIZE;

		remaining -= chunk;

		status = LZ4F_compressUpdate(fs->ctx, fs->buffer, fs->buflen,
									 ptr, chunk, NULL);
		if (LZ4F_isError(status))
		{
			fs->errcode = status;
			return -1;
		}

		if (fwrite(fs->buffer, 1, status, fs->fp) != status)
		{
			errno = errno ? : ENOSPC;
			return 1;
		}
	}

	return size;
}

/*
 * fread() equivalent implementation for LZ4 compressed files.
 */
static size_t
LZ4File_read(void *ptr, size_t size, CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;
	int			ret;

	ret = LZ4File_read_internal(fs, ptr, size, false);
	if (ret != size && !LZ4File_eof(CFH))
		pg_fatal("could not read from input file: %s", LZ4File_get_error(CFH));

	return ret;
}

/*
 * fgetc() equivalent implementation for LZ4 compressed files.
 */
static int
LZ4File_getc(CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;
	unsigned char c;

	if (LZ4File_read_internal(fs, &c, 1, false) != 1)
	{
		if (!LZ4File_eof(CFH))
			pg_fatal("could not read from input file: %s", LZ4File_get_error(CFH));
		else
			pg_fatal("could not read from input file: end of file");
	}

	return c;
}

/*
 * fgets() equivalent implementation for LZ4 compressed files.
 */
static char *
LZ4File_gets(char *ptr, int size, CompressFileHandle * CFH)
{
	LZ4File    *fs = (LZ4File *) CFH->private;
	size_t		dsize;

	dsize = LZ4File_read_internal(fs, ptr, size, true);
	if (dsize < 0)
		pg_fatal("could not read from input file: %s", LZ4File_get_error(CFH));

	/* Done reading */
	if (dsize == 0)
		return NULL;

	return ptr;
}

/*
 * Finalize (de)compression of a stream. When compressing it will write any
 * remaining content and/or generated footer from the LZ4 API.
 */
static int
LZ4File_close(CompressFileHandle * CFH)
{
	FILE	   *fp;
	LZ4File    *fs = (LZ4File *) CFH->private;
	size_t		status;
	int			ret;

	fp = fs->fp;
	if (fs->inited)
	{
		if (fs->compressing)
		{
			status = LZ4F_compressEnd(fs->ctx, fs->buffer, fs->buflen, NULL);
			if (LZ4F_isError(status))
				pg_fatal("failed to end compression: %s",
						 LZ4F_getErrorName(status));
			else if ((ret = fwrite(fs->buffer, 1, status, fs->fp)) != status)
			{
				errno = errno ? : ENOSPC;
				WRITE_ERROR_EXIT;
			}

			status = LZ4F_freeCompressionContext(fs->ctx);
			if (LZ4F_isError(status))
				pg_fatal("failed to end compression: %s",
						 LZ4F_getErrorName(status));
		}
		else
		{
			status = LZ4F_freeDecompressionContext(fs->dtx);
			if (LZ4F_isError(status))
				pg_fatal("failed to end decompression: %s",
						 LZ4F_getErrorName(status));
			pg_free(fs->overflowbuf);
		}

		pg_free(fs->buffer);
	}

	pg_free(fs);

	return fclose(fp);
}

static int
LZ4File_open(const char *path, int fd, const char *mode,
			 CompressFileHandle * CFH)
{
	FILE	   *fp;
	LZ4File    *lz4fp = (LZ4File *) CFH->private;

	if (fd >= 0)
		fp = fdopen(fd, mode);
	else
		fp = fopen(path, mode);
	if (fp == NULL)
	{
		lz4fp->errcode = errno;
		return 1;
	}

	lz4fp->fp = fp;

	return 0;
}

static int
LZ4File_open_write(const char *path, const char *mode, CompressFileHandle * CFH)
{
	char	   *fname;
	int			ret;

	fname = psprintf("%s.lz4", path);
	ret = CFH->open(fname, -1, mode, CFH);
	pg_free(fname);

	return ret;
}

void
InitCompressLZ4(CompressFileHandle * CFH, int compressionLevel)
{
	LZ4File    *lz4fp;

	CFH->open = LZ4File_open;
	CFH->open_write = LZ4File_open_write;
	CFH->read = LZ4File_read;
	CFH->write = LZ4File_write;
	CFH->gets = LZ4File_gets;
	CFH->getc = LZ4File_getc;
	CFH->eof = LZ4File_eof;
	CFH->close = LZ4File_close;
	CFH->get_error = LZ4File_get_error;

	lz4fp = pg_malloc0(sizeof(*lz4fp));
	if (compressionLevel >= 0)
		lz4fp->prefs.compressionLevel = compressionLevel;

	CFH->private = lz4fp;
}
#else							/* USE_LZ4 */
void
InitCompressorLZ4(CompressorState *cs, int compressionLevel)
{
	pg_fatal("not built with LZ4 support");
}

void
InitCompressLZ4(CompressFileHandle * CFH, int compressionLevel)
{
	pg_fatal("not built with LZ4 support");
}
#endif							/* USE_LZ4 */
