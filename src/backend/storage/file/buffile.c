/*-------------------------------------------------------------------------
 *
 * buffile.c
 *	  Management of large buffered temporary files.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/buffile.c
 *
 * NOTES:
 *
 * BufFiles provide a very incomplete emulation of stdio atop virtual Files
 * (as managed by fd.c).  Currently, we only support the buffered-I/O
 * aspect of stdio: a read or write of the low-level File occurs only
 * when the buffer is filled or emptied.  This is an even bigger win
 * for virtual Files than for ordinary kernel files, since reducing the
 * frequency with which a virtual File is touched reduces "thrashing"
 * of opening/closing file descriptors.
 *
 * Note that BufFile structs are allocated with palloc(), and therefore
 * will go away automatically at query/transaction end.  Since the underlying
 * virtual Files are made with OpenTemporaryFile, all resources for
 * the file are certain to be cleaned up even if processing is aborted
 * by ereport(ERROR).  The data structures required are made in the
 * palloc context that was current when the BufFile was created, and
 * any external resources such as temp files are owned by the ResourceOwner
 * that was current at that time.
 *
 * BufFile also supports temporary files that exceed the OS file size limit
 * (by opening multiple fd.c temporary files).  This is an essential feature
 * for sorts and hashjoins on large amounts of data.
 *
 * BufFile supports temporary files that can be shared with other backends, as
 * infrastructure for parallel execution.  Such files need to be created as a
 * member of a SharedFileSet that all participants are attached to.
 *
 * BufFile also supports temporary files that can be used by the single backend
 * when the corresponding files need to be survived across the transaction and
 * need to be opened and closed multiple times.  Such files need to be created
 * as a member of a FileSet.
 *
 * The BufFileStream structure is used internally to implement the buffering
 * of temporary files, but it can also be used for any file whose descriptor
 * was opened by the fd.c API. Once the stream is initialized (e.g. using
 * BufFileStreamInitFD()), functions like BufFileStreamWrite(),
 * BufFileStreamRead() and BufFileStreamSeek() can be used to access the file
 * data. Following are a few benefits to consider:
 *
 * 1. Less code is needed to access the file.
 *
 * 2.. Buffering reduces the number of I/O system calls.
 *
 * 3. The buffer size can be controlled by the user. (The larger the buffer,
 * the fewer I/O system calls are needed, but the more data needs to be
 * written to the buffer before the user recognizes that the file access
 * failed.)
 *
 * 4. It should make features like Transparent Data Encryption less invasive.
 *
 *
 * BufFileCopy() is a function to copy data from one descriptor to another
 * one. It's located in this module because it also uses buffered I/O.
 * -------------------------------------------------------------------------
 */

#include <unistd.h>

#include "postgres.h"

#include "commands/tablespace.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf_internals.h"
#include "storage/buffile.h"
#include "storage/fd.h"
#include "utils/resowner.h"

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large BufFiles to be spread across multiple
 * tablespaces when available.
 */
#define MAX_PHYSICAL_FILESIZE	0x40000000
/* The number of bufers (blocks) in a single segment file. */
#define BUFFILE_SEG_SIZE(file)	(MAX_PHYSICAL_FILESIZE / ((file)->stream.bufsize))

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile
{
	/*
	 * Our API (BufFileRead(), BufFileWrite(), etc.) uses this structure as an
	 * argument to access the individual physical files. Thus the same API can
	 * be used to access other files than those represented by BufFile.
	 */
	BufFileStream stream;

	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File	   *files;			/* palloc'd array with numFiles entries */

	bool		isInterXact;	/* keep open over transactions? */
	bool		readOnly;		/* has the file been set to read only? */

	FileSet    *fileset;		/* space for fileset based segment files */
	const char *name;			/* name of fileset based BufFile */

	/*
	 * resowner is the ResourceOwner to use for underlying temp files.  (We
	 * don't need to remember the memory context we're using explicitly,
	 * because after creation we only repalloc our arrays larger.)
	 */
	ResourceOwner resowner;

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int			curFile;		/* file index (0..n) part of current pos */

};

static BufFile *makeBufFileCommon(int nfiles);
static BufFile *makeBufFile(File firstfile);
static void BufFileStreamInitCommon(BufFileStream *stream, const char *name,
									int bufsize, int elevel);
static void extendBufFile(BufFile *file);
static void BufFileLoadBuffer(BufFileStream *stream, BufFile *file);
static void BufFileStreamLoadBuffer(BufFileStream *stream);
static void BufFileDumpBuffer(BufFileStream *stream, BufFile *file);
static void BufFileStreamDumpBuffer(BufFileStream *stream);
static size_t BufFileReadCommon(BufFileStream *stream,
								BufFile *file, void *ptr, size_t size);
static size_t BufFileWriteCommon(BufFileStream *stream,
								 BufFile *file, void *ptr, size_t size);
static void BufFileFlush(BufFileStream *stream, BufFile *file);
static File MakeNewFileSetSegment(BufFile *buffile, int segment);

/*
 * Create BufFile and perform the common initialization.
 */
static BufFile *
makeBufFileCommon(int nfiles)
{
	BufFile    *file = (BufFile *) palloc(sizeof(BufFile));

	file->numFiles = nfiles;
	file->curFile = 0;

	BufFileStreamInitCommon(&file->stream, NULL, BLCKSZ, ERROR);

	file->isInterXact = false;
	file->resowner = CurrentResourceOwner;
	return file;
}

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isInterXact if appropriate.
 */
static BufFile *
makeBufFile(File firstfile)
{
	BufFile    *file = makeBufFileCommon(1);

	file->files = (File *) palloc(sizeof(File));
	file->files[0] = firstfile;
	file->readOnly = false;
	file->fileset = NULL;
	file->name = NULL;

	return file;
}

static void
BufFileStreamInitCommon(BufFileStream *stream, const char *name, int bufsize,
						int elevel)
{
	/*
	 * If bufsize is not a whole multiple of MAX_PHYSICAL_FILESIZE,
	 * BufFileTellBlock() and BufFileSeekBlock() wont' work.
	 */
	if (bufsize < BUFSIZE_MIN || bufsize > BUFSIZE_MAX ||
		(MAX_PHYSICAL_FILESIZE % bufsize) != 0)
		ereport(ERROR, (errmsg("%d is not a valid buffer size", bufsize)));
	stream->bufsize = bufsize;

	stream->curOffset = 0L;
	stream->pos = 0;
	stream->nbytes = 0;
	stream->dirty = false;
	stream->elevel = elevel;

	if (name)
		strlcpy(stream->file.name, name, MAXPGPATH);
}

/*
 * Initialize the stream using a raw file descriptor "fd".
 *
 * If "transient" is true, OpenTransientFile() was used to obtain it, and thus
 * CloseTransientFile() needs to be used to close it. Otherwise we can close
 * the descriptor simply by close().
 */
void
BufFileStreamInitFD(BufFileStream *stream, int fd, bool transient,
					const char *name, uint32 wait_event_info, int bufsize,
					int elevel)
{
	BufFileStreamInitCommon(stream, name, bufsize, elevel);
	stream->file.kind = transient ? BFS_FILE_FD_TRANSIENT : BFS_FILE_FD;
	stream->file.f.fd = fd;
	stream->file.wait_event_info = wait_event_info;
}

/*
 * Initialize the stream using a virtual file descriptor "vfd".
 */
void
BufFileStreamInitVFD(BufFileStream *stream, File vfd, char *name,
					 uint32 wait_event_info, int bufsize, int elevel)
{
	BufFileStreamInitCommon(stream, name, bufsize, elevel);
	stream->file.kind = BFS_FILE_VFD;
	stream->file.f.vfd = vfd;
	stream->file.wait_event_info = wait_event_info;
}

/*
 * Dump the stream's buffer and close the underlying file.
 *
 * If sync_event is non-zero, fsync the file before closing and pass this
 * value to the stats collector. XXX Currently we don't support this for VFD -
 * should we do?
 *
 * Returns the return value of the underlying close() system call.
 *
 * XXX Currently we always return 0 for BFS_FILE_VFD. (Closing of the virtual
 * file descriptor does not necessarily imply closing of the kernel file
 * descriptor.)
 */
int
BufFileStreamClose(BufFileStream *stream, uint32 sync_event)
{
	int			res;

	if (stream->dirty)
	{
		BufFileStreamDumpBuffer(stream);
		if (stream->dirty)
			/* Only reachable if stream->elevel < ERROR. */
			return -1;
	}

	if (sync_event > 0)
	{
		Assert(stream->file.kind != BFS_FILE_VFD);

		pgstat_report_wait_start(sync_event);
		if (pg_fsync(stream->file.f.fd) != 0)
			ereport(data_sync_elevel(stream->elevel),
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							stream->file.name)));
		/* If stream->elevel < ERROR, handle closing below. */
		pgstat_report_wait_end();
	}

	if (stream->file.kind == BFS_FILE_VFD)
	{
		FileClose(stream->file.f.vfd);

		/* See the header comment of the function. */
		res = 0;

		stream->file.f.vfd = -1;
	}
	else
	{
		if (stream->file.kind == BFS_FILE_FD_TRANSIENT)
			res = CloseTransientFile(stream->file.f.fd);
		else
		{
			Assert(stream->file.kind == BFS_FILE_FD);

			res = close(stream->file.f.fd);
		}

		stream->file.f.fd = -1;
	}

	return res;
}

/*
 * Add another component temp file.
 */
static void
extendBufFile(BufFile *file)
{
	File		pfile;
	ResourceOwner oldowner;

	/* Be sure to associate the file with the BufFile's resource owner */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = file->resowner;

	if (file->fileset == NULL)
		pfile = OpenTemporaryFile(file->isInterXact);
	else
		pfile = MakeNewFileSetSegment(file, file->numFiles);

	Assert(pfile >= 0);

	CurrentResourceOwner = oldowner;

	file->files = (File *) repalloc(file->files,
									(file->numFiles + 1) * sizeof(File));
	file->files[file->numFiles] = pfile;
	file->numFiles++;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than MAX_PHYSICAL_FILESIZE bytes are
 * written to it).
 *
 * If interXact is true, the temp file will not be automatically deleted
 * at end of transaction.
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context, and with a resource owner, that will survive across
 * transaction boundaries.
 */
BufFile *
BufFileCreateTemp(bool interXact)
{
	BufFile    *file;
	File		pfile;

	/*
	 * Ensure that temp tablespaces are set up for OpenTemporaryFile to use.
	 * Possibly the caller will have done this already, but it seems useful to
	 * double-check here.  Failure to do this at all would result in the temp
	 * files always getting placed in the default tablespace, which is a
	 * pretty hard-to-detect bug.  Callers may prefer to do it earlier if they
	 * want to be sure that any required catalog access is done in some other
	 * resource context.
	 */
	PrepareTempTablespaces();

	pfile = OpenTemporaryFile(interXact);
	Assert(pfile >= 0);

	file = makeBufFile(pfile);
	file->isInterXact = interXact;

	return file;
}

/*
 * Build the name for a given segment of a given BufFile.
 */
static void
FileSetSegmentName(char *name, const char *buffile_name, int segment)
{
	snprintf(name, MAXPGPATH, "%s.%d", buffile_name, segment);
}

/*
 * Create a new segment file backing a fileset based BufFile.
 */
static File
MakeNewFileSetSegment(BufFile *buffile, int segment)
{
	char		name[MAXPGPATH];
	File		file;

	/*
	 * It is possible that there are files left over from before a crash
	 * restart with the same name.  In order for BufFileOpenFileSet() not to
	 * get confused about how many segments there are, we'll unlink the next
	 * segment number if it already exists.
	 */
	FileSetSegmentName(name, buffile->name, segment + 1);
	FileSetDelete(buffile->fileset, name, true);

	/* Create the new segment. */
	FileSetSegmentName(name, buffile->name, segment);
	file = FileSetCreate(buffile->fileset, name);

	/* FileSetCreate would've errored out */
	Assert(file > 0);

	return file;
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are attached to the same SharedFileSet using the same name.
 *
 * The naming scheme for fileset based BufFiles is left up to the calling code.
 * The name will appear as part of one or more filenames on disk, and might
 * provide clues to administrators about which subsystem is generating
 * temporary file data.  Since each SharedFileSet object is backed by one or
 * more uniquely named temporary directory, names don't conflict with
 * unrelated SharedFileSet objects.
 */
BufFile *
BufFileCreateFileSet(FileSet *fileset, const char *name)
{
	BufFile    *file;

	file = makeBufFileCommon(1);
	file->fileset = fileset;
	file->name = pstrdup(name);
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = MakeNewFileSetSegment(file, 0);
	file->readOnly = false;

	return file;
}

/*
 * Open a file that was previously created in another backend (or this one)
 * with BufFileCreateFileSet in the same FileSet using the same name.
 * The backend that created the file must have called BufFileClose() or
 * BufFileExportFileSet() to make sure that it is ready to be opened by other
 * backends and render it read-only.  If missing_ok is true, which indicates
 * that missing files can be safely ignored, then return NULL if the BufFile
 * with the given name is not found, otherwise, throw an error.
 */
BufFile *
BufFileOpenFileSet(FileSet *fileset, const char *name, int mode,
				   bool missing_ok)
{
	BufFile    *file;
	char		segment_name[MAXPGPATH];
	Size		capacity = 16;
	File	   *files;
	int			nfiles = 0;

	files = palloc(sizeof(File) * capacity);

	/*
	 * We don't know how many segments there are, so we'll probe the
	 * filesystem to find out.
	 */
	for (;;)
	{
		/* See if we need to expand our file segment array. */
		if (nfiles + 1 > capacity)
		{
			capacity *= 2;
			files = repalloc(files, sizeof(File) * capacity);
		}
		/* Try to load a segment. */
		FileSetSegmentName(segment_name, name, nfiles);
		files[nfiles] = FileSetOpen(fileset, segment_name, mode);
		if (files[nfiles] <= 0)
			break;
		++nfiles;

		CHECK_FOR_INTERRUPTS();
	}

	/*
	 * If we didn't find any files at all, then no BufFile exists with this
	 * name.
	 */
	if (nfiles == 0)
	{
		/* free the memory */
		pfree(files);

		if (missing_ok)
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open temporary file \"%s\" from BufFile \"%s\": %m",
						segment_name, name)));
	}

	file = makeBufFileCommon(nfiles);
	file->files = files;
	file->readOnly = (mode == O_RDONLY);
	file->fileset = fileset;
	file->name = pstrdup(name);

	return file;
}

/*
 * Delete a BufFile that was created by BufFileCreateFileSet in the given
 * FileSet using the given name.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the FileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given name, and should know
 * that it exists and has been exported or closed otherwise missing_ok should
 * be passed true.
 */
void
BufFileDeleteFileSet(FileSet *fileset, const char *name, bool missing_ok)
{
	char		segment_name[MAXPGPATH];
	int			segment = 0;
	bool		found = false;

	/*
	 * We don't know how many segments the file has.  We'll keep deleting
	 * until we run out.  If we don't manage to find even an initial segment,
	 * raise an error.
	 */
	for (;;)
	{
		FileSetSegmentName(segment_name, name, segment);
		if (!FileSetDelete(fileset, segment_name, true))
			break;
		found = true;
		++segment;

		CHECK_FOR_INTERRUPTS();
	}

	if (!found && !missing_ok)
		elog(ERROR, "could not delete unknown BufFile \"%s\"", name);
}

/*
 * BufFileExportFileSet --- flush and make read-only, in preparation for sharing.
 */
void
BufFileExportFileSet(BufFile *file)
{
	/* Must be a file belonging to a FileSet. */
	Assert(file->fileset != NULL);

	/* It's probably a bug if someone calls this twice. */
	Assert(!file->readOnly);

	BufFileFlush(&file->stream, file);
	file->readOnly = true;
}

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
void
BufFileClose(BufFile *file)
{
	int			i;

	/* flush any unwritten data */
	BufFileFlush(&file->stream, file);
	/* close and delete the underlying file(s) */
	for (i = 0; i < file->numFiles; i++)
		FileClose(file->files[i]);
	/* release the buffer space */
	pfree(file->files);
	pfree(file);
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, pos and nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
static void
BufFileLoadBuffer(BufFileStream *stream, BufFile *file)
{
	File		thisfile;
	instr_time	io_start;
	instr_time	io_time;

	/*
	 * Advance to next component file if necessary and possible.
	 */
	if (stream->curOffset >= MAX_PHYSICAL_FILESIZE &&
		file->curFile + 1 < file->numFiles)
	{
		file->curFile++;
		stream->curOffset = 0L;
	}

	thisfile = file->files[file->curFile];

	if (track_io_timing)
		INSTR_TIME_SET_CURRENT(io_start);

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	stream->nbytes = FileRead(thisfile,
							  stream->buffer.data,
							  stream->bufsize,
							  stream->curOffset,
							  WAIT_EVENT_BUFFILE_READ);
	if (stream->nbytes < 0)
	{
		stream->nbytes = 0;
		ereport(stream->elevel,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m",
						FilePathName(thisfile))));

		/* Only reachable if stream->elevel < ERROR. */
		return;
	}

	if (track_io_timing)
	{
		INSTR_TIME_SET_CURRENT(io_time);
		INSTR_TIME_SUBTRACT(io_time, io_start);
		INSTR_TIME_ADD(pgBufferUsage.temp_blk_read_time, io_time);
	}

	/* we choose not to advance curOffset here */

	if (stream->nbytes > 0)
		pgBufferUsage.temp_blks_read++;
}

/* Load buffer of a stream which is associated with a single physical file. */
static void
BufFileStreamLoadBuffer(BufFileStream *stream)
{
	char	   *buf = stream->buffer.data;
	int			bytestoread = stream->bufsize;

	if (stream->file.kind == BFS_FILE_VFD)
	{
		File		thisfile;

		/*
		 * Read whatever we can get, up to a full bufferload.
		 */
		thisfile = stream->file.f.vfd;
		stream->nbytes = FileRead(thisfile,
								  buf,
								  bytestoread,
								  stream->curOffset,
								  stream->file.wait_event_info);
		if (stream->nbytes < 0)
		{
			stream->nbytes = 0;
			ereport(stream->elevel,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							stream->file.name)));

			/* Only reachable if stream->elevel < ERROR. */
			return;
		}
	}
	else
	{
		int			thisfile;

		thisfile = stream->file.f.fd;

		do
		{
			errno = 0;

			pgstat_report_wait_start(stream->file.wait_event_info);
			stream->nbytes = pg_pread(thisfile, buf, bytestoread,
									  stream->curOffset);
			pgstat_report_wait_end();

			if (stream->nbytes < 0)
			{
				stream->nbytes = 0;

				if (errno == EINTR)
					continue;

				ereport(stream->elevel,
						(errcode_for_file_access(),
						 errmsg("could not read from file %s "
								"at offset %zu, length %u: %m",
								stream->file.name, stream->curOffset,
								bytestoread)));

				/* Only reachable if stream->elevel < ERROR. */
				return;
			}
			/* we choose not to advance curOffset here */

			/* Done. */
			break;
		} while (true);
	}
}

/*
 * BufFileDumpBuffer
 *
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
static void
BufFileDumpBuffer(BufFileStream *stream, BufFile *file)
{
	int			wpos = 0;
	int			bytestowrite;
	File		thisfile;

	/*
	 * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
	 * crosses a component-file boundary; so we need a loop.
	 */
	while (wpos < stream->nbytes)
	{
		off_t		availbytes;
		instr_time	io_start;
		instr_time	io_time;

		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (stream->curOffset >= MAX_PHYSICAL_FILESIZE)
		{
			while (file->curFile + 1 >= file->numFiles)
				extendBufFile(file);
			file->curFile++;
			stream->curOffset = 0L;
		}

		/*
		 * Determine how much we need to write into this file.
		 */
		bytestowrite = stream->nbytes - wpos;
		availbytes = MAX_PHYSICAL_FILESIZE - stream->curOffset;

		if ((off_t) bytestowrite > availbytes)
			bytestowrite = (int) availbytes;

		thisfile = file->files[file->curFile];

		if (track_io_timing)
			INSTR_TIME_SET_CURRENT(io_start);

		bytestowrite = FileWrite(thisfile,
								 stream->buffer.data + wpos,
								 bytestowrite,
								 stream->curOffset,
								 WAIT_EVENT_BUFFILE_WRITE,
								 stream->elevel);
		if (bytestowrite <= 0)
		{
			ereport(stream->elevel,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							FilePathName(thisfile))));

			/* Only reachable if stream->elevel < ERROR. */
			return;
		}

		if (track_io_timing)
		{
			INSTR_TIME_SET_CURRENT(io_time);
			INSTR_TIME_SUBTRACT(io_time, io_start);
			INSTR_TIME_ADD(pgBufferUsage.temp_blk_write_time, io_time);
		}

		stream->curOffset += bytestowrite;
		wpos += bytestowrite;

		pgBufferUsage.temp_blks_written++;
	}
	stream->dirty = false;

	/*
	 * At this point, curOffset has been advanced to the end of the buffer,
	 * ie, its original value + nbytes.  We need to make it point to the
	 * logical file position, ie, original value + pos, in case that is less
	 * (as could happen due to a small backwards seek in a dirty buffer!)
	 */
	stream->curOffset -= (stream->nbytes - stream->pos);
	if (stream->curOffset < 0)	/* handle possible segment crossing */
	{
		file->curFile--;
		Assert(file->curFile >= 0);
		stream->curOffset += MAX_PHYSICAL_FILESIZE;
	}

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	stream->pos = 0;
	stream->nbytes = 0;
}

/* Dump buffer of a stream which is associated with a single physical file. */
static void
BufFileStreamDumpBuffer(BufFileStream *stream)
{
	int			bytestowrite;

	/*
	 * Determine how much we need to write into the file.
	 */
	bytestowrite = stream->nbytes;

	if (stream->file.kind == BFS_FILE_VFD)
	{
		File		thisfile;

		thisfile = stream->file.f.vfd;
		bytestowrite = FileWrite(thisfile,
								 stream->buffer.data,
								 bytestowrite,
								 stream->curOffset,
								 stream->file.wait_event_info,
								 stream->elevel);
		if (bytestowrite <= 0)
		{
			ereport(stream->elevel,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m",
							stream->file.name)));

			/* Only reachable if stream->elevel < ERROR. */
			return;
		}
		stream->curOffset += bytestowrite;
	}
	else
	{
		int			thisfile;
		char	   *buf;

		thisfile = stream->file.f.fd;
		buf = stream->buffer.data;

		do
		{
			int			nwritten;

			errno = 0;

			pgstat_report_wait_start(stream->file.wait_event_info);
			nwritten = pg_pwrite(thisfile, buf, bytestowrite,
								 stream->curOffset);
			pgstat_report_wait_end();

			if (nwritten < 0 && errno == EINTR)
				continue;

			if (nwritten != bytestowrite)
			{
				/*
				 * if pg_pwrite didn't set errno, assume problem is no disk
				 * space
				 */
				if (errno == 0)
					errno = ENOSPC;

				/*
				 * XXX Should we close the fd explicitly? Not sure, it should
				 * happen automatically on ERROR.
				 */
				ereport(stream->elevel,
						(errcode_for_file_access(),
						 errmsg("could not write to file %s "
								"at offset %zu, length %u: %m",
								stream->file.name, stream->curOffset,
								bytestowrite)));

				/* Only reachable if stream->elevel < ERROR. */
				return;
			}
			stream->curOffset += nwritten;

			/* Done. */
			break;
		} while (true);
	}

	stream->dirty = false;

	/*
	 * Like in BufFileDumpBuffer(), make curOffset point to the original value
	 * + pos. in case that is less (as could happen due to a small backwards
	 * seek in a dirty buffer!)
	 */
	stream->curOffset -= (stream->nbytes - stream->pos);
	/* Unlike BufFileDumpBuffer(), no segment crossing could happen here. */
	Assert(stream->curOffset >= 0);

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	stream->pos = 0;
	stream->nbytes = 0;
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size and report I/O errors via
 * ereport().
 */
size_t
BufFileRead(BufFile *file, void *ptr, size_t size)
{
	return BufFileReadCommon(&file->stream, file, ptr, size);
}

/*
 * BufFileStreamRead
 *
 * Read data from a stream.
 *
 * If stream->elevel >= ERROR, the returned value is >=0, or ERROR is thrown if
 * the underlying read() system call returned -1.
 */
size_t
BufFileStreamRead(BufFileStream *stream, void *ptr, size_t size)
{
	return BufFileReadCommon(stream, NULL, ptr, size);
}

static size_t
BufFileReadCommon(BufFileStream *stream, BufFile *file, void *ptr,
				  size_t size)
{
	size_t		nread = 0;
	size_t		nthistime;

	BufFileFlush(stream, file);
	if (stream->dirty)
		/* Only reachable if stream->elevel < ERROR. */
		return 0;

	while (size > 0)
	{
		if (stream->pos >= stream->nbytes)
		{
			/* Try to load more data into buffer. */
			stream->curOffset += stream->pos;
			stream->pos = 0;
			stream->nbytes = 0;
			if (file)
				BufFileLoadBuffer(stream, file);
			else
				BufFileStreamLoadBuffer(stream);

			if (stream->nbytes <= 0)
				break;			/* no more data available */
		}

		nthistime = stream->nbytes - stream->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(ptr, stream->buffer.data + stream->pos, nthistime);

		stream->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}

	return nread;
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size and report errors via
 * ereport().
 */
void
BufFileWrite(BufFile *file, void *ptr, size_t size)
{
	BufFileWriteCommon(&file->stream, file, ptr, size);
}

/*
 * BufFileStreamWrite
 *
 * Write data to a stream.
 */
size_t
BufFileStreamWrite(BufFileStream *stream, void *ptr, size_t size)
{
	return BufFileWriteCommon(stream, NULL, ptr, size);
}

static size_t
BufFileWriteCommon(BufFileStream *stream, BufFile *file, void *ptr,
				   size_t size)
{
	size_t		nthistime;
	size_t		result = 0;

	while (size > 0)
	{
		if (stream->pos >= stream->bufsize)
		{
			/* Buffer full, dump it out */
			if (stream->dirty)
			{
				if (file)
				{
					Assert(!file->readOnly);
					BufFileDumpBuffer(stream, file);
				}
				else
					BufFileStreamDumpBuffer(stream);

				if (stream->dirty)
					/* Only reachable if stream->elevel < ERROR. */
					return result;
			}
			else
			{
				/* Hmm, went directly from reading to writing? */
				stream->curOffset += stream->pos;
				stream->pos = 0;
				stream->nbytes = 0;
			}
		}

		nthistime = stream->bufsize - stream->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(stream->buffer.data + stream->pos, ptr, nthistime);

		stream->dirty = true;
		stream->pos += nthistime;
		if (stream->nbytes < stream->pos)
			stream->nbytes = stream->pos;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		result += nthistime;
	}

	return result;
}

/*
 * Copy file contents of the file specified by 'src' to the file specified by
 * 'dst' via a buffer whose size is 'bufsize'.
 *
 * If 'upto' is greater than zero, only copy this amount of data, otherwise
 * copy the whole source file.
 *
 * Each time 'flush_distance' bytes has been written, the destination file is
 * flushed. (Do not flush if this argument is zero.)
 *
 * If 'unlink_dst' is true, unlink the destination file on write failure.
 *
 * Returns the number of bytes copied.
 */
size_t
BufFileCopyFD(BufFileCopyFDArg *dst, BufFileCopyFDArg *src, int bufsize,
			  size_t upto, int flush_distance, bool unlink_dst)
{
	char	   *buffer;
	off_t		offset;
	int			nbytes;
	off_t		flush_offset;
	size_t		nread,
				result = 0;

	/* Use palloc to ensure we get a maxaligned buffer */
	buffer = palloc(bufsize);

	flush_offset = 0;
	for (offset = 0;; offset += nbytes)
	{
		/* If we got a cancel signal during the copy of the file, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * We fsync the files later, but during the copy, flush them every so
		 * often to avoid spamming the cache and hopefully get the kernel to
		 * start writing them out before the fsync comes.
		 */
		if (flush_distance > 0 && offset - flush_offset >= flush_distance)
		{
			pg_flush_data(dst->fd, flush_offset, offset - flush_offset);
			flush_offset = offset;
		}

		/* Determine how much data we want to read. */
		if (upto > 0)
		{
			nread = upto - result;
			if (nread == 0)
				break;
			if (nread > bufsize)
				nread = bufsize;
		}
		else
			nread = bufsize;

		errno = 0;
		pgstat_report_wait_start(src->wait_event_info);
		nbytes = read(src->fd, buffer, nread);
		pgstat_report_wait_end();
		if (nbytes < 0 || errno != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", src->path)));
		if (nbytes == 0)
			break;

		BufFileWriteFD(dst, buffer, nbytes, unlink_dst);

		result += nbytes;
	}

	if (flush_offset > 0 && offset > flush_offset)
		pg_flush_data(dst->fd, flush_offset, offset - flush_offset);

	pfree(buffer);

	return result;
}

/*
 * BufFileWriteFD() - subroutine for BufFileCopyFD(), whose callers need it
 * sometimes to write additional data to the destination file.
 *
 * Writes 'nbytes' bytes of 'buffer' to the file specified by 'dst'.
 */
void
BufFileWriteFD(BufFileCopyFDArg *dst, char *buffer, int nbytes,
			   bool unlink_dst)
{
	errno = 0;
	pgstat_report_wait_start(dst->wait_event_info);
	if ((int) write(dst->fd, buffer, nbytes) != nbytes)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;

		/*
		 * If we fail to make the file (and if the caller requires so), delete
		 * it to release disk.
		 */
		if (unlink_dst)
		{
			int			save_errno = errno;

			unlink(dst->path);

			errno = save_errno;
		}

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						dst->path)));
	}
	pgstat_report_wait_end();
}

/*
 * BufFileFlush
 *
 * Like fflush(), except that I/O errors are reported with ereport().
 */
static void
BufFileFlush(BufFileStream *stream, BufFile *file)
{
	if (!stream->dirty)
		return;

	if (file)
		BufFileDumpBuffer(stream, file);
	else
		BufFileStreamDumpBuffer(stream);
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by off_t.
 * We do not support relative seeks across more than that, however.
 * I/O errors are reported by ereport().
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
	int			newFile;
	off_t		newOffset;
	BufFileStream *stream = &file->stream;

	switch (whence)
	{
		case SEEK_SET:
			if (fileno < 0)
				return EOF;
			newFile = fileno;
			newOffset = offset;
			break;
		case SEEK_CUR:

			/*
			 * Relative seek considers only the signed offset, ignoring
			 * fileno. Note that large offsets (> 1 GB) risk overflow in this
			 * add, unless we have 64-bit off_t.
			 */
			newFile = file->curFile;
			newOffset = (stream->curOffset + stream->pos) + offset;
			break;
		case SEEK_END:

			/*
			 * The file size of the last file gives us the end offset of that
			 * file.
			 */
			newFile = file->numFiles - 1;
			newOffset = FileSize(file->files[file->numFiles - 1]);
			if (newOffset < 0)
			{
				ereport(stream->elevel,
						(errcode_for_file_access(),
						 errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
								FilePathName(file->files[file->numFiles - 1]),
								file->name)));

				/* Only reachable if stream->elevel < ERROR. */
				return EOF;
			}
			break;
		default:
			elog(stream->elevel, "invalid whence: %d", whence);
			/* Only reachable if stream->elevel < ERROR. */
			return EOF;
	}
	while (newOffset < 0)
	{
		if (--newFile < 0)
			return EOF;
		newOffset += MAX_PHYSICAL_FILESIZE;
	}
	if (newFile == file->curFile &&
		newOffset >= stream->curOffset &&
		newOffset <= stream->curOffset + stream->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		stream->pos = (int) (newOffset - stream->curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	BufFileFlush(stream, file);

	/*
	 * At this point and no sooner, check for seek past last segment. The
	 * above flush could have created a new segment, so checking sooner would
	 * not work (at least not with this code).
	 */

	/* convert seek to "start of next seg" to "end of last seg" */
	if (newFile == file->numFiles && newOffset == 0)
	{
		newFile--;
		newOffset = MAX_PHYSICAL_FILESIZE;
	}
	while (newOffset > MAX_PHYSICAL_FILESIZE)
	{
		if (++newFile >= file->numFiles)
			return EOF;
		newOffset -= MAX_PHYSICAL_FILESIZE;
	}
	if (newFile >= file->numFiles)
		return EOF;
	/* Seek is OK! */
	file->curFile = newFile;
	stream->curOffset = newOffset;
	stream->pos = 0;
	stream->nbytes = 0;
	return 0;
}

/*
 * BufFileStreamSeek
 *
 * Like fseek(), but currently we only support whence==SEEK_CUR.
 *
 * Result is 0 if OK, EOF if not. Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileStreamSeek(BufFileStream *stream, off_t offset, int whence)
{
	off_t		newOffset;

	/* This is currently the only value needed. */
	Assert(whence == SEEK_CUR);

	newOffset = (stream->curOffset + stream->pos) + offset;
	if (newOffset < 0)
		return -1;

	if (newOffset >= stream->curOffset &&
		newOffset <= stream->curOffset + stream->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		stream->pos = (int) (newOffset - stream->curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	BufFileStreamDumpBuffer(stream);
	if (stream->dirty)
		/* Only reachable if stream->elevel < ERROR. */
		return -1;

	/*
	 * Just set the position info, the buffer will be loaded on the next read.
	 */
	stream->curOffset = newOffset;
	stream->pos = 0;
	stream->nbytes = 0;
	return 0;
}

void
BufFileTell(BufFile *file, int *fileno, off_t *offset)
{
	*fileno = file->curFile;
	*offset = file->stream.curOffset + file->stream.pos;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th bufsize-sized block of the
 * file.  Note that users of this interface will fail if their files exceed
 * BLCKSZ * LONG_MAX bytes (where BLCKSZ is the maximum buffer size), but that
 * is quite a lot; we don't work with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeekBlock(BufFile *file, long blknum)
{
	return BufFileSeek(file,
					   (int) (blknum / BUFFILE_SEG_SIZE(file)),
					   (off_t) (blknum % BUFFILE_SEG_SIZE(file)) * file->stream.bufsize,
					   SEEK_SET);
}

#ifdef NOT_USED
/*
 * BufFileTellBlock --- block-oriented tell
 *
 * Any fractional part of a block in the current seek position is ignored.
 */
long
BufFileTellBlock(BufFile *file)
{
	long		blknum;

	blknum = (file->stream.curOffset + file->stream.pos) / file->stream.bufsize;
	blknum += file->phys.curFile * BUFFILE_SEG_SIZE(file);
	return blknum;
}

#endif

/*
 * Return the current fileset based BufFile size.
 *
 * Counts any holes left behind by BufFileAppend as part of the size.
 * ereport()s on failure.
 */
int64
BufFileSize(BufFile *file)
{
	int64		lastFileSize;

	Assert(file->fileset != NULL);

	/* Get the size of the last physical file. */
	lastFileSize = FileSize(file->files[file->numFiles - 1]);
	if (lastFileSize < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
						FilePathName(file->files[file->numFiles - 1]),
						file->name)));

	return ((file->numFiles - 1) * (int64) MAX_PHYSICAL_FILESIZE) +
		lastFileSize;
}

/*
 * Append the contents of source file (managed within fileset) to
 * end of target file (managed within same fileset).
 *
 * Note that operation subsumes ownership of underlying resources from
 * "source".  Caller should never call BufFileClose against source having
 * called here first.  Resource owners for source and target must match,
 * too.
 *
 * This operation works by manipulating lists of segment files, so the
 * file content is always appended at a MAX_PHYSICAL_FILESIZE-aligned
 * boundary, typically creating empty holes before the boundary.  These
 * areas do not contain any interesting data, and cannot be read from by
 * caller.
 *
 * Returns the block number within target where the contents of source
 * begins.  Caller should apply this as an offset when working off block
 * positions that are in terms of the original BufFile space.
 *
 * TODO Think more about possible implications of a different bufsize among
 * the files. For now it seems to me o.k. as long as both passed the check in
 * BufFileStreamInitCommon().
 */
long
BufFileAppend(BufFile *target, BufFile *source)
{
#ifdef USE_ASSERT_CHECKING
	BufFileStream *stream_source = &source->stream;
#endif
	long		startBlock = target->numFiles * BUFFILE_SEG_SIZE(target);
	int			newNumFiles = target->numFiles + source->numFiles;
	int			i;

	Assert(target->fileset != NULL);
	Assert(source->readOnly);
	Assert(!stream_source->dirty);
	Assert(source->fileset != NULL);

	if (target->resowner != source->resowner)
		elog(ERROR, "could not append BufFile with non-matching resource owner");

	target->files = (File *)
		repalloc(target->files, sizeof(File) * newNumFiles);
	for (i = target->numFiles; i < newNumFiles; i++)
		target->files[i] = source->files[i - target->numFiles];
	target->numFiles = newNumFiles;

	return startBlock;
}

/*
 * Truncate a BufFile created by BufFileCreateFileSet up to the given fileno
 * and the offset.
 */
void
BufFileTruncateFileSet(BufFile *file, int fileno, off_t offset)
{
	BufFileStream *stream = &file->stream;
	int			numFiles = file->numFiles;
	int			newFile = fileno;
	off_t		newOffset = stream->curOffset;
	char		segment_name[MAXPGPATH];
	int			i;

	/*
	 * Loop over all the files up to the given fileno and remove the files
	 * that are greater than the fileno and truncate the given file up to the
	 * offset. Note that we also remove the given fileno if the offset is 0
	 * provided it is not the first file in which we truncate it.
	 */
	for (i = file->numFiles - 1; i >= fileno; i--)
	{
		if ((i != fileno || offset == 0) && i != 0)
		{
			FileSetSegmentName(segment_name, file->name, i);
			FileClose(file->files[i]);
			if (!FileSetDelete(file->fileset, segment_name, true))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not delete fileset \"%s\": %m",
								segment_name)));
			numFiles--;
			newOffset = MAX_PHYSICAL_FILESIZE;

			/*
			 * This is required to indicate that we have deleted the given
			 * fileno.
			 */
			if (i == fileno)
				newFile--;
		}
		else
		{
			if (FileTruncate(file->files[i], offset,
							 WAIT_EVENT_BUFFILE_TRUNCATE) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not truncate file \"%s\": %m",
								FilePathName(file->files[i]))));
			newOffset = offset;
		}
	}

	file->numFiles = numFiles;

	/*
	 * If the truncate point is within existing buffer then we can just adjust
	 * pos within buffer.
	 */
	if (newFile == file->curFile &&
		newOffset >= stream->curOffset &&
		newOffset <= stream->curOffset + stream->nbytes)
	{
		/* No need to reset the current pos if the new pos is greater. */
		if (newOffset <= stream->curOffset + stream->pos)
			stream->pos = (int) (newOffset - stream->curOffset);

		/* Adjust the nbytes for the current buffer. */
		stream->nbytes = (int) (newOffset - stream->curOffset);
	}
	else if (newFile == file->curFile &&
			 newOffset < stream->curOffset)
	{
		/*
		 * The truncate point is within the existing file but prior to the
		 * current position, so we can forget the current buffer and reset the
		 * current position.
		 */
		stream->curOffset = newOffset;
		stream->pos = 0;
		stream->nbytes = 0;
	}
	else if (newFile < file->curFile)
	{
		/*
		 * The truncate point is prior to the current file, so need to reset
		 * the current position accordingly.
		 */
		file->curFile = newFile;
		stream->curOffset = newOffset;
		stream->pos = 0;
		stream->nbytes = 0;
	}
	/* Nothing to do, if the truncate point is beyond current file. */
}
