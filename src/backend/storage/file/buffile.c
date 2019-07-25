/*-------------------------------------------------------------------------
 *
 * buffile.c
 *	  Management of large buffered temporary files.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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
 * BufFile supports temporary files that can be made read-only and shared with
 * other backends, as infrastructure for parallel execution.  Such files need
 * to be created as a member of a SharedFileSet that all participants are
 * attached to.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "commands/tablespace.h"
#include "common/sha2.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/buffile.h"
#include "storage/buf_internals.h"
#include "storage/encryption.h"
#include "utils/datetime.h"
#include "utils/resowner.h"

/*
 * The functions bellow actually use integer constants so that the size can be
 * controlled by GUC. This is useful for development and regression tests.
 */
int buffile_max_filesize	=  MAX_PHYSICAL_FILESIZE;
int buffile_seg_blocks	=	BUFFILE_SEG_BLOCKS(MAX_PHYSICAL_FILESIZE);

/*
 * Fields that both BufFile and TransientBufFile structures need. It must be
 * the first field of those structures.
 */
typedef struct BufFileCommon
{
	bool		dirty;			/* does buffer need to be written? */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int			curFile;		/* file index (0..n) part of current pos,
								 * always zero for TransientBufFile */
	off_t		curOffset;		/* offset part of current pos */

	bool		readOnly;		/* has the file been set to read only? */

	bool		append;			/* should new data be appended to the end? */

	/*
	 * If the file is encrypted, only the whole buffer can be loaded / dumped
	 * --- see BufFileLoadBuffer() for more info --- whether it's space is
	 * used up or not. Therefore we need to keep track of the actual on-disk
	 * size buffer of each component file, as it would be if there was no
	 * encryption.
	 *
	 * List would make coding simpler, however it would not be good for
	 * performance. Random access is important here.
	 */
	off_t	   *useful;

	/*
	 * The "useful" array may need to be expanded independent from
	 * extendBufFile() (i.e. earlier than the buffer gets dumped), so store
	 * the number of elements separate from numFiles.
	 *
	 * Always 1 for TransientBufFile.
	 */
	int			nuseful;

	PGAlignedBlock buffer;
} BufFileCommon;

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile
{
	BufFileCommon common;		/* Common fields, see above. */

	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly buffile_max_filesize */
	File	   *files;			/* palloc'd array with numFiles entries */

	/*
	 * Segment number is used to compute encryption tweak so we must remember
	 * the original numbers of segments if the file is encrypted and if it was
	 * passed as target to BufFileAppend() at least once. If this field is
	 * NULL, ->curFile is used to compute the tweak.
	 */
	off_t	   *segnos;

	bool		isInterXact;	/* keep open over transactions? */

	SharedFileSet *fileset;		/* space for segment files if shared */
	const char *name;			/* name of this BufFile if shared */

	/*
	 * Per-PID identifier if the file is encrypted and not shared. Used for
	 * tweak computation.
	 */
	uint32		number;

	/*
	 * resowner is the ResourceOwner to use for underlying temp files.  (We
	 * don't need to remember the memory context we're using explicitly,
	 * because after creation we only repalloc our arrays larger.)
	 */
	ResourceOwner resowner;
};

/*
 * Buffered variant of a transient file. Unlike BufFile this is simpler in
 * several ways: 1) it's not split into segments, 2) there's no need of seek,
 * 3) there's no need to combine read and write access.
 */
struct TransientBufFile
{
	BufFileCommon common;		/* Common fields, see above. */

	/* The underlying file. */
	char	   *path;
	int			fd;
};

static BufFile *makeBufFileCommon(int nfiles);
static BufFile *makeBufFile(File firstfile);
static void extendBufFile(BufFile *file);
static void BufFileLoadBuffer(BufFile *file);
static void BufFileDumpBuffer(BufFile *file);
static void BufFileDumpBufferEncrypted(BufFile *file);
static int	BufFileFlush(BufFile *file);
static File MakeNewSharedSegment(BufFile *file, int segment);

static void BufFileTweak(char *tweak, BufFileCommon *file, bool is_transient);
static void ensureUsefulArraySize(BufFileCommon *file, int required);
static void BufFileAppendMetadata(BufFile *target, BufFile *source);

static void BufFileLoadBufferTransient(TransientBufFile *file);
static void BufFileDumpBufferTransient(TransientBufFile *file);

static size_t BufFileReadCommon(BufFileCommon *file, void *ptr, size_t size,
				  bool is_transient);
static size_t BufFileWriteCommon(BufFileCommon *file, void *ptr, size_t size,
								 bool is_transient);
static void BufFileUpdateUsefulLength(BufFileCommon *file, bool is_transient);

/*
 * Create BufFile and perform the common initialization.
 */
static BufFile *
makeBufFileCommon(int nfiles)
{
	BufFile    *file = (BufFile *) palloc0(sizeof(BufFile));
	BufFileCommon *fcommon = &file->common;

	fcommon->dirty = false;
	fcommon->curFile = 0;
	fcommon->curOffset = 0L;
	fcommon->pos = 0;
	fcommon->nbytes = 0;

	file->numFiles = nfiles;
	file->isInterXact = false;
	file->resowner = CurrentResourceOwner;

	if (data_encrypted)
	{
		fcommon->useful = (off_t *) palloc0(sizeof(off_t) * nfiles);
		fcommon->nuseful = nfiles;

		file->segnos = NULL;

		/*
		 * The unused (trailing) part of the buffer should not contain
		 * undefined data: if we encrypt such a buffer and flush it to disk,
		 * the encrypted form of that "undefined part" can get zeroed due to
		 * seek and write beyond EOF. If such a buffer gets loaded and
		 * decrypted, the change of the undefined part to zeroes can affect
		 * the valid part if it does not end at block boundary. By setting the
		 * whole buffer to zeroes we ensure that the unused part of the buffer
		 * always contains zeroes.
		 */
		MemSet(fcommon->buffer.data, 0, BLCKSZ);
	}
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
	file->common.readOnly = false;
	file->fileset = NULL;
	file->name = NULL;

	return file;
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
		pfile = MakeNewSharedSegment(file, file->numFiles);

	Assert(pfile >= 0);

	CurrentResourceOwner = oldowner;

	file->files = (File *) repalloc(file->files,
									(file->numFiles + 1) * sizeof(File));

	if (data_encrypted)
	{
		ensureUsefulArraySize(&file->common, file->numFiles + 1);

		if (file->segnos)
		{
			file->segnos = (off_t *) repalloc(file->segnos,
											  (file->numFiles + 1) * sizeof(off_t));
			file->segnos[file->numFiles] = file->numFiles;
		}
	}

	file->files[file->numFiles] = pfile;
	file->numFiles++;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than buffile_max_filesize bytes are
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

	static uint32 counter_temp = 0;

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

	file->number = counter_temp;
	counter_temp = (counter_temp + 1) % INT_MAX;

	file->isInterXact = interXact;

	return file;
}

/*
 * Build the name for a given segment of a given BufFile.
 */
static void
SharedSegmentName(char *name, const char *buffile_name, int segment)
{
	snprintf(name, MAXPGPATH, "%s.%d", buffile_name, segment);
}

/*
 * Create a new segment file backing a shared BufFile.
 */
static File
MakeNewSharedSegment(BufFile *buffile, int segment)
{
	char		name[MAXPGPATH];
	File		file;

	/*
	 * It is possible that there are files left over from before a crash
	 * restart with the same name.  In order for BufFileOpenShared() not to
	 * get confused about how many segments there are, we'll unlink the next
	 * segment number if it already exists.
	 */
	SharedSegmentName(name, buffile->name, segment + 1);
	SharedFileSetDelete(buffile->fileset, name, true);

	/* Create the new segment. */
	SharedSegmentName(name, buffile->name, segment);
	file = SharedFileSetCreate(buffile->fileset, name);

	/* SharedFileSetCreate would've errored out */
	Assert(file > 0);

	return file;
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are attached to the same SharedFileSet using the same name.
 *
 * The naming scheme for shared BufFiles is left up to the calling code.  The
 * name will appear as part of one or more filenames on disk, and might
 * provide clues to administrators about which subsystem is generating
 * temporary file data.  Since each SharedFileSet object is backed by one or
 * more uniquely named temporary directory, names don't conflict with
 * unrelated SharedFileSet objects.
 */
BufFile *
BufFileCreateShared(SharedFileSet *fileset, const char *name)
{
	BufFile    *file;

	file = makeBufFileCommon(1);
	file->fileset = fileset;
	file->name = pstrdup(name);
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = MakeNewSharedSegment(file, 0);
	file->common.readOnly = false;

	return file;
}

/*
 * Open a file that was previously created in another backend (or this one)
 * with BufFileCreateShared in the same SharedFileSet using the same name.
 * The backend that created the file must have called BufFileClose() or
 * BufFileExportShared() to make sure that it is ready to be opened by other
 * backends and render it read-only.
 */
BufFile *
BufFileOpenShared(SharedFileSet *fileset, const char *name)
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
		SharedSegmentName(segment_name, name, nfiles);
		files[nfiles] = SharedFileSetOpen(fileset, segment_name);
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
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open temporary file \"%s\" from BufFile \"%s\": %m",
						segment_name, name)));

	file = makeBufFileCommon(nfiles);

	/*
	 * Shared encrypted segment should, at its end, contain information on the
	 * number of useful bytes in the last buffer.
	 */
	if (data_encrypted)
	{
		off_t		pos;
		int			i;

		for (i = 0; i < nfiles; i++)
		{
			int			nbytes;
			File		segment = files[i];

			pos = FileSize(segment) - sizeof(off_t);

			/*
			 * The word must immediately follow the last buffer of the
			 * segment.
			 */
			if (pos <= 0 || pos % BLCKSZ != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not find padding info in BufFile \"%s\": %m",
								name)));

			nbytes = FileRead(segment, (char *) &file->common.useful[i],
							  sizeof(off_t), pos, WAIT_EVENT_BUFFILE_READ);
			if (nbytes != sizeof(off_t))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read padding info from BufFile \"%s\": %m",
								name)));
			Assert(file->common.useful[i] > 0);

			CHECK_FOR_INTERRUPTS();
		}
	}

	file->files = files;

	if (data_encrypted)
		file->common.nuseful = nfiles;

	file->common.readOnly = true;	/* Can't write to files opened this way */
	file->fileset = fileset;
	file->name = pstrdup(name);

	return file;
}

/*
 * Delete a BufFile that was created by BufFileCreateShared in the given
 * SharedFileSet using the given name.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the SharedFileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given name, and should know
 * that it exists and has been exported or closed.
 */
void
BufFileDeleteShared(SharedFileSet *fileset, const char *name)
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
		SharedSegmentName(segment_name, name, segment);
		if (!SharedFileSetDelete(fileset, segment_name, true))
			break;
		found = true;
		++segment;

		CHECK_FOR_INTERRUPTS();
	}

	if (!found)
		elog(ERROR, "could not delete unknown shared BufFile \"%s\"", name);
}

/*
 * BufFileExportShared --- flush and make read-only, in preparation for sharing.
 */
void
BufFileExportShared(BufFile *file)
{
	/* Must be a file belonging to a SharedFileSet. */
	Assert(file->fileset != NULL);

	/* It's probably a bug if someone calls this twice. */
	Assert(!file->common.readOnly);

	BufFileFlush(file);
	file->common.readOnly = true;
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
	BufFileFlush(file);
	/* close and delete the underlying file(s) */
	for (i = 0; i < file->numFiles; i++)
		FileClose(file->files[i]);
	/* release the buffer space */
	pfree(file->files);

	if (data_encrypted)
	{
		if (file->segnos)
			pfree(file->segnos);
		pfree(file->common.useful);
	}

	pfree(file);
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
static void
BufFileLoadBuffer(BufFile *file)
{
	File		thisfile;

	/*
	 * Only whole multiple of ENCRYPTION_BLOCK can be encrypted / decrypted.
	 */
	Assert((file->common.curOffset % BLCKSZ == 0 &&
			file->common.curOffset % ENCRYPTION_BLOCK == 0) ||
		   !data_encrypted);

	/*
	 * Advance to next component file if necessary and possible.
	 */
	if (file->common.curOffset >= buffile_max_filesize &&
		file->common.curFile + 1 < file->numFiles)
	{
		file->common.curFile++;
		file->common.curOffset = 0L;
	}

	/*
	 * See makeBufFileCommon().
	 *
	 * Actually here we only handle the case of FileRead() returning zero
	 * bytes below. In contrast, if the buffer contains any data but it's not
	 * full, it should already have the trailing zeroes (encrypted) on disk.
	 * And as the encrypted buffer is always loaded in its entirety (i.e. EOF
	 * should only appear at buffer boundary if the data is encrypted), all
	 * unused bytes of the buffer should eventually be zeroes after
	 * decryption.
	 */
	if (data_encrypted)
		MemSet(file->common.buffer.data, 0, BLCKSZ);

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	thisfile = file->files[file->common.curFile];
	file->common.nbytes = FileRead(thisfile,
								   file->common.buffer.data,
								   sizeof(file->common.buffer),
								   file->common.curOffset,
								   WAIT_EVENT_BUFFILE_READ);
	if (file->common.nbytes < 0)
		file->common.nbytes = 0;
	/* we choose not to advance curOffset here */

	if (data_encrypted && file->common.nbytes > 0)
	{
		char		tweak[TWEAK_SIZE];
		int			nbytes = file->common.nbytes;

		/*
		 * The encrypted component file can only consist of whole number of
		 * our encryption units. (Only the whole buffers are dumped / loaded.)
		 * The only exception is that we're at the end of segment file and
		 * found the word indicating the number of useful bytes in the
		 * segment. This can only happen for shared file.
		 */
		if (nbytes % BLCKSZ != 0)
		{
			Assert(nbytes == sizeof(off_t) && file->fileset != NULL);

			/*
			 * This metadata his hidden to caller, so all he needs to know
			 * that there's no real data at the end of the file.
			 */
			file->common.nbytes = 0;
			return;
		}

		/* Decrypt the whole block at once. */
		BufFileTweak(tweak, &file->common, false);
		decrypt_block(file->common.buffer.data,
					  file->common.buffer.data,
					  BLCKSZ,
					  tweak,
					  false);

#ifdef	USE_ASSERT_CHECKING

		/*
		 * The unused part of the buffer which we've read from disk and
		 * decrypted should only contain zeroes, as explained in front of the
		 * MemSet() call.
		 */
		{
			int			i;

			for (i = file->common.nbytes; i < BLCKSZ; i++)
				Assert(file->common.buffer.data[i] == 0);
		}
#endif							/* USE_ASSERT_CHECKING */
	}

	if (file->common.nbytes > 0)
		pgBufferUsage.temp_blks_read++;
}

/*
 * BufFileDumpBuffer
 *
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
static void
BufFileDumpBuffer(BufFile *file)
{
	int			wpos = 0;
	int			bytestowrite;
	File		thisfile;

	/*
	 * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
	 * crosses a component-file boundary; so we need a loop.
	 */
	while (wpos < file->common.nbytes)
	{
		off_t		availbytes;

		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (file->common.curOffset >= buffile_max_filesize)
		{
			while (file->common.curFile + 1 >= file->numFiles)
				extendBufFile(file);
			file->common.curFile++;
			file->common.curOffset = 0L;
		}

		/*
		 * Determine how much we need to write into this file.
		 */
		bytestowrite = file->common.nbytes - wpos;
		availbytes = buffile_max_filesize - file->common.curOffset;

		if ((off_t) bytestowrite > availbytes)
			bytestowrite = (int) availbytes;

		thisfile = file->files[file->common.curFile];
		bytestowrite = FileWrite(thisfile,
								 file->common.buffer.data + wpos,
								 bytestowrite,
								 file->common.curOffset,
								 WAIT_EVENT_BUFFILE_WRITE);
		if (bytestowrite <= 0)
			return;				/* failed to write */
		file->common.curOffset += bytestowrite;
		wpos += bytestowrite;

		pgBufferUsage.temp_blks_written++;
	}
	file->common.dirty = false;

	/*
	 * At this point, curOffset has been advanced to the end of the buffer,
	 * ie, its original value + nbytes.  We need to make it point to the
	 * logical file position, ie, original value + pos, in case that is less
	 * (as could happen due to a small backwards seek in a dirty buffer!)
	 */
	file->common.curOffset -= (file->common.nbytes - file->common.pos);
	if (file->common.curOffset < 0) /* handle possible segment crossing */
	{
		file->common.curFile--;
		Assert(file->common.curFile >= 0);
		file->common.curOffset += buffile_max_filesize;
	}

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	file->common.pos = 0;
	file->common.nbytes = 0;
}

/*
 * BufFileDumpBufferEncrypted
 *
 * Encrypt buffer and dump it. The functionality is sufficiently different
 * from BufFileDumpBuffer to be implemented as a separate function. The most
 * notable difference is that no loop is needed here.
 */
static void
BufFileDumpBufferEncrypted(BufFile *file)
{
	char		tweak[TWEAK_SIZE];
	int			bytestowrite;
	File		thisfile;

	/*
	 * Caller's responsibility.
	 */
	Assert(file->common.pos <= file->common.nbytes);

	/*
	 * See comments in BufFileLoadBuffer();
	 */
	Assert((file->common.curOffset % BLCKSZ == 0 &&
			file->common.curOffset % ENCRYPTION_BLOCK == 0));

	/*
	 * Advance to next component file if necessary and possible.
	 */
	if (file->common.curOffset >= buffile_max_filesize)
	{
		while (file->common.curFile + 1 >= file->numFiles)
			extendBufFile(file);
		file->common.curFile++;
		file->common.curOffset = 0L;
	}

	/*
	 * This condition plus the alignment of curOffset to BLCKSZ (checked
	 * above) ensure that the encrypted buffer never crosses component file
	 * boundary.
	 */
	Assert((buffile_max_filesize % BLCKSZ) == 0);

	/*
	 * Encrypted data is dumped all at once.
	 *
	 * Unlike BufFileDumpBuffer(), we don't have to check here how much bytes
	 * is available in the segment. According to the assertions above,
	 * currOffset should be lower than buffile_max_filesize by non-zero
	 * multiple of BLCKSZ.
	 */
	bytestowrite = BLCKSZ;

	/*
	 * The amount of data encrypted must be a multiple of ENCRYPTION_BLOCK. We
	 * meet this condition simply by encrypting the whole buffer.
	 *
	 * XXX Alternatively we could encrypt only as few encryption blocks that
	 * encompass file->common.nbyte bytes, but then we'd have to care how many
	 * blocks should be decrypted: decryption of the unencrypted trailing
	 * zeroes produces garbage, which can be a problem if lseek() created
	 * "holes" in the file. Such a hole should be read as a sequence of
	 * zeroes.
	 */
	BufFileTweak(tweak, &file->common, false);

	encrypt_block(file->common.buffer.data,
				  encrypt_buf.data,
				  BLCKSZ,
				  tweak,
				  false);

	thisfile = file->files[file->common.curFile];
	bytestowrite = FileWrite(thisfile,
							 encrypt_buf.data,
							 bytestowrite,
							 file->common.curOffset,
							 WAIT_EVENT_BUFFILE_WRITE);
	if (bytestowrite <= 0 || bytestowrite != BLCKSZ)
		return;				/* failed to write */

	file->common.curOffset += bytestowrite;
	pgBufferUsage.temp_blks_written++;

	/*
	 * The number of useful bytes needs to be written at the end of each
	 * encrypted segment of a shared file so that the other backends know
	 * how many bytes of the last buffer are useful.
	 */
	if (file->fileset != NULL)
	{
		off_t		useful;

		/*
		 * nuseful may be increased earlier than numFiles but not later, so
		 * the corresponding entry should already exist in ->useful.
		 */
		Assert(file->common.curFile < file->common.nuseful);

		/*
		 * The number of useful bytes in the current segment file.
		 */
		useful = file->common.useful[file->common.curFile];

		/*
		 * Have we dumped the last buffer of the segment, i.e. the one that
		 * can contain padding?
		 */
		if (file->common.curOffset >= useful)
		{
			int			bytes_extra;

			/*
			 * Write the number of useful bytes in the segment.
			 *
			 * Do not increase curOffset afterwards. Thus we ensure that the
			 * next buffer appended will overwrite the "useful" value just
			 * written, instead of being appended to it.
			 */
			bytes_extra = FileWrite(file->files[file->common.curFile],
									(char *) &useful,
									sizeof(useful),
									file->common.curOffset,
									WAIT_EVENT_BUFFILE_WRITE);
			if (bytes_extra != sizeof(useful))
				return;		/* failed to write */
		}
	}
	file->common.dirty = false;

	if (file->common.pos >= BLCKSZ)
	{
		Assert(file->common.pos == BLCKSZ);

		/*
		 * curOffset points to the beginning of the next buffer, so just reset
		 * pos and nbytes.
		 */
		file->common.pos = 0;
		file->common.nbytes = 0;

		/* See makeBufFile() */
		MemSet(file->common.buffer.data, 0, BLCKSZ);
	}
	else
	{
		/*
		 * Move curOffset to the beginning of the just-written buffer and
		 * preserve pos.
		 */
		file->common.curOffset -= BLCKSZ;

		/*
		 * At least pos bytes should be written even if the first change since
		 * now appears at pos == nbytes, but in fact the whole buffer will be
		 * written regardless pos. This is the price we pay for the choosing
		 * BLCKSZ as the I/O unit for encrypted data.
		 */
		file->common.nbytes = BLCKSZ;
	}
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size.
 */
size_t
BufFileRead(BufFile *file, void *ptr, size_t size)
{
	return BufFileReadCommon(&file->common, ptr, size, false);
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
size_t
BufFileWrite(BufFile *file, void *ptr, size_t size)
{
	return BufFileWriteCommon(&file->common, ptr, size, false);
}

/*
 * BufFileFlush
 *
 * Like fflush()
 */
static int
BufFileFlush(BufFile *file)
{
	if (file->common.dirty)
	{
		if (!data_encrypted)
			BufFileDumpBuffer(file);
		else
			BufFileDumpBufferEncrypted(file);
		if (file->common.dirty)
			return EOF;
	}

	return 0;
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by off_t.
 * We do not support relative seeks across more than that, however.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
	int			newFile;
	off_t		newOffset;

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
			newFile = file->common.curFile;
			newOffset = (file->common.curOffset + file->common.pos) + offset;
			break;
#ifdef NOT_USED
		case SEEK_END:
			/* could be implemented, not needed currently */
			break;
#endif
		default:
			elog(ERROR, "invalid whence: %d", whence);
			return EOF;
	}
	while (newOffset < 0)
	{
		if (--newFile < 0)
			return EOF;
		newOffset += buffile_max_filesize;
	}
	if (newFile == file->common.curFile &&
		newOffset >= file->common.curOffset &&
		newOffset <= file->common.curOffset + file->common.nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		file->common.pos = (int) (newOffset - file->common.curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	if (BufFileFlush(file) != 0)
		return EOF;

	/*
	 * At this point and no sooner, check for seek past last segment. The
	 * above flush could have created a new segment, so checking sooner would
	 * not work (at least not with this code).
	 */

	/* convert seek to "start of next seg" to "end of last seg" */
	if (newFile == file->numFiles && newOffset == 0)
	{
		newFile--;
		newOffset = buffile_max_filesize;
	}
	while (newOffset > buffile_max_filesize)
	{
		if (++newFile >= file->numFiles)
			return EOF;
		newOffset -= buffile_max_filesize;
	}
	if (newFile >= file->numFiles)
		return EOF;
	/* Seek is OK! */
	file->common.curFile = newFile;
	if (!data_encrypted)
	{
		file->common.curOffset = newOffset;
		file->common.pos = 0;
		file->common.nbytes = 0;
	}
	else
	{
		/*
		 * Offset of an encrypted buffer must be a multiple of BLCKSZ.
		 */
		file->common.pos = newOffset % BLCKSZ;
		file->common.curOffset = newOffset - file->common.pos;

		/*
		 * BufFileLoadBuffer() will set nbytes iff it can read something.
		 */
		file->common.nbytes = 0;

		/*
		 * Load and decrypt the existing part of the buffer.
		 */
		BufFileLoadBuffer(file);
		if (file->common.nbytes == 0)
		{
			/*
			 * The data requested is not in the file, but this is not an
			 * error.
			 */
			return 0;
		}

		/*
		 * The whole buffer should have been loaded.
		 */
		Assert(file->common.nbytes == BLCKSZ);
	}
	return 0;
}

void
BufFileTell(BufFile *file, int *fileno, off_t *offset)
{
	*fileno = file->common.curFile;
	*offset = file->common.curOffset + file->common.pos;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * LONG_MAX bytes, but that is quite a lot; we don't work
 * with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeekBlock(BufFile *file, long blknum)
{
	return BufFileSeek(file,
					   (int) (blknum / buffile_seg_blocks),
					   (off_t) (blknum % buffile_seg_blocks) * BLCKSZ,
					   SEEK_SET);
}

static void
BufFileTweak(char *tweak, BufFileCommon *file, bool is_transient)
{
	off_t		block;

	/*
	 * The unused bytes should always be defined.
	 */
	memset(tweak, 0, TWEAK_SIZE);

	if (!is_transient)
	{
		BufFile    *tmpfile = (BufFile *) file;
		pid_t		pid;
		uint32		number;
		int			curFile;

		if (tmpfile->fileset)
		{
			pid = tmpfile->fileset->creator_pid;
			number = tmpfile->fileset->number;
		}
		else
		{
			pid = MyProcPid;
			number = tmpfile->number;
		}

		curFile = file->curFile;

		/*
		 * If the file was produced by BufFileAppend(), we need the original
		 * curFile, as it was used originally for encryption.
		 */
		if (tmpfile->segnos)
			curFile = tmpfile->segnos[curFile];

		block = curFile * buffile_seg_blocks + file->curOffset / BLCKSZ;

		StaticAssertStmt(sizeof(pid) + sizeof(number) + sizeof(block) <=
						 TWEAK_SIZE,
						 "tweak components do not fit into TWEAK_SIZE");

		/*
		 * The tweak consists of PID of the owning backend (the leader backend
		 * in the case of parallel query processing), number within the PID
		 * and block number.
		 *
		 * XXX Additional flag would be handy to distinguish local file from
		 * shared one. Since there's no more room within TWEAK_SIZE, should we
		 * use the highest bit in one of the existing components (preferably
		 * other than pid so that tweaks of different processes do not become
		 * identical)?
		 */
		*((pid_t *) tweak) = pid;
		tweak += sizeof(pid_t);
		*((uint32 *) tweak) = number;
		tweak += sizeof(number);
		*((off_t *) tweak) = block;
	}
	else
	{
		TransientBufFile *transfile = (TransientBufFile *) file;
		pg_sha256_ctx sha_ctx;
		unsigned char sha[PG_SHA256_DIGEST_LENGTH];
#define BUF_FILE_PATH_HASH_LEN	8

		/*
		 * For transient file we can't use any field of TransientBufFile
		 * because this info gets lost if the file is closed and reopened.
		 * Hash of the file path string is an easy way to get "persistent"
		 * tweak value, however usual hashes do not fit into TWEAK_SIZE. The
		 * hash portion that we can store is actually even smaller because
		 * block number needs to be stored too. Even though we only use part
		 * of the hash, the tweak we finally use for data encryption /
		 * decryption should not be predictable because the tweak we compute
		 * here is further processed, see cbc_essi_preprocess_tweak().
		 */
		pg_sha256_init(&sha_ctx);
		pg_sha256_update(&sha_ctx,
						 (uint8 *) transfile->path,
						 strlen(transfile->path));
		pg_sha256_final(&sha_ctx, sha);

		StaticAssertStmt(BUF_FILE_PATH_HASH_LEN + sizeof(block) <= TWEAK_SIZE,
						 "tweak components do not fit into TWEAK_SIZE");
		memcpy(tweak, sha, BUF_FILE_PATH_HASH_LEN);
		tweak += BUF_FILE_PATH_HASH_LEN;
		block = file->curOffset / BLCKSZ;
		*((off_t *) tweak) = block;
	}
}

/*
 * Make sure that BufFile.useful array has the required size.
 */
static void
ensureUsefulArraySize(BufFileCommon *file, int required)
{
	/*
	 * Does the array already have enough space?
	 */
	if (required <= file->nuseful)
		return;

	/*
	 * It shouldn't be possible to jump beyond the end of the last segment,
	 * i.e. skip more than 1 segment.
	 */
	Assert(file->nuseful + 1 == required);

	file->useful = (off_t *)
		repalloc(file->useful, required * sizeof(off_t));
	file->useful[file->nuseful] = 0L;
	file->nuseful++;
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

	blknum = (file->common.curOffset + file->common.pos) / BLCKSZ;
	blknum += file->common.curFile * buffile_seg_blocks;
	return blknum;
}

#endif

/*
 * Return the current shared BufFile size.
 *
 * Counts any holes left behind by BufFileAppend as part of the size.
 * ereport()s on failure.
 */
int64
BufFileSize(BufFile *file)
{
	int64		lastFileSize;

	Assert(file->fileset != NULL);

	if (!data_encrypted)
	{
		/* Get the size of the last physical file. */
		lastFileSize = FileSize(file->files[file->numFiles - 1]);
		if (lastFileSize < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
							FilePathName(file->files[file->numFiles - 1]),
							file->name)));
	}
	else
	{
		/*
		 * "useful" should be initialized even for shared file, see
		 * BufFileOpenShared().
		 */
		Assert(file->common.useful != NULL &&
			   file->common.nuseful >= file->numFiles);

		/*
		 * The number of useful bytes in the segment is what caller is
		 * interested in.
		 */
		lastFileSize = file->common.useful[file->common.nuseful - 1];
	}

	return ((file->numFiles - 1) * (int64) buffile_max_filesize) +
		lastFileSize;
}

/*
 * Append the contents of source file (managed within shared fileset) to
 * end of target file (managed within same shared fileset).
 *
 * Note that operation subsumes ownership of underlying resources from
 * "source".  Caller should never call BufFileClose against source having
 * called here first.  Resource owners for source and target must match,
 * too.
 *
 * This operation works by manipulating lists of segment files, so the file
 * content is always appended at a buffile_max_filesize-aligned boundary,
 * typically creating empty holes before the boundary.  These areas do not
 * contain any interesting data, and cannot be read from by caller.
 *
 * Returns the block number within target where the contents of source
 * begins.  Caller should apply this as an offset when working off block
 * positions that are in terms of the original BufFile space.
 */
long
BufFileAppend(BufFile *target, BufFile *source)
{
	long		startBlock = target->numFiles * buffile_seg_blocks;
	int			newNumFiles = target->numFiles + source->numFiles;
	int			i;

	Assert(target->fileset != NULL);
	Assert(source->common.readOnly);
	Assert(!source->common.dirty);
	Assert(source->fileset != NULL);

	if (target->resowner != source->resowner)
		elog(ERROR, "could not append BufFile with non-matching resource owner");

	target->files = (File *)
		repalloc(target->files, sizeof(File) * newNumFiles);
	for (i = target->numFiles; i < newNumFiles; i++)
		target->files[i] = source->files[i - target->numFiles];

	if (data_encrypted)
		BufFileAppendMetadata(target, source);

	target->numFiles = newNumFiles;

	return startBlock;
}

/*
 * Add encryption-specific metadata of the source file to the target.
 */
static void
BufFileAppendMetadata(BufFile *target, BufFile *source)
{
	int			newNumFiles = target->numFiles + source->numFiles;
	int			newNUseful = target->common.nuseful + source->common.nuseful;
	int	i;

	/*
	 * XXX As the typical use case is that parallel workers expose file to the
	 * leader, can we expect both target and source to have been exported,
	 * i.e. flushed? In such a case "nuseful" would have to be equal to
	 * "numFiles" for both input files and the code could get a bit
	 * simpler. It seems that at least source should be flushed, as
	 * source->readOnly is expected to be true above.
	 */
	target->common.useful = (off_t *)
		repalloc(target->common.useful, sizeof(off_t) * newNUseful);

	for (i = target->common.nuseful; i < newNUseful; i++)
		target->common.useful[i] = source->common.useful[i - target->common.nuseful];
	target->common.nuseful = newNUseful;

	/*
	 * File segments can appear at different position due to concatenation, so
	 * make sure we remember the original positions for the sake of encryption
	 * tweak.
	 */
	if (target->segnos == NULL)
	{
		/*
		 * If the target does not have the array yet, allocate it for both
		 * target and source and initialize the target part.
		 */
		target->segnos = (off_t *) palloc(newNumFiles * sizeof(off_t));
		for (i = 0; i < target->numFiles; i++)
			target->segnos[i] = i;
	}
	else
	{
		/*
		 * Use the existing target part and add space for the source part.
		 */
		target->segnos = (off_t *) repalloc(target->segnos,
											newNumFiles * sizeof(off_t));
	}

	/*
	 * The source segment number either equals to (0-based) index of the
	 * segment, or to an element of an already existing array.
	 */
	for (i = target->numFiles; i < newNumFiles; i++)
	{
		off_t		segno = i - target->numFiles;

		if (source->segnos == NULL)
			target->segnos[i] = segno;
		else
			target->segnos[i] = source->segnos[segno];
	}
}

/*
 * Open TransientBufFile at given path or create one if it does not
 * exist. User will be allowed either to write to the file or to read from it,
 * according to fileFlags, but not both.
 */
TransientBufFile *
BufFileOpenTransient(const char *path, int fileFlags)
{
	bool		readOnly;
	bool		append = false;
	TransientBufFile *file;
	BufFileCommon *fcommon;
	int			fd;
	off_t		size;

	/* Either read or write mode, but not both. */
	Assert((fileFlags & O_RDWR) == 0);

	/* Check whether user wants read or write access. */
	readOnly = (fileFlags & O_WRONLY) == 0;

	if (data_encrypted)
	{
		/*
		 * In the encryption case, even if user will only be allowed to write,
		 * internally we also need to read, see below.
		 */
		fileFlags &= ~O_WRONLY;
		fileFlags |= O_RDWR;

		/*
		 * We can only emulate the append behavior by setting curOffset to
		 * file size because if the underlying file was opened in append mode,
		 * we could not rewrite the old value of file->common.useful[0] with
		 * data.
		 */
		if (fileFlags & O_APPEND)
		{
			append = true;
			fileFlags &= ~O_APPEND;
		}
	}

	/*
	 * Append mode for read access is not useful, so don't bother implementing
	 * it.
	 */
	Assert(!(readOnly && append));

	errno = 0;
	fd = OpenTransientFile(path, fileFlags);
	if (fd < 0)
	{
		/*
		 * If caller wants to read from file and the file is not there, he
		 * should be able to handle the condition on his own.
		 *
		 * XXX Shouldn't we always let caller evaluate errno?
		 */
		if (errno == ENOENT && (fileFlags & O_RDONLY))
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	file = (TransientBufFile *) palloc(sizeof(TransientBufFile));
	fcommon = &file->common;
	fcommon->dirty = false;
	fcommon->pos = 0;
	fcommon->nbytes = 0;
	fcommon->readOnly = readOnly;
	fcommon->append = append;
	fcommon->curFile = 0;
	fcommon->useful = (off_t *) palloc0(sizeof(off_t));
	fcommon->nuseful = 1;

	file->path = pstrdup(path);
	file->fd = fd;

	errno = 0;
	size = lseek(file->fd, 0, SEEK_END);
	if (errno > 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not initialize TransientBufFile for file \"%s\": %m",
						path)));

	if (fcommon->append)
	{
		/* Position the buffer at the end of the file. */
		fcommon->curOffset = size;
	}
	else
		fcommon->curOffset = 0L;

	/*
	 * Encrypted transient file should, at its end, contain information on the
	 * number of useful bytes in the last buffer.
	 */
	if (data_encrypted)
	{
		off_t		pos = size;
		int			nbytes;

		/* No metadata in an empty file. */
		if (pos == 0)
			return file;

		pos -= sizeof(off_t);

		/*
		 * The word must immediately follow the last buffer of the segment.
		 */
		if (pos < 0 || pos % BLCKSZ != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not find padding info in TransientBufFile \"%s\": %m",
							path)));

		errno = 0;
		nbytes = pg_pread(file->fd,
						  (char *) &fcommon->useful[0],
						  sizeof(off_t),
						  pos);
		if (nbytes != sizeof(off_t))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read padding info from TransientBufFile \"%s\": %m",
							path)));
		Assert(fcommon->useful[0] > 0);

		if (fcommon->append)
		{
			off_t		useful = fcommon->useful[0];

			/*
			 * If new buffer should be added, make sure it will end up
			 * immediately after the last complete one, and also that the next
			 * write position follows the last valid byte.
			 */
			fcommon->pos = useful % BLCKSZ;
			fcommon->curOffset = useful - fcommon->pos;
		}
	}

	return file;
}

/*
 * Close a TransientBufFile.
 */
void
BufFileCloseTransient(TransientBufFile *file)
{
	/* Flush any unwritten data. */
	if (!file->common.readOnly &&
		file->common.dirty && file->common.nbytes > 0)
	{
		BufFileDumpBufferTransient(file);

		/*
		 * Caller of BufFileWriteTransient() recognizes the failure to flush
		 * buffer by the returned value, however this function has no return
		 * code.
		 */
		if (file->common.dirty)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not flush file \"%s\": %m", file->path)));
	}

	if (CloseTransientFile(file->fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file->path)));

	if (data_encrypted)
		pfree(file->common.useful);
	pfree(file->path);
	pfree(file);
}

/*
 * Load some data into buffer, if possible, starting from file->offset.  At
 * call, must have dirty = false, pos and nbytes = 0.  On exit, nbytes is
 * number of bytes loaded.
 */
static void
BufFileLoadBufferTransient(TransientBufFile *file)
{
	Assert(file->common.readOnly);
	Assert(!file->common.dirty);
	Assert(file->common.pos == 0 && file->common.nbytes == 0);

	/* See comments in BufFileLoadBuffer(). */
	if (data_encrypted)
		MemSet(file->common.buffer.data, 0, BLCKSZ);
retry:

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_BUFFILE_READ);
	file->common.nbytes = pg_pread(file->fd,
								   file->common.buffer.data,
								   sizeof(file->common.buffer),
								   file->common.curOffset);
	pgstat_report_wait_end();

	if (file->common.nbytes < 0)
	{
		/* TODO The W32 specific code, see FileWrite. */

		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		return;
	}
	/* we choose not to advance offset here */

	if (data_encrypted && file->common.nbytes > 0)
	{
		char		tweak[TWEAK_SIZE];
		int			nbytes = file->common.nbytes;

		/*
		 * The encrypted file can only consist of whole number of our
		 * encryption units. (Only the whole buffers are dumped / loaded.) The
		 * only exception is that we're at the end of segment file and found
		 * the word indicating the number of useful bytes in the segment. This
		 * can only happen for shared file.
		 */
		if (nbytes % BLCKSZ != 0)
		{
			Assert(nbytes == sizeof(off_t));

			/*
			 * This metadata his hidden to caller, so all he needs to know
			 * that there's no real data at the end of the file.
			 */
			file->common.nbytes = 0;
			return;
		}

		/* Decrypt the whole block at once. */
		BufFileTweak(tweak, &file->common, true);
		decrypt_block(file->common.buffer.data,
					  file->common.buffer.data,
					  BLCKSZ,
					  tweak,
					  false);

#ifdef	USE_ASSERT_CHECKING

		/*
		 * The unused part of the buffer which we've read from disk and
		 * decrypted should only contain zeroes, as explained in front of the
		 * MemSet() call.
		 */
		{
			int			i;

			for (i = file->common.nbytes; i < BLCKSZ; i++)
				Assert(file->common.buffer.data[i] == 0);
		}
#endif							/* USE_ASSERT_CHECKING */
	}
}

/*
 * Write contents of a transient file buffer to disk.
 */
static void
BufFileDumpBufferTransient(TransientBufFile *file)
{
	int			bytestowrite,
				nwritten;
	char	   *write_ptr;

	/* This function should only be needed during write access ... */
	Assert(!file->common.readOnly);

	/* ... and if there's some work to do. */
	Assert(file->common.dirty);
	Assert(file->common.nbytes > 0);

	if (!data_encrypted)
	{
		write_ptr = file->common.buffer.data;
		bytestowrite = file->common.nbytes;
	}
	else
	{
		char		tweak[TWEAK_SIZE];

		/*
		 * Encrypt the whole buffer, see comments in BufFileDumpBuffer().
		 */
		BufFileTweak(tweak, &file->common, true);
		encrypt_block(file->common.buffer.data,
					  encrypt_buf.data,
					  BLCKSZ,
					  tweak,
					  false);
		write_ptr = encrypt_buf.data;
		bytestowrite = BLCKSZ;
	}
retry:
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_BUFFILE_WRITE);
	nwritten = pg_pwrite(file->fd,
						 write_ptr,
						 bytestowrite,
						 file->common.curOffset);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nwritten != file->common.nbytes && errno == 0)
		errno = ENOSPC;

	if (nwritten < 0)
	{
		/* TODO The W32 specific code, see FileWrite. */

		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;

		return;					/* failed to write */
	}

	file->common.curOffset += nwritten;

	if (data_encrypted)
	{
		off_t		useful;

		/*
		 * The number of useful bytes in file.
		 */
		useful = file->common.useful[0];

		/*
		 * Have we dumped the last buffer of the segment, i.e. the one that
		 * can contain padding?
		 */
		if (file->common.curOffset >= useful)
		{
			int			bytes_extra;

			/*
			 * Write the number of useful bytes in the file
			 *
			 * Do not increase curOffset afterwards. Thus we ensure that the
			 * next buffer appended will overwrite the "useful" value just
			 * written, instead of being appended to it.
			 */
			pgstat_report_wait_start(WAIT_EVENT_BUFFILE_WRITE);
			bytes_extra = pg_pwrite(file->fd,
									(char *) &useful,
									sizeof(useful),
									file->common.curOffset);
			pgstat_report_wait_end();
			if (bytes_extra != sizeof(useful))
				return;			/* failed to write */
		}
	}

	file->common.dirty = false;

	file->common.pos = 0;
	file->common.nbytes = 0;
}

/*
 * Like BufFileRead() except it receives pointer to TransientBufFile.
 */
size_t
BufFileReadTransient(TransientBufFile *file, void *ptr, size_t size)
{
	return BufFileReadCommon(&file->common, ptr, size, true);
}

/*
 * Like BufFileWrite() except it receives pointer to TransientBufFile.
 */
size_t
BufFileWriteTransient(TransientBufFile *file, void *ptr, size_t size)
{
	return BufFileWriteCommon(&file->common, ptr, size, true);
}

/*
 * BufFileWriteCommon
 *
 * Functionality needed by both BufFileRead() and BufFileReadTransient().
 */
static size_t
BufFileReadCommon(BufFileCommon *file, void *ptr, size_t size,
				  bool is_transient)
{
	size_t		nread = 0;
	size_t		nthistime;

	if (file->dirty)
	{
		/*
		 * Transient file currently does not allow both read and write access,
		 * so this function should not see dirty buffer.
		 */
		Assert(!is_transient);

		if (BufFileFlush((BufFile *) file) != 0)
			return 0;			/* could not flush... */
		Assert(!file->dirty);
	}

	while (size > 0)
	{
		if (file->pos >= file->nbytes)
		{
			/*
			 * Neither read nor write nor seek should leave pos greater than
			 * nbytes, regardless the data is encrypted or not. pos can only
			 * be greater if nbytes is zero --- this situation can be caused
			 * by BufFileSeek().
			 */
			Assert(file->pos == file->nbytes || file->nbytes == 0);

			/*
			 * The Assert() above implies that pos is a whole multiple of
			 * BLCKSZ, so curOffset has meet the same encryption-specific
			 * requirement too.
			 */
			Assert(file->curOffset % BLCKSZ == 0 || !data_encrypted);

			file->nbytes = 0;
			/* Try to load more data into buffer. */
			if (!data_encrypted || file->pos % BLCKSZ == 0)
			{
				file->curOffset += file->pos;
				file->pos = 0;

				if (!is_transient)
					BufFileLoadBuffer((BufFile *) file);
				else
					BufFileLoadBufferTransient((TransientBufFile *) file);

				if (file->nbytes <= 0)
					break;		/* no more data available */
			}
			else
			{
				int			nbytes_orig = file->nbytes;

				/*
				 * Given that BLCKSZ is the I/O unit for encrypted data (see
				 * comments in BufFileDumpBuffer), we cannot add pos to
				 * curOffset because that would make it point outside block
				 * boundary. The only thing we can do is to reload the whole
				 * buffer and see if more data is eventually there than the
				 * previous load has fetched.
				 */
				if (!is_transient)
					BufFileLoadBuffer((BufFile *) file);
				else
					BufFileLoadBufferTransient((TransientBufFile *) file);

				Assert(file->nbytes >= nbytes_orig);
				if (file->nbytes == nbytes_orig)
					break;		/* no more data available */
			}
		}

		nthistime = file->nbytes - file->pos;

		/*
		 * The buffer can contain trailing zeroes because BLCKSZ is the I/O
		 * unit for encrypted data. These are not available for reading.
		 */
		if (data_encrypted)
		{
			off_t		useful = file->useful[file->curFile];

			/*
			 * The criterion is whether the useful data ends within the
			 * currently loaded buffer.
			 */
			if (useful < file->curOffset + BLCKSZ)
			{
				int			avail;

				/*
				 * Compute the number of bytes available in the current
				 * buffer.
				 */
				avail = useful - file->curOffset;
				Assert(avail >= 0);

				/*
				 * An empty buffer can exist, e.g. after a seek to the end of
				 * the last component file.
				 */
				if (avail == 0)
					break;

				/*
				 * Seek beyond the current EOF, which was not followed by
				 * write, could have resulted in position outside the useful
				 * data
				 */
				if (file->pos > avail)
					break;

				nthistime = avail - file->pos;
				Assert(nthistime >= 0);

				/*
				 * Have we reached the end of the valid data?
				 */
				if (nthistime == 0)
					break;
			}
		}

		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(ptr, file->buffer.data + file->pos, nthistime);

		file->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}

	return nread;
}

/*
 * BufFileWriteCommon
 *
 * Functionality needed by both BufFileWrite() and BufFileWriteTransient().
 */
static size_t
BufFileWriteCommon(BufFileCommon *file, void *ptr, size_t size,
				   bool is_transient)
{
	size_t		nwritten = 0;
	size_t		nthistime;

	Assert(!file->readOnly);

	while (size > 0)
	{
		if (file->pos >= BLCKSZ)
		{
			/* Buffer full, dump it out */
			if (file->dirty)
			{
				if (!is_transient)
				{
					if (!data_encrypted)
						BufFileDumpBuffer((BufFile *)file);
					else
						BufFileDumpBufferEncrypted((BufFile *) file);
				}
				else
					BufFileDumpBufferTransient((TransientBufFile *) file);

				if (file->dirty)
					break;		/* I/O error */
			}
			else
			{
				Assert(!is_transient);

				/*
				 * Hmm, went directly from reading to writing?
				 *
				 * As pos should be exactly BLCKSZ, there is nothing special
				 * to do about data_encrypted, except for zeroing the buffer.
				 */
				Assert(file->pos == BLCKSZ);

				file->curOffset += file->pos;
				file->pos = 0;
				file->nbytes = 0;

				/* See makeBufFile() */
				if (data_encrypted)
					MemSet(file->buffer.data, 0, BLCKSZ);
			}

			/*
			 * If curOffset changed above, it should still meet the assumption
			 * that buffer is the I/O unit for encrypted data.
			 */
			Assert(file->curOffset % BLCKSZ == 0 || !data_encrypted);
		}

		nthistime = BLCKSZ - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(file->buffer.data + file->pos, ptr, nthistime);

		file->dirty = true;
		file->pos += nthistime;
		if (file->nbytes < file->pos)
			file->nbytes = file->pos;

		/*
		 * Remember how many bytes of the file are valid - the rest is
		 * padding.
		 */
		if (data_encrypted)
			BufFileUpdateUsefulLength(file, is_transient);

		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nwritten += nthistime;
	}

	return nwritten;
}

/*
 * Update information about the effective length of the file.
 */
static void
BufFileUpdateUsefulLength(BufFileCommon *file, bool is_transient)
{
	off_t		new_useful;
	int			fileno;

	if (is_transient)

	{
		/*
		 * Transient file is a single file on the disk, so the whole thing is
		 * pretty simple.
		 */
		fileno = 0;

		new_useful = file->curOffset + file->pos;
		if (new_useful > file->useful[fileno])
			file->useful[fileno] = new_useful;
	}
	else
	{
		fileno = file->curFile;

		/*
		 * curFile does not necessarily correspond to the offset: it can still
		 * have the initial value if BufFileSeek() skipped the previous file
		 * w/o dumping anything of it. While curFile will be fixed during the
		 * next dump, we need valid fileno now.
		 */
		if (file->curOffset >= buffile_max_filesize)
		{
			/*
			 * Even BufFileSeek() should not allow curOffset to become more
			 * than buffile_max_filesize (if caller passes higher offset,
			 * curFile gets increased instead).
			 */
			Assert(file->curOffset == buffile_max_filesize);

			fileno++;
		}

		/*
		 * fileno can now point to a segment that does not exist on disk yet.
		 */
		ensureUsefulArraySize(file, fileno + 1);

		/*
		 * Update the number of useful bytes in the underlying component file
		 * if we've added any useful data.
		 */
		new_useful = file->curOffset;

		/*
		 * Make sure the offset is relative to the correct component file
		 * (segment). If the write just crossed segment boundary, curOffset
		 * can still point at the end of the previous segment, and so
		 * new_useful is also relative to the start of that previous
		 * segment. Make sure it's relative to the current (fileno) segment.
		 */
		if (file->curOffset % buffile_max_filesize == 0)
			new_useful %= buffile_max_filesize;

		/* Finalize the offset. */
		new_useful += file->pos;

		/*
		 * Adjust the number of useful bytes in the file if needed.  This has
		 * to happen immediately, independent from BufFileDumpBuffer(), so
		 * that BufFileRead() works correctly anytime.
		 */
		if (new_useful > file->useful[fileno])
			file->useful[fileno] = new_useful;
	}
}
