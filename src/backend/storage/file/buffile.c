/*-------------------------------------------------------------------------
 *
 * buffile.c
 *	  Management of large buffered files, primarily temporary files.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
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
 * will go away automatically at transaction end.  If the underlying
 * virtual File is made with OpenTemporaryFile, then all resources for
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
 * to be created as a member of a BufFileSet that all participants are
 * attached to.  The BufFileSet mechanism provides a namespace so that files
 * can be discovered by name, and a shared ownership semantics so that shared
 * files survive until the last user detaches.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablespace.h"
#include "executor/instrument.h"
#include "lib/arrayutils.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/buffile.h"
#include "storage/buf_internals.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/resowner.h"

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large temporary BufFiles to be spread across
 * multiple tablespaces when available.
 */
#define MAX_PHYSICAL_FILESIZE	0x40000000
#define BUFFILE_SEG_SIZE		(MAX_PHYSICAL_FILESIZE / BLCKSZ)

/*
 * A private type used internally by buffile.c.
 */
typedef struct BufFileSetDesc
{
	pid_t	creator_pid;		/* PID of the creating process */
	int		set_number;			/* per-PID identifier for a set of files */
} BufFileSetDesc;

/*
 * A set of BufFiles that can be shared with other backends.
 */
struct BufFileSet
{
	slock_t	mutex;				/* mutex protecting the reference count */
	int		refcnt;				/* number of attached backends */
	BufFileSetDesc descriptor;	/* descriptor identifying directory */
	int		nstripes;			/* number of stripes */

	/* an array of tablespaces indexed by stripe number */
	Oid		stripe_tablespaces[FLEXIBLE_ARRAY_MEMBER];
};

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile
{
	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File	   *files;			/* palloc'd array with numFiles entries */
	off_t	   *offsets;		/* palloc'd array with numFiles entries */

	/*
	 * offsets[i] is the current seek position of files[i].  We use this to
	 * avoid making redundant FileSeek calls.
	 */

	bool		isInterXact;	/* keep open over transactions? */
	bool		dirty;			/* does buffer need to be written? */
	bool		readOnly;		/* has the file been set to read only? */

	/*
	 * Shared BufFiles use deterministic paths for their segment files.  We
	 * need to record enough information to construct path names for new
	 * segments when extending the file.
	 */
	BufFileSetDesc set_desc;	/* info used to construct directory name */
	Oid			tablespace;		/* tablespace for this shared BufFile */
	char		name[MAXPGPATH];		/* name of this BufFile within set */

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
	off_t		curOffset;		/* offset part of current pos */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */
	char		buffer[BLCKSZ];
};

static BufFile *makeBufFile(File firstfile);
static void extendBufFile(BufFile *file);
static void BufFileLoadBuffer(BufFile *file);
static void BufFileDumpBuffer(BufFile *file);
static int	BufFileFlush(BufFile *file);
static File MakeSharedSegment(Oid tablespace,
							  const BufFileSetDesc *desc,
							  const char *name,
							  int segment_number);


/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isInterXact if appropriate.
 */
static BufFile *
makeBufFile(File firstfile)
{
	BufFile    *file = (BufFile *) palloc(sizeof(BufFile));

	file->numFiles = 1;
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = firstfile;
	file->offsets = (off_t *) palloc(sizeof(off_t));
	file->offsets[0] = 0L;
	file->isInterXact = false;
	file->dirty = false;
	file->readOnly = false;
	file->resowner = CurrentResourceOwner;
	file->curFile = 0;
	file->curOffset = 0L;
	file->pos = 0;
	file->nbytes = 0;

	/* Clear the members used only for shared BufFiles. */
	file->set_desc.creator_pid = InvalidPid;
	file->set_desc.set_number = 0;
	file->tablespace = InvalidOid;
	file->name[0] = '\0';

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

	if (file->set_desc.creator_pid == InvalidPid)
		pfile = OpenTemporaryFile(file->isInterXact);
	else
		pfile = MakeSharedSegment(file->tablespace, &file->set_desc,
								  file->name, file->numFiles);

	Assert(pfile >= 0);

	CurrentResourceOwner = oldowner;

	file->files = (File *) repalloc(file->files,
									(file->numFiles + 1) * sizeof(File));
	file->offsets = (off_t *) repalloc(file->offsets,
									   (file->numFiles + 1) * sizeof(off_t));
	file->files[file->numFiles] = pfile;
	file->offsets[file->numFiles] = 0L;
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

	pfile = OpenTemporaryFile(interXact);
	Assert(pfile >= 0);

	file = makeBufFile(pfile);
	file->isInterXact = interXact;

	return file;
}

#ifdef NOT_USED
/*
 * Create a BufFile and attach it to an already-opened virtual File.
 *
 * This is comparable to fdopen() in stdio.  This is the only way at present
 * to attach a BufFile to a non-temporary file.  Note that BufFiles created
 * in this way CANNOT be expanded into multiple files.
 */
BufFile *
BufFileCreate(File file)
{
	return makeBufFile(file);
}
#endif

/*
 * Build the path of the temp directory in a given tablespace.
 */
static void
MakeTempDirectoryPath(char *path, Oid tablespace)
{
	if (tablespace == InvalidOid ||
		tablespace == DEFAULTTABLESPACE_OID ||
		tablespace == GLOBALTABLESPACE_OID)
		snprintf(path, MAXPGPATH, "base/%s", PG_TEMP_FILES_DIR);
	else
	{
		snprintf(path, MAXPGPATH, "pg_tblspc/%u/%s/%s",
				 tablespace, TABLESPACE_VERSION_DIRECTORY,
				 PG_TEMP_FILES_DIR);
	}
}

/*
 * Build the path for the directory holding the files backing a BufFileSet in
 * a given tablespace.
 */
static void
MakeBufFileSetPath(char *path, Oid tablespace, const BufFileSetDesc *desc)
{
	char tempdirpath[MAXPGPATH];

	MakeTempDirectoryPath(tempdirpath, tablespace);
	snprintf(path, MAXPGPATH, "%s/%s%d.%d" PG_TEMP_FILE_SET_DIR_SUFFIX,
			 tempdirpath, PG_TEMP_FILE_PREFIX,
			 desc->creator_pid, desc->set_number);
}

/*
 * Build the path of a single segment file for a shared BufFile.
 */
static void
MakeSharedSegmentPath(char *path,
					  Oid tablespace,
					  const BufFileSetDesc *desc,
					  const char *name,
					  int segment_number)
{
	char setdirpath[MAXPGPATH];

	MakeBufFileSetPath(setdirpath, tablespace, desc);
	snprintf(path, MAXPGPATH, "%s/" PG_TEMP_FILE_PREFIX ".%s.%d", setdirpath, name, segment_number);
}

/*
 * Create a new segment file backing a shared BufFile.
 */
static File
MakeSharedSegment(Oid tablespace,
				  const BufFileSetDesc *desc,
				  const char *name,
				  int segment_number)
{
	File		file;
	char		segmentpath[MAXPGPATH];

	MakeSharedSegmentPath(segmentpath, tablespace, desc, name, segment_number);
	file = PathNameCreateTemporaryFile(segmentpath);
	if (file <= 0)
		elog(ERROR, "could not create temporary file \"%s\": %m",
			 segmentpath);

	return file;
}

/*
 * Callback function that will be invoked when this backend detaches from a
 * DSM segment holding a BufFileSet that it has creaed or attached to.  If we
 * are the last to detach, then try to remove the directory holding this set,
 * and everything in it.  We can't raise an error, because this runs in error
 * cleanup paths.
 */
static void
BufFileSetOnDetach(dsm_segment *segment, Datum datum)
{
	bool unlink_all = false;
	BufFileSet *set = (BufFileSet *) DatumGetPointer(datum);

	SpinLockAcquire(&set->mutex);
	Assert(set->refcnt > 0);
	if (--set->refcnt == 0)
		unlink_all = true;
	SpinLockRelease(&set->mutex);

	/*
	 * If we are the last to detach, we delete the directory in all referenced
	 * tablespaces.  Note that we are still actually attached for the rest of
	 * this function so we can safely access its data.
	 */
	if (unlink_all)
	{
		size_t num_oids;
		Oid *oids;
		char dirpath[MAXPGPATH];
		int i;

		/* Find the set of unique tablespace OIDs. */
		oids = (Oid *) palloc(sizeof(Oid) * set->nstripes);
		for (i = 0; i < set->nstripes; ++i)
			oids[i] = set->stripe_tablespaces[i];
		qsort(oids, set->nstripes, sizeof(Oid), oid_cmp);
		num_oids = qunique(oids, set->nstripes, sizeof(Oid), oid_cmp);

		/*
		 * Delete the directory we created in each tablespace.  Can't fail,
		 * but can generate LOG message on IO error.
		 */
		for (i = 0; i < num_oids; ++i)
		{
			MakeBufFileSetPath(dirpath, oids[i], &set->descriptor);
			PathNameDeleteTemporaryDirRecursively(dirpath);
		}

		pfree(oids);
	}
}

/*
 * Determine the size that a BufFileSet with the given number of stripes would
 * occupy.
 */
size_t
BufFileSetEstimate(int stripes)
{
	return offsetof(BufFileSet, stripe_tablespaces) + sizeof(Oid) * stripes;
}

/*
 * Create a set of named BufFiles that can be opened for read-only access by
 * other backends, in the shared memory pointed to by 'set'.  The provided
 * memory area must have enough space for the number of bytes estimated by
 * BufFileSetEstimate(stripes).  Other backends must attach to it before
 * accessing it.  Associate this set of BufFiles with 'seg'.  The set of files
 * will be deleted when no backends are attached.
 *
 * Files will be physically striped over the tablespace configured in
 * temp_tablespaces.  It is up to client code to determine how files should be
 * mapped to stripes, but the ParallelWorkerNumber of the backend writing a
 * file is one straightforward way.  While creating the set, 'stripes' should
 * be set to the number of stripes that will be used to created and open
 * files.  If it is set to 3, then BufFileSetCreateTemporaryFile can be called
 * with values 0, 1 or 2.  Stripe numbers will be mapped to the configured
 * tablespaces in a round-robin fashion.  'stripes' must be at least 1.
 *
 * Under the covers the set is one or more directories which will eventually
 * be deleted when there are no backends attached.
 */
void
BufFileSetCreate(BufFileSet *set, dsm_segment *seg, int stripes)
{
	size_t num_oids;
	Oid *oids;
	int i;
	static int counter = 0;

	if (stripes < 1)
		elog(ERROR, "cannot create BufFileSet with fewer than 1 stripe");

	SpinLockInit(&set->mutex);
	set->refcnt = 1;
	set->descriptor.creator_pid = MyProcPid;
	set->nstripes = stripes;

	/*
	 * The name of the directory will consist of the creator's PID and a
	 * counter that starts at startup for each process.  That's not ideal,
	 * because a BufFileSet could in theory live longer than the creator, and
	 * the operating system will eventually create a process with the same PID
	 * again.  We may want to use a shmem counter instead in future.
	 */
	++counter;
	if (counter == INT_MAX)
		counter = 1;
	set->descriptor.set_number = counter;

	/* Map each stripe to a tablespace round-robin. */
	PrepareTempTablespaces();
	oids = (Oid *) palloc(sizeof(Oid) * stripes);
	for (i = 0; i < stripes; ++i)
		oids[i] = set->stripe_tablespaces[i] = GetNextTempTableSpace();

	/* Find the set of unique tablespace OIDs. */
	qsort(oids, stripes, sizeof(Oid), oid_cmp);
	num_oids = qunique(oids, stripes, sizeof(Oid), oid_cmp);

	/* Create the set's directory in every tablespace OID. */
	for (i = 0; i < num_oids; ++i)
	{
		char tempdirpath[MAXPGPATH];
		char setdirpath[MAXPGPATH];

		/*
		 * We need to compute both the top-level temporary directory's path,
		 * and the directory for our set within it.  That's because fd.c needs
		 * to be able to create the former on demand if creating the latter
		 * fails.
		 */
		MakeTempDirectoryPath(tempdirpath, oids[i]);
		MakeBufFileSetPath(setdirpath, oids[i], &set->descriptor);
		PathNameCreateTemporaryDir(tempdirpath, setdirpath);
	}

	pfree(oids);

	/* Register our cleanup callback. */
	on_dsm_detach(seg, BufFileSetOnDetach, PointerGetDatum(set));
}

/*
 * Attach to a set of named BufFiles that was created with BufFileSetCreate.
 */
void
BufFileSetAttach(BufFileSet *set, dsm_segment *seg)
{
	bool success;

	SpinLockAcquire(&set->mutex);
	if (set->refcnt == 0)
		success = false;
	else
	{
		++set->refcnt;
		success = true;
	}
	SpinLockRelease(&set->mutex);

	if (!success)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not attach to a BufFileSet that is alreayd destroyed")));

	/* Register our cleanup callback. */
	on_dsm_detach(seg, BufFileSetOnDetach, PointerGetDatum(set));
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are also attached to 'set' using the same 'name' and 'stripe'
 * values.  Even though both 'name' and 'stripe' must be provided and the same
 * pair must be used to open the file from another backend, 'name' alone must
 * be unique within the set.  The caller must have either created or attached
 * to 'set'.
 */
BufFile *
BufFileCreateShared(const BufFileSet *set, const char *name, int stripe)
{
	BufFile    *file;

	if (stripe < 0 || stripe >= set->nstripes)
		elog(ERROR, "stripe number out of range");

	file = (BufFile *) palloc(sizeof(BufFile));
	file->numFiles = 1;
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = MakeSharedSegment(set->stripe_tablespaces[stripe],
									   &set->descriptor, name, 0);
	file->offsets = (off_t *) palloc(sizeof(off_t));
	file->offsets[0] = 0L;
	file->isInterXact = false;
	file->dirty = false;
	file->resowner = CurrentResourceOwner;
	file->curFile = 0;
	file->curOffset = 0L;
	file->pos = 0;
	file->nbytes = 0;
	file->readOnly = false;
	file->set_desc = set->descriptor;
	file->tablespace = set->stripe_tablespaces[stripe];
	strcpy(file->name, name);

	return file;
}

/*
 * Open a file that was previously created in another backend with
 * BufFileCreateShared in the same BufFileSet, using the same 'name' and
 * 'stripe' values.  The backend that created the file must have called
 * BufFileClose() or BufFileExport() to make sure that it is ready to be
 * opened by other backends and render it read-only.
 *
 * The caller must have either created or attached to 'set'.
 */
BufFile *
BufFileOpenShared(const BufFileSet *set, const char *name, int stripe)
{
	BufFile    *file = (BufFile *) palloc(sizeof(BufFile));
	char		path[MAXPGPATH];
	Size		capacity = 16;
	File	   *files = palloc(sizeof(File) * capacity);
	int			nfiles = 0;

	if (stripe < 0 || stripe >= set->nstripes)
		elog(ERROR, "stripe number out of range");

	file = (BufFile *) palloc(sizeof(BufFile));
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
		MakeSharedSegmentPath(path, set->stripe_tablespaces[stripe],
							  &set->descriptor, name, nfiles);
		files[nfiles] = PathNameOpenTemporaryFile(path);
		if (files[nfiles] <= 0)
			break;
		++nfiles;
	}

	/*
	 * If we didn't find any files at all, then no BufFile exists with this
	 * tag.
	 */
	if (nfiles == 0)
		return NULL;

	file->numFiles = nfiles;
	file->files = files;
	file->offsets = (off_t *) palloc0(sizeof(off_t) * nfiles);
	file->isInterXact = false;
	file->dirty = false;
	file->resowner = CurrentResourceOwner; /* Unused, can't extend */
	file->curFile = 0;
	file->curOffset = 0L;
	file->pos = 0;
	file->nbytes = 0;
	file->readOnly = true; /* Can't write to files opened this way */

	/* The following values aren't ever used but are set for consistency. */
	file->set_desc = set->descriptor;
	file->tablespace = set->stripe_tablespaces[stripe];
	strcpy(file->name, name);

	return file;
}

/*
 * Delete a BufFile that was created by BufFileCreateShared using the given
 * 'name' and 'stripe' values.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the BufFileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given (name, stripe), and
 * should know that it exists and has been exported with BufFileExportShared.
 *
 * The caller must have either created or attached to 'set'.
 */
void
BufFileDeleteShared(const BufFileSet *set, const char *name, int stripe)
{
	char		path[MAXPGPATH];
	int			segment = 0;
	bool		found = false;

	if (stripe < 0 || stripe >= set->nstripes)
		elog(ERROR, "stripe number out of range");

	/*
	 * We don't know how many segments the file has.  We'll keep deleting
	 * until we run out.  If we don't manage to find even an initial segment,
	 * raise an error.
	 */
	for (;;)
	{
		MakeSharedSegmentPath(path, set->stripe_tablespaces[stripe],
							  &set->descriptor, name, segment);
		if (!PathNameDeleteTemporaryFile(path, true))
			break;
		found = true;
		++segment;
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
	/* Must be a file belonging to a BufFileSet. */
	Assert(file->set_desc.creator_pid != InvalidPid);

	/* It's probably a bug if someone calls this twice. */
	Assert(!file->readOnly);

	BufFileFlush(file);
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
	BufFileFlush(file);
	/* close the underlying file(s) (with delete if it's a temp file) */
	for (i = 0; i < file->numFiles; i++)
		FileClose(file->files[i]);
	/* release the buffer space */
	pfree(file->files);
	pfree(file->offsets);
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
BufFileLoadBuffer(BufFile *file)
{
	File		thisfile;

	/*
	 * Advance to next component file if necessary and possible.
	 *
	 * This path can only be taken if there is more than one component, so it
	 * won't interfere with reading a non-temp file that is over
	 * MAX_PHYSICAL_FILESIZE.
	 */
	if (file->curOffset >= MAX_PHYSICAL_FILESIZE &&
		file->curFile + 1 < file->numFiles)
	{
		file->curFile++;
		file->curOffset = 0L;
	}

	/*
	 * May need to reposition physical file.
	 */
	thisfile = file->files[file->curFile];
	if (file->curOffset != file->offsets[file->curFile])
	{
		if (FileSeek(thisfile, file->curOffset, SEEK_SET) != file->curOffset)
			return;				/* seek failed, read nothing */
		file->offsets[file->curFile] = file->curOffset;
	}

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	file->nbytes = FileRead(thisfile,
							file->buffer,
							sizeof(file->buffer),
							WAIT_EVENT_BUFFILE_READ);
	if (file->nbytes < 0)
		file->nbytes = 0;
	file->offsets[file->curFile] += file->nbytes;
	/* we choose not to advance curOffset here */

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
	while (wpos < file->nbytes)
	{
		off_t		availbytes;

		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (file->curOffset >= MAX_PHYSICAL_FILESIZE)
		{
			while (file->curFile + 1 >= file->numFiles)
				extendBufFile(file);
			file->curFile++;
			file->curOffset = 0L;
		}

		/*
		 * Enforce per-file size limit only for temp files, else just try to
		 * write as much as asked...
		 */
		bytestowrite = file->nbytes - wpos;
		availbytes = MAX_PHYSICAL_FILESIZE - file->curOffset;

		if ((off_t) bytestowrite > availbytes)
			bytestowrite = (int) availbytes;

		/*
		 * May need to reposition physical file.
		 */
		thisfile = file->files[file->curFile];
		if (file->curOffset != file->offsets[file->curFile])
		{
			if (FileSeek(thisfile, file->curOffset, SEEK_SET) != file->curOffset)
				return;			/* seek failed, give up */
			file->offsets[file->curFile] = file->curOffset;
		}
		bytestowrite = FileWrite(thisfile,
								 file->buffer + wpos,
								 bytestowrite,
								 WAIT_EVENT_BUFFILE_WRITE);
		if (bytestowrite <= 0)
			return;				/* failed to write */
		file->offsets[file->curFile] += bytestowrite;
		file->curOffset += bytestowrite;
		wpos += bytestowrite;

		pgBufferUsage.temp_blks_written++;
	}
	file->dirty = false;

	/*
	 * At this point, curOffset has been advanced to the end of the buffer,
	 * ie, its original value + nbytes.  We need to make it point to the
	 * logical file position, ie, original value + pos, in case that is less
	 * (as could happen due to a small backwards seek in a dirty buffer!)
	 */
	file->curOffset -= (file->nbytes - file->pos);
	if (file->curOffset < 0)	/* handle possible segment crossing */
	{
		file->curFile--;
		Assert(file->curFile >= 0);
		file->curOffset += MAX_PHYSICAL_FILESIZE;
	}

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	file->pos = 0;
	file->nbytes = 0;
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size.
 */
size_t
BufFileRead(BufFile *file, void *ptr, size_t size)
{
	size_t		nread = 0;
	size_t		nthistime;

	if (file->dirty)
	{
		if (BufFileFlush(file) != 0)
			return 0;			/* could not flush... */
		Assert(!file->dirty);
	}

	while (size > 0)
	{
		if (file->pos >= file->nbytes)
		{
			/* Try to load more data into buffer. */
			file->curOffset += file->pos;
			file->pos = 0;
			file->nbytes = 0;
			BufFileLoadBuffer(file);
			if (file->nbytes <= 0)
				break;			/* no more data available */
		}

		nthistime = file->nbytes - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(ptr, file->buffer + file->pos, nthistime);

		file->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}

	return nread;
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
size_t
BufFileWrite(BufFile *file, void *ptr, size_t size)
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
				BufFileDumpBuffer(file);
				if (file->dirty)
					break;		/* I/O error */
			}
			else
			{
				/* Hmm, went directly from reading to writing? */
				file->curOffset += file->pos;
				file->pos = 0;
				file->nbytes = 0;
			}
		}

		nthistime = BLCKSZ - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(file->buffer + file->pos, ptr, nthistime);

		file->dirty = true;
		file->pos += nthistime;
		if (file->nbytes < file->pos)
			file->nbytes = file->pos;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nwritten += nthistime;
	}

	return nwritten;
}

/*
 * BufFileFlush
 *
 * Like fflush()
 */
static int
BufFileFlush(BufFile *file)
{
	if (file->dirty)
	{
		BufFileDumpBuffer(file);
		if (file->dirty)
			return EOF;
	}

	return 0;
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by long.
 * We do not support relative seeks across more than LONG_MAX, however.
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
			 * fileno. Note that large offsets (> 1 gig) risk overflow in this
			 * add, unless we have 64-bit off_t.
			 */
			newFile = file->curFile;
			newOffset = (file->curOffset + file->pos) + offset;
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
		newOffset += MAX_PHYSICAL_FILESIZE;
	}
	if (newFile == file->curFile &&
		newOffset >= file->curOffset &&
		newOffset <= file->curOffset + file->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		file->pos = (int) (newOffset - file->curOffset);
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
	file->curOffset = newOffset;
	file->pos = 0;
	file->nbytes = 0;
	return 0;
}

void
BufFileTell(BufFile *file, int *fileno, off_t *offset)
{
	*fileno = file->curFile;
	*offset = file->curOffset + file->pos;
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
					   (int) (blknum / BUFFILE_SEG_SIZE),
					   (off_t) (blknum % BUFFILE_SEG_SIZE) * BLCKSZ,
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

	blknum = (file->curOffset + file->pos) / BLCKSZ;
	blknum += file->curFile * BUFFILE_SEG_SIZE;
	return blknum;
}

#endif
