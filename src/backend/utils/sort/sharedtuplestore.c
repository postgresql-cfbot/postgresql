/*-------------------------------------------------------------------------
 *
 * sharedtuplestore.c
 *	  Simple mechanism for sharing tuples between backends.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/util/sort/sharedtuplestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "utils/sharedtuplestore.h"

/* Per-participant shared state. */
typedef struct SharedTuplestoreParticipant
{
	LWLock lock;
	bool error;					/* Error occurred flag. */
	bool eof;					/* End of file reached. */
	int read_fileno;			/* BufFile segment file number. */
	off_t read_offset;			/* Offset within segment file. */
} SharedTuplestoreParticipant;

/* The control object that lives in shared memory. */
struct SharedTuplestore
{
	int reading_partition;		/* The partition we are currently reading. */
	int nparticipants;			/* Number of participants that can write. */
	int flags;					/* Flag bits from SHARED_TUPLESTORE_XXX */
	size_t meta_data_size;		/* Size of per-tuple header. */

	/* Followed by shared state for 'nparticipants' participants. */
	SharedTuplestoreParticipant participants[FLEXIBLE_ARRAY_MEMBER];

	/* Followed by a BufFileSet.  See GetBufFileSet macro. */
};

/* Per-participant state that lives in backend-local memory. */
struct SharedTuplestoreAccessor
{
	int participant;			/* My partitipant number. */
	SharedTuplestore *sts;		/* The shared state. */
	int nfiles;					/* Size of local files array. */
	BufFile **files;			/* Per-partition files open for writing. */

	BufFile *read_file;			/* The current file to read from. */
	int read_partition;			/* The current partition to read from. */
	int read_participant;		/* The current participant to read from. */
	int read_fileno;			/* BufFile segment file number. */
	off_t read_offset;			/* Offset within segment file. */

	void *buffer;				/* A reusable tuple buffer. */
	size_t buffer_size;			/* Size of above buffer. */
};

/*
 * After the final element of SharedTuplestore's variable sized participants
 * array there is a BufFileSet.  This macro is needed to locate it, because
 * BufFileSet is of variable size so can't be declared as a regular member of
 * SharedTuplestore.
 */
#define GetBufFileSet(sts) \
	((BufFileSet *) &(sts)->participants[(sts)->nparticipants])

static void make_name(char *name, int partition, int participant);

/*
 * Return the amount of shared memory required to hold SharedTuplestore for a
 * given number of participants.
 */
size_t
sts_estimate(int participants)
{
	return offsetof(SharedTuplestore, participants) +
		sizeof(SharedTuplestoreParticipant) * participants +
		BufFileSetEstimate(participants);
}

/*
 * Initialize a SharedTuplestore in existing shared memory.  There must be
 * space for sts_size(participants) bytes.  If flags is set to the value
 * SHARED_TUPLESTORE_SINGLE_PASS then each partition may only be read once,
 * because underlying files will be deleted.
 *
 * Tuples that are stored may optionally carry a piece of fixed sized
 * meta-data which will be retrieved along with the tuple.  This is useful for
 * the hash codes used for multi-batch hash joins, but could have other
 * applications.
 */
SharedTuplestoreAccessor *
sts_initialize(SharedTuplestore *sts, int participants,
			   int my_participant_number,
			   size_t meta_data_size,
			   int flags,
			   dsm_segment *segment)
{
	SharedTuplestoreAccessor *accessor;
	BufFileSet *fileset;
	int i;

	Assert(my_participant_number < participants);

	sts->reading_partition = 0;
	sts->nparticipants = participants;
	sts->meta_data_size = meta_data_size;
	sts->flags = flags;
	for (i = 0; i < participants; ++i)
	{
		sts->participants[i].error = false;
		sts->participants[i].eof = false;
		sts->participants[i].read_fileno = 0;
		sts->participants[i].read_offset = 0;
		LWLockInitialize(&sts->participants[i].lock,
						 LWTRANCHE_SHARED_TUPLESTORE);
	}
	fileset = GetBufFileSet(sts);
	BufFileSetCreate(fileset, segment, participants);

	accessor = palloc0(sizeof(SharedTuplestoreAccessor));
	accessor->participant = my_participant_number;
	accessor->sts = sts;
	accessor->nfiles = 16;
	accessor->files = palloc0(sizeof(BufFile *) * accessor->nfiles);
	accessor->buffer_size = 1024;
	accessor->buffer = palloc(accessor->buffer_size);
	return accessor;
}

/*
 * Attach to a SharedTupleStore that has been initialized by another backend,
 * so that this backend can read and write tuples.
 */
SharedTuplestoreAccessor *
sts_attach(SharedTuplestore *sts,
		   int my_participant_number,
		   dsm_segment *segment)
{
	SharedTuplestoreAccessor *accessor;
	BufFileSet *fileset = GetBufFileSet(sts);

	Assert(my_participant_number < sts->nparticipants);

	BufFileSetAttach(fileset, segment);

	accessor = palloc0(sizeof(SharedTuplestoreAccessor));
	accessor->participant = my_participant_number;
	accessor->sts = sts;
	accessor->nfiles = 16;
	accessor->files = palloc0(sizeof(BufFile *) * accessor->nfiles);
	accessor->buffer_size = 1024;
	accessor->buffer = palloc(accessor->buffer_size);
	return accessor;
}

/*
 * Finish writing tuples.  This should be called by all backends that have
 * written data, before any backend begins reading it.
 */
void
sts_end_write(SharedTuplestoreAccessor *accessor, int partition)
{
	if (partition < accessor->nfiles && accessor->files[partition] != NULL)
	{
		BufFileClose(accessor->files[partition]);
		accessor->files[partition] = NULL;
	}
}

/*
 * Finish writing tuples in all partitions, so that other backends can begin
 * reading from any partition.
 */
void
sts_end_write_all_partitions(SharedTuplestoreAccessor *accessor)
{
	int partition;

	for (partition = 0; partition < accessor->nfiles; ++partition)
		sts_end_write(accessor, partition);
}

/*
 * Prepare to read one partition in all participants in parallel.  Each will
 * read an arbitrary subset of the tuples in the same partition until there
 * are none left.  One backend should call this.  After it returns, all
 * participating backends should call sts_begin_partial_scan() and then loop
 * over sts_gettuple().
 */
void
sts_prepare_partial_scan(SharedTuplestoreAccessor *accessor, int partition)
{
	int i;

	/* Reset the read head for all participants' files. */
	for (i = 0; i < accessor->sts->nparticipants; ++i)
	{
		/*
		 * We could require the caller to ensure that only one backend calls
		 * this, but it doesn't seem very expensive to acquire a small number
		 * of locks like this.
		 */
		LWLockAcquire(&accessor->sts->participants[i].lock, LW_EXCLUSIVE);
		accessor->sts->participants[i].read_fileno = 0;
		accessor->sts->participants[i].read_offset = 0;
		accessor->sts->participants[i].eof = false;
		LWLockRelease(&accessor->sts->participants[i].lock);
	}
}

/*
 * Begin scanning the contents of one partition.
 */
void
sts_begin_partial_scan(SharedTuplestoreAccessor *accessor, int partition)
{
	char name[MAXPGPATH];
	BufFileSet *fileset = GetBufFileSet(accessor->sts);

	/*
	 * We will start out reading the file that THIS backend wrote.  The idea
	 * is that all backends do this, so we read all the files in parallel with
	 * minimal contention at first.
	 */
	accessor->read_partition = partition;
	accessor->read_participant = accessor->participant;
	accessor->read_fileno = -1;
	accessor->read_offset = -1;
	make_name(name, accessor->read_partition, accessor->read_participant);
	accessor->read_file = BufFileOpenShared(fileset, name,
											accessor->read_participant);
}

/*
 * Finish a partial scan, freeing associated backend-local resources.
 */
void
sts_end_partial_scan(SharedTuplestoreAccessor *accessor)
{
	if (accessor->read_file != NULL)
	{
		BufFileClose(accessor->read_file);
		accessor->read_file = NULL;
	}
}

/*
 * Write a tuple into a given partition.  If a meta-data size was provided to
 * sts_initialize, then meta_data must be provided.
 */
void
sts_puttuple(SharedTuplestoreAccessor *accessor, int partition,
			 void *meta_data, MinimalTuple tuple)
{
	BufFile *file;
	size_t written;

	/* Sanity check to avoid overflow. */
	if (partition >= INT_MAX / 2)
		elog(ERROR, "too many shared tuplestore partitions");

	/* Do we need to extend our local array of files to cover 'partition'? */
	if (partition >= accessor->nfiles)
	{
		int new_nfiles = accessor->nfiles * 2;

		while (new_nfiles <= partition)
			new_nfiles *= 2;

		accessor->files = repalloc(accessor->files,
								   sizeof(BufFile *) * new_nfiles);
		memset(accessor->files + accessor->nfiles,
			   0,
			   sizeof(BufFile *) * (new_nfiles - accessor->nfiles));
		accessor->nfiles = new_nfiles;
	}

	/* Do we have our own file for this partition already? */
	file = accessor->files[partition];
	if (file == NULL)
	{
		char name[MAXPGPATH];
		BufFileSet *fileset = GetBufFileSet(accessor->sts);

		/* Create one.  Only this backend will write into it. */
		make_name(name, partition, accessor->participant);
		file = BufFileCreateShared(fileset, name, accessor->participant);
		accessor->files[partition] = file;
	}

	/*
	 * We could write data out in a format that is more amenable to being read
	 * in parallel.  For example, we could buffer a few tuples and then write
	 * out a 'granule' header that says how far forward to skip N tuples and
	 * then has the lengths all at once.  For now we write one tuple at a
	 * time.
	 */

	/* Write out the optional meta-data. */
	if (accessor->sts->meta_data_size > 0)
	{
		written = BufFileWrite(file, meta_data, accessor->sts->meta_data_size);
		if (written != accessor->sts->meta_data_size)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to temporary file: %m")));
	}

	/* Write out the tuple. */
	written = BufFileWrite(file, tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to temporary file: %m")));
}

/*
 * Get the next tuple in the current scan.  Points to a private buffer which
 * will remain untouched until the next call to this function.  Return NULL if
 * there are no more tuples.  If the SharedTuplestore was initialized with
 * non-zero meta_data_size, the meta-data associate with any tuple
 * successfully retrieved will be written to 'meta_data'.  The result should
 * not be freed.
 */
MinimalTuple
sts_gettuple(SharedTuplestoreAccessor *accessor, void *meta_data)
{
	BufFileSet *fileset = GetBufFileSet(accessor->sts);
	MinimalTuple tuple = NULL;

	for (;;)
	{
		SharedTuplestoreParticipant *participant;
		size_t nread;
		uint32 tuple_size;
		bool eof;

		if (accessor->read_file == NULL)
		{
			char name[MAXPGPATH];

			/*
			 * No file for the current read participant.  Let's try the next
			 * one.  (Other strategies would be possible, including finding
			 * the one that has the fewest participants reading from it; round
			 * robin starting from your own file is at least simple.)
			 */
			accessor->read_participant =
				(accessor->read_participant + 1) %
				accessor->sts->nparticipants;
			if (accessor->read_participant == accessor->participant)
			{
				/*
				 * We've made it all the way back around to the file we
				 * started with, that is, our own one.  So there are no more
				 * tuples to be read.
				 */
				break;
			}

			/*
			 * Try to import that participant's file, and go around again to
			 * check if that found a file.
			 */
			make_name(name, accessor->read_partition,
					  accessor->read_participant);
			accessor->read_file = BufFileOpenShared(fileset, name,
													accessor->read_participant);
			accessor->read_fileno = -1;
			accessor->read_offset = -1;
			continue;
		}

		/*
		 * We have a file to read.  Only one backend can read from a given
		 * participant's file at a time, so we need an exclusive lock.
		 */
		participant = &accessor->sts->participants[accessor->read_participant];
		LWLockAcquire(&participant->lock, LW_EXCLUSIVE);

		/* Check if another participant has somehow bombed out in mid-read. */
		if (participant->error)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from temporary file (error in another backend)")));

		/* Check if this participant's file has already been entirely read. */
		if (participant->eof)
		{
			LWLockRelease(&participant->lock);
			BufFileClose(accessor->read_file);
			accessor->read_file = NULL;
			continue;
		}

		/* Set the error flag which we'll clear on success. */
		participant->error = true;

		/*
		 * If another backend has moved the read head since our last read,
		 * we'll need to seek to that position.
		 */
		if (participant->read_fileno != accessor->read_fileno ||
			participant->read_offset != accessor->read_offset)
		{
			BufFileSeek(accessor->read_file,
						participant->read_fileno,
						participant->read_offset,
						SEEK_SET);
			accessor->read_fileno = participant->read_fileno;
			accessor->read_offset = participant->read_offset;
		}

		/*
		 * Read the optional meta-data, if configured for that.  If using
		 * meta-data, this it the point at which we'll discover the end of the
		 * file.
		 */
		eof = false;
		if (accessor->sts->meta_data_size > 0)
		{
			nread = BufFileRead(accessor->read_file, meta_data,
								accessor->sts->meta_data_size);
			if (nread == 0)
				eof = true;
			else if (nread != accessor->sts->meta_data_size)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from temporary file: %m")));
		}

		/*
		 * If we haven't already hit the end of the file above while trying to
		 * read per-tuple meta-data, then it's time to read the size.  If we
		 * aren't configured for meta-data, then this is the point at which we
		 * expect to discover the end of the file.
		 */
		if (!eof)
		{
			nread = BufFileRead(accessor->read_file, &tuple_size, sizeof(tuple_size));
			if (nread == 0)
				eof = true;
			else if (nread != sizeof(tuple_size))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from temporary file: %m")));
		}

		/*
		 * If we reached the end of the file, then check if we can delete
		 * backing files now.  Only one backend should ever attempt to delete
		 * the same file: the one that is first to each the end of the file.
		 */
		if (eof)
		{
			bool destroy = false;

			participant->eof = true;
			if ((accessor->sts->flags & SHARED_TUPLESTORE_SINGLE_PASS) != 0)
				destroy = true;
			participant->error = false;
			LWLockRelease(&participant->lock);

			if (destroy)
			{
				char name[MAXPGPATH];

				make_name(name, accessor->read_partition,
						  accessor->read_participant);
				BufFileDeleteShared(fileset, name, accessor->read_participant);
			}

			/* Move to next participant's file. */
			BufFileClose(accessor->read_file);
			accessor->read_file = NULL;
			continue;
		}

		/* Enlarge buffer if necessary. */
		if (tuple_size > accessor->buffer_size)
		{
			size_t new_buffer_size = Max(accessor->buffer_size * 2, tuple_size);
			void *new_buffer = palloc(new_buffer_size);

			pfree(accessor->buffer);
			accessor->buffer_size = new_buffer_size;
			accessor->buffer = new_buffer;
		}

		/* Read tuple. */
		tuple = (MinimalTuple) accessor->buffer;
		tuple->t_len = tuple_size;
		nread = BufFileRead(accessor->read_file,
							((char *) tuple) + sizeof(tuple->t_len),
							tuple_size - sizeof(tuple_size));
		if (nread != tuple_size - sizeof(tuple_size))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from temporary file: %m")));

		/*
		 * We could read more than one tuple into memory at once here and
		 * buffer for next time.
		 */

		/* Commit shared read head location to shmem and clear error flag. */
		BufFileTell(accessor->read_file,
					&participant->read_fileno,
					&participant->read_offset);
		accessor->read_fileno = participant->read_fileno;
		accessor->read_offset = participant->read_offset;
		participant->error = false;
		LWLockRelease(&participant->lock);

		break;
	}

	return tuple;
}

/*
 * Create the name used for our shared BufFiles.  The participant number must
 * also be used as a stripe number for the BufFile interface.
 */
static void
make_name(char *name, int partition, int participant)
{
	/* p = partition, b = backend */
	snprintf(name, MAXPGPATH, "p%d.b%d", partition, participant);
}
