/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"


/*
 * SMgrFileLocator contains all the information needed to locate the physical
 * storage of a relation fork, or some other file that is managed by the buffer
 * manager.
 *
 * The backend ID is InvalidBackendId for regular relations (those accessible
 * to more than one backend), or the owning backend's ID for backend-local
 * relations. Backend-local relations are always transient and removed in
 * case of a database crash; they are never WAL-logged or fsync'd.
 */
typedef struct SMgrFileLocator
{
	RelFileLocator locator;
	BackendId	backend;
	ForkNumber	forknum;
} SMgrFileLocator;

#define SMgrFileLocatorIsTemp(slocator) \
	((slocator).backend != InvalidBackendId)

/*
 * smgr.c maintains a table of SMgrFileData objects, which are essentially
 * cached file handles.  An SMgrFile is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrFile may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrFile is
 * closed.  We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrFiles that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 *
 * A file that is represented by an SMgrFile can be managed by the buffer
 * manager. Currently, it's only used for relation files, but could be used
 * for SLRUs and other things in the future.
 */
typedef struct SMgrFileData
{
	/* locator is the hashtable lookup key, so must be first! */
	SMgrFileLocator smgr_locator;	/* file physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrFileData **smgr_owner;

	/*
	 * The following fields are reset to InvalidBlockNumber upon a cache flush
	 * event, and hold the last known size for each fork.  This information is
	 * currently only reliable during recovery, since there is no cache
	 * invalidation for fork extension.
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_cached_nblocks;	/* last known size */

	/* additional public fields may someday exist here */

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	int			smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs;
	struct _MdfdVec *md_seg_fds;

	/* if unowned, list link in list of all unowned SMgrFiles */
	dlist_node	node;
} SMgrFileData;

typedef SMgrFileData *SMgrFileHandle;

#define SmgrIsTemp(smgr) \
	SMgrFileLocatorIsTemp((smgr)->smgr_locator)

extern void smgrinit(void);
extern SMgrFileHandle smgropen(RelFileLocator rlocator, BackendId backend, ForkNumber forkNum);
extern bool smgrexists(SMgrFileHandle sfile);
extern void smgrsetowner(SMgrFileHandle *owner, SMgrFileHandle sfile);
extern void smgrclearowner(SMgrFileHandle *owner, SMgrFileHandle sfile);
extern void smgrclose(SMgrFileHandle sfile);
extern void smgrcloseall(void);
extern void smgrreleaseall(void);
extern void smgrcreate(SMgrFileHandle sfile, bool isRedo);
extern void smgrextend(SMgrFileHandle sfile,
					   BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool smgrprefetch(SMgrFileHandle sfile, BlockNumber blocknum);
extern void smgrread(SMgrFileHandle sfile,
					 BlockNumber blocknum, char *buffer);
extern void smgrwrite(SMgrFileHandle sfile,
					  BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrwriteback(SMgrFileHandle sfile,
						  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrFileHandle sfile);
extern BlockNumber smgrnblocks_cached(SMgrFileHandle sfile);
extern void smgrimmedsync(SMgrFileHandle sfile);
extern void smgrunlink(SMgrFileHandle sfile, bool isRedo);

extern void smgrtruncate_multi(RelFileLocator rlocator, BackendId backend, ForkNumber *forks, int nforks, BlockNumber *nblocks);
extern void smgrunlink_multi(RelFileLocator rlocator, BackendId backend, ForkNumber *forks, int nforks, bool isRedo);

extern void smgrcloserellocator(RelFileLocator rlocator, BackendId backend);

extern void AtEOXact_SMgr(void);
extern bool ProcessBarrierSmgrRelease(void);

#endif							/* SMGR_H */
