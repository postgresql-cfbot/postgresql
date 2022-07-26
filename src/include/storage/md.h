/*-------------------------------------------------------------------------
 *
 * md.h
 *	  magnetic disk storage manager public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/md.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MD_H
#define MD_H

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* md storage manager functionality */
extern void mdinit(void);
extern void mdopen(SMgrFileHandle sfile);
extern void mdclose(SMgrFileHandle sfile);
extern void mdcreate(SMgrFileHandle sfile, bool isRedo);
extern bool mdexists(SMgrFileHandle sfile);
extern void mdunlink(SMgrFileLocator slocator, bool isRedo);
extern void mdextend(SMgrFileHandle sfile,
					 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool mdprefetch(SMgrFileHandle sfile,
					   BlockNumber blocknum);
extern void mdread(SMgrFileHandle sfile, BlockNumber blocknum,
				   char *buffer);
extern void mdwrite(SMgrFileHandle sfile,
					BlockNumber blocknum, char *buffer, bool skipFsync);
extern void mdwriteback(SMgrFileHandle sfile,
						BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber mdnblocks(SMgrFileHandle sfile);
extern void mdtruncate(SMgrFileHandle sfile, BlockNumber nblocks);
extern void mdimmedsync(SMgrFileHandle sfile);

extern void ForgetDatabaseSyncRequests(Oid dbid);

/* md sync callbacks */
extern int	mdsyncfiletag(const FileTag *ftag, char *path);
extern int	mdunlinkfiletag(const FileTag *ftag, char *path);
extern bool mdfiletagmatches(const FileTag *ftag, const FileTag *candidate);

#endif							/* MD_H */
