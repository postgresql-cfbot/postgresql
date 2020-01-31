/*-------------------------------------------------------------------------
 *
 * fe_archive.h
 *	  Routines to access WAL archive from frontend
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/fe_archive.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FE_ARCHIVE_H
#define FE_ARCHIVE_H

extern int RestoreArchivedWALFile(const char *path, const char *xlogfname,
								  off_t expectedSize, const char *restoreCommand);

#endif							/* FE_ARCHIVE_H */
