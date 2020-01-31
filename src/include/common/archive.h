/*-------------------------------------------------------------------------
 *
 * archive.h
 *	  Common WAL archive routines
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/common/archive.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ARCHIVE_H
#define ARCHIVE_H

extern int ConstructRestoreCommand(const char *restoreCommand,
								   const char *xlogpath,
								   const char *xlogfname,
								   const char *lastRestartPointFname,
								   char *result);

#endif							/* ARCHIVE_H */
