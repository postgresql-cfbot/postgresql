/*-------------------------------------------------------------------------
 *
 * File and directory permission constants and routines
 *
 * Group read/execute may optional be enabled on PGDATA so any frontend tools
 * That write into PGDATA must know what mask to set and the permissions to
 * use for creating files and directories.
 *
 * This module is located in common so the backend can use the constants.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/file_perm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILE_PERM_H
#define FILE_PERM_H

/*
 * Default mode mask for data directory permissions that does not allow
 * group execute/read.
 */
#define PG_MODE_MASK_DEFAULT		(S_IRWXG | S_IRWXO)

/*
 * Optional mode mask for data directory permissions that allows group
 * read/execute.
 */
#define PG_MODE_MASK_ALLOW_GROUP	(S_IWGRP | S_IRWXO)

/*
 * Default mode for created files, unless something else is specified using
 * the *Perm() function variants.
 */
#define PG_FILE_MODE_DEFAULT		(S_IRUSR | S_IWUSR | S_IRGRP)

/*
 * Default mode for directories created with MakeDirectoryDefaultPerm().
 */
#define PG_DIR_MODE_DEFAULT			(S_IRWXU | S_IRGRP | S_IXGRP)

#ifdef FRONTEND

/*
 * Determine the mask to use when writing to PGDATA
 */
mode_t DataDirectoryMask(const char *dataDir);

#endif							/* FRONTEND */


#endif							/* FILE_PERM_H */
