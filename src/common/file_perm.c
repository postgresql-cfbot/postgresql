/*-------------------------------------------------------------------------
 *
 * File and directory permission routines
 *
 * Group read/execute may optional be enabled on PGDATA so any frontend tools
 * That write into PGDATA must know what mask to set and the permissions to
 * use for creating files and directories.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/file_perm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>

#include "common/file_perm.h"


/*
 * Determine the mask to use when writing to PGDATA.
 *
 * Errors are not handled here and should be checked by the frontend
 * application.
 */
#ifdef FRONTEND

mode_t
DataDirectoryMask(const char *dataDir)
{
	struct stat statBuf;

	/*
	 * If an error occurs getting the mode then return the more restrictive mask.
	 * It is the reponsibility of the frontend application to generate an error.
	 */
	if (stat(dataDir, &statBuf) != 0)
		return PG_MODE_MASK_DEFAULT;

	/*
	 * Construct the mask that the caller should pass to umask().
	 */
	return PG_MODE_MASK_DEFAULT & ~statBuf.st_mode;
}

#endif 						/* FRONTEND */
