/*
 * controldata_utils.h
 *		Common code for pg_controldata output
 *
 *	Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/include/common/controldata_utils.h
 */
#ifndef COMMON_CONTROLDATA_UTILS_H
#define COMMON_CONTROLDATA_UTILS_H

#include "catalog/pg_control.h"

#define UPDATE_CONTROLFILE_FSYNC 1
#define UPDATE_CONTROLFILE_FDATASYNC 2
#define UPDATE_CONTROLFILE_O_SYNC 3
#define UPDATE_CONTROLFILE_O_DSYNC 4

extern ControlFileData *get_controlfile(const char *DataDir, bool *crc_ok_p);
extern void update_controlfile(const char *DataDir,
							   ControlFileData *ControlFile, int sync_op);

#endif							/* COMMON_CONTROLDATA_UTILS_H */
