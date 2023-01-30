/*-------------------------------------------------------------------------
 *
 * controllog_utils.c
 *		Common code for operation log file output.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/controllog_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "catalog/pg_controllog.h"
#include "common/controldata_utils.h"
#include "common/controllog_utils.h"
#include "common/file_perm.h"
#ifdef FRONTEND
#include "common/file_utils.h"
#include "common/logging.h"
#endif
#include "port/pg_crc32c.h"

#ifndef FRONTEND
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#endif

/*
 * Descriptions of supported operations of operation log.
 */
OperationLogTypeDesc OperationLogTypesDescs[] = {
	{OLT_BOOTSTRAP, OLM_INSERT, "bootstrap"},
	{OLT_STARTUP, OLM_MERGE, "startup"},
	{OLT_RESETWAL, OLM_MERGE, "pg_resetwal"},
	{OLT_REWIND, OLM_MERGE, "pg_rewind"},
	{OLT_UPGRADE, OLM_INSERT, "pg_upgrade"},
	{OLT_PROMOTED, OLM_INSERT, "promoted"}
};


/*
 * calculate_operation_log_crc()
 *
 * Calculate CRC of operation log.
 */
static pg_crc32c
calculate_operation_log_crc(OperationLogBuffer * log_buffer)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) log_buffer + sizeof(pg_crc32c),
				PG_OPERATION_LOG_FULL_SIZE - sizeof(pg_crc32c));
	FIN_CRC32C(crc);

	return crc;
}

/*
 * get_empty_operation_log()
 *
 * Function returns empty operation log buffer.
 */
OperationLogBuffer *
get_empty_operation_log_buffer(void)
{
	OperationLogBuffer *log_buffer;

	/* Initialize operation log file with zeros. */
	log_buffer = palloc0(PG_OPERATION_LOG_FULL_SIZE);

	/* Calculate CRC. */
	log_buffer->header.ol_crc = calculate_operation_log_crc(log_buffer);

	return log_buffer;
}

/*
 * create_operation_log_file()
 *
 * Create file for operation log and initialize it with zeros.
 * Function returns descriptor of created file or -1 in error case.
 * Function cannot generate report with ERROR and FATAL for correct lock
 * releasing on top level.
 */
static int
create_operation_log_file(char *ControlLogFilePath)
{
	int			fd;
	OperationLogBuffer *log_buffer;

#ifndef FRONTEND
	fd = OpenTransientFile(ControlLogFilePath, O_RDWR | O_CREAT | PG_BINARY);

	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m",
						PG_OPERATION_LOG_FILE)));
		return -1;
	}
#else
	fd = open(ControlLogFilePath, O_RDWR | O_CREAT | PG_BINARY, pg_file_create_mode);

	if (fd < 0)
		pg_fatal("could not create file \"%s\": %m",
				 ControlLogFilePath);
#endif

	/* Initialize operation log file with zeros. */
	log_buffer = get_empty_operation_log_buffer();

	errno = 0;
	if (write(fd, log_buffer, PG_OPERATION_LOG_FULL_SIZE) != PG_OPERATION_LOG_FULL_SIZE)
	{
		/* If write didn't set errno, assume problem is no disk space. */
		if (errno == 0)
			errno = ENOSPC;

#ifndef FRONTEND
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write operation log in the file \"%s\": %m",
						ControlLogFilePath)));
#else
		pg_fatal("could not write operation log in the file \"%s\": %m",
				 ControlLogFilePath);
#endif
		pfree(log_buffer);
		return -1;
	}

	pfree(log_buffer);

#ifndef FRONTEND
	if (pg_fsync(fd) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m",
						ControlLogFilePath)));
		return -1;
	}
#else
	if (fsync(fd) != 0)
		pg_fatal("could not fsync file \"%s\": %m", ControlLogFilePath);
#endif

	if (lseek(fd, 0, SEEK_SET) != 0)
	{
#ifndef FRONTEND
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not seek to position 0 of file \"%s\": %m",
						ControlLogFilePath)));
#else
		pg_fatal("could not seek to position 0 of file \"%s\": %m",
				 ControlLogFilePath);
#endif
		return -1;
	}

	return fd;
}

#define LWLockReleaseSaveErrno(lock) \
	save_errno = errno; \
	LWLockRelease(lock); \
	errno = save_errno; \

/*
 * get_operation_log()
 *
 * Get the operation log ring buffer. The result is returned as a palloc'd copy
 * of operation log buffer.
 *
 * crc_ok_p can be used by the caller to see whether the CRC of the operation
 * log is correct.
 */
OperationLogBuffer *
get_operation_log(const char *DataDir, bool *crc_ok_p)
{
	OperationLogBuffer *log_buffer = NULL;
	int			fd;
	char		ControlLogFilePath[MAXPGPATH];
	pg_crc32c	crc;
	int			r;
#ifndef FRONTEND
	int			save_errno;
#endif

	Assert(crc_ok_p);

	snprintf(ControlLogFilePath, MAXPGPATH, "%s/%s", DataDir, PG_OPERATION_LOG_FILE);

#ifndef FRONTEND
	LWLockAcquire(ControlLogFileLock, LW_EXCLUSIVE);
	fd = OpenTransientFile(ControlLogFilePath, O_RDONLY | PG_BINARY);
#else
	fd = open(ControlLogFilePath, O_RDONLY | PG_BINARY, 0);
#endif
	if (fd < 0)
	{
#ifndef FRONTEND
		LWLockReleaseSaveErrno(ControlLogFileLock);
#endif
		/* File doesn't exist - return empty operation log buffer. */
		if (errno == ENOENT)
		{
			*crc_ok_p = true; /* CRC will be calculated below. */
			return get_empty_operation_log_buffer();
		}
		else
		{
#ifndef FRONTEND
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" for reading: %m",
							ControlLogFilePath)));
#else
			pg_fatal("could not open file \"%s\" for reading: %m",
					 ControlLogFilePath);
#endif
		}
	}

	log_buffer = palloc(PG_OPERATION_LOG_FULL_SIZE);

	r = read(fd, log_buffer, PG_OPERATION_LOG_FULL_SIZE);
	if (r != PG_OPERATION_LOG_FULL_SIZE)
	{
#ifndef FRONTEND
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read operation log from the file \"%s\": read %d of %d",
						ControlLogFilePath, r, PG_OPERATION_LOG_FULL_SIZE)));
#else
		pg_fatal("could not read operation log from the file \"%s\": read %d of %d",
				 ControlLogFilePath, r, PG_OPERATION_LOG_FULL_SIZE);
#endif
	}

#ifndef FRONTEND
	if (CloseTransientFile(fd) != 0)
	{
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						ControlLogFilePath)));
	}
	LWLockRelease(ControlLogFileLock);
#else
	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", ControlLogFilePath);
#endif

	/* Check the CRC. */
	crc = calculate_operation_log_crc(log_buffer);

	*crc_ok_p = EQ_CRC32C(crc, log_buffer->header.ol_crc);

	return log_buffer;
}

/*
 * update_operation_log()
 *
 * Update the operation log ring buffer.
 * Note. To protect against failures a operation log file is written in two
 * stages: first a temporary file is created, then the temporary file is
 * renamed to the operation log file.
 */
void
update_operation_log(const char *DataDir, OperationLogBuffer * log_buffer)
{
	int			fd;
	char		ControlLogFilePath[MAXPGPATH];
	char		ControlLogFilePathTmp[MAXPGPATH];
#ifndef FRONTEND
	int			save_errno;
#endif

	snprintf(ControlLogFilePath, sizeof(ControlLogFilePath), "%s/%s", DataDir,
			 PG_OPERATION_LOG_FILE);
	snprintf(ControlLogFilePathTmp, sizeof(ControlLogFilePathTmp), "%s.tmp",
			 ControlLogFilePath);

	/* Recalculate CRC of operation log. */
	log_buffer->header.ol_crc = calculate_operation_log_crc(log_buffer);

#ifndef FRONTEND
	LWLockAcquire(ControlLogFileLock, LW_EXCLUSIVE);
#endif

	/* Create a temporary file. */
	fd = create_operation_log_file(ControlLogFilePathTmp);
	if (fd < 0)
	{
#ifndef FRONTEND
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						ControlLogFilePathTmp)));
#else
		pg_fatal("could not open file \"%s\": %m", ControlLogFilePathTmp);
#endif
	}

	/* Place operation log buffer into temporary file. */
	errno = 0;
	if (write(fd, log_buffer, PG_OPERATION_LOG_FULL_SIZE) != PG_OPERATION_LOG_FULL_SIZE)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;

#ifndef FRONTEND
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write operation log in the file \"%s\": %m",
						ControlLogFilePathTmp)));
#else
		pg_fatal("could not write operation log in the file \"%s\": %m",
				 ControlLogFilePathTmp);
#endif
	}

	/* Close the temporary file. */
#ifndef FRONTEND
	if (CloseTransientFile(fd) != 0)
	{
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						ControlLogFilePathTmp)));
	}
#else
	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", ControlLogFilePathTmp);
#endif

	/* Rename temporary file to required name. */
#ifndef FRONTEND
	if (durable_rename(ControlLogFilePathTmp, ControlLogFilePath, LOG) != 0)
	{
		LWLockReleaseSaveErrno(ControlLogFileLock);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						ControlLogFilePathTmp, ControlLogFilePath)));
	}
	LWLockRelease(ControlLogFileLock);
#else
	if (durable_rename(ControlLogFilePathTmp, ControlLogFilePath) != 0)
		pg_fatal("could not rename file \"%s\" to \"%s\": %m",
				 ControlLogFilePathTmp, ControlLogFilePath);
#endif
}

/*
 * is_enum_value_correct()
 *
 * Function returns true in case value is correct value of enum.
 *
 * val - test value;
 * minval - first enum value (correct value);
 * maxval - last enum value (incorrect value).
 */
static bool
is_enum_value_correct(int16 val, int16 minval, int16 maxval)
{
	Assert(val >= minval || val < maxval);

	if (val < minval || val >= maxval)
		return false;
	return true;
}

/*
 * get_operation_log_type_desc()
 *
 * Function returns pointer to OperationLogTypeDesc struct for given type of
 * operation ol_type.
 */
static OperationLogTypeDesc *
get_operation_log_type_desc(ol_type_enum ol_type)
{
	return &OperationLogTypesDescs[ol_type - 1];
}

/*
 * fill_operation_log_element()
 *
 * Fill new operation log element. Value of ol_lsn is last checkpoint record
 * pointer.
 */
static void
fill_operation_log_element(ControlFileData *ControlFile,
						   OperationLogTypeDesc * desc,
						   PgNumEdition edition, uint32 version_num,
						   OperationLogData * data)
{
	data->ol_type = desc->ol_type;
	data->ol_edition = edition;
	data->ol_count = 1;
	data->ol_version = version_num;
	data->ol_timestamp = (pg_time_t) time(NULL);
	data->ol_lsn = ControlFile->checkPoint;
}

/*
 * find_operation_log_element_for_merge()
 *
 * Find element into operation log ring buffer by ol_type and version.
 * Returns NULL in case element is not found.
 */
static OperationLogData *
find_operation_log_element_for_merge(ol_type_enum ol_type,
									 OperationLogBuffer * log_buffer,
									 PgNumEdition edition, uint32 version_num)
{
	uint32		first = log_buffer->header.ol_first;
	uint32		count = get_operation_log_count(log_buffer);
	OperationLogData *data;
	uint32		i;

	Assert(first < PG_OPERATION_LOG_COUNT && count <= PG_OPERATION_LOG_COUNT);

	for (i = 0; i < count; i++)
	{
		data = &log_buffer->data[(first + i) % PG_OPERATION_LOG_COUNT];
		if (data->ol_type == ol_type &&
			data->ol_edition == edition &&
			data->ol_version == version_num)
			return data;
	}

	return NULL;
}

/*
 * put_operation_log_element(), put_operation_log_element_version()
 *
 * Put element into operation log ring buffer.
 *
 * DataDir is the path to the top level of the PGDATA directory tree;
 * ol_type is type of operation;
 * edition is edition of current PostgreSQL version;
 * version_num is number of version (for example 13000802 for v13.8.2).
 *
 * Note that it is up to the caller to properly lock ControlFileLogLock when
 * calling this routine in the backend.
 */
void
put_operation_log_element_version(const char *DataDir, ol_type_enum ol_type,
								  PgNumEdition edition, uint32 version_num)
{
	OperationLogBuffer *log_buffer;
	ControlFileData *ControlFile;
	bool		crc_ok;
	OperationLogTypeDesc *desc;

	if (!is_enum_value_correct(ol_type, OLT_BOOTSTRAP, OLT_NumberOfTypes))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid type of operation (%u) for operation log", ol_type)));
#else
		pg_fatal("invalid type of operation (%u) for operation log", ol_type);
#endif
	}

	desc = get_operation_log_type_desc(ol_type);

	if (!is_enum_value_correct(desc->ol_mode, OLM_MERGE, OLM_NumberOfModes))
	{
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid mode of operation (%u) for operation log", ol_type)));
#else
		pg_fatal("invalid mode of operation (%u) for operation log", ol_type);
#endif
	}

	/* get a copy of the control file */
	ControlFile = get_controlfile(DataDir, &crc_ok);
	if (!crc_ok)
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control CRC value is incorrect")));
#else
		pg_fatal("pg_control CRC value is incorrect");
#endif

	/* get a copy of the operation log */
	log_buffer = get_operation_log(DataDir, &crc_ok);
	if (!crc_ok)
#ifndef FRONTEND
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("pg_control_log CRC value is incorrect")));
#else
		pg_fatal("pg_control_log CRC value is incorrect");
#endif

	switch (desc->ol_mode)
	{
		case OLM_MERGE:
			{
				OperationLogData *data;

				data = find_operation_log_element_for_merge(ol_type, log_buffer,
															edition, version_num);
				if (data)
				{
					/*
					 * We just found the element with the same type and the
					 * same version. Update it.
					 */
					if (data->ol_count < PG_UINT16_MAX) /* prevent overflow */
						data->ol_count++;
					data->ol_timestamp = (pg_time_t) time(NULL);
					data->ol_lsn = ControlFile->checkPoint;
					break;
				}
			}
			/* FALLTHROUGH */

		case OLM_INSERT:
			{
				uint16		first = log_buffer->header.ol_first;
				uint16		count = log_buffer->header.ol_count;
				uint16		current;

				Assert(first < PG_OPERATION_LOG_COUNT && count <= PG_OPERATION_LOG_COUNT);

				if (count == PG_OPERATION_LOG_COUNT)
				{
					current = first;
					/* Owerflow, shift the first element */
					log_buffer->header.ol_first = (first + 1) % PG_OPERATION_LOG_COUNT;
				}
				else
				{
					current = first + count;
					/* Increase number of elements: */
					log_buffer->header.ol_count++;
				}

				/* Fill operation log element. */
				fill_operation_log_element(ControlFile, desc, edition, version_num,
										   &log_buffer->data[current]);
				break;
			}

		default:
#ifndef FRONTEND
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected operation log mode %d",
							desc->ol_mode)));
#else
			pg_fatal("unexpected operation log mode %d", desc->ol_mode);
#endif
	}

	update_operation_log(DataDir, log_buffer);

	pfree(log_buffer);

	pfree(ControlFile);
}

/*
 * Helper constant for determine current edition.
 * Here can be custom editions.
 */
static const uint8 current_edition = ED_PG_ORIGINAL;

/*
 * Helper constant for determine current version.
 * Multiplier 100 used as reserve of last two digits for patch number.
 */
static const uint32 current_version_num = PG_VERSION_NUM * 100;

void
put_operation_log_element(const char *DataDir, ol_type_enum ol_type)
{
	put_operation_log_element_version(DataDir, ol_type, current_edition, current_version_num);
}

/*
 * get_operation_log_element()
 *
 * Returns operation log buffer element with number num.
 */
OperationLogData *
get_operation_log_element(OperationLogBuffer * log_buffer, uint16 num)
{
	uint32		first = log_buffer->header.ol_first;
#ifdef USE_ASSERT_CHECKING
	uint32		count = get_operation_log_count(log_buffer);

	Assert(num < count);
#endif

	return &log_buffer->data[(first + num) % PG_OPERATION_LOG_COUNT];
}

/*
 * get_operation_log_count()
 *
 * Returns number of elements in given operation log buffer.
 */
uint16
get_operation_log_count(OperationLogBuffer * log_buffer)
{
	return log_buffer->header.ol_count;
}

/*
 * get_operation_log_type_name()
 *
 * Returns name of given type.
 */
const char *
get_operation_log_type_name(ol_type_enum ol_type)
{
	if (is_enum_value_correct(ol_type, OLT_BOOTSTRAP, OLT_NumberOfTypes))
		return OperationLogTypesDescs[ol_type - 1].ol_name;
	else
		return psprintf("unknown name %u", ol_type);
}
