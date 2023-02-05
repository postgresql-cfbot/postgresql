/*-------------------------------------------------------------------------
 *
 * pg_controllog.h
 *	  The system operation log file "pg_control_log" is not a heap
 *    relation.
 *	  However, we define it here so that the format is documented.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_controllog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CONTROLLOG_H
#define PG_CONTROLLOG_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "pgtime.h"				/* for pg_time_t */
#include "port/pg_crc32c.h"

#define PG_OPERATION_LOG_FILE	"global/pg_control_log"

/*
 * Type of operation for operation log.
 */
typedef enum
{
	OLT_BOOTSTRAP = 1,			/* bootstrap */
	OLT_STARTUP,				/* server startup */
	OLT_RESETWAL,				/* pg_resetwal */
	OLT_REWIND,					/* pg_rewind */
	OLT_UPGRADE,				/* pg_upgrade */
	OLT_PROMOTED,				/* promoted */
	OLT_NumberOfTypes			/* should be last */
}			ol_type_enum;

/*
 * Mode of operation processing.
 */
typedef enum
{
	OLM_MERGE = 1,				/* insert element only if not exists element
								 * with the same ol_type and ol_version;
								 * otherwise update existing element */
	OLM_INSERT,					/* insert element into ring buffer 'as is' */
	OLM_NumberOfModes			/* should be last */
}			ol_mode_enum;

/*
 * Helper struct for describing supported operations.
 */
typedef struct OperationLogTypeDesc
{
	ol_type_enum ol_type;		/* element type */
	ol_mode_enum ol_mode;		/* element mode */
	const char *ol_name;		/* display name of element */
}			OperationLogTypeDesc;

/*
 * Element of operation log ring buffer (24 bytes).
 */
typedef struct OperationLogData
{
	uint8		ol_type;		/* operation type */
	uint8		ol_edition;		/* postgres edition */
	uint16		ol_count;		/* number of operations */
	uint32		ol_version;		/* postgres version */
	pg_time_t	ol_timestamp;	/* = int64, operation date/time */
	XLogRecPtr	ol_lsn;			/* = uint64, last check point record ptr */
}			OperationLogData;

/*
 * Header of operation log ring buffer (8 bytes).
 */
typedef struct OperationLogHeader
{
	pg_crc32c	ol_crc;			/* CRC of operation log ... MUST BE FIRST! */
	uint16		ol_first;		/* position of first ring buffer element */
	uint16		ol_count;		/* number of elements in ring buffer */
}			OperationLogHeader;

/*
 * Whole size of the operation log ring buffer (with header).
 */
#define PG_OPERATION_LOG_FULL_SIZE	8192

/*
 * Size of elements of the operation log ring buffer.
 * Value must be a multiple of sizeof(OperationLogData).
 */
#define PG_OPERATION_LOG_SIZE			(PG_OPERATION_LOG_FULL_SIZE - sizeof(OperationLogHeader))

/*
 * Number of elements in the operation log.
 */
#define PG_OPERATION_LOG_COUNT			(PG_OPERATION_LOG_SIZE / sizeof(OperationLogData))

/*
 * Operation log ring buffer.
 */
typedef struct OperationLogBuffer
{
	OperationLogHeader header;
	OperationLogData data[PG_OPERATION_LOG_COUNT];

}			OperationLogBuffer;

StaticAssertDecl(sizeof(OperationLogBuffer) == PG_OPERATION_LOG_FULL_SIZE,
				 "structure OperationLogBuffer must have size PG_OPERATION_LOG_FULL_SIZE");

/* Enum for postgres edition. */
typedef enum
{
	ED_PG_ORIGINAL = 0
	/* Here can be custom editions */
} PgNumEdition;

#define ED_PG_ORIGINAL_STR		"vanilla"
#define ED_UNKNOWN_STR			"unknown"

/*
 * get_str_edition()
 *
 * Returns edition string by edition number.
 */
static inline const char *
get_str_edition(PgNumEdition edition)
{
	switch (edition)
	{
		case ED_PG_ORIGINAL:
			return ED_PG_ORIGINAL_STR;

			/* Here can be custom editions */
	}
	return ED_UNKNOWN_STR;
}

#endif							/* PG_CONTROLLOG_H */
