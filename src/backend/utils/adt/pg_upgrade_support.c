/*
 *	pg_upgrade_support.c
 *
 *	server-side functions to set backend global variables
 *	to control oid and relfilenumber assignment, and do other special
 *	hacks needed for pg_upgrade.
 *
 *	Copyright (c) 2010-2023, PostgreSQL Global Development Group
 *	src/backend/utils/adt/pg_upgrade_support.c
 */

#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/binary_upgrade.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "storage/standbydefs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"


#define CHECK_IS_BINARY_UPGRADE									\
do {															\
	if (!IsBinaryUpgrade)										\
		ereport(ERROR,											\
				(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),	\
				 errmsg("function can only be called when server is in binary upgrade mode"))); \
} while (0)

Datum
binary_upgrade_set_next_pg_tablespace_oid(PG_FUNCTION_ARGS)
{
	Oid			tbspoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_pg_tablespace_oid = tbspoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_array_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_multirange_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_mrng_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_multirange_array_pg_type_oid(PG_FUNCTION_ARGS)
{
	Oid			typoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_mrng_array_pg_type_oid = typoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_heap_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_heap_pg_class_oid = reloid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_heap_relfilenode(PG_FUNCTION_ARGS)
{
	RelFileNumber relfilenumber = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_heap_pg_class_relfilenumber = relfilenumber;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_index_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_index_pg_class_oid = reloid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_index_relfilenode(PG_FUNCTION_ARGS)
{
	RelFileNumber relfilenumber = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_index_pg_class_relfilenumber = relfilenumber;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_toast_pg_class_oid(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_toast_pg_class_oid = reloid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_toast_relfilenode(PG_FUNCTION_ARGS)
{
	RelFileNumber relfilenumber = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_toast_pg_class_relfilenumber = relfilenumber;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_enum_oid(PG_FUNCTION_ARGS)
{
	Oid			enumoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_pg_enum_oid = enumoid;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_next_pg_authid_oid(PG_FUNCTION_ARGS)
{
	Oid			authoid = PG_GETARG_OID(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_next_pg_authid_oid = authoid;
	PG_RETURN_VOID();
}

Datum
binary_upgrade_create_empty_extension(PG_FUNCTION_ARGS)
{
	text	   *extName;
	text	   *schemaName;
	bool		relocatable;
	text	   *extVersion;
	Datum		extConfig;
	Datum		extCondition;
	List	   *requiredExtensions;

	CHECK_IS_BINARY_UPGRADE;

	/* We must check these things before dereferencing the arguments */
	if (PG_ARGISNULL(0) ||
		PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) ||
		PG_ARGISNULL(3))
		elog(ERROR, "null argument to binary_upgrade_create_empty_extension is not allowed");

	extName = PG_GETARG_TEXT_PP(0);
	schemaName = PG_GETARG_TEXT_PP(1);
	relocatable = PG_GETARG_BOOL(2);
	extVersion = PG_GETARG_TEXT_PP(3);

	if (PG_ARGISNULL(4))
		extConfig = PointerGetDatum(NULL);
	else
		extConfig = PG_GETARG_DATUM(4);

	if (PG_ARGISNULL(5))
		extCondition = PointerGetDatum(NULL);
	else
		extCondition = PG_GETARG_DATUM(5);

	requiredExtensions = NIL;
	if (!PG_ARGISNULL(6))
	{
		ArrayType  *textArray = PG_GETARG_ARRAYTYPE_P(6);
		Datum	   *textDatums;
		int			ndatums;
		int			i;

		deconstruct_array_builtin(textArray, TEXTOID, &textDatums, NULL, &ndatums);
		for (i = 0; i < ndatums; i++)
		{
			char	   *extName = TextDatumGetCString(textDatums[i]);
			Oid			extOid = get_extension_oid(extName, false);

			requiredExtensions = lappend_oid(requiredExtensions, extOid);
		}
	}

	InsertExtensionTuple(text_to_cstring(extName),
						 GetUserId(),
						 get_namespace_oid(text_to_cstring(schemaName), false),
						 relocatable,
						 text_to_cstring(extVersion),
						 extConfig,
						 extCondition,
						 requiredExtensions);

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_record_init_privs(PG_FUNCTION_ARGS)
{
	bool		record_init_privs = PG_GETARG_BOOL(0);

	CHECK_IS_BINARY_UPGRADE;
	binary_upgrade_record_init_privs = record_init_privs;

	PG_RETURN_VOID();
}

Datum
binary_upgrade_set_missing_value(PG_FUNCTION_ARGS)
{
	Oid			table_id = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	text	   *value = PG_GETARG_TEXT_P(2);
	char	   *cattname = text_to_cstring(attname);
	char	   *cvalue = text_to_cstring(value);

	CHECK_IS_BINARY_UPGRADE;
	SetAttrMissing(table_id, cattname, cvalue);

	PG_RETURN_VOID();
}

/*
 * Helper function for binary_upgrade_validate_wal_logical_end().
 */
static inline bool
is_xlog_record_type(RmgrId rmgrid, uint8 info,
					RmgrId expected_rmgrid, uint8 expected_info)
{
	return (rmgrid == expected_rmgrid) && (info == expected_info);
}

/*
 * Return false if we found unexpected WAL records, otherwise true.
 *
 * This is a special purpose function to ensure that there are no WAL records
 * pending to be decoded after the given LSN.
 *
 * It is used to ensure that there is no pending WAL to be consumed for
 * the logical slots.
 *
 * During the upgrade process there can be certain types of WAL records
 * generated that don't need to be decoded. Such records are ignored.
 *
 * XLOG_CHECKPOINT_SHUTDOWN and XLOG_SWITCH are ignored because they would be
 * inserted after the walsender exits. Moreover, the following types of records
 * could be generated during the pg_upgrade --check, so they are ignored too:
 * XLOG_CHECKPOINT_ONLINE, XLOG_RUNNING_XACTS, XLOG_FPI_FOR_HINT,
 * XLOG_HEAP2_PRUNE, XLOG_PARAMETER_CHANGE.
 */
Datum
binary_upgrade_validate_wal_logical_end(PG_FUNCTION_ARGS)
{
	XLogRecPtr	start_lsn;
	XLogReaderState *xlogreader;
	bool		initial_record = true;
	bool		is_valid = true;

	CHECK_IS_BINARY_UPGRADE;

	/* Quick exit if the input is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(false);

	start_lsn = PG_GETARG_LSN(0);

	/* Quick exit if the given lsn is larger than current one */
	if (start_lsn >= GetFlushRecPtr(NULL))
		PG_RETURN_BOOL(false);

	xlogreader = InitXLogReaderState(start_lsn);

	/* Loop until all WALs are read, or unexpected record is found */
	while (is_valid && ReadNextXLogRecord(xlogreader))
	{
		RmgrIds		rmid;
		uint8		info;

		/* Check the type of WAL */
		rmid = XLogRecGetRmid(xlogreader);
		info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;

		if (initial_record)
		{
			/*
			 * Initial record must be either XLOG_CHECKPOINT_SHUTDOWN or
			 * XLOG_SWITCH.
			 */
			is_valid = is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN) ||
				is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_SWITCH);

			initial_record = false;
			continue;
		}

		/*
		 * There is a possibility that following records may be generated
		 * during the upgrade.
		 */
		is_valid = is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN) ||
			is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE) ||
			is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_SWITCH) ||
			is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_FPI_FOR_HINT) ||
			is_xlog_record_type(rmid, info, RM_XLOG_ID, XLOG_PARAMETER_CHANGE) ||
			is_xlog_record_type(rmid, info, RM_STANDBY_ID, XLOG_RUNNING_XACTS) ||
			is_xlog_record_type(rmid, info, RM_HEAP2_ID, XLOG_HEAP2_PRUNE);

		CHECK_FOR_INTERRUPTS();
	}

	pfree(xlogreader->private_data);
	XLogReaderFree(xlogreader);

	PG_RETURN_BOOL(is_valid);
}
