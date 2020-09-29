/*-------------------------------------------------------------------------
 *
 * dbasedesc.c
 *	  rmgr descriptor routines for commands/dbcommands.c
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/dbasedesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/dbcommands_xlog.h"
#include "lib/stringinfo.h"


void
dbase_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char		*dbpath1, *dbpath2;

	if (info == XLOG_DBASE_CREATE)
	{
		xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *) rec;

		dbpath1 = GetDatabasePath(xlrec->src_db_id,  xlrec->src_tablespace_id);
		dbpath2 = GetDatabasePath(xlrec->db_id, xlrec->tablespace_id);
		appendStringInfo(buf, "copy dir %s to %s", dbpath1, dbpath2);
		pfree(dbpath2);
		pfree(dbpath1);
	}
	else if (info == XLOG_DBASE_DROP)
	{
		xl_dbase_drop_rec *xlrec = (xl_dbase_drop_rec *) rec;
		int			i;

		appendStringInfo(buf, "dir");
		for (i = 0; i < xlrec->ntablespaces; i++)
		{
			dbpath1 = GetDatabasePath(xlrec->db_id, xlrec->tablespace_ids[i]);
			appendStringInfo(buf,  " %s", dbpath1);
			pfree(dbpath1);
		}
	}
}

const char *
dbase_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_DBASE_CREATE:
			id = "CREATE";
			break;
		case XLOG_DBASE_DROP:
			id = "DROP";
			break;
	}

	return id;
}
