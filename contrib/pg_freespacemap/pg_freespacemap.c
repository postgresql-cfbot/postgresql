/*-------------------------------------------------------------------------
 *
 * pg_freespacemap.c
 *	  display contents of a free space map
 *
 *	  contrib/pg_freespacemap/pg_freespacemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/xloginsert.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * Returns the amount of free space on a given page, according to the
 * free space map.
 */
PG_FUNCTION_INFO_V1(pg_freespace);

PG_FUNCTION_INFO_V1(pg_truncate_freespace_map);

Datum
pg_freespace(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int64		blkno = PG_GETARG_INT64(1);
	int16		freespace;
	Relation	rel;

	rel = relation_open(relid, AccessShareLock);

	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number")));

	freespace = GetRecordedFreeSpace(rel, blkno);

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT16(freespace);
}


/*
 * Remove the free space map for a relation.
 *
 * This is useful in case of corruption of the FSM, as the
 * only other options are either triggering a VACUUM FULL or
 * shutting down the server and removing the FSM on the filesystem.
 */
Datum
pg_truncate_freespace_map(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	ForkNumber	fork;
	BlockNumber block;

	rel = relation_open(relid, AccessExclusiveLock);

	/* Only some relkinds have a freespace map */
	if (!RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is of wrong relation kind",
						RelationGetRelationName(rel)),
				 errdetail_relkind_not_supported(rel->rd_rel->relkind)));


	/* Forcibly reset cached file size */
	RelationGetSmgr(rel)->smgr_cached_nblocks[FSM_FORKNUM] = InvalidBlockNumber;

	/* Just pretend we're going to wipeout the whole rel */
	block = FreeSpaceMapPrepareTruncateRel(rel, 0);

	if (BlockNumberIsValid(block))
	{
		fork = FSM_FORKNUM;
		smgrtruncate(RelationGetSmgr(rel), &fork, 1, &block);
	}

	if (RelationNeedsWAL(rel))
	{
		xl_smgr_truncate xlrec;

		xlrec.blkno = 0;
		xlrec.rlocator = rel->rd_locator;
		xlrec.flags = SMGR_TRUNCATE_FSM;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));

		XLogInsert(RM_SMGR_ID, XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);
	}

	relation_close(rel, AccessExclusiveLock);

	PG_RETURN_VOID();
}
