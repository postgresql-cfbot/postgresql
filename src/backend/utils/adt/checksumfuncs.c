/*-------------------------------------------------------------------------
 *
 * checksumfuncs.c
 *	  Functions for checksums related feature such as online verification
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/checksumfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "catalog/pg_authid_d.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void check_one_relation(TupleDesc tupdesc, Tuplestorestate *tupstore,
					 Oid relid, ForkNumber single_forknum);
static void check_relation_fork(TupleDesc tupdesc, Tuplestorestate *tupstore,
									  Relation relation, ForkNumber forknum);
static void pg_check_relation_internal(FunctionCallInfo fcinfo, Oid relid,
									   Oid forknum);


Datum
pg_check_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = InvalidOid;

	pg_check_relation_internal(fcinfo, relid, InvalidForkNumber);

	return (Datum) 0;
}

Datum
pg_check_relation_fork(PG_FUNCTION_ARGS)
{
	Oid			relid = InvalidOid;
	const char *forkname;
	ForkNumber	forknum;

	forkname = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
	forknum = forkname_to_number(forkname);

	pg_check_relation_internal(fcinfo, relid, forknum);

	return (Datum) 0;
}

/* Common code for all versions of pg_check_relation() */
static void
pg_check_relation_internal(FunctionCallInfo fcinfo, Oid relid, Oid forknum)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!DataChecksumsEnabled())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("data checksums are not enabled in cluster")));

	if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_STAT_SCAN_TABLES))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superuser or a member of the pg_stat_scan_tables role may use this function")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	relid = PG_GETARG_OID(0);

	/* Set cost-based checksum verification delay */
	ChecksumCostActive = (ChecksumCostDelay > 0);
	ChecksumCostBalance = 0;

	check_one_relation(tupdesc, tupstore, relid, forknum);

	tuplestore_donestoring(tupstore);
}

/*
 * Perform the check on a single relation, possibly filtered with a single
 * fork.  This function will check if the given relation exists or not, as
 * a relation could be dropped after checking for the list of relations and
 * before getting here, and we don't want to error out in this case.
 */
static void
check_one_relation(TupleDesc tupdesc, Tuplestorestate *tupstore,
					 Oid relid, ForkNumber single_forknum)
{
	Relation	relation = NULL;
	ForkNumber	forknum;

	relation = relation_open(relid, AccessShareLock);

	/* sanity checks */
	if (!RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" does not have storage to be checked",
				 RelationGetRelationName(relation))));

	if (RELATION_IS_OTHER_TEMP(relation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot verify temporary tables of other sessions")));

	RelationOpenSmgr(relation);

	for (forknum = 0; forknum <= MAX_FORKNUM; forknum++)
	{
		if (single_forknum != InvalidForkNumber && single_forknum != forknum)
			continue;

		if (smgrexists(relation->rd_smgr, forknum))
			check_relation_fork(tupdesc, tupstore, relation, forknum);
	}

	relation_close(relation, AccessShareLock);	/* release the lock */
}

/*
 * For a given relation and fork, Do the real work of iterating over all pages
 * and doing the check.  Caller must hold an AccessShareLock lock on the given
 * relation.
 */
static void
check_relation_fork(TupleDesc tupdesc, Tuplestorestate *tupstore,
						  Relation relation, ForkNumber forknum)
{
	BlockNumber blkno,
				nblocks;

	/*
	 * We remember the number of blocks here.  Since caller must hold a lock on
	 * the relation, we know that it won't be truncated while we're iterating
	 * over the blocks.  Any block added after this function started won't be
	 * checked, but this is out of scope as such pages will be flushed before
	 * the next checkpoint's completion.
	 */
	nblocks = RelationGetNumberOfBlocksInFork(relation, forknum);

#define PG_CHECK_RELATION_COLS			5	/* Number of output arguments in the SRF */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		uint16		chk_expected,
					chk_found;
		Datum		values[PG_CHECK_RELATION_COLS];
		bool		nulls[PG_CHECK_RELATION_COLS];
		int			i = 0;

		/* Check the given buffer */
		if (check_one_block(relation, forknum, blkno, &chk_expected,
							&chk_found))
		{
			/* Buffer not corrupted or no worth checking, continue */
			continue;
		}

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(relation->rd_id);
		values[i++] = Int32GetDatum(forknum);
		values[i++] = UInt32GetDatum(blkno);
		/*
		 * This can happen if a corruption makes the block appears as
		 * PageIsNew() but isn't a new page.
		 */
		if (chk_expected == NoComputedChecksum)
			nulls[i++] = true;
		else
			values[i++] = UInt16GetDatum(chk_expected);
		values[i++] = UInt16GetDatum(chk_found);

		Assert(i == PG_CHECK_RELATION_COLS);

		/* Report the failure to the stat collector and the logs. */
		pgstat_report_checksum_failure();
		ereport(WARNING,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid page in block %u of relation %s",
						blkno,
						relpath(relation->rd_smgr->smgr_rnode, forknum))));

		/* Save the corrupted blocks in the tuplestore. */
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
}
