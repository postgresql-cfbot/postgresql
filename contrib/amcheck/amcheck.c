/*-------------------------------------------------------------------------
 *
 * amcheck.c
 *		Utility functions common to all access methods.
 *
 * Copyright (c) 2017-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/amcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "amcheck.h"
#include "catalog/index.h"
#include "commands/tablecmds.h"
#include "utils/guc.h"

static bool
amcheck_index_mainfork_expected(Relation rel);

/*
 * Check if index relation should have a file for its main relation
 * fork.  Verification uses this to skip unlogged indexes when in hot standby
 * mode, where there is simply nothing to verify.
 *
 * NB: Caller should call index_checkable()
 * before calling here.
 */
static bool
amcheck_index_mainfork_expected(Relation rel)
{
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED ||
		!RecoveryInProgress())
		return true;

	ereport(NOTICE,
			(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
			 errmsg("cannot verify unlogged index \"%s\" during recovery, skipping",
					RelationGetRelationName(rel))));

	return false;
}

void
amcheck_lock_relation_and_check(Oid indrelid, IndexCheckableCallback checkable,
												IndexDoCheckCallback check, LOCKMODE lockmode, void *state)
{
	Oid			heapid;
	Relation	indrel;
	Relation	heaprel;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	/*
	 * We must lock table before index to avoid deadlocks.  However, if the
	 * passed indrelid isn't an index then IndexGetRelation() will fail.
	 * Rather than emitting a not-very-helpful error message, postpone
	 * complaining, expecting that the is-it-an-index test below will fail.
	 *
	 * In hot standby mode this will raise an error when parentcheck is true.
	 */
	heapid = IndexGetRelation(indrelid, true);
	if (OidIsValid(heapid))
	{
		heaprel = table_open(heapid, lockmode);

		/*
		 * Switch to the table owner's userid, so that any index functions are
		 * run as that user.  Also lock down security-restricted operations
		 * and arrange to make GUC variable changes local to this command.
		 */
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(heaprel->rd_rel->relowner,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}
	else
	{
		heaprel = NULL;
		/* Set these just to suppress "uninitialized variable" warnings */
		save_userid = InvalidOid;
		save_sec_context = -1;
		save_nestlevel = -1;
	}

	/*
	 * Open the target index relations separately (like relation_openrv(), but
	 * with heap relation locked first to prevent deadlocking).  In hot
	 * standby mode this will raise an error when parentcheck is true.
	 *
	 * There is no need for the usual indcheckxmin usability horizon test
	 * here, even in the heapallindexed case, because index undergoing
	 * verification only needs to have entries for a new transaction snapshot.
	 * (If this is a parentcheck verification, there is no question about
	 * committed or recently dead heap tuples lacking index entries due to
	 * concurrent activity.)
	 */
	indrel = index_open(indrelid, lockmode);

	/*
	 * Since we did the IndexGetRelation call above without any lock, it's
	 * barely possible that a race against an index drop/recreation could have
	 * netted us the wrong table.
	 */
	if (heaprel == NULL || heapid != IndexGetRelation(indrelid, false))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("could not open parent table of index \"%s\"",
						RelationGetRelationName(indrel))));

	/* Relation suitable for checking */
	checkable(indrel);

	if (amcheck_index_mainfork_expected(indrel))
		check(indrel, heaprel, state);

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/*
	 * Release locks early. That's ok here because nothing in the called
	 * routines will trigger shared cache invalidations to be sent, so we can
	 * relax the usual pattern of only releasing locks after commit.
	 */
	index_close(indrel, lockmode);
	if (heaprel)
		table_close(heaprel, lockmode);
}

/*
 * PageGetItemId() wrapper that validates returned line pointer.
 *
 * Buffer page/page item access macros generally trust that line pointers are
 * not corrupt, which might cause problems for verification itself.  For
 * example, there is no bounds checking in PageGetItem().  Passing it a
 * corrupt line pointer can cause it to return a tuple/pointer that is unsafe
 * to dereference.
 *
 * Validating line pointers before tuples avoids undefined behavior and
 * assertion failures with corrupt indexes, making the verification process
 * more robust and predictable.
 */
ItemId
PageGetItemIdCareful(Relation rel, BlockNumber block, Page page,
					 OffsetNumber offset, size_t opaquesize)
{
	ItemId		itemid = PageGetItemId(page, offset);

	Assert(opaquesize == MAXALIGN(opaquesize));

	if (ItemIdGetOffset(itemid) + ItemIdGetLength(itemid) >
		BLCKSZ - MAXALIGN(opaquesize))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("line pointer points past end of tuple space in index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_internal("Index tid=(%u,%u) lp_off=%u, lp_len=%u lp_flags=%u.",
									block, offset, ItemIdGetOffset(itemid),
									ItemIdGetLength(itemid),
									ItemIdGetFlags(itemid))));

	/*
	 * Verify that line pointer isn't LP_REDIRECT or LP_UNUSED, since nbtree and gist
	 * never uses either.  Verify that line pointer has storage, too, since
	 * even LP_DEAD items should.
	 */
	if (ItemIdIsRedirected(itemid) || !ItemIdIsUsed(itemid) ||
		ItemIdGetLength(itemid) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid line pointer storage in index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail_internal("Index tid=(%u,%u) lp_off=%u, lp_len=%u lp_flags=%u.",
									block, offset, ItemIdGetOffset(itemid),
									ItemIdGetLength(itemid),
									ItemIdGetFlags(itemid))));

	return itemid;
}
