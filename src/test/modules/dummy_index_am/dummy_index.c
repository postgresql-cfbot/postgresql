/*-------------------------------------------------------------------------
 *
 * dummy_index.c
 *		Dummy index AM main file.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/dummy_index_am/dummy_index.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "catalog/index.h"
#include "access/reloptions.h"
#include "utils/guc.h"
#include "dummy_index.h"


PG_FUNCTION_INFO_V1(dihandler);
PG_MODULE_MAGIC;

void
_PG_init(void)

{
	create_reloptions_table();
	create_reloptions_test_GUC();
}

/*
 * Dummy index handler function: return IndexAmRoutine with access method
 * parameters and callbacks.
 */
Datum
dihandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 1;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = dibuild;
	amroutine->ambuildempty = dibuildempty;
	amroutine->aminsert = diinsert;
	amroutine->ambulkdelete = dibulkdelete;
	amroutine->amvacuumcleanup = divacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = dicostestimate;
	amroutine->amoptions = dioptions;
	amroutine->amproperty = NULL;
	amroutine->amvalidate = divalidate;
	amroutine->ambeginscan = dibeginscan;
	amroutine->amrescan = direscan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = diendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

IndexBuildResult *
dibuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	/* let's pretend that no tuples were scanned */
	result->heap_tuples = 0;
	/* and no index tuples were created (that is true) */
	result->index_tuples = 0;

	print_reloptions_test_output(index);

	return result;
}

void
dibuildempty(Relation index)
{
	/* Let's see what will happen if nothing is done here */
	/* Add tests for reloptions here */
}

bool
diinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 IndexInfo *indexInfo)
{
	/* This is Dummy Index we do nothing on insert :-) */
	print_reloptions_test_output(index);
	return false;

}

IndexBulkDeleteResult *
dibulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	/*
	 * Do not delete anything; Return NULL as we have nothing to pass to
	 * amvacuumcleanup
	 */
	return NULL;
}

IndexBulkDeleteResult *
divacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	/* Index have not been modified, so it is absolutely right to return NULL */
	return NULL;
}

void
dicostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
{
	/* Tell planner to never use this index! */
	*indexStartupCost = 1.0e10; /* AKA disable_cost */
	*indexTotalCost = 1.0e10;	/* AKA disable_cost */

	/* Do not care about the rest */
	*indexSelectivity = 1;
	*indexCorrelation = 0;
	*indexPages = 1;
}

bytea *
dioptions(Datum reloptions, bool validate)
{
	relopt_value *options;
	int			numoptions;
	DummyIndexOptions *rdopts;

	/* Parse the user-given reloptions */
	options = parseRelOptions(reloptions, validate, di_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(DummyIndexOptions), options, numoptions);
	fillRelOptions((void *) rdopts, sizeof(DummyIndexOptions), options, numoptions,
				   validate, di_relopt_tab, lengthof(di_relopt_tab));

	return (bytea *) rdopts;
}

bool
divalidate(Oid opclassoid)
{
	/* Index does not really work so we are happy with any opclass */
	return true;
}

IndexScanDesc
dibeginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan;

	/* Let's pretend we are doing something, just in case */
	scan = RelationGetIndexScan(r, nkeys, norderbys);
	return scan;
}

void
direscan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{
	/* Just do nothing */
}

void
diendscan(IndexScanDesc scan)
{
	/* Do nothing */
}
