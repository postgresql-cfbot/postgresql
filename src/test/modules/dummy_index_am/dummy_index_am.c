/*-------------------------------------------------------------------------
 *
 * dummy_index_am.c
 *		Index AM template main file.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/dummy_index_am/dummy_index_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/options.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "utils/guc.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

typedef enum DummyAmEnum
{
	DUMMY_AM_ENUM_ONE,
	DUMMY_AM_ENUM_TWO
}			DummyAmEnum;

/* Dummy index options */
typedef struct DummyIndexOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			option_int;
	double		option_real;
	bool		option_bool;
	DummyAmEnum option_enum;
	int			option_string_val_offset;
	int			option_string_null_offset;
}			DummyIndexOptions;

static opt_enum_elt_def dummyAmEnumValues[] =
{
	{"one", DUMMY_AM_ENUM_ONE},
	{"two", DUMMY_AM_ENUM_TWO},
	{(const char *) NULL}		/* list terminator */
};

/* Handler for index AM */
PG_FUNCTION_INFO_V1(dihandler);

/*
 * Validation function for string relation options.
 */
static void
divalidate_string_option(const char *value)
{
	ereport(NOTICE,
			(errmsg("new option value for string parameter %s",
					value ? value : "NULL")));
}

static options_spec_set *di_relopt_specset = NULL;
void	   *digetreloptspecset(void);

void *
digetreloptspecset(void)
{
	if (di_relopt_specset)
		return di_relopt_specset;

	di_relopt_specset = allocateOptionsSpecSet(NULL,
											   sizeof(DummyIndexOptions), false, 6);

	optionsSpecSetAddInt(
						 di_relopt_specset, "option_int",
						 "Integer option for dummy_index_am",
						 AccessExclusiveLock,
						 offsetof(DummyIndexOptions, option_int), NULL,
						 10, -10, 100
		);


	optionsSpecSetAddReal(
						  di_relopt_specset, "option_real",
						  "Real option for dummy_index_am",
						  AccessExclusiveLock,
						  offsetof(DummyIndexOptions, option_real), NULL,
						  3.1415, -10, 100
		);

	optionsSpecSetAddBool(
						  di_relopt_specset, "option_bool",
						  "Boolean option for dummy_index_am",
						  AccessExclusiveLock,
						  offsetof(DummyIndexOptions, option_bool), NULL, true
		);

	optionsSpecSetAddEnum(di_relopt_specset, "option_enum",
						  "Enum option for dummy_index_am",
						  AccessExclusiveLock,
						  offsetof(DummyIndexOptions, option_enum), NULL,
						  dummyAmEnumValues,
						  DUMMY_AM_ENUM_ONE,
						  "Valid values are \"one\" and \"two\"."
		);

	optionsSpecSetAddString(di_relopt_specset, "option_string_val",
							"String option for dummy_index_am with non-NULL default",
							AccessExclusiveLock,
							offsetof(DummyIndexOptions, option_string_val_offset), NULL,
							"DefaultValue", &divalidate_string_option, NULL
		);

	/*
	 * String option for dummy_index_am with NULL default, and without
	 * description.
	 */

	optionsSpecSetAddString(di_relopt_specset, "option_string_null",
							NULL,	/* description */
							AccessExclusiveLock,
							offsetof(DummyIndexOptions, option_string_null_offset), NULL,
							NULL, &divalidate_string_option, NULL
		);

	return di_relopt_specset;
}



/*
 * Build a new index.
 */
static IndexBuildResult *
dibuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	/* let's pretend that no tuples were scanned */
	result->heap_tuples = 0;
	/* and no index tuples were created (that is true) */
	result->index_tuples = 0;

	return result;
}

/*
 * Build an empty index for the initialization fork.
 */
static void
dibuildempty(Relation index)
{
	/* No need to build an init fork for a dummy index */
}

/*
 * Insert new tuple to index AM.
 */
static bool
diinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 bool indexUnchanged,
		 IndexInfo *indexInfo)
{
	/* nothing to do */
	return false;
}

/*
 * Bulk deletion of all index entries pointing to a set of table tuples.
 */
static IndexBulkDeleteResult *
dibulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	/*
	 * There is nothing to delete.  Return NULL as there is nothing to pass to
	 * amvacuumcleanup.
	 */
	return NULL;
}

/*
 * Post-VACUUM cleanup for index AM.
 */
static IndexBulkDeleteResult *
divacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	/* Index has not been modified, so returning NULL is fine */
	return NULL;
}

/*
 * Estimate cost of index AM.
 */
static void
dicostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
{
	/* Tell planner to never use this index! */
	*indexStartupCost = 1.0e10;
	*indexTotalCost = 1.0e10;

	/* Do not care about the rest */
	*indexSelectivity = 1;
	*indexCorrelation = 0;
	*indexPages = 1;
}

/*
 * Validator for index AM.
 */
static bool
divalidate(Oid opclassoid)
{
	/* Index is dummy so we are happy with any opclass */
	return true;
}

/*
 * Begin scan of index AM.
 */
static IndexScanDesc
dibeginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan;

	/* Let's pretend we are doing something */
	scan = RelationGetIndexScan(r, nkeys, norderbys);
	return scan;
}

/*
 * Rescan of index AM.
 */
static void
direscan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{
	/* nothing to do */
}

/*
 * End scan of index AM.
 */
static void
diendscan(IndexScanDesc scan)
{
	/* nothing to do */
}

/*
 * Index AM handler function: returns IndexAmRoutine with access method
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
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = dibuild;
	amroutine->ambuildempty = dibuildempty;
	amroutine->aminsert = diinsert;
	amroutine->ambulkdelete = dibulkdelete;
	amroutine->amvacuumcleanup = divacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = dicostestimate;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
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
	amroutine->amreloptspecset = digetreloptspecset;

	PG_RETURN_POINTER(amroutine);
}
