/*-------------------------------------------------------------------------
 *
 * dummy_index.h
 *	  Header for dummy_index_am index.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	 src/test/modules/dummy_index_am/dummy_index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _DUMMY_INDEX_H_
#define _DUMMY_INDEX_H_

#include "nodes/pathnodes.h"

/* Dummy index options */
typedef struct DummyIndexOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			int_option;		/* for reloptions test purposes */
	double		real_option;	/* for reloptions test purposes */
	bool		bool_option;	/* for reloptions test purposes */
	int			string_option_offset;	/* for reloptions test purposes */
	int			string_option2_offset;	/* for reloptions test purposes */
}	DummyIndexOptions;

extern void _PG_init(void);

/* index access method interface functions */
extern bool divalidate(Oid opclassoid);
extern bool diinsert(Relation index, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 struct IndexInfo *indexInfo);
extern IndexScanDesc dibeginscan(Relation r, int nkeys, int norderbys);
extern void direscan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys);
extern void diendscan(IndexScanDesc scan);
extern IndexBuildResult *dibuild(Relation heap, Relation index,
		struct IndexInfo *indexInfo);
extern void dibuildempty(Relation index);
extern IndexBulkDeleteResult *dibulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
			 void *callback_state);
extern IndexBulkDeleteResult *divacuumcleanup(IndexVacuumInfo *info,
				IndexBulkDeleteResult *stats);
extern bytea *dioptions(Datum reloptions, bool validate);
extern void dicostestimate(PlannerInfo *root, IndexPath *path,
			   double loop_count, Cost *indexStartupCost,
			   Cost *indexTotalCost, Selectivity *indexSelectivity,
			   double *indexCorrelation, double *indexPages);

/* direoptions.c */
/* Functions and variables needed for reloptions tests*/

extern relopt_parse_elt di_relopt_tab[5];
extern relopt_kind di_relopt_kind;

extern void create_reloptions_table(void);
extern void create_reloptions_test_GUC(void);
extern void print_reloptions_test_output(Relation index);
extern void validate_string_option(const char *value);

#endif
