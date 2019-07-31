/*-------------------------------------------------------------------------
 *
 * blcost.c
 *		Cost estimate function for bloom indexes.
 *
 * Copyright (c) 2016-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/bloom/blcost.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#include "bloom.h"

void
genericcostestimate2(PlannerInfo *root,
					IndexPath *path,
					double loop_count,
					GenericCosts *costs)
{
	IndexOptInfo *index = path->indexinfo;
	List	   *indexQuals = get_quals_from_indexclauses(path->indexclauses);
	List	   *indexOrderBys = path->indexorderbys;
	Cost		indexStartupCost;
	Cost		indexTotalCost;
	Selectivity indexSelectivity;
	double		indexCorrelation;
	double		numIndexPages;
	double		numIndexTuples;
	double		spc_seq_page_cost;
	double		num_sa_scans;
	double		num_outer_scans;
	double		num_scans;
	double		qual_op_cost;
	double		qual_arg_cost;
	List	   *selectivityQuals;
	ListCell   *l;

	/*
	 * If the index is partial, AND the index predicate with the explicitly
	 * given indexquals to produce a more accurate idea of the index
	 * selectivity.
	 */
	selectivityQuals = add_predicate_to_index_quals(index, indexQuals);

	/*
	 * Check for ScalarArrayOpExpr index quals, and estimate the number of
	 * index scans that will be performed.
	 */
	num_sa_scans = 1;
	foreach(l, indexQuals)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		if (IsA(rinfo->clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
			int			alength = estimate_array_length(lsecond(saop->args));

			if (alength > 1)
				num_sa_scans *= alength;
		}
	}

	/* Estimate the fraction of main-table tuples that will be visited */
	indexSelectivity = clauselist_selectivity(root, selectivityQuals,
											  index->rel->relid,
											  JOIN_INNER,
											  NULL);

	/* Bloom always reads all tuples */
	numIndexTuples = costs->numIndexTuples;

	/*
	 * Always estimate at least one tuple is touched
	 */
	if (numIndexTuples < 1.0)
		numIndexTuples = 1.0;

	/* Bloom always reads all pages */
	numIndexPages = index->pages;

	/* fetch estimated page cost for tablespace containing index */
	get_tablespace_page_costs(index->reltablespace,
							  NULL,
							  &spc_seq_page_cost);

	/*
	 * Now compute the disk access costs.
	 *
	 * The above calculations are all per-index-scan.  However, if we are in a
	 * nestloop inner scan, we can expect the scan to be repeated (with
	 * different search keys) for each row of the outer relation.  Likewise,
	 * ScalarArrayOpExpr quals result in multiple index scans.  This creates
	 * the potential for cache effects to reduce the number of disk page
	 * fetches needed.  We want to estimate the average per-scan I/O cost in
	 * the presence of caching.
	 *
	 * We use the Mackert-Lohman formula (see costsize.c for details) to
	 * estimate the total number of page fetches that occur.  While this
	 * wasn't what it was designed for, it seems a reasonable model anyway.
	 * Note that we are counting pages not tuples anymore, so we take N = T =
	 * index size, as if there were one "tuple" per page.
	 */
	num_outer_scans = loop_count;
	num_scans = num_sa_scans * num_outer_scans;

	if (num_scans > 1)
	{
		double		pages_fetched;

		/* total page fetches ignoring cache effects */
		pages_fetched = numIndexPages * num_scans;

		/* use Mackert and Lohman formula to adjust for cache effects */
		pages_fetched = index_pages_fetched(pages_fetched,
											index->pages,
											(double) index->pages,
											root);

		/*
		 * Now compute the total disk access cost, and then report a pro-rated
		 * share for each outer scan.  (Don't pro-rate for ScalarArrayOpExpr,
		 * since that's internal to the indexscan.)
		 */
		indexTotalCost = (pages_fetched * spc_seq_page_cost)
			/ num_outer_scans;
	}
	else
	{
		/*
		 * For a single index scan, we just charge spc_seq_page_cost per
		 * page touched.
		 */
		indexTotalCost = numIndexPages * spc_seq_page_cost;
	}

	/*
	 * CPU cost: any complex expressions in the indexquals will need to be
	 * evaluated once at the start of the scan to reduce them to runtime keys
	 * to pass to the index AM (see nodeIndexscan.c).  We model the per-tuple
	 * CPU costs as cpu_index_tuple_cost.  No cpu_operator_cost is added
	 * as Bloom indexes do not use ADT operators.  Because we have numIndexTuples as a per-scan
	 * number, we have to multiply by num_sa_scans to get the correct result
	 * for ScalarArrayOpExpr cases.
	 *
	 * Note: this neglects the possible costs of rechecking lossy operators.
	 * Detecting that that might be needed seems more expensive than it's
	 * worth, though, considering all the other inaccuracies here ...
	 */
	qual_arg_cost = index_other_operands_eval_cost(root, indexQuals) +
		index_other_operands_eval_cost(root, indexOrderBys);
	qual_op_cost = cpu_operator_cost *
		(list_length(indexQuals) + list_length(indexOrderBys));

	indexStartupCost = qual_arg_cost;
	indexTotalCost += qual_arg_cost;
	indexTotalCost += numIndexTuples * num_sa_scans * (cpu_index_tuple_cost);

	/*
	 * Generic assumption about index correlation: there isn't any.
	 */
	indexCorrelation = 0.0;

	/*
	 * Return everything to caller.
	 */
	costs->indexStartupCost = indexStartupCost;
	costs->indexTotalCost = indexTotalCost;
	costs->indexSelectivity = indexSelectivity;
	costs->indexCorrelation = indexCorrelation;
	costs->numIndexPages = numIndexPages;
	costs->numIndexTuples = numIndexTuples;
	costs->num_sa_scans = num_sa_scans;
}

/*
 * Estimate cost of bloom index scan.
 */
void
blcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
			   Cost *indexStartupCost, Cost *indexTotalCost,
			   Selectivity *indexSelectivity, double *indexCorrelation,
			   double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs;

	MemSet(&costs, 0, sizeof(costs));

	/* We have to visit all index tuples anyway */
	costs.numIndexTuples = index->tuples;

	/* Use generic estimate */
	genericcostestimate2(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}
