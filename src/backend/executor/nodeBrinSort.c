/*-------------------------------------------------------------------------
 *
 * nodeBrinSort.c
 *	  Routines to support sorted scan of relations using a BRIN index
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * The overall algorithm is roughly this:
 *
 * 0) initialize a tuplestore and a tuplesort
 *
 * 1) fetch list of page ranges from the BRIN index, sorted by minval
 *    (with the not-summarized ranges first, and all-null ranges last)
 *
 * 2) for NULLS FIRST ordering, walk all ranges that may contain NULL
 *    values and output them (and return to the beginning of the list)
 *
 * 3) while there are ranges in the list, do this:
 *
 *   a) get next (distinct) minval from the list, call it watermark
 *
 *   b) if there are any tuples in the tuplestore, move them to tuplesort
 *
 *   c) process all ranges with (minval < watermark) - read tuples and feed
 *      them either into tuplestore (when value < watermark) or tuplestore
 *
 *   d) sort the tuplestore, output all the tuples
 *
 * 4) if some tuples remain in the tuplestore, sort and output them
 *
 * 5) for NULLS LAST ordering, walk all ranges that may contain NULL
 *    values and output them (and return to the beginning of the list)
 *
 *
 * For DESC orderings the process is almost the same, except that we look
 * at maxval and use '>' operator (but that's transparent).
 *
 * There's a couple possible things that might be done in different ways:
 *
 * 1) Not using tuplestore, and feeding tuples only to a tuplesort. Then
 * while producing the tuples, we'd only output tuples up to the current
 * watermark, and then we'd keep the remaining tuples for the next round.
 * Either we'd need to transfer them into a second tuplesort, or allow
 * "reopening" the tuplesort and adding more tuples. And then only the
 * part since the watermark would get sorted (possibly using a merge-sort
 * with the already sorted part).
 *
 *
 * 2) The other question is what to do with NULL values - at the moment we
 * just read the ranges, output the NULL tuples and that's it - we're not
 * retaining any non-NULL tuples, so that we'll read the ranges again in
 * the second range. The logic here is that either there are very few
 * such ranges, so it's won't cost much to just re-read them. Or maybe
 * there are very many such ranges, and we'd do a lot of spilling to the
 * tuplestore, and it's not much more expensive to just re-read the source
 * data. There are counter-examples, though - e.g., there might be many
 * has_nulls ranges, but with very few non-NULL tuples. In this case it
 * might be better to actually spill the tuples instead of re-reading all
 * the ranges. Maybe this is something we can do at run-time, or maybe we
 * could estimate this at planning time. We do know the null_frac for the
 * column, so we know the number of NULL rows. And we also know the number
 * of all_nulls and has_nulls ranges. We can estimate the number of rows
 * per range, and we can estimate how many non-NULL rows are in the
 * has_nulls ranges (we don't need to re-read all-nulls ranges). There's
 * also the filter, which may reduce the amount of rows to store.
 *
 * So we'd need to compare two metrics calculated roughly like this:
 *
 *   cost(re-reading has-nulls ranges)
 *      = cost(random_page_cost * n_has_nulls + seq_page_cost * pages_per_range)
 *
 *   cost(spilling non-NULL rows from has-nulls ranges)
 *      = cost(numrows * width / BLCKSZ * seq_page_cost * 2)
 *
 * where numrows is the number of non-NULL rows in has_null ranges, which
 * can be calculated like this:
 *
 *   // estimated number of rows in has-null ranges
 *   rows_in_has_nulls = (reltuples / relpages) * pages_per_range * n_has_nulls
 *
 *   // number of NULL rows in the has-nulls ranges
 *   nulls_in_ranges = reltuples * null_frac - n_all_nulls * (reltuples / relpages)
 *
 *   // numrows is the difference, multiplied by selectivity of the index
 *   // filter condition (value between 0.0 and 1.0)
 *   numrows = (rows_in_has_nulls - nulls_in_ranges) * selectivity
 *
 * This ignores non-summarized ranges, but there should be only very few of
 * those, so it should not make a huge difference. Otherwise we can divide
 * them between regular, has-nulls and all-nulls pages to keep the ratio.
 *
 *
 * 3) How large step to make when updating the watermark?
 *
 * When updating the watermark, one option is to simply proceed to the next
 * distinct minval value, which is the smallest possible step we can make.
 * This may be both fine and very inefficient, depending on how many rows
 * end up in the tuplesort and how many rows we end up spilling (possibly
 * repeatedly to the tuplestore).
 *
 * When having to sort large number of rows, it's inefficient to run many
 * tiny sorts, even if it produces correct result. For example when sorting
 * 1M rows, we may split this as either (a) 100000x sorts of 10 rows, or
 * (b) 1000 sorts of 1000 rows. The (b) option is almost certainly more
 * efficient. Maybe sorts of 10k rows would be even better, if it fits
 * into work_mem.
 *
 * This gets back to how large the page ranges are, and if/how much they
 * overlap. With tiny ranges (e.g. a single-page ranges), a single range
 * can only add as many rows as we can fit on a single page. So we need
 * more ranges by default - how many watermark steps that is depends on
 * how many distinct minval values are there ...
 *
 * Then there's overlaps - if ranges do not overlap, we're done and we'll
 * add the whole range because the next watermark is above maxval. But
 * when the ranges overlap, we'll only add the first part (assuming the
 * minval of the next range is the watermark). Assume 10 overlapping
 * ranges - imagine for example ranges shifted by 10%, so something like
 *
 *   [0,100] [10,110], [20,120], [30, 130], ..., [90, 190]
 *
 * In the first step we use watermark=10 and load the first range, with
 * maybe 1000 rows in total. But assuming uniform distribution, only about
 * 100 rows will go into the tuplesort, the remaining 900 rows will go into
 * the tuplestore (assuming uniform distribution). Then in the second step
 * we sort another 100 rows and the remaining 800 rows will be moved into
 * a new tuplestore. And so on and so on.
 *
 * This means that incrementing the watermarks by single steps may be
 * quite inefficient, and we need to reflect both the range size and
 * how much the ranges overlap.
 *
 * In fact, maybe we should not determine the step as number of minval
 * values to skip, but how many ranges would that mean reading. Because
 * if we have a minval with many duplicates, that may load many rows.
 * Or even better, we could look at how many rows would that mean loading
 * into the tuplestore - if we track P(x<minval) for each range (e.g. by
 * calculating average value during ANALYZE, or perhaps by estimating
 * it from per-column stats), then we know the increment is going to be
 * about
 *
 *     P(x < minval[i]) - P(x < minval[i-1])
 *
 * and we can stop once we'd exceed work_mem (with some slack). See comment
 * for brin_minmax_stats() for more thoughts.
 *
 *
 * 4) LIMIT/OFFSET vs. full sort
 *
 * There's one case where very small sorts may be actually optimal, and
 * that's queries that need to process only very few rows - say, LIMIT
 * queries with very small bound.
 *
 *
 * FIXME Projection does not work (fails on projection slot expecting
 * buffer ops, but we're sending it minimal tuple slot).
 *
 * FIXME The tlists are not wired quite correctly - the sortColIdx is an
 * index to the tlist, but we need attnum from the heap table, so that we
 * can fetch the attribute etc. Or maybe fetching the value from the raw
 * tuple (before projection) is wrong and needs to be done differently.
 *
 * FIXME Indexes on expressions don't work (possibly related to the tlist
 * being done incorrectly).
 *
 * FIXME handling of other brin opclasses (minmax-multi)
 *
 * FIXME improve costing
 *
 *
 * Improvement ideas:
 *
 * 1) multiple tuplestores for overlapping ranges
 *
 * When there are many overlapping ranges (so that maxval > current.maxval),
 * we're loading all the "future" tuples into a new tuplestore. However, if
 * there are multiple such ranges (imagine ranges "shifting" by 10%, which
 * gives us 9 more ranges), we know in the next round we'll only need rows
 * until the next maxval. We'll not sort these rows, but we'll still shuffle
 * them around until we get to the proper range (so about 10x each row).
 * Maybe we should pre-allocate the tuplestores (or maybe even tuplesorts)
 * for future ranges, and route the tuples to the correct one? Maybe we
 * could be a bit smarter and discard tuples once we have enough rows for
 * the preceding ranges (say, with LIMIT queries). We'd also need to worry
 * about work_mem, though - we can't just use many tuplestores, each with
 * whole work_mem. So we'd probably use e.g. work_mem/2 for the next one,
 * and then /4, /8 etc. for the following ones. That's work_mem in total.
 * And there'd need to be some limit on number of tuplestores, I guess.
 *
 * 2) handling NULL values
 *
 * We need to handle NULLS FIRST / NULLS LAST cases. The question is how
 * to do that - the easiest way is to simply do a separate scan of ranges
 * that might contain NULL values, processing just rows with NULLs, and
 * discarding other rows. And then process non-NULL values as currently.
 * The NULL scan would happen before/after this regular phase.
 *
 * Byt maybe we could be smarter, and not do separate scans. When reading
 * a page, we might stash the tuple in a tuplestore, so that we can read
 * it the next round. Obviously, this might be expensive if we need to
 * keep too many rows, so the tuplestore would grow too large - in that
 * case it might be better to just do the two scans.
 *
 * 3) parallelism
 *
 * Presumably we could do a parallel version of this. The leader or first
 * worker would prepare the range information, and the workers would then
 * grab ranges (in a kinda round robin manner), sort them independently,
 * and then the results would be merged by Gather Merge.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBrinSort.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBrinSort			scans a relation using an index
 *		IndexNext				retrieve next tuple using index
 *		ExecInitBrinSort		creates and initializes state info.
 *		ExecReScanBrinSort		rescans the indexed relation.
 *		ExecEndBrinSort			releases all storage.
 *		ExecBrinSortMarkPos		marks scan position.
 *		ExecBrinSortRestrPos	restores scan position.
 *		ExecBrinSortEstimate	estimates DSM space needed for parallel index scan
 *		ExecBrinSortInitializeDSM initialize DSM for parallel BrinSort
 *		ExecBrinSortReInitializeDSM reinitialize DSM for fresh scan
 *		ExecBrinSortInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/brin.h"
#include "access/brin_internal.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "executor/execdebug.h"
#include "executor/nodeBrinSort.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"


static TupleTableSlot *IndexNext(BrinSortState *node);
static bool IndexRecheck(BrinSortState *node, TupleTableSlot *slot);
static void ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate);

#define BRINSORT_DEBUG

/* do various consistency checks */
static void
AssertCheckRanges(BrinSortState *node)
{
#ifdef USE_ASSERT_CHECKING

#endif
}

/*
 * brinsort_start_tidscan
 *		Start scanning tuples from a given page range.
 *
 * We open a TID range scan for the given range, and initialize the tuplesort.
 * Optionally, we update the watermark (with either high/low value). We only
 * need to do this for the main page range, not for the intersecting ranges.
 *
 * XXX Maybe we should initialize the tidscan only once, and then do rescan
 * for the following ranges? And similarly for the tuplesort?
 */
static void
brinsort_start_tidscan(BrinSortState *node)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	BrinRange  *range = node->bs_range;

	/* There must not be any TID scan in progress yet. */
	Assert(node->ss.ss_currentScanDesc == NULL);

	/* Initialize the TID range scan, for the provided block range. */
	if (node->ss.ss_currentScanDesc == NULL)
	{
		TableScanDesc		tscandesc;
		ItemPointerData		mintid,
							maxtid;

		ItemPointerSetBlockNumber(&mintid, range->blkno_start);
		ItemPointerSetOffsetNumber(&mintid, 0);

		ItemPointerSetBlockNumber(&maxtid, range->blkno_end);
		ItemPointerSetOffsetNumber(&maxtid, MaxHeapTuplesPerPage);

		elog(DEBUG1, "loading range blocks [%u, %u]",
			 range->blkno_start, range->blkno_end);

		tscandesc = table_beginscan_tidrange(node->ss.ss_currentRelation,
											 estate->es_snapshot,
											 &mintid, &maxtid);
		node->ss.ss_currentScanDesc = tscandesc;
	}

	if (node->bs_tuplesortstate == NULL)
	{
		TupleDesc	tupDesc = RelationGetDescr(node->ss.ss_currentRelation);

		node->bs_tuplesortstate = tuplesort_begin_heap(tupDesc,
													plan->numCols,
													plan->sortColIdx,
													plan->sortOperators,
													plan->collations,
													plan->nullsFirst,
													work_mem,
													NULL,
													TUPLESORT_NONE);
	}

	if (node->bs_tuplestore == NULL)
	{
		node->bs_tuplestore = tuplestore_begin_heap(false, false, work_mem);
	}
}

/*
 * brinsort_end_tidscan
 *		Finish the TID range scan.
 */
static void
brinsort_end_tidscan(BrinSortState *node)
{
	/* get the first range, read all tuples using a tid range scan */
	if (node->ss.ss_currentScanDesc != NULL)
	{
		table_endscan(node->ss.ss_currentScanDesc);
		node->ss.ss_currentScanDesc = NULL;
	}
}

/*
 * brinsort_update_watermark
 *		Advance the watermark to the next minval (or maxval for DESC).
 *
 * We could could actually advance the watermark by multiple steps (not to
 * the immediately following minval, but a couple more), to accumulate more
 * rows in the tuplesort. The number of steps we make correlates with the
 * amount of data we sort in a given step, but we don't know in advance
 * how many rows (or bytes) will that actually be. We could do some simple
 * heuristics (measure past sorts and extrapolate).
 */
static void
brinsort_update_watermark(BrinSortState *node, bool asc)
{
	int		cmp;
	bool	found = false;

	tuplesort_markpos(node->bs_scan->ranges);

	while (tuplesort_gettupleslot(node->bs_scan->ranges, true, false, node->bs_scan->slot, NULL))
	{
		bool	isnull;
		Datum	value;
		bool	all_nulls;
		bool	not_summarized;

		all_nulls = DatumGetBool(slot_getattr(node->bs_scan->slot, 4, &isnull));
		Assert(!isnull);

		not_summarized = DatumGetBool(slot_getattr(node->bs_scan->slot, 5, &isnull));
		Assert(!isnull);

		/* we ignore ranges that are either all_nulls or not summarized */
		if (all_nulls || not_summarized)
			continue;

		/* use either minval or maxval, depending on the ASC / DESC */
		if (asc)
			value = slot_getattr(node->bs_scan->slot, 6, &isnull);
		else
			value = slot_getattr(node->bs_scan->slot, 7, &isnull);

		if (!node->bs_watermark_set)
		{
			node->bs_watermark_set = true;
			node->bs_watermark = value;
			continue;
		}

		cmp = ApplySortComparator(node->bs_watermark, false, value, false,
								  &node->bs_sortsupport);

		if (cmp < 0)
		{
			node->bs_watermark_set = true;
			node->bs_watermark = value;
			found = true;
			break;
		}
	}

	tuplesort_restorepos(node->bs_scan->ranges);

	node->bs_watermark_set = found;
}

/*
 * brinsort_load_tuples
 *		Load tuples from the TID range scan, add them to tuplesort/store.
 *
 * When called for the "current" range, we don't need to check the watermark,
 * we know the tuple goes into the tuplesort. So with check_watermark we
 * skip the comparator call to save CPU cost.
 */
static void
brinsort_load_tuples(BrinSortState *node, bool check_watermark, bool null_processing)
{
	BrinSort	   *plan = (BrinSort *) node->ss.ps.plan;
	TableScanDesc	scan;
	EState		   *estate;
	ScanDirection	direction;
	TupleTableSlot *slot;
	BrinRange	   *range = node->bs_range;

	estate = node->ss.ps.state;
	direction = estate->es_direction;

	slot = node->ss.ss_ScanTupleSlot;

	Assert(node->bs_range != NULL);

	/*
	 * If we're not processign NULLS, and this is all-nulls range, we can
	 * just skip it - we won't find any non-NULL tuples in it.
	 *
	 * XXX Shouldn't happen, thanks to logic in brinsort_next_range().
	 */
	if (!null_processing && range->all_nulls)
		return;

	/*
	 * Similarly, if we're processing NULLs and this range does not have
	 * has_nulls flag, we can skip it.
	 *
	 * XXX Shouldn't happen, thanks to logic in brinsort_next_range().
	 */
	if (null_processing && !(range->has_nulls || range->not_summarized || range->all_nulls))
		return;

	brinsort_start_tidscan(node);

	scan = node->ss.ss_currentScanDesc;

	/*
	 * Read tuples, evaluate the filter (so that we don't keep tuples only to
	 * discard them later), and decide if it goes into the current range
	 * (tuplesort) or overflow (tuplestore).
	 */
	while (table_scan_getnextslot_tidrange(scan, direction, slot))
	{
		ExprContext *econtext;
		ExprState  *qual;

		/*
		 * Fetch data from node
		 */
		qual = node->bs_qual;
		econtext = node->ss.ps.ps_ExprContext;

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 *
		 * XXX Done here, because in ExecScan we'll get different slot type
		 * (minimal tuple vs. buffered tuple). Scan expects slot while reading
		 * from the table (like here), but we're stashing it into a tuplesort.
		 *
		 * XXX Maybe we could eliminate many tuples by leveraging the BRIN
		 * range, by executing the consistent function. But we don't have
		 * the qual in appropriate format at the moment, so we'd preprocess
		 * the keys similarly to bringetbitmap(). In which case we should
		 * probably evaluate the stuff while building the ranges? Although,
		 * if the "consistent" function is expensive, it might be cheaper
		 * to do that incrementally, as we need the ranges. Would be a win
		 * for LIMIT queries, for example.
		 *
		 * XXX However, maybe we could also leverage other bitmap indexes,
		 * particularly for BRIN indexes because that makes it simpler to
		 * eliminate the ranges incrementally - we know which ranges to
		 * load from the index, while for other indexes (e.g. btree) we
		 * have to read the whole index and build a bitmap in order to have
		 * a bitmap for any range. Although, if the condition is very
		 * selective, we may need to read only a small fraction of the
		 * index, so maybe that's OK.
		 */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			int		cmp = 0;	/* matters for check_watermark=false */
			Datum	value;
			bool	isnull;

			value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

			/*
			 * FIXME Not handling NULLS for now, we need to stash them into
			 * a separate tuplestore (so that we can output them first or
			 * last), and then skip them in the regular processing?
			 */
			if (null_processing)
			{
				/* Stash it to the tuplestore (when NULL, or ignore
				 * it (when not-NULL). */
				if (isnull)
					tuplestore_puttupleslot(node->bs_tuplestore, slot);

				/* NULL or not, we're done */
				continue;
			}

			/* we're not processing NULL values, so ignore NULLs */
			if (isnull)
				continue;

			/*
			 * Otherwise compare to watermark, and stash it either to the
			 * tuplesort or tuplestore.
			 */
			if (check_watermark && node->bs_watermark_set)
				cmp = ApplySortComparator(value, false,
										  node->bs_watermark, false,
										  &node->bs_sortsupport);

			if (cmp <= 0)
				tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
			else
				tuplestore_puttupleslot(node->bs_tuplestore, slot);
		}

		ExecClearTuple(slot);
	}

	ExecClearTuple(slot);

	brinsort_end_tidscan(node);
}

/*
 * brinsort_load_spill_tuples
 *		Load tuples from the spill tuplestore, and either stash them into
 *		a tuplesort or a new tuplestore.
 *
 * After processing the last range, we want to process all remaining ranges,
 * so with check_watermark=false we skip the check.
 */
static void
brinsort_load_spill_tuples(BrinSortState *node, bool check_watermark)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	Tuplestorestate *tupstore;
	TupleTableSlot *slot;

	if (node->bs_tuplestore == NULL)
		return;

	/* start scanning the existing tuplestore (XXX needed?) */
	tuplestore_rescan(node->bs_tuplestore);

	/*
	 * Create a new tuplestore, for tuples that exceed the watermark and so
	 * should not be included in the current sort.
	 */
	tupstore = tuplestore_begin_heap(false, false, work_mem);

	/*
	 * We need a slot for minimal tuples. The scan slot uses buffered tuples,
	 * so it'd trigger an error in the loop.
	 */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(node->ss.ss_currentRelation),
									&TTSOpsMinimalTuple);

	while (tuplestore_gettupleslot(node->bs_tuplestore, true, true, slot))
	{
		int		cmp = 0;	/* matters for check_watermark=false */
		bool	isnull;
		Datum	value;

		value = slot_getattr(slot, plan->sortColIdx[0], &isnull);

		/* We shouldn't have NULL values in the spill, at least not now. */
		Assert(!isnull);

		if (check_watermark && node->bs_watermark_set)
			cmp = ApplySortComparator(value, false,
									  node->bs_watermark, false,
									  &node->bs_sortsupport);

		if (cmp <= 0)
			tuplesort_puttupleslot(node->bs_tuplesortstate, slot);
		else
			tuplestore_puttupleslot(tupstore, slot);
	}

	/*
	 * Discard the existing tuplestore (that we just processed), use the new
	 * one instead.
	 */
	tuplestore_end(node->bs_tuplestore);
	node->bs_tuplestore = tupstore;

	ExecDropSingleTupleTableSlot(slot);
}

static bool
brinsort_next_range(BrinSortState *node, bool asc)
{
	/* FIXME free the current bs_range, if any */
	node->bs_range = NULL;

	/*
	 * Mark the position, so that we can restore it in case we reach the
	 * current watermark.
	 */
	tuplesort_markpos(node->bs_scan->ranges);

	/*
	 * Get the next range and return it, unless we can prove it's the last
	 * range that can possibly match the current conditon (thanks to how we
	 * order the ranges).
	 *
	 * Also skip ranges that can't possibly match (e.g. because we are in
	 * NULL processing, and the range has no NULLs).
	 */
	while (tuplesort_gettupleslot(node->bs_scan->ranges, true, false, node->bs_scan->slot, NULL))
	{
		bool		isnull;
		Datum		value;

		BrinRange  *range = (BrinRange *) palloc(sizeof(BrinRange));

		range->blkno_start = slot_getattr(node->bs_scan->slot, 1, &isnull);
		range->blkno_end = slot_getattr(node->bs_scan->slot, 2, &isnull);
		range->has_nulls = slot_getattr(node->bs_scan->slot, 3, &isnull);
		range->all_nulls = slot_getattr(node->bs_scan->slot, 4, &isnull);
		range->not_summarized = slot_getattr(node->bs_scan->slot, 5, &isnull);
		range->min_value = slot_getattr(node->bs_scan->slot, 6, &isnull);
		range->max_value = slot_getattr(node->bs_scan->slot, 7, &isnull);

		/*
		 * Not-summarized ranges match irrespectedly of the watermark (if
		 * it's set at all).
		 */
		if (range->not_summarized)
		{
			node->bs_range = range;
			return true;
		}

		/*
		 * The range is summarized, but maybe the watermark is not? That
		 * would mean we're processing NULL values, so we skip ranges that
		 * can't possibly match (i.e. with all_nulls=has_nulls=false).
		 */
		if (!node->bs_watermark_set)
		{
			if (range->all_nulls || range->has_nulls)
			{
				node->bs_range = range;
				return true;
			}

			/* update the position and try the next range */
			tuplesort_markpos(node->bs_scan->ranges);
			pfree(range);

			continue;
		}

		/*
		 * So now we have a summarized range, and we know the watermark
		 * is set too (so we're not processing NULLs). We place the ranges
		 * with only nulls last, so once we hit one we're done.
		 */
		if (range->all_nulls)
		{
			pfree(range);
			return false;	/* no more matching ranges */
		}

		/*
		 * Compare the range to the watermark, using either the minval or
		 * maxval, depending on ASC/DESC ordering. If the range precedes the
		 * watermark, return it. Otherwise abort, all the future ranges are
		 * either not matching the watermark (thanks to ordering) or contain
		 * only NULL values.
		 */

		/* use minval or maxval, depending on ASC / DESC */
		value = (asc) ? range->min_value : range->max_value;

		/*
		 * compare it to the current watermark (if set)
		 *
		 * XXX We don't use (... <= 0) here, because then we'd load ranges
		 * with that minval (and there might be multiple), but most of the
		 * rows would go into the tuplestore, because only rows matching the
		 * minval exactly would be loaded into tuplesort.
		 */
		if (ApplySortComparator(value, false,
								 node->bs_watermark, false,
								 &node->bs_sortsupport) < 0)
		{
			node->bs_range = range;
			return true;
		}

		pfree(range);
		break;
	}

	/* not a matching range, we're done */
	tuplesort_restorepos(node->bs_scan->ranges);

	return false;
}

static bool
brinsort_range_with_nulls(BrinSortState *node)
{
	BrinRange *range = node->bs_range;

	if (range->all_nulls || range->has_nulls || range->not_summarized)
		return true;

	return false;
}

static void
brinsort_rescan(BrinSortState *node)
{
	tuplesort_rescan(node->bs_scan->ranges);
}

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the BrinSort node's currentRelation
 *		using the index specified in the BrinSortState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexNext(BrinSortState *node)
{
	BrinSort   *plan = (BrinSort *) node->ss.ps.plan;
	EState	   *estate;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	bool		nullsFirst;
	bool		asc;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;

	/* flip direction if this is an overall backward scan */
	/* XXX For BRIN indexes this is always forward direction */
	// if (ScanDirectionIsBackward(((BrinSort *) node->ss.ps.plan)->indexorderdir))
	if (false)
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->iss_ScanDesc;
	slot = node->ss.ss_ScanTupleSlot;

	nullsFirst = plan->nullsFirst[0];
	asc = ScanDirectionIsForward(plan->indexorderdir);

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */
		scandesc = index_beginscan(node->ss.ss_currentRelation,
								   node->iss_RelationDesc,
								   estate->es_snapshot,
								   node->iss_NumScanKeys,
								   node->iss_NumOrderByKeys);

		node->iss_ScanDesc = scandesc;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->iss_ScanKeys, node->iss_NumScanKeys,
						 node->iss_OrderByKeys, node->iss_NumOrderByKeys);

		/*
		 * Load info about BRIN ranges, sort them to match the desired ordering.
		 */
		ExecInitBrinSortRanges(plan, node);
		node->bs_phase = BRINSORT_START;
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	while (node->bs_phase != BRINSORT_FINISHED)
	{
		CHECK_FOR_INTERRUPTS();

		elog(DEBUG1, "phase = %d", node->bs_phase);

		AssertCheckRanges(node);

		switch (node->bs_phase)
		{
			case BRINSORT_START:

				elog(DEBUG1, "phase = START");

				/*
				 * If we have NULLS FIRST, move to that stage. Otherwise
				 * start scanning regular ranges.
				 */
				if (nullsFirst)
					node->bs_phase = BRINSORT_LOAD_NULLS;
				else
				{
					node->bs_phase = BRINSORT_LOAD_RANGE;

					/* set the first watermark */
					brinsort_update_watermark(node, asc);
				}

				break;

			case BRINSORT_LOAD_RANGE:
				{
					elog(DEBUG1, "phase = LOAD_RANGE");

					/*
					 * Load tuples matching the new watermark from the existing
					 * spill tuplestore. We do this before loading tuples from
					 * the next chunk of ranges, because those will add tuples
					 * to the spill, and we'd end up processing those twice.
					 */
					brinsort_load_spill_tuples(node, true);

					/*
					 * Load tuples from ranges, until we find a range that has
					 * min_value >= watermark.
					 *
					 * XXX In fact, we are guaranteed to find an exact match
					 * for the watermark, because of how we pick the watermark.
					 */
					while (brinsort_next_range(node, asc))
						brinsort_load_tuples(node, true, false);

					/*
					 * If we have loaded any tuples into the tuplesort, try
					 * sorting it and move to producing the tuples.
					 *
					 * XXX The range might have no rows matching the current
					 * watermark, in which case the tuplesort is empty.
					 */
					if (node->bs_tuplesortstate)
					{
						tuplesort_performsort(node->bs_tuplesortstate);
#ifdef BRINSORT_DEBUG
						{
							TuplesortInstrumentation stats;

							tuplesort_get_stats(node->bs_tuplesortstate, &stats);

							elog(DEBUG1, "method: %s  space: %lld kB (%s)",
								 tuplesort_method_name(stats.sortMethod),
								 (long long)stats.spaceUsed,
								 tuplesort_space_type_name(stats.spaceType));
						}
#endif
					}

					node->bs_phase = BRINSORT_PROCESS_RANGE;
					break;
				}

			case BRINSORT_PROCESS_RANGE:

				elog(DEBUG1, "phase BRINSORT_PROCESS_RANGE");

				slot = node->ss.ps.ps_ResultTupleSlot;

				/* read tuples from the tuplesort range, and output them */
				if (node->bs_tuplesortstate != NULL)
				{
					if (tuplesort_gettupleslot(node->bs_tuplesortstate,
										ScanDirectionIsForward(direction),
										false, slot, NULL))
						return slot;

					/* once we're done with the tuplesort, reset it */
					tuplesort_reset(node->bs_tuplesortstate);
				}

				/*
				 * Now that we processed tuples from the last range batch,
				 * see if we reached the end of if we should try updating
				 * the watermark once again. If the watermark is not set,
				 * we've already processed the last range.
				 */
				if (!node->bs_watermark_set)
				{
					if (nullsFirst)
						node->bs_phase = BRINSORT_FINISHED;
					else
					{
						brinsort_rescan(node);
						node->bs_phase = BRINSORT_LOAD_NULLS;
					}
				}
				else
				{
					/* updte the watermark and try reading more ranges */
					node->bs_phase = BRINSORT_LOAD_RANGE;
					brinsort_update_watermark(node, asc);
				}

				break;

			case BRINSORT_LOAD_NULLS:
				{
					elog(DEBUG1, "phase = LOAD_NULLS");

					/*
					 * Try loading another range. If there are no more ranges,
					 * we're done and we move either to loading regular ranges.
					 * Otherwise check if this range can contain 
					 */
					while (true)
					{
						/* no more ranges - terminate or load regular ranges */
						if (!brinsort_next_range(node, asc))
						{
							if (nullsFirst)
							{
								brinsort_rescan(node);
								node->bs_phase = BRINSORT_LOAD_RANGE;
								brinsort_update_watermark(node, asc);
							}
							else
								node->bs_phase = BRINSORT_FINISHED;

							break;
						}

						/* If this range (may) have nulls, proces them */
						if (brinsort_range_with_nulls(node))
							break;
					}

					if (node->bs_range == NULL)
						break;

					/*
					 * There should be nothing left in the tuplestore, because
					 * we flush that at the end of processing regular tuples,
					 * and we don't retain tuples between NULL ranges.
					 */
					// Assert(node->bs_tuplestore == NULL);

					/*
					 * Load the next unprocessed / NULL range. We don't need to
					 * check watermark while processing NULLS.
					 */
					brinsort_load_tuples(node, false, true);

					node->bs_phase = BRINSORT_PROCESS_NULLS;
					break;
				}

				break;

			case BRINSORT_PROCESS_NULLS:

				elog(DEBUG1, "phase = LOAD_NULLS");

				slot = node->ss.ps.ps_ResultTupleSlot;

				Assert(node->bs_tuplestore != NULL);

				/* read tuples from the tuplesort range, and output them */
				if (node->bs_tuplestore != NULL)
				{
					while (tuplestore_gettupleslot(node->bs_tuplestore, true, true, slot))
						return slot;

					tuplestore_end(node->bs_tuplestore);
					node->bs_tuplestore = NULL;

					node->bs_phase = BRINSORT_LOAD_NULLS;	/* load next range */
				}

				break;

			case BRINSORT_FINISHED:
				elog(ERROR, "unexpected BrinSort phase: FINISHED");
				break;
		}
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	node->iss_ReachedEnd = true;
	return ExecClearTuple(slot);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
IndexRecheck(BrinSortState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the indexqual condition? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->indexqualorig, econtext);
}


/* ----------------------------------------------------------------
 *		ExecBrinSort(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBrinSort(PlanState *pstate)
{
	BrinSortState *node = castNode(BrinSortState, pstate);

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 */
	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		ExecReScan((PlanState *) node);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) IndexNext,
					(ExecScanRecheckMtd) IndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBrinSort(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 * ----------------------------------------------------------------
 */
void
ExecReScanBrinSort(BrinSortState *node)
{
	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.  But first,
	 * reset the context so we don't leak memory as each outer tuple is
	 * scanned.  Note this assumes that we will recalculate *all* runtime keys
	 * on each call.
	 */
	if (node->iss_NumRuntimeKeys != 0)
	{
		ExprContext *econtext = node->iss_RuntimeContext;

		ResetExprContext(econtext);
		ExecIndexEvalRuntimeKeys(econtext,
								 node->iss_RuntimeKeys,
								 node->iss_NumRuntimeKeys);
	}
	node->iss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->iss_ScanDesc)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
	node->iss_ReachedEnd = false;

	ExecScanReScan(&node->ss);
}


/* ----------------------------------------------------------------
 *		ExecEndBrinSort
 * ----------------------------------------------------------------
 */
void
ExecEndBrinSort(BrinSortState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc IndexScanDesc;

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->iss_RelationDesc;
	IndexScanDesc = node->iss_ScanDesc;

	/*
	 * clear out tuple table slots
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (IndexScanDesc)
		index_endscan(IndexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);

	if (node->ss.ss_currentScanDesc != NULL)
		table_endscan(node->ss.ss_currentScanDesc);

	if (node->bs_tuplestore != NULL)
		tuplestore_end(node->bs_tuplestore);
	node->bs_tuplestore = NULL;

	if (node->bs_tuplesortstate != NULL)
		tuplesort_end(node->bs_tuplesortstate);
	node->bs_tuplesortstate = NULL;
}

/* ----------------------------------------------------------------
 *		ExecBrinSortMarkPos
 *
 * Note: we assume that no caller attempts to set a mark before having read
 * at least one tuple.  Otherwise, iss_ScanDesc might still be NULL.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortMarkPos(BrinSortState *node)
{
	EState	   *estate = node->ss.ps.state;
	EPQState   *epqstate = estate->es_epq_active;

	if (epqstate != NULL)
	{
		/*
		 * We are inside an EvalPlanQual recheck.  If a test tuple exists for
		 * this relation, then we shouldn't access the index at all.  We would
		 * instead need to save, and later restore, the state of the
		 * relsubs_done flag, so that re-fetching the test tuple is possible.
		 * However, given the assumption that no caller sets a mark at the
		 * start of the scan, we can only get here with relsubs_done[i]
		 * already set, and so no state need be saved.
		 */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (epqstate->relsubs_slot[scanrelid - 1] != NULL ||
			epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/* Verify the claim above */
			if (!epqstate->relsubs_done[scanrelid - 1])
				elog(ERROR, "unexpected ExecBrinSortMarkPos call in EPQ recheck");
			return;
		}
	}

	index_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
void
ExecBrinSortRestrPos(BrinSortState *node)
{
	EState	   *estate = node->ss.ps.state;
	EPQState   *epqstate = estate->es_epq_active;

	if (estate->es_epq_active != NULL)
	{
		/* See comments in ExecIndexMarkPos */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (epqstate->relsubs_slot[scanrelid - 1] != NULL ||
			epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/* Verify the claim above */
			if (!epqstate->relsubs_done[scanrelid - 1])
				elog(ERROR, "unexpected ExecBrinSortRestrPos call in EPQ recheck");
			return;
		}
	}

	index_restrpos(node->iss_ScanDesc);
}

/*
 * somewhat crippled verson of bringetbitmap
 *
 * XXX We don't call consistent function (or any other function), so unlike
 * bringetbitmap we don't set a separate memory context. If we end up filtering
 * the ranges somehow (e.g. by WHERE conditions), this might be necessary.
 *
 * XXX Should be part of opclass, to somewhere in brin_minmax.c etc.
 */
static void
ExecInitBrinSortRanges(BrinSort *node, BrinSortState *planstate)
{
	IndexScanDesc	scan = planstate->iss_ScanDesc;
	Relation	indexRel = planstate->iss_RelationDesc;
	int			attno;
	FmgrInfo   *rangeproc;
	BrinRangeScanDesc *brscan;
	bool		asc;

	/* BRIN Sort only allows ORDER BY using a single column */
	Assert(node->numCols == 1);

	/*
	 * Determine index attnum we're interested in. The sortColIdx has attnums
	 * from the table, but we need index attnum so that we can fetch the right
	 * range summary.
	 *
	 * XXX Maybe we could/should arrange the tlists differently, so that this
	 * is not necessary?
	 *
	 * FIXME This is broken, node->sortColIdx[0] is an index into the target
	 * list, not table attnum.
	 *
	 * FIXME Also the projection is broken.
	 */
	attno = 0;
	for (int i = 0; i < indexRel->rd_index->indnatts; i++)
	{
		if (indexRel->rd_index->indkey.values[i] == node->sortColIdx[0])
		{
			attno = (i + 1);
			break;
		}
	}

	/* make sure we matched the argument */
	Assert(attno > 0);

	/* get procedure to generate sort ranges */
	rangeproc = index_getprocinfo(indexRel, attno, BRIN_PROCNUM_RANGES);

	/*
	 * Should not get here without a proc, thanks to the check before
	 * building the BrinSort path.
	 */
	Assert(OidIsValid(rangeproc->fn_oid));

	memset(&planstate->bs_sortsupport, 0, sizeof(SortSupportData));
	PrepareSortSupportFromOrderingOp(node->sortOperators[0], &planstate->bs_sortsupport);

	/*
	 * Determine if this ASC or DESC sort, so that we can request the
	 * ranges in the appropriate order (ordered either by minval for
	 * ASC, or by maxval for DESC).
	 */
	asc = ScanDirectionIsForward(node->indexorderdir);

	/*
	 * Ask the opclass to produce ranges in appropriate ordering.
	 *
	 * XXX Pass info about ASC/DESC, NULLS FIRST/LAST.
	 */
	brscan = (BrinRangeScanDesc *) DatumGetPointer(FunctionCall3Coll(rangeproc,
											InvalidOid,	/* FIXME use proper collation*/
											PointerGetDatum(scan),
											Int16GetDatum(attno),
											BoolGetDatum(asc)));

	/* allocate for space, and also for the alternative ordering */
	planstate->bs_scan = brscan;
}

/* ----------------------------------------------------------------
 *		ExecInitBrinSort
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
BrinSortState *
ExecInitBrinSort(BrinSort *node, EState *estate, int eflags)
{
	BrinSortState *indexstate;
	Relation	currentRelation;
	LOCKMODE	lockmode;

	/*
	 * create state structure
	 */
	indexstate = makeNode(BrinSortState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;
	indexstate->ss.ps.ExecProcNode = ExecBrinSort;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &indexstate->ss.ps);

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	indexstate->ss.ss_currentRelation = currentRelation;
	indexstate->ss.ss_currentScanDesc = NULL;	/* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &indexstate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&indexstate->ss.ps);
	ExecAssignScanProjectionInfo(&indexstate->ss);

	/*
	 * initialize child expressions
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).  Likewise for
	 * indexorderby, if any.  But the indexqualorig expression is always
	 * initialized even though it will only be used in some uncommon cases ---
	 * would be nice to improve that.  (Problem is that any SubPlans present
	 * in the expression must be found now...)
	 */
	indexstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) indexstate);
	indexstate->indexqualorig =
		ExecInitQual(node->indexqualorig, (PlanState *) indexstate);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return indexstate;

	/* Open the index relation. */
	lockmode = exec_rt_fetch(node->scan.scanrelid, estate)->rellockmode;
	indexstate->iss_RelationDesc = index_open(node->indexid, lockmode);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->iss_RuntimeKeysReady = false;
	indexstate->iss_RuntimeKeys = NULL;
	indexstate->iss_NumRuntimeKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->iss_RelationDesc,
						   node->indexqual,
						   false,
						   &indexstate->iss_ScanKeys,
						   &indexstate->iss_NumScanKeys,
						   &indexstate->iss_RuntimeKeys,
						   &indexstate->iss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/*
	 * If we have runtime keys, we need an ExprContext to evaluate them. The
	 * node's standard context won't do because we want to reset that context
	 * for every tuple.  So, build another context just like the other one...
	 * -tgl 7/11/00
	 */
	if (indexstate->iss_NumRuntimeKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->iss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->iss_RuntimeContext = NULL;
	}

	indexstate->bs_tuplesortstate = NULL;
	indexstate->bs_qual = indexstate->ss.ps.qual;
	indexstate->ss.ps.qual = NULL;
	ExecInitResultTupleSlotTL(&indexstate->ss.ps, &TTSOpsMinimalTuple);

	/*
	 * all done.
	 */
	return indexstate;
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecBrinSortEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortEstimate(BrinSortState *node,
					  ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->iss_PscanLen = index_parallelscan_estimate(node->iss_RelationDesc,
													 estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->iss_PscanLen);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortInitializeDSM
 *
 *		Set up a parallel index scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortInitializeDSM(BrinSortState *node,
						   ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan;

	piscan = shm_toc_allocate(pcxt->toc, node->iss_PscanLen);
	index_parallelscan_initialize(node->ss.ss_currentRelation,
								  node->iss_RelationDesc,
								  estate->es_snapshot,
								  piscan);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, piscan);
	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortReInitializeDSM(BrinSortState *node,
							 ParallelContext *pcxt)
{
	index_parallelrescan(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecBrinSortInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBrinSortInitializeWorker(BrinSortState *node,
							  ParallelWorkerContext *pwcxt)
{
	ParallelIndexScanDesc piscan;

	piscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->iss_ScanDesc =
		index_beginscan_parallel(node->ss.ss_currentRelation,
								 node->iss_RelationDesc,
								 node->iss_NumScanKeys,
								 node->iss_NumOrderByKeys,
								 piscan);

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and pass
	 * the scankeys to the index AM.
	 */
	if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
		index_rescan(node->iss_ScanDesc,
					 node->iss_ScanKeys, node->iss_NumScanKeys,
					 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
}
