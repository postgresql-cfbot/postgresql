/*
 * AM-callable functions for BRIN indexes
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/brin.h
 */
#ifndef BRIN_H
#define BRIN_H

#include "nodes/execnodes.h"
#include "utils/relcache.h"


/*
 * Storage type for BRIN's reloptions
 */
typedef struct BrinOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	BlockNumber pagesPerRange;
	bool		autosummarize;
} BrinOptions;


/*
 * BrinStatsData represents stats data for planner use
 */
typedef struct BrinStatsData
{
	BlockNumber pagesPerRange;
	BlockNumber revmapNumPages;
} BrinStatsData;

typedef struct BrinMinmaxStats
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int64		n_ranges;
	int64		n_summarized;
	int64		n_all_nulls;
	int64		n_has_nulls;

	/* average number of overlapping ranges */
	double		avg_overlaps;

	/* average number of matching ranges (per value) */
	double		avg_matches;
	double		avg_matches_unique;

	/* minval/maxval stats (ndistinct, correlation to blkno) */
	int64		minval_ndistinct;
	int64		maxval_ndistinct;
	double		minval_correlation;
	double		maxval_correlation;

	/* minval/maxval increment stats */
	double		minval_increment_avg;
	double		minval_increment_max;
	double		maxval_increment_avg;
	double		maxval_increment_max;

} BrinMinmaxStats;

#define BRIN_DEFAULT_PAGES_PER_RANGE	128
#define BrinGetPagesPerRange(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == BRIN_AM_OID), \
	 (relation)->rd_options ? \
	 ((BrinOptions *) (relation)->rd_options)->pagesPerRange : \
	  BRIN_DEFAULT_PAGES_PER_RANGE)
#define BrinGetAutoSummarize(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == BRIN_AM_OID), \
	 (relation)->rd_options ? \
	 ((BrinOptions *) (relation)->rd_options)->autosummarize : \
	  false)


extern void brinGetStats(Relation index, BrinStatsData *stats);

#endif							/* BRIN_H */
