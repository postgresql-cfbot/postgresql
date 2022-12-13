/*
 * AM-callable functions for BRIN indexes
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

/*
 * Info about ranges for BRIN Sort.
 */
typedef struct BrinRange
{
	BlockNumber blkno_start;
	BlockNumber blkno_end;

	Datum	min_value;
	Datum	max_value;
	bool	has_nulls;
	bool	all_nulls;
	bool	not_summarized;

	/*
	 * Index of the range when ordered by min_value (if there are multiple
	 * ranges with the same min_value, it's the lowest one).
	 */
	uint32	min_index;

	/*
	 * Minimum min_index from all ranges with higher max_value (i.e. when
	 * sorted by max_value). If there are multiple ranges with the same
	 * max_value, it depends on the ordering (i.e. the ranges may get
	 * different min_index_lowest, depending on the exact ordering).
	 */
	uint32	min_index_lowest;
} BrinRange;

typedef struct BrinRanges
{
	int			nranges;
	BrinRange	ranges[FLEXIBLE_ARRAY_MEMBER];
} BrinRanges;

typedef struct BrinMinmaxStats
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int64		n_ranges;
	int64		n_summarized;
	int64		n_all_nulls;
	int64		n_has_nulls;
	double		avg_overlaps;
	double		avg_matches;
	double		avg_matches_unique;
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
