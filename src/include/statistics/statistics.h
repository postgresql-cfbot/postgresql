/*-------------------------------------------------------------------------
 *
 * statistics.h
 *	  Extended statistics and selectivity estimation functions.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/statistics/statistics.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STATISTICS_H
#define STATISTICS_H

#include "commands/vacuum.h"
#include "nodes/relation.h"

/*
 * Degree of how much MCV item matches a clause.
 * This is then considered when computing the selectivity.
 */
#define STATS_MATCH_NONE		0	/* no match at all */
#define STATS_MATCH_PARTIAL		1	/* partial match */
#define STATS_MATCH_FULL		2	/* full match */

#define STATS_MAX_DIMENSIONS	8	/* max number of attributes */

/* Multivariate distinct coefficients */
#define STATS_NDISTINCT_MAGIC		0xA352BFA4	/* struct identifier */
#define STATS_NDISTINCT_TYPE_BASIC	1	/* struct version */

/* MVDistinctItem represents a single combination of columns */
typedef struct MVNDistinctItem
{
	double		ndistinct;		/* ndistinct value for this combination */
	Bitmapset  *attrs;			/* attr numbers of items */
} MVNDistinctItem;

/* size of the struct, excluding attribute list */
#define SizeOfMVNDistinctItem \
	(offsetof(MVNDistinctItem, ndistinct) + sizeof(double))

/* A MVNDistinct object, comprising all possible combinations of columns */
typedef struct MVNDistinct
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of ndistinct (BASIC) */
	uint32		nitems;			/* number of items in the statistic */
	MVNDistinctItem items[FLEXIBLE_ARRAY_MEMBER];
} MVNDistinct;

/* size of the struct excluding the items array */
#define SizeOfMVNDistinct	(offsetof(MVNDistinct, nitems) + sizeof(uint32))


/* size of the struct excluding the items array */
#define SizeOfMVNDistinct	(offsetof(MVNDistinct, nitems) + sizeof(uint32))

#define STATS_DEPS_MAGIC		0xB4549A2C	/* marks serialized bytea */
#define STATS_DEPS_TYPE_BASIC	1	/* basic dependencies type */

/*
 * Functional dependencies, tracking column-level relationships (values
 * in one column determine values in another one).
 */
typedef struct MVDependency
{
	double		degree;			/* degree of validity (0-1) */
	AttrNumber	nattributes;	/* number of attributes */
	AttrNumber	attributes[FLEXIBLE_ARRAY_MEMBER];	/* attribute numbers */
} MVDependency;

/* size of the struct excluding the deps array */
#define SizeOfDependency \
	(offsetof(MVDependency, nattributes) + sizeof(AttrNumber))

typedef struct MVDependencies
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MV Dependencies (BASIC) */
	uint32		ndeps;			/* number of dependencies */
	MVDependency *deps[FLEXIBLE_ARRAY_MEMBER];	/* dependencies */
} MVDependencies;

/* size of the struct excluding the deps array */
#define SizeOfDependencies	(offsetof(MVDependencies, ndeps) + sizeof(uint32))

/* used to flag stats serialized to bytea */
#define STATS_MCV_MAGIC                        0xE1A651C2	/* marks serialized
															 * bytea */
#define STATS_MCV_TYPE_BASIC   1	/* basic MCV list type */

/* max items in MCV list (mostly arbitrary number) */
#define STATS_MCVLIST_MAX_ITEMS        8192

/*
 * Multivariate MCV (most-common value) lists
 *
 * A straight-forward extension of MCV items - i.e. a list (array) of
 * combinations of attribute values, together with a frequency and null flags.
 */
typedef struct MCVItem
{
	double		frequency;		/* frequency of this combination */
	double		base_frequency;	/* frequency if independent */
	bool	   *isnull;			/* lags of NULL values (up to 32 columns) */
	Datum	   *values;			/* variable-length (ndimensions) */
}			MCVItem;

/* multivariate MCV list - essentally an array of MCV items */
typedef struct MCVList
{
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of MCV list (BASIC) */
	uint32		nitems;			/* number of MCV items in the array */
	AttrNumber	ndimensions;	/* number of dimensions */
	Oid			types[STATS_MAX_DIMENSIONS];	/* OIDs of data types */
	MCVItem   **items;			/* array of MCV items */
}			MCVList;


/* used to flag stats serialized to bytea */
#define STATS_HIST_MAGIC       0x7F8C5670	/* marks serialized bytea */
#define STATS_HIST_TYPE_BASIC  1	/* basic histogram type */

/* max buckets in a histogram (mostly arbitrary number) */
#define STATS_HIST_MAX_BUCKETS 16384

/*
 * Histogram in a partially serialized form, with deduplicated boundary
 * values etc.
 */
typedef struct MVBucket
{
	/* Frequencies of this bucket. */
	float		frequency;

	/*
	 * Information about dimensions being NULL-only. Not yet used.
	 */
	bool	   *nullsonly;

	/* lower boundaries - values and information about the inequalities */
	uint16	   *min;
	bool	   *min_inclusive;

	/*
	 * indexes of upper boundaries - values and information about the
	 * inequalities (exclusive vs. inclusive)
	 */
	uint16	   *max;
	bool	   *max_inclusive;
}			MVBucket;

typedef struct MVHistogram
{
	/* varlena header (do not touch directly!) */
	int32		vl_len_;
	uint32		magic;			/* magic constant marker */
	uint32		type;			/* type of histogram (BASIC) */
	uint32		nbuckets;		/* number of buckets (buckets array) */
	uint32		ndimensions;	/* number of dimensions */
	Oid			types[STATS_MAX_DIMENSIONS];	/* OIDs of data types */

	/*
	 * keep this the same with MVHistogram, because of deserialization (same
	 * offset)
	 */
	MVBucket  **buckets;		/* array of buckets */

	/*
	 * serialized boundary values, one array per dimension, deduplicated (the
	 * min/max indexes point into these arrays)
	 */
	int		   *nvalues;
	Datum	  **values;
}			MVHistogram;

extern MVNDistinct *statext_ndistinct_load(Oid mvoid);
extern MVDependencies *statext_dependencies_load(Oid mvoid);
extern MCVList * statext_mcv_load(Oid mvoid);
extern MVHistogram * statext_histogram_load(Oid mvoid);

extern void BuildRelationExtStatistics(Relation onerel, double totalrows,
						   int numrows, HeapTuple *rows,
						   int natts, VacAttrStats **vacattrstats);
extern bool statext_is_kind_built(HeapTuple htup, char kind);
extern Selectivity dependencies_clauselist_selectivity(PlannerInfo *root,
									List *clauses,
									int varRelid,
									JoinType jointype,
									SpecialJoinInfo *sjinfo,
									RelOptInfo *rel,
									Bitmapset **estimatedclauses);
extern Selectivity statext_clauselist_selectivity(PlannerInfo *root,
							   List *clauses,
							   int varRelid,
							   JoinType jointype,
							   SpecialJoinInfo *sjinfo,
							   RelOptInfo *rel,
							   Bitmapset **estimatedclauses);
extern bool has_stats_of_kind(List *stats, int requiredkinds);
extern StatisticExtInfo *choose_best_statistics(List *stats,
					   Bitmapset *attnums, int requiredkinds);

#endif							/* STATISTICS_H */
