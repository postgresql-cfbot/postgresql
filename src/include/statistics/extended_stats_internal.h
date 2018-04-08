/*-------------------------------------------------------------------------
 *
 * extended_stats_internal.h
 *	  POSTGRES extended statistics internal declarations
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/statistics/extended_stats_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXTENDED_STATS_INTERNAL_H
#define EXTENDED_STATS_INTERNAL_H

#include "utils/sortsupport.h"
#include "statistics/statistics.h"


typedef struct
{
	Oid			eqopr;			/* '=' operator for datatype, if any */
	Oid			eqfunc;			/* and associated function */
	Oid			ltopr;			/* '<' operator for datatype, if any */
} StdAnalyzeData;

typedef struct
{
	Datum		value;			/* a data value */
	int			tupno;			/* position index for tuple it came from */
} ScalarItem;

/* (de)serialization info */
typedef struct DimensionInfo
{
	int			nvalues;		/* number of deduplicated values */
	int			nbytes;			/* number of bytes (serialized) */
	int			typlen;			/* pg_type.typlen */
	bool		typbyval;		/* pg_type.typbyval */
}			DimensionInfo;

/* multi-sort */
typedef struct MultiSortSupportData
{
	int			ndims;			/* number of dimensions supported by the */
	SortSupportData ssup[1];	/* sort support data for each dimension */
} MultiSortSupportData;

typedef MultiSortSupportData *MultiSortSupport;

typedef struct SortItem
{
	Datum	   *values;
	bool	   *isnull;
	int			count;
} SortItem;

extern MVNDistinct *statext_ndistinct_build(double totalrows,
						int numrows, HeapTuple *rows,
						Bitmapset *attrs, VacAttrStats **stats);
extern bytea *statext_ndistinct_serialize(MVNDistinct *ndistinct);
extern MVNDistinct *statext_ndistinct_deserialize(bytea *data);

extern MVDependencies *statext_dependencies_build(int numrows, HeapTuple *rows,
						   Bitmapset *attrs, VacAttrStats **stats);
extern bytea *statext_dependencies_serialize(MVDependencies *dependencies);
extern MVDependencies *statext_dependencies_deserialize(bytea *data);

extern MCVList * statext_mcv_build(int numrows, HeapTuple *rows,
								   Bitmapset *attrs, VacAttrStats **stats,
								   HeapTuple **rows_filtered, int *numrows_filtered);
extern bytea *statext_mcv_serialize(MCVList * mcv, VacAttrStats **stats);
extern MCVList * statext_mcv_deserialize(bytea *data);

extern MVHistogram * statext_histogram_build(int numrows, HeapTuple *rows,
											 Bitmapset *attrs, VacAttrStats **stats,
											 int numrows_total);
extern MVHistogram * statext_histogram_deserialize(bytea *data);

extern MultiSortSupport multi_sort_init(int ndims);
extern void multi_sort_add_dimension(MultiSortSupport mss, int sortdim,
						 Oid oper);
extern int	multi_sort_compare(const void *a, const void *b, void *arg);
extern int multi_sort_compare_dim(int dim, const SortItem *a,
					   const SortItem *b, MultiSortSupport mss);
extern int multi_sort_compare_dims(int start, int end, const SortItem *a,
						const SortItem *b, MultiSortSupport mss);
extern int	compare_scalars_simple(const void *a, const void *b, void *arg);
extern int	compare_datums_simple(Datum a, Datum b, SortSupport ssup);
extern int	compare_scalars_partition(const void *a, const void *b, void *arg);

extern void *bsearch_arg(const void *key, const void *base,
			size_t nmemb, size_t size,
			int (*compar) (const void *, const void *, void *),
			void *arg);

extern int *build_attnums(Bitmapset *attrs);

extern SortItem *build_sorted_items(int numrows, HeapTuple *rows,
				   TupleDesc tdesc, MultiSortSupport mss,
				   int numattrs, int *attnums);

extern int	bms_member_index(Bitmapset *keys, AttrNumber varattno);

extern Selectivity mcv_clauselist_selectivity(PlannerInfo *root,
						   StatisticExtInfo *stat,
						   List *clauses,
						   int varRelid,
						   JoinType jointype,
						   SpecialJoinInfo *sjinfo,
						   RelOptInfo *rel,
						   Selectivity *lowsel,
						   Selectivity *totalsel,
						   int *mcv_count);

extern Selectivity histogram_clauselist_selectivity(PlannerInfo *root,
								 StatisticExtInfo *stat,
								 List *clauses,
								 int varRelid,
								 JoinType jointype,
								 SpecialJoinInfo *sjinfo,
								 RelOptInfo *rel);

#endif							/* EXTENDED_STATS_INTERNAL_H */
