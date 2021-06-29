#ifndef SORTSCALAR_H
#define SORTSCALAR_H

/* We'll define the sort functions in sortscalar.c (they'd better match). */
extern void qsort_uint32(uint32 *array, size_t n);

/* The unique and search functions are defined here so they can be inlined. */
#define ST_UNIQUE unique_uint32
#define ST_SEARCH bsearch_uint32
#define ST_ELEMENT_TYPE uint32
#define ST_COMPARE(a, b) (((int64) *(a)) - ((int64) *(b)))
#define ST_COMPARE_RET_TYPE int64
#define ST_SCOPE static inline
#define ST_DEFINE
#include "lib/sort_template.h"

/* Provide names for other common types that are currently uint32. */
#define qsort_oid			qsort_uint32
#define unique_oid			unique_uint32
#define bsearch_oid			bsearch_uint32

#define qsort_blocknum		qsort_uint32
#define unique_blocknum		unique_uint32
#define bsearch_blocknum	bsearch_uint32

#endif
