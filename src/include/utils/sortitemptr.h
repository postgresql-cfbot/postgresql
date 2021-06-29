#ifndef SORTITEMPTR_H
#define SORTITEMPTR_H

#include "catalog/index.h"

/* Declare sort function, from .c file. */
extern void qsort_itemptr(ItemPointerData *tids, size_t n);

/* Search and unique functions inline in header. */
#define ST_SEARCH bsearch_itemptr
#define ST_UNIQUE unique_itemptr
#define ST_ELEMENT_TYPE ItemPointerData
#define ST_COMPARE(a, b) (itemptr_encode(a) - itemptr_encode(b))
#define ST_COMPARE_RET_TYPE int64
#define ST_SCOPE static inline
#define ST_DEFINE
#include "lib/sort_template.h"

#endif
