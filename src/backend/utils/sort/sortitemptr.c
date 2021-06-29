#include "postgres.h"

#include "catalog/index.h"
#include "utils/sortitemptr.h"

#define ST_SORT qsort_itemptr
#define ST_SCOPE
#define ST_ELEMENT_TYPE ItemPointerData
#define ST_COMPARE(a, b) (itemptr_encode(a) - itemptr_encode(b))
#define ST_COMPARE_RET_TYPE int64
#define ST_DEFINE
#include "lib/sort_template.h"
