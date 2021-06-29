#include "postgres.h"

#include "utils/sortscalar.h"

#define ST_SORT qsort_uint32
#define ST_ELEMENT_TYPE uint32
#define ST_COMPARE(a, b) (((int64) *(a)) - ((int64) *(b)))
#define ST_COMPARE_RET_TYPE int64
#define ST_SCOPE
#define ST_DEFINE
#include "lib/sort_template.h"
