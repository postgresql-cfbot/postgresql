#include "postgres.h"

#include "funcapi.h"
#include "catalog/index.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_opfamily_d.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/itemptr.h"
#include "utils/lsyscache.h"

#include <stdlib.h>

// standard comparators

// comparator for qsort_arg() copied from nbtutils.c
static int
btint4fastcmp(const void * x, const void * y)
{
	int32		*a = (int32 *) x;
	int32		*b = (int32 *) y;

	if (*a > *b)
		return 1;
	else if (*a == *b)
		return 0;
	else
		return -1;
}

// specialized qsort with inlined comparator
#define ST_SORT qsort_int32
#define ST_ELEMENT_TYPE Datum
#define ST_COMPARE(a, b) (btint4fastcmp(a, b))
#define ST_SCOPE static
#define ST_DEFINE
#define ST_DECLARE
#include "lib/sort_template.h"

// SQL-callable comparators

typedef struct BTSortArrayContext
{
	FmgrInfo	flinfo;
	Oid			collation;
	bool		reverse;
} BTSortArrayContext;

// comparator for qsort arg
static int
_bt_compare_array_elements(const void *a, const void *b, void *arg)
{
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	BTSortArrayContext *cxt = (BTSortArrayContext *) arg;
	int32		compare;

	compare = DatumGetInt32(FunctionCall2Coll(&cxt->flinfo,
											  cxt->collation,
											  da, db));
	if (cxt->reverse)
		INVERT_COMPARE_RESULT(compare);
	return compare;
}

/* Define a specialized sort function for _bt_sort_array_elements. */
#define ST_SORT qsort_bt_array_elements
#define ST_ELEMENT_TYPE Datum
#define ST_COMPARE(a, b, cxt) \
	DatumGetInt32(FunctionCall2Coll(&cxt->flinfo, cxt->collation, *a, *b))
#define ST_COMPARE_ARG_TYPE BTSortArrayContext
#define ST_SCOPE static
#define ST_DEFINE
#include "lib/sort_template.h"

PG_MODULE_MAGIC;

/* include the test suites */
#include "test_sort_cmp_weight_include.c"

PG_FUNCTION_INFO_V1(test_sort_cmp_weight);
Datum
test_sort_cmp_weight(PG_FUNCTION_ARGS)
{
	const int32 nobjects = PG_GETARG_INT32(0);

	do_sort_cmp_weight(nobjects);
	PG_RETURN_NULL();
}
