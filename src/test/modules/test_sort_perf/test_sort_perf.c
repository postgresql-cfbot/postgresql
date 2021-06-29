#include "postgres.h"

#include "funcapi.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/itemptr.h"

#include <stdlib.h>

static int
itemptr_comparator(const void *a, const void *b)
{
	const ItemPointerData *ipa = (const ItemPointerData *) a;
	const ItemPointerData *ipb = (const ItemPointerData *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(ipa);
	BlockNumber bb = ItemPointerGetBlockNumber(ipb);
	OffsetNumber oa = ItemPointerGetOffsetNumber(ipa);
	OffsetNumber ob = ItemPointerGetOffsetNumber(ipb);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}

PG_MODULE_MAGIC;

/* include the generated code */
#include "test_sort_object_include.c"
#include "test_sort_itemptr_include.c"

PG_FUNCTION_INFO_V1(test_sort_object);
PG_FUNCTION_INFO_V1(test_sort_itemptr);

Datum
test_sort_object(PG_FUNCTION_ARGS)
{
	do_sort_object();

	PG_RETURN_NULL();
}

Datum
test_sort_itemptr(PG_FUNCTION_ARGS)
{
	do_sort_itemptr();

	PG_RETURN_NULL();
}
