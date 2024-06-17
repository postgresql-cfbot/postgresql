/*
 *	qsort.c: standard quicksort algorithm
 */

#include "c.h"

#define ST_SORT pg_qsort
#define ST_ELEMENT_TYPE_VOID
#define ST_COMPARE_RUNTIME_POINTER
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"

/*
 * qsort comparator wrapper for strcmp.
 */
int
pg_qsort_strcmp(const void *a, const void *b)
{
	return strcmp(*(const char *const *) a, *(const char *const *) b);
}

static inline int
sort_int32_asc_cmp(int32* a, int32* b)
{
	if (*a < *b)
		return -1;
	if (*a > *b)
		return 1;
	return 0;
}

#define ST_SORT sort_int32_asc
#define ST_ELEMENT_TYPE int32
#define ST_COMPARE sort_int32_asc_cmp
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"

static inline int
sort_int32_desc_cmp(int32* a, int32* b)
{
	if (*a < *b)
		return 1;
	if (*a > *b)
		return -1;
	return 0;
}
#define ST_SORT sort_int32_desc
#define ST_ELEMENT_TYPE int32
#define ST_COMPARE sort_int32_desc_cmp
#define ST_SCOPE
#define ST_DECLARE
#define ST_DEFINE
#include "lib/sort_template.h"
