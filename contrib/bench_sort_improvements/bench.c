#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include <limits.h>
#include <time.h>


PG_MODULE_MAGIC;

Datum bench_int_sort(PG_FUNCTION_ARGS);
Datum bench_int16_sort(PG_FUNCTION_ARGS);
Datum bench_float8_sort(PG_FUNCTION_ARGS);
Datum bench_oid_sort(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bench_oid_sort);
PG_FUNCTION_INFO_V1(bench_int_sort);
PG_FUNCTION_INFO_V1(bench_int16_sort);
PG_FUNCTION_INFO_V1(bench_float8_sort);

Datum
bench_oid_sort(PG_FUNCTION_ARGS)
{
    int32 list_size = PG_GETARG_INT32(0);
    List *list_first = NIL;
    List *list_second = NIL;
    Oid random_oid;
    struct timespec start, end;
    long time_spent_first;
    long time_spent_second;
    double percentage_difference = 0.0;
    char *result_message;

    for (int i = 0; i < list_size; i++)
    {
        random_oid = (Oid) (random() % (UINT_MAX - 1) + 1); 
        list_first = lappend_oid(list_first, random_oid);
        list_second = lappend_oid(list_second, random_oid);
    }

    // Timing the first sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    list_sort(list_first, list_oid_cmp);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_first = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    // Timing the second sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    list_oid_sort(list_second);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_second = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    percentage_difference = ((double)(time_spent_first - time_spent_second) / time_spent_first) * 100.0;

    list_free(list_first);
    list_free(list_second);
    
    result_message = psprintf("Time taken by list_sort: %ld ns, Time taken by list_oid_sort: %ld ns, Percentage difference: %.2f%%", 
                              time_spent_first, time_spent_second, percentage_difference);
    PG_RETURN_TEXT_P(cstring_to_text(result_message));
}

Datum
bench_int_sort(PG_FUNCTION_ARGS)
{
    int32 list_size = PG_GETARG_INT32(0);
    List *list_first = NIL;
    List *list_second = NIL;
    int random_int;
    struct timespec start, end;
    long time_spent_first;
    long time_spent_second;
    double percentage_difference = 0.0;
    char *result_message;

    for (int i = 0; i < list_size; i++)
    {
        random_int = rand(); 
        list_first = lappend_int(list_first, random_int); 
        list_second = lappend_int(list_second, random_int); 
    }

    // Timing the first sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    list_sort(list_first, list_oid_cmp);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_first = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    // Timing the second sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    list_int_sort(list_second);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_second = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    percentage_difference = ((double)(time_spent_first - time_spent_second) / time_spent_first) * 100.0;

    list_free(list_first);
    list_free(list_second);
    
    result_message = psprintf("Time taken by list_sort: %ld ns, Time taken by list_int_sort: %ld ns, Percentage difference: %.2f%%", 
                              time_spent_first, time_spent_second, percentage_difference);
    PG_RETURN_TEXT_P(cstring_to_text(result_message));
}

/*
stupid copy for tests
*/
static int
compare_int16(const void *a, const void *b)
{
	int			av = *(const int16 *) a;
	int			bv = *(const int16 *) b;

	/* this can't overflow if int is wider than int16 */
	return (av - bv);
}

Datum
bench_int16_sort(PG_FUNCTION_ARGS)
{
    int32 arr_size = PG_GETARG_INT32(0);
    int16 *arr_first = (int16 *)palloc(arr_size * sizeof(int16));
    int16 *arr_second = (int16 *)palloc(arr_size * sizeof(int16));
    struct timespec start, end;
    long time_spent_first;
    long time_spent_second;
    double percentage_difference = 0.0;
    char *result_message;

    for (int i = 0; i < arr_size; i++)
    {
        arr_first[i] = (int16)rand();
        arr_second[i] = (int16)rand();
    }

    // Timing the first sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    qsort(arr_first, arr_size, sizeof(int16), compare_int16);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_first = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    // Timing the second sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    sort_int_16_arr(arr_second, arr_size);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_second = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    percentage_difference = ((double)(time_spent_first - time_spent_second) / time_spent_first) * 100.0;

    pfree(arr_first);
    pfree(arr_second);
    
    result_message = psprintf("Time taken by usual sort: %ld ns, Time taken by optimized sort: %ld ns, Percentage difference: %.2f%%", 
                              time_spent_first, time_spent_second, percentage_difference);
    PG_RETURN_TEXT_P(cstring_to_text(result_message));
}

double inline rand_double() {
    return (double)rand() / RAND_MAX; 
}

/*
stupid copy for tests
*/
static int
compareDoubles(const void *a, const void *b)
{
	float8		x = *(float8 *) a;
	float8		y = *(float8 *) b;

	if (x == y)
		return 0;
	return (x > y) ? 1 : -1;
}

/*
stupid copy for tests
*/
#define ST_SORT sort_float8_arr
#define ST_ELEMENT_TYPE float8
#define ST_COMPARE(a, b) compareDoubles(a, b)
#define ST_SCOPE static
#define ST_DEFINE
#include <lib/sort_template.h>

Datum
bench_float8_sort(PG_FUNCTION_ARGS)
{
    int32 arr_size = PG_GETARG_INT32(0);
    float8 *arr_first = (float8 *)palloc(arr_size * sizeof(float8));
    float8 *arr_second = (float8 *)palloc(arr_size * sizeof(float8));
    struct timespec start, end;
    long time_spent_first;
    long time_spent_second;
    double percentage_difference = 0.0;
    char *result_message;

    for (int i = 0; i < arr_size; i++)
    {
        arr_first[i] = rand_double();
        arr_second[i] = rand_double();
    }

    // Timing the first sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    qsort(arr_first, arr_size, sizeof(float8), compareDoubles);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_first = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    // Timing the second sort function
    clock_gettime(CLOCK_MONOTONIC, &start);
    sort_float8_arr(arr_second, arr_size);
    clock_gettime(CLOCK_MONOTONIC, &end);
    time_spent_second = (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);

    percentage_difference = ((double)(time_spent_first - time_spent_second) / time_spent_first) * 100.0;

    pfree(arr_first);
    pfree(arr_second);
    
    result_message = psprintf("Time taken by usual sort: %ld ns, Time taken by optimized sort: %ld ns, Percentage difference: %.2f%%", 
                              time_spent_first, time_spent_second, percentage_difference);
    PG_RETURN_TEXT_P(cstring_to_text(result_message));
}

void
_PG_init()
{
    srand(time(NULL));
}