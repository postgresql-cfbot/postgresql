#include <math.h>

static void run_tests(char* type, Datum *unsorted, Datum *sorted, BTSortArrayContext *cxt, int nobjects)
{
	const int numtests = 10;
	double		time,
				min;

	if (nobjects <= 100)
	{
		elog(NOTICE, "order: %s", type);
		for (int j = 0; j < nobjects; ++j)
			elog(NOTICE, "%d", DatumGetInt32(unsorted[j]));
		return;
	}

	min = 1000000;
	for (int i = 1; i <= numtests; ++i)
	{
		instr_time start_time, end_time;
		memcpy(sorted, unsorted, sizeof(Datum) * nobjects);
		INSTR_TIME_SET_CURRENT(start_time);
		qsort_arg((void *) sorted, nobjects, sizeof(Datum), _bt_compare_array_elements, (void *) cxt);
		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);
		time = INSTR_TIME_GET_DOUBLE(end_time);
		if (time < min)
			min = time;
	}
	elog(NOTICE, "[fmgr runtime] num=%d, threshold=%d, order=%s, time=%f", nobjects, ST_INSERTION_SORT_THRESHOLD, type, min);

	min = 1000000;
	for (int i = 1; i <= numtests; ++i)
	{
		instr_time start_time, end_time;
		memcpy(sorted, unsorted, sizeof(Datum) * nobjects);
		INSTR_TIME_SET_CURRENT(start_time);
		qsort_bt_array_elements(sorted, nobjects, cxt);
		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);
		time = INSTR_TIME_GET_DOUBLE(end_time);
		if (time < min)
			min = time;
	}
	elog(NOTICE, "[fmgr specialized] num=%d, threshold=%d, order=%s, time=%f", nobjects, ST_INSERTION_SORT_THRESHOLD, type, min);

	min = 1000000;
	for (int i = 1; i <= numtests; ++i)
	{
		instr_time start_time, end_time;
		memcpy(sorted, unsorted, sizeof(Datum) * nobjects);
		INSTR_TIME_SET_CURRENT(start_time);
		qsort(sorted, nobjects, sizeof(Datum), btint4fastcmp);
		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);
		time = INSTR_TIME_GET_DOUBLE(end_time);
		if (time < min)
			min = time;
	}
	elog(NOTICE, "[C runtime] num=%d, threshold=%d, order=%s, time=%f", nobjects, ST_INSERTION_SORT_THRESHOLD, type, min);

	min = 1000000;
	for (int i = 0; i < numtests; ++i)
	{
		instr_time start_time, end_time;
		memcpy(sorted, unsorted, sizeof(Datum) * nobjects);
		INSTR_TIME_SET_CURRENT(start_time);
		qsort_int32(sorted, nobjects);
		INSTR_TIME_SET_CURRENT(end_time);
		INSTR_TIME_SUBTRACT(end_time, start_time);
		time = INSTR_TIME_GET_DOUBLE(end_time);
		if (time < min)
			min = time;
	}
	elog(NOTICE, "[C specialized] num=%d, threshold=%d, order=%s, time=%f", nobjects, ST_INSERTION_SORT_THRESHOLD, type, min);
}

typedef struct source_value
{
	int32 value;
	float rand;
} source_value;

static int
source_rand_cmp(const void * x, const void * y)
{
	source_value		*a = (source_value *) x;
	source_value		*b = (source_value *) y;

	if (a->rand > b->rand)
		return 1;
	else if (a->rand < b->rand)
		return -1;
	else
		return 0;
}

static int
source_dither_cmp(const void * x, const void * y, void * nobjects)
{
	source_value		*a = (source_value *) x;
	source_value		*b = (source_value *) y;
	float dither;

	if (*(int *) nobjects <= 100)
		dither = 5;
	else
		dither = 100;

	if ((a->value + dither * a->rand) > (b->value + dither * b->rand))
		return 1;
	else if ((a->value + dither * a->rand) < (b->value + dither * b->rand))
		return -1;
	else
		return 0;
}

static void
do_sort_cmp_weight(int nobjects)
{
	source_value *source = malloc(sizeof(source_value) * nobjects);
	source_value *tmp = malloc(sizeof(source_value) * nobjects);
	Datum *unsorted = malloc(sizeof(Datum) * nobjects);
	Datum *sorted = malloc(sizeof(Datum) * nobjects);

	// for fmgr comparator tests

	BTSortArrayContext cxt;
	RegProcedure cmp_proc ;

	// to keep from pulling in nbtree.h
#define BTORDER_PROC 1

	cmp_proc = get_opfamily_proc(INTEGER_BTREE_FAM_OID,
								 INT4OID,
								 INT4OID,
								 BTORDER_PROC);

	fmgr_info(cmp_proc, &cxt.flinfo);
	cxt.collation = DEFAULT_COLLATION_OID;
	cxt.reverse = false;

	// needed for some distributions: first populate source array with ascending value and random tag
	for (int i = 0; i < nobjects; ++i)
	{
		source[i].value = i;
		source[i].rand = (float) random() / (float) RAND_MAX; // between 0 and 1
	}
// 	if (nobjects <= 100)
// 		for (int j = 0; j < nobjects; ++j)
// 			elog(NOTICE, "%f", (source[j].rand));
	qsort(source, nobjects, sizeof(source_value), source_rand_cmp);

	// random
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(source[i].value);
	run_tests("random", unsorted, sorted, &cxt, nobjects);

	// for these, sort the first X % of the array

	// sort50
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(source[i].value);
	// sort first part of unsorted[]
	qsort(unsorted, nobjects/2, sizeof(Datum), btint4fastcmp);
	run_tests("sort50", unsorted, sorted, &cxt, nobjects);

	// sort90
	qsort(unsorted, nobjects/10 * 9, sizeof(Datum), btint4fastcmp);
	run_tests("sort90", unsorted, sorted, &cxt, nobjects);

	// sort99
	qsort(unsorted, nobjects/100 * 99, sizeof(Datum), btint4fastcmp);
	run_tests("sort99", unsorted, sorted, &cxt, nobjects);

	// dither -- copy source to tmp array because it sorts differently
	memcpy(tmp, source, sizeof(source_value) * nobjects);
	qsort_arg(tmp, nobjects, sizeof(source_value), source_dither_cmp, (void *) &nobjects);
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(tmp[i].value);
	run_tests("dither", unsorted, sorted, &cxt, nobjects);

	// descending
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(nobjects - i);
	run_tests("descending", unsorted, sorted, &cxt, nobjects);

	// organ
	for (int i = 0; i < nobjects/2; ++i)
		unsorted[i] = Int32GetDatum(i);
	for (int i = nobjects/2; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(nobjects - i);
	run_tests("organ", unsorted, sorted, &cxt, nobjects);

	// merge
	for (int i = 0; i < nobjects/2; ++i)
		unsorted[i] = Int32GetDatum(i);
	for (int i = nobjects/2; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(i - nobjects/2);
	run_tests("merge", unsorted, sorted, &cxt, nobjects);

	// dup8
	for (int i = 0; i < nobjects; ++i)
	{
		uint32 tmp = 1; // XXX this will only show duplicates for large nobjects
		for (int j=0; j<8; ++j)
			tmp *= (uint32) source[i].value;
		tmp = (tmp + nobjects/2) % nobjects;
		unsorted[i] = Int32GetDatum((int) tmp);
	}
	run_tests("dup8", unsorted, sorted, &cxt, nobjects);

	// dupsq
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum((int) sqrt(source[i].value));
	run_tests("dupsq", unsorted, sorted, &cxt, nobjects);

	// mod100
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(source[i].value % 100);
	run_tests("mod100", unsorted, sorted, &cxt, nobjects);

	// mod8
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(source[i].value % 8);
	run_tests("mod8", unsorted, sorted, &cxt, nobjects);

	// ascending
	for (int i = 0; i < nobjects; ++i)
		unsorted[i] = Int32GetDatum(i);
	run_tests("ascending", unsorted, sorted, &cxt, nobjects);

	free(tmp);
	free(source);
	free(sorted);
	free(unsorted);
}
