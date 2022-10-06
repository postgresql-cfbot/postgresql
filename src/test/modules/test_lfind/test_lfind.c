/*--------------------------------------------------------------------------
 *
 * test_lfind.c
 *		Test correctness of optimized linear search functions.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_lfind/test_lfind.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "port/pg_lfind.h"

/*
 * Convenience macros for testing both vector and scalar operations. The 2x
 * factor is to make sure iteration works
 */
#define LEN_NO_TAIL(vectortype) (2 * sizeof(vectortype))
#define LEN_WITH_TAIL(vectortype) (LEN_NO_TAIL(vectortype) + 3)

PG_MODULE_MAGIC;

/* workhorse for test_lfind8 */
static void
test_lfind8_internal(uint8 key)
{
	uint8		charbuf[LEN_WITH_TAIL(Vector8)];
	const int	len_no_tail = LEN_NO_TAIL(Vector8);
	const int	len_with_tail = LEN_WITH_TAIL(Vector8);

	memset(charbuf, 0xFF, len_with_tail);
	/* search tail to test one-byte-at-a-time path */
	charbuf[len_with_tail - 1] = key;
	if (key > 0x00 && pg_lfind8(key - 1, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key - 1);
	if (key < 0xFF && !pg_lfind8(key, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8() did not find existing element '0x%x'", key);
	if (key < 0xFE && pg_lfind8(key + 1, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key + 1);

	memset(charbuf, 0xFF, len_with_tail);
	/* search with vector operations */
	charbuf[len_no_tail - 1] = key;
	if (key > 0x00 && pg_lfind8(key - 1, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key - 1);
	if (key < 0xFF && !pg_lfind8(key, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8() did not find existing element '0x%x'", key);
	if (key < 0xFE && pg_lfind8(key + 1, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8() found nonexistent element '0x%x'", key + 1);
}

PG_FUNCTION_INFO_V1(test_lfind8);
Datum
test_lfind8(PG_FUNCTION_ARGS)
{
	test_lfind8_internal(0);
	test_lfind8_internal(1);
	test_lfind8_internal(0x7F);
	test_lfind8_internal(0x80);
	test_lfind8_internal(0x81);
	test_lfind8_internal(0xFD);
	test_lfind8_internal(0xFE);
	test_lfind8_internal(0xFF);

	PG_RETURN_VOID();
}

/* workhorse for test_lfind8_le */
static void
test_lfind8_le_internal(uint8 key)
{
	uint8		charbuf[LEN_WITH_TAIL(Vector8)];
	const int	len_no_tail = LEN_NO_TAIL(Vector8);
	const int	len_with_tail = LEN_WITH_TAIL(Vector8);

	memset(charbuf, 0xFF, len_with_tail);
	/* search tail to test one-byte-at-a-time path */
	charbuf[len_with_tail - 1] = key;
	if (key > 0x00 && pg_lfind8_le(key - 1, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8_le() found nonexistent element <= '0x%x'", key - 1);
	if (key < 0xFF && !pg_lfind8_le(key, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8_le() did not find existing element <= '0x%x'", key);
	if (key < 0xFE && !pg_lfind8_le(key + 1, charbuf, len_with_tail))
		elog(ERROR, "pg_lfind8_le() did not find existing element <= '0x%x'", key + 1);

	memset(charbuf, 0xFF, len_with_tail);
	/* search with vector operations */
	charbuf[len_no_tail - 1] = key;
	if (key > 0x00 && pg_lfind8_le(key - 1, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8_le() found nonexistent element <= '0x%x'", key - 1);
	if (key < 0xFF && !pg_lfind8_le(key, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8_le() did not find existing element <= '0x%x'", key);
	if (key < 0xFE && !pg_lfind8_le(key + 1, charbuf, len_no_tail))
		elog(ERROR, "pg_lfind8_le() did not find existing element <= '0x%x'", key + 1);
}

PG_FUNCTION_INFO_V1(test_lfind8_le);
Datum
test_lfind8_le(PG_FUNCTION_ARGS)
{
	test_lfind8_le_internal(0);
	test_lfind8_le_internal(1);
	test_lfind8_le_internal(0x7F);
	test_lfind8_le_internal(0x80);
	test_lfind8_le_internal(0x81);
	test_lfind8_le_internal(0xFD);
	test_lfind8_le_internal(0xFE);
	test_lfind8_le_internal(0xFF);

	PG_RETURN_VOID();
}

static void
test_lsearch8_internal(uint8 key)
{
	uint8		charbuf[LEN_WITH_TAIL(Vector8)];
	const int	len_no_tail = LEN_NO_TAIL(Vector8);
	const int	len_with_tail = LEN_WITH_TAIL(Vector8);
	int			keypos;

	memset(charbuf, 0xFF, len_with_tail);
	/* search tail to test one-byte-at-a-time path */
	keypos = len_with_tail - 1;
	charbuf[keypos] = key;
	if (key > 0x00 && (pg_lsearch8(key - 1, charbuf, len_with_tail) != -1))
		elog(ERROR, "pg_lsearch8() found nonexistent element '0x%x'", key - 1);
	if (key < 0xFF && (pg_lsearch8(key, charbuf, len_with_tail) != keypos))
		elog(ERROR, "pg_lsearch8() did not find existing element '0x%x'", key);
	if (key < 0xFE && (pg_lsearch8(key + 1, charbuf, len_with_tail) != -1))
		elog(ERROR, "pg_lsearch8() found nonexistent element '0x%x'", key + 1);

	memset(charbuf, 0xFF, len_with_tail);
	/* search with vector operations */
	keypos = len_no_tail - 1;
	charbuf[keypos] = key;
	if (key > 0x00 && (pg_lsearch8(key - 1, charbuf, len_no_tail) != -1))
		elog(ERROR, "pg_lsearch8() found nonexistent element '0x%x'", key - 1);
	if (key < 0xFF && (pg_lsearch8(key, charbuf, len_no_tail) != keypos))
		elog(ERROR, "pg_lsearch8() did not find existing element '0x%x'", key);
	if (key < 0xFE && (pg_lsearch8(key + 1, charbuf, len_no_tail) != -1))
		elog(ERROR, "pg_lsearch8() found nonexistent element '0x%x'", key + 1);
}

PG_FUNCTION_INFO_V1(test_lsearch8);
Datum
test_lsearch8(PG_FUNCTION_ARGS)
{
	test_lsearch8_internal(0);
	test_lsearch8_internal(1);
	test_lsearch8_internal(0x7F);
	test_lsearch8_internal(0x80);
	test_lsearch8_internal(0x81);
	test_lsearch8_internal(0xFD);
	test_lsearch8_internal(0xFE);
	test_lsearch8_internal(0xFF);

	PG_RETURN_VOID();
}

static void
report_lsearch8_error(uint8 *buf, int size, uint8 key, int result, int expected)
{
	StringInfoData bufstr;
	char *sep = "";

	initStringInfo(&bufstr);

	for (int i = 0; i < size; i++)
	{
		appendStringInfo(&bufstr, "%s0x%02x", sep, buf[i]);
		sep = ",";
	}

	elog(ERROR,
		 "pg_lsearch8_ge returned %d, expected %d, key 0x%02x buffer %s",
		 result, expected, key, bufstr.data);
}

/* workhorse for test_lsearch8_ge */
static void
test_lsearch8_ge_internal(uint8 *buf, uint8 key)
{
	const int	len_no_tail = LEN_NO_TAIL(Vector8);
	const int	len_with_tail = LEN_WITH_TAIL(Vector8);
	int			expected;
	int			result;
	int			i;

	/* search tail to test one-byte-at-a-time path */
	for (i = 0; i < len_with_tail; i++)
	{
		if (buf[i] >= key)
			break;
	}
	expected = i;
	result = pg_lsearch8_ge(key, buf, len_with_tail);

	if (result != expected)
		report_lsearch8_error(buf, len_with_tail, key, result, expected);

	/* search with vector operations */
	for (i = 0; i < len_no_tail; i++)
	{
		if (buf[i] >= key)
			break;
	}
	expected = i;
	result = pg_lsearch8_ge(key, buf, len_no_tail);

	if (result != expected)
		report_lsearch8_error(buf, len_no_tail, key, result, expected);
}

static int
cmp(const void *p1, const void *p2)
{
	uint8	v1 = *((const uint8 *) p1);
	uint8	v2 = *((const uint8 *) p2);

	if (v1 < v2)
		return -1;
	if (v1 > v2)
		return 1;
	return 0;
}

PG_FUNCTION_INFO_V1(test_lsearch8_ge);
Datum
test_lsearch8_ge(PG_FUNCTION_ARGS)
{
	uint8		charbuf[LEN_WITH_TAIL(Vector8)];
	const int	len_with_tail = LEN_WITH_TAIL(Vector8);

	for (int i = 0; i < len_with_tail; i++)
		charbuf[i] = (uint8) rand();

	qsort(charbuf, len_with_tail, sizeof(uint8), cmp);

	test_lsearch8_ge_internal(charbuf, 0);
	test_lsearch8_ge_internal(charbuf, 1);
	test_lsearch8_ge_internal(charbuf, 0x7F);
	test_lsearch8_ge_internal(charbuf, 0x80);
	test_lsearch8_ge_internal(charbuf, 0x81);
	test_lsearch8_ge_internal(charbuf, 0xFD);
	test_lsearch8_ge_internal(charbuf, 0xFE);
	test_lsearch8_ge_internal(charbuf, 0xFF);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_lfind32);
Datum
test_lfind32(PG_FUNCTION_ARGS)
{
#define TEST_ARRAY_SIZE 135
	uint32		test_array[TEST_ARRAY_SIZE] = {0};

	test_array[8] = 1;
	test_array[64] = 2;
	test_array[TEST_ARRAY_SIZE - 1] = 3;

	if (pg_lfind32(1, test_array, 4))
		elog(ERROR, "pg_lfind32() found nonexistent element");
	if (!pg_lfind32(1, test_array, TEST_ARRAY_SIZE))
		elog(ERROR, "pg_lfind32() did not find existing element");

	if (pg_lfind32(2, test_array, 32))
		elog(ERROR, "pg_lfind32() found nonexistent element");
	if (!pg_lfind32(2, test_array, TEST_ARRAY_SIZE))
		elog(ERROR, "pg_lfind32() did not find existing element");

	if (pg_lfind32(3, test_array, 96))
		elog(ERROR, "pg_lfind32() found nonexistent element");
	if (!pg_lfind32(3, test_array, TEST_ARRAY_SIZE))
		elog(ERROR, "pg_lfind32() did not find existing element");

	if (pg_lfind32(4, test_array, TEST_ARRAY_SIZE))
		elog(ERROR, "pg_lfind32() found nonexistent element");

	PG_RETURN_VOID();
}
