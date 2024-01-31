/*--------------------------------------------------------------------------
 *
 * test_misc.c
 *
 * Copyright (c) 2022-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_dsa/test_misc.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"

#define BIT_ADD 0
#define BIT_DEL 1

static void compare_bms_bs(Bitmapset **bms, Bitset *bs, int member, int op);


PG_MODULE_MAGIC;

/* Test basic DSA functionality */
PG_FUNCTION_INFO_V1(test_bitset);

Datum
test_bitset(PG_FUNCTION_ARGS)
{
	Bitset	   *bs;
	Bitset	   *bs2;
	char	   *str1,
			   *str2,
			   *empty_str;
	Bitmapset  *bms = NULL;
	int			i;

	empty_str = bmsToString(NULL);

	/* size = 0 */
	bs = bitset_init(0);
	Assert(bs == NULL);
	bitset_clear(bs);
	Assert(bitset_is_empty(bs));
	/* bitset_add_member(bs, 0); // crash. */
	/* bitset_del_member(bs, 0); // crash. */
	Assert(!bitset_is_member(0, bs));
	Assert(bitset_next_member(bs, -1) == -2);
	bs2 = bitset_copy(bs);
	Assert(bs2 == NULL);
	bitset_free(bs);
	bitset_free(bs2);

	/* size == 68, nword == 2 */
	bs = bitset_init(68);

	for (i = 0; i < 68; i = i + 3)
	{
		compare_bms_bs(&bms, bs, i, BIT_ADD);
	}

	for (i = 67; i >= 0; i = i - 3)
	{
		compare_bms_bs(&bms, bs, i, BIT_DEL);
	}

	bitset_clear(bs);
	str1 = bitsetToString(bs, true);
	Assert(strcmp(str1, empty_str) == 0);

	bms = bitset_to_bitmap(bs);
	str2 = bmsToString(bms);
	Assert(strcmp(str1, str2) == 0);

	bms = bitset_to_bitmap(NULL);
	Assert(strcmp(bmsToString(bms), empty_str) == 0);

	bitset_free(bs);

	PG_RETURN_VOID();
}


static void
compare_bms_bs(Bitmapset **bms, Bitset *bs, int member, int op)
{
	char	   *str1,
			   *str2,
			   *str3,
			   *str4;
	Bitmapset  *bms3;
	Bitset	   *bs4;

	if (op == BIT_ADD)
	{
		*bms = bms_add_member(*bms, member);
		bitset_add_member(bs, member);
		Assert(bms_is_member(member, *bms));
		Assert(bitset_is_member(member, bs));
	}
	else if (op == BIT_DEL)
	{
		*bms = bms_del_member(*bms, member);
		bitset_del_member(bs, member);
		Assert(!bms_is_member(member, *bms));
		Assert(!bitset_is_member(member, bs));
	}
	else
		Assert(false);

	/* compare the rest existing bit */
	str1 = bmsToString(*bms);
	str2 = bitsetToString(bs, true);
	Assert(strcmp(str1, str2) == 0);

	/* test bitset_to_bitmap */
	bms3 = bitset_to_bitmap(bs);
	str3 = bmsToString(bms3);
	Assert(strcmp(str3, str2) == 0);

	/* test bitset_copy */
	bs4 = bitset_copy(bs);
	str4 = bitsetToString(bs4, true);
	Assert(strcmp(str3, str4) == 0);

	pfree(str1);
	pfree(str2);
	pfree(str3);
	pfree(str4);
}
