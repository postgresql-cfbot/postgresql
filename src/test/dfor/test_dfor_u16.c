/*
 * test_dfor.c
 */

#include "lib/bitpack_u16.h"
#include "lib/dfor_u16.h"
#include "lib/vect_u16.h"
#include "libtap/tap.h"
#include "test.h"

void test_delta_calculation(size_t cnt, uint16_t inArr[], size_t cntDelta,
							uint16_t marDeltasExpected[], size_t cntWidth,
							uint16_t marWidthsExpected[], size_t cntStat,
							uint16_t marWidthsStatExpected[],
							bool deltaSinednessRequested,
							bool deltaSinednessExpected);

void test_calc_exceptions(size_t numDeltas, size_t numWidths,
						  uint16_t marWidths[], size_t numCounts,
						  uint16_t marCounts[], size_t szAwaitedWidth,
						  size_t cntAwaitedExcCount);

void test_dfor(size_t cnt, uint16_t arr[], excalg_t isExcUsage,
			   size_t widDeltaAwaited, size_t cntExcCntAwaited,
			   size_t widExcWidAwaited, size_t widExcPosWidAwaited,
			   size_t cntBitsCountAwaited, size_t cntByteCountAwaited,
			   float flMinRatioAwaited, uint8_t u8arPackAwaited[]);

void
test_delta_calculation(size_t cnt, uint16_t inArr[],
					   size_t cntDelta, uint16_t marDeltasExpected[],
					   size_t cntWidth, uint16_t marWidthsExpected[],
					   size_t cntStat, uint16_t marWidthsStatExpected[],
					   bool deltaSignednessRequested,
					   bool deltaSignednessExpected)
{
	vect_u16_t vDeltas;
	vect_u16_t vWidthCounters;
	uniqsortvect_u16_t usvDeltaWidths;
	vect_u16_t awaited;
	int res;
	bool deltaSignedness;

	printf("------------------------------------------------\n");
	printf("Test\n");
	printf("------------------------------------------------\n");

	printf("  inArr:");
	for (size_t i = 0; i < cnt; i++)
		printf(" %u", (uint32_t)inArr[i]);
	printf("\n");

	vect_u16_init(&vDeltas, 0, NULL);
	vect_u16_init(&usvDeltaWidths, 0, NULL);
	vect_u16_init(&vWidthCounters, 0, NULL);

	deltaSignedness = deltaSignednessRequested;
	/* Tested function */
	res = dfor_u16_calc_deltas(cnt, inArr, &vDeltas, &usvDeltaWidths,
							   &vWidthCounters, &deltaSignedness);
	cmp_ok(res, "==", 0);

	printf("  Delta expected:");
	for (size_t i = 0; i < cntDelta; i++)
		printf(" %u", (uint32_t)marDeltasExpected[i]);
	printf("\n");

	printf("  Delta fact:    ");
	vect_u16_print(&vDeltas);

	cmp_ok(vDeltas.cnt, "==", cnt, "The Delta count is OK.");
	vect_u16_init(&awaited, 0, NULL);
	vect_u16_fill(&awaited, cnt, marDeltasExpected);
	cmp_ok(deltaSignedness, "==", deltaSignednessExpected, "The signedness result is OK.");
	cmp_ok(vect_u16_compare(&vDeltas, &awaited), "==", 0,
		   "All deltas are calculated properly");
	vect_u16_clear(&awaited);

	printf("  Width expected:");
	for (size_t i = 0; i < cntWidth; i++)
		printf(" %u", (uint32_t)marWidthsExpected[i]);
	printf("\n");

	printf("  Width fact:    ");
	vect_u16_print(&usvDeltaWidths);

	cmp_ok(usvDeltaWidths.cnt, "==", cntWidth, "The Width count is OK.");

	/* don't really need initialisation after vect_clean having been done
	 * above*/
	/* vect_u16_init(&awaited, 0, NULL); */

	vect_u16_fill(&awaited, cntWidth, marWidthsExpected);
	cmp_ok(vect_u16_compare(&usvDeltaWidths, &awaited), "==", 0,
		   "All delta widths is OK.");
	vect_u16_clear(&awaited);

	printf("  Deltas statistics expected:");
	for (size_t i = 0; i < cntStat; i++)
		printf(" %u", (uint32_t)marWidthsStatExpected[i]);
	printf("\n");

	printf("  Deltas statistics statistics fact:    ");
	vect_u16_print(&vWidthCounters);

	cmp_ok(
		usvDeltaWidths.cnt, "==", vWidthCounters.cnt,
		"The count of statistics of widths is equal to the count of widths.");

	/* don't really need initialisation after vect_clean has been done
	 * above*/
	vect_u16_fill(&awaited, cntStat, marWidthsStatExpected);
	cmp_ok(vect_u16_compare(&vWidthCounters, &awaited), "==", 0,
		   "Width statistics is OK.");
	vect_u16_clear(&awaited);

	vect_u16_clear(&vDeltas);
	vect_u16_clear(&usvDeltaWidths);
	vect_u16_clear(&vWidthCounters);
}

void
test_calc_exceptions(size_t numDeltas, size_t numWidths, uint16_t marWidths[],
					 size_t numCounts, uint16_t marCounts[],
					 size_t szAwaitedWidth, size_t cntAwaitedExcCount)
{
	int res;
	vect_u16_t vWidthCounters;
	uniqsortvect_u16_t usvDeltaWidths;
	size_t width, cntExceptions;

	vect_u16_init(&usvDeltaWidths, 0, NULL);
	vect_u16_fill(&usvDeltaWidths, numWidths, marWidths);

	vect_u16_init(&vWidthCounters, 0, NULL);
	vect_u16_fill(&vWidthCounters, numCounts, marCounts);

	res = dfor_u16_calc_width(numDeltas, &usvDeltaWidths, &vWidthCounters,
							  &width, &cntExceptions);
	cmp_ok(res, "==", 0);
	cmp_ok(width, "==", szAwaitedWidth, "Width is OK.");
	cmp_ok(cntExceptions, "==", cntAwaitedExcCount, "Exceptions num is OK");

	vect_u16_clear(&usvDeltaWidths);
	vect_u16_clear(&vWidthCounters);
}

void
test_dfor(size_t cnt, uint16_t arr[], excalg_t isExcUsage,
		  size_t widDeltaWidthAwaited, size_t cntExcCntAwaited,
		  size_t widExcWidAwaited, size_t widExcPosWidAwaited,
		  size_t cntBitsCountAwaited, size_t cntByteCountAwaited,
		  float flMinRatioAwaited, uint8_t u8arPackAwaited[])
{
	int res;
	dfor_meta_t dfor;
	dfor_stats_t stats;
	uniqsortvect_u16_t extracted;

	res = dfor_u16_pack(cnt, arr, isExcUsage, &dfor, 0, NULL);
	cmp_ok(res, "==", 0, "dfor_pack func has processed OK.");
	cmp_ok(dfor.item_cnt, "==", cnt, "Count of deltas is OK.");
	cmp_ok(dfor.delta_wid, "==", widDeltaWidthAwaited, "Delta width is OK.");
	cmp_ok(dfor.exc_cnt, "==", cntExcCntAwaited, "Exception count is OK.");
	cmp_ok(dfor.exc_wid, "==", widExcWidAwaited, "Exception width is OK.");
	cmp_ok(dfor.exc_pos_wid, "==", widExcPosWidAwaited,
		   "Exception position width is OK.");
	ok(dfor.pack != NULL, "Pack is created (not NULL).");

	stats = dfor_u16_calc_stats(dfor);
	cmp_ok(stats.nbits, "==", cntBitsCountAwaited, "Bits count is OK.");
	cmp_ok(dfor.nbytes, "==", cntByteCountAwaited, "Bytes count is OK.");
	ok(stats.ratio > flMinRatioAwaited, "Compression ratio is OK.");

	if (u8arPackAwaited != NULL)
		ok(0 == memcmp(dfor.pack, u8arPackAwaited, cntByteCountAwaited),
		   "Pack content is OK.");
	else
		ok(0 == 0, "Pack content check is skipped.");

	test_print_u16_array(cnt, (uint16_t *)arr, "\n\nOriginal integer array");
	test_print_u8_array(dfor.nbytes, dfor.pack, "Compressed integer array");
	printf("Compression ratio:%f\n\n", stats.ratio);

	vect_u16_init(&extracted, 0, NULL);

	dfor_u16_unpack(&dfor, &extracted, 0, NULL);
	cmp_ok(extracted.cnt, "==", cnt, "Extracted count is OK");
	cmp_ok(0, "==", memcmp(arr, extracted.m, cnt),
		   "Extracted array is equal to original");

	free(dfor.pack);
	vect_u16_clear(&extracted);
}

int
main(void)
{
	plan(267);

	printf("========================================\n");
	printf("Test DELTA CALCULATION for sorted sequences \n");
	{
		/* Sorted sequences */
		test_delta_calculation(
			10, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, /* inArr */
			10,
			(uint16_t[]) { 0, 1, 1, 1, 1, 1, 1, 1, 1, 1 }, /* marDeltasExpected */
			1, (uint16_t[]) { 1 }, /* marWidthsExpected */
			1, (uint16_t[]) { 10 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			false  /* deltaSignedness expected */);

		test_delta_calculation(
			10, (uint16_t[]) { 1, 3, 5, 7, 9, 11, 13, 15, 17, 19 }, /* inArr */
			10,
			(uint16_t[]) { 1, 2, 2, 2, 2, 2, 2, 2, 2, 2 }, /* marDeltasExpected*/
			2, (uint16_t[]) { 1, 2 }, /* marWidthsExpected */
			2, (uint16_t[]) { 1, 9 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			false  /* deltaSignedness expected */);

		test_delta_calculation(
			14,
			(uint16_t[]) { 100, 200, 300, 400, 401, 402, 403, 404, 406, 408,
						   410, 412, 414, 416 }, /* inArr */
			14,
			(uint16_t[]) { 100, 100, 100, 100, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2 },
										 /* marDeltasExpected*/
			3, (uint16_t[]) { 1, 2, 7 }, /* marWidthsExpected */
			3, (uint16_t[]) { 4, 6, 4 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			false  /* deltaSignedness expected */);

		test_delta_calculation(1, (uint16_t[]) { 123 }, /* inArr */
							   1, (uint16_t[]) { 123 }, /* marDeltasExpected*/
							   1, (uint16_t[]) { 7 },	/* marWidthsExpected */
							   1,
							   (uint16_t[]) { 1 }, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   false  /* deltaSignedness expected */);

		test_delta_calculation(0, NULL, /* inArr */
							   0, NULL, /* marDeltasExpected*/
							   0, NULL, /* marWidthsExpected */
							   0, NULL, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   false  /* deltaSignedness expected */);

	printf("Test DELTA CALCULATION for sorted sequences PASSED \n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test DELTA CALCULATION for unsorted sequences and test SIGNEDNESS \n");

		/* Unsorted sequences */
		test_delta_calculation(
			10, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, /* inArr */
			10,
			(uint16_t[]) { 0x0, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2 },	/* marDeltasExpected*/
					/*BIN:  00,  10,  10,  10,  10,  10,  10,  10,  10,  10 */
					/*DEC:  +0,  +1,  +1,  +1,  +1,  +1,  +1,  +1,  +1,  +1 */
			1, (uint16_t[]) { 2 }, /* marWidthsExpected */
			1, (uint16_t[]) { 10 }, /* marWidthsStatExpected */
			true, /* deltaSignedness requested */
			true  /* deltaSignedness expected */);

		test_delta_calculation(
			10, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 7 }, /* inArr */
			10,
			(uint16_t[]) { 0,  2,  2,  2,  2,  2,  2,  2,  2,  3 },	/* marDeltasExpected*/
					/*BIN: 00, 10, 10, 10, 10, 10, 10, 10, 10, 11 */
					/*DEC: +0, +1, +1, +1, +1, +1, +1, +1, +1, -1 */
			1, (uint16_t[]) { 2 }, /* marWidthsExpected */
			1, (uint16_t[]) { 10 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			true  /* deltaSignedness expected */);

		test_delta_calculation(
			10, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 7 }, /* inArr */
			10,
			(uint16_t[]) { 0,  2,  2,  2,  2,  2,  2,  2,  2,  3 },	/* marDeltasExpected*/
					/*BIN: 00, 10, 10, 10, 10, 10, 10, 10, 10, 11 */
					/*DEC: +0, +1, +1, +1, +1, +1, +1, +1, +1, -1 */
			1, (uint16_t[]) { 2 }, /* marWidthsExpected */
			1, (uint16_t[]) { 10 }, /* marWidthsStatExpected */
			true, /* deltaSignedness requested */
			true  /* deltaSignedness expected */);

		test_delta_calculation(
			10, (uint16_t[]) { 1, 3, 5, 3, 5, 7, 9, 11, 13, 15 }, /* inArr */
			10,
			(uint16_t[]) { 0x2, 0x4, 0x4, 0x5, 0x4, 0x4, 0x4, 0x4, 0x4, 0x4 }, /* marDeltasExpected*/
					/*BIN:  10, 100, 100, 101, 100, 100, 100, 100, 100, 100 */
					/*DEC: +1,  +2,  +2,  -2,  +2,  +2,  +2,  +2,  +2,  +2 */
			2, (uint16_t[]) { 2, 3 }, /* marWidthsExpected */
			2, (uint16_t[]) { 1, 9 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			true  /* deltaSignedness expected */);
		test_delta_calculation(
			14,
			(uint16_t[]) { 100, 200, 300, 400, 401, 402, 403, 404, 406, 408,
						   410, 412, 414, 412 }, /* inArr */
			14,
			(uint16_t[]) { 200, 200, 200, 200, 2, 2, 2, 2, 4, 4, 4, 4, 4, 5 }, /* marDeltasExpected code*/
					/* DEC: +100, +100, +100, +1, +1, +1, +1, +1, +2, +2, +2, +2, +2, -2 */
			3, (uint16_t[]) { 2, 3, 8 }, /* marWidthsExpected */
			3, (uint16_t[]) { 4, 6, 4 }, /* marWidthsStatExpected */
			false, /* deltaSignedness requested */
			true  /* deltaSignedness expected */);

		test_delta_calculation(1, (uint16_t[]) { 123 }, /* inArr */
							   1, (uint16_t[]) { 246 }, /* marDeltasExpected*/
							   1, (uint16_t[]) { 8 },	/* marWidthsExpected */
							   1, (uint16_t[]) { 1 }, /* marWidthsStatExpected */
							   true, /* deltaSignedness requested */
							   true  /* deltaSignedness expected */);

		test_delta_calculation(0, NULL, /* inArr */
							   0, NULL, /* marDeltasExpected*/
							   0, NULL, /* marWidthsExpected */
							   0, NULL, /* marWidthsStatExpected */
							   true, /* deltaSignedness requested */
							   true  /* deltaSignedness expected */);

	printf("Test DELTA CALCULATION for unsorted sequences and test SIGNEDNESS PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test DELTA CALCULATION for unsorted sequences and test OVERLAPPING trick \n");

		test_delta_calculation(1, (uint16_t[]) { 33000 }, /* inArr */
							   1, (uint16_t[]) { 33000 }, /* marDeltasExpected*/
							   1, (uint16_t[]) { 16 },	/* marWidthsExpected */
							   1, (uint16_t[]) { 1 }, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   false  /* deltaSignedness expected */);

		test_delta_calculation(1, (uint16_t[]) { 33000 }, /* inArr */
								/* 65535 - 33000 + 1 = 32536 = 0x7F18
								   -32536 = (0x7F18 << 1) + 1 = 0xFE31 = 65073 */
							   1, (uint16_t[]) { 0xFE31 }, /* marDeltasExpected*/
							   1, (uint16_t[]) { 16 },	/* marWidthsExpected */
							   1, (uint16_t[]) { 1 }, /* marWidthsStatExpected */
							   true, /* deltaSignedness requested */
							   true  /* deltaSignedness expected */);

		test_delta_calculation(2, (uint16_t[]) { 0, 33000 }, /* inArr */
							   2, (uint16_t[]) { 0, 33000 }, /* marDeltasExpected*/
							   2, (uint16_t[]) { 1, 16 },	/* marWidthsExpected */
							   2, (uint16_t[]) { 1, 1 }, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   false  /* deltaSignedness expected */);

		test_delta_calculation(2, (uint16_t[]) { 33000, 0 }, /* inArr */
								/* 65535 - 33000 + 1 = 32536 = 0x7F18;
								   (0x7F18 << 1) = 0xFE30 = 65072;
								   -32536 = (0x7F18 << 1) | 1 = 0xFE31 = 65073 */
							   2, (uint16_t[]) { 0xFE31, 0xFE30 }, /* marDeltasExpected*/
							   1, (uint16_t[]) { 16 },	/* marWidthsExpected */
							   1, (uint16_t[]) { 2 }, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   true  /* deltaSignedness expected */);

		test_delta_calculation(16, (uint16_t[]) { 0, 1, 0, 3, 4, 5, 6, 7,
												  8, 9, 10, 11, 12, 13, 14, 65534 }, /* inArr */
							   16, (uint16_t[]) { 0x0, 0x2, 0x3, 0x6, 0x2, 0x2, 0x2, 0x2,
												  0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x21 }, /* marDeltasExpected*/
							   3, (uint16_t[]) {  2, 3, 6 }, /* marWidthsExpected */
							   3, (uint16_t[]) { 14, 1, 1 }, /* marWidthsStatExpected */
							   false, /* deltaSignedness requested */
							   true  /* deltaSignedness expected */);

	printf("Test DELTA CALCULATION for unsorted sequences and test OVERLAPPING trick PASSED \n");
	printf("========================================\n\n");
	}

	printf("========================================\n");
	printf("Test EXCEPTIONS CALCULATION\n");

	test_calc_exceptions(14, 3, (uint16_t[]) { 1, 2, 7 }, /* widths of deltas*/
						 3, (uint16_t[]) { 4, 6, 4 },	  /* statistics on widths */
						 7,	 /* width of short deltas */
						 0); /* number of exceptions*/

	test_calc_exceptions(14, 3, (uint16_t[]) { 1, 2, 7 }, /* widths */
						 3, (uint16_t[]) { 6, 7, 1 },	  /* stat */
						 2, 1); /* short deltas width, exceptions count*/

	test_calc_exceptions(40, 3, (uint16_t[]) { 5, 6, 12 }, /* widths */
						 3, (uint16_t[]) { 36, 2, 2 },	   /* stat */
						 5, 4); /* short deltas width, exceptions count*/

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 36, 1, 1, 2 }, 5, 4);

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 35, 1, 2, 2 }, 6, 4);

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 34, 1, 2, 3 }, 7, 3);

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 1, 34, 2, 3 }, 7, 3);

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 1, 33, 2, 4 }, 7, 4);

	test_calc_exceptions(40, 4, (uint16_t[]) { 5, 6, 7, 12 }, 4,
						 (uint16_t[]) { 1, 32, 2, 5 }, 12, 0);

	printf("Test EXCEPTIONS CALCULATION PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test DELTA FRAME OF REFERENCES PACKING\n");

	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 15 }, /* cnt, array */
			  DFOR_EXC_DONT_USE,   /* flag on use of exceptions */
			  1,				   /* awaited width of short deltas */
			  0,				   /* awaited count of exceptions */
			  0,				   /* awaited exception width*/
			  0,				   /* awaited exception position width*/
			  16,				   /* awaited bits count */
			  2,				   /* cntByteCountAwaited */
			  15.99, /* awaited ratio min value (ratio >= 15.99) */
			  (uint8_t[]) { 0xFE, 0xFF });

	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 15 }, /* cnt, array */
			  DFOR_EXC_USE, /* flag on use of exceptions */
			  1,			/* awaited width of short deltas */
			  0,			/* awaited count of exceptions */
			  0,			/* awaited exception width*/
			  0,			/* awaited exception position width*/
			  16,			/* awaited bits count */
			  2,			/* cntByteCountAwaited */
			  15.99,		/* awaited ratio min value (ratio >= 15.99) */
			  (uint8_t[]) { 0xFE, 0xFF });

	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 1023 }, /* cnt, array */
			  DFOR_EXC_DONT_USE,	 /* flag on use of exceptions */
			  10,					 /* awaited width of short deltas */
			  0,					 /* awaited count of exceptions */
			  0,					 /* awaited exception width*/
			  0,					 /* awaited exception position width*/
			  10 * 16,				 /* awaited bits count */
			  20,					 /* awaited bytes count */
			  1.5,					 /* awaited ratio min value */
			  (uint8_t[]) { 0x00, 0x04, 0x10, 0x40, 0x00, 0x01, 0x04,
							0x10, 0x40, 0x00, 0x01, 0x04, 0x10, 0x40,
							0x00, 0x01, 0x04, 0x10, 0x40, 0xFC });

	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 1023 }, /* cnt, array */
			  DFOR_EXC_USE,			 /* flag on use of exceptions */
			  1,					 /* awaited width of short deltas */
			  1,					 /* awaited count of exceptions */
			  9,					 /* awaited exception width*/
			  4,					 /* awaited exception position width*/
			  16 * 1 + 9 + 4,		 /* awaited bits count */
			  4,					 /* awaited bytes count */
			  7.99,					 /* awaited ratio min value */
			  (uint8_t[]) { 0xFE, 0xFF, 0xF8, 0x1F });

	test_dfor(30, /* cnt */
					  (uint16_t[]) { 0, 1, 2, 3, 4,
									 5, 6, 7, 8, 9,
  /* delta=2, pos=10, deltapos=10 */ 11, 12, 13, 14, 15,
									 16, 17, 18, 19, 20,
									 21, 22, 23, 24, 25,
  /* delta=3, pos=25, deltapos=15 */ 28, 29, 30, 31,
  /* delta=3, pos=29, deltapos=4 */	 34 },  				/* array */
			  DFOR_EXC_USE,			  /* flag on use of exceptions */
			  1,					  /* awaited width of short deltas */
			  3,					  /* awaited count of exceptions */
			  1,					  /* awaited exception width*/
			  4,					  /* awaited exception position width*/
			  30 * 1 + 3 * 1 + 3 * 4, /* awaited bits count */
			  6,					  /* awaited bytes count */
			  9.99,					  /* awaited ratio min value */
			  (uint8_t[]) { 0xFE, 0xFB, 0xFF, 0xFF, 0xF5, 0x09 });

	/* Unsorted sequences */
	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 13 }, /* cnt, array */
			  DFOR_EXC_DONT_USE,   /* flag on use of exceptions */
			  2,				   /* awaited width of short deltas */
			  0,				   /* awaited count of exceptions */
			  0,				   /* awaited exception width*/
			  0,				   /* awaited exception position width*/
			  32,				   /* awaited bits count */
			  4,				   /* cntByteCountAwaited */
			  7.99, /* awaited ratio min value (ratio >= 7.99) */
			  (uint8_t[]) { 0xA8, 0xAA, 0xAA, 0xEA });

	/* Test the decoding algorithm for the overlap trick */
	test_dfor(16,
			  (uint16_t[]) { 0, 1, 65535, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
							 15 }, /* cnt, array */
			  DFOR_EXC_DONT_USE,   /* flag on use of exceptions */
			  3,				   /* awaited width of short deltas */
			  0,				   /* awaited count of exceptions */
			  0,				   /* awaited exception width*/
			  0,				   /* awaited exception position width*/
			  48,				   /* awaited bits count */
			  6,				   /* cntByteCountAwaited */
			  4, /* awaited ratio min value */
			  (uint8_t[]) { 0x50, 0x4d, 0x49, 0x92, 0x24, 0x49 });

	/* The next test does not result in activation of the overlap trick since
	the sequence is sorted. */
	test_dfor(16,
			  (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 65535 }, /* cnt, array */
			  DFOR_EXC_DONT_USE,   /* flag on use of exceptions */
			  16,				   /* awaited width of short deltas */
			  0,				   /* awaited count of exceptions */
			  0,				   /* awaited exception width*/
			  0,				   /* awaited exception position width*/
			  256,				   /* awaited bits count */
			  32,				   /* cntByteCountAwaited */
			  0.9999,			   /* awaited ratio min value = 1 */
			  (uint8_t[]) { 00, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 01, 00, 0xf1, 0xff });

	printf("Test DELTA FRAME OF REFERENCES PACKING PASSED\n");
	printf("========================================\n\n");

	done_testing();
}
