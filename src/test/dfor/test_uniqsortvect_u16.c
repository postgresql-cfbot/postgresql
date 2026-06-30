/*
 * test_uniqsortvect.c
 */

#include "lib/vect_u16.h"
#include "libtap/tap.h"

int test_usv(size_t src_cnt, uint16_t *src_in, size_t expected_cnt,
			 uint16_t *expected_in);

int
test_usv(size_t src_cnt, uint16_t *src_in, size_t expected_cnt,
		 uint16_t *expected_in)
{
	int result = -1;
	vect_u16_t src;
	vect_u16_t expected;
	uniqsortvect_u16_t x;

	vect_u16_init(&src, src_cnt, NULL);
	vect_u16_fill(&src, src_cnt, src_in);

	vect_u16_init(&expected, 0, NULL);
	vect_u16_fill(&expected, expected_cnt, expected_in);

	vect_u16_init(&x, 0, NULL);

	for (size_t i = 0; i < src_cnt; i++)
		usv_u16_insert(&x, src.m[i]);

	result = vect_u16_compare(&x, &expected);

	vect_u16_clear(&x);
	vect_u16_clear(&expected);
	vect_u16_clear(&src);
	return result;
}

int
main(void)
{
	plan(56);

	printf("========================================\n");
	printf("Test CREATE AND DESTROY SORTED UNIQUE VECTOR (EMPTY)\n");
	{
		uniqsortvect_u16_t usv;
		size_t capacity = 10;
		vect_u16_init(&usv, capacity, NULL);
		cmp_ok(usv.cap, "==", capacity,
			   "Vectors capacity is equal to requested one");
		cmp_ok(usv.cnt, "==", 0, "No members in vector");
		ok(usv.m != NULL, "Array for members is reserved");

		vect_u16_clear(&usv);
	}
	printf("Test CREATE AND DESTROY SORTED UNIQUE VECTOR (EMPTY) PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test SEARCH IN UNIQUE SORT VECT\n");
	{
		uniqsortvect_u16_t usv;
		usv_srch_res_t srch;
		uint16_t input[4] = { 5, 10, 20, 30 };

		vect_u16_init(&usv, 0, NULL);

		for (size_t i = 0; i < 4; i++)
			usv_u16_insert(&usv, input[i]);

		srch = usv_u16_search(&usv, 0);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_SMALLEST,
			   "Not found, smallest");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 1);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_SMALLEST,
			   "Not found, smallest");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 5);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 6);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND, "Not found");
		cmp_ok(srch.pos, "==", 1, "Pos =1");

		srch = usv_u16_search(&usv, 10);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 1, "Pos =1");

		srch = usv_u16_search(&usv, 15);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND, "Not found");
		cmp_ok(srch.pos, "==", 2, "Pos =2");

		srch = usv_u16_search(&usv, 20);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 2, "Pos =2");

		srch = usv_u16_search(&usv, 25);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND, "Not found");
		cmp_ok(srch.pos, "==", 3, "Pos =3");

		srch = usv_u16_search(&usv, 30);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 3, "Pos =2");

		srch = usv_u16_search(&usv, 45);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_LARGEST, "Not found, largest");
		cmp_ok(srch.pos, "==", 3, "Pos =3");

		vect_u16_clear(&usv);
	}
	{
		uniqsortvect_u16_t usv;
		usv_srch_res_t srch;
		uint16_t input[3] = { 5, 10, 20 };
		uint16_t buf[3]; /* overindulge in testing the outer memory vector */

		vect_u16_init(&usv, 3, buf);

		for (size_t i = 0; i < 3; i++)
			usv_u16_insert(&usv, input[i]);

		srch = usv_u16_search(&usv, 0);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_SMALLEST,
			   "Not found, smallest");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 5);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 6);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND, "Not found");
		cmp_ok(srch.pos, "==", 1, "Pos =1");

		srch = usv_u16_search(&usv, 10);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 1, "Pos =1");

		srch = usv_u16_search(&usv, 15);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND, "Not found");
		cmp_ok(srch.pos, "==", 2, "Pos =2");

		/*
		 * When scopes l and g are neighbours (g-l=1) but
		 * val==m[g] instead of val==m[l].
		 */
		srch = usv_u16_search(&usv, 20);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 2, "Pos =2");

		srch = usv_u16_search(&usv, 21);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_LARGEST, "Not found, largest");
		cmp_ok(srch.pos, "==", 2, "Pos =2");

		vect_u16_clear(&usv);
	}
	{ /* single member*/

		uniqsortvect_u16_t usv;
		usv_srch_res_t srch;

		vect_u16_init(&usv, 3, NULL);

		usv_u16_insert(&usv, 5); /* The only item in list is 5 */

		srch = usv_u16_search(&usv, 0);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_SMALLEST,
			   "Not found, smallest");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 5);
		cmp_ok(srch.st, "==", USV_SRCH_FOUND, "Found");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 6);
		cmp_ok(srch.st, "==", USV_SRCH_NOT_FOUND_LARGEST, "Not found, largest");
		cmp_ok(srch.pos, "==", 0, "Pos =2");

		vect_u16_clear(&usv);
	}
	{ /* empty vector*/

		uniqsortvect_u16_t usv;
		usv_srch_res_t srch;

		vect_u16_init(&usv, 1, NULL);

		srch = usv_u16_search(&usv, 0);
		cmp_ok(srch.st, "==", USV_SRCH_EMPTY, "Vector is empty.");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 4);
		cmp_ok(srch.st, "==", USV_SRCH_EMPTY, "Vector is empty.");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 5);
		cmp_ok(srch.st, "==", USV_SRCH_EMPTY, "Vector is empty.");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		srch = usv_u16_search(&usv, 6);
		cmp_ok(srch.st, "==", USV_SRCH_EMPTY, "Vector is empty.");
		cmp_ok(srch.pos, "==", 0, "Pos =0");

		vect_u16_clear(&usv);
	}
	printf("Test SEARCH IN UNIQUE SORT VECT PASSED.\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test ERROR ON NO VECTOR FOR SEARCHING\n");
	{
		usv_srch_res_t srch;
		srch = usv_u16_search(NULL, 0);
		cmp_ok(srch.st, "==", USV_SRCH_ERROR,
			   "Error: no vector (empty pointer on vectror).");
	}
	{
		uniqsortvect_u16_t usv;
		usv_srch_res_t srch;
		vect_u16_init(&usv, 0, NULL);

		srch = usv_u16_search(&usv, 0);
		cmp_ok(srch.st, "==", USV_SRCH_EMPTY,
			   "Search in empty vector is not an error.");

		vect_u16_clear(&usv);
	}
	printf("Test ERROR ON NO VECTOR FOR SEARCHING PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test CREATED VECTORS ARE SORTED AND UNIQUE\n");
	{
		cmp_ok(
			0, "==",
			test_usv(10, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, // src
					 10,
					 (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }), // expected
			"Unique sorted vector remains the same.");

		cmp_ok(0, "==",
			   test_usv(10,
						(uint16_t[]) { 0, 1, 2, 3, 4, 5, 3, 7, 5, 9 }, // src
						8, (uint16_t[]) { 0, 1, 2, 3, 4, 5, 7, 9 }), // expected
			   "Duplicates are removed.");

		cmp_ok(
			0, "==",
			test_usv(10, (uint16_t[]) { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 }, // src
					 10,
					 (uint16_t[]) { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }), // expected
			"Unsorted became sorted.");
	}
	printf("Test CREATED VECTORS ARE SORTED AND UNIQUE PASSED\n");
	printf("========================================\n\n");

	done_testing();
}
