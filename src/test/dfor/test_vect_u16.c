/*
 * test_vect_u16.c
 */

#include "libtap/tap.h"
#include "lib/vect_u16.h"

int
main(void)
{
	plan(35);

	printf("========================================\n");
	printf("Test INIT AND CLEAR VECTOR\n");
	{
		vect_u16_t v;
		size_t capacity = 10;
		cmp_ok(0, "==", vect_u16_init(&v, capacity, NULL),
			   "Result of vect_init is 0");

		cmp_ok(v.cap, "==", capacity,
			   "Vectors capacity is equal to requested one");
		cmp_ok(v.cnt, "==", 0, "No members in vector");
		ok(v.m != NULL, "Array for members is reserved");
		cmp_ok(v.mem_is_outer, "==", false, "Vector does not use outer memory");

		vect_u16_clear(&v);

		cmp_ok(v.cap, "==", 0, "Vectors capacity is 0 after cleanup");
		cmp_ok(v.cnt, "==", 0, "No members in vector after cleanup");
		ok(v.m == NULL, "Array for members is absent");
		cmp_ok(v.mem_is_outer, "==", false, "Vector does not use outer memory");
	}
	printf("Test INIT AND CLEAR VECTOR PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test INIT AND FILL VECTOR\n");
	{
		vect_u16_t v;
		size_t capacity = 10;
		const uint16_t vals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		cmp_ok(0, "==", vect_u16_init(&v, capacity, NULL),
			   "Result of vect_init is 0");
		cmp_ok(0, "==", vect_u16_fill(&v, capacity, vals),
			   "Result of vect_fill is 0");

		cmp_ok(v.cap, "==", capacity,
			   "Vectors capacity is equal to requested one");
		ok(v.m != NULL, "Array for members is reserved");
		cmp_ok(v.cnt, "==", capacity, "Members are in vector.");
		{
			int equal = 0;
			for (size_t i = 0; i < capacity; i++) {
				if (v.m[i] == i)
					equal = equal + 1;
				else
					break;
			}
			cmp_ok(equal, "==", 10, "Members are correct");
		}
		vect_u16_clear(&v);
	}
	printf("Test INIT AND FILL VECTOR PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test INIT AND FILL VECTOR with zero capcaity\n");
	{
		vect_u16_t v;
		size_t capacity = 0;

		cmp_ok(0, "==", vect_u16_init(&v, capacity, NULL),
			   "Result of vect_init is 0");
		cmp_ok(0, "==", vect_u16_fill(&v, capacity, NULL),
			   "Result of vect_fill is 0");

		cmp_ok(v.cap, "==", 0, "Vector's capacity is zero");
		ok(v.m == NULL,
		   "Pointer to members is NULL (array for members is not reserved)");
		ok(v.cnt == 0, "Counter of members is zero.");
		vect_u16_clear(&v);
	}
	{
		vect_u16_t v;
		size_t capacity = 10;
		const uint16_t vals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

		cmp_ok(0, "==", vect_u16_init(&v, 0, NULL), "Result of vect_init is 0");
		cmp_ok(0, "==", vect_u16_fill(&v, capacity, vals),
			   "Result of vect_fill is 0");

		cmp_ok(v.cap, "==", capacity,
			   "Vector's capacity is not zero after filling");
		ok(v.m != NULL,
		   "Pointer to members is not NULL fater filling (array for members has been reserved)");
		ok(v.cnt == capacity, "Counter of members is not zero after filling.");
		vect_u16_clear(&v);
	}
	printf("Test INIT AND FILL VECTOR with zero capcaity is finished\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test COMPARE VECTORS\n");
	{
		vect_u16_t a, b, c, d;

		uint16_t avals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		uint16_t bvals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		uint16_t cvals[] = { 1, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		uint16_t dvals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };

		vect_u16_init(&a, 0, NULL);
		vect_u16_init(&b, 0, NULL);
		vect_u16_init(&c, 0, NULL);
		vect_u16_init(&d, 0, NULL);

		vect_u16_fill(&a, 10, avals);
		vect_u16_fill(&b, 10, bvals);
		vect_u16_fill(&c, 10, cvals);
		vect_u16_fill(&d, 9, dvals);

		cmp_ok(0, "==", vect_u16_compare(&a, &b), "Vectors are equal");
		cmp_ok(-1, "==", vect_u16_compare(&a, &c),
			   "Vectors are not equal because of value of members");
		cmp_ok(-1, "==", vect_u16_compare(&a, &d),
			   "Vectors are not equal because of number of members");
	}
	printf("Test COMPARE VECTORS is finished. \n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test VECTOR WITH OUTER MEMORY\n");
	{
#define VECT_CAP 100
		vect_u16_t vect;
		uint16_t buf[VECT_CAP]; /* uint16_t is the item's type */
		vect_u16_init(&vect, VECT_CAP, buf);
		cmp_ok(
			vect.cap, "==", VECT_CAP,
			"Initialisation of vector having external memory resulted in proper capacity.");
		cmp_ok(
			vect.cnt, "==", 0,
			"Initialisation of vector having external memory resulted in proper number of items.");
		ok(((void *)vect.m == (void *)buf),
		   "Initialisation of vector having external memory set buf to vect->m.");
		ok(vect.mem_is_outer,
		   "Initialisation of vector having external memory set mem_is_outer flag.");

		for (size_t i = 0; i < VECT_CAP; i++)
		{
			if (vect_u16_append(&vect, i) != 0)
				fail(
					"ERROR: New value can't be appended into vector having external memory.");
		}
		pass(
			"All values have been appended into vector having external memory.");

		cmp_ok(vect.cnt, "==", VECT_CAP, "Vector is full.");
		cmp_ok(vect_u16_append(&vect, VECT_CAP), "==", -1,
			   "Once vector is full, extra item can't be appended.");
	}
	printf("Test VECTOR WITH OUTER MEMORY is finished\n");
	printf("========================================\n");

	done_testing();
}
