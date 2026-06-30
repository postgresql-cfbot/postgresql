
/*
 * test.h
 */
#ifndef _TEST_H_
#define _TEST_H_

#include <inttypes.h>
#include <stdio.h>

static inline void
test_print_u8_array(size_t cnt, uint8_t arr[], const char *name)
{
	printf("%s(hex): { ", name);
	for (size_t j = 0; j < cnt - 1; j++)
		printf("%02" PRIx8 ", ", arr[j]);

	printf("%02" PRIx8 " }\n", arr[cnt - 1]);
}

static inline void
test_print_u16_array(size_t cnt, uint16_t arr[], const char *name)
{
	printf("%s(hex): { ", name);
	for (size_t j = 0; j < cnt - 1; j++)
		printf("%04" PRIx16 ", ", arr[j]);

	printf("%04" PRIx16 "}\n", arr[cnt - 1]);
}

#endif /* _TEST_H_ */