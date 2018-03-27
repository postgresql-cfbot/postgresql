/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/lib/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BLOOMFILTER_H_
#define _BLOOMFILTER_H_

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
			 uint32 seed);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
				  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
					size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);

#endif
