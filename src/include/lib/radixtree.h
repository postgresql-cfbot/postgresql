/*-------------------------------------------------------------------------
 *
 * radixtree.h
 *	  Interface for radix tree.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/radixtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RADIXTREE_H
#define RADIXTREE_H

#include "postgres.h"
#include "utils/dsa.h"

#define RT_DEBUG 1

typedef struct radix_tree radix_tree;
typedef struct rt_iter rt_iter;
typedef dsa_pointer rt_handle;

extern radix_tree *rt_create(MemoryContext ctx, dsa_area *dsa);
extern void rt_free(radix_tree *tree);
extern bool rt_search(radix_tree *tree, uint64 key, uint64 *val_p);
extern bool rt_set(radix_tree *tree, uint64 key, uint64 val);
extern rt_iter *rt_begin_iterate(radix_tree *tree);

extern rt_handle rt_get_handle(radix_tree *tree);
extern radix_tree *rt_attach(dsa_area *dsa, dsa_pointer dp);
extern void rt_detach(radix_tree *tree);

extern bool rt_iterate_next(rt_iter *iter, uint64 *key_p, uint64 *value_p);
extern void rt_end_iterate(rt_iter *iter);
extern bool rt_delete(radix_tree *tree, uint64 key);

extern uint64 rt_memory_usage(radix_tree *tree);
extern uint64 rt_num_entries(radix_tree *tree);

#ifdef RT_DEBUG
extern void rt_dump(radix_tree *tree);
extern void rt_dump_search(radix_tree *tree, uint64 key);
extern void rt_stats(radix_tree *tree);
#endif

#endif							/* RADIXTREE_H */
