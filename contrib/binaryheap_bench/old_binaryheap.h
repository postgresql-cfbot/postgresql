/*
 * old_binaryheap.h
 *
 * A simple binary heap implementation
 *
 * Portions Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * src/include/lib/old_binaryheap.h
 */

#ifndef OLD_BINARYHEAP_H
#define OLD_BINARYHEAP_H

/*
 * We provide a Datum-based API for backend code and a void *-based API for
 * frontend code (since the Datum definitions are not available to frontend
 * code).  You should typically avoid using bh_node_type directly and instead
 * use Datum or void * as appropriate.
 */
#ifdef FRONTEND
typedef void *bh_node_type;
#else
typedef Datum bh_node_type;
#endif

/*
 * For a max-heap, the comparator must return <0 iff a < b, 0 iff a == b,
 * and >0 iff a > b.  For a min-heap, the conditions are reversed.
 */
typedef int (*old_binaryheap_comparator) (bh_node_type a, bh_node_type b, void *arg);

/*
 * old_binaryheap
 *
 *		bh_size			how many nodes are currently in "nodes"
 *		bh_space		how many nodes can be stored in "nodes"
 *		bh_has_heap_property	no unordered operations since last heap build
 *		bh_compare		comparison function to define the heap property
 *		bh_arg			user data for comparison function
 *		bh_nodes		variable-length array of "space" nodes
 */
typedef struct old_binaryheap
{
	int			bh_size;
	int			bh_space;
	bool		bh_has_heap_property;	/* debugging cross-check */
	old_binaryheap_comparator bh_compare;
	void	   *bh_arg;
	bh_node_type *bh_nodes;
} old_binaryheap;

extern old_binaryheap *old_binaryheap_allocate(int num_nodes,
									   old_binaryheap_comparator compare,
									   void *arg);
extern void old_binaryheap_reset(old_binaryheap *heap);
extern void old_binaryheap_free(old_binaryheap *heap);
extern void old_binaryheap_add_unordered(old_binaryheap *heap, bh_node_type d);
extern void old_binaryheap_build(old_binaryheap *heap);
extern void old_binaryheap_add(old_binaryheap *heap, bh_node_type d);
extern bh_node_type old_binaryheap_first(old_binaryheap *heap);
extern bh_node_type old_binaryheap_remove_first(old_binaryheap *heap);
extern void old_binaryheap_remove_node(old_binaryheap *heap, int n);
extern void old_binaryheap_replace_first(old_binaryheap *heap, bh_node_type d);

#define old_binaryheap_empty(h)			((h)->bh_size == 0)
#define old_binaryheap_size(h)			((h)->bh_size)
#define old_binaryheap_get_node(h, n)	((h)->bh_nodes[n])

#endif							/* OLD_BINARYHEAP_H */
