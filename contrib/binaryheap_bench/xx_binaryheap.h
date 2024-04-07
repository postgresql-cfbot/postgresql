/*
 * xx_binaryheap.h
 *
 * A simple binary heap implementation
 *
 * Portions Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * src/include/lib/xx_binaryheap.h
 */

#ifndef XX_BINARYHEAP_H
#define XX_BINARYHEAP_H

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
typedef int (*xx_binaryheap_comparator) (bh_node_type a, bh_node_type b, void *arg);

typedef void (*xx_binaryheap_update_index_fn) (bh_node_type node, int new_element_index);

/*
 * xx_binaryheap
 *
 *		bh_size			how many nodes are currently in "nodes"
 *		bh_space		how many nodes can be stored in "nodes"
 *		bh_has_heap_property	no unordered operations since last heap build
 *		bh_compare		comparison function to define the heap property
 *		bh_arg			user data for comparison function
 *		bh_nodes		variable-length array of "space" nodes
 */
typedef struct xx_binaryheap
{
	int			bh_size;
	int			bh_space;
	bool		bh_has_heap_property;	/* debugging cross-check */
	xx_binaryheap_comparator bh_compare;
	xx_binaryheap_update_index_fn	bh_update_index;
	void	   *bh_arg;
	bh_node_type *bh_nodes;
} xx_binaryheap;

extern xx_binaryheap *xx_binaryheap_allocate(int num_nodes,
											 xx_binaryheap_comparator compare,
											 void *arg,
											 xx_binaryheap_update_index_fn update_index);
extern void xx_binaryheap_reset(xx_binaryheap *heap);
extern void xx_binaryheap_free(xx_binaryheap *heap);
extern void xx_binaryheap_add_unordered(xx_binaryheap *heap, bh_node_type d);
extern void xx_binaryheap_build(xx_binaryheap *heap);
extern void xx_binaryheap_add(xx_binaryheap *heap, bh_node_type d);
extern bh_node_type xx_binaryheap_first(xx_binaryheap *heap);
extern bh_node_type xx_binaryheap_remove_first(xx_binaryheap *heap);
extern void xx_binaryheap_remove_node(xx_binaryheap *heap, int n);
extern void xx_binaryheap_replace_first(xx_binaryheap *heap, bh_node_type d);
extern void xx_binaryheap_update_up(xx_binaryheap *heap, int index);
extern void xx_binaryheap_update_down(xx_binaryheap *heap, int index);

#define xx_binaryheap_empty(h)			((h)->bh_size == 0)
#define xx_binaryheap_size(h)			((h)->bh_size)
#define xx_binaryheap_get_node(h, n)	((h)->bh_nodes[n])
#define xx_binaryheap_indexed(h)		((h)->bh_update_index != NULL)

#endif							/* XX_BINARYHEAP_H */
