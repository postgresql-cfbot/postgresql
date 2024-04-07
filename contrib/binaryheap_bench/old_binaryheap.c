/*-------------------------------------------------------------------------
 *
 * old_binaryheap.c
 *	  A simple binary heap implementation
 *
 * Portions Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/old_binaryheap.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include <math.h>

#ifdef FRONTEND
#include "common/logging.h"
#endif
#include "old_binaryheap.h"

static void sift_down(old_binaryheap *heap, int node_off);
static void sift_up(old_binaryheap *heap, int node_off);

/*
 * old_binaryheap_allocate
 *
 * Returns a pointer to a newly-allocated heap with the given initial number
 * of nodes, and with the heap property defined by the given comparator
 * function, which will be invoked with the additional argument specified by
 * 'arg'.
 */
old_binaryheap *
old_binaryheap_allocate(int num_nodes, old_binaryheap_comparator compare, void *arg)
{
	old_binaryheap *heap;

	heap = (old_binaryheap *) palloc(sizeof(old_binaryheap));
	heap->bh_space = num_nodes;
	heap->bh_compare = compare;
	heap->bh_arg = arg;

	heap->bh_size = 0;
	heap->bh_has_heap_property = true;
	heap->bh_nodes = (bh_node_type *) palloc(sizeof(bh_node_type) * num_nodes);

	return heap;
}

/*
 * old_binaryheap_reset
 *
 * Resets the heap to an empty state, losing its data content but not the
 * parameters passed at allocation.
 */
void
old_binaryheap_reset(old_binaryheap *heap)
{
	heap->bh_size = 0;
	heap->bh_has_heap_property = true;
}

/*
 * old_binaryheap_free
 *
 * Releases memory used by the given old_binaryheap.
 */
void
old_binaryheap_free(old_binaryheap *heap)
{
	pfree(heap->bh_nodes);
	pfree(heap);
}

/*
 * These utility functions return the offset of the left child, right
 * child, and parent of the node at the given index, respectively.
 *
 * The heap is represented as an array of nodes, with the root node
 * stored at index 0. The left child of node i is at index 2*i+1, and
 * the right child at 2*i+2. The parent of node i is at index (i-1)/2.
 */

static inline int
left_offset(int i)
{
	return 2 * i + 1;
}

static inline int
right_offset(int i)
{
	return 2 * i + 2;
}

static inline int
parent_offset(int i)
{
	return (i - 1) / 2;
}

/*
 * Double the space allocated for nodes.
 */
static void
enlarge_node_array(old_binaryheap *heap)
{
	heap->bh_space *= 2;
	heap->bh_nodes = repalloc(heap->bh_nodes,
							  sizeof(bh_node_type) * heap->bh_space);
}

/*
 * old_binaryheap_add_unordered
 *
 * Adds the given datum to the end of the heap's list of nodes in O(1) without
 * preserving the heap property. This is a convenience to add elements quickly
 * to a new heap. To obtain a valid heap, one must call old_binaryheap_build()
 * afterwards.
 */
void
old_binaryheap_add_unordered(old_binaryheap *heap, bh_node_type d)
{
	/* make sure enough space for a new node */
	if (heap->bh_size >= heap->bh_space)
		enlarge_node_array(heap);

	heap->bh_has_heap_property = false;
	heap->bh_nodes[heap->bh_size] = d;
	heap->bh_size++;
}

/*
 * old_binaryheap_build
 *
 * Assembles a valid heap in O(n) from the nodes added by
 * old_binaryheap_add_unordered(). Not needed otherwise.
 */
void
old_binaryheap_build(old_binaryheap *heap)
{
	int			i;

	for (i = parent_offset(heap->bh_size - 1); i >= 0; i--)
		sift_down(heap, i);
	heap->bh_has_heap_property = true;
}

/*
 * old_binaryheap_add
 *
 * Adds the given datum to the heap in O(log n) time, while preserving
 * the heap property.
 */
void
old_binaryheap_add(old_binaryheap *heap, bh_node_type d)
{
	/* make sure enough space for a new node */
	if (heap->bh_size >= heap->bh_space)
		enlarge_node_array(heap);

	heap->bh_nodes[heap->bh_size] = d;
	heap->bh_size++;
	sift_up(heap, heap->bh_size - 1);
}

/*
 * old_binaryheap_first
 *
 * Returns a pointer to the first (root, topmost) node in the heap
 * without modifying the heap. The caller must ensure that this
 * routine is not used on an empty heap. Always O(1).
 */
bh_node_type
old_binaryheap_first(old_binaryheap *heap)
{
	Assert(!old_binaryheap_empty(heap) && heap->bh_has_heap_property);
	return heap->bh_nodes[0];
}

/*
 * old_binaryheap_remove_first
 *
 * Removes the first (root, topmost) node in the heap and returns a
 * pointer to it after rebalancing the heap. The caller must ensure
 * that this routine is not used on an empty heap. O(log n) worst
 * case.
 */
bh_node_type
old_binaryheap_remove_first(old_binaryheap *heap)
{
	bh_node_type result;

	Assert(!old_binaryheap_empty(heap) && heap->bh_has_heap_property);

	/* extract the root node, which will be the result */
	result = heap->bh_nodes[0];

	/* easy if heap contains one element */
	if (heap->bh_size == 1)
	{
		heap->bh_size--;
		return result;
	}

	/*
	 * Remove the last node, placing it in the vacated root entry, and sift
	 * the new root node down to its correct position.
	 */
	heap->bh_nodes[0] = heap->bh_nodes[--heap->bh_size];
	sift_down(heap, 0);

	return result;
}

/*
 * old_binaryheap_remove_node
 *
 * Removes the nth (zero based) node from the heap.  The caller must ensure
 * that there are at least (n + 1) nodes in the heap.  O(log n) worst case.
 */
void
old_binaryheap_remove_node(old_binaryheap *heap, int n)
{
	int			cmp;

	Assert(!old_binaryheap_empty(heap) && heap->bh_has_heap_property);
	Assert(n >= 0 && n < heap->bh_size);

	/* compare last node to the one that is being removed */
	cmp = heap->bh_compare(heap->bh_nodes[--heap->bh_size],
						   heap->bh_nodes[n],
						   heap->bh_arg);

	/* remove the last node, placing it in the vacated entry */
	heap->bh_nodes[n] = heap->bh_nodes[heap->bh_size];

	/* sift as needed to preserve the heap property */
	if (cmp > 0)
		sift_up(heap, n);
	else if (cmp < 0)
		sift_down(heap, n);
}

/*
 * old_binaryheap_replace_first
 *
 * Replace the topmost element of a non-empty heap, preserving the heap
 * property.  O(1) in the best case, or O(log n) if it must fall back to
 * sifting the new node down.
 */
void
old_binaryheap_replace_first(old_binaryheap *heap, bh_node_type d)
{
	Assert(!old_binaryheap_empty(heap) && heap->bh_has_heap_property);

	heap->bh_nodes[0] = d;

	if (heap->bh_size > 1)
		sift_down(heap, 0);
}

/*
 * Sift a node up to the highest position it can hold according to the
 * comparator.
 */
static void
sift_up(old_binaryheap *heap, int node_off)
{
	bh_node_type node_val = heap->bh_nodes[node_off];

	/*
	 * Within the loop, the node_off'th array entry is a "hole" that
	 * notionally holds node_val, but we don't actually store node_val there
	 * till the end, saving some unnecessary data copying steps.
	 */
	while (node_off != 0)
	{
		int			cmp;
		int			parent_off;
		bh_node_type parent_val;

		/*
		 * If this node is smaller than its parent, the heap condition is
		 * satisfied, and we're done.
		 */
		parent_off = parent_offset(node_off);
		parent_val = heap->bh_nodes[parent_off];
		cmp = heap->bh_compare(node_val,
							   parent_val,
							   heap->bh_arg);
		if (cmp <= 0)
			break;

		/*
		 * Otherwise, swap the parent value with the hole, and go on to check
		 * the node's new parent.
		 */
		heap->bh_nodes[node_off] = parent_val;
		node_off = parent_off;
	}
	/* Re-fill the hole */
	heap->bh_nodes[node_off] = node_val;
}

/*
 * Sift a node down from its current position to satisfy the heap
 * property.
 */
static void
sift_down(old_binaryheap *heap, int node_off)
{
	bh_node_type node_val = heap->bh_nodes[node_off];

	/*
	 * Within the loop, the node_off'th array entry is a "hole" that
	 * notionally holds node_val, but we don't actually store node_val there
	 * till the end, saving some unnecessary data copying steps.
	 */
	while (true)
	{
		int			left_off = left_offset(node_off);
		int			right_off = right_offset(node_off);
		int			swap_off = 0;

		/* Is the left child larger than the parent? */
		if (left_off < heap->bh_size &&
			heap->bh_compare(node_val,
							 heap->bh_nodes[left_off],
							 heap->bh_arg) < 0)
			swap_off = left_off;

		/* Is the right child larger than the parent? */
		if (right_off < heap->bh_size &&
			heap->bh_compare(node_val,
							 heap->bh_nodes[right_off],
							 heap->bh_arg) < 0)
		{
			/* swap with the larger child */
			if (!swap_off ||
				heap->bh_compare(heap->bh_nodes[left_off],
								 heap->bh_nodes[right_off],
								 heap->bh_arg) < 0)
				swap_off = right_off;
		}

		/*
		 * If we didn't find anything to swap, the heap condition is
		 * satisfied, and we're done.
		 */
		if (!swap_off)
			break;

		/*
		 * Otherwise, swap the hole with the child that violates the heap
		 * property; then go on to check its children.
		 */
		heap->bh_nodes[node_off] = heap->bh_nodes[swap_off];
		node_off = swap_off;
	}
	/* Re-fill the hole */
	heap->bh_nodes[node_off] = node_val;
}
