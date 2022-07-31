/*-------------------------------------------------------------------------
 *
 * radixtree.c
 *		Implementation for adaptive radix tree.
 *
 * This module employs the idea from the paper "The Adaptive Radix Tree: ARTful
 * Indexing for Main-Memory Databases" by Viktor Leis, Alfons Kemper, and Thomas
 * Neumann, 2013. The radix tree uses adaptive node sizes, a small number of node
 * types, each with a different numbers of elements. Depending on the number of
 * children, the appropriate node type is used.
 *
 * There are some differences from the proposed implementation. For instance,
 * this radix tree module utilizes AVX2 instruction, enabling us to use 256-bit
 * width SIMD vector, whereas 128-bit width SIMD vector is used in the paper.
 * Also, there is no support for path compression and lazy path expansion. The
 * radix tree supports fixed length of the key so we don't expect the tree level
 * wouldn't be high.
 *
 * Both the key and the value are 64-bit unsigned integer. The internal nodes and
 * the leaf nodes have slightly different structure: for internal tree nodes,
 * shift > 0, store the pointer to its child node as the value. The leaf nodes,
 * shift == 0, have the 64-bit unsigned integer that is specified by the user as
 * the value. The paper refers to this technique as "Multi-value leaves".  We
 * choose it to avoid an additional pointer traversal.  It is the reason this code
 * currently does not support variable-length keys.
 *
 * XXX: the radix tree node never be shrunk.
 *
 * Interface
 * ---------
 *
 * rt_create		- Create a new, empty radix tree
 * rt_free			- Free the radix tree
 * rt_search		- Search a key-value pair
 * rt_set			- Set a key-value pair
 * rt_delete		- Delete a key-value pair
 * rt_begin_iter	- Begin iterating through all key-value pairs
 * rt_iter_next		- Return next key-value pair, if any
 * rt_end_iter		- End iteration
 *
 * rt_create() creates an empty radix tree in the given memory context
 * and memory contexts for all kinds of radix tree node under the memory context.
 *
 * rt_iterate_next() ensures returning key-value pairs in the ascending
 * order of the key.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/radixtree.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "utils/memutils.h"
#include "lib/radixtree.h"
#include "lib/stringinfo.h"

#if defined(__SSE2__)
#include <emmintrin.h>			/* SSE2 intrinsics */
#endif

/* The number of bits encoded in one tree level */
#define RT_NODE_SPAN	BITS_PER_BYTE

/* The number of maximum slots in the node */
#define RT_NODE_MAX_SLOTS (1 << RT_NODE_SPAN)

/*
 * Return the number of bits required to represent nslots slots, used
 * nodes indexed by array lookup.
 */
#define RT_NODE_NSLOTS_BITS(nslots) ((nslots) / (sizeof(uint8) * BITS_PER_BYTE))

/* Mask for extracting a chunk from the key */
#define RT_CHUNK_MASK ((1 << RT_NODE_SPAN) - 1)

/* Maximum shift the radix tree uses */
#define RT_MAX_SHIFT	key_get_shift(UINT64_MAX)

/* Tree level the radix tree uses */
#define RT_MAX_LEVEL	((sizeof(uint64) * BITS_PER_BYTE) / RT_NODE_SPAN)

/* Invalid index used in node-128 */
#define RT_NODE_128_INVALID_IDX	0xFF

/* Get a chunk from the key */
#define RT_GET_KEY_CHUNK(key, shift) \
	((uint8) (((key) >> (shift)) & RT_CHUNK_MASK))

/*
 * Mapping from the value to the bit in is-set bitmap in the node-256.
 */
#define RT_NODE_BITMAP_BYTE(v) ((v) / BITS_PER_BYTE)
#define RT_NODE_BITMAP_BIT(v) (UINT64CONST(1) << ((v) % RT_NODE_SPAN))

/* Enum used rt_node_search() */
typedef enum
{
	RT_ACTION_FIND = 0,			/* find the key-value */
	RT_ACTION_DELETE,			/* delete the key-value */
} rt_action;

/*
 * Supported radix tree nodes.
 *
 * XXX: These are currently not well chosen. To reduce memory fragmentation
 * smaller class should optimally fit neatly into the next larger class
 * (except perhaps at the lowest end). Right now its
 * 48 -> 152 -> 296 -> 1304 -> 2088 bytes for inner/leaf nodes, leading to
 * large amounts of allocator padding with aset.c. Hence the use of slab.
 *
 * XXX: need to have node-1 until there is no path compression optimization?
 *
 * XXX: need to explain why we choose these node types based on benchmark
 * results etc.
 */
typedef enum rt_node_kind
{
	RT_NODE_KIND_4 = 0,
	RT_NODE_KIND_16,
	RT_NODE_KIND_32,
	RT_NODE_KIND_128,
	RT_NODE_KIND_256
} rt_node_kind;
#define RT_NODE_KIND_COUNT (RT_NODE_KIND_256 + 1)

/*
 * Base type for all nodes types.
 */
typedef struct rt_node
{
	/*
	 * Number of children.  We use uint16 to be able to indicate 256 children
	 * at the fanout of 8.
	 */
	uint16		count;

	/*
	 * Shift indicates which part of the key space is represented by this
	 * node. That is, the key is shifted by 'shift' and the lowest
	 * RT_NODE_SPAN bits are then represented in chunk.
	 */
	uint8		shift;
	uint8		chunk;

	/* Size class of the node */
	rt_node_kind kind;
} rt_node;

/* Macros for radix tree nodes */
#define IS_LEAF_NODE(n) (((rt_node *) (n))->shift == 0)
#define IS_EMPTY_NODE(n) (((rt_node *) (n))->count == 0)
#define NODE_HAS_FREE_SLOT(n) \
	(((rt_node *) (n))->count < rt_node_info[((rt_node *) (n))->kind].fanout)

/* Base types for inner and leaf nodes of each node type */
typedef struct rd_node_base_4
{
	rt_node		n;

	/* 4 children, for key chunks */
	uint8		chunks[4];
} rt_node_base_4;

typedef struct rd_node_base_16
{
	rt_node		n;

	/* 16 children, for key chunks */
	uint8		chunks[16];
}			rt_node_base_16;

typedef struct rd_node_base_32
{
	rt_node		n;

	/* 32 children, for key chunks */
	uint8		chunks[32];
} rt_node_base_32;

typedef struct rd_node_base_128
{
	rt_node		n;

	/* The index of slots for each fanout */
	uint8		slot_idxs[RT_NODE_MAX_SLOTS];

	/* isset is a bitmap to track which slot is in use */
	uint8		isset[RT_NODE_NSLOTS_BITS(128)];
} rt_node_base_128;

typedef struct rd_node_base_256
{
	rt_node		n;

	/* isset is a bitmap to track which slot is in use */
	uint8		isset[RT_NODE_NSLOTS_BITS(RT_NODE_MAX_SLOTS)];
} rt_node_base_256;

/*
 * Inner and leaf nodes.
 *
 * There are separate from inner node size classes for two main reasons:
 *
 * 1) the value type might be different than something fitting into a pointer
 *    width type
 * 2) Need to represent non-existing values in a key-type independent way.
 *
 * 1) is clearly worth being concerned about, but it's not clear 2) is as
 * good. It might be better to just indicate non-existing entries the same way
 * in inner nodes.
 */
typedef struct rt_node_inner_4
{
	rt_node_base_4 base;

	/* 4 children, for key chunks */
	rt_node    *children[4];
} rt_node_inner_4;

typedef struct rt_node_leaf_4
{
	rt_node_base_4 base;

	/* 4 values, for key chunks */
	uint64		values[4];
}			rt_node_leaf_4;

typedef struct rt_node_inner_16
{
	rt_node_base_16 base;

	/* 16 children, for key chunks */
	rt_node    *children[16];
}			rt_node_inner_16;

typedef struct rt_node_leaf_16
{
	rt_node_base_16 base;

	/* 16 values, for key chunks */
	uint64		values[16];
}			rt_node_leaf_16;

typedef struct rt_node_inner_32
{
	rt_node_base_32 base;

	/* 32 children, for key chunks */
	rt_node    *children[32];
} rt_node_inner_32;

typedef struct rt_node_leaf_32
{
	rt_node_base_32 base;

	/* 32 values, for key chunks */
	uint64		values[32];
} rt_node_leaf_32;

typedef struct rt_node_inner_128
{
	rt_node_base_128 base;

	/* Slots for 128 children */
	rt_node    *children[128];
} rt_node_inner_128;

typedef struct rt_node_leaf_128
{
	rt_node_base_128 base;

	/* Slots for 128 values */
	uint64		values[128];
} rt_node_leaf_128;

typedef struct rt_node_inner_256
{
	rt_node_base_256 base;

	/* Slots for 256 children */
	rt_node    *children[RT_NODE_MAX_SLOTS];
} rt_node_inner_256;

typedef struct rt_node_leaf_256
{
	rt_node_base_256 base;

	/* Slots for 256 values */
	uint64		values[RT_NODE_MAX_SLOTS];
} rt_node_leaf_256;

/* Information of each size class */
typedef struct rt_node_info_elem
{
	const char *name;
	int			fanout;
	Size		inner_size;
	Size		leaf_size;
} rt_node_info_elem;

static rt_node_info_elem rt_node_info[RT_NODE_KIND_COUNT] = {

	[RT_NODE_KIND_4] = {
		.name = "radix tree node 4",
		.fanout = 4,
		.inner_size = sizeof(rt_node_inner_4),
		.leaf_size = sizeof(rt_node_leaf_4),
	},
	[RT_NODE_KIND_16] = {
		.name = "radix tree node 16",
		.fanout = 16,
		.inner_size = sizeof(rt_node_inner_16),
		.leaf_size = sizeof(rt_node_leaf_16),
	},
	[RT_NODE_KIND_32] = {
		.name = "radix tree node 32",
		.fanout = 32,
		.inner_size = sizeof(rt_node_inner_32),
		.leaf_size = sizeof(rt_node_leaf_32),
	},
	[RT_NODE_KIND_128] = {
		.name = "radix tree node 128",
		.fanout = 128,
		.inner_size = sizeof(rt_node_inner_128),
		.leaf_size = sizeof(rt_node_leaf_128),
	},
	[RT_NODE_KIND_256] = {
		.name = "radix tree node 256",
		.fanout = 256,
		.inner_size = sizeof(rt_node_inner_256),
		.leaf_size = sizeof(rt_node_leaf_256),
	},
};

/*
 * Iteration support.
 *
 * Iterating the radix tree returns each pair of key and value in the ascending
 * order of the key. To support this, the we iterate nodes of each level.
 * rt_iter_node_data struct is used to track the iteration within a node.
 * rt_iter has the array of this struct, stack, in order to track the iteration
 * of every level. During the iteration, we also construct the key to return
 * whenever we update the node iteration information, e.g., when advancing the
 * current index within the node or when moving to the next node at the same level.
 */
typedef struct rt_iter_node_data
{
	rt_node    *node;			/* current node being iterated */
	int			current_idx;	/* current position. -1 for initial value */
} rt_iter_node_data;

struct rt_iter
{
	radix_tree *tree;

	/* Track the iteration on nodes of each level */
	rt_iter_node_data stack[RT_MAX_LEVEL];
	int			stack_len;

	/* The key is being constructed during the iteration */
	uint64		key;
};

/* A radix tree with nodes */
struct radix_tree
{
	MemoryContext context;

	rt_node    *root;
	uint64		max_val;
	uint64		num_keys;

	MemoryContextData *inner_slabs[RT_NODE_KIND_COUNT];
	MemoryContextData *leaf_slabs[RT_NODE_KIND_COUNT];

	/* statistics */
	int32		cnt[RT_NODE_KIND_COUNT];
};

static rt_node *rt_node_grow(radix_tree *tree, rt_node *parent,
							 rt_node *node, uint64 key);
static rt_node *rt_alloc_node(radix_tree *tree, rt_node_kind kind, bool inner);
static void rt_free_node(radix_tree *tree, rt_node *node);
static void rt_copy_node_common(rt_node *src, rt_node *dst);
static void rt_extend(radix_tree *tree, uint64 key);
static void rt_new_root(radix_tree *tree, uint64 key);

/* search */
static bool rt_node_search_inner(rt_node *node, uint64 key, rt_action action,
								 rt_node **child_p);
static bool rt_node_search_leaf(rt_node *node, uint64 key, rt_action action,
								uint64 *value_p);
static bool rt_node_search(rt_node *node, uint64 key, rt_action action, void **slot_p);

/* insertion */
static rt_node *rt_node_add_new_child(radix_tree *tree, rt_node *parent,
									  rt_node *node, uint64 key);
static int	rt_node_prepare_insert(radix_tree *tree, rt_node *parent,
								   rt_node **node_p, uint64 key,
								   bool *will_replace_p);
static void rt_node_insert_inner(radix_tree *tree, rt_node *parent, rt_node *node,
								 uint64 key, rt_node *child, bool *replaced_p);
static void rt_node_insert_leaf(radix_tree *tree, rt_node *parent, rt_node *node,
								uint64 key, uint64 value, bool *replaced_p);

static rt_node *rt_alloc_node(radix_tree *tree, rt_node_kind kind, bool inner);
static void rt_extend(radix_tree *tree, uint64 key);
static void rt_new_root(radix_tree *tree, uint64 key);
static void rt_copy_node_common(rt_node *src, rt_node *dst);

/* iteration */
static pg_attribute_always_inline void rt_iter_update_key(rt_iter *iter, uint8 chunk,
														  uint8 shift);
static void *rt_node_iterate_next(rt_iter *iter, rt_iter_node_data *node_iter,
								  bool *found_p);
static void rt_store_iter_node(rt_iter *iter, rt_iter_node_data *node_iter,
							   rt_node *node);
static void rt_update_iter_stack(rt_iter *iter, int from);

/* verification (available only with assertion) */
static void rt_verify_node(rt_node *node);

/*
 * The fanout threshold to choice how to search the key in the chunk array.
 *
 * On platforms where vector instructions, we use the simple for-loop approach for
 * all cases.
 */
#define RT_SIMPLE_LOOP_THRESHOLD		4	/* use simple for-loop */
#define RT_VECRTORIZED_LOOP_THRESHOLD	32	/* use SIMD instructions */

static pg_attribute_always_inline int
search_chunk_array_eq(uint8 *chunks, uint8 key, uint8 node_fanout, uint8 node_count)
{
	if (node_fanout <= RT_SIMPLE_LOOP_THRESHOLD)
	{
		for (int i = 0; i < node_count; i++)
		{
			if (chunks[i] > key)
				return -1;

			if (chunks[i] == key)
				return i;
		}

		return -1;
	}
	else if (node_fanout <= RT_VECRTORIZED_LOOP_THRESHOLD)
	{
		/*
		 * On Windows, even if we use SSE intrinsics, pg_rightmost_one_pos32
		 * is slow. So we guard with HAVE__BUILTIN_CTZ as well.
		 *
		 * XXX: once we have the correct interfaces to pg_bitutils.h for
		 * Windows we can remove the HAVE__BUILTIN_CTZ condition.
		 */
#if defined(__SSE2__) && defined(HAVE__BUILTIN_CTZ)
		int			index = 0;
		__m128i		key_v = _mm_set1_epi8(key);

		while (index < node_count)
		{
			__m128i		data_v = _mm_loadu_si128((__m128i_u *) & (chunks[index]));
			__m128i		cmp_v = _mm_cmpeq_epi8(key_v, data_v);
			uint32		bitfield = _mm_movemask_epi8(cmp_v);

			bitfield &= ((UINT64CONST(1) << node_count) - 1);

			if (bitfield)
			{
				index += pg_rightmost_one_pos32(bitfield);
				break;
			}

			index += 16;
		}

		return (index < node_count) ? index : -1;
#else
		for (int i = 0; i < node_count; i++)
		{
			if (chunks[i] > key)
				return -1;

			if (chunks[i] == key)
				return i;
		}

		return -1;
#endif
	}
	else
		elog(ERROR, "unsupported fanout size %u for chunk array search",
			 node_fanout);
}

/*
 * This is a bit more complicated than search_chunk_array_16_eq(), because
 * until recently no unsigned uint8 comparison instruction existed on x86. So
 * we need to play some trickery using _mm_min_epu8() to effectively get
 * <=. There never will be any equal elements in the current uses, but that's
 * what we get here...
 */
static pg_attribute_always_inline int
search_chunk_array_le(uint8 *chunks, uint8 key, uint8 node_fanout, uint8 node_count)
{
	if (node_fanout <= RT_SIMPLE_LOOP_THRESHOLD)
	{
		int			index;

		for (index = 0; index < node_count; index++)
		{
			if (chunks[index] >= key)
				break;
		}

		return index;
	}
	else if (node_fanout <= RT_VECRTORIZED_LOOP_THRESHOLD)
	{
#if defined(__SSE2__) && defined(HAVE__BUILTIN_CTZ)
		int			index = 0;
		bool		found = false;
		__m128i		key_v = _mm_set1_epi8(key);

		while (index < node_count)
		{
			__m128i		data_v = _mm_loadu_si128((__m128i_u *) & (chunks[index]));
			__m128i		min_v = _mm_min_epu8(data_v, key_v);
			__m128i		cmp_v = _mm_cmpeq_epi8(key_v, min_v);
			uint32		bitfield = _mm_movemask_epi8(cmp_v);

			bitfield &= ((UINT64CONST(1) << node_count) - 1);

			if (bitfield)
			{
				index += pg_rightmost_one_pos32(bitfield);
				found = true;
				break;
			}

			index += 16;
		}

		return found ? index : node_count;
#else
		int			index;

		for (index = 0; index < node_count; index++)
		{
			if (chunks[index] >= key)
				break;
		}

		return index;
#endif
	}
	else
		elog(ERROR, "unsupported fanout size %u for chunk array search",
			 node_fanout);
}

/* Node support functions for all node types to get its children or values */

/* Return the array of children in the inner node */
static rt_node **
rt_node_get_inner_children(rt_node *node)
{
	rt_node   **children = NULL;

	Assert(!IS_LEAF_NODE(node));

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			children = (rt_node **) ((rt_node_inner_4 *) node)->children;
			break;
		case RT_NODE_KIND_16:
			children = (rt_node **) ((rt_node_inner_16 *) node)->children;
			break;
		case RT_NODE_KIND_32:
			children = (rt_node **) ((rt_node_inner_32 *) node)->children;
			break;
		case RT_NODE_KIND_128:
			children = (rt_node **) ((rt_node_inner_128 *) node)->children;
			break;
		case RT_NODE_KIND_256:
			children = (rt_node **) ((rt_node_inner_256 *) node)->children;
			break;
		default:
			elog(ERROR, "unexpected node type %u", node->kind);
	}

	return children;
}

/* Return the array of values in the leaf node */
static uint64 *
rt_node_get_leaf_values(rt_node *node)
{
	uint64	   *values = NULL;

	Assert(IS_LEAF_NODE(node));

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			values = ((rt_node_leaf_4 *) node)->values;
			break;
		case RT_NODE_KIND_16:
			values = ((rt_node_leaf_16 *) node)->values;
			break;
		case RT_NODE_KIND_32:
			values = ((rt_node_leaf_32 *) node)->values;
			break;
		case RT_NODE_KIND_128:
			values = ((rt_node_leaf_128 *) node)->values;
			break;
		case RT_NODE_KIND_256:
			values = ((rt_node_leaf_256 *) node)->values;
			break;
		default:
			elog(ERROR, "unexpected node type %u", node->kind);
	}

	return values;
}

/*
 * Node support functions for node-4, node-16, and node-32.
 *
 * These three node types have similar structure -- they have the array of chunks with
 * different length and corresponding pointers or values depending on inner nodes or
 * leaf nodes.
 */
#define ENSURE_CHUNK_ARRAY_NODE(node) \
	Assert(((((rt_node*) node)->kind) == RT_NODE_KIND_4) || \
		   ((((rt_node*) node)->kind) == RT_NODE_KIND_16) || \
		   ((((rt_node*) node)->kind) == RT_NODE_KIND_32))

/* Get the pointer to either the child or the value at 'idx */
static void *
chunk_array_node_get_slot(rt_node *node, int idx)
{
	void	   *slot;

	ENSURE_CHUNK_ARRAY_NODE(node);

	if (IS_LEAF_NODE(node))
	{
		uint64	   *values = rt_node_get_leaf_values(node);

		slot = (void *) &(values[idx]);
	}
	else
	{
		rt_node   **children = rt_node_get_inner_children(node);

		slot = (void *) children[idx];
	}

	return slot;
}

/* Return the chunk array in the node */
static uint8 *
chunk_array_node_get_chunks(rt_node *node)
{
	uint8	   *chunk = NULL;

	ENSURE_CHUNK_ARRAY_NODE(node);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			chunk = (uint8 *) ((rt_node_base_4 *) node)->chunks;
			break;
		case RT_NODE_KIND_16:
			chunk = (uint8 *) ((rt_node_base_16 *) node)->chunks;
			break;
		case RT_NODE_KIND_32:
			chunk = (uint8 *) ((rt_node_base_32 *) node)->chunks;
			break;
		default:
			/* this function don't support node-128 and node-256 */
			elog(ERROR, "unsupported node type %d", node->kind);
	}

	return chunk;
}

/* Copy the contents of the node from 'src' to 'dst' */
static void
chunk_array_node_copy_chunks_and_slots(rt_node *src, rt_node *dst)
{
	uint8	   *chunks_src,
			   *chunks_dst;

	ENSURE_CHUNK_ARRAY_NODE(src);
	ENSURE_CHUNK_ARRAY_NODE(dst);

	/* Copy base type */
	rt_copy_node_common(src, dst);

	/* Copy chunk array */
	chunks_src = chunk_array_node_get_chunks(src);
	chunks_dst = chunk_array_node_get_chunks(dst);
	memcpy(chunks_dst, chunks_src, sizeof(uint8) * src->count);

	/* Copy children or values */
	if (IS_LEAF_NODE(src))
	{
		uint64	   *values_src,
				   *values_dst;

		Assert(IS_LEAF_NODE(dst));
		values_src = rt_node_get_leaf_values(src);
		values_dst = rt_node_get_leaf_values(dst);
		memcpy(values_dst, values_src, sizeof(uint64) * src->count);
	}
	else
	{
		rt_node   **children_src,
				  **children_dst;

		Assert(!IS_LEAF_NODE(dst));
		children_src = rt_node_get_inner_children(src);
		children_dst = rt_node_get_inner_children(dst);
		memcpy(children_dst, children_src, sizeof(rt_node *) * src->count);
	}
}

/*
 * Return the index of the (sorted) chunk array where the chunk is inserted.
 * Set true to replaced_p if the chunk already exists in the array.
 */
static int
chunk_array_node_find_insert_pos(rt_node *node, uint8 chunk, bool *found_p)
{
	uint8	   *chunks;
	int			idx;

	ENSURE_CHUNK_ARRAY_NODE(node);

	*found_p = false;
	chunks = chunk_array_node_get_chunks(node);

	/* Find the insert pos */
	idx = search_chunk_array_le(chunks, chunk,
								rt_node_info[node->kind].fanout,
								node->count);

	if (idx < node->count && chunks[idx] == chunk)
		*found_p = true;

	return idx;
}

/* Delete the chunk at idx */
static void
chunk_array_node_delete(rt_node *node, int idx)
{
	uint8	   *chunks = chunk_array_node_get_chunks(node);

	/* delete the chunk from the chunk array */
	memmove(&(chunks[idx]), &(chunks[idx + 1]),
			sizeof(uint8) * (node->count - idx - 1));

	/* delete either the value or the child as well */
	if (IS_LEAF_NODE(node))
	{
		uint64	   *values = rt_node_get_leaf_values(node);

		memmove(&(values[idx]),
				&(values[idx + 1]),
				sizeof(uint64) * (node->count - idx - 1));
	}
	else
	{
		rt_node   **children = rt_node_get_inner_children(node);

		memmove(&(children[idx]),
				&(children[idx + 1]),
				sizeof(rt_node *) * (node->count - idx - 1));
	}
}

/* Support function for both node-128 */

/* Does the given chunk in the node has the value? */
static pg_attribute_always_inline bool
node_128_is_chunk_used(rt_node_base_128 *node, uint8 chunk)
{
	return node->slot_idxs[chunk] != RT_NODE_128_INVALID_IDX;
}

/* Is the slot in the node used? */
static pg_attribute_always_inline bool
node_128_is_slot_used(rt_node_base_128 *node, uint8 slot)
{
	return ((node->isset[RT_NODE_BITMAP_BYTE(slot)] & RT_NODE_BITMAP_BIT(slot)) != 0);
}

/* Get the pointer to either the child or the value corresponding to chunk */
static void *
node_128_get_slot(rt_node_base_128 *node, uint8 chunk)
{
	int			slotpos;
	void	   *slot;

	slotpos = node->slot_idxs[chunk];
	Assert(slotpos != RT_NODE_128_INVALID_IDX);

	if (IS_LEAF_NODE(node))
		slot = (void *) &(((rt_node_leaf_128 *) node)->values[slotpos]);
	else
		slot = (void *) (((rt_node_inner_128 *) node)->children[slotpos]);

	return slot;
}

/* Delete the chunk in the node */
static void
node_128_delete(rt_node_base_128 *node, uint8 chunk)
{
	int			slotpos = node->slot_idxs[chunk];

	node->isset[RT_NODE_BITMAP_BYTE(slotpos)] &= ~(RT_NODE_BITMAP_BIT(slotpos));
	node->slot_idxs[chunk] = RT_NODE_128_INVALID_IDX;
}

/* Return an unused slot in node-128 */
static int
node_128_find_unused_slot(rt_node_base_128 *node, uint8 chunk)
{
	int			slotpos;

	/*
	 * Find an unused slot. We iterate over the isset bitmap per byte then
	 * check each bit.
	 */
	for (slotpos = 0; slotpos < RT_NODE_NSLOTS_BITS(128); slotpos++)
	{
		if (node->isset[slotpos] < 0xFF)
			break;
	}
	Assert(slotpos < RT_NODE_NSLOTS_BITS(128));

	slotpos *= BITS_PER_BYTE;
	while (node_128_is_slot_used(node, slotpos))
		slotpos++;

	return slotpos;
}


/* XXX: duplicate with node_128_set_leaf */
static void
node_128_set_inner(rt_node_base_128 *node, uint8 chunk, rt_node *child)
{
	int			slotpos;
	rt_node_inner_128 *n128 = (rt_node_inner_128 *) node;

	/* Overwrite the existing value if exists */
	if (node_128_is_chunk_used(node, chunk))
	{
		n128->children[n128->base.slot_idxs[chunk]] = child;
		return;
	}

	/* find unused slot */
	slotpos = node_128_find_unused_slot(node, chunk);

	n128->base.slot_idxs[chunk] = slotpos;
	n128->base.isset[RT_NODE_BITMAP_BYTE(slotpos)] |= RT_NODE_BITMAP_BIT(slotpos);
	n128->children[slotpos] = child;
}

/* Set the slot at the corresponding chunk */
static void
node_128_set_leaf(rt_node_base_128 *node, uint8 chunk, uint64 value)
{
	int			slotpos;
	rt_node_leaf_128 *n128 = (rt_node_leaf_128 *) node;

	/* Overwrite the existing value if exists */
	if (node_128_is_chunk_used(node, chunk))
	{
		n128->values[n128->base.slot_idxs[chunk]] = value;
		return;
	}

	/* find unused slot */
	slotpos = node_128_find_unused_slot(node, chunk);

	n128->base.slot_idxs[chunk] = slotpos;
	n128->base.isset[RT_NODE_BITMAP_BYTE(slotpos)] |= RT_NODE_BITMAP_BIT(slotpos);
	n128->values[slotpos] = value;
}

/* Return true if the slot corresponding to the given chunk is in use */
static bool
node_256_is_chunk_used(rt_node_base_256 *node, uint8 chunk)
{
	return (node->isset[RT_NODE_BITMAP_BYTE(chunk)] & RT_NODE_BITMAP_BIT(chunk)) != 0;
}

/* Get the pointer to either the child or the value corresponding to chunk */
static void *
node_256_get_slot(rt_node_base_256 *node, uint8 chunk)
{
	void	   *slot;

	Assert(node_256_is_chunk_used(node, chunk));
	if (IS_LEAF_NODE(node))
		slot = (void *) &(((rt_node_leaf_256 *) node)->values[chunk]);
	else
		slot = (void *) (((rt_node_inner_256 *) node)->children[chunk]);

	return slot;
}

/* Set the child in the node-256 */
static pg_attribute_always_inline void
node_256_set_inner(rt_node_base_256 *node, uint8 chunk, rt_node *child)
{
	rt_node_inner_256 *n256 = (rt_node_inner_256 *) node;

	n256->base.isset[RT_NODE_BITMAP_BYTE(chunk)] |= RT_NODE_BITMAP_BIT(chunk);
	n256->children[chunk] = child;
}

/* Set the value in the node-256 */
static pg_attribute_always_inline void
node_256_set_leaf(rt_node_base_256 *node, uint8 chunk, uint64 value)
{
	rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node;

	n256->base.isset[RT_NODE_BITMAP_BYTE(chunk)] |= RT_NODE_BITMAP_BIT(chunk);
	n256->values[chunk] = value;
}

/* Set the slot at the given chunk position */
static pg_attribute_always_inline void
node_256_delete(rt_node_base_256 *node, uint8 chunk)
{
	node->isset[RT_NODE_BITMAP_BYTE(chunk)] &= ~(RT_NODE_BITMAP_BIT(chunk));
}

/*
 * Return the shift that is satisfied to store the given key.
 */
static pg_attribute_always_inline int
key_get_shift(uint64 key)
{
	return (key == 0)
		? 0
		: (pg_leftmost_one_pos64(key) / RT_NODE_SPAN) * RT_NODE_SPAN;
}

/*
 * Return the max value stored in a node with the given shift.
 */
static uint64
shift_get_max_val(int shift)
{
	if (shift == RT_MAX_SHIFT)
		return UINT64_MAX;

	return (UINT64CONST(1) << (shift + RT_NODE_SPAN)) - 1;
}

/*
 * Allocate a new node with the given node kind.
 */
static rt_node *
rt_alloc_node(radix_tree *tree, rt_node_kind kind, bool inner)
{
	rt_node    *newnode;

	if (inner)
		newnode = (rt_node *) MemoryContextAllocZero(tree->inner_slabs[kind],
													 rt_node_info[kind].inner_size);
	else
		newnode = (rt_node *) MemoryContextAllocZero(tree->leaf_slabs[kind],
													 rt_node_info[kind].leaf_size);

	newnode->kind = kind;

	/* Initialize slot_idxs to invalid values */
	if (kind == RT_NODE_KIND_128)
	{
		rt_node_base_128 *n128 = (rt_node_base_128 *) newnode;

		memset(n128->slot_idxs, RT_NODE_128_INVALID_IDX, sizeof(n128->slot_idxs));
	}

	/* update the statistics */
	tree->cnt[kind]++;

	return newnode;
}

/* Free the given node */
static void
rt_free_node(radix_tree *tree, rt_node *node)
{
	/* If we're deleting the root node, make the tree empty */
	if (tree->root == node)
		tree->root = NULL;

	/* update the statistics */
	tree->cnt[node->kind]--;

	Assert(tree->cnt[node->kind] >= 0);

	pfree(node);
}

/* Copy the common fields without the node kind */
static void
rt_copy_node_common(rt_node *src, rt_node *dst)
{
	dst->shift = src->shift;
	dst->chunk = src->chunk;
	dst->count = src->count;
}

/*
 * The radix tree doesn't sufficient height. Extend the radix tree so it can
 * store the key.
 */
static void
rt_extend(radix_tree *tree, uint64 key)
{
	int			target_shift;
	int			shift = tree->root->shift + RT_NODE_SPAN;

	target_shift = key_get_shift(key);

	/* Grow tree from 'shift' to 'target_shift' */
	while (shift <= target_shift)
	{
		rt_node_inner_4 *node =
		(rt_node_inner_4 *) rt_alloc_node(tree, RT_NODE_KIND_4, true);

		node->base.n.count = 1;
		node->base.n.shift = shift;
		node->base.chunks[0] = 0;
		node->children[0] = tree->root;

		tree->root->chunk = 0;
		tree->root = (rt_node *) node;

		shift += RT_NODE_SPAN;
	}

	tree->max_val = shift_get_max_val(target_shift);
}

/*
 * Wrapper for rt_node_search to search the pointer to the child node in the
 * node.
 *
 * Return true if the corresponding child is found, otherwise return false.  On success,
 * it sets child_p.
 */
static bool
rt_node_search_inner(rt_node *node, uint64 key, rt_action action, rt_node **child_p)
{
	rt_node    *child;

	if (!rt_node_search(node, key, action, (void **) &child))
		return false;

	if (child_p)
		*child_p = child;

	return true;
}

static bool
rt_node_search_leaf(rt_node *node, uint64 key, rt_action action, uint64 *value_p)
{
	uint64	   *value;

	if (!rt_node_search(node, key, action, (void **) &value))
		return false;

	if (value_p)
		*value_p = *value;

	return true;
}

/*
 * Return true if the corresponding slot is used, otherwise return false.  On success,
 * sets the pointer to the slot to slot_p.
 */
static bool
rt_node_search(rt_node *node, uint64 key, rt_action action, void **slot_p)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);
	bool		found = false;

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				int			idx;
				uint8	   *chunks = chunk_array_node_get_chunks(node);

				idx = search_chunk_array_eq(chunks, chunk,
											rt_node_info[node->kind].fanout,
											node->count);

				if (idx < 0)
					break;

				found = true;
				if (action == RT_ACTION_FIND)
					*slot_p = chunk_array_node_get_slot(node, idx);
				else			/* RT_ACTION_DELETE */
					chunk_array_node_delete(node, idx);

				break;
			}
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;

				/* If we find the chunk in the node, do the specified action */
				if (node_128_is_chunk_used(n128, chunk))
				{
					if (action == RT_ACTION_FIND)
						*slot_p = node_128_get_slot(n128, chunk);
					else		/* RT_ACTION_DELETE */
						node_128_delete(n128, chunk);

					found = true;
				}

				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_base_256 *n256 = (rt_node_base_256 *) node;

				/* If we find the chunk in the node, do the specified action */
				if (node_256_is_chunk_used(n256, chunk))
				{
					found = true;

					if (action == RT_ACTION_FIND)
						*slot_p = node_256_get_slot(n256, chunk);
					else		/* RT_ACTION_DELETE */
						node_256_delete(n256, chunk);
				}

				break;
			}
	}

	/* Update the statistics */
	if (action == RT_ACTION_DELETE && found)
		node->count--;

	return found;
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
rt_new_root(radix_tree *tree, uint64 key)
{
	int			shift = key_get_shift(key);
	rt_node    *node;

	node = (rt_node *) rt_alloc_node(tree, RT_NODE_KIND_4, shift > 0);
	node->shift = shift;
	tree->max_val = shift_get_max_val(shift);
	tree->root = node;
}

/* Insert 'node' as a child node of 'parent' */
static rt_node *
rt_node_add_new_child(radix_tree *tree, rt_node *parent, rt_node *node, uint64 key)
{
	uint8		newshift = node->shift - RT_NODE_SPAN;
	rt_node    *newchild =
	(rt_node *) rt_alloc_node(tree, RT_NODE_KIND_4, newshift > 0);

	Assert(!IS_LEAF_NODE(node));

	newchild->shift = newshift;
	newchild->chunk = RT_GET_KEY_CHUNK(key, node->shift);

	rt_node_insert_inner(tree, parent, node, key, newchild, NULL);

	return (rt_node *) newchild;
}

/*
 * For upcoming insertions, we make sure that the node has enough free slots or
 * grow the node if necessary.  We set true to will_replace_p if the chunk
 * already exists and will be replaced on insertion.
 *
 * Return the index in the chunk array where the key can be inserted. We always
 * return 0 in node-128 and node-256 cases.
 */
static int
rt_node_prepare_insert(radix_tree *tree, rt_node *parent, rt_node **node_p,
					   uint64 key, bool *will_replace_p)
{
	rt_node    *node = *node_p;
	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);
	bool		will_replace = false;
	int			idx = 0;

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				bool		can_insert = false;

				while ((node->kind == RT_NODE_KIND_4) ||
					   (node->kind == RT_NODE_KIND_16) ||
					   (node->kind == RT_NODE_KIND_32))
				{
					/* Find the insert pos */
					idx = chunk_array_node_find_insert_pos(node, chunk, &will_replace);

					if (will_replace || NODE_HAS_FREE_SLOT(node))
					{
						can_insert = true;
						break;
					}

					node = rt_node_grow(tree, parent, node, key);
				}

				if (can_insert)
				{
					uint8	   *chunks = chunk_array_node_get_chunks(node);

					/*
					 * The node has unused slot for this chunk. If the key
					 * needs to be inserted in the middle of the array, we
					 * make space for the new key.
					 */
					if (!will_replace && node->count != 0 && idx != node->count)
					{
						memmove(&(chunks[idx + 1]), &(chunks[idx]),
								sizeof(uint8) * (node->count - idx));

						/* shift either the values array or the children array */
						if (IS_LEAF_NODE(node))
						{
							uint64	   *values = rt_node_get_leaf_values(node);

							memmove(&(values[idx + 1]),
									&(values[idx]),
									sizeof(uint64) * (node->count - idx));
						}
						else
						{
							rt_node   **children = rt_node_get_inner_children(node);

							memmove(&(children[idx + 1]),
									&(children[idx]),
									sizeof(rt_node *) * (node->count - idx));
						}
					}

					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;

				if (node_128_is_chunk_used(n128, chunk) || NODE_HAS_FREE_SLOT(n128))
				{
					if (node_128_is_chunk_used(n128, chunk))
						will_replace = true;

					break;
				}

				node = rt_node_grow(tree, parent, node, key);
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_256:
			{
				rt_node_base_256 *n256 = (rt_node_base_256 *) node;

				if (node_256_is_chunk_used(n256, chunk))
					will_replace = true;

				break;
			}
	}

	*node_p = node;
	*will_replace_p = will_replace;

	return idx;
}

/* Insert the child to the inner node */
static void
rt_node_insert_inner(radix_tree *tree, rt_node *parent, rt_node *node,
					 uint64 key, rt_node *child, bool *replaced_p)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);
	int			idx;
	bool		replaced;

	Assert(!IS_LEAF_NODE(node));

	idx = rt_node_prepare_insert(tree, parent, &node, key, &replaced);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				uint8	   *chunks = chunk_array_node_get_chunks(node);
				rt_node   **children = rt_node_get_inner_children(node);

				Assert(idx >= 0);
				chunks[idx] = chunk;
				children[idx] = child;
				break;
			}
		case RT_NODE_KIND_128:
			{
				node_128_set_inner((rt_node_base_128 *) node, chunk, child);
				break;
			}
		case RT_NODE_KIND_256:
			{
				node_256_set_inner((rt_node_base_256 *) node, chunk, child);
				break;
			}
	}

	/* Update statistics */
	if (!replaced)
		node->count++;

	if (replaced_p)
		*replaced_p = replaced;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	rt_verify_node(node);
}

/* Insert the value to the leaf node */
static void
rt_node_insert_leaf(radix_tree *tree, rt_node *parent, rt_node *node,
					uint64 key, uint64 value, bool *replaced_p)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);
	int			idx;
	bool		replaced;

	Assert(IS_LEAF_NODE(node));

	idx = rt_node_prepare_insert(tree, parent, &node, key, &replaced);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				uint8	   *chunks = chunk_array_node_get_chunks(node);
				uint64	   *values = rt_node_get_leaf_values(node);

				Assert(idx >= 0);
				chunks[idx] = chunk;
				values[idx] = value;
				break;
			}
		case RT_NODE_KIND_128:
			{
				node_128_set_leaf((rt_node_base_128 *) node, chunk, value);
				break;
			}
		case RT_NODE_KIND_256:
			{
				node_256_set_leaf((rt_node_base_256 *) node, chunk, value);
				break;
			}
	}

	/* Update statistics */
	if (!replaced)
		node->count++;

	*replaced_p = replaced;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	rt_verify_node(node);
}

/* Change the node type to the next larger one */
static rt_node *
rt_node_grow(radix_tree *tree, rt_node *parent, rt_node *node, uint64 key)
{
	rt_node    *newnode = NULL;

	Assert(node->count == rt_node_info[node->kind].fanout);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				newnode = rt_alloc_node(tree, RT_NODE_KIND_16,
										IS_LEAF_NODE(node));

				/* Copy both chunks and slots to the new node */
				chunk_array_node_copy_chunks_and_slots(node, newnode);
				break;
			}
		case RT_NODE_KIND_16:
			{
				newnode = rt_alloc_node(tree, RT_NODE_KIND_32,
										IS_LEAF_NODE(node));

				/* Copy both chunks and slots to the new node */
				chunk_array_node_copy_chunks_and_slots(node, newnode);
				break;
			}
		case RT_NODE_KIND_32:
			{
				newnode = rt_alloc_node(tree, RT_NODE_KIND_128,
										IS_LEAF_NODE(node));

				/* Copy both chunks and slots to the new node */
				rt_copy_node_common(node, newnode);

				if (IS_LEAF_NODE(node))
				{
					rt_node_leaf_32 *n32 = (rt_node_leaf_32 *) node;

					for (int i = 0; i < node->count; i++)
						node_128_set_leaf((rt_node_base_128 *) newnode,
										  n32->base.chunks[i], n32->values[i]);
				}
				else
				{
					rt_node_inner_32 *n32 = (rt_node_inner_32 *) node;

					for (int i = 0; i < node->count; i++)
						node_128_set_inner((rt_node_base_128 *) newnode,
										   n32->base.chunks[i], n32->children[i]);
				}

				break;
			}
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;
				int			cnt = 0;

				newnode = rt_alloc_node(tree, RT_NODE_KIND_256,
										IS_LEAF_NODE(node));

				/* Copy both chunks and slots to the new node */
				rt_copy_node_common(node, newnode);

				for (int i = 0; i < RT_NODE_MAX_SLOTS && cnt < n128->n.count; i++)
				{
					void	   *slot;

					if (!node_128_is_chunk_used(n128, i))
						continue;

					slot = node_128_get_slot(n128, i);

					if (IS_LEAF_NODE(node))
						node_256_set_leaf((rt_node_base_256 *) newnode, i,
										  *(uint64 *) slot);
					else
						node_256_set_inner((rt_node_base_256 *) newnode, i,
										   (rt_node *) slot);

					cnt++;
				}

				break;
			}
		case RT_NODE_KIND_256:
			elog(ERROR, "radix tree node-256 cannot grow");
			break;
	}

	if (parent == node)
	{
		/* Replace the root node with the new large node */
		tree->root = newnode;
	}
	else
	{
		/* Set the new node to the parent node */
		rt_node_insert_inner(tree, NULL, parent, key, newnode, NULL);
	}

	/* Verify the node has grown properly */
	rt_verify_node(newnode);

	/* Free the old node */
	rt_free_node(tree, node);

	return newnode;
}

/*
 * Create the radix tree in the given memory context and return it.
 */
radix_tree *
rt_create(MemoryContext ctx)
{
	radix_tree *tree;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = palloc(sizeof(radix_tree));
	tree->context = ctx;
	tree->root = NULL;
	tree->max_val = 0;
	tree->num_keys = 0;

	/* Create the slab allocator for each size class */
	for (int i = 0; i < RT_NODE_KIND_COUNT; i++)
	{
		tree->inner_slabs[i] = SlabContextCreate(ctx,
												 rt_node_info[i].name,
												 SLAB_DEFAULT_BLOCK_SIZE,
												 rt_node_info[i].inner_size);
		tree->leaf_slabs[i] = SlabContextCreate(ctx,
												rt_node_info[i].name,
												SLAB_DEFAULT_BLOCK_SIZE,
												rt_node_info[i].leaf_size);
		tree->cnt[i] = 0;
	}

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

/*
 * Free the given radix tree.
 */
void
rt_free(radix_tree *tree)
{
	for (int i = 0; i < RT_NODE_KIND_COUNT; i++)
	{
		MemoryContextDelete(tree->inner_slabs[i]);
		MemoryContextDelete(tree->leaf_slabs[i]);
	}

	pfree(tree);
}

/*
 * Set key to value. If the entry exists, we update its value to 'value' and return
 * true. Returns false if entry doesn't yet exist.
 */
bool
rt_set(radix_tree *tree, uint64 key, uint64 value)
{
	int			shift;
	bool		replaced;
	rt_node    *node;
	rt_node    *parent = tree->root;

	/* Empty tree, create the root */
	if (!tree->root)
		rt_new_root(tree, key);

	/* Extend the tree if necessary */
	if (key > tree->max_val)
		rt_extend(tree, key);

	Assert(tree->root);

	shift = tree->root->shift;
	node = tree->root;

	while (shift > 0)
	{
		rt_node    *child;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			child = rt_node_add_new_child(tree, parent, node, key);

		Assert(child);

		parent = node;
		node = child;
		shift -= RT_NODE_SPAN;
	}

	/* arrived at a leaf */
	Assert(IS_LEAF_NODE(node));

	rt_node_insert_leaf(tree, parent, node, key, value, &replaced);

	/* Update the statistics */
	if (!replaced)
		tree->num_keys++;

	return replaced;
}

/*
 * Search the given key in the radix tree. Return true if the key is successfully
 * found, otherwise return false.  On success, we set the value to *val_p so
 * it must not be NULL.
 */
bool
rt_search(radix_tree *tree, uint64 key, uint64 *value_p)
{
	rt_node    *node;
	int			shift;

	Assert(value_p);

	if (!tree->root || key > tree->max_val)
		return false;

	node = tree->root;
	shift = tree->root->shift;
	while (shift > 0)
	{
		rt_node    *child;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			return false;

		node = child;
		shift -= RT_NODE_SPAN;
	}

	/* We reached at a leaf node, search the corresponding slot */
	Assert(IS_LEAF_NODE(node));

	if (!rt_node_search_leaf(node, key, RT_ACTION_FIND, value_p))
		return false;

	return true;
}

/*
 * Delete the given key from the radix tree. Return true if the key is found (and
 * deleted), otherwise do nothing and return false.
 */
bool
rt_delete(radix_tree *tree, uint64 key)
{
	rt_node    *node;
	int			shift;
	rt_node    *stack[RT_MAX_LEVEL] = {0};
	int			level;
	bool		deleted;

	if (!tree->root || key > tree->max_val)
		return false;

	/*
	 * Descending the tree to search the key while building a stack of nodes
	 * we visited.
	 */
	node = tree->root;
	shift = tree->root->shift;
	level = 0;
	while (shift >= 0)
	{
		rt_node    *child;

		/* Push the current node to the stack */
		stack[level] = node;

		if (IS_LEAF_NODE(node))
			break;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			return false;

		node = child;
		shift -= RT_NODE_SPAN;
		level++;
	}

	/*
	 * Delete the key from the leaf node and recursively delete internal nodes
	 * if necessary.
	 */
	Assert(IS_LEAF_NODE(stack[level]));
	while (level >= 0)
	{
		rt_node    *node = stack[level--];

		if (IS_LEAF_NODE(node))
			deleted = rt_node_search_leaf(node, key, RT_ACTION_DELETE, NULL);
		else
			deleted = rt_node_search_inner(node, key, RT_ACTION_DELETE, NULL);

		/* If the node didn't become empty, we stop deleting the key */
		if (!IS_EMPTY_NODE(node))
			break;

		Assert(deleted);

		/* The node became empty */
		rt_free_node(tree, node);

	}

	/*
	 * If we eventually deleted the root node while recursively deleting empty
	 * nodes, we make the tree empty.
	 */
	if (level == 0)
	{
		tree->root = NULL;
		tree->max_val = 0;
	}

	if (deleted)
		tree->num_keys--;

	return deleted;
}

/* Create and return the iterator for the given radix tree */
rt_iter *
rt_begin_iterate(radix_tree *tree)
{
	MemoryContext old_ctx;
	rt_iter    *iter;
	int			top_level;

	old_ctx = MemoryContextSwitchTo(tree->context);

	iter = (rt_iter *) palloc0(sizeof(rt_iter));
	iter->tree = tree;

	/* empty tree */
	if (!iter->tree)
		return iter;

	top_level = iter->tree->root->shift / RT_NODE_SPAN;

	iter->stack_len = top_level;
	iter->stack[top_level].node = iter->tree->root;
	iter->stack[top_level].current_idx = -1;

	/*
	 * Descend to the left most leaf node from the root. The key is being
	 * constructed while descending to the leaf.
	 */
	rt_update_iter_stack(iter, top_level);

	MemoryContextSwitchTo(old_ctx);

	return iter;
}

/*
 * Update the stack of the radix tree node while descending to the leaf from
 * the 'from' level.
 */
static void
rt_update_iter_stack(rt_iter *iter, int from)
{
	rt_node    *node = iter->stack[from].node;
	int			level = from;

	for (;;)
	{
		rt_iter_node_data *node_iter = &(iter->stack[level--]);
		bool		found;

		/* Set the node to this level */
		rt_store_iter_node(iter, node_iter, node);

		/* Finish if we reached to the leaf node */
		if (IS_LEAF_NODE(node))
			break;

		/* Advance to the next slot in the node */
		node = (rt_node *) rt_node_iterate_next(iter, node_iter, &found);

		/*
		 * Since we always get the first slot in the node, we have to found
		 * the slot.
		 */
		Assert(found);
	}
}

/*
 * Return true with setting key_p and value_p if there is next key.  Otherwise,
 * return false.
 */
bool
rt_iterate_next(rt_iter *iter, uint64 *key_p, uint64 *value_p)
{
	bool		found = false;
	void	   *slot;

	/* Empty tree */
	if (!iter->tree)
		return false;

	for (;;)
	{
		rt_node    *node;
		rt_iter_node_data *node_iter;
		int			level;

		/*
		 * Iterate node at each level from the bottom of the tree, i.e., the
		 * lead node, until we find the next slot.
		 */
		for (level = 0; level <= iter->stack_len; level++)
		{
			slot = rt_node_iterate_next(iter, &(iter->stack[level]), &found);

			if (found)
				break;
		}

		/* We could not find any new key-value pair, the iteration finished */
		if (!found)
			break;

		/* found the next slot at the leaf node, return it */
		if (level == 0)
		{
			*key_p = iter->key;
			*value_p = *((uint64 *) slot);
			break;
		}

		/*
		 * We have advanced slots more than one nodes including both the lead
		 * node and internal nodes. So we update the stack by descending to
		 * the left most leaf node from this level.
		 */
		node = (rt_node *) (rt_node *) slot;
		node_iter = &(iter->stack[level - 1]);
		rt_store_iter_node(iter, node_iter, node);
		rt_update_iter_stack(iter, level - 1);
	}

	return found;
}

void
rt_end_iterate(rt_iter *iter)
{
	pfree(iter);
}

/*
 * Iterate over the given radix tree node and returns the next slot of the given
 * node and set true to *found_p, if any.  Otherwise, set false to *found_p.
 */
static void *
rt_node_iterate_next(rt_iter *iter, rt_iter_node_data *node_iter, bool *found_p)
{
	rt_node    *node = node_iter->node;
	void	   *slot = NULL;

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				node_iter->current_idx++;

				if (node_iter->current_idx >= node->count)
					goto not_found;

				slot = chunk_array_node_get_slot(node, node_iter->current_idx);

				/* Update the part of the key by the current chunk */
				if (IS_LEAF_NODE(node))
				{
					uint8	   *chunks = chunk_array_node_get_chunks(node);

					rt_iter_update_key(iter, chunks[node_iter->current_idx], 0);
				}

				break;
			}
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;
				int			i;

				for (i = node_iter->current_idx + 1; i < 256; i++)
				{
					if (node_128_is_chunk_used(n128, i))
						break;
				}

				if (i >= 256)
					goto not_found;

				node_iter->current_idx = i;
				slot = node_128_get_slot(n128, i);

				/* Update the part of the key */
				if (IS_LEAF_NODE(n128))
					rt_iter_update_key(iter, node_iter->current_idx, 0);

				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_base_256 *n256 = (rt_node_base_256 *) node;
				int			i;

				for (i = node_iter->current_idx + 1; i < 256; i++)
				{
					if (node_256_is_chunk_used(n256, i))
						break;
				}

				if (i >= 256)
					goto not_found;

				node_iter->current_idx = i;
				slot = node_256_get_slot(n256, i);

				/* Update the part of the key */
				if (IS_LEAF_NODE(n256))
					rt_iter_update_key(iter, node_iter->current_idx, 0);

				break;
			}
	}

	Assert(slot);
	*found_p = true;
	return slot;

not_found:
	*found_p = false;
	return NULL;
}

/*
 * Set the node to the node_iter so we can begin the iteration of the node.
 * Also, we update the part of the key by the chunk of the given node.
 */
static void
rt_store_iter_node(rt_iter *iter, rt_iter_node_data *node_iter,
				   rt_node *node)
{
	node_iter->node = node;
	node_iter->current_idx = -1;

	rt_iter_update_key(iter, node->chunk, node->shift + RT_NODE_SPAN);
}

static pg_attribute_always_inline void
rt_iter_update_key(rt_iter *iter, uint8 chunk, uint8 shift)
{
	iter->key &= ~(((uint64) RT_CHUNK_MASK) << shift);
	iter->key |= (((uint64) chunk) << shift);
}

/*
 * Return the number of keys in the radix tree.
 */
uint64
rt_num_entries(radix_tree *tree)
{
	return tree->num_keys;
}

/*
 * Return the statistics of the amount of memory used by the radix tree.
 */
uint64
rt_memory_usage(radix_tree *tree)
{
	Size		total = 0;

	for (int i = 0; i < RT_NODE_KIND_COUNT; i++)
	{
		total += MemoryContextMemAllocated(tree->inner_slabs[i], true);
		total += MemoryContextMemAllocated(tree->leaf_slabs[i], true);
	}

	return total;
}

/*
 * Verify the radix tree node.
 */
static void
rt_verify_node(rt_node *node)
{
#ifdef USE_ASSERT_CHECKING
	Assert(node->count >= 0);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				uint8	   *chunks = chunk_array_node_get_chunks(node);

				/* Check if the chunks in the node are sorted */
				for (int i = 1; i < node->count; i++)
					Assert(chunks[i - 1] < chunks[i]);

				break;
			}
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;
				int			cnt = 0;

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_128_is_chunk_used(n128, i))
						continue;

					/* Check if the corresponding slot is used */
					Assert(node_128_is_slot_used(n128, n128->slot_idxs[i]));

					cnt++;
				}

				Assert(n128->n.count == cnt);
				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_base_256 *n256 = (rt_node_base_256 *) node;
				int			cnt = 0;

				for (int i = 0; i < RT_NODE_NSLOTS_BITS(RT_NODE_MAX_SLOTS); i++)
					cnt += pg_popcount32(n256->isset[i]);

				/* Check if the number of used chunk matches */
				Assert(n256->n.count == cnt);

				break;
			}
	}
#endif
}

/***************** DEBUG FUNCTIONS *****************/
#ifdef RT_DEBUG
void
rt_stats(radix_tree *tree)
{
	fprintf(stderr, "num_keys = %lu, height = %u, n4 = %u, n16 = %u,n32 = %u, n128 = %u, n256 = %u",
			tree->num_keys,
			tree->root->shift / RT_NODE_SPAN,
			tree->cnt[0],
			tree->cnt[1],
			tree->cnt[2],
			tree->cnt[3],
			tree->cnt[4]);
	/* rt_dump(tree); */
}

static void
rt_print_slot(StringInfo buf, uint8 chunk, uint64 value, int idx, bool is_leaf, int level)
{
	char		space[128] = {0};

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	if (is_leaf)
		appendStringInfo(buf, "%s[%d] \"0x%X\" val(0x%lX) LEAF\n",
						 space,
						 idx,
						 chunk,
						 value);
	else
		appendStringInfo(buf, "%s[%d] \"0x%X\" -> ",
						 space,
						 idx,
						 chunk);
}

static void
rt_dump_node(rt_node *node, int level, StringInfo buf, bool recurse)
{
	bool		is_leaf = IS_LEAF_NODE(node);

	appendStringInfo(buf, "[\"%s\" type %d, cnt %u, shift %u, chunk \"0x%X\"] chunks:\n",
					 IS_LEAF_NODE(node) ? "LEAF" : "INNR",
					 (node->kind == RT_NODE_KIND_4) ? 4 :
					 (node->kind == RT_NODE_KIND_32) ? 32 :
					 (node->kind == RT_NODE_KIND_128) ? 128 : 256,
					 node->count, node->shift, node->chunk);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
		case RT_NODE_KIND_32:
			{
				uint8	   *chunks = chunk_array_node_get_chunks(node);

				for (int i = 0; i < node->count; i++)
				{
					if (IS_LEAF_NODE(node))
					{
						uint64	   *values = rt_node_get_leaf_values(node);

						rt_print_slot(buf, chunks[i],
									  values[i],
									  i, is_leaf, level);
					}
					else
						rt_print_slot(buf, chunks[i],
									  UINT64_MAX,
									  i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							rt_node   **children = rt_node_get_inner_children(node);
							StringInfoData buf2;

							initStringInfo(&buf2);
							rt_dump_node(children[i],
										 level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}

				break;
			}
		case RT_NODE_KIND_128:
			{
				rt_node_base_128 *n128 = (rt_node_base_128 *) node;
				uint8	   *tmp = (uint8 *) n128->isset;

				appendStringInfo(buf, "slot_idxs:");
				for (int j = 0; j < 256; j++)
				{
					if (!node_128_is_chunk_used(n128, j))
						continue;

					appendStringInfo(buf, " [%d]=%d, ", j, n128->slot_idxs[j]);
				}
				appendStringInfo(buf, "\nisset-bitmap:");
				for (int j = 0; j < 16; j++)
				{
					appendStringInfo(buf, "%X ", (uint8) tmp[j]);
				}
				appendStringInfo(buf, "\n");

				for (int i = 0; i < 256; i++)
				{
					void	   *slot;

					if (!node_128_is_chunk_used(n128, i))
						continue;

					slot = node_128_get_slot(n128, i);

					if (is_leaf)
						rt_print_slot(buf, i, *(uint64 *) slot,
									  i, is_leaf, level);
					else
						rt_print_slot(buf, i, UINT64_MAX, i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							rt_dump_node((rt_node *) slot,
										 level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_base_256 *n256 = (rt_node_base_256 *) node;

				for (int i = 0; i < 256; i++)
				{
					void	   *slot;

					if (!node_256_is_chunk_used(n256, i))
						continue;

					slot = node_256_get_slot(n256, i);

					if (is_leaf)
						rt_print_slot(buf, i, *(uint64 *) slot, i, is_leaf, level);
					else
						rt_print_slot(buf, i, UINT64_MAX, i, is_leaf, level);

					if (!is_leaf)
					{
						if (recurse)
						{
							StringInfoData buf2;

							initStringInfo(&buf2);
							rt_dump_node((rt_node *) slot, level + 1, &buf2, recurse);
							appendStringInfo(buf, "%s", buf2.data);
						}
						else
							appendStringInfo(buf, "\n");
					}
				}
				break;
			}
	}
}

void
rt_dump_search(radix_tree *tree, uint64 key)
{
	StringInfoData buf;
	rt_node    *node;
	int			shift;
	int			level = 0;

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = %lu (0x%lX)", tree->max_val, tree->max_val);

	if (!tree->root)
	{
		elog(NOTICE, "tree is empty");
		return;
	}

	if (key > tree->max_val)
	{
		elog(NOTICE, "key %lu (0x%lX) is larger than max val",
			 key, key);
		return;
	}

	initStringInfo(&buf);
	node = tree->root;
	shift = tree->root->shift;
	while (shift >= 0)
	{
		rt_node    *child;

		rt_dump_node(node, level, &buf, false);

		if (IS_LEAF_NODE(node))
		{
			uint64		dummy;

			/* We reached at a leaf node, find the corresponding slot */
			rt_node_search_leaf(node, key, RT_ACTION_FIND, &dummy);

			break;
		}

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			break;

		node = child;
		shift -= RT_NODE_SPAN;
		level++;
	}

	elog(NOTICE, "\n%s", buf.data);
}

void
rt_dump(radix_tree *tree)
{
	StringInfoData buf;

	initStringInfo(&buf);

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = %lu", tree->max_val);
	rt_dump_node(tree->root, 0, &buf, true);
	elog(NOTICE, "\n%s", buf.data);
	elog(NOTICE, "-----------------------------------------------------------");
}
#endif
