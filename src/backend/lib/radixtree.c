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
 * there is not support for path compression and lazy path expansion. The radix
 * tree supports fixed length of the key so we don't expect the tree level
 * wouldn't be high.
 *
 * Both the key and the value are 64-bit unsigned integer. The inner nodes and
 * the leaf nodes have slightly different structure: for inner tree nodes,
 * shift > 0, store the pointer to its child node as the value. The leaf nodes,
 * shift == 0, have the 64-bit unsigned integer that is specified by the user as
 * the value. The paper refers to this technique as "Multi-value leaves".  We
 * choose it to avoid an additional pointer traversal.  It is the reason this code
 * currently does not support variable-length keys.
 *
 * If DSA area is specified for rt_create(), the radix tree is created in the
 * DSA area so that multiple processes can access to it simultaneously. The process
 * who created the shared radix tree needs to tell both DSA area specified when
 * calling to rt_create() and dsa_pointer of the radix tree, fetched by
 * rt_get_dsa_pointer(), to other processes so that they can attach by rt_attach().
 *
 * XXX: shared radix tree is still PoC state as it doesn't have any locking support.
 * Also, it supports the iteration only by one process.
 *
 * XXX: Most functions in this file have two variants for inner nodes and leaf
 * nodes, therefore there are duplication codes. While this sometimes makes the
 * code maintenance tricky, this reduces branch prediction misses when judging
 * whether the node is a inner node of a leaf node.
 *
 * XXX: the radix tree node never be shrunk.
 *
 * Interface
 * ---------
 *
 * rt_create		- Create a new, empty radix tree
 * rt_free			- Free the radix tree
 * rt_attach		- Attach to the radix tree
 * rt_detach		- Detach from the radix tree
 * rt_get_handle	- Return the handle of the radix tree
 * rt_search		- Search a key-value pair
 * rt_set			- Set a key-value pair
 * rt_delete		- Delete a key-value pair
 * rt_begin_iterate	- Begin iterating through all key-value pairs
 * rt_iterate_next	- Return next key-value pair, if any
 * rt_end_iter		- End iteration
 * rt_memory_usage	- Get the memory usage
 * rt_num_entries	- Get the number of key-value pairs
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

#include "lib/radixtree.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "port/pg_lfind.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

#ifdef RT_DEBUG
#define UINT64_FORMAT_HEX "%" INT64_MODIFIER "X"
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

/* Invalid index used in node-125 */
#define RT_NODE_125_INVALID_IDX	0xFF

/* Get a chunk from the key */
#define RT_GET_KEY_CHUNK(key, shift) ((uint8) (((key) >> (shift)) & RT_CHUNK_MASK))

/*
 * Mapping from the value to the bit in is-set bitmap in the node-256.
 */
#define RT_NODE_BITMAP_BYTE(v) ((v) / BITS_PER_BYTE)
#define RT_NODE_BITMAP_BIT(v) (UINT64CONST(1) << ((v) % RT_NODE_SPAN))

/* FIXME rename */
#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

/* Enum used rt_node_search() */
typedef enum
{
	RT_ACTION_FIND = 0,			/* find the key-value */
	RT_ACTION_DELETE,			/* delete the key-value */
} rt_action;

/*
 * Supported radix tree node kinds and size classes.
 *
 * There are 4 node kinds and each node kind have one or two size classes,
 * partial and full. The size classes in the same node kind have the same
 * node structure but have the different number of fanout that is stored
 * in 'fanout' of rt_node. For example in size class 15, when a 16th element
 * is to be inserted, we allocate a larger area and memcpy the entire old
 * node to it.
 *
 * This technique allows us to limit the node kinds to 4, which limits the
 * number of cases in switch statements. It also allows a possible future
 * optimization to encode the node kind in a pointer tag.
 *
 * These size classes have been chose carefully so that it minimizes the
 * allocator padding in both the inner and leaf nodes on DSA.
 * node
 */
#define RT_NODE_KIND_4			0x00
#define RT_NODE_KIND_32			0x01
#define RT_NODE_KIND_125		0x02
#define RT_NODE_KIND_256		0x03
#define RT_NODE_KIND_COUNT		4

typedef enum rt_size_class
{
	RT_CLASS_4_FULL = 0,
	RT_CLASS_32_PARTIAL,
	RT_CLASS_32_FULL,
	RT_CLASS_125_FULL,
	RT_CLASS_256

#define RT_SIZE_CLASS_COUNT (RT_CLASS_256 + 1)
} rt_size_class;

/*
 * rt_pointer is a pointer compatible with a pointer to local memory and a
 * pointer for DSA area (i.e. dsa_pointer). Since the radix tree node can be
 * allocated in backend local memory as well as DSA area, we cannot use a
 * C-pointer to rt_node (i.e. backend local memory address) for child pointers
 * in inner nodes. Inner nodes need to use rt_pointer instead. We can get
 * the backend local memory address of a node from a rt_pointer by using
 * rt_pointer_decode().
*/
typedef uintptr_t rt_pointer;
#define InvalidRTPointer		((rt_pointer) 0)
#define RTPointerIsValid(x) 	(((rt_pointer) (x)) != InvalidRTPointer)

/* Common type for all nodes types */
typedef struct rt_node
{
	/*
	 * Number of children.  We use uint16 to be able to indicate 256 children
	 * at the fanout of 8.
	 */
	uint16		count;

	/* Max number of children. We can use uint8 because we never need to store 256 */
	/* WIP: if we don't have a variable sized node4, this should instead be in the base
	types as needed, since saving every byte is crucial for the smallest node kind */
	uint8		fanout;

	/*
	 * Shift indicates which part of the key space is represented by this
	 * node. That is, the key is shifted by 'shift' and the lowest
	 * RT_NODE_SPAN bits are then represented in chunk.
	 */
	uint8		shift;
	uint8		chunk;

	/* Node kind, one per search/set algorithm */
	uint8		kind;
} rt_node;
#define RT_NODE_IS_LEAF(n)			(((rt_node *) (n))->shift == 0)
#define VAR_NODE_HAS_FREE_SLOT(node) \
	((node)->base.n.count < (node)->base.n.fanout)
#define FIXED_NODE_HAS_FREE_SLOT(node, class) \
	((node)->base.n.count < rt_size_class_info[class].fanout)

/* Base type of each node kinds for leaf and inner nodes */
/* The base types must be a be able to accommodate the largest size
class for variable-sized node kinds*/
typedef struct rt_node_base_4
{
	rt_node		n;

	/* 4 children, for key chunks */
	uint8		chunks[4];
} rt_node_base_4;

typedef struct rt_node_base32
{
	rt_node		n;

	/* 32 children, for key chunks */
	uint8		chunks[32];
} rt_node_base_32;

/*
 * node-125 uses slot_idx array, an array of RT_NODE_MAX_SLOTS length, typically
 * 256, to store indexes into a second array that contains up to 125 values (or
 * child pointers in inner nodes).
 */
typedef struct rt_node_base125
{
	rt_node		n;

	/* The index of slots for each fanout */
	uint8		slot_idxs[RT_NODE_MAX_SLOTS];

	/* isset is a bitmap to track which slot is in use */
	bitmapword		isset[WORDNUM(128)];
} rt_node_base_125;

typedef struct rt_node_base256
{
	rt_node		n;
} rt_node_base_256;

/*
 * Inner and leaf nodes.
 *
 * Theres are separate for two main reasons:
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

	/* number of children depends on size class */
	rt_pointer    children[FLEXIBLE_ARRAY_MEMBER];
} rt_node_inner_4;

typedef struct rt_node_leaf_4
{
	rt_node_base_4 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} rt_node_leaf_4;

typedef struct rt_node_inner_32
{
	rt_node_base_32 base;

	/* number of children depends on size class */
	rt_pointer    children[FLEXIBLE_ARRAY_MEMBER];
} rt_node_inner_32;

typedef struct rt_node_leaf_32
{
	rt_node_base_32 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} rt_node_leaf_32;

typedef struct rt_node_inner_125
{
	rt_node_base_125 base;

	/* number of children depends on size class */
	rt_pointer    children[FLEXIBLE_ARRAY_MEMBER];
} rt_node_inner_125;

typedef struct rt_node_leaf_125
{
	rt_node_base_125 base;

	/* number of values depends on size class */
	uint64		values[FLEXIBLE_ARRAY_MEMBER];
} rt_node_leaf_125;

/*
 * node-256 is the largest node type. This node has RT_NODE_MAX_SLOTS length array
 * for directly storing values (or child pointers in inner nodes).
 */
typedef struct rt_node_inner_256
{
	rt_node_base_256 base;

	/* Slots for 256 children */
	rt_pointer    children[RT_NODE_MAX_SLOTS];
} rt_node_inner_256;

typedef struct rt_node_leaf_256
{
	rt_node_base_256 base;

	/* isset is a bitmap to track which slot is in use */
	uint8		isset[RT_NODE_NSLOTS_BITS(RT_NODE_MAX_SLOTS)];

	/* Slots for 256 values */
	uint64		values[RT_NODE_MAX_SLOTS];
} rt_node_leaf_256;

/* rt_node_ptr is a data structure representing a pointer for a rt_node */
typedef struct rt_node_ptr
{
	rt_pointer		encoded;
	rt_node			*decoded;
} rt_node_ptr;
#define InvalidRTNodePtr \
	(rt_node_ptr) {.encoded = InvalidRTPointer, .decoded = NULL}
#define RTNodePtrIsValid(n) \
	(!rt_node_ptr_eq((rt_node_ptr *) &(n), &(InvalidRTNodePtr)))

/* Macros for rt_node_ptr to access the fields of rt_node */
#define NODE_RAW(n)			(n.decoded)
#define NODE_IS_LEAF(n)		(NODE_RAW(n)->shift == 0)
#define NODE_IS_EMPTY(n)	(NODE_COUNT(n) == 0)
#define NODE_KIND(n)	(NODE_RAW(n)->kind)
#define NODE_COUNT(n)	(NODE_RAW(n)->count)
#define NODE_SHIFT(n)	(NODE_RAW(n)->shift)
#define NODE_CHUNK(n)	(NODE_RAW(n)->chunk)
#define NODE_FANOUT(n)	(NODE_RAW(n)->fanout)
#define NODE_HAS_FREE_SLOT(n) \
	(NODE_COUNT(n) < rt_node_kind_info[NODE_KIND(n)].fanout)

/* Information for each size class */
typedef struct rt_size_class_elem
{
	const char *name;
	int			fanout;

	/* slab chunk size */
	Size		inner_size;
	Size		leaf_size;

	/* slab block size */
	Size		inner_blocksize;
	Size		leaf_blocksize;
} rt_size_class_elem;

/*
 * Calculate the slab blocksize so that we can allocate at least 32 chunks
 * from the block.
 */
#define NODE_SLAB_BLOCK_SIZE(size)	\
	Max((SLAB_DEFAULT_BLOCK_SIZE / (size)) * (size), (size) * 32)
static rt_size_class_elem rt_size_class_info[RT_SIZE_CLASS_COUNT] = {
	[RT_CLASS_4_FULL] = {
		.name = "radix tree node 4",
		.fanout = 4,
		.inner_size = sizeof(rt_node_inner_4) + 4 * sizeof(rt_node *),
		.leaf_size = sizeof(rt_node_leaf_4) + 4 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_inner_4) + 4 * sizeof(rt_node *)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_leaf_4) + 4 * sizeof(uint64)),
	},
	[RT_CLASS_32_PARTIAL] = {
		.name = "radix tree node 15",
		.fanout = 15,
		.inner_size = sizeof(rt_node_inner_32) + 15 * sizeof(rt_node *),
		.leaf_size = sizeof(rt_node_leaf_32) + 15 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_inner_32) + 15 * sizeof(rt_node *)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_leaf_32) + 15 * sizeof(uint64)),
	},
	[RT_CLASS_32_FULL] = {
		.name = "radix tree node 32",
		.fanout = 32,
		.inner_size = sizeof(rt_node_inner_32) + 32 * sizeof(rt_node *),
		.leaf_size = sizeof(rt_node_leaf_32) + 32 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_inner_32) + 32 * sizeof(rt_node *)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_leaf_32) + 32 * sizeof(uint64)),
	},
	[RT_CLASS_125_FULL] = {
		.name = "radix tree node 125",
		.fanout = 125,
		.inner_size = sizeof(rt_node_inner_125) + 125 * sizeof(rt_node *),
		.leaf_size = sizeof(rt_node_leaf_125) + 125 * sizeof(uint64),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_inner_125) + 125 * sizeof(rt_node *)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_leaf_125) + 125 * sizeof(uint64)),
	},
	[RT_CLASS_256] = {
		.name = "radix tree node 256",
		.fanout = 256,
		.inner_size = sizeof(rt_node_inner_256),
		.leaf_size = sizeof(rt_node_leaf_256),
		.inner_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_inner_256)),
		.leaf_blocksize = NODE_SLAB_BLOCK_SIZE(sizeof(rt_node_leaf_256)),
	},
};

/* Map from the node kind to its minimum size class */
static rt_size_class kind_min_size_class[RT_NODE_KIND_COUNT] = {
	[RT_NODE_KIND_4] = RT_CLASS_4_FULL,
	[RT_NODE_KIND_32] = RT_CLASS_32_PARTIAL,
	[RT_NODE_KIND_125] = RT_CLASS_125_FULL,
	[RT_NODE_KIND_256] = RT_CLASS_256,
};

/*
 * Iteration support.
 *
 * Iterating the radix tree returns each pair of key and value in the ascending
 * order of the key. To support this, the we iterate nodes of each level.
 *
 * rt_node_iter struct is used to track the iteration within a node.
 *
 * rt_iter is the struct for iteration of the radix tree, and uses rt_node_iter
 * in order to track the iteration of each level. During the iteration, we also
 * construct the key whenever updating the node iteration information, e.g., when
 * advancing the current index within the node or when moving to the next node
 * at the same level.
 *
 * XXX: We need either a safeguard to disallow other processes to begin the
 * iteration while one process is doing or to allow multiple processes to do
 * the iteration.
 */
typedef struct rt_node_iter
{
	rt_node_ptr	node;			/* current node being iterated */
	int			current_idx;	/* current position. -1 for initial value */
} rt_node_iter;

struct rt_iter
{
	radix_tree *tree;

	/* Track the iteration on nodes of each level */
	rt_node_iter stack[RT_MAX_LEVEL];
	int			stack_len;

	/* The key is being constructed during the iteration */
	uint64		key;
};

/* A magic value used to identify our radix tree */
#define RADIXTREE_MAGIC 0x54A48167

/* Control information for an radix tree */
typedef struct radix_tree_control
{
	rt_handle	handle;
	uint32		magic;

	/* Root node */
	rt_pointer	root;

	uint64		max_val;
	uint64		num_keys;

	/* statistics */
#ifdef RT_DEBUG
	int32		cnt[RT_SIZE_CLASS_COUNT];
#endif
} radix_tree_control;

/* A radix tree with nodes */
struct radix_tree
{
	MemoryContext context;

	/* control object in either backend-local memory or DSA */
	radix_tree_control *ctl;

	/* used only when the radix tree is shared */
	dsa_area   *area;

	/* used only when the radix tree is private */
	MemoryContextData *inner_slabs[RT_SIZE_CLASS_COUNT];
	MemoryContextData *leaf_slabs[RT_SIZE_CLASS_COUNT];
};
#define RadixTreeIsShared(rt) ((rt)->area != NULL)

static void rt_new_root(radix_tree *tree, uint64 key);

static rt_node_ptr rt_alloc_node(radix_tree *tree, rt_size_class size_class, bool inner);
static inline void rt_init_node(rt_node_ptr node, uint8 kind, rt_size_class size_class,
								bool inner);
static void rt_free_node(radix_tree *tree, rt_node_ptr node);
static void rt_extend(radix_tree *tree, uint64 key);
static inline bool rt_node_search_inner(rt_node_ptr node_ptr, uint64 key, rt_action action,
										rt_pointer *child_p);
static inline bool rt_node_search_leaf(rt_node_ptr node_ptr, uint64 key, rt_action action,
									   uint64 *value_p);
static bool rt_node_insert_inner(radix_tree *tree, rt_node_ptr parent, rt_node_ptr node,
								 uint64 key, rt_node_ptr child);
static bool rt_node_insert_leaf(radix_tree *tree, rt_node_ptr parent, rt_node_ptr node,
								uint64 key, uint64 value);
static inline bool rt_node_inner_iterate_next(rt_iter *iter, rt_node_iter *node_iter,
											  rt_node_ptr *child_p);
static inline bool rt_node_leaf_iterate_next(rt_iter *iter, rt_node_iter *node_iter,
											 uint64 *value_p);
static void rt_update_iter_stack(rt_iter *iter, rt_node_ptr from_node, int from);
static inline void rt_iter_update_key(rt_iter *iter, uint8 chunk, uint8 shift);

/* verification (available only with assertion) */
static void rt_verify_node(rt_node_ptr node);

/* Decode and encode functions of rt_pointer */
static inline rt_node *
rt_pointer_decode(radix_tree *tree, rt_pointer encoded)
{
	if (RadixTreeIsShared(tree))
		return (rt_node *) dsa_get_address(tree->area, encoded);
	else
		return (rt_node *) encoded;
}

static inline rt_pointer
rt_pointer_encode(rt_node *decoded)
{
	return (rt_pointer) decoded;
}

/* Return a rt_node_ptr created from the given encoded pointer */
static inline rt_node_ptr
rt_node_ptr_encoded(radix_tree *tree, rt_pointer encoded)
{
	return (rt_node_ptr) {
		.encoded = encoded,
			.decoded = rt_pointer_decode(tree, encoded)
			};
}

static inline bool
rt_node_ptr_eq(rt_node_ptr *a, rt_node_ptr *b)
{
	return (a->decoded == b->decoded) && (a->encoded == b->encoded);
}

/*
 * Return index of the first element in 'base' that equals 'key'. Return -1
 * if there is no such element.
 */
static inline int
node_4_search_eq(rt_node_base_4 *node, uint8 chunk)
{
	int			idx = -1;

	for (int i = 0; i < node->n.count; i++)
	{
		if (node->chunks[i] == chunk)
		{
			idx = i;
			break;
		}
	}

	return idx;
}

/*
 * Return index of the chunk to insert into chunks in the given node.
 */
static inline int
node_4_get_insertpos(rt_node_base_4 *node, uint8 chunk)
{
	int			idx;

	for (idx = 0; idx < node->n.count; idx++)
	{
		if (node->chunks[idx] >= chunk)
			break;
	}

	return idx;
}

/*
 * Return index of the first element in 'base' that equals 'key'. Return -1
 * if there is no such element.
 */
static inline int
node_32_search_eq(rt_node_base_32 *node, uint8 chunk)
{
	int			count = node->n.count;
#ifndef USE_NO_SIMD
	Vector8		spread_chunk;
	Vector8		haystack1;
	Vector8		haystack2;
	Vector8		cmp1;
	Vector8		cmp2;
	uint32		bitfield;
	int			index_simd = -1;
#endif

#if defined(USE_NO_SIMD) || defined(USE_ASSERT_CHECKING)
	int			index = -1;

	for (int i = 0; i < count; i++)
	{
		if (node->chunks[i] == chunk)
		{
			index = i;
			break;
		}
	}
#endif

#ifndef USE_NO_SIMD
	spread_chunk = vector8_broadcast(chunk);
	vector8_load(&haystack1, &node->chunks[0]);
	vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
	cmp1 = vector8_eq(spread_chunk, haystack1);
	cmp2 = vector8_eq(spread_chunk, haystack2);
	bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));
	bitfield &= ((UINT64CONST(1) << count) - 1);

	if (bitfield)
		index_simd = pg_rightmost_one_pos32(bitfield);

	Assert(index_simd == index);
	return index_simd;
#else
	return index;
#endif
}

/*
 * Return index of the chunk to insert into chunks in the given node.
 */
static inline int
node_32_get_insertpos(rt_node_base_32 *node, uint8 chunk)
{
	int			count = node->n.count;
#ifndef USE_NO_SIMD
	Vector8		spread_chunk;
	Vector8		haystack1;
	Vector8		haystack2;
	Vector8		cmp1;
	Vector8		cmp2;
	Vector8		min1;
	Vector8		min2;
	uint32		bitfield;
	int			index_simd;
#endif

#if defined(USE_NO_SIMD) || defined(USE_ASSERT_CHECKING)
	int			index;

	for (index = 0; index < count; index++)
	{
		if (node->chunks[index] >= chunk)
			break;
	}
#endif

#ifndef USE_NO_SIMD
	spread_chunk = vector8_broadcast(chunk);
	vector8_load(&haystack1, &node->chunks[0]);
	vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
	min1 = vector8_min(spread_chunk, haystack1);
	min2 = vector8_min(spread_chunk, haystack2);
	cmp1 = vector8_eq(spread_chunk, min1);
	cmp2 = vector8_eq(spread_chunk, min2);
	bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));
	bitfield &= ((UINT64CONST(1) << count) - 1);

	if (bitfield)
		index_simd = pg_rightmost_one_pos32(bitfield);
	else
		index_simd = count;

	Assert(index_simd == index);
	return index_simd;
#else
	return index;
#endif
}

/*
 * Functions to manipulate both chunks array and children/values array.
 * These are used for node-4 and node-32.
 */

/* Shift the elements right at 'idx' by one */
static inline void
chunk_children_array_shift(uint8 *chunks, rt_pointer *children, int count, int idx)
{
	memmove(&(chunks[idx + 1]), &(chunks[idx]), sizeof(uint8) * (count - idx));
	memmove(&(children[idx + 1]), &(children[idx]), sizeof(rt_pointer) * (count - idx));
}

static inline void
chunk_values_array_shift(uint8 *chunks, uint64 *values, int count, int idx)
{
	memmove(&(chunks[idx + 1]), &(chunks[idx]), sizeof(uint8) * (count - idx));
	memmove(&(values[idx + 1]), &(values[idx]), sizeof(uint64 *) * (count - idx));
}

/* Delete the element at 'idx' */
static inline void
chunk_children_array_delete(uint8 *chunks, rt_pointer *children, int count, int idx)
{
	memmove(&(chunks[idx]), &(chunks[idx + 1]), sizeof(uint8) * (count - idx - 1));
	memmove(&(children[idx]), &(children[idx + 1]), sizeof(rt_pointer) * (count - idx - 1));
}

static inline void
chunk_values_array_delete(uint8 *chunks, uint64 *values, int count, int idx)
{
	memmove(&(chunks[idx]), &(chunks[idx + 1]), sizeof(uint8) * (count - idx - 1));
	memmove(&(values[idx]), &(values[idx + 1]), sizeof(uint64) * (count - idx - 1));
}

/* Copy both chunks and children/values arrays */
static inline void
chunk_children_array_copy(uint8 *src_chunks, rt_pointer *src_children,
						  uint8 *dst_chunks, rt_pointer *dst_children)
{
	const int fanout = rt_size_class_info[RT_CLASS_4_FULL].fanout;
	const Size chunk_size = sizeof(uint8) * fanout;
	const Size children_size = sizeof(rt_pointer) * fanout;

	memcpy(dst_chunks, src_chunks, chunk_size);
	memcpy(dst_children, src_children, children_size);
}

static inline void
chunk_values_array_copy(uint8 *src_chunks, uint64 *src_values,
						uint8 *dst_chunks, uint64 *dst_values)
{
	const int fanout = rt_size_class_info[RT_CLASS_4_FULL].fanout;
	const Size chunk_size = sizeof(uint8) * fanout;
	const Size values_size = sizeof(uint64) * fanout;

	memcpy(dst_chunks, src_chunks, chunk_size);
	memcpy(dst_values, src_values, values_size);
}

/* Functions to manipulate inner and leaf node-125 */

/* Does the given chunk in the node has the value? */
static inline bool
node_125_is_chunk_used(rt_node_base_125 *node, uint8 chunk)
{
	return node->slot_idxs[chunk] != RT_NODE_125_INVALID_IDX;
}

#ifdef USE_ASSERT_CHECKING
/* Is the slot in the node used? */
static inline bool
node_inner_125_is_slot_used(rt_node_inner_125 *node, uint8 slot)
{
	Assert(!RT_NODE_IS_LEAF(node));
	Assert(slot < node->base.n.fanout);
	return (node->base.isset[WORDNUM(slot)] & ((bitmapword) 1 << BITNUM(slot))) != 0;
}

static inline bool
node_leaf_125_is_slot_used(rt_node_leaf_125 *node, uint8 slot)
{
	Assert(RT_NODE_IS_LEAF(node));
	Assert(slot < node->base.n.fanout);
	return (node->base.isset[WORDNUM(slot)] & ((bitmapword) 1 << BITNUM(slot))) != 0;
}
#endif

static inline rt_pointer
node_inner_125_get_child(rt_node_inner_125 *node, uint8 chunk)
{
	Assert(!RT_NODE_IS_LEAF(node));
	return node->children[node->base.slot_idxs[chunk]];
}

static inline uint64
node_leaf_125_get_value(rt_node_leaf_125 *node, uint8 chunk)
{
	Assert(RT_NODE_IS_LEAF(node));
	Assert(((rt_node_base_125 *) node)->slot_idxs[chunk] != RT_NODE_125_INVALID_IDX);
	return node->values[node->base.slot_idxs[chunk]];
}

static void
node_inner_125_delete(rt_node_inner_125 *node, uint8 chunk)
{
	int			slotpos = node->base.slot_idxs[chunk];

	Assert(!RT_NODE_IS_LEAF(node));
	node->base.isset[WORDNUM(slotpos)] &= ~((bitmapword) 1 << BITNUM(slotpos));
	node->children[node->base.slot_idxs[chunk]] = InvalidRTPointer;
	node->base.slot_idxs[chunk] = RT_NODE_125_INVALID_IDX;
}

static void
node_leaf_125_delete(rt_node_leaf_125 *node, uint8 chunk)
{
	int			slotpos = node->base.slot_idxs[chunk];

	Assert(RT_NODE_IS_LEAF(node));
	node->base.isset[WORDNUM(slotpos)] &= ~((bitmapword) 1 << BITNUM(slotpos));
	node->base.slot_idxs[chunk] = RT_NODE_125_INVALID_IDX;
}

/* Return an unused slot in node-125 */
static int
node_125_find_unused_slot(bitmapword *isset)
{
	int			slotpos;
	int			idx;
	bitmapword	inverse;

	/* get the first word with at least one bit not set */
	for (idx = 0; idx < WORDNUM(128); idx++)
	{
		if (isset[idx] < ~((bitmapword) 0))
			break;
	}

	/* To get the first unset bit in X, get the first set bit in ~X */
	inverse = ~(isset[idx]);
	slotpos = idx * BITS_PER_BITMAPWORD;
	slotpos += bmw_rightmost_one_pos(inverse);

	/* mark the slot used */
	isset[idx] |= bmw_rightmost_one(inverse);

	return slotpos;
 }

static inline void
node_inner_125_insert(rt_node_inner_125 *node, uint8 chunk, rt_pointer child)
{
	int			slotpos;

	Assert(!RT_NODE_IS_LEAF(node));

	slotpos = node_125_find_unused_slot(node->base.isset);
	Assert(slotpos < node->base.n.fanout);

	node->base.slot_idxs[chunk] = slotpos;
	node->children[slotpos] = child;
}

/* Set the slot at the corresponding chunk */
static inline void
node_leaf_125_insert(rt_node_leaf_125 *node, uint8 chunk, uint64 value)
{
	int			slotpos;

	Assert(RT_NODE_IS_LEAF(node));

	slotpos = node_125_find_unused_slot(node->base.isset);
	Assert(slotpos < node->base.n.fanout);

	node->base.slot_idxs[chunk] = slotpos;
	node->values[slotpos] = value;
}

/* Update the child corresponding to 'chunk' to 'child' */
static inline void
node_inner_125_update(rt_node_inner_125 *node, uint8 chunk, rt_pointer child)
{
	Assert(!RT_NODE_IS_LEAF(node));
	node->children[node->base.slot_idxs[chunk]] = child;
}

static inline void
node_leaf_125_update(rt_node_leaf_125 *node, uint8 chunk, uint64 value)
{
	Assert(RT_NODE_IS_LEAF(node));
	node->values[node->base.slot_idxs[chunk]] = value;
}

/* Functions to manipulate inner and leaf node-256 */

/* Return true if the slot corresponding to the given chunk is in use */
static inline bool
node_inner_256_is_chunk_used(rt_node_inner_256 *node, uint8 chunk)
{
	Assert(!RT_NODE_IS_LEAF(node));
	return RTPointerIsValid(node->children[chunk]);
}

static inline bool
node_leaf_256_is_chunk_used(rt_node_leaf_256 *node, uint8 chunk)
{
	Assert(RT_NODE_IS_LEAF(node));
	return (node->isset[RT_NODE_BITMAP_BYTE(chunk)] & RT_NODE_BITMAP_BIT(chunk)) != 0;
}

static inline rt_pointer
node_inner_256_get_child(rt_node_inner_256 *node, uint8 chunk)
{
	Assert(!RT_NODE_IS_LEAF(node));
	Assert(node_inner_256_is_chunk_used(node, chunk));
	return node->children[chunk];
}

static inline uint64
node_leaf_256_get_value(rt_node_leaf_256 *node, uint8 chunk)
{
	Assert(RT_NODE_IS_LEAF(node));
	Assert(node_leaf_256_is_chunk_used(node, chunk));
	return node->values[chunk];
}

/* Set the child in the node-256 */
static inline void
node_inner_256_set(rt_node_inner_256 *node, uint8 chunk, rt_pointer child)
{
	Assert(!RT_NODE_IS_LEAF(node));
	node->children[chunk] = child;
}

/* Set the value in the node-256 */
static inline void
node_leaf_256_set(rt_node_leaf_256 *node, uint8 chunk, uint64 value)
{
	Assert(RT_NODE_IS_LEAF(node));
	node->isset[RT_NODE_BITMAP_BYTE(chunk)] |= RT_NODE_BITMAP_BIT(chunk);
	node->values[chunk] = value;
}

/* Set the slot at the given chunk position */
static inline void
node_inner_256_delete(rt_node_inner_256 *node, uint8 chunk)
{
	Assert(!RT_NODE_IS_LEAF(node));
	node->children[chunk] = InvalidRTPointer;
}

static inline void
node_leaf_256_delete(rt_node_leaf_256 *node, uint8 chunk)
{
	Assert(RT_NODE_IS_LEAF(node));
	node->isset[RT_NODE_BITMAP_BYTE(chunk)] &= ~(RT_NODE_BITMAP_BIT(chunk));
}

/*
 * Return the shift that is satisfied to store the given key.
 */
static inline int
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
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
rt_new_root(radix_tree *tree, uint64 key)
{
	int			shift = key_get_shift(key);
	bool		inner = shift > 0;
	rt_node_ptr	newnode;

	newnode = rt_alloc_node(tree, RT_CLASS_4_FULL, inner);
	rt_init_node(newnode, RT_NODE_KIND_4, RT_CLASS_4_FULL, inner);
	NODE_SHIFT(newnode) = shift;

	tree->ctl->max_val = shift_get_max_val(shift);
	tree->ctl->root = newnode.encoded;
}

/*
 * Allocate a new node with the given node kind.
 */
static rt_node_ptr
rt_alloc_node(radix_tree *tree, rt_size_class size_class, bool inner)
{
	rt_node_ptr newnode;

	if (tree->area != NULL)
	{
		dsa_pointer dp;

		if (inner)
			dp = dsa_allocate(tree->area, rt_size_class_info[size_class].inner_size);
		else
			dp = dsa_allocate(tree->area, rt_size_class_info[size_class].leaf_size);

		newnode.encoded = (rt_pointer) dp;
		newnode.decoded = rt_pointer_decode(tree, newnode.encoded);
	}
	else
	{
		if (inner)
			newnode.decoded = (rt_node *) MemoryContextAlloc(tree->inner_slabs[size_class],
															 rt_size_class_info[size_class].inner_size);
		else
			newnode.decoded = (rt_node *) MemoryContextAlloc(tree->leaf_slabs[size_class],
															 rt_size_class_info[size_class].leaf_size);

		newnode.encoded = rt_pointer_encode(newnode.decoded);
	}

#ifdef RT_DEBUG
	/* update the statistics */
	tree->ctl->cnt[size_class]++;
#endif

	return newnode;
}

/* Initialize the node contents */
static inline void
rt_init_node(rt_node_ptr node, uint8 kind, rt_size_class size_class, bool inner)
{
	if (inner)
		MemSet(node.decoded, 0, rt_size_class_info[size_class].inner_size);
	else
		MemSet(node.decoded, 0, rt_size_class_info[size_class].leaf_size);

	NODE_KIND(node) = kind;
	NODE_FANOUT(node) = rt_size_class_info[size_class].fanout;

	/* Initialize slot_idxs to invalid values */
	if (kind == RT_NODE_KIND_125)
	{
		rt_node_base_125 *n125 = (rt_node_base_125 *) node.decoded;

		memset(n125->slot_idxs, RT_NODE_125_INVALID_IDX, sizeof(n125->slot_idxs));
	}

	/*
	 * Technically it's 256, but we cannot store that in a uint8,
	 * and this is the max size class to it will never grow.
	 */
	if (kind == RT_NODE_KIND_256)
		NODE_FANOUT(node) = 0;
}

static inline void
rt_copy_node(rt_node_ptr newnode, rt_node_ptr oldnode)
{
	NODE_SHIFT(newnode) = NODE_SHIFT(oldnode);
	NODE_CHUNK(newnode) = NODE_CHUNK(oldnode);
	NODE_COUNT(newnode) = NODE_COUNT(oldnode);
}

/*
 * Create a new node with 'new_kind' and the same shift, chunk, and
 * count of 'node'.
 */
static rt_node_ptr
rt_grow_node_kind(radix_tree *tree, rt_node_ptr node, uint8 new_kind)
{
	rt_node_ptr	newnode;
	bool inner = !NODE_IS_LEAF(node);

	newnode = rt_alloc_node(tree, kind_min_size_class[new_kind], inner);
	rt_init_node(newnode, new_kind, kind_min_size_class[new_kind], inner);
	rt_copy_node(newnode, node);

	return newnode;
}

/* Free the given node */
static void
rt_free_node(radix_tree *tree, rt_node_ptr node)
{
	/* If we're deleting the root node, make the tree empty */
	if (tree->ctl->root == node.encoded)
	{
		tree->ctl->root = InvalidRTPointer;
		tree->ctl->max_val = 0;
	}

#ifdef RT_DEBUG
	{
		int i;

		/* update the statistics */
		for (i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		{
			if (NODE_FANOUT(node) == rt_size_class_info[i].fanout)
				break;
		}

		/* fanout of node256 is intentionally 0 */
		if (i == RT_SIZE_CLASS_COUNT)
			i = RT_CLASS_256;

		tree->ctl->cnt[i]--;
		Assert(tree->ctl->cnt[i] >= 0);
	}
#endif

	if (RadixTreeIsShared(tree))
		dsa_free(tree->area, (dsa_pointer) node.encoded);
	else
		pfree(node.decoded);
}

/*
 * Replace old_child with new_child, and free the old one.
 */
static void
rt_replace_node(radix_tree *tree, rt_node_ptr parent, rt_node_ptr old_child,
				rt_node_ptr new_child, uint64 key)
{
	Assert(NODE_CHUNK(old_child) == NODE_CHUNK(new_child));
	Assert(NODE_SHIFT(old_child) == NODE_SHIFT(new_child));

	if (rt_node_ptr_eq(&parent, &old_child))
	{
		/* Replace the root node with the new large node */
		tree->ctl->root = new_child.encoded;
	}
	else
	{
		bool		replaced PG_USED_FOR_ASSERTS_ONLY;

		replaced = rt_node_insert_inner(tree, InvalidRTNodePtr, parent, key,
										new_child);
		Assert(replaced);
	}

	rt_free_node(tree, old_child);
}

/*
 * The radix tree doesn't sufficient height. Extend the radix tree so it can
 * store the key.
 */
static void
rt_extend(radix_tree *tree, uint64 key)
{
	int			target_shift;
	rt_node		*root = rt_pointer_decode(tree, tree->ctl->root);
	int			shift = root->shift + RT_NODE_SPAN;

	target_shift = key_get_shift(key);

	/* Grow tree from 'shift' to 'target_shift' */
	while (shift <= target_shift)
	{
		rt_node_ptr	node;
		rt_node_inner_4 *n4;

		node = rt_alloc_node(tree, RT_CLASS_4_FULL, true);
		rt_init_node(node, RT_NODE_KIND_4, RT_CLASS_4_FULL, true);

		n4 = (rt_node_inner_4 *) node.decoded;
		n4->base.n.shift = shift;
		n4->base.n.count = 1;
		n4->base.chunks[0] = 0;
		n4->children[0] = tree->ctl->root;

		root->chunk = 0;
		tree->ctl->root = node.encoded;

		shift += RT_NODE_SPAN;
	}

	tree->ctl->max_val = shift_get_max_val(target_shift);
}

/*
 * The radix tree doesn't have inner and leaf nodes for given key-value pair.
 * Insert inner and leaf nodes from 'node' to bottom.
 */
static inline void
rt_set_extend(radix_tree *tree, uint64 key, uint64 value, rt_node_ptr parent,
			  rt_node_ptr node)
{
	int			shift = NODE_SHIFT(node);

	while (shift >= RT_NODE_SPAN)
	{
		rt_node_ptr    newchild;
		int			newshift = shift - RT_NODE_SPAN;
		bool		inner = newshift > 0;

		newchild = rt_alloc_node(tree, RT_CLASS_4_FULL, inner);
		rt_init_node(newchild, RT_NODE_KIND_4, RT_CLASS_4_FULL, inner);
		NODE_SHIFT(newchild) = newshift;
		NODE_CHUNK(newchild) = RT_GET_KEY_CHUNK(key, NODE_SHIFT(node));

		rt_node_insert_inner(tree, parent, node, key, newchild);

		parent = node;
		node = newchild;
		shift -= RT_NODE_SPAN;
	}

	rt_node_insert_leaf(tree, parent, node, key, value);
	tree->ctl->num_keys++;
}

/*
 * Search for the child pointer corresponding to 'key' in the given node, and
 * do the specified 'action'.
 *
 * Return true if the key is found, otherwise return false. On success, the child
 * pointer is set to child_p.
 */
static inline bool
rt_node_search_inner(rt_node_ptr node, uint64 key, rt_action action, rt_pointer *child_p)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, NODE_SHIFT(node));
	bool		found = false;
	rt_pointer	child = InvalidRTPointer;

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_inner_4 *n4 = (rt_node_inner_4 *) node.decoded;
				int			idx = node_4_search_eq((rt_node_base_4 *) n4, chunk);

				if (idx < 0)
					break;

				found = true;

				if (action == RT_ACTION_FIND)
					child = n4->children[idx];
				else			/* RT_ACTION_DELETE */
					chunk_children_array_delete(n4->base.chunks, n4->children,
												n4->base.n.count, idx);

				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_inner_32 *n32 = (rt_node_inner_32 *) node.decoded;
				int			idx = node_32_search_eq((rt_node_base_32 *) n32, chunk);

				if (idx < 0)
					break;

				found = true;

				if (action == RT_ACTION_FIND)
					child = n32->children[idx];
				else			/* RT_ACTION_DELETE */
					chunk_children_array_delete(n32->base.chunks, n32->children,
												n32->base.n.count, idx);
				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_inner_125 *n125 = (rt_node_inner_125 *) node.decoded;

				if (!node_125_is_chunk_used((rt_node_base_125 *) n125, chunk))
					break;

				found = true;

				if (action == RT_ACTION_FIND)
					child = node_inner_125_get_child(n125, chunk);
				else			/* RT_ACTION_DELETE */
					node_inner_125_delete(n125, chunk);

				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_inner_256 *n256 = (rt_node_inner_256 *) node.decoded;

				if (!node_inner_256_is_chunk_used(n256, chunk))
					break;

				found = true;
				if (action == RT_ACTION_FIND)
					child = node_inner_256_get_child(n256, chunk);
				else			/* RT_ACTION_DELETE */
					node_inner_256_delete(n256, chunk);

				break;
			}
	}

	/* update statistics */
	if (action == RT_ACTION_DELETE && found)
		NODE_COUNT(node)--;

	if (found && child_p)
		*child_p = child;

	return found;
}

/*
 * Search for the value corresponding to 'key' in the given node, and do the
 * specified 'action'.
 *
 * Return true if the key is found, otherwise return false. On success, the pointer
 * to the value is set to value_p.
 */
static inline bool
rt_node_search_leaf(rt_node_ptr node, uint64 key, rt_action action, uint64 *value_p)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, NODE_SHIFT(node));
	bool		found = false;
	uint64		value = 0;

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_leaf_4 *n4 = (rt_node_leaf_4 *) node.decoded;
				int			idx = node_4_search_eq((rt_node_base_4 *) n4, chunk);

				if (idx < 0)
					break;

				found = true;

				if (action == RT_ACTION_FIND)
					value = n4->values[idx];
				else			/* RT_ACTION_DELETE */
					chunk_values_array_delete(n4->base.chunks, (uint64 *) n4->values,
											  n4->base.n.count, idx);

				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_leaf_32 *n32 = (rt_node_leaf_32 *) node.decoded;
				int			idx = node_32_search_eq((rt_node_base_32 *) n32, chunk);

				if (idx < 0)
					break;

				found = true;
				if (action == RT_ACTION_FIND)
					value = n32->values[idx];
				else			/* RT_ACTION_DELETE */
					chunk_values_array_delete(n32->base.chunks, (uint64 *) n32->values,
											  n32->base.n.count, idx);
				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_leaf_125 *n125 = (rt_node_leaf_125 *) node.decoded;

				if (!node_125_is_chunk_used((rt_node_base_125 *) n125, chunk))
					break;

				found = true;

				if (action == RT_ACTION_FIND)
					value = node_leaf_125_get_value(n125, chunk);
				else			/* RT_ACTION_DELETE */
					node_leaf_125_delete(n125, chunk);

				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node.decoded;

				if (!node_leaf_256_is_chunk_used(n256, chunk))
					break;

				found = true;
				if (action == RT_ACTION_FIND)
					value = node_leaf_256_get_value(n256, chunk);
				else			/* RT_ACTION_DELETE */
					node_leaf_256_delete(n256, chunk);

				break;
			}
	}

	/* update statistics */
	if (action == RT_ACTION_DELETE && found)
		NODE_COUNT(node)--;

	if (found && value_p)
		*value_p = value;

	return found;
}

/* Insert the child to the inner node */
static bool
rt_node_insert_inner(radix_tree *tree, rt_node_ptr parent, rt_node_ptr node,
					 uint64 key, rt_node_ptr child)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, NODE_SHIFT(node));
	bool		chunk_exists = false;

	Assert(!NODE_IS_LEAF(node));

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_inner_4 *n4 = (rt_node_inner_4 *) node.decoded;
				int			idx;

				idx = node_4_search_eq((rt_node_base_4 *) n4, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
					n4->children[idx] = child.encoded;
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n4)))
				{
					rt_node_ptr	new;
					rt_node_inner_32 *new32;

					Assert(RTNodePtrIsValid(parent));

					/* grow node from 4 to 32 */
					new = rt_grow_node_kind(tree, node, RT_NODE_KIND_32);
					new32 = (rt_node_inner_32 *) new.decoded;

					chunk_children_array_copy(n4->base.chunks, n4->children,
											  new32->base.chunks, new32->children);

					Assert(RTNodePtrIsValid(parent));
					rt_replace_node(tree, parent, node, new, key);
					node = new;
				}
				else
				{
					int			insertpos = node_4_get_insertpos((rt_node_base_4 *) n4, chunk);
					uint16		count = n4->base.n.count;

					/* shift chunks and children */
					if (count != 0 && insertpos < count)
						chunk_children_array_shift(n4->base.chunks, n4->children,
												   count, insertpos);

					n4->base.chunks[insertpos] = chunk;
					n4->children[insertpos] = child.encoded;
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_32:
			{
				rt_node_inner_32 *n32 = (rt_node_inner_32 *) node.decoded;
				int			idx;

				idx = node_32_search_eq((rt_node_base_32 *) n32, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
					n32->children[idx] = child.encoded;
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n32)))
				{
					Assert(RTNodePtrIsValid(parent));

					if (n32->base.n.count == rt_size_class_info[RT_CLASS_32_PARTIAL].fanout)
					{
						/* use the same node kind, but expand to the next size class */
						const Size size = rt_size_class_info[RT_CLASS_32_PARTIAL].inner_size;
						const int fanout = rt_size_class_info[RT_CLASS_32_FULL].fanout;
						rt_node_ptr	new;
						rt_node_inner_32 *new32;

						new = rt_alloc_node(tree, RT_CLASS_32_FULL, true);
						new32 = (rt_node_inner_32 *) new.decoded;
						memcpy(new32, n32, size);
						new32->base.n.fanout = fanout;

						rt_replace_node(tree, parent, node, new, key);

						/*
						 * Must update both pointers here since we update n32 and
						 * verify node.
						 */
						node = new;
						n32 = new32;

						goto retry_insert_inner_32;
					}
					else
					{
						rt_node_ptr	new;
						rt_node_inner_125 *new125;

						/* grow node from 32 to 125 */
						new = rt_grow_node_kind(tree, node, RT_NODE_KIND_125);
						new125 = (rt_node_inner_125 *) new.decoded;

						for (int i = 0; i < n32->base.n.count; i++)
							node_inner_125_insert(new125, n32->base.chunks[i], n32->children[i]);

						rt_replace_node(tree, parent, node, new, key);
						node = new;
					}
				}
				else
				{
retry_insert_inner_32:
					{
						int	insertpos = node_32_get_insertpos((rt_node_base_32 *) n32, chunk);
						int16 count = n32->base.n.count;

						if (count != 0 && insertpos < count)
							chunk_children_array_shift(n32->base.chunks, n32->children,
													   count, insertpos);

						n32->base.chunks[insertpos] = chunk;
						n32->children[insertpos] = child.encoded;
						break;
					}
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_125:
			{
				rt_node_inner_125 *n125 = (rt_node_inner_125 *) node.decoded;
				int			cnt = 0;

				if (node_125_is_chunk_used((rt_node_base_125 *) n125, chunk))
				{
					/* found the existing chunk */
					chunk_exists = true;
					node_inner_125_update(n125, chunk, child.encoded);
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n125)))
				{
					rt_node_ptr	new;
					rt_node_inner_256 *new256;

					Assert(RTNodePtrIsValid(parent));

					/* grow node from 125 to 256 */
					new = rt_grow_node_kind(tree, node, RT_NODE_KIND_256);
					new256 = (rt_node_inner_256 *) new.decoded;

					for (int i = 0; i < RT_NODE_MAX_SLOTS && cnt < n125->base.n.count; i++)
					{
						if (!node_125_is_chunk_used((rt_node_base_125 *) n125, i))
							continue;

						node_inner_256_set(new256, i, node_inner_125_get_child(n125, i));
						cnt++;
					}

					rt_replace_node(tree, parent, node, new, key);
					node = new;
				}
				else
				{
					node_inner_125_insert(n125, chunk, child.encoded);
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_256:
			{
				rt_node_inner_256 *n256 = (rt_node_inner_256 *) node.decoded;

				chunk_exists = node_inner_256_is_chunk_used(n256, chunk);
				Assert(chunk_exists || FIXED_NODE_HAS_FREE_SLOT(n256, RT_CLASS_256));

				node_inner_256_set(n256, chunk, child.encoded);
				break;
			}
	}

	/* Update statistics */
	if (!chunk_exists)
		NODE_COUNT(node)++;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	rt_verify_node(node);

	return chunk_exists;
}

/* Insert the value to the leaf node */
static bool
rt_node_insert_leaf(radix_tree *tree, rt_node_ptr parent, rt_node_ptr node,
					uint64 key, uint64 value)
{
	uint8		chunk = RT_GET_KEY_CHUNK(key, NODE_SHIFT(node));
	bool		chunk_exists = false;

	Assert(NODE_IS_LEAF(node));

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_leaf_4 *n4 = (rt_node_leaf_4 *) node.decoded;
				int			idx;

				idx = node_4_search_eq((rt_node_base_4 *) n4, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
					n4->values[idx] = value;
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n4)))
				{
					rt_node_ptr	new;
					rt_node_leaf_32 *new32;

					Assert(RTNodePtrIsValid(parent));

					/* grow node from 4 to 32 */
					new = rt_grow_node_kind(tree, node, RT_NODE_KIND_32);
					new32 = (rt_node_leaf_32 *) new.decoded;
					chunk_values_array_copy(n4->base.chunks, n4->values,
											new32->base.chunks, new32->values);
					rt_replace_node(tree, parent, node, new, key);
					node = new;
				}
				else
				{
					int			insertpos = node_4_get_insertpos((rt_node_base_4 *) n4, chunk);
					int			count = n4->base.n.count;

					/* shift chunks and values */
					if (count != 0 && insertpos < count)
						chunk_values_array_shift(n4->base.chunks, n4->values,
												 count, insertpos);

					n4->base.chunks[insertpos] = chunk;
					n4->values[insertpos] = value;
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_32:
			{
				rt_node_leaf_32 *n32 = (rt_node_leaf_32 *) node.decoded;
				int			idx;

				idx = node_32_search_eq((rt_node_base_32 *) n32, chunk);
				if (idx != -1)
				{
					/* found the existing chunk */
					chunk_exists = true;
					n32->values[idx] = value;
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n32)))
				{
					Assert(RTNodePtrIsValid(parent));

					if (n32->base.n.count == rt_size_class_info[RT_CLASS_32_PARTIAL].fanout)
					{
						/* use the same node kind, but expand to the next size class */
						const Size size = rt_size_class_info[RT_CLASS_32_PARTIAL].leaf_size;
						const int fanout = rt_size_class_info[RT_CLASS_32_FULL].fanout;
						rt_node_ptr new;
						rt_node_leaf_32 *new32;

						new = rt_alloc_node(tree, RT_CLASS_32_FULL, false);
						new32 = (rt_node_leaf_32 *) new.decoded;
						memcpy(new32, n32, size);
						new32->base.n.fanout = fanout;

						rt_replace_node(tree, parent, node, new, key);

						/*
						 * Must update both pointers here since we update n32 and
						 * verify node.
						 */
						node = new;
						n32 = new32;

						goto retry_insert_leaf_32;
					}
					else
					{
						rt_node_ptr	new;
						rt_node_leaf_125 *new125;

						/* grow node from 32 to 125 */
						new = rt_grow_node_kind(tree, node, RT_NODE_KIND_125);
						new125 = (rt_node_leaf_125 *) new.decoded;

						for (int i = 0; i < n32->base.n.count; i++)
							node_leaf_125_insert(new125, n32->base.chunks[i], n32->values[i]);

						rt_replace_node(tree, parent, node, new, key);
						node = new;
					}
				}
				else
				{
retry_insert_leaf_32:
					{
						int	insertpos = node_32_get_insertpos((rt_node_base_32 *) n32, chunk);
						int	count = n32->base.n.count;

						if (count != 0 && insertpos < count)
							chunk_values_array_shift(n32->base.chunks, n32->values,
													 count, insertpos);

						n32->base.chunks[insertpos] = chunk;
						n32->values[insertpos] = value;
						break;
					}
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_125:
			{
				rt_node_leaf_125 *n125 = (rt_node_leaf_125 *) node.decoded;
				int			cnt = 0;

				if (node_125_is_chunk_used((rt_node_base_125 *) n125, chunk))
				{
					/* found the existing chunk */
					chunk_exists = true;
					node_leaf_125_update(n125, chunk, value);
					break;
				}

				if (unlikely(!VAR_NODE_HAS_FREE_SLOT(n125)))
				{
					rt_node_ptr	new;
					rt_node_leaf_256 *new256;

					Assert(RTNodePtrIsValid(parent));

					/* grow node from 125 to 256 */
					new = rt_grow_node_kind(tree, node, RT_NODE_KIND_256);
					new256 = (rt_node_leaf_256 *) new.decoded;
					for (int i = 0; i < RT_NODE_MAX_SLOTS && cnt < n125->base.n.count; i++)
					{
						if (!node_125_is_chunk_used((rt_node_base_125 *) n125, i))
							continue;

						node_leaf_256_set(new256, i, node_leaf_125_get_value(n125, i));
						cnt++;
					}

					rt_replace_node(tree, parent, node, new, key);
					node = new;
				}
				else
				{
					node_leaf_125_insert(n125, chunk, value);
					break;
				}
			}
			/* FALLTHROUGH */
		case RT_NODE_KIND_256:
			{
				rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node.decoded;

				chunk_exists = node_leaf_256_is_chunk_used(n256, chunk);
				Assert(chunk_exists || FIXED_NODE_HAS_FREE_SLOT(n256, RT_CLASS_256));

				node_leaf_256_set(n256, chunk, value);
				break;
			}
	}

	/* Update statistics */
	if (!chunk_exists)
		NODE_COUNT(node)++;

	/*
	 * Done. Finally, verify the chunk and value is inserted or replaced
	 * properly in the node.
	 */
	rt_verify_node(node);

	return chunk_exists;
}

/*
 * Create the radix tree in the given memory context and return it.
 */
radix_tree *
rt_create(MemoryContext ctx, dsa_area *area)
{
	radix_tree *tree;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = (radix_tree *) palloc0(sizeof(radix_tree));
	tree->context = ctx;

	if (area != NULL)
	{
		dsa_pointer dp;

		tree->area = area;
		dp = dsa_allocate0(area, sizeof(radix_tree_control));
		tree->ctl = (radix_tree_control *) dsa_get_address(area, dp);
		tree->ctl->handle = (rt_handle) dp;
	}
	else
	{
		tree->ctl = (radix_tree_control *) palloc0(sizeof(radix_tree_control));
		tree->ctl->handle = InvalidDsaPointer;
	}

	tree->ctl->magic = RADIXTREE_MAGIC;
	tree->ctl->root = InvalidRTPointer;

	/* Create the slab allocator for each size class */
	if (area == NULL)
	{
		for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		{
			tree->inner_slabs[i] = SlabContextCreate(ctx,
													 rt_size_class_info[i].name,
													 rt_size_class_info[i].inner_blocksize,
													 rt_size_class_info[i].inner_size);
			tree->leaf_slabs[i] = SlabContextCreate(ctx,
													rt_size_class_info[i].name,
													rt_size_class_info[i].leaf_blocksize,
													rt_size_class_info[i].leaf_size);
#ifdef RT_DEBUG
			tree->ctl->cnt[i] = 0;
#endif
		}
	}

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

/*
 * Get a handle that can be used by other processes to attach to this radix
 * tree.
 */
dsa_pointer
rt_get_handle(radix_tree *tree)
{
	Assert(RadixTreeIsShared(tree));
	Assert(tree->ctl->magic == RADIXTREE_MAGIC);

	return tree->ctl->handle;
}

/*
 * Attach to an existing radix tree using a handle. The returned object is
 * allocated in backend-local memory using the CurrentMemoryContext.
 */
radix_tree *
rt_attach(dsa_area *area, rt_handle handle)
{
	radix_tree *tree;
	dsa_pointer	control;

	/* Allocate the backend-local object representing the radix tree */
	tree = (radix_tree *) palloc0(sizeof(radix_tree));

	/* Find the control object in shard memory */
	control = handle;

	/* Set up the local radix tree */
	tree->area = area;
	tree->ctl = (radix_tree_control *) dsa_get_address(area, control);
	Assert(tree->ctl->magic == RADIXTREE_MAGIC);

	return tree;
}

/*
 * Detach from a radix tree. This frees backend-local resources associated
 * with the radix tree, but the radix tree will continue to exist until
 * it is explicitly freed.
 */
void
rt_detach(radix_tree *tree)
{
	Assert(RadixTreeIsShared(tree));
	Assert(tree->ctl->magic == RADIXTREE_MAGIC);

	pfree(tree);
}

/*
 * Recursively free all nodes allocated to the dsa area.
 */
static void
rt_free_recurse(radix_tree *tree, rt_pointer ptr)
{
	rt_node_ptr	node = rt_node_ptr_encoded(tree, ptr);

	Assert(RadixTreeIsShared(tree));

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	/* The leaf node doesn't have child pointers, so free it */
	if (NODE_IS_LEAF(node))
	{
		dsa_free(tree->area, (dsa_pointer) node.encoded);
		return;
	}

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_inner_4 *n4 = (rt_node_inner_4 *) node.decoded;

				/* Free all children recursively */
				for (int i = 0; i < NODE_COUNT(node); i++)
					rt_free_recurse(tree, n4->children[i]);

				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_inner_32 *n32 = (rt_node_inner_32 *) node.decoded;

				/* Free all children recursively */
				for (int i = 0; i < NODE_COUNT(node); i++)
					rt_free_recurse(tree, n32->children[i]);

				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_inner_125 *n125 = (rt_node_inner_125 *) node.decoded;

				/* Free all children recursively */
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_125_is_chunk_used((rt_node_base_125 *) n125, i))
						continue;

					rt_free_recurse(tree, node_inner_125_get_child(n125, i));
				}
				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_inner_256 *n256 = (rt_node_inner_256 *) node.decoded;

				/* Free all children recursively */
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_inner_256_is_chunk_used(n256, i))
						continue;

					rt_free_recurse(tree, node_inner_256_get_child(n256, i));
				}
				break;
			}
	}

	/* Free the inner node itself */
	dsa_free(tree->area, node.encoded);
}

/*
 * Free the given radix tree.
 */
void
rt_free(radix_tree *tree)
{
	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);

	if (RadixTreeIsShared(tree))
	{
		/* Free all memory used for radix tree nodes */
		if (RTPointerIsValid(tree->ctl->root))
			rt_free_recurse(tree, tree->ctl->root);

		/*
		 * Vandalize the control block to help catch programming error where
		 * other backends access the memory formerly occupied by this radix tree.
		 */
		tree->ctl->magic = 0;
		dsa_free(tree->area, tree->ctl->handle);
	}
	else
	{
		/* Free all memory used for radix tree nodes */
		for (int i = 0; i < RT_NODE_KIND_COUNT; i++)
		{
			MemoryContextDelete(tree->inner_slabs[i]);
			MemoryContextDelete(tree->leaf_slabs[i]);
		}
		pfree(tree->ctl);
	}

	pfree(tree);
}

/*
 * Set key to value. If the entry already exists, we update its value to 'value'
 * and return true. Returns false if entry doesn't yet exist.
 */
bool
rt_set(radix_tree *tree, uint64 key, uint64 value)
{
	int			shift;
	bool		updated;
	rt_node_ptr	node;
	rt_node_ptr parent;

	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);

	/* Empty tree, create the root */
	if (!RTPointerIsValid(tree->ctl->root))
		rt_new_root(tree, key);

	/* Extend the tree if necessary */
	if (key > tree->ctl->max_val)
		rt_extend(tree, key);

	/* Descend the tree until a leaf node */
	node = parent = rt_node_ptr_encoded(tree, tree->ctl->root);
	shift = NODE_SHIFT(node);
	while (shift >= 0)
	{
		rt_pointer    child;

		if (NODE_IS_LEAF(node))
			break;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
		{
			rt_set_extend(tree, key, value, parent, node);
			return false;
		}

		parent = node;
		node = rt_node_ptr_encoded(tree, child);
		shift -= RT_NODE_SPAN;
	}

	updated = rt_node_insert_leaf(tree, parent, node, key, value);

	/* Update the statistics */
	if (!updated)
		tree->ctl->num_keys++;

	return updated;
}

/*
 * Search the given key in the radix tree. Return true if there is the key,
 * otherwise return false.  On success, we set the value to *val_p so it must
 * not be NULL.
 */
bool
rt_search(radix_tree *tree, uint64 key, uint64 *value_p)
{
	rt_node_ptr    node;
	int			shift;

	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);
	Assert(value_p != NULL);

	if (!RTPointerIsValid(tree->ctl->root) || key > tree->ctl->max_val)
		return false;

	node = rt_node_ptr_encoded(tree, tree->ctl->root);
	shift = NODE_SHIFT(node);

	/* Descend the tree until a leaf node */
	while (shift >= 0)
	{
		rt_pointer	child;

		if (NODE_IS_LEAF(node))
			break;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			return false;

		node = rt_node_ptr_encoded(tree, child);
		shift -= RT_NODE_SPAN;
	}

	return rt_node_search_leaf(node, key, RT_ACTION_FIND, value_p);
}

/*
 * Delete the given key from the radix tree. Return true if the key is found (and
 * deleted), otherwise do nothing and return false.
 */
bool
rt_delete(radix_tree *tree, uint64 key)
{
	rt_node_ptr	node;
	rt_node_ptr	stack[RT_MAX_LEVEL] = {0};
	int			shift;
	int			level;
	bool		deleted;

	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);

	if (!RTPointerIsValid(tree->ctl->root) || key > tree->ctl->max_val)
		return false;

	/*
	 * Descend the tree to search the key while building a stack of nodes we
	 * visited.
	 */
	node = rt_node_ptr_encoded(tree, tree->ctl->root);
	shift = NODE_SHIFT(node);
	level = -1;
	while (shift > 0)
	{
		rt_pointer	child;

		/* Push the current node to the stack */
		stack[++level] = node;

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			return false;

		node = rt_node_ptr_encoded(tree, child);
		shift -= RT_NODE_SPAN;
	}

	/* Delete the key from the leaf node if exists */
	Assert(NODE_IS_LEAF(node));
	deleted = rt_node_search_leaf(node, key, RT_ACTION_DELETE, NULL);

	if (!deleted)
	{
		/* no key is found in the leaf node */
		return false;
	}

	/* Found the key to delete. Update the statistics */
	tree->ctl->num_keys--;

	/*
	 * Return if the leaf node still has keys and we don't need to delete the
	 * node.
	 */
	if (!NODE_IS_EMPTY(node))
		return true;

	/* Free the empty leaf node */
	rt_free_node(tree, node);

	/* Delete the key in inner nodes recursively */
	while (level >= 0)
	{
		node = stack[level--];

		deleted = rt_node_search_inner(node, key, RT_ACTION_DELETE, NULL);
		Assert(deleted);

		/* If the node didn't become empty, we stop deleting the key */
		if (!NODE_IS_EMPTY(node))
			break;

		/* The node became empty */
		rt_free_node(tree, node);
	}

	return true;
}

/* Create and return the iterator for the given radix tree */
rt_iter *
rt_begin_iterate(radix_tree *tree)
{
	MemoryContext old_ctx;
	rt_node_ptr	root;
	rt_iter    *iter;
	int			top_level;

	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);

	old_ctx = MemoryContextSwitchTo(tree->context);

	iter = (rt_iter *) palloc0(sizeof(rt_iter));
	iter->tree = tree;

	/* empty tree */
	if (!RTPointerIsValid(iter->tree) || !RTPointerIsValid(iter->tree->ctl->root))
		return iter;

	root = rt_node_ptr_encoded(tree, iter->tree->ctl->root);
	top_level = NODE_SHIFT(root) / RT_NODE_SPAN;
	iter->stack_len = top_level;

	/*
	 * Descend to the left most leaf node from the root. The key is being
	 * constructed while descending to the leaf.
	 */
	rt_update_iter_stack(iter, root, top_level);

	MemoryContextSwitchTo(old_ctx);

	return iter;
}

/*
 * Update each node_iter for inner nodes in the iterator node stack.
 */
static void
rt_update_iter_stack(rt_iter *iter, rt_node_ptr from_node, int from)
{
	int			level = from;
	rt_node_ptr node = from_node;

	for (;;)
	{
		rt_node_iter *node_iter = &(iter->stack[level--]);
		bool found PG_USED_FOR_ASSERTS_ONLY;

		node_iter->node = node;
		node_iter->current_idx = -1;

		/* We don't advance the leaf node iterator here */
		if (NODE_IS_LEAF(node))
			return;

		/* Advance to the next slot in the inner node */
		found = rt_node_inner_iterate_next(iter, node_iter, &node);

		/* We must find the first children in the node */
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
	Assert(!RadixTreeIsShared(iter->tree) || iter->tree->ctl->magic == RADIXTREE_MAGIC);

	/* Empty tree */
	if (!iter->tree->ctl->root)
		return false;

	for (;;)
	{
		rt_node_ptr	child = InvalidRTNodePtr;
		uint64		value;
		int			level;
		bool		found;

		/* Advance the leaf node iterator to get next key-value pair */
		found = rt_node_leaf_iterate_next(iter, &(iter->stack[0]), &value);

		if (found)
		{
			*key_p = iter->key;
			*value_p = value;
			return true;
		}

		/*
		 * We've visited all values in the leaf node, so advance inner node
		 * iterators from the level=1 until we find the next child node.
		 */
		for (level = 1; level <= iter->stack_len; level++)
		{
			if (rt_node_inner_iterate_next(iter, &(iter->stack[level]), &child))
				break;
		}

		/* the iteration finished */
		if (!RTNodePtrIsValid(child))
			return false;

		/*
		 * Set the node to the node iterator and update the iterator stack
		 * from this node.
		 */
		rt_update_iter_stack(iter, child, level - 1);

		/* Node iterators are updated, so try again from the leaf */
	}

	return false;
}

void
rt_end_iterate(rt_iter *iter)
{
	pfree(iter);
}

static inline void
rt_iter_update_key(rt_iter *iter, uint8 chunk, uint8 shift)
{
	iter->key &= ~(((uint64) RT_CHUNK_MASK) << shift);
	iter->key |= (((uint64) chunk) << shift);
}

/*
 * Advance the slot in the inner node. Return the child if exists, otherwise
 * null.
 */
static inline bool
rt_node_inner_iterate_next(rt_iter *iter, rt_node_iter *node_iter, rt_node_ptr *child_p)
{
	rt_node_ptr	node = node_iter->node;
	rt_pointer	child;
	bool		found = false;
	uint8		key_chunk;

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_inner_4 *n4 = (rt_node_inner_4 *) node.decoded;

				node_iter->current_idx++;
				if (node_iter->current_idx >= n4->base.n.count)
					break;

				child = n4->children[node_iter->current_idx];
				key_chunk = n4->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_inner_32 *n32 = (rt_node_inner_32 *) node.decoded;

				node_iter->current_idx++;
				if (node_iter->current_idx >= n32->base.n.count)
					break;

				child = n32->children[node_iter->current_idx];
				key_chunk = n32->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_inner_125 *n125 = (rt_node_inner_125 *) node.decoded;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (node_125_is_chunk_used((rt_node_base_125 *) n125, i))
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
				child = node_inner_125_get_child(n125, i);
				key_chunk = i;
				found = true;
				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_inner_256 *n256 = (rt_node_inner_256 *) node.decoded;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (node_inner_256_is_chunk_used(n256, i))
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
				child = node_inner_256_get_child(n256, i);
				key_chunk = i;
				found = true;
				break;
			}
	}

	if (found)
	{
		rt_iter_update_key(iter, key_chunk, NODE_SHIFT(node));
		*child_p = rt_node_ptr_encoded(iter->tree, child);
	}

	return found;
}

/*
 * Advance the slot in the leaf node. On success, return true and the value
 * is set to value_p, otherwise return false.
 */
static inline bool
rt_node_leaf_iterate_next(rt_iter *iter, rt_node_iter *node_iter, uint64 *value_p)
{
	rt_node_ptr node = node_iter->node;
	bool		found = false;
	uint64		value;
	uint8		key_chunk;

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_leaf_4 *n4 = (rt_node_leaf_4 *) node.decoded;

				node_iter->current_idx++;
				if (node_iter->current_idx >= n4->base.n.count)
					break;

				value = n4->values[node_iter->current_idx];
				key_chunk = n4->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_leaf_32 *n32 = (rt_node_leaf_32 *) node.decoded;

				node_iter->current_idx++;
				if (node_iter->current_idx >= n32->base.n.count)
					break;

				value = n32->values[node_iter->current_idx];
				key_chunk = n32->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_leaf_125 *n125 = (rt_node_leaf_125 *) node.decoded;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (node_125_is_chunk_used((rt_node_base_125 *) n125, i))
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
				value = node_leaf_125_get_value(n125, i);
				key_chunk = i;
				found = true;
				break;
			}
		case RT_NODE_KIND_256:
			{
				rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node.decoded;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (node_leaf_256_is_chunk_used(n256, i))
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
				value = node_leaf_256_get_value(n256, i);
				key_chunk = i;
				found = true;
				break;
			}
	}

	if (found)
	{
		rt_iter_update_key(iter, key_chunk, NODE_SHIFT(node));
		*value_p = value;
	}

	return found;
}

/*
 * Return the number of keys in the radix tree.
 */
uint64
rt_num_entries(radix_tree *tree)
{
	return tree->ctl->num_keys;
}

/*
 * Return the statistics of the amount of memory used by the radix tree.
 */
uint64
rt_memory_usage(radix_tree *tree)
{
	Size		total = sizeof(radix_tree) + sizeof(radix_tree_control);

	Assert(!RadixTreeIsShared(tree) || tree->ctl->magic == RADIXTREE_MAGIC);

	if (RadixTreeIsShared(tree))
		total = dsa_get_total_size(tree->area);
	else
	{
		for (int i = 0; i < RT_NODE_KIND_COUNT; i++)
		{
			total += MemoryContextMemAllocated(tree->inner_slabs[i], true);
			total += MemoryContextMemAllocated(tree->leaf_slabs[i], true);
		}
	}

	return total;
}

/*
 * Verify the radix tree node.
 */
static void
rt_verify_node(rt_node_ptr node)
{
#ifdef USE_ASSERT_CHECKING
	Assert(NODE_COUNT(node) >= 0);

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				rt_node_base_4 *n4 = (rt_node_base_4 *) node.decoded;

				for (int i = 1; i < n4->n.count; i++)
					Assert(n4->chunks[i - 1] < n4->chunks[i]);

				break;
			}
		case RT_NODE_KIND_32:
			{
				rt_node_base_32 *n32 = (rt_node_base_32 *) node.decoded;

				for (int i = 1; i < n32->n.count; i++)
					Assert(n32->chunks[i - 1] < n32->chunks[i]);

				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_base_125 *n125 = (rt_node_base_125 *) node.decoded;
				int			cnt = 0;

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_125_is_chunk_used(n125, i))
						continue;

					/* Check if the corresponding slot is used */
					if (NODE_IS_LEAF(node))
						Assert(node_leaf_125_is_slot_used((rt_node_leaf_125 *) n125,
														  n125->slot_idxs[i]));
					else
						Assert(node_inner_125_is_slot_used((rt_node_inner_125 *) n125,
														   n125->slot_idxs[i]));

					cnt++;
				}

				Assert(n125->n.count == cnt);
				break;
			}
		case RT_NODE_KIND_256:
			{
				if (NODE_IS_LEAF(node))
				{
					rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node.decoded;
					int			cnt = 0;

					for (int i = 0; i < RT_NODE_NSLOTS_BITS(RT_NODE_MAX_SLOTS); i++)
						cnt += pg_popcount32(n256->isset[i]);

					/* Check if the number of used chunk matches */
					Assert(n256->base.n.count == cnt);

					break;
				}
			}
	}
#endif
}

/***************** DEBUG FUNCTIONS *****************/
#ifdef RT_DEBUG
void
rt_stats(radix_tree *tree)
{
	rt_node *root = rt_pointer_decode(tree, tree->ctl->root);

	if (root == NULL)
		return;

	ereport(NOTICE, (errmsg("num_keys = " UINT64_FORMAT ", height = %d, n4 = %u, n15 = %u, n32 = %u, n125 = %u, n256 = %u",
							tree->ctl->num_keys,
							root->shift / RT_NODE_SPAN,
							tree->ctl->cnt[RT_CLASS_4_FULL],
							tree->ctl->cnt[RT_CLASS_32_PARTIAL],
							tree->ctl->cnt[RT_CLASS_32_FULL],
							tree->ctl->cnt[RT_CLASS_125_FULL],
							tree->ctl->cnt[RT_CLASS_256])));
}

static void
rt_dump_node(radix_tree *tree, rt_node_ptr node, int level, bool recurse)
{
	rt_node		*n = node.decoded;
	char		space[128] = {0};

	fprintf(stderr, "[%s] kind %d, fanout %d, count %u, shift %u, chunk 0x%X:\n",
			NODE_IS_LEAF(node) ? "LEAF" : "INNR",

			(n->kind == RT_NODE_KIND_4) ? 4 :
			(n->kind == RT_NODE_KIND_32) ? 32 :
			(n->kind == RT_NODE_KIND_125) ? 125 : 256,
			n->fanout == 0 ? 256 : n->fanout,
			n->count, n->shift, n->chunk);

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	switch (NODE_KIND(node))
	{
		case RT_NODE_KIND_4:
			{
				for (int i = 0; i < NODE_COUNT(node); i++)
				{
					if (NODE_IS_LEAF(node))
					{
						rt_node_leaf_4 *n4 = (rt_node_leaf_4 *) node.decoded;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, n4->base.chunks[i], n4->values[i]);
					}
					else
					{
						rt_node_inner_4 *n4 = (rt_node_inner_4 *) node.decoded;

						fprintf(stderr, "%schunk 0x%X ->",
								space, n4->base.chunks[i]);

						if (recurse)
							rt_dump_node(tree, rt_node_ptr_encoded(tree, n4->children[i]),
										 level + 1, recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_32:
			{
				for (int i = 0; i < NODE_KIND(node); i++)
				{
					if (NODE_IS_LEAF(node))
					{
						rt_node_leaf_32 *n32 = (rt_node_leaf_32 *) node.decoded;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, n32->base.chunks[i], n32->values[i]);
					}
					else
					{
						rt_node_inner_32 *n32 = (rt_node_inner_32 *) node.decoded;

						fprintf(stderr, "%schunk 0x%X ->",
								space, n32->base.chunks[i]);

						if (recurse)
						{
							rt_dump_node(tree, rt_node_ptr_encoded(tree, n32->children[i]),
										 level + 1, recurse);
						}
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_125:
			{
				rt_node_base_125 *b125 = (rt_node_base_125 *) node.decoded;

				fprintf(stderr, "slot_idxs ");
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_125_is_chunk_used(b125, i))
						continue;

					fprintf(stderr, " [%d]=%d, ", i, b125->slot_idxs[i]);
				}
				if (NODE_IS_LEAF(node))
				{
					rt_node_leaf_125 *n = (rt_node_leaf_125 *) node.decoded;

					fprintf(stderr, ", isset-bitmap:");
					for (int i = 0; i < WORDNUM(128); i++)
					{
						fprintf(stderr, UINT64_FORMAT_HEX " ", n->base.isset[i]);
					}
					fprintf(stderr, "\n");
				}

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!node_125_is_chunk_used(b125, i))
						continue;

					if (NODE_IS_LEAF(node))
					{
						rt_node_leaf_125 *n125 = (rt_node_leaf_125 *) b125;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, i, node_leaf_125_get_value(n125, i));
					}
					else
					{
						rt_node_inner_125 *n125 = (rt_node_inner_125 *) b125;

						fprintf(stderr, "%schunk 0x%X ->",
								space, i);

						if (recurse)
							rt_dump_node(tree,
										 rt_node_ptr_encoded(tree,
															 node_inner_125_get_child(n125, i)),
										 level + 1, recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
		case RT_NODE_KIND_256:
			{
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (NODE_IS_LEAF(node))
					{
						rt_node_leaf_256 *n256 = (rt_node_leaf_256 *) node.decoded;

						if (!node_leaf_256_is_chunk_used(n256, i))
							continue;

						fprintf(stderr, "%schunk 0x%X value 0x" UINT64_FORMAT_HEX "\n",
								space, i, node_leaf_256_get_value(n256, i));
					}
					else
					{
						rt_node_inner_256 *n256 = (rt_node_inner_256 *) node.decoded;

						if (!node_inner_256_is_chunk_used(n256, i))
							continue;

						fprintf(stderr, "%schunk 0x%X ->",
								space, i);

						if (recurse)
							rt_dump_node(tree,
										 rt_node_ptr_encoded(tree,
															 node_inner_256_get_child(n256, i)),
										 level + 1, recurse);
						else
							fprintf(stderr, "\n");
					}
				}
				break;
			}
	}
}

void
rt_dump_search(radix_tree *tree, uint64 key)
{
	rt_node_ptr node;
	int			shift;
	int			level = 0;

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = " UINT64_FORMAT "(0x" UINT64_FORMAT_HEX ")",
		 tree->ctl->max_val, tree->ctl->max_val);

	if (!RTPointerIsValid(tree->ctl->root))
	{
		elog(NOTICE, "tree is empty");
		return;
	}

	if (key > tree->ctl->max_val)
	{
		elog(NOTICE, "key " UINT64_FORMAT "(0x" UINT64_FORMAT_HEX ") is larger than max val",
			 key, key);
		return;
	}

	node = rt_node_ptr_encoded(tree, tree->ctl->root);
	shift = NODE_SHIFT(node);
	while (shift >= 0)
	{
		rt_pointer   child;

		rt_dump_node(tree, node, level, false);

		if (NODE_IS_LEAF(node))
		{
			uint64		dummy;

			/* We reached at a leaf node, find the corresponding slot */
			rt_node_search_leaf(node, key, RT_ACTION_FIND, &dummy);

			break;
		}

		if (!rt_node_search_inner(node, key, RT_ACTION_FIND, &child))
			break;

		node = rt_node_ptr_encoded(tree, child);
		shift -= RT_NODE_SPAN;
		level++;
	}
}

void
rt_dump(radix_tree *tree)
{
	rt_node_ptr root;

	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		fprintf(stderr, "%s\tinner_size %zu\tinner_blocksize %zu\tleaf_size %zu\tleaf_blocksize %zu\n",
				rt_size_class_info[i].name,
				rt_size_class_info[i].inner_size,
				rt_size_class_info[i].inner_blocksize,
				rt_size_class_info[i].leaf_size,
				rt_size_class_info[i].leaf_blocksize);
	fprintf(stderr, "max_val = " UINT64_FORMAT "\n", tree->ctl->max_val);

	if (!RTPointerIsValid(tree->ctl->root))
	{
		fprintf(stderr, "empty tree\n");
		return;
	}

	root = rt_node_ptr_encoded(tree, tree->ctl->root);
	rt_dump_node(tree, root, 0, true);
}
#endif
