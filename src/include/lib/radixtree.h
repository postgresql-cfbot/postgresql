/*-------------------------------------------------------------------------
 *
 * radixtree.h
 *		Template for adaptive radix tree.
 *
 * This module employs the idea from the paper "The Adaptive Radix Tree: ARTful
 * Indexing for Main-Memory Databases" by Viktor Leis, Alfons Kemper, and Thomas
 * Neumann, 2013. The radix tree uses adaptive node sizes, a small number of node
 * types, each with a different numbers of elements. Depending on the number of
 * children, the appropriate node type is used.
 *
 * WIP: notes about traditional radix tree trading off span vs height...
 *
 * The ART paper mentions three ways to implement leaves:
 *
 * "- Single-value leaves: The values are stored using an addi-
 *  tional leaf node type which stores one value.
 *  - Multi-value leaves: The values are stored in one of four
 *  different leaf node types, which mirror the structure of
 *  inner nodes, but contain values instead of pointers.
 *  - Combined pointer/value slots: If values fit into point-
 *  ers, no separate node types are necessary. Instead, each
 *  pointer storage location in an inner node can either
 *  store a pointer or a value."
 *
 * We use "combined pointer/value slots", as recommended. Values of size equal or smaller
 * than the platform's pointer type are stored in the child slots of the last level node,
 * while larger values are the same as 'single-value' leaves above.
 * This offers flexibility and efficiency.
 *
 * For simplicity, the key is assumed to be 64-bit unsigned integer (FIXME). The
 * tree doesn't need to contain paths where the highest bytes of all keys
 * are zero. That way, the tree's height adapts to the distribution of keys.
 *
 * There are some optimizations not yet implemented, particularly path
 * compression and lazy path expansion.
 *
 * To handle concurrency, we use a single reader-writer lock for the radix
 * tree. The radix tree is exclusively locked during write operations such
 * as RT_SET() and RT_DELETE(), and shared locked during read operations
 * such as RT_SEARCH(). An iteration also holds the shared lock on the radix
 * tree until it is completed.
 *
 * TODO: The current locking mechanism is not optimized for high concurrency
 * with mixed read-write workloads. In the future it might be worthwhile
 * to replace it with the Optimistic Lock Coupling or ROWEX mentioned in
 * the paper "The ART of Practical Synchronization" by the same authors as
 * the ART paper, 2016.
 *
 * WIP: the radix tree nodes don't shrink.
 *
 * To generate a radix tree and associated functions for a use case several
 * macros have to be #define'ed before this file is included.  Including
 * the file #undef's all those, so a new radix tree can be generated
 * afterwards.
 * The relevant parameters are:
 * - RT_PREFIX - prefix for all symbol names generated. A prefix of 'foo'
 * 	 will result in radix tree type 'foo_radix_tree' and functions like
 *	 'foo_create'/'foo_free' and so forth.
 * - RT_DECLARE - if defined function prototypes and type declarations are
 *	 generated
 * - RT_DEFINE - if defined function definitions are generated
 * - RT_SCOPE - in which scope (e.g. extern, static inline) do function
 *	 declarations reside
 * - RT_VALUE_TYPE - the type of the value.
 *
 * Optional parameters:
 * - RT_SHMEM - if defined, the radix tree is created in the DSA area
 *	 so that multiple processes can access it simultaneously.
 * - RT_DEBUG - if defined add stats tracking and debugging functions
 *
 * Interface
 * ---------
 *
 * RT_CREATE		- Create a new, empty radix tree
 * RT_FREE			- Free the radix tree
 * RT_SEARCH		- Search a key-value pair
 * RT_SET			- Set a key-value pair
 * RT_BEGIN_ITERATE	- Begin iterating through all key-value pairs
 * RT_ITERATE_NEXT	- Return next key-value pair, if any
 * RT_END_ITERATE	- End iteration
 * RT_MEMORY_USAGE	- Get the memory usage
 *
 * Interface for Shared Memory
 * ---------
 *
 * RT_ATTACH		- Attach to the radix tree
 * RT_DETACH		- Detach from the radix tree
 * RT_GET_HANDLE	- Return the handle of the radix tree
 *
 * Optional Interface
 * ---------
 *
 * FIXME: just declare the functions unused
 * RT_DELETE		- Delete a key-value pair. Declared/define if RT_USE_DELETE is defined
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/include/lib/radixtree.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "port/pg_bitutils.h"
#include "port/simd.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/* helpers */
#define RT_MAKE_PREFIX(a) CppConcat(a,_)
#define RT_MAKE_NAME(name) RT_MAKE_NAME_(RT_MAKE_PREFIX(RT_PREFIX),name)
#define RT_MAKE_NAME_(a,b) CppConcat(a,b)

/* function declarations */
#define RT_CREATE RT_MAKE_NAME(create)
#define RT_FREE RT_MAKE_NAME(free)
#define RT_SEARCH RT_MAKE_NAME(search)
#define RT_FIND RT_MAKE_NAME(find) /* xxx don't need both */
#ifdef RT_SHMEM
#define RT_ATTACH RT_MAKE_NAME(attach)
#define RT_DETACH RT_MAKE_NAME(detach)
#define RT_GET_HANDLE RT_MAKE_NAME(get_handle)
#endif
#define RT_SET RT_MAKE_NAME(set)
#define RT_GET RT_MAKE_NAME(get) /* xxx don't need both */
#define RT_BEGIN_ITERATE RT_MAKE_NAME(begin_iterate)
#define RT_ITERATE_NEXT RT_MAKE_NAME(iterate_next)
#define RT_ITERATE_NEXT_PTR RT_MAKE_NAME(iterate_next_ptr)  /* xxx don't need both */
#define RT_END_ITERATE RT_MAKE_NAME(end_iterate)
#ifdef RT_USE_DELETE
#define RT_DELETE RT_MAKE_NAME(delete)
#endif
#define RT_MEMORY_USAGE RT_MAKE_NAME(memory_usage)
#define RT_DUMP RT_MAKE_NAME(dump)
#define RT_DUMP_NODE RT_MAKE_NAME(dump_node)
#define RT_DUMP_SEARCH RT_MAKE_NAME(dump_search)

#define RT_STATS RT_MAKE_NAME(stats)

/* internal helper functions (no externally visible prototypes) */
#define RT_NEW_ROOT RT_MAKE_NAME(new_root)
#define RT_GET_SLOT_RECURSIVE RT_MAKE_NAME(get_slot_recursive)
#define RT_RECURSIVE_DELETE RT_MAKE_NAME(recursive_delete)
#define RT_ALLOC_NODE RT_MAKE_NAME(alloc_node)
#define RT_ALLOC_LEAF RT_MAKE_NAME(alloc_leaf)
#define RT_FREE_NODE RT_MAKE_NAME(free_node)
#define RT_FREE_LEAF RT_MAKE_NAME(free_leaf)
#define RT_FREE_RECURSE RT_MAKE_NAME(free_recurse)
#define RT_EXTEND_UP RT_MAKE_NAME(extend_up)
#define RT_EXTEND_DOWN RT_MAKE_NAME(extend_down)
#define RT_COPY_COMMON RT_MAKE_NAME(copy_common)
#define RT_PTR_SET_LOCAL RT_MAKE_NAME(ptr_set_local)
#define RT_PTR_ALLOC_IS_VALID RT_MAKE_NAME(ptr_stored_is_valid)
#define RT_NODE_4_SEARCH_EQ RT_MAKE_NAME(node_3_search_eq)
#define RT_NODE_16_SEARCH_EQ RT_MAKE_NAME(node_32_search_eq)
#define RT_NODE_4_GET_INSERTPOS RT_MAKE_NAME(node_3_get_insertpos)
#define RT_NODE_16_GET_INSERTPOS RT_MAKE_NAME(node_32_get_insertpos)
#define RT_CHUNK_CHILDREN_ARRAY_SHIFT RT_MAKE_NAME(chunk_children_array_shift)
#define RT_CHUNK_CHILDREN_ARRAY_DELETE RT_MAKE_NAME(chunk_children_array_delete)
#define RT_CHUNK_CHILDREN_ARRAY_COPY RT_MAKE_NAME(chunk_children_array_copy)
#define RT_CHUNK_VALUES_ARRAY_COPY RT_MAKE_NAME(chunk_values_array_copy)
#define RT_NODE_48_IS_CHUNK_USED RT_MAKE_NAME(node_48_is_chunk_used)
#define RT_NODE_48_GET_CHILD RT_MAKE_NAME(node_48_get_child)
#define RT_NODE_256_IS_CHUNK_USED RT_MAKE_NAME(node_256_is_chunk_used)
#define RT_NODE_256_GET_CHILD RT_MAKE_NAME(node_256_get_child)
#define RT_NODE_256_SET RT_MAKE_NAME(node_256_set)
#define RT_NODE_256_DELETE RT_MAKE_NAME(node_256_delete)
#define RT_KEY_GET_SHIFT RT_MAKE_NAME(key_get_shift)
#define RT_SHIFT_GET_MAX_VAL RT_MAKE_NAME(shift_get_max_val)
#define RT_NODE_SEARCH RT_MAKE_NAME(node_search)
#define RT_NODE_DELETE RT_MAKE_NAME(node_delete)
#define RT_NODE_INSERT RT_MAKE_NAME(node_insert)
#define RT_ADD_CHILD_4 RT_MAKE_NAME(add_child_4)
#define RT_ADD_CHILD_16 RT_MAKE_NAME(add_child_16)
#define RT_ADD_CHILD_48 RT_MAKE_NAME(add_child_48)
#define RT_ADD_CHILD_256 RT_MAKE_NAME(add_child_256)
#define RT_GROW_NODE_4 RT_MAKE_NAME(grow_node_4)
#define RT_GROW_NODE_16 RT_MAKE_NAME(grow_node_16)
#define RT_GROW_NODE_48 RT_MAKE_NAME(grow_node_48)
#define RT_GROW_NODE_256 RT_MAKE_NAME(grow_node_256)
#define RT_REMOVE_CHILD_4 RT_MAKE_NAME(remove_child_4)
#define RT_REMOVE_CHILD_16 RT_MAKE_NAME(remove_child_16)
#define RT_REMOVE_CHILD_48 RT_MAKE_NAME(remove_child_48)
#define RT_REMOVE_CHILD_256 RT_MAKE_NAME(remove_child_256)
#define RT_NODE_ITERATE_NEXT RT_MAKE_NAME(node_iterate_next)
#define RT_ITER_SET_NODE_FROM RT_MAKE_NAME(iter_set_node_from)
#define RT_ITER_UPDATE_KEY RT_MAKE_NAME(iter_update_key)
#define RT_VERIFY_NODE RT_MAKE_NAME(verify_node)

/* type declarations */
#define RT_RADIX_TREE RT_MAKE_NAME(radix_tree)
#define RT_RADIX_TREE_CONTROL RT_MAKE_NAME(radix_tree_control)
#define RT_ITER RT_MAKE_NAME(iter)
#ifdef RT_SHMEM
#define RT_HANDLE RT_MAKE_NAME(handle)
#endif
#define RT_NODE RT_MAKE_NAME(node)
#define RT_NODE_PTR RT_MAKE_NAME(node_ptr)
#define RT_NODE_ITER RT_MAKE_NAME(node_iter)
#define RT_NODE_BASE_4 RT_MAKE_NAME(node_base_4)
#define RT_NODE_BASE_16 RT_MAKE_NAME(node_base_16)
#define RT_NODE_BASE_48 RT_MAKE_NAME(node_base_48)
#define RT_NODE_BASE_256 RT_MAKE_NAME(node_base_256)
#define RT_NODE_4 RT_MAKE_NAME(node_4)
#define RT_NODE_16 RT_MAKE_NAME(node_16)
#define RT_NODE_48 RT_MAKE_NAME(node_48)
#define RT_NODE_256 RT_MAKE_NAME(node_256)
#define RT_SIZE_CLASS RT_MAKE_NAME(size_class)
#define RT_SIZE_CLASS_ELEM RT_MAKE_NAME(size_class_elem)
#define RT_SIZE_CLASS_INFO RT_MAKE_NAME(size_class_info)
#define RT_CLASS_4 RT_MAKE_NAME(class_4)
#define RT_CLASS_16_LO RT_MAKE_NAME(class_32_min)
#define RT_CLASS_16_HI RT_MAKE_NAME(class_32_max)
#define RT_CLASS_48 RT_MAKE_NAME(class_48)
#define RT_CLASS_256 RT_MAKE_NAME(class_256)

/* generate forward declarations necessary to use the radix tree */
#ifdef RT_DECLARE

typedef struct RT_RADIX_TREE RT_RADIX_TREE;
typedef struct RT_ITER RT_ITER;

#ifdef RT_SHMEM
// WIP: do we really need this?
typedef dsa_pointer RT_HANDLE;
#endif

// TODO: get rid of ifdefs -- e.g. for create, pass NULL dsa
#ifdef RT_SHMEM
RT_SCOPE RT_RADIX_TREE * RT_CREATE(MemoryContext ctx, dsa_area *dsa, int tranche_id);
RT_SCOPE RT_RADIX_TREE * RT_ATTACH(dsa_area *dsa, dsa_pointer dp);
RT_SCOPE void RT_DETACH(RT_RADIX_TREE *tree);
RT_SCOPE RT_HANDLE RT_GET_HANDLE(RT_RADIX_TREE *tree);
#else
RT_SCOPE RT_RADIX_TREE * RT_CREATE(MemoryContext ctx);
#endif
RT_SCOPE void RT_FREE(RT_RADIX_TREE *tree);

RT_SCOPE bool RT_SEARCH(RT_RADIX_TREE *tree, uint64 key, RT_VALUE_TYPE *value_p);
RT_SCOPE bool RT_SET(RT_RADIX_TREE *tree, uint64 key, RT_VALUE_TYPE *value_p);
RT_SCOPE RT_VALUE_TYPE * RT_GET(RT_RADIX_TREE *tree, uint64 key, bool *found);
#ifdef RT_USE_DELETE
RT_SCOPE bool RT_DELETE(RT_RADIX_TREE *tree, uint64 key);
#endif

RT_SCOPE RT_ITER * RT_BEGIN_ITERATE(RT_RADIX_TREE *tree);
RT_SCOPE bool RT_ITERATE_NEXT(RT_ITER *iter, uint64 *key_p, RT_VALUE_TYPE *value_p);
RT_SCOPE void RT_END_ITERATE(RT_ITER *iter);

RT_SCOPE uint64 RT_MEMORY_USAGE(RT_RADIX_TREE *tree);

#if 0
RT_SCOPE void RT_DUMP(RT_RADIX_TREE *tree);
RT_SCOPE void RT_DUMP_SEARCH(RT_RADIX_TREE *tree, uint64 key);
#endif

RT_SCOPE void RT_STATS(RT_RADIX_TREE *tree);

#endif							/* RT_DECLARE */


/* generate implementation of the radix tree */
#ifdef RT_DEFINE

/* The number of bits encoded in one tree level */
#define RT_SPAN	BITS_PER_BYTE

/* The number of maximum slots in the node */
#define RT_NODE_MAX_SLOTS (1 << RT_SPAN)

/* Mask for extracting a chunk from the key */
#define RT_CHUNK_MASK ((1 << RT_SPAN) - 1)

/* Maximum shift the radix tree uses */
#define RT_MAX_SHIFT	RT_KEY_GET_SHIFT(UINT64_MAX)

/* Tree level the radix tree uses */
#define RT_MAX_LEVEL	((sizeof(uint64) * BITS_PER_BYTE) / RT_SPAN)

/*
 * Number of bits necessary for isset array in node48.
 * Since bitmapword can be 64 bits, the only values that make sense
 * here are 64 and 128.
 * WIP: The paper uses at most 64 for this node kind, but is it worth going a bit higher?
 */
#define RT_SLOT_IDX_LIMIT (RT_NODE_MAX_SLOTS / 4)

/* Invalid index used in node48 */
#define RT_INVALID_SLOT_IDX	0xFF

/* Get a chunk from the key */
#define RT_GET_KEY_CHUNK(key, shift) ((uint8) (((key) >> (shift)) & RT_CHUNK_MASK))

/* For accessing bitmaps */
#define RT_BM_IDX(x)	((x) / BITS_PER_BITMAPWORD)
#define RT_BM_BIT(x)	((x) % BITS_PER_BITMAPWORD)

/*
 * Node kinds
 *
 * The different node kinds are what make the tree "adaptive".
 *
 * Each node kind is associated with a different datatype and different
 * search/set/delete/iterate algorithms adapted for its size. The largest
 * kind, node256 is basically the same as a traditional radix tree,
 * and would be most wasteful of memory when sparsely populated. The
 * smaller nodes expend some additional CPU time to enable a smaller
 * memory footprint.
 *
 * XXX There are 4 node kinds, and this should never be increased,
 * for several reasons:
 * 1. With 5 or more kinds, gcc tends to use a jump table for switch
 *    statements.
 * 2. The 4 kinds can be represented with 2 bits, so we have the option
 *    in the future to tag the node pointer with the kind, even on
 *    platforms with 32-bit pointers. This might speed up node traversal
 *    in trees with highly random node kinds.
 * 3. We can have multiple size classes per node kind.
 */
#define RT_NODE_KIND_4			0x00
#define RT_NODE_KIND_16			0x01
#define RT_NODE_KIND_48			0x02
#define RT_NODE_KIND_256		0x03
#define RT_NODE_KIND_COUNT		4

/*
 * Calculate the slab blocksize so that we can allocate at least 32 chunks
 * from the block.
 */
#define RT_SLAB_BLOCK_SIZE(size)	\
	Max((SLAB_DEFAULT_BLOCK_SIZE / (size)) * (size), (size) * 32)

/* Common header for all nodes */
typedef struct RT_NODE
{
	/*
	 * Number of children. uint8 is
	 sufficient for all node kinds, because nodes shrink when this number
	 gets lower than some thresold. Since node256 cannot possibly have zero
	 children, we let the counter overflow and we intepret zero as "256" for
	 this node kind.
	 */
	uint8		count;

	/*
	 * Max capacity for the current size class. Storing this in the
	 * node enables multiple size classes per node kind.
	 * Technically, kinds with a single size class don't need this, so we could
	 * keep this in the individual base types, but the code is simpler this way.
	 * Note: node256 is unique in that it cannot possibly have more than a
	 * single size class, so for that kind we store zero, and uint8 is
	 * sufficient for other kinds.
	 */
	uint8		fanout;

	/* Node kind, one per search/set algorithm */
	uint8		kind;
} RT_NODE;


// WIP: do we really need this anymore?
#define RT_PTR_LOCAL RT_NODE *

/* pointer returned by allocation */
#ifdef RT_SHMEM
#define RT_PTR_ALLOC dsa_pointer
#else
#define RT_PTR_ALLOC RT_PTR_LOCAL
#endif


#ifdef RT_SHMEM
#define RT_INVALID_PTR_ALLOC InvalidDsaPointer
#else
#define RT_INVALID_PTR_ALLOC NULL
#endif

// todo: put these in scope
#ifdef RT_SHMEM
#define RT_LOCK_EXCLUSIVE(tree)	LWLockAcquire(&tree->ctl->lock, LW_EXCLUSIVE)
#define RT_LOCK_SHARED(tree)	LWLockAcquire(&tree->ctl->lock, LW_SHARED)
#define RT_UNLOCK(tree)			LWLockRelease(&tree->ctl->lock);
#else
#define RT_LOCK_EXCLUSIVE(tree)	((void) 0)
#define RT_LOCK_SHARED(tree)	((void) 0)
#define RT_UNLOCK(tree)			((void) 0)
#endif

//todo: caller can define function to abbreviate value
#define RT_VALUE_IS_EMBEDDABLE (sizeof(RT_VALUE_TYPE) <= SIZEOF_VOID_P)


#define RT_NODE_MUST_GROW(node) \
	((node)->base.n.count == (node)->base.n.fanout)

#ifdef RT_SHMEM
typedef struct RT_NODE_PTR
#else
typedef union RT_NODE_PTR
#endif
{
	RT_PTR_ALLOC	alloc;
	RT_PTR_LOCAL	local;
} RT_NODE_PTR;

/* max possible key chunks without struct padding */
#define RT_FANOUT_4_HI (8 - sizeof(RT_NODE))

/* equal to two 128-bit SIMD registers, regardless of availability */
#define RT_FANOUT_16_HI	32

/*
 * Base type of each node kind, meaning all except for the child pointer slots.
 * The base types must be able to accommodate the largest size
 * class for variable-sized node kinds.
 */
typedef struct RT_NODE_BASE_4
{
	RT_NODE		n;

	uint8		chunks[RT_FANOUT_4_HI];
} RT_NODE_BASE_4;

typedef struct RT_NODE_BASE_16
{
	RT_NODE		n;

	uint8		chunks[RT_FANOUT_16_HI];
} RT_NODE_BASE_16;

/*
 * node48 uses slot_idx array, an array of RT_NODE_MAX_SLOTS length
 * to store indexes into a second array that contains the values (or
 * child pointers).
 */
typedef struct RT_NODE_BASE_48
{
	RT_NODE		n;

	/* The index of slots for each fanout */
	uint8		slot_idxs[RT_NODE_MAX_SLOTS];

	/* bitmap to track which slots are in use */
	bitmapword		isset[RT_BM_IDX(RT_SLOT_IDX_LIMIT)];
} RT_NODE_BASE_48;

typedef struct RT_NODE_BASE_256
{
	RT_NODE		n;
} RT_NODE_BASE_256;

/*
 * fanout values for each size class
 */

/* WIP: consider 5 with subclass 1 or 2 */
#define RT_FANOUT_4		4
StaticAssertDecl(RT_FANOUT_4 <= RT_FANOUT_4_HI, "watch struct padding");

#if defined(RT_SHMEM)
/* make sure the node16 subclass and node48 fit neatly into a DSA size class */

#if SIZEOF_VOID_P < 8
#define RT_FANOUT_16_LO	((96 - sizeof(RT_NODE_BASE_16)) / sizeof(RT_PTR_ALLOC))
StaticAssertDecl(RT_FANOUT_16_LO == 15, "expect 15"); // wip for demonstration
#define RT_FANOUT_48	((512 - sizeof(RT_NODE_BASE_48)) / sizeof(RT_PTR_ALLOC))
StaticAssertDecl(RT_FANOUT_48 == 61, "expect 61"); // wip for demonstration
#else
#define RT_FANOUT_16_LO	((160 - sizeof(RT_NODE_BASE_16)) / sizeof(RT_PTR_ALLOC))
StaticAssertDecl(RT_FANOUT_16_LO == 15, "expect 15"); // wip for demonstration
#define RT_FANOUT_48	((768 - sizeof(RT_NODE_BASE_48)) / sizeof(RT_PTR_ALLOC))
StaticAssertDecl(RT_FANOUT_48 == 62, "expect 62"); // wip for demonstration
#endif /* SIZEOF_VOID_P < 8 */

#else /* ! RT_SHMEM */

/* doesn't really matter, but may as well use the namesake */
#define RT_FANOUT_16_LO	16
/* use maximum posible */
#define RT_FANOUT_48	RT_SLOT_IDX_LIMIT

#endif /* RT_SHMEM */

#define RT_FANOUT_256	256

StaticAssertDecl(RT_FANOUT_16_LO < RT_FANOUT_16_HI, "LO subclass bigger than HI");
StaticAssertDecl(RT_FANOUT_48 <= RT_SLOT_IDX_LIMIT, "more slots than isset bits");

/*
 * Node structs, one for each "kind"
 */
typedef struct RT_NODE_4
{
	RT_NODE_BASE_4 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_4;

typedef struct RT_NODE_16
{
	RT_NODE_BASE_16 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_16;

typedef struct RT_NODE_48
{
	RT_NODE_BASE_48 base;

	/* number of children depends on size class */
	RT_PTR_ALLOC children[FLEXIBLE_ARRAY_MEMBER];
} RT_NODE_48;

/*
 * node-256 is the largest node type. This node has an array
 * of children/values directly indexed by chunk.
 * Unlike other node kinds, its array size is by definition
 * fixed.
 */
typedef struct RT_NODE_256
{
	RT_NODE_BASE_256 base;

	/*
	 * Zero is a valid value for embedded values, so we use a
	 * bitmap to track which slots are in use.
	 * WIP: put into base type for consistency?
	 */
	bitmapword	isset[RT_BM_IDX(RT_NODE_MAX_SLOTS)];

	/* Slots for 256 children */
	RT_PTR_ALLOC children[RT_NODE_MAX_SLOTS];
} RT_NODE_256;

/*
 * Node size classes
 *
 * Nodes of different kinds necessarily belong to different size classes.
 * The main innovation in our implementation compared to the ART paper
 * is decoupling the notion of size class from kind.
 *
 * The size classes within a given node kind have the same underlying
 * type, but a variable number of children/values. This is possible
 * because the base type contains small fixed data structures that
 * work the same way regardless of how many child slots there are. We store the
 * node's allocated capacity in the "fanout" member of RT_NODE, to allow
 * runtime introspection.
 */
typedef enum RT_SIZE_CLASS
{
	RT_CLASS_4 = 0,
	RT_CLASS_16_LO,
	RT_CLASS_16_HI,
	RT_CLASS_48,
	RT_CLASS_256
} RT_SIZE_CLASS;

/* Information for each size class */
typedef struct RT_SIZE_CLASS_ELEM
{
	const char *name;
	int			fanout;
	size_t		allocsize;
} RT_SIZE_CLASS_ELEM;

// todo: adjust name automatically - scanf()?
// can we just use the the name of the fanout, instead of a platform/shmem-dependent value?
static const RT_SIZE_CLASS_ELEM RT_SIZE_CLASS_INFO[] = {
	[RT_CLASS_4] = {
		.name = "radix tree node 3",
		.fanout = RT_FANOUT_4,
		.allocsize = sizeof(RT_NODE_4) + RT_FANOUT_4 * sizeof(RT_PTR_ALLOC),
	},
	[RT_CLASS_16_LO] = {
		.name = "radix tree node 15",
		.fanout = RT_FANOUT_16_LO,
		.allocsize = sizeof(RT_NODE_16) + RT_FANOUT_16_LO * sizeof(RT_PTR_ALLOC),
	},
	[RT_CLASS_16_HI] = {
		.name = "radix tree node 32",
		.fanout = RT_FANOUT_16_HI,
		.allocsize = sizeof(RT_NODE_16) + RT_FANOUT_16_HI * sizeof(RT_PTR_ALLOC),
	},
	[RT_CLASS_48] = {
		.name = "radix tree node 125",
		.fanout = RT_FANOUT_48,
		.allocsize = sizeof(RT_NODE_48) + RT_FANOUT_48 * sizeof(RT_PTR_ALLOC),
	},
	[RT_CLASS_256] = {
		.name = "radix tree node 256",
		.fanout = RT_FANOUT_256,
		.allocsize = sizeof(RT_NODE_256),
	},
};

#define RT_SIZE_CLASS_COUNT lengthof(RT_SIZE_CLASS_INFO)

#ifdef RT_SHMEM
/* A magic value used to identify our radix tree */
#define RT_RADIX_TREE_MAGIC 0x54A48167
#endif

/* Contains the actual tree and ancillary info */
typedef struct RT_RADIX_TREE_CONTROL
{
#ifdef RT_SHMEM
	RT_HANDLE	handle;
	uint32		magic;
	LWLock		lock;
#endif

	RT_PTR_ALLOC root;
	uint64		max_val;
	uint64		num_keys;
	int			start_shift;

	/* statistics */
#ifdef RT_DEBUG
	int32		cnt[RT_SIZE_CLASS_COUNT];
	int32		leafcnt;
#endif
} RT_RADIX_TREE_CONTROL;

/* Entry point for allocating and accessing the tree */
typedef struct RT_RADIX_TREE
{
	MemoryContext context;

	/* pointing to either local memory or DSA */
	RT_RADIX_TREE_CONTROL *ctl;

#ifdef RT_SHMEM
	dsa_area   *dsa;
#else
	MemoryContextData *node_slabs[RT_SIZE_CLASS_COUNT];
	MemoryContextData *leaf_slab;
#endif
} RT_RADIX_TREE;

/*
 * Iteration support.
 *
 * Iterating the radix tree returns each pair of key and value in the ascending
 * order of the key.
 *
 * RT_NODE_ITER is the struct for iteration of one radix tree node.
 *
 * RT_ITER is the struct for iteration of the radix tree, and uses RT_NODE_ITER
 * for each level to track the iteration within the node.
 */
typedef struct RT_NODE_ITER
{
	RT_NODE_PTR node;

	/*
	 * The next index of the chunk array in RT_NODE_KIND_4 and
	 * RT_NODE_KIND_16 nodes, or the next chunk in RT_NODE_KIND_48 and
	 * RT_NODE_KIND_256 nodes. 0 for the initial value.
	 */
	int		idx;
} RT_NODE_ITER;

typedef struct RT_ITER
{
	RT_RADIX_TREE *tree;

	/* Track the nodes for each level. level = 0 is for a leaf node */
	RT_NODE_ITER node_iters[RT_MAX_LEVEL];
	int			top_level;

	/* The key constructed during the iteration */
	uint64		key;
} RT_ITER;


static RT_PTR_ALLOC* RT_NODE_INSERT(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
								 uint8 chunk, RT_PTR_ALLOC child);

/* verification (available only with assertion) */
static void RT_VERIFY_NODE(RT_PTR_LOCAL node);

static inline void
RT_PTR_SET_LOCAL(RT_RADIX_TREE *tree, RT_NODE_PTR *node)
{
#ifdef RT_SHMEM
	node->local = dsa_get_address(tree->dsa, node->alloc);
#else
#endif
}

static inline bool
RT_PTR_ALLOC_IS_VALID(RT_PTR_ALLOC ptr)
{
#ifdef RT_SHMEM
	return DsaPointerIsValid(ptr);
#else
	return PointerIsValid(ptr);
#endif
}

/*
 * Return index of the first element in the node's chunk array that equals
 * 'chunk'. Return -1 if there is no such element.
 */
static inline int
RT_NODE_4_SEARCH_EQ(RT_NODE_BASE_4 *node, uint8 chunk)
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
 * Return index of the chunk and slot arrays for inserting into the node,
 * such that the chunk array remains ordered.
 */
static inline int
RT_NODE_4_GET_INSERTPOS(RT_NODE_BASE_4 *node, uint8 chunk)
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
 * Return index of the first element in the node's chunk array that equals
 * 'chunk'. Return -1 if there is no such element.
 * TODO: See if SWAR techniques can be used for non-SIMD platforms.
 */
static inline int
RT_NODE_16_SEARCH_EQ(RT_NODE_BASE_16 *node, uint8 chunk)
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
	/* replicate the search key */
	spread_chunk = vector8_broadcast(chunk);

	/* compare to all 32 keys stored in the node */
	vector8_load(&haystack1, &node->chunks[0]);
	vector8_load(&haystack2, &node->chunks[sizeof(Vector8)]);
	cmp1 = vector8_eq(spread_chunk, haystack1);
	cmp2 = vector8_eq(spread_chunk, haystack2);

	/* convert comparison to a bitfield */
	bitfield = vector8_highbit_mask(cmp1) | (vector8_highbit_mask(cmp2) << sizeof(Vector8));

	/* mask off invalid entries */
	bitfield &= ((UINT64CONST(1) << count) - 1);

	/* convert bitfield to index by counting trailing zeros */
	if (bitfield)
		index_simd = pg_rightmost_one_pos32(bitfield);

	Assert(index_simd == index);
	return index_simd;
#else
	return index;
#endif
}

/*
 * Return index of the chunk and slot arrays for inserting into the node,
 * such that the chunk array remains ordered.
 * TODO: See if SWAR techniques can be used for non-SIMD platforms.
 */
static inline int
RT_NODE_16_GET_INSERTPOS(RT_NODE_BASE_16 *node, uint8 chunk)
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
		/*
		 * This is coded with '>=' to match what we can do with SIMD,
		 * with an assert to keep us honest.
		 */
		if (node->chunks[index] >= chunk)
		{
			Assert(node->chunks[index] != chunk);
			break;
		}
	}
#endif

#ifndef USE_NO_SIMD
	/*
	 * This is a bit more complicated than RT_NODE_16_SEARCH_EQ(), because
	 * no unsigned uint8 comparison instruction exists, at least for SSE2. So
	 * we need to play some trickery using vector8_min() to effectively get
	 * >=. There'll never be any equal elements in current uses, but that's
	 * what we get here...
	 */
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
 * These are used for node-3 and node-32.
 */

/* Shift the elements right at 'idx' by one */
static inline void
RT_CHUNK_CHILDREN_ARRAY_SHIFT(uint8 *chunks, RT_PTR_ALLOC *children, int count, int idx)
{
	/*
	 * This is basically a memmove, but written in a simple loop for speed with
	 * small inputs. The useless comparison with zero is there to confuse
	 * gcc 11+ (-O2) enough to keep it from transforming this to memmove().
	 * WIP: Maybe node16 children should use memmove anyway?
	 *
	 * See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101481
	 */
	for (int i = count - 1; i >= 0; i--)
	{
		if (i >= idx)
		{
			chunks[i+1] = chunks[i];
			children[i+1] = children[i];
		}
		else
			break;
	}
}

/* Delete the element at 'idx' */
// TODO: replace slow memmove's
static inline void
RT_CHUNK_CHILDREN_ARRAY_DELETE(uint8 *chunks, RT_PTR_ALLOC *children, int count, int idx)
{
	memmove(&(chunks[idx]), &(chunks[idx + 1]), sizeof(uint8) * (count - idx - 1));
	memmove(&(children[idx]), &(children[idx + 1]), sizeof(RT_PTR_ALLOC) * (count - idx - 1));
}

/*
 * Copy both chunks and children/values arrays into the right
 * place. The caller is responsible for inserting the new element.
 */
static inline void
RT_CHUNK_CHILDREN_ARRAY_COPY(uint8 *src_chunks, RT_PTR_ALLOC *src_children,
						  uint8 *dst_chunks, RT_PTR_ALLOC *dst_children,
						  int insertpos, int count)
{
	for (int i = 0; i < count; i++)
	{
		/* use a branch-free computation to skip the index of the new element */
		int destidx = i + (i >= insertpos);

		dst_chunks[destidx] = src_chunks[i];
		dst_children[destidx] = src_children[i];
	}
}

/* Functions to manipulate node48 */

/* Does the given chunk in the node has the value? */
static inline bool
RT_NODE_48_IS_CHUNK_USED(RT_NODE_BASE_48 *node, uint8 chunk)
{
	return node->slot_idxs[chunk] != RT_INVALID_SLOT_IDX;
}

static inline RT_PTR_ALLOC*
RT_NODE_48_GET_CHILD(RT_NODE_48 *node, uint8 chunk)
{
	return &node->children[node->base.slot_idxs[chunk]];
}

/* Functions to manipulate inner and leaf node-256 */

/* Return true if the slot corresponding to the given chunk is in use */
static inline bool
RT_NODE_256_IS_CHUNK_USED(RT_NODE_256 *node, uint8 chunk)
{
	int			idx = RT_BM_IDX(chunk);
	int			bitnum = RT_BM_BIT(chunk);

	return (node->isset[idx] & ((bitmapword) 1 << bitnum)) != 0;
}

static inline RT_PTR_ALLOC*
RT_NODE_256_GET_CHILD(RT_NODE_256 *node, uint8 chunk)
{
	Assert(RT_NODE_256_IS_CHUNK_USED(node, chunk));
	return &node->children[chunk];
}

/* Set the child in the node-256 */
static inline void
RT_NODE_256_SET(RT_NODE_256 *node, uint8 chunk, RT_PTR_ALLOC child)
{
	int			idx = RT_BM_IDX(chunk);
	int			bitnum = RT_BM_BIT(chunk);

	node->isset[idx] |= ((bitmapword) 1 << bitnum);
	node->children[chunk] = child;
}

/* Set the slot at the given chunk position */
static inline void
RT_NODE_256_DELETE(RT_NODE_256 *node, uint8 chunk)
{
	int			idx = RT_BM_IDX(chunk);
	int			bitnum = RT_BM_BIT(chunk);

	node->isset[idx] &= ~((bitmapword) 1 << bitnum);
}

/*
 * Return the largest shift that will allowing storing the given key.
 */
static inline int
RT_KEY_GET_SHIFT(uint64 key)
{
	if (key == 0)
		return 0;
	else
		return (pg_leftmost_one_pos64(key) / RT_SPAN) * RT_SPAN;
}

/*
 * Return the max value that can be stored in the tree with the given shift.
 */
static uint64
RT_SHIFT_GET_MAX_VAL(int shift)
{
	if (shift == RT_MAX_SHIFT)
		return UINT64_MAX;

	return (UINT64CONST(1) << (shift + RT_SPAN)) - 1;
}

/*
 * Allocate a new node with the given node kind and size class.
 */
static inline RT_NODE_PTR
RT_ALLOC_NODE(RT_RADIX_TREE *tree, const uint8 kind, const RT_SIZE_CLASS size_class)
{
	RT_NODE_PTR allocnode;
	RT_PTR_LOCAL node;
	size_t allocsize;

		allocsize = RT_SIZE_CLASS_INFO[size_class].allocsize;

#ifdef RT_SHMEM
	allocnode.alloc = dsa_allocate(tree->dsa, allocsize);
#else
	allocnode.alloc = (RT_PTR_ALLOC) MemoryContextAlloc(tree->node_slabs[size_class],
													  allocsize);
#endif

	RT_PTR_SET_LOCAL(tree, &allocnode);
	node = allocnode.local;

	/* initialize contents */

	memset(node, 0, sizeof(RT_NODE));
	switch(kind)
	{
		case RT_NODE_KIND_4:
		case RT_NODE_KIND_16:
			break;
		case RT_NODE_KIND_48:
			{
				RT_NODE_BASE_48 *n48 = (RT_NODE_BASE_48 *) node;

				memset(n48->isset, 0, sizeof(n48->isset));
				memset(n48->slot_idxs, RT_INVALID_SLOT_IDX, sizeof(n48->slot_idxs));
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE_256 *n256 = (RT_NODE_256 *) node;

				memset(n256->isset, 0, sizeof(n256->isset));
				break;
			}
		default:
			pg_unreachable();
	}

	node->kind = kind;
	if (kind == RT_NODE_KIND_256)
		/* See comment for the RT_NODE type */
		// todo remove actual value from lookup table
		Assert(node->fanout == 0);
	else
		node->fanout = RT_SIZE_CLASS_INFO[size_class].fanout;

#ifdef RT_DEBUG
	/* update the statistics */
	tree->ctl->cnt[size_class]++;
#endif

	return allocnode;
}

/*
 * Allocate a new leaf.
 */
static RT_NODE_PTR
RT_ALLOC_LEAF(RT_RADIX_TREE *tree)
{
	RT_NODE_PTR		leaf;
	size_t allocsize = sizeof(RT_VALUE_TYPE);

#ifdef RT_SHMEM
	leaf.alloc = dsa_allocate(tree->dsa, allocsize);
	RT_PTR_SET_LOCAL(tree, &leaf);
#else
	leaf.alloc = (RT_PTR_ALLOC) MemoryContextAlloc(tree->leaf_slab, allocsize);
#endif

	/* likely more efficient than *AllocZero -- first use cases will have small, aligned types */
	MemSet(leaf.local, 0, allocsize);

#ifdef RT_DEBUG
	tree->ctl->leafcnt++;
#endif

	return leaf;
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static pg_noinline void
RT_NEW_ROOT(RT_RADIX_TREE *tree, uint64 key)
{
	int			shift = RT_KEY_GET_SHIFT(key);
	RT_NODE_PTR node;

	node = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);
	tree->ctl->start_shift = shift;
	tree->ctl->max_val = RT_SHIFT_GET_MAX_VAL(shift);
	tree->ctl->root = node.alloc;
}

/*
 * Copy relevant members of the node header.
 * This is a separate function in case other fields are added.
 */
static inline void
RT_COPY_COMMON(RT_NODE_PTR newnode, RT_NODE_PTR oldnode)
{
	(newnode.local)->count = (oldnode.local)->count;
}

/* Free the given node */
static void
RT_FREE_NODE(RT_RADIX_TREE *tree, RT_NODE_PTR node)
{
#ifdef RT_DEBUG
	{
		int i;

		/* update the statistics */
		for (i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		{
			if ((node.local)->fanout == RT_SIZE_CLASS_INFO[i].fanout)
				break;
		}

		/* fanout of node256 is intentionally 0 */
		if (i == RT_SIZE_CLASS_COUNT)
			i = RT_CLASS_256;

		tree->ctl->cnt[i]--;
		Assert(tree->ctl->cnt[i] >= 0);
	}
#endif

#ifdef RT_SHMEM
	dsa_free(tree->dsa, node.alloc);
#else
	pfree(node.alloc);
#endif
}

static inline void
RT_FREE_LEAF(RT_RADIX_TREE *tree, RT_NODE_PTR node)
{
	// because no lazy expansion yet
	Assert(node.alloc != tree->ctl->root);

#ifdef RT_DEBUG
	tree->ctl->leafcnt--;
	Assert(tree->ctl->leafcnt >= 0);
#endif

#ifdef RT_SHMEM
	dsa_free(tree->dsa, node.alloc);
#else
	pfree(node.alloc);
#endif
}

/*
 * The radix tree doesn't have sufficient height. Extend the radix tree so
 * it can store the key.
 */
static pg_noinline void
RT_EXTEND_UP(RT_RADIX_TREE *tree, uint64 key)
{
	int			target_shift;
	int			shift = tree->ctl->start_shift + RT_SPAN;

	target_shift = RT_KEY_GET_SHIFT(key);

	/* Grow tree from 'shift' to 'target_shift' */
	while (shift <= target_shift)
	{
		RT_NODE_PTR		node;
		RT_NODE_4 *n4;

		node = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);
		n4 = (RT_NODE_4 *) node.local;
		n4->base.n.count = 1;
		n4->base.chunks[0] = 0;
		n4->children[0] = tree->ctl->root;

		/* Update the root */
		tree->ctl->root = node.alloc;

		shift += RT_SPAN;
	}

	tree->ctl->max_val = RT_SHIFT_GET_MAX_VAL(target_shift);
	tree->ctl->start_shift = target_shift;
}

/*
 * Search for the child pointer corresponding to 'key' in the given node.
 *
 * Return child if the key is found, otherwise return NULL.
 */
static inline RT_PTR_ALLOC *
RT_NODE_SEARCH(RT_PTR_LOCAL node, uint8 chunk)
{
	/* Make sure we already converted to local pointer */
	Assert(node != NULL);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE_4 *n4 = (RT_NODE_4 *) node;
				int			idx = RT_NODE_4_SEARCH_EQ((RT_NODE_BASE_4 *) n4, chunk);

				if (idx < 0)
					return NULL;

				return &n4->children[idx];
			}
		case RT_NODE_KIND_16:
			{
				RT_NODE_16 *n16 = (RT_NODE_16 *) node;
				int			idx = RT_NODE_16_SEARCH_EQ((RT_NODE_BASE_16 *) n16, chunk);

				if (idx < 0)
					return NULL;

				return &n16->children[idx];
			}
		case RT_NODE_KIND_48:
			{
				RT_NODE_48 *n48 = (RT_NODE_48 *) node;
				int			slotpos = n48->base.slot_idxs[chunk];

				if (slotpos == RT_INVALID_SLOT_IDX)
					return NULL;

				return RT_NODE_48_GET_CHILD(n48, chunk);
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE_256 *n256 = (RT_NODE_256 *) node;

				if (!RT_NODE_256_IS_CHUNK_USED(n256, chunk))
					return NULL;

				return RT_NODE_256_GET_CHILD(n256, chunk);
			}
		default:
			pg_unreachable();
	}
}

#ifdef RT_USE_DELETE

/*
 * When shrinking nodes, we generally wait until the count is about 3/4
 * of the next lower node's fanout. This prevents ping-ponging between
 * different node sizes.
 * TODO: When shrinking to node4, 3 should be hard-coded, as that's the
 * largest count where linear search is faster than SIMD, at least on
 * x86-64.
 */

static inline void
RT_REMOVE_CHILD_256(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint8 chunk)
{
	int shrink_threshold;
	RT_NODE_256 *n256 = (RT_NODE_256 *) node.local;

	RT_NODE_256_DELETE(n256, chunk);

	n256->base.n.count--;

	/* to keep isset coding below simple, for now at least */
	shrink_threshold = sizeof(bitmapword) * BITS_PER_BYTE;
	shrink_threshold = Min(RT_FANOUT_48 / 4 * 3, shrink_threshold);

	if (n256->base.n.count < shrink_threshold)
	{
		RT_NODE_PTR newnode;
		RT_NODE_48 *new48;
		int slot_idx = 0;

		/* initialize new node */
		newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_48, RT_CLASS_48);
		new48 = (RT_NODE_48 *) newnode.local;

		/* copy over the entries */
		RT_COPY_COMMON(newnode, node);
		for (int i = 0; i < 256; i++)
		{
			if (RT_NODE_256_IS_CHUNK_USED(n256, i))
			{
				new48->base.slot_idxs[i] = slot_idx;
				new48->children[slot_idx] = n256->children[i];
				slot_idx++;
			}
		}

		/*
		 * Since we just copied a dense array, we can fill "isset"
		 * using a single store, provided the length of that array
		 * is at most the number of bits in a bitmapword.
		 */
		Assert(n256->base.n.count <= sizeof(bitmapword) * BITS_PER_BYTE);
		new48->base.isset[0] = (bitmapword) (((uint64) 1 << n256->base.n.count) - 1);

		/* free old node and update reference in parent */
		*ref = newnode.alloc;
		RT_FREE_NODE(tree, node);
	}
}


static inline void
RT_REMOVE_CHILD_48(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint8 chunk)
{
	RT_NODE_48 *n48 = (RT_NODE_48 *) node.local;
	int			slotpos = n48->base.slot_idxs[chunk];
	int			idx;
	int			bitnum;

	Assert(slotpos != RT_INVALID_SLOT_IDX);

	idx = RT_BM_IDX(slotpos);
	bitnum = RT_BM_BIT(slotpos);
	n48->base.isset[idx] &= ~((bitmapword) 1 << bitnum);
	n48->base.slot_idxs[chunk] = RT_INVALID_SLOT_IDX;

	n48->base.n.count--;
}

static inline void
RT_REMOVE_CHILD_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
{
	RT_NODE_16 *n16 = (RT_NODE_16 *) node.local;
	int			idx = slot - n16->children;;

	Assert(idx >= 0);
	Assert(n16->base.chunks[idx] == chunk);

	RT_CHUNK_CHILDREN_ARRAY_DELETE(n16->base.chunks, n16->children,
								n16->base.n.count, idx);
	n16->base.n.count--;
}

static inline void
RT_REMOVE_CHILD_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
{
	RT_NODE_4 *n4 = (RT_NODE_4 *) node.local;

	if (n4->base.n.count == 1)
	{
		Assert(n4->base.chunks[0] == chunk);

		/* deleting last entry, so just free the node and null out the parent's slot */
		// We assume the caller already freed the child, if necessary
		RT_FREE_NODE(tree, node);
		*ref = RT_INVALID_PTR_ALLOC;

		/* If we're deleting the root node, make the tree empty */
		if (ref == &tree->ctl->root)
			tree->ctl->max_val = 0;
	}
	else
	{
		int			idx = slot - n4->children;;

		Assert(idx >= 0);
		Assert(n4->base.chunks[idx] == chunk);

		RT_CHUNK_CHILDREN_ARRAY_DELETE(n4->base.chunks, n4->children,
								n4->base.n.count, idx);

		n4->base.n.count--;
	}
}

/*
 * Search for the child pointer corresponding to 'key' in the given node.
 *
 * Delete the node and return true if the key is found, otherwise return false.
 */
static inline void
RT_NODE_DELETE(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint8 chunk, RT_PTR_ALLOC *slot)
{
	switch ((node.local)->kind)
	{
		case RT_NODE_KIND_4:
			return RT_REMOVE_CHILD_4(tree, ref, node, chunk, slot);
		case RT_NODE_KIND_16:
			return RT_REMOVE_CHILD_16(tree, ref, node, chunk, slot);
		case RT_NODE_KIND_48:
			return RT_REMOVE_CHILD_48(tree, ref, node, chunk);
		case RT_NODE_KIND_256:
			return RT_REMOVE_CHILD_256(tree, ref, node, chunk);
		default:
			pg_unreachable();
	}
}

#endif

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_256(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child)
{
	RT_NODE_256 *n256 = (RT_NODE_256 *) node.local;

	RT_NODE_256_SET(n256, chunk, child);

	n256->base.n.count++;
	RT_VERIFY_NODE((RT_NODE *) n256);

	return RT_NODE_256_GET_CHILD(n256, chunk);
}

static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_48(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child)
{
	RT_NODE_48 *n48 = (RT_NODE_48 *) node.local;

		RT_NODE_PTR newnode;
		RT_NODE_256 *new256;
		int			cnt = 0;

		/* initialize new node */
		newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_256, RT_CLASS_256);
		new256 = (RT_NODE_256 *) newnode.local;

		/* copy over the entries */
		RT_COPY_COMMON(newnode, node);
		for (int i = 0; i < RT_NODE_MAX_SLOTS && cnt < n48->base.n.count; i++)
		{
			if (!RT_NODE_48_IS_CHUNK_USED(&n48->base, i))
				continue;

			// FIXME: this shows up in profiles, see prototype
			RT_NODE_256_SET(new256, i,
									*RT_NODE_48_GET_CHILD(n48, i));
			cnt++;
		}

		/* free old node and update reference in parent */
		*ref = newnode.alloc;
		RT_FREE_NODE(tree, node);

		return RT_ADD_CHILD_256(tree, ref, newnode, chunk, child);
}

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_48(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child)
{
	RT_NODE_48 *n48 = (RT_NODE_48 *) node.local;

	if (unlikely(RT_NODE_MUST_GROW(n48)))
	{
		return RT_GROW_NODE_48(tree, ref, node, chunk, child);
	}
	else
	{
		int			slotpos;
		int			idx = 0;
		bitmapword	w, inverse;

		/* get the first word with at least one bit not set */
		for (int i = 0; i < RT_BM_IDX(RT_SLOT_IDX_LIMIT); i++)
		{
			w = n48->base.isset[i];
			if (w < ~((bitmapword) 0))
			{
				idx = i;
				break;
			}
		}

		/* To get the first unset bit in w, get the first set bit in ~w */
		inverse = ~w;
		slotpos = idx * BITS_PER_BITMAPWORD;
		slotpos += bmw_rightmost_one_pos(inverse);
		Assert(slotpos < n48->base.n.fanout);

		/* mark the slot used by setting the rightmost 0-bit */
		n48->base.isset[idx] |= w + 1;

		n48->base.slot_idxs[chunk] = slotpos;
		n48->children[slotpos] = child;
		n48->base.n.count++;
		RT_VERIFY_NODE((RT_NODE *) n48);

		return &n48->children[slotpos];
	}
}

static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child, int insertpos)
{
	RT_NODE_16 *n16 = (RT_NODE_16 *) node.local;

	if (n16->base.n.fanout < RT_FANOUT_16_HI)
	{
		RT_NODE_PTR newnode;
		RT_NODE_16 *new16;

		Assert(n16->base.n.fanout == RT_FANOUT_16_LO);

		/* initialize new node */
		newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_16, RT_CLASS_16_HI);
		new16 = (RT_NODE_16 *) newnode.local;

		/* copy over existing entries */
		RT_COPY_COMMON(newnode, node);
		RT_CHUNK_CHILDREN_ARRAY_COPY(n16->base.chunks, n16->children,
		new16->base.chunks, new16->children,
		insertpos, n16->base.n.count);

		/* then insert the new element */
		new16->base.chunks[insertpos] = chunk;
		new16->children[insertpos] = child;

		/* update the fanout */
		new16->base.n.fanout = RT_FANOUT_16_HI;

		new16->base.n.count++;
		RT_VERIFY_NODE((RT_NODE *) new16);

		/* free old node and update references */
		RT_FREE_NODE(tree, node);
		*ref = newnode.alloc;

		return &new16->children[insertpos];
	}
	else
	{
		RT_NODE_PTR newnode;
		RT_NODE_48 *new48;
		const int			slotpos = RT_FANOUT_16_HI;
		const int 			idx = RT_BM_IDX(slotpos);
		const int 			bit = RT_BM_BIT(slotpos);

		Assert(n16->base.n.fanout == RT_FANOUT_16_HI);

		/* initialize new node */
		newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_48, RT_CLASS_48);
		new48 = (RT_NODE_48 *) newnode.local;

		/* copy over the entries */
		RT_COPY_COMMON(newnode, node);
		for (int i = 0; i < RT_FANOUT_16_HI; i++)
			new48->base.slot_idxs[n16->base.chunks[i]] = i;
		memcpy(&new48->children[0], &n16->children[0], RT_FANOUT_16_HI * sizeof(new48->children[0]));

		/*
		 * Since we just copied a dense array, we can fill "isset"
		 * using a single store, provided the length of that array
		 * is at most the number of bits in a bitmapword.
		 */
		Assert(RT_FANOUT_16_HI <= sizeof(bitmapword) * BITS_PER_BYTE);
		new48->base.isset[0] = (bitmapword) (((uint64) 1 << RT_FANOUT_16_HI) - 1);

		/* add new value */

		/* mark slot used */
		new48->base.isset[idx] |= ((bitmapword) 1 << bit);
		new48->base.slot_idxs[chunk] = slotpos;

		new48->children[slotpos] = child;
		new48->base.n.count++;
		RT_VERIFY_NODE((RT_NODE *) new48);

		/* free old node and update reference in parent */
		*ref = newnode.alloc;
		RT_FREE_NODE(tree, node);

		return &new48->children[slotpos];
	}
}

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_16(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child)
{
	RT_NODE_16 *n16 = (RT_NODE_16 *) node.local;
	int	insertpos = RT_NODE_16_GET_INSERTPOS(&n16->base, chunk);

	if (unlikely(RT_NODE_MUST_GROW(n16)))
		return RT_GROW_NODE_16(tree, ref, node, chunk, child, insertpos);
	else
	{
		int count = n16->base.n.count;

		RT_CHUNK_CHILDREN_ARRAY_SHIFT(n16->base.chunks, n16->children,
								   count, insertpos);

		n16->base.chunks[insertpos] = chunk;
		n16->children[insertpos] = child;
		n16->base.n.count++;
		RT_VERIFY_NODE((RT_NODE *) n16);

		return &n16->children[insertpos];
	}
}

static pg_noinline RT_PTR_ALLOC *
RT_GROW_NODE_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child, int insertpos)
{
	RT_NODE_4 *n4 = (RT_NODE_4 *) (node.local);
	RT_NODE_PTR newnode;
	RT_NODE_16 *new16;

	/* initialize new node */
	newnode = RT_ALLOC_NODE(tree, RT_NODE_KIND_16, RT_CLASS_16_LO);
	new16 = (RT_NODE_16 *) newnode.local;

	/* copy over existing entries */
	RT_COPY_COMMON(newnode, node);
	RT_CHUNK_CHILDREN_ARRAY_COPY(n4->base.chunks, n4->children,
	new16->base.chunks, new16->children,
	insertpos, n4->base.n.count);

	/* then insert the new element */
	new16->base.chunks[insertpos] = chunk;
	new16->children[insertpos] = child;

	new16->base.n.count++;
	RT_VERIFY_NODE((RT_NODE *) new16);

	/* free old node and update reference in parent */
	*ref = newnode.alloc;
	RT_FREE_NODE(tree, node);

	return &new16->children[insertpos];
}

static inline RT_PTR_ALLOC *
RT_ADD_CHILD_4(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
				uint8 chunk, RT_PTR_ALLOC child)
{
	RT_NODE_4 *n4 = (RT_NODE_4 *) (node.local);
	int			insertpos = RT_NODE_4_GET_INSERTPOS(&n4->base, chunk);

	if (unlikely(RT_NODE_MUST_GROW(n4)))
	{
		return RT_GROW_NODE_4(tree, ref, node, chunk, child, insertpos);
	}
	else
	{
		int			count = n4->base.n.count;

		/* shift chunks and children */
		RT_CHUNK_CHILDREN_ARRAY_SHIFT(n4->base.chunks, n4->children,
								   count, insertpos);

		/* finally, put the new child in place */
		n4->base.chunks[insertpos] = chunk;
		n4->children[insertpos] = child;
		n4->base.n.count++;
		RT_VERIFY_NODE((RT_NODE *) n4);

		return &n4->children[insertpos];
	}
}

/*
 * Insert "child" into "node".
 *
 * "ref" is the parent's child pointer to "node".
 * If the node we're inserting into needs to grow, we update the parent's
 * child pointer with the pointer to the new larger node.
 */
static RT_PTR_ALLOC *
RT_NODE_INSERT(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node,
								 uint8 chunk, RT_PTR_ALLOC child)
{

	switch ((node.local)->kind)
	{
		case RT_NODE_KIND_4:
			return RT_ADD_CHILD_4(tree, ref, node, chunk, child);
		case RT_NODE_KIND_16:
			return RT_ADD_CHILD_16(tree, ref, node, chunk, child);
		case RT_NODE_KIND_48:
			return RT_ADD_CHILD_48(tree, ref, node, chunk, child);
		case RT_NODE_KIND_256:
			return RT_ADD_CHILD_256(tree, ref, node, chunk, child);
		default:
			pg_unreachable();
	}
}

/*
 * Create the radix tree in the given memory context and return it.
 */
RT_SCOPE RT_RADIX_TREE *
#ifdef RT_SHMEM
RT_CREATE(MemoryContext ctx, dsa_area *dsa, int tranche_id)
#else
RT_CREATE(MemoryContext ctx)
#endif
{
	RT_RADIX_TREE *tree;
	MemoryContext old_ctx;
#ifdef RT_SHMEM
	dsa_pointer dp;
#endif

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = (RT_RADIX_TREE *) palloc0(sizeof(RT_RADIX_TREE));
	tree->context = ctx;

#ifdef RT_SHMEM
	tree->dsa = dsa;
	dp = dsa_allocate0(dsa, sizeof(RT_RADIX_TREE_CONTROL));
	tree->ctl = (RT_RADIX_TREE_CONTROL *) dsa_get_address(dsa, dp);
	tree->ctl->handle = dp;
	tree->ctl->magic = RT_RADIX_TREE_MAGIC;
	LWLockInitialize(&tree->ctl->lock, tranche_id);
#else
	tree->ctl = (RT_RADIX_TREE_CONTROL *) palloc0(sizeof(RT_RADIX_TREE_CONTROL));

	/* Create a slab context for each size class */
	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		RT_SIZE_CLASS_ELEM size_class = RT_SIZE_CLASS_INFO[i];
		size_t inner_blocksize = RT_SLAB_BLOCK_SIZE(size_class.allocsize);

		tree->node_slabs[i] = SlabContextCreate(ctx,
												 size_class.name,
												 inner_blocksize,
												 size_class.allocsize);
	}
		tree->leaf_slab = SlabContextCreate(ctx,
												"radix tree leaves",
												RT_SLAB_BLOCK_SIZE(sizeof(RT_VALUE_TYPE)),
												sizeof(RT_VALUE_TYPE));
#endif

	tree->ctl->root = RT_INVALID_PTR_ALLOC;

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

#ifdef RT_SHMEM
RT_SCOPE RT_RADIX_TREE *
RT_ATTACH(dsa_area *dsa, RT_HANDLE handle)
{
	RT_RADIX_TREE *tree;
	dsa_pointer	control;

	tree = (RT_RADIX_TREE *) palloc0(sizeof(RT_RADIX_TREE));

	/* Find the control object in shard memory */
	control = handle;

	tree->dsa = dsa;
	tree->ctl = (RT_RADIX_TREE_CONTROL *) dsa_get_address(dsa, control);
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);

	return tree;
}

RT_SCOPE void
RT_DETACH(RT_RADIX_TREE *tree)
{
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	pfree(tree);
}

RT_SCOPE RT_HANDLE
RT_GET_HANDLE(RT_RADIX_TREE *tree)
{
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	return tree->ctl->handle;
}

/*
 * Recursively free all nodes allocated to the DSA area.
 */
static void
RT_FREE_RECURSE(RT_RADIX_TREE *tree, RT_PTR_ALLOC ptr)
{
#if 0
	RT_PTR_LOCAL node = RT_PTR_SET_LOCAL(tree, ptr);

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	/* The leaf node doesn't have child pointers */
	/* TODO: track depth */
	if (RT_NODE_IS_LEAF(node))
	{
		dsa_free(tree->dsa, ptr);
		return;
	}

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE_4 *n4 = (RT_NODE_4 *) node;

				for (int i = 0; i < n4->base.n.count; i++)
					RT_FREE_RECURSE(tree, n4->children[i]);

				break;
			}
		case RT_NODE_KIND_16:
			{
				RT_NODE_16 *n16 = (RT_NODE_16 *) node;

				for (int i = 0; i < n16->base.n.count; i++)
					RT_FREE_RECURSE(tree, n16->children[i]);

				break;
			}
		case RT_NODE_KIND_48:
			{
				RT_NODE_48 *n48 = (RT_NODE_48 *) node;

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_48_IS_CHUNK_USED(&n48->base, i))
						continue;

					RT_FREE_RECURSE(tree, *RT_NODE_48_GET_CHILD(n48, i));
				}

				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE_256 *n256 = (RT_NODE_256 *) node;

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_256_IS_CHUNK_USED(n256, i))
						continue;

					RT_FREE_RECURSE(tree, *RT_NODE_256_GET_CHILD(n256, i));
				}

				break;
			}
	}

	/* Free the inner node */
	dsa_free(tree->dsa, ptr);
#endif // 0
}
#endif

/*
 * Free the given radix tree.
 */
RT_SCOPE void
RT_FREE(RT_RADIX_TREE *tree)
{
#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);

	/* Free all memory used for radix tree nodes */
	if (RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
		RT_FREE_RECURSE(tree, tree->ctl->root);

	/*
	 * Vandalize the control block to help catch programming error where
	 * other backends access the memory formerly occupied by this radix tree.
	 */
	tree->ctl->magic = 0;
	dsa_free(tree->dsa, tree->ctl->handle);
#else
	pfree(tree->ctl);

	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		MemoryContextDelete(tree->node_slabs[i]);
	}
		MemoryContextDelete(tree->leaf_slab);
#endif

	pfree(tree);
}

static pg_noinline RT_PTR_ALLOC *
RT_EXTEND_DOWN(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint64 key, int shift)
{
	RT_NODE_PTR		child;
	RT_NODE_4	*n4;

	child = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);
	/*
	 * At first we don't know the kind of the node we're inserting into,
	 * and an insertion could cause "node" to grow, so use the general-
	 * purpose function.
	 */
	(void) RT_NODE_INSERT(tree, ref, node,
								RT_GET_KEY_CHUNK(key, shift), child.alloc);
	node = child;
	shift -= RT_SPAN;

	/* At this point we open-code the insertion ourselves, for speed. */
	while (shift > 0)
	{
		child = RT_ALLOC_NODE(tree, RT_NODE_KIND_4, RT_CLASS_4);

		n4 = (RT_NODE_4 *) node.local;
		n4->base.n.count = 1;
		n4->base.chunks[0] = RT_GET_KEY_CHUNK(key, shift);
		n4->children[0] = child.alloc;

		node = child;
		shift -= RT_SPAN;
	}

	/* Insert placeholder for the value. */
	Assert((node.local)->kind == RT_NODE_KIND_4);
	n4 = (RT_NODE_4 *) node.local;
	Assert(shift == 0);
	n4->base.chunks[0] = RT_GET_KEY_CHUNK(key, shift);
	n4->children[0] = RT_INVALID_PTR_ALLOC;
	n4->base.n.count = 1;

	return &n4->children[0];
}

/* Workhorse for RT_SET
 * "ref" is the address of "child" in the parent's array of children,
 * needed if inserting into "child" causes it to grow.
 */
static RT_PTR_ALLOC *
RT_GET_SLOT_RECURSIVE(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_PTR_ALLOC child, uint64 key, int shift, bool *found)
{
	RT_PTR_ALLOC *slot;
	RT_NODE_PTR node;
	uint8 chunk = RT_GET_KEY_CHUNK(key, shift);

	node.alloc = child;
	RT_PTR_SET_LOCAL(tree, &node);
	slot = RT_NODE_SEARCH(node.local, chunk);

	if (shift > 0)
	{
		if (unlikely(slot == NULL))
		{
			*found = false;
			return RT_EXTEND_DOWN(tree, ref, node, key, shift);
		}
		else
		{
			return RT_GET_SLOT_RECURSIVE(tree, slot, *slot, key, shift - RT_SPAN, found);
		}
	}
	else
	{
		if (slot != NULL)
		{
			*found = true;
			return slot;
		}
		else
		{
			*found = false;

			/* insert placeholder for our value, the caller will update it */
			return RT_NODE_INSERT(tree, ref, node, chunk, RT_INVALID_PTR_ALLOC);
		}
	}
}

/*
 * Set key to value. If the entry already exists, we update its value to 'value'
 * and return true. Returns false if entry doesn't yet exist.
 */
RT_SCOPE bool pg_attribute_unused()
RT_SET(RT_RADIX_TREE *tree, uint64 key, RT_VALUE_TYPE *value_p)
{
	bool		found;
	RT_PTR_ALLOC *slot;
#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	RT_LOCK_EXCLUSIVE(tree);

	/* Empty tree, create the root */
	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
		RT_NEW_ROOT(tree, key);

	/* Extend the tree if necessary */
	if (key > tree->ctl->max_val)
		RT_EXTEND_UP(tree, key);

	slot = RT_GET_SLOT_RECURSIVE(tree, &tree->ctl->root, tree->ctl->root,
								key, tree->ctl->start_shift, &found);

	Assert(slot != NULL);

	if (RT_VALUE_IS_EMBEDDABLE)
	{
		/* store value directly in child pointer slot */
		memcpy(slot, value_p, sizeof(RT_VALUE_TYPE));
	}
	else
	{
		RT_NODE_PTR leaf;

		if (found)
		{
			/* found the value, so prepare to overwrite it */
			Assert(RT_PTR_ALLOC_IS_VALID(*slot));
			leaf.alloc = *slot;
			RT_PTR_SET_LOCAL(tree, &leaf);
		}
		else
		{
			Assert(!RT_PTR_ALLOC_IS_VALID(*slot));

			/* allocate new leaf and store it in the child array */
			leaf = RT_ALLOC_LEAF(tree);
			*slot = leaf.alloc;
		}

		memcpy(leaf.local, value_p, sizeof(RT_VALUE_TYPE));
	}

	/* Update the statistics */
	if (!found)
		tree->ctl->num_keys++;

	RT_UNLOCK(tree);
	return found;
}

/*
 * If an entry already exists, return its pointer and set "found" to true.
 * If it doesn't exist, insert an empty slot and return its pointer and set "found" to false.
 * The caller is responsible for locking/unlocking the tree in exclusive mode.
 * TODO: pass the required size
 */
RT_SCOPE RT_VALUE_TYPE pg_attribute_unused() *
RT_GET(RT_RADIX_TREE *tree, uint64 key, bool *found)
//RT_GET(RT_RADIX_TREE *tree, uint64 key, size_t size)
{
	RT_PTR_ALLOC *slot;
#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	/* Empty tree, create the root */
	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
		RT_NEW_ROOT(tree, key);

	/* Extend the tree if necessary */
	if (key > tree->ctl->max_val)
		RT_EXTEND_UP(tree, key);

	slot = RT_GET_SLOT_RECURSIVE(tree, &tree->ctl->root, tree->ctl->root,
								key, tree->ctl->start_shift, found);

	Assert(slot != NULL);

	/* Update the statistics */
	if (!*found)
		tree->ctl->num_keys++;

	if (RT_VALUE_IS_EMBEDDABLE)
	{
		return (RT_VALUE_TYPE*) slot;
	}
	else
	{
		RT_NODE_PTR leaf;

		if (*found)
		{
			/* found the value, so prepare for caller */
			Assert(RT_PTR_ALLOC_IS_VALID(*slot));
			leaf.alloc = *slot;
			RT_PTR_SET_LOCAL(tree, &leaf);
		}
		else
		{
			Assert(!RT_PTR_ALLOC_IS_VALID(*slot));

			/* allocate new leaf and store the pointer in the child array */
			leaf = RT_ALLOC_LEAF(tree);
			*slot = leaf.alloc;
		}

		/* WIP: maybe need RT_LEAF_PTR type? or make the combined pointer have void* ? */
		return (RT_VALUE_TYPE*) leaf.local;
	}
}

/*
 * Search the given key in the radix tree. Return true if there is the key,
 * otherwise return false. On success, we set the value to *value_p so it must
 * not be NULL.
 * The caller is responsible for locking/unlocking the tree in shared mode.
 */
RT_SCOPE bool pg_attribute_unused()
RT_SEARCH(RT_RADIX_TREE *tree, uint64 key, RT_VALUE_TYPE *value_p)
{
	RT_NODE_PTR node;
	RT_PTR_ALLOC *slot;
	int			shift;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif
	Assert(value_p != NULL);

	RT_LOCK_SHARED(tree);

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root) || key > tree->ctl->max_val)
	{
		RT_UNLOCK(tree);
		return false;
	}

	node.alloc = tree->ctl->root;

	shift = tree->ctl->start_shift;

	/* Descend the tree */
	while (shift >= 0)
	{
		RT_PTR_SET_LOCAL(tree, &node);
		slot = RT_NODE_SEARCH(node.local, RT_GET_KEY_CHUNK(key, shift));
		if (slot == NULL)
		{
			RT_UNLOCK(tree);
			return false;
		}

		node.alloc = *slot;
		shift -= RT_SPAN;
	}

	if (RT_VALUE_IS_EMBEDDABLE)
	{
		memcpy(value_p, &node.alloc, sizeof(RT_VALUE_TYPE));
	}
	else
	{
		RT_PTR_SET_LOCAL(tree, &node);
		memcpy(value_p, node.local, sizeof(RT_VALUE_TYPE));
	}

	RT_UNLOCK(tree);
	return true;
}

/*
 * Search the given key in the radix tree. Return the pointer to the value if found,
 * otherwise return NULL.
 * The caller is responsible for locking/unlocking the tree in shared mode.
 */
RT_SCOPE RT_VALUE_TYPE pg_attribute_unused() *
RT_FIND(RT_RADIX_TREE *tree, uint64 key)
{
	RT_NODE_PTR node;
	RT_PTR_ALLOC *slot;
	int			shift;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root) || key > tree->ctl->max_val)
	{
		return NULL;
	}

	node.alloc = tree->ctl->root;

	shift = tree->ctl->start_shift;

	/* Descend the tree */
	while (shift >= 0)
	{
		RT_PTR_SET_LOCAL(tree, &node);
		slot = RT_NODE_SEARCH(node.local, RT_GET_KEY_CHUNK(key, shift));
		if (slot == NULL)
			return NULL;

		node.alloc = *slot;
		shift -= RT_SPAN;
	}

	if (RT_VALUE_IS_EMBEDDABLE)
	{
		return (RT_VALUE_TYPE*) slot;
	}
	else
	{
		RT_PTR_SET_LOCAL(tree, &node);
		return (RT_VALUE_TYPE*) node.local;
	}
}

#ifdef RT_USE_DELETE

static bool
RT_RECURSIVE_DELETE(RT_RADIX_TREE *tree, RT_PTR_ALLOC *ref, RT_NODE_PTR node, uint64 key, int shift)
{
	uint8 chunk = RT_GET_KEY_CHUNK(key, shift);
	RT_PTR_ALLOC *slot = RT_NODE_SEARCH(node.local, chunk);
	RT_NODE_PTR child;

	if (!slot)
		return false;

	child.alloc = *slot;

	if (shift == 0)
	{
		if (!RT_VALUE_IS_EMBEDDABLE)
			RT_FREE_LEAF(tree, child);

		RT_NODE_DELETE(tree, ref, node, chunk, slot);
		return true;
	}
	else
	{
		bool deleted;

		/* since we're not at lowest level, we know this is a pointer and not an embedded value */
		RT_PTR_SET_LOCAL(tree, &child);

		deleted = RT_RECURSIVE_DELETE(tree, slot, child, key, shift - RT_SPAN);

		/* Child node was freed, so delete its slot now */
		if (*slot == RT_INVALID_PTR_ALLOC)
		{
			Assert(deleted);
			RT_NODE_DELETE(tree, ref, node, chunk, slot);
		}

		return deleted;
	}

}

/*
 * Delete the given key from the radix tree. Return true if the key is found (and
 * deleted), otherwise do nothing and return false.
 */
RT_SCOPE bool
RT_DELETE(RT_RADIX_TREE *tree, uint64 key)
{
	RT_NODE_PTR rootnode;
	bool		deleted;

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	RT_LOCK_EXCLUSIVE(tree);

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root) || key > tree->ctl->max_val)
	{
		RT_UNLOCK(tree);
		return false;
	}

	rootnode.alloc = tree->ctl->root;
	RT_PTR_SET_LOCAL(tree, &rootnode);

	deleted = RT_RECURSIVE_DELETE(tree, &tree->ctl->root, rootnode,
									key, tree->ctl->start_shift);

	/* Found the key to delete. Update the statistics */
	if (deleted)
		tree->ctl->num_keys--;

	RT_UNLOCK(tree);
	return deleted;
}
#endif


/*
 * Scan the inner node and return the next child node if exist, otherwise
 * return NULL.
 */
static inline RT_PTR_ALLOC *
RT_NODE_ITERATE_NEXT(RT_ITER *iter, int level)
{

	uint8		key_chunk = 0;
	RT_NODE_ITER *node_iter;
	RT_NODE_PTR	node;
	RT_PTR_ALLOC *slot = NULL;

#ifdef RT_SHMEM
	Assert(iter->tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	node_iter = &(iter->node_iters[level]);
	node = node_iter->node;

	Assert(node.local != NULL);

	switch ((node.local)->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE_4 *n4 = (RT_NODE_4 *) (node.local);

				if (node_iter->idx >= n4->base.n.count)
					return NULL;

				slot = &n4->children[node_iter->idx];
				key_chunk = n4->base.chunks[node_iter->idx];
				node_iter->idx++;
				break;
			}
		case RT_NODE_KIND_16:
			{
				RT_NODE_16 *n16 = (RT_NODE_16 *) (node.local);

				if (node_iter->idx >= n16->base.n.count)
					return NULL;

				slot = &n16->children[node_iter->idx];
				key_chunk = n16->base.chunks[node_iter->idx];
				node_iter->idx++;
				break;
			}
		case RT_NODE_KIND_48:
			{
				RT_NODE_48 *n48 = (RT_NODE_48 *) (node.local);
				int			chunk;

				for (chunk = node_iter->idx; chunk < RT_NODE_MAX_SLOTS; chunk++)
				{
					if (RT_NODE_48_IS_CHUNK_USED((RT_NODE_BASE_48 *) n48, chunk))
						break;
				}

				if (chunk >= RT_NODE_MAX_SLOTS)
					return NULL;

				slot = RT_NODE_48_GET_CHILD(n48, chunk);

				key_chunk = chunk;
				node_iter->idx = chunk + 1;
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE_256 *n256 = (RT_NODE_256 *) (node.local);
				int			chunk;

				for (chunk = node_iter->idx; chunk < RT_NODE_MAX_SLOTS; chunk++)
				{
					if (RT_NODE_256_IS_CHUNK_USED(n256, chunk))
						break;
				}

				if (chunk >= RT_NODE_MAX_SLOTS)
					return NULL;

				slot = RT_NODE_256_GET_CHILD(n256, chunk);

				key_chunk = chunk;
				node_iter->idx = chunk + 1;
				break;
			}
	}

	/* Update the part of the key */
	iter->key &= ~(((uint64) RT_CHUNK_MASK) << (level * RT_SPAN));
	iter->key |= (((uint64) key_chunk) << (level * RT_SPAN));

	return slot;
}

/*
 * While descending the radix tree from the 'from' node to the bottom, we
 * set the next node to iterate for each level.
 */
static void
RT_ITER_SET_NODE_FROM(RT_ITER *iter, RT_NODE_PTR from, int level)
{
	RT_NODE_PTR node = from;

	for (;;)
	{
		RT_NODE_ITER *node_iter = &(iter->node_iters[level]);

		RT_PTR_SET_LOCAL(iter->tree, &node);

#if 0 //def USE_ASSERT_CHECKING fixme
		if (node_iter->node)
		{
			/* We must have finished the iteration on the previous node */
			if (RT_NODE_IS_LEAF(node_iter->node))
			{
				uint64 dummy;
				Assert(!RT_NODE_LEAF_ITERATE_NEXT(iter, node_iter, &dummy));
			}
			else
				Assert(!RT_NODE_ITERATE_NEXT(iter, node_iter, level));
		}
#endif

		/* Set the node to the node iterator of this level */
		node_iter->node = node;
		node_iter->idx = 0;

		if (level == 0)
		{
			/* We will visit the leaf node when RT_ITERATE_NEXT() */
			break;
		}

		/*
		 * Get the first child node from the node, which corresponds to the
		 * lowest chunk within the node.
		 */
		node.alloc = *RT_NODE_ITERATE_NEXT(iter, level);

		/* The first child must be found */
		Assert(RT_PTR_ALLOC_IS_VALID(node.alloc));

		level--;
	}
}

/*
 * Create and return the iterator for the given radix tree.
 *
 * The radix tree is locked in shared mode during the iteration, so
 * RT_END_ITERATE needs to be called when finished to release the lock.
 */
RT_SCOPE RT_ITER *
RT_BEGIN_ITERATE(RT_RADIX_TREE *tree)
{
	RT_ITER    *iter;
	RT_NODE_PTR root;

	iter = (RT_ITER *) MemoryContextAllocZero(tree->context,
											  sizeof(RT_ITER));
	iter->tree = tree;

	RT_LOCK_SHARED(tree);

	/* empty tree */
	if (!iter->tree->ctl->root)
		return iter;

	root.alloc = iter->tree->ctl->root;
	RT_PTR_SET_LOCAL(tree, &root);

	iter->top_level = iter->tree->ctl->start_shift / RT_SPAN;

	/*
	 * Set the next node to iterate for each level from the level of the
	 * root node.
	 */
	RT_ITER_SET_NODE_FROM(iter, root, iter->top_level);

	return iter;
}

/*
 * Return true with setting key_p and value_p if there is next key.  Otherwise
 * return false.
 */
RT_SCOPE bool pg_attribute_unused()
RT_ITERATE_NEXT(RT_ITER *iter, uint64 *key_p, RT_VALUE_TYPE *value_p)
{
	RT_PTR_ALLOC *slot = NULL;

	Assert(value_p != NULL);

	/* Empty tree */
	// FIXME this is ugly -- why aren't we passing the tree pointer separately???
	if (!iter->tree->ctl->root)
		return false;

	do
	{
		RT_NODE_PTR child;

		/* Get the next chunk of the leaf node */
		slot = RT_NODE_ITERATE_NEXT(iter, 0);

		if (slot)
		{
			*key_p = iter->key;
			child.alloc = *slot;

			// todo: deduplicate with rt_set?
			if (RT_VALUE_IS_EMBEDDABLE)
			{
				memcpy(value_p, &child.alloc, sizeof(RT_VALUE_TYPE));
			}
			else
			{
				RT_PTR_SET_LOCAL(iter->tree, &child);
				memcpy(value_p, child.local, sizeof(RT_VALUE_TYPE));
			}

			return true;
		}

		/*
		 * We've visited all values in the leaf node, so advance all inner node
		 * iterators by visiting inner nodes from the level = 1 until we find the
		 * next inner node that has a child node.
		 */
		for (int level = 1; level <= iter->top_level; level++)
		{
			slot = RT_NODE_ITERATE_NEXT(iter, level);

			if (slot)
			{
				child.alloc = *slot;

				/*
				 * Found the new child node. We update the next node to iterate for each
				 * level from the level of this child node.
				 */
				RT_ITER_SET_NODE_FROM(iter, child, level - 1);
				break;
			}
		}
	} while (slot != NULL);

	/* We've visited all nodes, so the iteration finished */
	return false;
}

/*
 * Return pointer to value and  set key_p as long as there is a key.  Otherwise
 * return NULL.
 */
RT_SCOPE RT_VALUE_TYPE pg_attribute_unused() *
RT_ITERATE_NEXT_PTR(RT_ITER *iter, uint64 *key_p)
{
	RT_PTR_ALLOC *slot = NULL;

	/* Empty tree */
	if (!RT_PTR_ALLOC_IS_VALID(iter->tree->ctl->root))
		return NULL;

	do
	{
		RT_NODE_PTR child;

		/* Get the next chunk of the leaf node */
		slot = RT_NODE_ITERATE_NEXT(iter, 0);

		if (slot != NULL)
		{
			*key_p = iter->key;
			child.alloc = *slot;

			if (RT_VALUE_IS_EMBEDDABLE)
			{
				return (RT_VALUE_TYPE*) slot;
			}
			else
			{
				RT_PTR_SET_LOCAL(iter->tree, &child);
				return (RT_VALUE_TYPE*) child.local;
			}
		}

		/*
		 * We've visited all values in the leaf node, so advance all inner node
		 * iterators by visiting inner nodes from the level = 1 until we find the
		 * next inner node that has a child node.
		 */
		for (int level = 1; level <= iter->top_level; level++)
		{
			slot = RT_NODE_ITERATE_NEXT(iter, level);

			if (slot != NULL)
			{
				child.alloc = *slot;

				/*
				 * Found the new child node. We update the next node to iterate for each
				 * level from the level of this child node.
				 */
				RT_ITER_SET_NODE_FROM(iter, child, level - 1);
				break;
			}
		}
	} while (slot != NULL);

	/* We've visited all nodes, so the iteration finished */
	return NULL;
}

/*
 * Terminate the iteration and release the lock.
 *
 * This function needs to be called after finishing or when exiting an
 * iteration.
 */
RT_SCOPE void
RT_END_ITERATE(RT_ITER *iter)
{
#ifdef RT_SHMEM
	Assert(LWLockHeldByMe(&iter->tree->ctl->lock));
#endif

	RT_UNLOCK(iter->tree);
	pfree(iter);
}

/*
 * Return the statistics of the amount of memory used by the radix tree.
 */
RT_SCOPE uint64
RT_MEMORY_USAGE(RT_RADIX_TREE *tree)
{
	Size		total = 0;

	RT_LOCK_SHARED(tree);

#ifdef RT_SHMEM
	Assert(tree->ctl->magic == RT_RADIX_TREE_MAGIC);
	total = dsa_get_total_size(tree->dsa);
#else
	for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
	{
		total += MemoryContextMemAllocated(tree->node_slabs[i], true);
	}
		total += MemoryContextMemAllocated(tree->leaf_slab, true);
#endif

	RT_UNLOCK(tree);
	return total;
}


static void pg_attribute_unused()
RT_DUMP_NODE(RT_PTR_LOCAL node, int level,
			 bool recurse, StringInfo buf)
{
#ifdef RT_DEBUG
	StringInfoData spaces;

	recurse = false; // xxx

	initStringInfo(&spaces);
	appendStringInfoSpaces(&spaces, (level * 4) + 1);
	// todo: clean up, can we use one of our tables for the kind-to-fanout mapping?
	appendStringInfo(buf, "%s%s kind %d, fanout %d, count %u:\n",
					 spaces.data,
					 level == 0 ? "" : "-> ",
					 (node->kind == RT_NODE_KIND_4) ? 3 :
					 (node->kind == RT_NODE_KIND_16) ? 32 :
					 (node->kind == RT_NODE_KIND_48) ? 125 : 256,
					 node->fanout == 0 ? 256 : node->fanout,

					 (node->kind == RT_NODE_KIND_256) ?
					 (node->count == 0 ? 256 : node->count) :
					 node->count);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				for (int i = 0; i < node->count; i++)
				{
#if 0
					if (RT_NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_4 *n4 = (RT_NODE_LEAF_4 *) node;

						appendStringInfo(buf, "%schunk[%d] 0x%X\n",
										 spaces.data, i, n4->base.chunks[i]);
					}
					else
#endif
					{
						RT_NODE_4 *n4 = (RT_NODE_4 *) node;

						appendStringInfo(buf, "%schunk[%d] 0x%X",
										 spaces.data, i, n4->base.chunks[i]);
#if 0
						if (recurse)
						{
							appendStringInfo(buf, "\n");
							RT_DUMP_NODE(n4->children[i], level + 1,
										 recurse, buf);
						}
						else
							appendStringInfo(buf, " (skipped)\n");
#endif

#if 0
						/* quick hack for inspecting values in a one-level tree */
						if (n4->children[i] != NULL)
							appendStringInfo(buf, " %lu\n", *((RT_VALUE_TYPE *) n4->children[i]));
						else
							appendStringInfo(buf, " (NULL)\n");
#endif
					}
				}
				break;
			}
		case RT_NODE_KIND_16:
			{
				for (int i = 0; i < node->count; i++)
				{
#if 0
					if (RT_NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_16 *n16 = (RT_NODE_LEAF_16 *) node;

						appendStringInfo(buf, "%schunk[%d] 0x%X\n",
										 spaces.data, i, n16->base.chunks[i]);
					}
					else
#endif
					{
						RT_NODE_16 *n16 = (RT_NODE_16 *) node;

						appendStringInfo(buf, "%schunk[%d] 0x%X",
										 spaces.data, i, n16->base.chunks[i]);

#if 0
						if (recurse)
						{
							appendStringInfo(buf, "\n");
							RT_DUMP_NODE(n16->children[i], level + 1,
										 recurse, buf);
						}
						else
#endif
						if (n16->children[i] != RT_INVALID_PTR_ALLOC)
							appendStringInfo(buf, " %lu\n", *((RT_VALUE_TYPE *) n16->children[i]));
						else
							appendStringInfo(buf, " (NULL)\n");

					}
				}
				break;
			}
		case RT_NODE_KIND_48:
			{
				RT_NODE_BASE_48 *b125 = (RT_NODE_BASE_48 *) node;
				char *sep = "";

				appendStringInfo(buf, "%sslot_idxs: ", spaces.data);
				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_48_IS_CHUNK_USED(b125, i))
						continue;

					appendStringInfo(buf, "%s[%d]=%d ",
									 sep, i, b125->slot_idxs[i]);
					sep = ",";
				}

				appendStringInfo(buf, "\n%sisset-bitmap: ", spaces.data);
				for (int i = 0; i < (RT_SLOT_IDX_LIMIT / BITS_PER_BYTE); i++)
					appendStringInfo(buf, "%X ", ((uint8 *) b125->isset)[i]);
				appendStringInfo(buf, "\n");

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (!RT_NODE_48_IS_CHUNK_USED(b125, i))
						continue;
#if 0
					if (RT_NODE_IS_LEAF(node))
						appendStringInfo(buf, "%schunk 0x%X\n",
										 spaces.data, i);
					else
#endif
					{
						appendStringInfo(buf, "%schunk 0x%X",
										 spaces.data, i);

#if 0
						if (recurse)
						{
							RT_NODE_48 *n48 = (RT_NODE_48 *) b125;

							appendStringInfo(buf, "\n");
							RT_DUMP_NODE(*(RT_NODE_48_GET_CHILD(n48, i)),
										 level + 1, recurse, buf);
						}
						else
#endif
							appendStringInfo(buf, " (skipped)\n");
					}
				}
				break;
			}
#if 0
		case RT_NODE_KIND_256:
			{
				if (RT_NODE_IS_LEAF(node))
				{
					RT_NODE_LEAF_256 *n256 = (RT_NODE_LEAF_256 *) node;

					appendStringInfo(buf, "%sisset-bitmap: ", spaces.data);
					for (int i = 0; i < (RT_SLOT_IDX_LIMIT / BITS_PER_BYTE); i++)
						appendStringInfo(buf, "%X ", ((uint8 *) n256->isset)[i]);
					appendStringInfo(buf, "\n");
				}

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (RT_NODE_IS_LEAF(node))
					{
						RT_NODE_LEAF_256 *n256 = (RT_NODE_LEAF_256 *) node;

						if (!RT_NODE_LEAF_256_IS_CHUNK_USED(n256, i))
							continue;

						appendStringInfo(buf, "%schunk 0x%X\n",
										 spaces.data, i);
					}
					else
					{
						RT_NODE_256 *n256 = (RT_NODE_256 *) node;

						if (!RT_NODE_256_IS_CHUNK_USED(n256, i))
							continue;

						appendStringInfo(buf, "%schunk 0x%X",
										 spaces.data, i);

						if (recurse)
						{
							appendStringInfo(buf, "\n");
							RT_DUMP_NODE(RT_NODE_256_GET_CHILD(n256, i),
										 level + 1, recurse, buf);
						}
						else
							appendStringInfo(buf, " (skipped)\n");
					}
				}
				break;
			}
#endif
	}
#endif
}

/*
 * Verify the radix tree node.
 */
// XXX somewhat whacked around to allow dumping single node types for debugging
static void
RT_VERIFY_NODE(RT_PTR_LOCAL node)
{
#ifdef USE_ASSERT_CHECKING
	StringInfoData buf;

	initStringInfo(&buf);

	switch (node->kind)
	{
		case RT_NODE_KIND_4:
			{
				RT_NODE_BASE_4 *n4 = (RT_NODE_BASE_4 *) node;

				if (false)
				{
					RT_DUMP_NODE(node, 0, false, &buf);
					fprintf(stderr, "%s",buf.data);
				}

				for (int i = 1; i < n4->n.count; i++)
					Assert(n4->chunks[i - 1] < n4->chunks[i]);

				break;
			}
		case RT_NODE_KIND_16:
			{
				RT_NODE_BASE_16 *n16 = (RT_NODE_BASE_16 *) node;

				if (false)
				{
					RT_DUMP_NODE(node, 0, false, &buf);
					fprintf(stderr, "%s",buf.data);
				}

				for (int i = 1; i < n16->n.count; i++)
					Assert(n16->chunks[i - 1] < n16->chunks[i]);

				break;
			}
		case RT_NODE_KIND_48:
			{
				RT_NODE_BASE_48 *n48 = (RT_NODE_BASE_48 *) node;
				int			cnt = 0;

				if (false)
				{
					RT_DUMP_NODE(node, 0, false, &buf);
					fprintf(stderr, "%s",buf.data);
				}

				for (int i = 0; i < RT_NODE_MAX_SLOTS; i++)
				{
					uint8		slot = n48->slot_idxs[i];
					int			idx = RT_BM_IDX(slot);
					int			bitnum = RT_BM_BIT(slot);

					if (!RT_NODE_48_IS_CHUNK_USED(n48, i))
						continue;

					/* Check if the corresponding slot is used */
					Assert(slot < node->fanout);
					Assert((n48->isset[idx] & ((bitmapword) 1 << bitnum)) != 0);

					cnt++;
				}

				Assert(n48->n.count == cnt);

				break;
			}
		case RT_NODE_KIND_256:
			{
				// FIXME
				{
#if 0
					RT_NODE_LEAF_256 *n256 = (RT_NODE_LEAF_256 *) node;
					int			cnt = 0;

					for (int i = 0; i < RT_BM_IDX(RT_NODE_MAX_SLOTS); i++)
						cnt += bmw_popcount(n256->isset[i]);

					/* Check if the number of used chunk matches, accounting for overflow */
					if (cnt == 256)
						Assert(n256->base.n.count == 0);
					else
						Assert(n256->base.n.count == cnt);
#endif
					break;
				}
			}
	}

	pfree(buf.data);
#endif
}


/***************** DEBUG FUNCTIONS *****************/

#define RT_UINT64_FORMAT_HEX "%" INT64_MODIFIER "X"

RT_SCOPE void pg_attribute_unused()
RT_STATS(RT_RADIX_TREE *tree)
{
#ifdef RT_DEBUG
	RT_LOCK_SHARED(tree);

	fprintf(stderr, "max_val = " UINT64_FORMAT "\n", tree->ctl->max_val);
	fprintf(stderr, "num_keys = " UINT64_FORMAT "\n", tree->ctl->num_keys);

#ifdef RT_SHMEM
	fprintf(stderr, "handle = " UINT64_FORMAT "\n", tree->ctl->handle);
#endif

	if (RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
	{
		fprintf(stderr, "height = %d", tree->ctl->start_shift / RT_SPAN);

		for (int i = 0; i < RT_SIZE_CLASS_COUNT; i++)
		{
			RT_SIZE_CLASS_ELEM size_class = RT_SIZE_CLASS_INFO[i];
			fprintf(stderr, ", n%d = %u", size_class.fanout, tree->ctl->cnt[i]);
		}

		fprintf(stderr, "\n");
	}

	RT_UNLOCK(tree);
#endif
}


#if 0
RT_SCOPE void
RT_DUMP_SEARCH(RT_RADIX_TREE *tree, uint64 key)
{
	RT_PTR_ALLOC allocnode;
	RT_PTR_LOCAL node;
	StringInfoData buf;
	int			shift;
	int			level = 0;

	RT_STATS(tree);

	RT_LOCK_SHARED(tree);

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
	{
		RT_UNLOCK(tree);
		fprintf(stderr, "empty tree\n");
		return;
	}

	if (key > tree->ctl->max_val)
	{
		RT_UNLOCK(tree);
		fprintf(stderr, "key " UINT64_FORMAT "(0x" RT_UINT64_FORMAT_HEX ") is larger than max val\n",
				key, key);
		return;
	}

	initStringInfo(&buf);
	allocnode = tree->ctl->root;
	node = RT_PTR_SET_LOCAL(tree, allocnode);
	shift = node->shift;
	while (shift >= 0)
	{
		RT_PTR_ALLOC child;

		RT_DUMP_NODE(tree, allocnode, level, false, &buf);

		if (RT_NODE_IS_LEAF(node))
		{
			RT_VALUE_TYPE	dummy;

			/* We reached at a leaf node, find the corresponding slot */
			RT_NODE_SEARCH_LEAF(node, key, &dummy);

			break;
		}

		if (!RT_NODE_SEARCH(node.local, RT_GET_KEY_CHUNK(key, shift)))
			break;

		allocnode = child;
		node = RT_PTR_SET_LOCAL(tree, allocnode);
		shift -= RT_SPAN;
		level++;
	}
	RT_UNLOCK(tree);

	fprintf(stderr, "%s", buf.data);
	pfree(buf.data);
}

// this might be better as "iterate over nodes", plus a callback to RT_DUMP_NODE,
// which should really only concern itself with single nodes
RT_SCOPE void
RT_DUMP(RT_RADIX_TREE *tree)
{
	StringInfoData buf;

	RT_STATS(tree);

	RT_LOCK_SHARED(tree);

	if (!RT_PTR_ALLOC_IS_VALID(tree->ctl->root))
	{
		RT_UNLOCK(tree);
		fprintf(stderr, "empty tree\n");
		return;
	}

	initStringInfo(&buf);

	RT_DUMP_NODE(tree->ctl->root, 0, true, &buf);
	RT_UNLOCK(tree);

	fprintf(stderr, "%s",buf.data);
	pfree(buf.data);
}

#endif /* 0 */


#endif							/* RT_DEFINE */


/* undefine external parameters, so next radix tree can be defined */
#undef RT_PREFIX
#undef RT_SCOPE
#undef RT_DECLARE
#undef RT_DEFINE
#undef RT_VALUE_TYPE

/* locally declared macros */
#undef RT_MAKE_PREFIX
#undef RT_MAKE_NAME
#undef RT_MAKE_NAME_
#undef RT_SPAN
#undef RT_NODE_MAX_SLOTS
#undef RT_CHUNK_MASK
#undef RT_MAX_SHIFT
#undef RT_MAX_LEVEL
#undef RT_GET_KEY_CHUNK
#undef RT_BM_IDX
#undef RT_BM_BIT
#undef RT_LOCK_EXCLUSIVE
#undef RT_LOCK_SHARED
#undef RT_UNLOCK
#undef RT_NODE_MUST_GROW
#undef RT_NODE_KIND_COUNT
#undef RT_SIZE_CLASS_COUNT
#undef RT_SLOT_IDX_LIMIT
#undef RT_INVALID_SLOT_IDX
#undef RT_SLAB_BLOCK_SIZE
#undef RT_RADIX_TREE_MAGIC
#undef RT_UINT64_FORMAT_HEX

/* type declarations */
#undef RT_RADIX_TREE
#undef RT_RADIX_TREE_CONTROL
#undef RT_NODE_PTR
#undef RT_PTR_LOCAL
#undef RT_PTR_ALLOC
#undef RT_INVALID_PTR_ALLOC
#undef RT_HANDLE
#undef RT_ITER
#undef RT_NODE
#undef RT_NODE_ITER
#undef RT_NODE_KIND_4
#undef RT_NODE_KIND_16
#undef RT_NODE_KIND_48
#undef RT_NODE_KIND_256
#undef RT_NODE_BASE_4
#undef RT_NODE_BASE_16
#undef RT_NODE_BASE_48
#undef RT_NODE_BASE_256
#undef RT_NODE_4
#undef RT_NODE_16
#undef RT_NODE_48
#undef RT_NODE_256
#undef RT_SIZE_CLASS
#undef RT_SIZE_CLASS_ELEM
#undef RT_SIZE_CLASS_INFO
#undef RT_CLASS_4
#undef RT_CLASS_16_LO
#undef RT_CLASS_16_HI
#undef RT_CLASS_48
#undef RT_CLASS_256
#undef RT_FANOUT_4
#undef RT_FANOUT_16_LO
#undef RT_FANOUT_16_HI
#undef RT_FANOUT_48
#undef RT_FANOUT_256

/* function declarations */
#undef RT_CREATE
#undef RT_FREE
#undef RT_ATTACH
#undef RT_DETACH
#undef RT_GET_HANDLE
#undef RT_SEARCH
#undef RT_FIND
#undef RT_SET
#undef RT_GET
#undef RT_BEGIN_ITERATE
#undef RT_ITERATE_NEXT
#undef RT_ITERATE_NEXT_PTR
#undef RT_END_ITERATE
#undef RT_USE_DELETE
#undef RT_DELETE
#undef RT_MEMORY_USAGE
#undef RT_DUMP
#undef RT_DUMP_NODE
#undef RT_DUMP_SEARCH
#undef RT_STATS

/* internal helper functions */
#undef RT_NEW_ROOT
#undef RT_GET_SLOT_RECURSIVE
#undef RT_RECURSIVE_DELETE
#undef RT_ALLOC_NODE
#undef RT_ALLOC_LEAF
#undef RT_FREE_NODE
#undef RT_FREE_LEAF
#undef RT_FREE_RECURSE
#undef RT_EXTEND_UP
#undef RT_EXTEND_DOWN
#undef RT_COPY_COMMON
#undef RT_PTR_SET_LOCAL
#undef RT_PTR_ALLOC_IS_VALID
#undef RT_NODE_4_SEARCH_EQ
#undef RT_NODE_16_SEARCH_EQ
#undef RT_NODE_4_GET_INSERTPOS
#undef RT_NODE_16_GET_INSERTPOS
#undef RT_CHUNK_CHILDREN_ARRAY_SHIFT
#undef RT_CHUNK_CHILDREN_ARRAY_DELETE
#undef RT_CHUNK_CHILDREN_ARRAY_COPY
#undef RT_NODE_48_IS_CHUNK_USED
#undef RT_NODE_48_GET_CHILD
#undef RT_NODE_256_IS_CHUNK_USED
#undef RT_NODE_256_GET_CHILD
#undef RT_NODE_256_SET
#undef RT_NODE_256_DELETE
#undef RT_KEY_GET_SHIFT
#undef RT_SHIFT_GET_MAX_VAL
#undef RT_NODE_SEARCH
#undef RT_ADD_CHILD_4
#undef RT_ADD_CHILD_16
#undef RT_ADD_CHILD_48
#undef RT_ADD_CHILD_256
#undef RT_GROW_NODE_4
#undef RT_GROW_NODE_16
#undef RT_GROW_NODE_48
#undef RT_GROW_NODE_256
#undef RT_REMOVE_CHILD_4
#undef RT_REMOVE_CHILD_16
#undef RT_REMOVE_CHILD_48
#undef RT_REMOVE_CHILD_256
#undef RT_NODE_DELETE
#undef RT_NODE_INSERT
#undef RT_NODE_ITERATE_NEXT
#undef RT_RT_ITER_SET_NODE_FROM
#undef RT_VERIFY_NODE

#undef RT_DEBUG
