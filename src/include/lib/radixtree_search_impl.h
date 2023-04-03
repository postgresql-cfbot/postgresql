/*-------------------------------------------------------------------------
 *
 * radixtree_search_impl.h
 *	  Common implementation for search in leaf and inner nodes, plus
 *	  update for inner nodes only.
 *
 * Note: There is deliberately no #include guard here
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/lib/radixtree_search_impl.h
 *
 *-------------------------------------------------------------------------
 */

#if defined(RT_NODE_LEVEL_INNER)
#define RT_NODE3_TYPE RT_NODE_INNER_3
#define RT_NODE32_TYPE RT_NODE_INNER_32
#define RT_NODE125_TYPE RT_NODE_INNER_125
#define RT_NODE256_TYPE RT_NODE_INNER_256
#elif defined(RT_NODE_LEVEL_LEAF)
#define RT_NODE3_TYPE RT_NODE_LEAF_3
#define RT_NODE32_TYPE RT_NODE_LEAF_32
#define RT_NODE125_TYPE RT_NODE_LEAF_125
#define RT_NODE256_TYPE RT_NODE_LEAF_256
#else
#error node level must be either inner or leaf
#endif

	uint8		chunk = RT_GET_KEY_CHUNK(key, node->shift);

#ifdef RT_NODE_LEVEL_LEAF
	Assert(value_p != NULL);
	Assert(RT_NODE_IS_LEAF(node));
#else
#ifndef RT_ACTION_UPDATE
	Assert(child_p != NULL);
#endif
	Assert(!RT_NODE_IS_LEAF(node));
#endif

	switch (node->kind)
	{
		case RT_NODE_KIND_3:
			{
				RT_NODE3_TYPE *n3 = (RT_NODE3_TYPE *) node;
				int			idx = RT_NODE_3_SEARCH_EQ((RT_NODE_BASE_3 *) n3, chunk);

#ifdef RT_ACTION_UPDATE
				Assert(idx >= 0);
				n3->children[idx] = new_child;
#else
				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = n3->values[idx];
#else
				*child_p = n3->children[idx];
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node;
				int			idx = RT_NODE_32_SEARCH_EQ((RT_NODE_BASE_32 *) n32, chunk);

#ifdef RT_ACTION_UPDATE
				Assert(idx >= 0);
				n32->children[idx] = new_child;
#else
				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = n32->values[idx];
#else
				*child_p = n32->children[idx];
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node;
				int			slotpos = n125->base.slot_idxs[chunk];

#ifdef RT_ACTION_UPDATE
				Assert(slotpos != RT_INVALID_SLOT_IDX);
				n125->children[slotpos] = new_child;
#else
				if (slotpos == RT_INVALID_SLOT_IDX)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = RT_NODE_LEAF_125_GET_VALUE(n125, chunk);
#else
				*child_p = RT_NODE_INNER_125_GET_CHILD(n125, chunk);
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node;

#ifdef RT_ACTION_UPDATE
				RT_NODE_INNER_256_SET(n256, chunk, new_child);
#else
#ifdef RT_NODE_LEVEL_LEAF
				if (!RT_NODE_LEAF_256_IS_CHUNK_USED(n256, chunk))
#else
				if (!RT_NODE_INNER_256_IS_CHUNK_USED(n256, chunk))
#endif
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = RT_NODE_LEAF_256_GET_VALUE(n256, chunk);
#else
				*child_p = RT_NODE_INNER_256_GET_CHILD(n256, chunk);
#endif
#endif							/* RT_ACTION_UPDATE */
				break;
			}
	}

#ifdef RT_ACTION_UPDATE
	return;
#else
	return true;
#endif							/* RT_ACTION_UPDATE */

#undef RT_NODE3_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
