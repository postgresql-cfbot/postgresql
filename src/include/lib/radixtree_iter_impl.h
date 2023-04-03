/*-------------------------------------------------------------------------
 *
 * radixtree_iter_impl.h
 *	  Common implementation for iteration in leaf and inner nodes.
 *
 * Note: There is deliberately no #include guard here
 *
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/lib/radixtree_iter_impl.h
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

	uint8		key_chunk = 0;

#ifdef RT_NODE_LEVEL_LEAF
	Assert(value_p != NULL);
	Assert(RT_NODE_IS_LEAF(node_iter->node));
#else
	RT_PTR_LOCAL child = NULL;

	Assert(!RT_NODE_IS_LEAF(node_iter->node));
#endif

#ifdef RT_SHMEM
	Assert(iter->tree->ctl->magic == RT_RADIX_TREE_MAGIC);
#endif

	switch (node_iter->node->kind)
	{
		case RT_NODE_KIND_3:
			{
				RT_NODE3_TYPE *n3 = (RT_NODE3_TYPE *) node_iter->node;

				if (node_iter->idx >= n3->base.n.count)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = n3->values[node_iter->idx];
#else
				child = RT_PTR_GET_LOCAL(iter->tree, n3->children[node_iter->idx]);
#endif
				key_chunk = n3->base.chunks[node_iter->idx];
				node_iter->idx++;
				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node_iter->node;

				if (node_iter->idx >= n32->base.n.count)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = n32->values[node_iter->idx];
#else
				child = RT_PTR_GET_LOCAL(iter->tree, n32->children[node_iter->idx]);
#endif
				key_chunk = n32->base.chunks[node_iter->idx];
				node_iter->idx++;
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node_iter->node;
				int			chunk;

				for (chunk = node_iter->idx; chunk < RT_NODE_MAX_SLOTS; chunk++)
				{
					if (RT_NODE_125_IS_CHUNK_USED((RT_NODE_BASE_125 *) n125, chunk))
						break;
				}

				if (chunk >= RT_NODE_MAX_SLOTS)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = RT_NODE_LEAF_125_GET_VALUE(n125, chunk);
#else
				child = RT_PTR_GET_LOCAL(iter->tree, RT_NODE_INNER_125_GET_CHILD(n125, chunk));
#endif
				key_chunk = chunk;
				node_iter->idx = chunk + 1;
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node_iter->node;
				int			chunk;

				for (chunk = node_iter->idx; chunk < RT_NODE_MAX_SLOTS; chunk++)
				{
#ifdef RT_NODE_LEVEL_LEAF
					if (RT_NODE_LEAF_256_IS_CHUNK_USED(n256, chunk))
#else
					if (RT_NODE_INNER_256_IS_CHUNK_USED(n256, chunk))
#endif
						break;
				}

				if (chunk >= RT_NODE_MAX_SLOTS)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				*value_p = RT_NODE_LEAF_256_GET_VALUE(n256, chunk);
#else
				child = RT_PTR_GET_LOCAL(iter->tree, RT_NODE_INNER_256_GET_CHILD(n256, chunk));
#endif
				key_chunk = chunk;
				node_iter->idx = chunk + 1;
				break;
			}
	}

	/* Update the part of the key */
	iter->key &= ~(((uint64) RT_CHUNK_MASK) << node_iter->node->shift);
	iter->key |= (((uint64) key_chunk) << node_iter->node->shift);

#ifdef RT_NODE_LEVEL_LEAF
	return true;
#else
	return child;
#endif

#undef RT_NODE3_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
