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

	bool		found = false;
	uint8		key_chunk;

#ifdef RT_NODE_LEVEL_LEAF
	RT_VALUE_TYPE		value;

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

				node_iter->current_idx++;
				if (node_iter->current_idx >= n3->base.n.count)
					break;
#ifdef RT_NODE_LEVEL_LEAF
				value = n3->values[node_iter->current_idx];
#else
				child = RT_PTR_GET_LOCAL(iter->tree, n3->children[node_iter->current_idx]);
#endif
				key_chunk = n3->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node_iter->node;

				node_iter->current_idx++;
				if (node_iter->current_idx >= n32->base.n.count)
					break;

#ifdef RT_NODE_LEVEL_LEAF
				value = n32->values[node_iter->current_idx];
#else
				child = RT_PTR_GET_LOCAL(iter->tree, n32->children[node_iter->current_idx]);
#endif
				key_chunk = n32->base.chunks[node_iter->current_idx];
				found = true;
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node_iter->node;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
					if (RT_NODE_125_IS_CHUNK_USED((RT_NODE_BASE_125 *) n125, i))
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
#ifdef RT_NODE_LEVEL_LEAF
				value = RT_NODE_LEAF_125_GET_VALUE(n125, i);
#else
				child = RT_PTR_GET_LOCAL(iter->tree, RT_NODE_INNER_125_GET_CHILD(n125, i));
#endif
				key_chunk = i;
				found = true;
				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node_iter->node;
				int			i;

				for (i = node_iter->current_idx + 1; i < RT_NODE_MAX_SLOTS; i++)
				{
#ifdef RT_NODE_LEVEL_LEAF
					if (RT_NODE_LEAF_256_IS_CHUNK_USED(n256, i))
#else
					if (RT_NODE_INNER_256_IS_CHUNK_USED(n256, i))
#endif
						break;
				}

				if (i >= RT_NODE_MAX_SLOTS)
					break;

				node_iter->current_idx = i;
#ifdef RT_NODE_LEVEL_LEAF
				value = RT_NODE_LEAF_256_GET_VALUE(n256, i);
#else
				child = RT_PTR_GET_LOCAL(iter->tree, RT_NODE_INNER_256_GET_CHILD(n256, i));
#endif
				key_chunk = i;
				found = true;
				break;
			}
	}

	if (found)
	{
		RT_ITER_UPDATE_KEY(iter, key_chunk, node_iter->node->shift);
#ifdef RT_NODE_LEVEL_LEAF
		*value_p = value;
#endif
	}

#ifdef RT_NODE_LEVEL_LEAF
	return found;
#else
	return child;
#endif

#undef RT_NODE3_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
