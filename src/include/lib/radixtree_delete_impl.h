/* TODO: shrink nodes */

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
	Assert(RT_NODE_IS_LEAF(node));
#else
	Assert(!RT_NODE_IS_LEAF(node));
#endif

	switch (node->kind)
	{
		case RT_NODE_KIND_3:
			{
				RT_NODE3_TYPE *n3 = (RT_NODE3_TYPE *) node;
				int			idx = RT_NODE_3_SEARCH_EQ((RT_NODE_BASE_3 *) n3, chunk);

				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				RT_CHUNK_VALUES_ARRAY_DELETE(n3->base.chunks, n3->values,
										  n3->base.n.count, idx);
#else
				RT_CHUNK_CHILDREN_ARRAY_DELETE(n3->base.chunks, n3->children,
											n3->base.n.count, idx);
#endif
				break;
			}
		case RT_NODE_KIND_32:
			{
				RT_NODE32_TYPE *n32 = (RT_NODE32_TYPE *) node;
				int			idx = RT_NODE_32_SEARCH_EQ((RT_NODE_BASE_32 *) n32, chunk);

				if (idx < 0)
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				RT_CHUNK_VALUES_ARRAY_DELETE(n32->base.chunks, n32->values,
										  n32->base.n.count, idx);
#else
				RT_CHUNK_CHILDREN_ARRAY_DELETE(n32->base.chunks, n32->children,
											n32->base.n.count, idx);
#endif
				break;
			}
		case RT_NODE_KIND_125:
			{
				RT_NODE125_TYPE *n125 = (RT_NODE125_TYPE *) node;
				int			slotpos = n125->base.slot_idxs[chunk];
				int			idx;
				int			bitnum;

				if (slotpos == RT_INVALID_SLOT_IDX)
					return false;

				idx = BM_IDX(slotpos);
				bitnum = BM_BIT(slotpos);
				n125->base.isset[idx] &= ~((bitmapword) 1 << bitnum);
				n125->base.slot_idxs[chunk] = RT_INVALID_SLOT_IDX;

				break;
			}
		case RT_NODE_KIND_256:
			{
				RT_NODE256_TYPE *n256 = (RT_NODE256_TYPE *) node;

#ifdef RT_NODE_LEVEL_LEAF
				if (!RT_NODE_LEAF_256_IS_CHUNK_USED(n256, chunk))
#else
				if (!RT_NODE_INNER_256_IS_CHUNK_USED(n256, chunk))
#endif
					return false;

#ifdef RT_NODE_LEVEL_LEAF
				RT_NODE_LEAF_256_DELETE(n256, chunk);
#else
				RT_NODE_INNER_256_DELETE(n256, chunk);
#endif
				break;
			}
	}

	/* update statistics */
	node->count--;

	return true;

#undef RT_NODE3_TYPE
#undef RT_NODE32_TYPE
#undef RT_NODE125_TYPE
#undef RT_NODE256_TYPE
