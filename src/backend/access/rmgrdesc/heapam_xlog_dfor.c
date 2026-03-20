#include "lib/bitpack_u16.h"
#include "access/heapam_xlog_dfor.h"

bool wal_prune_dfor_compression = true;

size_t
log_heap_prune_and_freeze_pack_meta(const dfor_meta_t *meta, uint8 buf[])
{
	size_t caret = 0;
	caret = bitpack_u16_pack(buf, caret, meta->item_cnt,
							 XLHPF_META_ITEM_COUNT_SZ);
	caret = bitpack_u16_pack(buf, caret, meta->delta_wid,
							 XLHPF_META_DELTA_WIDTH_SZ);
	caret = bitpack_u16_pack(buf, caret, (meta->signed_deltas == 0) ? 0 : 1,
							 XLHPF_META_SIGNEDDELTAS_FLAG_SZ);
	caret = bitpack_u16_pack(buf, caret, (meta->exc_cnt == 0) ? 0 : 1,
							 XLHPF_META_EXCEPTION_FLAG_SZ);
	if (meta->exc_cnt != 0)
	{
		caret = bitpack_u16_pack(buf, caret, meta->exc_cnt,
								 XLHPF_META_EXCEPTION_COUNT_SZ);
		caret = bitpack_u16_pack(buf, caret, meta->exc_wid,
								 XLHPF_META_EXCEPTION_WIDTH_SZ);
		caret = bitpack_u16_pack(buf, caret, meta->exc_pos_wid,
								 XLHPF_META_EXCEPTION_POSITION_WIDTH_SZ);
	}
	Assert(meta->item_cnt != 0);
	Assert(meta->delta_wid != 0);
	Assert(meta->nbytes != 0);

#ifdef USE_ASSERT_CHECKING
	{
		dfor_meta_t checker;
		log_heap_prune_and_freeze_unpack_meta(&checker, buf);
		Assert(meta->item_cnt == checker.item_cnt);
		Assert(meta->delta_wid == checker.delta_wid);
		Assert(meta->signed_deltas == checker.signed_deltas);
		Assert(meta->exc_cnt == checker.exc_cnt);
		Assert(meta->exc_wid == checker.exc_wid);
		Assert(meta->exc_pos_wid == checker.exc_pos_wid);
	}
#endif
	return (caret + 7) / 8; /* the length of packed dfor_meta, in bytes*/
}

size_t
log_heap_prune_and_freeze_unpack_meta(dfor_meta_t *meta,
									  const uint8 packed_meta[])
{
	size_t caret = 0;
	bool exc;

	meta->item_cnt = bitpack_u16_unpack(packed_meta, &caret,
										XLHPF_META_ITEM_COUNT_SZ);
	meta->delta_wid = bitpack_u16_unpack(packed_meta, &caret,
										 XLHPF_META_DELTA_WIDTH_SZ);
	meta->signed_deltas = bitpack_u16_unpack(packed_meta, &caret,
											 XLHPF_META_SIGNEDDELTAS_FLAG_SZ);
	exc = bitpack_u16_unpack(packed_meta, &caret, XLHPF_META_EXCEPTION_FLAG_SZ);

	if (exc)
	{
		meta->exc_cnt = bitpack_u16_unpack(packed_meta, &caret,
										 XLHPF_META_EXCEPTION_COUNT_SZ);
		meta->exc_wid = bitpack_u16_unpack(packed_meta, &caret,
										 XLHPF_META_EXCEPTION_WIDTH_SZ);
		meta->exc_pos_wid =
			bitpack_u16_unpack(packed_meta, &caret,
							   XLHPF_META_EXCEPTION_POSITION_WIDTH_SZ);
	}
	else
	{
		meta->exc_cnt = 0;
		meta->exc_wid = 0;
		meta->exc_pos_wid = 0;
	}
	meta->nbytes = dfor_u16_calc_nbytes(*meta);

	Assert(meta->item_cnt != 0);
	Assert(meta->delta_wid != 0);
	Assert(meta->nbytes != 0);

	return (caret + 7) / 8;
}

void
heap_xlog_deserialize_dfor(char **cursor, int *nitems,
						   OffsetNumber **items,
						   uint8 dfor_buf[])
{
	dfor_meta_t dfor = {0};
	size_t packed_meta_nbytes;
	uniqsortvect_u16_t vect;

	packed_meta_nbytes =
		log_heap_prune_and_freeze_unpack_meta(&dfor, (uint8*) *cursor);

	*cursor += packed_meta_nbytes;

	dfor.pack = (uint8 *)*cursor;
	dfor_u16_unpack(&dfor, &vect, 4 * DFOR_BUF_PART_SIZE,
					dfor_buf);

	*cursor += dfor.nbytes;

	Assert(dfor.nbytes != 0);

	Assert(vect.cnt != 0);
	Assert(vect.mem_is_outer == true);
	Assert((void*)vect.m == (void*)dfor_buf);

	*nitems = vect.cnt;
	*items = vect.m;
}
