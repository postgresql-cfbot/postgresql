#ifndef HEAPAM_XLOG_DFOR_H
#define HEAPAM_XLOG_DFOR_H

#include "postgres.h"

#include "access/htup_details.h"
#include "lib/dfor_u16.h"
#include "storage/bufpage.h"

/*
 * DFoR's meta block for PRUNE/FREEZE record
 *
 * A meta block contains parameters required for decompression of the following
 * DFoR pack. It is densely bit-packed. If the exception flag is zero, fields
 * pertaining to exceptions is absent, which means that DFoR pack does not
 * contain exceptions. Calculation of field widths takes into account
 * next considerations:
 *
 * Max Item Count should be more or equal to MaxHeapTuplesPerPage * 2. We
 * multiply it by 2 since the sequence of redirected tuple offsets have a pair
 * of offsets for each redirection and in the worst case we can have all tuples
 * redirected.
 *
 * Since we can't calculate MaxHeapTuplesPerPage on preprocessor
 * stage, we intentionally overestimate it as:
 *       Max Item Count > BLCKSZ / Min Tuple Size = BLCKSZ / 24
 * to provide a margin. In general, depending on BLCKSZ, it should not result in
 * DFoR meta block overhead.
 * For instance, for a block size of 32768, we have Max Item Count on a page = 1366. Redirected sequences can and
 * it needs 11 bits width field.
 *
 * Size of field Item Count:
 *       ITEM_COUNT_SZ = log2(MaxItemCount) * 2.
 * We mu
 *
 * Maximum Delta Width is equal to ITEM_COUNT_SZ. So DELTA_WIDTH_SZ in a DFoR
 * meta block can be calculated as:
 *      DELTA_WIDTH_SZ >= log2(Max Delta Width) = log2(ITEM_COUNT_SZ)
 *
 * Max Exception Count = 0.1 * MaxItemCount, according to DFoR algorithm which
 * guarantees that not less than 90% of items will be covered without using
 * exceptions. So:
 *      EXCEPTION_COUNT_SZ >= log2(0.1 * MaxItemCount).
 *
 * An exception is part of a delta, exceeding choosen delta width. Exception is
 * saved in separated part of DFoR pack and, since delta width is not less
 * than 1:
 *    EXCEPTION_WIDTH_SZ >= log2(Max Delta Width - 1) = log2(ITEM_COUNT_SZ - 1).
 *
 * An exception's position shows the position of of a delta to wich the
 * exception has to be applied. Values of an exception position must cover the
 * same value range as an Item Count, so the Max Width of an Exception Position
 * is equal to width of Delta. Consequently, the size of Exception Position
 * Width calculated as:
 *     EXCEPTION_POSITION_WIDTH_SIZE = log2(Max Delta Width) = DELTA_WIDTH_SZ
 *
 * For example, Meta for BLCKSZ equal to 32768 has next sizes of field
 * | sect. | byte | bits      |   param            |  size  | range of values |
 * |-------|------|-----------|--------------------|--------|-----------------|
 * |  main | 0, 1 | 0-11      | item count         | 12 bit | 1...2730        |
 * |  main |    1 | 12-15     | delta width        |  4 bit | 1...12          |
 * |  main |    2 | 16        | signed deltas flag |  1 bit | 0...1           |
 * |  main |    2 | 17        | exception flag     |  1 bit | 0...1           |
 * | extra | 2, 3 | 18-26     | exception count    |  9 bit | 1...274         |
 * | extra |    3 | 27-29     | exception width    |  4 bit | 1...11          |
 * | extra | 3, 4 | 30-33     | except pos. width  |  4 bit | 1...12          |
 */

/*
 * The sizes of fields in the compressed DFoR Meta structure of an
 * XLOG_HEAP2_PRUNE* record.
 */
#if BLCKSZ == 32768
#define XLHPF_META_ITEM_COUNT_SZ  12
#define XLHPF_META_DELTA_WIDTH_SZ 4
#elif BLCKSZ == 16384
#define XLHPF_META_ITEM_COUNT_SZ  11
#define XLHPF_META_DELTA_WIDTH_SZ 4
#elif BLCKSZ == 8192
#define XLHPF_META_ITEM_COUNT_SZ  10
#define XLHPF_META_DELTA_WIDTH_SZ 4
#elif BLCKSZ == 4096
#define XLHPF_META_ITEM_COUNT_SZ  9
#define XLHPF_META_DELTA_WIDTH_SZ 4
#elif BLCKSZ == 2048
#define XLHPF_META_ITEM_COUNT_SZ  8
#define XLHPF_META_DELTA_WIDTH_SZ 4
#elif BLCKSZ == 1024
#define XLHPF_META_ITEM_COUNT_SZ  7
#define XLHPF_META_DELTA_WIDTH_SZ 3
#elif BLCKSZ == 512
#define XLHPF_META_ITEM_COUNT_SZ  6
#define XLHPF_META_DELTA_WIDTH_SZ 3
#elif BLCKSZ == 256
#define XLHPF_META_ITEM_COUNT_SZ  5
#define XLHPF_META_DELTA_WIDTH_SZ 3
#elif BLCKSZ == 128
#define XLHPF_META_ITEM_COUNT_SZ  4
#define XLHPF_META_DELTA_WIDTH_SZ 2
#elif BLCKSZ == 64
#define XLHPF_META_ITEM_COUNT_SZ  3
#define XLHPF_META_DELTA_WIDTH_SZ 2
#else
#error "Unsupported BLCKSZ in XLog Heap And Prune."
#endif

#define XLHPF_META_SIGNEDDELTAS_FLAG_SZ 1 /* Flag about signedness of deltas */

#define XLHPF_META_EXCEPTION_FLAG_SZ 1 /* Flag about Extra Section presence */

/* Size of Exception Count field */
#if XLHPF_META_ITEM_COUNT_SZ > 6
#define XLHPF_META_EXCEPTION_COUNT_SZ XLHPF_META_ITEM_COUNT_SZ - 3
#else
#define XLHPF_META_EXCEPTION_COUNT_SZ XLHPF_META_ITEM_COUNT_SZ 3
#endif

#define XLHPF_META_EXCEPTION_WIDTH_SZ \
	XLHPF_META_DELTA_WIDTH_SZ /* Size of Exception Width field */

#define XLHPF_META_EXCEPTION_POSITION_WIDTH_SZ \
	XLHPF_META_DELTA_WIDTH_SZ /* Size of Exception Position width field */

/* Maximal size of packed meta */
#define MAX_PACKED_META_SIZE \
	(XLHPF_META_ITEM_COUNT_SZ + XLHPF_META_DELTA_WIDTH_SZ +                  \
	 XLHPF_META_EXCEPTION_FLAG_SZ + XLHPF_META_EXCEPTION_POSITION_WIDTH_SZ + \
	 XLHPF_META_EXCEPTION_WIDTH_SZ + XLHPF_META_EXCEPTION_COUNT_SZ + 7) / 8

/* The size of a typical chunk of memory used by dfor_pack */
#define DFOR_BUF_PART_SIZE MaxHeapTuplesPerPage * sizeof(uint16)

extern bool wal_prune_dfor_compression; /* GUC */

extern size_t log_heap_prune_and_freeze_pack_meta(const dfor_meta_t *meta,
												  uint8 buf[]);

extern size_t log_heap_prune_and_freeze_unpack_meta(dfor_meta_t *meta,
													const uint8 packed_meta[]);

extern void heap_xlog_deserialize_dfor(char **cursor, int *nitems,
									   OffsetNumber **items, uint8 dfor_buf[]);

#endif							/* HEAPAM_XLOG_DFOR_H */