/*
 * bitpack_templ.c
 *
 * The BITPACK unit implements routines pertaining to bit-packing. The bitpack
 * unit allow higher-level functions to create high-density arrays packed
 * bit-by-bit. In general, width of each item in a bitpacked array can vary and
 * have not to be of fixed size, items can have different length.
 */

#include "lib/bitpack_templ_staple.h"

item_t width_from_val(item_t val);
item_t width_to_mask(size_t width);
size_t bitpack_pack(uint8_t *pack, size_t caret, item_t item,
					size_t szItemWidth);
item_t bitpack_unpack(const uint8_t *pack, size_t *caret, size_t szItemWidth);

/*
 * Since width of item_t cannot be more than length of item_t
 * lg(MAX(item_t))+1, we use the item_t type for returned value
 */
item_t
width_from_val(item_t val)
{
	item_t width = 0;

	while (val) {
		width++;
		val = val >> 1;
	}

	return width == 0 ? 1 : width;
}

item_t
width_to_mask(size_t width)
{
	size_t mask = 0;

	Assert(width != 0);
	Assert(width <= sizeof(item_t) * 8);

	if (likely(width < sizeof(size_t)))
		mask = (1 << width) - 1;
	else
		while (width--)
			mask = (mask << 1) | 1;

	return (item_t)mask;
}

size_t
bitpack_pack(uint8_t *pack, size_t caret, item_t item, size_t szItemWidth)
{
	size_t szItemWidthToGo = szItemWidth;
	item_t itmMaskToGo = width_to_mask(szItemWidth);

	while (szItemWidthToGo > 0) {
		size_t cntSavedBits;
		size_t byte = caret / 8;
		size_t off = caret % 8;
		uint8_t ubChunk = (uint8_t)item << off;
		item_t itmChunkMask = itmMaskToGo << off;
		/*
		 * Applying chunk using the mask. Setting bits to one and resetting bits
		 * to zero is only in scopes defined by the mask. Zeroing of bits
		 * according to a mask, we can use even a pack not been nulled in
		 * advance.
		 */
		pack[byte] |= (ubChunk & itmChunkMask);
		pack[byte] &= (ubChunk | ~itmChunkMask);
		cntSavedBits = (8 - off > szItemWidthToGo) ?
			szItemWidthToGo :
			8 - off; // number of saved bits
		szItemWidthToGo -= cntSavedBits;
		caret += cntSavedBits;
		item = item >> cntSavedBits;
		itmMaskToGo = itmMaskToGo >> cntSavedBits;
	}
	return caret;
}

item_t
bitpack_unpack(const uint8_t *pack, size_t *caret, size_t widItem)
{
	size_t szItemCaret;
	size_t szItemWidthToGo;
	uint8_t item[sizeof(item_t)]; /* size of item array */

	size_t szPackByte;
	size_t szPackOff;
	size_t szItemByte;
	size_t szItemOff;
	uint8_t ubChunk;

	szItemCaret = 0;
	szItemWidthToGo = widItem;
	memset(item, 0, sizeof(item_t));

	while (szItemWidthToGo > 0) {
		size_t szChunkSize;
		size_t szChunkLowSize, szChunkHighSize;

		szPackByte = *caret / 8;
		szPackOff = *caret % 8;
		szItemByte = szItemCaret / 8;
		szItemOff = szItemCaret % 8;

		ubChunk = pack[szPackByte] >> szPackOff;

		szChunkSize = 8 - szPackOff;
		if (szItemWidthToGo < szChunkSize) {
			szChunkSize = szItemWidthToGo;
			ubChunk = ubChunk & (uint8_t)width_to_mask(szItemWidthToGo);
		}

		if (szChunkSize > (8 - szItemOff)) /* Free space of item[szItemByte] */
		{
			szChunkLowSize = 8 - szItemOff;
			szChunkHighSize = szChunkSize - szChunkLowSize;
		} else {
			szChunkLowSize = szChunkSize;
			szChunkHighSize = 0;
		}

		item[szItemByte] |= ubChunk << szItemOff; /* chunk_low */

		if (szChunkHighSize != 0) {
			Assert((szItemByte + 1) < sizeof(item_t)); /* size of item array */
			item[szItemByte + 1] |= ubChunk >> szChunkLowSize; /* chunk_high */
		}

		*caret += szChunkSize;
		szItemCaret += szChunkSize;
		szItemWidthToGo -= szChunkSize;
	}

	/*
	 * Reordering bytes in accordance with endianness of the system.
	 *
	 * Here for a Little-endian system we can avoid reordering, but in such a
	 * case we need to keep the item array aligned with item_t type, but we do
	 * not keep.
	 */
	{
		size_t j = 1;
		item_t val = item[sizeof(item_t) - j];
		while (++j <= sizeof(item_t)) {
			val = val << 8;
			val |= item[sizeof(item_t) - j];
		}
		return val;
	}
}

#include "lib/bitpack_templ_undef.h"
