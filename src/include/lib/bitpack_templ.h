/*
 * bitpack_templ.h
 *
 */

#include "bitpack_templ_staple.h"

extern item_t width_from_val(item_t val);
extern item_t width_to_mask(size_t width);
extern size_t bitpack_pack(uint8_t *pack, size_t caret, item_t item,
						   size_t szItemWidth);
extern item_t bitpack_unpack(const uint8_t *pack, size_t *caret, size_t szItemWidth);

#include "bitpack_templ_undef.h"
