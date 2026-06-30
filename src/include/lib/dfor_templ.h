/*
 * File: dfor_templ.h
 */
#include "dfor_templ_staple.h"

extern int dfor_calc_deltas(size_t cnt, const item_t arr[], vect_t *vDeltas,
							uniqsortvect_t *usvDeltaWidths,
							vect_t *vWidthCounters, bool *sign);

extern int dfor_calc_width(size_t cntDelta,
						   const uniqsortvect_t *usvDeltaWidths,
						   const vect_t *vWidthCounters, size_t *width,
						   size_t *cntExceptions);

extern int dfor_pack(size_t cnt, const item_t arr[], excalg_t isExcUsage,
					 dfor_meta_t *dfor, size_t bufSize, uint8_t buf[]);

extern int dfor_unpack(const dfor_meta_t *dfor, uniqsortvect_t *vVals,
					   size_t bufSize, uint8_t buf[]);

extern void dfor_clear_meta(dfor_meta_t *dfor);

extern dfor_stats_t dfor_calc_stats(dfor_meta_t dfor);

extern size_t dfor_calc_nbytes(dfor_meta_t dfor);

#include "dfor_templ_undef.h"
