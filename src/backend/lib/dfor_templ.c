/*
 * dfor_templ.c
 *
 * Implement the variant of the Frame of Reference with Delta
 * container and corresponding algorithms.
 *
 * The current implementation of DFOR algorithm supports items of unsigned
 * integer type. The type of original items is defined with the item_t macro.
 *
 * The compressed pack comprises three parts:
 *     - deltas (least significant bits of deltas);
 *     - exceptions positions.
 *     - exceptions (most significant bits of deltas);
 * Each of them is a densily bit-packed sequence.
 *
 * The delta is a difference between the current item and the previous one. The
 * delta of the first item (the item having the zero index) is its actual value:
 * delta[0] = m[0]-0 = m[0]. A serialised delta is a sequence of bits. Each
 * serialised delta in 'deltas' has a fixed bit width. If the delta's width
 * exceeds some critical size, defined during analyzing, the higher bits of this
 * delta is put into exceptions, which position is saved into the 'excepton
 * positions' section. Usage of exceptions can be deliberately turned off.
 *
 * If signed deltas are used, the module of a delta is represented by most
 * siginificant bits and the sign is saved as the least significant bit: 0 -
 * delta is positive, 1 - delta is negative.
 *
 * DFoR supports both external memory (outer memory) provided by a caller
 * and automatically managed memory, allocated by means of malloc, palloc
 * or similar functions. Memory management configuration must be defined
 * during initialization. All subsequent operations follow this
 * configuration. For example, a caller can place a buffer on the stack to
 * avoid heap allocation and pass the buffer to a DFoR unit. As a result,
 * the packing and unpacking processes exclude dynamic allocation.
 *
 * The DFoR unit is implemented as a set of templates. Developers can
 * generate DFoR implementations for any unsigned integer type (uint8_t,
 * uint16_t, uint32_t, uint64_t).
 */

#include "lib/dfor_templ_staple.h"

int dfor_calc_deltas(size_t cnt, const item_t arr[], vect_t *vDeltas,
					 uniqsortvect_t *usvDeltaWidths, vect_t *vWidthCounters,
					 bool *sign);

int dfor_calc_width(size_t cntDelta,
					const uniqsortvect_t *usvDeltaWidths,
					const vect_t *vWidthCounters, size_t *width,
					size_t *cntExceptions);

int dfor_analyze(size_t cnt, const item_t arr[],
			 dfor_meta_t *dfor, vect_t *vDeltas,
			 uniqsortvect_t *usvExcPos);

int dfor_pack(size_t cnt, const item_t arr[], excalg_t isExcUsage,
			  dfor_meta_t *dfor, size_t bufSize, uint8_t buf[]);

int dfor_unpack(const dfor_meta_t *dfor, uniqsortvect_t *vVals, size_t bufSize,
				uint8_t buf[]);

void dfor_clear_meta(dfor_meta_t *dfor);

dfor_stats_t dfor_calc_stats(dfor_meta_t dfor);

size_t dfor_calc_nbytes(dfor_meta_t dfor);

/*
 * Calculate deltas
 *
 * vWidthCounters being equal to NULL means 'Do not calculate counts of widths'.
 * In this case usvDeltaWidth comprise only one member m[0] which saves max
 * width of delta, which can be used by caller.
 *
 * The value of the signed_deltas argument can be changed by this function from
 * false to true, but not vice versa. Once signed_deltas is set to true, it cannot
 * be changed.
 */
int
dfor_calc_deltas(size_t cnt, const item_t arr[], vect_t *vDeltas,
				 uniqsortvect_t *usvDeltaWidths, vect_t *vWidthCounters,
				 bool *signedDeltas)
{
	item_t delta;
	item_t prev; /* value of previous number*/
	size_t width;
	size_t maxItemWidth;
	item_t maxItemMask;
	item_t maxModulusOfDelta;

	usv_ins_res_t insWidthInsert;

	if (vDeltas == NULL || usvDeltaWidths == NULL || signedDeltas == NULL)
		return -1;

	vect_clear(vDeltas);
	vect_clear(usvDeltaWidths);

	if (vWidthCounters == NULL)
		usv_insert(usvDeltaWidths, 0);
	else
		vect_clear(vWidthCounters);

	/*
	 * We use the maximum possible item width here which is equal ro the width
	 * of item_t. TODO: we can analyze items and calculate the real max item
	 * width. This must improve compression ratio.
	 */
	maxItemWidth = sizeof(item_t) * 8;
	maxItemMask = width_to_mask(maxItemWidth);
	maxModulusOfDelta = (*signedDeltas) ? maxItemMask >> 1 : maxItemMask;

	prev = 0;
	for (size_t j = 0; j < cnt; j++)
	{
		bool negDelta = arr[j] < prev;

		if (negDelta)
		{
			if (unlikely(*signedDeltas == false))
			{ /* Recalculate all deltas as signed. */
				*signedDeltas = true;
				return dfor_calc_deltas(cnt, arr, vDeltas, usvDeltaWidths,
										vWidthCounters, signedDeltas);
			}
			Assert(*signedDeltas == true);
			delta = prev - arr[j];
		}
		else
			delta = arr[j] - prev;

		if (delta > maxModulusOfDelta)
		{
			Assert(*signedDeltas == true);
			/* Use trick with overlapping here */
			delta = maxItemMask - delta + 1;
			negDelta = !negDelta;
		}

		Assert(delta <= maxModulusOfDelta);

		if (*signedDeltas == true)
		{
			uint8 signCode = (negDelta) ? 1 : 0;
			delta = delta << 1 | signCode;
			width = (delta == 0) ? 2 : width_from_val(delta);
		}
		else
			width = width_from_val(delta);

		vect_append(vDeltas, delta);

		if (vWidthCounters == NULL)
		{
			if (usvDeltaWidths->m[0] < width)
				usvDeltaWidths->m[0] = width;
		}
		else
		{
			insWidthInsert = usv_insert(usvDeltaWidths, width);

			if (insWidthInsert.st == USV_INS_NEW)
				vect_insert(vWidthCounters, insWidthInsert.pos, (item_t)1);
			else if (insWidthInsert.st == USV_INS_EXISTS)
				vWidthCounters->m[insWidthInsert.pos]++;
			else
				return -1;
		}
		prev = arr[j];
	}
	return 0;
}

/*
 * Calculate width of short deltas, width of exceptions, and number of
 * exceptions
 */
int
dfor_calc_width(size_t cntDelta, const uniqsortvect_t *usvDeltaWidths,
				const vect_t *vWidthCounters, size_t *width,
				size_t *cntExceptions)
{
#define MIN_DELTAS_QUANTITY_ALLOWING_EXCEPTIONS 4

	size_t cntShortDeltas; /* number of deltas presented without exceptions */
	size_t indxWidth;	/* the width of short deltas (index from vWidthCounters
						 * (and from vDeltaWidth accordingly)
						 */
	if (usvDeltaWidths == NULL || vWidthCounters == NULL || width == NULL ||
		cntExceptions == NULL)
		return -1;

	cntShortDeltas = cntDelta;
	indxWidth = usvDeltaWidths->cnt - 1; /* counter into index */
	*cntExceptions = 0;

	/*
	 * Here we try to decrease the width of short deltas in order to compress
	 * the array of deltas in the meantime we are eager to cover no less than
	 * 90% of deltas we have. It is an heuristic analysis based on the
	 * suggestion "no less than 90% of deltas we have".
	 *
	 * TODO: analyzing we might want calulate the full size of the pack for each
	 * variant of the width.
	 */
	if (cntDelta >= MIN_DELTAS_QUANTITY_ALLOWING_EXCEPTIONS) {
		size_t szMinCoverage; /* threshold */
		size_t j;

		if (cntDelta >= 10)
			szMinCoverage = cntDelta - cntDelta / 10;
		else
			szMinCoverage = cntDelta - 1;

		j = indxWidth;

		while (j > 0) {
			if (cntShortDeltas - vWidthCounters->m[j] < szMinCoverage)
				break;

			cntShortDeltas -= vWidthCounters->m[j];
			j--;
			indxWidth = j;
		}
		*cntExceptions = cntDelta - cntShortDeltas;
	}

	*width = usvDeltaWidths->m[indxWidth];
	return 0;
}

/*
 * dfor_analyze
 * Analyze input array, calculate deltas and their width, define exceptions and
 * their positions. Returns them through the dfor, vDeltas, usvExcPos. If
 * usvExcPos == NULL - don't calculate exceptions.
 *
 * dfor_analyze function does not use dynamic memory allocation for its
 * local containers.
 *
 * A caller has to control whether vDeltas and usvExcPos use outer memory
 * provided by caller or manage memory allocation automatically, which defines
 * whether vect_insert and vect_append functions, invoked from here, use dynamic
 * memory or not.
 *
 * A caller should take into account that dfor_meta_t dfor are going to be
 * nullified in this function, so it should not have any meaningfull data by
 * start of dfor_analyze, especially its pack field should not be used as a
 * pointer on dynamic memory, otherwise memory leakage is possible.
 *
 */
int
dfor_analyze(size_t cnt, const item_t arr[], /* input */
			 dfor_meta_t *dfor, vect_t *vDeltas,
			 uniqsortvect_t *usvExcPos) /* output */
{
#define DELTA_WIDTH_MAX_NUMBER (sizeof(item_t) * 8)
	uniqsortvect_t usvDeltaWidths;
	item_t bufDeltaWidth[DELTA_WIDTH_MAX_NUMBER];
	vect_t vWidthCounters;
	item_t bufWidthCounters[DELTA_WIDTH_MAX_NUMBER];
	item_t mask;
	int res = -1;

	excalg_t isExcUsage = (usvExcPos == NULL) ? DFOR_EXC_DONT_USE :
												DFOR_EXC_USE;

	if (dfor == NULL)
		goto dfor_analyze_error;

	memset(dfor, 0, sizeof(dfor_meta_t));

	if (cnt == 0)
		/* dfor->item_cnt = 0; */ /* it's been already done with memset */
		goto dfor_analyze_ret;
	else if (arr == NULL)
		goto dfor_analyze_error;

	if (0 != vect_init(&usvDeltaWidths, DELTA_WIDTH_MAX_NUMBER, bufDeltaWidth))
		goto dfor_analyze_error;

	if (isExcUsage == DFOR_EXC_USE)
	{
		if (0 !=
			vect_init(&vWidthCounters, DELTA_WIDTH_MAX_NUMBER,
					  bufWidthCounters))
			goto dfor_analyze_error;
	}

	dfor->item_cnt = cnt;
	dfor->signed_deltas = false;
	if (0 !=
		dfor_calc_deltas(dfor->item_cnt, arr, vDeltas, &usvDeltaWidths,
						 (isExcUsage == DFOR_EXC_USE) ? &vWidthCounters : NULL,
						 &dfor->signed_deltas))
		goto dfor_analyze_error;

	Assert(cnt == vDeltas->cnt);
	Assert(usvDeltaWidths.cnt > 0);

	if (isExcUsage == DFOR_EXC_USE)
	{
		if (0 !=
			dfor_calc_width(vDeltas->cnt, &usvDeltaWidths, &vWidthCounters,
							&dfor->delta_wid, &dfor->exc_cnt))
			goto dfor_analyze_error;
	}
	else
	{
		dfor->delta_wid =
			usvDeltaWidths.m[usvDeltaWidths.cnt - 1]; /* max width */
		dfor->exc_cnt = 0;
	}

	dfor->exc_wid = usvDeltaWidths.m[usvDeltaWidths.cnt - 1] - dfor->delta_wid;

	/* A mask looks like 0001111. It is also the max value of a short delta */
	mask = width_to_mask(dfor->delta_wid);

	for (size_t i = 0; i < vDeltas->cnt; i++)
	{
		if (vDeltas->m[i] > mask)
		{
			Assert(isExcUsage == DFOR_EXC_USE);
			if (0 != vect_append(usvExcPos, (item_t)i))
				goto dfor_analyze_error;
		}
	}
	Assert(dfor->delta_wid + dfor->exc_wid <= sizeof(item_t) * 8);
	res = 0;
dfor_analyze_ret:
	return res;
dfor_analyze_error:
	/* dfor_analyze doesn't affect the pack field (doesn't allocate, delete or
	 * otherwise), so we can nullify the whole dfor and it
	 * is safe, no leakage */
	memset(dfor, 0, sizeof(dfor_meta_t));
	res = -1;
	goto dfor_analyze_ret;
}

/*
 * dfor_pack
 *
 * The input array arr has to be sorted.
 *
 * If a caller needs to avoid using dynamic memory allocation, they have to
 * provide the external memory buffer. The size of this buffer should be not
 * less than 4 * cnt * sizeof(item_t). It will be used for arrays pointed by
 * *(dfor->pack), *(vDeltas->m), *(vExcPosDeltas->m), *(usvExcPos->m).
 *
 * If dynamic allocation has been used by the dfor_pack, a caller has to free
 * the piece of memory pointed by dfor->pack, since it is alocated by the
 * dfor_pack with DFOR_MALLOC. Freeing has to be performed by function
 * conforming to DFOR_MALLOC (paired with it). For instance, if DFOR_MALLOC is
 * malloc, than memory should be freed by free.
 */
int
dfor_pack(size_t cnt, const item_t arr[], excalg_t isExcUsage,
		  dfor_meta_t *dfor, size_t bufSize, uint8_t buf[])
{
	int res;
	vect_t vDeltas = { 0 };
	vect_t vExcPosDeltas = { 0 };
	uniqsortvect_t usvExcPos = { 0 };

	if (dfor == NULL ||
		(bufSize != 0 && bufSize < 4 * cnt * sizeof(item_t)))
	{
		goto dfor_pack_error;
	}

	/*
	 * We don't need it here:
	 * 			memset(dfor, 0, sizeof(dfor_meta_t)).
	 * It is going to be done in dfor_analyze.
	 */

	{
		item_t *deltaBuf = NULL;
		item_t *excPosDeltasBuf = NULL;
		item_t *excPosBuf = NULL;

		int res1 = 0;
		int res2 = 0;
		int res3 = 0;

		if (bufSize != 0)
		{
			/* Step over the maximal allowed DFoR pack size */
			deltaBuf		= (item_t*)(buf + cnt * sizeof(item_t));
			excPosDeltasBuf = (item_t*)(buf + cnt * sizeof(item_t) * 2);
			excPosBuf		= (item_t*)(buf + cnt * sizeof(item_t) * 3);
		}

		/* Setup containers with outer memory */
		res1 = vect_init(&vDeltas, cnt, deltaBuf);

		if (isExcUsage)
		{
			res2 = vect_init(&vExcPosDeltas, cnt, excPosDeltasBuf);
			res3 = vect_init(&usvExcPos, cnt, excPosBuf);
		}

		if (res1 != 0 || res2 != 0 || res3 != 0)
			goto dfor_pack_error;
	}

	if (0 !=
		dfor_analyze(cnt, arr, dfor, &vDeltas,
					 (isExcUsage == DFOR_EXC_USE) ? &usvExcPos : NULL))
		goto dfor_pack_error;

	if (dfor->exc_cnt != 0)
	{
		/* We treat exception positions as a sorted sequence, apply the
		 * DFoR algorithm to it, and save not their absolute values but their
		 * deltas. */
		dfor_meta_t dforExcPos;
		Assert(dfor->exc_cnt == usvExcPos.cnt);
		if (0 !=
			dfor_analyze(usvExcPos.cnt, usvExcPos.m, &dforExcPos,
						 &vExcPosDeltas, NULL))
			goto dfor_pack_error;

		Assert(dfor->exc_cnt == vExcPosDeltas.cnt);
		Assert(dfor->exc_cnt == dforExcPos.item_cnt);

		dfor->exc_pos_wid = dforExcPos.delta_wid;
	}
	else
	{
		Assert(usvExcPos.cnt == 0); /* usvExcPos has to remain zeroed. */
		Assert(dfor->exc_wid == 0); /* No exceptions, no exceptions' width. */
		Assert(dfor->exc_pos_wid == 0); /* No exceptions' positions width too. */
	}

	/* dfor_pack serialisation packing */
	{
		/* index of the next free bit to be used: */
		size_t d; /* - by a delta */
		size_t e; /* - by an exception */
		size_t p; /* - by an exception position */
		item_t mask;
		dfor_stats_t stats;
		size_t j;

		stats = dfor_calc_stats(*dfor);
		dfor->nbytes = dfor_calc_nbytes(*dfor);

		if (bufSize != 0)
		{
			/* Max size of the dfor->pack is cnt * sizeof(size_t) */
			dfor->pack = buf;
			dfor->outer_mem = true;
			if (dfor->nbytes > cnt * sizeof(size_t))
				goto dfor_pack_error;
		}
		else
		{
			/* If a buffer was not provided by caller we allocate it by
			 * ourselves
			 */
			dfor->pack = (uint8_t *)DFOR_MALLOC((dfor->nbytes));

			dfor->outer_mem = false;
		}

		if (dfor->pack == NULL)
			goto dfor_pack_error;

		memset(dfor->pack, 0, dfor->nbytes);

		/* index of the next free bit to be used: */
		d = 0;							/* - by a delta */
		e = stats.delta_pack_nbits;		/* - by an exception */
		p = e + stats.exc_pack_nbits;	/* - by an exception position index */
		/* A mask looks like 0001111. It is also the
		 * max value of a short delta */
		mask = width_to_mask(dfor->delta_wid);

		j = 0;
		for (size_t i = 0; i < vDeltas.cnt; i++)
		{
			d = bitpack_pack(dfor->pack, d, vDeltas.m[i] & mask,
							 dfor->delta_wid);

			if (vDeltas.m[i] > mask)
			{
				Assert(isExcUsage == DFOR_EXC_USE);
				Assert(usvExcPos.m[j] == i);
				Assert(j < usvExcPos.cnt);
				Assert(j < vExcPosDeltas.cnt);
				Assert(dfor->exc_wid != 0);
				Assert(dfor->exc_pos_wid != 0);

				e = bitpack_pack(dfor->pack, e, vDeltas.m[i] >> dfor->delta_wid,
								 dfor->exc_wid);
				p = bitpack_pack(dfor->pack, p, vExcPosDeltas.m[j], dfor->exc_pos_wid);
				j++;
			}
		}

		if (isExcUsage == DFOR_EXC_USE)
			Assert(j == usvExcPos.cnt);
		else
			Assert(j == 0);

		Assert(d == stats.delta_pack_nbits);
		Assert(e == stats.delta_pack_nbits + stats.exc_pack_nbits);
		Assert(p ==
			   stats.delta_pack_nbits + stats.exc_pack_nbits +
				   stats.exc_pos_pack_nbits);
		res = 0;
	}
dfor_pack_ret:
	vect_clear(&usvExcPos);
	vect_clear(&vExcPosDeltas);
	vect_clear(&vDeltas);
	return res;
dfor_pack_error:
	if (dfor != NULL)
		dfor_clear_meta(dfor);
	res = -1;
	goto dfor_pack_ret;
}

/*
 * dfor_unpack
 *
 * If a caller needs to avoid using dynamic memory allocation, they have to:
 * 1) provide the external memory buffer. The size of this buffer should be not
 *    less than:
 *        	2 * dfor.item_cnt * sizeof(item_t) + 2 * dfor.exc_cnt * sizeof(item_t)
 *
 * 2) the vVals vector has to be created but must not be initialised. The
 *    dfor_unpack sets vVals in the 'outer memory' regimen and will set vVal->m
 *    to buf.
 *
 * Provided dynamic allocation is used by the dfor_unpack, a caller will have to
 * free the piece of memory pointed by vVals->m, using vect_clear(&vVals).
 *
 * Are the outer memory is used
 */
int
dfor_unpack(const dfor_meta_t *dfor, uniqsortvect_t *vVals, size_t bufSize,
			uint8_t buf[])
{
	int res = -1;
	size_t szDeltaPack;
	vect_t vExcs = { 0 };
	vect_t vExcPoss = { 0 };
	excalg_t isExcUsage = (dfor->exc_cnt == 0) ? DFOR_EXC_DONT_USE :
												 DFOR_EXC_USE;

	if (vVals == NULL)
		goto dfor_unpack_error;

	if (bufSize != 0 &&
		bufSize < (2 * dfor->item_cnt * sizeof(item_t) +
				   2 * dfor->exc_cnt * sizeof(item_t)))
		goto dfor_unpack_error;

	szDeltaPack = dfor->delta_wid * dfor->item_cnt;

	{
		uint8_t *valsBuf = NULL;
		if (bufSize != 0)
			valsBuf = buf;

		if (vect_init(vVals, dfor->item_cnt, (item_t *)valsBuf) != 0)
			goto dfor_unpack_error;
	}

	/* Calculate exceptions */
	if (isExcUsage == DFOR_EXC_USE)
	{
		size_t szExcPack;
		size_t crExc; /* caret (cursor) */
		size_t crPos; /* caret (cursor) */

		uint8_t *excBuf = NULL;
		uint8_t *excPossBuf = NULL;

		int res1 = 0;
		int res2 = 0;

		szExcPack = dfor->exc_cnt * dfor->exc_wid;
		crExc = szDeltaPack;
		crPos = crExc + szExcPack;

		if (bufSize != 0)
		{
			/* step over the memory occupied by vVals */
			excBuf = buf + dfor->item_cnt * sizeof(item_t);
			excPossBuf = excBuf + dfor->exc_cnt * sizeof(item_t);
		}

		res1 = vect_init(&vExcs, dfor->exc_cnt, (item_t *)excBuf);
		res2 = vect_init(&vExcPoss, dfor->exc_cnt, (item_t *)excPossBuf);

		if (res1 != 0 || res2 != 0)
			goto dfor_unpack_error;


		for (size_t posExc = 0, j = 0; j < dfor->exc_cnt; j++)
		{
			item_t deltaPos;
			res1 = vect_append(&vExcs,
							   bitpack_unpack(dfor->pack, &crExc,
											  dfor->exc_wid));
			/* Calculate the position of the exception from the delta of the
			 * position of the exception */
			deltaPos = bitpack_unpack(dfor->pack, &crPos, dfor->exc_pos_wid);
			posExc += deltaPos;
			res2 = vect_append(&vExcPoss, posExc);
			if (res1 != 0 || res2 != 0)
				goto dfor_unpack_error;
		}
		Assert(crExc == szDeltaPack + szExcPack);
		Assert(crPos ==
			   szDeltaPack + szExcPack + dfor->exc_pos_wid * dfor->exc_cnt);
	}

	{ /* Unpack deltas and calculate target values */
		item_t delta;
		bool negDelta = false;
		item_t prev = 0;
		size_t j = 0; /* index of an exception and its position in vectors */
		size_t crDelta = 0;

		size_t maxItemWidth;
		item_t maxItemMask;

		maxItemWidth = sizeof(item_t) * 8;
		maxItemMask = width_to_mask(maxItemWidth);

		for (size_t i = 0; i < dfor->item_cnt; i++)
		{
			delta = bitpack_unpack(dfor->pack, &crDelta, dfor->delta_wid);

			if (isExcUsage == DFOR_EXC_USE &&
				j < vExcs.cnt &&
				i == vExcPoss.m[j])
			{
				Assert(j < dfor->exc_cnt);
				delta |= vExcs.m[j] << dfor->delta_wid;
				j++;
			}

			if(dfor->signed_deltas)
			{
				negDelta = delta & 0x01;
				delta >>= 1;
				if ((negDelta && prev < delta) ||
					(!negDelta && delta > maxItemMask - prev))
				{
					delta = maxItemMask - delta + 1;
					negDelta = !negDelta;
				}
			}

			if (negDelta)
				prev -= delta;
			else
				prev += delta;

			vect_append(vVals, prev);
		}
		Assert(crDelta == szDeltaPack);
		res = 0;
	}

dfor_unpack_ret:
	vect_clear(&vExcPoss);
	vect_clear(&vExcs);
	return res;
dfor_unpack_error:
	vect_clear(vVals);
	res = -1;
	goto dfor_unpack_ret;
}

void
dfor_clear_meta(dfor_meta_t *meta)
{
	if (meta == NULL)
		return;

	if (meta->pack != NULL && !meta->outer_mem)
		DFOR_FREE(meta->pack);

	memset(meta, 0, sizeof(dfor_meta_t));
}

dfor_stats_t
dfor_calc_stats(dfor_meta_t dfor)
{
	dfor_stats_t stat;
	size_t nbytes;
	stat.delta_pack_nbits = dfor.delta_wid * dfor.item_cnt;
	stat.exc_pack_nbits = dfor.exc_wid * dfor.exc_cnt;
	stat.exc_pos_pack_nbits = dfor.exc_pos_wid * dfor.exc_cnt;

	stat.nbits = stat.delta_pack_nbits + stat.exc_pack_nbits + stat.exc_pos_pack_nbits;

	/* If the division results in the remainder, we use an additional
	 * byte */
	nbytes = (stat.nbits + 7) / 8;
	stat.ratio = (float)(sizeof(item_t) * dfor.item_cnt) / (float)nbytes;

	return stat;
}

size_t dfor_calc_nbytes(dfor_meta_t dfor)
{
	dfor_stats_t stat;
	stat = dfor_calc_stats(dfor);
	return (stat.nbits + 7) / 8;
}

#include "lib/dfor_templ_undef.h"
