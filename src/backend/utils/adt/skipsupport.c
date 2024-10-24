/*-------------------------------------------------------------------------
 *
 * skipsupport.c
 *	  Support routines for B-Tree skip scan.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/skipsupport.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "utils/lsyscache.h"
#include "utils/skipsupport.h"

/*
 * Fill in SkipSupport given an operator class (opfamily + opcintype).
 *
 * On success, returns true, and initializes all SkipSupport fields for
 * caller.  Otherwise returns false, indicating that operator class has no
 * skip support function.
 */
bool
PrepareSkipSupportFromOpclass(Oid opfamily, Oid opcintype, bool reverse,
							  SkipSupport sksup)
{
	Oid			skipSupportFunction;

	/* Look for a skip support function */
	skipSupportFunction = get_opfamily_proc(opfamily, opcintype, opcintype,
											BTSKIPSUPPORT_PROC);
	if (!OidIsValid(skipSupportFunction))
		return false;

	OidFunctionCall1(skipSupportFunction, PointerGetDatum(sksup));

	if (reverse)
	{
		/*
		 * DESC/reverse case: swap low_elem with high_elem, and swap decrement
		 * with increment
		 */
		Datum		low_elem = sksup->low_elem;
		SkipSupportIncDec decrement = sksup->decrement;

		sksup->low_elem = sksup->high_elem;
		sksup->decrement = sksup->increment;

		sksup->high_elem = low_elem;
		sksup->increment = decrement;
	}

	return true;
}
