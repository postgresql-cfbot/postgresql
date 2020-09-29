/*-------------------------------------------------------------------------
 *
 * tid.c
 *	  Functions for the built-in type tuple id
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tid.c
 *
 * NOTES
 *	  input routine largely stolen from boxin().
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"


#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)	 PointerGetDatum(X)
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_ITEMPOINTER(x) return ItemPointerGetDatum(x)

#define LDELIM			'('
#define RDELIM			')'
#define DELIM			','
#define NTIDARGS		2

/* ----------------------------------------------------------------
 *		tidin
 * ----------------------------------------------------------------
 */
Datum
tidin(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	char	   *p,
			   *coord[NTIDARGS];
	int			i;
	ItemPointer result;
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;
	char	   *badp;
	int			hold_offset;

	for (i = 0, p = str; *p && i < NTIDARGS && *p != RDELIM; p++)
		if (*p == DELIM || (*p == LDELIM && !i))
			coord[i++] = p + 1;

	if (i < NTIDARGS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));

	errno = 0;
	blockNumber = strtoul(coord[0], &badp, 10);
	if (errno || *badp != DELIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));

	hold_offset = strtol(coord[1], &badp, 10);
	if (errno || *badp != RDELIM ||
		hold_offset > USHRT_MAX || hold_offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"tid", str)));

	offsetNumber = hold_offset;

	result = (ItemPointer) palloc(sizeof(ItemPointerData));

	ItemPointerSet(result, blockNumber, offsetNumber);

	PG_RETURN_ITEMPOINTER(result);
}

/* ----------------------------------------------------------------
 *		tidout
 * ----------------------------------------------------------------
 */
Datum
tidout(PG_FUNCTION_ARGS)
{
	ItemPointer itemPtr = PG_GETARG_ITEMPOINTER(0);
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;
	char		buf[32];

	blockNumber = ItemPointerGetBlockNumberNoCheck(itemPtr);
	offsetNumber = ItemPointerGetOffsetNumberNoCheck(itemPtr);

	/* Perhaps someday we should output this as a record. */
	snprintf(buf, sizeof(buf), "(%u,%u)", blockNumber, offsetNumber);

	PG_RETURN_CSTRING(pstrdup(buf));
}

/*
 *		tidrecv			- converts external binary format to tid
 */
Datum
tidrecv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	ItemPointer result;
	BlockNumber blockNumber;
	OffsetNumber offsetNumber;

	blockNumber = pq_getmsgint(buf, sizeof(blockNumber));
	offsetNumber = pq_getmsgint(buf, sizeof(offsetNumber));

	result = (ItemPointer) palloc(sizeof(ItemPointerData));

	ItemPointerSet(result, blockNumber, offsetNumber);

	PG_RETURN_ITEMPOINTER(result);
}

/*
 *		tidsend			- converts tid to binary format
 */
Datum
tidsend(PG_FUNCTION_ARGS)
{
	ItemPointer itemPtr = PG_GETARG_ITEMPOINTER(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint32(&buf, ItemPointerGetBlockNumberNoCheck(itemPtr));
	pq_sendint16(&buf, ItemPointerGetOffsetNumberNoCheck(itemPtr));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

Datum
tideq(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) == 0);
}

Datum
tidne(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) != 0);
}

Datum
tidlt(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) < 0);
}

Datum
tidle(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) <= 0);
}

Datum
tidgt(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) > 0);
}

Datum
tidge(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_BOOL(ItemPointerCompare(arg1, arg2) >= 0);
}

Datum
bttidcmp(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_INT32(ItemPointerCompare(arg1, arg2));
}

Datum
tidlarger(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_ITEMPOINTER(ItemPointerCompare(arg1, arg2) >= 0 ? arg1 : arg2);
}

Datum
tidsmaller(PG_FUNCTION_ARGS)
{
	ItemPointer arg1 = PG_GETARG_ITEMPOINTER(0);
	ItemPointer arg2 = PG_GETARG_ITEMPOINTER(1);

	PG_RETURN_ITEMPOINTER(ItemPointerCompare(arg1, arg2) <= 0 ? arg1 : arg2);
}

Datum
hashtid(PG_FUNCTION_ARGS)
{
	ItemPointer key = PG_GETARG_ITEMPOINTER(0);

	/*
	 * While you'll probably have a lot of trouble with a compiler that
	 * insists on appending pad space to struct ItemPointerData, we can at
	 * least make this code work, by not using sizeof(ItemPointerData).
	 * Instead rely on knowing the sizes of the component fields.
	 */
	return hash_any((unsigned char *) key,
					sizeof(BlockIdData) + sizeof(OffsetNumber));
}

Datum
hashtidextended(PG_FUNCTION_ARGS)
{
	ItemPointer key = PG_GETARG_ITEMPOINTER(0);
	uint64		seed = PG_GETARG_INT64(1);

	/* As above */
	return hash_any_extended((unsigned char *) key,
							 sizeof(BlockIdData) + sizeof(OffsetNumber),
							 seed);
}
