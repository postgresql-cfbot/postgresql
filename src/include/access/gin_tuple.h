/*--------------------------------------------------------------------------
 * gin.h
 *	  Public header file for Generalized Inverted Index access method.
 *
 *	Copyright (c) 2006-2024, PostgreSQL Global Development Group
 *
 *	src/include/access/gin.h
 *--------------------------------------------------------------------------
 */
#ifndef GIN_TUPLE_
#define GIN_TUPLE_

#include "storage/itemptr.h"
#include "utils/sortsupport.h"

/*
 * Each worker sees tuples in CTID order, so if we track the first TID and
 * compare that when combining results in the worker, we would not need to
 * do an expensive sort in workers (the mergesort is already smart about
 * detecting this and just concatenating the lists). We'd still need the
 * full mergesort in the leader, but that's much cheaper.
 *
 * XXX do we still need all the fields now that we use SortSupport?
 */
typedef struct GinTuple
{
	Size		tuplen;			/* length of the whole tuple */
	Size		keylen;			/* bytes in data for key value */
	int16		typlen;			/* typlen for key */
	bool		typbyval;		/* typbyval for key */
	OffsetNumber attrnum;		/* attnum of index key */
	signed char category;		/* category: normal or NULL? */
	ItemPointerData first;		/* first TID in the array */
	int			nitems;			/* number of TIDs in the data */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} GinTuple;

extern int	_gin_compare_tuples(GinTuple *a, GinTuple *b, SortSupport ssup);

#endif							/* GIN_TUPLE_H */
