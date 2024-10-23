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

#include "access/ginblock.h"
#include "storage/itemptr.h"
#include "utils/sortsupport.h"

/*
 * XXX: Update description with new architecture
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
	int			tuplen;			/* length of the whole tuple */
	OffsetNumber attrnum;		/* attnum of index key */
	uint16		keylen;			/* bytes in data for key value */
	int16		typlen;			/* typlen for key */
	bool		typbyval;		/* typbyval for key */
	signed char category;		/* category: normal or NULL? */
	int			nitems;			/* number of TIDs in the data */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} GinTuple;

static inline ItemPointer
GinTupleGetFirst(GinTuple *tup)
{
	GinPostingList *list;

	list = (GinPostingList *) SHORTALIGN(tup->data + tup->keylen);

	return &list->first;
}

typedef struct GinBuffer GinBuffer;

extern int	_gin_compare_tuples(GinTuple *a, GinTuple *b, SortSupport ssup);

extern GinBuffer *GinBufferInit(Relation index);
extern bool GinBufferIsEmpty(GinBuffer *buffer);
extern bool GinBufferCanAddKey(GinBuffer *buffer, GinTuple *tup);
extern void GinBufferReset(GinBuffer *buffer);
extern void GinBufferFree(GinBuffer *buffer);
extern void GinBufferMergeTuple(GinBuffer *buffer, GinTuple *tup);
extern GinTuple *GinBufferBuildTuple(GinBuffer *buffer);

#endif							/* GIN_TUPLE_H */
