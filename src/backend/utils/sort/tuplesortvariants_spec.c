/*-------------------------------------------------------------------------
 *
 * tuplesortvariants_spec.c
 *	  Index shape-specialized functions for tuplesortvariants.c
 *
 * NOTES
 *	  See also: access/nbtree/README section "nbtree specialization"
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/tuplesortvariants_spec.c
 *
 *-------------------------------------------------------------------------
 */

#define comparetup_index_btree NBTS_FUNCTION(comparetup_index_btree)
#define comparetup_index_btree_tiebreak NBTS_FUNCTION(comparetup_index_btree_tiebreak)

static int	comparetup_index_btree(const SortTuple *a, const SortTuple *b,
								   Tuplesortstate *state);
static int	comparetup_index_btree_tiebreak(const SortTuple *a, const SortTuple *b,
											Tuplesortstate *state);

static int
comparetup_index_btree(const SortTuple *a, const SortTuple *b,
					   Tuplesortstate *state)
{
	/*
	 * This is similar to comparetup_heap(), but expects index tuples.  There
	 * is also special handling for enforcing uniqueness, and special
	 * treatment for equal keys at the end.
	 */
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	SortSupport sortKey = base->sortKeys;
	int32		compare;

	/* Compare the leading sort key */
	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		return compare;

	/* Compare additional sort keys */
	return comparetup_index_btree_tiebreak(a, b, state);
}

static int
comparetup_index_btree_tiebreak(const SortTuple *a, const SortTuple *b,
								Tuplesortstate *state)
{	/*
	 * This is similar to comparetup_heap(), but expects index tuples.  There
	 * is also special handling for enforcing uniqueness, and special
	 * treatment for equal keys at the end.
	 */
	TuplesortPublic *base = TuplesortstateGetPublic(state);
	TuplesortIndexBTreeArg *arg = (TuplesortIndexBTreeArg *) base->arg;
	SortSupport sortKey = base->sortKeys;
	IndexTuple	tuple1;
	IndexTuple	tuple2;
	int			keysz;
	TupleDesc	tupDes;
	bool		equal_hasnull = false;
	int			nkey;
	int32		compare;
	nbts_attiterdeclare(tuple1);
	nbts_attiterdeclare(tuple2);

	tuple1 = (IndexTuple) a->tuple;
	tuple2 = (IndexTuple) b->tuple;
	keysz = base->nKeys;
	tupDes = RelationGetDescr(arg->index.indexRel);

	if (!sortKey->abbrev_converter)
	{
		nkey = 2;
		sortKey++;
	}
	else
	{
		nkey = 1;
	}

	/* they are equal, so we only need to examine one null flag */
	if (a->isnull1)
		equal_hasnull = true;

	nbts_attiterinit(tuple1, nkey, tupDes);
	nbts_attiterinit(tuple2, nkey, tupDes);

	nbts_foreachattr(nkey, keysz)
	{
		Datum	datum1,
				datum2;
		datum1 = nbts_attiter_nextattdatum(tuple1, tupDes);
		datum2 = nbts_attiter_nextattdatum(tuple2, tupDes);

		if (nbts_attiter_attnum == 1)
			compare = ApplySortAbbrevFullComparator(datum1, nbts_attiter_curattisnull(tuple1),
													datum2, nbts_attiter_curattisnull(tuple2),
													sortKey);
		else
			compare = ApplySortComparator(datum1, nbts_attiter_curattisnull(tuple1),
										  datum2, nbts_attiter_curattisnull(tuple2),
										  sortKey);

		if (compare != 0)
			return compare;

		if (nbts_attiter_curattisnull(tuple1))
			equal_hasnull = true;

		sortKey++;
	}

	/*
	 * If btree has asked us to enforce uniqueness, complain if two equal
	 * tuples are detected (unless there was at least one NULL field and NULLS
	 * NOT DISTINCT was not set).
	 *
	 * It is sufficient to make the test here, because if two tuples are equal
	 * they *must* get compared at some stage of the sort --- otherwise the
	 * sort algorithm wouldn't have checked whether one must appear before the
	 * other.
	 */
	if (arg->enforceUnique && !(!arg->uniqueNullsNotDistinct && equal_hasnull))
	{
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		char	   *key_desc;

		/*
		 * Some rather brain-dead implementations of qsort (such as the one in
		 * QNX 4) will sometimes call the comparison routine to compare a
		 * value to itself, but we always use our own implementation, which
		 * does not.
		 */
		Assert(tuple1 != tuple2);

		index_deform_tuple(tuple1, tupDes, values, isnull);

		key_desc = BuildIndexValueDescription(arg->index.indexRel, values, isnull);

		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
					errmsg("could not create unique index \"%s\"",
						   RelationGetRelationName(arg->index.indexRel)),
					key_desc ? errdetail("Key %s is duplicated.", key_desc) :
					errdetail("Duplicate keys exist."),
					errtableconstraint(arg->index.heapRel,
									   RelationGetRelationName(arg->index.indexRel))));
	}

	/*
	 * If key values are equal, we sort on ItemPointer.  This is required for
	 * btree indexes, since heap TID is treated as an implicit last key
	 * attribute in order to ensure that all keys in the index are physically
	 * unique.
	 */
	{
		BlockNumber blk1 = ItemPointerGetBlockNumber(&tuple1->t_tid);
		BlockNumber blk2 = ItemPointerGetBlockNumber(&tuple2->t_tid);

		if (blk1 != blk2)
			return (blk1 < blk2) ? -1 : 1;
	}
	{
		OffsetNumber pos1 = ItemPointerGetOffsetNumber(&tuple1->t_tid);
		OffsetNumber pos2 = ItemPointerGetOffsetNumber(&tuple2->t_tid);

		if (pos1 != pos2)
			return (pos1 < pos2) ? -1 : 1;
	}

	/* ItemPointer values should never be equal */
	Assert(false);

	return 0;
}
