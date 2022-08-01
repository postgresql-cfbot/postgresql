/*
 * Specialized functions for nbtree.c
 */

/*
 * _bt_specialize() -- Specialize this index relation for its index key.
 */
void
NBTS_FUNCTION(_bt_specialize)(Relation rel)
{
#ifdef NBTS_SPECIALIZING_DEFAULT
	PopulateTupleDescCacheOffsets(rel->rd_att);
	nbts_call_norel(_bt_specialize, rel, rel);
#else
	rel->rd_indam->aminsert = NBTS_FUNCTION(btinsert);
#endif
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
bool
NBTS_FUNCTION(btinsert)(Relation rel, Datum *values, bool *isnull,
						ItemPointer ht_ctid, Relation heapRel,
						IndexUniqueCheck checkUnique,
						bool indexUnchanged,
						IndexInfo *indexInfo)
{
#ifdef NBTS_SPECIALIZING_DEFAULT
	nbts_call_norel(_bt_specialize, rel, rel);

	return nbts_call(btinsert, rel, values, isnull, ht_ctid, heapRel,
					 checkUnique, indexUnchanged, indexInfo);
#else
	bool		result;
	IndexTuple	itup;

	/* generate an index tuple */
	itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
	itup->t_tid = *ht_ctid;

	result = nbts_call(_bt_doinsert, rel, itup, checkUnique, indexUnchanged, heapRel);

	pfree(itup);

	return result;
#endif
}
