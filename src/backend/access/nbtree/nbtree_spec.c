/*-------------------------------------------------------------------------
 *
 * nbtree_spec.c
 *	  Index shape-specialized functions for nbtree.c
 *
 * NOTES
 *	  See also: access/nbtree/README section "nbtree specialization"
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtree_spec.c
 *
 *-------------------------------------------------------------------------
 */


/*
 * _bt_specialize() -- Specialize this index relation for its index key.
 */
void
_bt_specialize(Relation rel)
{
#ifdef NBTS_SPECIALIZING_DEFAULT
	NBTS_MAKE_CTX(rel);
	/*
	 * We can't directly address _bt_specialize here because it'd be macro-
	 * expanded, nor can we utilize NBTS_SPECIALIZE_NAME here because it'd
	 * try to call _bt_specialize, which would be an infinite recursive call.
	 */
	switch (__nbts_ctx) {
		case NBTS_CTX_CACHED:
			_bt_specialize_cached(rel);
			break;
		case NBTS_CTX_UNCACHED:
			_bt_specialize_uncached(rel);
			break;
		case NBTS_CTX_SINGLE_KEYATT:
			_bt_specialize_single_keyatt(rel);
			break;
		case NBTS_CTX_DEFAULT:
			break;
	}
#else
	rel->rd_indam->aminsert = btinsert;
#endif
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
bool
btinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique,
		 bool indexUnchanged,
		 IndexInfo *indexInfo)
{
	bool		result;
	IndexTuple	itup;

	/* generate an index tuple */
	itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
	itup->t_tid = *ht_ctid;

	result = _bt_doinsert(rel, itup, checkUnique, indexUnchanged, heapRel);

	pfree(itup);

	return result;
}
