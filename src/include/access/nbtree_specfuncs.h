/*
 * prototypes for functions that are included in nbtree.h
 */

#define _bt_specialize		NBTS_FUNCTION(_bt_specialize)
#define btinsert			NBTS_FUNCTION(btinsert)
#define _bt_dedup_pass		NBTS_FUNCTION(_bt_dedup_pass)
#define _bt_doinsert		NBTS_FUNCTION(_bt_doinsert)
#define _bt_search			NBTS_FUNCTION(_bt_search)
#define _bt_binsrch_insert	NBTS_FUNCTION(_bt_binsrch_insert)
#define _bt_compare			NBTS_FUNCTION(_bt_compare)
#define _bt_mkscankey		NBTS_FUNCTION(_bt_mkscankey)
#define _bt_checkkeys		NBTS_FUNCTION(_bt_checkkeys)
#define _bt_truncate		NBTS_FUNCTION(_bt_truncate)
#define _bt_keep_natts_fast	NBTS_FUNCTION(_bt_keep_natts_fast)

/*
 * prototypes for functions in nbtree_spec.h
 */
extern void _bt_specialize(Relation rel);

extern bool btinsert(Relation rel, Datum *values, bool *isnull,
					 ItemPointer ht_ctid, Relation heapRel,
					 IndexUniqueCheck checkUnique, bool indexUnchanged,
					 struct IndexInfo *indexInfo);

/*
 * prototypes for functions in nbtdedup_spec.h
 */
extern void _bt_dedup_pass(Relation rel, Buffer buf, IndexTuple newitem,
						   Size newitemsz, bool bottomupdedup);

/*
 * prototypes for functions in nbtinsert_spec.h
 */

extern bool _bt_doinsert(Relation rel, IndexTuple itup,
						 IndexUniqueCheck checkUnique, bool indexUnchanged,
						 Relation heapRel);

/*
 * prototypes for functions in nbtsearch_spec.h
 */
extern BTStack _bt_search(Relation rel, Relation heaprel, BTScanInsert key,
						  Buffer *bufP, int access);
extern OffsetNumber _bt_binsrch_insert(Relation rel, BTInsertState insertstate);
extern int32 _bt_compare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum);

/*
 * prototypes for functions in nbtutils_spec.h
 */
extern BTScanInsert _bt_mkscankey(Relation rel, IndexTuple itup);
extern bool _bt_checkkeys(IndexScanDesc scan, IndexTuple tuple, int tupnatts,
						  ScanDirection dir, bool *continuescan,
						  bool requiredMatchedByPrecheck, bool haveFirstMatch);
extern IndexTuple _bt_truncate(Relation rel, IndexTuple lastleft,
							   IndexTuple firstright, BTScanInsert itup_key);
extern int _bt_keep_natts_fast(Relation rel, IndexTuple lastleft,
							   IndexTuple firstright);
