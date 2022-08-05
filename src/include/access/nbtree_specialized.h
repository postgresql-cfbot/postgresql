/*
 * prototypes for functions that are included in nbtree.h
 */

/*
 * prototypes for functions in nbtree_spec.h
 */
extern void
NBTS_FUNCTION(_bt_specialize)(Relation rel);

extern bool
NBTS_FUNCTION(btinsert)(Relation rel, Datum *values, bool *isnull,
						ItemPointer ht_ctid, Relation heapRel,
						IndexUniqueCheck checkUnique,
						bool indexUnchanged,
						struct IndexInfo *indexInfo);

/*
 * prototypes for functions in nbtdedup_spec.h
 */
extern void
NBTS_FUNCTION(_bt_dedup_pass)(Relation rel, Buffer buf, Relation heapRel,
							  IndexTuple newitem, Size newitemsz,
							  bool bottomupdedup);


/*
 * prototypes for functions in nbtinsert_spec.h
 */

extern bool
NBTS_FUNCTION(_bt_doinsert)(Relation rel, IndexTuple itup,
							IndexUniqueCheck checkUnique, bool indexUnchanged,
							Relation heapRel);

/*
 * prototypes for functions in nbtsearch_spec.h
 */
extern BTStack
NBTS_FUNCTION(_bt_search)(Relation rel, BTScanInsert key,
						  Buffer *bufP, int access,
						  Snapshot snapshot);
extern Buffer
NBTS_FUNCTION(_bt_moveright)(Relation rel, BTScanInsert key, Buffer buf,
							 bool forupdate, BTStack stack, int access,
							 Snapshot snapshot, AttrNumber *comparecol,
							 char *tupdatabuf);
extern OffsetNumber
NBTS_FUNCTION(_bt_binsrch_insert)(Relation rel, BTInsertState insertstate,
								  AttrNumber highcmpcol);
extern int32
NBTS_FUNCTION(_bt_compare)(Relation rel, BTScanInsert key,
						   Page page, OffsetNumber offnum,
						   AttrNumber *comparecol);

/*
 * prototypes for functions in nbtutils_spec.h
 */
extern BTScanInsert
NBTS_FUNCTION(_bt_mkscankey)(Relation rel, IndexTuple itup);
extern bool
NBTS_FUNCTION(_bt_checkkeys)(Relation rel, IndexScanDesc scan,
							 IndexTuple tuple, int tupnatts,
							 ScanDirection dir, bool *continuescan);
extern IndexTuple
NBTS_FUNCTION(_bt_truncate)(Relation rel, IndexTuple lastleft,
							IndexTuple firstright, BTScanInsert itup_key);
extern int
NBTS_FUNCTION(_bt_keep_natts_fast)(Relation rel, IndexTuple lastleft,
								   IndexTuple firstright);
