/*-------------------------------------------------------------------------
 *
 * storage_gtt.h
 *	  prototypes for functions in backend/catalog/storage_gtt.c
 *
 * src/include/catalog/storage_gtt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_GTT_H
#define STORAGE_GTT_H

#include "access/htup.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "nodes/execnodes.h"
#include "utils/relcache.h"

extern int		vacuum_gtt_defer_check_age;

extern Size active_gtt_shared_hash_size(void);
extern void active_gtt_shared_hash_init(void);
extern void remember_gtt_storage_info(RelFileNode rnode, Relation rel);
extern void forget_gtt_storage_info(Oid relid, RelFileNode relfilenode, bool isCommit);
extern bool is_other_backend_use_gtt(Oid relid);
extern bool gtt_storage_attached(Oid relid);
extern Bitmapset *copy_active_gtt_bitmap(Oid relid);
extern void up_gtt_att_statistic(Oid reloid, int attnum, bool inh, int natts,
								TupleDesc tupleDescriptor, Datum *values, bool *isnull);
extern HeapTuple get_gtt_att_statistic(Oid reloid, int attnum, bool inh);
extern void release_gtt_statistic_cache(HeapTuple tup);
extern void up_gtt_relstats(Relation relation,
							BlockNumber num_pages,
							double num_tuples,
							BlockNumber num_all_visible_pages,
							TransactionId relfrozenxid,
							TransactionId relminmxid);
extern bool get_gtt_relstats(Oid relid, BlockNumber *relpages, double *reltuples,
							BlockNumber *relallvisible, TransactionId *relfrozenxid,
							TransactionId *relminmxid);
extern void gtt_force_enable_index(Relation index);
extern void gtt_fix_index_state(Relation index);
extern void init_gtt_storage(CmdType operation, ResultRelInfo *resultRelInfo);
extern Oid gtt_fetch_current_relfilenode(Oid relid);
extern void gtt_switch_rel_relfilenode(Oid rel1, Oid relfilenode1, Oid rel2, Oid relfilenode2, bool footprint);

#endif							/* STORAGE_H */
