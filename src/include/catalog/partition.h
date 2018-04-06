/*-------------------------------------------------------------------------
 *
 * partition.h
 *		Header file for structures and utility functions related to
 *		partitioning
 *
 * Copyright (c) 2007-2018, PostgreSQL Global Development Group
 *
 * src/include/catalog/partition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTITION_H
#define PARTITION_H

#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "utils/rel.h"

/* Seed for the extended hash function */
#define HASH_PARTITION_SEED UINT64CONST(0x7A5B22367996DCFD)

extern Oid	get_partition_parent(Oid relid);
extern List *get_partition_ancestors(Oid relid);
extern List *get_qual_from_partbound(Relation rel, Relation parent,
						PartitionBoundSpec *spec);
extern List *map_partition_varattnos(List *expr, int fromrel_varno,
						Relation to_rel, Relation from_rel,
						bool *found_whole_row);

extern Oid	get_default_partition_oid(Oid parentId);
extern void update_default_partition_oid(Oid parentId, Oid defaultPartId);
extern void check_default_allows_bound(Relation parent, Relation defaultRel,
						   PartitionBoundSpec *new_spec);
extern List *get_proposed_default_constraint(List *new_part_constaints);

#endif							/* PARTITION_H */
