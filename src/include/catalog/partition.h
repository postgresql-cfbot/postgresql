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
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "parser/parse_node.h"
#include "utils/rel.h"

/* Seed for the extended hash function */
#define HASH_PARTITION_SEED UINT64CONST(0x7A5B22367996DCFD)

/*
 * PartitionBoundInfoData encapsulates a set of partition bounds. It is
 * usually associated with partitioned tables as part of its partition
 * descriptor, but may also be used to represent a virtual partitioned
 * table such as a partitioned joinrel within the planner.
 *
 * A list partition datum that is known to be NULL is never put into the
 * datums array. Instead, it is tracked using the null_index field.
 *
 * In the case of range partitioning, ndatums will typically be far less than
 * 2 * nparts, because a partition's upper bound and the next partition's lower
 * bound are the same in most common cases, and we only store one of them (the
 * upper bound).  In case of hash partitioning, ndatums will be same as the
 * number of partitions.
 *
 * For range and list partitioned tables, datums is an array of datum-tuples
 * with key->partnatts datums each.  For hash partitioned tables, it is an array
 * of datum-tuples with 2 datums, modulus and remainder, corresponding to a
 * given partition.
 *
 * The datums in datums array are arranged in increasing order as defined by
 * functions qsort_partition_rbound_cmp(), qsort_partition_list_value_cmp() and
 * qsort_partition_hbound_cmp() for range, list and hash partitioned tables
 * respectively. For range and list partitions this simply means that the
 * datums in the datums array are arranged in increasing order as defined by
 * the partition key's operator classes and collations.
 *
 * In the case of list partitioning, the indexes array stores one entry for
 * every datum, which is the index of the partition that accepts a given datum.
 * In case of range partitioning, it stores one entry per distinct range
 * datum, which is the index of the partition for which a given datum
 * is an upper bound.  In the case of hash partitioning, the number of the
 * entries in the indexes array is same as the greatest modulus amongst all
 * partitions.  For a given partition key datum-tuple, the index of the
 * partition which would accept that datum-tuple would be given by the entry
 * pointed by remainder produced when hash value of the datum-tuple is divided
 * by the greatest modulus.
 */

typedef struct PartitionBoundInfoData
{
	char		strategy;		/* hash, list or range? */
	int			ndatums;		/* Length of the datums following array */
	Datum	  **datums;
	PartitionRangeDatumKind **kind; /* The kind of each range bound datum;
									 * NULL for hash and list partitioned
									 * tables */
	int		   *indexes;		/* Partition indexes */
	int			null_index;		/* Index of the null-accepting partition; -1
								 * if there isn't one */
	int			default_index;	/* Index of the default partition; -1 if there
								 * isn't one */
} PartitionBoundInfoData;

typedef struct PartitionBoundInfoData *PartitionBoundInfo;

#define partition_bound_accepts_nulls(bi) ((bi)->null_index != -1)
#define partition_bound_has_default(bi) ((bi)->default_index != -1)

/*
 * Information about partitions of a partitioned table.
 */
typedef struct PartitionDescData
{
	int			nparts;			/* Number of partitions */
	Oid		   *oids;			/* OIDs of partitions */
	PartitionBoundInfo boundinfo;	/* collection of partition bounds */
} PartitionDescData;

typedef struct PartitionDescData *PartitionDesc;

/*
 * PartitionPruneContext
 *
 * Information about a partitioned table needed to perform partition pruning.
 */
typedef struct PartitionPruneContext
{
	/* Partition key information */
	char		strategy;
	int			partnatts;
	Oid		   *partopfamily;
	Oid		   *partopcintype;
	Oid		   *partcollation;
	FmgrInfo   *partsupfunc;

	/* Number of partitions */
	int			nparts;

	/* Partition boundary info */
	PartitionBoundInfo boundinfo;
} PartitionPruneContext;

extern void RelationBuildPartitionDesc(Relation relation);
extern bool partition_bounds_equal(int partnatts, int16 *parttyplen,
					   bool *parttypbyval, PartitionBoundInfo b1,
					   PartitionBoundInfo b2);
extern PartitionBoundInfo partition_bounds_copy(PartitionBoundInfo src,
					  PartitionKey key);

extern void check_new_partition_bound(char *relname, Relation parent,
						  PartitionBoundSpec *spec);
extern Oid	get_partition_parent(Oid relid);
extern List *get_partition_ancestors(Oid relid);
extern List *get_qual_from_partbound(Relation rel, Relation parent,
						PartitionBoundSpec *spec);
extern List *map_partition_varattnos(List *expr, int fromrel_varno,
						Relation to_rel, Relation from_rel,
						bool *found_whole_row);
extern List *RelationGetPartitionQual(Relation rel);
extern Expr *get_partition_qual_relid(Oid relid);
extern bool has_partition_attrs(Relation rel, Bitmapset *attnums,
					bool *used_in_expr);

extern Oid	get_default_oid_from_partdesc(PartitionDesc partdesc);
extern Oid	get_default_partition_oid(Oid parentId);
extern void update_default_partition_oid(Oid parentId, Oid defaultPartId);
extern void check_default_allows_bound(Relation parent, Relation defaultRel,
						   PartitionBoundSpec *new_spec);
extern List *get_proposed_default_constraint(List *new_part_constaints);

/* For tuple routing */
extern int get_partition_for_tuple(Relation relation, Datum *values,
						bool *isnull);

/* For partition-pruning */
extern Bitmapset *get_matching_partitions(PartitionPruneContext *context,
						List *pruning_steps);
#endif							/* PARTITION_H */
