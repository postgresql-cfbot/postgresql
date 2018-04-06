/*-------------------------------------------------------------------------
 *
 * partcache.h
 *		Header file for partitioning related cached data structures and
 *		manipulation functions
 *
 * Copyright (c) 2007-2018, PostgreSQL Global Development Group
 *
 * src/include/utils/partcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTCACHE_H
#define PARTCACHE_H

#include "fmgr.h"
#include "utils/relcache.h"

/*
 * Information about the partition key of a relation
 */
typedef struct PartitionKeyData
{
	char		strategy;		/* partitioning strategy */
	int16		partnatts;		/* number of columns in the partition key */
	AttrNumber *partattrs;		/* attribute numbers of columns in the
								 * partition key */
	List	   *partexprs;		/* list of expressions in the partitioning
								 * key, or NIL */

	Oid		   *partopfamily;	/* OIDs of operator families */
	Oid		   *partopcintype;	/* OIDs of opclass declared input data types */
	FmgrInfo   *partsupfunc;	/* lookup info for support funcs */

	/* Partitioning collation per attribute */
	Oid		   *partcollation;

	/* Type information per attribute */
	Oid		   *parttypid;
	int32	   *parttypmod;
	int16	   *parttyplen;
	bool	   *parttypbyval;
	char	   *parttypalign;
	Oid		   *parttypcoll;
}			PartitionKeyData;

typedef struct PartitionKeyData *PartitionKey;

typedef struct PartitionBoundInfoData *PartitionBoundInfo;

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
 * Information about bounds of a partitioned relation
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

#define partition_bound_accepts_nulls(bi) ((bi)->null_index != -1)
#define partition_bound_has_default(bi) ((bi)->default_index != -1)

/*
 * PartitionKey inquiry functions
 */
static inline int
get_partition_strategy(PartitionKey key)
{
	return key->strategy;
}

static inline int
get_partition_natts(PartitionKey key)
{
	return key->partnatts;
}

static inline List *
get_partition_exprs(PartitionKey key)
{
	return key->partexprs;
}

/*
 * PartitionKey inquiry functions - one column
 */
static inline int16
get_partition_col_attnum(PartitionKey key, int col)
{
	return key->partattrs[col];
}

static inline Oid
get_partition_col_typid(PartitionKey key, int col)
{
	return key->parttypid[col];
}

static inline int32
get_partition_col_typmod(PartitionKey key, int col)
{
	return key->parttypmod[col];
}

extern void RelationBuildPartitionKey(Relation relation);
extern void RelationBuildPartitionDesc(Relation relation);
extern bool partition_bounds_equal(int partnatts, int16 *parttyplen,
					   bool *parttypbyval, PartitionBoundInfo b1,
					   PartitionBoundInfo b2);

extern PartitionBoundInfo partition_bounds_copy(PartitionBoundInfo src,
					  PartitionKey key);

extern void check_new_partition_bound(char *relname, Relation parent,
						  PartitionBoundSpec *spec);

extern List *RelationGetPartitionQual(Relation rel);
extern Expr *get_partition_qual_relid(Oid relid);

extern bool has_partition_attrs(Relation rel, Bitmapset *attnums,
					bool *used_in_expr);

extern Oid	get_default_oid_from_partdesc(PartitionDesc partdesc);

extern int get_greatest_modulus(PartitionBoundInfo b);
extern uint64 compute_hash_value(int partnatts, FmgrInfo *partsupfunc,
						Datum *values, bool *isnull);

/* For tuple routing */
extern int get_partition_for_tuple(Relation relation, Datum *values,
						bool *isnull);

#endif							/* PARTCACHE_H */
