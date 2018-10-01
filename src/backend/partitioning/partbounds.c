/*-------------------------------------------------------------------------
 *
 * partbounds.c
 *		Support routines for manipulating partition bounds
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/partitioning/partbounds.c
 *
 *-------------------------------------------------------------------------
*/
#include "postgres.h"

#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "partitioning/partprune.h"
#include "partitioning/partbounds.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/hashutils.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

static int	get_partition_bound_num_indexes(PartitionBoundInfo b);
static Expr *make_partition_op_expr(PartitionKey key, int keynum,
					   uint16 strategy, Expr *arg1, Expr *arg2);
static Oid get_partition_operator(PartitionKey key, int col,
					   StrategyNumber strategy, bool *need_relabel);
static List *get_qual_for_hash(Relation parent, PartitionBoundSpec *spec);
static List *get_qual_for_list(Relation parent, PartitionBoundSpec *spec);
static List *get_qual_for_range(Relation parent, PartitionBoundSpec *spec,
				   bool for_default);
static void get_range_key_properties(PartitionKey key, int keynum,
						 PartitionRangeDatum *ldatum,
						 PartitionRangeDatum *udatum,
						 ListCell **partexprs_item,
						 Expr **keyCol,
						 Const **lower_val, Const **upper_val);
static List *get_range_nulltest(PartitionKey key);
static bool partition_hbounds_equal(PartitionBoundInfo b1,
									PartitionBoundInfo b2);
static PartitionBoundInfo partition_range_bounds_merge(int partnatts,
							 FmgrInfo *supfuncs, Oid *collations,
							 PartitionBoundInfo boundinfo1, int nparts1,
							 PartitionBoundInfo boundinfo2, int nparts2,
							 JoinType jointype, List **parts1, List **parts2);
static PartitionBoundInfo partition_list_bounds_merge(int partnatts,
							FmgrInfo *partsupfunc, Oid *collations,
							PartitionBoundInfo boundinfo1, int nparts1,
							PartitionBoundInfo boundinfo2, int nparts2,
							JoinType jointype, List **parts1, List **parts2);
static PartitionBoundInfo partition_hash_bounds_merge(int partnatts,
							FmgrInfo *partsupfunc,
							Oid *partcollation, PartitionBoundInfo left_bi,
							int left_nparts, PartitionBoundInfo right_bi,
							int right_nparts, JoinType jointype, List **left_parts,
							List **right_parts);
static void generate_matching_part_pairs(int *mergemap1, int npart1,
							 int *mergemap2, int nparts2, JoinType jointype,
							 int nparts, List **parts1, List **parts2);
static PartitionBoundInfo build_merged_partition_bounds(char strategy,
							  List *merged_datums, List *merged_indexes,
							  List *merged_contents, int null_index,
							  int default_index);
static int map_and_merge_partitions(int *partmap1, int *mergemap1, int index1,
						 int *partmap2, int *mergemap2, int index2,
						 int *next_index);
static int32 partition_range_bound_cmp(int partnatts, FmgrInfo *partsupfunc,
						  Oid *collations, PartitionRangeBound *bound1,
						  PartitionRangeBound *bound2);
static bool partition_range_cmp(int partnatts, FmgrInfo *supfuncs,
						   Oid *collations, PartitionRangeBound *lower_bound1,
						   PartitionRangeBound *upper_bound1,
						   PartitionRangeBound *lower_bound2,
						   PartitionRangeBound *upper_bound2, int *ub_cmpval,
						   int *lb_cmpval);
static bool partition_range_merge_next_lb(int partnatts, FmgrInfo *supfuncs,
							  Oid *collations, Datum *next_lb_datums,
							  PartitionRangeDatumKind *next_lb_kind,
							  List **merged_datums, List **merged_kinds,
							  List **merged_indexes);
static bool merge_default_partitions(PartitionBoundInfo outer_bi,
						 int *outer_pmap,
						 int *outer_mmap, PartitionBoundInfo inner_bi,
						 int *inner_pmap, int *inner_mmap, JoinType jointype,
						 int *next_index, int *default_index);
static bool merge_null_partitions(PartitionBoundInfo outer_bi, int *outer_pmap,
					  int *outer_mmap, PartitionBoundInfo inner_bi,
					  int *inner_pmap, int *inner_mmap, JoinType jointype,
					  int *next_index, int *null_index, int *default_index);
static bool handle_missing_partition(int *missing_side_pmap,
						 int *missing_side_mmap,
						 int missing_side_default, int *other_pmap,
						 int *other_mmap, int other_part,
						 bool missing_side_outer,
						 bool missing_side_inner,
						 int *next_index, int *default_index,
						 int *merged_index);


/*
 * get_qual_from_partbound
 *		Given a parser node for partition bound, return the list of executable
 *		expressions as partition constraint
 */
List *
get_qual_from_partbound(Relation rel, Relation parent,
						PartitionBoundSpec *spec)
{
	PartitionKey key = RelationGetPartitionKey(parent);
	List	   *my_qual = NIL;

	Assert(key != NULL);

	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			Assert(spec->strategy == PARTITION_STRATEGY_HASH);
			my_qual = get_qual_for_hash(parent, spec);
			break;

		case PARTITION_STRATEGY_LIST:
			Assert(spec->strategy == PARTITION_STRATEGY_LIST);
			my_qual = get_qual_for_list(parent, spec);
			break;

		case PARTITION_STRATEGY_RANGE:
			Assert(spec->strategy == PARTITION_STRATEGY_RANGE);
			my_qual = get_qual_for_range(parent, spec, false);
			break;

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	return my_qual;
}

/*
 * Are two hash partition bound collections logically equal?
 *
 * Hash partition bounds store modulus and remainder in datums array which are
 * always integers irrespective of the number of partition keys and their data
 * types. Hence we can compare the hash bound collection without any partition
 * key specific information. Separating this logic in a function which does not
 * require partition key specific information allows it be called from places
 * where the partition key specific information is not completely available.
 */
static bool
partition_hbounds_equal(PartitionBoundInfo b1, PartitionBoundInfo b2)
{
	int			greatest_modulus = get_hash_partition_greatest_modulus(b1);
	int			i;

	Assert(b1->strategy == PARTITION_STRATEGY_HASH &&
		   b2->strategy == PARTITION_STRATEGY_HASH);

	/*
	 * If two hash partitioned tables have different greatest moduli,
	 * their partition schemes don't match.  For hash partitioned table,
	 * the greatest modulus is given by the last datum and number of
	 * partitions is given by ndatums.
	 */
	if (greatest_modulus != get_hash_partition_greatest_modulus(b2))
		return false;

	/*
	 * We arrange the partitions in the ascending order of their modulus and
	 * remainders.  Also every modulus is factor of next larger modulus.
	 * Therefore we can safely store index of a given partition in indexes
	 * array at remainder of that partition.  Also entries at (remainder + N *
	 * modulus) positions in indexes array are all same for (modulus,
	 * remainder) specification for any partition.  Thus datums array from both
	 * the given bounds are same, if and only if their indexes array will be
	 * same.  So, it suffices to compare indexes array.
	 */
	for (i = 0; i < greatest_modulus; i++)
		if (b1->indexes[i] != b2->indexes[i])
			return false;

#ifdef USE_ASSERT_CHECKING

	/*
	 * Nonetheless make sure that the bounds are indeed same when the indexes
	 * match.  Hash partition bound stores modulus and remainder at
	 * b1->datums[i][0] and b1->datums[i][1] position respectively.
	 */
	for (i = 0; i < b1->ndatums; i++)
		Assert((b1->datums[i][0] == b2->datums[i][0] &&
				b1->datums[i][1] == b2->datums[i][1]));
#endif

	return true;
}

/*
 * Are two partition bound collections logically equal?
 *
 * Used in the keep logic of relcache.c (ie, in RelationClearRelation()).
 * This is also useful when b1 and b2 are bound collections of two separate
 * relations, respectively, because PartitionBoundInfo is a canonical
 * representation of partition bounds.
 */
bool
partition_bounds_equal(int partnatts, int16 *parttyplen, bool *parttypbyval,
					   PartitionBoundInfo b1, PartitionBoundInfo b2)
{
	int			i;

	if (b1->strategy != b2->strategy)
		return false;

	if (b1->ndatums != b2->ndatums)
		return false;

	if (b1->null_index != b2->null_index)
		return false;

	if (b1->default_index != b2->default_index)
		return false;

	if (b1->strategy == PARTITION_STRATEGY_HASH)
	{
		if (!partition_hbounds_equal(b1, b2))
			return false;
	}
	else
	{
		for (i = 0; i < b1->ndatums; i++)
		{
			int			j;

			for (j = 0; j < partnatts; j++)
			{
				/* For range partitions, the bounds might not be finite. */
				if (b1->kind != NULL)
				{
					/* The different kinds of bound all differ from each other */
					if (b1->kind[i][j] != b2->kind[i][j])
						return false;

					/*
					 * Non-finite bounds are equal without further
					 * examination.
					 */
					if (b1->kind[i][j] != PARTITION_RANGE_DATUM_VALUE)
						continue;
				}

				/*
				 * Compare the actual values. Note that it would be both
				 * incorrect and unsafe to invoke the comparison operator
				 * derived from the partitioning specification here.  It would
				 * be incorrect because we want the relcache entry to be
				 * updated for ANY change to the partition bounds, not just
				 * those that the partitioning operator thinks are
				 * significant.  It would be unsafe because we might reach
				 * this code in the context of an aborted transaction, and an
				 * arbitrary partitioning operator might not be safe in that
				 * context.  datumIsEqual() should be simple enough to be
				 * safe.
				 */
				if (!datumIsEqual(b1->datums[i][j], b2->datums[i][j],
								  parttypbyval[j], parttyplen[j]))
					return false;
			}

			if (b1->indexes[i] != b2->indexes[i])
				return false;
		}

		/* There are ndatums+1 indexes in case of range partitions */
		if (b1->strategy == PARTITION_STRATEGY_RANGE &&
			b1->indexes[i] != b2->indexes[i])
			return false;
	}
	return true;
}

/*
 * Return a copy of given PartitionBoundInfo structure. The data types of bounds
 * are described by given partition key specification.
 */
PartitionBoundInfo
partition_bounds_copy(PartitionBoundInfo src,
					  PartitionKey key)
{
	PartitionBoundInfo dest;
	int			i;
	int			ndatums;
	int			partnatts;
	int			num_indexes;

	dest = (PartitionBoundInfo) palloc(sizeof(PartitionBoundInfoData));

	dest->strategy = src->strategy;
	ndatums = dest->ndatums = src->ndatums;
	partnatts = key->partnatts;

	num_indexes = get_partition_bound_num_indexes(src);

	/* List partitioned tables have only a single partition key. */
	Assert(key->strategy != PARTITION_STRATEGY_LIST || partnatts == 1);

	dest->datums = (Datum **) palloc(sizeof(Datum *) * ndatums);

	if (src->kind != NULL)
	{
		dest->kind = (PartitionRangeDatumKind **) palloc(ndatums *
														 sizeof(PartitionRangeDatumKind *));
		for (i = 0; i < ndatums; i++)
		{
			dest->kind[i] = (PartitionRangeDatumKind *) palloc(partnatts *
															   sizeof(PartitionRangeDatumKind));

			memcpy(dest->kind[i], src->kind[i],
				   sizeof(PartitionRangeDatumKind) * key->partnatts);
		}
	}
	else
		dest->kind = NULL;

	for (i = 0; i < ndatums; i++)
	{
		int			j;

		/*
		 * For a corresponding to hash partition, datums array will have two
		 * elements - modulus and remainder.
		 */
		bool		hash_part = (key->strategy == PARTITION_STRATEGY_HASH);
		int			natts = hash_part ? 2 : partnatts;

		dest->datums[i] = (Datum *) palloc(sizeof(Datum) * natts);

		for (j = 0; j < natts; j++)
		{
			bool		byval;
			int			typlen;

			if (hash_part)
			{
				typlen = sizeof(int32); /* Always int4 */
				byval = true;	/* int4 is pass-by-value */
			}
			else
			{
				byval = key->parttypbyval[j];
				typlen = key->parttyplen[j];
			}

			if (dest->kind == NULL ||
				dest->kind[i][j] == PARTITION_RANGE_DATUM_VALUE)
				dest->datums[i][j] = datumCopy(src->datums[i][j],
											   byval, typlen);
		}
	}

	dest->indexes = (int *) palloc(sizeof(int) * num_indexes);
	memcpy(dest->indexes, src->indexes, sizeof(int) * num_indexes);

	dest->null_index = src->null_index;
	dest->default_index = src->default_index;

	return dest;
}

/*
 * check_new_partition_bound
 *
 * Checks if the new partition's bound overlaps any of the existing partitions
 * of parent.  Also performs additional checks as necessary per strategy.
 */
void
check_new_partition_bound(char *relname, Relation parent,
						  PartitionBoundSpec *spec)
{
	PartitionKey key = RelationGetPartitionKey(parent);
	PartitionDesc partdesc = RelationGetPartitionDesc(parent);
	PartitionBoundInfo boundinfo = partdesc->boundinfo;
	ParseState *pstate = make_parsestate(NULL);
	int			with = -1;
	bool		overlap = false;

	if (spec->is_default)
	{
		/*
		 * The default partition bound never conflicts with any other
		 * partition's; if that's what we're attaching, the only possible
		 * problem is that one already exists, so check for that and we're
		 * done.
		 */
		if (boundinfo == NULL || !partition_bound_has_default(boundinfo))
			return;

		/* Default partition already exists, error out. */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("partition \"%s\" conflicts with existing default partition \"%s\"",
						relname, get_rel_name(partdesc->oids[boundinfo->default_index])),
				 parser_errposition(pstate, spec->location)));
	}

	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			{
				Assert(spec->strategy == PARTITION_STRATEGY_HASH);
				Assert(spec->remainder >= 0 && spec->remainder < spec->modulus);

				if (partdesc->nparts > 0)
				{
					Datum	  **datums = boundinfo->datums;
					int			ndatums = boundinfo->ndatums;
					int			greatest_modulus;
					int			remainder;
					int			offset;
					bool		valid_modulus = true;
					int			prev_modulus,	/* Previous largest modulus */
								next_modulus;	/* Next largest modulus */

					/*
					 * Check rule that every modulus must be a factor of the
					 * next larger modulus.  For example, if you have a bunch
					 * of partitions that all have modulus 5, you can add a
					 * new partition with modulus 10 or a new partition with
					 * modulus 15, but you cannot add both a partition with
					 * modulus 10 and a partition with modulus 15, because 10
					 * is not a factor of 15.
					 *
					 * Get the greatest (modulus, remainder) pair contained in
					 * boundinfo->datums that is less than or equal to the
					 * (spec->modulus, spec->remainder) pair.
					 */
					offset = partition_hash_bsearch(boundinfo,
													spec->modulus,
													spec->remainder);
					if (offset < 0)
					{
						next_modulus = DatumGetInt32(datums[0][0]);
						valid_modulus = (next_modulus % spec->modulus) == 0;
					}
					else
					{
						prev_modulus = DatumGetInt32(datums[offset][0]);
						valid_modulus = (spec->modulus % prev_modulus) == 0;

						if (valid_modulus && (offset + 1) < ndatums)
						{
							next_modulus = DatumGetInt32(datums[offset + 1][0]);
							valid_modulus = (next_modulus % spec->modulus) == 0;
						}
					}

					if (!valid_modulus)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("every hash partition modulus must be a factor of the next larger modulus")));

					greatest_modulus = get_hash_partition_greatest_modulus(boundinfo);
					remainder = spec->remainder;

					/*
					 * Normally, the lowest remainder that could conflict with
					 * the new partition is equal to the remainder specified
					 * for the new partition, but when the new partition has a
					 * modulus higher than any used so far, we need to adjust.
					 */
					if (remainder >= greatest_modulus)
						remainder = remainder % greatest_modulus;

					/* Check every potentially-conflicting remainder. */
					do
					{
						if (boundinfo->indexes[remainder] != -1)
						{
							overlap = true;
							with = boundinfo->indexes[remainder];
							break;
						}
						remainder += spec->modulus;
					} while (remainder < greatest_modulus);
				}

				break;
			}

		case PARTITION_STRATEGY_LIST:
			{
				Assert(spec->strategy == PARTITION_STRATEGY_LIST);

				if (partdesc->nparts > 0)
				{
					ListCell   *cell;

					Assert(boundinfo &&
						   boundinfo->strategy == PARTITION_STRATEGY_LIST &&
						   (boundinfo->ndatums > 0 ||
							partition_bound_accepts_nulls(boundinfo) ||
							partition_bound_has_default(boundinfo)));

					foreach(cell, spec->listdatums)
					{
						Const	   *val = castNode(Const, lfirst(cell));

						if (!val->constisnull)
						{
							int			offset;
							bool		equal;

							offset = partition_list_bsearch(&key->partsupfunc[0],
															key->partcollation,
															boundinfo,
															val->constvalue,
															&equal);
							if (offset >= 0 && equal)
							{
								overlap = true;
								with = boundinfo->indexes[offset];
								break;
							}
						}
						else if (partition_bound_accepts_nulls(boundinfo))
						{
							overlap = true;
							with = boundinfo->null_index;
							break;
						}
					}
				}

				break;
			}

		case PARTITION_STRATEGY_RANGE:
			{
				PartitionRangeBound *lower,
						   *upper;

				Assert(spec->strategy == PARTITION_STRATEGY_RANGE);
				lower = make_one_partition_rbound(key, -1, spec->lowerdatums, true);
				upper = make_one_partition_rbound(key, -1, spec->upperdatums, false);

				/*
				 * First check if the resulting range would be empty with
				 * specified lower and upper bounds
				 */
				if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
										 key->partcollation, lower->datums,
										 lower->kind, true, upper) >= 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("empty range bound specified for partition \"%s\"",
									relname),
							 errdetail("Specified lower bound %s is greater than or equal to upper bound %s.",
									   get_range_partbound_string(spec->lowerdatums),
									   get_range_partbound_string(spec->upperdatums)),
							 parser_errposition(pstate, spec->location)));
				}

				if (partdesc->nparts > 0)
				{
					int			offset;
					bool		equal;

					Assert(boundinfo &&
						   boundinfo->strategy == PARTITION_STRATEGY_RANGE &&
						   (boundinfo->ndatums > 0 ||
							partition_bound_has_default(boundinfo)));

					/*
					 * Test whether the new lower bound (which is treated
					 * inclusively as part of the new partition) lies inside
					 * an existing partition, or in a gap.
					 *
					 * If it's inside an existing partition, the bound at
					 * offset + 1 will be the upper bound of that partition,
					 * and its index will be >= 0.
					 *
					 * If it's in a gap, the bound at offset + 1 will be the
					 * lower bound of the next partition, and its index will
					 * be -1. This is also true if there is no next partition,
					 * since the index array is initialised with an extra -1
					 * at the end.
					 */
					offset = partition_range_bsearch(key->partnatts,
													 key->partsupfunc,
													 key->partcollation,
													 boundinfo, lower,
													 &equal);

					if (boundinfo->indexes[offset + 1] < 0)
					{
						/*
						 * Check that the new partition will fit in the gap.
						 * For it to fit, the new upper bound must be less
						 * than or equal to the lower bound of the next
						 * partition, if there is one.
						 */
						if (offset + 1 < boundinfo->ndatums)
						{
							int32		cmpval;
							Datum	   *datums;
							PartitionRangeDatumKind *kind;
							bool		is_lower;

							datums = boundinfo->datums[offset + 1];
							kind = boundinfo->kind[offset + 1];
							is_lower = (boundinfo->indexes[offset + 1] == -1);

							cmpval = partition_rbound_cmp(key->partnatts,
														  key->partsupfunc,
														  key->partcollation,
														  datums, kind,
														  is_lower, upper);
							if (cmpval < 0)
							{
								/*
								 * The new partition overlaps with the
								 * existing partition between offset + 1 and
								 * offset + 2.
								 */
								overlap = true;
								with = boundinfo->indexes[offset + 2];
							}
						}
					}
					else
					{
						/*
						 * The new partition overlaps with the existing
						 * partition between offset and offset + 1.
						 */
						overlap = true;
						with = boundinfo->indexes[offset + 1];
					}
				}

				break;
			}

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	if (overlap)
	{
		Assert(with >= 0);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("partition \"%s\" would overlap partition \"%s\"",
						relname, get_rel_name(partdesc->oids[with])),
				 parser_errposition(pstate, spec->location)));
	}
}

/*
 * check_default_partition_contents
 *
 * This function checks if there exists a row in the default partition that
 * would properly belong to the new partition being added.  If it finds one,
 * it throws an error.
 */
void
check_default_partition_contents(Relation parent, Relation default_rel,
								 PartitionBoundSpec *new_spec)
{
	List	   *new_part_constraints;
	List	   *def_part_constraints;
	List	   *all_parts;
	ListCell   *lc;

	new_part_constraints = (new_spec->strategy == PARTITION_STRATEGY_LIST)
		? get_qual_for_list(parent, new_spec)
		: get_qual_for_range(parent, new_spec, false);
	def_part_constraints =
		get_proposed_default_constraint(new_part_constraints);

	/*
	 * If the existing constraints on the default partition imply that it will
	 * not contain any row that would belong to the new partition, we can
	 * avoid scanning the default partition.
	 */
	if (PartConstraintImpliedByRelConstraint(default_rel, def_part_constraints))
	{
		ereport(INFO,
				(errmsg("updated partition constraint for default partition \"%s\" is implied by existing constraints",
						RelationGetRelationName(default_rel))));
		return;
	}

	/*
	 * Scan the default partition and its subpartitions, and check for rows
	 * that do not satisfy the revised partition constraints.
	 */
	if (default_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		all_parts = find_all_inheritors(RelationGetRelid(default_rel),
										AccessExclusiveLock, NULL);
	else
		all_parts = list_make1_oid(RelationGetRelid(default_rel));

	foreach(lc, all_parts)
	{
		Oid			part_relid = lfirst_oid(lc);
		Relation	part_rel;
		Expr	   *constr;
		Expr	   *partition_constraint;
		EState	   *estate;
		HeapTuple	tuple;
		ExprState  *partqualstate = NULL;
		Snapshot	snapshot;
		TupleDesc	tupdesc;
		ExprContext *econtext;
		HeapScanDesc scan;
		MemoryContext oldCxt;
		TupleTableSlot *tupslot;

		/* Lock already taken above. */
		if (part_relid != RelationGetRelid(default_rel))
		{
			part_rel = heap_open(part_relid, NoLock);

			/*
			 * If the partition constraints on default partition child imply
			 * that it will not contain any row that would belong to the new
			 * partition, we can avoid scanning the child table.
			 */
			if (PartConstraintImpliedByRelConstraint(part_rel,
													 def_part_constraints))
			{
				ereport(INFO,
						(errmsg("updated partition constraint for default partition \"%s\" is implied by existing constraints",
								RelationGetRelationName(part_rel))));

				heap_close(part_rel, NoLock);
				continue;
			}
		}
		else
			part_rel = default_rel;

		/*
		 * Only RELKIND_RELATION relations (i.e. leaf partitions) need to be
		 * scanned.
		 */
		if (part_rel->rd_rel->relkind != RELKIND_RELATION)
		{
			if (part_rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
				ereport(WARNING,
						(errcode(ERRCODE_CHECK_VIOLATION),
						 errmsg("skipped scanning foreign table \"%s\" which is a partition of default partition \"%s\"",
								RelationGetRelationName(part_rel),
								RelationGetRelationName(default_rel))));

			if (RelationGetRelid(default_rel) != RelationGetRelid(part_rel))
				heap_close(part_rel, NoLock);

			continue;
		}

		tupdesc = CreateTupleDescCopy(RelationGetDescr(part_rel));
		constr = linitial(def_part_constraints);
		partition_constraint = (Expr *)
			map_partition_varattnos((List *) constr,
									1, part_rel, parent, NULL);
		estate = CreateExecutorState();

		/* Build expression execution states for partition check quals */
		partqualstate = ExecPrepareExpr(partition_constraint, estate);

		econtext = GetPerTupleExprContext(estate);
		snapshot = RegisterSnapshot(GetLatestSnapshot());
		scan = heap_beginscan(part_rel, snapshot, 0, NULL);
		tupslot = MakeSingleTupleTableSlot(tupdesc);

		/*
		 * Switch to per-tuple memory context and reset it for each tuple
		 * produced, so we don't leak memory.
		 */
		oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			ExecStoreHeapTuple(tuple, tupslot, false);
			econtext->ecxt_scantuple = tupslot;

			if (!ExecCheck(partqualstate, econtext))
				ereport(ERROR,
						(errcode(ERRCODE_CHECK_VIOLATION),
						 errmsg("updated partition constraint for default partition \"%s\" would be violated by some row",
								RelationGetRelationName(default_rel))));

			ResetExprContext(econtext);
			CHECK_FOR_INTERRUPTS();
		}

		MemoryContextSwitchTo(oldCxt);
		heap_endscan(scan);
		UnregisterSnapshot(snapshot);
		ExecDropSingleTupleTableSlot(tupslot);
		FreeExecutorState(estate);

		if (RelationGetRelid(default_rel) != RelationGetRelid(part_rel))
			heap_close(part_rel, NoLock);	/* keep the lock until commit */
	}
}

/*
 * get_hash_partition_greatest_modulus
 *
 * Returns the greatest modulus of the hash partition bound. The greatest
 * modulus will be at the end of the datums array because hash partitions are
 * arranged in the ascending order of their moduli and remainders.
 */
int
get_hash_partition_greatest_modulus(PartitionBoundInfo bound)
{
	Assert(bound && bound->strategy == PARTITION_STRATEGY_HASH);
	Assert(bound->datums && bound->ndatums > 0);
	Assert(DatumGetInt32(bound->datums[bound->ndatums - 1][0]) > 0);

	return DatumGetInt32(bound->datums[bound->ndatums - 1][0]);
}

/*
 * make_one_partition_rbound
 *
 * Return a PartitionRangeBound given a list of PartitionRangeDatum elements
 * and a flag telling whether the bound is lower or not.  Made into a function
 * because there are multiple sites that want to use this facility.
 */
PartitionRangeBound *
make_one_partition_rbound(PartitionKey key, int index, List *datums, bool lower)
{
	PartitionRangeBound *bound;
	ListCell   *lc;
	int			i;

	Assert(datums != NIL);

	bound = (PartitionRangeBound *) palloc0(sizeof(PartitionRangeBound));
	bound->index = index;
	bound->datums = (Datum *) palloc0(key->partnatts * sizeof(Datum));
	bound->kind = (PartitionRangeDatumKind *) palloc0(key->partnatts *
													  sizeof(PartitionRangeDatumKind));
	bound->lower = lower;

	i = 0;
	foreach(lc, datums)
	{
		PartitionRangeDatum *datum = castNode(PartitionRangeDatum, lfirst(lc));

		/* What's contained in this range datum? */
		bound->kind[i] = datum->kind;

		if (datum->kind == PARTITION_RANGE_DATUM_VALUE)
		{
			Const	   *val = castNode(Const, datum->value);

			if (val->constisnull)
				elog(ERROR, "invalid range bound datum");
			bound->datums[i] = val->constvalue;
		}

		i++;
	}

	return bound;
}

/*
 * partition_rbound_cmp
 *
 * Return for two range bounds whether the 1st one (specified in datums1,
 * kind1, and lower1) is <, =, or > the bound specified in *b2.
 *
 * partnatts, partsupfunc and partcollation give the number of attributes in the
 * bounds to be compared, comparison function to be used and the collations of
 * attributes, respectively.
 *
 * Note that if the values of the two range bounds compare equal, then we take
 * into account whether they are upper or lower bounds, and an upper bound is
 * considered to be smaller than a lower bound. This is important to the way
 * that RelationBuildPartitionDesc() builds the PartitionBoundInfoData
 * structure, which only stores the upper bound of a common boundary between
 * two contiguous partitions.
 */
int32
partition_rbound_cmp(int partnatts, FmgrInfo *partsupfunc,
					 Oid *partcollation,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2)
{
	int32		cmpval = 0;		/* placate compiler */
	int			i;
	Datum	   *datums2 = b2->datums;
	PartitionRangeDatumKind *kind2 = b2->kind;
	bool		lower2 = b2->lower;

	for (i = 0; i < partnatts; i++)
	{
		/*
		 * First, handle cases where the column is unbounded, which should not
		 * invoke the comparison procedure, and should not consider any later
		 * columns. Note that the PartitionRangeDatumKind enum elements
		 * compare the same way as the values they represent.
		 */
		if (kind1[i] < kind2[i])
			return -1;
		else if (kind1[i] > kind2[i])
			return 1;
		else if (kind1[i] != PARTITION_RANGE_DATUM_VALUE)

			/*
			 * The column bounds are both MINVALUE or both MAXVALUE. No later
			 * columns should be considered, but we still need to compare
			 * whether they are upper or lower bounds.
			 */
			break;

		cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[i],
												 partcollation[i],
												 datums1[i],
												 datums2[i]));
		if (cmpval != 0)
			break;
	}

	/*
	 * If the comparison is anything other than equal, we're done. If they
	 * compare equal though, we still have to consider whether the boundaries
	 * are inclusive or exclusive.  Exclusive one is considered smaller of the
	 * two.
	 */
	if (cmpval == 0 && lower1 != lower2)
		cmpval = lower1 ? 1 : -1;

	return cmpval;
}

/*
 * partition_rbound_datum_cmp
 *
 * Return whether range bound (specified in rb_datums, rb_kind, and rb_lower)
 * is <, =, or > partition key of tuple (tuple_datums)
 *
 * n_tuple_datums, partsupfunc and partcollation give number of attributes in
 * the bounds to be compared, comparison function to be used and the collations
 * of attributes resp.
 *
 */
int32
partition_rbound_datum_cmp(FmgrInfo *partsupfunc, Oid *partcollation,
						   Datum *rb_datums, PartitionRangeDatumKind *rb_kind,
						   Datum *tuple_datums, int n_tuple_datums)
{
	int			i;
	int32		cmpval = -1;

	for (i = 0; i < n_tuple_datums; i++)
	{
		if (rb_kind[i] == PARTITION_RANGE_DATUM_MINVALUE)
			return -1;
		else if (rb_kind[i] == PARTITION_RANGE_DATUM_MAXVALUE)
			return 1;

		cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[i],
												 partcollation[i],
												 rb_datums[i],
												 tuple_datums[i]));
		if (cmpval != 0)
			break;
	}

	return cmpval;
}

/*
 * partition_hbound_cmp
 *
 * Compares modulus first, then remainder if modulus is equal.
 */
int32
partition_hbound_cmp(int modulus1, int remainder1, int modulus2, int remainder2)
{
	if (modulus1 < modulus2)
		return -1;
	if (modulus1 > modulus2)
		return 1;
	if (modulus1 == modulus2 && remainder1 != remainder2)
		return (remainder1 > remainder2) ? 1 : -1;
	return 0;
}

/*
 * partition_list_bsearch
 *		Returns the index of the greatest bound datum that is less than equal
 * 		to the given value or -1 if all of the bound datums are greater
 *
 * *is_equal is set to true if the bound datum at the returned index is equal
 * to the input value.
 */
int
partition_list_bsearch(FmgrInfo *partsupfunc, Oid *partcollation,
					   PartitionBoundInfo boundinfo,
					   Datum value, bool *is_equal)
{
	int			lo,
				hi,
				mid;

	lo = -1;
	hi = boundinfo->ndatums - 1;
	while (lo < hi)
	{
		int32		cmpval;

		mid = (lo + hi + 1) / 2;
		cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[0],
												 partcollation[0],
												 boundinfo->datums[mid][0],
												 value));
		if (cmpval <= 0)
		{
			lo = mid;
			*is_equal = (cmpval == 0);
			if (*is_equal)
				break;
		}
		else
			hi = mid - 1;
	}

	return lo;
}

/*
 * partition_range_bsearch
 *		Returns the index of the greatest range bound that is less than or
 *		equal to the given range bound or -1 if all of the range bounds are
 *		greater
 *
 * *is_equal is set to true if the range bound at the returned index is equal
 * to the input range bound
 */
int
partition_range_bsearch(int partnatts, FmgrInfo *partsupfunc,
						Oid *partcollation,
						PartitionBoundInfo boundinfo,
						PartitionRangeBound *probe, bool *is_equal)
{
	int			lo,
				hi,
				mid;

	lo = -1;
	hi = boundinfo->ndatums - 1;
	while (lo < hi)
	{
		int32		cmpval;

		mid = (lo + hi + 1) / 2;
		cmpval = partition_rbound_cmp(partnatts, partsupfunc,
									  partcollation,
									  boundinfo->datums[mid],
									  boundinfo->kind[mid],
									  (boundinfo->indexes[mid] == -1),
									  probe);
		if (cmpval <= 0)
		{
			lo = mid;
			*is_equal = (cmpval == 0);

			if (*is_equal)
				break;
		}
		else
			hi = mid - 1;
	}

	return lo;
}

/*
 * partition_range_bsearch
 *		Returns the index of the greatest range bound that is less than or
 *		equal to the given tuple or -1 if all of the range bounds are greater
 *
 * *is_equal is set to true if the range bound at the returned index is equal
 * to the input tuple.
 */
int
partition_range_datum_bsearch(FmgrInfo *partsupfunc, Oid *partcollation,
							  PartitionBoundInfo boundinfo,
							  int nvalues, Datum *values, bool *is_equal)
{
	int			lo,
				hi,
				mid;

	lo = -1;
	hi = boundinfo->ndatums - 1;
	while (lo < hi)
	{
		int32		cmpval;

		mid = (lo + hi + 1) / 2;
		cmpval = partition_rbound_datum_cmp(partsupfunc,
											partcollation,
											boundinfo->datums[mid],
											boundinfo->kind[mid],
											values,
											nvalues);
		if (cmpval <= 0)
		{
			lo = mid;
			*is_equal = (cmpval == 0);

			if (*is_equal)
				break;
		}
		else
			hi = mid - 1;
	}

	return lo;
}

/*
 * partition_hash_bsearch
 *		Returns the index of the greatest (modulus, remainder) pair that is
 *		less than or equal to the given (modulus, remainder) pair or -1 if
 *		all of them are greater
 */
int
partition_hash_bsearch(PartitionBoundInfo boundinfo,
					   int modulus, int remainder)
{
	int			lo,
				hi,
				mid;

	lo = -1;
	hi = boundinfo->ndatums - 1;
	while (lo < hi)
	{
		int32		cmpval,
					bound_modulus,
					bound_remainder;

		mid = (lo + hi + 1) / 2;
		bound_modulus = DatumGetInt32(boundinfo->datums[mid][0]);
		bound_remainder = DatumGetInt32(boundinfo->datums[mid][1]);
		cmpval = partition_hbound_cmp(bound_modulus, bound_remainder,
									  modulus, remainder);
		if (cmpval <= 0)
		{
			lo = mid;

			if (cmpval == 0)
				break;
		}
		else
			hi = mid - 1;
	}

	return lo;
}

/*
 * get_partition_bound_num_indexes
 *
 * Returns the number of the entries in the partition bound indexes array.
 */
static int
get_partition_bound_num_indexes(PartitionBoundInfo bound)
{
	int			num_indexes;

	Assert(bound);

	switch (bound->strategy)
	{
		case PARTITION_STRATEGY_HASH:

			/*
			 * The number of the entries in the indexes array is same as the
			 * greatest modulus.
			 */
			num_indexes = get_hash_partition_greatest_modulus(bound);
			break;

		case PARTITION_STRATEGY_LIST:
			num_indexes = bound->ndatums;
			break;

		case PARTITION_STRATEGY_RANGE:
			/* Range partitioned table has an extra index. */
			num_indexes = bound->ndatums + 1;
			break;

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) bound->strategy);
	}

	return num_indexes;
}

/*
 * get_partition_operator
 *
 * Return oid of the operator of the given strategy for the given partition
 * key column.  It is assumed that the partitioning key is of the same type as
 * the chosen partitioning opclass, or at least binary-compatible.  In the
 * latter case, *need_relabel is set to true if the opclass is not of a
 * polymorphic type (indicating a RelabelType node needed on top), otherwise
 * false.
 */
static Oid
get_partition_operator(PartitionKey key, int col, StrategyNumber strategy,
					   bool *need_relabel)
{
	Oid			operoid;

	/*
	 * Get the operator in the partitioning opfamily using the opclass'
	 * declared input type as both left- and righttype.
	 */
	operoid = get_opfamily_member(key->partopfamily[col],
								  key->partopcintype[col],
								  key->partopcintype[col],
								  strategy);
	if (!OidIsValid(operoid))
		elog(ERROR, "missing operator %d(%u,%u) in partition opfamily %u",
			 strategy, key->partopcintype[col], key->partopcintype[col],
			 key->partopfamily[col]);

	/*
	 * If the partition key column is not of the same type as the operator
	 * class and not polymorphic, tell caller to wrap the non-Const expression
	 * in a RelabelType.  This matches what parse_coerce.c does.
	 */
	*need_relabel = (key->parttypid[col] != key->partopcintype[col] &&
					 key->partopcintype[col] != RECORDOID &&
					 !IsPolymorphicType(key->partopcintype[col]));

	return operoid;
}

/*
 * make_partition_op_expr
 *		Returns an Expr for the given partition key column with arg1 and
 *		arg2 as its leftop and rightop, respectively
 */
static Expr *
make_partition_op_expr(PartitionKey key, int keynum,
					   uint16 strategy, Expr *arg1, Expr *arg2)
{
	Oid			operoid;
	bool		need_relabel = false;
	Expr	   *result = NULL;

	/* Get the correct btree operator for this partitioning column */
	operoid = get_partition_operator(key, keynum, strategy, &need_relabel);

	/*
	 * Chosen operator may be such that the non-Const operand needs to be
	 * coerced, so apply the same; see the comment in
	 * get_partition_operator().
	 */
	if (!IsA(arg1, Const) &&
		(need_relabel ||
		 key->partcollation[keynum] != key->parttypcoll[keynum]))
		arg1 = (Expr *) makeRelabelType(arg1,
										key->partopcintype[keynum],
										-1,
										key->partcollation[keynum],
										COERCE_EXPLICIT_CAST);

	/* Generate the actual expression */
	switch (key->strategy)
	{
		case PARTITION_STRATEGY_LIST:
			{
				List	   *elems = (List *) arg2;
				int			nelems = list_length(elems);

				Assert(nelems >= 1);
				Assert(keynum == 0);

				if (nelems > 1 &&
					!type_is_array(key->parttypid[keynum]))
				{
					ArrayExpr  *arrexpr;
					ScalarArrayOpExpr *saopexpr;

					/* Construct an ArrayExpr for the right-hand inputs */
					arrexpr = makeNode(ArrayExpr);
					arrexpr->array_typeid =
						get_array_type(key->parttypid[keynum]);
					arrexpr->array_collid = key->parttypcoll[keynum];
					arrexpr->element_typeid = key->parttypid[keynum];
					arrexpr->elements = elems;
					arrexpr->multidims = false;
					arrexpr->location = -1;

					/* Build leftop = ANY (rightop) */
					saopexpr = makeNode(ScalarArrayOpExpr);
					saopexpr->opno = operoid;
					saopexpr->opfuncid = get_opcode(operoid);
					saopexpr->useOr = true;
					saopexpr->inputcollid = key->partcollation[keynum];
					saopexpr->args = list_make2(arg1, arrexpr);
					saopexpr->location = -1;

					result = (Expr *) saopexpr;
				}
				else
				{
					List	   *elemops = NIL;
					ListCell   *lc;

					foreach(lc, elems)
					{
						Expr	   *elem = lfirst(lc),
								   *elemop;

						elemop = make_opclause(operoid,
											   BOOLOID,
											   false,
											   arg1, elem,
											   InvalidOid,
											   key->partcollation[keynum]);
						elemops = lappend(elemops, elemop);
					}

					result = nelems > 1 ? makeBoolExpr(OR_EXPR, elemops, -1) : linitial(elemops);
				}
				break;
			}

		case PARTITION_STRATEGY_RANGE:
			result = make_opclause(operoid,
								   BOOLOID,
								   false,
								   arg1, arg2,
								   InvalidOid,
								   key->partcollation[keynum]);
			break;

		default:
			elog(ERROR, "invalid partitioning strategy");
			break;
	}

	return result;
}

/*
 * get_qual_for_hash
 *
 * Returns a CHECK constraint expression to use as a hash partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * The partition constraint for a hash partition is always a call to the
 * built-in function satisfies_hash_partition().
 */
static List *
get_qual_for_hash(Relation parent, PartitionBoundSpec *spec)
{
	PartitionKey key = RelationGetPartitionKey(parent);
	FuncExpr   *fexpr;
	Node	   *relidConst;
	Node	   *modulusConst;
	Node	   *remainderConst;
	List	   *args;
	ListCell   *partexprs_item;
	int			i;

	/* Fixed arguments. */
	relidConst = (Node *) makeConst(OIDOID,
									-1,
									InvalidOid,
									sizeof(Oid),
									ObjectIdGetDatum(RelationGetRelid(parent)),
									false,
									true);

	modulusConst = (Node *) makeConst(INT4OID,
									  -1,
									  InvalidOid,
									  sizeof(int32),
									  Int32GetDatum(spec->modulus),
									  false,
									  true);

	remainderConst = (Node *) makeConst(INT4OID,
										-1,
										InvalidOid,
										sizeof(int32),
										Int32GetDatum(spec->remainder),
										false,
										true);

	args = list_make3(relidConst, modulusConst, remainderConst);
	partexprs_item = list_head(key->partexprs);

	/* Add an argument for each key column. */
	for (i = 0; i < key->partnatts; i++)
	{
		Node	   *keyCol;

		/* Left operand */
		if (key->partattrs[i] != 0)
		{
			keyCol = (Node *) makeVar(1,
									  key->partattrs[i],
									  key->parttypid[i],
									  key->parttypmod[i],
									  key->parttypcoll[i],
									  0);
		}
		else
		{
			keyCol = (Node *) copyObject(lfirst(partexprs_item));
			partexprs_item = lnext(partexprs_item);
		}

		args = lappend(args, keyCol);
	}

	fexpr = makeFuncExpr(F_SATISFIES_HASH_PARTITION,
						 BOOLOID,
						 args,
						 InvalidOid,
						 InvalidOid,
						 COERCE_EXPLICIT_CALL);

	return list_make1(fexpr);
}

/*
 * get_qual_for_list
 *
 * Returns an implicit-AND list of expressions to use as a list partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * The function returns NIL for a default partition when it's the only
 * partition since in that case there is no constraint.
 */
static List *
get_qual_for_list(Relation parent, PartitionBoundSpec *spec)
{
	PartitionKey key = RelationGetPartitionKey(parent);
	List	   *result;
	Expr	   *keyCol;
	Expr	   *opexpr;
	NullTest   *nulltest;
	ListCell   *cell;
	List	   *elems = NIL;
	bool		list_has_null = false;

	/*
	 * Only single-column list partitioning is supported, so we are worried
	 * only about the partition key with index 0.
	 */
	Assert(key->partnatts == 1);

	/* Construct Var or expression representing the partition column */
	if (key->partattrs[0] != 0)
		keyCol = (Expr *) makeVar(1,
								  key->partattrs[0],
								  key->parttypid[0],
								  key->parttypmod[0],
								  key->parttypcoll[0],
								  0);
	else
		keyCol = (Expr *) copyObject(linitial(key->partexprs));

	/*
	 * For default list partition, collect datums for all the partitions. The
	 * default partition constraint should check that the partition key is
	 * equal to none of those.
	 */
	if (spec->is_default)
	{
		int			i;
		int			ndatums = 0;
		PartitionDesc pdesc = RelationGetPartitionDesc(parent);
		PartitionBoundInfo boundinfo = pdesc->boundinfo;

		if (boundinfo)
		{
			ndatums = boundinfo->ndatums;

			if (partition_bound_accepts_nulls(boundinfo))
				list_has_null = true;
		}

		/*
		 * If default is the only partition, there need not be any partition
		 * constraint on it.
		 */
		if (ndatums == 0 && !list_has_null)
			return NIL;

		for (i = 0; i < ndatums; i++)
		{
			Const	   *val;

			/*
			 * Construct Const from known-not-null datum.  We must be careful
			 * to copy the value, because our result has to be able to outlive
			 * the relcache entry we're copying from.
			 */
			val = makeConst(key->parttypid[0],
							key->parttypmod[0],
							key->parttypcoll[0],
							key->parttyplen[0],
							datumCopy(*boundinfo->datums[i],
									  key->parttypbyval[0],
									  key->parttyplen[0]),
							false,	/* isnull */
							key->parttypbyval[0]);

			elems = lappend(elems, val);
		}
	}
	else
	{
		/*
		 * Create list of Consts for the allowed values, excluding any nulls.
		 */
		foreach(cell, spec->listdatums)
		{
			Const	   *val = castNode(Const, lfirst(cell));

			if (val->constisnull)
				list_has_null = true;
			else
				elems = lappend(elems, copyObject(val));
		}
	}

	if (elems)
	{
		/*
		 * Generate the operator expression from the non-null partition
		 * values.
		 */
		opexpr = make_partition_op_expr(key, 0, BTEqualStrategyNumber,
										keyCol, (Expr *) elems);
	}
	else
	{
		/*
		 * If there are no partition values, we don't need an operator
		 * expression.
		 */
		opexpr = NULL;
	}

	if (!list_has_null)
	{
		/*
		 * Gin up a "col IS NOT NULL" test that will be AND'd with the main
		 * expression.  This might seem redundant, but the partition routing
		 * machinery needs it.
		 */
		nulltest = makeNode(NullTest);
		nulltest->arg = keyCol;
		nulltest->nulltesttype = IS_NOT_NULL;
		nulltest->argisrow = false;
		nulltest->location = -1;

		result = opexpr ? list_make2(nulltest, opexpr) : list_make1(nulltest);
	}
	else
	{
		/*
		 * Gin up a "col IS NULL" test that will be OR'd with the main
		 * expression.
		 */
		nulltest = makeNode(NullTest);
		nulltest->arg = keyCol;
		nulltest->nulltesttype = IS_NULL;
		nulltest->argisrow = false;
		nulltest->location = -1;

		if (opexpr)
		{
			Expr	   *or;

			or = makeBoolExpr(OR_EXPR, list_make2(nulltest, opexpr), -1);
			result = list_make1(or);
		}
		else
			result = list_make1(nulltest);
	}

	/*
	 * Note that, in general, applying NOT to a constraint expression doesn't
	 * necessarily invert the set of rows it accepts, because NOT (NULL) is
	 * NULL.  However, the partition constraints we construct here never
	 * evaluate to NULL, so applying NOT works as intended.
	 */
	if (spec->is_default)
	{
		result = list_make1(make_ands_explicit(result));
		result = list_make1(makeBoolExpr(NOT_EXPR, result, -1));
	}

	return result;
}

/*
 * get_qual_for_range
 *
 * Returns an implicit-AND list of expressions to use as a range partition's
 * constraint, given the parent relation and partition bound structure.
 *
 * For a multi-column range partition key, say (a, b, c), with (al, bl, cl)
 * as the lower bound tuple and (au, bu, cu) as the upper bound tuple, we
 * generate an expression tree of the following form:
 *
 *	(a IS NOT NULL) and (b IS NOT NULL) and (c IS NOT NULL)
 *		AND
 *	(a > al OR (a = al AND b > bl) OR (a = al AND b = bl AND c >= cl))
 *		AND
 *	(a < au OR (a = au AND b < bu) OR (a = au AND b = bu AND c < cu))
 *
 * It is often the case that a prefix of lower and upper bound tuples contains
 * the same values, for example, (al = au), in which case, we will emit an
 * expression tree of the following form:
 *
 *	(a IS NOT NULL) and (b IS NOT NULL) and (c IS NOT NULL)
 *		AND
 *	(a = al)
 *		AND
 *	(b > bl OR (b = bl AND c >= cl))
 *		AND
 *	(b < bu) OR (b = bu AND c < cu))
 *
 * If a bound datum is either MINVALUE or MAXVALUE, these expressions are
 * simplified using the fact that any value is greater than MINVALUE and less
 * than MAXVALUE. So, for example, if cu = MAXVALUE, c < cu is automatically
 * true, and we need not emit any expression for it, and the last line becomes
 *
 *	(b < bu) OR (b = bu), which is simplified to (b <= bu)
 *
 * In most common cases with only one partition column, say a, the following
 * expression tree will be generated: a IS NOT NULL AND a >= al AND a < au
 *
 * For default partition, it returns the negation of the constraints of all
 * the other partitions.
 *
 * External callers should pass for_default as false; we set it to true only
 * when recursing.
 */
static List *
get_qual_for_range(Relation parent, PartitionBoundSpec *spec,
				   bool for_default)
{
	List	   *result = NIL;
	ListCell   *cell1,
			   *cell2,
			   *partexprs_item,
			   *partexprs_item_saved;
	int			i,
				j;
	PartitionRangeDatum *ldatum,
			   *udatum;
	PartitionKey key = RelationGetPartitionKey(parent);
	Expr	   *keyCol;
	Const	   *lower_val,
			   *upper_val;
	List	   *lower_or_arms,
			   *upper_or_arms;
	int			num_or_arms,
				current_or_arm;
	ListCell   *lower_or_start_datum,
			   *upper_or_start_datum;
	bool		need_next_lower_arm,
				need_next_upper_arm;

	if (spec->is_default)
	{
		List	   *or_expr_args = NIL;
		PartitionDesc pdesc = RelationGetPartitionDesc(parent);
		Oid		   *inhoids = pdesc->oids;
		int			nparts = pdesc->nparts,
					i;

		for (i = 0; i < nparts; i++)
		{
			Oid			inhrelid = inhoids[i];
			HeapTuple	tuple;
			Datum		datum;
			bool		isnull;
			PartitionBoundSpec *bspec;

			tuple = SearchSysCache1(RELOID, inhrelid);
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for relation %u", inhrelid);

			datum = SysCacheGetAttr(RELOID, tuple,
									Anum_pg_class_relpartbound,
									&isnull);
			if (isnull)
				elog(ERROR, "null relpartbound for relation %u", inhrelid);

			bspec = (PartitionBoundSpec *)
				stringToNode(TextDatumGetCString(datum));
			if (!IsA(bspec, PartitionBoundSpec))
				elog(ERROR, "expected PartitionBoundSpec");

			if (!bspec->is_default)
			{
				List	   *part_qual;

				part_qual = get_qual_for_range(parent, bspec, true);

				/*
				 * AND the constraints of the partition and add to
				 * or_expr_args
				 */
				or_expr_args = lappend(or_expr_args, list_length(part_qual) > 1
									   ? makeBoolExpr(AND_EXPR, part_qual, -1)
									   : linitial(part_qual));
			}
			ReleaseSysCache(tuple);
		}

		if (or_expr_args != NIL)
		{
			Expr	   *other_parts_constr;

			/*
			 * Combine the constraints obtained for non-default partitions
			 * using OR.  As requested, each of the OR's args doesn't include
			 * the NOT NULL test for partition keys (which is to avoid its
			 * useless repetition).  Add the same now.
			 */
			other_parts_constr =
				makeBoolExpr(AND_EXPR,
							 lappend(get_range_nulltest(key),
									 list_length(or_expr_args) > 1
									 ? makeBoolExpr(OR_EXPR, or_expr_args,
													-1)
									 : linitial(or_expr_args)),
							 -1);

			/*
			 * Finally, the default partition contains everything *NOT*
			 * contained in the non-default partitions.
			 */
			result = list_make1(makeBoolExpr(NOT_EXPR,
											 list_make1(other_parts_constr), -1));
		}

		return result;
	}

	lower_or_start_datum = list_head(spec->lowerdatums);
	upper_or_start_datum = list_head(spec->upperdatums);
	num_or_arms = key->partnatts;

	/*
	 * If it is the recursive call for default, we skip the get_range_nulltest
	 * to avoid accumulating the NullTest on the same keys for each partition.
	 */
	if (!for_default)
		result = get_range_nulltest(key);

	/*
	 * Iterate over the key columns and check if the corresponding lower and
	 * upper datums are equal using the btree equality operator for the
	 * column's type.  If equal, we emit single keyCol = common_value
	 * expression.  Starting from the first column for which the corresponding
	 * lower and upper bound datums are not equal, we generate OR expressions
	 * as shown in the function's header comment.
	 */
	i = 0;
	partexprs_item = list_head(key->partexprs);
	partexprs_item_saved = partexprs_item;	/* placate compiler */
	forboth(cell1, spec->lowerdatums, cell2, spec->upperdatums)
	{
		EState	   *estate;
		MemoryContext oldcxt;
		Expr	   *test_expr;
		ExprState  *test_exprstate;
		Datum		test_result;
		bool		isNull;

		ldatum = castNode(PartitionRangeDatum, lfirst(cell1));
		udatum = castNode(PartitionRangeDatum, lfirst(cell2));

		/*
		 * Since get_range_key_properties() modifies partexprs_item, and we
		 * might need to start over from the previous expression in the later
		 * part of this function, save away the current value.
		 */
		partexprs_item_saved = partexprs_item;

		get_range_key_properties(key, i, ldatum, udatum,
								 &partexprs_item,
								 &keyCol,
								 &lower_val, &upper_val);

		/*
		 * If either value is NULL, the corresponding partition bound is
		 * either MINVALUE or MAXVALUE, and we treat them as unequal, because
		 * even if they're the same, there is no common value to equate the
		 * key column with.
		 */
		if (!lower_val || !upper_val)
			break;

		/* Create the test expression */
		estate = CreateExecutorState();
		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
		test_expr = make_partition_op_expr(key, i, BTEqualStrategyNumber,
										   (Expr *) lower_val,
										   (Expr *) upper_val);
		fix_opfuncids((Node *) test_expr);
		test_exprstate = ExecInitExpr(test_expr, NULL);
		test_result = ExecEvalExprSwitchContext(test_exprstate,
												GetPerTupleExprContext(estate),
												&isNull);
		MemoryContextSwitchTo(oldcxt);
		FreeExecutorState(estate);

		/* If not equal, go generate the OR expressions */
		if (!DatumGetBool(test_result))
			break;

		/*
		 * The bounds for the last key column can't be equal, because such a
		 * range partition would never be allowed to be defined (it would have
		 * an empty range otherwise).
		 */
		if (i == key->partnatts - 1)
			elog(ERROR, "invalid range bound specification");

		/* Equal, so generate keyCol = lower_val expression */
		result = lappend(result,
						 make_partition_op_expr(key, i, BTEqualStrategyNumber,
												keyCol, (Expr *) lower_val));

		i++;
	}

	/* First pair of lower_val and upper_val that are not equal. */
	lower_or_start_datum = cell1;
	upper_or_start_datum = cell2;

	/* OR will have as many arms as there are key columns left. */
	num_or_arms = key->partnatts - i;
	current_or_arm = 0;
	lower_or_arms = upper_or_arms = NIL;
	need_next_lower_arm = need_next_upper_arm = true;
	while (current_or_arm < num_or_arms)
	{
		List	   *lower_or_arm_args = NIL,
				   *upper_or_arm_args = NIL;

		/* Restart scan of columns from the i'th one */
		j = i;
		partexprs_item = partexprs_item_saved;

		for_both_cell(cell1, lower_or_start_datum, cell2, upper_or_start_datum)
		{
			PartitionRangeDatum *ldatum_next = NULL,
					   *udatum_next = NULL;

			ldatum = castNode(PartitionRangeDatum, lfirst(cell1));
			if (lnext(cell1))
				ldatum_next = castNode(PartitionRangeDatum,
									   lfirst(lnext(cell1)));
			udatum = castNode(PartitionRangeDatum, lfirst(cell2));
			if (lnext(cell2))
				udatum_next = castNode(PartitionRangeDatum,
									   lfirst(lnext(cell2)));
			get_range_key_properties(key, j, ldatum, udatum,
									 &partexprs_item,
									 &keyCol,
									 &lower_val, &upper_val);

			if (need_next_lower_arm && lower_val)
			{
				uint16		strategy;

				/*
				 * For the non-last columns of this arm, use the EQ operator.
				 * For the last column of this arm, use GT, unless this is the
				 * last column of the whole bound check, or the next bound
				 * datum is MINVALUE, in which case use GE.
				 */
				if (j - i < current_or_arm)
					strategy = BTEqualStrategyNumber;
				else if (j == key->partnatts - 1 ||
						 (ldatum_next &&
						  ldatum_next->kind == PARTITION_RANGE_DATUM_MINVALUE))
					strategy = BTGreaterEqualStrategyNumber;
				else
					strategy = BTGreaterStrategyNumber;

				lower_or_arm_args = lappend(lower_or_arm_args,
											make_partition_op_expr(key, j,
																   strategy,
																   keyCol,
																   (Expr *) lower_val));
			}

			if (need_next_upper_arm && upper_val)
			{
				uint16		strategy;

				/*
				 * For the non-last columns of this arm, use the EQ operator.
				 * For the last column of this arm, use LT, unless the next
				 * bound datum is MAXVALUE, in which case use LE.
				 */
				if (j - i < current_or_arm)
					strategy = BTEqualStrategyNumber;
				else if (udatum_next &&
						 udatum_next->kind == PARTITION_RANGE_DATUM_MAXVALUE)
					strategy = BTLessEqualStrategyNumber;
				else
					strategy = BTLessStrategyNumber;

				upper_or_arm_args = lappend(upper_or_arm_args,
											make_partition_op_expr(key, j,
																   strategy,
																   keyCol,
																   (Expr *) upper_val));
			}

			/*
			 * Did we generate enough of OR's arguments?  First arm considers
			 * the first of the remaining columns, second arm considers first
			 * two of the remaining columns, and so on.
			 */
			++j;
			if (j - i > current_or_arm)
			{
				/*
				 * We must not emit any more arms if the new column that will
				 * be considered is unbounded, or this one was.
				 */
				if (!lower_val || !ldatum_next ||
					ldatum_next->kind != PARTITION_RANGE_DATUM_VALUE)
					need_next_lower_arm = false;
				if (!upper_val || !udatum_next ||
					udatum_next->kind != PARTITION_RANGE_DATUM_VALUE)
					need_next_upper_arm = false;
				break;
			}
		}

		if (lower_or_arm_args != NIL)
			lower_or_arms = lappend(lower_or_arms,
									list_length(lower_or_arm_args) > 1
									? makeBoolExpr(AND_EXPR, lower_or_arm_args, -1)
									: linitial(lower_or_arm_args));

		if (upper_or_arm_args != NIL)
			upper_or_arms = lappend(upper_or_arms,
									list_length(upper_or_arm_args) > 1
									? makeBoolExpr(AND_EXPR, upper_or_arm_args, -1)
									: linitial(upper_or_arm_args));

		/* If no work to do in the next iteration, break away. */
		if (!need_next_lower_arm && !need_next_upper_arm)
			break;

		++current_or_arm;
	}

	/*
	 * Generate the OR expressions for each of lower and upper bounds (if
	 * required), and append to the list of implicitly ANDed list of
	 * expressions.
	 */
	if (lower_or_arms != NIL)
		result = lappend(result,
						 list_length(lower_or_arms) > 1
						 ? makeBoolExpr(OR_EXPR, lower_or_arms, -1)
						 : linitial(lower_or_arms));
	if (upper_or_arms != NIL)
		result = lappend(result,
						 list_length(upper_or_arms) > 1
						 ? makeBoolExpr(OR_EXPR, upper_or_arms, -1)
						 : linitial(upper_or_arms));

	/*
	 * As noted above, for non-default, we return list with constant TRUE. If
	 * the result is NIL during the recursive call for default, it implies
	 * this is the only other partition which can hold every value of the key
	 * except NULL. Hence we return the NullTest result skipped earlier.
	 */
	if (result == NIL)
		result = for_default
			? get_range_nulltest(key)
			: list_make1(makeBoolConst(true, false));

	return result;
}

/*
 * get_range_key_properties
 *		Returns range partition key information for a given column
 *
 * This is a subroutine for get_qual_for_range, and its API is pretty
 * specialized to that caller.
 *
 * Constructs an Expr for the key column (returned in *keyCol) and Consts
 * for the lower and upper range limits (returned in *lower_val and
 * *upper_val).  For MINVALUE/MAXVALUE limits, NULL is returned instead of
 * a Const.  All of these structures are freshly palloc'd.
 *
 * *partexprs_item points to the cell containing the next expression in
 * the key->partexprs list, or NULL.  It may be advanced upon return.
 */
static void
get_range_key_properties(PartitionKey key, int keynum,
						 PartitionRangeDatum *ldatum,
						 PartitionRangeDatum *udatum,
						 ListCell **partexprs_item,
						 Expr **keyCol,
						 Const **lower_val, Const **upper_val)
{
	/* Get partition key expression for this column */
	if (key->partattrs[keynum] != 0)
	{
		*keyCol = (Expr *) makeVar(1,
								   key->partattrs[keynum],
								   key->parttypid[keynum],
								   key->parttypmod[keynum],
								   key->parttypcoll[keynum],
								   0);
	}
	else
	{
		if (*partexprs_item == NULL)
			elog(ERROR, "wrong number of partition key expressions");
		*keyCol = copyObject(lfirst(*partexprs_item));
		*partexprs_item = lnext(*partexprs_item);
	}

	/* Get appropriate Const nodes for the bounds */
	if (ldatum->kind == PARTITION_RANGE_DATUM_VALUE)
		*lower_val = castNode(Const, copyObject(ldatum->value));
	else
		*lower_val = NULL;

	if (udatum->kind == PARTITION_RANGE_DATUM_VALUE)
		*upper_val = castNode(Const, copyObject(udatum->value));
	else
		*upper_val = NULL;
}

/*
 * get_range_nulltest
 *
 * A non-default range partition table does not currently allow partition
 * keys to be null, so emit an IS NOT NULL expression for each key column.
 */
static List *
get_range_nulltest(PartitionKey key)
{
	List	   *result = NIL;
	NullTest   *nulltest;
	ListCell   *partexprs_item;
	int			i;

	partexprs_item = list_head(key->partexprs);
	for (i = 0; i < key->partnatts; i++)
	{
		Expr	   *keyCol;

		if (key->partattrs[i] != 0)
		{
			keyCol = (Expr *) makeVar(1,
									  key->partattrs[i],
									  key->parttypid[i],
									  key->parttypmod[i],
									  key->parttypcoll[i],
									  0);
		}
		else
		{
			if (partexprs_item == NULL)
				elog(ERROR, "wrong number of partition key expressions");
			keyCol = copyObject(lfirst(partexprs_item));
			partexprs_item = lnext(partexprs_item);
		}

		nulltest = makeNode(NullTest);
		nulltest->arg = keyCol;
		nulltest->nulltesttype = IS_NOT_NULL;
		nulltest->argisrow = false;
		nulltest->location = -1;
		result = lappend(result, nulltest);
	}

	return result;
}

/*
 * compute_partition_hash_value
 *
 * Compute the hash value for given partition key values.
 */
uint64
compute_partition_hash_value(int partnatts, FmgrInfo *partsupfunc,
							 Datum *values, bool *isnull)
{
	int			i;
	uint64		rowHash = 0;
	Datum		seed = UInt64GetDatum(HASH_PARTITION_SEED);

	for (i = 0; i < partnatts; i++)
	{
		/* Nulls are just ignored */
		if (!isnull[i])
		{
			Datum		hash;

			Assert(OidIsValid(partsupfunc[i].fn_oid));

			/*
			 * Compute hash for each datum value by calling respective
			 * datatype-specific hash functions of each partition key
			 * attribute.
			 */
			hash = FunctionCall2(&partsupfunc[i], values[i], seed);

			/* Form a single 64-bit hash value */
			rowHash = hash_combine64(rowHash, DatumGetUInt64(hash));
		}
	}

	return rowHash;
}

/*
 * satisfies_hash_partition
 *
 * This is an SQL-callable function for use in hash partition constraints.
 * The first three arguments are the parent table OID, modulus, and remainder.
 * The remaining arguments are the value of the partitioning columns (or
 * expressions); these are hashed and the results are combined into a single
 * hash value by calling hash_combine64.
 *
 * Returns true if remainder produced when this computed single hash value is
 * divided by the given modulus is equal to given remainder, otherwise false.
 *
 * See get_qual_for_hash() for usage.
 */
Datum
satisfies_hash_partition(PG_FUNCTION_ARGS)
{
	typedef struct ColumnsHashData
	{
		Oid			relid;
		int			nkeys;
		Oid			variadic_type;
		int16		variadic_typlen;
		bool		variadic_typbyval;
		char		variadic_typalign;
		FmgrInfo	partsupfunc[PARTITION_MAX_KEYS];
	} ColumnsHashData;
	Oid			parentId;
	int			modulus;
	int			remainder;
	Datum		seed = UInt64GetDatum(HASH_PARTITION_SEED);
	ColumnsHashData *my_extra;
	uint64		rowHash = 0;

	/* Return null if the parent OID, modulus, or remainder is NULL. */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	parentId = PG_GETARG_OID(0);
	modulus = PG_GETARG_INT32(1);
	remainder = PG_GETARG_INT32(2);

	/* Sanity check modulus and remainder. */
	if (modulus <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("modulus for hash partition must be a positive integer")));
	if (remainder < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("remainder for hash partition must be a non-negative integer")));
	if (remainder >= modulus)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("remainder for hash partition must be less than modulus")));

	/*
	 * Cache hash function information.
	 */
	my_extra = (ColumnsHashData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL || my_extra->relid != parentId)
	{
		Relation	parent;
		PartitionKey key;
		int			j;

		/* Open parent relation and fetch partition keyinfo */
		parent = try_relation_open(parentId, AccessShareLock);
		if (parent == NULL)
			PG_RETURN_NULL();
		key = RelationGetPartitionKey(parent);

		/* Reject parent table that is not hash-partitioned. */
		if (parent->rd_rel->relkind != RELKIND_PARTITIONED_TABLE ||
			key->strategy != PARTITION_STRATEGY_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a hash partitioned table",
							get_rel_name(parentId))));

		if (!get_fn_expr_variadic(fcinfo->flinfo))
		{
			int			nargs = PG_NARGS() - 3;

			/* complain if wrong number of column values */
			if (key->partnatts != nargs)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("number of partitioning columns (%d) does not match number of partition keys provided (%d)",
								key->partnatts, nargs)));

			/* allocate space for our cache */
			fcinfo->flinfo->fn_extra =
				MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt,
									   offsetof(ColumnsHashData, partsupfunc) +
									   sizeof(FmgrInfo) * nargs);
			my_extra = (ColumnsHashData *) fcinfo->flinfo->fn_extra;
			my_extra->relid = parentId;
			my_extra->nkeys = key->partnatts;

			/* check argument types and save fmgr_infos */
			for (j = 0; j < key->partnatts; ++j)
			{
				Oid			argtype = get_fn_expr_argtype(fcinfo->flinfo, j + 3);

				if (argtype != key->parttypid[j] && !IsBinaryCoercible(argtype, key->parttypid[j]))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("column %d of the partition key has type \"%s\", but supplied value is of type \"%s\"",
									j + 1, format_type_be(key->parttypid[j]), format_type_be(argtype))));

				fmgr_info_copy(&my_extra->partsupfunc[j],
							   &key->partsupfunc[j],
							   fcinfo->flinfo->fn_mcxt);
			}

		}
		else
		{
			ArrayType  *variadic_array = PG_GETARG_ARRAYTYPE_P(3);

			/* allocate space for our cache -- just one FmgrInfo in this case */
			fcinfo->flinfo->fn_extra =
				MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt,
									   offsetof(ColumnsHashData, partsupfunc) +
									   sizeof(FmgrInfo));
			my_extra = (ColumnsHashData *) fcinfo->flinfo->fn_extra;
			my_extra->relid = parentId;
			my_extra->nkeys = key->partnatts;
			my_extra->variadic_type = ARR_ELEMTYPE(variadic_array);
			get_typlenbyvalalign(my_extra->variadic_type,
								 &my_extra->variadic_typlen,
								 &my_extra->variadic_typbyval,
								 &my_extra->variadic_typalign);

			/* check argument types */
			for (j = 0; j < key->partnatts; ++j)
				if (key->parttypid[j] != my_extra->variadic_type)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("column %d of the partition key has type \"%s\", but supplied value is of type \"%s\"",
									j + 1,
									format_type_be(key->parttypid[j]),
									format_type_be(my_extra->variadic_type))));

			fmgr_info_copy(&my_extra->partsupfunc[0],
						   &key->partsupfunc[0],
						   fcinfo->flinfo->fn_mcxt);
		}

		/* Hold lock until commit */
		relation_close(parent, NoLock);
	}

	if (!OidIsValid(my_extra->variadic_type))
	{
		int			nkeys = my_extra->nkeys;
		int			i;

		/*
		 * For a non-variadic call, neither the number of arguments nor their
		 * types can change across calls, so avoid the expense of rechecking
		 * here.
		 */

		for (i = 0; i < nkeys; i++)
		{
			Datum		hash;

			/* keys start from fourth argument of function. */
			int			argno = i + 3;

			if (PG_ARGISNULL(argno))
				continue;

			Assert(OidIsValid(my_extra->partsupfunc[i].fn_oid));

			hash = FunctionCall2(&my_extra->partsupfunc[i],
								 PG_GETARG_DATUM(argno),
								 seed);

			/* Form a single 64-bit hash value */
			rowHash = hash_combine64(rowHash, DatumGetUInt64(hash));
		}
	}
	else
	{
		ArrayType  *variadic_array = PG_GETARG_ARRAYTYPE_P(3);
		int			i;
		int			nelems;
		Datum	   *datum;
		bool	   *isnull;

		deconstruct_array(variadic_array,
						  my_extra->variadic_type,
						  my_extra->variadic_typlen,
						  my_extra->variadic_typbyval,
						  my_extra->variadic_typalign,
						  &datum, &isnull, &nelems);

		/* complain if wrong number of column values */
		if (nelems != my_extra->nkeys)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("number of partitioning columns (%d) does not match number of partition keys provided (%d)",
							my_extra->nkeys, nelems)));

		for (i = 0; i < nelems; i++)
		{
			Datum		hash;

			if (isnull[i])
				continue;

			Assert(OidIsValid(my_extra->partsupfunc[0].fn_oid));

			hash = FunctionCall2(&my_extra->partsupfunc[0],
								 datum[i],
								 seed);

			/* Form a single 64-bit hash value */
			rowHash = hash_combine64(rowHash, DatumGetUInt64(hash));
		}
	}

	PG_RETURN_BOOL(rowHash % modulus == remainder);
}

/*
 * partition_bounds_merge
 *
 * The function produces the partition bounds for a join between two relations
 * whose partition bounds are given. The function also returns two lists of
 * partition indexes one for each of the joining relations. Both the lists
 * contain the same number of elements. The partition indexes at the same
 * positions in the lists indicate the pair partitions, one from each side, to
 * be joined and the position itself corresponds to the index of partition
 * produced by that child-join in the partitioned join.
 *
 * The function returns NULL if we can not find the matching pair of
 * partitions. This happens if 1. multiple partitions on one side match with
 * one partition on the other side. 2. a given partition on the outer side
 * doesn't have a matching partition on the inner side. We can not support the
 * first case since we don't have a way to represent multiple partitions as a
 * single relation (RelOptInfo) and then perform join using the ganged
 * relation. We can not support the second case since the missing inner
 * partition needs to be represented as an empty relation and we don't have a
 * way to introduce empty relation during join planning after creating paths
 * for all the base relations.
 */
extern PartitionBoundInfo
partition_bounds_merge(int partnatts, FmgrInfo *partsupfunc,
					   Oid *partcollation,
					   PartitionBoundInfo outer_bi, int outer_nparts,
					   PartitionBoundInfo inner_bi, int inner_nparts,
					   JoinType jointype, List **outer_parts,
					   List **inner_parts)
{
	PartitionBoundInfo merged_bounds;
	char		strategy;

	/* Bail out if partitioning strategies are different. */
	if (outer_bi->strategy != inner_bi->strategy)
		return NULL;

	if (jointype != JOIN_LEFT && jointype != JOIN_INNER &&
		jointype != JOIN_SEMI && jointype != JOIN_ANTI &&
		jointype != JOIN_FULL)
		elog(ERROR, "unexpected join type %d", jointype);

	*outer_parts = NIL;
	*inner_parts = NIL;
	strategy = outer_bi->strategy;
	switch (strategy)
	{
		case PARTITION_STRATEGY_LIST:
			merged_bounds = partition_list_bounds_merge(partnatts, partsupfunc,
														partcollation,
														outer_bi, outer_nparts,
														inner_bi, inner_nparts,
														jointype, outer_parts,
														inner_parts);
			break;

		case PARTITION_STRATEGY_RANGE:
			merged_bounds = partition_range_bounds_merge(partnatts,
														 partsupfunc,
														 partcollation,
														 outer_bi,
														 outer_nparts,
														 inner_bi,
														 inner_nparts,
														 jointype,
														 outer_parts,
														 inner_parts);
			break;

		case PARTITION_STRATEGY_HASH:
			merged_bounds = partition_hash_bounds_merge(partnatts,
														partsupfunc,
														partcollation,
														outer_bi, outer_nparts,
														inner_bi, inner_nparts,
														jointype, outer_parts,
														inner_parts);
			break;

		default:
			elog(ERROR, "unexpected partition strategy: %d", strategy);
	}

	Assert(merged_bounds || (*outer_parts == NIL && *inner_parts == NIL));

	Assert(list_length(*outer_parts) == list_length(*inner_parts));

	Assert((*outer_parts == NIL || *inner_parts != NIL) &&
		   (*inner_parts == NIL || *outer_parts != NIL));

	return merged_bounds;
}

/*
 * partition_get_range_bounds
 *
 * Given the index of lower bound in datums array, return lower and upper
 * bounds and the index of the partition with that lower bound.
 */
static int
partition_get_range_bounds(PartitionBoundInfo bi, int lb_index,
						   PartitionRangeBound *lower,
						   PartitionRangeBound *upper)
{
	int			part_index;

	/* A lower bound should have at least one more bound after it. */
	Assert(lb_index < bi->ndatums - 1);

	/* The lower bound should correspond to a valid partition. */
	part_index = bi->indexes[lb_index + 1];
	Assert(part_index >= 0);

	lower->kind = bi->kind[lb_index];
	lower->datums = bi->datums[lb_index];
	lower->lower = true;
	upper->kind = bi->kind[lb_index + 1];
	upper->datums = bi->datums[lb_index + 1];
	upper->lower = false;

	return part_index;
}

/*
 * partition_range_get_next_lb_index
 *
 * Given the index of lower bound in datums array return the
 * index of lower bound of the next partition. When the given index corresponds
 * to the last partition, return number of datums (ndatums).
 */
static int
partition_range_get_next_lb_index(PartitionBoundInfo bi, int lb_index)
{
	/* A lower bound should have at least one more bound after it. */
	Assert(lb_index < bi->ndatums - 1);

	/* The partition index corresponding to the upper bound should be valid. */
	Assert(bi->indexes[lb_index + 1] >= 0);

	/*
	 * If there are no bounds left beyond the upper bound, we have reached the
	 * last partition.
	 */
	if (lb_index + 2 < bi->ndatums)
	{
		/*
		 * If the bound next to the upper bound corresponds to no partition,
		 * that's the next lower bound of the next partition. Otherwise, the
		 * current upper bound is the lower bound of the next partition.
		 */
		if (bi->indexes[lb_index + 2] < 0)
			return lb_index + 2;
		else
			return lb_index + 1;
	}
	else
		return bi->ndatums;
}

static int32
partition_range_bound_cmp(int partnatts, FmgrInfo *partsupfunc,
						  Oid *partcollations, PartitionRangeBound *bound1,
						  PartitionRangeBound *bound2)
{
	return partition_rbound_cmp(partnatts, partsupfunc, partcollations,
								bound1->datums, bound1->kind, bound1->lower,
								bound2);
}

/*
 * partition_range_cmp
 *
 * Compare the bounds of two range partitions. Set ub_cmpval <, = or > 0, if the
 * first partition's upper bound is lower than, equal to or higher than the
 * second partition's upper bound resp. Similarly set lb_cmpval <, =  or > 0,
 * if the first partition's lower bound is lower than, equal to or higher than
 * the second partition's lower bound resp.
 *
 * Return true, if the ranges overlap, otherwise return false.
 */
static bool
partition_range_cmp(int partnatts, FmgrInfo *partsupfuncs, Oid *partcollations,
					PartitionRangeBound *lower_bound1,
					PartitionRangeBound *upper_bound1,
					PartitionRangeBound *lower_bound2,
					PartitionRangeBound *upper_bound2, int *ub_cmpval,
					int *lb_cmpval)
{
	bool		overlap;

	/*
	 * Compare upper bound of the first partition with the lower bound of the
	 * second and vice-versa. If lower bound is higher than the upper bound,
	 * the partitions are not overlapping. All other cases indicate overlapping
	 * partitions.
	 */
	if (partition_range_bound_cmp(partnatts, partsupfuncs, partcollations,
								  lower_bound1, upper_bound2) > 0)
	{
		overlap = false;
		*ub_cmpval = 1;
		*lb_cmpval = 1;
	}
	else if (partition_range_bound_cmp(partnatts, partsupfuncs, partcollations,
									   lower_bound2, upper_bound1) > 0)
	{
		overlap = false;
		*ub_cmpval = -1;
		*lb_cmpval = -1;
	}
	else
	{
		overlap = true;
		*ub_cmpval = partition_range_bound_cmp(partnatts, partsupfuncs,
											   partcollations, upper_bound1,
											   upper_bound2);
		*lb_cmpval = partition_range_bound_cmp(partnatts, partsupfuncs,
											   partcollations, lower_bound1,
											   lower_bound2);
	}

	return overlap;
}

/*
 * partition_range_merge
 *
 * Merge the partition bounds of given two partitions such that the join
 * between the given two partitions fits merged bounds.
 *
 * "merged_upper" will be set to one of the given upper bounds and
 * "merged_lower" will be set to one of the given lower bounds.
 */
static void
partition_range_merge(int partnatts, FmgrInfo *partsupfuncs,
					  Oid *partcollations, JoinType jointype,
					  PartitionRangeBound *left_lb,
					  PartitionRangeBound *left_ub,
					  PartitionRangeBound *right_lb,
					  PartitionRangeBound *right_ub,
					  PartitionRangeBound **merged_lb,
					  PartitionRangeBound **merged_ub)
{
	/*
	 * An outer join will have all the rows from the outer side, so merged
	 * bounds will be same as the outer bounds. An inner join will have rows
	 * that fit both the bounds, thus lower merged bound will be higher of two
	 * lower bounds and upper merged bound will be lower of the two upper
	 * bounds.
	 */
	switch (jointype)
	{
		case JOIN_LEFT:
		case JOIN_ANTI:
			*merged_ub = left_ub;
			*merged_lb = left_lb;
			break;

		case JOIN_INNER:
		case JOIN_SEMI:
			if (partition_range_bound_cmp(partnatts, partsupfuncs,
										  partcollations, left_ub,
										  right_ub) < 0)
				*merged_ub = left_ub;
			else
				*merged_ub = right_ub;

			if (partition_range_bound_cmp(partnatts, partsupfuncs,
										  partcollations, left_lb,
										  right_lb) > 0)
				*merged_lb = left_lb;
			else
				*merged_lb = right_lb;
			break;

		case JOIN_FULL:
			if (partition_range_bound_cmp(partnatts, partsupfuncs,
										  partcollations, left_ub,
										  right_ub) > 0)
				*merged_ub = left_ub;
			else
				*merged_ub = right_ub;

			if (partition_range_bound_cmp(partnatts, partsupfuncs,
										  partcollations, left_lb,
										  right_lb) < 0)
				*merged_lb = left_lb;
			else
				*merged_lb = right_lb;
			break;

		default:
			elog(ERROR, "unexpected join type %d", jointype);
	}

	return;
}

/*
 * Add the lower bound of the next range to the list of bounds, if the lower
 * bound is higher or equal to the previous upper bound. If successful return
 * true, otherwise false.
 */
static bool
partition_range_merge_next_lb(int partnatts, FmgrInfo *partsupfuncs,
							  Oid *partcollations, Datum *next_lb_datums,
							  PartitionRangeDatumKind *next_lb_kind,
							  List **merged_datums, List **merged_kinds,
							  List **merged_indexes)
{
	int			cmpval;

	if (!*merged_datums)
	{
		Assert(!*merged_kinds && !*merged_indexes);
		cmpval = 1;
	}
	else
	{
		PartitionRangeBound	prev_ub;

		prev_ub.datums = llast(*merged_datums);
		prev_ub.kind = llast(*merged_kinds);
		prev_ub.lower = false;

		cmpval = partition_rbound_cmp(partnatts, partsupfuncs, partcollations,
									  next_lb_datums, next_lb_kind, false,
									  &prev_ub);
	}

	/*
	 * The lower bound is lower than the last upper bound, thus does not fit
	 * the bounds created so far and hence can not be merged with the existing
	 * bounds.
	 */
	if (cmpval < 0)
		return false;

	/*
	 * Add bounds of the new merged partition. If the next lower bound is
	 * higher than the last upper bound, add new range with index
	 * corresponding to the lower bound as -1. If the merged lower bound
	 * is same as the last merged upper bound, the last upper bound will be
	 * reused as the lower bound of the next range.
	 */
	if (cmpval > 0)
	{
		*merged_datums = lappend(*merged_datums, next_lb_datums);
		*merged_kinds = lappend(*merged_kinds, next_lb_kind);
		*merged_indexes = lappend_int(*merged_indexes, -1);
	}

	return true;
}

/*
 * handle_missing_partition
 *
 * If a range appears in one of the joining relations but not the other, a row
 * in the corresponding partition will not have any join partner in the other
 * relation, unless the other relation has a default partition. If a given list
 * value is present in one joining relation but not the other, the default
 * partition on the other side may contain that value.
 *
 * In both these cases, such an extra partition forms a joining pair with the
 * default partition, if any,  on the other side.
 *
 * If the default partition happens to be on the outer side of the join, the
 * resultant partition will act as the default partition of the join relation.
 * Otherwise the resultant partition will be associated with the range.
 *
 * When the default partition is not present in the other relation, the rows in
 * the extra partition will be included in the bounds of the join result, if it
 * appears on the outer side of the join, since all rows from the outer side
 * are included in the join result.
 *
 * This function handles all these cases.
 *
 * missing_side_pmap, missing_side_mmap and missing_side_default are the
 * partition map, merge map (See partition_range/list_bounds_merge() for
 * details) and the index of default partition respectively corresponding the
 * side with missing partition.
 *
 * other_pmap, other_mmap and other_part are the partition map, merge map (See
 * partition_range/list_bounds_merge() for details) and the index of extra
 * partition respectively corresponding to the side with the extra partition.
 *
 * It returns true if the matching succeeds, otherwise returns false.
 */
static bool
handle_missing_partition(int *missing_side_pmap, int *missing_side_mmap,
						 int missing_side_default, int *other_pmap,
						 int *other_mmap, int other_part,
						 bool missing_side_outer,
						 bool missing_side_inner,
						 int *next_index, int *default_index,
						 int *merged_index)
{
	bool		missing_has_default = (missing_side_default != -1);

	if (missing_has_default)
	{
		*merged_index = map_and_merge_partitions(missing_side_pmap,
												 missing_side_mmap,
												 missing_side_default,
												 other_pmap, other_mmap,
												 other_part,
												 next_index);
		if (*merged_index < 0)
			return false;

		if (missing_side_outer)
		{
			/*
			 * Default partition on the outer side forms the default
			 * partition of the join result.
			 */
			if (*default_index < 0)
				*default_index = *merged_index;
			else if(*default_index != *merged_index)
			{
				/*
				 * Ended up with default partition on the outer side
				 * being joined with multiple partitions on the inner
				 * side. We don't support this case.
				 */
				return false;
			}

			/*
			 * Since the merged partition acts as a default partition, it
			 * doesn't need a separate index.
			 */
			*merged_index = -1;
		}
	}
	else if (missing_side_inner)
	{
		/*
		 * If this partition has already been mapped (say because we
		 * found an overlapping range earlier), we know where does it
		 * fit in the join result. Nothing to do in that case. Else
		 * create a new merged partition.
		 */
		if (other_mmap[other_part] < 0)
		{
			other_mmap[other_part] = *next_index;
			*next_index = *next_index + 1;
			*merged_index = other_mmap[other_part];
		}
	}

	return true;
}

/*
 * partition_range_bounds_merge
 *
 * partition_bounds_merge()'s arm for range partitioned tables.
 */
static PartitionBoundInfo
partition_range_bounds_merge(int partnatts, FmgrInfo *partsupfuncs,
							 Oid *partcollations, PartitionBoundInfo outer_bi,
							 int outer_nparts, PartitionBoundInfo inner_bi,
							 int inner_nparts, JoinType jointype,
							 List **outer_parts, List **inner_parts)
{
	int		   *outer_pmap;
	int		   *outer_mmap;
	int		   *inner_pmap;
	int		   *inner_mmap;
	int			cnt1;
	int			cnt2;
	int			outer_part;
	int			inner_part;
	PartitionBoundInfo merged_bounds = NULL;
	bool		merged = true;
	int			outer_lb_index;
	int			inner_lb_index;
	int			next_index;
	int			default_index = -1;
	List	   *merged_datums = NIL;
	List	   *merged_indexes = NIL;
	List	   *merged_kinds = NIL;
	int			inner_default = inner_bi->default_index;
	int			outer_default = outer_bi->default_index;
	bool		inner_has_default = partition_bound_has_default(inner_bi);
	bool		outer_has_default = partition_bound_has_default(outer_bi);

	Assert(outer_bi->strategy == inner_bi->strategy &&
		   outer_bi->strategy == PARTITION_STRATEGY_RANGE);

	*outer_parts = NIL;
	*inner_parts = NIL;

	/*
	 * Allocate and initialize partition maps. We maintain four maps, two maps
	 * for each joining relation. pmap[i] gives the partition from the other
	 * relation which would join with ith partition of the given relation.
	 * Partition i from the given relation will join with partition pmap[i]
	 * from the other relation to produce partition mmap[i] of the join (merged
	 * partition).
	 *
	 * pmap[i] = -1 indicates that ith partition of a given relation does not
	 * have a matching partition from the other relation.
	 *
	 * mmap[i] = -1 indicates that ith partition of a given relation does not
	 * contribute to the join result. That can happen only when the given
	 * relation is the inner relation and it doesn't have a matching partition
	 * from the outer relation, hence pmap[i] should be -1.
	 *
	 * In case of an outer join, every partition of the outer join will appear
	 * in the join result, and thus has mmap[i] set for all i. But it's not
	 * necessary that every partition on the outer side will have a matching
	 * partition on the inner side. In such a case, we end up with pmap[i] = -1
	 * and mmap[i] != -1.
	 */
	outer_pmap = (int *) palloc(sizeof(int) * outer_nparts);
	outer_mmap = (int *) palloc(sizeof(int) * outer_nparts);
	inner_pmap = (int *) palloc(sizeof(int) * inner_nparts);
	inner_mmap = (int *) palloc(sizeof(int) * inner_nparts);

	for (cnt1 = 0; cnt1 < outer_nparts; cnt1++)
	{
		outer_pmap[cnt1] = -1;
		outer_mmap[cnt1] = -1;
	}
	for (cnt2 = 0; cnt2 < inner_nparts; cnt2++)
	{
		inner_pmap[cnt2] = -1;
		inner_mmap[cnt2] = -1;
	}

	/*
	 * Merge the ranges (partitions) from both sides. Every iteration compares
	 * a pair of ranges, one from each side, advancing to the next range from
	 * the side with smaller upper range bound. If upper bounds of ranges from
	 * both sides match exactly, both the sides are advanced. For a given pair
	 * of ranges, we decide whether the corresponding partition match or not.
	 * lb_index, for inner or outer side, keeps track of the index of lower bound
	 * datum in PartitionBoundInfo::datums of that side.
	 */
	outer_lb_index = 0;
	inner_lb_index = 0;
	next_index = 0;
	while (outer_lb_index < outer_bi->ndatums ||
		   inner_lb_index < inner_bi->ndatums)
	{
		PartitionRangeBound outer_lb;
		PartitionRangeBound outer_ub;
		PartitionRangeBound inner_lb;
		PartitionRangeBound inner_ub;
		PartitionRangeBound *merged_lb = NULL;
		PartitionRangeBound *merged_ub = NULL;
		int			merged_index = -1;
		bool		overlap;
		bool		finished_outer = false;
		bool		finished_inner = false;

		/* Result of bounds comparison per partition_rbound_cmp(). */
		int			ub_cmpval;	/* Upper bounds comparison result. */
		int			lb_cmpval;	/* Lower bounds comparison result. */

		/* Get the range bounds of the next pair of partitions. */
		if (outer_lb_index < outer_bi->ndatums)
			outer_part = partition_get_range_bounds(outer_bi, outer_lb_index,
												&outer_lb, &outer_ub);
		else
			finished_outer = true;

		if (inner_lb_index < inner_bi->ndatums)
			inner_part = partition_get_range_bounds(inner_bi, inner_lb_index,
												&inner_lb, &inner_ub);
		else
			finished_inner = true;

		Assert(!finished_outer || !finished_inner);

		/*
		 * We run this loop till both the sides finish. This allows to avoid
		 * duplicating code to handle the remaining partitions on the side
		 * which finishes later. For that we set the comparison parameters
		 * overlap, ub_cmpval and lb_cmpval in such a way that it appears as if
		 * the side which finishes earlier has an extra partition with lower
		 * and upper bounds higher than any other partition of the unfinished
		 * side. That way we advance the partitions on that side till all of
		 * them are  exhausted.
		 */
		if (finished_outer)
		{
			overlap = false;
			ub_cmpval = 1;
			lb_cmpval = 1;
		}
		else if (finished_inner)
		{
			overlap = false;
			ub_cmpval = -1;
			lb_cmpval = -1;
		}
		else
			overlap = partition_range_cmp(partnatts, partsupfuncs, partcollations,
										  &outer_lb, &outer_ub, &inner_lb,
										  &inner_ub, &ub_cmpval, &lb_cmpval);

		if (overlap)
		{
			/*
			 * The rows from overlapping portion of ranges on both sides may
			 * join, hence the corresponding pair of partitions form a joining
			 * pair. Match them and produce the bounds of the joint partition
			 * and its index by merging the bounds according to the type of
			 * join.
			 */
			partition_range_merge(partnatts, partsupfuncs, partcollations,
								  jointype, &outer_lb, &outer_ub, &inner_lb,
								  &inner_ub, &merged_lb, &merged_ub);
			merged_index = map_and_merge_partitions(outer_pmap, outer_mmap,
													outer_part, inner_pmap,
													inner_mmap, inner_part,
													&next_index);

			if (merged_index < 0)
			{
				/* Failed to match the partitions. */
				merged = false;
				break;
			}

			/*
			 * If the ranges overlap but don't exactly match, a row from
			 * non-overlapping portion of the range from one side of join may
			 * find its join partner in the previous or next overlapping
			 * partition or default partition on the other side , if such a
			 * partition exists. All those cases, if true, will cause one
			 * partition from that side to match at least two partitions on the
			 * other side; a case that we do not support now. Previous
			 * partition has been delt with in the previous iteration of this
			 * loop, next partition will be delt in the next iteration. We will
			 * deal with the default partition here.
			 */
			if ((lb_cmpval < 0 && inner_has_default) ||
				/* Non-overlapping range on the lower side of outer range. */
				(lb_cmpval > 0 && outer_has_default) ||
				/* Non-overlapping range on the lower side of inner range. */
				(ub_cmpval < 0 && outer_has_default) ||
				/* Non-overlapping range on the upper side of inner range. */
				(ub_cmpval > 0 && inner_has_default))
				/* Non-overlapping range on the upper side of outer range. */
			{
				merged = false;
				break;
			}
		}

		if (ub_cmpval == 0)
		{
			/* Upper bounds of both the ranges match. */
			Assert(overlap);

			/* Move to the next pair of partitions. */
			outer_lb_index = partition_range_get_next_lb_index(outer_bi,
															   outer_lb_index);
			inner_lb_index = partition_range_get_next_lb_index(inner_bi,
															   inner_lb_index);
		}
		else if (ub_cmpval < 0)
		{
			/* Upper bound of inner range higher than that of the outer. */

			if (overlap)
			{
				/* We have already dealt with overlapping ranges. */
			}
			else
			{
				/* A range missing from the inner side. */
				bool		missing_side_outer;
				bool		missing_side_inner;

				merged_lb = &outer_lb;
				merged_ub = &outer_ub;

				/*
				 * For a FULL join, inner relation acts as both OUTER and INNER
				 * relation.  For LEFT and ANTI join the inner relation acts as
				 * INNER relation. For INNER and SEMI join OUTER and INNER
				 * differentiation is immaterial.
				 */
				missing_side_inner = (jointype == JOIN_FULL ||
									  jointype == JOIN_LEFT ||
									  jointype == JOIN_ANTI);
				missing_side_outer = (jointype == JOIN_FULL);
				merged = handle_missing_partition(inner_pmap, inner_mmap,
														inner_default, outer_pmap,
														outer_mmap, outer_part,
														missing_side_outer,
														missing_side_inner,
														&next_index,
														&default_index,
														&merged_index);

				if (!merged)
					break;
			}

			/* Move to the next partition on the outer side. */
			Assert(!finished_outer);
			outer_lb_index = partition_range_get_next_lb_index(outer_bi,
															   outer_lb_index);
		}
		else
		{
			Assert(ub_cmpval > 0);

			/* Upper bound of outer range higher than that of the inner. */
			if (overlap)
			{
				/* We have already dealt with overlapping ranges. */
			}
			else
			{
				/* A range missing from the outer side. */
				bool		missing_side_outer;
				bool		missing_side_inner;

				merged_lb = &inner_lb;
				merged_ub = &inner_ub;

				/*
				 * For a FULL join, outer relation acts as both OUTER and INNER
				 * relation.  For LEFT and ANTI join the outer relation acts as
				 * OUTER relation. For INNER and SEMI join OUTER and INNER
				 * differentiation is immaterial.
				 */
				missing_side_outer = (jointype == JOIN_FULL ||
									  jointype == JOIN_LEFT ||
									  jointype == JOIN_ANTI);
				missing_side_inner = (jointype == JOIN_FULL);
				merged = handle_missing_partition(outer_pmap, outer_mmap,
												  outer_default, inner_pmap,
												  inner_mmap, inner_part,
												  missing_side_outer,
												  missing_side_inner,
												  &next_index,
												  &default_index,
												  &merged_index);

				if (!merged)
					break;
			}

			/* Move to the next partition on the inner side. */
			Assert (!finished_inner);
			inner_lb_index = partition_range_get_next_lb_index(inner_bi,
															   inner_lb_index);
		}

		Assert(merged);

		if (merged_index < 0)
		{
			/* We didn't find a new merged partition. */
			continue;
		}

		/*
		 * We have a valid partition index for the next partition of join. The
		 * partition should have valid range.
		 */
		Assert(merged_lb && merged_ub);

		/* Try merging new lower bound with the last upper bound. */
		merged = partition_range_merge_next_lb(partnatts, partsupfuncs,
											   partcollations,
											   merged_lb->datums,
											   merged_lb->kind, &merged_datums,
											   &merged_kinds, &merged_indexes);
		if (merged)
		{
			/* Add upper bound with the merged partition index. */
			merged_datums = lappend(merged_datums, merged_ub->datums);
			merged_kinds = lappend(merged_kinds, merged_ub->kind);
			merged_indexes = lappend_int(merged_indexes, merged_index);
		}
		else
			break;
	}

	if (merged)
		merged = merge_default_partitions(outer_bi, outer_pmap, outer_mmap,
										  inner_bi, inner_pmap, inner_mmap,
										  jointype, &next_index,
										  &default_index);

	/* Create PartitionBoundInfo for the join result. */
	if (merged)
	{
		/* Use maps to match partition from the joining relations. */
		generate_matching_part_pairs(outer_mmap, outer_nparts, inner_mmap,
									 inner_nparts, jointype, next_index,
									 outer_parts, inner_parts);

		/* Craft a PartitionBoundInfo to return. */
		if (*outer_parts && *inner_parts)
		{
			Assert(list_length(*outer_parts) == list_length(*inner_parts));
			Assert(list_length(*outer_parts) == next_index);
			merged_bounds = build_merged_partition_bounds(outer_bi->strategy,
														  merged_datums,
														  merged_indexes,
														  merged_kinds,
														  -1, default_index);
		}
	}

	/* Free any memory we used in this function. */
	list_free(merged_datums);
	list_free(merged_indexes);
	list_free(merged_kinds);
	pfree(outer_pmap);
	pfree(inner_pmap);
	pfree(outer_mmap);
	pfree(inner_mmap);

	return merged_bounds;
}

/*
 * partition_list_bounds_merge
 *
 * partition_bounds_merge()'s arm for list partitioned tables.
 *
 */
static PartitionBoundInfo
partition_list_bounds_merge(int partnatts, FmgrInfo *partsupfunc,
							Oid *partcollation, PartitionBoundInfo outer_bi,
							int outer_nparts, PartitionBoundInfo inner_bi,
							int inner_nparts, JoinType jointype,
							List **outer_parts, List **inner_parts)
{
	int		   *outer_pmap;	/* outer to inner partition map */
	int		   *outer_mmap;	/* outer to merged partition map */
	int		   *inner_pmap;	/* inner to outer partition map */
	int		   *inner_mmap;	/* inner to merged partition map */
	int			cnto;
	int			cnti;
	bool		merged = true;
	List	   *merged_datums = NIL;
	List	   *merged_indexes = NIL;
	int			next_index = 0;
	int			null_index;
	int			default_index = -1;
	PartitionBoundInfo merged_bounds = NULL;
	int		   *outer_indexes = outer_bi->indexes;
	int		   *inner_indexes = inner_bi->indexes;
	int			outer_default = outer_bi->default_index;
	int			inner_default = inner_bi->default_index;

	Assert(outer_bi->strategy == inner_bi->strategy &&
		   outer_bi->strategy == PARTITION_STRATEGY_LIST);

	/* List partitions do not require unbounded ranges. */
	Assert(!outer_bi->kind && !inner_bi->kind);

	/*
	 * Allocate and initialize partition maps. We maintain four maps, two maps
	 * for each joining relation. pmap[i] gives the partition from the other
	 * relation which would join with ith partition of the given relation.
	 * Partition i from the given relation will join with partition pmap[i]
	 * from the other relation to produce partition mmap[i] of the join (merged
	 * partition).
	 *
	 * pmap[i] = -1 indicates that ith partition of a given relation does not
	 * have a matching partition from the other relation.
	 *
	 * mmap[i] = -1 indicates that ith partition of a given relation does not
	 * contribute to the join result. That can happen only when the given
	 * relation is the inner relation and it doesn't have a matching partition
	 * from the outer relation, hence pmap[i] should be -1.
	 *
	 * In case of an outer join, every partition of the outer join will appear
	 * in the join result, and thus has mmap[i] set for all i. But it's not
	 * necessary that every partition on the outer side will have a matching
	 * partition on the inner side. In such a case, we end up with pmap[i] = -1
	 * and mmap[i] != -1.
	 */
	outer_pmap = (int *) palloc(sizeof(int) * outer_nparts);
	outer_mmap = (int *) palloc(sizeof(int) * outer_nparts);
	inner_pmap = (int *) palloc(sizeof(int) * inner_nparts);
	inner_mmap = (int *) palloc(sizeof(int) * inner_nparts);

	for (cnto = 0; cnto < outer_nparts; cnto++)
	{
		outer_pmap[cnto] = -1;
		outer_mmap[cnto] = -1;
	}
	for (cnti = 0; cnti < inner_nparts; cnti++)
	{
		inner_pmap[cnti] = -1;
		inner_mmap[cnti] = -1;
	}

	/*
	 * Merge the list value datums from both sides. Every iteration compares a
	 * pair of datums, one from each side, advancing to the next datum from the
	 * side with smaller datum. If datums from both sides match exactly, both
	 * the sides are advanced. For a given pair of datums, we decide whether
	 * the corresponding partition match or not.
	 */
	cnto = cnti = 0;
	while (cnto < outer_bi->ndatums || cnti < inner_bi->ndatums)
	{
		Datum	   *odatums;
		Datum	   *idatums;
		int			o_index;
		int			i_index;
		int			cmpval;
		int			merged_index = -1;
		Datum	   *merged_datum;
		bool		finished_inner;
		bool		finished_outer;

		/*
		 * We run this loop till both the sides finish. This allows to avoid
		 * duplicating code to handle the remaining datums on the side which
		 * finishes later. For that we set the comparison parameter cmpval in
		 * such a way that it appears as if the side which finishes earlier has
		 * an extra datum higher than any other datum on the unfinished side.
		 * That way we advance the datums on the unfinished side till all of
		 * its datums are exhausted.
		 */
		if (cnto >= outer_bi->ndatums)
		{
			finished_outer = true;
			odatums = NULL;
			o_index = -1;
		}
		else
		{
			finished_outer = false;
			odatums = outer_bi->datums[cnto];
			o_index = outer_indexes[cnto];
		}

		if (cnti >= inner_bi->ndatums)
		{
			finished_inner = true;
			idatums = NULL;
			i_index = -1;
		}
		else
		{
			finished_inner = false;
			idatums = inner_bi->datums[cnti];
			i_index = inner_indexes[cnti];
		}

		/* If we exhausted both the sides, we won't enter the loop. */
		Assert(!finished_inner || !finished_outer);

		if (finished_outer)
			cmpval = 1;
		else if (finished_inner)
			cmpval = -1;
		else
		{
			/* Every list datum should map to a valid partition index. */
			Assert(o_index >= 0 && i_index >= 0 &&
				   odatums != NULL && idatums != NULL);

			cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[0],
													 partcollation[0],
													 odatums[0], idatums[0]));
		}

		if (cmpval == 0)
		{
			/*
			 * Datums match. Rows on either side with these datums as partition
			 * key value will join and will be part of the partition of the
			 * join result produced by joining the corresponding partitions.
			 * Match the corresponding partitions and if successful, add the
			 * datum to the list of merged datums with index of merged
			 * partition containing it.
			 */
			merged_datum = odatums;
			merged_index = map_and_merge_partitions(outer_pmap, outer_mmap,
													o_index, inner_pmap,
													inner_mmap, i_index,
													&next_index);

			if (merged_index < 0)
			{
				merged = false;
				break;
			}

			/* Move to the next pair of bounds. */
			cnto++;
			cnti++;
		}
		else if (cmpval < 0)
		{
			bool		missing_side_outer;
			bool		missing_side_inner;

			/* A datum missing from the inner side. */
			merged_index = -1;
			merged_datum = odatums;

			/*
			 * For a FULL join, inner relation acts as both OUTER and INNER
			 * relation.  For LEFT and ANTI join the inner relation acts as
			 * INNER relation. For INNER and SEMI join OUTER and INNER
			 * differentiation is immaterial.
			 */
			missing_side_inner = (jointype == JOIN_FULL ||
								  jointype == JOIN_LEFT ||
								  jointype == JOIN_ANTI);
			missing_side_outer = (jointype == JOIN_FULL);
			merged = handle_missing_partition(inner_pmap, inner_mmap,
													inner_default, outer_pmap,
													outer_mmap, o_index,
													missing_side_outer,
													missing_side_inner,
													&next_index,
													&default_index,
													&merged_index);

			if (!merged)
				break;

			/* Move to the next datum on the outer side. */
			Assert(!finished_outer);
			cnto++;
		}
		else
		{
			bool		missing_side_outer;
			bool		missing_side_inner;

			Assert(cmpval > 0);

			/* A datum missing from the outer side. */
			merged_index = -1;
			merged_datum = idatums;

			/*
			 * For a FULL join, outer relation acts as both OUTER and INNER
			 * relation.  For LEFT and ANTI join the outer relation acts as
			 * OUTER relation. For INNER and SEMI join OUTER and INNER
			 * differentiation is immaterial.
			 */
			missing_side_outer = (jointype == JOIN_FULL ||
								  jointype == JOIN_LEFT ||
								  jointype == JOIN_ANTI);
			missing_side_inner = (jointype == JOIN_FULL);
			merged = handle_missing_partition(outer_pmap, outer_mmap,
											  outer_default, inner_pmap,
											  inner_mmap, i_index,
											  missing_side_outer,
											  missing_side_inner,
											  &next_index,
											  &default_index,
											  &merged_index);

			if (!merged)
				break;

			/* Move to the next datum on the right side. */
			Assert(!finished_inner);
			cnti++;
		}

		/*
		 * Add the list value with appropriate index in the list of datums, if
		 * we have associated a partition with this list value.
		 */
		if (merged_index >= 0)
		{
			merged_indexes = lappend_int(merged_indexes, merged_index);
			merged_datums = lappend(merged_datums, merged_datum);
		}
	}

	if (merged)
		merged = merge_null_partitions(outer_bi, outer_pmap, outer_mmap,
									   inner_bi, inner_pmap, inner_mmap,
									   jointype, &next_index, &null_index,
									   &default_index);

	if (merged)
		merged = merge_default_partitions(outer_bi, outer_pmap, outer_mmap,
										  inner_bi, inner_pmap, inner_mmap,
										  jointype, &next_index,
										  &default_index);

	/* If successful build the output structures. */
	if (merged)
	{

		/* Use maps to match partition from the joining relations. */
		generate_matching_part_pairs(outer_mmap, outer_nparts, inner_mmap,
									 inner_nparts, jointype, next_index,
									 outer_parts, inner_parts);
		/* Craft a PartitionBoundInfo to return. */
		if (*outer_parts && *inner_parts)
		{
			Assert(list_length(*outer_parts) == list_length(*inner_parts));
			Assert(list_length(*outer_parts) == next_index);
			merged_bounds = build_merged_partition_bounds(outer_bi->strategy,
														  merged_datums,
														  merged_indexes, NIL,
														  null_index, default_index);
		}
	}

	/* Free up all extra memory before returning from this function. */
	list_free(merged_datums);
	list_free(merged_indexes);
	pfree(outer_pmap);
	pfree(inner_pmap);
	pfree(outer_mmap);
	pfree(inner_mmap);

	return merged_bounds;
}

/*
 * partition_bounds_merge()'s arm for hash partitioned tables.
 *
 * If the given two hash bounds are same, the function returns the first one
 * without any change, alongwith the lists of matching partitions. Otherwise it
 * returns NULL.
 *
 * We could try merging the bounds when both the bounds have same greatest
 * modulii. But there seems to be hardly any requirement for the same.
 */
static PartitionBoundInfo
partition_hash_bounds_merge(int partnatts, FmgrInfo *partsupfunc,
							Oid *partcollation, PartitionBoundInfo outer_bi,
							int outer_nparts, PartitionBoundInfo inner_bi,
							int inner_nparts, JoinType jointype,
							List **outer_parts, List **inner_parts)
{
	int			nparts;
	int			cnt;

	Assert(outer_bi->strategy == inner_bi->strategy &&
		   outer_bi->strategy == PARTITION_STRATEGY_HASH);

	/*
	 * Hash partitioned table does not have explicit NULL accepting partition
	 * and also does not have a default partition.
	 */
	Assert(!partition_bound_has_default(outer_bi) &&
		   !partition_bound_has_default(inner_bi));
	Assert(!partition_bound_accepts_nulls(outer_bi) &&
		   !partition_bound_accepts_nulls(inner_bi));

	*outer_parts = NIL;
	*inner_parts = NIL;

	if (outer_nparts != inner_nparts)
		return NULL;
	nparts = outer_nparts;

	if (outer_bi->ndatums != inner_bi->ndatums ||
		!partition_hbounds_equal(outer_bi, inner_bi))
		return NULL;

	 /*
	  * Cook up list of matching partitions. Since bounds are exactly same the
	  * partitions at the same position from both the relations match.
	  */
	for (cnt = 0; cnt < nparts; cnt++)
	{
		*outer_parts = lappend_int(*outer_parts, cnt);
		*inner_parts = lappend_int(*inner_parts, cnt);
	}

	return outer_bi;
}

/*
 * map_and_merge_partitions
 *
 * If the two given partitions (given by index1 and index2 resp.) are already
 * mapped to each other return the index of corresponding partition in the
 * merged set of partitions.  If they do not have a merged partition associated
 * with them, assign a new merged partition index.  If the partitions are
 * already mapped and their mapped partitions are different from each other,
 * they can not be merged, so return -1.
 *
 * partmap1[i] gives the partition of relation 2 which matches ith partition of
 * relation 1. Similarly for partmap2.
 *
 * mergemap1[i] gives the partition in the merged set to which ith partition of
 * relation 1 maps to. Similarly for mergemap2.
 *
 * index1 and index2 are the indexes of matching partition from respective
 * relations.
 *
 * *next_index is used and incremented when the given partitions require a new
 * merged partition.
 */
static int
map_and_merge_partitions(int *partmap1, int *mergemap1, int index1,
						 int *partmap2, int *mergemap2, int index2,
						 int *next_index)
{
	int			merged_index;

	/*
	 * If both the partitions are not mapped to each other, update the
	 * maps.
	 */
	if (partmap1[index1] < 0 && partmap2[index2] < 0)
	{
		partmap1[index1] = index2;
		partmap2[index2] = index1;
	}

	/*
	 * If the given to partitions map to each other, find the corresponding
	 * merged partition index .
	 */
	if (partmap1[index1] == index2 && partmap2[index2] == index1)
	{
		/*
		 * If both the partitions are mapped to the same merged partition, get
		 * the index of merged partition.
		 */
		if (mergemap1[index1] == mergemap2[index2])
		{
			merged_index = mergemap1[index1];

			/*
			 * If the given two partitions do not have a merged partition
			 * associated with them, allocate a new merged partition.
			 */
			if (merged_index < 0)
			{
				merged_index = *next_index;
				*next_index = *next_index + 1;
				mergemap1[index1] = merged_index;
				mergemap2[index2] = merged_index;
			}
		}

		/*
		 * If partition from one relation was mapped to a merged partition but
		 * not the partition from the other relation, map the same merged
		 * partition to the partition from other relation, since matching
		 * partitions map to the same merged partition.
		 */
		else if (mergemap1[index1] >= 0 && mergemap2[index2] < 0)
		{
			mergemap2[index2] = mergemap1[index1];
			merged_index = mergemap1[index1];
		}
		else if (mergemap1[index1] < 0 && mergemap2[index2] >= 0)
		{
			mergemap1[index1] = mergemap2[index2];
			merged_index = mergemap2[index2];
		}
		else
		{
			Assert(mergemap1[index1] != mergemap2[index2] &&
				   mergemap1[index1] >= 0 && mergemap2[index2] >= 0);

			/*
			 * Both the partitions map to different merged partitions. This
			 * means that multiple partitions from one relation matches to one
			 * partition from the other relation. Partition-wise join does not
			 * handle this case right now, since it requires ganging multiple
			 * partitions together (into one RelOptInfo).
			 */
			merged_index = -1;
		}
	}
	else
	{
		/*
		 * Multiple partitions from one relation map to one partition from the
		 * other relation. Partition-wise join does not handle this case right
		 * now, since it requires ganging multiple partitions together (into
		 * one RelOptInfo).
		 */
		merged_index = -1;
	}

	return merged_index;
}

/*
 * generate_matching_part_pairs
 *
 * mergemap1 and mergemap2 map each partition from either side of the join to a
 * merged partition resp. E.g. mergemap1[i] gives the merged partition to which
 * ith partition of first relation maps and mergemap2[j] gives the merged
 * partition to which jth partition of second relation maps. If mergemap1[i] =
 * mergemap2[j], i and j form the matching pair of partitions.
 *
 * Given these maps this function produces the list pairs of partitions which
 * when joined produce the merged partitions in the order of merged partition
 * indexes.
 *
 * nparts1 and nparts2 are the number of partitions of the joining relations
 * resp.
 *
 * nparts is the number of merged partitions.
 *
 * If successful, the pairs of partitions are returned as two separate lists,
 * parts1 and parts2 resp., one for each side. Otherwise, those lists will be
 * set to NIL.
 */
static void
generate_matching_part_pairs(int *mergemap1, int nparts1, int *mergemap2,
							 int nparts2, JoinType jointype, int nparts,
							 List **parts1, List **parts2)
{
	bool		merged = true;
	int		  **matching_parts;
	int			cnt1;
	int			cnt2;

	matching_parts = (int **) palloc(sizeof(int *) * 2);
	matching_parts[0] = (int *) palloc(sizeof(int) * nparts);
	matching_parts[1] = (int *) palloc(sizeof(int) * nparts);

	*parts1 = NIL;
	*parts2 = NIL;

	for (cnt1 = 0; cnt1 < nparts; cnt1++)
	{
		matching_parts[0][cnt1] = -1;
		matching_parts[1][cnt1] = -1;
	}

	/* Set pairs of matching partitions. */
	for (cnt1 = 0; cnt1 < nparts1; cnt1++)
	{
		if (mergemap1[cnt1] >= 0)
		{
			Assert(mergemap1[cnt1] < nparts);
			matching_parts[0][mergemap1[cnt1]] = cnt1;
		}
	}
	for (cnt2 = 0; cnt2 < nparts2; cnt2++)
	{
		if (mergemap2[cnt2] >= 0)
		{
			Assert(mergemap2[cnt2] < nparts);
			matching_parts[1][mergemap2[cnt2]] = cnt2;
		}
	}

	/*
	 * If we have a partition missing on an inner side, we need to add a dummy
	 * relation which joins with the outer partition. If the inner relation
	 * happens to be a base relation, it will require adding a dummy child
	 * base relation during join processing. Right now, we freeze the base
	 * relation arrays like PlannerInfo::simple_rte_array after planning for
	 * base relations. Adding a new (dummy) base relation would require some
	 * changes to that. So, right now, we do not implement partition-wise join
	 * in such cases.
	 */
	for (cnt1 = 0; cnt1 < nparts; cnt1++)
	{
		int			part1 = matching_parts[0][cnt1];
		int			part2 = matching_parts[1][cnt1];

		/* At least one of the partitions should exist. */
		Assert(part1 >= 0 || part2 >= 0);

		switch (jointype)
		{
			case JOIN_INNER:
			case JOIN_SEMI:

				/*
				 * An inner or semi join can not return any row when the
				 * matching partition on either side is missing. We should
				 * have eliminated all such cases while merging the bounds.
				 */
				Assert(part1 >= 0 && part2 >= 0);
				break;

			case JOIN_LEFT:
			case JOIN_ANTI:
				Assert(part1 >= 0);
				if (part2 < 0)
					merged = false;
				break;

			case JOIN_FULL:
				if (part1 < 0 || part2 < 0)
					merged = false;
				break;

			default:
				elog(ERROR, "unrecognized join type: %d", (int) jointype);
		}

		if (!merged)
			break;

		*parts1 = lappend_int(*parts1, part1);
		*parts2 = lappend_int(*parts2, part2);
	}

	pfree(matching_parts[0]);
	pfree(matching_parts[1]);
	pfree(matching_parts);

	if (!merged)
	{
		list_free(*parts1);
		list_free(*parts2);
		*parts1 = NIL;
		*parts2 = NIL;
	}
}

static PartitionBoundInfo
build_merged_partition_bounds(char strategy, List *merged_datums,
							  List *merged_indexes, List *merged_kinds,
							  int null_index, int default_index)
{
	int			cnt;
	PartitionBoundInfo merged_bounds;
	ListCell   *lc;

	/* We expect the same number of elements in datums and indexes lists. */
	Assert(list_length(merged_datums) == list_length(merged_indexes));

	merged_bounds = (PartitionBoundInfo) palloc(sizeof(PartitionBoundInfoData));
	merged_bounds->strategy = strategy;
	merged_bounds->ndatums = list_length(merged_datums);

	if (strategy == PARTITION_STRATEGY_RANGE)
	{
		Assert(list_length(merged_datums) == list_length(merged_kinds));
		merged_bounds->kind =
			(PartitionRangeDatumKind **) palloc(sizeof(PartitionRangeDatumKind *) *
												list_length(merged_kinds));
		cnt = 0;
		foreach(lc, merged_kinds)
			merged_bounds->kind[cnt++] = lfirst(lc);

		/* There are ndatums+1 indexes in case of range partitions */
		merged_indexes = lappend_int(merged_indexes, -1);
	}
	else
		merged_bounds->kind = NULL;

	cnt = 0;
	merged_bounds->datums = (Datum **) palloc(sizeof(Datum *) *
											  list_length(merged_datums));
	foreach(lc, merged_datums)
		merged_bounds->datums[cnt++] = lfirst(lc);

	merged_bounds->indexes = (int *) palloc(sizeof(int) *
											list_length(merged_indexes));
	cnt = 0;
	foreach(lc, merged_indexes)
		merged_bounds->indexes[cnt++] = lfirst_int(lc);

	merged_bounds->null_index = null_index;
	merged_bounds->default_index = default_index;

	return merged_bounds;
}

/*
 * Merge default partitions from both sides, if any, and assign the default
 * partition for the join result, if necessary.
 *
 * If both the relations have default partitions, try mapping those to each
 * other. If the mapping succeeds corresponding merged partition will act as
 * the default partition of the join result.
 *
 * If inner side of the join has default but not the outer side, rows in it
 * won't appear in the join result. So don't create a default partition. If
 * outer side of the join has default but not the inner side, rows in it will
 * appear in the join result, so create a default merged partition.
 */
static bool
merge_default_partitions(PartitionBoundInfo outer_bi, int *outer_pmap,
						 int *outer_mmap, PartitionBoundInfo inner_bi,
						 int *inner_pmap, int *inner_mmap, JoinType jointype,
						 int *next_index, int *default_index)
{
	int			outer_default = outer_bi->default_index;
	int			inner_default = inner_bi->default_index;
	bool		outer_has_default = partition_bound_has_default(outer_bi);
	bool		inner_has_default = partition_bound_has_default(inner_bi);
	bool		merged = true;

	if (!outer_has_default && !inner_has_default)
		Assert(*default_index < 0);
	else if (outer_has_default && !inner_has_default)
	{
		if (jointype == JOIN_LEFT || jointype == JOIN_FULL ||
			jointype == JOIN_ANTI)
		{
			if (outer_mmap[outer_default] < 0)
			{
				outer_mmap[outer_default] = *next_index;
				*next_index = *next_index + 1;
				Assert(*default_index < 0);
				*default_index = outer_mmap[outer_default];
			}
			else
				Assert(*default_index == outer_mmap[outer_default]);
		}
		else
			Assert(*default_index < 0);
	}
	else if (!outer_has_default && inner_has_default)
	{
		if (jointype == JOIN_FULL)
		{
			if (inner_mmap[inner_default] < 0)
			{
				inner_mmap[inner_default] = *next_index;
				*next_index = *next_index + 1;
				Assert(*default_index < 0);
				*default_index = inner_mmap[inner_default];
			}
			else
				Assert(*default_index == inner_mmap[inner_default]);
		}
		else
			Assert(*default_index < 0);
	}
	else
	{
		Assert(outer_has_default && inner_has_default);

		*default_index = map_and_merge_partitions(outer_pmap, outer_mmap,
												 outer_default, inner_pmap,
												 inner_mmap, inner_default,
												 next_index);

		if (*default_index < 0)
			merged = false;
	}

	return merged;
}

/*
 * merge_null_partitions
 *
 * Merge NULL partitions, i.e. a partition that can hold NULL values for a list
 * partitioned table, if any. Find the index of merged partition to which the
 * NULL values would belong in the join result. If one joining relation has a
 * NULL partition but not the other, try matching it with the default partition
 * from the other relation since the default partition may have rows with NULL
 * partition key. We can eliminate a NULL partition when it appears only on the
 * inner side of the join and the outer side doesn't have a default partition.
 *
 * When the equality operator used for join is strict, two NULL values will not
 * be considered as equal, and thus a NULL partition can be eliminated for an
 * inner join. But we don't check the strictness operator here.
 */
static bool
merge_null_partitions(PartitionBoundInfo outer_bi, int *outer_pmap,
					  int *outer_mmap, PartitionBoundInfo inner_bi,
					  int *inner_pmap, int *inner_mmap, JoinType jointype,
					  int *next_index, int *null_index, int *default_index)
{
	bool		outer_has_null = partition_bound_accepts_nulls(outer_bi);
	bool		inner_has_null = partition_bound_accepts_nulls(inner_bi);
	int			outer_ni = outer_bi->null_index;
	int			inner_ni = inner_bi->null_index;
	int			outer_default = outer_bi->default_index;
	int			inner_default = inner_bi->default_index;
	bool		merged = true;

	if (!outer_has_null && !inner_has_null)
		*null_index = -1;
	else if (outer_has_null && !inner_has_null)
	{
		int			merged_index;
		bool		missing_side_outer;
		bool		missing_side_inner;

		/*
		 * For a FULL join, inner relation acts as both OUTER and INNER
		 * relation.  For LEFT and ANTI join the inner relation acts as
		 * INNER relation. For INNER and SEMI join OUTER and INNER
		 * differentiation is immaterial.
		 */
		missing_side_inner = (jointype == JOIN_FULL ||
							  jointype == JOIN_LEFT ||
							  jointype == JOIN_ANTI);
		missing_side_outer = (jointype == JOIN_FULL);

		merged = handle_missing_partition(inner_pmap, inner_mmap,
										  inner_default, outer_pmap,
										  outer_mmap, outer_ni,
										  missing_side_outer,
										  missing_side_inner, next_index,
										  default_index, &merged_index);
		*null_index = merged_index;

		/*
		 * If the NULL partition was missing from the inner side of the join,
		 * the partition of the join to which the outer null partition maps
		 * will contain the NULL values and thus becomes the NULL partition of
		 * the join.
		 */
		if (missing_side_inner)
			*null_index = outer_mmap[outer_ni];
	}
	else if (!outer_has_null && inner_has_null)
	{
		int			merged_index;
		bool		missing_side_outer;
		bool		missing_side_inner;

		/*
		 * For a FULL join, outer relation acts as both OUTER and INNER
		 * relation.  For LEFT and ANTI join the outer relation acts as OUTER
		 * relation. For INNER and SEMI join OUTER and INNER differentiation is
		 * immaterial.
		 */
		missing_side_outer = (jointype == JOIN_FULL ||
							  jointype == JOIN_LEFT ||
							  jointype == JOIN_ANTI);
		missing_side_inner = (jointype == JOIN_FULL);
		merged = handle_missing_partition(outer_pmap, outer_mmap,
										  outer_default, inner_pmap,
										  inner_mmap, inner_ni,
										  missing_side_outer,
										  missing_side_inner,
										  next_index, default_index,
										  &merged_index);
		*null_index = merged_index;

		/*
		 * If the NULL partition was missing from the inner side of the join,
		 * the partition of the join, to which the outer side null partition maps,
		 * will contain the NULL values and thus becomes the NULL partition of
		 * the join.
		 */
		if (missing_side_inner)
			*null_index = inner_mmap[inner_ni];
	}
	else
	{
		/* Both the relations have NULL partitions, try merging them. */
		*null_index = map_and_merge_partitions(outer_pmap, outer_mmap,
											  outer_ni, inner_pmap,
											  inner_mmap, inner_ni,
											  next_index);
		if (*null_index < 0)
			merged = false;
	}

	return merged;
}
