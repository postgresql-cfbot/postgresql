/*-------------------------------------------------------------------------
 *
 * partition.c
 *		  Partitioning related data structures and functions.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		  src/backend/catalog/partition.c
 *
 *-------------------------------------------------------------------------
*/

#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/partition.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "rewrite/rewriteManip.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/hashutils.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

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
 * When qsort'ing partition bounds after reading from the catalog, each bound
 * is represented with one of the following structs.
 */

/* One bound of a hash partition */
typedef struct PartitionHashBound
{
	int			modulus;
	int			remainder;
	int			index;
} PartitionHashBound;

/* One value coming from some (index'th) list partition */
typedef struct PartitionListValue
{
	int			index;
	Datum		value;
} PartitionListValue;

/* One bound of a range partition */
typedef struct PartitionRangeBound
{
	int			index;
	Datum	   *datums;			/* range bound datums */
	PartitionRangeDatumKind *kind;	/* the kind of each datum */
	bool		lower;			/* this is the lower (vs upper) bound */
} PartitionRangeBound;

/*
 * Information about a clause matched with a partition key column kept to
 * avoid recomputing it in remove_redundant_clauses().
 */
typedef struct PartClause
{
	Oid		opno;		/* opno to compare partkey to 'value' */
	Oid		inputcollid; /* collation to compare partkey to 'value' */
	Expr   *value;	/* The value the partition key is being compared to */

	/* cached info. */
	bool	valid_cache;	/* Are the following fields populated? */
	int		op_strategy;
	Oid		op_subtype;
	FmgrInfo op_func;
} PartClause;

/*
 * Strategy of a partition clause operator per the partitioning operator class
 * definition.
 */
typedef enum PartOpStrategy
{
	PART_OP_EQUAL,
	PART_OP_LESS,
	PART_OP_GREATER
} PartOpStrategy;

/*
 * PartScanKeyInfo
 *		Information about partition look up keys to be passed to
 *		get_partitions_for_keys()
 *
 * Stores Datums and nullness properties found in clauses which match the
 * partition key.  Properties found are cached and are indexed by the
 * partition key index.
 */
typedef struct PartScanKeyInfo
{
	/*
	 * Equality look up key.  Used to store known Datums values from clauses
	 * compared by an equality operation to the partition key.
	 */
	Datum	eqkeys[PARTITION_MAX_KEYS];

	/*
	 * Lower and upper bounds on a sequence of selected partitions.  These may
	 * contain values for only a prefix of the partition keys.
	 */
	Datum	minkeys[PARTITION_MAX_KEYS];
	Datum	maxkeys[PARTITION_MAX_KEYS];

	/*
	 * Number of values stored in the corresponding array above
	 */
	int		n_eqkeys;
	int		n_minkeys;
	int		n_maxkeys;

	/*
	 * Properties to mark if the clauses found for the corresponding partition
	 * are inclusive of the stored value or not.
	 */
	bool	min_incl;
	bool	max_incl;

	/*
	 * Information about nullness of the partition keys, either specified
	 * explicitly in the query (in the form of a IS [NOT] NULL clause) or
	 * implied from strict clauses matching the partition key.
	 */
	Bitmapset   *keyisnull;
	Bitmapset   *keyisnotnull;
} PartScanKeyInfo;

static int32 qsort_partition_hbound_cmp(const void *a, const void *b);
static int32 qsort_partition_list_value_cmp(const void *a, const void *b,
							   void *arg);
static int32 qsort_partition_rbound_cmp(const void *a, const void *b,
						   void *arg);

static Oid get_partition_operator(PartitionKey key, int col,
					   StrategyNumber strategy, bool *need_relabel);
static Expr *make_partition_op_expr(PartitionKey key, int keynum,
					   uint16 strategy, Expr *arg1, Expr *arg2);
static void get_range_key_properties(PartitionKey key, int keynum,
						 PartitionRangeDatum *ldatum,
						 PartitionRangeDatum *udatum,
						 ListCell **partexprs_item,
						 Expr **keyCol,
						 Const **lower_val, Const **upper_val);
static List *get_qual_for_hash(Relation parent, PartitionBoundSpec *spec);
static List *get_qual_for_list(Relation parent, PartitionBoundSpec *spec);
static List *get_qual_for_range(Relation parent, PartitionBoundSpec *spec,
				   bool for_default);
static List *get_range_nulltest(PartitionKey key);
static List *generate_partition_qual(Relation rel);

static PartitionRangeBound *make_one_range_bound(PartitionKey key, int index,
					 List *datums, bool lower);
static int32 partition_hbound_cmp(int modulus1, int remainder1, int modulus2,
					 int remainder2);
static int32 partition_rbound_cmp(PartitionKey key,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2);
static int32 partition_rbound_datum_cmp(PartitionKey key,
						   Datum *rb_datums, PartitionRangeDatumKind *rb_kind,
						   Datum *tuple_datums);

static int32 partition_bound_cmp(PartitionKey key,
					PartitionBoundInfo boundinfo,
					int offset, void *probe, bool probe_is_bound);
static int partition_bound_bsearch(PartitionKey key,
						PartitionBoundInfo boundinfo,
						void *probe, bool probe_is_bound, bool *is_equal);
static int	get_partition_bound_num_indexes(PartitionBoundInfo b);
static int	get_greatest_modulus(PartitionBoundInfo b);
static uint64 compute_hash_value(PartitionKey key, Datum *values, bool *isnull);

/* SQL-callable function for use in hash partition CHECK constraints */
PG_FUNCTION_INFO_V1(satisfies_hash_partition);

static Bitmapset *get_partitions_excluded_by_ne_clauses(
									  PartitionPruneContext *context,
									  List *ne_clauses);
static Bitmapset *get_partitions_from_or_clause_args(
								   PartitionPruneContext *context,
								   List *or_clause_args);
static PartitionClauseInfo *extract_partition_key_clauses(
							  PartitionKey partkey, List *clauses,
							  int rt_index);
static bool extract_bounding_datums(PartitionKey partkey,
						PartitionPruneContext *context,
						List **minimalclauses, PartScanKeyInfo *keys);
static bool partition_cmp_args(PartitionKey key, int partkeyidx,
				   PartClause *pc, PartClause *leftarg, PartClause *rightarg,
				   bool *result);
static PartOpStrategy partition_op_strategy(PartitionKey key, PartClause *pc,
					bool *incl);
static bool partkey_datum_from_expr(PartitionKey key, int partkeyidx,
						Expr *expr, Datum *value);
static void remove_redundant_clauses(PartitionKey partkey,
						 PartitionPruneContext *context,
						 List **minimalclauses);
static Bitmapset *get_partitions_for_keys(Relation rel,
						PartScanKeyInfo *keys);
static Bitmapset *get_partitions_for_keys_hash(Relation rel,
						PartScanKeyInfo *keys);
static Bitmapset *get_partitions_for_keys_list(Relation rel,
						PartScanKeyInfo *keys);
static Bitmapset *get_partitions_for_keys_range(Relation rel,
						PartScanKeyInfo *keys);

/*
 * RelationBuildPartitionDesc
 *		Form rel's partition descriptor
 *
 * Not flushed from the cache by RelationClearRelation() unless changed because
 * of addition or removal of partition.
 */
void
RelationBuildPartitionDesc(Relation rel)
{
	List	   *inhoids,
			   *partoids;
	Oid		   *oids = NULL;
	List	   *boundspecs = NIL;
	ListCell   *cell;
	int			i,
				nparts;
	PartitionKey key = RelationGetPartitionKey(rel);
	PartitionDesc result;
	MemoryContext oldcxt;

	int			ndatums = 0;
	int			default_index = -1;

	/* Hash partitioning specific */
	PartitionHashBound **hbounds = NULL;

	/* List partitioning specific */
	PartitionListValue **all_values = NULL;
	int			null_index = -1;

	/* Range partitioning specific */
	PartitionRangeBound **rbounds = NULL;

	/*
	 * The following could happen in situations where rel has a pg_class entry
	 * but not the pg_partitioned_table entry yet.
	 */
	if (key == NULL)
		return;

	/* Get partition oids from pg_inherits */
	inhoids = find_inheritance_children(RelationGetRelid(rel), NoLock);

	/* Collect bound spec nodes in a list */
	i = 0;
	partoids = NIL;
	foreach(cell, inhoids)
	{
		Oid			inhrelid = lfirst_oid(cell);
		HeapTuple	tuple;
		Datum		datum;
		bool		isnull;
		Node	   *boundspec;

		tuple = SearchSysCache1(RELOID, inhrelid);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", inhrelid);

		/*
		 * It is possible that the pg_class tuple of a partition has not been
		 * updated yet to set its relpartbound field.  The only case where
		 * this happens is when we open the parent relation to check using its
		 * partition descriptor that a new partition's bound does not overlap
		 * some existing partition.
		 */
		if (!((Form_pg_class) GETSTRUCT(tuple))->relispartition)
		{
			ReleaseSysCache(tuple);
			continue;
		}

		datum = SysCacheGetAttr(RELOID, tuple,
								Anum_pg_class_relpartbound,
								&isnull);
		Assert(!isnull);
		boundspec = (Node *) stringToNode(TextDatumGetCString(datum));

		/*
		 * Sanity check: If the PartitionBoundSpec says this is the default
		 * partition, its OID should correspond to whatever's stored in
		 * pg_partitioned_table.partdefid; if not, the catalog is corrupt.
		 */
		if (castNode(PartitionBoundSpec, boundspec)->is_default)
		{
			Oid			partdefid;

			partdefid = get_default_partition_oid(RelationGetRelid(rel));
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}

		boundspecs = lappend(boundspecs, boundspec);
		partoids = lappend_oid(partoids, inhrelid);
		ReleaseSysCache(tuple);
	}

	nparts = list_length(partoids);

	if (nparts > 0)
	{
		oids = (Oid *) palloc(nparts * sizeof(Oid));
		i = 0;
		foreach(cell, partoids)
			oids[i++] = lfirst_oid(cell);

		/* Convert from node to the internal representation */
		if (key->strategy == PARTITION_STRATEGY_HASH)
		{
			ndatums = nparts;
			hbounds = (PartitionHashBound **)
				palloc(nparts * sizeof(PartitionHashBound *));

			i = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));

				if (spec->strategy != PARTITION_STRATEGY_HASH)
					elog(ERROR, "invalid strategy in partition bound spec");

				hbounds[i] = (PartitionHashBound *)
					palloc(sizeof(PartitionHashBound));

				hbounds[i]->modulus = spec->modulus;
				hbounds[i]->remainder = spec->remainder;
				hbounds[i]->index = i;
				i++;
			}

			/* Sort all the bounds in ascending order */
			qsort(hbounds, nparts, sizeof(PartitionHashBound *),
				  qsort_partition_hbound_cmp);
		}
		else if (key->strategy == PARTITION_STRATEGY_LIST)
		{
			List	   *non_null_values = NIL;

			/*
			 * Create a unified list of non-null values across all partitions.
			 */
			i = 0;
			null_index = -1;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				ListCell   *c;

				if (spec->strategy != PARTITION_STRATEGY_LIST)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the list of non-null
				 * datums for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i;
					i++;
					continue;
				}

				foreach(c, spec->listdatums)
				{
					Const	   *val = castNode(Const, lfirst(c));
					PartitionListValue *list_value = NULL;

					if (!val->constisnull)
					{
						list_value = (PartitionListValue *)
							palloc0(sizeof(PartitionListValue));
						list_value->index = i;
						list_value->value = val->constvalue;
					}
					else
					{
						/*
						 * Never put a null into the values array, flag
						 * instead for the code further down below where we
						 * construct the actual relcache struct.
						 */
						if (null_index != -1)
							elog(ERROR, "found null more than once");
						null_index = i;
					}

					if (list_value)
						non_null_values = lappend(non_null_values,
												  list_value);
				}

				i++;
			}

			ndatums = list_length(non_null_values);

			/*
			 * Collect all list values in one array. Alongside the value, we
			 * also save the index of partition the value comes from.
			 */
			all_values = (PartitionListValue **) palloc(ndatums *
														sizeof(PartitionListValue *));
			i = 0;
			foreach(cell, non_null_values)
			{
				PartitionListValue *src = lfirst(cell);

				all_values[i] = (PartitionListValue *)
					palloc(sizeof(PartitionListValue));
				all_values[i]->value = src->value;
				all_values[i]->index = src->index;
				i++;
			}

			qsort_arg(all_values, ndatums, sizeof(PartitionListValue *),
					  qsort_partition_list_value_cmp, (void *) key);
		}
		else if (key->strategy == PARTITION_STRATEGY_RANGE)
		{
			int			k;
			PartitionRangeBound **all_bounds,
					   *prev;

			all_bounds = (PartitionRangeBound **) palloc0(2 * nparts *
														  sizeof(PartitionRangeBound *));

			/*
			 * Create a unified list of range bounds across all the
			 * partitions.
			 */
			i = ndatums = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				PartitionRangeBound *lower,
						   *upper;

				if (spec->strategy != PARTITION_STRATEGY_RANGE)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the allbounds array
				 * for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i++;
					continue;
				}

				lower = make_one_range_bound(key, i, spec->lowerdatums,
											 true);
				upper = make_one_range_bound(key, i, spec->upperdatums,
											 false);
				all_bounds[ndatums++] = lower;
				all_bounds[ndatums++] = upper;
				i++;
			}

			Assert(ndatums == nparts * 2 ||
				   (default_index != -1 && ndatums == (nparts - 1) * 2));

			/* Sort all the bounds in ascending order */
			qsort_arg(all_bounds, ndatums,
					  sizeof(PartitionRangeBound *),
					  qsort_partition_rbound_cmp,
					  (void *) key);

			/* Save distinct bounds from all_bounds into rbounds. */
			rbounds = (PartitionRangeBound **)
				palloc(ndatums * sizeof(PartitionRangeBound *));
			k = 0;
			prev = NULL;
			for (i = 0; i < ndatums; i++)
			{
				PartitionRangeBound *cur = all_bounds[i];
				bool		is_distinct = false;
				int			j;

				/* Is the current bound distinct from the previous one? */
				for (j = 0; j < key->partnatts; j++)
				{
					Datum		cmpval;

					if (prev == NULL || cur->kind[j] != prev->kind[j])
					{
						is_distinct = true;
						break;
					}

					/*
					 * If the bounds are both MINVALUE or MAXVALUE, stop now
					 * and treat them as equal, since any values after this
					 * point must be ignored.
					 */
					if (cur->kind[j] != PARTITION_RANGE_DATUM_VALUE)
						break;

					cmpval = FunctionCall2Coll(&key->partsupfunc[j],
											   key->partcollation[j],
											   cur->datums[j],
											   prev->datums[j]);
					if (DatumGetInt32(cmpval) != 0)
					{
						is_distinct = true;
						break;
					}
				}

				/*
				 * Only if the bound is distinct save it into a temporary
				 * array i.e. rbounds which is later copied into boundinfo
				 * datums array.
				 */
				if (is_distinct)
					rbounds[k++] = all_bounds[i];

				prev = cur;
			}

			/* Update ndatums to hold the count of distinct datums. */
			ndatums = k;
		}
		else
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	/* Now build the actual relcache partition descriptor */
	rel->rd_pdcxt = AllocSetContextCreateExtended(CacheMemoryContext,
												  RelationGetRelationName(rel),
												  MEMCONTEXT_COPY_NAME,
												  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(rel->rd_pdcxt);

	result = (PartitionDescData *) palloc0(sizeof(PartitionDescData));
	result->nparts = nparts;
	if (nparts > 0)
	{
		PartitionBoundInfo boundinfo;
		int		   *mapping;
		int			next_index = 0;

		result->oids = (Oid *) palloc0(nparts * sizeof(Oid));

		boundinfo = (PartitionBoundInfoData *)
			palloc0(sizeof(PartitionBoundInfoData));
		boundinfo->strategy = key->strategy;
		boundinfo->default_index = -1;
		boundinfo->ndatums = ndatums;
		boundinfo->null_index = -1;
		boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));

		/* Initialize mapping array with invalid values */
		mapping = (int *) palloc(sizeof(int) * nparts);
		for (i = 0; i < nparts; i++)
			mapping[i] = -1;

		switch (key->strategy)
		{
			case PARTITION_STRATEGY_HASH:
				{
					/* Modulus are stored in ascending order */
					int			greatest_modulus = hbounds[ndatums - 1]->modulus;

					boundinfo->indexes = (int *) palloc(greatest_modulus *
														sizeof(int));

					for (i = 0; i < greatest_modulus; i++)
						boundinfo->indexes[i] = -1;

					for (i = 0; i < nparts; i++)
					{
						int			modulus = hbounds[i]->modulus;
						int			remainder = hbounds[i]->remainder;

						boundinfo->datums[i] = (Datum *) palloc(2 *
																sizeof(Datum));
						boundinfo->datums[i][0] = Int32GetDatum(modulus);
						boundinfo->datums[i][1] = Int32GetDatum(remainder);

						while (remainder < greatest_modulus)
						{
							/* overlap? */
							Assert(boundinfo->indexes[remainder] == -1);
							boundinfo->indexes[remainder] = i;
							remainder += modulus;
						}

						mapping[hbounds[i]->index] = i;
						pfree(hbounds[i]);
					}
					pfree(hbounds);
					break;
				}

			case PARTITION_STRATEGY_LIST:
				{
					boundinfo->indexes = (int *) palloc(ndatums * sizeof(int));

					/*
					 * Copy values.  Indexes of individual values are mapped
					 * to canonical values so that they match for any two list
					 * partitioned tables with same number of partitions and
					 * same lists per partition.  One way to canonicalize is
					 * to assign the index in all_values[] of the smallest
					 * value of each partition, as the index of all of the
					 * partition's values.
					 */
					for (i = 0; i < ndatums; i++)
					{
						boundinfo->datums[i] = (Datum *) palloc(sizeof(Datum));
						boundinfo->datums[i][0] = datumCopy(all_values[i]->value,
															key->parttypbyval[0],
															key->parttyplen[0]);

						/* If the old index has no mapping, assign one */
						if (mapping[all_values[i]->index] == -1)
							mapping[all_values[i]->index] = next_index++;

						boundinfo->indexes[i] = mapping[all_values[i]->index];
					}

					/*
					 * If null-accepting partition has no mapped index yet,
					 * assign one.  This could happen if such partition
					 * accepts only null and hence not covered in the above
					 * loop which only handled non-null values.
					 */
					if (null_index != -1)
					{
						Assert(null_index >= 0);
						if (mapping[null_index] == -1)
							mapping[null_index] = next_index++;
						boundinfo->null_index = mapping[null_index];
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						/*
						 * The default partition accepts any value not
						 * specified in the lists of other partitions, hence
						 * it should not get mapped index while assigning
						 * those for non-null datums.
						 */
						Assert(default_index >= 0 &&
							   mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}

					/* All partition must now have a valid mapping */
					Assert(next_index == nparts);
					break;
				}

			case PARTITION_STRATEGY_RANGE:
				{
					boundinfo->kind = (PartitionRangeDatumKind **)
						palloc(ndatums *
							   sizeof(PartitionRangeDatumKind *));
					boundinfo->indexes = (int *) palloc((ndatums + 1) *
														sizeof(int));

					for (i = 0; i < ndatums; i++)
					{
						int			j;

						boundinfo->datums[i] = (Datum *) palloc(key->partnatts *
																sizeof(Datum));
						boundinfo->kind[i] = (PartitionRangeDatumKind *)
							palloc(key->partnatts *
								   sizeof(PartitionRangeDatumKind));
						for (j = 0; j < key->partnatts; j++)
						{
							if (rbounds[i]->kind[j] == PARTITION_RANGE_DATUM_VALUE)
								boundinfo->datums[i][j] =
									datumCopy(rbounds[i]->datums[j],
											  key->parttypbyval[j],
											  key->parttyplen[j]);
							boundinfo->kind[i][j] = rbounds[i]->kind[j];
						}

						/*
						 * There is no mapping for invalid indexes.
						 *
						 * Any lower bounds in the rbounds array have invalid
						 * indexes assigned, because the values between the
						 * previous bound (if there is one) and this (lower)
						 * bound are not part of the range of any existing
						 * partition.
						 */
						if (rbounds[i]->lower)
							boundinfo->indexes[i] = -1;
						else
						{
							int			orig_index = rbounds[i]->index;

							/* If the old index has no mapping, assign one */
							if (mapping[orig_index] == -1)
								mapping[orig_index] = next_index++;

							boundinfo->indexes[i] = mapping[orig_index];
						}
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						Assert(default_index >= 0 && mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}
					boundinfo->indexes[i] = -1;
					break;
				}

			default:
				elog(ERROR, "unexpected partition strategy: %d",
					 (int) key->strategy);
		}

		result->boundinfo = boundinfo;

		/*
		 * Now assign OIDs from the original array into mapped indexes of the
		 * result array.  Order of OIDs in the former is defined by the
		 * catalog scan that retrieved them, whereas that in the latter is
		 * defined by canonicalized representation of the partition bounds.
		 */
		for (i = 0; i < nparts; i++)
			result->oids[mapping[i]] = oids[i];
		pfree(mapping);
	}

	MemoryContextSwitchTo(oldcxt);
	rel->rd_partdesc = result;
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
		int			greatest_modulus = get_greatest_modulus(b1);

		/*
		 * If two hash partitioned tables have different greatest moduli,
		 * their partition schemes don't match.
		 */
		if (greatest_modulus != get_greatest_modulus(b2))
			return false;

		/*
		 * We arrange the partitions in the ascending order of their modulus
		 * and remainders.  Also every modulus is factor of next larger
		 * modulus.  Therefore we can safely store index of a given partition
		 * in indexes array at remainder of that partition.  Also entries at
		 * (remainder + N * modulus) positions in indexes array are all same
		 * for (modulus, remainder) specification for any partition.  Thus
		 * datums array from both the given bounds are same, if and only if
		 * their indexes array will be same.  So, it suffices to compare
		 * indexes array.
		 */
		for (i = 0; i < greatest_modulus; i++)
			if (b1->indexes[i] != b2->indexes[i])
				return false;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Nonetheless make sure that the bounds are indeed same when the
		 * indexes match.  Hash partition bound stores modulus and remainder
		 * at b1->datums[i][0] and b1->datums[i][1] position respectively.
		 */
		for (i = 0; i < b1->ndatums; i++)
			Assert((b1->datums[i][0] == b2->datums[i][0] &&
					b1->datums[i][1] == b2->datums[i][1]));
#endif
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
extern PartitionBoundInfo
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
					PartitionBoundInfo boundinfo = partdesc->boundinfo;
					Datum	  **datums = boundinfo->datums;
					int			ndatums = boundinfo->ndatums;
					int			greatest_modulus;
					int			remainder;
					int			offset;
					bool		equal,
								valid_modulus = true;
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
					 * Get greatest bound in array boundinfo->datums which is
					 * less than or equal to spec->modulus and
					 * spec->remainder.
					 */
					offset = partition_bound_bsearch(key, boundinfo, spec,
													 true, &equal);
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

					greatest_modulus = get_greatest_modulus(boundinfo);
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

							offset = partition_bound_bsearch(key, boundinfo,
															 &val->constvalue,
															 true, &equal);
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
				lower = make_one_range_bound(key, -1, spec->lowerdatums, true);
				upper = make_one_range_bound(key, -1, spec->upperdatums, false);

				/*
				 * First check if the resulting range would be empty with
				 * specified lower and upper bounds
				 */
				if (partition_rbound_cmp(key, lower->datums, lower->kind, true,
										 upper) >= 0)
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
					PartitionBoundInfo boundinfo = partdesc->boundinfo;
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
					offset = partition_bound_bsearch(key, boundinfo, lower,
													 true, &equal);

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

							cmpval = partition_bound_cmp(key, boundinfo,
														 offset + 1, upper,
														 true);
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
 * check_default_allows_bound
 *
 * This function checks if there exists a row in the default partition that
 * would properly belong to the new partition being added.  If it finds one,
 * it throws an error.
 */
void
check_default_allows_bound(Relation parent, Relation default_rel,
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
			ExecStoreTuple(tuple, tupslot, InvalidBuffer, false);
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
 * get_partition_parent
 *
 * Returns inheritance parent of a partition by scanning pg_inherits
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
Oid
get_partition_parent(Oid relid)
{
	Form_pg_inherits form;
	Relation	catalogRelation;
	SysScanDesc scan;
	ScanKeyData key[2];
	HeapTuple	tuple;
	Oid			result;

	catalogRelation = heap_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[1],
				Anum_pg_inherits_inhseqno,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(1));

	scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId, true,
							  NULL, 2, key);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for parent of relation %u", relid);

	form = (Form_pg_inherits) GETSTRUCT(tuple);
	result = form->inhparent;

	systable_endscan(scan);
	heap_close(catalogRelation, AccessShareLock);

	return result;
}

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
 * map_partition_varattnos - maps varattno of any Vars in expr from the
 * attno's of 'from_rel' to the attno's of 'to_rel' partition, each of which
 * may be either a leaf partition or a partitioned table, but both of which
 * must be from the same partitioning hierarchy.
 *
 * Even though all of the same column names must be present in all relations
 * in the hierarchy, and they must also have the same types, the attnos may
 * be different.
 *
 * If found_whole_row is not NULL, *found_whole_row returns whether a
 * whole-row variable was found in the input expression.
 *
 * Note: this will work on any node tree, so really the argument and result
 * should be declared "Node *".  But a substantial majority of the callers
 * are working on Lists, so it's less messy to do the casts internally.
 */
List *
map_partition_varattnos(List *expr, int fromrel_varno,
						Relation to_rel, Relation from_rel,
						bool *found_whole_row)
{
	bool		my_found_whole_row = false;

	if (expr != NIL)
	{
		AttrNumber *part_attnos;

		part_attnos = convert_tuples_by_name_map(RelationGetDescr(to_rel),
												 RelationGetDescr(from_rel),
												 gettext_noop("could not convert row type"));
		expr = (List *) map_variable_attnos((Node *) expr,
											fromrel_varno, 0,
											part_attnos,
											RelationGetDescr(from_rel)->natts,
											RelationGetForm(to_rel)->reltype,
											&my_found_whole_row);
	}

	if (found_whole_row)
		*found_whole_row = my_found_whole_row;

	return expr;
}

/*
 * RelationGetPartitionQual
 *
 * Returns a list of partition quals
 */
List *
RelationGetPartitionQual(Relation rel)
{
	/* Quick exit */
	if (!rel->rd_rel->relispartition)
		return NIL;

	return generate_partition_qual(rel);
}

/*
 * get_partition_qual_relid
 *
 * Returns an expression tree describing the passed-in relation's partition
 * constraint. If there is no partition constraint returns NULL; this can
 * happen if the default partition is the only partition.
 */
Expr *
get_partition_qual_relid(Oid relid)
{
	Relation	rel = heap_open(relid, AccessShareLock);
	Expr	   *result = NULL;
	List	   *and_args;

	/* Do the work only if this relation is a partition. */
	if (rel->rd_rel->relispartition)
	{
		and_args = generate_partition_qual(rel);

		if (and_args == NIL)
			result = NULL;
		else if (list_length(and_args) > 1)
			result = makeBoolExpr(AND_EXPR, and_args, -1);
		else
			result = linitial(and_args);
	}

	/* Keep the lock. */
	heap_close(rel, NoLock);

	return result;
}

/*
 * generate_partition_clauses
 *		Processes 'clauses' to try to match them to relation's partition
 *		keys.  If any compatible clauses are found which match a partition
 *		key, then these clauses are stored in 'partclauseinfo'.
 *
 * The caller must ensure that 'clauses' is not an empty List.  Upon return,
 * callers must also check if the 'partclauseinfo' constfalse has been set, if
 * so, then they must be aware that the 'partclauseinfo' may only be partially
 * populated.
 */
PartitionClauseInfo *
generate_partition_clauses(Relation relation,
						   int rt_index, List *clauses)
{
	PartitionDesc		partdesc;
	PartitionKey		partkey;
	PartitionBoundInfo	boundinfo;

	Assert(clauses != NIL);

	partkey = RelationGetPartitionKey(relation);
	partdesc = RelationGetPartitionDesc(relation);

	/* Some functions called below modify this list */
	clauses = list_copy(clauses);
	boundinfo = partdesc->boundinfo;

	/*
	 * For sub-partitioned tables there's a corner case where if the
	 * sub-partitioned table shares any partition keys with its parent, then
	 * it's possible that the partitioning hierarchy allows the parent
	 * partition to only contain a narrower range of values than the
	 * sub-partitioned table does.  In this case it is possible that we'd
	 * include partitions that could not possibly have any tuples matching
	 * 'clauses'.  The possibility of such a partition arrangement
	 * is perhaps unlikely for non-default partitions, but it may be more
	 * likely in the case of default partitions, so we'll add the parent
	 * partition table's partition qual to the clause list in this case only.
	 * This may result in the default partition being eliminated.
	 */
	if (partition_bound_has_default(boundinfo))
	{
		List *partqual = RelationGetPartitionQual(relation);

		partqual = (List *) expression_planner((Expr *) partqual);

		/* Fix Vars to have the desired varno */
		if (rt_index != 1)
			ChangeVarNodes((Node *) partqual, 1, rt_index, 0);

		clauses = list_concat(clauses, partqual);
	}

	return extract_partition_key_clauses(partkey, clauses, rt_index);
}

/*
 * get_partitions_from_clauses
 *		Determine all partitions of context->relation that could possibly
 *		contain a record that matches clauses as described in
 *		context->clauseinfo
 *
 * Returns a Bitmapset of the matching partition indexes, or NULL if none can
 * match.
 */
Bitmapset *
get_partitions_from_clauses(PartitionPruneContext *context)
{
	PartitionClauseInfo	*partclauseinfo = context->clauseinfo;
	PartitionDesc		partdesc;
	PartScanKeyInfo		keys;
	Bitmapset 		   *result;
	ListCell *lc;

	Assert(partclauseinfo != NULL);

	/*
	 * Check if there were proofs that no partitions can match due to some
	 * clause items contradicting another.
	 */
	if (partclauseinfo->constfalse)
		return NULL;

	partdesc = RelationGetPartitionDesc(context->relation);

	if (!partclauseinfo->foundkeyclauses)
	{
		/* No interesting clauses were found to eliminate partitions. */
		result = bms_add_range(NULL, 0, partdesc->nparts - 1);
	}
	else
	{
		PartitionKey	partkey = RelationGetPartitionKey(context->relation);
		List *minimalclauses[PARTITION_MAX_KEYS];

		/*
		 * Populate minimal clauses with the most restrictive
		 * of clauses from context's partclauseinfo.
		 */
		remove_redundant_clauses(partkey, context, minimalclauses);

		/* Did remove_redundant_clauses find any contradicting clauses? */
		if (partclauseinfo->constfalse)
			return NULL;

		if (extract_bounding_datums(partkey, context, minimalclauses, &keys))
		{
			result = get_partitions_for_keys(context->relation, &keys);

			/*
			 * No point in trying to look at other conjunctive clauses, if we
			 * got an empty set in the first place.
			 */
			if (bms_is_empty(result))
				return NULL;
		}
		else
		{
			/*
			 * Looks like we didn't have the values we'd need to eliminate
			 * partitions using get_partitions_for_keys, likely because
			 * context->clauseinfo only contained <> clauses and/or OR
			 * clauses, which are handled further below in this function.
			 */
			result = bms_add_range(NULL, 0, partdesc->nparts - 1);
		}
	}

	/* Select partitions by applying the clauses containing <> operators. */
	if (partclauseinfo->ne_clauses)
	{
		Bitmapset *ne_parts;

		ne_parts = get_partitions_excluded_by_ne_clauses(context,
												partclauseinfo->ne_clauses);

		/* Remove any partitions we found to not be needed */
		result = bms_del_members(result, ne_parts);
		bms_free(ne_parts);
	}

	/* Select partitions by applying OR clauses. */
	foreach(lc, partclauseinfo->or_clauses)
	{
		List *or_args = (List *) lfirst(lc);
		PartitionPruneContext orcontext;
		Bitmapset *or_parts;

		orcontext.rt_index = context->rt_index;
		orcontext.relation = context->relation;
		orcontext.clauseinfo = NULL;

		or_parts = get_partitions_from_or_clause_args(&orcontext, or_args);

		/*
		 * Clauses in or_clauses are mutually conjunctive and also in
		 * in conjunction with the rest of the clauses above, so combine the
		 * partitions thus selected with those in result using set
		 * intersection.
		 */
		result = bms_int_members(result, or_parts);
		bms_free(or_parts);
	}

	return result;
}

/* Module-local functions */

/*
 * get_partitions_excluded_by_ne_clauses
 *
 * Returns a Bitmapset of partition indexes of any partition that can safely
 * be removed due to 'ne_clauses' containing not-equal clauses for all
 * possible values that the partition can contain.
 */
static Bitmapset *
get_partitions_excluded_by_ne_clauses(PartitionPruneContext *context,
									  List *ne_clauses)
{
	ListCell	   *lc;
	Bitmapset	   *excluded_parts;
	Bitmapset	   *foundoffsets = NULL;
	Relation		relation = context->relation;
	PartitionKey	partkey = RelationGetPartitionKey(relation);
	PartitionDesc	partdesc = RelationGetPartitionDesc(relation);
	PartitionBoundInfo boundinfo = partdesc->boundinfo;
	int			   *datums_in_part;
	int			   *datums_found;
	int				i;

	Assert(partkey->strategy == PARTITION_STRATEGY_LIST);
	Assert(partkey->partnatts == 1);

	/*
	 * Build a Bitmapset to record the indexes of all datums of the
	 * query that are found in boundinfo.
	 */
	foreach(lc, ne_clauses)
	{
		PartClause *pc = (PartClause *) lfirst(lc);
		Datum	datum;

		if (partkey_datum_from_expr(partkey, 0, pc->value, &datum))
		{
			int		offset;
			bool	is_equal;

			offset = partition_list_bsearch(partkey, boundinfo, datum,
											&is_equal);
			if (offset >= 0 && is_equal)
			{
				Assert(boundinfo->indexes[offset] >= 0);
				foundoffsets = bms_add_member(foundoffsets, offset);
			}
		}
	}

	/* No partitions can be excluded if none of the datums were found. */
	if (bms_is_empty(foundoffsets))
		return NULL;

	/*
	 * Since each list partition can permit multiple values, we must ensure
	 * that we got clauses for all those values before we can eliminate the
	 * the entire partition.
	 *
	 * We'll need two arrays for this, one to count the number of unique
	 * datums found in the query which belong to each partition, and another
	 * to record the number of datums permitted in each partition.  Once we've
	 * counted all this, we can eliminate any partition where the number of
	 * datums found matches the number of datums allowed in the partition.
	 */
	datums_in_part = (int *) palloc0(sizeof(int) * partdesc->nparts);
	datums_found = (int *) palloc0(sizeof(int) * partdesc->nparts);

	i = -1;
	while ((i = bms_next_member(foundoffsets, i)) >= 0)
		datums_found[boundinfo->indexes[i]]++;

	/*
	 * Now, in a single pass over all the datums, count the number of datums
	 * permitted in each partition.
	 */
	for (i = 0; i < boundinfo->ndatums; i++)
		datums_in_part[boundinfo->indexes[i]]++;

	/*
	 * Now compare the counts and eliminate any partition for which we found
	 * clauses for all its permitted values.  We must be careful here not to
	 * eliminate the default partition.  We can recognize that by it having a
	 * zero value in both arrays.
	 */
	excluded_parts = NULL;

	for (i = 0; i < partdesc->nparts; i++)
	{
		if (datums_found[i] >= datums_in_part[i] && datums_found[i] > 0)
			excluded_parts = bms_add_member(excluded_parts, i);
	}

	/*
	 * Because the above clauses are strict, we can also exclude the NULL
	 * partition, provided it does not also allow non-NULL values.
	 */
	if (partition_bound_accepts_nulls(boundinfo) &&
		datums_in_part[boundinfo->null_index] == 0)
		excluded_parts = bms_add_member(excluded_parts,
										boundinfo->null_index);

	pfree(datums_in_part);
	pfree(datums_found);

	return excluded_parts;
}

/*
 * get_partitions_from_or_clause_args
 *
 * Returns the set of partitions of relation, each of which satisfies some
 * clause in or_clause_args.
 */
static Bitmapset *
get_partitions_from_or_clause_args(PartitionPruneContext *context,
								   List *or_clause_args)
{
	PartitionKey	partkey = RelationGetPartitionKey(context->relation);
	Bitmapset	   *result = NULL;
	ListCell	   *lc;

	/*
	 * When matching an OR expression, it is only checked if at least one of
	 * its args matches the partition key, not all.  For arguments that don't
	 * match, we cannot eliminate any of its partitions using
	 * get_partitions_from_clauses().  However, if the table is itself a
	 * partition, we may be able to prove using constraint exclusion that the
	 * clause refutes its partition constraint, that is, we can eliminate all
	 * of its partitions.
	 */
	foreach(lc, or_clause_args)
	{
		List *clauses = list_make1(lfirst(lc));
		PartitionClauseInfo *partclauseinfo;
		PartitionPruneContext subcontext;
		Bitmapset *arg_partset;

		partclauseinfo = extract_partition_key_clauses(partkey, clauses,
													   context->rt_index);

		if (!partclauseinfo->foundkeyclauses)
		{
			List *partconstr = RelationGetPartitionQual(context->relation);
			PartitionDesc partdesc;

			if (partconstr)
			{
				partconstr = (List *) expression_planner((Expr *) partconstr);
				if (context->rt_index != 1)
					ChangeVarNodes((Node *) partconstr, 1, context->rt_index,
								   0);
				if (predicate_refuted_by(partconstr, clauses, false))
					continue;
			}

			/* Couldn't eliminate any of the partitions. */
			partdesc = RelationGetPartitionDesc(context->relation);
			return bms_add_range(NULL, 0, partdesc->nparts - 1);
		}

		subcontext.rt_index = context->rt_index;
		subcontext.relation = context->relation;
		subcontext.clauseinfo = partclauseinfo;
		arg_partset = get_partitions_from_clauses(&subcontext);

		result = bms_add_members(result, arg_partset);
		bms_free(arg_partset);
	}

	return result;
}

/* Match partition key (partattno/partexpr) to an expression (expr). */
#define EXPR_MATCHES_PARTKEY(expr, partattno, partexpr) \
		((partattno) != 0 ? \
		 (IsA((expr), Var) && \
		 ((Var *) (expr))->varattno == (partattno)) : \
		 equal((expr), (partexpr)))

#define COLLATION_MATCH(partcoll, exprcoll) \
	(!OidIsValid(partcoll) || (partcoll) == (exprcoll))

/*
 * extract_partition_key_clauses
 *		Processes 'clauses' to extract clause matching the partition key.
 *		This adds matched clauses to the list corresponding to particular key
 *		in 'partclauseinfo'.  Also collects other useful clauses to assist
 *		in partition elimination, such as OR clauses, clauses containing <>
 *		operator, and IS [NOT] NULL clauses
 *
 * We may also discover some contradiction in the clauses which means that no
 * partition can possibly match.  In this case, the function sets
 * partclauseinfo's 'constfalse' to true and exits immediately without
 * processing any further clauses.  In this case, the caller must be careful
 * not to assume the PartitionClauseInfo is fully populated with all clauses.
 */
static PartitionClauseInfo *
extract_partition_key_clauses(PartitionKey partkey, List *clauses,
							  int rt_index)
{
	PartitionClauseInfo *partclauseinfo = makeNode(PartitionClauseInfo);
	int			i;
	ListCell   *lc;

	memset(partclauseinfo->keyclauses, 0, sizeof(partclauseinfo->keyclauses));
	partclauseinfo->or_clauses = NIL;
	partclauseinfo->ne_clauses = NIL;
	partclauseinfo->keyisnull = NULL;
	partclauseinfo->keyisnotnull = NULL;
	partclauseinfo->constfalse = false;
	partclauseinfo->foundkeyclauses = false;

	foreach(lc, clauses)
	{
		Expr	   *clause = (Expr *) lfirst(lc);
		ListCell   *partexprs_item;

		if (IsA(clause, RestrictInfo))
		{
			RestrictInfo *rinfo = (RestrictInfo *) clause;

			clause = rinfo->clause;
			if (rinfo->pseudoconstant &&
				!DatumGetBool(((Const *) clause)->constvalue))
			{
				partclauseinfo->constfalse = true;
				return partclauseinfo;
			}
		}

		/* Get the BoolExpr's out of the way.*/
		if (IsA(clause, BoolExpr))
		{
			if (or_clause((Node *) clause))
			{
				partclauseinfo->or_clauses =
										lappend(partclauseinfo->or_clauses,
												((BoolExpr *) clause)->args);
				continue;
			}
			else if (and_clause((Node *) clause))
			{
				clauses = list_concat(clauses,
									  list_copy(((BoolExpr *) clause)->args));
				continue;
			}
			/* Fall-through for a NOT clause, which is handled below. */
		}

		partexprs_item = list_head(partkey->partexprs);
		for (i = 0; i < partkey->partnatts; i++)
		{
			PartClause *pc;
			Oid			partopfamily = partkey->partopfamily[i];
			Oid			partcoll = partkey->partcollation[i];
			Oid			commutator = InvalidOid;
			AttrNumber	partattno = partkey->partattrs[i];
			Expr	   *partexpr = NULL;

			/*
			 * A zero attno means the partition key is an expression, so grab
			 * the next expression from the list.
			 */
			if (partattno == 0)
			{
				if (partexprs_item == NULL)
					elog(ERROR, "wrong number of partition key expressions");

				partexpr = (Expr *) lfirst(partexprs_item);

				/*
				 * Expressions stored for the PartitionKey in the relcache are
				 * all stored with the dummy varno of 1. Change that to what
				 * we need.
				 */
				if (rt_index != 1)
				{
					/* make a copy so as not to overwrite the relcache */
					partexpr = (Expr *) copyObject(partexpr);
					ChangeVarNodes((Node *) partexpr, 1, rt_index, 0);
				}

				partexprs_item = lnext(partexprs_item);
			}

			if (IsA(clause, OpExpr))
			{
				OpExpr	   *opclause = (OpExpr *) clause;
				Expr	   *leftop,
						   *rightop,
						   *valueexpr;
				bool		is_ne_listp = false;

				leftop = (Expr *) get_leftop(clause);
				if (IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;
				rightop = (Expr *) get_rightop(clause);
				if (IsA(rightop, RelabelType))
					rightop = ((RelabelType *) rightop)->arg;

				/* check if the clause matches this partition key */
				if (EXPR_MATCHES_PARTKEY(leftop, partattno, partexpr))
					valueexpr = rightop;
				else if (EXPR_MATCHES_PARTKEY(rightop, partattno, partexpr))
				{
					valueexpr = leftop;

					commutator = get_commutator(opclause->opno);

					/* nothing we can do unless we can swap the operands */
					if (!OidIsValid(commutator))
						continue;
				}
				else
					/* Clause does not match this partition key. */
					continue;

				/*
				 * Also, useless, if the clause's collation is different from
				 * the partitioning collation.
				 */
				if (!COLLATION_MATCH(partcoll, opclause->inputcollid))
					continue;

				/*
				 * Only allow strict operators.  This will guarantee nulls are
				 * filtered.
				 */
				if (!op_strict(opclause->opno))
					continue;

				/* We can't use any volatile value to prune partitions. */
				if (contain_volatile_functions((Node *) valueexpr))
					continue;

				/*
				 * Handle cases where the clause's operator does not belong to
				 * the partitioning operator family.  We currently handle two
				 * such cases: 1. Operators named '<>' are not listed in any
				 * operator family whatsoever, 2.  Ordering operators like '<'
				 * are not listed in the hash operator families.  For 1, check
				 * if list partitioning is in use and if so, proceed to pass
				 * the clause to the caller without doing any more processing
				 * ourselves.  2 cannot be handled at all, so the clause is
				 * simply skipped.
				 */
				if (!op_in_opfamily(opclause->opno, partopfamily))
				{
					Oid		negator;

					/*
					 * To confirm if the operator is really '<>', check if its
					 * negator is a equality operator.  If it's a btree
					 * equality operator *and* this is a list partitioned
					 * table, we can use it prune partitions.
					 */
					negator = get_negator(opclause->opno);
					if (OidIsValid(negator) &&
						op_in_opfamily(negator, partopfamily))
					{
						Oid		lefttype;
						Oid		righttype;
						int		strategy;

						get_op_opfamily_properties(negator, partopfamily,
												   false,
												   &strategy,
												   &lefttype, &righttype);

						if (strategy == BTEqualStrategyNumber &&
							partkey->strategy == PARTITION_STRATEGY_LIST)
							is_ne_listp = true;
					}

					/* Cannot handle this clause. */
					if (!is_ne_listp)
						continue;
				}

				pc = (PartClause *) palloc0(sizeof(PartClause));
				pc->opno = OidIsValid(commutator) ? commutator : opclause->opno;
				pc->inputcollid = opclause->inputcollid;
				pc->value = valueexpr;

				/*
				 * We don't turn a <> operator clause into a key right away.
				 * Instead, the caller will hand over such clauses to
				 * get_partitions_excluded_by_ne_clauses().
				 */
				if (is_ne_listp)
					partclauseinfo->ne_clauses =
										lappend(partclauseinfo->ne_clauses,
												pc);
				else
					partclauseinfo->keyclauses[i] =
										lappend(partclauseinfo->keyclauses[i],
												pc);

				/*
				 * Since we only allow strict operators, check for any
				 * contradicting IS NULLs.
				 */
				if (bms_is_member(i, partclauseinfo->keyisnull))
				{
					partclauseinfo->constfalse = true;
					return partclauseinfo;
				}
				/* Record that a strict clause has been seen for this key */
				partclauseinfo->keyisnotnull =
								bms_add_member(partclauseinfo->keyisnotnull,
											   i);
				partclauseinfo->foundkeyclauses = true;
			}
			else if (IsA(clause, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
				Oid		saop_op = saop->opno;
				Oid		saop_coll = saop->inputcollid;
				Expr   *leftop = (Expr *) linitial(saop->args),
					   *rightop = (Expr *) lsecond(saop->args);
				List   *elem_exprs,
					   *elem_clauses;
				ListCell *lc1;

				if (IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;

				/* Check it matches this partition key */
				if (!EXPR_MATCHES_PARTKEY(leftop, partattno, partexpr))
					continue;

				/*
				 * Also, useless, if the clause's collation is different from
				 * the partitioning collation.
				 */
				if (!COLLATION_MATCH(partcoll, saop->inputcollid))
					continue;

				/*
				 * Only allow strict operators.  This will guarantee null are
				 * filtered.
				 */
				if (!op_strict(saop->opno))
					continue;

				/* Useless if the array has any volatile functions. */
				if (contain_volatile_functions((Node *) rightop))
					continue;

				/*
				 * In case of NOT IN (..), we get a '<>', which while not
				 * listed as part of any operator family, we are able to
				 * handle it if its negator is indeed a part of the
				 * partitioning equality operator.
				 */
				if (!op_in_opfamily(saop_op, partopfamily))
				{
					Oid		negator = get_negator(saop_op);
					int		strategy;
					Oid		lefttype,
							righttype;

					if (!OidIsValid(negator))
						continue;
					get_op_opfamily_properties(negator, partopfamily, false,
											   &strategy,
											   &lefttype, &righttype);
					if (strategy != BTEqualStrategyNumber)
						continue;
				}

				/*
				 * First generate a list of Const nodes, one for each array
				 * element.
				 */
				elem_exprs = NIL;
				if (IsA(rightop, Const))
				{
					Const *arr = (Const *) lsecond(saop->args);
					ArrayType *arrval = DatumGetArrayTypeP(arr->constvalue);
					int16	elemlen;
					bool	elembyval;
					char	elemalign;
					Datum  *elem_values;
					bool   *elem_nulls;
					int		num_elems;

					get_typlenbyvalalign(ARR_ELEMTYPE(arrval),
										 &elemlen, &elembyval, &elemalign);
					deconstruct_array(arrval,
									  ARR_ELEMTYPE(arrval),
									  elemlen, elembyval, elemalign,
									  &elem_values, &elem_nulls,
									  &num_elems);
					for (i = 0; i < num_elems; i++)
					{
						/* Only consider non-null values. */
						if (!elem_nulls[i])
						{
							Const *elem_expr = makeConst(ARR_ELEMTYPE(arrval),
														 -1, arr->constcollid,
														 elemlen,
														 elem_values[i],
														 false, elembyval);

							elem_exprs = lappend(elem_exprs, elem_expr);
						}
					}
				}
				else
				{
					ArrayExpr *arrexpr = castNode(ArrayExpr, rightop);

					/*
					 * For a nested ArrayExpr, we don't know how to get the
					 * actual scalar values out into a flat list, so we give
					 * up doing anything with this ScalarArrayOpExpr.
					 */
					if (arrexpr->multidims)
						continue;

					elem_exprs = arrexpr->elements;
				}

				/*
				 * Now generate a list of clauses, one for each array element,
				 * of the form: saop_leftop saop_op elem_expr
				 */
				elem_clauses = NIL;
				foreach(lc1, elem_exprs)
				{
					Expr   *rightop = (Expr *) lfirst(lc1),
						   *elem_clause;

					elem_clause = (Expr *) make_opclause(saop_op, BOOLOID,
														 false,
														 leftop, rightop,
														 InvalidOid,
														 saop_coll);
					elem_clauses = lappend(elem_clauses, elem_clause);
				}

				/*
				 * Build the OR clause if needed or add the clauses to the end
				 * of the list that's being processed currently.
				 */
				if (saop->useOr && list_length(elem_clauses) > 1)
					partclauseinfo->or_clauses =
										lappend(partclauseinfo->or_clauses,
												elem_clauses);
				else
					clauses = list_concat(clauses, elem_clauses);
				partclauseinfo->foundkeyclauses = true;
			}
			else if (IsA(clause, NullTest))
			{
				NullTest *nulltest = (NullTest *) clause;
				Expr *arg = nulltest->arg;

				if (IsA(arg, RelabelType))
					arg = ((RelabelType *) arg)->arg;

				/* Does leftop match with this partition key column? */
				if (EXPR_MATCHES_PARTKEY(arg, partattno, partexpr))
				{
					if (nulltest->nulltesttype == IS_NULL)
					{
						/* check for conflicting IS NOT NULLs */
						if (bms_is_member(i, partclauseinfo->keyisnotnull))
						{
							partclauseinfo->constfalse = true;
							return partclauseinfo;
						}
						partclauseinfo->keyisnull =
									bms_add_member(partclauseinfo->keyisnull,
												   i);
					}
					else
					{
						/* check for conflicting IS NULLs */
						if (bms_is_member(i, partclauseinfo->keyisnull))
						{
							partclauseinfo->constfalse = true;
							return partclauseinfo;
						}

						partclauseinfo->keyisnotnull =
								bms_add_member(partclauseinfo->keyisnotnull,
											   i);
					}
					partclauseinfo->foundkeyclauses = true;
				}
			}
			/*
			 * Boolean clauses have a special shape, which would've been
			 * accepted if the partitioning opfamily accepts Boolean
			 * conditions.
			 */
			else if (IsBooleanOpfamily(partopfamily) &&
					 (IsA(clause, BooleanTest) ||
					  IsA(clause, Var) ||
					  not_clause((Node *) clause)))
			{
				Expr   *leftop,
					   *rightop;

				if (IsA(clause, BooleanTest))
				{
					BooleanTest *btest = (BooleanTest *) clause;

					/* Only IS [NOT] TRUE/FALSE are any good to us */
					if (btest->booltesttype == IS_UNKNOWN ||
						btest->booltesttype == IS_NOT_UNKNOWN)
						continue;

					leftop = btest->arg;
					if (IsA(leftop, RelabelType))
						leftop = ((RelabelType *) leftop)->arg;

					/* Clause does not match this partition key. */
					if (!EXPR_MATCHES_PARTKEY(leftop, partattno, partexpr))
						continue;

					rightop = (btest->booltesttype == IS_TRUE ||
							   btest->booltesttype == IS_NOT_FALSE)
									? (Expr *) makeBoolConst(true, false)
									: (Expr *) makeBoolConst(false, false);
				}
				else
				{
					leftop = IsA(clause, Var)
								? (Expr *) clause
								: (Expr *) get_notclausearg((Expr *) clause);
					if (IsA(leftop, RelabelType))
						leftop = ((RelabelType *) leftop)->arg;

					/* Clause does not match this partition key. */
					if (!EXPR_MATCHES_PARTKEY(leftop, partattno, partexpr))
						continue;

					rightop = IsA(clause, Var)
									? (Expr *) makeBoolConst(true, false)
									: (Expr *) makeBoolConst(false, false);
				}

				pc = (PartClause *) palloc0(sizeof(PartClause));
				pc->opno = BooleanEqualOperator;
				pc->inputcollid = InvalidOid;
				pc->value = rightop;

				partclauseinfo->keyclauses[i] =
										lappend(partclauseinfo->keyclauses[i],
												pc);
				partclauseinfo->foundkeyclauses = true;
			}
		}
	}

	return partclauseinfo;
}

/*
 * extract_bounding_datums
 *		Process clauses in context->clauseinfo and populate 'keys' with all
 *		min/max/equal values that we're able to determine.
 *
 * For RANGE partitioning we do not need to match and find values for all
 * partition keys.  We may be able to eliminate some partitions with just a
 * prefix of the partition keys.  HASH partitioning does require all keys are
 * matched to with at least some combinations of equality clauses and IS NULL
 * clauses. LIST partitions don't support multiple partition keys.
 *
 * Returns true if at least one key was found; false otherwise.
 */
static bool
extract_bounding_datums(PartitionKey partkey, PartitionPruneContext *context,
						List **minimalclauses, PartScanKeyInfo *keys)
{
	PartitionClauseInfo *clauseinfo = context->clauseinfo;
	bool		need_next_eq,
				need_next_min,
				need_next_max;
	int 		i;
	ListCell   *lc;

	/*
	 * Based on the strategies of the clauses' operators (=, </<=, >/>=), try
	 * to construct a tuple of those datums that serve as the exact lookup
	 * tuple or two tuples that serve as minimum and maximum bound.
	 *
	 * If we find datums for all partition key columns that appear in =
	 * operator clauses, then we have the exact match lookup tuple, which will
	 * be used to match just one partition (although that's required only for
	 * range partitioning, finding datums for just some columns is fine for
	 * hash partitioning).
	 *
	 * If the last datum in a tuple comes from a clause containing </<= or
	 * >/>= operator, then that constitutes the minimum or maximum bound tuple,
	 * respectively.  There is one exception -- if we have a tuple containing
	 * values for only a prefix of partition key columns, where none of its
	 * values come from a </<= or >/>= operator clause, we still consider such
	 * tuple as both minimum and maximum bound tuple.
	 */
	need_next_eq = true;
	need_next_min = true;
	need_next_max = true;
	memset(keys, 0, sizeof(PartScanKeyInfo));
	for (i = 0; i < partkey->partnatts; i++)
	{
		List *clauselist = minimalclauses[i];

		/*
		 * Min and max keys must constitute a prefix of the partition key and
		 * must appear in the same order as partition keys.  Equal keys have
		 * to satisfy that requirement only for non-hash partitioning.
		 */
		if (i > keys->n_eqkeys &&
			partkey->strategy != PARTITION_STRATEGY_HASH)
			need_next_eq = false;

		if (i > keys->n_minkeys)
			need_next_min = false;

		if (i > keys->n_maxkeys)
			need_next_max = false;

		foreach(lc, clauselist)
		{
			PartClause *clause = (PartClause *) lfirst(lc);
			Expr *value = clause->value;
			bool incl;
			PartOpStrategy	op_strategy;

			op_strategy = partition_op_strategy(partkey, clause, &incl);
			switch (op_strategy)
			{
				case PART_OP_EQUAL:
					Assert(incl);
					if (need_next_eq &&
						partkey_datum_from_expr(partkey, i, value,
												&keys->eqkeys[i]))
						keys->n_eqkeys++;

					if (need_next_max &&
						partkey_datum_from_expr(partkey, i, value,
												&keys->maxkeys[i]))
					{
						keys->n_maxkeys++;
						keys->max_incl = true;
					}

					if (need_next_min &&
						partkey_datum_from_expr(partkey, i, value,
												&keys->minkeys[i]))
					{
						keys->n_minkeys++;
						keys->min_incl = true;
					}
					break;

				case PART_OP_LESS:
					if (need_next_max &&
						partkey_datum_from_expr(partkey, i, value,
												&keys->maxkeys[i]))
					{
						keys->n_maxkeys++;
						keys->max_incl = incl;
						if (!incl)
							need_next_eq = need_next_max = false;
					}
					break;

				case PART_OP_GREATER:
					if (need_next_min &&
						partkey_datum_from_expr(partkey, i, value,
												&keys->minkeys[i]))
					{
						keys->n_minkeys++;
						keys->min_incl = incl;
						if (!incl)
							need_next_eq = need_next_min = false;
					}
					break;

				default:
					Assert(false);
			}
		}
	}

	/*
	 * To set eqkeys, we must have found matching clauses containing =
	 * operator for all partition key columns and if present we don't need
	 * the values in minkeys and maxkeys anymore.  In the case hash
	 * partitioning, we don't require all of eqkeys to be operator clausses.
	 * In that case, any IS NULL clauses involving partition key columns are
	 * also considered as equality keys by the code for hash partition pruning,
	 * which checks that all partition columns are covered before actually
	 * performing the pruning.
	 */
	if (keys->n_eqkeys == partkey->partnatts ||
		partkey->strategy == PARTITION_STRATEGY_HASH)
		keys->n_minkeys = keys->n_maxkeys = 0;
	else
		keys->n_eqkeys = 0;

	/* Finally, also set the keyisnull and keyisnotnull values. */
	keys->keyisnull = clauseinfo->keyisnull;
	keys->keyisnotnull = clauseinfo->keyisnotnull;

	return (keys->n_eqkeys > 0 || keys->n_minkeys > 0 ||
			keys->n_maxkeys > 0 || !bms_is_empty(keys->keyisnull) ||
			!bms_is_empty(keys->keyisnotnull));
}

/*
 * partition_op_strategy
 *		Returns whether the clause in 'pc' contains an =, </<=, or >/>=
 *		operator and set *incl to true if the operator's strategy is
 *		inclusive.
 */
static PartOpStrategy
partition_op_strategy(PartitionKey key, PartClause *pc, bool *incl)
{
	*incl = false;	/* may be overwritten below */

	switch (key->strategy)
	{
		/* Hash partitioning allows only hash equality. */
		case PARTITION_STRATEGY_HASH:
			if (pc->op_strategy == HTEqualStrategyNumber)
			{
				*incl = true;
				return PART_OP_EQUAL;
			}
			elog(ERROR, "unexpected operator strategy number: %d",
				 pc->op_strategy);

		/* List and range partitioning support all btree operators. */
		case PARTITION_STRATEGY_LIST:
		case PARTITION_STRATEGY_RANGE:
			switch (pc->op_strategy)
			{
				case BTLessEqualStrategyNumber:
					*incl = true;
					/* fall through */

				case BTLessStrategyNumber:
					return PART_OP_LESS;

				case BTEqualStrategyNumber:
					*incl = true;
					return PART_OP_EQUAL;

				case BTGreaterEqualStrategyNumber:
					*incl = true;
					/* fall through */

				case BTGreaterStrategyNumber:
					return PART_OP_GREATER;
			}

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	return PART_OP_EQUAL;		/* keep compiler quiet */
}

/*
 * partkey_datum_from_expr
 *		Set *value to the constant value obtained by evaluating 'expr'
 *
 * Note that we may not be able to evaluate the input expression, in which
 * case, the function returns false to indicate that *value has not been
 * set.  True is returned otherwise.
 */
static bool
partkey_datum_from_expr(PartitionKey key, int partkeyidx,
						Expr *expr, Datum *value)
{
	Oid		exprtype = exprType((Node *) expr);

	if (exprtype != key->parttypid[partkeyidx])
	{
		ParseState *pstate = make_parsestate(NULL);

		expr = (Expr *) coerce_to_target_type(pstate, (Node *) expr,
									 exprtype,
									 key->parttypid[partkeyidx], -1,
									 COERCION_EXPLICIT,
									 COERCE_IMPLICIT_CAST, -1);
		free_parsestate(pstate);

		/*
		 * If we couldn't coerce to the partition key's type, that is, the
		 * type of the datums stored in PartitionBoundInfo for this partition
		 * key, there's no hope of using this expression for anything
		 * partitioning-related.
		 */
		if (expr == NULL)
			return false;

		/*
		 * Transform into a form that the following code can do something
		 * useful with.
		 */
		expr = evaluate_expr(expr,
							 exprType((Node *) expr),
							 exprTypmod((Node *) expr),
							 exprCollation((Node *) expr));
	}

	/*
	 * Add more expression types here as needed to support higher-level
	 * code.
	 */
	if (IsA(expr, Const))
	{
		*value = ((Const *) expr)->constvalue;
		return true;
	}

	return false;
}

/*
 * remove_redundant_clauses
 *		Processes the clauses contained in context->clauseinfo to remove the
 *		ones that are superseeded by other clauses which are more restrictive.
 *
 * For example, x > 1 AND x > 2 and x >= 5, the latter is the most
 * restrictive, so 5 is the best minimum bound for x.
 *
 * We also look for clauses which contradict one another in a way that proves
 * that the clauses cannot possibly match any partition.  Impossible clauses
 * include things like: x = 1 AND x = 2, x > 0 and x < 10.  The function
 * returns right after finding such a clause and before returning, sets a field
 * in context->clauseinfo to inform the caller that we found such clause.
 */
static void
remove_redundant_clauses(PartitionKey partkey,
						 PartitionPruneContext *context,
						 List **minimalclauses)
{
	PartClause *hash_clause,
			   *btree_clauses[BTMaxStrategyNumber];
	PartitionClauseInfo *partclauseinfo = context->clauseinfo;
	ListCell *lc;
	int		s;
	int		i;
	bool	test_result;

	for (i = 0; i < partkey->partnatts; i++)
	{
		List *keyclauses = partclauseinfo->keyclauses[i];

		minimalclauses[i] = NIL;
		hash_clause = NULL;

		memset(btree_clauses, 0, sizeof(btree_clauses));

		foreach(lc, keyclauses)
		{
			PartClause *pc = (PartClause *) lfirst(lc);

			if (!pc->valid_cache)
			{
				Oid		lefttype;

				get_op_opfamily_properties(pc->opno,
										   partkey->partopfamily[i],
										   false,
										   &pc->op_strategy,
										   &lefttype,
										   &pc->op_subtype);
				fmgr_info(get_opcode(pc->opno), &pc->op_func);
				pc->valid_cache = true;
			}

			/*
			 * Hash-partitioning knows only about equality.  So, if we've
			 * matched a clause and found another clause whose constant
			 * operand doesn't match the constant operand of the former, then
			 * we have found mutually contradictory clauses.
			 */
			if (partkey->strategy == PARTITION_STRATEGY_HASH)
			{
				if (hash_clause == NULL)
					hash_clause = pc;
				/* check if another clause would contradict the one we have */
				else if (partition_cmp_args(partkey, i,
											pc, pc, hash_clause,
											&test_result))
				{
					if (!test_result)
					{
						partclauseinfo->constfalse = true;
						return;
					}
				}
				/*
				 * Couldn't compare; keep hash_clause set to the previous value,
				 * and add this one directly to the result.  Caller would
				 * arbitrarily choose one of the many and perform
				 * partition-pruning with it.
				 */
				else
					minimalclauses[i] = lappend(minimalclauses[i], pc);

				/*
				 * The code below handles btree operators, so not relevant for
				 * hash partitioning.
				 */
				continue;
			}

			/*
			 * The code that follows closely mimics similar processing done by
			 * nbtutils.c: _bt_preprocess_keys().
			 *
			 * btree_clauses[s] points currently best clause containing the
			 * operator strategy type s+1; it is NULL if we haven't yet found
			 * such a clause.
			 */
			s = pc->op_strategy - 1;
			if (btree_clauses[s] == NULL)
			{
				btree_clauses[s] = pc;
			}
			else
			{
				/*
				 * Is this one more restrictive than what we already have?
				 *
				 * Consider some examples: 1. If btree_clauses[BTLT] now contains
				 * a < 5, and cur is a < 3, then because 3 < 5 is true, a < 5
				 * currently at btree_clauses[BTLT] will be replaced by a < 3.
				 *
				 * 2. If btree_clauses[BTEQ] now contains a = 5 and cur is a = 7,
				 * then because 5 = 7 is false, we found a mutual contradiction,
				 * so we set *constfalse to true and return.
				 *
				 * 3. If btree_clauses[BTLT] now contains a < 5 and cur is a < 7,
				 * then because 7 < 5 is false, we leave a < 5 where it is and
				 * effectively discard a < 7 as being redundant.
				 */
				if (partition_cmp_args(partkey, i,
									   pc, pc, btree_clauses[s],
									   &test_result))
				{
					/* cur is more restrictive, so replace the existing. */
					if (test_result)
						btree_clauses[s] = pc;
					else if (s == BTEqualStrategyNumber - 1)
					{
						partclauseinfo->constfalse = true;
						return;
					}

					/* Old one is more restrictive, so keep around. */
				}
				else
				{
					/*
					 * We couldn't determine which one is more restrictive.  Keep
					 * the previous one in btree_clauses[s] and push this one directly
					 * to the output list.
					 */
					minimalclauses[i] = lappend(minimalclauses[i], pc);
				}
			}
		}

		if (partkey->strategy == PARTITION_STRATEGY_HASH)
		{
			/* Note we didn't add this one to the result yet. */
			if (hash_clause)
				minimalclauses[i] = lappend(minimalclauses[i], hash_clause);
			continue;
		}

		/* Compare btree operator clauses across strategies. */

		/* Compare the equality clause with clauses of other strategies. */
		if (btree_clauses[BTEqualStrategyNumber - 1])
		{
			PartClause *eq = btree_clauses[BTEqualStrategyNumber - 1];

			for (s = 0; s < BTMaxStrategyNumber; s++)
			{
				PartClause *chk = btree_clauses[s];

				if (!chk || s == (BTEqualStrategyNumber - 1))
					continue;

				/*
				 * Suppose btree_clauses[BTLT] contained a < 5 and the eq clause
				 * is a = 5, then because 5 < 5 is false, we found contradiction.
				 * That is, a < 5 and a = 5 are mutually contradictory.  OTOH, if
				 * eq clause is a = 3, then because 3 < 5, we no longer need
				 * a < 5, because a = 3 is more restrictive.
				 */
				if (partition_cmp_args(partkey, i,
									   chk, eq, chk,
									   &test_result))
				{
					if (!test_result)
					{
						partclauseinfo->constfalse = true;
						return;
					}
					/* Discard the no longer needed clause. */
					btree_clauses[s] = NULL;
				}
			}
		}

		/*
		 * Try to keep only one of <, <=.
		 *
		 * Suppose btree_clauses[BTLT] contains a < 3 and btree_clauses[BTLE]
		 * contains a <= 3 (or a <= 4), then because 3 <= 3 (or 3 <= 4) is true,
		 * we discard the a <= 3 (or a <= 4) as redundant.  If the latter contains
		 * contains a <= 2, then because 3 <= 2 is false, we dicard a < 3 as
		 * redundant.
		 */
		if (btree_clauses[BTLessStrategyNumber - 1] &&
			btree_clauses[BTLessEqualStrategyNumber - 1])
		{
			PartClause *lt = btree_clauses[BTLessStrategyNumber - 1],
					   *le = btree_clauses[BTLessEqualStrategyNumber - 1];

			if (partition_cmp_args(partkey, i,
								   le, lt, le,
								   &test_result))
			{
				if (test_result)
					btree_clauses[BTLessEqualStrategyNumber - 1] = NULL;
				else
					btree_clauses[BTLessStrategyNumber - 1] = NULL;
			}
		}

		/* Try to keep only one of >, >=.  See the example above. */
		if (btree_clauses[BTGreaterStrategyNumber - 1] &&
			btree_clauses[BTGreaterEqualStrategyNumber - 1])
		{
			PartClause *gt = btree_clauses[BTGreaterStrategyNumber - 1],
					   *ge = btree_clauses[BTGreaterEqualStrategyNumber - 1];

			if (partition_cmp_args(partkey, i,
								   ge, gt, ge,
								   &test_result))
			{
				if (test_result)
					btree_clauses[BTGreaterEqualStrategyNumber - 1] = NULL;
				else
					btree_clauses[BTGreaterStrategyNumber - 1] = NULL;
			}
		}

		/*
		 * btree_clauses now contains the "best" clause or NULL for each btree
		 * strategy number.  Add to the newlist.
		 */
		for (s = 0; s < BTMaxStrategyNumber; s++)
		{
			if (btree_clauses[s])
				minimalclauses[i] = lappend(minimalclauses[i],
											btree_clauses[s]);
		}
	}
}

/*
 * partition_cmp_args
 *		Try to compare the constant arguments of 'leftarg' and 'rightarg', in
 *		that order, using the operator of 'op' and set *result to the result
 *		of this comparison.
 *
 * Returns true if we could actually perform the comparison; otherwise false.
 *
 * Note: We may not be able to perform the comparison if operand values are
 * unknown in this context or if the type of any of the operands are
 * incompatible with the operator.
 */
static bool
partition_cmp_args(PartitionKey key, int partkeyidx,
				   PartClause *pc, PartClause *leftarg, PartClause *rightarg,
				   bool *result)
{
	Datum	left_value;
	Datum	right_value;

	Assert(pc->valid_cache && leftarg->valid_cache && rightarg->valid_cache);

	/*
	 * Try to extract an actual value from each arg.  This may fail if the
	 * value is unknown in this context, in which case we cannot compare.
	 */
	if (!partkey_datum_from_expr(key, partkeyidx,
								 leftarg->value, &left_value))
		return false;

	if (!partkey_datum_from_expr(key, partkeyidx,
								 rightarg->value, &right_value))
		return false;

	/*
	 * We can compare left_value and right_value using op's operator
	 * only if both are of the expected type.
	 */
	if (leftarg->op_subtype == pc->op_subtype &&
		rightarg->op_subtype == pc->op_subtype)
	{
		*result = DatumGetBool(FunctionCall2Coll(&pc->op_func,
												 pc->inputcollid,
												 left_value,
												 right_value));
		return true;
	}
	else
	{
		Oid		partopfamily = key->partopfamily[partkeyidx];
		Oid		cmp_op;

		/* Otherwise, look one up in the partitioning operator family. */
		cmp_op = get_opfamily_member(partopfamily,
									 leftarg->op_subtype,
									 rightarg->op_subtype,
									 pc->op_strategy);
		if (OidIsValid(cmp_op))
		{
			*result = DatumGetBool(OidFunctionCall2Coll(get_opcode(cmp_op),
														pc->inputcollid,
														left_value,
														right_value));
			return true;
		}
	}

	/* Couldn't do the comparison. */
	*result = false;
	return false;
}

/*
 * get_partitions_for_keys
 *		Returns the partitions of 'rel' that will need to be scanned for the
 *		given look up keys
 *
 * Input:
 *	See the comments above the definition of PartScanKeyInfo to see what
 *	kind of information is contained in 'keys'.
 *
 * Outputs:
 *	Bitmapset containing indexes of the selceted partitions
 */
static Bitmapset *
get_partitions_for_keys(Relation rel, PartScanKeyInfo *keys)
{
	/* Return an empty set if no partitions to see. */
	if (RelationGetPartitionDesc(rel)->nparts == 0)
		return NULL;

	switch (RelationGetPartitionKey(rel)->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			return get_partitions_for_keys_hash(rel, keys);

		case PARTITION_STRATEGY_LIST:
			return get_partitions_for_keys_list(rel, keys);

		case PARTITION_STRATEGY_RANGE:
			return get_partitions_for_keys_range(rel, keys);

		default:
			elog(ERROR, "unexpected partition strategy: %d",
						RelationGetPartitionKey(rel)->strategy);
	}

	return NULL;	/* keep compiler quiet */
}

/*
 * get_partitions_for_keys_hash
 *		Return partitions of a hash partitioned table for requested
 *		keys
 *
 * This interprets the keys and looks up partitions in the partition bound
 * descriptor using the hash partitioning semantics.
 */
static Bitmapset *
get_partitions_for_keys_hash(Relation rel, PartScanKeyInfo *keys)
{
	PartitionKey	partkey = RelationGetPartitionKey(rel);
	PartitionDesc	partdesc = RelationGetPartitionDesc(rel);
	PartitionBoundInfo boundinfo = partdesc->boundinfo;
	bool			keyisnull[PARTITION_MAX_KEYS];
	int				i;

	Assert(partdesc->nparts > 0);

	/*
	 * Since tuples with NULL values in the partition key columns are stored
	 * in regular partitions, we'll treat any IS NULL clauses here as regular
	 * equality clauses.
	 */
	memset(keyisnull, false, sizeof(keyisnull));
	for (i = 0; i < partkey->partnatts; i++)
	{
		if (bms_is_member(i, keys->keyisnull))
		{
			keys->n_eqkeys++;
			keyisnull[i] = true;
		}
	}

	/*
	 * Can only do pruning if we know all the keys and they're all equality
	 * keys including the nulls that we just counted above.
	 */
	if (keys->n_eqkeys == partkey->partnatts)
	{
		uint64	rowHash;
		int 	greatest_modulus = get_greatest_modulus(boundinfo),
				result_index;

		rowHash = compute_hash_value(partkey, keys->eqkeys, keyisnull);
		result_index = boundinfo->indexes[rowHash % greatest_modulus];
		if (result_index >= 0)
			return bms_make_singleton(result_index);
	}
	else
		/* Can't do pruning otherwise, so return all partitions. */
		return bms_add_range(NULL, 0, partdesc->nparts - 1);

	return NULL;
}

/*
 * get_partitions_for_keys_list
 *		Return partitions of a list partitioned table for requested keys
 *
 * This interprets the keys and looks up partitions in the partition bound
 * descriptor using the list partitioning semantics.
 */
static Bitmapset *
get_partitions_for_keys_list(Relation rel, PartScanKeyInfo *keys)
{
	Bitmapset			   *result = NULL;
	PartitionKey			partkey = RelationGetPartitionKey(rel);
	PartitionBoundInfo		boundinfo = RelationGetPartitionDesc(rel)->boundinfo;
	int		i,
			eqoff,
			minoff,
			maxoff;
	bool	is_equal;

	Assert(RelationGetPartitionDesc(rel)->nparts > 0);
	Assert(partkey->partnatts == 1);

	/*
	 * If the query is looking for null keys, there can only be one such
	 * partition.  Return the same if one exists.
	 */
	if (!bms_is_empty(keys->keyisnull))
	{
		/*
		 * NULLs may only exist in the NULL partition, or in the
		 * default, if there's no NULL partition.
		 */
		if (partition_bound_accepts_nulls(boundinfo))
			return bms_make_singleton(boundinfo->null_index);
		else if (partition_bound_has_default(boundinfo))
			return bms_make_singleton(boundinfo->default_index);
		else
			return NULL;
	}

	/*
	 * If there are no datums to compare keys with, but there are partitions,
	 * just return the default partition if one exists.
	 */
	if (boundinfo->ndatums == 0)
	{
		if (partition_bound_has_default(boundinfo))
			return bms_make_singleton(boundinfo->default_index);
		else
			return NULL;	/* shouldn't happen */
	}

	/* Equality key. */
	if (keys->n_eqkeys > 0)
	{
		eqoff = partition_list_bsearch(partkey, boundinfo, keys->eqkeys[0],
									   &is_equal);
		if (eqoff >= 0 && is_equal)
		{
			/* Exactly matching datum exists. */
			Assert(boundinfo->indexes[eqoff] >= 0);
			return bms_make_singleton(boundinfo->indexes[eqoff]);
		}
		else if (partition_bound_has_default(boundinfo))
			return bms_make_singleton(boundinfo->default_index);
		else
			return NULL;
	}

	/*
	 * Find the left-most bound that satisfies the query, i.e., the one that
	 * satisfies minkeys.
	 */
	minoff = 0;
	if (keys->n_minkeys > 0)
	{
		minoff = partition_list_bsearch(partkey, boundinfo, keys->minkeys[0],
										&is_equal);
		if (minoff >= 0)
		{
			/*
			 * The bound at minoff is <= minkeys, given the way
			 * partition_bound_bsearch() works.  If it's not equal (<), then
			 * increment minoff to make it point to the datum on the right
			 * that necessarily satisfies minkeys.  Also do the same if it is
			 * equal but minkeys is exclusive.
			 */
			if (!is_equal || !keys->min_incl)
				minoff++;
		}
		else
		{
			/*
			 * minoff set to -1 means all datums are greater than minkeys,
			 * which means all partitions satisfy minkeys.  In that case, set
			 * minoff to the index of the leftmost datum, viz. 0.
			 */
			minoff = 0;
		}

		/*
		 * minkeys is greater than the datums of all non-default partitions,
		 * meaning there isn't one to return.  Return the default partition if
		 * one exists.
		 */
		if (minoff > boundinfo->ndatums - 1)
			return partition_bound_has_default(boundinfo)
						? bms_make_singleton(boundinfo->default_index)
						: NULL;
	}

	/*
	 * Find the right-most bound that satisfies the query, i.e., the one that
	 * satisfies maxkeys.
	 */
	maxoff = boundinfo->ndatums - 1;
	if (keys->n_maxkeys > 0)
	{
		maxoff = partition_list_bsearch(partkey, boundinfo, keys->maxkeys[0],
										&is_equal);
		if (maxoff >= 0)
		{
			/*
			 * The bound at maxoff is <= maxkeys, given the way
			 * partition_bound_bsearch works.  If the bound at maxoff exactly
			 * matches maxkey (is_equal), but the maxkey is exclusive, then
			 * decrement maxoff to point to the bound on the left.
			 */
			if (is_equal && !keys->max_incl)
				maxoff--;
		}

		/*
		 * maxkeys is smaller than the datums of all non-default partitions,
		 * meaning there isn't one to return.  Return the default partition if
		 * one exists.
		 */
		if (maxoff < 0)
			return partition_bound_has_default(boundinfo)
						? bms_make_singleton(boundinfo->default_index)
						: NULL;
	}

	Assert (minoff >= 0 && maxoff >= 0);

	/*
	 * All datums between those at minoff and maxoff satisfy query's keys, so
	 * add the corresponding partitions to the result set.
	 */
	for (i = minoff; i <= maxoff; i++)
		result = bms_add_member(result, boundinfo->indexes[i]);

	/*
	 * For range queries, always include the default list partition,
	 * because list partitions divide the key space in a discontinuous
	 * manner, not all values in the given range will have a partition
	 * assigned.
	 */
	if (partition_bound_has_default(boundinfo))
		return bms_add_member(result, boundinfo->default_index);

	return result;
}

/*
 * get_partitions_for_keys_range
 *		Return partitions of a range partitioned table for requested keys
 *
 * This interprets the keys and looks up partitions in the partition bound
 * descriptor using the range partitioning semantics.
 */
static Bitmapset *
get_partitions_for_keys_range(Relation rel, PartScanKeyInfo *keys)
{
	Bitmapset			   *result = NULL;
	PartitionKey			partkey = RelationGetPartitionKey(rel);
	PartitionBoundInfo 		boundinfo = RelationGetPartitionDesc(rel)->boundinfo;
	int		i,
			eqoff,
			minoff,
			maxoff;
	bool	is_equal,
			include_def = false;

	Assert(RelationGetPartitionDesc(rel)->nparts > 0);

	/*
	 * We might be able to get the answer sooner based on the nullness of
	 * keys, so get that out of the way.
	 */
	for (i = 0; i < partkey->partnatts; i++)
	{
		if (bms_is_member(i, keys->keyisnull))
		{
			/* Only the default partition accepts nulls. */
			if (partition_bound_has_default(boundinfo))
				return bms_make_singleton(boundinfo->default_index);
			else
				return NULL;
		}
	}

	/*
	 * If there are no datums to compare keys with, but there are partitions,
	 * just return the default partition, if one exists.
	 */
	if (boundinfo->ndatums == 0)
	{
		if (partition_bound_has_default(boundinfo))
			return bms_make_singleton(boundinfo->default_index);
		else
			return NULL;
	}

	/* Equality keys. */
	if (keys->n_eqkeys > 0)
	{
		/* Valid iff there are as many as partition key columns. */
		Assert(keys->n_eqkeys == partkey->partnatts);
		eqoff = partition_range_datum_bsearch(partkey, boundinfo,
											  keys->n_eqkeys, keys->eqkeys,
											  &is_equal);
		/*
		 * The bound at eqoff is known to be <= eqkeys, given the way
		 * partition_bound_bsearch works.  Considering it as the lower bound
		 * of the partition that eqkeys falls into, the bound at eqoff + 1
		 * would be its upper bound, so use eqoff + 1 to get the desired
		 * partition's index.
		 */
		if (eqoff >= 0 && boundinfo->indexes[eqoff + 1] >= 0)
			return bms_make_singleton(boundinfo->indexes[eqoff+1]);
		/*
		 * eqkeys falls into a range of values for which no non-default
		 * partition exists.
		 */
		else if (partition_bound_has_default(boundinfo))
			return bms_make_singleton(boundinfo->default_index);
		else
			return NULL;
	}

	/*
	 * Find the leftmost bound that satisfies the query, that is, make minoff
	 * point to the datum corresponding to the upper bound of the left-most
	 * partition to be selected.
	 */
	minoff = 0;
	if (keys->n_minkeys > 0)
	{
		minoff = partition_range_datum_bsearch(partkey, boundinfo,
											   keys->n_minkeys, keys->minkeys,
											   &is_equal);

		/*
		 * If minkeys does not contain values for all partition key columns,
		 * that is, only a prefix is specified, then there may be multiple
		 * bounds in boundinfo that share the same prefix.  But
		 * partition_bound_bsearch would've returned the offset of just one of
		 * those.  If minkey is inclusive, we must decrement minoff until it
		 * reaches the leftmost of those bound values, so that partitions
		 * corresponding to all those bound values are selected.  If minkeys
		 * is exclusive, we must increment minoff until it reaches the first
		 * bound greater than this prefix, so that none of the partitions
		 * corresponding to those bound values are selected.
		 */
		if (is_equal && keys->n_minkeys < partkey->partnatts)
		{
			while (minoff >= 1 && minoff < boundinfo->ndatums - 1)
			{
				int32	cmpval;
				int		nextoff;

				nextoff = keys->min_incl ? minoff - 1 : minoff + 1;
				cmpval = partition_rbound_datum_cmp(partkey,
												boundinfo->datums[nextoff],
												boundinfo->kind[nextoff],
												keys->minkeys,
												keys->n_minkeys);
				if (cmpval != 0)
				{
					/* Move to the non-equal bound only in this case. */
					if (!keys->min_incl)
						minoff += 1;
					break;
				}

				if (keys->min_incl)
					minoff -= 1;
				else
					minoff += 1;
			}
		}
		/*
		 * Assuming minoff currently points to the lower bound of the left-
		 * most selected partition, increment it so that it points to the
		 * upper bound.
		 */
		else
			minoff += 1;
	}

	/*
	 * Find the rightmost bound that satisfies the query, that is, make maxoff
	 * maxoff point to the datum corresponding to the upper bound of the
	 * right-most partition to be selected.
	 */
	maxoff = boundinfo->ndatums;
	if (keys->n_maxkeys > 0)
	{
		maxoff = partition_range_datum_bsearch(partkey, boundinfo,
											   keys->n_maxkeys, keys->maxkeys,
											   &is_equal);

		/* See the comment written above for minkeys. */
		if (is_equal && keys->n_maxkeys < partkey->partnatts)
		{
			while (maxoff >= 1 && maxoff < boundinfo->ndatums - 1)
			{
				int32	cmpval;
				int		nextoff;

				nextoff = keys->max_incl ? maxoff + 1 : maxoff - 1;
				cmpval = partition_rbound_datum_cmp(partkey,
												boundinfo->datums[nextoff],
												boundinfo->kind[nextoff],
												keys->maxkeys,
												keys->n_maxkeys);
				if (cmpval != 0)
				{
					/* Move to the non-equal bound only in this case. */
					if (!keys->max_incl)
						maxoff -= 1;
					break;
				}

				if (keys->max_incl)
					maxoff += 1;
				else
					maxoff -= 1;
			}

			/*
			 * Assuming maxoff currently points to the lower bound of the
			 * right-most partition, increment it so that it points to the
			 * upper bound.
			 */
			maxoff += 1;
		}
		/*
		 * Assuming maxoff currently points to the lower bound of the right-
		 * most selected partition, increment it so that it points to the
		 * upper bound.  We do not need to include that partition though if
		 * maxkeys exactly matched the bound in question and it is exclusive.
		 * Not incrementing simply means we treat the matched bound itself
		 * the upper bound of the right-most selected partition.
		 */
		else if (!is_equal || keys->max_incl)
			maxoff += 1;
	}

	Assert (minoff >= 0 && maxoff >= 0);

	/*
	 * At this point, we believe that minoff/maxoff point to the upper bound
	 * of some partition, but it may not be the case.  It might actually be
	 * the upper bound of an unassigned range of values, which if so, move
	 * minoff/maxoff to the adjacent bound which must be the upper bound of
	 * a valid partition.
	 *
	 * By doing that, we skip over a portion of values that do indeed satisfy
	 * the query, but don't have a valid partition assigned.  The default
	 * partition will have to be included to cover those values.  Although, if
	 * the original bound in question contains an infinite value, there would
	 * not be any unassigned range to speak of, because the range us unbounded
	 * in that direction by definition, so no need to include the default.
	 */
	if (boundinfo->indexes[minoff] < 0)
	{
		int		lastkey = partkey->partnatts - 1;

		if (keys->n_minkeys > 0)
			lastkey = keys->n_minkeys - 1;
		if (minoff >=0 && minoff < boundinfo->ndatums &&
			boundinfo->kind[minoff][lastkey] == PARTITION_RANGE_DATUM_VALUE)
			include_def = true;
		minoff += 1;
	}

	if (maxoff >= 1 && boundinfo->indexes[maxoff] < 0)
	{
		int		lastkey = partkey->partnatts - 1;

		if (keys->n_maxkeys > 0)
			lastkey = keys->n_maxkeys - 1;
		if (maxoff >=0 && maxoff <= boundinfo->ndatums &&
			boundinfo->kind[maxoff - 1][lastkey] == PARTITION_RANGE_DATUM_VALUE)
			include_def = true;
		maxoff -= 1;
	}

	if (minoff <= maxoff)
		result = bms_add_range(result,
							   boundinfo->indexes[minoff],
							   boundinfo->indexes[maxoff]);
	/*
	 * There may exist a range of values unassigned to any non-default
	 * partition between the datums at minoff and maxoff.
	 */
	for (i = minoff; i <= maxoff; i++)
	{
		if (boundinfo->indexes[i] < 0)
		{
			include_def = true;
			break;
		}
	}

	/*
	 * Since partition keys with nulls are mapped to the default range
	 * partition, we must include the default partition if some keys
	 * could be null.
	 */
	if (keys->n_minkeys < partkey->partnatts ||
		keys->n_maxkeys < partkey->partnatts)
	{
		for (i = 0; i < partkey->partnatts; i++)
		{
			if (!bms_is_member(i, keys->keyisnotnull))
			{
				include_def = true;
				break;
			}
		}
	}

	if (include_def && partition_bound_has_default(boundinfo))
		result = bms_add_member(result, boundinfo->default_index);

	return result;
}

/*
 * get_partition_operator
 *
 * Return oid of the operator of given strategy for a given partition key
 * column.
 */
static Oid
get_partition_operator(PartitionKey key, int col, StrategyNumber strategy,
					   bool *need_relabel)
{
	Oid			operoid;

	/*
	 * First check if there exists an operator of the given strategy, with
	 * this column's type as both its lefttype and righttype, in the
	 * partitioning operator family specified for the column.
	 */
	operoid = get_opfamily_member(key->partopfamily[col],
								  key->parttypid[col],
								  key->parttypid[col],
								  strategy);

	/*
	 * If one doesn't exist, we must resort to using an operator in the same
	 * operator family but with the operator class declared input type.  It is
	 * OK to do so, because the column's type is known to be binary-coercible
	 * with the operator class input type (otherwise, the operator class in
	 * question would not have been accepted as the partitioning operator
	 * class).  We must however inform the caller to wrap the non-Const
	 * expression with a RelabelType node to denote the implicit coercion. It
	 * ensures that the resulting expression structurally matches similarly
	 * processed expressions within the optimizer.
	 */
	if (!OidIsValid(operoid))
	{
		operoid = get_opfamily_member(key->partopfamily[col],
									  key->partopcintype[col],
									  key->partopcintype[col],
									  strategy);
		if (!OidIsValid(operoid))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategy, key->partopcintype[col], key->partopcintype[col],
				 key->partopfamily[col]);
		*need_relabel = true;
	}
	else
		*need_relabel = false;

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

					foreach (lc, elems)
					{
						Expr   *elem = lfirst(lc),
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
 * Given a list of partition columns, modulus and remainder corresponding to a
 * partition, this function returns CHECK constraint expression Node for that
 * partition.
 *
 * The partition constraint for a hash partition is always a call to the
 * built-in function satisfies_hash_partition().  The first two arguments are
 * the modulus and remainder for the partition; the remaining arguments are the
 * values to be hashed.
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
 * constraint, given the partition key and bound structures.
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
 * get_qual_for_range
 *
 * Returns an implicit-AND list of expressions to use as a range partition's
 * constraint, given the partition key and bound structures.
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

			Assert(!isnull);
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
 * generate_partition_qual
 *
 * Generate partition predicate from rel's partition bound expression. The
 * function returns a NIL list if there is no predicate.
 *
 * Result expression tree is stored CacheMemoryContext to ensure it survives
 * as long as the relcache entry. But we should be running in a less long-lived
 * working context. To avoid leaking cache memory if this routine fails partway
 * through, we build in working memory and then copy the completed structure
 * into cache memory.
 */
static List *
generate_partition_qual(Relation rel)
{
	HeapTuple	tuple;
	MemoryContext oldcxt;
	Datum		boundDatum;
	bool		isnull;
	PartitionBoundSpec *bound;
	List	   *my_qual = NIL,
			   *result = NIL;
	Relation	parent;
	bool		found_whole_row;

	/* Guard against stack overflow due to overly deep partition tree */
	check_stack_depth();

	/* Quick copy */
	if (rel->rd_partcheck != NIL)
		return copyObject(rel->rd_partcheck);

	/* Grab at least an AccessShareLock on the parent table */
	parent = heap_open(get_partition_parent(RelationGetRelid(rel)),
					   AccessShareLock);

	/* Get pg_class.relpartbound */
	tuple = SearchSysCache1(RELOID, RelationGetRelid(rel));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(rel));

	boundDatum = SysCacheGetAttr(RELOID, tuple,
								 Anum_pg_class_relpartbound,
								 &isnull);
	if (isnull)					/* should not happen */
		elog(ERROR, "relation \"%s\" has relpartbound = null",
			 RelationGetRelationName(rel));
	bound = castNode(PartitionBoundSpec,
					 stringToNode(TextDatumGetCString(boundDatum)));
	ReleaseSysCache(tuple);

	my_qual = get_qual_from_partbound(rel, parent, bound);

	/* Add the parent's quals to the list (if any) */
	if (parent->rd_rel->relispartition)
		result = list_concat(generate_partition_qual(parent), my_qual);
	else
		result = my_qual;

	/*
	 * Change Vars to have partition's attnos instead of the parent's. We do
	 * this after we concatenate the parent's quals, because we want every Var
	 * in it to bear this relation's attnos. It's safe to assume varno = 1
	 * here.
	 */
	result = map_partition_varattnos(result, 1, rel, parent,
									 &found_whole_row);
	/* There can never be a whole-row reference here */
	if (found_whole_row)
		elog(ERROR, "unexpected whole-row reference found in partition key");

	/* Save a copy in the relcache */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	rel->rd_partcheck = copyObject(result);
	MemoryContextSwitchTo(oldcxt);

	/* Keep the parent locked until commit */
	heap_close(parent, NoLock);

	return result;
}

/*
 * get_partition_for_tuple
 *		Finds partition of relation which accepts the partition key specified
 *		in values and isnull
 *
 * Return value is index of the partition (>= 0 and < partdesc->nparts) if one
 * found or -1 if none found.
 */
int
get_partition_for_tuple(Relation relation, Datum *values, bool *isnull)
{
	int			bound_offset;
	int			part_index = -1;
	PartitionKey key = RelationGetPartitionKey(relation);
	PartitionDesc partdesc = RelationGetPartitionDesc(relation);

	/* Route as appropriate based on partitioning strategy. */
	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			{
				PartitionBoundInfo boundinfo = partdesc->boundinfo;
				int			greatest_modulus = get_greatest_modulus(boundinfo);
				uint64		rowHash = compute_hash_value(key, values, isnull);

				part_index = boundinfo->indexes[rowHash % greatest_modulus];
			}
			break;

		case PARTITION_STRATEGY_LIST:
			if (isnull[0])
			{
				if (partition_bound_accepts_nulls(partdesc->boundinfo))
					part_index = partdesc->boundinfo->null_index;
			}
			else
			{
				bool		equal = false;

				bound_offset = partition_bound_bsearch(key,
													   partdesc->boundinfo,
													   values,
													   false,
													   &equal);
				if (bound_offset >= 0 && equal)
					part_index = partdesc->boundinfo->indexes[bound_offset];
			}
			break;

		case PARTITION_STRATEGY_RANGE:
			{
				bool		equal = false,
							range_partkey_has_null = false;
				int			i;

				/*
				 * No range includes NULL, so this will be accepted by the
				 * default partition if there is one, and otherwise rejected.
				 */
				for (i = 0; i < key->partnatts; i++)
				{
					if (isnull[i])
					{
						range_partkey_has_null = true;
						break;
					}
				}

				if (!range_partkey_has_null)
				{
					bound_offset = partition_bound_bsearch(key,
														   partdesc->boundinfo,
														   values,
														   false,
														   &equal);

					/*
					 * The bound at bound_offset is less than or equal to the
					 * tuple value, so the bound at offset+1 is the upper
					 * bound of the partition we're looking for, if there
					 * actually exists one.
					 */
					part_index = partdesc->boundinfo->indexes[bound_offset + 1];
				}
			}
			break;

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	/*
	 * part_index < 0 means we failed to find a partition of this parent. Use
	 * the default partition, if there is one.
	 */
	if (part_index < 0)
		part_index = partdesc->boundinfo->default_index;

	return part_index;
}

/*
 * Checks if any of the 'attnums' is a partition key attribute for rel
 *
 * Sets *used_in_expr if any of the 'attnums' is found to be referenced in some
 * partition key expression.  It's possible for a column to be both used
 * directly and as part of an expression; if that happens, *used_in_expr may
 * end up as either true or false.  That's OK for current uses of this
 * function, because *used_in_expr is only used to tailor the error message
 * text.
 */
bool
has_partition_attrs(Relation rel, Bitmapset *attnums,
					bool *used_in_expr)
{
	PartitionKey key;
	int			partnatts;
	List	   *partexprs;
	ListCell   *partexprs_item;
	int			i;

	if (attnums == NULL || rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return false;

	key = RelationGetPartitionKey(rel);
	partnatts = get_partition_natts(key);
	partexprs = get_partition_exprs(key);

	partexprs_item = list_head(partexprs);
	for (i = 0; i < partnatts; i++)
	{
		AttrNumber	partattno = get_partition_col_attnum(key, i);

		if (partattno != 0)
		{
			if (bms_is_member(partattno - FirstLowInvalidHeapAttributeNumber,
							  attnums))
			{
				if (used_in_expr)
					*used_in_expr = false;
				return true;
			}
		}
		else
		{
			/* Arbitrary expression */
			Node	   *expr = (Node *) lfirst(partexprs_item);
			Bitmapset  *expr_attrs = NULL;

			/* Find all attributes referenced */
			pull_varattnos(expr, 1, &expr_attrs);
			partexprs_item = lnext(partexprs_item);

			if (bms_overlap(attnums, expr_attrs))
			{
				if (used_in_expr)
					*used_in_expr = true;
				return true;
			}
		}
	}

	return false;
}

/*
 * qsort_partition_hbound_cmp
 *
 * We sort hash bounds by modulus, then by remainder.
 */
static int32
qsort_partition_hbound_cmp(const void *a, const void *b)
{
	PartitionHashBound *h1 = (*(PartitionHashBound *const *) a);
	PartitionHashBound *h2 = (*(PartitionHashBound *const *) b);

	return partition_hbound_cmp(h1->modulus, h1->remainder,
								h2->modulus, h2->remainder);
}

/*
 * partition_hbound_cmp
 *
 * Compares modulus first, then remainder if modulus are equal.
 */
static int32
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
 * qsort_partition_list_value_cmp
 *
 * Compare two list partition bound datums
 */
static int32
qsort_partition_list_value_cmp(const void *a, const void *b, void *arg)
{
	Datum		val1 = (*(const PartitionListValue **) a)->value,
				val2 = (*(const PartitionListValue **) b)->value;
	PartitionKey key = (PartitionKey) arg;

	return DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[0],
										   key->partcollation[0],
										   val1, val2));
}

/*
 * make_one_range_bound
 *
 * Return a PartitionRangeBound given a list of PartitionRangeDatum elements
 * and a flag telling whether the bound is lower or not.  Made into a function
 * because there are multiple sites that want to use this facility.
 */
static PartitionRangeBound *
make_one_range_bound(PartitionKey key, int index, List *datums, bool lower)
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

/* Used when sorting range bounds across all range partitions */
static int32
qsort_partition_rbound_cmp(const void *a, const void *b, void *arg)
{
	PartitionRangeBound *b1 = (*(PartitionRangeBound *const *) a);
	PartitionRangeBound *b2 = (*(PartitionRangeBound *const *) b);
	PartitionKey key = (PartitionKey) arg;

	return partition_rbound_cmp(key, b1->datums, b1->kind, b1->lower, b2);
}

/*
 * partition_rbound_cmp
 *
 * Return for two range bounds whether the 1st one (specified in datums1,
 * kind1, and lower1) is <, =, or > the bound specified in *b2.
 *
 * Note that if the values of the two range bounds compare equal, then we take
 * into account whether they are upper or lower bounds, and an upper bound is
 * considered to be smaller than a lower bound. This is important to the way
 * that RelationBuildPartitionDesc() builds the PartitionBoundInfoData
 * structure, which only stores the upper bound of a common boundary between
 * two contiguous partitions.
 */
static int32
partition_rbound_cmp(PartitionKey key,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2)
{
	int32		cmpval = 0;		/* placate compiler */
	int			i;
	Datum	   *datums2 = b2->datums;
	PartitionRangeDatumKind *kind2 = b2->kind;
	bool		lower2 = b2->lower;

	for (i = 0; i < key->partnatts; i++)
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

		cmpval = DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[i],
												 key->partcollation[i],
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
 */
static int32
partition_rbound_datum_cmp(PartitionKey key,
						   Datum *rb_datums, PartitionRangeDatumKind *rb_kind,
						   Datum *tuple_datums)
{
	int			i;
	int32		cmpval = -1;

	for (i = 0; i < key->partnatts; i++)
	{
		if (rb_kind[i] == PARTITION_RANGE_DATUM_MINVALUE)
			return -1;
		else if (rb_kind[i] == PARTITION_RANGE_DATUM_MAXVALUE)
			return 1;

		cmpval = DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[i],
												 key->partcollation[i],
												 rb_datums[i],
												 tuple_datums[i]));
		if (cmpval != 0)
			break;
	}

	return cmpval;
}

/*
 * partition_bound_cmp
 *
 * Return whether the bound at offset in boundinfo is <, =, or > the argument
 * specified in *probe.
 */
static int32
partition_bound_cmp(PartitionKey key, PartitionBoundInfo boundinfo,
					int offset, void *probe, bool probe_is_bound)
{
	Datum	   *bound_datums = boundinfo->datums[offset];
	int32		cmpval = -1;

	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			{
				PartitionBoundSpec *spec = (PartitionBoundSpec *) probe;

				cmpval = partition_hbound_cmp(DatumGetInt32(bound_datums[0]),
											  DatumGetInt32(bound_datums[1]),
											  spec->modulus, spec->remainder);
				break;
			}
		case PARTITION_STRATEGY_LIST:
			cmpval = DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[0],
													 key->partcollation[0],
													 bound_datums[0],
													 *(Datum *) probe));
			break;

		case PARTITION_STRATEGY_RANGE:
			{
				PartitionRangeDatumKind *kind = boundinfo->kind[offset];

				if (probe_is_bound)
				{
					/*
					 * We need to pass whether the existing bound is a lower
					 * bound, so that two equal-valued lower and upper bounds
					 * are not regarded equal.
					 */
					bool		lower = boundinfo->indexes[offset] < 0;

					cmpval = partition_rbound_cmp(key,
												  bound_datums, kind, lower,
												  (PartitionRangeBound *) probe);
				}
				else
					cmpval = partition_rbound_datum_cmp(key,
														bound_datums, kind,
														(Datum *) probe);
				break;
			}

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	return cmpval;
}

/*
 * Binary search on a collection of partition bounds. Returns greatest
 * bound in array boundinfo->datums which is less than or equal to *probe.
 * If all bounds in the array are greater than *probe, -1 is returned.
 *
 * *probe could either be a partition bound or a Datum array representing
 * the partition key of a tuple being routed; probe_is_bound tells which.
 * We pass that down to the comparison function so that it can interpret the
 * contents of *probe accordingly.
 *
 * *is_equal is set to whether the bound at the returned index is equal with
 * *probe.
 */
static int
partition_bound_bsearch(PartitionKey key, PartitionBoundInfo boundinfo,
						void *probe, bool probe_is_bound, bool *is_equal)
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
		cmpval = partition_bound_cmp(key, boundinfo, mid, probe,
									 probe_is_bound);
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
 * get_default_oid_from_partdesc
 *
 * Given a partition descriptor, return the OID of the default partition, if
 * one exists; else, return InvalidOid.
 */
Oid
get_default_oid_from_partdesc(PartitionDesc partdesc)
{
	if (partdesc && partdesc->boundinfo &&
		partition_bound_has_default(partdesc->boundinfo))
		return partdesc->oids[partdesc->boundinfo->default_index];

	return InvalidOid;
}

/*
 * get_default_partition_oid
 *
 * Given a relation OID, return the OID of the default partition, if one
 * exists.  Use get_default_oid_from_partdesc where possible, for
 * efficiency.
 */
Oid
get_default_partition_oid(Oid parentId)
{
	HeapTuple	tuple;
	Oid			defaultPartId = InvalidOid;

	tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(parentId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_partitioned_table part_table_form;

		part_table_form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
		defaultPartId = part_table_form->partdefid;
		ReleaseSysCache(tuple);
	}

	return defaultPartId;
}

/*
 * update_default_partition_oid
 *
 * Update pg_partition_table.partdefid with a new default partition OID.
 */
void
update_default_partition_oid(Oid parentId, Oid defaultPartId)
{
	HeapTuple	tuple;
	Relation	pg_partitioned_table;
	Form_pg_partitioned_table part_table_form;

	pg_partitioned_table = heap_open(PartitionedRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(parentId));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for partition key of relation %u",
			 parentId);

	part_table_form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
	part_table_form->partdefid = defaultPartId;
	CatalogTupleUpdate(pg_partitioned_table, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	heap_close(pg_partitioned_table, RowExclusiveLock);
}

/*
 * get_proposed_default_constraint
 *
 * This function returns the negation of new_part_constraints, which
 * would be an integral part of the default partition constraints after
 * addition of the partition to which the new_part_constraints belongs.
 */
List *
get_proposed_default_constraint(List *new_part_constraints)
{
	Expr	   *defPartConstraint;

	defPartConstraint = make_ands_explicit(new_part_constraints);

	/*
	 * Derive the partition constraints of default partition by negating the
	 * given partition constraints. The partition constraint never evaluates
	 * to NULL, so negating it like this is safe.
	 */
	defPartConstraint = makeBoolExpr(NOT_EXPR,
									 list_make1(defPartConstraint),
									 -1);
	defPartConstraint =
		(Expr *) eval_const_expressions(NULL,
										(Node *) defPartConstraint);
	defPartConstraint = canonicalize_qual(defPartConstraint);

	return list_make1(defPartConstraint);
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
			num_indexes = get_greatest_modulus(bound);
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
 * get_greatest_modulus
 *
 * Returns the greatest modulus of the hash partition bound. The greatest
 * modulus will be at the end of the datums array because hash partitions are
 * arranged in the ascending order of their modulus and remainders.
 */
static int
get_greatest_modulus(PartitionBoundInfo bound)
{
	Assert(bound && bound->strategy == PARTITION_STRATEGY_HASH);
	Assert(bound->datums && bound->ndatums > 0);
	Assert(DatumGetInt32(bound->datums[bound->ndatums - 1][0]) > 0);

	return DatumGetInt32(bound->datums[bound->ndatums - 1][0]);
}

/*
 * compute_hash_value
 *
 * Compute the hash value for given not null partition key values.
 */
static uint64
compute_hash_value(PartitionKey key, Datum *values, bool *isnull)
{
	int			i;
	int			nkeys = key->partnatts;
	uint64		rowHash = 0;
	Datum		seed = UInt64GetDatum(HASH_PARTITION_SEED);

	for (i = 0; i < nkeys; i++)
	{
		if (!isnull[i])
		{
			Datum		hash;

			Assert(OidIsValid(key->partsupfunc[i].fn_oid));

			/*
			 * Compute hash for each datum value by calling respective
			 * datatype-specific hash functions of each partition key
			 * attribute.
			 */
			hash = FunctionCall2(&key->partsupfunc[i], values[i], seed);

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
