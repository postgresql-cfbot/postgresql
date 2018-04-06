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

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/partition.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
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
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static Oid	get_partition_parent_worker(Relation inhRel, Oid relid);
static void get_partition_ancestors_worker(Relation inhRel, Oid relid,
							   List **ancestors);
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
 *		Obtain direct parent of given relation
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
	Relation	catalogRelation;
	Oid			result;

	catalogRelation = heap_open(InheritsRelationId, AccessShareLock);

	result = get_partition_parent_worker(catalogRelation, relid);

	if (!OidIsValid(result))
		elog(ERROR, "could not find tuple for parent of relation %u", relid);

	heap_close(catalogRelation, AccessShareLock);

	return result;
}

/*
 * get_partition_ancestors
 *		Obtain ancestors of given relation
 *
 * Returns a list of ancestors of the given relation.
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument and each ancestor will have precisely one parent, it should
 * only be called when it is known that the relation is a partition.
 */
List *
get_partition_ancestors(Oid relid)
{
	List	   *result = NIL;
	Relation	inhRel;

	inhRel = heap_open(InheritsRelationId, AccessShareLock);

	get_partition_ancestors_worker(inhRel, relid, &result);

	heap_close(inhRel, AccessShareLock);

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

	/* Simplify, to put the negated expression into canonical form */
	defPartConstraint =
		(Expr *) eval_const_expressions(NULL,
										(Node *) defPartConstraint);
	defPartConstraint = canonicalize_qual(defPartConstraint, true);

	return make_ands_implicit(defPartConstraint);
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

/* Module-local functions */

/*
 * get_partition_parent_worker
 *		Scan the pg_inherits relation to return the OID of the parent of the
 *		given relation
 */
static Oid
get_partition_parent_worker(Relation inhRel, Oid relid)
{
	SysScanDesc scan;
	ScanKeyData key[2];
	Oid			result = InvalidOid;
	HeapTuple	tuple;

	ScanKeyInit(&key[0],
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[1],
				Anum_pg_inherits_inhseqno,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(1));

	scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId, true,
							  NULL, 2, key);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_inherits form = (Form_pg_inherits) GETSTRUCT(tuple);

		result = form->inhparent;
	}

	systable_endscan(scan);

	return result;
}

/*
 * get_partition_ancestors_worker
 *		recursive worker for get_partition_ancestors
 */
static void
get_partition_ancestors_worker(Relation inhRel, Oid relid, List **ancestors)
{
	Oid			parentOid;

	/* Recursion ends at the topmost level, ie., when there's no parent */
	parentOid = get_partition_parent_worker(inhRel, relid);
	if (parentOid == InvalidOid)
		return;

	*ancestors = lappend_oid(*ancestors, parentOid);
	get_partition_ancestors_worker(inhRel, parentOid, ancestors);
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
