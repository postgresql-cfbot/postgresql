/*-------------------------------------------------------------------------
 *
 * constraint.c
 *	  PostgreSQL CONSTRAINT support code.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/constraint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_constraint.h"
#include "commands/constraint.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/*
 * unique_key_recheck - trigger function to do a deferred uniqueness check.
 *
 * This now also does deferred exclusion-constraint checks, so the name is
 * somewhat historical.
 *
 * This is invoked as an AFTER ROW trigger for both INSERT and UPDATE,
 * for any rows recorded as potentially violating a deferrable unique
 * or exclusion constraint.
 *
 * This may be an end-of-statement check, a commit-time check, or a
 * check triggered by a SET CONSTRAINTS command.
 */
Datum
unique_key_recheck(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *funcname = "unique_key_recheck";
	ItemPointerData checktid;
	ItemPointerData tmptid;
	Relation	indexRel;
	IndexInfo  *indexInfo;
	EState	   *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	/*
	 * Make sure this is being called as an AFTER ROW trigger.  Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER ROW",
						funcname)));

	/*
	 * Get the new data that was inserted/updated.
	 */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		checktid = trigdata->tg_trigslot->tts_tid;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		checktid = trigdata->tg_newslot->tts_tid;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						funcname)));
		ItemPointerSetInvalid(&checktid);	/* keep compiler quiet */
	}

	slot = table_slot_create(trigdata->tg_relation, NULL);

	/*
	 * If the row pointed at by checktid is now dead (ie, inserted and then
	 * deleted within our transaction), we can skip the check.  However, we
	 * have to be careful, because this trigger gets queued only in response
	 * to index insertions; which means it does not get queued e.g. for HOT
	 * updates.  The row we are called for might now be dead, but have a live
	 * HOT child, in which case we still need to make the check ---
	 * effectively, we're applying the check against the live child row,
	 * although we can use the values from this row since by definition all
	 * columns of interest to us are the same.
	 *
	 * This might look like just an optimization, because the index AM will
	 * make this identical test before throwing an error.  But it's actually
	 * needed for correctness, because the index AM will also throw an error
	 * if it doesn't find the index entry for the row.  If the row's dead then
	 * it's possible the index entry has also been marked dead, and even
	 * removed.
	 */
	tmptid = checktid;
	{
		IndexFetchTableData *scan = table_index_fetch_begin(trigdata->tg_relation);
		bool		call_again = false;

		if (!table_index_fetch_tuple(scan, &tmptid, SnapshotSelf, slot,
									 &call_again, NULL))
		{
			/*
			 * All rows referenced by the index entry are dead, so skip the
			 * check.
			 */
			ExecDropSingleTupleTableSlot(slot);
			table_index_fetch_end(scan);
			return PointerGetDatum(NULL);
		}
		table_index_fetch_end(scan);
	}

	/*
	 * Open the index, acquiring a RowExclusiveLock, just as if we were going
	 * to update it.  (This protects against possible changes of the index
	 * schema, not against concurrent updates.)
	 */
	indexRel = index_open(trigdata->tg_trigger->tgconstrindid,
						  RowExclusiveLock);
	indexInfo = BuildIndexInfo(indexRel);

	/*
	 * Typically the index won't have expressions, but if it does we need an
	 * EState to evaluate them.  We need it for exclusion constraints too,
	 * even if they are just on simple columns.
	 */
	if (indexInfo->ii_Expressions != NIL ||
		indexInfo->ii_ExclusionOps != NULL)
	{
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		econtext->ecxt_scantuple = slot;
	}
	else
		estate = NULL;

	/*
	 * Form the index values and isnull flags for the index entry that we need
	 * to check.
	 *
	 * Note: if the index uses functions that are not as immutable as they are
	 * supposed to be, this could produce an index tuple different from the
	 * original.  The index AM can catch such errors by verifying that it
	 * finds a matching index entry with the tuple's TID.  For exclusion
	 * constraints we check this in check_exclusion_constraint().
	 */
	FormIndexDatum(indexInfo, slot, estate, values, isnull);

	/*
	 * Now do the appropriate check.
	 */
	if (indexInfo->ii_ExclusionOps == NULL)
	{
		/*
		 * Note: this is not a real insert; it is a check that the index entry
		 * that has already been inserted is unique.  Passing the tuple's tid
		 * (i.e. unmodified by table_index_fetch_tuple()) is correct even if
		 * the row is now dead, because that is the TID the index will know
		 * about.
		 */
		index_insert(indexRel, values, isnull, &checktid,
					 trigdata->tg_relation, UNIQUE_CHECK_EXISTING,
					 false, indexInfo);
	}
	else
	{
		/*
		 * For exclusion constraints we just do the normal check, but now it's
		 * okay to throw error.  In the HOT-update case, we must use the live
		 * HOT child's TID here, else check_exclusion_constraint will think
		 * the child is a conflict.
		 */
		check_exclusion_constraint(trigdata->tg_relation, indexRel, indexInfo,
								   &tmptid, values, isnull,
								   estate, false);
	}

	/*
	 * If that worked, then this index entry is unique or non-excluded, and we
	 * are done.
	 */
	if (estate != NULL)
		FreeExecutorState(estate);

	ExecDropSingleTupleTableSlot(slot);

	index_close(indexRel, RowExclusiveLock);

	return PointerGetDatum(NULL);
}

/*
 * Create and return a Constraint node representing a "col IS NOT NULL"
 * expression using a ColumnRef within, for the given relation and column.
 *
 * If the constraint name is not provided, a standard one is generated.
 * XXX provide a list of constraint names already reserved so we can ignore
 * those.
 *
 * Note: this is a "raw" node that must undergo transformation.
 */
Constraint *
makeCheckNotNullConstraint(Oid nspid, char *constraint_name,
						   const char *relname, const char *colname,
						   bool is_row, Oid parent_oid)
{
	Constraint *check;
	ColumnRef  *colref;
	Node	   *nullexpr;

	colref = (ColumnRef *) makeNode(ColumnRef);
	colref->fields = list_make1(makeString(pstrdup(colname)));

	if (is_row)
	{
		A_Expr     *expr;
		A_Const	   *constnull;

		constnull = makeNode(A_Const);
		constnull->isnull = true;

		expr = makeSimpleA_Expr(AEXPR_DISTINCT, "=",
								(Node *) colref, (Node *) constnull, -1);
		nullexpr = (Node *) expr;
	}
	else
	{
		NullTest   *nulltest;

		nulltest = makeNode(NullTest);
		nulltest->argisrow = is_row;
		nulltest->nulltesttype = IS_NOT_NULL;
		nulltest->arg = (Expr *) colref;

		nullexpr = (Node *) nulltest;
	}

	check = makeNode(Constraint);
	check->contype = CONSTR_CHECK;
	check->location = -1;
	check->conname = constraint_name ? constraint_name :
		ChooseConstraintName(relname, colname, "not_null", nspid, NIL);
	check->parent_oid = parent_oid;
	check->deferrable = false;
	check->initdeferred = false;

	check->is_no_inherit = false;
	check->raw_expr = nullexpr;
	check->cooked_expr = NULL;

	check->skip_validation = false;
	check->initially_valid = true;

	return check;
}

/*
 * Given the Node representation for a CHECK (col IS NOT NULL) constraint,
 * return the column name that it is for.  If it doesn't represent a constraint
 * of that shape, NULL is returned. 'rel' is the relation that the constraint is
 * for.
 */
char *
tryExtractNotNullFromNode(Node *node, Relation rel)
{
	if (node == NULL)
		return NULL;

	/* Whatever we got, we can look inside a Constraint node */
	if (IsA(node, Constraint))
	{
		Constraint	*constraint = (Constraint *) node;

		if (constraint->cooked_expr != NULL)
			return tryExtractNotNullFromNode(stringToNode(constraint->cooked_expr), rel);
		else
			return tryExtractNotNullFromNode(constraint->raw_expr, rel);
	}

	if (IsA(node, NullTest))
	{
		NullTest *nulltest = (NullTest *) node;

		if (nulltest->nulltesttype == IS_NOT_NULL &&
			IsA(nulltest->arg, ColumnRef))
		{
			ColumnRef *colref = (ColumnRef *) nulltest->arg;

			if (list_length(colref->fields) == 1)
				return strVal(linitial(colref->fields));
		}
	}

	/*
	 * if no rel is passed, we can only check this much
	 */
	if (rel == NULL)
		return NULL;

	if (IsA(node, NullTest))
	{
		NullTest *nulltest = (NullTest *) node;

		if (nulltest->nulltesttype == IS_NOT_NULL)
		{
			if (IsA(nulltest->arg, Var))
			{
				Var    *var = (Var *) nulltest->arg;

				return NameStr(TupleDescAttr(RelationGetDescr(rel),
											 var->varattno - 1)->attname);
			}
		}
	}

	/*
	 * XXX Need to check a few more possible wordings of NOT NULL:
	 *
	 * - foo IS DISTINCT FROM NULL
	 * - NOT (foo IS NULL)
	 */

	return NULL;
}

/*
 * Given a pg_constraint tuple for a CHECK constraint, see if it is a
 * CHECK (IS NOT NULL) constraint, and return the column number it is for if
 * so.  Otherwise return InvalidAttrNumber.
 */
AttrNumber
tryExtractNotNullFromCatalog(HeapTuple constrTup)
{
	Form_pg_constraint conForm = (Form_pg_constraint) GETSTRUCT(constrTup);
	AttrNumber colnum = InvalidAttrNumber;
	Datum   val;
	bool    isnull;
	char   *conbin;
	Node   *node;

	/* only tuples for CHECK constraints should be given */
	Assert(conForm->contype == CONSTRAINT_CHECK);

	val = SysCacheGetAttr(CONSTROID, constrTup, Anum_pg_constraint_conbin,
						  &isnull);
	if (isnull)
		elog(ERROR, "null conbin for constraint %u", conForm->oid);

	conbin = TextDatumGetCString(val);
	node = (Node *) stringToNode(conbin);

	/* We expect a NullTest with an single Var within. */
	if (IsA(node, NullTest))
	{
		NullTest *nulltest = (NullTest *) node;

		if (nulltest->nulltesttype == IS_NOT_NULL)
		{
			if (IsA(nulltest->arg, Var))
			{
				Var    *var = (Var *) nulltest->arg;

				colnum = var->varattno;
			}
		}
	}

	/*
	 * XXX Need to check a few more possible wordings of NOT NULL:
	 *
	 * - foo IS DISTINCT FROM NULL
	 * - NOT (foo IS NULL)
	 */

	pfree(conbin);
	pfree(node);

	return colnum;
}
