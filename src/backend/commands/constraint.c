/*-------------------------------------------------------------------------
 *
 * constraint.c
 *	  PostgreSQL CONSTRAINT support code.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/constraint.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "catalog/index.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tqual.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/constraint.h"
#include "executor/functions.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/ruleutils.h"
#include "utils/fmgroids.h"


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
	TriggerData *trigdata = castNode(TriggerData, fcinfo->context);
	const char *funcname = "unique_key_recheck";
	HeapTuple new_row;
	ItemPointerData tmptid;
	Relation indexRel;
	IndexInfo *indexInfo;
	EState *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	Datum values[INDEX_MAX_KEYS];
	bool isnull[INDEX_MAX_KEYS];

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
		new_row = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		new_row = trigdata->tg_newtuple;
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
					errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						   funcname)));
		new_row = NULL;            /* keep compiler quiet */
	}

	/*
	 * If the new_row is now dead (ie, inserted and then deleted within our
	 * transaction), we can skip the check.  However, we have to be careful,
	 * because this trigger gets queued only in response to index insertions;
	 * which means it does not get queued for HOT updates.  The row we are
	 * called for might now be dead, but have a live HOT child, in which case
	 * we still need to make the check --- effectively, we're applying the
	 * check against the live child row, although we can use the values from
	 * this row since by definition all columns of interest to us are the
	 * same.
	 *
	 * This might look like just an optimization, because the index AM will
	 * make this identical test before throwing an error.  But it's actually
	 * needed for correctness, because the index AM will also throw an error
	 * if it doesn't find the index entry for the row.  If the row's dead then
	 * it's possible the index entry has also been marked dead, and even
	 * removed.
	 */
	tmptid = new_row->t_self;
	if (!heap_hot_search(&tmptid, trigdata->tg_relation, SnapshotSelf, NULL))
	{
		/*
		 * All rows in the HOT chain are dead, so skip the check.
		 */
		return PointerGetDatum(NULL);
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
	 * The heap tuple must be put into a slot for FormIndexDatum.
	 */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(trigdata->tg_relation));

	ExecStoreTuple(new_row, slot, InvalidBuffer, false);

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
		 * that has already been inserted is unique.  Passing t_self is
		 * correct even if t_self is now dead, because that is the TID the
		 * index will know about.
		 */
		index_insert(indexRel, values, isnull, &(new_row->t_self),
					 trigdata->tg_relation, UNIQUE_CHECK_EXISTING,
					 indexInfo);
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


static void
test_assertion_expr(char *name, char *expression)
{
	char *sql;
	int returnCode;
	bool isNull;
	Datum value;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI connect failed when executing ASSERTION statement");

	sql = psprintf("SELECT %s", expression);
	returnCode = SPI_exec(sql, 1);

	if (returnCode > 0 && SPI_tuptable != NULL)
	{
		value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_CHECK_VIOLATION),
							errmsg("assertion \"%s\" truth is unknown", name)));
		else if (!isNull && !DatumGetBool(value))
			ereport(ERROR,
					(errcode(ERRCODE_CHECK_VIOLATION),
							errmsg("assertion \"%s\" violated", name)));
	}
	else
		elog(ERROR, "unexpected SPI result when executing ASSERTION statement");

	SPI_finish();
}


Datum
assertion_check(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *funcname = "assertion_check";
	Oid constraintOid;
	HeapTuple tup;
	Datum adatum;
	bool isNull;

	/*
	 * Make sure this is being called as an AFTER STATEMENT trigger.	Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
					errmsg("function \"%s\" was not called by trigger manager",
						   funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
					errmsg("function \"%s\" must be fired AFTER STATEMENT",
						   funcname)));

	constraintOid = trigdata->tg_trigger->tgconstraint;
	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for constraint %u", constraintOid);

	// XXX bogus
	adatum = SysCacheGetAttr(CONSTROID, tup,
							 Anum_pg_constraint_consrc, &isNull);
	if (isNull)
		elog(ERROR, "constraint %u has null consrc", constraintOid);

	test_assertion_expr(get_constraint_name(constraintOid), TextDatumGetCString(adatum));

	ReleaseSysCache(tup);
	return PointerGetDatum(NULL);
}

#define NO_FUNC				(0)
#define OTHER_FUNC			(1 << 0)
#define MIN_AGG_FUNC		(1 << 1)
#define MAX_AGG_FUNC		(1 << 2)
#define COUNT_AGG_FUNC		(1 << 3)
#define EVERY_AGG_FUNC		(1 << 4)
#define BOOL_AND_AGG_FUNC	(1 << 5)
#define BOOL_OR_AGG_FUNC	(1 << 6)

#define EQ_COMPARISONS 		(bms_make_singleton(ROWCOMPARE_EQ))
#define NE_COMPARISONS 		(bms_make_singleton(ROWCOMPARE_NE))
#define LTE_COMPARISONS 	(bms_add_member(bms_make_singleton(ROWCOMPARE_LE), ROWCOMPARE_LT))
#define GTE_COMPARISONS 	(bms_add_member(bms_make_singleton(ROWCOMPARE_GE), ROWCOMPARE_GT))

#define MatchesOnly(a,b)	(((a) & (b)) && !((a) & (~(b))))
#define CanOptimise(s) 		(!bms_is_empty(s) && \
							(bms_is_subset(s, EQ_COMPARISONS)  || \
							 bms_is_subset(s, NE_COMPARISONS)  || \
							 bms_is_subset(s, LTE_COMPARISONS) || \
							 bms_is_subset(s, GTE_COMPARISONS) ))


// TODO review TODOs in the tests
// TODO assertions should allow unknown according to the SQL specification
// TODO traversal into views needs rethinking - dependencies?
// TODO pg_dump support

/*
 * DML operations that could affect the truth of an assertion. Only
 * INSERT and DELETE are considered as part of the labeling algorithm.
 * UPDATEs are inferred using different logic.
 */
typedef enum DmlOp
{
	INSERT,
	DELETE,
	INSERT_DELETE
} DmlOp;


/*
 * A context used to capture operations that may invalidate an assertion.
 */
typedef struct AssertionInfo
{
	DmlOp	label;         /* current invaliding operation */
	Expr   *expr;          /* current Expression node */
	List   *rtable;        /* current range tables */
	List   *operators;     /* current operators */
	SetOperation setOp;         /* current set-operation */
	bool	invert;		   /* ... */
	bool	inView;        /* if the walker has entered a view */
	bool	inTargetEntry; /* if the walker has entered a TargetEntry node */
	bool	inComparison;  /* if the walker has entered a comparison operation */
	bool	inExists;      /* if the walker has entered an EXISTS node */

	List   *dependencies; /* ... */
	List   *relations;    /* relation Oids for all tables involved in the assertion check */
	List   *inserts;      /* relation Oids for which INSERT could invalidate the assertion */
	List   *deletes;      /* relation Oids for which DELETE could invalidate the assertion */
	List   *updates;      /* relation Oids for which UPDATE could invalidate the assertion */
	List   *columns;      /* list of ObjectAddresses referencing involved columns */
} AssertionInfo;


static void initAssertionInfo(AssertionInfo *info);
static void copyAssertionInfo(AssertionInfo *target, AssertionInfo *source);
static DmlOp oppositeDmlOp(DmlOp operation);
static RowCompareType oppositeCompareType(RowCompareType type);
static DmlOp labelForComparisonWithAggFuncs(DmlOp label, RowCompareType compOp, int aggFuncs);
static int16 triggerOnEvents(AssertionInfo *info, Oid relationId);
static List *triggerOnColumns(AssertionInfo *info, Oid relationId);
static bool listContainsObjectAddress(List *list, ObjectAddress *address);
static Bitmapset * strategiesForOperators(List *operators);
static int functionsForTargetList(List *targetList);
static int funcMaskForFuncOid(Oid funcOid);
static Query * queryForSQLFunction(FuncExpr *funcExpr);

/* Expression visiting */
static bool visitAllNodes(Node *node, AssertionInfo *info);
static bool visitRangeTblRef(RangeTblRef *node, AssertionInfo *info);
static bool visitQuery(Query *node, AssertionInfo *info);
static bool visitSetOperationStmt(SetOperationStmt *node, AssertionInfo *info);
static bool visitBoolExpr(BoolExpr *node, AssertionInfo *info);
static bool visitSubLink(SubLink *node, AssertionInfo *info);
static bool visitFuncExpr(FuncExpr *node, AssertionInfo *info);
static bool visitExpr(Expr *node, AssertionInfo *info);
static bool visitVar(Var *node, AssertionInfo *info);

/* TargetList visiting */
static bool visitAggrefNodes(Node *node, int *aggFuncs);
static bool visitAggref(Aggref *node, int *aggFuncs);
static bool visitWindowFunc(WindowFunc *node, int *aggFuncs);


static void
initAssertionInfo(AssertionInfo *info)
{
	info->label = DELETE;
	info->expr = NULL;
	info->rtable = NIL;
	info->operators = NIL;
	info->setOp = SETOP_NONE;
	info->invert = false;
	info->inView = false;
	info->inTargetEntry = false;
	info->inComparison = false;
	info->inExists = false;

	info->dependencies = NIL;
	info->relations = NIL;
	info->inserts = NIL;
	info->deletes = NIL;
	info->updates = NIL;
	info->columns = NIL;
}


static void
copyAssertionInfo(AssertionInfo *target, AssertionInfo *source)
{
	target->label = source->label;
	target->expr = source->expr;
	target->rtable = source->rtable;
	target->operators = source->operators;
	target->setOp = source->setOp;
	target->invert = source->invert;
	target->inView = source->inView;
	target->inTargetEntry = source->inTargetEntry;
	target->inComparison = source->inComparison;
	target->inExists = source->inExists;
}


static DmlOp
oppositeDmlOp(DmlOp operation)
{
	switch (operation)
	{
		case INSERT:
			return DELETE;
		case DELETE:
			return INSERT;
		case INSERT_DELETE:
			return INSERT_DELETE;
	}
}


static RowCompareType
oppositeCompareType(RowCompareType type)
{
	switch (type)
	{
		case ROWCOMPARE_LE:
			return ROWCOMPARE_GE;
		case ROWCOMPARE_LT:
			return ROWCOMPARE_GT;
		case ROWCOMPARE_GT:
			return ROWCOMPARE_LT;
		case ROWCOMPARE_GE:
			return ROWCOMPARE_LE;
		default:
			return type; /* we do not flip EQ and NE comparisons */
	}
}


static DmlOp
labelForComparisonWithAggFuncs(DmlOp label, RowCompareType compOp, int aggFuncs)
{
	int stronger, weaker;

	switch (compOp)
	{
		case ROWCOMPARE_LT:
		case ROWCOMPARE_LE:
			stronger = MIN_AGG_FUNC;
			weaker = MAX_AGG_FUNC | COUNT_AGG_FUNC;
			break;

		case ROWCOMPARE_EQ:
			stronger = BOOL_AND_AGG_FUNC | EVERY_AGG_FUNC;
			weaker = BOOL_OR_AGG_FUNC;
			break;

		case ROWCOMPARE_NE:
			stronger = BOOL_AND_AGG_FUNC | EVERY_AGG_FUNC;
			weaker = BOOL_OR_AGG_FUNC;
			break;

		case ROWCOMPARE_GE:
		case ROWCOMPARE_GT:
			stronger = MAX_AGG_FUNC | COUNT_AGG_FUNC;
			weaker = MIN_AGG_FUNC;
			break;

		default:
			stronger = weaker = NO_FUNC;
	}

	if (MatchesOnly(aggFuncs, weaker))
		return label;
	else if (MatchesOnly(aggFuncs, stronger))
		return oppositeDmlOp(label);
	else
		return INSERT_DELETE;
}


static int16
triggerOnEvents(AssertionInfo *info, Oid relationId)
{
	int16 result = 0;
	if (list_member_oid(info->inserts, relationId))
		result |= TRIGGER_TYPE_INSERT;
	if (list_member_oid(info->deletes, relationId))
		result |= TRIGGER_TYPE_DELETE | TRIGGER_TYPE_TRUNCATE;
	if (list_member_oid(info->updates, relationId))
		result |= TRIGGER_TYPE_UPDATE;
	return result;
}


/*
 * Returns a list of string nodes, suitable for use in the trigger
 * definition, that contain the column names to be triggered against
 * on UPDATE operations.
 */
static List *
triggerOnColumns(AssertionInfo *info, Oid relationId)
{
	List     	  *columns = NIL;
	ListCell 	  *cell;

	foreach(cell, info->columns)
	{
		ObjectAddress *column = (ObjectAddress *) lfirst(cell);
		AttrNumber attrNumber = (AttrNumber) column->objectSubId;

		if (column->objectId == relationId)
		{
			Value *name = makeString(get_attname(relationId, attrNumber, false));
			columns = lappend(columns, name);
		}
	}

	return columns;
}


static bool
listContainsObjectAddress(List *list, ObjectAddress *address)
{
	ListCell 	  *cell;
	ObjectAddress *item;

	foreach(cell, list)
	{
		item = (ObjectAddress *) lfirst(cell);
		if (item->classId == address->classId &&
			item->objectId == address->objectId &&
			item->objectSubId == address->objectSubId)
			return true;
	}

	return false;
}


static Bitmapset *
strategiesForOperators(List *operators)
{
	ListCell  *operatorCell;
	Bitmapset *strategies = NULL;

	foreach(operatorCell, operators)
	{
		Oid operator = lfirst_oid(operatorCell);
		List *interpretations = get_op_btree_interpretation(operator);
		ListCell *interpretationCell;

		foreach(interpretationCell, interpretations)
			strategies = bms_add_member(strategies,
										((OpBtreeInterpretation *) lfirst(interpretationCell))->strategy);
	}

	return strategies;
}


static int
functionsForTargetList(List *targetList)
{
	int functionMask = NO_FUNC;
	expression_tree_walker((Node *) targetList, visitAggrefNodes, &functionMask);
	return functionMask;
}


static int
funcMaskForFuncOid(Oid funcOid)
{
	char *name = get_func_name(funcOid);

	if (name == NULL)
		return OTHER_FUNC;
	else if (strncmp(name, "min", strlen("min")) == 0)
		return MIN_AGG_FUNC;
	else if (strncmp(name, "max", strlen("max")) == 0)
		return MAX_AGG_FUNC;
	else if (strncmp(name, "count", strlen("count")) == 0)
		return COUNT_AGG_FUNC;
	else if (strncmp(name, "every", strlen("every")) == 0)
		return EVERY_AGG_FUNC;
	else if (strncmp(name, "bool_and", strlen("bool_and")) == 0)
		return BOOL_AND_AGG_FUNC;
	else if (strncmp(name, "bool_or", strlen("bool_or")) == 0)
		return BOOL_OR_AGG_FUNC;
	else
		return OTHER_FUNC;
}


static Query *
queryForSQLFunction(FuncExpr *funcExpr)
{
	Oid			funcId;
	Relation 	procRel;
	HeapTuple	tuple;
	Datum		datum;
	bool		isNull;
	char	   *sql;
	List	   *rawParseTree;
	ParseState *pstate;
	Query	   *query;
	SQLFunctionParseInfoPtr pinfo;

	funcId = funcExpr->funcid;
	procRel = heap_open(ProcedureRelationId, ShareLock);

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcId);

	datum = SysCacheGetAttr(PROCOID,
							tuple,
							Anum_pg_proc_prosrc,
							&isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", funcId);

	sql = TextDatumGetCString(datum);
	rawParseTree = pg_parse_query(sql);

	pinfo = prepare_sql_fn_parse_info(tuple,
									  (Node *) funcExpr,
									  funcExpr->inputcollid);

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = sql;
	sql_fn_parser_setup(pstate, pinfo);

	query = transformTopLevelStmt(pstate, linitial(rawParseTree));
	free_parsestate(pstate);

	ReleaseSysCache(tuple);
	heap_close(procRel, NoLock);

	return query;
}


static bool
visitRangeTblRef(RangeTblRef *node, AssertionInfo *info)
{
	RangeTblEntry  *entry;
	Oid				relationId;
	RTEKind			rtekind;
	char 		    relkind;
	bool			result;

	entry = rt_fetch(node->rtindex, info->rtable);
	rtekind = entry->rtekind;
	result = false;

	if (rtekind == RTE_RELATION)
	{
		relationId = getrelid(node->rtindex, info->rtable);
		relkind = get_rel_relkind(relationId);

		if (relkind == RELKIND_RELATION)
		{
			if (info->label == INSERT || info->label == INSERT_DELETE)
				info->inserts = list_append_unique_oid(info->inserts, relationId);

			if (info->label == DELETE || info->label == INSERT_DELETE)
				info->deletes = list_append_unique_oid(info->deletes, relationId);

			if (!info->inView)
				info->dependencies = list_append_unique_oid(info->dependencies, relationId);

			info->relations = list_append_unique_oid(info->relations, relationId);

		}
		else if (relkind == RELKIND_VIEW)
		{
			Relation view = heap_open(relationId, AccessShareLock);
			Query *query = get_view_query(view);

			if (!info->inView)
				info->dependencies = list_append_unique_oid(info->dependencies, relationId);

			info->inView = true;
			result = visitQuery(query, info);

			heap_close(view, NoLock);
		}

	}
	else if (rtekind == RTE_TABLEFUNC)
	{
		result = visitAllNodes((Node *) entry->tablefunc, info);
	}
	else if (rtekind == RTE_FUNCTION)
	{
		result = visitAllNodes((Node *) entry->functions, info);
	}
	else if (rtekind == RTE_SUBQUERY)
	{
		result = visitQuery(entry->subquery, info);
	}

	return result;
}


static bool
visitQuery(Query *node, AssertionInfo *info)
{
	info->rtable = node->rtable;

	if (info->inComparison)
	{
		int functions = functionsForTargetList(node->targetList);
		Bitmapset *strategies = strategiesForOperators(info->operators);

		if (CanOptimise(strategies))
		{
			RowCompareType rowCompareType = (RowCompareType) bms_first_member(strategies);
			info->label = labelForComparisonWithAggFuncs(
				info->label,
				info->invert ? oppositeCompareType(rowCompareType) : rowCompareType,
				functions
			);
		}
		else
		{
			/*
			 * Either no btree interpretation was found for the operator(s), there were
			 * multiple interpretations that were incompatible with each other, or the
			 * found interpretations were not able to be optimised. We must therefore
			 * assume that both INSERT and DELETE operations may be invalidating.
			 */
			info->label = INSERT_DELETE;
		}
	}
	else if (node->hasWindowFuncs)
	{
		info->label = INSERT_DELETE;
	}

	return query_tree_walker(node,
							 visitAllNodes,
							 info,
							 QTW_IGNORE_RANGE_TABLE);
}


static bool
visitSetOperationStmt(SetOperationStmt *node, AssertionInfo *info)
{
	info->setOp = node->op;

	if (visitAllNodes(node->larg, info))
		return true;

	if (info->setOp == SETOP_EXCEPT)
		info->label = oppositeDmlOp(info->label);

	return visitAllNodes(node->rarg, info);
}


static bool
visitBoolExpr(BoolExpr *node, AssertionInfo *info)
{
	if (node->boolop == NOT_EXPR)
		info->label = oppositeDmlOp(info->label);

	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


// TODO refactor this function
static bool
visitSubLink(SubLink *node, AssertionInfo *info)
{
	List *operators;
	bool  invert;
	bool  inComparison;
	bool  inExists;

	switch (node->subLinkType)
	{
		case ARRAY_SUBLINK: // TODO write tests for this
		case EXPR_SUBLINK:
		{
			Expr *expr = info->expr;
			bool hasExpr = (expr != NULL);

			if (hasExpr && IsA(expr, OpExpr))
			{
				OpExpr *opExpr = (OpExpr *) expr;
				inComparison = true;
				inExists = false;

				/*
				 * Optimisation code is written under the assumption that the sub-select is the
				 * right operand. If it is the left operand the comparison needs to be inverted.
				 */
				invert = (list_nth_node(SubLink, opExpr->args, 0) == node);
				operators = list_make1_oid(opExpr->opno);
			}
			else if (hasExpr && IsA(expr, FuncExpr) &&
					((FuncExpr *)expr)->funcresulttype == BOOLOID)
			{
				/*
				 * We are inside a function invocation that returns Boolean but is not an OpExpr.
				 * Let's exploit the fact that "expr == TRUE -> expr", and pretend there is an
				 * equality operator.
				 */
				inComparison = true;
				inExists = false;
				operators = list_make1_oid(F_BOOLEQ);
				invert = false;
			}
			else if (exprType((const Node *) node) == BOOLOID)
			{
				/*
				 * We are _not_ inside either an OpExpr or FuncExpr, but we are a query that can be
				 * coerced to Boolean. We use the same logic above e.g. "expr == TRUE -> expr"
				 */
				inComparison = true;
				inExists = false;
				operators = list_make1_oid(F_BOOLEQ);
				invert = false;
			}
			else
			{
				inComparison = false;
				inExists = false;
				invert = false;
				operators = NIL;
			}
		}
		break;

		case ALL_SUBLINK:
		case ANY_SUBLINK:
		case ROWCOMPARE_SUBLINK:
		{
			if (IsA(node->testexpr, OpExpr))
			{
				OpExpr *expr = (OpExpr *) node->testexpr;
				inComparison = true;
				inExists = false;
				invert = false;
				operators = list_make1_oid(expr->opno);
			}
			else if (IsA(node->testexpr, RowCompareExpr))
			{
				RowCompareExpr *expr = (RowCompareExpr *) node->testexpr;
				inComparison = true;
				inExists = false;
				invert = false;
				operators = list_copy(expr->opnos);
			}
			else
			{
				inComparison = false;
				inExists = false;
				invert = false;
				operators = NIL;
			}
		}
		break;

		case EXISTS_SUBLINK:
		{
			/* existential quantification, no operators */
			inComparison = false;
			inExists = true;
			invert = false;
			operators = NIL;
		}
		break;
			// case MULTIEXPR_SUBLINK: // TODO write tests for MULTIEXPR_SUBLINK? -- or, can it only occur in UPDATEs?
		default:
		{
			elog(ERROR, "unhandled sublink type %u", node->subLinkType);
			return true;
		}

	}

	info->operators = operators;
	info->invert = invert;
	info->inComparison = inComparison;
	info->inExists = inExists;
	info->setOp = SETOP_NONE;

	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


static bool
visitTargetEntry(TargetEntry *node, AssertionInfo *info)
{
	info->inTargetEntry = true;
	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


static bool
visitFromExpr(FromExpr *node, AssertionInfo *info)
{
	info->inExists = false;
	info->inComparison = false;
	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


static bool
visitFuncExpr(FuncExpr *node, AssertionInfo *info)
{
	Oid lang = get_func_lang(node->funcid);

	info->expr = (Expr *) node;

	if (!(lang == INTERNALlanguageId || lang == SQLlanguageId))
	{
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("function \"%s\" uses unsupported language \"%s\"",
				   get_func_name(node->funcid), get_language_name(lang, false))));
		return true;
	}

	if (expression_tree_walker((Node *) node, visitAllNodes, (void *) info))
		return true;

	if (lang == SQLlanguageId)
	{
		Query *query = queryForSQLFunction(node);
		Node *next;
		if (node->funcretset)
			next = (Node *) query;
		else
			next = (Node *) ((TargetEntry *) linitial(query->targetList))->expr;

		return visitAllNodes(next, info);
	}

	return false;
}


static bool
visitExpr(Expr *node, AssertionInfo *info)
{
	info->expr = node;
	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


static bool
visitVar(Var *node, AssertionInfo *info)
{
	RangeTblEntry *entry = rt_fetch(node->varno, info->rtable);

	// TODO we should do this only for Tables and not Views?
	// TODO optimisations for set-operations
	if (entry->rtekind == RTE_RELATION && !(info->inExists && info->setOp == SETOP_NONE))
	{
		Oid relationId = getrelid(node->varno, info->rtable);
		ObjectAddress *column = (ObjectAddress *) palloc(sizeof(ObjectAddress));

		column->classId = RelationRelationId;
		column->objectId = relationId;
		column->objectSubId = node->varattno;

		if (!listContainsObjectAddress(info->columns, column))
			info->columns = lcons(column, info->columns);

		info->relations = list_append_unique_oid(info->relations, relationId);
		info->updates = list_append_unique_oid(info->updates, relationId);
	}

	return expression_tree_walker((Node *) node, visitAllNodes, (void *) info);
}


static bool
visitAllNodes(Node *node, AssertionInfo *info)
{
	AssertionInfo stored;
	bool result;

	copyAssertionInfo(&stored, info);

	if (node == NULL)
		result = false;
	else if (IsA(node, RangeTblRef))
		result = visitRangeTblRef((RangeTblRef *) node, info);
	else if (IsA(node, Query))
		result = visitQuery((Query *) node, info);
	else if (IsA(node, BoolExpr))
		result = visitBoolExpr((BoolExpr *) node, info);
	else if (IsA(node, SubLink))
		result = visitSubLink((SubLink *) node, info);
	else if (IsA(node, SetOperationStmt))
		result = visitSetOperationStmt((SetOperationStmt *) node, info);
	else if (IsA(node, FuncExpr))
		result = visitFuncExpr((FuncExpr *) node, info);
	else if (IsA(node, OpExpr) ||
			 IsA(node, DistinctExpr) ||
			 IsA(node, NullIfExpr))
		result = visitExpr((Expr *) node, info);
	else if (IsA(node, TargetEntry))
		result = visitTargetEntry((TargetEntry *) node, info);
	else if (IsA(node, Var))
		result = visitVar((Var *) node, info);
	else if (IsA(node, FromExpr))
		result = visitFromExpr((FromExpr *) node, info);
	else
		result = expression_tree_walker(node, visitAllNodes, (void *) info);

	copyAssertionInfo(info, &stored);

	return result;
}


static bool
visitAggrefNodes(Node *node, int *aggFuncs)
{
	if (node == NULL)
		return false;
	else if (IsA(node, Aggref))
		return visitAggref((Aggref *) node, aggFuncs);
	else if(IsA(node, WindowFunc))
		return visitWindowFunc((WindowFunc *) node, aggFuncs);
	else
		return expression_tree_walker(node, visitAggrefNodes, (void *) aggFuncs);
}


static bool
visitAggref(Aggref *node, int *aggFuncs)
{
	*aggFuncs |= funcMaskForFuncOid(node->aggfnoid);
	return expression_tree_walker((Node *) node, visitAggrefNodes, (void *) aggFuncs);
}


static bool
visitWindowFunc(WindowFunc *node, int *aggFuncs)
{
	*aggFuncs |= funcMaskForFuncOid(node->winfnoid);

	return expression_tree_walker((Node *) node, visitAggrefNodes, (void *) aggFuncs);
}


ObjectAddress
CreateAssertion(CreateAssertionStmt *stmt)
{
	Oid namespaceId;
	char *assertion_name;
	AclResult aclresult;
	Node *expr;
	ParseState *pstate;
	char *ccsrc;
	char *ccbin;
	Oid constrOid;
	AssertionInfo info;
	ListCell *lc;
	ObjectAddress address;

	namespaceId = QualifiedNameGetCreationNamespace(stmt->assertion_name,
													&assertion_name);

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_SCHEMA,
					   get_namespace_name(namespaceId));

	if (ConstraintNameIsUsed(CONSTRAINT_ASSERTION, InvalidOid, namespaceId, assertion_name))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("assertion \"%s\" already exists", assertion_name)));

	pstate = make_parsestate(NULL);
	expr = transformExpr(pstate, stmt->constraint->raw_expr, EXPR_KIND_ASSERTION_CHECK);
	expr = coerce_to_boolean(pstate, expr, "CHECK");

	ccbin = nodeToString(expr);
	ccsrc = deparse_expression(expr, NIL, false, false);

	constrOid = CreateConstraintEntry(assertion_name,
									  namespaceId,
									  CONSTRAINT_CHECK, /* constraint type */
									  stmt->constraint->deferrable,
									  stmt->constraint->initdeferred,
									  !stmt->constraint->skip_validation,
									  InvalidOid, /* no parent constraint */
									  InvalidOid, /* not a relation constraint */
									  NULL,       /* no keys */
									  0,          /* no keys */
									  0,          /* no keys */
									  InvalidOid, /* not a domain constraint */
									  InvalidOid, /* no associated index */
									  InvalidOid, /* foreign key fields ... */
									  NULL,
									  NULL,
									  NULL,
									  NULL,
									  0,
									  ' ',
									  ' ',
									  ' ',
									  NULL,    /* not an exclusion constraint */
									  expr, /* tree form of check constraint */
									  ccbin, /* binary form of check constraint */
									  ccsrc, /* source form of check constraint */
									  true, /* is local */
									  0,   /* inhcount */
									  false, /* noinherit XXX */
									  false); /* is_internal */

	initAssertionInfo(&info);
	visitAllNodes(expr, &info);

	foreach (lc, info.relations)
	{
		Oid relationId = lfirst_oid(lc);
		CreateTrigStmt *trigger;
		Relation rel;

		rel = heap_open(relationId, ShareLock); // XXX

		trigger = makeNode(CreateTrigStmt);
		trigger->trigname = "AssertionTrigger";
		trigger->relation = makeRangeVar(get_namespace_name(namespaceId),
										 pstrdup(RelationGetRelationName(rel)),
										 -1);
		trigger->funcname = SystemFuncName("assertion_check");
		trigger->args = NIL;
		trigger->row = false;
		trigger->timing = TRIGGER_TYPE_AFTER;
		trigger->events = triggerOnEvents(&info, relationId);
		trigger->columns = triggerOnColumns(&info, relationId);
		trigger->whenClause = NULL;
		trigger->isconstraint = true;
		trigger->deferrable = stmt->constraint->deferrable;
		trigger->initdeferred = stmt->constraint->initdeferred;
		trigger->constrrel = NULL;

		CreateTrigger(trigger,
		              NULL,
			      InvalidOid,
			      relationId,
			      constrOid,
			      InvalidOid, /* no index */
			      InvalidOid, /* no func - use trigger->funcname */
			      InvalidOid, /* no parent */
			      NULL,       /* no when */
			      true,       /* is_internal */
			      false);

		heap_close(rel, NoLock);
	}

	/*
	 * Record dependencies between the constraint and the relations found in the
	 * top-level expression. Dependencies to specific columns will already have
	 * been recorded by the trigger creation.
	 */
	ObjectAddress myself, referenced;
	ListCell *cell;

	myself.classId = ConstraintRelationId;
	myself.objectId = constrOid;
	myself.objectSubId = 0;

	foreach (cell, info.dependencies)
	{
		referenced.classId = RelationRelationId;
		referenced.objectId = lfirst_oid(cell);
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	test_assertion_expr(assertion_name, ccsrc);

	ObjectAddressSet(address, ConstraintRelationId, constrOid);

	return address;
}


ObjectAddress
RenameAssertion(RenameStmt *stmt)
{
	Oid           assertionOid;
	ObjectAddress address;
	Relation      rel;
	HeapTuple     tuple;
	Form_pg_constraint con;
	AclResult     aclresult;

	List *oldName = castNode(List, stmt->object);
	char *newName = stmt->newname;

	rel = heap_open(ConstraintRelationId, RowExclusiveLock);
	assertionOid = get_assertion_oid(oldName, false);

	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(assertionOid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for constraint %u",
			 assertionOid);
	con = (Form_pg_constraint) GETSTRUCT(tuple);

	if (!pg_constraint_ownercheck(assertionOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_CONVERSION,
					   NameListToString(oldName));

	aclresult = pg_namespace_aclcheck(con->connamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_SCHEMA,
					   get_namespace_name(con->connamespace));

	ReleaseSysCache(tuple);
	RenameConstraintById(assertionOid, newName);
	ObjectAddressSet(address, ConstraintRelationId, assertionOid);
	heap_close(rel, NoLock);

	return address;
}
