/*-------------------------------------------------------------------------
 *
 * schemavariable.c
 *	  schema variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/schemavariable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_variable.h"

#include "commands/schemavariable.h"

#include "executor/executor.h"
#include "executor/svariableReceiver.h"

#include "optimizer/planner.h"

#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"

#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Stack of variable's versions
 */
typedef struct StackedValue
{
	Datum		value;
	bool		isnull;
	bool		is_valid;
	bool		freeval;

	SubTransactionId creating_subid;

	struct StackedValue *prev_value;
} StackedValue;

/*
 * ON COMMIT action list
 */
typedef struct OnCommitItem
{
	Oid			varid;			/* relid of relation */
	VariableEOXAction eoxaction;	/* what to do at end of xact */
	bool		is_stacked;		/* varid is transactional */

	/*
	 * If this entry was created during the current transaction,
	 * creating_subid is the ID of the creating subxact; if created in a prior
	 * transaction, creating_subid is zero.  If deleted during the current
	 * transaction, deleting_subid is the ID of the deleting subxact; if no
	 * deletion request is pending, deleting_subid is zero.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
} OnCommitItem;

static List *on_commits = NIL;

/*
 * The content of variables is not transactional. Due this fact the
 * implementation of DROP can be simple, because although DROP VARIABLE
 * can be reverted, the content of variable can be lost. In this example,
 * DROP VARIABLE is same like reset variable.
 */

typedef struct SchemaVariableData
{
	Oid			varid;			/* pg_variable OID of this sequence (hash key) */
	Oid			typid;			/* OID of the data type */
	int16		typlen;
	bool		typbyval;
	bool		isnull;
	bool		freeval;
	Datum		value;
	bool		is_rowtype;		/* true when variable is composite */
	bool		is_valid;		/* true when variable was successfuly initialized */
	bool		reset_auto;		/* next read fix invalidate value by self */

	bool		is_not_null;	/* don't allow null values */
	bool		is_stacked;	/* emulate MVCC */
	bool		has_defexpr;	/* true, when there are default value */

	SubTransactionId creating_subid;

	/* value stored for possible rollback, rollback to x */
	StackedValue *prev_value;
} SchemaVariableData;

typedef SchemaVariableData *SchemaVariable;

static HTAB *schemavarhashtab = NULL;		/* hash table for session variables */
static MemoryContext SchemaVariableMemoryContext = NULL;

static bool first_time = true;
static bool clean_cache_req = false;

static void create_schema_variable_hashtable(void);
static void clean_cache_callback(XactEvent event, void *arg);
static void free_schema_variable(SchemaVariable svar, bool force);
static void set_schema_variable(SchemaVariable svar, Datum value, bool isnull, Oid typid);
static void init_schema_variable(SchemaVariable svar, Variable *var);
static SchemaVariable prepare_variable_for_reading(Oid varid, bool reset);
static void remove_variable_on_commit_actions(Oid varid, VariableEOXAction eoxaction);
static void register_transact_variable_on_commit_action(Oid varid, SubTransactionId creating_subid);
static void clean_cache_variable(Oid varid);

/*
 * Save info about necessity to clean hash table, because some
 * schema variable was dropped. Don't do here more, recheck
 * needs to be in transaction state.
 */
static void
InvalidateSchemaVariableCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
	if (cacheid != VARIABLEOID)
		return;

	clean_cache_req = true;
}

/*
 * Recheck existence of all schema variables against system catalog.
 * When instance of schema variables (in memory) has not own entry
 * inside related system catalog, remove cache of schema variables.
 */
static void
clean_cache_callback(XactEvent event, void *arg)
{
	/*
	 * should continue only in transaction time, when
	 * syscache is available.
	 */
	if (clean_cache_req && IsTransactionState())
	{
		HASH_SEQ_STATUS status;
		SchemaVariable		svar;

		if (!schemavarhashtab)
			return;

		hash_seq_init(&status, schemavarhashtab);

		while ((svar = (SchemaVariable) hash_seq_search(&status)) != NULL)
		{
			HeapTuple	tp;

			tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));
			if (!HeapTupleIsValid(tp))
			{
				elog(DEBUG1, "variable %d is removed from cache", svar->varid);

				free_schema_variable(svar, true);

				remove_variable_on_commit_actions(svar->varid, VARIABLE_EOX_DROP);

				if (hash_search(schemavarhashtab,
									(void *) &svar->varid,
									HASH_REMOVE,
									NULL) == NULL)
					elog(DEBUG1, "hash table corrupted");
			}
			else
				ReleaseSysCache(tp);
		}

		clean_cache_req = false;
	}
}

/*
 * Create the hash table for storing schema variables
 */
static void
create_schema_variable_hashtable(void)
{
	HASHCTL		ctl;

	/* set callbacks */
	if (first_time)
	{
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  InvalidateSchemaVariableCacheCallback,
									  (Datum) 0);

		RegisterXactCallback(clean_cache_callback, NULL);

		first_time = false;
	}

	/* needs own long life memory context */
	if (SchemaVariableMemoryContext == NULL)
	{
		SchemaVariableMemoryContext =
						AllocSetContextCreate(TopMemoryContext,
											  "schema variables",
											  ALLOCSET_START_SMALL_SIZES);
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SchemaVariableData);
	ctl.hcxt = SchemaVariableMemoryContext;

	schemavarhashtab = hash_create("Schema variables", 64, &ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Fast drop complete content of schema variables
 */
void
ResetSchemaVariableCache(void)
{
	/* Remove temporal schema variables */
	AtPreEOXact_SchemaVariable_on_commit_actions(true);

	/* Destroy hash table and reset related memory context */
	if (schemavarhashtab)
	{
		hash_destroy(schemavarhashtab);
		schemavarhashtab = NULL;
	}

	/* Release memory allocated by schema variables */
	if (SchemaVariableMemoryContext != NULL)
		MemoryContextReset(SchemaVariableMemoryContext);
}

/*
 * Release data stored inside svar. When a variable is transactional,
 * and was not modified already in this transaction, then it archives
 * current value for possible future usage on rollback.
 *
 * When force is true, then release current and possibly archived value.
 */
static void
free_schema_variable(SchemaVariable svar, bool force)
{
	SubTransactionId	current_subid;

	if (force)
	{
		StackedValue *stacked_var = svar->prev_value;

		if (svar->freeval)
			pfree(DatumGetPointer(svar->value));

		/* release history of all values of variable */
		while (stacked_var)
		{
			StackedValue	   *current_var = stacked_var;

			if (current_var->freeval)
				pfree(DatumGetPointer(current_var->value));

			stacked_var = current_var->prev_value;

			pfree(current_var);
		}

		svar->freeval = false;
		svar->is_valid = false;

		svar->prev_value = NULL;

		return;
	}

	/* The transact variable should not be released, when it is archived. */
	if (svar->is_stacked &&
			svar->creating_subid != (current_subid = GetCurrentSubTransactionId()))
	{
		StackedValue *prev_value;

		prev_value = MemoryContextAlloc(SchemaVariableMemoryContext,
										sizeof(StackedValue));

		prev_value->value = svar->value;
		prev_value->isnull = svar->isnull;
		prev_value->is_valid = svar->is_valid;
		prev_value->freeval = svar->freeval;
		prev_value->creating_subid = svar->creating_subid;
		prev_value->prev_value = svar->prev_value;

		svar->prev_value = prev_value;
		svar->creating_subid = current_subid;

		/*
		 * In this moment, we would to register or update (change
		 * the creating_subid) OnCommit event. Now, some rollback
		 * is possible, and on commit action is necessary to release
		 * stacked variable.
		 */
		register_transact_variable_on_commit_action(svar->varid, current_subid);
	}
	else
	{
		if (svar->freeval)
			pfree(DatumGetPointer(svar->value));
	}

	/* Clean current value, and mark it as invalid */
	svar->value = (Datum) 0;
	svar->isnull = true;
	svar->freeval = false;

	svar->is_valid = false;
}

/*
 * Assign some content to the schema variable. It does copy to
 * SchemaVariableMemoryContext if it is necessary
 */
static void
set_schema_variable(SchemaVariable svar, Datum value, bool isnull, Oid typid)
{
	MemoryContext	oldcxt;
	Datum			newval = value;

	/* Don't allow assign null to NOT NULL variable */
	if (isnull && svar->is_not_null)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("null value is not allowed for NOT NULL schema variable \"%s\"",
					schema_variable_get_name(svar->varid))));


	if (!isnull && svar->typid != typid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type \"%s\" of assigned value is different than type \"%s\" of schema variable \"%s\"",
						format_type_be(typid),
						format_type_be(svar->typid),
						schema_variable_get_name(svar->varid))));

	/* copy value to session persistent context */
	oldcxt = MemoryContextSwitchTo(SchemaVariableMemoryContext);
	if (!isnull)
		newval = datumCopy(value, svar->typbyval, svar->typlen);
	MemoryContextSwitchTo(oldcxt);

	free_schema_variable(svar, false);

	svar->value = newval;
	svar->isnull = isnull;
	svar->freeval = newval != value;
	svar->is_valid = true;
	svar->reset_auto = false;
}

/*
 * Initialize svar from var
 * svar - SchemaVariable - holds data
 * var  - Variable - holds metadata
 */
static void
init_schema_variable(SchemaVariable svar, Variable *var)
{
	Assert(OidIsValid(var->oid));

	svar->varid = var->oid;
	svar->typid = var->typid;

	get_typlenbyval(var->typid,
								&svar->typlen,
								&svar->typbyval);

	svar->isnull = true;
	svar->freeval = false;
	svar->value = (Datum) 0;

	svar->is_rowtype = type_is_rowtype(var->typid);
	svar->is_not_null = var->is_not_null;

	/* transaction schema variable has stacked content */
	svar->is_stacked = var->is_transact;
	svar->has_defexpr = var->has_defexpr;

	/* the variable initialization was not complete here */
	svar->is_valid = false;
	svar->reset_auto = false;

	svar->creating_subid = InvalidSubTransactionId;
	svar->prev_value = NULL;
}

/*
 * Revert changes of last subtransaction. Restore value from
 * stacked values. Returns creating_subid of this restored value.
 */
static SubTransactionId
rollback_transact_schema_variable(SchemaVariable svar)
{
	StackedValue *prev_value;

	prev_value = svar->prev_value;

	Assert(svar->is_stacked);
	Assert(prev_value != NULL);

	if (svar->freeval)
		pfree(DatumGetPointer(svar->value));

	svar->is_valid = prev_value->is_valid;
	svar->value = prev_value->value;
	svar->isnull = prev_value->isnull;
	svar->freeval = prev_value->freeval;

	svar->creating_subid = prev_value->creating_subid;
	svar->prev_value = prev_value->prev_value;

	/*
	 * allow reinit when variable is not valid and creating_subid
	 * is InvalidSubTransactionId - the variable was not initialized
	 * before reverted subtransaction.
	 */
	svar->reset_auto = !svar->is_valid &&
						svar->creating_subid == InvalidSubTransactionId;

	pfree(prev_value);

	return svar->creating_subid;
}

/*
 * Revert changes of last subtransaction. Returns subtransaction id
 * of previous value.
 */
static SubTransactionId
rollback_transact_variable(Oid varid)
{
	SchemaVariable	svar;
	bool			found;

	Assert(schemavarhashtab != NULL);

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
													  HASH_ENTER, &found);

	if (found)
		return rollback_transact_schema_variable(svar);
	else
		return InvalidSubTransactionId;
}

/*
 * Try to search value in hash table. If doesn't
 * exists insert it (and calculate defexpr if exists.
 * When reset is true, we would to enforce calculate
 * defexpr. When some is wrong, then this function try
 * don't break previous value.
 */
static SchemaVariable
prepare_variable_for_reading(Oid varid, bool reset)
{
	SchemaVariable	svar;
	Variable		var;
	bool			found;

	var.oid = InvalidOid;

	if (schemavarhashtab == NULL)
		create_schema_variable_hashtable();

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
													  HASH_ENTER, &found);

	/* Return content if it is available, and reset is not required */
	if (found && !reset && !svar->reset_auto)
	{
		/* raise exception when content is not valid */
		if (!svar->is_valid)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("variable \"%s\" has not valid content",
							schema_variable_get_name(varid)),
			errhint("Overwrite the content of variable or assign DEFAULT again.")));

		return svar;
	}

	/* We need to load defexpr. */
	initVariable(&var, varid, false, false);

	if (!found)
		init_schema_variable(svar, &var);

	/* Raise a error when we cannot to initialize variable correctly */
	if (var.is_not_null && !var.defexpr)
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("null value is not allowed for NOT NULL schema variable \"%s\"",
					schema_variable_get_name(varid)),
			 errdetail("The schema variable was not initialized yet.")));

	if (!found)
		register_variable_on_commit_action(varid, var.eoxaction);

	if (svar->has_defexpr)
	{
		Datum		value = (Datum) 0;
		bool		isnull;
		EState	   *estate = NULL;
		Expr	   *defexpr;
		ExprState  *defexprs;
		MemoryContext	oldcxt;

		/* Prepare default expr */
		estate = CreateExecutorState();

		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		defexpr = expression_planner((Expr *) var.defexpr);
		defexprs = ExecInitExpr(defexpr, NULL);
		value = ExecEvalExprSwitchContext(defexprs,
											GetPerTupleExprContext(estate),
											&isnull);


		/* Store result before releasing Executor memory */
		set_schema_variable(svar, value, isnull, svar->typid);

		MemoryContextSwitchTo(oldcxt);

		FreeExecutorState(estate);
	}
	else
		set_schema_variable(svar, (Datum) 0, true, svar->typid);

	return svar;
}

/*
 * Write value to variable. We expect secured access in this moment.
 * We try to don't break previous value, if some is wrong.
 */
void
SetSchemaVariable(Oid varid, Datum value, bool isNull, Oid typid)
{
	SchemaVariable svar;
	bool		found;

	if (schemavarhashtab == NULL)
		create_schema_variable_hashtable();

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
											  HASH_ENTER, &found);

	/* Initialize svar when was not initialized or when stored value is null */
	if (!found)
	{
		Variable var;

		/* don't need defexpr and acl here */
		initVariable(&var, varid, false, true);
		init_schema_variable(svar, &var);
		register_variable_on_commit_action(varid, var.eoxaction);
	}

	set_schema_variable(svar, value, isNull, typid);
}

/*
 * Returns copy of value of schema variable spcified by varid
 */
Datum
CopySchemaVariable(Oid varid, bool *isNull, Oid *typid)
{
	SchemaVariable	svar;

	svar = prepare_variable_for_reading(varid, false);
	Assert(svar != NULL && svar->is_valid);

	*isNull = svar->isnull;
	*typid = svar->typid;

	if (!svar->isnull)
		return datumCopy(svar->value, svar->typbyval, svar->typlen);

	return (Datum) 0;
}

/*
 * Returns value of schema variable specified by varid. Check correct
 * result type. Optionaly the result can be copy.
 */
Datum
GetSchemaVariable(Oid varid, bool *isNull, Oid expected_typid, bool copy)
{
	SchemaVariable	svar;
	Datum			value;
	bool			isnull;

	svar = prepare_variable_for_reading(varid, false);
	Assert(svar != NULL);

	if (expected_typid != svar->typid)
		elog(ERROR, "type of variable \"%s\" is different than expected",
				   schema_variable_get_name(varid));

	value = svar->value;
	isnull = svar->isnull;

	*isNull = isnull;

	if (!isnull && copy)
		return datumCopy(value, svar->typbyval, svar->typlen);

	return value;
}

/*
 * Clean variable defined by varid
 */
static void
clean_cache_variable(Oid varid)
{
	SchemaVariable	svar;
	bool			found;

	if (schemavarhashtab == NULL)
		return;

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
											  HASH_FIND, &found);
	if (found)
	{
		/* clean content, if it is necessary */
		free_schema_variable(svar, true);

		if (hash_search(schemavarhashtab,
							(void *) &svar->varid,
							HASH_REMOVE,
							NULL) == NULL)
			elog(DEBUG1, "hash table corrupted");
	}
}

/*
 * Drop variable by OID
 */
void
RemoveSchemaVariable(Oid varid)
{
	Relation	rel;
	HeapTuple	tup;

	rel = heap_open(VariableRelationId, RowExclusiveLock);

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(rel, RowExclusiveLock);

	clean_cache_variable(varid);

	/* remove variable from on_commits list */
	remove_variable_on_commit_actions(varid, VARIABLE_EOX_DROP);
}

/*
 * Reset to default specified schema variable
 */
void
doLetStmtReset(PlannedStmt *pstmt)
{
	LetStmt *stmt = (LetStmt *) pstmt->utilityStmt;

	Assert(stmt->to_null || stmt->to_default);

	if (stmt->to_default)
	{
		/* recalculate default expression */
		prepare_variable_for_reading(pstmt->resultVariable, true);

	}
	else if (stmt->to_null)
	{
		/* set to null */
		SetSchemaVariable(pstmt->resultVariable,
							(Datum) 0,
							true,
							InvalidOid);
	}
}

/*
 * Assign result of evaluated expression to schema variable
 */
void
doLetStmtEval(PlannedStmt *pstmt,
		  ParamListInfo params,
		  QueryEnvironment *queryEnv,
		  const char *queryString)
{
	QueryDesc *queryDesc;
	DestReceiver *dest;

	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create dest receiver for LET */
	dest = CreateDestReceiver(DestVariable);

	SetVariableDestReceiverParams(dest, pstmt->resultVariable);

	/* Create a QueryDesc requesting no output */
	queryDesc = CreateQueryDesc(pstmt, queryString,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, params, queryEnv, 0);

	ExecutorStart(queryDesc, 0);
	ExecutorRun(queryDesc, ForwardScanDirection, 2L, true);
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
}

/*
 * Creates new variable - entry in pg_catalog.pg_variable table
 *
 * Used by CREATE VARIABLE command
 */
ObjectAddress
DefineSchemaVariable(ParseState *pstate, CreateSchemaVarStmt *stmt)
{
	Oid			namespaceid;
	AclResult	aclresult;
	Oid			typid;
	int32		typmod;
	Oid			varowner = GetUserId();
	Oid			collation;
	Oid			typcollation;
	ObjectAddress variable;

	Node	   *cooked_default = NULL;

	/*
	 * Check consistency of arguments
	 */
	if (stmt->eoxaction == VARIABLE_EOX_DROP
			&& stmt->variable->relpersistence != RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("ON COMMIT DROP can only be used on temporary variables")));

	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->variable, NoLock, NULL);

	typenameTypeIdAndMod(pstate, stmt->typeName, &typid, &typmod);
	typcollation = get_typcollation(typid);

	aclresult = pg_type_aclcheck(typid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typid);

	if (stmt->collClause)
		collation = LookupCollation(pstate,
									stmt->collClause->collname,
									stmt->collClause->location);
	else
		collation = typcollation;;

	/* Complain if COLLATE is applied to an uncollatable type */
	if (OidIsValid(collation) && !OidIsValid(typcollation))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						format_type_be(typid)),
				 parser_errposition(pstate, stmt->collClause->location)));

	if (stmt->defexpr)
	{
		cooked_default = transformExpr(pstate, stmt->defexpr,
										EXPR_KIND_VARIABLE_DEFAULT);

		cooked_default = coerce_to_specific_type(pstate,
												 cooked_default, typid, "DEFAULT");
		assign_expr_collations(pstate, cooked_default);
	}

	variable = VariableCreate(stmt->variable->relname,
						  namespaceid,
						  typid,
						  typmod,
						  varowner,
						  collation,
						  cooked_default,
						  stmt->eoxaction,
						  stmt->is_not_null,
						  stmt->is_transact,
						  stmt->if_not_exists);

	/*
	 * We must bump the command counter to make the newly-created variable
	 * tuple visible for any other operations.
	 */
	CommandCounterIncrement();

	return variable;
}

/*
 * Register a newly-created variable ON COMMIT action.
 */
void
register_variable_on_commit_action(Oid varid,
								  VariableEOXAction action)
{
	OnCommitItem *oc;
	MemoryContext oldcxt;

	/*
	 * We needn't bother registering the relation unless there is an ON COMMIT
	 * action we need to take.
	 */
	if (action == VARIABLE_EOX_NOOP)
		return;

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	oc = (OnCommitItem *) palloc(sizeof(OnCommitItem));
	oc->varid = varid;
	oc->eoxaction = action;
	oc->is_stacked = false;

	oc->creating_subid = GetCurrentSubTransactionId();
	oc->deleting_subid = InvalidSubTransactionId;

	on_commits = lcons(oc, on_commits);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Register or reregister a transact schema variable created inside
 * creating_subid subtransaction. After this function we have joined
 * this subtransaction id with specified variable. When this subtransaction
 * is aborted, we revert schema variable to previous value.
 */
static void
register_transact_variable_on_commit_action(Oid varid,
								   SubTransactionId creating_subid)
{
	OnCommitItem *oc;
	ListCell	   *l;
	MemoryContext oldcxt;

	/* try to find already registered transaction variable */
	foreach(l, on_commits)
	{
		oc = (OnCommitItem *) lfirst(l);

		if (oc->varid == varid && oc->is_stacked)
		{
			oc->creating_subid = creating_subid;
			return;
		}
	}

	/* Create new transact on commit event */
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	oc = (OnCommitItem *) palloc(sizeof(OnCommitItem));
	oc->varid = varid;
	oc->eoxaction = VARIABLE_EOX_NOOP;
	oc->is_stacked = true;

	oc->creating_subid = creating_subid;
	oc->deleting_subid = InvalidSubTransactionId;

	on_commits = lcons(oc, on_commits);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Remove variable from on_commits action
 */
static void
remove_variable_on_commit_actions(Oid varid,
								  VariableEOXAction eoxaction)
{
	ListCell	   *l;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		if (oc->varid == varid)
		{
			if (eoxaction == VARIABLE_EOX_DROP ||
					(oc->eoxaction == VARIABLE_EOX_RESET &&
					eoxaction == VARIABLE_EOX_RESET))
			oc->deleting_subid = GetCurrentSubTransactionId();
		}
	}
}

/*
 * Finalize value of transact variable
 */
static void
complete_transact_variable(Oid varid, bool isCommit)
{
	SchemaVariable	svar;
	bool			found;

	if (schemavarhashtab == NULL)
		return;

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
											  HASH_FIND, &found);

	if (found)
	{
		if (svar->creating_subid != InvalidSubTransactionId)
		{
			if (isCommit)
			{
				StackedValue *prev_value = svar->prev_value;

				/* release archived value if there are any */
				while (prev_value)
				{
					StackedValue *current_value = prev_value;

					if (current_value->freeval)
						pfree(DatumGetPointer(current_value->value));
					prev_value = current_value->prev_value;
					pfree(current_value);
				}

				svar->prev_value = NULL;
				svar->creating_subid = InvalidSubTransactionId;
			}
			else
			{
				SubTransactionId	subid PG_USED_FOR_ASSERTS_ONLY;

#ifndef USE_ASSERT_CHECKING

				rollback_transact_schema_variable(svar);

#else

				subid = rollback_transact_schema_variable(svar);

#endif

				/* subtransactions should be reverted before */
				Assert(subid == InvalidSubTransactionId);
				Assert(svar->prev_value == NULL);
			}
		}
	}
}

/*
 * ensure revert value of transaction variables updated in specified
 * subtransaction.
 */
void
AtSubAbort_SchemaVariable(SubTransactionId aborted_subid)
{
	ListCell	   *l;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		if (oc->is_stacked && oc->creating_subid == aborted_subid)
			oc->creating_subid = rollback_transact_variable(oc->varid);
	}
}

/*
 * Perform ON TRANSACTION END RESET, ON COMMIT DROP
 * and COMMIT/ROLLBACK of transaction schema variables.
 */
void
AtPreEOXact_SchemaVariable_on_commit_actions(bool isCommit)
{
	ListCell	   *l;

	foreach(l, on_commits)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(l);

		if (oc->is_stacked && oc->creating_subid != InvalidSubTransactionId)
		{
			complete_transact_variable(oc->varid, isCommit);
			continue;
		}

		/* Ignore entry if already dropped in this xact */
		if (oc->deleting_subid != InvalidSubTransactionId)
			continue;

		switch (oc->eoxaction)
		{
			case VARIABLE_EOX_NOOP:
				/* Do nothing */
				break;
			case VARIABLE_EOX_RESET:
				clean_cache_variable(oc->varid);
				remove_variable_on_commit_actions(oc->varid,
												  VARIABLE_EOX_RESET);
				break;
			case VARIABLE_EOX_DROP:
				{
					/*
					 * ON COMMIT DROP is allowed only for temp schema variables.
					 * So we should explicit delete only when current transaction
					 * was committed. When is rollback, then schema variable is
					 * removed automatically.
					 */
					if (isCommit)
					{
						ObjectAddress object;

						object.classId = VariableRelationId;
						object.objectId = oc->varid;
						object.objectSubId = 0;

						/*
						 * Since this is an automatic drop, rather than one
						 * directly initiated by the user, we pass the
						 * PERFORM_DELETION_INTERNAL flag.
						 */
						performDeletion(&object,
										DROP_CASCADE, PERFORM_DELETION_INTERNAL);
					}
				}
				break;
		}
	}
}

/*
 * Post-commit or post-abort cleanup for ON COMMIT management.
 *
 * All we do here is remove no-longer-needed OnCommitItem entries.
 *
 */
void
AtEOXact_SchemaVariable_on_commit_actions(bool isCommit)
{
	ListCell   *cur_item;
	ListCell   *prev_item;

	prev_item = NULL;
	cur_item = list_head(on_commits);

	while (cur_item != NULL)
	{
		OnCommitItem *oc = (OnCommitItem *) lfirst(cur_item);

		/*
		 * During commit, remove entries that were deleted during
		 * this transaction; during abort, remove those created during this
		 * transaction.
		 *
		 * The transact variable event is removed every time.
		 */

		if ((isCommit ? oc->deleting_subid != InvalidSubTransactionId :
					oc->creating_subid != InvalidSubTransactionId) ||
			 oc->is_stacked)
		{
			on_commits = list_delete_cell(on_commits, cur_item, prev_item);
			pfree(oc);
			if (prev_item)
				cur_item = lnext(prev_item);
			else
				cur_item = list_head(on_commits);
		}
		else
		{
			oc->creating_subid = InvalidSubTransactionId;
			oc->deleting_subid = InvalidSubTransactionId;
			prev_item = cur_item;
			cur_item = lnext(prev_item);
		}
	}
}
