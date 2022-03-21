/*-------------------------------------------------------------------------
 *
 * sessionvariable.c
 *	  session variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/sessionvariable.c
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
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
#include "executor/executor.h"
#include "executor/svariableReceiver.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Values of session variables are stored in local memory, in
 * sessionvars hash table. This local memory has to be cleaned,
 * when:
 * - a session variable is dropped by the current or another
 * session
 * - a user enforce it by using the ON TRANSACTION END RESET
 * clause. The life cycle of temporary session variable can be
 * limmited by using clause ON COMMIT DROP.
 *
 * Although session variables are not transactional, we don't want
 * (and cannot) clean the entries in sessionvars hash table
 * immediately, when we get the sinval message.  Session variables
 * usage is protected by heavyweight locks, so there is no risk of
 * unwanted invalidation due to a drop variable done in a
 * different session. But it's still possible to drop the session
 * variable in the current session. Without delayed cleanup we
 * would lose the value if the drop command is done in a sub
 * transaction that is then rollbacked.  The check of session
 * variable validity requires access to system catalog, so it can
 * only be done in transaction state).
 *
 * This is why memory cleanup (session variable reset) is
 * postponed to the end of transaction, and why we need to hold
 * some actions lists. We have to hold two separate action lists:
 * one for dropping the session variable from system catalog, and
 * another one for resetting its value. Both are necessary, since
 * dropping a session variable also needs to enforce a reset of
 * the value. The drop operation can be executed when we iterate
 * over the action list, and at that moment we shouldn't modify
 * the action list.
 *
 * We want to support the possibility of resetting a session
 * variable at the end of transaction, with the ON TRANSACTION END
 * RESET option. This ensures the initial state of session
 * variables at the begin of each transaction.  The reset is
 * implemented as a removal of the session variable from
 * sessionvars hash table.  This enforce full initialization in
 * the next usage.  Careful though, this is not same as dropping
 * the session variable.
 *
 * Another functionality is dropping temporary session variable
 * with the option ON COMMIT DROP.
 */
typedef enum SVariableXActAction
{
	SVAR_ON_COMMIT_DROP,		/* used for ON COMMIT DROP */
	SVAR_ON_COMMIT_RESET,		/* used for DROP VARIABLE */
	SVAR_RESET,					/* used for ON TRANSACTION END RESET */
	SVAR_RECHECK				/* verify if session variable still exists */
} SVariableXActAction;

typedef struct SVariableXActActionItem
{
	Oid			varid;			/* varid of session variable */
	SVariableXActAction action;	/* reset or drop */

	/*
	 * creating_subid is the ID of the creating subxact. If the action was
	 * unregistered during the current transaction, deleting_subid is the ID of
	 * the deleting subxact, otherwise InvalidSubTransactionId.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
}  SVariableXActActionItem;

/* Both lists hold fields of SVariableXActActionItem type */
static List *xact_drop_actions = NIL;
static List *xact_reset_actions = NIL;

typedef struct SVariableData
{
	Oid			varid;			/* pg_variable OID of this sequence (hash key) */
	Oid			typid;			/* OID of the data type */
	int16		typlen;
	bool		typbyval;
	bool		isnull;
	bool		freeval;
	Datum		value;

	bool		is_rowtype;		/* true when variable is composite */
	bool		is_not_null;	/* don't allow null values */
	bool		is_immutable;	/* true when variable is immutable */
	bool		has_defexpr;	/* true when variable has a default value */

	bool		is_valid;		/* true when variable was successfuly
								 * initialized */

	uint32		hashvalue;
}			SVariableData;

typedef SVariableData * SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */
static MemoryContext SVariableMemoryContext = NULL;

static bool first_time = true;

static void register_session_variable_xact_action(Oid varid, SVariableXActAction action);
static void unregister_session_variable_xact_action(Oid varid, SVariableXActAction action);

/*
 * Releases stored data from session variable, but preserve the hash entry
 * in sessionvars.
 */
static void
free_session_variable_value(SVariable svar)
{
	if (svar->freeval)
		pfree(DatumGetPointer(svar->value));

	/* Clean current value */
	svar->value = (Datum) 0;
	svar->isnull = true;
	svar->freeval = false;

	/*
	 * We can mark this session variable as valid when
	 * it has not default expression, and when null is
	 * allowed. When it has defexpr, then the content
	 * will be valid after an assignment or defexp evaluation.
	 */
	svar->is_valid = !svar->has_defexpr && !svar->is_not_null;
}

/*
 * Release the variable defined by varid from sessionvars
 * hashtab.
 */
static void
free_session_variable(SVariable svar)
{
	free_session_variable_value(svar);

	if (hash_search(sessionvars,
					(void *) &svar->varid,
					HASH_REMOVE,
					NULL) == NULL)
		elog(DEBUG1, "hash table corrupted");
}

/*
 * Release the variable defined by varid from sessionvars
 * hashtab.
 */
static void
free_session_variable_varid(Oid varid)
{
	SVariable svar;
	bool		found;

	if (!sessionvars)
		return;

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_FIND, &found);
	if (found)
		free_session_variable(svar);
}

/*
 * Callback function for session variable invalidation.
 *
 * Register SVAR_RECHECK actions on appropriate currently cached
 * session variables. Those will be processed later, rechecking against the
 * catalog to detect dropped session variable.
 */
static void
pg_variable_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	/*
	 * There is no guarantee of sessionvars being initialized, even when
	 * receiving an invalidation callback, as DISCARD [ ALL | VARIABLES ]
	 * destroys the hash table entirely.
	 */
	if (!sessionvars)
		return;

	/*
	 * Since we can't guarantee the exact session variable from its hashValue,
	 * we have to iterate over all currently known session variables to find
	 * the ones with the same hashValue. On second hand, this can save us
	 * some CPU later, because we don't need to check any used
	 * session variable (by current session) against system catalog.
	 */
	if (hashvalue != 0)
	{
		HASH_SEQ_STATUS status;
		SVariable svar;

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			if (svar->hashvalue == hashvalue)
				register_session_variable_xact_action(svar->varid, SVAR_RECHECK);

			/*
			 * although it there is low probability, we have to iterate
			 * over all actively used session variables, because hashvalue
			 * is not unique identifier.
			 */
		}
	}
}

/*
 * Create the hash table for storing session variables
 */
static void
create_sessionvars_hashtable(void)
{
	HASHCTL		ctl;

	/* set callbacks */
	if (first_time)
	{
		/* Read sinval messages */
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  pg_variable_cache_callback,
									  (Datum) 0);

		first_time = false;
	}

	/* needs its own long lived memory context */
	if (SVariableMemoryContext == NULL)
	{
		SVariableMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "session variables",
								  ALLOCSET_START_SMALL_SIZES);
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SVariableData);
	ctl.hcxt = SVariableMemoryContext;

	Assert(sessionvars == NULL);

	sessionvars = hash_create("Session variables", 64, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Assign some content to the session variable. It's copied to
 * SVariableMemoryContext if necessary.
 *
 * init_mode is true, when the value of session variable is initialized
 * by default expression or by null. Only in this moment we can allow to
 * modify immutable variables with default expression.
 */
static void
set_session_variable(SVariable svar, Datum value,
					 bool isnull, Oid typid,
					 bool init_mode)
{
	MemoryContext oldcxt;
	Datum		newval = value;

	/* Don't allow assignment of null to NOT NULL variable */
	if (isnull && svar->is_not_null)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	if (!isnull && svar->typid != typid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type \"%s\" of assigned value is different than type \"%s\" of session variable \"%s.%s\"",
						format_type_be(typid),
						format_type_be(svar->typid),
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/*
	 * Don't allow updating of immutable session variable that has assigned
	 * not null value or has default expression (and then the value should be
	 * result of default expression always). Don't do this check, when variable
	 * is initialized.
	 */
	if (!init_mode &&
		(svar->is_immutable && (svar->is_valid || svar->has_defexpr)))
		ereport(ERROR,
				(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
				 errmsg("session variable \"%s.%s\" is declared IMMUTABLE",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/* copy value to session persistent context */
	oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);
	if (!isnull)
		newval = datumCopy(value, svar->typbyval, svar->typlen);
	MemoryContextSwitchTo(oldcxt);

	free_session_variable_value(svar);

	svar->value = newval;
	svar->isnull = isnull;
	svar->freeval = newval != value;
	svar->is_valid = true;
}

/*
 * Initialize svar from var
 * svar - SVariable - holds value
 * var  - Variable - holds metadata
 */
static void
init_session_variable(SVariable svar, Variable *var)
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
	svar->is_immutable = var->is_immutable;
	svar->has_defexpr = var->has_defexpr;

	svar->hashvalue = GetSysCacheHashValue1(VARIABLEOID,
											ObjectIdGetDatum(var->oid));

	/* the value of variable is not known yet */
	svar->is_valid = false;

	if (var->eoxaction == VARIABLE_EOX_RESET ||
			var->eoxaction == VARIABLE_EOX_DROP)
		register_session_variable_xact_action(var->oid, SVAR_RESET);
}

/*
 * Search the given session variable in the hash table. If it doesn't
 * exist, then insert it (and calculate defexpr if it exists).
 *
 * Caller is responsible for doing permission checks.
 *
 * As side efect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
static SVariable
prepare_variable_for_reading(Oid varid)
{
	SVariable svar;
	Variable	var;
	bool		found;

	var.oid = InvalidOid;

	if (!sessionvars)
		create_sessionvars_hashtable();

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_ENTER, &found);

	/* Return content if it is available and valid */
	if (found && svar->is_valid)
		return svar;

	/* We need to load defexpr. */
	initVariable(&var, varid, false);

	if (!found)
		init_session_variable(svar, &var);

	/* Raise an error when we cannot initialize variable correctly */
	if (var.is_not_null && !var.defexpr)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
						get_namespace_name(get_session_variable_namespace(varid)),
						get_session_variable_name(varid)),
				 errdetail("The session variable was not initialized yet.")));

	if (svar->has_defexpr)
	{
		Datum		value = (Datum) 0;
		bool		isnull;
		EState	   *estate = NULL;
		Expr	   *defexpr;
		ExprState  *defexprs;
		MemoryContext oldcxt;

		/* Prepare default expr */
		estate = CreateExecutorState();

		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		defexpr = expression_planner((Expr *) var.defexpr);
		defexprs = ExecInitExpr(defexpr, NULL);
		value = ExecEvalExprSwitchContext(defexprs,
										  GetPerTupleExprContext(estate),
										  &isnull);


		/* Store result before releasing Executor memory */
		set_session_variable(svar, value, isnull, svar->typid, true);

		MemoryContextSwitchTo(oldcxt);

		FreeExecutorState(estate);
	}
	else
		set_session_variable(svar, (Datum) 0, true, svar->typid, true);

	return svar;
}

/*
 * Store the given value in an SVariable, and cache it if not already present.
 *
 * Caller is responsible for doing permission checks.
 * We try not to break the previous value, if something is wrong.
 *
 * As side efect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
void
SetSessionVariable(Oid varid, Datum value, bool isNull, Oid typid)
{
	SVariable svar;
	bool		found;

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtable();

	svar = (SVariable) hash_search(sessionvars, &varid,
										HASH_ENTER, &found);

	/* Initialize svar when not initialized or when stored value is null */
	if (!found)
	{
		Variable	var;

		/* We don't need defexpr here */
		initVariable(&var, varid, true);
		init_session_variable(svar, &var);
	}

	set_session_variable(svar, value, isNull, typid, false);
}

/*
 * Wrapper around SetSessionVariable after checking for correct permission.
 */
void
SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull, Oid typid)
{
	AclResult	aclresult;

	/*
	 * Is possible to write to session variable?
	 */
	aclresult = pg_variable_aclcheck(varid, GetUserId(), ACL_WRITE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	SetSessionVariable(varid, value, isNull, typid);
}

/*
 * Returns a copy of value of the session variable specified by varid
 * Caller is responsible for doing permission checks.
 */
Datum
CopySessionVariable(Oid varid, bool *isNull, Oid *typid)
{
	SVariable svar;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	*isNull = svar->isnull;
	*typid = svar->typid;

	if (!svar->isnull)
		return datumCopy(svar->value, svar->typbyval, svar->typlen);

	return (Datum) 0;
}

/*
 * Returns the value of the session variable specified by varid. Check correct
 * result type. Optionally the result can be copied.
 * Caller is responsible for doing permission checks.
 */
Datum
GetSessionVariable(Oid varid, bool *isNull, Oid expected_typid, bool copy)
{
	SVariable svar;
	Datum		value;
	bool		isnull;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL);

	if (expected_typid != svar->typid)
		elog(ERROR, "type of variable \"%s.%s\" is different than expected",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid));

	value = svar->value;
	isnull = svar->isnull;

	*isNull = isnull;

	if (!isnull && copy)
		return datumCopy(value, svar->typbyval, svar->typlen);

	return value;
}


/*
 * Routines used for manipulation with session variables from
 * SQL level
 */

/*
 * Creates new variable - entry in pg_catalog.pg_variable table
 *
 * Used by CREATE VARIABLE command
 */
ObjectAddress
DefineSessionVariable(ParseState *pstate, CreateSessionVarStmt *stmt)
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

	if (stmt->is_not_null && stmt->is_immutable && !stmt->defexpr)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("IMMUTABLE NOT NULL variable requires default expression")));

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
							  stmt->if_not_exists,
							  stmt->is_immutable);

	/*
	 * We must bump the command counter to make the newly-created variable
	 * tuple visible for any other operations.
	 */
	CommandCounterIncrement();

	return variable;
}

/*
 * Create new ON_COMMIT_DROP xact action. We have to drop
 * ON COMMIT DROP variable, although this variable should not
 * be used. So we need to register this action in CREATE VARIABLE
 * time.
 */
void
RegisterOnCommitDropSessionVariable(Oid varid)
{
	register_session_variable_xact_action(varid, SVAR_ON_COMMIT_DROP);
}

/*
 * Drop variable by OID. This routine doesn't try to remove
 * the value of session variable immediately. It will be
 * removed on transaction end in sync_sessionvars_xact_callback
 * routine. This routine manipulate just with system catalog.
 */
void
RemoveSessionVariable(Oid varid)
{
	Relation	rel;
	HeapTuple	tup;

	rel = table_open(VariableRelationId, RowExclusiveLock);

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for variable %u", varid);

	CatalogTupleDelete(rel, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(rel, RowExclusiveLock);

	/*
	 * We removed entry from catalog already, we should not do it
	 * again at end of xact time.
	 */
	unregister_session_variable_xact_action(varid, SVAR_ON_COMMIT_DROP);

	/*
	 * And we want to enforce variable clearning when this transaction or
	 * subtransaction will be committed (we don't need to wait for
	 * sinval message). The cleaning action for one session variable
	 * can be repeated in the action list without causing any problem,
	 * so we don't need to ensure uniqueness. We need a different action
	 * from RESET, because RESET is executed on any transaction end,
	 * but we want to execute cleaning only when the current transaction
	 * will be committed.
	 */
	register_session_variable_xact_action(varid, SVAR_ON_COMMIT_RESET);
}

/*
 * Fast drop of the complete content of all session variables hash table.
 * This is code for DISCARD VARIABLES command. This command
 * cannot be run inside transaction, so we don't need to handle
 * end of transaction actions.
 */
void
ResetSessionVariables(void)
{
	/* Destroy hash table and reset related memory context */
	if (sessionvars)
	{
		hash_destroy(sessionvars);
		sessionvars = NULL;
	}

	/* Release memory allocated by session variables */
	if (SVariableMemoryContext != NULL)
		MemoryContextReset(SVariableMemoryContext);

	/*
	 * There are not any session variables left, so simply trim
	 * both xact action lists.
	 */
	list_free_deep(xact_drop_actions);
	xact_drop_actions = NIL;

	list_free_deep(xact_reset_actions);
	xact_reset_actions = NIL;
}

/*
 * Assign result of evaluated expression to session variable
 */
void
ExecuteLetStmt(ParseState *pstate,
			   LetStmt *stmt,
			   ParamListInfo params,
			   QueryEnvironment *queryEnv,
			   QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	List	   *rewritten;
	DestReceiver *dest;
	AclResult	aclresult;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Oid			varid = query->resultVariable;

	Assert(OidIsValid(varid));

	/*
	 * Is it allowed to write to session variable?
	 */
	aclresult = pg_variable_aclcheck(varid, GetUserId(), ACL_WRITE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	/* Create dest receiver for LET */
	dest = CreateDestReceiver(DestVariable);
	SetVariableDestReceiverParams(dest, varid);

	/* run rewriter - can be used for replacement of DEFAULT node */
	query = copyObject(query);

	rewritten = QueryRewrite(query);

	Assert(list_length(rewritten) == 1);

	query = linitial_node(Query, rewritten);
	Assert(query->commandType == CMD_SELECT);

	/* plan the query */
	plan = pg_plan_query(query, pstate->p_sourcetext,
						 CURSOR_OPT_PARALLEL_OK, params);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only
	 * matter if the planner executed an allegedly-stable function that
	 * changed the database contents, but let's do it anyway to be
	 * parallel to the EXPLAIN code path.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan to completion */
	ExecutorRun(queryDesc, ForwardScanDirection, 2L, true);

	/* save the rowcount if we're given a qc to fill */
	if (qc)
		SetQueryCompletion(qc, CMDTAG_LET, queryDesc->estate->es_processed);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
}

/*
 * Registration of actions to be executed on session variables at transaction
 * end time. We want to drop temporary session variables with clause ON COMMIT
 * DROP, or we want to reset values of session variables with clause ON
 * TRANSACTION END RESET or we want to clean (reset) local memory allocated by
 * values of dropped session variables.
 */

/*
 * Register a session variable xact action.
 */
static void
register_session_variable_xact_action(Oid varid,
									  SVariableXActAction action)
{
	SVariableXActActionItem *xact_ai;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	xact_ai = (SVariableXActActionItem *)
							palloc(sizeof(SVariableXActActionItem));

	xact_ai->varid = varid;
	xact_ai->action = action;

	xact_ai->creating_subid = GetCurrentSubTransactionId();
	xact_ai->deleting_subid = InvalidSubTransactionId;

	if (action == SVAR_ON_COMMIT_DROP)
		xact_drop_actions = lcons(xact_ai, xact_drop_actions);
	else
		xact_reset_actions = lcons(xact_ai, xact_reset_actions);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister an action on a given session variable from action list. In this
 * moment, the action is just marked as deleted by setting deleting_subid. The
 * calling even might be rollbacked, in which case we should not lose this
 * action.
 */
static void
unregister_session_variable_xact_action(Oid varid,
									SVariableXActAction action)
{
	ListCell   *l;

	Assert(action == SVAR_ON_COMMIT_DROP);

	foreach(l, xact_drop_actions)
	{
		SVariableXActActionItem *xact_ai =
					(SVariableXActActionItem *) lfirst(l);

		if (xact_ai->varid == varid && xact_ai->action == action)
			xact_ai->deleting_subid = GetCurrentSubTransactionId();
	}
}

/*
 * Perform ON TRANSACTION END RESET or ON COMMIT DROP
 * and COMMIT/ROLLBACK of transaction session variables.
 */
void
AtPreEOXact_SessionVariable_on_xact_actions(bool isCommit)
{
	ListCell   *l;

	foreach(l, xact_drop_actions)
	{
		SVariableXActActionItem *xact_ai =
							(SVariableXActActionItem *) lfirst(l);

		/* Iterate only over entries that are still pending */
		if (xact_ai->deleting_subid == InvalidSubTransactionId)
		{
			Assert(xact_ai->action == SVAR_ON_COMMIT_DROP);

			/*
			 * ON COMMIT DROP is allowed only for temp session
			 * variables. So we should explicitly delete only when
			 * current transaction was committed. When it's rollback,
			 * then session variable is removed automatically.
			 */
			if (isCommit)
			{
				ObjectAddress object;

				object.classId = VariableRelationId;
				object.objectId = xact_ai->varid;
				object.objectSubId = 0;

				/*
				 * Since this is an automatic drop, rather than one
				 * directly initiated by the user, we pass the
				 * PERFORM_DELETION_INTERNAL flag.
				 */
				performDeletion(&object, DROP_CASCADE,
								PERFORM_DELETION_INTERNAL |
								PERFORM_DELETION_QUIETLY);
			}
		}
	}

	/*
	 * Any drop action left is an entry that was unregistered and not
	 * rollbacked, so we can simply remove them.
	 */
	list_free_deep(xact_drop_actions);
	xact_drop_actions = NIL;

	foreach(l, xact_reset_actions)
	{
		SVariableXActActionItem *xact_ai =
							(SVariableXActActionItem *) lfirst(l);

		if (xact_ai->action == SVAR_RECHECK)
		{
			/*
			 * We can do the recheck only when the transaction is commited as
			 * we need to access system catalog. When transaction is
			 * ROLLBACKed, then we have to postpone the check to next
			 * transaction. We therefore don't check for a matching
			 * creating_subid here.
			 */
			if (isCommit)
			{
				SVariable	svar;
				bool		found;

				svar = (SVariable) hash_search(sessionvars, &xact_ai->varid,
											HASH_FIND, &found);

				if (found)
				{
					HeapTuple	tp;

					tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

					if (HeapTupleIsValid(tp))
						ReleaseSysCache(tp);
					else
						free_session_variable(svar);
				}

				xact_reset_actions = foreach_delete_current(xact_reset_actions, l);
				pfree(xact_ai);
			}
		}
		else
		{
			/*
			 * We want to reset session variable (release it from
			 * local memory) when RESET is required or when session
			 * variable was removed explicitly (DROP VARIABLE) or
			 * implicitly (ON COMMIT DROP). Explicit releasing should
			 * be done only if the transaction is commited.
			 */
			if ((xact_ai->action == SVAR_RESET) ||
				(xact_ai->action == SVAR_ON_COMMIT_RESET &&
				 xact_ai->deleting_subid == InvalidSubTransactionId &&
				 isCommit))
				free_session_variable_varid(xact_ai->varid);

			/*
			 * Any non SVAR_RECHECK action can now be removed.  It was either
			 * explicitly processed, or implicitly due to some ROLLBACK action.
			 */
			xact_reset_actions = foreach_delete_current(xact_reset_actions, l);
			pfree(xact_ai);
		}
	}
}

/*
 * Post-subcommit or post-subabort cleanup of xact action list.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction. During subcommit, just transfer entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_SessionVariable_on_xact_actions(bool isCommit, SubTransactionId mySubid,
											SubTransactionId parentSubid)
{
	ListCell   *cur_item;

	foreach(cur_item, xact_drop_actions)
	{
		SVariableXActActionItem *xact_ai =
								  (SVariableXActActionItem *) lfirst(cur_item);

		if (!isCommit && xact_ai->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			xact_drop_actions = foreach_delete_current(xact_drop_actions, cur_item);
			pfree(xact_ai);
		}
		else
		{
			/* cur_item must be preserved */
			if (xact_ai->creating_subid == mySubid)
				xact_ai->creating_subid = parentSubid;
			if (xact_ai->deleting_subid == mySubid)
				xact_ai->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
		}
	}

	/*
	 * Reset and recheck actions - cleaning memory should be done every time
	 * (when the variable with short life cycle was used) and then
	 * cannot be removed from xact action list.
	 */
	foreach(cur_item, xact_reset_actions)
	{
		SVariableXActActionItem *xact_ai =
								  (SVariableXActActionItem *) lfirst(cur_item);

		if (!isCommit &&
			xact_ai->creating_subid == mySubid &&
			xact_ai->action != SVAR_RESET &&
			xact_ai->action != SVAR_RECHECK)
		{
			/* cur_item must be removed */
			xact_reset_actions = foreach_delete_current(xact_reset_actions, cur_item);
			pfree(xact_ai);
		}
		else
		{
			/* cur_item must be preserved */
			if (xact_ai->creating_subid == mySubid)
				xact_ai->creating_subid = parentSubid;
			if (xact_ai->deleting_subid == mySubid)
				xact_ai->deleting_subid = isCommit ? parentSubid : InvalidSubTransactionId;
		}
	}
}
