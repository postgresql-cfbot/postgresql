/*-------------------------------------------------------------------------
 *
 * session_variable.c
 *	  session variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/session_variable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_variable.h"
#include "commands/session_variable.h"
#include "executor/svariableReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Values of session variables are stored in the backend local memory,
 * in sessionvars hash table in binary format, in a dedicated memory
 * context SVariableMemoryContext.  A session variable value can stay
 * valid for longer than the transaction that assigns its value.  To
 * make sure that the underlying memory is eventually freed, but not
 * before it's guarantee that the value won't be needed anymore, we
 * need to handle the two following points:
 *
 * - We need detect when a variable is dropped, whether in the current
 * transaction in the current session or by another session, and mark
 * the underlying entries for removal.  To protect the content against
 * possibly rollbacked DROP VARIABLE commands, the entries (and
 * memory) shouldn't be freed immediately but be postponed until the
 * end of the transaction.
 *
 * - The session variable can be dropped explicitly (by DROP VARIABLE
 *  command) or implicitly (using ON COMMIT DROP clause), and the
 *  value can be implicitly removed (using the ON TRANSACTION END
 *  clause).  In all those cases the memory should also be freed at
 *  the transaction end.
 *
 * To achieve that, we maintain 3 queues of actions to be performed at
 * certain time:
 * - a List of SVariableXActActionItem, to handle ON COMMIT DROP
 * variables, and delayed memory cleanup of variable dropped by the
 * current transaction.  Those actions are transactional (for instance
 * we don't want to cleanup the memory of a rollbacked DROP VARIABLE)
 * so the structure is needed to keep track of the final transaction
 * state
 * - a List of variable Oid for the ON TRANSACTION ON RESET variables
 * - a List of variable Oid for the concurrent DROP VARIABLE
 * notification we receive via shared invalidations.
 *
 * Note that although resetting a variable doesn't technically require
 * to remove the entry from the sessionvars hash table, we currently
 * do it.  It's a simple way to implement the reset, and helps to
 * reduce memory usage and prevents the hash table from bloating.
 *
 * There are two different ways to do the final access to session
 * variables: buffered (indirect) or direct. Buffered access is used
 * in regular DML statements, where we have to ensure the stability of
 * the variable values.  The session variables have the same behaviour
 * as external query parameters, which is consistent with using
 * PL/pgSQL's variables in embedded queries in PL/pgSQL.
 *
 * This is implemented by using an aux buffer (an array) that holds a
 * copy of values of used (in query) session variables, which is also
 * transmitted to the parallel workers.  The values from this array
 * are passed as constant (EEOP_CONST).
 *
 * Direct access is used by simple expression evaluation (PLpgSQL).
 * In this case we don't need to ensure the stability of passed
 * values, and maintaining the buffer with copies of values of session
 * variables would be useless overhead.  In this case we just read the
 * value of the session variable directly (EEOP_PARAM_VARIABLE). This
 * strategy removes the necessity to modify related PL/pgSQL code to
 * support session variables (the reading of session variables is
 * fully transparent for PL/pgSQL).
 */
typedef enum SVariableXActAction
{
	SVAR_ON_COMMIT_DROP,		/* used for ON COMMIT DROP */
	SVAR_ON_COMMIT_RESET,		/* used for DROP VARIABLE */
} SVariableXActAction;

typedef struct SVariableXActActionItem
{
	Oid			varid;			/* varid of session variable */
	SVariableXActAction action;

	/*
	 * creating_subid is the ID of the sub-transaction that registered
	 * the action. If the action was unregistered during the current
	 * transaction, deleting_subid is the ID of the deleting
	 * sub-transaction, otherwise InvalidSubTransactionId.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
} SVariableXActActionItem;

/* List of SVariableXActActionItem */
static List *xact_on_commit_actions = NIL;

/*
 * To process ON TRANSACTION END RESET variables, for which we always
 * need to clear the saved values.
 */
static List *xact_reset_varids = NIL;

/*
 * When the session variable is dropped we need to free local memory. The
 * session variable can be dropped by current session, but it can be
 * dropped by other's sessions too, so we have to watch sinval message.
 * But because we don't want to free local memory immediately, we need to
 * hold list of possibly dropped session variables and at the end of
 * transaction, we check session variables from this list against system
 * catalog. This check can be postponed into next transaction if
 * current transactions is in aborted state, as we wouldn't be able to
 * access the system catalog.
 */
static List *xact_recheck_varids = NIL;

typedef struct SVariableData
{
	Oid			varid;			/* pg_variable OID of the variable (hash key) */

	/*
	 * The session variable is identified by oid. The oid is unique in
	 * catalog. Unfortunately, the memory cleanup can be postponed to
	 * the beginning
	 * of the session next transaction, and it's possible that this next
	 * transaction sees a different variable with the same oid.  We
	 * therefore need an extra identifier to distinguish both cases. We
	 * use the LSN number of session variable at creation time. The
	 * value of session variable (in memory) is valid, when there is a
	 * record in pg_variable with same oid and same create_lsn.
	 */
	XLogRecPtr	create_lsn;

	bool		isnull;
	bool		freeval;
	Datum		value;

	Oid			typid;
	int16		typlen;
	bool		typbyval;

	bool		is_domain;
	void	   *domain_check_extra;
	LocalTransactionId domain_check_extra_lxid;

	/*
	 * Top level local transaction id of the last transaction that dropped the
	 * variable if any.  We need this information to avoid freeing memory for
	 * variable dropped by the local backend that may be eventually rollbacked.
	 */
	LocalTransactionId drop_lxid;

	bool		is_not_null;	/* don't allow null values */
	bool		is_immutable;	/* true when variable is immutable */
	bool		has_defexpr;	/* true when variable has a default value */

	bool		is_valid;		/* true when variable was successfully
								 * initialized */

	uint32		hashvalue;		/* used for pairing sinval message */

	bool		eox_reset;		/* true, when lifecycle is limitted by
								 * transaction */
} SVariableData;

typedef SVariableData *SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */

static MemoryContext SVariableMemoryContext = NULL;

static void create_sessionvars_hashtables(void);
static void free_session_variable_value(SVariable svar);
static void init_session_variable(SVariable svar, Variable *var);
static bool is_session_variable_valid(SVariable svar);
static void pg_variable_cache_callback(Datum arg, int cacheid,
									   uint32 hashvalue);
static SVariable prepare_variable_for_reading(Oid varid);
static void register_session_variable_xact_action(Oid varid,
												  SVariableXActAction action);
static void remove_session_variable(SVariable svar);
static void remove_session_variable_by_id(Oid varid);
static void set_session_variable(SVariable svar, Datum value, bool isnull,
								 bool init_mode);
static const char *SVariableXActActionName(SVariableXActAction action);
static void sync_sessionvars_all(bool filter_lxid);
static void unregister_session_variable_xact_action(Oid varid,
													SVariableXActAction action);


/*
 * Returns human readable name of SVariableXActAction value.
 */
static const char *
SVariableXActActionName(SVariableXActAction action)
{
	switch (action)
	{
		case SVAR_ON_COMMIT_DROP:
			return "ON COMMIT DROP";
		case SVAR_ON_COMMIT_RESET:
			return "ON COMMIT RESET";
		default:
			elog(ERROR, "unknown SVariableXActAction action %d", action);
	}
}

/*
 * Free all memory allocated for the given session variable, but
 * preserve the hash entry in sessionvars.
 */
static void
free_session_variable_value(SVariable svar)
{
	/* Clean current value */
	if (!svar->isnull)
	{
		if (svar->freeval)
		{
			pfree(DatumGetPointer(svar->value));
			svar->freeval = false;
		}

		svar->isnull = true;
	}

	svar->value = (Datum) 0;
	svar->freeval = false;

	/*
	 * We can mark this session variable as valid when it has not default
	 * expression, and when null is allowed. When it has defexpr, then the
	 * content will be valid after an assignment or defexp evaluation.
	 */
	svar->is_valid = !svar->has_defexpr && !svar->is_not_null;
}

/*
 * Registration of actions to be executed on session variables at transaction
 * end time. We want to drop temporary session variables with clause ON COMMIT
 * DROP, or we want to clean (reset) local memory allocated by
 * values of session variables dropped in other backends.
 */
static void
register_session_variable_xact_action(Oid varid,
									  SVariableXActAction action)
{
	SVariableXActActionItem *xact_ai;
	MemoryContext oldcxt;

	elog(DEBUG1, "SVariableXActAction \"%s\" is registered for session variable (oid:%u)",
		 SVariableXActActionName(action), varid);

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	xact_ai = (SVariableXActActionItem *)
		palloc(sizeof(SVariableXActActionItem));

	xact_ai->varid = varid;
	xact_ai->action = action;

	xact_ai->creating_subid = GetCurrentSubTransactionId();
	xact_ai->deleting_subid = InvalidSubTransactionId;

	xact_on_commit_actions = lcons(xact_ai, xact_on_commit_actions);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister an action on a given session variable from the action list.
 * The action is just marked as deleted by setting deleting_subid.
 * The calling subtransaction even might be rollbacked, in which case the
 * action shouldn't be removed.
 */
static void
unregister_session_variable_xact_action(Oid varid,
										SVariableXActAction action)
{
	ListCell   *l;

	elog(DEBUG1, "SVariableXActAction \"%s\" is unregistered for session variable (oid:%u)",
		 SVariableXActActionName(action), varid);

	foreach(l, xact_on_commit_actions)
	{
		SVariableXActActionItem *xact_ai =
		(SVariableXActActionItem *) lfirst(l);

		if (xact_ai->action == action && xact_ai->varid == varid)
			xact_ai->deleting_subid = GetCurrentSubTransactionId();
	}
}

/*
 * Release the given session variable from sessionvars hashtab and free
 * all underlying allocated memory.
 */
static void
remove_session_variable(SVariable svar)
{
	free_session_variable_value(svar);

	/*
	 * In this moment, the session variable is not in catalog, so only saved
	 * oid can be displayed.
	 */
	elog(DEBUG1, "session variable (oid:%u) is removing from memory",
		 svar->varid);

	if (hash_search(sessionvars,
					(void *) &svar->varid,
					HASH_REMOVE,
					NULL) == NULL)
		elog(DEBUG1, "hash table corrupted");
}

/*
 * Release the session variable defined by varid from sessionvars
 * hashtab and free all underlying allocated memory.
 */
static void
remove_session_variable_by_id(Oid varid)
{
	SVariable	svar;
	bool		found;

	if (!sessionvars)
		return;

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_FIND, &found);
	if (found)
		remove_session_variable(svar);
}

/*
 * Callback function for session variable invalidation.
 *
 * It queues a list of variable Oid in xact_recheck_varids.
 */
static void
pg_variable_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	SVariable	svar;

	/*
	 * There is no guarantee of sessionvars being initialized, even when
	 * receiving an invalidation callback, as DISCARD [ ALL | VARIABLES ]
	 * destroys the hash table entirely.
	 */
	if (!sessionvars)
		return;

	elog(DEBUG1, "pg_variable_cache_callback %u %u", cacheid, hashvalue);

	/*
	 * When the hashvalue is not specified, then we have to recheck all
	 * currently used session variables. Since we can't guarantee the exact
	 * session variable from its hashValue, we also have to iterate over
	 * all items of the sessionvars hash table.
	 */
	hash_seq_init(&status, sessionvars);

	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (hashvalue == 0 || svar->hashvalue == hashvalue)
		{
			MemoryContext oldcxt;

			/* The list needs to be able to survive the transaction */
			oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

			xact_recheck_varids = lappend_oid(xact_recheck_varids,
											  svar->varid);

			MemoryContextSwitchTo(oldcxt);

			elog(DEBUG1, "session variable (oid:%u) should be rechecked (forced by sinval)",
				 svar->varid);
		}

		/*
		 * although it there is low probability, we have to iterate over all
		 * locally set session variables, because hashvalue is not a unique
		 * identifier.
		 */
	}
}

/*
 * Returns true when the entry in pg_variable is valid for the given session
 * variable.
 */
static bool
is_session_variable_valid(SVariable svar)
{
	HeapTuple	tp;
	bool		result = false;

	tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

	if (HeapTupleIsValid(tp))
	{
		/*
		 * In this case, the only oid cannot be used as unique identifier,
		 * because the oid counter can wraparound, and the oid can be used for
		 * new other session variable. We do a second check against 64bit
		 * unique identifier.
		 */
		if (svar->create_lsn == ((Form_pg_variable) GETSTRUCT(tp))->create_lsn)
			result = true;

		ReleaseSysCache(tp);
	}

	return result;
}

/*
 * Recheck the possibly invalidated variables (in memory) against system
 * catalog. This routine is called before any read or any write from/to session
 * variables and when processing a committed transaction.
 * If filter_lxid is true, this function will ignore the recheck for variables
 * that have the same cached local transaction id as the transaction current
 * top level local transaction id, ie. the variables dropped in the current top
 * level transaction or any underlying subtransaction.
 */
static void
sync_sessionvars_all(bool filter_lxid)
{
	SVariable	svar;
	ListCell   *l;
	List	   *xact_recheck_varids_snapshot;

	if (!xact_recheck_varids)
		return;

	/*
	 * If the sessionvars hashtable is NULL (which can be done by DISCARD
	 * VARIABLES), we are sure that there aren't any active session variable
	 * in this session.
	 */
	if (!sessionvars)
	{
		list_free(xact_recheck_varids);
		xact_recheck_varids = NIL;
		return;
	}

	elog(DEBUG1, "effective call of sync_sessionvars_all()");

	/*
	 * The recheck list can contain many duplicates, so clean it up before
	 * processing to avoid extraneous work.
	 */
	list_sort(xact_recheck_varids, list_oid_cmp);
	list_deduplicate_oid(xact_recheck_varids);

	/*
	 * When we check the variables, the system cache can be invalidated,
	 * and then xact_recheck_varids can be extended. But we need to iterate
	 * over stable list, and we must not at same time discard invalidation
	 * messages.
	 *
	 * Steps of possible solution:
	 *
	 *   1. move xact_recheck_varids to aux variable, and reset
	 *      xact_recheck_varids,
	 *
	 *   2. process fields in list of aux variable,
	 *
	 *   3. merge the content of aux variable back to xact_recheck_varids.
	 */
	xact_recheck_varids_snapshot = xact_recheck_varids;
	xact_recheck_varids = NIL;

	/*
	 * This routine is called before any reading, so the session should be in
	 * transaction state. This is required to access the system catalog.
	 */
	Assert(IsTransactionState());

	foreach(l, xact_recheck_varids_snapshot)
	{
		bool		found;
		Oid			varid = lfirst_oid(l);

		svar = (SVariable) hash_search(sessionvars, &varid,
									   HASH_FIND, &found);

		/*
		 * Remove invalid variables, but don't touch variables that were
		 * dropped by the current top level local transaction or any
		 * subtransaction underneath, as there's no guarantee that the
		 * transaction will be committed. Such variables will be removed in
		 * the next transaction if needed.
		 */
		if (found)
		{
			/*
			 * If this is a variable dropped by the current transaction,
			 * ignore it and keep the oid to recheck in the next transaction.
			 */
			if (filter_lxid && svar->drop_lxid == MyProc->lxid)
				continue;

			if (!is_session_variable_valid(svar))
				remove_session_variable(svar);
		}

		/*
		 * If caller asked to filter the list, we have to clean items as they
		 * are processed.
		 */
		if (filter_lxid)
			xact_recheck_varids_snapshot =
							foreach_delete_current(xact_recheck_varids_snapshot,
												   l);
	}

	/*
	 * If caller ask to filter the list, some items are not processed
	 * and we should to merge these items to xact_recheck_varids.
	 */
	if (filter_lxid)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

		xact_recheck_varids = list_concat(xact_recheck_varids,
										  xact_recheck_varids_snapshot);

		MemoryContextSwitchTo(oldcxt);
	}

	list_free(xact_recheck_varids_snapshot);
}

/*
 * Create the hash table for storing session variables.
 */
static void
create_sessionvars_hashtables(void)
{
	HASHCTL		vars_ctl;

	Assert(!sessionvars);

	/* set callbacks */
	if (!SVariableMemoryContext)
	{
		/* Read sinval messages */
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  pg_variable_cache_callback,
									  (Datum) 0);

		/* We need our own long lived memory context */
		SVariableMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "session variables",
								  ALLOCSET_START_SMALL_SIZES);
	}

	Assert(SVariableMemoryContext);

	memset(&vars_ctl, 0, sizeof(vars_ctl));
	vars_ctl.keysize = sizeof(Oid);
	vars_ctl.entrysize = sizeof(SVariableData);
	vars_ctl.hcxt = SVariableMemoryContext;

	Assert(sessionvars == NULL);

	sessionvars = hash_create("Session variables", 64, &vars_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Assign some content to the session variable. It's copied to
 * SVariableMemoryContext if necessary.
 *
 * init_mode is true when the value of session variable should be initialized
 * by the default expression if any. This is the only case where we allow the
 * modification of an immutable variables with default expression.
 *
 * If any error happens, the existing value shouldn't be modified.
 */
static void
set_session_variable(SVariable svar, Datum value, bool isnull, bool init_mode)
{
	Datum		newval = value;

	Assert(svar && OidIsValid(svar->typid));

	/* Don't allow assignment of null to NOT NULL variable */
	if (isnull && svar->is_not_null)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/*
	 * Don't allow the modification of an immutable session variable that
	 * already has an assigned value (possibly NULL) or has a default
	 * expression (in which case the value should always be the result of
	 * default expression evaluation) unless the variable is being initialized.
	 */
	if (!init_mode &&
		(svar->is_immutable &&
		 (svar->is_valid || svar->has_defexpr)))
		ereport(ERROR,
				(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
				 errmsg("session variable \"%s.%s\" is declared IMMUTABLE",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	if (!isnull)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

		newval = datumCopy(value, svar->typbyval, svar->typlen);

		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		/* The caller shouldn't have provided any real value. */
		Assert(value == (Datum) 0);
	}

	free_session_variable_value(svar);

	svar->value = newval;

	svar->isnull = isnull;
	svar->freeval = newval != value;
	svar->is_valid = true;

	/*
	 * XXX While unlikely, an error here is possible.
	 * It wouldn't leak memory as the allocated chunk has already been
	 * correctly assigned to the session variable, but would contradict this
	 * function contract, which is that this function should either succeed or
	 * leave the current value untouched.
	 */
	elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new value",
		 get_namespace_name(get_session_variable_namespace(svar->varid)),
		 get_session_variable_name(svar->varid),
		 svar->varid);
}

/*
 * Initialize session variable svar from variable var
 */
static void
init_session_variable(SVariable svar, Variable *var)
{
	MemoryContext oldcxt;

	Assert(OidIsValid(var->oid));

	svar->varid = var->oid;
	svar->create_lsn = var->create_lsn;

	svar->isnull = true;
	svar->freeval = false;
	svar->value = (Datum) 0;

	svar->typid = var->typid;
	get_typlenbyval(var->typid, &svar->typlen, &svar->typbyval);

	svar->is_domain = (get_typtype(var->typid) == TYPTYPE_DOMAIN);
	svar->domain_check_extra = NULL;
	svar->domain_check_extra_lxid = InvalidLocalTransactionId;

	svar->drop_lxid = InvalidLocalTransactionId;

	svar->is_not_null = var->is_not_null;
	svar->is_immutable = var->is_immutable;
	svar->has_defexpr = var->has_defexpr;

	/* the value of variable is not known yet */
	svar->is_valid = false;

	svar->hashvalue = GetSysCacheHashValue1(VARIABLEOID,
											ObjectIdGetDatum(var->oid));

	svar->eox_reset = var->eoxaction == VARIABLE_EOX_RESET ||
		var->eoxaction == VARIABLE_EOX_DROP;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	if (svar->eox_reset)
		xact_reset_varids = lappend_oid(xact_reset_varids, var->oid);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Search a seesion variable in the hash table given its oid. If it
 * doesn't exist, then insert it (and calculate defexpr if it exists).
 *
 * Caller is responsible for doing permission checks.
 *
 * As side effect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
static SVariable
prepare_variable_for_reading(Oid varid)
{
	SVariable	svar;
	Variable	var;
	bool		found;

	var.oid = InvalidOid;

	if (!sessionvars)
		create_sessionvars_hashtables();

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	/*
	 * Make sure that all entries in sessionvars hash table are valid, but
	 * keeping variables dropped by the current transaction.
	 */
	sync_sessionvars_all(true);

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_ENTER, &found);

	/* Return content if it is available and valid */
	if (!found || !svar->is_valid)
	{
		/* We need to load defexpr. */
		InitVariable(&var, varid, false);

		if (!found)
		{
			init_session_variable(svar, &var);

			elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new entry in memory (emitted by READ)",
				 get_namespace_name(get_session_variable_namespace(varid)),
				 get_session_variable_name(varid),
				 varid);
		}

		/*
		 * Raise an error if this is a NOT NULL variable without default
		 * expression.
		 */
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

			MemoryContextSwitchTo(oldcxt);

			/* Store result before releasing Executor memory */
			set_session_variable(svar, value, isnull, true);

			FreeExecutorState(estate);
		}
		else
			set_session_variable(svar, (Datum) 0, true, true);
	}

	/*
	 * Although the value of domain type should be valid (it is checked when
	 * it is assigned to session variable), we have to check related
	 * constraints each time we access the variable. It can be more expensive
	 * than in PL/pgSQL, as PL/pgSQL forces domain checks only when the value is assigned
	 * to the variable or when the value is returned from function.
	 * However, domain types have a constraint cache so it's not too much
	 * expensive..
	 */
	if (svar->is_domain)
	{
		/*
		 * Store domain_check extra in TopTransactionContext. When we are in
		 * other transaction, the domain_check_extra cache is not valid
		 * anymore.
		 */
		if (svar->domain_check_extra_lxid != MyProc->lxid)
			svar->domain_check_extra = NULL;

		domain_check(svar->value, svar->isnull,
					 svar->typid, &svar->domain_check_extra,
					 TopTransactionContext);

		svar->domain_check_extra_lxid = MyProc->lxid;
	}

	return svar;
}

/*
 * Store the given value in an SVariable, and cache it if not already present.
 *
 * Caller is responsible for doing permission checks.
 *
 * As side effect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
void
SetSessionVariable(Oid varid, Datum value, bool isNull)
{
	SVariable	svar;
	bool		found;

	if (!sessionvars)
		create_sessionvars_hashtables();

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	/*
	 * Make sure that all entries in sessionvars hash table are valid, but
	 * keeping variables dropped by the current transaction.
	 */
	sync_sessionvars_all(true);

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_ENTER, &found);

	if (!found)
	{
		Variable	var;

		/* We don't need to know defexpr here */
		InitVariable(&var, varid, true);
		init_session_variable(svar, &var);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new entry in memory (emitted by WRITE)",
			 get_namespace_name(get_session_variable_namespace(svar->varid)),
			 get_session_variable_name(svar->varid),
			 varid);
	}

	/*
	 * This should either succeed or fail without changing the currently stored
	 * value.
	 */
	set_session_variable(svar, value, isNull, false);
}

/*
 * Wrapper around SetSessionVariable after checking for correct permission.
 */
void
SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull)
{
	AclResult	aclresult;

	/*
	 * Is caller allowed to update the session variable?
	 */
	aclresult = object_aclcheck(VariableRelationId, varid, GetUserId(), ACL_UPDATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	SetSessionVariable(varid, value, isNull);
}

/*
 * Returns a copy of the value of the session variable specified by its oid.
 * Caller is responsible for doing permission checks.
 */
Datum
CopySessionVariable(Oid varid, bool *isNull, Oid *typid)
{
	SVariable	svar;
	Datum		result;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	*typid = svar->typid;

	/* force copy of non NULL value */
	if (!svar->isnull)
	{
		result = datumCopy(svar->value, svar->typbyval, svar->typlen);
		*isNull = false;
	}
	else
	{
		result = (Datum) 0;
		*isNull = true;
	}

	return (Datum) result;
}

/*
 * Returns a copy of ths value of the session variable specified by its oid
 * with a check of the expected type. Like previous CopySessionVariable, the
 * caller is responsible for doing permission checks.
 */
Datum
CopySessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid)
{
	SVariable	svar;
	Datum		result;

	svar = prepare_variable_for_reading(varid);
	Assert(svar != NULL && svar->is_valid);

	if (expected_typid != svar->typid)
		elog(ERROR, "type of variable \"%s.%s\" is different than expected",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid));

	if (!svar->isnull)
	{
		result = datumCopy(svar->value, svar->typbyval, svar->typlen);
		*isNull = false;
	}
	else
	{
		result = (Datum) 0;
		*isNull = true;
	}

	return (Datum) result;
}

/*
 * Do the necessary work to setup local memory management of a new
 * variable.
 *
 * Caller should already have created the necessary entry in catalog
 * and made them visible.
 */
void
SessionVariableCreatePostprocess(Oid varid, char eoxaction)
{
	/*
	 * For temporary variables, we need to create a new end of xact action to
	 * ensure deletion from catalog.
	 */
	if (eoxaction == VARIABLE_EOX_DROP)
	{
		Assert(isTempNamespace(get_session_variable_namespace(varid)));

		register_session_variable_xact_action(varid, SVAR_ON_COMMIT_DROP);
	}
}

/*
 * Handle the local memory cleanup for a DROP VARIABLE command.
 *
 * Caller should take care of removing the pg_variable entry first.
 */
void
SessionVariableDropPostprocess(Oid varid)
{
	/*
	 * The entry was removed from catalog already, we must not do it
	 * again at end of xact time.
	 */
	unregister_session_variable_xact_action(varid, SVAR_ON_COMMIT_DROP);

	if (sessionvars)
	{
		bool		found;
		SVariable	svar = (SVariable) hash_search(sessionvars, &varid,
												   HASH_FIND, &found);

		if (found)
		{
			/*
			 * Save the current top level local transaction id to make sure we
			 * don't automatically remove the local variable storage in
			 * sync_sessionvars_all, as the DROP VARIABLE will send an
			 * invalidation message.
			 */
			Assert(LocalTransactionIdIsValid(MyProc->lxid));
			svar->drop_lxid = MyProc->lxid;

			/*
			 * For variables that are not ON TRANSACTION END RESET, we need to
			 * register an SVAR_ON_COMMIT_RESET action to free the local
			 * memory for this variable when the top level transaction
			 * is committed (we don't need to wait for sinval
			 * message).  The cleanup action for one session variable can be
			 * duplicated in the action list without causing any problem, so we
			 * don't need to ensure uniqueness.  We need a different action
			 * from RESET, because RESET is executed on any transaction end,
			 * but we want to execute this cleanup only when the current
			 * transaction will be committed. This action can be reverted by
			 * ABORT of DROP VARIABLE command.
			 */
			if (!svar->eox_reset)
				register_session_variable_xact_action(varid,
													  SVAR_ON_COMMIT_RESET);
		}
	}
}

/*
 * Fast drop of the complete content of all session variables hash table, and
 * cleanup of any list that wouldn't be relevant anymore.
 * This is used by DISCARD VARIABLES (and DISCARD ALL) command.
 */
void
ResetSessionVariables(void)
{
	ListCell   *lc;

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
	 * There isn't any session variable left, but we still need to retain the
	 * ON COMMIT DROP actions if any.
	 */
	foreach(lc, xact_on_commit_actions)
	{
		SVariableXActActionItem *xact_ai =
			(SVariableXActActionItem *) lfirst(lc);

		if (xact_ai->action == SVAR_ON_COMMIT_DROP)
			continue;

		pfree(xact_ai);
		xact_on_commit_actions = foreach_delete_current(xact_on_commit_actions,
													   lc);
	}

	/* We should clean xact_reset_varids */
	list_free(xact_reset_varids);
	xact_reset_varids = NIL;

	/*
	 * xact_recheck_varids is stored in SVariableMemoryContext, so it has
	 * already been freed, just reset the list.
	 */
	xact_recheck_varids = NIL;
}

/*
 * Perform the necessary work for ON TRANSACTION END RESET and ON COMMIT DROP
 * session variables.
 * If the transaction is committed, also process the delayed memory cleanup of
 * local DROP VARIABLE and process all pending rechecks.
 */
void
AtPreEOXact_SessionVariable(bool isCommit)
{
	ListCell   *l;

	/*
	 * Clean memory for all ON TRANSACTION END RESET  variables. Do it first,
	 * as it reduces the overhead of the RECHECK action list.
	 */
	foreach(l, xact_reset_varids)
	{
		remove_session_variable_by_id(lfirst_oid(l));
	}

	/* We can now clean xact_reset_varids */
	list_free(xact_reset_varids);
	xact_reset_varids = NIL;

	if (isCommit && xact_on_commit_actions)
	{
		foreach(l, xact_on_commit_actions)
		{
			SVariableXActActionItem *xact_ai =
				(SVariableXActActionItem *) lfirst(l);

			/* Iterate only over entries that are still pending */
			if (xact_ai->deleting_subid != InvalidSubTransactionId)
				continue;

			/*
			 * ON COMMIT DROP is allowed only for temp session variables.
			 * So we should explicitly delete only when the current
			 * transaction is committed. When it's rollbacked, the session
			 * variable is removed automatically.
			 */
			if (xact_ai->action == SVAR_ON_COMMIT_DROP)
			{
				ObjectAddress object;

				object.classId = VariableRelationId;
				object.objectId = xact_ai->varid;
				object.objectSubId = 0;

				/*
				 * Since this is an automatic drop, rather than one directly
				 * initiated by the user, we pass the
				 * PERFORM_DELETION_INTERNAL flag.
				 */
				elog(DEBUG1, "session variable (oid:%u) will be deleted (forced by SVAR_ON_COMMIT_DROP action)",
					 xact_ai->varid);

				/*
				 * If the variable was locally set, the memory will be
				 * automatically cleaned up when we process the underlying
				 * shared invalidation for this drop.  There can't be a recheck
				 * action for this variable, so there's nothing to gain
				 * explicitly removing it here.
				 */
				performDeletion(&object, DROP_CASCADE,
								PERFORM_DELETION_INTERNAL |
								PERFORM_DELETION_QUIETLY);
			}
			else
			{
				/*
				 * When we process DROP VARIABLE statement issued by the
				 * current transaction, we create an SVAR_ON_COMMIT_RESET xact
				 * action. We want to process this action only when related
				 * transaction is commited (when DROP VARIABLE statement
				 * sucessfully processed) as we need to preserve the variable
				 * content if the transaction that issued the DROP VARAIBLE
				 * statement is rollbacked.
				 */
				remove_session_variable_by_id(xact_ai->varid);
			}
		}
	}

	/*
	 * Any drop action left is an entry that was unregistered and not
	 * rollbacked, so we can simply remove them.
	 */
	list_free_deep(xact_on_commit_actions);
	xact_on_commit_actions = NIL;

	/*
	 * We process the list of recheck last for performance reason,the previous
	 * steps might remove entries from the hash table.
	 * We need catalog access to process the recheck, so this can only be done
	 * if the transaction is committed.  Otherwise, we just keep the recheck
	 * list as-is and it will be processed at the next (committed) transaction.
	 */
	if (isCommit && xact_recheck_varids)
	{
		Assert(sessionvars);

		sync_sessionvars_all(false);
	}
}

/*
 * Post-subcommit or post-subabort cleanup of xact_on_commit_actions list.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction. During subcommit, just transfer entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_SessionVariable(bool isCommit,
							SubTransactionId mySubid,
							SubTransactionId parentSubid)
{
	ListCell   *cur_item;

	foreach(cur_item, xact_on_commit_actions)
	{
		SVariableXActActionItem *xact_ai =
		(SVariableXActActionItem *) lfirst(cur_item);

		/*
		 * The subtransaction that created this entry was rollbacked, we can
		 * remove it.
		 */
		if (!isCommit && xact_ai->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			xact_on_commit_actions = foreach_delete_current(xact_on_commit_actions, cur_item);
			pfree(xact_ai);
		}
		else
		{
			/* Otherwise cur_item must be preserved */
			if (xact_ai->creating_subid == mySubid)
				xact_ai->creating_subid = parentSubid;
			if (xact_ai->deleting_subid == mySubid)
				xact_ai->deleting_subid = isCommit ? parentSubid
												   : InvalidSubTransactionId;
		}
	}
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
	aclresult = object_aclcheck(VariableRelationId, varid, GetUserId(), ACL_UPDATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	/* Create dest receiver for LET */
	dest = CreateDestReceiver(DestVariable);
	SetVariableDestReceiverVarid(dest, varid);

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
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be parallel to the EXPLAIN
	 * code path.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/*
	 * run the plan to completion. The result should be only one
	 * row. For an check too_many_rows we need to read two rows.
	 */
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
 * pg_session_variables - designed for testing
 *
 * This is a function designed for testing and debugging.  It returns the
 * content of sessionvars as-is, and can therefore display entries about
 * session variables that were dropped but for which this backend didn't
 * process the shared invalidations yet.
 */
Datum
pg_session_variables(PG_FUNCTION_ARGS)
{
#define NUM_PG_SESSION_VARIABLES_ATTS 10

	elog(DEBUG1, "pg_session_variables start");

	InitMaterializedSRF(fcinfo, 0);

	if (sessionvars)
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		HASH_SEQ_STATUS status;
		SVariable	svar;

		/*
		 * Make sure that all entries in sessionvars hash table are valid, but
		 * keeping variables dropped by the current transaction.
		 */
		sync_sessionvars_all(true);

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			Datum		values[NUM_PG_SESSION_VARIABLES_ATTS];
			bool		nulls[NUM_PG_SESSION_VARIABLES_ATTS];
			HeapTuple	tp;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[0] = ObjectIdGetDatum(svar->varid);
			values[3] = ObjectIdGetDatum(svar->typid);

			/* check if session variable is visible in system catalog */
			tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

			/*
			 * Sessionvars can hold data of variables removed from catalog,
			 * (and not purged) and then namespacename and name cannot be read
			 * from catalog.
			 */
			if (HeapTupleIsValid(tp))
			{
				Form_pg_variable varform = (Form_pg_variable) GETSTRUCT(tp);

				/* When we see data in catalog */
				values[1] = CStringGetTextDatum(
												get_namespace_name(varform->varnamespace));

				values[2] = CStringGetTextDatum(NameStr(varform->varname));

				values[4] = CStringGetTextDatum(format_type_be(svar->typid));
				values[5] = BoolGetDatum(false);
				values[6] = BoolGetDatum(svar->is_valid);

				values[8] = BoolGetDatum(
										 object_aclcheck(VariableRelationId, svar->varid,
														 GetUserId(), ACL_SELECT) == ACLCHECK_OK);

				values[9] = BoolGetDatum(
										 object_aclcheck(VariableRelationId, svar->varid,
														 GetUserId(), ACL_UPDATE) == ACLCHECK_OK);

				ReleaseSysCache(tp);
			}
			else
			{
				/*
				 * When session variable was removed from catalog, but we
				 * haven't processed the invlidation yet.
				 */
				nulls[1] = true;
				values[2] = CStringGetTextDatum(
												DatumGetCString(DirectFunctionCall1(oidout, svar->varid)));
				values[4] = PointerGetDatum(
											cstring_to_text(format_type_be(svar->typid)));
				values[5] = BoolGetDatum(true);
				values[6] = BoolGetDatum(svar->is_valid);
				nulls[7] = true;
				nulls[8] = true;
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	elog(DEBUG1, "pg_session_variables end");

	return (Datum) 0;
}
