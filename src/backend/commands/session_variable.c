/*-------------------------------------------------------------------------
 *
 * session_variable.c
 *	  session variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/session_variable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_variable.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "commands/session_variable.h"
#include "executor/svariableReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

typedef struct SVariableXActDropItem
{
	Oid			varid;			/* varid of session variable */

	/*
	 * creating_subid is the ID of the creating subxact. If the action was
	 * unregistered during the current transaction, deleting_subid is the ID
	 * of the deleting subxact, otherwise InvalidSubTransactionId.
	 */
	SubTransactionId creating_subid;
	SubTransactionId deleting_subid;
} SVariableXActDropItem;

/*
 * The values of session variables are stored in the backend's private memory
 * in the dedicated memory context SVariableMemoryContext in binary format.
 * They are stored in the "sessionvars" hash table, whose key is the OID of the
 * variable.  However, the OID is not good enough to identify a session
 * variable: concurrent sessions could drop the session variable and create a
 * new one, which could be assigned the same OID.  To ensure that the values
 * stored in memory and the catalog definition match, we also keep track of
 * the "create_lsn".  Before any access to the variable values, we need to
 * check if the LSN stored in memory matches the LSN in the catalog.  If there
 * is a mismatch between the LSNs, or if the OID is not present in pg_variable
 * at all, the value stored in memory is released.
 */
typedef struct SVariableData
{
	Oid			varid;			/* pg_variable OID of the variable (hash key) */
	XLogRecPtr	create_lsn;

	bool		isnull;
	Datum		value;

	Oid			typid;
	int16		typlen;
	bool		typbyval;

	bool		is_domain;

	/*
	 * domain_check_extra holds cached domain metadata.  This "extra" is
	 * usually stored in fn_mcxt.  We do not have access to that memory context
	 * for session variables, but we can use TopTransactionContext instead.
	 * A fresh value is forced when we detect we are in a different transaction
	 * (the local transaction ID differs from domain_check_extra_lxid).
	 */
	void	   *domain_check_extra;
	LocalTransactionId domain_check_extra_lxid;

	/*
	 * Top level local transaction id of the last transaction that dropped the
	 * variable if any.  We need this information to avoid freeing memory for
	 * variable dropped by the local backend that may be eventually
	 * rollbacked.
	 */
	LocalTransactionId drop_lxid;

	/*
	 * Stored value and type description can be outdated when we receive a
	 * sinval message.  We then have to check if the stored data are still
	 * trustworthy.
	 */
	bool		is_valid;

	uint32		hashvalue;		/* used for pairing sinval message */
} SVariableData;

typedef SVariableData *SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */

static MemoryContext SVariableMemoryContext = NULL;

/* true after accepted sinval message */
static bool needs_validation = false;

/*
 * The content of session variables is not removed immediately. When it
 * is possible we do this at the transaction end. But when the transaction failed,
 * we cannot do it, because we lost access to the system catalog. So we
 * try to do it in the next transaction before any get or set of any session
 * variable. We don't want to repeat this opening cleaning in transaction,
 * So we store the id of the transaction where opening validation was done.
 */
static LocalTransactionId validated_lxid = InvalidLocalTransactionId;

/* list holds fields of SVariableXActDropItem type */
static List *xact_drop_items = NIL;

static void register_session_variable_xact_drop(Oid varid);
static void unregister_session_variable_xact_drop(Oid varid);

/*
 * Callback function for session variable invalidation.
 */
static void
pg_variable_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	SVariable	svar;

	elog(DEBUG1, "pg_variable_cache_callback %u %u", cacheid, hashvalue);

	/*
	 * There is no guarantee of sessionvars being initialized, even when
	 * receiving an invalidation callback, as DISCARD [ ALL | VARIABLES ]
	 * destroys the hash table entirely.
	 */
	if (!sessionvars)
		return;

	/*
	 * If the hashvalue is not specified, we have to recheck all currently
	 * used session variables.  Since we can't tell the exact session variable
	 * from its hashvalue, we have to iterate over all items in the hash bucket.
	 */
	hash_seq_init(&status, sessionvars);

	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (hashvalue == 0 || svar->hashvalue == hashvalue)
		{
			svar->is_valid = false;
			needs_validation = true;
		}
	}
}

/*
 * Do the necessary work to setup local memory management of a new
 * variable.
 *
 * Caller should already have created the necessary entry in catalog
 * and made them visible.
 */
void
SessionVariableCreatePostprocess(Oid varid, char XactEndAction)
{
	/*
	 * For temporary variables, we need to create a new end of xact action to
	 * ensure deletion from catalog.
	 */
	if (XactEndAction == VARIABLE_XACTEND_DROP)
	{
		Assert(isTempNamespace(get_session_variable_namespace(varid)));

		register_session_variable_xact_drop(varid);
	}
}

/*
 * Handle the local memory cleanup for a DROP VARIABLE command.
 *
 * Caller should take care of removing the pg_variable entry first.
 */
void
SessionVariableDropPostprocess(Oid varid, char XactEndAction)
{
	Assert(LocalTransactionIdIsValid(MyProc->vxid.lxid));

	if (XactEndAction == VARIABLE_XACTEND_DROP)
	{
		Assert(isTempNamespace(get_session_variable_namespace(varid)));

		unregister_session_variable_xact_drop(varid);
	}

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
			 * validate_all_session_variables, as the DROP VARIABLE will send
			 * an invalidation message.
			 */
			svar->is_valid = false;
			svar->drop_lxid = MyProc->vxid.lxid;

			needs_validation = true;
		}
	}
}

/*
 * Registration of actions to be executed on session variables at transaction
 * end time. We want to drop temporary session variables with clause ON COMMIT
 * DROP.
 */

/*
 * Register a session variable xact action.
 */
static void
register_session_variable_xact_drop(Oid varid)
{
	SVariableXActDropItem *xact_ai;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	xact_ai = (SVariableXActDropItem *)
		palloc(sizeof(SVariableXActDropItem));

	xact_ai->varid = varid;

	xact_ai->creating_subid = GetCurrentSubTransactionId();
	xact_ai->deleting_subid = InvalidSubTransactionId;

	xact_drop_items = lcons(xact_ai, xact_drop_items);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Unregister an id of a given session variable from drop list. In this
 * moment, the action is just marked as deleted by setting deleting_subid. The
 * calling even might be rollbacked, in which case we should not lose this
 * action.
 */
static void
unregister_session_variable_xact_drop(Oid varid)
{
	ListCell   *l;

	foreach(l, xact_drop_items)
	{
		SVariableXActDropItem *xact_ai =
			(SVariableXActDropItem *) lfirst(l);

		if (xact_ai->varid == varid)
			xact_ai->deleting_subid = GetCurrentSubTransactionId();
	}
}

/*
 * Release stored value, free memory
 */
static void
free_session_variable_value(SVariable svar)
{
	/* clean the current value */
	if (!svar->isnull)
	{
		if (!svar->typbyval)
			pfree(DatumGetPointer(svar->value));

		svar->isnull = true;
	}

	svar->value = (Datum) 0;
}

/*
 * Returns true when the entry in pg_variable is consistent with the given
 * session variable.
 */
static bool
is_session_variable_valid(SVariable svar)
{
	HeapTuple	tp;
	bool		result = false;

	Assert(OidIsValid(svar->varid));

	tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(svar->varid));

	if (HeapTupleIsValid(tp))
	{
		/*
		 * The OID alone is not enough as an unique identifier, because OID
		 * values get recycled, and a new session variable could have got
		 * the same OID.  We do a second check against the 64-bit LSN when
		 * the variable was created.
		 */
		if (svar->create_lsn == ((Form_pg_variable) GETSTRUCT(tp))->varcreate_lsn)
			result = true;

		ReleaseSysCache(tp);
	}

	return result;
}

/*
 * It checks all possibly invalid entries against the system catalog.
 * During this validation, the system cache can be invalidated, and the
 * some sinval message can be accepted. This routine doesn't ensure
 * all living entries of sessionvars will have is_valid flag, but it ensures
 * that all entries are checked once.
 *
 * This routine is called before any usage (read, write, debug) of session
 * variables (only once in a transaction) or at a transaction end.  At the
 * end of transaction (specified by true of argument atEOX) we can
 * throw all invalid variables. Inside the transaction (atEOX is false) we want
 * to postpone cleaning of variables dropped inside the current transaction.
 * The current transaction can be aborted, the related drop command will be
 * aborted too. In this case we don't want lose the content of the variable,
 * and therefore we need to hold the content of the dropped session variable
 * until the end of the transaction (where the variable was dropped).
 */
static void
remove_invalid_session_variables(bool atEOX)
{
	HASH_SEQ_STATUS status;
	SVariable	svar;

	/*
	 * The validation requires an access to system catalog, and then the
	 * session state should be "in transaction".
	 */
	Assert(IsTransactionState());

	if (!needs_validation || !sessionvars)
		return;

	/*
	 * Reset this flag here, before we start the validation. It can be set to
	 * on by incomming sinval message.
	 */
	needs_validation = false;

	elog(DEBUG1, "effective call of validate_all_session_variables()");

	hash_seq_init(&status, sessionvars);
	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (!svar->is_valid)
		{
			if (!atEOX && svar->drop_lxid == MyProc->vxid.lxid)
			{
				needs_validation = true;
				continue;
			}

			if (!is_session_variable_valid(svar))
			{
				Oid			varid = svar->varid;

				free_session_variable_value(svar);
				hash_search(sessionvars, &varid, HASH_REMOVE, NULL);
				svar = NULL;
			}
			else
				svar->is_valid = true;
		}
	}
}

/*
  * Perform ON COMMIT DROP for temporary session variables,
  * and remove all dropped variables from memory.
 */
void
AtPreEOXact_SessionVariables(bool isCommit)
{
	if (isCommit)
	{
		if (xact_drop_items)
		{
			ListCell   *l;

			foreach(l, xact_drop_items)
			{
				SVariableXActDropItem *xact_ai =
					(SVariableXActDropItem *) lfirst(l);

				/* iterate only over entries that are still pending */
				if (xact_ai->deleting_subid == InvalidSubTransactionId)
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
					elog(DEBUG1, "session variable (oid:%u) will be deleted (forced by ON COMMIT DROP clause)",
						 xact_ai->varid);

					performDeletion(&object, DROP_CASCADE,
									PERFORM_DELETION_INTERNAL |
									PERFORM_DELETION_QUIETLY);
				}
			}
		}

		remove_invalid_session_variables(true);
	}

	/*
	 * We have to clean xact_drop_items. All related variables are dropped
	 * now, or lost inside aborted transaction.
	 */
	list_free_deep(xact_drop_items);
	xact_drop_items = NULL;
}

/*
 * Post-subcommit or post-subabort cleanup of xact drop list.
 *
 * During subabort, we can immediately remove entries created during this
 * subtransaction. During subcommit, just transfer entries marked during
 * this subtransaction as being the parent's responsibility.
 */
void
AtEOSubXact_SessionVariables(bool isCommit,
							 SubTransactionId mySubid,
							 SubTransactionId parentSubid)
{
	ListCell   *cur_item;

	foreach(cur_item, xact_drop_items)
	{
		SVariableXActDropItem *xact_ai =
			(SVariableXActDropItem *) lfirst(cur_item);

		if (!isCommit && xact_ai->creating_subid == mySubid)
		{
			/* cur_item must be removed */
			xact_drop_items = foreach_delete_current(xact_drop_items, cur_item);
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

/*
 * Initialize attributes cached in "svar"
 */
static void
setup_session_variable(SVariable svar, Oid varid)
{
	HeapTuple	tup;
	Form_pg_variable varform;

	Assert(OidIsValid(varid));

	tup = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(varid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for session variable %u", varid);

	varform = (Form_pg_variable) GETSTRUCT(tup);

	svar->varid = varid;
	svar->create_lsn = varform->varcreate_lsn;

	svar->typid = varform->vartype;

	get_typlenbyval(svar->typid, &svar->typlen, &svar->typbyval);

	svar->is_domain = (get_typtype(varform->vartype) == TYPTYPE_DOMAIN);
	svar->domain_check_extra = NULL;
	svar->domain_check_extra_lxid = InvalidLocalTransactionId;

	svar->drop_lxid = InvalidTransactionId;

	svar->isnull = true;
	svar->value = (Datum) 0;

	svar->is_valid = true;

	svar->hashvalue = GetSysCacheHashValue1(VARIABLEOID,
											ObjectIdGetDatum(varid));

	ReleaseSysCache(tup);
}

/*
 * Assign a new value to the session variable.  It is copied to
 * SVariableMemoryContext if necessary.
 *
 * If any error happens, the existing value won't be modified.
 */
static void
set_session_variable(SVariable svar, Datum value, bool isnull)
{
	Datum		newval;
	SVariableData locsvar,
			   *_svar;

	Assert(svar);
	Assert(!isnull || value == (Datum) 0);

	/*
	 * Use typbyval, typbylen from session variable only when they are
	 * trustworthy (the invalidation message was not accepted for this
	 * variable).  If the variable might be invalid, force setup.
	 *
	 * Do not overwrite the passed session variable until we can be certain
	 * that no error can be thrown.
	 */
	if (!svar->is_valid)
	{
		setup_session_variable(&locsvar, svar->varid);
		_svar = &locsvar;
	}
	else
		_svar = svar;

	if (!isnull)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

		newval = datumCopy(value, _svar->typbyval, _svar->typlen);

		MemoryContextSwitchTo(oldcxt);
	}
	else
		newval = value;

	free_session_variable_value(svar);

	elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new value",
		 get_namespace_name(get_session_variable_namespace(svar->varid)),
		 get_session_variable_name(svar->varid),
		 svar->varid);

	/* no more error expected, so we can overwrite the old variable now */
	if (svar != _svar)
		memcpy(svar, _svar, sizeof(SVariableData));

	svar->value = newval;
	svar->isnull = isnull;
}

/*
 * Create the hash table for storing session variables.
 */
static void
create_sessionvars_hashtables(void)
{
	HASHCTL		vars_ctl;

	Assert(!sessionvars);

	if (!SVariableMemoryContext)
	{
		/* read sinval messages */
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  pg_variable_cache_callback,
									  (Datum) 0);

		/* we need our own long-lived memory context */
		SVariableMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "session variables",
								  ALLOCSET_START_SMALL_SIZES);
	}

	memset(&vars_ctl, 0, sizeof(vars_ctl));
	vars_ctl.keysize = sizeof(Oid);
	vars_ctl.entrysize = sizeof(SVariableData);
	vars_ctl.hcxt = SVariableMemoryContext;

	sessionvars = hash_create("Session variables", 64, &vars_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Search a session variable in the hash table given its OID.  If it
 * doesn't exist, then insert it there.
 *
 * The caller is responsible for doing permission checks.
 *
 * As a side effect, this function acquires a AccessShareLock on the
 * session variable until the end of the transaction.
 */
static SVariable
get_session_variable(Oid varid)
{
	SVariable	svar;
	bool		found;

	/* protect the used session variable against DROP */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtables();

	if (validated_lxid == InvalidLocalTransactionId ||
		validated_lxid != MyProc->vxid.lxid)
	{
		/* throw invalid entries, skip entries dropped by this transaction. */
		remove_invalid_session_variables(false);

		/* don't repeat it in this transaction */
		validated_lxid = MyProc->vxid.lxid;
	}

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_ENTER, &found);

	if (found)
	{
		/*
		 * The session variable can be dropped by DROP VARIABLE command,
		 * but effect of this command can be reverted by ROLLBACK to savepoint,
		 * so we can work here with readable value in variable marked as invalid.
		 */
		if (!svar->is_valid)
		{
			/*
			 * If there was an invalidation message, the variable might still be
			 * valid, but we have to check with the system catalog.
			 *
			 * If we access this session variable, then the variable should be
			 * possibly validated. The oid should be valid, because related
			 * session variable is locked already, and remove_invalid_session_variables
			 * should to remove variables dropped by other transactions.
			 */
			if (is_session_variable_valid(svar))
				svar->is_valid = true;
			else
			{
				/* we don't expect it */
				elog(ERROR, "unexpected state of session variable %u", varid);
			}
		}
	}
	else
		svar->is_valid = false;

	/*
	 * Force setup for not yet initialized variables or variables that cannot
	 * be validated.
	 */
	if (!svar->is_valid)
	{
		setup_session_variable(svar, varid);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has assigned entry in memory (emitted by READ)",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid),
			 varid);
	}

	/* ensure the returned data is still of the correct domain */
	if (svar->is_domain)
	{
		/*
		 * Store "extra" for domain_check() in TopTransactionContext.  When we
		 * are in a new transaction, domain_check_extra cache is not valid any
		 * more.
		 */
		if (svar->domain_check_extra_lxid != MyProc->vxid.lxid)
			svar->domain_check_extra = NULL;

		domain_check(svar->value, svar->isnull,
					 svar->typid, &svar->domain_check_extra,
					 TopTransactionContext);

		svar->domain_check_extra_lxid = MyProc->vxid.lxid;
	}

	return svar;
}

/*
 * Store the given value in a session variable in the cache.
 *
 * The caller is responsible for doing permission checks.
 *
 * As a side effect, this function acquires a AccessShareLock on the session
 * variable until the end of the transaction.
 */
void
SetSessionVariable(Oid varid, Datum value, bool isNull)
{
	SVariable	svar;
	bool		found;

	/* protect used session variable against DROP */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtables();

	if (validated_lxid == InvalidLocalTransactionId ||
		validated_lxid != MyProc->vxid.lxid)
	{
		/* throw invalid entries, skip entries dropped by this transaction */
		remove_invalid_session_variables(false);

		/* don't repeat it in this transaction */
		validated_lxid = MyProc->vxid.lxid;
	}

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_ENTER, &found);

	if (!found)
	{
		setup_session_variable(svar, varid);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has assigned entry in memory (emitted by WRITE)",
			 get_namespace_name(get_session_variable_namespace(svar->varid)),
			 get_session_variable_name(svar->varid),
			 varid);
	}

	/* if this fails, it won't change the stored value */
	set_session_variable(svar, value, isNull);
}

/*
 * Wrapper around SetSessionVariable with permission checks.
 */
void
SetSessionVariableWithSecurityCheck(Oid varid, Datum value, bool isNull)
{
	AclResult	aclresult;

	/* is the caller allowed to update the session variable? */
	aclresult = object_aclcheck(VariableRelationId, varid, GetUserId(), ACL_UPDATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	SetSessionVariable(varid, value, isNull);
}

/*
 * Returns a copy of the value stored in a variable.
 */
static inline Datum
copy_session_variable_value(SVariable svar, bool *isNull)
{
	Datum		value;

	/* force copy of non NULL value */
	if (!svar->isnull)
	{
		value = datumCopy(svar->value, svar->typbyval, svar->typlen);
		*isNull = false;
	}
	else
	{
		value = (Datum) 0;
		*isNull = true;
	}

	return value;
}

/*
 * Returns a copy of the value of the session variable (in the current memory
 * context).  The caller is responsible for permission checks.
 */
Datum
GetSessionVariable(Oid varid, bool *isNull, Oid *typid)
{
	SVariable	svar;

	svar = get_session_variable(varid);

	/*
	 * Although "svar" is freshly validated in this point, svar->is_valid can
	 * be false, if an invalidation message ws processed during the domain check.
	 * But the variable and all its dependencies are locked now, so we don't need
	 * to repeat the validation.
	 */
	Assert(svar);

	*typid = svar->typid;

	return copy_session_variable_value(svar, isNull);
}

/*
 * Returns a copy of the value of the session variable after checking if the
 * type is the same as "expected_typid".  The caller is responsible for
 * permission checks.
 */
Datum
GetSessionVariableWithTypeCheck(Oid varid, bool *isNull, Oid expected_typid)
{
	SVariable	svar;

	svar = get_session_variable(varid);

	Assert(svar && svar->is_valid);

	if (expected_typid != svar->typid)
		elog(ERROR, "type of variable \"%s.%s\" is different than expected",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid));

	return copy_session_variable_value(svar, isNull);
}

/*
 * Assign the result of the evaluated expression to the session variable
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

	/* do we have permission to write to the session variable? */
	aclresult = object_aclcheck(VariableRelationId, varid, GetUserId(), ACL_UPDATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_VARIABLE, get_session_variable_name(varid));

	/* create a dest receiver for LET */
	dest = CreateVariableDestReceiver(varid);

	/* run the query rewriter */
	query = copyObject(query);

	rewritten = QueryRewrite(query);

	Assert(list_length(rewritten) == 1);

	query = linitial_node(Query, rewritten);
	Assert(query->commandType == CMD_SELECT);

	/* plan the query */
	plan = pg_plan_query(query, pstate->p_sourcetext,
						 CURSOR_OPT_PARALLEL_OK, params);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees the
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be parallel to the EXPLAIN
	 * code path.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/*
	 * Run the plan to completion.  The result should be only one row.  To
	 * check if there are too many result rows, we try to fetch two.
	 */
	ExecutorRun(queryDesc, ForwardScanDirection, 2L, true);

	/* save the rowcount if we're given a QueryCompletion to fill */
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
#define NUM_PG_SESSION_VARIABLES_ATTS 8

	elog(DEBUG1, "pg_session_variables start");

	remove_invalid_session_variables(false);

	InitMaterializedSRF(fcinfo, 0);

	if (sessionvars)
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		HASH_SEQ_STATUS status;
		SVariable	svar;

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			Datum		values[NUM_PG_SESSION_VARIABLES_ATTS];
			bool		nulls[NUM_PG_SESSION_VARIABLES_ATTS];
			HeapTuple	tp;
			bool		var_is_valid = false;

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

				/* when we see data in catalog */
				if (svar->create_lsn == varform->varcreate_lsn)
				{
					/* and when when these data are not out of date */
					values[1] = CStringGetTextDatum(
													get_namespace_name(varform->varnamespace));

					values[2] = CStringGetTextDatum(NameStr(varform->varname));
					values[4] = CStringGetTextDatum(format_type_be(svar->typid));
					values[5] = BoolGetDatum(false);

					values[6] = BoolGetDatum(
											 object_aclcheck(VariableRelationId, svar->varid,
															 GetUserId(), ACL_SELECT) == ACLCHECK_OK);

					values[7] = BoolGetDatum(
											 object_aclcheck(VariableRelationId, svar->varid,
															 GetUserId(), ACL_UPDATE) == ACLCHECK_OK);

					var_is_valid = true;
				}

				ReleaseSysCache(tp);
			}

			if (!var_is_valid)
			{
				/*
				 * When session variable was removed from catalog, but we
				 * haven't processed the invlidation yet. In this case, we can
				 * display only few oids. Other data are not available
				 * (without Form_pg_variable record), or can be lost (because
				 * there is not protection by dependency (more).
				 */
				nulls[1] = true;
				nulls[2] = true;
				nulls[4] = true;
				nulls[6] = true;
				nulls[7] = true;

				values[5] = BoolGetDatum(true);
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	elog(DEBUG1, "pg_session_variables end");

	return (Datum) 0;
}

/*
 * Fast drop of the complete content of all session variables hash table, and
 * cleanup of any list that wouldn't be relevant anymore.
 * This is used by DISCARD VARIABLES (and DISCARD ALL) command.
 */
void
ResetSessionVariables(void)
{
	/* destroy hash table and reset related memory context */
	if (sessionvars)
	{
		hash_destroy(sessionvars);
		sessionvars = NULL;
	}

	/* release memory allocated by session variables */
	if (SVariableMemoryContext != NULL)
		MemoryContextReset(SVariableMemoryContext);
}
