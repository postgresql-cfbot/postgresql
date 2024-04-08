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
#include "optimizer/optimizer.h"
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
 * Used for transactional variables. Holds prev version.
 */
typedef struct PrevValue
{
	Datum		value;
	bool		isnull;

	SubTransactionId modify_subid;

	struct PrevValue *prev_value;
} PrevValue;

/*
 * Values of session variables are stored in the backend local memory
 * inside sessionvars hash table in binary format inside a dedicated memory
 * context SVariableMemoryContext.  The hash key is oid
 * of related entry in pg_variable table. But long term unambiguity of oid is
 * not guaranteed. As an example, for a value with one oid a session can be
 * inactive for long time, while in the meantime the related session variable
 * can be dropped in another session, assigned oid can be released, and
 * theoreticaly this oid can be assigned to different session variable.
 * At the end, the reading of value stored in old session should to fail,
 * because related entry in pg_variable will not be consistent with
 * stored value. This is reason why we do check consistency between stored
 * value and catalog by create_lsn value.
 *
 * Before any usage (not only read in transaction) we need to check consistency
 * with pg_variable entry. When there is not entry with stored oid, the related
 * variable was dropped, and stored value is not consistent. When entry with
 * known oid, but lsn number is different, entry of pg_variable was created
 * for different variable and stored value is not consistent again.
 */
typedef struct SVariableData
{
	Oid			varid;			/* pg_variable OID of the variable (hash key) */
	XLogRecPtr	create_lsn;

	bool		isnull;
	Datum		value;

	/*
	 * We don't need stack versions modified in same subtransaction.
	 * Used by transactional variables only. The value of transactional
	 * variable can be returned immediately when modify_subid is same
	 * like current subid.
	 */
	SubTransactionId modify_subid;

	/*
	 * When the modify_subid is different than current subid, then
	 * we need to recheck versions and throw versions related to
	 * reverted transactions. When purge_subid is same like current subid
	 * we can return the value of transaction variable without this
	 * recheck.
	 */
	SubTransactionId purge_subid;

	PrevValue  *prev_value;

	Oid			typid;
	int16		typlen;
	bool		typbyval;

	bool		is_domain;

	/*
	 * domain_check_extra holds an extra (cache) for domain check.
	 * This extra is usually stored in fn_mcxt. We do not have same
	 * memory context for session variables, but we can use
	 * TopTransactionContext instead. Fresh extra is forced when
	 * we detect we are in a different transaction (different
	 * local transaction id domain_check_extra_lxid).
	 */
	void	   *domain_check_extra;
	LocalTransactionId domain_check_extra_lxid;

	bool		is_not_null;
	bool		is_immutable;
	bool		is_transact;

	bool		reset_at_eox;

	/*
	 * Top level local transaction id of the last transaction that dropped the
	 * variable if any.  We need this information to avoid freeing memory for
	 * variable dropped by the local backend that may be eventually
	 * rollbacked.
	 */
	LocalTransactionId drop_lxid;

	/*
	 * Stored value and type description can be outdated when we receive
	 * sinval message. We have to check always if the stored data are
	 * trustful.
	 */
	bool		is_valid;

	uint32		hashvalue;		/* used for pairing sinval message */

	/* true, when the value is already set, and cannot be changed more */
	bool		protect_value;
} SVariableData;

typedef SVariableData *SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */

static MemoryContext SVariableMemoryContext = NULL;

/* true after accepted sinval message */
static bool needs_validation = false;

/*
 * true, when some used session variable has ON COMMIT DROP
 * or ON TRANSACTION END RESET clauses
 */
static bool has_session_variables_with_reset_at_eox = false;

/* true, when transactional variables was modified */
static bool has_modified_transactional_variables = false;

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
static bool purge_session_variable(SVariable svar);

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
	 * When the hashvalue is not specified, then we have to recheck all
	 * currently used session variables. Since we can't guarantee the exact
	 * session variable from its hashValue, we also have to iterate over all
	 * items of the sessionvars hash table.
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
SessionVariableCreatePostprocess(Oid varid, char eoxaction)
{
	/*
	 * For temporary variables, we need to create a new end of xact action to
	 * ensure deletion from catalog.
	 */
	if (eoxaction == VARIABLE_EOX_DROP)
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
SessionVariableDropPostprocess(Oid varid, char eoxaction)
{
	Assert(LocalTransactionIdIsValid(MyProc->vxid.lxid));

	if (eoxaction == VARIABLE_EOX_DROP)
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
free_session_variable_value(SVariable svar, bool deep_free)
{
	if (deep_free)
	{
		PrevValue *prev_value = svar->prev_value;
		PrevValue *next_value;

		while (prev_value)
		{
			if (!svar->typbyval)
				pfree(DatumGetPointer(prev_value->value));

			next_value = prev_value->prev_value;
			pfree(prev_value);

			prev_value = next_value;
		}
	}

	/* Clean current value */
	if (!svar->isnull)
	{
		if (!svar->typbyval)
			pfree(DatumGetPointer(svar->value));

		svar->isnull = true;
	}

	svar->value = (Datum) 0;
}

/*
 * Returns true when the entry in pg_variable is consistent with
 * the given session variable.
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
		 * In this case, the only oid cannot be used as unique identifier,
		 * because the oid counter can wraparound, and the oid can be used for
		 * new other session variable. We do a second check against 64bit
		 * unique identifier.
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

				free_session_variable_value(svar, true);
				hash_search(sessionvars, &varid, HASH_REMOVE, NULL);
				svar = NULL;
			}
			else
				svar->is_valid = true;
		}
	}
}

/*
 * remove entries marked as "reset_at_eox"
 */
static void
remove_session_variables_with_reset_at_eox(void)
{
	HASH_SEQ_STATUS status;
	SVariable	svar;

	if (!sessionvars)
		return;

	/* leave quckly, when there are not that variables */
	if (!has_session_variables_with_reset_at_eox)
		return;

	hash_seq_init(&status, sessionvars);
	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (svar->reset_at_eox)
			hash_search(sessionvars, &svar->varid, HASH_REMOVE, NULL);
	}

	has_session_variables_with_reset_at_eox = false;
}

/*
 * remove prev values at eox
 */
static void
remove_prev_values_at_eox(bool isCommit)
{
	HASH_SEQ_STATUS status;
	SVariable	svar;

	if (!sessionvars)
		return;

	/* leave quckly, when there are not that variables */
	if (!has_modified_transactional_variables)
		return;

	hash_seq_init(&status, sessionvars);
	while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
	{
		if (svar->is_transact && svar->modify_subid != InvalidSubTransactionId)
		{
			if (isCommit)
			{
				if (purge_session_variable(svar))
				{
					PrevValue *prev_value = svar->prev_value;

					while (prev_value)
					{
						PrevValue *current_pv = prev_value;

						if (!svar->typbyval && !current_pv->isnull)
							pfree(DatumGetPointer(current_pv->value));

						prev_value = current_pv->prev_value;
						pfree(current_pv);
					}
				}
				else
				{
					hash_search(sessionvars, &svar->varid, HASH_REMOVE, NULL);
					svar = NULL;
				}
			}
			else
			{
				PrevValue *prev_value = svar->prev_value;

				while (prev_value)
				{
					PrevValue *current_pv = prev_value;

					if (current_pv->modify_subid == InvalidSubTransactionId)
						break;

					if (!svar->typbyval && !current_pv->isnull)
						pfree(DatumGetPointer(current_pv->value));

					prev_value = current_pv->prev_value;
					pfree(current_pv);
				}

				if (prev_value)
				{
					svar->value = prev_value->value;
					svar->isnull = prev_value->isnull;

					pfree(prev_value);
				}
				else
				{
					hash_search(sessionvars, &svar->varid, HASH_REMOVE, NULL);
					svar = NULL;
				}
			}

			/* When svar is still valid (not removed from sessionvars */
			if (svar)
			{
				svar->modify_subid = InvalidSubTransactionId;
				svar->prev_value = NULL;
			}
		}
	}

	has_modified_transactional_variables = false;
}

/*
  * Perform ON COMMIT DROP for temporary session variables,
  * and remove all dropped variables from memory.
 */
void
AtPreEOXact_SessionVariables(bool isCommit)
{
	remove_session_variables_with_reset_at_eox();

	if (isCommit)
	{
		if (xact_drop_items)
		{
			ListCell   *l;

			foreach(l, xact_drop_items)
			{
				SVariableXActDropItem *xact_ai =
					(SVariableXActDropItem *) lfirst(l);

				/* Iterate only over entries that are still pending */
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

	remove_prev_values_at_eox(isCommit);

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
 * evaluate an expression
 */
static void
eval_assign_defexpr(SVariable svar, HeapTuple tup)
{
	Datum		defexpr_value;
	bool		isnull;

	Assert(svar);
	Assert(svar->is_valid);
	Assert(HeapTupleIsValid(tup));

	defexpr_value = SysCacheGetAttr(VARIABLEOID,
									tup,
									Anum_pg_variable_vardefexpr,
									&isnull);

	if (!isnull)
	{
		EState	   *estate;
		ExprState  *defexprs;
		Expr	   *defexpr;
		char	   *defexpr_str;
		Datum		value;
		MemoryContext oldcxt;

		estate = CreateExecutorState();

		defexpr_str = TextDatumGetCString(defexpr_value);
		defexpr = (Expr *) stringToNode(defexpr_str);

		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

		defexpr = expression_planner((Expr *) defexpr);
		defexprs = ExecInitExpr(defexpr, NULL);

		value = ExecEvalExprSwitchContext(defexprs,
										  GetPerTupleExprContext(estate),
										  &isnull);

		MemoryContextSwitchTo(oldcxt);

		if (!isnull)
		{
			oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

			svar->value = datumCopy(value, svar->typbyval, svar->typlen);
			svar->isnull = false;

			MemoryContextSwitchTo(oldcxt);
		}
		else
		{
			/*
			 * Raise an error if this is a NOT NULL variable but the result of
			 * DEFAULT expression is NULL.
			 */
			if (svar->is_not_null)
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
								get_namespace_name(get_session_variable_namespace(svar->varid)),
								get_session_variable_name(svar->varid)),
						 errdetail("The result of DEFAULT expression is NULL.")));
		}

		if (svar->is_immutable)
			svar->protect_value = true;

		FreeExecutorState(estate);
	}
}

/*
 * Update attributes cached in svar
 */
static void
setup_session_variable(SVariable svar, Oid varid, bool is_write)
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

	svar->is_not_null = varform->varisnotnull;
	svar->is_immutable = varform->varisimmutable;
	svar->is_transact = varform->varistransact;

	svar->is_domain = (get_typtype(varform->vartype) == TYPTYPE_DOMAIN);
	svar->domain_check_extra = NULL;
	svar->domain_check_extra_lxid = InvalidLocalTransactionId;

	/*
	 * We don't need to explicitly reset variables marked ON COMMIT DROP. It
	 * can be done by sinval message processing. But this processing can be
	 * postponed due aborted transaction. On second hand there is not a
	 * reason, why don't do it at transaction end immediately.
	 */
	if (varform->vareoxaction == VARIABLE_EOX_RESET ||
		varform->vareoxaction == VARIABLE_EOX_DROP)
	{
		svar->reset_at_eox = true;
		has_session_variables_with_reset_at_eox = true;
	}
	else
		svar->reset_at_eox = false;

	svar->drop_lxid = InvalidTransactionId;

	svar->isnull = true;
	svar->value = (Datum) 0;

	if (svar->is_transact)
	{
		svar->modify_subid = GetCurrentSubTransactionId();
		has_modified_transactional_variables = true;
	}
	else
		svar->modify_subid = InvalidSubTransactionId;

	svar->purge_subid = InvalidSubTransactionId;
	svar->prev_value = NULL;

	svar->is_valid = true;

	svar->hashvalue = GetSysCacheHashValue1(VARIABLEOID,
											ObjectIdGetDatum(varid));

	svar->protect_value = false;

	/*
	 * When the variable is marked as IMMUTABLE, we prefer to evaluate
	 * possible DEFAULT before write op. In this case we want to protect
	 * default value against any overwrite.
	 */
	if (!is_write ||
		svar->is_immutable)
	{
		eval_assign_defexpr(svar, tup);

		/*
		 * Raise an error if this is a NOT NULL variable without default
		 * expression.
		 */
		if (svar->isnull && svar->is_not_null)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
							get_namespace_name(get_session_variable_namespace(varid)),
							get_session_variable_name(varid)),
					 errdetail("The session variable was not initialized yet.")));
	}

	ReleaseSysCache(tup);
}

/*
 * Try to remove all previous versions related to reverted transactions.
 * Returns true, when valid version was found.
 */
static bool
purge_session_variable(SVariable svar)
{
	SubTransactionId current_subid;
	PrevValue	   *prev_value;
	bool			found = true;

	Assert(svar->is_transact);

	if (svar->modify_subid == InvalidSubTransactionId)
		return true;

	current_subid = GetCurrentSubTransactionId();

	if (svar->modify_subid == current_subid)
		return true;

	if (svar->purge_subid == current_subid)
		return true;

	if (SubTransactionIsActive(svar->modify_subid))
	{
		svar->purge_subid = current_subid;
		return true;
	}

	prev_value = svar->prev_value;

	while (prev_value)
	{
		PrevValue *current_pv = prev_value;

		if (current_pv->modify_subid == InvalidSubTransactionId ||
			SubTransactionIsActive(current_pv->modify_subid))
		{
			svar->value = current_pv->value;
			svar->isnull = current_pv->isnull;
			svar->modify_subid = current_pv->modify_subid;

			prev_value = current_pv->prev_value;
			pfree(current_pv);

			found = true;
			break;
		}

		if (!svar->typbyval && !current_pv->isnull)
			pfree(DatumGetPointer(current_pv->value));

		prev_value = current_pv->prev_value;
		pfree(current_pv);
	}

	svar->prev_value = prev_value;
	svar->purge_subid = current_subid;

	return found;
}

/*
 * Assign some content to the session variable. It's copied to
 * SVariableMemoryContext if necessary.
 *
 * If any error happens, the existing value shouldn't be modified.
 */
static void
set_session_variable(SVariable svar, Datum value, bool isnull)
{
	Datum		newval;
	SVariableData locsvar,
			   *_svar;
	SubTransactionId current_subid = GetCurrentSubTransactionId();
	SubTransactionId prev_purge_subid = InvalidSubTransactionId;
	bool		save_prev_value;
	PrevValue  *prev_value;

	Assert(svar);
	Assert(!isnull || value == (Datum) 0);

	if (svar->protect_value)
		ereport(ERROR,
				(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
				 errmsg("session variable \"%s.%s\" is declared IMMUTABLE",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/* don't allow assignment of null to NOT NULL variable */
	if (isnull && svar->is_not_null)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("null value is not allowed for NOT NULL session variable \"%s.%s\"",
						get_namespace_name(get_session_variable_namespace(svar->varid)),
						get_session_variable_name(svar->varid))));

	/*
	 * Use typbyval, typbylen from session variable only when they are
	 * trustable (the invalidation message was not accepted for this variable).
	 * When the variable is possibly invalid, force setup.
	 *
	 * Do not do it against passed svar, it should be unchanged, when an
	 * assignment is not successful (the datumCopy can fail).
	 */
	if (!svar->is_valid)
	{
		setup_session_variable(&locsvar, svar->varid, false);
		_svar = &locsvar;
	}
	else
		_svar = svar;

	if (_svar->is_transact && _svar->create_lsn == svar->create_lsn)
	{
		Assert(svar->typid == _svar->typid);
		Assert(svar->typbyval == _svar->typbyval);
		Assert(svar->typlen == _svar->typlen);

		save_prev_value = svar->modify_subid != current_subid;
		prev_value = svar->prev_value;
	}
	else
	{
		save_prev_value = false;
		prev_value = NULL;
	}

	if (!isnull)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);

		newval = datumCopy(value, _svar->typbyval, _svar->typlen);

		MemoryContextSwitchTo(oldcxt);
	}
	else
		newval = value;

	if (save_prev_value)
	{
		volatile PrevValue *new_prev_value;

		PG_TRY();
		{
			new_prev_value = MemoryContextAlloc(SVariableMemoryContext,
												sizeof(PrevValue));
		}
		PG_CATCH();
		{
			/* release mem from persistent content */
			if (newval != value)
				pfree(DatumGetPointer(newval));
			PG_RE_THROW();
		}
		PG_END_TRY();

		new_prev_value->value = svar->value;
		new_prev_value->isnull = svar->isnull;
		new_prev_value->modify_subid = svar->modify_subid;
		new_prev_value->prev_value = prev_value;

		prev_value = (PrevValue *) new_prev_value;
		prev_purge_subid = svar->purge_subid;

		has_modified_transactional_variables = true;

	}
	else
		free_session_variable_value(svar, prev_value == NULL);

	/* We can overwrite old variable now. No error expected */
	if (svar != _svar)
		memcpy(svar, _svar, sizeof(SVariableData));

	svar->value = newval;
	svar->isnull = isnull;
	svar->modify_subid = current_subid;
	svar->purge_subid = prev_purge_subid;
	svar->prev_value = prev_value;

	/* don't allow more changes of value when variable is IMMUTABLE */
	if (svar->is_immutable)
		svar->protect_value = true;

	/*
	 * XXX While unlikely, an error here is possible. It wouldn't leak memory
	 * as the allocated chunk has already been correctly assigned to the
	 * session variable, but would contradict this function contract, which is
	 * that this function should either succeed or leave the current value
	 * untouched.
	 */
	elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has new value",
		 get_namespace_name(get_session_variable_namespace(svar->varid)),
		 get_session_variable_name(svar->varid),
		 svar->varid);
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

	memset(&vars_ctl, 0, sizeof(vars_ctl));
	vars_ctl.keysize = sizeof(Oid);
	vars_ctl.entrysize = sizeof(SVariableData);
	vars_ctl.hcxt = SVariableMemoryContext;

	sessionvars = hash_create("Session variables", 64, &vars_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Search a seesion variable in the hash table given its oid. If it
 * doesn't exist, then insert it there.
 *
 * Caller is responsible for doing permission checks.
 *
 * As side effect this function acquires AccessShareLock on
 * related session variable until the end of the transaction.
 */
static SVariable
get_session_variable(Oid varid)
{
	SVariable	svar;
	bool		found;

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtables();

	if (validated_lxid == InvalidLocalTransactionId ||
		validated_lxid != MyProc->vxid.lxid)
	{
		/* Throw invalid entries, skip entries dropped by this transaction. */
		remove_invalid_session_variables(false);

		/* Don't repeat it in this transaction */
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
			 * The variable can be flagged as invalid by processing invalidation
			 * message, but can be validated by recheck against system catalog.
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
				/* We don't expect it */
				elog(ERROR, "unexpected state of session variable %u", varid);
			}
		}
	}
	else
		svar->is_valid = false;

reinit:

	/*
	 * Force setup for not yet initialized variables or variables that cannot
	 * be validated.
	 */
	if (!svar->is_valid)
	{
		/* in this case we want to use defexp if it is defined */
		PG_TRY();
		{
			/*
			 * In this case, the setup can execute default expression. When
			 * the execution of default expression fails, then we need to
			 * remove entry from session vars.
			 */
			setup_session_variable(svar, varid, false);
		}
		PG_CATCH();
		{
			/* This entry cannot be valid, remove from sessionvars */
			hash_search(sessionvars, &varid, HASH_REMOVE, NULL);

			/* propagate the error */
			PG_RE_THROW();
		}
		PG_END_TRY();

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has assigned entry in memory (emitted by READ)",
			 get_namespace_name(get_session_variable_namespace(varid)),
			 get_session_variable_name(varid),
			 varid);
	}

	/*
	 * Transactional variables should be purged before (remove
	 * versions created by possibly reverted subtransactions).
	 */
	if (svar->is_transact &&
		svar->modify_subid != GetCurrentSubTransactionId() &&
		svar->modify_subid != InvalidSubTransactionId)
	{
		if (!purge_session_variable(svar))
		{
			/* force reinit */
			svar->is_valid = false;

			/*
			 * In next iteration modify_subid should be
			 * InvalidSubTransactionId or current subid,
			 * so there is not risk of infinity cycle.
			 */
			goto reinit;
		}
	}

	/* Ensure so returned data is still correct domain */
	if (svar->is_domain)
	{
		/*
		 * Store domain_check extra in TopTransactionContext. When we are in
		 * other transaction, the domain_check_extra cache is not valid
		 * anymore.
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

	/* Protect used session variable against drop until transaction end */
	LockDatabaseObject(VariableRelationId, varid, 0, AccessShareLock);

	if (!sessionvars)
		create_sessionvars_hashtables();

	if (validated_lxid == InvalidLocalTransactionId ||
		validated_lxid != MyProc->vxid.lxid)
	{
		/* Throw invalid entries, skip entries dropped by this transaction. */
		remove_invalid_session_variables(false);

		/* Don't repeat it in this transaction */
		validated_lxid = MyProc->vxid.lxid;
	}

	svar = (SVariable) hash_search(sessionvars, &varid,
								   HASH_ENTER, &found);

	if (!found)
	{
		setup_session_variable(svar, varid, true);

		elog(DEBUG1, "session variable \"%s.%s\" (oid:%u) has assigned entry in memory (emitted by WRITE)",
			 get_namespace_name(get_session_variable_namespace(svar->varid)),
			 get_session_variable_name(svar->varid),
			 varid);
	}

	/*
	 * This should either succeed or fail without changing the currently
	 * stored value.
	 */
	set_session_variable(svar, value, isNull);
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
 * Returns copy of value stored in variable.
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
 * Returns a copy of the value of the session variable (in current memory
 * context) specified by its oid. Caller is responsible for doing permission
 * checks.
 */
Datum
GetSessionVariable(Oid varid, bool *isNull, Oid *typid)
{
	SVariable	svar;

	svar = get_session_variable(varid);

	/*
	 * Although svar is freshly validated in this point, the svar->is_valid can
	 * be false, due possible accepting invalidation message inside domain
	 * check. Now, the validation is done after lock, that can also accept
	 * invalidation message, so validation should be trustful.
	 *
	 * For now, we don't need to repeat validation. Only svar should be valid
	 * pointer.
	 */
	Assert(svar);

	*typid = svar->typid;

	return copy_session_variable_value(svar, isNull);
}

/*
 * Returns a copy of the value of the session variable specified by its oid
 * with a check of the expected type. Like previous GetSessionVariable, the
 * caller is responsible for doing permission checks.
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
	dest = CreateVariableDestReceiver(varid);

	/* run rewriter */
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
	 * Run the plan to completion. The result should be only one row. For an
	 * check too_many_rows we need to read two rows.
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

				/* When we see data in catalog */
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
	/* Destroy hash table and reset related memory context */
	if (sessionvars)
	{
		hash_destroy(sessionvars);
		sessionvars = NULL;
	}

	/* Release memory allocated by session variables */
	if (SVariableMemoryContext != NULL)
		MemoryContextReset(SVariableMemoryContext);
}
