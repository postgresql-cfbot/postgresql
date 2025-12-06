/*-------------------------------------------------------------------------
 *
 * session_variable.c
 *	  session variable creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/session_variable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "commands/session_variable.h"
#include "executor/executor.h"
#include "executor/svariableReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

/*
 * The session variables are stored in the backend's private memory (data,
 * metadata) in the dedicated memory context SVariableMemoryContext in binary
 * format. They are stored in the "sessionvars" hash table, whose key is the
 * name of the variable.
 *
 * Only owner (creator) can access the session variables. Because there is
 * not catalog support, there is not possibility to track dependecies, and
 * then only buildin types.
 */
typedef struct SVariableData
{
	NameData	varname;

	Oid			varowner;
	Oid			vartype;
	int32		vartypmod;
	Oid			varcollation;

	bool		isnull;
	Datum		value;

	int16		typlen;
	bool		typbyval;

	struct SVariableData *prev;
	bool		stacked;
	LocalTransactionId created_lxid;
	LocalTransactionId dropped_lxid;
} SVariableData;

typedef SVariableData *SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */

static MemoryContext SVariableMemoryContext = NULL;

/*
 * When we to remove committed dropped variables or uncommitted
 * created variables from sessionvars tab. created_or_dropped_lxid
 * is transaction id of transaction when some of DROP or CREATE variable
 * was executed.
 */
static LocalTransactionId created_or_dropped_lxid = InvalidLocalTransactionId;

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
		/* we need our own long-lived memory context */
		SVariableMemoryContext =
			AllocSetContextCreate(TopMemoryContext,
								  "session variables",
								  ALLOCSET_START_SMALL_SIZES);
	}

	vars_ctl.keysize = NAMEDATALEN;
	vars_ctl.entrysize = sizeof(SVariableData);
	vars_ctl.hcxt = SVariableMemoryContext;

	sessionvars = hash_create("Session variables", 64, &vars_ctl,
							  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

/*
 * Returns entry of session variable specified by name
 */
static SVariable
search_variable(char *varname, bool missing_ok)
{
	SVariable	svar;

	if (!sessionvars)
		create_sessionvars_hashtables();

	svar = (SVariable) hash_search(sessionvars, varname,
								   HASH_FIND, NULL);

	/* Session variable can be dropped inside current transaction */
	if (svar && svar->dropped_lxid != InvalidLocalTransactionId)
	{
		Assert(created_or_dropped_lxid == MyProc->vxid.lxid);
		Assert(svar->dropped_lxid == MyProc->vxid.lxid);
		svar = NULL;
	}

	if (!svar && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("session variable \"%s\" doesn't exist",
						varname)));

	return svar;
}

/*
 * Returns the type, typmod and collid of the given session variable.
 *
 * Raises an error when the variable doesn't exists and *error is null.
 */
void
get_session_variable_type_typmod_collid(char *varname,
										Oid *typid,
										int32 *typmod,
										Oid *collid)
{
	SVariable	svar;

	svar = search_variable(varname, false);

	/* only owner can set content of variable */
	*typid = svar->vartype;
	*typmod = svar->vartypmod;
	*collid = svar->varcollation;
}

/*
 * Returns a copy of the value of the session variable (in the current memory
 * context).
 */
Datum
GetSessionVariableWithTypecheck(char *varname,
								Oid typid, int32 typmod,
								bool *isnull)
{
	SVariable	svar;
	Datum		result;

	svar = search_variable(varname, false);

	if (svar->vartype != typid || svar->vartypmod != typmod)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("session variable %s is not of a type %s but type %s",
						varname,
						format_type_with_typemod(typid, typmod),
						format_type_with_typemod(svar->vartype, svar->vartypmod))));

	/* only owner can get content of variable */
	if (svar->varowner != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for session variable %s",
						varname)));

	if (!svar->isnull)
		result = datumCopy(svar->value, svar->typbyval, svar->typlen);
	else
		result = (Datum) 0;

	*isnull = svar->isnull;

	return result;
}

/*
 * Store the given value in a session variable in the cache.
 */
void
SetSessionVariableWithTypecheck(char *varname,
								Oid typid, int32 typmod,
								Datum value, bool isnull)
{
	SVariable	svar;

	svar = search_variable(varname, false);

	if (svar->vartype != typid || svar->vartypmod != typmod)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("session variable %s is not of a type %s but type %s",
						varname,
						format_type_with_typemod(typid, typmod),
						format_type_with_typemod(svar->vartype, svar->vartypmod))));

	/* only owner can set content of variable */
	if (svar->varowner != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for session variable %s",
						varname)));

	if (!svar->typbyval)
	{
		if (!isnull)
		{
			MemoryContext oldcxt;

			/*
			 * Do copy of value in session variables context. This operation
			 * can fail, so do it before releasing the old content.
			 */
			oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);
			value = datumCopy(value, svar->typbyval, svar->typlen);
			MemoryContextSwitchTo(oldcxt);
		}

		if (!svar->isnull)
			pfree(DatumGetPointer(svar->value));
	}

	svar->value = value;
	svar->isnull = isnull;
}

/*
 * Creates a new variable - does new entry in sessionvars
 *
 * Used by CREATE VARIABLE command
 */
void
CreateVariable(ParseState *pstate, CreateSessionVarStmt *stmt)
{
	Oid			typeid;
	int32		typmod;
	Oid			typcollation;
	Oid			varowner = GetUserId();
	SVariable	svar;
	SVariable	prev_svar = NULL;
	bool		found;
	int16		typlen;
	bool		typbyval;

	/*
	 * Current implementation is not catalog based, but we expect catalog
	 * based implementation for future, so we force same limits.
	 */
	PreventCommandIfReadOnly("CREATE VARIABLE");
	PreventCommandIfParallelMode("CREATE VARIABLE");
	PreventCommandDuringRecovery("CREATE VARIABLE");

	typenameTypeIdAndMod(pstate, stmt->typeName, &typeid, &typmod);

	if (get_typtype(typeid) != TYPTYPE_BASE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s is not a base type",
						format_type_be(typeid))));

	if (OidIsValid(get_element_type(typeid)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s type is an array",
						format_type_be(typeid))));

	/* allow only buildin types */
	if (typeid >= FirstUnpinnedObjectId)
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("session variable cannot have a user-defined type"),
				errdetail("Session variables that make use of user-defined types are not yet supported."));

	get_typlenbyval(typeid, &typlen, &typbyval);
	typcollation = get_typcollation(typeid);

	if (!sessionvars)
		create_sessionvars_hashtables();

	svar = hash_search(sessionvars, stmt->name,
					   HASH_ENTER, &found);

	if (found)
	{
		if (svar->dropped_lxid == InvalidLocalTransactionId)
		{
			if (stmt->if_not_exists)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("session variable \"%s\" already exists, skipping",
								stmt->name)));
				return;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("session variable \"%s\" already exists",
								stmt->name)));
		}
		else
		{
			MemoryContext oldcxt;

			Assert(created_or_dropped_lxid == MyProc->vxid.lxid);
			Assert(svar->dropped_lxid == MyProc->vxid.lxid);

			oldcxt = MemoryContextSwitchTo(SVariableMemoryContext);
			prev_svar = palloc_object(SVariableData);
			memcpy(prev_svar, svar, sizeof(SVariableData));
			prev_svar->stacked = true;
			memset(svar, 0, sizeof(SVariableData));

			MemoryContextSwitchTo(oldcxt);
		}
	}

	namestrcpy(&svar->varname, stmt->name);
	svar->vartype = typeid;
	svar->vartypmod = typmod;
	svar->varcollation = typcollation;
	svar->varowner = varowner;
	svar->typlen = typlen;
	svar->typbyval = typbyval;

	svar->value = (Datum) 0;
	svar->isnull = true;

	svar->prev = prev_svar;
	svar->stacked = false;
	svar->dropped_lxid = InvalidLocalTransactionId;
	svar->created_lxid = MyProc->vxid.lxid;
	created_or_dropped_lxid = MyProc->vxid.lxid;
}

/*
 * Drop variable by name
 */
void
DropVariableByName(DropSessionVarStmt *stmt)
{
	SVariable	svar;

	/*
	 * Current implementation is not catalog based, but we expect catalog
	 * based implementation for future, so we force same limits.
	 */
	PreventCommandIfReadOnly("DROP VARIABLE");
	PreventCommandIfParallelMode("DROP VARIABLE");
	PreventCommandDuringRecovery("DROP VARIABLE");

	svar = search_variable(stmt->name, stmt->missing_ok);
	if (!svar)
	{
		ereport(NOTICE,
				(errmsg("session variable \"%s\" does not exists, skipping",
						stmt->name)));
		return;
	}

	/* only owner can get content of variable */
	if (svar->varowner != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of session variable %s",
						stmt->name)));

	svar->dropped_lxid = MyProc->vxid.lxid;
	created_or_dropped_lxid = MyProc->vxid.lxid;
}

static void
free_svar_value(SVariable svar)
{
	if (!svar->isnull && !svar->typbyval)
		pfree(DatumGetPointer(svar->value));
}

static void
free_stacked_svars(SVariable svar)
{
	while (svar)
	{
		SVariable	current = svar;

		free_svar_value(current);
		svar = current->prev;
		pfree(current);
	}
}

/*
 * remove dropped committed entries or created uncommitted entries
 * from hash table.
 */
void
AtPreEOXact_SessionVariables(bool isCommit)
{
	if (created_or_dropped_lxid != InvalidLocalTransactionId)
	{
		HASH_SEQ_STATUS status;
		SVariable	svar;

		Assert(created_or_dropped_lxid == MyProc->vxid.lxid);
		Assert(sessionvars);

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			if ((svar->dropped_lxid != InvalidLocalTransactionId) ||
				(svar->created_lxid != InvalidLocalTransactionId))
			{
				Assert((svar->dropped_lxid == InvalidLocalTransactionId) ||
						(svar->dropped_lxid == MyProc->vxid.lxid));

				Assert((svar->created_lxid == InvalidLocalTransactionId) ||
						(svar->created_lxid == MyProc->vxid.lxid));

				if (isCommit)
				{
					if (svar->dropped_lxid == MyProc->vxid.lxid)
					{
						free_stacked_svars(svar->prev);
						free_svar_value(svar);

						(void) hash_search(sessionvars,
										   NameStr(svar->varname),
										   HASH_REMOVE,
										   NULL);
						svar = NULL;
					}
					else
					{
						free_stacked_svars(svar->prev);
						svar->prev = NULL;
						svar->created_lxid = InvalidLocalTransactionId;
					}
				}
				else
				{
					SVariable	iter;

					/*
					 * We have to search value the oldest svar in the stack. If it is just dropped,
					 * then we revert dropped flag. If it is created in current transaction, then
					 * we remove this svar too.
					 */
					iter = svar;
					while (iter->prev)
					{
						SVariable	current = iter;

						free_svar_value(current);

						iter = current->prev;

						if (current->stacked)
							pfree(current);
					}

					if (iter->created_lxid == MyProc->vxid.lxid)
					{
						free_svar_value(iter);
						if (iter->stacked)
							pfree(iter);

						(void) hash_search(sessionvars,
										   NameStr(svar->varname),
										   HASH_REMOVE,
										   NULL);
					}
					else
					{
						if (iter->stacked)
						{
							memcpy(svar, iter, sizeof(SVariableData));
							svar->stacked = false;
							pfree(iter);
						}

						/* revert dropped flag */
						svar->dropped_lxid = InvalidLocalTransactionId;
					}
				}
			}
		}

		created_or_dropped_lxid = InvalidLocalTransactionId;
	}
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
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	char	   *varname = query->resultVariable;
	SVariable	svar;

	svar = search_variable(varname, false);

	/* only owner can set content of variable */
	if (svar->varowner != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for session variable %s",
						varname)));

	/* create a dest receiver for LET */
	dest = CreateVariableDestReceiver(varname);

	/* run the query rewriter */
	query = copyObject(query);

	rewritten = QueryRewrite(query);

	Assert(list_length(rewritten) == 1);

	query = linitial_node(Query, rewritten);
	Assert(query->commandType == CMD_SELECT);

	/* plan the query */
	plan = pg_plan_query(query, pstate->p_sourcetext,
						 CURSOR_OPT_PARALLEL_OK, params, NULL);

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
	ExecutorRun(queryDesc, ForwardScanDirection, 2L);

	/* save the rowcount if we're given a QueryCompletion to fill */
	if (qc)
		SetQueryCompletion(qc, CMDTAG_LET, queryDesc->estate->es_processed);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	dest->rDestroy(dest);
	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
}

/*
 * This is used by the DISCARD TEMP.
 */
void
ResetSessionVariables(void)
{
	/* mark all session variables as dropped */
	if (sessionvars)
	{
		HASH_SEQ_STATUS status;
		SVariable	svar;
		bool		found = false;

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			svar->dropped_lxid = MyProc->vxid.lxid;
			found = true;
		}

		if (found)
			created_or_dropped_lxid = MyProc->vxid.lxid;
	}
}

/*
 * pg_get_temporary_session_variables_names
 *
 * Returns list of temporary session variables. It is used by psql's
 * tab complete for DROP VARIABLE and LET commands.
 */
Datum
pg_get_temporary_session_variables_names(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	if (sessionvars)
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		HASH_SEQ_STATUS status;
		SVariable	svar;

		hash_seq_init(&status, sessionvars);

		while ((svar = (SVariable) hash_seq_search(&status)) != NULL)
		{
			Datum		values[1];
			bool		nulls[1];

			values[0] = CStringGetTextDatum((NameStr(svar->varname)));
			nulls[0] = false;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}

	return (Datum) 0;
}
