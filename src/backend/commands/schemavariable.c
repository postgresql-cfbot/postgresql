#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_variable.h"
#include "commands/schemavariable.h"
#include "executor/executor.h"
#include "executor/svariableReceiver.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

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
	int32		typmod;
	int16		typlen;
	bool		typbyval;
	bool		isnull;
	bool		freeval;
	Datum		value;
	bool		is_rowtype;		/* true when variable is composite */
	bool		is_valid;		/* true when variable was successfuly initialized */
} SchemaVariableData;

typedef SchemaVariableData *SchemaVariable;

static HTAB *schemavarhashtab = NULL;		/* hash table for session variables */
static MemoryContext SchemaVariableMemoryContext = NULL;

static bool first_time = true;
static void create_schemavar_hashtable(void);
static bool clean_cache_req = false;

static void clean_cache(void);
static void force_clean_cache(XactEvent event, void *arg);


/*
 * Save info about ncessity to clean hash table, because some
 * schema variable was dropped. Don't do here more, recheck
 * needs to be in transaction state.
 */
static void
InvalidateSchemaVarCacheCallback(Datum arg, int cacheid, uint32 hashvalue)
{
	if (cacheid != VARIABLEOID)
		return;

	clean_cache_req = true;
}

static void
force_clean_cache(XactEvent event, void *arg)
{
	/*
	 * should continue only in transaction time, when
	 * syscache is available.
	 */
	if (clean_cache_req && IsTransactionState())
	{
		clean_cache();
		clean_cache_req = false;
	}
}

static void
clean_cache(void)
{
	HASH_SEQ_STATUS status;
	SchemaVariable		var;

	if (!schemavarhashtab)
		return;

	hash_seq_init(&status, schemavarhashtab);

	/*
	 * Every valid variable have to have entry in system
	 * catalog. Removed if there is nothing.
	 */
	while ((var = (SchemaVariable) hash_seq_search(&status)) != NULL)
	{
		HeapTuple	tp = InvalidOid;

		tp = SearchSysCache1(VARIABLEOID, ObjectIdGetDatum(var->varid));
		if (!HeapTupleIsValid(tp))
		{
			elog(DEBUG1, "variable %d is removed from cache", var->varid);

			if (var->freeval)
			{
				pfree(DatumGetPointer(var->value));
				var->freeval = false;
			}

			if (hash_search(schemavarhashtab,
								(void *) &var->varid,
								HASH_REMOVE,
								NULL) == NULL)
				elog(DEBUG1, "hash table corrupted");
		}
		else
			ReleaseSysCache(tp);
	}
}

char *
VariableGetName(Variable *var)
{
	char   *nspname;

	if (VariableIsVisible(var->oid))
		nspname = NULL;
	else
		nspname = get_namespace_name(var->namespace);

	return quote_qualified_identifier(nspname, var->name);
}

/*
 * Create the hash table for storing schema variables
 */
static void
create_schemavar_hashtable(void)
{
	HASHCTL		ctl;

	/* set callbacks */
	if (first_time)
	{
		CacheRegisterSyscacheCallback(VARIABLEOID,
									  InvalidateSchemaVarCacheCallback,
									  (Datum) 0);

		RegisterXactCallback(force_clean_cache, NULL);

		first_time = false;
	}

	/* needs own long life memory context */
	if (SchemaVariableMemoryContext == NULL)
	{
		SchemaVariableMemoryContext = AllocSetContextCreate(TopMemoryContext,
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
	if (schemavarhashtab)
	{
		hash_destroy(schemavarhashtab);
		schemavarhashtab = NULL;
	}

	if (SchemaVariableMemoryContext != NULL)
	{
		MemoryContextReset(SchemaVariableMemoryContext);
	}
}

/*
 * Drop variable by OID
 */
void
RemoveVariableById(Oid varid)
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
}

/*
 * Creates new variable - entry in pg_catalog.pg_variable table
 */
ObjectAddress
DefineSchemaVariable(ParseState *pstate, CreateSchemaVarStmt *stmt)
{
	Oid			namespaceid;
	AclResult	aclresult;
	Oid			typid;
	int32		typmod;
	Oid			varowner = GetUserId();

	Node	   *cooked_default = NULL;

	namespaceid =
		RangeVarGetAndCheckCreationNamespace(stmt->variable, NoLock, NULL);

	typenameTypeIdAndMod(pstate, stmt->typeName, &typid, &typmod);

	aclresult = pg_type_aclcheck(typid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typid);

	if (stmt->defexpr)
	{
		cooked_default = transformExpr(pstate, stmt->defexpr,
										EXPR_KIND_VARIABLE_DEFAULT);

		cooked_default = coerce_to_specific_type(pstate,
												 cooked_default, typid, "DEFAULT");
	}

	return VariableCreate(stmt->variable->relname,
						  namespaceid,
						  typid,
						  typmod,
						  varowner,
						  cooked_default,
						  stmt->if_not_exists);
}

/*
 * Try to search value in hash table. If doesn't
 * exists insert it (and calculate defexpr if exists.
 */
static SchemaVariable
PrepareSchemaVariableForReading(Oid varid)
{
	SchemaVariable	svar;
	Variable	*var;
	bool			found;

	if (schemavarhashtab == NULL)
		create_schemavar_hashtable();

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
											  HASH_ENTER, &found);
	if (!found)
	{
		var = GetVariable(varid, false);
		get_typlenbyval(var->typid, &svar->typlen, &svar->typbyval);

		svar->varid = varid;
		svar->typid = var->typid;
		svar->typmod = var->typmod;
		svar->isnull = true;
		svar->freeval = false;
		svar->value = (Datum) 0;
		svar->is_rowtype = type_is_rowtype(var->typid);

		/* when we don't need calculate defexpr, value is valid already */
		svar->is_valid = var->defexpr ? false : true;
	}
	else if (!svar->is_valid)
	{
		/* we need var to recalculate defexpr */
		var = GetVariable(varid, false);
	}
	else
		/* we don't need to go to sys cache */
		var = NULL;

	/*
	 * Initialize variable when it is necessary. It is fresh
	 * or last initialization was not successfull.
	 */
	if (var != NULL && var->defexpr && !svar->is_valid)
	{
		MemoryContext oldcontext = NULL;

		Datum	   value = (Datum) 0;
		bool	   null;
		EState	   *estate = NULL;
		Expr	   *defexpr;
		ExprState  *defexprs;

		/* Prepare default expr */
		estate = CreateExecutorState();
		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		defexpr = expression_planner((Expr *) var->defexpr);
		defexprs = ExecInitExpr(defexpr, NULL);
		value = ExecEvalExprSwitchContext(defexprs, GetPerTupleExprContext(estate), &null);

		MemoryContextSwitchTo(SchemaVariableMemoryContext);

		if (!null)
		{
			svar->value = datumCopy(value, svar->typbyval, svar->typlen);
			svar->freeval = svar->value != value;
			svar->isnull = false;
			svar->is_valid = true;
		}
		else
		{
			svar->isnull = true;
			svar->is_valid = true;
		}

		MemoryContextSwitchTo(oldcontext);

		FreeExecutorState(estate);
	}

	if (!svar->is_valid)
		elog(ERROR, "the content of variable is not valid");

	return svar;
}

/*
 * Returns content of variable. We expext secured access now.
 * Secure check should be done before.
 */
Datum
GetSchemaVariable(Oid varid, bool *isNull, Oid expected_typid)
{
	SchemaVariable	svar;

	svar = PrepareSchemaVariableForReading(varid);
	*isNull = svar->isnull;

	if (expected_typid != svar->typid)
		elog(ERROR, "type of variable \"%s\" is different than expected",
				   schema_variable_get_name(varid));

	return (Datum) svar->value;
}

/*
 * Write value to variable. We expect secured access in this moment.
 * In this time, we recheck syschache about used type.
 */
void
SetSchemaVariable(Oid varid, Datum value, bool isNull, Oid typid, int32 typmod)
{
	MemoryContext oldcontext = NULL;

	SchemaVariable svar;
	Oid			var_typid;
	int32		var_typmod;
	bool		found;

	if (schemavarhashtab == NULL)
		create_schemavar_hashtable();

	svar = (SchemaVariable) hash_search(schemavarhashtab, &varid,
											  HASH_ENTER, &found);

	get_schema_variable_type_typmod(varid, &var_typid, &var_typmod);

	/* check types first */
	if (var_typid != typid)
		elog(ERROR, "type of expression is different than schema variable type");

	if (found)
	{
		/* release current content first */
		if (svar->freeval)
		{
			pfree(DatumGetPointer(svar->value));
			svar->value = (Datum) 0;
			svar->isnull = true;
			svar->freeval = false;
		}
	}

	get_typlenbyval(typid, &svar->typlen, &svar->typbyval);

	svar->varid = varid;
	svar->typid = typid;
	svar->typmod = typmod;

	svar->isnull = true;
	svar->freeval = false;
	svar->value = (Datum) 0;

	svar->is_rowtype = type_is_rowtype(typid);
	svar->is_valid = false;

	oldcontext = MemoryContextSwitchTo(SchemaVariableMemoryContext);

	if (!isNull)
	{
		svar->value = datumCopy(value, svar->typbyval, svar->typlen);
		svar->freeval = svar->value != value;
		svar->isnull = false;
		svar->is_valid = true;
	}
	else
	{
		svar->isnull = true;
		svar->is_valid = true;
	}

	MemoryContextSwitchTo(oldcontext);
}

void
doLetStmt(PlannedStmt *pstmt,
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

