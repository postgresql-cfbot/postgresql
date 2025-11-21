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
#include "miscadmin.h"
#include "parser/parse_type.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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
} SVariableData;

typedef SVariableData *SVariable;

static HTAB *sessionvars = NULL;	/* hash table for session variables */

static MemoryContext SVariableMemoryContext = NULL;

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
search_variable(char *varname)
{
	SVariable	svar;

	if (!sessionvars)
		create_sessionvars_hashtables();

	svar = (SVariable) hash_search(sessionvars, varname,
								   HASH_FIND, NULL);

	if (!svar)
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

	svar = search_variable(varname);

	/* only owner can set content of variable */
	*typid = svar->vartype;
	*typmod = svar->vartypmod;
	*collid = svar->varcollation;
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
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("session variable \"%s\" already exists",
						stmt->name)));

	namestrcpy(&svar->varname, stmt->name);
	svar->vartype = typeid;
	svar->vartypmod = typmod;
	svar->varcollation = typcollation;
	svar->varowner = varowner;
	svar->typlen = typlen;
	svar->typbyval = typbyval;

	svar->value = (Datum) 0;
	svar->isnull = true;
}

/*
 * Drop variable by name
 */
void
DropVariableByName(char *varname)
{
	SVariable	svar;

	/*
	 * Current implementation is not catalog based, but we expect catalog
	 * based implementation for future, so we force same limits.
	 */
	PreventCommandIfReadOnly("DROP VARIABLE");
	PreventCommandIfParallelMode("DROP VARIABLE");
	PreventCommandDuringRecovery("DROP VARIABLE");

	svar = search_variable(varname);

	/* only owner can get content of variable */
	if (svar->varowner != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of session variable %s",
						varname)));

	if (!svar->typbyval && !svar->isnull)
		pfree(DatumGetPointer(svar->value));

	if (hash_search(sessionvars,
					   varname,
					   HASH_REMOVE,
					   NULL) == NULL)
		elog(ERROR, "hash table corrupted");
}
