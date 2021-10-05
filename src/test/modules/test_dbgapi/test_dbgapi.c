/*-------------------------------------------------------------------------
 *
 * test_dbgapi.c
 *	  Simple PLpgSQL tracer designed to test PLpgSQL's debug API
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/test/modules/test_dbgapi/test_dbgapi.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "plpgsql.h"
#include "utils/guc.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/* Module load/unload functions */
void		_PG_init(void);
void		_PG_fini(void);

static void func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);
static void stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt);

static bool is_nested_of(PLpgSQL_stmt *stmt, PLpgSQL_stmt *outer_stmt);
static PLpgSQL_nsitem *outer_ns(PLpgSQL_stmt *stmt);
static char *get_string_value(PLpgSQL_execstate *estate, PLpgSQL_datum *datum);
static char *get_string_expr(PLpgSQL_execstate *estate, PLpgSQL_nsitem *ns, char *query);

static PLpgSQL_plugin plugin_funcs = {NULL,
	func_beg,
	func_end,
	stmt_beg,
	stmt_end,
	NULL,
NULL};

typedef const char *(*test_dbgapi_stmt_typename_t) (PLpgSQL_stmt *stmt);
typedef PLpgSQL_nsitem *(*test_dbgapi_ns_lookup_t) (PLpgSQL_nsitem *ns_cur,
													bool localmode, const char *name1, const char *name2,
													const char *name3, int *names_used);

static test_dbgapi_stmt_typename_t stmt_typename_p;
static test_dbgapi_ns_lookup_t ns_lookup_p;

static PLpgSQL_plugin **test_dbgapi_plugin_var = NULL;

static bool trace_forloop_variable = false;
static char *trace_variable = NULL;

#define LOAD_EXTERNAL_FUNCTION(file, funcname) \
	((void *) (load_external_function(file, funcname, true, NULL)))

#define get_eval_mcontext(estate) \
	((estate)->eval_econtext->ecxt_per_tuple_memory)

PG_FUNCTION_INFO_V1(trace_plpgsql);

typedef struct
{
	List	   *fori_stmt_stack;
}			test_dbgapi_info;


static void
func_beg(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	MemoryContext oldcxt;

	if (func->fn_signature)
		ereport(NOTICE,
				(errmsg("start of function: \"%s\"", func->fn_signature)));
	else
		ereport(NOTICE,
				(errmsg("start of anonymous block")));

	oldcxt = MemoryContextSwitchTo(estate->datum_context);

	estate->plugin_info = palloc0(sizeof(test_dbgapi_info));

	MemoryContextSwitchTo(oldcxt);
}

static void
func_end(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	if (func->fn_signature)
		ereport(NOTICE,
				(errmsg("end of function: \"%s\"", func->fn_signature)));
	else
		ereport(NOTICE,
				(errmsg("end of anonymous block")));
}

static void
stmt_beg(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	test_dbgapi_info *pinfo = estate->plugin_info;

	ereport(NOTICE,
			(errmsg("start of statement: \"%s\", no: %d on line %d",
					(char *) stmt_typename_p(stmt),
					stmt->stmtid, stmt->lineno)));

	/*
	 * We have to recheck accuracy of saved statement stack, because it can be
	 * broken by any handled exception. In this case, the *end handlers are
	 * not executed (so we cannot to maintain this stack by appending in
	 * stmt_beg and trimming in stmt_end).
	 *
	 * This check can be simple - the namespace of current statement should be
	 * subset of namespace of last fori statement from stack.
	 */
	if (pinfo && (pinfo->fori_stmt_stack != NIL))
	{
		int			i;
		bool		found = false;

		/*
		 * Try to search top most accurate fori statement. In usual cases the
		 * most top fori statement is accurate, and this cycle is quickly
		 * leaved. This is NxM algorithm, but a) this is just test
		 * application, b) in reality the N and M are low, so it is not
		 * necessary to implement faster but more complex algorithm.
		 */
		for (i = list_length(pinfo->fori_stmt_stack); i > 0; i--)
		{
			if (is_nested_of(stmt, (PLpgSQL_stmt *)
							 list_nth(pinfo->fori_stmt_stack, i - 1)))
			{
				if (i < list_length(pinfo->fori_stmt_stack))
				{
					/* We need to trim stack of fori statements */
					pinfo->fori_stmt_stack = list_truncate(pinfo->fori_stmt_stack, i);

					ereport(NOTICE,
							(errmsg("the stack of fori statements is truncated to %d",
									i)));
				}

				found = true;
				break;
			}
		}

		if (!found)
		{
			list_free(pinfo->fori_stmt_stack);
			pinfo->fori_stmt_stack = NIL;

			ereport(NOTICE, (errmsg("now, there is not any outer fori cycle")));
		}
	}

	if (pinfo &&
		pinfo->fori_stmt_stack &&
		(list_length(pinfo->fori_stmt_stack) > 0))
	{
		PLpgSQL_stmt_fori *top_fori_stmt;
		PLpgSQL_var *var;

		top_fori_stmt = (PLpgSQL_stmt_fori *) llast(pinfo->fori_stmt_stack);
		var = top_fori_stmt->var;

		ereport(NOTICE,
				(errmsg("most inner fori loop control variable \"%s\" is %s",
						var->refname,
						get_string_value(estate, estate->datums[var->dno]))));
	}

	if (trace_forloop_variable &&
		(stmt->cmd_type == PLPGSQL_STMT_FORI))
		pinfo->fori_stmt_stack = lappend(pinfo->fori_stmt_stack, stmt);

	/*
	 * Another case - print content of variable specified by name. The
	 * variable can be shadowed, and we want to chose variable that is visible
	 * from current namespace.
	 *
	 * Note: there are two ways to do it. Since this code is used as a
	 * regression test, both are shown.
	 */
	if (trace_variable && (*trace_variable != '\0'))
	{
		PLpgSQL_nsitem *nsi;

		nsi = ns_lookup_p(outer_ns(stmt), false, "v", NULL, NULL, NULL);
		if (nsi &&
			((nsi->itemtype == PLPGSQL_NSTYPE_VAR) ||
			 (nsi->itemtype == PLPGSQL_NSTYPE_REC)))
		{
			ereport(NOTICE,
					(errmsg("value of variable \"%s\" is \"%s\" (before)",
							nsi->name,
							get_string_value(estate,
											 estate->datums[nsi->itemno]))));
		}
	}
}

static void
stmt_end(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
	ereport(NOTICE,
			(errmsg("end of statement: \"%s\", no: %d on line %d",
					(char *) stmt_typename_p(stmt),
					stmt->stmtid, stmt->lineno)));

	if (trace_variable && (*trace_variable != '\0'))
	{
		char	   *str = get_string_expr(estate, stmt->ns, trace_variable);

		if (str)
			ereport(NOTICE,
					(errmsg("value of variable \"%s\" is \"%s\" (after)",
							trace_variable, str)));
	}
}

/*
 * Module initialization
 */
void
_PG_init(void)
{
	static bool inited = false;

	if (inited)
		return;

	/*
	 * Currently these asserts don't ensure any safety, but it can be changed
	 * if referenced functions will be declared in a header file.
	 */
	AssertVariableIsOfType(stmt_typename_p, test_dbgapi_stmt_typename_t);
	stmt_typename_p = (test_dbgapi_stmt_typename_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_stmt_typename");

	AssertVariableIsOfType(ns_lookup_p, test_dbgapi_ns_lookup_t);
	ns_lookup_p = (test_dbgapi_ns_lookup_t)
		LOAD_EXTERNAL_FUNCTION("$libdir/plpgsql", "plpgsql_ns_lookup");

	DefineCustomBoolVariable("test_dbgapi.trace_forloop_variable",
							 "when it is true, then control variable of for loop cycles will be printed",
							 NULL,
							 &trace_forloop_variable,
							 false,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("test_dbgapi.trace_variable",
							   "the content of specified variable is printed before and after any executed statement",
							   NULL,
							   &trace_variable,
							   "",
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	EmitWarningsOnPlaceholders("test_dbgapi");

	inited = true;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	*test_dbgapi_plugin_var = NULL;
}

Datum
trace_plpgsql(PG_FUNCTION_ARGS)
{
	bool		enable_tracer = PG_GETARG_BOOL(0);

	if (enable_tracer)
	{
		test_dbgapi_plugin_var = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
		*test_dbgapi_plugin_var = &plugin_funcs;
	}
	else
		*test_dbgapi_plugin_var = NULL;

	PG_RETURN_VOID();
}

/*
 * Returns true, if statement stmt is any inner statement of
 * outer (surely some block statement e.g. loop, case, block).
 */
static bool
is_nested_of(PLpgSQL_stmt *stmt, PLpgSQL_stmt *outer_stmt)
{
	PLpgSQL_nsitem *ns = stmt->ns;
	PLpgSQL_nsitem *outer_stmt_ns = outer_stmt->ns;

	/*
	 * There are two possibilities how to assign namespace to some block
	 * statement. First (that was not implemented) is using real outer
	 * namespace. The advantage of this method is consistency for all plpgsql
	 * statements. Unfortunately, in this case we lose one level of
	 * namespaces. Instead we assign a namespace created for block statement
	 * (this is namespace for all body's statements). There is inconsistency
	 * in semantic of assigned namespaces between block and non block
	 * statements, but we can easy detect if some statement is nested inside
	 * another statement, and we can detect implicit variables.
	 */
	while (ns)
	{
		if (ns == outer_stmt_ns)
			return true;

		ns = ns->prev;
	}

	return false;
}

/*
 * Returns outer statement's namespace. For block's statements
 * we have to go one level up, to skip statement's namespace.
 */
static PLpgSQL_nsitem *
outer_ns(PLpgSQL_stmt *stmt)
{
	switch (stmt->cmd_type)
	{
		case PLPGSQL_STMT_BLOCK:
		case PLPGSQL_STMT_LOOP:
		case PLPGSQL_STMT_WHILE:
		case PLPGSQL_STMT_FORI:
		case PLPGSQL_STMT_FORS:
		case PLPGSQL_STMT_FORC:
		case PLPGSQL_STMT_FOREACH_A:
		case PLPGSQL_STMT_DYNFORS:
			return stmt->ns->prev;

		default:
			return stmt->ns;
	}
}

/*
 * Don't free the returned string, since it can be constant! Returned string,
 * when it is not a const string, will be released when eval memory
 * context will be reset.
 */
static char *
get_string_value(PLpgSQL_execstate *estate, PLpgSQL_datum *datum)
{
	Oid			typeid;
	int32		typmod;
	Datum		value,
				text_value;
	bool		isnull;
	char	   *result;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(get_eval_mcontext(estate));

	/* Example of usage debug API eval_datum and cast_value functions */
	(*test_dbgapi_plugin_var)->eval_datum(estate, datum,
										  &typeid, &typmod, &value, &isnull);

	/* cast value to text */
	if (!isnull)
	{
		/*
		 * Generic cast is more usable than just cast to string. There can be
		 * casts from any to boolean or from numeric to double.
		 */
		text_value = (*test_dbgapi_plugin_var)->cast_value(estate,
														   value, &isnull,
														   typeid, typmod,
														   TEXTOID, -1);

		result = TextDatumGetCString(text_value);
	}
	else
		result = "NULL";

	MemoryContextSwitchTo(oldcxt);

	return result;
}

/*
 * The value of variable can be taken by evaluation of
 * an expression. This is older technique based on assign_expr
 * method.
 */
static char *
get_string_expr(PLpgSQL_execstate *estate, PLpgSQL_nsitem *ns, char *query)
{
	PLpgSQL_var result;
	PLpgSQL_type typ;
	PLpgSQL_expr expr;
	bool		iserror = false;
	char	   *result_str = NULL;
	MemoryContext oldcxt;
	ResourceOwner oldowner;

	memset(&result, 0, sizeof(result));
	memset(&typ, 0, sizeof(typ));
	memset(&expr, 0, sizeof(expr));

	result.dtype = PLPGSQL_DTYPE_VAR;
	result.dno = -1;
	result.refname = "*auxstorage*";
	result.datatype = &typ;

	typ.typoid = TEXTOID;
	typ.ttype = PLPGSQL_TTYPE_SCALAR;
	typ.typlen = -1;
	typ.typbyval = false;
	typ.typtype = 'b';

	expr.query = query;
	expr.ns = ns;
	expr.parseMode = RAW_PARSE_PLPGSQL_EXPR;

	oldowner = CurrentResourceOwner;
	oldcxt = MemoryContextSwitchTo(get_eval_mcontext(estate));

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(get_eval_mcontext(estate));

	PG_TRY();
	{
		/* It can fail when the variable doesn't exist */
		(*test_dbgapi_plugin_var)->assign_expr(estate,
											   (PLpgSQL_datum *) &result,
											   &expr);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(get_eval_mcontext(estate));
		CurrentResourceOwner = oldowner;

		SPI_restore_connection();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(get_eval_mcontext(estate));
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(get_eval_mcontext(estate));
		CurrentResourceOwner = oldowner;

		iserror = true;

		SPI_restore_connection();
	}
	PG_END_TRY();

	if (!iserror)
	{
		if (!result.isnull)
			result_str = text_to_cstring(DatumGetTextP(result.value));
		else
			result_str = "NULL";
	}

	MemoryContextSwitchTo(oldcxt);

	return result_str;
}
