/*-------------------------------------------------------------------------
 *
 * parse_jsontable.c
 *	  parsing of JSON_TABLE
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_jsontable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"

static JsonTablePlan *transformJsonTableColumns(ParseState *pstate,
												JsonTable *jt, TableFunc *tf,
												JsonTablePathSpec *rootPathSpec);
static Node *transformJsonTableColumn(JsonTableColumn *jtc,
									  Node *contextItemExpr,
									  List *passingArgs,
									  bool errorOnError);
static bool typeIsComposite(Oid typid);
static JsonTablePlan *makeJsonTablePlan(JsonTablePathSpec *pathspec,
										JsonBehavior *on_error);
static void CheckDuplicateJsonTableColumnNames(ParseState *pstate,
											   List *columns);


/*
 * transformJsonTable -
 *			Transform a raw JsonTable into TableFunc
 *
 * Transform the document-generating expression (jt->context_item), the
 * row-generating expression (jt->pathspec), and the column-generating
 * expressions (jt->columns).
 */
ParseNamespaceItem *
transformJsonTable(ParseState *pstate, JsonTable *jt)
{
	TableFunc  *tf;
	JsonFuncExpr *jfe;
	JsonExpr   *je;
	JsonTablePathSpec *rootPathSpec = jt->pathspec;
	bool		is_lateral;

	Assert(IsA(rootPathSpec->string, A_Const) &&
		   castNode(A_Const, rootPathSpec->string)->val.node.type == T_String);

	if (rootPathSpec->name == NULL)
		rootPathSpec->name = pstrdup("json_table_path_0");
	CheckDuplicateJsonTableColumnNames(pstate, jt->columns);

	/*
	 * We make lateral_only names of this level visible, whether or not the
	 * RangeTableFunc is explicitly marked LATERAL.  This is needed for SQL
	 * spec compliance and seems useful on convenience grounds for all
	 * functions in FROM.
	 *
	 * (LATERAL can't nest within a single pstate level, so we don't need
	 * save/restore logic here.)
	 */
	Assert(!pstate->p_lateral_active);
	pstate->p_lateral_active = true;

	tf = makeNode(TableFunc);
	tf->functype = TFT_JSON_TABLE;

	/*
	 * Transform JsonFuncExpr representing the top JSON_TABLE context_item and
	 * pathspec into a JSON_TABLE_OP JsonExpr.
	 */
	jfe = makeNode(JsonFuncExpr);
	jfe->op = JSON_TABLE_OP;
	jfe->context_item = jt->context_item;
	jfe->pathspec = (Node *) rootPathSpec->string;
	jfe->pathname = rootPathSpec->name;
	jfe->passing = jt->passing;
	jfe->on_empty = NULL;
	jfe->on_error = jt->on_error;
	jfe->location = jt->location;
	tf->docexpr = transformExpr(pstate, (Node *) jfe, EXPR_KIND_FROM_FUNCTION);

	/*
	 * Create a JsonTablePlan that will generate row pattern for jt->columns
	 * and add the columns' transformed JsonExpr nodes into tf->colvalexprs.
	 */
	tf->plan = (Node *) transformJsonTableColumns(pstate, jt, tf,
												  rootPathSpec);

	/*
	 * Also save a copy of the PASSING arguments in the TableFunc node. This
	 * is to allow initializng them once in ExecInitTableFuncScan().
	 */
	je = (JsonExpr *) tf->docexpr;
	tf->passingvalexprs = copyObject(je->passing_values);

	tf->ordinalitycol = -1;		/* undefine ordinality column number */
	tf->location = jt->location;

	pstate->p_lateral_active = false;

	/*
	 * Mark the RTE as LATERAL if the user said LATERAL explicitly, or if
	 * there are any lateral cross-references in it.
	 */
	is_lateral = jt->lateral || contain_vars_of_level((Node *) tf, 0);

	return addRangeTableEntryForTableFunc(pstate,
										  tf, jt->alias, is_lateral, true);
}

/* Check if a column name is duplicated in the given list */
static void
CheckDuplicateJsonTableColumnNames(ParseState *pstate,
								   List *columns)
{
	List	   *pathNames = NIL;
	ListCell   *lc1;

	foreach(lc1, columns)
	{
		JsonTableColumn *jtc = castNode(JsonTableColumn, lfirst(lc1));
		ListCell   *lc2;

		foreach(lc2, pathNames)
		{
			if (strcmp(jtc->name, (const char *) lfirst(lc2)) == 0)
				ereport(ERROR,
						errcode(ERRCODE_DUPLICATE_ALIAS),
						errmsg("duplicate JSON_TABLE column name: %s",
							   jtc->name),
						parser_errposition(pstate, jtc->location));
		}

		pathNames = lappend(pathNames, jtc->name);
	}
}

/*
 * Create a JsonTablePlan that will supply the source row for jt->columns
 * using 'pathspec' and append the columns' transformed JsonExpr nodes to
 * TableFunc.colvalexprs.
 */
static JsonTablePlan *
transformJsonTableColumns(ParseState *pstate, JsonTable *jt,
						  TableFunc *tf, JsonTablePathSpec *pathspec)
{
	List	   *columns = jt->columns;
	ListCell   *col;
	bool		ordinality_found = false;
	JsonBehavior *on_error = jt->on_error;
	bool		errorOnError = on_error &&
		on_error->btype == JSON_BEHAVIOR_ERROR;
	Oid			contextItemTypid = exprType(tf->docexpr);

	foreach(col, columns)
	{
		JsonTableColumn *rawc = castNode(JsonTableColumn, lfirst(col));
		Oid			typid;
		int32		typmod;
		Node	   *colexpr;

		if (rawc->name)
			tf->colnames = lappend(tf->colnames,
								   makeString(pstrdup(rawc->name)));

		/*
		 * Determine the type and typmod for the new column. FOR ORDINALITY
		 * columns are INTEGER by standard; the others are user-specified.
		 */
		switch (rawc->coltype)
		{
			case JTC_FOR_ORDINALITY:
				if (ordinality_found)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("cannot use more than one FOR ORDINALITY column"),
							 parser_errposition(pstate, rawc->location)));
				ordinality_found = true;
				colexpr = NULL;
				typid = INT4OID;
				typmod = -1;
				break;

			case JTC_REGULAR:
				typenameTypeIdAndMod(pstate, rawc->typeName, &typid, &typmod);

				/*
				 * Use implicit FORMAT JSON for composite types (arrays and
				 * records) or if a non-default WRAPPER / QUOTES behavior is
				 * specified.
				 */
				if (typeIsComposite(typid) ||
					rawc->quotes != JS_QUOTES_UNSPEC ||
					rawc->wrapper != JSW_UNSPEC)
					rawc->coltype = JTC_FORMATTED;

				/* FALLTHROUGH */
			case JTC_FORMATTED:
			case JTC_EXISTS:
				{
					Node	   *je;
					CaseTestExpr *param = makeNode(CaseTestExpr);

					param->collation = InvalidOid;
					param->typeId = contextItemTypid;
					param->typeMod = -1;

					je = transformJsonTableColumn(rawc, (Node *) param,
												  NIL, errorOnError);

					colexpr = transformExpr(pstate, je, EXPR_KIND_FROM_FUNCTION);
					assign_expr_collations(pstate, colexpr);

					typid = exprType(colexpr);
					typmod = exprTypmod(colexpr);
					break;
				}

			default:
				elog(ERROR, "unknown JSON_TABLE column type: %d", rawc->coltype);
				break;
		}

		tf->coltypes = lappend_oid(tf->coltypes, typid);
		tf->coltypmods = lappend_int(tf->coltypmods, typmod);
		tf->colcollations = lappend_oid(tf->colcollations, get_typcollation(typid));
		tf->colvalexprs = lappend(tf->colvalexprs, colexpr);
	}

	return makeJsonTablePlan(pathspec, jt->on_error);
}

/*
 * Transform JSON_TABLE column
 *   - regular column into JSON_VALUE()
 *   - FORMAT JSON column into JSON_QUERY()
 *   - EXISTS column into JSON_EXISTS()
 */
static Node *
transformJsonTableColumn(JsonTableColumn *jtc, Node *contextItemExpr,
						 List *passingArgs, bool errorOnError)
{
	Node	   *pathspec;
	JsonFuncExpr *jfexpr = makeNode(JsonFuncExpr);

	/*
	 * XXX consider inventing JSON_TABLE_VALUE_OP, etc. and pass the column
	 * name via JsonExpr so that JsonPathValue(), etc. can provide error
	 * message tailored to JSON_TABLE(), such as by mentioning the column
	 * names in the message.
	 */
	if (jtc->coltype == JTC_REGULAR)
		jfexpr->op = JSON_VALUE_OP;
	else if (jtc->coltype == JTC_EXISTS)
		jfexpr->op = JSON_EXISTS_OP;
	else
		jfexpr->op = JSON_QUERY_OP;

	jfexpr->context_item = makeJsonValueExpr((Expr *) contextItemExpr, NULL,
											 makeJsonFormat(JS_FORMAT_DEFAULT,
															JS_ENC_DEFAULT,
															-1));
	if (jtc->pathspec)
		pathspec = (Node *) jtc->pathspec->string;
	else
	{
		/* Construct default path as '$."column_name"' */
		StringInfoData path;

		initStringInfo(&path);

		appendStringInfoString(&path, "$.");
		escape_json(&path, jtc->name);

		pathspec = makeStringConst(path.data, -1);
	}
	jfexpr->pathspec = pathspec;
	jfexpr->pathname = NULL;
	jfexpr->passing = passingArgs;
	jfexpr->output = makeNode(JsonOutput);
	jfexpr->output->typeName = jtc->typeName;
	jfexpr->output->returning = makeNode(JsonReturning);
	jfexpr->output->returning->format = jtc->format;
	jfexpr->on_empty = jtc->on_empty;
	jfexpr->on_error = jtc->on_error;
	if (jfexpr->on_error == NULL && errorOnError)
		jfexpr->on_error = makeJsonBehavior(JSON_BEHAVIOR_ERROR, NULL, -1);
	jfexpr->quotes = jtc->quotes;
	jfexpr->wrapper = jtc->wrapper;
	jfexpr->location = jtc->location;

	return (Node *) jfexpr;
}

/*
 * Create a JsonTablePlan for given path and ON ERROR behavior.
 */
static JsonTablePlan *
makeJsonTablePlan(JsonTablePathSpec *pathspec, JsonBehavior *on_error)
{
	JsonTablePlan *plan = makeNode(JsonTablePlan);
	char	   *pathstring;
	Const	   *value;

	Assert(IsA(pathspec->string, A_Const));
	pathstring = castNode(A_Const, pathspec->string)->val.sval.sval;
	value = makeConst(JSONPATHOID, -1, InvalidOid, -1,
					  DirectFunctionCall1(jsonpath_in,
										  CStringGetDatum(pathstring)),
					  false, false);
	plan->path = makeJsonTablePath(value, pathspec->name);
	plan->errorOnError = on_error && on_error->btype == JSON_BEHAVIOR_ERROR;

	return plan;
}

/* Check whether type is json/jsonb, array, record, or domain. */
static bool
typeIsComposite(Oid typid)
{
	char		typtype;

	if (typid == JSONOID ||
		typid == JSONBOID ||
		typid == RECORDOID ||
		type_is_array(typid))
		return true;

	typtype = get_typtype(typid);

	if (typtype == TYPTYPE_COMPOSITE)
		return true;

	if (typtype == TYPTYPE_DOMAIN)
		return typeIsComposite(getBaseType(typid));

	return false;
}
