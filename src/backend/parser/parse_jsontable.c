/*-------------------------------------------------------------------------
 *
 * parse_jsontable.c
 *	  parsing of JSON_TABLE
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

/* Context for JSON_TABLE transformation */
typedef struct JsonTableParseContext
{
	ParseState *pstate;			/* parsing state */
	JsonTable  *table;			/* untransformed node */
	TableFunc  *tablefunc;		/* transformed node	*/
	List	   *pathNames;		/* list of all path and columns names */
	int			pathNameId;		/* path name id counter */
	Oid			contextItemTypid;	/* type oid of context item (json/jsonb) */
}			JsonTableParseContext;

static JsonTablePlan *transformJsonTableColumns(JsonTableParseContext * cxt,
												JsonTablePlanSpec *planspec,
												List *columns,
												JsonTablePathSpec *pathspec);

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
	JsonFuncExpr *jfexpr = makeNode(JsonFuncExpr);
	Node	   *pathspec;
	JsonFormat *default_format;

	if (jtc->coltype == JTC_REGULAR)
		jfexpr->op = JSON_VALUE_OP;
	else if (jtc->coltype == JTC_EXISTS)
		jfexpr->op = JSON_EXISTS_OP;
	else
		jfexpr->op = JSON_QUERY_OP;
	jfexpr->output = makeNode(JsonOutput);
	jfexpr->on_empty = jtc->on_empty;
	jfexpr->on_error = jtc->on_error;
	if (jfexpr->on_error == NULL && errorOnError)
		jfexpr->on_error = makeJsonBehavior(JSON_BEHAVIOR_ERROR, NULL,
											NULL, -1);
	jfexpr->quotes = jtc->quotes;
	jfexpr->wrapper = jtc->wrapper;
	jfexpr->location = jtc->location;

	jfexpr->output->typeName = jtc->typeName;
	jfexpr->output->returning = makeNode(JsonReturning);
	jfexpr->output->returning->format = jtc->format;

	default_format = makeJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1);

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

	jfexpr->context_item =	makeJsonValueExpr((Expr *) contextItemExpr, NULL,
											  default_format);
	jfexpr->pathspec = pathspec;
	jfexpr->pathname = NULL;
	jfexpr->passing = passingArgs;

	return (Node *) jfexpr;
}

/*
 * Register a column/path name in the path name list, flagging if the name is
 * already taken by another column/path.
 */
static void
registerJsonTableColumn(JsonTableParseContext * cxt, char *colname,
						int location)
{
	ListCell   *lc;

	foreach(lc, cxt->pathNames)
	{
		if (strcmp(colname, (const char *) lfirst(lc)) == 0)
			ereport(ERROR,
					errcode(ERRCODE_DUPLICATE_ALIAS),
					errmsg("duplicate JSON_TABLE column name: %s", colname),
					parser_errposition(cxt->pstate, location));
	}

	cxt->pathNames = lappend(cxt->pathNames, colname);
}

static void
registerJsonTablePath(JsonTableParseContext * cxt, char *pathname,
					  int location)
{
	ListCell   *lc;

	foreach(lc, cxt->pathNames)
	{
		if (strcmp(pathname, (const char *) lfirst(lc)) == 0)
			ereport(ERROR,
					errcode(ERRCODE_DUPLICATE_ALIAS),
					errmsg("duplicate JSON_TABLE path name: %s", pathname),
					parser_errposition(cxt->pstate, location));
	}

	cxt->pathNames = lappend(cxt->pathNames, pathname);
}

/*
 * Recursively register all nested column names in the shared columns/path name
 * list.
 */
static void
registerAllJsonTableColumnsAndPaths(JsonTableParseContext * cxt,
									List *columns)
{
	ListCell   *lc;

	foreach(lc, columns)
	{
		JsonTableColumn *jtc = castNode(JsonTableColumn, lfirst(lc));

		if (jtc->coltype == JTC_NESTED)
		{
			if (jtc->pathspec->name)
				registerJsonTablePath(cxt, jtc->pathspec->name,
									  jtc->pathspec->name_location);

			registerAllJsonTableColumnsAndPaths(cxt, jtc->columns);
		}
		else
		{
			registerJsonTableColumn(cxt, jtc->name, jtc->location);
		}
	}
}

/* Generate a new unique JSON_TABLE path name. */
static char *
generateJsonTablePathName(JsonTableParseContext * cxt)
{
	char		namebuf[32];
	char	   *name = namebuf;

	snprintf(namebuf, sizeof(namebuf), "json_table_path_%d",
			 cxt->pathNameId++);

	name = pstrdup(name);
	cxt->pathNames = lappend(cxt->pathNames, name);

	return name;
}

/* Collect sibling path names from plan to the specified list. */
static void
collectSiblingPathsInJsonTablePlan(JsonTablePlanSpec *plan, List **paths)
{
	if (plan->plan_type == JSTP_SIMPLE)
		*paths = lappend(*paths, plan->pathname);
	else if (plan->plan_type == JSTP_JOINED)
	{
		if (plan->join_type == JSTP_JOIN_INNER ||
			plan->join_type == JSTP_JOIN_OUTER)
		{
			Assert(plan->plan1->plan_type == JSTP_SIMPLE);
			*paths = lappend(*paths, plan->plan1->pathname);
		}
		else if (plan->join_type == JSTP_JOIN_CROSS ||
				 plan->join_type == JSTP_JOIN_UNION)
		{
			collectSiblingPathsInJsonTablePlan(plan->plan1, paths);
			collectSiblingPathsInJsonTablePlan(plan->plan2, paths);
		}
		else
			elog(ERROR, "invalid JSON_TABLE join type %d",
				 plan->join_type);
	}
}

/*
 * Validate child JSON_TABLE plan by checking that:
 *  - all nested columns have path names specified
 *  - all nested columns have corresponding node in the sibling plan
 *  - plan does not contain duplicate or extra nodes
 */
static void
validateJsonTableChildPlan(ParseState *pstate, JsonTablePlanSpec *plan,
						   List *columns)
{
	ListCell   *lc1;
	List	   *siblings = NIL;
	int			nchildren = 0;

	if (plan)
		collectSiblingPathsInJsonTablePlan(plan, &siblings);

	foreach(lc1, columns)
	{
		JsonTableColumn *jtc = castNode(JsonTableColumn, lfirst(lc1));

		if (jtc->coltype == JTC_NESTED)
		{
			ListCell   *lc2;
			bool		found = false;

			if (jtc->pathspec->name == NULL)
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("nested JSON_TABLE columns must contain"
								" an explicit AS pathname specification"
								" if an explicit PLAN clause is used"),
						parser_errposition(pstate, jtc->location));

			/* find nested path name in the list of sibling path names */
			foreach(lc2, siblings)
			{
				if ((found = !strcmp(jtc->pathspec->name, lfirst(lc2))))
					break;
			}

			if (!found)
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("invalid JSON_TABLE specification"),
						errdetail("PLAN clause for nested path %s was not found.",
								  jtc->pathspec->name),
						parser_errposition(pstate, jtc->location));

			nchildren++;
		}
	}

	if (list_length(siblings) > nchildren)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("invalid JSON_TABLE plan clause"),
				errdetail("PLAN clause contains some extra or duplicate sibling nodes."),
				parser_errposition(pstate, plan ? plan->location : -1));
}

static JsonTableColumn *
findNestedJsonTableColumn(List *columns, const char *pathname)
{
	ListCell   *lc;

	foreach(lc, columns)
	{
		JsonTableColumn *jtc = castNode(JsonTableColumn, lfirst(lc));

		if (jtc->coltype == JTC_NESTED &&
			jtc->pathspec->name &&
			!strcmp(jtc->pathspec->name, pathname))
			return jtc;
	}

	return NULL;
}

static Node *
transformNestedJsonTableColumn(JsonTableParseContext * cxt, JsonTableColumn *jtc,
							   JsonTablePlanSpec *planspec)
{
	if (jtc->pathspec->name == NULL)
	{
		if (cxt->table->planspec != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid JSON_TABLE expression"),
					 errdetail("JSON_TABLE path must contain"
							   " explicit AS pathname specification if"
							   " explicit PLAN clause is used"),
					 parser_errposition(cxt->pstate, jtc->location)));

		jtc->pathspec->name = generateJsonTablePathName(cxt);
	}

	return (Node *) transformJsonTableColumns(cxt, planspec, jtc->columns,
											  jtc->pathspec);
}

static Node *
makeJsonTableSiblingJoin(bool cross, Node *lnode, Node *rnode)
{
	JsonTableSibling *join = makeNode(JsonTableSibling);

	join->larg = lnode;
	join->rarg = rnode;
	join->cross = cross;

	return (Node *) join;
}

/*
 * Recursively transform child JSON_TABLE plan.
 *
 * Default plan is transformed into a cross/union join of its nested columns.
 * Simple and outer/inner plans are transformed into a JsonTablePlan by
 * finding and transforming corresponding nested column.
 * Sibling plans are recursively transformed into a JsonTableSibling.
 */
static Node *
transformJsonTableChildPlan(JsonTableParseContext * cxt,
							JsonTablePlanSpec *plan,
							List *columns)
{
	JsonTableColumn *jtc = NULL;

	if (!plan || plan->plan_type == JSTP_DEFAULT)
	{
		/* unspecified or default plan */
		Node	   *res = NULL;
		ListCell   *lc;
		bool		cross = plan && (plan->join_type & JSTP_JOIN_CROSS);

		/* transform all nested columns into cross/union join */
		foreach(lc, columns)
		{
			JsonTableColumn *col = castNode(JsonTableColumn, lfirst(lc));
			Node	   *node;

			if (col->coltype != JTC_NESTED)
				continue;

			node = transformNestedJsonTableColumn(cxt, col, plan);

			/* join transformed node with previous sibling nodes */
			res = res ? makeJsonTableSiblingJoin(cross, res, node) : node;
		}

		return res;
	}
	else if (plan->plan_type == JSTP_SIMPLE)
	{
		jtc = findNestedJsonTableColumn(columns, plan->pathname);
	}
	else if (plan->plan_type == JSTP_JOINED)
	{
		if (plan->join_type == JSTP_JOIN_INNER ||
			plan->join_type == JSTP_JOIN_OUTER)
		{
			Assert(plan->plan1->plan_type == JSTP_SIMPLE);
			jtc = findNestedJsonTableColumn(columns, plan->plan1->pathname);
		}
		else
		{
			Node	   *node1 = transformJsonTableChildPlan(cxt, plan->plan1,
															columns);
			Node	   *node2 = transformJsonTableChildPlan(cxt, plan->plan2,
															columns);

			return makeJsonTableSiblingJoin(plan->join_type == JSTP_JOIN_CROSS,
											node1, node2);
		}
	}
	else
		elog(ERROR, "invalid JSON_TABLE plan type %d", plan->plan_type);

	if (!jtc)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid JSON_TABLE plan clause"),
				 errdetail("PATH name was %s not found in nested columns list.",
						   plan->pathname),
				 parser_errposition(cxt->pstate, plan->location)));

	return transformNestedJsonTableColumn(cxt, jtc, plan);
}

/* Check whether type is json/jsonb, array, or record. */
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

/* Append transformed non-nested JSON_TABLE columns to the TableFunc node */
static void
appendJsonTableColumns(JsonTableParseContext * cxt, List *columns)
{
	ListCell   *col;
	ParseState *pstate = cxt->pstate;
	JsonTable  *jt = cxt->table;
	TableFunc  *tf = cxt->tablefunc;
	bool		ordinality_found = false;
	JsonBehavior *on_error = jt->on_error;
	bool		errorOnError = on_error &&
		on_error->btype == JSON_BEHAVIOR_ERROR;

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
				 * records) or if a non-default WRAPPER / QUOTES behavior
				 * is specified.
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
					param->typeId = cxt->contextItemTypid;
					param->typeMod = -1;

					je = transformJsonTableColumn(rawc, (Node *) param,
												  NIL, errorOnError);

					colexpr = transformExpr(pstate, je, EXPR_KIND_FROM_FUNCTION);
					assign_expr_collations(pstate, colexpr);

					typid = exprType(colexpr);
					typmod = exprTypmod(colexpr);
					break;
				}

			case JTC_NESTED:
				continue;

			default:
				elog(ERROR, "unknown JSON_TABLE column type: %d", rawc->coltype);
				break;
		}

		tf->coltypes = lappend_oid(tf->coltypes, typid);
		tf->coltypmods = lappend_int(tf->coltypmods, typmod);
		tf->colcollations = lappend_oid(tf->colcollations, get_typcollation(typid));
		tf->colvalexprs = lappend(tf->colvalexprs, colexpr);
	}
}

/*
 * Create transformed JSON_TABLE parent plan node by appending all non-nested
 * columns to the TableFunc node and remembering their indices in the
 * colvalexprs list.
 */
static JsonTablePlan *
makeParentJsonTablePlan(JsonTableParseContext * cxt, JsonTablePathSpec *pathspec,
						List *columns)
{
	JsonTablePlan *plan = makeNode(JsonTablePlan);
	JsonBehavior *on_error = cxt->table->on_error;
	char		 *pathstring;
	Const		 *value;

	Assert(IsA(pathspec->string, A_Const));
	pathstring = castNode(A_Const, pathspec->string)->val.sval.sval;
	value = makeConst(JSONPATHOID, -1, InvalidOid, -1,
					  DirectFunctionCall1(jsonpath_in,
										  CStringGetDatum(pathstring)),
					  false, false);
	plan->path = makeJsonTablePath(value, pathspec->name);

	/* save start of column range */
	plan->colMin = list_length(cxt->tablefunc->colvalexprs);

	appendJsonTableColumns(cxt, columns);

	/* save end of column range */
	plan->colMax = list_length(cxt->tablefunc->colvalexprs) - 1;

	plan->errorOnError = on_error && on_error->btype == JSON_BEHAVIOR_ERROR;

	return plan;
}

static JsonTablePlan *
transformJsonTableColumns(JsonTableParseContext * cxt,
						  JsonTablePlanSpec *planspec,
						  List *columns,
						  JsonTablePathSpec *pathspec)
{
	JsonTablePlan *plan;
	JsonTablePlanSpec *childPlanSpec;
	bool		defaultPlan = planspec == NULL ||
		planspec->plan_type == JSTP_DEFAULT;

	if (defaultPlan)
		childPlanSpec = planspec;
	else
	{
		/* validate parent and child plans */
		JsonTablePlanSpec *parentPlanSpec;

		if (planspec->plan_type == JSTP_JOINED)
		{
			if (planspec->join_type != JSTP_JOIN_INNER &&
				planspec->join_type != JSTP_JOIN_OUTER)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid JSON_TABLE plan clause"),
						 errdetail("Expected INNER or OUTER."),
						 parser_errposition(cxt->pstate, planspec->location)));

			parentPlanSpec = planspec->plan1;
			childPlanSpec = planspec->plan2;

			Assert(parentPlanSpec->plan_type != JSTP_JOINED);
			Assert(parentPlanSpec->pathname);
		}
		else
		{
			parentPlanSpec = planspec;
			childPlanSpec = NULL;
		}

		if (strcmp(parentPlanSpec->pathname, pathspec->name) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid JSON_TABLE plan"),
					 errdetail("PATH name mismatch: expected %s but %s is given.",
							   pathspec->name, parentPlanSpec->pathname),
					 parser_errposition(cxt->pstate, planspec->location)));

		validateJsonTableChildPlan(cxt->pstate, childPlanSpec, columns);
	}

	/* transform only non-nested columns */
	plan = makeParentJsonTablePlan(cxt, pathspec, columns);

	if (childPlanSpec || defaultPlan)
	{
		/* transform recursively nested columns */
		plan->child = transformJsonTableChildPlan(cxt, childPlanSpec, columns);
		if (plan->child)
			plan->outerJoin = planspec == NULL ||
				(planspec->join_type & JSTP_JOIN_OUTER);
		/* else: default plan case, no children found */
	}

	return plan;
}

/*
 * transformJsonTable -
 *			Transform a raw JsonTable into TableFunc.
 *
 * Transform the document-generating expression, the row-generating expression,
 * the column-generating expressions, and the default value expressions.
 */
ParseNamespaceItem *
transformJsonTable(ParseState *pstate, JsonTable *jt)
{
	JsonTableParseContext cxt;
	TableFunc  *tf = makeNode(TableFunc);
	JsonFuncExpr *jfe = makeNode(JsonFuncExpr);
	JsonExpr   *je;
	JsonTablePlanSpec *plan = jt->planspec;
	JsonTablePathSpec *rootPathSpec = jt->pathspec;
	bool		is_lateral;

	Assert(IsA(rootPathSpec->string, A_Const) &&
		   castNode(A_Const, rootPathSpec->string)->val.node.type == T_String);

	cxt.pstate = pstate;
	cxt.table = jt;
	cxt.tablefunc = tf;
	cxt.pathNames = NIL;
	cxt.pathNameId = 0;

	if (rootPathSpec->name)
		registerJsonTablePath(&cxt, rootPathSpec->name,
							  rootPathSpec->name_location);
	else
	{
		if (jt->planspec != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid JSON_TABLE expression"),
					 errdetail("JSON_TABLE path must contain"
							   " explicit AS pathname specification if"
							   " explicit PLAN clause is used"),
					 parser_errposition(pstate, rootPathSpec->location)));

		rootPathSpec->name = generateJsonTablePathName(&cxt);
	}

	registerAllJsonTableColumnsAndPaths(&cxt, jt->columns);

	jfe->op = JSON_TABLE_OP;
	jfe->context_item =	jt->context_item;
	jfe->pathspec = (Node *) rootPathSpec->string;
	jfe->pathname = rootPathSpec->name;
	jfe->passing = jt->passing;
	jfe->on_empty = NULL;
	jfe->on_error = jt->on_error;
	jfe->location = jt->location;

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

	tf->functype = TFT_JSON_TABLE;
	tf->docexpr = transformExpr(pstate, (Node *) jfe, EXPR_KIND_FROM_FUNCTION);

	cxt.contextItemTypid = exprType(tf->docexpr);

	tf->plan = (Node *) transformJsonTableColumns(&cxt, plan, jt->columns,
												  rootPathSpec);

	/* Also save a copy of the PASSING arguments in the TableFunc node. */
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
