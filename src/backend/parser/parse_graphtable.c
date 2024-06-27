/*-------------------------------------------------------------------------
 *
 * parse_graphtable.c
 *	  parsing of GRAPH_TABLE
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_graphtable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_propgraph_label.h"
#include "catalog/pg_propgraph_property.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_graphtable.h"
#include "parser/parse_node.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"


/*
 * Resolve a property reference.
 */
Node *
graph_table_property_reference(ParseState *pstate, ColumnRef *cref, Node *var)
{
	GraphTableParseState *gpstate = pstate->p_ref_hook_state;

	if (list_length(cref->fields) == 2)
	{
		Node	   *field1 = linitial(cref->fields);
		Node	   *field2 = lsecond(cref->fields);
		char	   *elvarname;
		char	   *propname;

		elvarname = strVal(field1);
		propname = strVal(field2);

		if (list_member(gpstate->variables, field1))
		{
			GraphPropertyRef *gpr = makeNode(GraphPropertyRef);
			Oid			propid;

			propid = GetSysCacheOid2(PROPGRAPHPROPNAME, Anum_pg_propgraph_property_oid, ObjectIdGetDatum(gpstate->graphid), CStringGetDatum(propname));
			if (!propid)
				ereport(ERROR,
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("property \"%s\" does not exist", propname));

			gpr->location = cref->location;
			gpr->elvarname = elvarname;
			gpr->propid = propid;
			gpr->typeId = GetSysCacheOid1(PROPGRAPHPROPOID, Anum_pg_propgraph_property_pgptypid, ObjectIdGetDatum(propid));

			return (Node *) gpr;
		}
	}

	return NULL;
}

/*
 * Transform a label expression.
 */
static Node *
transformLabelExpr(GraphTableParseState *gpstate, Node *labelexpr)
{
	Node	   *result;

	if (labelexpr == NULL)
		return NULL;

	check_stack_depth();

	switch (nodeTag(labelexpr))
	{
		case T_ColumnRef:
			{
				ColumnRef  *cref = (ColumnRef *) labelexpr;
				const char *labelname;
				Oid			labelid;
				GraphLabelRef *lref;

				Assert(list_length(cref->fields) == 1);
				labelname = strVal(linitial(cref->fields));

				labelid = GetSysCacheOid2(PROPGRAPHLABELNAME, Anum_pg_propgraph_label_oid, ObjectIdGetDatum(gpstate->graphid), CStringGetDatum(labelname));
				if (!labelid)
					ereport(ERROR,
							errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("label \"%s\" does not exist in property graph \"%s\"", labelname, get_rel_name(gpstate->graphid)));

				lref = makeNode(GraphLabelRef);
				lref->labelid = labelid;
				lref->location = cref->location;

				result = (Node *) lref;
				break;
			}

		case T_BoolExpr:
			{
				BoolExpr   *be = (BoolExpr *) labelexpr;
				ListCell   *lc;
				List	   *args = NIL;

				foreach(lc, be->args)
				{
					Node	   *arg = (Node *) lfirst(lc);

					arg = transformLabelExpr(gpstate, arg);
					args = lappend(args, arg);
				}

				result = (Node *) makeBoolExpr(be->boolop, args, be->location);
				break;
			}

		default:
			/* should not reach here */
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(labelexpr));
			result = NULL;		/* keep compiler quiet */
			break;
	}

	return result;
}

/*
 * Transform a GraphElementPattern.
 */
static Node *
transformGraphElementPattern(ParseState *pstate, GraphTableParseState *gpstate, GraphElementPattern *gep)
{
	if (gep->variable)
		gpstate->variables = lappend(gpstate->variables, makeString(pstrdup(gep->variable)));

	gep->labelexpr = transformLabelExpr(gpstate, gep->labelexpr);

	gep->whereClause = transformExpr(pstate, gep->whereClause, EXPR_KIND_WHERE);
	assign_expr_collations(pstate, gep->whereClause);

	return (Node *) gep;
}

/*
 * Transform a path term (list of GraphElementPattern's).
 */
static Node *
transformPathTerm(ParseState *pstate, GraphTableParseState *gpstate, List *path_term)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, path_term)
	{
		Node	   *n = transformGraphElementPattern(pstate, gpstate, lfirst_node(GraphElementPattern, lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

/*
 * Transform a path pattern list (list of path terms).
 */
static Node *
transformPathPatternList(ParseState *pstate, GraphTableParseState *gpstate, List *path_pattern)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, path_pattern)
	{
		Node	   *n = transformPathTerm(pstate, gpstate, lfirst(lc));

		result = lappend(result, n);
	}

	return (Node *) result;
}

/*
 * Transform a GraphPattern.
 */
Node *
transformGraphPattern(ParseState *pstate, GraphTableParseState *gpstate, GraphPattern *graph_pattern)
{
	graph_pattern->path_pattern_list = (List *) transformPathPatternList(pstate, gpstate, graph_pattern->path_pattern_list);
	graph_pattern->whereClause = transformExpr(pstate, graph_pattern->whereClause, EXPR_KIND_WHERE);
	assign_expr_collations(pstate, graph_pattern->whereClause);

	return (Node *) graph_pattern;
}
