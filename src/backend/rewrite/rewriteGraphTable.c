/*-------------------------------------------------------------------------
 *
 * rewriteGraphTable.c
 *		Support for rewriting GRAPH_TABLE clauses.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/rewrite/rewriteGraphTable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_propgraph_element.h"
#include "catalog/pg_propgraph_element_label.h"
#include "catalog/pg_propgraph_label.h"
#include "catalog/pg_propgraph_label_property.h"
#include "catalog/pg_propgraph_property.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "parser/parse_graphtable.h"
#include "rewrite/rewriteGraphTable.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


/*
 * Represents one path factor in a path.
 *
 * In a non-cyclic path, one path factor corresponds to one element pattern.
 *
 * In a cyclic path, one path factor may correspond to one or more element
 * patterns sharing the same variable name, thus pointing to the same graph
 * element.
 */
struct path_factor
{
	GraphElementPatternKind kind;
	const char *variable;
	Node	   *labelexpr;
	Node	   *whereClause;
	int			factorpos;		/* Position of this path factor in a path. */
	List	   *labeloids;		/* OIDs of all the labels referenced in
								 * labelexpr. */
	/* Links to vertex path factors connected by this edge path factor. */
	struct path_factor *src_pf;
	struct path_factor *dest_pf;
};

/*
 * Represents one property graph element (vertex or edge) in the path.
 *
 * Label expression in an element pattern resolves into a set of elements. For
 * each of those elements we create one path_element object.
 */
struct path_element
{
	/* Path factor from which this element is derived. */
	struct path_factor *path_factor;
	Oid			elemoid;
	Oid			reloid;
	/* Source and destination vertex elements for an edge element. */
	Oid			srcvertexid;
	Oid			destvertexid;
	/* Source and destination conditions for an edge element. */
	List	   *src_quals;
	List	   *dest_quals;
};

static Node *replace_property_refs(Oid propgraphid, Node *node, const List *mappings);
static List *build_edge_vertex_link_quals(HeapTuple edgetup, int edgerti, int refrti, AttrNumber catalog_key_attnum, AttrNumber catalog_ref_attnum);
static List *generate_queries_for_path_pattern(RangeTblEntry *rte, List *element_patterns);
static Query *generate_query_for_graph_path(RangeTblEntry *rte, List *path);
static Node *generate_setop_from_pathqueries(List *pathqueries, List **rtable, List **targetlist);
static List *generate_queries_for_path_pattern_recurse(RangeTblEntry *rte, List *pathqueries, List *cur_path, List *path_pattern_lists, int elempos);
static Query *generate_query_for_empty_path_pattern(RangeTblEntry *rte);
static Query *generate_union_from_pathqueries(List **pathqueries);
static const char *get_gep_kind_name(GraphElementPatternKind gepkind);
static List *get_path_elements_for_path_factor(Oid propgraphid, struct path_factor *pf);
static bool is_property_associated_with_label(Oid labeloid, Oid propoid);
static const char *get_graph_elem_kind_name(GraphElementPatternKind gepkind);
static Node *get_element_property_expr(Oid elemoid, Oid propoid, int rtindex);

/*
 * Convert GRAPH_TABLE clause into a subquery using relational
 * operators.
 */
Query *
rewriteGraphTable(Query *parsetree, int rt_index)
{
	RangeTblEntry *rte;
	Query	   *graph_table_query;
	List	   *path_pattern;
	List	   *pathqueries = NIL;

	rte = rt_fetch(rt_index, parsetree->rtable);

	if (list_length(rte->graph_pattern->path_pattern_list) != 1)
		elog(ERROR, "unsupported path pattern list length");

	path_pattern = linitial(rte->graph_pattern->path_pattern_list);
	pathqueries = generate_queries_for_path_pattern(rte, path_pattern);
	graph_table_query = generate_union_from_pathqueries(&pathqueries);

	AcquireRewriteLocks(graph_table_query, true, false);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = graph_table_query;
	rte->lateral = true;

	/*
	 * Reset no longer applicable fields, to appease
	 * WRITE_READ_PARSE_PLAN_TREES.
	 */
	rte->graph_pattern = NULL;
	rte->graph_table_columns = NIL;

#if 0
	elog(INFO, "rewritten:\n%s", pg_get_querydef(copyObject(parsetree), false));
#endif

	return parsetree;
}

/*
 * Generate queries represeting the given path pattern applied to the given
 * property graph.
 *
 * A path pattern consists of one or more element patterns. Each of the element
 * patterns may be satisfied by multiple elements. A path satisfying the given
 * path pattern consists of one element from each element pattern. Each of these
 * paths is converted into a query connecting all the elements in that path.
 * There can be as many paths as the number of combinations of the elements.
 * Compute all these paths and convert each of them into a query. Set of these
 * queries is returned.
 *
 * Assuming that the numbering starts at 0, every element pattern at an even
 * numbered position in the path is a vertex pattern. Every element in even
 * numbered position is an edge pattern. Thus every even numbered element is a
 * vertex table and odd numbered element is an edge table. An edge connects two
 * vertices identified by the source and destination keys respectively. The
 * connections between vertex rows from different vertex tables can be computed
 * by applying relational join between edge table and the adjacent vertex tables
 * respectively. Hence a query representing a path consists of JOIN between
 * adjacent vertex and edge tables.
 *
 * A path pattern in itself is a K-partite graph where K = number of element
 * patterns in the path pattern. The possible paths are computed by performing a
 * DFS in this graph. The DFS is implemented as recursion. A path is converted
 * into the corresponding query as soon as the last vertex table is reached.
 *
 * generate_queries_for_path_pattern() starts the recursion but actual work is
 * done by generate_queries_for_path_pattern_recurse().
 * generate_query_for_graph_path() constructs a query for a given path.
 *
 * A path pattern may result into no path if any of the element pattern yields
 * no elements or edge patterns yield no edges connecting adjacent vertex
 * patterns. In such a case a query which returns no result is returned
 * (generate_query_for_empty_path_pattern()).
 *
 * 'path_pattern' is given path pattern
 * 'rte' references the property graph in the GRAPH_TABLE clause
 */
static List *
generate_queries_for_path_pattern(RangeTblEntry *rte, List *path_pattern)
{
	List	   *pathqueries = NIL;
	List	   *path_elem_lists = NIL;
	int			factorpos = 0;
	List	   *path_factors = NIL;
	struct path_factor *prev_pf = NULL;

	Assert(list_length(path_pattern) > 0);

	/*
	 * For every element pattern in the given path pattern collect all the
	 * graph elements that satisfy the element pattern.
	 *
	 * Element patterns with the same name represent the same element and
	 * hence same path factor. They do not add a new graph element to the
	 * query but affect the links of adjacent elements. Merge such elements
	 * patterns into a single path factor.
	 */
	foreach_node(GraphElementPattern, gep, path_pattern)
	{
		struct path_factor *pf = NULL;

		if (gep->kind != VERTEX_PATTERN && !IS_EDGE_PATTERN(gep->kind))
			elog(ERROR, "unsupported element pattern kind: \"%s\"", get_gep_kind_name(gep->kind));

		if (gep->quantifier)
			elog(ERROR, "element pattern quantifier not supported yet");

		foreach_ptr(struct path_factor, other, path_factors)
		{
			if (gep->variable && other->variable &&
				strcmp(gep->variable, other->variable) == 0)
			{
				if (other->kind != gep->kind)
					ereport(ERROR,
					/* XXX: use correct error code. */
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("element patterns with same variable name \"%s\" but different element pattern types",
									gep->variable)));

				/*
				 * If only one of the two element patterns has a label
				 * expression use it. Otherwise make sure that both of them
				 * have the same label expression. If they have different
				 * label expressions they need to be conjuncted. Label
				 * conjuction is not supported right now.
				 */
				if (!other->labelexpr)
					other->labelexpr = gep->labelexpr;
				else if (gep->labelexpr && !equal(other->labelexpr, gep->labelexpr))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("element patterns with same variable name \"%s\" but different label expressions are not supported",
									gep->variable)));

				/*
				 * Conditions from both elements patterns constrain the graph
				 * element. Combine by ANDing them.
				 */
				if (!other->whereClause)
					other->whereClause = gep->whereClause;
				else if (gep->whereClause)
					other->whereClause = (Node *) makeBoolExpr(AND_EXPR,
															   list_make2(other->whereClause, gep->whereClause),
															   -1);
				pf = other;
				break;
			}
		}

		if (!pf)
		{
			{
				pf = palloc0_object(struct path_factor);
				pf->factorpos = factorpos++;
				pf->kind = gep->kind;
				pf->labelexpr = gep->labelexpr;
				pf->variable = gep->variable;
				pf->whereClause = gep->whereClause;

				path_factors = lappend(path_factors, pf);
			}
		}

		/*
		 * Setup links to the previous path factor. If the previous path
		 * factor is an edge, this path factor represents an adjacent vertex;
		 * source vertex for an edge pointing left or destination vertex for
		 * an edge pointing right. Edge pointing in any direction is treated
		 * similar to that pointing in right direction here. When constructing
		 * a query, in generate_query_for_graph_path() we will swap source and
		 * destination elements if the edge element turns out to be and edge
		 * pointing in left direction.
		 *
		 * If multiple edge patterns share the same variable name, they
		 * constain the adjacent vertex patterns since an edge can connect
		 * only one pair of vertexes. Those vertex patterns also need to
		 * repeated and merged along with the repeated edge (a walk of graph)
		 * even though they have different variables.  E.g.
		 * (a)-[b]->(c)<-[b]-(d) implies that (a) and (d) represent the same
		 * vertex element pattern. This is slighly harder to implement and
		 * probably less useful. Hence not supported for now.
		 */
		if (prev_pf)
		{
			if (prev_pf->kind == EDGE_PATTERN_RIGHT || prev_pf->kind == EDGE_PATTERN_ANY)
			{
				Assert(!IS_EDGE_PATTERN(pf->kind));
				if (prev_pf->dest_pf && prev_pf->dest_pf != pf)
					elog(ERROR, "An edge can not connect more than two vertexes even in a cyclic pattern.");
				prev_pf->dest_pf = pf;
			}
			else if (prev_pf->kind == EDGE_PATTERN_LEFT)
			{
				Assert(!IS_EDGE_PATTERN(pf->kind));
				if (prev_pf->src_pf && prev_pf->src_pf != pf)
					elog(ERROR, "An edge can not connect more than two vertexes even in a cyclic pattern.");
				prev_pf->src_pf = pf;
			}

			if (pf->kind == EDGE_PATTERN_RIGHT || pf->kind == EDGE_PATTERN_ANY)
			{
				Assert(!IS_EDGE_PATTERN(prev_pf->kind));
				if (pf->src_pf && pf->src_pf != prev_pf)
					elog(ERROR, "An edge can not connect more than two vertexes even in a cyclic pattern.");
				pf->src_pf = prev_pf;
			}
			else if (pf->kind == EDGE_PATTERN_LEFT)
			{
				Assert(!IS_EDGE_PATTERN(prev_pf->kind));
				if (pf->dest_pf && pf->dest_pf != prev_pf)
					elog(ERROR, "An edge can not connect more than two vertexes even in a cyclic pattern.");
				pf->dest_pf = prev_pf;
			}
		}

		prev_pf = pf;
	}

	/*
	 * Collect list of elements for each path factor. Do this after all the
	 * edge links are setup correctly.
	 */
	foreach_ptr(struct path_factor, pf, path_factors)
		path_elem_lists = lappend(path_elem_lists,
								  get_path_elements_for_path_factor(rte->relid, pf));

	pathqueries = generate_queries_for_path_pattern_recurse(rte, pathqueries,
															NIL, path_elem_lists, 0);
	if (!pathqueries)
		pathqueries = list_make1(generate_query_for_empty_path_pattern(rte));

	return pathqueries;
}

/*
 * Recursive workhorse function of generate_queries_for_path_pattern().
 */
static List *
generate_queries_for_path_pattern_recurse(RangeTblEntry *rte, List *pathqueries, List *cur_path, List *path_elem_lists, int elempos)
{
	List	   *path_elems = list_nth_node(List, path_elem_lists, elempos);

	foreach_ptr(struct path_element, pe, path_elems)
	{
		/* Update current path being built with current element. */
		cur_path = lappend(cur_path, pe);

		/*
		 * If this is the last element in the path, generate query for the
		 * completed path. Else recurse processing the next element.
		 */
		if (list_length(path_elem_lists) == list_length(cur_path))
		{
			Query	   *pathquery = generate_query_for_graph_path(rte, cur_path);

			Assert(elempos == list_length(path_elem_lists) - 1);
			if (pathquery)
				pathqueries = lappend(pathqueries, pathquery);
		}
		else
			pathqueries = generate_queries_for_path_pattern_recurse(rte, pathqueries,
																	cur_path,
																	path_elem_lists,
																	elempos + 1);
		/* Make way for the next element at the same position. */
		cur_path = list_delete_last(cur_path);
	}

	return pathqueries;
}

/*
 * Construct a query representing given graph path.
 *
 * More details in the prologue of generate_queries_for_path_pattern().
 */
static Query *
generate_query_for_graph_path(RangeTblEntry *rte, List *graph_path)
{
	Query	   *path_query = makeNode(Query);
	List	   *fromlist = NIL;
	List	   *qual_exprs = NIL;

	path_query->commandType = CMD_SELECT;

	foreach_ptr(struct path_element, pe, graph_path)
	{
		struct path_factor *pf = pe->path_factor;
		RangeTblRef *rtr;
		Relation	rel;
		ParseNamespaceItem *pni;

		Assert(pf->kind == VERTEX_PATTERN || IS_EDGE_PATTERN(pf->kind));

		/* Add conditions representing edge connnections. */
		if (IS_EDGE_PATTERN(pf->kind))
		{
			struct path_element *src_pe;
			struct path_element *dest_pe;
			List	   *src_quals;
			List	   *dest_quals;

			Assert(pf->src_pf && pf->dest_pf);
			src_pe = list_nth(graph_path, pf->src_pf->factorpos);
			dest_pe = list_nth(graph_path, pf->dest_pf->factorpos);

			/* Make sure that the links of adjacent vertices are correct. */
			Assert(pf->src_pf == src_pe->path_factor &&
				   pf->dest_pf == dest_pe->path_factor);

			/*
			 * If the given edge element does not connect the adjacent vertex
			 * elements in this path, the path is broken. Abandon this path as
			 * it won't return any rows.
			 *
			 * For an edge element pattern pointing in any direction, try
			 * swapping the source and destination vertex elements.
			 */
			if (src_pe->elemoid != pe->srcvertexid ||
				dest_pe->elemoid != pe->destvertexid)
			{
				if (pf->kind == EDGE_PATTERN_ANY &&
					dest_pe->elemoid == pe->srcvertexid &&
					src_pe->elemoid == pe->destvertexid)
				{
					dest_quals = copyObject(pe->src_quals);
					src_quals = copyObject(pe->dest_quals);

					/* Swap the source and destination varnos in the quals. */
					ChangeVarNodes((Node *) dest_quals, pe->path_factor->src_pf->factorpos + 1,
								   pe->path_factor->dest_pf->factorpos + 1, 0);
					ChangeVarNodes((Node *) src_quals, pe->path_factor->dest_pf->factorpos + 1,
								   pe->path_factor->src_pf->factorpos + 1, 0);
				}
				else
					return NULL;
			}
			else
			{
				src_quals = copyObject(pe->src_quals);
				dest_quals = copyObject(pe->dest_quals);
			}

			qual_exprs = list_concat(qual_exprs, src_quals);
			qual_exprs = list_concat(qual_exprs, dest_quals);
		}
		else
			Assert(!pe->src_quals && !pe->dest_quals);

		/* Create RangeTblEntry for this element table. */
		rel = table_open(pe->reloid, AccessShareLock);
		pni = addRangeTableEntryForRelation(make_parsestate(NULL), rel, AccessShareLock,
											NULL, true, false);
		table_close(rel, NoLock);
		path_query->rtable = lappend(path_query->rtable, pni->p_rte);
		path_query->rteperminfos = lappend(path_query->rteperminfos, pni->p_perminfo);
		pni->p_rte->perminfoindex = list_length(path_query->rteperminfos);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = list_length(path_query->rtable);
		fromlist = lappend(fromlist, rtr);

		/*
		 * Make sure that the assumption mentioned in create_gpe_for_element()
		 * holds true. That the elements' RangeTblEntrys are added in the
		 * order in which they appear in the path.
		 */
		Assert(pf->factorpos + 1 == rtr->rtindex);

		if (pf->whereClause)
		{
			Node	   *tr;

			tr = replace_property_refs(rte->relid, pf->whereClause, list_make1(pe));

			qual_exprs = lappend(qual_exprs, tr);
		}
	}

	if (rte->graph_pattern->whereClause)
	{
		Node	   *path_quals = replace_property_refs(rte->relid,
													   (Node *) rte->graph_pattern->whereClause,
													   graph_path);

		qual_exprs = lappend(qual_exprs, path_quals);
	}

	path_query->jointree = makeFromExpr(fromlist,
										(Node *) makeBoolExpr(AND_EXPR, qual_exprs, -1));

	/* Each path query projects the COLUMNS specified in the GRAH_TABLE. */
	path_query->targetList = castNode(List,
									  replace_property_refs(rte->relid,
															(Node *) rte->graph_table_columns,
															graph_path));
	return path_query;
}

/*
 * Construct a query which would not return any rows.
 *
 * More details in the prologue of generate_queries_for_path_pattern().
 */
static Query *
generate_query_for_empty_path_pattern(RangeTblEntry *rte)
{
	Query	   *query = makeNode(Query);

	query->commandType = CMD_SELECT;


	query->rtable = NIL;
	query->rteperminfos = NIL;


	query->jointree = makeFromExpr(NIL, (Node *) makeBoolConst(false, false));

	/*
	 * Even though no rows are returned, the result still projects the same
	 * columns as projected by GRAPH_TABLE clause. Do this by constructing a
	 * target list full of NULL values.
	 */
	foreach_node(TargetEntry, te, rte->graph_table_columns)
	{
		Node	   *nte = (Node *) te->expr;

		te->expr = (Expr *) makeNullConst(exprType(nte), exprTypmod(nte), exprCollation(nte));
		query->targetList = lappend(query->targetList, te);
	}

	return query;
}

/*
 * Construct a query which is UNION of given path queries.
 *
 * The function destroys given pathqueries list while constructing
 * SetOperationStmt recrursively. Hence the function always returns with
 * `pathqueries` set to NIL.
 */
static Query *
generate_union_from_pathqueries(List **pathqueries)
{
	List	   *rtable = NIL;
	Query	   *sampleQuery = linitial_node(Query, *pathqueries);
	SetOperationStmt *sostmt;
	Query	   *union_query;
	int			resno;
	ListCell   *lctl,
			   *lct,
			   *lcm,
			   *lcc;

	Assert(list_length(*pathqueries) > 0);

	/* If there's only one pathquery, no need to construct a UNION query. */
	if (list_length(*pathqueries) == 1)
	{
		*pathqueries = NIL;
		return sampleQuery;
	}

	sostmt = castNode(SetOperationStmt,
					  generate_setop_from_pathqueries(*pathqueries, &rtable, NULL));

	/* Encapsulate the set operation statement into a Query. */
	union_query = makeNode(Query);
	union_query->commandType = CMD_SELECT;
	union_query->rtable = rtable;
	union_query->setOperations = (Node *) sostmt;
	union_query->rteperminfos = NIL;
	union_query->jointree = makeFromExpr(NIL, NULL);

	/*
	 * Generate dummy targetlist for outer query using column names from one
	 * of the queries and common datatypes/collations of topmost set
	 * operation.  It shouldn't matter which query. Also it shouldn't matter
	 * which RT index is used as varno in the target list entries, as long as
	 * it corresponds to a real RT entry; else funny things may happen when
	 * the tree is mashed by rule rewriting. So we use 1 since there's always
	 * one RT entry at least.
	 */
	Assert(rt_fetch(1, rtable));
	union_query->targetList = NULL;
	resno = 1;
	forfour(lct, sostmt->colTypes,
			lcm, sostmt->colTypmods,
			lcc, sostmt->colCollations,
			lctl, sampleQuery->targetList)
	{
		Oid			colType = lfirst_oid(lct);
		int32		colTypmod = lfirst_int(lcm);
		Oid			colCollation = lfirst_oid(lcc);
		TargetEntry *sample_tle = (TargetEntry *) lfirst(lctl);
		char	   *colName;
		TargetEntry *tle;
		Var		   *var;

		Assert(!sample_tle->resjunk);
		colName = pstrdup(sample_tle->resname);
		var = makeVar(1, sample_tle->resno, colType, colTypmod, colCollation, 0);
		var->location = exprLocation((Node *) sample_tle->expr);
		tle = makeTargetEntry((Expr *) var, (AttrNumber) resno++, colName, false);
		union_query->targetList = lappend(union_query->targetList, tle);
	}

	*pathqueries = NIL;
	return union_query;
}

/*
 * Construct a query which is UNION of all the given path queries.
 *
 * The function destroys given pathqueries list while constructing
 * SetOperationStmt recursively.
 */
static Node *
generate_setop_from_pathqueries(List *pathqueries, List **rtable, List **targetlist)
{
	SetOperationStmt *sostmt;
	Query	   *lquery;
	Node	   *rarg;
	RangeTblRef *lrtr = makeNode(RangeTblRef);
	List	   *rtargetlist;
	ParseNamespaceItem *pni;

	/* Recursion termination condition. */
	if (list_length(pathqueries) == 0)
	{
		*targetlist = NIL;
		return NULL;
	}

	lquery = linitial_node(Query, pathqueries);

	pni = addRangeTableEntryForSubquery(make_parsestate(NULL), lquery, NULL,
										false, false);
	*rtable = lappend(*rtable, pni->p_rte);
	lrtr->rtindex = list_length(*rtable);
	rarg = generate_setop_from_pathqueries(list_delete_first(pathqueries), rtable, &rtargetlist);
	if (rarg == NULL)
	{
		/*
		 * No further path queries in the list. Convert the last query into an
		 * RangeTblRef as expected by SetOperationStmt. Extract a list of the
		 * non-junk TLEs for upper-level processing.
		 */
		if (targetlist)
		{
			*targetlist = NIL;
			foreach_node(TargetEntry, tle, lquery->targetList)
			{
				if (!tle->resjunk)
					*targetlist = lappend(*targetlist, tle);
			}
		}
		return (Node *) lrtr;
	}

	sostmt = makeNode(SetOperationStmt);
	sostmt->op = SETOP_UNION;
	sostmt->all = true;
	sostmt->larg = (Node *) lrtr;
	sostmt->rarg = rarg;
	constructSetOpTargetlist(sostmt, lquery->targetList, rtargetlist, targetlist, "UNION", NULL, false);

	return (Node *) sostmt;
}

/*
 * Construct a graph_path_element object for the graph element given by `elemoid`
 * statisfied by the graph element pattern `gep`.
 *
 * 'elempos` is the position of given element pattern in the path pattern.
 *
 * If the type of graph element does not fit the element pattern kind, the
 * function returns NULL.
 */
static struct path_element *
create_gpe_for_element(struct path_factor *pf, Oid elemoid)
{
	HeapTuple	eletup = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(elemoid));
	Form_pg_propgraph_element pgeform;
	struct path_element *pe;

	if (!eletup)
		elog(ERROR, "cache lookup failed for property graph element %u", elemoid);
	pgeform = ((Form_pg_propgraph_element) GETSTRUCT(eletup));

	if ((pgeform->pgekind == PGEKIND_VERTEX && pf->kind != VERTEX_PATTERN) ||
		(pgeform->pgekind == PGEKIND_EDGE && !IS_EDGE_PATTERN(pf->kind)))
	{
		ReleaseSysCache(eletup);
		return NULL;
	}

	pe = palloc0_object(struct path_element);
	pe->path_factor = pf;
	pe->elemoid = elemoid;
	pe->reloid = pgeform->pgerelid;

	/*
	 * When the path containing this element will be converted into a query
	 * (generate_query_for_graph_path()) this element will be converted into a
	 * RangeTblEntry. The RangeTblEntrys are created in the same order in
	 * which elements appear in the path and thus get consecutive rtindexes.
	 * Knowing those rtindexes here makes it possible to craft elements' qual
	 * expressions only once. Otherwise they need to be crafted as many times
	 * as the number of paths this element appears in. Hence save the assumed
	 * rtindex so that it can be verified later.
	 */
	if (IS_EDGE_PATTERN(pf->kind))
	{
		pe->srcvertexid = pgeform->pgesrcvertexid;
		pe->destvertexid = pgeform->pgedestvertexid;
		Assert(pf->src_pf && pf->dest_pf);

		/*
		 * When the path containing this element will be converted into a
		 * query (generate_query_for_graph_path()) this element will be
		 * converted into a RangeTblEntry. The RangeTblEntrys are created in
		 * the same order in which elements appear in the path and thus get
		 * consecutive RangeTable indexes. So we can safely assign the
		 * RangeTable index of this element before creating RangeTblEntry for
		 * it. This helps crafting the quals linking an edge to the adjacent
		 * vertexes only once while we have access to the catalog entry of the
		 * element. Otherwise, we need to craft the quals as many times as the
		 * number of paths this element appears in, fetching the catalog entry
		 * each time.
		 */
		pe->src_quals = build_edge_vertex_link_quals(eletup, pf->factorpos + 1, pf->src_pf->factorpos + 1,
													 Anum_pg_propgraph_element_pgesrckey,
													 Anum_pg_propgraph_element_pgesrcref);
		pe->dest_quals = build_edge_vertex_link_quals(eletup, pf->factorpos + 1, pf->dest_pf->factorpos + 1,
													  Anum_pg_propgraph_element_pgedestkey,
													  Anum_pg_propgraph_element_pgedestref);
	}

	ReleaseSysCache(eletup);

	return pe;
}

static const char *
get_gep_kind_name(GraphElementPatternKind gepkind)
{
	switch (gepkind)
	{
		case VERTEX_PATTERN:
			return "vertex";
		case EDGE_PATTERN_LEFT:
			return "edge pointing left";
		case EDGE_PATTERN_RIGHT:
			return "edge pointing right";
		case EDGE_PATTERN_ANY:
			return "edge pointing any direction";
		case PAREN_EXPR:
			return "nested path pattern";
	}

	pg_unreachable();
}

/*
 * Returns the list of OIDs of graph labels which the given label expression
 * resolves to in the given property graph.
 */
static List *
get_labels_for_expr(Oid propgraphid, Node *labelexpr)
{
	List	   *label_oids;

	if (!labelexpr)
	{
		Relation	rel;
		SysScanDesc scan;
		ScanKeyData key[1];
		HeapTuple	tup;

		/*
		 * According to section 9.2 "Contextual inference of a set of labels"
		 * subclause 2.a.ii of SQL/PGQ standard, element pattern which does
		 * not have a label expression is considered to have label expression
		 * equivalent to '%|!%' which is set of all labels.
		 */
		label_oids = NIL;
		rel = table_open(PropgraphLabelRelationId, AccessShareLock);
		ScanKeyInit(&key[0],
					Anum_pg_propgraph_label_pglpgid,
					BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(propgraphid));
		scan = systable_beginscan(rel, PropgraphLabelGraphNameIndexId,
								  true, NULL, 1, key);
		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_propgraph_label label = (Form_pg_propgraph_label) GETSTRUCT(tup);

			label_oids = lappend_oid(label_oids, label->oid);
		}
		systable_endscan(scan);
		table_close(rel, AccessShareLock);
	}
	else if (IsA(labelexpr, GraphLabelRef))
	{
		GraphLabelRef *glr = castNode(GraphLabelRef, labelexpr);

		label_oids = list_make1_oid(glr->labelid);
	}
	else if (IsA(labelexpr, BoolExpr))
	{
		BoolExpr   *be = castNode(BoolExpr, labelexpr);
		List	   *label_exprs = be->args;

		label_oids = NIL;
		foreach_node(GraphLabelRef, glr, label_exprs)
			label_oids = lappend_oid(label_oids, glr->labelid);
	}
	else
		elog(ERROR, "unsupported label expression type: %d", (int) nodeTag(labelexpr));

	return label_oids;
}

/*
 * Given a graph element pattern `gep`, return a list of all the graph elements
 * that satisfy the graph pattern.
 *
 * First we find all the graph labels that satisfy the label expression in
 * graph element pattern. Each label has associated with one or more graph
 * elements. A union of all such elements satisfies the element pattern. The
 * returned list contains one graph_path_element object representing each of
 * these elements respectively.
 *
 * `elempos` is position of the element pattern in the path pattern.
 */
static List *
get_path_elements_for_path_factor(Oid propgraphid, struct path_factor *pf)
{
	List	   *label_oids = get_labels_for_expr(propgraphid, pf->labelexpr);
	List	   *elem_oids_seen = NIL;
	List	   *pf_elem_oids = NIL;
	List	   *path_elements = NIL;
	List	   *unresolved_labels = NIL;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tup;

	rel = table_open(PropgraphElementLabelRelationId, AccessShareLock);
	foreach_oid(labeloid, label_oids)
	{
		bool		found = false;

		ScanKeyInit(&key[0],
					Anum_pg_propgraph_element_label_pgellabelid,
					BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(labeloid));
		scan = systable_beginscan(rel, PropgraphElementLabelLabelIndexId, true,
								  NULL, 1, key);
		while (HeapTupleIsValid(tup = systable_getnext(scan)))
		{
			Form_pg_propgraph_element_label label_elem = (Form_pg_propgraph_element_label) GETSTRUCT(tup);
			Oid			elem_oid = label_elem->pgelelid;

			if (!list_member_oid(elem_oids_seen, elem_oid))
			{
				/*
				 * Found a new element that is associated with labels in the
				 * given element pattern. If it fits the element pattern kind
				 * we will create GraphPathPattern object for it and flag that
				 * the current label has at least one element, that satisfies
				 * the given element pattern, associated with it.
				 */
				struct path_element *pe = create_gpe_for_element(pf, elem_oid);

				if (pe)
				{
					path_elements = lappend(path_elements, pe);
					pf_elem_oids = lappend_oid(pf_elem_oids, elem_oid);
					found = true;
				}

				/*
				 * Add the graph element to the elements considered so far to
				 * avoid processing same element associated with multiple
				 * labels multiple times. Also avoids creating duplicate
				 * path_element structures.
				 */
				elem_oids_seen = lappend_oid(elem_oids_seen, label_elem->pgelelid);
			}
			else if (list_member_oid(pf_elem_oids, elem_oid))
			{
				/*
				 * The graph element is known to qualify the given element
				 * pattern. Flag that the current label has at least one
				 * element, that satisfies the given element pattern,
				 * associated with it.
				 */
				found = true;
			}
		}

		if (!found)
		{
			/*
			 * We did not find any element, that fits given element pattern
			 * kind, associated with this label. The label or its properties
			 * can not be associated with the given element pattern. Throw an
			 * error if the label was explicitly specified in the element
			 * pattern. Otherwise just Remove it from the list.
			 */
			if (!pf->labelexpr)
				unresolved_labels = lappend_oid(unresolved_labels, labeloid);
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("can not find label \"%s\" in property graph \"%s\" for element type \"%s\"",
								get_propgraph_label_name(labeloid),
								get_rel_name(propgraphid),
								get_graph_elem_kind_name(pf->kind))));
		}

		systable_endscan(scan);
	}
	table_close(rel, AccessShareLock);

	/*
	 * Remember the OIDs of resolved labels for the given path factor for
	 * later use .
	 */
	pf->labeloids = list_difference_oid(label_oids, unresolved_labels);
	return path_elements;
}

static const char *
get_graph_elem_kind_name(GraphElementPatternKind gepkind)
{
	if (gepkind == VERTEX_PATTERN)
		return "vertex";
	else if (IS_EDGE_PATTERN(gepkind))
		return "edge";
	else if (gepkind == PAREN_EXPR)
		return "nested path pattern";

	return "unknown";
}

/*
 * Mutating property references into table variables
 */

struct replace_property_refs_context
{
	Oid			propgraphid;
	const List *mappings;
};

static Node *
replace_property_refs_mutator(Node *node, struct replace_property_refs_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		Var		   *newvar = copyObject(var);

		/*
		 * If it's already a Var, then it was a lateral reference.  Since we
		 * are in a subquery after the rewrite, we have to increase the level
		 * by one.
		 */
		newvar->varlevelsup++;

		return (Node *) newvar;
	}
	else if (IsA(node, GraphPropertyRef))
	{
		GraphPropertyRef *gpr = (GraphPropertyRef *) node;
		Node	   *n = NULL;
		struct path_element *found_mapping = NULL;
		struct path_factor *mapping_factor = NULL;
		List	   *unrelated_labels = NIL;

		foreach_ptr(struct path_element, m, context->mappings)
		{
			if (m->path_factor->variable && strcmp(gpr->elvarname, m->path_factor->variable) == 0)
			{
				found_mapping = m;
				break;
			}
		}
		if (!found_mapping)
			elog(ERROR, "undefined element variable \"%s\"", gpr->elvarname);

		mapping_factor = found_mapping->path_factor;

		/*
		 * Find property definition for given element through any of the
		 * associated labels.
		 */
		foreach_oid(labeloid, mapping_factor->labeloids)
		{
			Oid			elem_labelid = GetSysCacheOid2(PROPGRAPHELEMENTLABELELEMENTLABEL,
													   Anum_pg_propgraph_element_label_oid,
													   ObjectIdGetDatum(found_mapping->elemoid),
													   ObjectIdGetDatum(labeloid));

			if (OidIsValid(elem_labelid))
			{
				HeapTuple	tup = SearchSysCache2(PROPGRAPHLABELPROP, ObjectIdGetDatum(elem_labelid),
												  ObjectIdGetDatum(gpr->propid));

				if (!tup)
				{
					/*
					 * The label is associated with the given element but it
					 * is not associated with the required property. Check
					 * next label.
					 */
					continue;
				}

				n = stringToNode(TextDatumGetCString(SysCacheGetAttrNotNull(PROPGRAPHLABELPROP,
																			tup, Anum_pg_propgraph_label_property_plpexpr)));
				ChangeVarNodes(n, 1, mapping_factor->factorpos + 1, 0);

				ReleaseSysCache(tup);
			}
			else
			{
				/*
				 * Label is not associated with the element but it may be
				 * associated with the property through some other element.
				 * Save it for later use.
				 */
				unrelated_labels = lappend_oid(unrelated_labels, labeloid);
			}
		}

		/* See if we can resolve the property in some other way. */
		if (!n)
		{
			bool		prop_associated = false;

			foreach_oid(loid, unrelated_labels)
			{
				if (is_property_associated_with_label(loid, gpr->propid))
				{
					prop_associated = true;
					break;
				}
			}

			if (prop_associated)
			{
				/*
				 * The property is associated with at least one of the labels
				 * that satisfy given element pattern. If it's associated with
				 * the given element through some any of the labels, use
				 * correspondig value expression. Otherwise NULL. Ref. SQL/PGQ
				 * standard section 6.5 Property Referece, General Rule 2.b.
				 *
				 * NOTE: An element path pattern may resolve to multiple
				 * elements.  The above section does not seem to describe this
				 * case. But it depends upon how the term ER is interpreted.
				 * For a given path there's only one element bound to a given
				 * ER. Hence the above stated rule can be applied here.  The
				 * section also states the case when no element binds to ER.
				 * We consider such paths as broken and do not contribute any
				 * rows to the GRAPH_TABLE.
				 */
				n = get_element_property_expr(found_mapping->elemoid, gpr->propid,
											  mapping_factor->factorpos + 1);

				if (!n)
				{
					/* XXX: Does collation of NULL value matter? */
					n = (Node *) makeNullConst(gpr->typeId, -1, InvalidOid);
				}
			}

		}

		if (!n)
			elog(ERROR, "property \"%s\" of element variable \"%s\" not found",
				 get_propgraph_property_name(gpr->propid), mapping_factor->variable);

		return n;
	}

	return expression_tree_mutator(node, replace_property_refs_mutator, context);
}

static Node *
replace_property_refs(Oid propgraphid, Node *node, const List *mappings)
{
	struct replace_property_refs_context context;

	context.mappings = mappings;
	context.propgraphid = propgraphid;

	return expression_tree_mutator(node, replace_property_refs_mutator, &context);
}

/*
 * Build join qualification expressions between edge and vertex tables.
 */
static List *
build_edge_vertex_link_quals(HeapTuple edgetup, int edgerti, int refrti, AttrNumber catalog_key_attnum, AttrNumber catalog_ref_attnum)
{
	List	   *quals = NIL;
	Form_pg_propgraph_element pgeform;
	Datum		datum;
	Datum	   *d1,
			   *d2;
	int			n1,
				n2;

	pgeform = (Form_pg_propgraph_element) GETSTRUCT(edgetup);

	datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, edgetup, catalog_key_attnum);
	deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d1, NULL, &n1);

	datum = SysCacheGetAttrNotNull(PROPGRAPHELOID, edgetup, catalog_ref_attnum);
	deconstruct_array_builtin(DatumGetArrayTypeP(datum), INT2OID, &d2, NULL, &n2);

	if (n1 != n2)
		elog(ERROR, "array size key (%d) vs ref (%d) mismatch for element ID %u", catalog_key_attnum, catalog_ref_attnum, pgeform->oid);

	for (int i = 0; i < n1; i++)
	{
		AttrNumber	keyattn = DatumGetInt16(d1[i]);
		AttrNumber	refattn = DatumGetInt16(d2[i]);
		Oid			atttypid;
		TypeCacheEntry *typentry;
		OpExpr	   *op;

		/*
		 * TODO: Assumes types the same on both sides; no collations yet. Some
		 * of this could probably be shared with foreign key triggers.
		 */
		atttypid = get_atttype(pgeform->pgerelid, keyattn);
		typentry = lookup_type_cache(atttypid, TYPECACHE_EQ_OPR);

		op = makeNode(OpExpr);
		op->location = -1;
		op->opno = typentry->eq_opr;
		op->opresulttype = BOOLOID;
		op->args = list_make2(makeVar(edgerti, keyattn, atttypid, -1, 0, 0),
							  makeVar(refrti, refattn, atttypid, -1, 0, 0));
		quals = lappend(quals, op);
	}

	return quals;
}

/*
 * Check if the given property is associated with the given label.
 *
 * A label projects the same set of properties through every element it is
 * associated with. Find any of the elements and return true if that element is
 * associated with the given property. False otherwise.
 */
static bool
is_property_associated_with_label(Oid labeloid, Oid propoid)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tup;
	bool		associated = false;

	rel = table_open(PropgraphElementLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_element_label_pgellabelid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(labeloid));
	scan = systable_beginscan(rel, PropgraphElementLabelLabelIndexId,
							  true, NULL, 1, key);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_propgraph_element_label ele_label = (Form_pg_propgraph_element_label) GETSTRUCT(tup);

		associated = SearchSysCacheExists2(PROPGRAPHLABELPROP,
										   ObjectIdGetDatum(ele_label->oid), ObjectIdGetDatum(propoid));
	}
	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return associated;
}

/*
 * If given element has the given property associated with it, through any of
 * the associated labels, return value expression of the property. Otherwise
 * NULL.
 */
static Node *
get_element_property_expr(Oid elemoid, Oid propoid, int rtindex)
{
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	labeltup;
	Node	   *n = NULL;

	rel = table_open(PropgraphElementLabelRelationId, RowShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_propgraph_element_label_pgelelid,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(elemoid));
	scan = systable_beginscan(rel, PropgraphElementLabelElementLabelIndexId,
							  true, NULL, 1, key);

	while (HeapTupleIsValid(labeltup = systable_getnext(scan)))
	{
		Form_pg_propgraph_element_label ele_label = (Form_pg_propgraph_element_label) GETSTRUCT(labeltup);

		HeapTuple	proptup = SearchSysCache2(PROPGRAPHLABELPROP,
											  ObjectIdGetDatum(ele_label->oid), ObjectIdGetDatum(propoid));

		if (!proptup)
			continue;
		n = stringToNode(TextDatumGetCString(SysCacheGetAttrNotNull(PROPGRAPHLABELPROP,
																	proptup, Anum_pg_propgraph_label_property_plpexpr)));
		ChangeVarNodes(n, 1, rtindex, 0);

		ReleaseSysCache(proptup);
		break;
	}
	systable_endscan(scan);
	table_close(rel, RowShareLock);

	return n;
}
