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
 * Represents one property graph element (vertex or edge) in the path.
 *
 * Label expression in an element pattern resolves into a set of elements. For
 * each of those elements we create one graph_path_element object.
 */
struct graph_path_element
{
	Oid			elemoid;
	Oid			reloid;
	int			rtindex;
	List	   *labeloids;
	Oid			srcvertexid;
	int			srcelem_pos;
	Oid			destvertexid;
	int			destelem_pos;
	List	   *qual_exprs;
	GraphElementPattern *parent_gep;
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
static List *get_elements_for_gep(Oid propgraphid, GraphElementPattern *gep, int elempos);
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
	ListCell   *lc;
	int			elempos = 0;

	Assert(list_length(path_pattern) > 0);

	/*
	 * For every element pattern in the given path pattern collect all the
	 * graph elements that satisfy the element pattern.
	 */
	foreach(lc, path_pattern)
	{
		GraphElementPattern *gep = lfirst_node(GraphElementPattern, lc);

		if (gep->kind != VERTEX_PATTERN &&
			gep->kind != EDGE_PATTERN_LEFT && gep->kind != EDGE_PATTERN_RIGHT)
			elog(ERROR, "unsupported element pattern kind: %s", get_gep_kind_name(gep->kind));

		if (gep->quantifier)
			elog(ERROR, "element pattern quantifier not supported yet");

		path_elem_lists = lappend(path_elem_lists,
								  get_elements_for_gep(rte->relid, gep, elempos++));
	}

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
	List	   *gep_elems = list_nth_node(List, path_elem_lists, elempos);
	ListCell   *lc;

	foreach(lc, gep_elems)
	{
		struct graph_path_element *elem = lfirst(lc);

		/* Update current path being built with current element. */
		cur_path = lappend(cur_path, elem);

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
	ListCell   *lc;
	Query	   *path_query = makeNode(Query);
	List	   *fromlist = NIL;
	List	   *qual_exprs = NIL;

	path_query->commandType = CMD_SELECT;

	foreach(lc, graph_path)
	{
		struct graph_path_element *gpe = (struct graph_path_element *) lfirst(lc);
		GraphElementPattern *gep = gpe->parent_gep;
		RangeTblRef *rtr;
		Relation	rel;
		ParseNamespaceItem *pni;

		Assert(gep->kind == VERTEX_PATTERN ||
			   gep->kind == EDGE_PATTERN_LEFT || gep->kind == EDGE_PATTERN_RIGHT);
		Assert(!gep->quantifier);

		if (gep->kind == EDGE_PATTERN_LEFT || gep->kind == EDGE_PATTERN_RIGHT)
		{
			struct graph_path_element *src_gpe = list_nth(graph_path, gpe->srcelem_pos);
			struct graph_path_element *dest_gpe = list_nth(graph_path, gpe->destelem_pos);

			/*
			 * Make sure that the source and destination elements of this edge
			 * are placed at the expected position and have the corret
			 * RangeTblEntry indexes as setup by create_gpe_for_element().
			 */
			Assert(gpe->srcelem_pos == src_gpe->rtindex - 1 &&
				   gpe->destelem_pos == dest_gpe->rtindex - 1);

			/*
			 * If the given edge element does not connect the adjacent vertex
			 * elements in this path, the path is broken. Abandon this path as
			 * it won't return any rows.
			 */
			if (src_gpe->elemoid != gpe->srcvertexid ||
				dest_gpe->elemoid != gpe->destvertexid)
				return NULL;
		}

		/* Create RangeTblEntry for this element table. */
		rel = table_open(gpe->reloid, AccessShareLock);
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
		Assert(gpe->rtindex == rtr->rtindex);

		if (gep->whereClause)
		{
			Node	   *tr;

			tr = replace_property_refs(rte->relid, gep->whereClause, list_make1(gpe));

			qual_exprs = lappend(qual_exprs, tr);
		}
		qual_exprs = list_concat(qual_exprs, gpe->qual_exprs);
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

	/* Each path query projects the columns specified in the GRAH_TABLE clause */
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
	ListCell   *lc;

	query->commandType = CMD_SELECT;


	query->rtable = NIL;
	query->rteperminfos = NIL;


	query->jointree = makeFromExpr(NIL, (Node *) makeBoolConst(false, false));

	/*
	 * Even though no rows are returned, the result still projects the same
	 * columns as projected by GRAPH_TABLE clause. Do this by constructing a
	 * target list full of NULL values.
	 */
	foreach(lc, rte->graph_table_columns)
	{
		TargetEntry *te = copyObject(lfirst_node(TargetEntry, lc));
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
			ListCell   *tl;

			*targetlist = NIL;
			foreach(tl, lquery->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

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
static struct graph_path_element *
create_gpe_for_element(GraphElementPattern *gep, Oid elemoid, int elempos)
{
	HeapTuple	eletup = SearchSysCache1(PROPGRAPHELOID, ObjectIdGetDatum(elemoid));
	Form_pg_propgraph_element pgeform;
	struct graph_path_element *gpe;

	if (!eletup)
		elog(ERROR, "cache lookup failed for property graph element %u", elemoid);
	pgeform = ((Form_pg_propgraph_element) GETSTRUCT(eletup));

	if ((pgeform->pgekind == PGEKIND_VERTEX && gep->kind != VERTEX_PATTERN) ||
		(pgeform->pgekind == PGEKIND_EDGE && !IS_EDGE_PATTERN(gep->kind)))
	{
		ReleaseSysCache(eletup);
		return NULL;
	}

	gpe = palloc0_object(struct graph_path_element);
	gpe->parent_gep = gep;
	gpe->elemoid = elemoid;
	gpe->reloid = pgeform->pgerelid;
	gpe->qual_exprs = NIL;

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
	gpe->rtindex = elempos + 1;

	if (IS_EDGE_PATTERN(gep->kind))
	{
		int			src_rtindex;
		int			dest_rtindex;
		List	   *edge_qual;

		gpe->srcvertexid = pgeform->pgesrcvertexid;
		gpe->destvertexid = pgeform->pgedestvertexid;

		if (gep->kind == EDGE_PATTERN_RIGHT)
		{
			gpe->srcelem_pos = elempos - 1;
			gpe->destelem_pos = elempos + 1;
			src_rtindex = gpe->rtindex - 1;
			dest_rtindex = gpe->rtindex + 1;
		}
		else if (gep->kind == EDGE_PATTERN_LEFT)
		{
			gpe->srcelem_pos = elempos + 1;
			gpe->destelem_pos = elempos - 1;
			src_rtindex = gpe->rtindex + 1;
			dest_rtindex = gpe->rtindex - 1;
		}
		else
		{
			/* We don't support undirected edges yet. */
			Assert(false);
			gpe->srcelem_pos = elempos;
			gpe->destelem_pos = elempos;
			src_rtindex = gpe->rtindex;
			dest_rtindex = gpe->rtindex;
		}

		edge_qual = build_edge_vertex_link_quals(eletup, gpe->rtindex, src_rtindex,
												 Anum_pg_propgraph_element_pgesrckey,
												 Anum_pg_propgraph_element_pgesrcref);
		gpe->qual_exprs = list_concat(gpe->qual_exprs, edge_qual);
		edge_qual = build_edge_vertex_link_quals(eletup, gpe->rtindex, dest_rtindex,
												 Anum_pg_propgraph_element_pgedestkey,
												 Anum_pg_propgraph_element_pgedestref);
		gpe->qual_exprs = list_concat(gpe->qual_exprs, edge_qual);
	}

	ReleaseSysCache(eletup);

	return gpe;
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
			return "undirected edge";
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
		ListCell   *llc;

		label_oids = NIL;
		foreach(llc, label_exprs)
		{
			GraphLabelRef *glr = lfirst_node(GraphLabelRef, llc);

			label_oids = lappend_oid(label_oids, glr->labelid);
		}
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
get_elements_for_gep(Oid propgraphid, GraphElementPattern *gep, int elempos)
{
	List	   *label_oids = get_labels_for_expr(propgraphid, gep->labelexpr);
	List	   *elem_oids_seen = NIL;
	List	   *elem_gpe_oids = NIL;
	List	   *elem_gpes = NIL;
	ListCell   *lc;
	Relation	rel;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	tup;

	rel = table_open(PropgraphElementLabelRelationId, AccessShareLock);
	foreach(lc, label_oids)
	{
		Oid			labeloid = lfirst_oid(lc);
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
				struct graph_path_element *gpe = create_gpe_for_element(gep, elem_oid, elempos);

				if (gpe)
				{
					elem_gpes = lappend(elem_gpes, gpe);
					elem_gpe_oids = lappend_oid(elem_gpe_oids, elem_oid);
					found = true;
				}

				/*
				 * Add the graph element to the elements considered so far to
				 * avoid processing same element associated with multiple
				 * labels multiple times. Also avoids creating duplicate
				 * GraphPathElements.
				 */
				elem_oids_seen = lappend_oid(elem_oids_seen, label_elem->pgelelid);
			}
			else if (list_member_oid(elem_gpe_oids, elem_oid))
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
			if (!gep->labelexpr)
				foreach_delete_current(label_oids, lc);
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("can not find label \"%s\" in property graph \"%s\" for element type \"%s\"",
								get_propgraph_label_name(labeloid),
								get_rel_name(propgraphid),
								get_graph_elem_kind_name(gep->kind))));
		}

		systable_endscan(scan);
	}
	table_close(rel, AccessShareLock);

	/* Update the filtered label list in each graph_path_element. */
	foreach(lc, elem_gpes)
	{
		struct graph_path_element *gpe = lfirst(lc);

		gpe->labeloids = label_oids;
	}

	return elem_gpes;
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
		ListCell   *lc;
		struct graph_path_element *found_mapping = NULL;
		List	   *unrelated_labels = NIL;

		foreach(lc, context->mappings)
		{
			struct graph_path_element *m = lfirst(lc);

			if (m->parent_gep->variable && strcmp(gpr->elvarname, m->parent_gep->variable) == 0)
			{
				found_mapping = m;
				break;
			}
		}
		if (!found_mapping)
			elog(ERROR, "undefined element variable \"%s\"", gpr->elvarname);

		/*
		 * Find property definition for given element through any of the
		 * associated labels.
		 */
		foreach(lc, found_mapping->labeloids)
		{
			Oid			labeloid = lfirst_oid(lc);
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
				ChangeVarNodes(n, 1, found_mapping->rtindex, 0);

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
			ListCell   *lcu;
			bool		prop_associated = false;

			foreach(lcu, unrelated_labels)
			{
				if (is_property_associated_with_label(lfirst_oid(lcu), gpr->propid))
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
											  found_mapping->rtindex);

				if (!n)
				{
					/* XXX: Does collation of NULL value matter? */
					n = (Node *) makeNullConst(gpr->typeId, -1, InvalidOid);
				}
			}

		}

		if (!n)
			elog(ERROR, "property \"%s\" of element variable \"%s\" not found",
				 get_propgraph_property_name(gpr->propid), found_mapping->parent_gep->variable);

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
