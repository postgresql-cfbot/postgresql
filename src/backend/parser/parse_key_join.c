/*-------------------------------------------------------------------------
 *
 * parse_key_join.c
 *	  handle key joins in parser
 *
 * A key join is accepted only after parse analysis proves that its rewritten
 * equijoin satisfies these conditions over the named referencing and
 * referenced columns:
 *
 *	  1. The referenced values are unique.
 *	  2. Every non-null referencing value is contained in those referenced
 *		 values.
 *	  3. Referencing values are non-null, unless the join type preserves
 *		 that side.
 *
 * Surface facts are transient parser summaries attached to RangeTblEntry
 * nodes while proving FOR KEY joins.  They are computed on demand and are
 * not part of stored query semantics.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_key_join.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_key_join.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/*
 * KeyJoinColumn
 *
 *		Resolved form of one column named by a raw key-join clause.
 *		Tracks both the namespace item column and the exposed operand
 *		surface column used by proof facts.
 */
typedef struct KeyJoinColumn
{
	char	   *name;
	AttrNumber	nsattno;
	AttrNumber	surface_attno;
	ParseNamespaceColumn *nscol;
} KeyJoinColumn;

/*
 * KeyJoinReq
 *
 *		Proof requirements in the order find_key_join_match checks them; on
 *		failure it reports the first unmet one via the KeyJoinMatch fail*
 *		fields.
 */
typedef enum KeyJoinReq
{
	REQ_NONE = 0,
	REQ_FK,						/* referencing FK fact covering the columns */
	REQ_UNIQUE,					/* referenced unique fact for the FK target */
	REQ_COVERAGE,				/* referenced row-coverage fact */
	REQ_FKPAIR,					/* FK pairs the named columns (cond. 2a) */
	REQ_FILTER,					/* referenced filters remap (cond. 2c) */
	REQ_NOTNULL,				/* referencing not-null evidence (cond. 3) */
} KeyJoinReq;

/*
 * KeyJoinMatch
 *
 *		Complete proof selected for one validated key join.
 *		The parser uses this to build equality quals and to record catalog
 *		dependencies in the stored KeyJoinNode.
 */
typedef struct KeyJoinMatch
{
	Oid			constraint;
	List	   *eqoperators;
	List	   *eqtypes;
	List	   *eqtypmods;
	List	   *notnulldeps;
	List	   *proofdeps;
	/* Filled only on failure, for the rejection message. */
	KeyJoinReq	failReq;
	bool		failInactivated;	/* a matching fact went inactive */
	KeyJoinInactiveReason failReason;
	Oid			failOriginView;
} KeyJoinMatch;

/*
 * KeyJoinFailureSide
 *
 *		Display fields for one side of an unproven key join.  Live parse
 *		analysis has raw KeyJoinColumn names; stored-query revalidation has
 *		only RTE aliases and attnums.
 */
typedef struct KeyJoinFailureSide
{
	const char *alias;
	KeyJoinColumn *columns;
	int			ncolumns;
	RangeTblEntry *rte;
	List	   *attnums;
} KeyJoinFailureSide;

/*
 * KeyJoinQueryStack
 *
 *		View fact projection and stored-query revalidation run on a copied
 *		Query with no ParseState chain.  Keep the copied Query ownership stack
 *		explicitly so CTE RTEs can resolve ctelevelsup without guessing or
 *		exposing facts from the wrong WITH level.
 */
typedef struct KeyJoinQueryStack
{
	struct KeyJoinQueryStack *parent;
	Query	   *query;
} KeyJoinQueryStack;

/*
 * KeyJoinFactContext
 *
 *		Fact computation runs during live parse analysis, or on a copied Query
 *		for either view fact projection or stored-query revalidation.  Keep
 *		that mode explicit instead of spreading nullable ParseState/Query
 *		arguments through the proof code.  A NULL pstate marks the copied-query
 *		case: outer scopes resolve through query_stack rather than a ParseState
 *		(for revalidation the owner-aware replay has already visited the CTE and
 *		FROM-subquery trees).
 */
typedef struct KeyJoinFactContext
{
	ParseState *pstate;
	Query	   *query;
	KeyJoinQueryStack *query_stack;
	bool		for_stored_object;
} KeyJoinFactContext;

/*
 * KeyJoinRowCollapse
 *
 *		One GROUP BY or DISTINCT stage a row-coverage key must survive, with the
 *		reason to record on the fact if it does not.
 */
typedef struct KeyJoinRowCollapse
{
	KeyJoinInactiveReason reason;	/* KJI_GROUP_BY or KJI_DISTINCT */
	List	   *position_sets;		/* list of lists of KeyJoinKeyPosition */
} KeyJoinRowCollapse;

static bool find_key_join_match(RangeTblEntry *referencing_rte,
								RangeTblEntry *referenced_rte,
								List *referencing_attnums,
								List *referenced_attnums,
								bool need_notnull, KeyJoinMatch *match);
static Node *remap_filter_conjunct(Node *conjunct, List *position_map);
static bool filter_conjunct_can_remap(Node *conjunct, List *position_map);
static bool filter_conjunct_matches_key_positions(Node *conjunct,
												  List *keyPositions);
static bool filter_value_allowed(Node *node);
static void ensure_key_join_surface_facts(KeyJoinFactContext *context,
										  RangeTblEntry *rte);
static void compute_key_join_relation_facts(KeyJoinFactContext *context,
											RangeTblEntry *rte,
											Relation rel);
static KeyJoinSurfaceFacts *project_key_join_query_facts(KeyJoinFactContext *context,
														 Query *query);
static JoinExpr *find_join_expr_for_rtindex(KeyJoinFactContext *context,
											Index rtindex);
static void project_key_join_facts_from_rte(KeyJoinSurfaceFacts *dst,
											RangeTblEntry *src, List **attrmap,
											bool preserve_notnull,
											KeyJoinInactiveReason notnull_inact_reason,
											bool preserve_unique,
											KeyJoinInactiveReason unique_inact_reason,
											bool preserve_rowcoverage,
											Node *filter_qual, Query *filter_query,
											Node *filter_jtnode, Index filter_rtindex,
											List *rowcoverage_key_position_sets,
											List *extra_unique_deps,
											KeyJoinInactiveReason inact_reason);
static List *make_rowcollapse_key_positions(Query *query, List *clauses);
static bool add_filter_conjuncts(List **dst, List *keyPositions,
								 Node *qual, Query *filter_query,
								 Node *filter_jtnode, Index filter_rtindex,
								 List **filter_attrmap, int filter_natts,
								 List **dependencies,
								 bool reject_lossy_filter);
static bool key_join_contains_volatile_after_planning(Node *node);
static bool key_join_expression_contains_volatile_after_planning(Node *node);
static bool key_join_after_planning_query_walker(Node *node, void *context);
static bool key_join_nested_query_walker(Node *node, void *context);
static List *map_var_to_jtnode_surface(Query *query, Node *jtnode,
									   Index varno, AttrNumber attno);
static bool collect_filter_expr_dependencies_walker(Node *node,
													void *context_arg);
static bool filter_function_dependency_checker(Oid func_id, void *context_arg);
static List *add_op_function_deps(List *deps, Oid opno, Oid opfuncid);
static void compute_join_output_facts(JoinExpr *j, Index left_rtindex,
									  RangeTblEntry *left_rte,
									  Index right_rtindex,
									  RangeTblEntry *right_rte,
									  RangeTblEntry *joinrte);
static bool revalidate_stored_key_join_node_walker(Node *node, void *context);
static void revalidate_stored_key_join_proofs_in_query(Query *query,
													   KeyJoinQueryStack *parent_stack);
static void revalidate_query_jointree_proofs(Query *query, Node *jtnode,
											 KeyJoinQueryStack *query_stack);
static void add_inactive_projected(KeyJoinSurfaceFacts *dst, KeyJoinFact *old,
								   List **attrmap,
								   KeyJoinInactiveReason reason);

static KeyJoinColumn *resolve_columns_on_nsitem(ParseState *pstate,
												ParseNamespaceItem *lookup,
												ParseNamespaceItem *surface,
												List *names, ParseLoc location,
												bool is_referencing);
static Var *make_var_from_nscolumn(ParseState *pstate,
								   ParseNamespaceColumn *nscol);
static bool join_preserves_side(JoinType jointype, bool leftside);
static Node *build_key_join_quals(List *referenced_args,
								  List *referencing_args,
								  List *eqoperators,
								  List *eqtypes,
								  List *eqtypmods,
								  List *locations,
								  ParseLoc default_location);
static bool select_key_position_parts(List *selected_attnums,
									  List *keyPositions, List *baseAttnums,
									  List **selected_base_attnums,
									  List **selected_key_positions);
static bool rowcollapse_preserves_rowcoverage(List *rowcoverage_key_positions,
											  KeyJoinRowCollapse *rowcollapse);
static int	key_position_index_for_attnum(List *keyPositions, int attno);
static bool key_position_identity_lists_equal(List *left, List *right);
static bool key_position_identity_equal(KeyJoinKeyPosition *left,
										KeyJoinKeyPosition *right);
static bool int_lists_same_members(List *a, List *b);
static List *make_filter_position_map(List *src_base_attnums,
									  List *src_selected_base,
									  List *dst_base_attnums,
									  List *dst_selected_base);
static Node *remap_filter_param_mutator(Node *node, void *context_arg);
static bool filter_conjunct_unremappable_param_walker(Node *node,
													  void *context_arg);
static Node *make_canonical_filter_conjunct(KeyJoinKeyPosition *keypos,
											int pos, OpExpr *op,
											Node *value);
static bool filter_operator_matches_key_position(OpExpr *op,
												 KeyJoinKeyPosition *keypos,
												 bool lock_operator);
static bool filter_operator_is_compatible_equality(Oid opno, Oid keyop);
static bool list_contains_equal_node(List *list, Node *node);
static List *append_dependencies_unique(List *dst, List *src);
static List *append_dependency_unique(List *dependencies, Oid classId,
									  Oid objectId);
static bool dependency_member(List *deps, Oid classId, Oid objectId);
static KeyJoinProofDependency *make_dependency(Oid classId, Oid objectId);
static void append_dependency_to_facts(KeyJoinSurfaceFacts *facts,
									   Oid classId, Oid objectId);
static void lock_key_join_dependency_or_error(const KeyJoinProofDependency *dep);
static void lock_key_join_dependencies_or_error(List *dependencies);
static HeapTuple lock_and_fetch_key_join_constraint(Oid constraintOid);
static HeapTuple lock_and_fetch_key_join_operator(Oid opno);
static HeapTuple lock_and_fetch_key_join_proc(Oid procid);
static Index rtindex_for_rte(KeyJoinFactContext *context,
							 RangeTblEntry *rte);
static JoinExpr *find_join_expr_in_jointree(Node *jtnode, Index rtindex);
static List *project_key_positions(List *keyPositions, List **attrmap);
static Index jtnode_surface_rtindex(Node *jtnode);
static List *append_join_input_mapping(RangeTblEntry *joinrte, bool leftside,
									   List *input_attnums);
static int	join_output_attno_for_input(RangeTblEntry *joinrte,
										bool leftside, int input_colno);
static Var *direct_var_from_node(Node *node);
static Var *direct_filter_var_from_node(Node *node);
static Node *make_filter_param(KeyJoinKeyPosition *keypos, int pos);
static List **build_join_attrmap(RangeTblEntry *joinrte, bool leftside,
								 int nattrs);
static bool join_null_extends_side(JoinType jointype, bool leftside);
static Node *join_filter_for_side(JoinType jointype, bool leftside,
								  Node *filter);
static bool stored_node_contains_key_join_walker(Node *node, void *context);
static void extract_key_join_qual_arg(Node *qual, List **referenced_args,
									  List **referencing_args,
									  List **locations);
static KeyJoinFact *add_fact(KeyJoinSurfaceFacts *set,
							 KeyJoinFactKind kind);
static void add_paired_row_coverage(KeyJoinSurfaceFacts *set,
									List *keypositions, Oid relid,
									List *baseAttnums, List *deps);
static bool key_join_collation_is_usable(Oid collationOid);
static bool key_join_equality_identity_is_usable(Oid typeOid, int32 typmod,
												 Oid eqTypeOid,
												 int32 *eqTypmod);
static bool key_join_equality_operator_is_usable(Oid opno,
												 Oid expectedTypeOid,
												 Oid *eqTypeOid,
												 List **dependencies);
static Oid	key_join_equality_type(Oid typeOid, int32 typmod,
								   int32 *eqTypmod);
static List *make_key_positions_from_attrnums(const TupleDesc tupdesc,
											  const AttrNumber *attnums,
											  int nattnums,
											  const Oid *eqTypes,
											  const int32 *eqTypmods,
											  const Oid *eqOperators);
static KeyJoinKeyPosition *make_key_position(List *attnums, Oid typeOid,
											 int32 typmod, Oid collationOid,
											 Oid eqTypeOid, int32 eqTypmod,
											 Oid eqOperator);
static List *list_make_attrnums(const AttrNumber *attnums, int nattnums);
static char *key_join_failure_detail(KeyJoinReq req, bool inactivated,
									 KeyJoinInactiveReason reason,
									 Oid origin_view,
									 const char *referencing_relcols,
									 const char *referenced_relcols,
									 const char *referencing_relation,
									 const char *referenced_relation,
									 const char *join_name);
static void key_join_report_failure(ParseState *pstate, ParseLoc location,
									JoinType jointype,
									const KeyJoinMatch *match,
									const KeyJoinFailureSide *referencing,
									const KeyJoinFailureSide *referenced);

/*
 * transformAndValidateKeyJoin
 *
 *		Transform raw key-join syntax into proven join quals and a
 *		KeyJoinNode.
 *
 *		This resolves the named columns, ensures operand RTEs expose proof
 *		facts, proves the key join, and installs strict equality quals.
 */
void
transformAndValidateKeyJoin(ParseState *pstate, JoinExpr *j,
							ParseNamespaceItem *l_nsitem,
							ParseNamespaceItem *r_nsitem,
							List *l_namespace)
{
	KeyJoinClause *key_clause = castNode(KeyJoinClause, j->keyJoin);
	ParseNamespaceItem *ref_nsitem = NULL;
	bool		local_is_referencing = (key_clause->direction == KEY_JOIN_RIGHT_ARROW);
	bool		referencing_left = !local_is_referencing;
	ParseNamespaceItem *fk_surface = local_is_referencing ?
		r_nsitem : l_nsitem;
	ParseNamespaceItem *pk_surface = local_is_referencing ?
		l_nsitem : r_nsitem;
	KeyJoinColumn *local_cols;
	KeyJoinColumn *ref_cols;
	List	   *referencing_attnums = NIL;
	List	   *referenced_attnums = NIL;
	List	   *ref_alias_attnums = NIL;
	List	   *referencing_vars = NIL;
	List	   *referenced_vars = NIL;
	KeyJoinFactContext fk_context = {.pstate = pstate,
								   .for_stored_object = pstate->p_stored_object_supports_key_join};
	KeyJoinFactContext pk_context = {.pstate = pstate,
								   .for_stored_object = pstate->p_stored_object_supports_key_join};
	KeyJoinMatch match;
	KeyJoinNode *key_join;
	int			ncols = list_length(key_clause->localCols);

	/*
	 * A FOR KEY join records catalog dependencies so its proof can be
	 * revalidated after a referenced object changes.  A stored-object
	 * container that has not opted into that recording cannot keep such a
	 * proof sound, so reject a key join there.
	 */
	if (pstate->p_creating_stored_object &&
		!pstate->p_stored_object_supports_key_join)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FOR KEY join is not supported in a stored object definition"),
				 parser_errposition(pstate, key_clause->location)));

	if (ncols != list_length(key_clause->refCols))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("key join column lists must have the same length"),
				 parser_errposition(pstate, key_clause->location)));

	/* The key-join alias must name exactly one visible relation on the left. */
	foreach_ptr(ParseNamespaceItem, nsitem, l_namespace)
	{
		Assert(nsitem->p_names != NULL);
		if (!nsitem->p_rel_visible ||
			strcmp(nsitem->p_names->aliasname, key_clause->refAlias) != 0)
			continue;
		if (ref_nsitem != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_ALIAS),
					 errmsg("table reference \"%s\" is ambiguous",
							key_clause->refAlias),
					 parser_errposition(pstate, key_clause->location)));
		ref_nsitem = nsitem;
	}
	if (ref_nsitem == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("key join alias \"%s\" is not present in the left join operand",
						key_clause->refAlias),
				 parser_errposition(pstate, key_clause->location)));

	/*
	 * Local columns are named on the right operand itself.  The arrow alias,
	 * however, may name a visible item inside the left operand, so resolve
	 * against that item and map onto the whole left operand surface.
	 */
	local_cols = resolve_columns_on_nsitem(pstate, r_nsitem, r_nsitem,
										   key_clause->localCols,
										   key_clause->location,
										   local_is_referencing);
	ref_cols = resolve_columns_on_nsitem(pstate, ref_nsitem, l_nsitem,
										 key_clause->refCols,
										 key_clause->location,
										 !local_is_referencing);

	for (int i = 0; i < list_length(key_clause->localCols); i++)
	{
		Var		   *localvar = make_var_from_nscolumn(pstate,
													  local_cols[i].nscol);
		Var		   *refvar = make_var_from_nscolumn(pstate, ref_cols[i].nscol);
		KeyJoinColumn *fkcol = local_is_referencing ?
			&local_cols[i] : &ref_cols[i];
		KeyJoinColumn *pkcol = local_is_referencing ?
			&ref_cols[i] : &local_cols[i];

		ref_alias_attnums = lappend_int(ref_alias_attnums,
										ref_cols[i].nsattno);
		referencing_attnums = lappend_int(referencing_attnums,
										  fkcol->surface_attno);
		referenced_attnums = lappend_int(referenced_attnums,
										 pkcol->surface_attno);
		referencing_vars = lappend(referencing_vars,
								   local_is_referencing ? localvar : refvar);
		referenced_vars = lappend(referenced_vars,
								  local_is_referencing ? refvar : localvar);
	}

	ensure_key_join_surface_facts(&fk_context, fk_surface->p_rte);

	ensure_key_join_surface_facts(&pk_context, pk_surface->p_rte);

	/*
	 * Deliberately ignore j->joinFilter here: the proof is about the two
	 * operand surfaces before any join-local FILTER is applied.
	 */
	if (!find_key_join_match(fk_surface->p_rte, pk_surface->p_rte,
							 referencing_attnums, referenced_attnums,
							 !join_preserves_side(j->jointype, referencing_left),
							 &match))
	{
		KeyJoinFailureSide referencing = {0};
		KeyJoinFailureSide referenced = {0};

		referencing.alias = local_is_referencing ?
			r_nsitem->p_names->aliasname : ref_nsitem->p_names->aliasname;
		referencing.columns = local_is_referencing ? local_cols : ref_cols;
		referencing.ncolumns = ncols;
		referenced.alias = local_is_referencing ?
			ref_nsitem->p_names->aliasname : r_nsitem->p_names->aliasname;
		referenced.columns = local_is_referencing ? ref_cols : local_cols;
		referenced.ncolumns = ncols;
		key_join_report_failure(pstate, key_clause->location, j->jointype,
								&match, &referencing, &referenced);
	}

	INJECTION_POINT("key-join-after-proof-match", NULL);

	/* Install the equality quals proven by condition 2. */
	j->quals = build_key_join_quals(referenced_vars, referencing_vars,
									match.eqoperators, match.eqtypes,
									match.eqtypmods, NIL,
									key_clause->location);

	key_join = makeNode(KeyJoinNode);
	key_join->direction = key_clause->direction;
	key_join->referencingVarno = fk_surface->p_rtindex;
	key_join->referencedVarno = pk_surface->p_rtindex;
	key_join->referencingAttnums = referencing_attnums;
	key_join->referencedAttnums = referenced_attnums;
	key_join->refAliasVarno = ref_nsitem->p_rtindex;
	key_join->refAliasAttnums = ref_alias_attnums;
	key_join->constraint = match.constraint;
	key_join->notNullConstraints = match.notnulldeps;
	key_join->proofDependencies = match.proofdeps;

	j->keyJoin = (Node *) key_join;
}

/*
 * resolve_columns_on_nsitem
 *
 *		Resolve key-join column names against an operand namespace item.
 *
 *		Reject names that are missing, ambiguous, or not exposed by the
 *		operand surface.
 */
static KeyJoinColumn *
resolve_columns_on_nsitem(ParseState *pstate,
						  ParseNamespaceItem *lookup,
						  ParseNamespaceItem *surface, List *names,
						  ParseLoc location, bool is_referencing)
{
	int			ncols = list_length(names);
	int			surface_ncols = list_length(surface->p_names->colnames);
	KeyJoinColumn *cols = palloc0_array(KeyJoinColumn, ncols);
	int			i = 0;

	foreach_ptr(Node, namenode, names)
	{
		char	   *name = strVal(namenode);
		int			attno = 0;
		int			match = 0;

		foreach_ptr(Node, cnnode, lookup->p_names->colnames)
		{
			attno++;
			if (strcmp(strVal(cnnode), name) == 0)
			{
				cols[i].name = name;
				cols[i].nsattno = attno;
				cols[i].nscol = lookup->p_nscolumns + attno - 1;
				match++;
			}
		}
		if (match == 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 is_referencing ?
					 errmsg("referencing column \"%s\" does not exist", name) :
					 errmsg("referenced column \"%s\" does not exist", name),
					 parser_errposition(pstate, location)));
		if (match > 1)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 is_referencing ?
					 errmsg("referencing column \"%s\" is ambiguous", name) :
					 errmsg("referenced column \"%s\" is ambiguous", name),
					 parser_errposition(pstate, location)));

		/* Find the operand surface attnum for the resolved namespace column. */
		for (int j = 0; j < surface_ncols; j++)
		{
			ParseNamespaceColumn *scol = surface->p_nscolumns + j;

			if (scol->p_varno == cols[i].nscol->p_varno &&
				scol->p_varattno == cols[i].nscol->p_varattno)
			{
				cols[i].surface_attno = j + 1;
				break;
			}
		}

		if (cols[i].surface_attno == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 is_referencing ?
					 errmsg("referencing column \"%s\" is not exposed by the join operand",
							name) :
					 errmsg("referenced column \"%s\" is not exposed by the join operand",
							name),
					 parser_errposition(pstate, location)));
		i++;
	}
	return cols;
}

/*
 * make_var_from_nscolumn
 *
 *		Build a Var for a generated key-join qual and mark nullable state
 *		and column read privilege.
 */
static Var *
make_var_from_nscolumn(ParseState *pstate, ParseNamespaceColumn *nscol)
{
	Var		   *var = makeVar(nscol->p_varno, nscol->p_varattno,
							  nscol->p_vartype, nscol->p_vartypmod,
							  nscol->p_varcollid, 0);

	var->varreturningtype = nscol->p_varreturningtype;
	var->varnosyn = nscol->p_varnosyn;
	var->varattnosyn = nscol->p_varattnosyn;
	markNullableIfNeeded(pstate, var);
	markVarForSelectPriv(pstate, var);
	return var;
}

/*
 * join_preserves_side
 *
 *		Return true if the join type preserves rows from the requested side.
 */
static bool
join_preserves_side(JoinType jointype, bool leftside)
{
	return jointype == JOIN_FULL ||
		(leftside ? jointype == JOIN_LEFT : jointype == JOIN_RIGHT);
}

/*
 * build_key_join_quals
 *
 *		Build the executable equality quals for a proven key join.
 */
static Node *
build_key_join_quals(List *referenced_args,
					 List *referencing_args, List *eqoperators,
					 List *eqtypes, List *eqtypmods,
					 List *locations, ParseLoc default_location)
{
	List	   *quals = NIL;
	ListCell   *lcrpk;
	ListCell   *lcrfk;
	ListCell   *lcop;
	ListCell   *lctype;
	ListCell   *lctypmod;
	ListCell   *lcloc;

	Assert(list_length(referenced_args) == list_length(referencing_args));
	Assert(list_length(referenced_args) == list_length(eqoperators));
	Assert(list_length(referenced_args) == list_length(eqtypes));
	Assert(list_length(referenced_args) == list_length(eqtypmods));
	Assert(locations == NIL ||
		   list_length(locations) == list_length(eqoperators));

	lcloc = list_head(locations);
	forfive(lcrpk, referenced_args, lcrfk, referencing_args,
			lcop, eqoperators, lctype, eqtypes, lctypmod, eqtypmods)
	{
		Node	   *pkarg = (Node *) lfirst(lcrpk);
		Node	   *fkarg = (Node *) lfirst(lcrfk);
		Oid			opno = lfirst_oid(lcop);
		Oid			eqtype = lfirst_oid(lctype);
		int32		eqtypmod = lfirst_int(lctypmod);
		ParseLoc	location = default_location;
		Oid			opfuncid;
#ifdef USE_ASSERT_CHECKING
		Oid			lefttype;
		Oid			righttype;
#endif
		OpExpr	   *result;

		if (locations != NIL)
		{
			location = lfirst_int(lcloc);
			lcloc = lnext(locations, lcloc);
		}

		/*
		 * Build a key-join equality OpExpr from a catalog operator OID.  For
		 * domains, the proof identity remains the domain type, while the
		 * executable equality operator is proven and run against the base type.
		 * The proof matcher only passes operators checked against that
		 * normalized equality-input identity and known to be strict,
		 * non-set-returning boolean equality.
		 */
		opfuncid = get_opcode(opno);
		pkarg = applyRelabelType(pkarg, eqtype, eqtypmod,
								 exprCollation(pkarg),
								 COERCE_IMPLICIT_CAST, -1, false);
		fkarg = applyRelabelType(fkarg, eqtype, eqtypmod,
								 exprCollation(fkarg),
								 COERCE_IMPLICIT_CAST, -1, false);
#ifdef USE_ASSERT_CHECKING
		op_input_types(opno, &lefttype, &righttype);
#endif
		Assert(RegProcedureIsValid(opfuncid));
		Assert(lefttype == exprType(pkarg));
		Assert(righttype == exprType(fkarg));
		Assert(get_op_rettype(opno) == BOOLOID);
		Assert(exprTypmod(pkarg) == exprTypmod(fkarg));
		Assert(exprCollation(pkarg) == exprCollation(fkarg));
		Assert(!get_func_retset(opfuncid));
		Assert(func_strict(opfuncid));

		result = makeNode(OpExpr);
		result->opno = opno;
		result->opfuncid = opfuncid;
		result->opresulttype = BOOLOID;
		result->opretset = false;
		result->opcollid = InvalidOid;
		result->inputcollid = exprCollation(pkarg);
		result->args = list_make2(pkarg, fkarg);
		result->location = location;

		quals = lappend(quals, result);
	}

	return (list_length(quals) == 1) ? linitial(quals) :
		(Node *) makeBoolExpr(AND_EXPR, quals, -1);
}

/*
 * find_key_join_match
 *
 *		Find proof facts that validate one key join.
 *
 * The proof has three pieces:
 *	  1. The referenced side has a unique fact covering the selected columns.
 *	  2. The referencing side has a foreign-key fact pointing at that unique
 *		 fact, and any filter quals on the referenced side remap into matching
 *		 filter quals on the referencing side.
 *	  3. If the join type does not preserve the referencing side, each
 *		 referencing column has not-null evidence.
 *
 * On success, *match receives the selected constraint, equality operators
 * and equality-input identities in key-join column order, and the
 * accumulated dependency lists.  On failure, *match receives diagnostic
 * fields describing the first unmet proof requirement.
 */
static bool
find_key_join_match(RangeTblEntry *referencing_rte,
					RangeTblEntry *referenced_rte,
					List *referencing_attnums,
					List *referenced_attnums,
					bool need_notnull,
					KeyJoinMatch *match)
{
	KeyJoinSurfaceFacts *rfacts;
	KeyJoinSurfaceFacts *pfacts;
	KeyJoinReq	reached = REQ_NONE;
	KeyJoinFact *inactive_unique = NULL;
	KeyJoinFact *inactive_coverage = NULL;
	KeyJoinFact *inactive_notnull = NULL;
	bool		fk_target_matched = false;
	bool		fk_target_rel_failure = false;
	bool		fk_target_pair_failure = false;

	Assert(match != NULL);
	Assert(referencing_rte != NULL);
	Assert(referenced_rte != NULL);

	/* Diagnostics default to "missing FK"; the failure tail refines this. */
	match->failReq = REQ_FK;
	match->failInactivated = false;
	match->failReason = KJI_NONE;
	match->failOriginView = InvalidOid;

	if (referenced_rte->tablesample != NULL)
		return false;

	Assert(referencing_rte->keyJoinFactsComputed);
	Assert(referenced_rte->keyJoinFactsComputed);

	if (referencing_rte->keyJoinFacts == NULL ||
		referenced_rte->keyJoinFacts == NULL)
		return false;

	rfacts = referencing_rte->keyJoinFacts;
	pfacts = referenced_rte->keyJoinFacts;

	/* ---- Candidate 1: pick an FK fact on the referencing side ---- */
	foreach_node(KeyJoinFact, fkfact, rfacts->facts)
	{
		List	   *referencing_base;

		if (fkfact->kind != KJF_FOREIGN_KEY)
			continue;
		Assert(fkfact->active);

		Assert(list_length(fkfact->keyPositions) ==
			   list_length(fkfact->baseAttnums));
		Assert(list_length(fkfact->baseAttnums) ==
			   list_length(fkfact->referencedAttnums));

		if (!select_key_position_parts(referencing_attnums,
									   fkfact->keyPositions,
									   fkfact->baseAttnums,
									   &referencing_base, NULL))
			continue;
		if (reached < REQ_FK)
			reached = REQ_FK;

		/*
		 * Before blaming referenced-side proof facts, use active row
		 * provenance to detect when the written referenced columns are not
		 * the target of this FK at all.
		 */
		{
			List	   *fk_referenced_base;
			bool		fk_referenced_selected PG_USED_FOR_ASSERTS_ONLY;

			fk_referenced_selected =
				select_key_position_parts(referencing_attnums,
										  fkfact->keyPositions,
										  fkfact->referencedAttnums,
										  &fk_referenced_base, NULL);
			Assert(fk_referenced_selected);

			foreach_node(KeyJoinFact, coverage, pfacts->facts)
			{
				List	   *coverage_base;

				if (coverage->kind != KJF_ROW_COVERAGE)
					continue;
				if (!coverage->active)
					continue;

				Assert(list_length(coverage->keyPositions) ==
					   list_length(coverage->baseAttnums));

				if (!select_key_position_parts(referenced_attnums,
											   coverage->keyPositions,
											   coverage->baseAttnums,
											   &coverage_base, NULL))
					continue;
				if (coverage->relid != fkfact->referencedRelid)
				{
					fk_target_rel_failure = true;
					continue;
				}

				if (equal(fk_referenced_base, coverage_base))
				{
					fk_target_matched = true;
					break;
				}
				fk_target_pair_failure = true;
			}
		}

		/* ---- Candidate 2: pick a unique fact on the referenced side ---- */
		foreach_node(KeyJoinFact, uniqfact, pfacts->facts)
		{
			List	   *unique_base;
			List	   *unique_key_positions;
			bool		catalog_unique;

			if (uniqfact->kind != KJF_UNIQUE)
				continue;

			Assert(list_length(uniqfact->keyPositions) ==
				   list_length(uniqfact->baseAttnums));

			if (!select_key_position_parts(referenced_attnums,
										   uniqfact->keyPositions,
										   uniqfact->baseAttnums,
										   &unique_base,
										   &unique_key_positions))
				continue;
			Assert(int_lists_same_members(uniqfact->baseAttnums, unique_base));
			if (!uniqfact->active)
			{
				if (inactive_unique == NULL)
					inactive_unique = uniqfact;
				continue;
			}
			catalog_unique = OidIsValid(uniqfact->relid);

			/*
			 * A catalog-backed unique fact has to be on the same relation the
			 * FK fact targets; without that, condition 1 doesn't hold over
			 * the relevant rows.
			 */
			if (catalog_unique &&
				uniqfact->relid != fkfact->referencedRelid)
				continue;
			if (reached < REQ_UNIQUE)
				reached = REQ_UNIQUE;

			/* ---- Candidate 3: pick a row-coverage fact ---- */
			foreach_node(KeyJoinFact, coverage, pfacts->facts)
			{
				/*
				 * Per-coverage scratch.  These live only inside this
				 * candidate; if any check below rejects it, the next
				 * coverage candidate starts fresh.
				 */
				List	   *coverage_base;
				List	   *coverage_key_positions;
				List	   *fk_key_positions = NIL;
				List	   *fk_eqoperators = NIL;
				List	   *fk_eqtypes = NIL;
				List	   *fk_eqtypmods = NIL;
				List	   *local_notnulldeps = NIL;

				if (coverage->kind != KJF_ROW_COVERAGE)
					continue;

				Assert(list_length(coverage->keyPositions) ==
					   list_length(coverage->baseAttnums));

				if (!select_key_position_parts(referenced_attnums,
											   coverage->keyPositions,
											   coverage->baseAttnums,
											   &coverage_base,
											   &coverage_key_positions))
					continue;
				if (!coverage->active)
				{
					if (inactive_coverage == NULL)
						inactive_coverage = coverage;
					continue;
				}

				/*
				 * Matching row-coverage key positions for the selected
				 * referenced columns must be rooted in the FK target. Fact
				 * projection does not merge row-coverage identities across
				 * base relations.
				 */
				if (coverage->relid != fkfact->referencedRelid)
					continue;
				Assert(!catalog_unique ||
					   int_lists_same_members(coverage_base, unique_base));
				Assert(int_lists_same_members(coverage->baseAttnums,
											  coverage_base));
				if (reached < REQ_COVERAGE)
					reached = REQ_COVERAGE;

				/* ---- Condition 2a: FK pairs match the selected columns ---- */
				{
					ListCell   *lcfkbase;
					ListCell   *lcpkbase;
					bool		fk_pairs_match = true;

					/*
					 * Key-join columns must cover the whole FK; a partial FK
					 * match cannot prove containment for a multi-column key.
					 * referencing_base and coverage_base are already in
					 * key-join column order, so we drive the result lists
					 * from them.
					 */
					Assert(list_length(referencing_base) ==
						   list_length(fkfact->keyPositions));
					Assert(list_length(coverage_base) ==
						   list_length(fkfact->keyPositions));

					forboth(lcfkbase, referencing_base,
							lcpkbase, coverage_base)
					{
						int			fkbase = lfirst_int(lcfkbase);
						int			pkbase = lfirst_int(lcpkbase);
						int			fk_catalog_pos = -1;
						ListCell   *lcfkatt;
						ListCell   *lcrefatt;
						KeyJoinKeyPosition *keypos;

						/*
						 * FK facts pair referencing and referenced base
						 * attnums in catalog order.  Locate the pair for this
						 * selected referencing column; the matching
						 * referenced attnum must equal the column we selected
						 * on the other side.
						 */
						forboth(lcfkatt, fkfact->baseAttnums,
								lcrefatt, fkfact->referencedAttnums)
						{
							if (lfirst_int(lcfkatt) != fkbase)
								continue;
							if (lfirst_int(lcrefatt) != pkbase)
							{
								fk_pairs_match = false;
								break;
							}
							fk_catalog_pos = foreach_current_index(lcfkatt);
							break;
						}
						if (!fk_pairs_match)
							break;

						/*
						 * referencing_base was selected from
						 * fkfact->baseAttnums, so this lookup must find the
						 * catalog position.
						 */
						Assert(fk_catalog_pos >= 0);

						keypos = list_nth_node(KeyJoinKeyPosition,
											   fkfact->keyPositions,
											   fk_catalog_pos);
						fk_key_positions = lappend(fk_key_positions, keypos);
						fk_eqoperators = lappend_oid(fk_eqoperators,
													 keypos->eqOperator);
						fk_eqtypes = lappend_oid(fk_eqtypes,
												 keypos->eqTypeOid);
						fk_eqtypmods = lappend_int(fk_eqtypmods,
												   keypos->eqTypmod);
					}
					if (!fk_pairs_match)
						continue;
				}
				if (reached < REQ_FKPAIR)
					reached = REQ_FKPAIR;

				/*
				 * ---- Condition 2b: unique + coverage agree on identity ----
				 *
				 * A relation can expose multiple usable unique indexes on the
				 * same column list.  Only the unique and row-coverage facts
				 * whose key identity matches the FK key identity can prove
				 * this key join.  Identity mismatches are normal candidate
				 * misses, not separately reportable user-facing failures.
				 */
				if (!key_position_identity_lists_equal(unique_key_positions,
													   fk_key_positions))
					continue;
				if (!key_position_identity_lists_equal(coverage_key_positions,
													   fk_key_positions))
					continue;

				/* ---- Condition 2c: referenced filters remap into FK ---- */
				if (coverage->filterConjuncts != NIL)
				{
					List	   *position_map;
					bool		filters_match = true;

					position_map =
						make_filter_position_map(coverage->baseAttnums,
												 coverage_base,
												 fkfact->baseAttnums,
												 referencing_base);

					foreach_ptr(Node, needed, coverage->filterConjuncts)
					{
						Node	   *remapped;

						/*
						 * The selected key-join columns cover both complete
						 * key lists, so every canonical coverage filter has
						 * a target FK key position.
						 */
						Assert(filter_conjunct_can_remap(needed,
														 position_map));
						remapped = remap_filter_conjunct(needed, position_map);
						Assert(filter_conjunct_matches_key_positions(remapped,
																	 fkfact->keyPositions));
						if (!list_contains_equal_node(fkfact->filterConjuncts,
													  remapped))
						{
							filters_match = false;
							break;
						}
					}
					if (!filters_match)
						continue;
				}
				if (reached < REQ_FILTER)
					reached = REQ_FILTER;

				/* ---- Condition 3: not-null evidence on referencing side ---- */
				if (need_notnull)
				{
					bool		notnull_match = true;

					foreach_int(attno, referencing_attnums)
					{
						bool		found = false;

						foreach_node(KeyJoinFact, fact, rfacts->facts)
						{
							if (fact->kind != KJF_NOT_NULL)
								continue;
							if (fact->attnum != attno)
								continue;
							if (!fact->active)
							{
								if (inactive_notnull == NULL)
									inactive_notnull = fact;
								continue;
							}
							local_notnulldeps =
								append_dependencies_unique(local_notnulldeps,
														   fact->dependencies);
							found = true;
							break;
						}
						if (!found)
						{
							notnull_match = false;
							break;
						}
					}
					if (!notnull_match)
						continue;
				}

				/*
				 * ---- Success: construct the result, exactly once ----
				 *
				 * Proof fields are filled only here.  proofdeps is built
				 * locally before commit, so a partial accumulation cannot
				 * leak into the result.
				 */
				{
					List	   *proofdeps = NIL;

					proofdeps = append_dependencies_unique(proofdeps,
														   fkfact->dependencies);
					proofdeps = append_dependencies_unique(proofdeps,
														   uniqfact->dependencies);
					proofdeps = append_dependencies_unique(proofdeps,
														   coverage->dependencies);
					proofdeps = append_dependencies_unique(proofdeps,
														   local_notnulldeps);

					match->constraint = fkfact->constraint;
					match->eqoperators = fk_eqoperators;
					match->eqtypes = fk_eqtypes;
					match->eqtypmods = fk_eqtypmods;
					match->notnulldeps = local_notnulldeps;
					match->proofdeps = proofdeps;
				}
				return true;
			}
		}
	}

	/*
	 * No proof.  Report the first unmet requirement: the deepest an active
	 * candidate reached, plus one.  When a later requirement is a fact we
	 * never found active, blame the inactive one stashed for it, if any.
	 */
	if (reached < REQ_FK)
		match->failReq = REQ_FK;
	else if (!fk_target_matched && fk_target_pair_failure)
		match->failReq = REQ_FKPAIR;
	else if (!fk_target_matched && fk_target_rel_failure)
		match->failReq = REQ_FK;
	else if (reached < REQ_UNIQUE)
		match->failReq = REQ_UNIQUE;
	else if (reached < REQ_COVERAGE)
		match->failReq = REQ_COVERAGE;
	else if (reached < REQ_FILTER)
	{
		Assert(reached >= REQ_FKPAIR);
		match->failReq = REQ_FILTER;
	}
	else
		match->failReq = REQ_NOTNULL;

	{
		KeyJoinFact *blame = NULL;

		if (match->failReq == REQ_UNIQUE)
			blame = inactive_unique;
		else if (match->failReq == REQ_COVERAGE)
			blame = inactive_coverage;
		else if (match->failReq == REQ_NOTNULL)
			blame = inactive_notnull;

		if (blame != NULL)
		{
			match->failInactivated = true;
			match->failReason = blame->inactiveReason;
			match->failOriginView = blame->inactiveOriginView;
		}
	}

	return false;
}

/*
 * select_key_position_parts
 *
 *		Map selected surface attnums to base attnums and/or key positions.
 *
 *		Each selected attnum must identify a distinct key position.
 */
static bool
select_key_position_parts(List *selected_attnums, List *keyPositions,
						  List *baseAttnums, List **selected_base_attnums,
						  List **selected_key_positions)
{
	List	   *base_result = NIL;
	List	   *position_result = NIL;
	List	   *used_positions = NIL;

	if (list_length(selected_attnums) != list_length(keyPositions))
		return false;
	if (selected_base_attnums != NULL)
	{
		Assert(list_length(keyPositions) == list_length(baseAttnums));
	}

	foreach_int(attno, selected_attnums)
	{
		int			pos = key_position_index_for_attnum(keyPositions, attno);

		if (pos < 0 || list_member_int(used_positions, pos))
			return false;
		used_positions = lappend_int(used_positions, pos);

		if (selected_base_attnums != NULL)
			base_result = lappend_int(base_result,
									  list_nth_int(baseAttnums, pos));
		if (selected_key_positions != NULL)
			position_result = lappend(position_result,
									  list_nth(keyPositions, pos));
	}

	if (selected_base_attnums != NULL)
		*selected_base_attnums = base_result;
	if (selected_key_positions != NULL)
		*selected_key_positions = position_result;
	return true;
}

/*
 * rowcollapse_set_preserves_rowcoverage
 *
 *		Return true if one GROUP BY or DISTINCT key alternative preserves one
 *		existing row-coverage fact.
 *
 *		Every projected row-coverage key position must appear in the
 *		grouped/distinct key under the same identity.  Nullable referenced
 *		rows are excluded from the referenced multiset, so collapsing
 *		null-containing referenced rows cannot remove a value required by an
 *		all-non-null referencing key.  Extra row-collapse columns do not merge
 *		distinct covered key values.
 */
static bool
rowcollapse_set_preserves_rowcoverage(List *rowcoverage_key_positions,
									  List *rowcollapse_key_positions)
{
	List	   *used_row_positions = NIL;

	foreach_node(KeyJoinKeyPosition, keypos, rowcoverage_key_positions)
	{
		bool		position_match = false;
		int			pos = 0;

		foreach_node(KeyJoinKeyPosition, rowpos, rowcollapse_key_positions)
		{
			if (list_member_int(used_row_positions, pos))
			{
				pos++;
				continue;
			}
			if (!key_position_identity_equal(rowpos, keypos))
			{
				pos++;
				continue;
			}
			foreach_int(attno, rowpos->attnums)
			{
				if (list_member_int(keypos->attnums, attno))
				{
					position_match = true;
					break;
				}
			}
			if (position_match)
			{
				used_row_positions = lappend_int(used_row_positions, pos);
				break;
			}
			pos++;
		}
		if (!position_match)
			return false;
	}

	return true;
}

/*
 * rowcollapse_preserves_rowcoverage
 *
 *		Return true if a GROUP BY or DISTINCT row-collapse stage preserves one
 *		existing row-coverage fact.
 *
 *		Simple GROUP BY and DISTINCT have one alternative key.  GROUPING SETS,
 *		ROLLUP, and CUBE have multiple alternatives, and row coverage survives
 *		if any one resulting grouping set contains the full key.
 */
static bool
rowcollapse_preserves_rowcoverage(List *rowcoverage_key_positions,
								  KeyJoinRowCollapse *rowcollapse)
{
	foreach_ptr(List, rowcollapse_key_positions, rowcollapse->position_sets)
	{
		if (rowcollapse_set_preserves_rowcoverage(rowcoverage_key_positions,
												  rowcollapse_key_positions))
			return true;
	}

	return false;
}

static int
key_position_index_for_attnum(List *keyPositions, int attno)
{
	int			match = -1;

	foreach_node(KeyJoinKeyPosition, keypos, keyPositions)
	{
		if (list_member_int(keypos->attnums, attno))
		{
			if (match >= 0)
				return -1;
			match = foreach_current_index(keypos);
		}
	}
	return match;
}

/*
 * key_position_identity_lists_equal
 *
 *		Return true if two key-position lists have matching key identities.
 */
static bool
key_position_identity_lists_equal(List *left, List *right)
{
	ListCell   *lcleft;
	ListCell   *lcright;

	Assert(list_length(left) == list_length(right));
	forboth(lcleft, left, lcright, right)
	{
		if (!key_position_identity_equal(lfirst_node(KeyJoinKeyPosition, lcleft),
										 lfirst_node(KeyJoinKeyPosition, lcright)))
			return false;
	}
	return true;
}

/*
 * key_position_identity_equal
 *
 *		Return true if two key positions have the same type/collation/op
 *		identity.
 */
static bool
key_position_identity_equal(KeyJoinKeyPosition *left, KeyJoinKeyPosition *right)
{
	if (left->typeOid != right->typeOid)
		return false;
	if (left->typmod != right->typmod)
		return false;
	if (left->collationOid != right->collationOid)
		return false;
	if (left->eqTypeOid != right->eqTypeOid)
		return false;
	Assert(left->eqTypmod == right->eqTypmod);
	return left->eqOperator == right->eqOperator;
}

static bool
int_lists_same_members(List *a, List *b)
{
	if (list_length(a) != list_length(b))
		return false;
	foreach_int(value, a)
	{
		if (!list_member_int(b, value))
			return false;
	}
	return true;
}

/*
 * make_filter_position_map
 *
 *		Build a Param-position map between source and target key lists.
 *
 *		Entries of -1 mark source filters outside the selected key join.
 */
static List *
make_filter_position_map(List *src_base_attnums, List *src_selected_base,
						 List *dst_base_attnums, List *dst_selected_base)
{
	List	   *result = NIL;

	Assert(list_length(src_selected_base) == list_length(dst_selected_base));

	foreach_int(srcbase, src_base_attnums)
	{
		int			dstpos = -1;
		ListCell   *lcsrc;
		ListCell   *lcdst;

		forboth(lcsrc, src_selected_base, lcdst, dst_selected_base)
		{
			if (lfirst_int(lcsrc) == srcbase)
			{
				int			dstbase = lfirst_int(lcdst);

				foreach_int(baseattno, dst_base_attnums)
				{
					if (baseattno == dstbase)
					{
						dstpos = foreach_current_index(baseattno);
						break;
					}
				}
				break;
			}
		}
		result = lappend_int(result, dstpos);
	}
	return result;
}

/*
 * remap_filter_conjunct
 *
 *		Remap proof-filter Params through a position map.
 */
static Node *
remap_filter_conjunct(Node *conjunct, List *position_map)
{
	return remap_filter_param_mutator(conjunct, position_map);
}

/*
 * filter_conjunct_can_remap
 *
 *		Return true if every proof-filter Param used by this conjunct has a
 *		target key position in the supplied map.
 */
static bool
filter_conjunct_can_remap(Node *conjunct, List *position_map)
{
	return !filter_conjunct_unremappable_param_walker(conjunct, position_map);
}

/*
 * remap_filter_param_mutator
 *
 *		Mutator callback for remapping key-join proof filter Params.
 */
static Node *
remap_filter_param_mutator(Node *node, void *context_arg)
{
	List	   *position_map = (List *) context_arg;

	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *param = castNode(Param, node);
		Param	   *newparam;
		int			oldpos;
		int			newpos;

		Assert(param->paramkind == PARAM_KEYJOIN);

		/*
		 * paramid is a 1-based position into the key-position list that the
		 * caller already sized.  These two conditions are therefore
		 * programmer invariants, not user-reachable errors.
		 */
		oldpos = param->paramid - 1;
		Assert(oldpos >= 0);
		Assert(oldpos < list_length(position_map));

		newpos = list_nth_int(position_map, oldpos);
		Assert(newpos >= 0);
		newparam = copyObject(param);
		newparam->paramid = newpos + 1;
		return (Node *) newparam;
	}

	return expression_tree_mutator(node, remap_filter_param_mutator, position_map);
}

/*
 * filter_conjunct_unremappable_param_walker
 *
 *		Walker callback for finding proof-filter Params that cannot remap.
 */
static bool
filter_conjunct_unremappable_param_walker(Node *node, void *context_arg)
{
	List	   *position_map = (List *) context_arg;

	Assert(node != NULL);

	if (IsA(node, Param))
	{
		Param	   *param = castNode(Param, node);
		int			oldpos;

		Assert(param->paramkind == PARAM_KEYJOIN);

		oldpos = param->paramid - 1;
		Assert(oldpos >= 0);
		Assert(oldpos < list_length(position_map));
		return list_nth_int(position_map, oldpos) < 0;
	}

	return expression_tree_walker(node,
								  filter_conjunct_unremappable_param_walker,
								  position_map);
}

/*
 * filter_conjunct_matches_key_positions
 *
 *		Check that a key-join proof filter matches key identity.
 */
static bool
filter_conjunct_matches_key_positions(Node *conjunct, List *keyPositions)
{
	OpExpr	   *op;
	Param	   *param;
	KeyJoinKeyPosition *keypos;
	int			pos;
#ifdef USE_ASSERT_CHECKING
	Node	   *value;
#endif

	Assert(conjunct != NULL);
	op = castNode(OpExpr, conjunct);
	Assert(list_length(op->args) == 2);
	param = castNode(Param, linitial(op->args));
	Assert(param->paramkind == PARAM_KEYJOIN);

	pos = param->paramid - 1;
	Assert(pos >= 0);
	Assert(pos < list_length(keyPositions));
	keypos = list_nth_node(KeyJoinKeyPosition, keyPositions, pos);

	/*
	 * add_filter_conjuncts() stores only canonical key = value filters.
	 * Remapping changes only Param ids; the original filter operator and
	 * value expression must remain compatible with the destination key.
	 */
	Assert(param->paramtype == keypos->eqTypeOid);
	Assert(param->paramtypmod == keypos->eqTypmod);
	Assert(param->paramcollid == keypos->collationOid);
	if (!filter_operator_matches_key_position(op, keypos, false))
		return false;

#ifdef USE_ASSERT_CHECKING
	value = lsecond(op->args);
	Assert(filter_value_allowed(value));
#endif
	return true;
}

/*
 * filter_value_allowed
 *
 *		Return true if a filter value can be stored in proof-filter form.
 *		Proof filters use a strict allowlist: constants, SQL value functions,
 *		non-volatile scalar functions, and transparent type/collation wrappers
 *		whose arguments are also allowed.  The full after-planning volatility
 *		check runs on the canonical filter after locking its dependencies.
 */
static bool
filter_value_allowed(Node *node)
{
	Assert(node != NULL);

	if (IsA(node, Const))
		return true;

	if (IsA(node, SQLValueFunction))
		return true;

	if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = castNode(FuncExpr, node);

		Assert(!expr->funcretset);
		if (func_volatile(expr->funcid) == PROVOLATILE_VOLATILE)
			return false;
		foreach_ptr(Node, arg, expr->args)
		{
			if (!filter_value_allowed(arg))
				return false;
		}
		return true;
	}

	if (IsA(node, RelabelType))
		return filter_value_allowed((Node *) castNode(RelabelType, node)->arg);

	if (IsA(node, CoerceViaIO))
		return filter_value_allowed((Node *) castNode(CoerceViaIO, node)->arg);

	if (IsA(node, CollateExpr))
		return filter_value_allowed((Node *) castNode(CollateExpr, node)->arg);

	return false;
}

/*
 * make_canonical_filter_conjunct
 *
 *		Build the stored proof-filter representation for a direct key filter.
 */
static Node *
make_canonical_filter_conjunct(KeyJoinKeyPosition *keypos, int pos, OpExpr *op,
							   Node *value)
{
	OpExpr	   *newop;

	Assert(op != NULL);
	Assert(value != NULL);
	Assert(filter_value_allowed(value));
	Assert(filter_operator_matches_key_position(op, keypos, false));

	newop = copyObject(op);
	set_opfuncid(newop);
	Assert(OidIsValid(newop->opfuncid));
	newop->args = list_make2(make_filter_param(keypos, pos),
							 copyObject(value));
	newop->location = -1;
	return (Node *) newop;
}

/*
 * filter_operator_matches_key_position
 *
 *		Check whether a direct key-filter operator is compatible with keypos.
 */
static bool
filter_operator_matches_key_position(OpExpr *op, KeyJoinKeyPosition *keypos,
									 bool lock_operator)
{
	Node	   *left;
	Node	   *right;
	RegProcedure funcid;
	Oid			lefttype;
	Oid			righttype;
	Oid			rettype;
	bool		result;

	Assert(op != NULL);
	Assert(keypos != NULL);
	Assert(list_length(op->args) == 2);

	left = linitial(op->args);
	right = lsecond(op->args);

	Assert(OidIsValid(op->opno));

	if (lock_operator)
	{
		HeapTuple	optup;
		HeapTuple	proctup;
		Form_pg_operator opform;
		Form_pg_proc procform;

		optup = lock_and_fetch_key_join_operator(op->opno);
		opform = (Form_pg_operator) GETSTRUCT(optup);
		funcid = opform->oprcode;
		lefttype = opform->oprleft;
		righttype = opform->oprright;
		rettype = opform->oprresult;
		ReleaseSysCache(optup);

		Assert(OidIsValid(funcid));

		proctup = lock_and_fetch_key_join_proc((Oid) funcid);
		procform = (Form_pg_proc) GETSTRUCT(proctup);
		Assert(!procform->proretset);
		result = procform->proisstrict &&
			procform->provolatile == PROVOLATILE_IMMUTABLE;
		ReleaseSysCache(proctup);
	}
	else
	{
		funcid = get_opcode(op->opno);
		op_input_types(op->opno, &lefttype, &righttype);
		rettype = get_op_rettype(op->opno);
		Assert(OidIsValid(funcid));
		Assert(func_strict(funcid));
		Assert(func_volatile(funcid) == PROVOLATILE_IMMUTABLE);
		Assert(!get_func_retset(funcid));
		result = true;
	}

	result &= lefttype == keypos->eqTypeOid;
	result &= righttype == exprType(right);
	result &= rettype == BOOLOID;
	result &= !op->opretset;
	result &= op->opresulttype == BOOLOID;
	result &= exprType(left) == keypos->eqTypeOid;
	result &= exprTypmod(left) == keypos->eqTypmod;
	result &= exprCollation(left) == keypos->collationOid;
	result &= filter_operator_is_compatible_equality(op->opno,
													 keypos->eqOperator);
	result &= collations_agree_on_equality(op->inputcollid,
										   keypos->collationOid);

	return result;
}

/*
 * filter_operator_is_compatible_equality
 *
 *		Check whether opno is itself an equality operator compatible with keyop.
 */
static bool
filter_operator_is_compatible_equality(Oid opno, Oid keyop)
{
	RegProcedure hash_proc;

	if (opno == keyop)
		return true;
	if (!equality_ops_are_compatible(opno, keyop))
		return false;

	foreach_ptr(OpIndexInterpretation, interpretation,
				get_op_index_interpretation(opno))
	{
		if (interpretation->cmptype != COMPARE_EQ)
			continue;
		if (op_in_opfamily(keyop, interpretation->opfamily_id))
			return true;
	}

	return get_op_hash_functions(opno, &hash_proc, NULL);
}

/*
 * key_join_contains_volatile_after_planning
 *
 *		Return true if a key-join proof expression or query contains
 *		volatile functions after planner expression preprocessing.
 *
 * contain_volatile_functions_after_planning() accepts expressions, not whole
 * Query trees.  Key-join proof code needs both forms, so handle Query nodes
 * by walking each top-level expression subtree once.  A separate nested-Query
 * walk restarts after-planning preprocessing inside subqueries, which
 * expression_planner() deliberately does not descend into when called on a
 * standalone expression.
 */
static bool
key_join_contains_volatile_after_planning(Node *node)
{
	Assert(node != NULL);

	if (IsA(node, Query))
		return query_tree_walker(castNode(Query, node),
								 key_join_after_planning_query_walker,
								 NULL, 0);

	return key_join_expression_contains_volatile_after_planning(node);
}

static bool
key_join_expression_contains_volatile_after_planning(Node *node)
{
	Assert(node != NULL);
	Assert(!IsA(node, Query));

	if (contain_volatile_functions_after_planning((Expr *) node))
		return true;

	return expression_tree_walker(node, key_join_nested_query_walker, NULL);
}

static bool
key_join_after_planning_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
		return key_join_contains_volatile_after_planning(node);

	return key_join_expression_contains_volatile_after_planning(node);
}

static bool
key_join_nested_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
		return key_join_contains_volatile_after_planning(node);

	/*
	 * key_join_expression_contains_volatile_after_planning() already checked
	 * this whole expression subtree after planning.  Continue walking only so
	 * that nested Query nodes get their own after-planning pass.
	 */
	return expression_tree_walker(node, key_join_nested_query_walker, context);
}

static bool
list_contains_equal_node(List *list, Node *node)
{
	foreach_ptr(Node, oldnode, list)
	{
		if (equal(node, oldnode))
			return true;
	}
	return false;
}

/*
 * append_dependencies_unique
 *
 *		Append dependency entries from src to dst, suppressing duplicates.
 */
static List *
append_dependencies_unique(List *dst, List *src)
{
	foreach_node(KeyJoinProofDependency, dep, src)
	{
		if (!dependency_member(dst, dep->classId, dep->objectId))
			dst = lappend(dst, copyObject(dep));
	}
	return dst;
}

/*
 * append_dependency_unique
 *
 *		Append one dependency entry if not already present.
 */
static List *
append_dependency_unique(List *dependencies, Oid classId, Oid objectId)
{
	Assert(OidIsValid(objectId));

	if (dependency_member(dependencies, classId, objectId))
		return dependencies;
	return lappend(dependencies, make_dependency(classId, objectId));
}

static bool
dependency_member(List *deps, Oid classId, Oid objectId)
{
	foreach_node(KeyJoinProofDependency, dep, deps)
	{
		if (dep->classId != classId)
			continue;
		if (dep->objectId != objectId)
			continue;
		return true;
	}
	return false;
}

/*
 * make_dependency
 *
 *		Build a KeyJoinProofDependency node for a whole-object dependency.
 */
static KeyJoinProofDependency *
make_dependency(Oid classId, Oid objectId)
{
	KeyJoinProofDependency *dep = makeNode(KeyJoinProofDependency);

	dep->classId = classId;
	dep->objectId = objectId;
	return dep;
}

/*
 * append_dependency_to_facts
 *
 *		Mark every projected fact as depending on the same proof surface.
 */
static void
append_dependency_to_facts(KeyJoinSurfaceFacts *facts,
						   Oid classId, Oid objectId)
{
	if (facts == NULL)
		return;

	foreach_node(KeyJoinFact, fact, facts->facts)
		fact->dependencies =
			append_dependency_unique(fact->dependencies, classId, objectId);
}

/*
 * lock_key_join_dependency_or_error
 *
 *		Lock one filter expression dependency with the same object-lock family
 *		used by dependency deletion, then verify the locked object still
 *		exists.  Filter expressions can depend on operators and functions only.
 *		Relation and constraint proof dependencies are locked by their fact
 *		builders before publication.
 */
static void
lock_key_join_dependency_or_error(const KeyJoinProofDependency *dep)
{
	Assert(dep->classId == OperatorRelationId ||
		   dep->classId == ProcedureRelationId);

	if (dep->classId == OperatorRelationId)
	{
		HeapTuple	optup;

		optup = lock_and_fetch_key_join_operator(dep->objectId);
		ReleaseSysCache(optup);
	}
	else
	{
		HeapTuple	proctup;

		Assert(dep->classId == ProcedureRelationId);
		proctup = lock_and_fetch_key_join_proc(dep->objectId);
		ReleaseSysCache(proctup);
	}
}

/*
 * lock_key_join_dependencies_or_error
 *
 *		Block until every dependency is locked and known to still exist.  Facts
 *		may only publish dependencies that have passed through this helper or an
 *		equivalent lock-and-fetch path.
 */
static void
lock_key_join_dependencies_or_error(List *dependencies)
{
	INJECTION_POINT("key-join-before-filter-dependency-lock", NULL);

	foreach_node(KeyJoinProofDependency, dep, dependencies)
		lock_key_join_dependency_or_error(dep);
}

/*
 * lock_and_fetch_key_join_constraint
 *
 *		Lock one constraint proof dependency, then return its syscache tuple.
 *		The caller must release the tuple.
 */
static HeapTuple
lock_and_fetch_key_join_constraint(Oid constraintOid)
{
	HeapTuple	contup;

	Assert(OidIsValid(constraintOid));

	LockDatabaseObject(ConstraintRelationId, constraintOid, 0,
					   AccessShareLock);
	contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	Assert(HeapTupleIsValid(contup));
	return contup;
}

/*
 * lock_and_fetch_key_join_operator
 *
 *		Lock one operator proof dependency, then return its syscache tuple.
 *		The caller must release the tuple.
 */
static HeapTuple
lock_and_fetch_key_join_operator(Oid opno)
{
	HeapTuple	optup;

	Assert(OidIsValid(opno));

	LockDatabaseObject(OperatorRelationId, opno, 0, AccessShareLock);
	optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (!HeapTupleIsValid(optup))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("key join proof dependency operator was concurrently dropped")));
	return optup;
}

/*
 * lock_and_fetch_key_join_proc
 *
 *		Lock one function proof dependency, then return its syscache tuple.
 *		The caller must release the tuple.
 */
static HeapTuple
lock_and_fetch_key_join_proc(Oid procid)
{
	HeapTuple	proctup;

	Assert(OidIsValid(procid));

	LockDatabaseObject(ProcedureRelationId, procid, 0, AccessShareLock);
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procid));
	if (!HeapTupleIsValid(proctup))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("key join proof dependency function was concurrently dropped")));
	return proctup;
}

/*
 * ensure_key_join_surface_facts
 *
 *		Key-join validation needs a demand-driven fact cache on each RTE it
 *		inspects.  Some RTEs have no sound surface facts in this proof model
 *		(for example, lateral subqueries, recursive CTEs, and ordinary
 *		joins whose output cannot preserve input facts), so the cache also
 *		records "computed, but no facts".
 *
 *		Keep that decision on a single path across live parse analysis, view
 *		projection, and stored-query revalidation.  Those callers reach the
 *		same RTEs with different context available (ParseState for live
 *		parsing, copied Query stacks for stored queries).
 */
static void
ensure_key_join_surface_facts(KeyJoinFactContext *context, RangeTblEntry *rte)
{
	Assert(context != NULL);
	Assert(rte != NULL);
	Assert(context->pstate != NULL ||
		   (context->query != NULL &&
			context->query_stack != NULL));

	if (rte->keyJoinFactsComputed)
		return;

	Assert(rte->keyJoinFacts == NULL);

	switch (rte->rtekind)
	{
		case RTE_RELATION:
			{
				/*
				 * Live parser RTEs already hold their rellockmode lock from
				 * range table construction, but stored-query revalidation and
				 * view fact computation can reach this helper with only a
				 * copied Query tree.  Hold this lock until transaction end so
				 * DDL cannot change relation state or view definitions between
				 * fact selection and stored dependency creation.
				 */
				Relation	rel = relation_open(rte->relid, AccessShareLock);

				if (rte->relkind == RELKIND_VIEW)
				{
					/*
					 * Views expose facts by projecting a copy of their
					 * query.  Keep this query-projection path at the RTE
					 * dispatcher rather than inside the catalog fact
					 * collector for base relations.
					 */
					Query	   *viewquery = get_view_query(rel);
					KeyJoinQueryStack qs;
					KeyJoinFactContext view_context = *context;

					viewquery = copyObject(viewquery);
					revalidate_stored_key_join_proofs_in_query(viewquery,
															   NULL);
					qs.parent = NULL;
					qs.query = viewquery;
					view_context.pstate = NULL;
					view_context.query = viewquery;
					view_context.query_stack = &qs;
					view_context.for_stored_object = true;
					rte->keyJoinFacts =
						project_key_join_query_facts(&view_context,
													 viewquery);
					append_dependency_to_facts(rte->keyJoinFacts,
											   RelationRelationId,
											   rte->relid);

					/*
					 * Facts the view discarded inside its own body carry no
					 * usable caret, since its text is not the current
					 * statement.  When the view is referenced from locatable
					 * top-level text, name it so a rejection can point the
					 * user at the view to inspect.  Only this outermost
					 * boundary (enclosing context live) claims origin; inner
					 * views ran with a null pstate (copied-query mode).
					 */
					if (rte->keyJoinFacts != NULL &&
						context->pstate != NULL)
					{
						foreach_node(KeyJoinFact, fact,
									 rte->keyJoinFacts->facts)
						{
							if (!fact->active)
							{
								Assert(!OidIsValid(fact->inactiveOriginView));
								fact->inactiveOriginView = rte->relid;
							}
						}
					}
				}
				else
					compute_key_join_relation_facts(context, rte, rel);
				relation_close(rel, NoLock);
			}
			break;
		case RTE_SUBQUERY:
			{
				KeyJoinQueryStack qs;

				Assert(rte->subquery != NULL);

				/*
				 * LATERAL subqueries need per-outer-row cardinality proof
				 * before their projected facts can be used soundly, so expose
				 * no facts from them.
				 */
				if (rte->lateral)
				{
					rte->keyJoinFacts = NULL;
					break;
				}

				/*
				 * Stored-query replay visits FROM-subqueries before their
				 * owner's jointree, and view fact projection reaches them on
				 * demand; either way this demand path projects facts only.
				 */
				Assert(context->pstate != NULL ||
					   context->query_stack != NULL);

				qs.parent = context->query_stack;
				qs.query = rte->subquery;
				{
					KeyJoinFactContext subcontext = *context;

					subcontext.query = rte->subquery;
					subcontext.query_stack = &qs;
					rte->keyJoinFacts =
						project_key_join_query_facts(&subcontext,
													 rte->subquery);
				}
				break;
			}
		case RTE_CTE:
			{
				CommonTableExpr *cte = NULL;
				KeyJoinQueryStack *cte_owner_stack = NULL;

				if (context->query_stack != NULL)
				{
					int			levelsup = (int) rte->ctelevelsup;

					/*
					 * Query-backed CTE resolution starts from this RTE
					 * reference site.  Walk rte->ctelevelsup to the Query
					 * that owns the CTE, and keep that owner frame as the CTE
					 * query's parent stack.
					 */
					for (KeyJoinQueryStack *qs = context->query_stack;
						 qs != NULL;
						 qs = qs->parent)
					{
						if (levelsup == 0)
						{
							foreach_node(CommonTableExpr, candidate,
										 qs->query->cteList)
							{
								if (strcmp(candidate->ctename,
										   rte->ctename) == 0)
								{
									cte = candidate;
									cte_owner_stack = qs;
									break;
								}
							}
							break;
						}
						levelsup--;
					}
					Assert(cte == NULL || cte_owner_stack != NULL);
				}

				/*
				 * A recursive self-reference names the recursive working
				 * table, not an ordinary CTE result surface.
				 */
				if (rte->self_reference)
					break;

				if (cte == NULL)
				{
					/*
					 * Stored projection resolves ordinary CTE references
					 * through the query stack.  If that failed, only live
					 * demand-driven projection can still resolve the CTE from a
					 * visible WITH namespace.
					 */
					Assert(context->pstate != NULL);
					for (ParseState *ps = context->pstate;
						 ps != NULL && cte == NULL;
						 ps = ps->parentParseState)
					{
						foreach_node(CommonTableExpr, candidate,
									 ps->p_ctenamespace)
						{
							if (strcmp(candidate->ctename,
									   rte->ctename) == 0)
							{
								cte = candidate;
								break;
							}
						}
					}
					cte_owner_stack = NULL;
				}

				/*
				 * Only ordinary, resolved, non-recursive CTE queries have a
				 * single query result surface we can project facts from.  A
				 * recursive CTE needs fixpoint reasoning this proof model does
				 * not attempt.
				 */
				Assert(cte != NULL);
				Assert(IsA(cte->ctequery, Query));
				if (cte->cterecursive)
					break;

				/*
				 * Stored-query replay visits CTE queries from their owning
				 * Query, and view fact projection keeps that Query on the
				 * stack, before any RTE_CTE fact projection reaches them.
				 */
				Assert(context->pstate != NULL ||
					   cte_owner_stack != NULL);

				{
					KeyJoinQueryStack qs;

					qs.parent = cte_owner_stack;
					qs.query = (Query *) cte->ctequery;
					{
						KeyJoinFactContext cte_context = *context;

						cte_context.query = (Query *) cte->ctequery;
						cte_context.query_stack = &qs;
						rte->keyJoinFacts =
							project_key_join_query_facts(&cte_context,
														 (Query *) cte->ctequery);
					}
				}
				break;
			}
		case RTE_JOIN:
			{
				Index		rtindex = rtindex_for_rte(context, rte);
				JoinExpr   *j = find_join_expr_for_rtindex(context, rtindex);
				Index		left_rtindex;
				Index		right_rtindex;
				RangeTblEntry *left_rte;
				RangeTblEntry *right_rte;
				bool		use_query = (context->pstate == NULL);

				/*
				 * Live parse analysis can find the JoinExpr through ParseState.
				 * View fact projection and stored-query revalidation have only
				 * the copied Query tree, so retry the lookup there.
				 */
				if (j == NULL)
				{
					KeyJoinFactContext query_context = *context;

					Assert(context->query != NULL);
					query_context.pstate = NULL;
					rtindex = rtindex_for_rte(&query_context, rte);
					j = find_join_expr_for_rtindex(&query_context, rtindex);
					use_query = true;
				}

				/* Without the JoinExpr, we cannot find the input surfaces. */
				Assert(j != NULL);

				/*
				 * Ordinary joins expose no key-join facts.  Only accepted key
				 * joins need the input surfaces below.
				 */
				if (j->keyJoin == NULL)
					break;

				left_rtindex = jtnode_surface_rtindex(j->larg);
				right_rtindex = jtnode_surface_rtindex(j->rarg);

				/*
				 * A transformed JoinExpr's operands must expose concrete
				 * surface RTEs.
				 */
				Assert(left_rtindex != 0);
				Assert(right_rtindex != 0);
				if (use_query)
				{
					left_rte = rt_fetch(left_rtindex, context->query->rtable);
					right_rte = rt_fetch(right_rtindex, context->query->rtable);
				}
				else
				{
					left_rte = rt_fetch(left_rtindex, context->pstate->p_rtable);
					right_rte = rt_fetch(right_rtindex, context->pstate->p_rtable);
				}

				ensure_key_join_surface_facts(context, left_rte);
				ensure_key_join_surface_facts(context, right_rte);

				compute_join_output_facts(j, left_rtindex, left_rte,
										  right_rtindex, right_rte,
										  rte);
				break;
			}
		default:
			break;
	}

	rte->keyJoinFactsComputed = true;
}

/*
 * compute_key_join_relation_facts
 *
 *		Collect catalog-backed surface facts from a base relation RTE.
 *
 *		Validated enforced NOT NULL constraints, validated nondeferrable
 *		FKs, and usable unique indexes become surface facts.
 */
static void
compute_key_join_relation_facts(KeyJoinFactContext *context,
								RangeTblEntry *rte, Relation rel)
{
	KeyJoinSurfaceFacts *set;
	TupleDesc	tupdesc;
	List	   *fkeylist;
	bool		locked_inheritance_shape = false;
	LOCKMODE	lockmode = context->for_stored_object ? ShareUpdateExclusiveLock : AccessShareLock;

	Assert(rte->rtekind == RTE_RELATION);

	/* An ONLY scan of a partitioned parent is not the partition tree. */
	if (rte->relkind == RELKIND_PARTITIONED_TABLE && !rte->inh)
		return;

	/* Other relkinds do not provide base table facts here. */
	if (rte->relkind != RELKIND_RELATION &&
		rte->relkind != RELKIND_PARTITIONED_TABLE)
		return;

	if (rel->rd_rel->relrowsecurity)
		return;

	/*
	 * An inherited scan of a table with children may include child rows, so
	 * derive facts only when the table has no children.  Take the inheritance-
	 * shape interlock before the has_subclass() check: this ShareLock conflicts
	 * with the ExclusiveLock that child attachment takes in
	 * StoreCatalogInheritance1, held to transaction end, so no child can be
	 * added between the check and the proof's commit.  It is a dedicated lock
	 * tag, not a relation lock, so it conflicts only with child attachment --
	 * not with vacuum, ANALYZE or index DDL -- and is taken on the live path
	 * too, so a live SELECT ... FOR KEY blocks a concurrent INHERIT on this
	 * parent until its transaction ends.
	 *
	 * The tag only spans one transaction, and plan-cache invalidation alone
	 * cannot keep a cached analysis sound against a later INHERIT: no lock
	 * conflict forces the invalidation to be seen before the analysis is
	 * replanned.  preprocess_relation_rtes() therefore re-checks this
	 * assumption at plan time, at the same relhassubclass read that decides
	 * child expansion, and flags the plan; plancache then discards it and
	 * re-derives the analysis, which re-runs this proof (see
	 * BuildCachedPlan).
	 */
	if (rte->relkind == RELKIND_RELATION && rte->inh)
	{
		LockKeyJoinInheritanceShape(rte->relid, ShareLock);
		if (has_subclass(rte->relid))
		{
			UnlockKeyJoinInheritanceShape(rte->relid, ShareLock);
			return;
		}
		locked_inheritance_shape = true;
	}

	set = makeNode(KeyJoinSurfaceFacts);
	tupdesc = RelationGetDescr(rel);

	/*
	 * Fact dependencies are the publication boundary: every object recorded
	 * below must already be locked for the transaction before the fact is added.
	 */

	/* Validated NOT NULL constraints feed condition 3. */
	for (int attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno - 1);
		HeapTuple	contup;
		Form_pg_constraint con;
		Oid			conoid;

		if (att->attisdropped || !att->attnotnull)
			continue;
		contup = findNotNullConstraintAttnum(rte->relid, attno);
		if (!HeapTupleIsValid(contup))
			continue;
		con = (Form_pg_constraint) GETSTRUCT(contup);
		conoid = con->oid;
		heap_freetuple(contup);

		contup = lock_and_fetch_key_join_constraint(conoid);
		con = (Form_pg_constraint) GETSTRUCT(contup);
		Assert(con->contype == CONSTRAINT_NOTNULL);
		Assert(con->conrelid == rte->relid);
		Assert(extractNotNullColumn(contup) == attno);
		Assert(con->conenforced);

		if (con->convalidated)
		{
			KeyJoinFact *fact = add_fact(set, KJF_NOT_NULL);

			fact->attnum = attno;
			fact->dependencies =
				list_make1(make_dependency(ConstraintRelationId,
											con->oid));
			fact->dependencies =
				append_dependency_unique(fact->dependencies,
										 RelationRelationId, rte->relid);
		}
		ReleaseSysCache(contup);
	}

	/* Usable unique indexes feed condition 1 plus base row coverage. */
	foreach_oid(indexoid, RelationGetIndexList(rel))
	{
		Relation	indexrel;
		Form_pg_index index;
		AttrNumber	attnums[INDEX_MAX_KEYS];
		Oid			eqtypes[INDEX_MAX_KEYS];
		int32		eqtypmods[INDEX_MAX_KEYS];
		Oid			eqoperators[INDEX_MAX_KEYS];
		List	   *deps = NIL;
		Oid			constraint;
		bool		usable = true;
		bool		keep_index_lock = false;

		indexrel = index_open(indexoid, lockmode);
		index = indexrel->rd_index;

		/*
		 * Unique proof facts must be unconditional, immediate, valid, plain
		 * column indexes.  Expressions and predicates would need additional
		 * implication proof before they could validate arbitrary key joins.
		 */
		Assert(index->indnkeyatts > 0);
		if (!index->indisunique ||
			!index->indisvalid ||
			!index->indimmediate ||
			RelationGetIndexExpressions(indexrel) != NIL ||
			RelationGetIndexPredicate(indexrel) != NIL)
		{
			index_close(indexrel, lockmode);
			continue;
		}

		constraint = get_index_constraint(indexoid);
		if (OidIsValid(constraint))
		{
			HeapTuple	contup;

			contup = lock_and_fetch_key_join_constraint(constraint);
			ReleaseSysCache(contup);
		}
		deps = list_make1(make_dependency(OidIsValid(constraint) ?
										  ConstraintRelationId :
										  RelationRelationId,
										  OidIsValid(constraint) ?
										  constraint : indexoid));
		deps = append_dependency_unique(deps, RelationRelationId,
										rte->relid);

		for (int i = 0; i < index->indnkeyatts; i++)
		{
			AttrNumber	attno = index->indkey.values[i];
			Form_pg_attribute att;
			Oid			eqtype;
			int32		eqtypmod;
			Oid			eqop;
			Oid			opcintype;

			Assert(attno > 0);
			att = TupleDescAttr(tupdesc, attno - 1);
			opcintype = indexrel->rd_opcintype[i];

			/*
			 * The index key must be the live table column under that column's
			 * collation, and the collation must make equality deterministic.
			 */
			Assert(!att->attisdropped);
			if (indexrel->rd_indcollation[i] != att->attcollation ||
				!key_join_collation_is_usable(att->attcollation))
			{
				usable = false;
				break;
			}
			eqop = get_opfamily_member_for_cmptype(indexrel->rd_opfamily[i],
												   opcintype, opcintype,
												   COMPARE_EQ);
			if (!OidIsValid(eqop) ||
				!key_join_equality_operator_is_usable(eqop, opcintype,
													  &eqtype, &deps))
			{
				usable = false;
				break;
			}
			if (!key_join_equality_identity_is_usable(att->atttypid,
													  att->atttypmod,
													  eqtype,
													  &eqtypmod))
			{
				usable = false;
				break;
			}
			attnums[i] = attno;
			eqtypes[i] = eqtype;
			eqtypmods[i] = eqtypmod;
			eqoperators[i] = eqop;
		}

		if (usable)
		{
			List	   *keyattnums =
				list_make_attrnums(attnums, index->indnkeyatts);
			List	   *keypositions =
				make_key_positions_from_attrnums(tupdesc, attnums,
												 index->indnkeyatts,
												 eqtypes, eqtypmods,
												 eqoperators);
			KeyJoinFact *ufact = add_fact(set, KJF_UNIQUE);

			ufact->keyPositions = copyObject(keypositions);
			ufact->relid = rte->relid;
			ufact->baseAttnums = list_copy(keyattnums);
			ufact->dependencies = copyObject(deps);

			add_paired_row_coverage(set, keypositions, rte->relid,
									keyattnums, copyObject(deps));
			keep_index_lock = true;
		}

		index_close(indexrel, keep_index_lock ? NoLock : lockmode);
	}

	/*
	 * Validated, catalog-enforced, nondeferrable equality FKs feed condition
	 * 2 plus base row coverage; period FKs are skipped.
	 *
	 * This intentionally trusts pg_constraint's catalog contract.  We do not
	 * inspect current or historical RI trigger enablement here; orphan rows
	 * created through privileged trigger bypass are referential-integrity
	 * corruption outside the key-join proof model, not proof facts to audit
	 * during parse analysis.
	 */
	fkeylist = copyObject(RelationGetFKeyList(rel));

	INJECTION_POINT("key-join-after-fkey-list-copy", NULL);

	foreach_node(ForeignKeyCacheInfo, fk, fkeylist)
	{
		HeapTuple	contup;
		Form_pg_constraint con;
		KeyJoinFact *fact;
		Relation	refrel;
		TupleDesc	reftupdesc;
		List	   *deps;
		int			nkeys;
		AttrNumber	conkey[INDEX_MAX_KEYS];
		AttrNumber	confkey[INDEX_MAX_KEYS];
		Oid			pf_eq_oprs[INDEX_MAX_KEYS];
		Oid			pp_eq_oprs[INDEX_MAX_KEYS];
		Oid			ff_eq_oprs[INDEX_MAX_KEYS];
		Oid			eqtypes[INDEX_MAX_KEYS];
		int32		eqtypmods[INDEX_MAX_KEYS];
		Oid			eqoperators[INDEX_MAX_KEYS];
		bool		usable = true;

		contup = lock_and_fetch_key_join_constraint(fk->conoid);
		con = (Form_pg_constraint) GETSTRUCT(contup);
		Assert(con->contype == CONSTRAINT_FOREIGN);
		Assert(con->conrelid == rte->relid);
		if (!con->conenforced ||
			!con->convalidated ||
			con->condeferrable ||
			con->conperiod)
		{
			ReleaseSysCache(contup);
			continue;
		}
		deps = list_make1(make_dependency(ConstraintRelationId, con->oid));
		deps = append_dependency_unique(deps, RelationRelationId,
										rte->relid);

		/*
		 * FKs referencing partitioned tables have child pg_constraint rows
		 * for each referenced partition.  Those rows are enforcement
		 * machinery for the parent FK; they do not prove that every
		 * referencing value is contained in that one partition's keyspace. In
		 * contrast, a referencing-side partition can inherit an FK whose
		 * referenced relation is the same as the root FK's referenced
		 * relation, and that remains a valid containment proof for the leaf
		 * relation.
		 */
		if (OidIsValid(con->conparentid))
		{
			Oid			referencedRelid = con->confrelid;
			Oid			parentid = con->conparentid;

			while (OidIsValid(parentid))
			{
				HeapTuple	parenttup;
				Form_pg_constraint parentcon;
				Oid			nextparentid;

				parenttup = lock_and_fetch_key_join_constraint(parentid);
				parentcon = (Form_pg_constraint) GETSTRUCT(parenttup);
				Assert(parentcon->contype == CONSTRAINT_FOREIGN);
				Assert(!dependency_member(deps, ConstraintRelationId,
										  parentid));
				deps = lappend(deps,
							   make_dependency(ConstraintRelationId,
											   parentid));

				nextparentid = parentcon->conparentid;
				if (!OidIsValid(nextparentid) &&
					parentcon->confrelid != referencedRelid)
					usable = false;

				ReleaseSysCache(parenttup);

				if (!usable)
					break;
				if (!OidIsValid(nextparentid))
					break;
				parentid = nextparentid;
			}

			if (!usable)
			{
				ReleaseSysCache(contup);
				continue;
			}
		}

		DeconstructFkConstraintRow(contup, &nkeys, conkey, confkey,
								   pf_eq_oprs, pp_eq_oprs, ff_eq_oprs,
								   NULL, NULL);

		/*
		 * The per-column checks below are proof-eligibility checks, not
		 * catalog invariants.  PostgreSQL can enforce valid FKs whose types,
		 * typmods, collations, or RI equality operators are not identical
		 * enough for key-join proof facts.  Such FKs remain valid; they just
		 * do not contribute facts here.
		 */
		refrel = relation_open(con->confrelid, AccessShareLock);
		reftupdesc = RelationGetDescr(refrel);
		for (int i = 0; i < nkeys; i++)
		{
			Form_pg_attribute fkatt;
			Form_pg_attribute pkatt;
			Oid			eqtype;
			int32		eqtypmod;

			Assert(conkey[i] > 0);
			Assert(confkey[i] > 0);
			fkatt = TupleDescAttr(tupdesc, conkey[i] - 1);
			pkatt = TupleDescAttr(reftupdesc, confkey[i] - 1);

			/*
			 * A live foreign-key constraint cannot name dropped columns:
			 * dependency processing would have removed the constraint, or
			 * rejected the drop, before either column could be marked dropped.
			 */
			Assert(!fkatt->attisdropped);
			Assert(!pkatt->attisdropped);
			if (fkatt->atttypid != pkatt->atttypid ||
				fkatt->atttypmod != pkatt->atttypmod ||
				fkatt->attcollation != pkatt->attcollation ||
				!key_join_collation_is_usable(fkatt->attcollation) ||
				pf_eq_oprs[i] != pp_eq_oprs[i] ||
				!key_join_equality_operator_is_usable(pf_eq_oprs[i],
													  InvalidOid,
													  &eqtype, &deps) ||
				!key_join_equality_identity_is_usable(fkatt->atttypid,
													  fkatt->atttypmod,
													  eqtype,
													  &eqtypmod))
			{
				usable = false;
				break;
			}
			Assert(pf_eq_oprs[i] == ff_eq_oprs[i]);
			eqtypes[i] = eqtype;
			eqtypmods[i] = eqtypmod;
			eqoperators[i] = pf_eq_oprs[i];
		}
		relation_close(refrel, AccessShareLock);
		if (!usable)
		{
			ReleaseSysCache(contup);
			continue;
		}

		fact = add_fact(set, KJF_FOREIGN_KEY);
		fact->keyPositions =
			make_key_positions_from_attrnums(tupdesc, conkey, nkeys,
											 eqtypes, eqtypmods,
											 eqoperators);
		fact->relid = con->conrelid;
		fact->baseAttnums = list_make_attrnums(conkey, nkeys);
		fact->referencedRelid = con->confrelid;
		fact->referencedAttnums = list_make_attrnums(confkey, nkeys);
		fact->constraint = con->oid;
		fact->dependencies = copyObject(deps);

		add_paired_row_coverage(set, copyObject(fact->keyPositions),
								fact->relid, list_copy(fact->baseAttnums),
								deps);

		ReleaseSysCache(contup);
	}
	list_free_deep(fkeylist);

	if (set->facts != NIL)
		rte->keyJoinFacts = set;
	else if (locked_inheritance_shape)
		UnlockKeyJoinInheritanceShape(rte->relid, ShareLock);
}

/*
 * rtindex_for_rte
 *
 *		Find the range-table index for an RTE pointer in the live ParseState
 *		or saved Query that owns it.
 */
static Index
rtindex_for_rte(KeyJoinFactContext *context, RangeTblEntry *rte)
{
	List	   *rtable;
	int			rtindex = 1;

	Assert(context != NULL);
	Assert(rte != NULL);

	if (context->pstate != NULL)
		rtable = context->pstate->p_rtable;
	else
	{
		Assert(context->query != NULL);
		rtable = context->query->rtable;
	}

	foreach_node(RangeTblEntry, candidate, rtable)
	{
		if (candidate == rte)
			return rtindex;
		rtindex++;
	}

	Assert(context->pstate != NULL);
	return 0;
}

/*
 * find_join_expr_for_rtindex
 *
 *		Find the JoinExpr attached to a JOIN RTE.
 */
static JoinExpr *
find_join_expr_for_rtindex(KeyJoinFactContext *context, Index rtindex)
{
	Assert(context != NULL);

	if (rtindex == 0)
		return NULL;

	if (context->pstate != NULL)
	{
		Node	   *node;

		Assert(rtindex <= list_length(context->pstate->p_joinexprs));
		node = list_nth(context->pstate->p_joinexprs, rtindex - 1);
		Assert(node != NULL);
		return castNode(JoinExpr, node);
	}

	Assert(context->query != NULL);
	return find_join_expr_in_jointree((Node *) context->query->jointree,
									  rtindex);
}

/*
 * find_join_expr_in_jointree
 *
 *		Search a saved Query jointree for the JoinExpr with rtindex.
 */
static JoinExpr *
find_join_expr_in_jointree(Node *jtnode, Index rtindex)
{
	Assert(jtnode != NULL);

	if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = castNode(JoinExpr, jtnode);
		JoinExpr   *result;

		if (j->rtindex == rtindex)
			return j;
		result = find_join_expr_in_jointree(j->larg, rtindex);
		return result ? result : find_join_expr_in_jointree(j->rarg, rtindex);
	}
	if (IsA(jtnode, FromExpr))
	{
		JoinExpr   *result = NULL;

		foreach_ptr(Node, child, castNode(FromExpr, jtnode)->fromlist)
		{
			result = find_join_expr_in_jointree(child, rtindex);

			if (result != NULL)
				break;
		}
		return result;
	}

	return NULL;
}

/*
 * project_key_join_query_facts
 *
 *		Compute surface facts exposed by a query targetlist.
 *
 *		Query shape determines which base facts survive projection and
 *		whether GROUP BY or DISTINCT introduces query-level uniqueness.
 */
static KeyJoinSurfaceFacts *
project_key_join_query_facts(KeyJoinFactContext *context, Query *query)
{
	KeyJoinSurfaceFacts *result;
	int			natts;
	int		   *srcvarno;
	int		   *srcattno;
	int			outattno = 0;
	Node	   *topjtnode = NULL;
	Index		top_rtindex = 0;
	RangeTblEntry *toprte = NULL;
	bool		block_rowcoverage;
	List	   *rowcoverage_key_position_sets = NIL;

	Assert(context != NULL);
	Assert(query != NULL);
	Assert(context->query == NULL || context->query == query);
	Assert(context->pstate != NULL ||
		   context->query_stack != NULL);

	/*
	 * Non-SELECT query trees, set operations, and SRFs do not expose a simple
	 * targetlist projection surface for facts to pass through.
	 */
	if (query->commandType != CMD_SELECT ||
		query->setOperations != NULL ||
		query->hasTargetSRFs)
		return NULL;

	/*
	 * Volatile expressions can change state that matched-filter proofs read
	 * while the executor evaluates later operands.  Treat them as a complete
	 * proof barrier for computed query facts.
	 */
	if (key_join_contains_volatile_after_planning((Node *) query))
		return NULL;

	natts = list_length(query->targetList);
	srcvarno = palloc0_array(int, natts + 1);
	srcattno = palloc0_array(int, natts + 1);

	foreach_node(TargetEntry, tle, query->targetList)
	{
		Var		   *var;

		if (tle->resjunk)
			continue;
		outattno++;
		var = direct_var_from_node((Node *) tle->expr);
		if (var)
		{
			RangeTblEntry *varrte = rt_fetch(var->varno, query->rtable);

			if (varrte->rtekind == RTE_GROUP)
			{
				Assert(var->varattno > 0);
				Assert(var->varattno <= list_length(varrte->groupexprs));
				var = direct_var_from_node((Node *)
										   list_nth(varrte->groupexprs,
													var->varattno - 1));
			}
		}
		if (var)
		{
			srcvarno[outattno] = var->varno;
			srcattno[outattno] = var->varattno;
		}
	}

	result = makeNode(KeyJoinSurfaceFacts);

	/*
	 * HAVING/LIMIT/OFFSET/FOR UPDATE can remove rows post-base, so they block
	 * row coverage but not other facts.
	 */
	block_rowcoverage = query->havingQual != NULL ||
		query->limitOffset != NULL ||
		query->limitCount != NULL ||
		query->rowMarks != NIL;

	/*
	 * GROUP BY and DISTINCT can remove rows.  A row-coverage key survives
	 * only when every row-collapsing stage groups/distincts by that key under
	 * exactly the same equality identity used by the key proof.  If both
	 * stages are present, either one could otherwise discard a needed key.
	 */
	if (query->groupClause != NIL)
	{
		KeyJoinRowCollapse *rc = palloc(sizeof(KeyJoinRowCollapse));

		rc->reason = KJI_GROUP_BY;
		if (query->groupingSets != NIL)
		{
			List	   *expanded_sets;

			/*
			 * Use the same expansion limit as aggregate parse checking.
			 * Parse analysis rejects queries that exceed it, including
			 * stored queries we later revalidate.
			 */
			expanded_sets = expand_grouping_sets(query->groupingSets,
												 query->groupDistinct, 4096);
			Assert(expanded_sets != NIL);

			rc->position_sets = NIL;
			foreach_ptr(List, sortgrouprefs, expanded_sets)
			{
				List	   *position_set = NIL;
				List	   *attnums = NIL;

				foreach_int(sortgroupref, sortgrouprefs)
				{
					SortGroupClause *sgc;
					TargetEntry *tle;
					Node	   *expr;
					List	   *deps = NIL;
					Oid			eqtype;
					int32		eqtypmod;

					sgc = get_sortgroupref_clause(sortgroupref,
												  query->groupClause);
					tle = get_sortgroupref_tle(sortgroupref,
											   query->targetList);

					Assert(tle != NULL);
					Assert(OidIsValid(sgc->eqop));
					if (tle->resjunk)
						continue;
					Assert(direct_var_from_node((Node *) tle->expr) != NULL);
					Assert(!list_member_int(attnums, tle->resno));

					expr = (Node *) tle->expr;
					if (!key_join_equality_operator_is_usable(sgc->eqop,
															  InvalidOid,
															  &eqtype, &deps))
						continue;
					if (!key_join_equality_identity_is_usable(exprType(expr),
															  exprTypmod(expr),
															  eqtype,
															  &eqtypmod))
						continue;

					attnums = lappend_int(attnums, tle->resno);
					position_set =
						lappend(position_set,
								make_key_position(list_make1_int(tle->resno),
												  exprType(expr),
												  exprTypmod(expr),
												  exprCollation(expr),
												  eqtype, eqtypmod,
												  sgc->eqop));
				}
				rc->position_sets = lappend(rc->position_sets, position_set);
			}
		}
		else
			rc->position_sets =
				list_make1(make_rowcollapse_key_positions(query,
														  query->groupClause));
		rowcoverage_key_position_sets =
			lappend(rowcoverage_key_position_sets, rc);
	}
	if (query->distinctClause != NIL)
	{
		KeyJoinRowCollapse *rc = palloc(sizeof(KeyJoinRowCollapse));

		rc->reason = KJI_DISTINCT;
		rc->position_sets =
			list_make1(make_rowcollapse_key_positions(query,
													  query->distinctClause));
		rowcoverage_key_position_sets =
			lappend(rowcoverage_key_position_sets, rc);
	}

	Assert(query->jointree != NULL);
	if (list_length(query->jointree->fromlist) == 1)
	{
		topjtnode = linitial(query->jointree->fromlist);
		top_rtindex = jtnode_surface_rtindex(topjtnode);
		Assert(top_rtindex > 0);
		toprte = rt_fetch(top_rtindex, query->rtable);
		ensure_key_join_surface_facts(context, toprte);
	}

	if (toprte != NULL && toprte->keyJoinFacts != NULL)
	{
		int			ncols = list_length(toprte->eref->colnames);
		List	  **attrmap = palloc0_array(List *, ncols + 1);

		for (int i = 1; i <= outattno; i++)
		{
			List	   *mapped_attnums;

			if (srcattno[i] <= 0)
				continue;
			mapped_attnums = map_var_to_jtnode_surface(query, topjtnode,
													   srcvarno[i], srcattno[i]);
			foreach_int(topattno, mapped_attnums)
			{
				Assert(topattno > 0);
				Assert(topattno <= ncols);
				attrmap[topattno] = lappend_int(attrmap[topattno], i);
			}
		}

		project_key_join_facts_from_rte(result, toprte, attrmap,
										query->groupingSets == NIL,
										KJI_GROUP_BY,
										query->groupingSets == NIL,
										KJI_GROUP_BY,
										!block_rowcoverage,
										query->jointree->quals,
										query, topjtnode, top_rtindex,
										rowcoverage_key_position_sets, NIL,
										KJI_ROW_REMOVING_CLAUSE);
		pfree(attrmap);
	}

	/*
	 * GROUP BY / DISTINCT also proves uniqueness of the grouped/distinct
	 * output columns.  This mirrors the planner's query_is_distinct_for()
	 * (analyzejoins.c); we re-derive it here rather than call that because this
	 * runs at parse time and, unlike a bool distinctness check, must also emit
	 * the catalog dependencies the proof relied on (for revalidation) and the
	 * per-column key identity used for cross-side matching.  These query-level
	 * facts have no base-relation relid, but still depend on the equality
	 * operators and functions used below.  GROUPING SETS, ROLLUP, and CUBE do
	 * not prove uniqueness; if SELECT DISTINCT is also present, derive
	 * uniqueness from that final row collapse.
	 */
	if (query->distinctClause != NIL ||
		(query->groupClause != NIL && query->groupingSets == NIL))
	{
		List	   *clauses = query->distinctClause != NIL ?
			query->distinctClause : query->groupClause;
		List	   *attnums = NIL;
		List	   *keypositions = NIL;
		List	   *deps = NIL;
		bool		usable = true;

		foreach_node(SortGroupClause, sgc, clauses)
		{
			TargetEntry *tle = get_sortgroupref_tle(sgc->tleSortGroupRef,
													query->targetList);

			Assert(tle != NULL);
			Assert(OidIsValid(sgc->eqop));
			if (tle->resjunk)
			{
				usable = false;
				break;
			}
			if (direct_var_from_node((Node *) tle->expr) == NULL)
			{
				usable = false;
				break;
			}
			if (!list_member_int(attnums, tle->resno))
			{
				Node	   *expr = (Node *) tle->expr;
				Oid			eqtype;
				int32		eqtypmod;

				if (!key_join_collation_is_usable(exprCollation(expr)))
				{
					usable = false;
					break;
				}
				if (!key_join_equality_operator_is_usable(sgc->eqop,
														  InvalidOid,
														  &eqtype, &deps))
				{
					usable = false;
					break;
				}
				if (!key_join_equality_identity_is_usable(exprType(expr),
														  exprTypmod(expr),
														  eqtype,
														  &eqtypmod))
				{
					usable = false;
					break;
				}
				attnums = lappend_int(attnums, tle->resno);
				keypositions =
					lappend(keypositions,
							make_key_position(list_make1_int(tle->resno),
											  exprType(expr),
											  exprTypmod(expr),
											  exprCollation(expr),
											  eqtype, eqtypmod,
											  sgc->eqop));
			}
		}

		if (usable)
		{
			KeyJoinFact *fact = add_fact(result, KJF_UNIQUE);

			Assert(attnums != NIL);
			fact->keyPositions = keypositions;
			fact->relid = InvalidOid;
			fact->baseAttnums = list_copy(attnums);
			fact->dependencies = deps;
		}
	}

	pfree(srcvarno);
	pfree(srcattno);

	if (result->facts == NIL)
		return NULL;
	return result;
}

/*
 * project_key_join_facts_from_rte
 *
 *		Project surface facts from one RTE into another surface fact set.
 *
 *		The preservation flags describe which proof meanings the caller preserved.
 *		Foreign-key containment projects whenever its key columns survive;
 *		row-coverage projection rejects lossy filter handling because filters
 *		define the referenced multiset.
 */
static void
project_key_join_facts_from_rte(KeyJoinSurfaceFacts *dst, RangeTblEntry *src,
								List **attrmap, bool preserve_notnull,
								KeyJoinInactiveReason notnull_inact_reason,
								bool preserve_unique,
								KeyJoinInactiveReason unique_inact_reason,
								bool preserve_rowcoverage,
								Node *filter_qual, Query *filter_query,
								Node *filter_jtnode, Index filter_rtindex,
								List *rowcoverage_key_position_sets,
								List *extra_unique_deps,
								KeyJoinInactiveReason inact_reason)
{
	bool		tablesample;
	int			natts;

	Assert(src != NULL);
	Assert(src->keyJoinFacts != NULL);

	tablesample = (src->rtekind == RTE_RELATION && src->tablesample != NULL);
	Assert(attrmap != NULL);
	natts = list_length(src->eref->colnames);

	foreach_node(KeyJoinFact, old, src->keyJoinFacts->facts)
	{
		KeyJoinFact *new;
		List	   *newpositions;

		/* Carry an already-inactive fact forward inert (keeps its stamp). */
		if (!old->active)
		{
			add_inactive_projected(dst, old, attrmap, KJI_NONE);
			continue;
		}

		switch (old->kind)
		{
			case KJF_NOT_NULL:
				if (!preserve_notnull)
				{
					Assert(notnull_inact_reason != KJI_NONE);
					add_inactive_projected(dst, old, attrmap,
										   notnull_inact_reason);
					continue;
				}
				Assert(old->attnum > 0 && old->attnum <= natts);
				if (attrmap[old->attnum] == NIL)
					continue;
				foreach_int(attno, attrmap[old->attnum])
				{
					new = copyObject(old);
					new->attnum = attno;
					dst->facts = lappend(dst->facts, new);
				}
				continue;

			case KJF_UNIQUE:
				if (!preserve_unique)
				{
					Assert(unique_inact_reason != KJI_NONE);
					add_inactive_projected(dst, old, attrmap,
										   unique_inact_reason);
					continue;
				}
				Assert(list_length(old->baseAttnums) ==
					   list_length(old->keyPositions));
				newpositions = project_key_positions(old->keyPositions, attrmap);
				if (newpositions == NIL)
					continue;
				new = copyObject(old);
				new->keyPositions = newpositions;
				new->dependencies =
					append_dependencies_unique(new->dependencies,
											   extra_unique_deps);
				dst->facts = lappend(dst->facts, new);
				continue;

			case KJF_ROW_COVERAGE:
				{
					bool		rowcollapse_sets_cover = true;
					KeyJoinInactiveReason rowcollapse_reason = KJI_NONE;

					if (!preserve_rowcoverage)
					{
						add_inactive_projected(dst, old, attrmap, inact_reason);
						continue;
					}
					if (tablesample)
					{
						add_inactive_projected(dst, old, attrmap,
											   KJI_ROW_REMOVING_CLAUSE);
						continue;
					}
					Assert(list_length(old->baseAttnums) ==
						   list_length(old->keyPositions));
					newpositions = project_key_positions(old->keyPositions,
														 attrmap);
					if (newpositions == NIL)
						continue;

					/*
					 * Every row-collapsing stage must group/distinct on this
					 * row-coverage key under the same equality identity.
					 * Null-containing referenced rows are outside the
					 * referenced multiset.
					 */
					foreach_ptr(KeyJoinRowCollapse, rc,
								rowcoverage_key_position_sets)
					{
						if (!rowcollapse_preserves_rowcoverage(newpositions,
															   rc))
						{
							rowcollapse_sets_cover = false;
							rowcollapse_reason = rc->reason;
							break;
						}
					}
					if (!rowcollapse_sets_cover)
					{
						add_inactive_projected(dst, old, attrmap,
											   rowcollapse_reason);
						continue;
					}

					new = copyObject(old);
					new->keyPositions = newpositions;
					/*
					 * Row coverage must account for every filter.  Dropping
					 * one would claim coverage for rows no longer visible.
					 */
					if (!add_filter_conjuncts(&new->filterConjuncts,
											  new->keyPositions, filter_qual,
											  filter_query, filter_jtnode,
											  filter_rtindex, attrmap, natts,
											  &new->dependencies, true))
					{
						add_inactive_projected(dst, old, attrmap,
											   KJI_UNACCOUNTED_FILTER);
						continue;
					}
					dst->facts = lappend(dst->facts, new);
					continue;
				}

			default:
				Assert(old->kind == KJF_FOREIGN_KEY);
				Assert(list_length(old->baseAttnums) ==
					   list_length(old->keyPositions));
				newpositions = project_key_positions(old->keyPositions, attrmap);
				if (newpositions == NIL)
					continue;
				new = copyObject(old);
				new->keyPositions = newpositions;
				/*
				 * FK-side filters are optional precision; unrecognized
				 * conjuncts only remove referencing rows.
				 */
				(void) add_filter_conjuncts(&new->filterConjuncts,
											new->keyPositions, filter_qual,
											filter_query, filter_jtnode,
											filter_rtindex, attrmap, natts,
											&new->dependencies, false);
				dst->facts = lappend(dst->facts, new);
				continue;
		}
	}
}

/*
 * project_key_positions
 *
 *		Project key positions through an attrmap.
 *
 *		Returns NIL if any key position is lost by the projection.
 */
static List *
project_key_positions(List *keyPositions, List **attrmap)
{
	List	   *result = NIL;

	foreach_node(KeyJoinKeyPosition, oldpos, keyPositions)
	{
		List	   *newattnums = NIL;

		foreach_int(attno, oldpos->attnums)
		{
			Assert(attno > 0);
			newattnums = list_concat(newattnums, list_copy(attrmap[attno]));
		}
		if (newattnums == NIL)
			return NIL;
		result = lappend(result,
						 make_key_position(newattnums, oldpos->typeOid,
										   oldpos->typmod, oldpos->collationOid,
										   oldpos->eqTypeOid,
										   oldpos->eqTypmod,
										   oldpos->eqOperator));
	}
	return result;
}

/*
 * make_rowcollapse_key_positions
 *
 *		Build key-position evidence for one row-collapsing stage.
 */
static List *
make_rowcollapse_key_positions(Query *query, List *clauses)
{
	List	   *result = NIL;
	List	   *attnums = NIL;

	foreach_node(SortGroupClause, sgc, clauses)
	{
		TargetEntry *tle = get_sortgroupref_tle(sgc->tleSortGroupRef,
												query->targetList);
		Node	   *expr;

		Assert(tle != NULL);
		Assert(OidIsValid(sgc->eqop));
		if (tle->resjunk)
			continue;
		if (direct_var_from_node((Node *) tle->expr) == NULL)
			continue;
		if (list_member_int(attnums, tle->resno))
			continue;

		expr = (Node *) tle->expr;
		{
			List	   *deps = NIL;
			Oid			eqtype;
			int32		eqtypmod;

			if (!key_join_equality_operator_is_usable(sgc->eqop,
													  InvalidOid,
													  &eqtype, &deps))
				continue;
			if (!key_join_equality_identity_is_usable(exprType(expr),
													  exprTypmod(expr),
													  eqtype,
													  &eqtypmod))
				continue;

			attnums = lappend_int(attnums, tle->resno);
			result =
				lappend(result,
						make_key_position(list_make1_int(tle->resno),
										  exprType(expr),
										  exprTypmod(expr),
										  exprCollation(expr),
										  eqtype, eqtypmod,
										  sgc->eqop));
		}
	}
	return result;
}

/*
 * add_filter_conjuncts
 *
 *		Canonicalize filter conjuncts for a projected proof fact.
 *
 *		Only direct key = value filters matching the key's equality-input
 *		identity are retained.  If reject_lossy_filter is true, failure to
 *		retain any conjunct makes the whole projection fail; otherwise such
 *		conjuncts are ignored.
 */
static bool
add_filter_conjuncts(List **dst, List *keyPositions,
					 Node *qual, Query *filter_query, Node *filter_jtnode,
					 Index filter_rtindex, List **filter_attrmap,
					 int filter_natts, List **dependencies,
					 bool reject_lossy_filter)
{
	if (qual == NULL)
		return true;
	Assert(filter_attrmap != NULL);

	foreach_ptr(Node, conjunct, make_ands_implicit((Expr *) qual))
	{
		Node	   *canon = NULL;

		Assert(conjunct != NULL);
		if (IsA(conjunct, OpExpr))
		{
			OpExpr	   *op = castNode(OpExpr, conjunct);

			if (list_length(op->args) == 2)
			{
				Node	   *left = linitial(op->args);
				Node	   *right = lsecond(op->args);
				Var		   *leftvar = direct_filter_var_from_node(left);
				int			pos = -1;

				if (leftvar != NULL &&
					!contain_vars_of_level(right, 0))
				{
					List	   *surface_attnums = NIL;

					/*
					 * Map the filter Var through the optional query,
					 * jointree, and attrmap context before matching a
					 * projected key position.
					 */
					Assert(leftvar->varlevelsup == 0);
					Assert(leftvar->varattno > 0);
					if (filter_query != NULL)
					{
						Assert(filter_jtnode != NULL);

						/*
						 * A filter attached above a join tree names an RTE
						 * below that tree.  Map it to the visible surface
						 * column(s) of the filtered jointree.
						 */
						surface_attnums =
							map_var_to_jtnode_surface(filter_query,
													  filter_jtnode,
													  leftvar->varno,
													  leftvar->varattno);
					}
					else
					{
						Assert(filter_rtindex != 0);
						if (leftvar->varno == filter_rtindex)
						{
							/*
							 * A base-relation filter already names its
							 * surface directly.
							 */
							surface_attnums =
								list_make1_int(leftvar->varattno);
						}
					}

					foreach_int(srcattno, surface_attnums)
					{
						List	   *dst_attnums;

						Assert(srcattno > 0);
						Assert(srcattno <= filter_natts);

						/*
						 * Projection may duplicate or drop source columns.
						 * Follow every surviving output attnum.
						 */
						dst_attnums = filter_attrmap[srcattno];

						foreach_int(dstattno, dst_attnums)
						{
							int			keypos =
								key_position_index_for_attnum(keyPositions,
															  dstattno);

							if (keypos < 0)
								continue;
							Assert(pos < 0 || pos == keypos);
							pos = keypos;
						}
					}
				}

				if (pos >= 0)
				{
					List	   *value_deps = NIL;

					/*
					 * The RHS must be inspected only after locking objects that
					 * could change filter proof semantics, such as functions.
					 */
					(void) collect_filter_expr_dependencies_walker(right,
																   &value_deps);
					lock_key_join_dependencies_or_error(value_deps);

					if (filter_value_allowed(right))
					{
						KeyJoinKeyPosition *keypos =
							list_nth_node(KeyJoinKeyPosition, keyPositions,
										  pos);

						Assert(leftvar->vartype == keypos->typeOid);
						Assert(leftvar->vartypmod == keypos->typmod);
						Assert(leftvar->varcollid == keypos->collationOid);
						Assert(exprType(left) == keypos->eqTypeOid);
						Assert(exprTypmod(left) == keypos->eqTypmod);
						if (filter_operator_matches_key_position(op, keypos,
																 true))
							canon = make_canonical_filter_conjunct(keypos,
																   pos,
																   op, right);
					}
				}
			}
		}

		if (canon != NULL)
		{
			List	   *lockdeps = NIL;

			/*
			 * Lock filter operators and functions before publishing them as
			 * fact dependencies.
			 */
			(void) collect_filter_expr_dependencies_walker(canon,
														   &lockdeps);
			lock_key_join_dependencies_or_error(lockdeps);
			Assert(!contain_subplans(canon));
			if (key_join_contains_volatile_after_planning(canon))
			{
				if (reject_lossy_filter)
					return false;
				continue;
			}
			Assert(dependencies != NULL);
			(void) collect_filter_expr_dependencies_walker(canon,
														   dependencies);
			if (!list_contains_equal_node(*dst, canon))
				*dst = lappend(*dst, canon);
			continue;
		}
		if (reject_lossy_filter)
			return false;
	}
	return true;
}

/*
 * jtnode_surface_rtindex
 *
 *		Return the RTE index for a jointree node surface.
 */
static Index
jtnode_surface_rtindex(Node *jtnode)
{
	Assert(jtnode != NULL);
	Assert(IsA(jtnode, RangeTblRef) || IsA(jtnode, JoinExpr));

	if (IsA(jtnode, RangeTblRef))
		return castNode(RangeTblRef, jtnode)->rtindex;
	else
		return castNode(JoinExpr, jtnode)->rtindex;
}

/*
 * map_var_to_jtnode_surface
 *
 *		Map a Var reference to column numbers on a jointree surface.
 */
static List *
map_var_to_jtnode_surface(Query *query, Node *jtnode,
						  Index varno, AttrNumber attno)
{
	Assert(jtnode != NULL);
	Assert(attno > 0);

	if (IsA(jtnode, RangeTblRef))
	{
		Index		rtindex = castNode(RangeTblRef, jtnode)->rtindex;

		return (rtindex == varno) ? list_make1_int(attno) : NIL;
	}

	{
		JoinExpr   *j = castNode(JoinExpr, jtnode);
		RangeTblEntry *joinrte = rt_fetch(j->rtindex, query->rtable);
		List	   *result = NIL;

		Assert(j->rtindex != varno);

		result = list_concat(result,
							 append_join_input_mapping(joinrte, true,
													   map_var_to_jtnode_surface(query, j->larg,
																				 varno, attno)));
		result = list_concat(result,
							 append_join_input_mapping(joinrte, false,
													   map_var_to_jtnode_surface(query, j->rarg,
																				 varno, attno)));
		return result;
	}
}

/*
 * append_join_input_mapping
 *
 *		Map input attnums from one join side to JOIN output attnums.
 */
static List *
append_join_input_mapping(RangeTblEntry *joinrte, bool leftside,
						  List *input_attnums)
{
	List	   *result = NIL;
	List	   *joincols = leftside ? joinrte->joinleftcols :
		joinrte->joinrightcols;

	foreach_int(input_attno, input_attnums)
	{
		foreach_int(joinattno, joincols)
		{
			int			input_colno = foreach_current_index(joinattno) + 1;

			if (joinattno == input_attno)
				result = list_append_unique_int(result,
												join_output_attno_for_input(joinrte,
																			leftside,
																			input_colno));
		}
	}
	return result;
}

/*
 * join_output_attno_for_input
 *
 *		Return the JOIN output attnum for one input column number.
 */
static int
join_output_attno_for_input(RangeTblEntry *joinrte, bool leftside, int input_colno)
{
	if (leftside)
		return input_colno;

	Assert(joinrte->joinmergedcols == 0);
	return list_length(joinrte->joinleftcols) +
		input_colno;
}

/*
 * direct_var_from_node
 *
 *		Return a direct current-query Var, rejecting parser coercion wrappers.
 */
static Var *
direct_var_from_node(Node *node)
{
	Assert(node != NULL);

	if (IsA(node, Var))
	{
		Var		   *var = castNode(Var, node);

		return (var->varattno > 0 && var->varlevelsup == 0) ? var : NULL;
	}
	if (IsA(node, RelabelType))
	{
		RelabelType *relabel = castNode(RelabelType, node);
		Node	   *arg PG_USED_FOR_ASSERTS_ONLY = (Node *) relabel->arg;

		Assert(arg != NULL);
		/*
		 * The parser constructors that can reach key-join proof either return
		 * the original node for no-op coercions or generate RelabelTypes that
		 * change type, typmod, or collation.  An identity RelabelType here
		 * would be a parse-tree invariant violation, not a testable SQL
		 * shape.
		 */
		Assert(relabel->resulttype != exprType(arg) ||
			   relabel->resulttypmod != exprTypmod(arg) ||
			   relabel->resultcollid != exprCollation(arg));
		return NULL;
	}
	return NULL;
}

/*
 * direct_filter_var_from_node
 *
 *		Return a direct current-query Var from a key-filter operand.  Domain
 *		equality operators are resolved on the base type, so the parser may
 *		wrap a domain Var in a RelabelType before filter canonicalization sees
 *		the OpExpr.  Other RelabelType shapes are not direct key filters.
 */
static Var *
direct_filter_var_from_node(Node *node)
{
	Var		   *var = direct_var_from_node(node);

	if (var != NULL)
		return var;
	Assert(node != NULL);
	if (!IsA(node, RelabelType))
		return NULL;

	{
		RelabelType *relabel = castNode(RelabelType, node);
		int32		baseTypmod;
		Oid			baseType;

		var = direct_var_from_node((Node *) relabel->arg);
		if (var == NULL)
			return NULL;

		baseTypmod = var->vartypmod;
		baseType = key_join_equality_type(var->vartype, var->vartypmod,
										  &baseTypmod);
		Assert(relabel->resultcollid == var->varcollid);
		if (relabel->resulttype == baseType &&
			relabel->resulttypmod == baseTypmod)
			return var;
		if (baseType == VARCHAROID &&
			relabel->resulttype == TEXTOID)
		{
			Assert(relabel->resulttypmod == -1);
			Assert(IsBinaryCoercible(var->vartype, relabel->resulttype));
			return var;
		}
		return NULL;
	}
}

/*
 * make_filter_param
 *
 *		Build the placeholder Param for a key-join proof filter.
 *
 *		These PARAM_KEYJOIN nodes are parser-private placeholders that may
 *		appear only inside transient KeyJoinSurfaceFacts.filterConjuncts.
 *		Accepted KeyJoinNodes carry only the dependencies consumed by the proof,
 *		never these proof filter expressions.
 */
static Node *
make_filter_param(KeyJoinKeyPosition *keypos, int pos)
{
	Param	   *param = makeNode(Param);

	param->paramkind = PARAM_KEYJOIN;
	param->paramid = pos + 1;
	param->paramtype = keypos->eqTypeOid;
	param->paramtypmod = keypos->eqTypmod;
	param->paramcollid = keypos->collationOid;
	param->location = -1;
	return (Node *) param;
}

/*
 * collect_filter_expr_dependencies_walker
 *
 *		Walker callback for collecting filter expression dependencies.
 */
static bool
collect_filter_expr_dependencies_walker(Node *node, void *context_arg)
{
	List	  **dependencies = (List **) context_arg;

	if (node == NULL)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = castNode(OpExpr, node);

		set_opfuncid(expr);
		*dependencies = add_op_function_deps(*dependencies, expr->opno,
											 expr->opfuncid);
	}
	else
		(void) check_functions_in_node(node, filter_function_dependency_checker,
									   context_arg);

	return expression_tree_walker(node, collect_filter_expr_dependencies_walker,
								  context_arg);
}

/*
 * filter_function_dependency_checker
 *
 *		check_functions_in_node callback for collecting procedure dependencies.
 */
static bool
filter_function_dependency_checker(Oid func_id, void *context_arg)
{
	List	  **dependencies = (List **) context_arg;

	*dependencies = append_dependency_unique(*dependencies, ProcedureRelationId,
											 func_id);
	return false;
}

/*
 * add_op_function_deps
 *
 *		Append an operator OID and its underlying function as dependencies.
 */
static List *
add_op_function_deps(List *deps, Oid opno, Oid opfuncid)
{
	Assert(OidIsValid(opno));
	Assert(OidIsValid(opfuncid));

	deps = append_dependency_unique(deps, OperatorRelationId, opno);
	return append_dependency_unique(deps, ProcedureRelationId, opfuncid);
}

/*
 * compute_join_output_facts
 *
 *		Project still-valid surface facts into a join result RTE.
 *
 *		Join type, key-join proof, referencing-side uniqueness, and filters
 *		decide which facts survive.
 */
static void
compute_join_output_facts(JoinExpr *j,
						  Index left_rtindex, RangeTblEntry *left_rte,
						  Index right_rtindex, RangeTblEntry *right_rte,
						  RangeTblEntry *joinrte)
{
	KeyJoinSurfaceFacts *result;
	List	  **lmap;
	List	  **rmap;
	KeyJoinNode *key_join_node;
	bool		referencing_left;
	bool		referenced_left;
	RangeTblEntry *referencing_rte;
	RangeTblEntry *referenced_rte;
	List	  **referencing_map;
	List	  **referenced_map;
	Index		referencing_rtindex;
	Index		referenced_rtindex;
	bool		referenced_preserved;
	bool		preserve_referencing_notnull;
	bool		preserve_referenced_notnull;
	bool		referencing_unique = false;
	List	   *referencing_unique_deps = NIL;

	Assert(left_rte != NULL);
	Assert(right_rte != NULL);
	Assert(joinrte->rtekind == RTE_JOIN);
	Assert(j->keyJoin != NULL);

	joinrte->keyJoinFacts = NULL;
	result = makeNode(KeyJoinSurfaceFacts);

	lmap = build_join_attrmap(joinrte, true,
							  list_length(left_rte->eref->colnames));
	rmap = build_join_attrmap(joinrte, false,
							  list_length(right_rte->eref->colnames));

	/* Accepted key joins can export facts for later key-join proofs. */
	key_join_node = castNode(KeyJoinNode, j->keyJoin);
	referencing_left = (key_join_node->referencingVarno == left_rtindex);
	referenced_left = (key_join_node->referencedVarno == left_rtindex);
	referencing_rte = referencing_left ? left_rte : right_rte;
	referenced_rte = referenced_left ? left_rte : right_rte;
	referencing_map = referencing_left ? lmap : rmap;
	referenced_map = referenced_left ? lmap : rmap;
	referencing_rtindex = referencing_left ? left_rtindex : right_rtindex;
	referenced_rtindex = referenced_left ? left_rtindex : right_rtindex;
	referenced_preserved = join_preserves_side(j->jointype, referenced_left);
	preserve_referencing_notnull =
		!join_null_extends_side(j->jointype, referencing_left);
	preserve_referenced_notnull =
		!join_null_extends_side(j->jointype, referenced_left);

	Assert(key_join_node->referencingVarno == left_rtindex ||
		   key_join_node->referencingVarno == right_rtindex);
	Assert(key_join_node->referencedVarno == left_rtindex ||
		   key_join_node->referencedVarno == right_rtindex);
	Assert(key_join_node->referencingVarno != key_join_node->referencedVarno);

	Assert(referencing_rte->keyJoinFactsComputed);
	Assert(referenced_rte->keyJoinFactsComputed);
	Assert(referencing_rte->keyJoinFacts != NULL);
	Assert(referenced_rte->keyJoinFacts != NULL);

	/*
	 * Output-fact maintenance: detect referencing-side uniqueness
	 * compatible with the accepted FK join predicate.
	 */
	{
		KeyJoinSurfaceFacts *set = referencing_rte->keyJoinFacts;

		foreach_node(KeyJoinFact, fkfact, set->facts)
		{
			List	   *fk_key_positions;

			if (fkfact->kind != KJF_FOREIGN_KEY)
				continue;
			Assert(fkfact->active);
			if (fkfact->constraint != key_join_node->constraint)
				continue;
			if (!select_key_position_parts(key_join_node->referencingAttnums,
										   fkfact->keyPositions,
										   NIL, NULL, &fk_key_positions))
				continue;
			foreach_node(KeyJoinFact, fact, set->facts)
			{
				bool		unique_matches = true;

				if (fact->kind != KJF_UNIQUE)
					continue;
				if (!fact->active)
					continue;

				Assert(list_length(key_join_node->referencingAttnums) ==
					   list_length(fk_key_positions));

				/*
				 * A referencing-side unique fact proves at-most-one match
				 * only if it covers each referencing join column with the
				 * same key identity as the FK positions selected for the
				 * accepted join predicate.
				 */
				foreach_node(KeyJoinKeyPosition, keypos, fact->keyPositions)
				{
					ListCell   *lcattno;
					ListCell   *lcfkpos;
					bool		found = false;

					forboth(lcattno, key_join_node->referencingAttnums,
							lcfkpos, fk_key_positions)
					{
						KeyJoinKeyPosition *fkpos =
							lfirst_node(KeyJoinKeyPosition, lcfkpos);

						if (!list_member_int(keypos->attnums,
											 lfirst_int(lcattno)))
							continue;
						if (!key_position_identity_equal(keypos, fkpos))
						{
							unique_matches = false;
							break;
						}
						found = true;
						break;
					}
					if (!found)
						unique_matches = false;
					if (!unique_matches)
						break;
				}

				if (unique_matches)
				{
					referencing_unique = true;
					referencing_unique_deps =
						append_dependencies_unique(referencing_unique_deps,
												   fact->dependencies);
					break;
				}
			}
			if (referencing_unique)
				break;
		}
	}

	/*
	 * Project both input surfaces through the join output.  Null
	 * extension can kill not-null facts; FK containment survives as
	 * nullable containment, with not-null facts carrying condition 3.
	 */
	project_key_join_facts_from_rte(result, referencing_rte, referencing_map,
									preserve_referencing_notnull,
									KJI_NULL_EXTENDING_JOIN,
									true, KJI_NONE, true,
									join_filter_for_side(j->jointype,
														 referencing_left,
														 j->joinFilter),
									NULL, NULL, referencing_rtindex, NIL, NIL,
									KJI_NONE);
	project_key_join_facts_from_rte(result, referenced_rte, referenced_map,
									preserve_referenced_notnull,
									KJI_NULL_EXTENDING_JOIN,
									referencing_unique,
									KJI_JOIN_FANOUT,
									referenced_preserved,
									join_filter_for_side(j->jointype,
														 referenced_left,
														 j->joinFilter),
									NULL, NULL, referenced_rtindex, NIL,
									referencing_unique_deps,
									KJI_JOIN_NOT_PRESERVED);

	/*
	 * Filter propagation: only safe when referenced side is fully
	 * matched.  Filters may move only onto facts rooted in the FK's
	 * referenced relation; FK output facts additionally match their own
	 * constraint OID below.
	 */
	if (!referenced_preserved)
	{
		KeyJoinSurfaceFacts *rfacts = referencing_rte->keyJoinFacts;
		KeyJoinSurfaceFacts *pfacts = referenced_rte->keyJoinFacts;

		foreach_node(KeyJoinFact, source, rfacts->facts)
		{
			List	   *source_selected_base;
			List	   *target_selected_base = NIL;

			if (source->kind != KJF_FOREIGN_KEY)
				continue;
			Assert(source->active);
			if (source->constraint != key_join_node->constraint ||
				source->filterConjuncts == NIL)
				continue;
			Assert(list_length(source->baseAttnums) ==
				   list_length(source->referencedAttnums));
			if (!select_key_position_parts(key_join_node->referencingAttnums,
										   source->keyPositions,
										   source->baseAttnums,
										   &source_selected_base, NULL))
				continue;
			foreach_int(srcbase, source_selected_base)
			{
				ListCell   *lcbase;
				ListCell   *lcref;

				forboth(lcbase, source->baseAttnums,
						lcref, source->referencedAttnums)
				{
					if (lfirst_int(lcbase) == srcbase)
					{
						target_selected_base =
							lappend_int(target_selected_base,
										lfirst_int(lcref));
						break;
					}
				}
			}

			Assert(list_length(target_selected_base) == list_length(source_selected_base));

			/*
			 * Match FK and RowCoverage targets to out-facts; the FK case
			 * also requires constraint match so canonical filters can't
			 * cross distinct FKs.
			 */
			foreach_node(KeyJoinFact, target, pfacts->facts)
			{
				List	   *projected_positions;
				List	   *position_map;

				if (target->kind != KJF_FOREIGN_KEY &&
					target->kind != KJF_ROW_COVERAGE)
					continue;
				if (!target->active)
					continue;
				if (target->relid != source->referencedRelid)
					continue;
				projected_positions =
					project_key_positions(target->keyPositions,
										  referenced_map);
				Assert(projected_positions != NIL);
				position_map =
					make_filter_position_map(source->baseAttnums,
											 source_selected_base,
											 target->baseAttnums,
											 target_selected_base);

				foreach_node(KeyJoinFact, out, result->facts)
				{
					if (out->kind != target->kind)
						continue;
					if (!out->active)
						continue;
					if (out->kind == KJF_FOREIGN_KEY)
					{
						if (out->constraint != target->constraint)
							continue;
					}
					if (out->relid != target->relid)
						continue;
					if (!int_lists_same_members(out->baseAttnums,
												target->baseAttnums))
						continue;
					if (!equal(out->keyPositions, projected_positions))
						continue;

					/*
					 * Remap each canonical FK-side filter onto the output
					 * fact.  Keep only filters that still constrain the
					 * output key positions after projection.
					 */
					foreach_ptr(Node, conjunct, source->filterConjuncts)
					{
						Node	   *remapped;

						if (!filter_conjunct_can_remap(conjunct,
													   position_map))
							continue;
						remapped = remap_filter_conjunct(conjunct,
														 position_map);
						if (!filter_conjunct_matches_key_positions(remapped,
																   out->keyPositions))
							continue;

						(void) collect_filter_expr_dependencies_walker(remapped,
																	   &out->dependencies);
						if (!list_contains_equal_node(out->filterConjuncts,
													  remapped))
							out->filterConjuncts =
								lappend(out->filterConjuncts, remapped);
					}
				}
			}
		}
	}
	Assert(result->facts != NIL);
	joinrte->keyJoinFacts = result;

	pfree(lmap);
	pfree(rmap);
}

/*
 * build_join_attrmap
 *
 *		Build a mapping from one join input to JOIN output columns.
 */
static List **
build_join_attrmap(RangeTblEntry *joinrte, bool leftside, int nattrs)
{
	List	  **attrmap = palloc0_array(List *, nattrs + 1);
	List	   *joincols = leftside ? joinrte->joinleftcols :
		joinrte->joinrightcols;

	foreach_int(input_attno, joincols)
	{
		int			jcolno =
			join_output_attno_for_input(joinrte, leftside,
										foreach_current_index(input_attno) + 1);

		Assert(input_attno > 0);
		Assert(input_attno <= nattrs);
		attrmap[input_attno] = lappend_int(attrmap[input_attno], jcolno);
	}
	return attrmap;
}

/*
 * join_null_extends_side
 *
 *		Return true if the join type can null-extend the requested side.
 */
static bool
join_null_extends_side(JoinType jointype, bool leftside)
{
	return jointype == JOIN_FULL ||
		(leftside ? jointype == JOIN_RIGHT : jointype == JOIN_LEFT);
}

/*
 * join_filter_for_side
 *
 *		Return the join filter applicable to facts projected from one side.
 */
static Node *
join_filter_for_side(JoinType jointype, bool leftside, Node *filter)
{
	Assert(jointype == JOIN_INNER || jointype == JOIN_LEFT ||
		   jointype == JOIN_RIGHT || jointype == JOIN_FULL);

	if (jointype == JOIN_INNER)
		return filter;
	if (jointype == JOIN_LEFT)
		return leftside ? NULL : filter;
	if (jointype == JOIN_RIGHT)
		return leftside ? filter : NULL;
	return NULL;
}

/*
 * storedNodeContainsKeyJoin
 *
 *		Report whether a stored query or expression tree contains a
 *		KeyJoinNode.
 */
bool
storedNodeContainsKeyJoin(Node *node)
{
	return stored_node_contains_key_join_walker(node, NULL);
}

/*
 * stored_node_contains_key_join_walker
 *
 *		Walk stored query and expression nodes looking for KeyJoinNodes.
 */
static bool
stored_node_contains_key_join_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, KeyJoinNode))
		return true;

	if (IsA(node, JoinExpr))
	{
		JoinExpr   *join = castNode(JoinExpr, node);

		if (stored_node_contains_key_join_walker(join->keyJoin, context))
			return true;
	}

	if (IsA(node, Query))
		return query_tree_walker(castNode(Query, node),
								 stored_node_contains_key_join_walker,
								 context, 0);

	return expression_tree_walker(node, stored_node_contains_key_join_walker,
								  context);
}

/*
 * keyJoinExecutableFormUnchanged
 *
 *		Return true if a revalidated copy of a stored key-join tree has the same
 *		executable form as what is stored: the re-derivation changed no executed
 *		expression (the key-equality operators / rebuilt quals) and no structural
 *		proof field.  If it differs, the stored tree would execute incorrectly and
 *		the triggering operation must be rejected.
 *
 *		The proof's catalog evidence (constraint, notNullConstraints,
 *		proofDependencies) is deliberately ignored: those fields are equal_ignore,
 *		and pg_depend -- not the stored tree -- is the authoritative record.  The
 *		caller reconciles pg_depend to the re-derived evidence, so the proof may
 *		legitimately re-pin to equivalent catalog objects without rewriting the
 *		stored tree.
 */
bool
keyJoinExecutableFormUnchanged(Node *stored, Node *revalidated)
{
	return equal(stored, revalidated);
}

/*
 * revalidateStoredKeyJoinProofsInNode
 *
 *		Revalidate and rebuild key-join proofs contained in a copied stored
 *		query or expression tree.
 */
void
revalidateStoredKeyJoinProofsInNode(Node *node)
{
	(void) revalidate_stored_key_join_node_walker(node, NULL);
}

/*
 * revalidate_stored_key_join_node_walker
 *
 *		Walk a stored query or expression tree and revalidate any Query nodes
 *		with the supplied owning-query stack, if any.
 */
static bool
revalidate_stored_key_join_node_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		revalidate_stored_key_join_proofs_in_query(castNode(Query, node),
												   (KeyJoinQueryStack *) context);
		return false;
	}

	return expression_tree_walker(node, revalidate_stored_key_join_node_walker,
								  context);
}

/*
 * revalidate_stored_key_join_proofs_in_query
 *
 *		Revalidate one stored Query with an explicit stack of owning query
 *		levels.
 *
 *		Stored key-join proofs may depend on CTE ownership, outer references,
 *		and facts cached on RTEs.  This routine rebuilds that context for one
 *		Query level, clears cached facts, recursively revalidates CTEs
 *		and FROM-subqueries with the current Query as their parent frame, then
 *		revalidates key joins in the jointree.  The final expression walker
 *		handles expression subqueries and intentionally skips FROM/CTE
 *		subqueries already handled in owner-aware passes above.
 *
 *		Demand-driven RTE projection depends on these owner-aware passes and
 *		must not call back here for CTE or FROM-subquery bodies.
 */
static void
revalidate_stored_key_join_proofs_in_query(Query *query,
										   KeyJoinQueryStack *parent_stack)
{
	KeyJoinQueryStack qs;

	Assert(query != NULL);

	qs.parent = parent_stack;
	qs.query = query;

	/*
	 * Stored key-join proofs are re-derived from scratch below, so first reset
	 * any surface facts cached on each RTE.  The input may be a catalog-reloaded
	 * stored tree (already fact-clean, since key-join evidence is
	 * read_write_ignore and never serialized) or a freshly parsed expression
	 * that still carries the parser's scratch facts -- e.g. ALTER POLICY
	 * re-supplying a USING/WITH CHECK clause.  Both are normalized to the
	 * one-shot replay state here.
	 */
	foreach_node(RangeTblEntry, rte, query->rtable)
	{
		rte->keyJoinFacts = NULL;
		rte->keyJoinFactsComputed = false;
	}

	foreach_node(CommonTableExpr, cte, query->cteList)
	{
		revalidate_stored_key_join_proofs_in_query(castNode(Query, cte->ctequery),
												   &qs);
	}

	foreach_node(RangeTblEntry, rte, query->rtable)
	{
		if (rte->rtekind == RTE_SUBQUERY)
		{
			Assert(rte->subquery != NULL);
			revalidate_stored_key_join_proofs_in_query(rte->subquery, &qs);
		}
	}

	revalidate_query_jointree_proofs(query, (Node *) query->jointree, &qs);

	(void) query_tree_walker(query, revalidate_stored_key_join_node_walker,
							 &qs,
							 QTW_IGNORE_RT_SUBQUERIES |
							 QTW_IGNORE_CTE_SUBQUERIES);
}

/*
 * revalidate_query_jointree_proofs
 *
 *		Recompute join facts and revalidate the proofs of stored KeyJoinNodes.
 */
static void
revalidate_query_jointree_proofs(Query *query, Node *jtnode,
								 KeyJoinQueryStack *query_stack)
{
	Assert(query != NULL);
	Assert(jtnode != NULL);
	Assert(query_stack != NULL);
	Assert(query_stack->query == query);

	if (IsA(jtnode, FromExpr))
	{
		foreach_ptr(Node, child, castNode(FromExpr, jtnode)->fromlist)
			revalidate_query_jointree_proofs(query, child, query_stack);
		return;
	}
	if (!IsA(jtnode, JoinExpr))
		return;

	{
		JoinExpr   *j = castNode(JoinExpr, jtnode);
		Index		left_rtindex = jtnode_surface_rtindex(j->larg);
		Index		right_rtindex = jtnode_surface_rtindex(j->rarg);
		RangeTblEntry *left_rte;
		RangeTblEntry *right_rte;

		revalidate_query_jointree_proofs(query, j->larg, query_stack);
		revalidate_query_jointree_proofs(query, j->rarg, query_stack);

		Assert(left_rtindex != 0);
		Assert(right_rtindex != 0);
		Assert(j->rtindex != 0);

		left_rte = rt_fetch(left_rtindex, query->rtable);
		right_rte = rt_fetch(right_rtindex, query->rtable);

		if (j->keyJoin != NULL)
		{
			KeyJoinNode *key_join;
			bool		referencing_left;
			RangeTblEntry *referencing_rte;
			RangeTblEntry *referenced_rte;
			List	   *referenced_args;
			List	   *referencing_args;
			List	   *locations;
			Node	   *key_quals = NULL;
			int			nkeys;
			KeyJoinMatch match;
			KeyJoinFactContext context = {0};

			key_join = castNode(KeyJoinNode, j->keyJoin);
			referencing_left = (key_join->referencingVarno == left_rtindex);
			referencing_rte = referencing_left ? left_rte : right_rte;
			referenced_rte =
				(key_join->referencedVarno == left_rtindex) ? left_rte : right_rte;
			nkeys = list_length(key_join->referencingAttnums);

			Assert(key_join->referencingVarno == left_rtindex ||
				   key_join->referencingVarno == right_rtindex);
			Assert(key_join->referencedVarno == left_rtindex ||
				   key_join->referencedVarno == right_rtindex);
			Assert(key_join->referencingVarno != key_join->referencedVarno);

			context.query = query;
			context.query_stack = query_stack;
			context.for_stored_object = true;
			ensure_key_join_surface_facts(&context, referencing_rte);
			ensure_key_join_surface_facts(&context, referenced_rte);

			if (!find_key_join_match(referencing_rte, referenced_rte,
									 key_join->referencingAttnums,
									 key_join->referencedAttnums,
									 !join_preserves_side(j->jointype,
														  referencing_left),
									 &match))
			{
				bool		local_is_referencing =
					(key_join->direction == KEY_JOIN_RIGHT_ARROW);
				RangeTblEntry *ref_alias_rte =
					rt_fetch(key_join->refAliasVarno, query->rtable);
				KeyJoinFailureSide referencing = {0};
				KeyJoinFailureSide referenced = {0};

				referencing.rte =
					local_is_referencing ? right_rte : ref_alias_rte;
				referencing.attnums =
					local_is_referencing ? key_join->referencingAttnums :
					key_join->refAliasAttnums;
				referenced.rte =
					local_is_referencing ? ref_alias_rte : right_rte;
				referenced.attnums =
					local_is_referencing ? key_join->refAliasAttnums :
					key_join->referencedAttnums;
				key_join_report_failure(NULL, -1, j->jointype, &match,
										&referencing, &referenced);
			}

			/*
			 * Join-local FILTER quals are kept separately on joinFilter and
			 * are re-merged after the key equality is rebuilt.  Stored
			 * non-filtered joins keep the key equality directly in quals;
			 * filtered joins store quals as key equality AND joinFilter.
			 */
			if (j->joinFilter == NULL)
				key_quals = j->quals;
			else
			{
				BoolExpr   *andexpr;

				Assert(j->quals != NULL);
				andexpr = castNode(BoolExpr, j->quals);
				Assert(andexpr->boolop == AND_EXPR);
				Assert(list_length(andexpr->args) == 2);
				Assert(equal(lsecond(andexpr->args), j->joinFilter));

				key_quals = linitial(andexpr->args);
			}
			Assert(key_quals != NULL);

			referenced_args = NIL;
			referencing_args = NIL;
			locations = NIL;

			/*
			 * The parser stores a single key equality as the OpExpr itself.
			 * Multi-column key joins are stored as an AND whose arguments are
			 * the key equalities in key-column order.
			 */
			if (nkeys == 1)
				extract_key_join_qual_arg(key_quals, &referenced_args,
										  &referencing_args, &locations);
			else
			{
				BoolExpr   *andexpr;

				andexpr = castNode(BoolExpr, key_quals);
				Assert(andexpr->boolop == AND_EXPR);
				Assert(list_length(andexpr->args) == nkeys);

				foreach_ptr(Node, qual, andexpr->args)
					extract_key_join_qual_arg(qual, &referenced_args,
											  &referencing_args, &locations);
			}

			/*
			 * Rebuild the executable equality quals from the stored argument
			 * expressions and the freshly proven equality operators.  The
			 * stored operators might no longer be the proof operators after
			 * DDL, but the argument expressions preserve locations and parse
			 * structure.
			 */
			key_quals = build_key_join_quals(referenced_args,
											 referencing_args,
											 match.eqoperators,
											 match.eqtypes,
											 match.eqtypmods,
											 locations, -1);
			j->quals = key_quals;
			if (j->joinFilter != NULL)
				j->quals = (Node *) makeBoolExpr(AND_EXPR,
												 list_make2(j->quals,
															copyObject(j->joinFilter)),
												 -1);

			key_join->constraint = match.constraint;
			key_join->notNullConstraints = match.notnulldeps;
			key_join->proofDependencies = match.proofdeps;
		}
	}
}

/*
 * extract_key_join_qual_arg
 *
 *		Extract one stored key equality.  The parser constructs key equality
 *		operators as referenced-column argument first, referencing-column
 *		argument second.
 */
static void
extract_key_join_qual_arg(Node *qual, List **referenced_args,
						  List **referencing_args, List **locations)
{
	OpExpr	   *op;

	Assert(qual != NULL);
	op = castNode(OpExpr, qual);
	Assert(list_length(op->args) == 2);

	*referenced_args = lappend(*referenced_args,
							   copyObject(linitial(op->args)));
	*referencing_args = lappend(*referencing_args,
								copyObject(lsecond(op->args)));
	*locations = lappend_int(*locations, op->location);
}

static KeyJoinFact *
add_fact(KeyJoinSurfaceFacts *set, KeyJoinFactKind kind)
{
	KeyJoinFact *fact = makeNode(KeyJoinFact);

	fact->kind = kind;
	fact->active = true;
	fact->inactiveReason = KJI_NONE;
	fact->inactiveOriginView = InvalidOid;
	set->facts = lappend(set->facts, fact);
	return fact;
}

/*
 * add_paired_row_coverage
 *
 *		Append a row-coverage fact paired with the current unique or FK fact.
 */
static void
add_paired_row_coverage(KeyJoinSurfaceFacts *set, List *keypositions,
						Oid relid, List *baseAttnums, List *deps)
{
	KeyJoinFact *cov = add_fact(set, KJF_ROW_COVERAGE);

	cov->keyPositions = keypositions;
	cov->relid = relid;
	cov->baseAttnums = baseAttnums;
	cov->dependencies = deps;
}

/*
 * key_join_collation_is_usable
 *
 *		Return true for noncollatable keys or deterministic collations.
 */
static bool
key_join_collation_is_usable(Oid collationOid)
{
	if (!OidIsValid(collationOid))
		return true;
	return get_collation_isdeterministic(collationOid);
}

/*
 * key_join_equality_identity_is_usable
 *
 *		Compute the typmod to use while executing equality against eqTypeOid.
 *
 *		The exposed proof identity remains exact, but some opclasses execute
 *		equality through a binary-compatible input type.  In core this matters
 *		for varchar_ops, whose btree opclass uses text equality.
 */
static bool
key_join_equality_identity_is_usable(Oid typeOid, int32 typmod,
									 Oid eqTypeOid, int32 *eqTypmod)
{
	int32		baseTypmod = typmod;
	Oid			baseType;

	Assert(eqTypmod != NULL);

	baseType = key_join_equality_type(typeOid, typmod, &baseTypmod);
	if (eqTypeOid == baseType)
	{
		*eqTypmod = baseTypmod;
		return true;
	}

	if (baseType == VARCHAROID &&
		eqTypeOid == TEXTOID)
	{
		Assert(IsBinaryCoercible(typeOid, eqTypeOid));
		*eqTypmod = -1;
		return true;
	}

	return false;
}

/*
 * key_join_equality_operator_is_usable
 *
 *		Check whether an operator is usable as key-join equality.
 *
 *		The caller must provide a valid operator OID.  The operator must be
 *		immutable, strict, boolean, non-set-returning, and accept exactly one
 *		equality input type.  If expectedTypeOid is valid, the operator must
 *		accept that exact type.
 */
static bool
key_join_equality_operator_is_usable(Oid opno, Oid expectedTypeOid,
									 Oid *eqTypeOid, List **dependencies)
{
	RegProcedure funcid;
	HeapTuple	optup;
	HeapTuple	proctup;
	Form_pg_operator operform;
	Form_pg_proc procform;
	Oid			lefttype;

	Assert(OidIsValid(opno));

	optup = lock_and_fetch_key_join_operator(opno);
	operform = (Form_pg_operator) GETSTRUCT(optup);
	lefttype = operform->oprleft;
	funcid = operform->oprcode;

	INJECTION_POINT("key-join-after-equality-operator-lock", NULL);

	Assert(OidIsValid(lefttype));
	Assert(lefttype == operform->oprright);
	Assert(!OidIsValid(expectedTypeOid) || lefttype == expectedTypeOid);
	Assert(operform->oprresult == BOOLOID);

	Assert(RegProcedureIsValid(funcid));

	proctup = lock_and_fetch_key_join_proc((Oid) funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);
	if (procform->proretset ||
		procform->provolatile != PROVOLATILE_IMMUTABLE ||
		!procform->proisstrict)
	{
		ReleaseSysCache(proctup);
		ReleaseSysCache(optup);
		return false;
	}

	ReleaseSysCache(proctup);
	ReleaseSysCache(optup);

	Assert(dependencies != NULL);
	Assert(eqTypeOid != NULL);
	*dependencies = add_op_function_deps(*dependencies, opno, (Oid) funcid);
	*eqTypeOid = lefttype;
	return true;
}

/*
 * key_join_equality_type
 *
 *		Return the type identity used by equality operators for a key value.
 *		Domains remain the exposed proof identity, but their equality
 *		operators are resolved and executed on the base type.
 */
static Oid
key_join_equality_type(Oid typeOid, int32 typmod, int32 *eqTypmod)
{
	int32		localTypmod = typmod;
	Oid			result;

	result = getBaseTypeAndTypmod(typeOid, &localTypmod);
	Assert(eqTypmod != NULL);
	*eqTypmod = localTypmod;
	return result;
}

/*
 * make_key_positions_from_attrnums
 *
 *		Build key-position descriptors for relation attribute numbers.
 */
static List *
make_key_positions_from_attrnums(const TupleDesc tupdesc, const AttrNumber *attnums,
								 int nattnums, const Oid *eqTypes,
								 const int32 *eqTypmods,
								 const Oid *eqOperators)
{
	List	   *result = NIL;

	for (int i = 0; i < nattnums; i++)
	{
		Form_pg_attribute att;

		Assert(attnums[i] > 0);
		att = TupleDescAttr(tupdesc, attnums[i] - 1);
		result = lappend(result,
						 make_key_position(list_make1_int(att->attnum),
										   att->atttypid, att->atttypmod,
										   att->attcollation,
										   eqTypes[i], eqTypmods[i],
										   eqOperators[i]));
	}
	return result;
}

/*
 * make_key_position
 *
 *		Build one KeyJoinKeyPosition node.
 */
static KeyJoinKeyPosition *
make_key_position(List *attnums, Oid typeOid, int32 typmod,
				  Oid collationOid, Oid eqTypeOid, int32 eqTypmod,
				  Oid eqOperator)
{
	KeyJoinKeyPosition *pos = makeNode(KeyJoinKeyPosition);

	pos->attnums = list_copy(attnums);
	pos->typeOid = typeOid;
	pos->typmod = typmod;
	pos->collationOid = collationOid;
	pos->eqTypeOid = eqTypeOid;
	pos->eqTypmod = eqTypmod;
	pos->eqOperator = eqOperator;
	return pos;
}

static List *
list_make_attrnums(const AttrNumber *attnums, int nattnums)
{
	List	   *result = NIL;

	for (int i = 0; i < nattnums; i++)
		result = lappend_int(result, attnums[i]);
	return result;
}

/*
 * add_inactive_projected
 *
 *		Carry a fact forward as inactive (diagnostics only), remapping its
 *		columns through attrmap so a later rejection can still locate it.  An
 *		active fact is stamped now (first death wins); an already-inactive fact
 *		keeps its earlier stamp.  A fact whose columns the projection does not
 *		carry is dropped.
 */
static void
add_inactive_projected(KeyJoinSurfaceFacts *dst, KeyJoinFact *old,
					   List **attrmap, KeyJoinInactiveReason reason)
{
	if (old->kind == KJF_NOT_NULL)
	{
		Assert(old->attnum > 0);
		foreach_int(attno, attrmap[old->attnum])
		{
			KeyJoinFact *new = copyObject(old);

			new->attnum = attno;
			if (old->active)
			{
				new->active = false;
				new->inactiveReason = reason;
			}
			dst->facts = lappend(dst->facts, new);
		}
	}
	else
	{
		List	   *newpositions;
		KeyJoinFact *new;

		newpositions = project_key_positions(old->keyPositions, attrmap);
		if (newpositions == NIL)
			return;
		new = copyObject(old);
		new->keyPositions = newpositions;
		if (old->active)
		{
			new->active = false;
			new->inactiveReason = reason;
		}
		dst->facts = lappend(dst->facts, new);
	}
}

/*
 * key_join_failure_detail
 *
 *		Build the DETAIL for a rejected key join using relation aliases and
 *		columns from the rejected FOR KEY clause.
 */
static char *
key_join_failure_detail(KeyJoinReq req, bool inactivated,
						KeyJoinInactiveReason reason, Oid origin_view,
						const char *referencing_relcols,
						const char *referenced_relcols,
						const char *referencing_relation,
						const char *referenced_relation,
						const char *join_name)
{
	char	   *origin_view_name = NULL;
	char	   *origin_sentence = NULL;
	char	   *reason_sentence = NULL;
	char	   *requirement_sentence;
	StringInfoData detail;

	if (OidIsValid(origin_view))
	{
		char	   *relname = get_rel_name(origin_view);
		char	   *nspname = get_namespace_name(get_rel_namespace(origin_view));

		Assert(relname != NULL);
		Assert(nspname != NULL);
		origin_view_name = quote_qualified_identifier(nspname, relname);
		origin_sentence =
			psprintf(_("The relevant operation occurs inside view %s."),
					 origin_view_name);
	}

	Assert(req != REQ_NONE);

	switch (req)
	{
		case REQ_FK:
			requirement_sentence =
				psprintf(_("There is no matching foreign key constraint for %s referencing %s."),
						 referencing_relcols, referenced_relcols);
			break;
		case REQ_FKPAIR:
			requirement_sentence =
				psprintf(_("The matching foreign key for %s references different columns than %s."),
						 referencing_relcols, referenced_relcols);
			break;
		case REQ_FILTER:
		case REQ_COVERAGE:
			requirement_sentence =
				psprintf(_("Not every %s value can be proven to have a matching %s row."),
						 referencing_relcols, referenced_relation);
			break;
		case REQ_UNIQUE:
			requirement_sentence =
				psprintf(_("Referenced columns %s are not proven unique."),
						 referenced_relcols);
			break;
		default:
			Assert(req == REQ_NOTNULL);
			requirement_sentence =
				psprintf(_("This %s could filter rows from %s."),
						 join_name, referencing_relation);
			break;
	}

	switch (req)
	{
		case REQ_FK:
		case REQ_FKPAIR:
			break;
		case REQ_FILTER:
			reason_sentence =
				psprintf(_("Referenced relation %s has a filter that is not matched by referencing relation %s."),
						 referenced_relation, referencing_relation);
			break;
		case REQ_UNIQUE:
			if (inactivated)
			{
				if (reason == KJI_JOIN_FANOUT)
				{
					reason_sentence =
						psprintf(_("A preceding join may duplicate rows from referenced relation %s."),
								 referenced_relation);
				}
				else
				{
					Assert(reason == KJI_GROUP_BY);
					reason_sentence =
						psprintf(_("Referenced relation %s may duplicate key values because of GROUP BY."),
								 referenced_relation);
				}
			}
			break;
		case REQ_COVERAGE:
			if (inactivated)
			{
				switch (reason)
				{
					case KJI_UNACCOUNTED_FILTER:
						reason_sentence =
							psprintf(_("Referenced relation %s is filtered before this key join."),
									 referenced_relation);
						break;
					case KJI_JOIN_NOT_PRESERVED:
						reason_sentence =
							psprintf(_("Referenced relation %s may lose rows before this key join because of a preceding join."),
									 referenced_relation);
						break;
					case KJI_ROW_REMOVING_CLAUSE:
						reason_sentence =
							psprintf(_("Referenced relation %s may lose rows before this key join because of HAVING, LIMIT, OFFSET, FOR UPDATE or TABLESAMPLE."),
									 referenced_relation);
						break;
					case KJI_GROUP_BY:
						reason_sentence =
							psprintf(_("Referenced relation %s may lose rows before this key join because of GROUP BY."),
									 referenced_relation);
						break;
					default:
						Assert(reason == KJI_DISTINCT);
						reason_sentence =
							psprintf(_("Referenced relation %s may lose rows before this key join because of DISTINCT."),
									 referenced_relation);
						break;
				}
			}
			else
				reason_sentence =
					psprintf(_("Referenced relation %s is not proven to contain every referenced key row."),
							 referenced_relation);
			break;
		default:
			Assert(req == REQ_NOTNULL);
			if (inactivated)
			{
				if (reason == KJI_NULL_EXTENDING_JOIN)
				{
					reason_sentence =
						psprintf(_("Referencing columns %s can be null because a preceding outer join can null-extend the referencing side."),
								 referencing_relcols);
				}
				else
				{
					Assert(reason == KJI_GROUP_BY);
					reason_sentence =
						psprintf(_("Referencing columns %s can be null because GROUP BY can output nulls for omitted grouping columns."),
								 referencing_relcols);
				}
			}
			else
				reason_sentence =
					psprintf(_("Referencing columns %s can be null."),
							 referencing_relcols);
			break;
	}

	initStringInfo(&detail);
	appendStringInfoString(&detail, requirement_sentence);
	if (reason_sentence != NULL)
	{
		appendStringInfoChar(&detail, ' ');
		appendStringInfoString(&detail, reason_sentence);
	}
	if (origin_sentence != NULL)
	{
		appendStringInfoChar(&detail, ' ');
		appendStringInfoString(&detail, origin_sentence);
	}
	return detail.data;
}

/*
 * key_join_report_failure
 *
 *		Format and report an unproven key join.  The two callers only decide
 *		which query side is referencing/referenced; this function owns the
 *		user-facing message shape.
 */
static void
key_join_report_failure(ParseState *pstate, ParseLoc location,
						JoinType jointype, const KeyJoinMatch *match,
						const KeyJoinFailureSide *referencing,
						const KeyJoinFailureSide *referenced)
{
	const KeyJoinFailureSide *sides[2] = {referencing, referenced};
	const char *relations[2];
	char	   *relcols[2];
	char	   *detail;

	for (int i = 0; i < 2; i++)
	{
		const KeyJoinFailureSide *side = sides[i];
		StringInfoData colbuf;
		bool		first = true;

		Assert(side != NULL);
		Assert((side->columns != NULL) != (side->rte != NULL));
		Assert(side->columns == NULL || side->ncolumns > 0);
		Assert(side->rte == NULL || side->attnums != NIL);

		relations[i] = quote_identifier(side->alias != NULL ?
										 side->alias :
										 side->rte->eref->aliasname);

		initStringInfo(&colbuf);
		if (side->columns != NULL)
		{
			for (int j = 0; j < side->ncolumns; j++)
			{
				if (!first)
					appendStringInfoString(&colbuf, ", ");
				appendStringInfoString(&colbuf,
									   quote_identifier(side->columns[j].name));
				first = false;
			}
		}
		else
		{
			foreach_int(attno, side->attnums)
			{
				char	   *colname = get_rte_attribute_name(side->rte, attno);

				if (!first)
					appendStringInfoString(&colbuf, ", ");
				appendStringInfoString(&colbuf, quote_identifier(colname));
				first = false;
			}
		}
		relcols[i] = psprintf("%s (%s)", relations[i], colbuf.data);
	}

	detail =
		key_join_failure_detail(match->failReq, match->failInactivated,
								match->failReason, match->failOriginView,
								relcols[0], relcols[1],
								relations[0], relations[1],
								jointype == JOIN_INNER ?
								"inner join" : "join");

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_FOREIGN_KEY),
			 errmsg("key join from referencing relation %s to referenced relation %s cannot be proven",
					relations[0], relations[1]),
			 errdetail_internal("%s", detail),
			 parser_errposition(pstate, location)));
}
