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
#include "parser/parse_coerce.h"
#include "parser/parse_key_join.h"
#include "parser/parse_relation.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
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
	REQ_FKPAIR,					/* FK pairs the named columns (cond. 2a) */
	REQ_NOTNULL,				/* referencing not-null evidence (cond. 3) */
} KeyJoinReq;

/*
 * KeyJoinMatch
 *
 *		Complete proof selected for one validated key join.
 *		The parser uses this to build equality quals.
 */
typedef struct KeyJoinMatch
{
	Oid			constraint;
	List	   *eqoperators;
	List	   *eqtypes;
	List	   *eqtypmods;
	/* Filled only on failure, for the rejection message. */
	KeyJoinReq	failReq;
} KeyJoinMatch;

/*
 * KeyJoinFailureSide
 *
 *		Display fields for one side of an unproven key join: the relation alias
 *		and its raw KeyJoinColumn names from parse analysis.
 */
typedef struct KeyJoinFailureSide
{
	const char *alias;
	KeyJoinColumn *columns;
	int			ncolumns;
} KeyJoinFailureSide;

static bool find_key_join_match(RangeTblEntry *referencing_rte,
								RangeTblEntry *referenced_rte,
								List *referencing_attnums,
								List *referenced_attnums,
								bool need_notnull, KeyJoinMatch *match);
static void ensure_key_join_surface_facts(RangeTblEntry *rte);
static void compute_key_join_relation_facts(RangeTblEntry *rte, Relation rel);

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
								  ParseLoc default_location);
static bool select_key_position_parts(List *selected_attnums,
									  List *keyPositions, List *baseAttnums,
									  List **selected_base_attnums,
									  List **selected_key_positions);
static int	key_position_index_for_attnum(List *keyPositions, int attno);
static bool key_position_identity_lists_equal(List *left, List *right);
static bool key_position_identity_equal(KeyJoinKeyPosition *left,
										KeyJoinKeyPosition *right);
#ifdef USE_ASSERT_CHECKING
static bool int_lists_same_members(List *a, List *b);
#endif
static HeapTuple lock_and_fetch_key_join_constraint(Oid constraintOid);
static HeapTuple lock_and_fetch_key_join_operator(Oid opno);
static HeapTuple lock_and_fetch_key_join_proc(Oid procid);
static KeyJoinFact *add_fact(KeyJoinSurfaceFacts *set,
							 KeyJoinFactKind kind);
static bool key_join_collation_is_usable(Oid collationOid);
static bool key_join_equality_identity_is_usable(Oid typeOid, int32 typmod,
												 Oid eqTypeOid,
												 int32 *eqTypmod);
static bool key_join_equality_operator_is_usable(Oid opno,
												 Oid expectedTypeOid,
												 Oid *eqTypeOid);
static Oid	key_join_equality_type(Oid typeOid, int32 typmod,
								   int32 *eqTypmod);
static List *make_key_positions_from_attrnums(const TupleDesc tupdesc,
											  const AttrNumber *attnums,
											  int nattnums,
											  const Oid *eqTypes,
											  const int32 *eqTypmods,
											  const Oid *eqOperators);
static KeyJoinKeyPosition *make_key_position(AttrNumber attnum, Oid typeOid,
											 int32 typmod, Oid collationOid,
											 Oid eqTypeOid, int32 eqTypmod,
											 Oid eqOperator);
static List *list_make_attrnums(const AttrNumber *attnums, int nattnums);
static char *key_join_failure_detail(KeyJoinReq req,
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
	List	   *referencing_vars = NIL;
	List	   *referenced_vars = NIL;
	KeyJoinMatch match;
	KeyJoinNode *key_join;
	int			ncols = list_length(key_clause->localCols);

	/*
	 * A FOR KEY join is supported only in a directly executed query, not in
	 * the stored definition of a view, materialized view, rule, policy, or
	 * routine.
	 */
	if (pstate->p_creating_stored_object)
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

		referencing_attnums = lappend_int(referencing_attnums,
										  fkcol->surface_attno);
		referenced_attnums = lappend_int(referenced_attnums,
										 pkcol->surface_attno);
		referencing_vars = lappend(referencing_vars,
								   local_is_referencing ? localvar : refvar);
		referenced_vars = lappend(referenced_vars,
								  local_is_referencing ? refvar : localvar);
	}

	/*
	 * FOR KEY proof facts are derived only from base relations named directly
	 * as the join operands.  Reject any operand whose surface is a derived
	 * table -- a join tree, subquery, CTE, view, or function -- before
	 * computing facts.
	 */
	if (fk_surface->p_rte->rtekind != RTE_RELATION ||
		fk_surface->p_rte->relkind == RELKIND_VIEW ||
		pk_surface->p_rte->rtekind != RTE_RELATION ||
		pk_surface->p_rte->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FOR KEY join operands must be tables referenced directly in the join"),
				 parser_errposition(pstate, key_clause->location)));

	/* With a directly referenced left operand, the arrow alias is it. */
	Assert(ref_nsitem == l_nsitem);

	ensure_key_join_surface_facts(fk_surface->p_rte);
	ensure_key_join_surface_facts(pk_surface->p_rte);

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

	/* Install the equality quals proven by condition 2. */
	j->quals = build_key_join_quals(referenced_vars, referencing_vars,
									match.eqoperators, match.eqtypes,
									match.eqtypmods,
									key_clause->location);

	key_join = makeNode(KeyJoinNode);
	key_join->referencingVarno = fk_surface->p_rtindex;
	key_join->referencedVarno = pk_surface->p_rtindex;
	key_join->referencingAttnums = referencing_attnums;
	key_join->referencedAttnums = referenced_attnums;
	key_join->constraint = match.constraint;

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
					 ParseLoc default_location)
{
	List	   *quals = NIL;
	ListCell   *lcrpk;
	ListCell   *lcrfk;
	ListCell   *lcop;
	ListCell   *lctype;
	ListCell   *lctypmod;

	Assert(list_length(referenced_args) == list_length(referencing_args));
	Assert(list_length(referenced_args) == list_length(eqoperators));
	Assert(list_length(referenced_args) == list_length(eqtypes));
	Assert(list_length(referenced_args) == list_length(eqtypmods));

	forfive(lcrpk, referenced_args, lcrfk, referencing_args,
			lcop, eqoperators, lctype, eqtypes, lctypmod, eqtypmods)
	{
		Node	   *pkarg = (Node *) lfirst(lcrpk);
		Node	   *fkarg = (Node *) lfirst(lcrfk);
		Oid			opno = lfirst_oid(lcop);
		Oid			eqtype = lfirst_oid(lctype);
		int32		eqtypmod = lfirst_int(lctypmod);
		Oid			opfuncid;
#ifdef USE_ASSERT_CHECKING
		Oid			lefttype;
		Oid			righttype;
#endif
		OpExpr	   *result;

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
		result->location = default_location;

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
 *		 fact.
 *	  3. If the join type does not preserve the referencing side, each
 *		 referencing column has not-null evidence.
 *
 * On success, *match receives the selected constraint, equality operators
 * and equality-input identities in key-join column order.  On failure,
 * *match receives diagnostic fields describing the first unmet proof
 * requirement.
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
	bool		fk_target_matched = false;
	bool		fk_target_rel_failure = false;
	bool		fk_target_pair_failure = false;

	Assert(match != NULL);
	Assert(referencing_rte != NULL);
	Assert(referenced_rte != NULL);

	/* Diagnostics default to "missing FK"; the failure tail refines this. */
	match->failReq = REQ_FK;

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
		 * Before blaming referenced-side proof facts, use row provenance to
		 * detect when the written referenced columns are not the target of
		 * this FK at all.
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

			foreach_node(KeyJoinFact, uniq, pfacts->facts)
			{
				List	   *uniq_base;

				if (uniq->kind != KJF_UNIQUE)
					continue;

				Assert(list_length(uniq->keyPositions) ==
					   list_length(uniq->baseAttnums));

				if (!select_key_position_parts(referenced_attnums,
											   uniq->keyPositions,
											   uniq->baseAttnums,
											   &uniq_base, NULL))
					continue;
				if (uniq->relid != fkfact->referencedRelid)
				{
					fk_target_rel_failure = true;
					continue;
				}

				if (equal(fk_referenced_base, uniq_base))
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
			/*
			 * Per-candidate scratch.  These live only inside this candidate;
			 * if any check below rejects it, the next unique candidate
			 * starts fresh.
			 */
			List	   *unique_base;
			List	   *unique_key_positions;
			List	   *fk_key_positions = NIL;
			List	   *fk_eqoperators = NIL;
			List	   *fk_eqtypes = NIL;
			List	   *fk_eqtypmods = NIL;

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
			/*
			 * The unique fact has to be on the same relation the FK fact
			 * targets; without that, condition 1 doesn't hold over the
			 * relevant rows.
			 */
			if (uniqfact->relid != fkfact->referencedRelid)
				continue;
			if (reached < REQ_UNIQUE)
				reached = REQ_UNIQUE;

			/* ---- Condition 2a: FK pairs match the selected columns ---- */
			{
				ListCell   *lcfkbase;
				ListCell   *lcpkbase;
				bool		fk_pairs_match = true;

				/*
				 * Key-join columns must cover the whole FK; a partial FK
				 * match cannot prove containment for a multi-column key.
				 * referencing_base and unique_base are already in key-join
				 * column order, so we drive the result lists from them.
				 */
				Assert(list_length(referencing_base) ==
					   list_length(fkfact->keyPositions));
				Assert(list_length(unique_base) ==
					   list_length(fkfact->keyPositions));

				forboth(lcfkbase, referencing_base,
						lcpkbase, unique_base)
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
			 * ---- Condition 2b: unique agrees with the FK on identity ----
			 *
			 * A relation can expose multiple usable unique indexes on the
			 * same column list.  Only a unique fact whose key identity
			 * matches the FK key identity can prove this key join.  Identity
			 * mismatches are normal candidate misses, not separately
			 * reportable user-facing failures.
			 */
			if (!key_position_identity_lists_equal(unique_key_positions,
												   fk_key_positions))
				continue;

			if (reached < REQ_NOTNULL)
				reached = REQ_NOTNULL;

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
			 * Proof fields are filled only here.
			 */
			match->constraint = fkfact->constraint;
			match->eqoperators = fk_eqoperators;
			match->eqtypes = fk_eqtypes;
			match->eqtypmods = fk_eqtypmods;
			return true;
		}
	}

	/*
	 * No proof.  Report the first unmet requirement: the deepest a candidate
	 * reached, plus one.
	 */
	if (reached < REQ_FK)
		match->failReq = REQ_FK;
	else if (!fk_target_matched && fk_target_pair_failure)
		match->failReq = REQ_FKPAIR;
	else if (!fk_target_matched && fk_target_rel_failure)
		match->failReq = REQ_FK;
	else if (reached < REQ_UNIQUE)
		match->failReq = REQ_UNIQUE;
	else if (reached < REQ_NOTNULL)
	{
		Assert(reached >= REQ_FKPAIR);
		match->failReq = REQ_UNIQUE;
	}
	else
		match->failReq = REQ_NOTNULL;

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

static int
key_position_index_for_attnum(List *keyPositions, int attno)
{
	int			match = -1;

	foreach_node(KeyJoinKeyPosition, keypos, keyPositions)
	{
		if (keypos->attnum == attno)
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

#ifdef USE_ASSERT_CHECKING
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
#endif

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
 *		inspects.  Some relations have no sound surface facts in this proof
 *		model (for example, relation kinds that provide no usable
 *		constraints), so the cache also records "computed, but no facts".
 */
static void
ensure_key_join_surface_facts(RangeTblEntry *rte)
{
	Relation	rel;

	Assert(rte != NULL);
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->relkind != RELKIND_VIEW);

	if (rte->keyJoinFactsComputed)
		return;

	Assert(rte->keyJoinFacts == NULL);

	/*
	 * The RTE already holds its rellockmode lock from range table
	 * construction.  Hold this lock until transaction end so DDL cannot
	 * change relation state during fact selection.
	 */
	rel = relation_open(rte->relid, AccessShareLock);
	compute_key_join_relation_facts(rte, rel);
	relation_close(rel, NoLock);

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
compute_key_join_relation_facts(RangeTblEntry *rte, Relation rel)
{
	KeyJoinSurfaceFacts *set;
	TupleDesc	tupdesc;
	List	   *fkeylist;
	LOCKMODE	lockmode = AccessShareLock;

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
	 * An inherited scan of a table with children may include child rows that
	 * break join-to-one, so derive facts only when the table has no children.
	 */
	if (rte->relkind == RELKIND_RELATION && rte->inh && has_subclass(rte->relid))
		return;

	set = makeNode(KeyJoinSurfaceFacts);
	tupdesc = RelationGetDescr(rel);

	/*
	 * Every catalog object a fact below relies on is locked for the
	 * transaction before the fact is added.
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
		}
		ReleaseSysCache(contup);
	}

	/* Usable unique indexes feed condition 1. */
	foreach_oid(indexoid, RelationGetIndexList(rel))
	{
		Relation	indexrel;
		Form_pg_index index;
		AttrNumber	attnums[INDEX_MAX_KEYS];
		Oid			eqtypes[INDEX_MAX_KEYS];
		int32		eqtypmods[INDEX_MAX_KEYS];
		Oid			eqoperators[INDEX_MAX_KEYS];
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
													  &eqtype))
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
			KeyJoinFact *ufact = add_fact(set, KJF_UNIQUE);

			ufact->keyPositions =
				make_key_positions_from_attrnums(tupdesc, attnums,
												 index->indnkeyatts,
												 eqtypes, eqtypmods,
												 eqoperators);
			ufact->relid = rte->relid;
			ufact->baseAttnums = list_make_attrnums(attnums,
													index->indnkeyatts);
			keep_index_lock = true;
		}

		index_close(indexrel, keep_index_lock ? NoLock : lockmode);
	}

	/*
	 * Validated, catalog-enforced, nondeferrable equality FKs feed condition
	 * 2; period FKs are skipped.
	 *
	 * This intentionally trusts pg_constraint's catalog contract.  We do not
	 * inspect current or historical RI trigger enablement here; orphan rows
	 * created through privileged trigger bypass are referential-integrity
	 * corruption outside the key-join proof model, not proof facts to audit
	 * during parse analysis.
	 */
	fkeylist = copyObject(RelationGetFKeyList(rel));

	foreach_node(ForeignKeyCacheInfo, fk, fkeylist)
	{
		HeapTuple	contup;
		Form_pg_constraint con;
		KeyJoinFact *fact;
		Relation	refrel;
		TupleDesc	reftupdesc;
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
													  &eqtype) ||
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

		ReleaseSysCache(contup);
	}
	list_free_deep(fkeylist);

	if (set->facts != NIL)
		rte->keyJoinFacts = set;
}

static KeyJoinFact *
add_fact(KeyJoinSurfaceFacts *set, KeyJoinFactKind kind)
{
	KeyJoinFact *fact = makeNode(KeyJoinFact);

	fact->kind = kind;
	set->facts = lappend(set->facts, fact);
	return fact;
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
									 Oid *eqTypeOid)
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

	Assert(eqTypeOid != NULL);
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
						 make_key_position(att->attnum,
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
make_key_position(AttrNumber attnum, Oid typeOid, int32 typmod,
				  Oid collationOid, Oid eqTypeOid, int32 eqTypmod,
				  Oid eqOperator)
{
	KeyJoinKeyPosition *pos = makeNode(KeyJoinKeyPosition);

	pos->attnum = attnum;
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
 * key_join_failure_detail
 *
 *		Build the DETAIL for a rejected key join using relation aliases and
 *		columns from the rejected FOR KEY clause.
 */
static char *
key_join_failure_detail(KeyJoinReq req,
						const char *referencing_relcols,
						const char *referenced_relcols,
						const char *referencing_relation,
						const char *referenced_relation,
						const char *join_name)
{
	char	   *reason_sentence = NULL;
	char	   *requirement_sentence;
	StringInfoData detail;

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
		case REQ_UNIQUE:
			break;
		default:
			Assert(req == REQ_NOTNULL);
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
	return detail.data;
}

/*
 * key_join_report_failure
 *
 *		Format and report an unproven key join.  The caller only decides
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
		Assert(side->columns != NULL);
		Assert(side->ncolumns > 0);

		relations[i] = quote_identifier(side->alias);

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
		relcols[i] = psprintf("%s (%s)", relations[i], colbuf.data);
	}

	detail =
		key_join_failure_detail(match->failReq,
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
