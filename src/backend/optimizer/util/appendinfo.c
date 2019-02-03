/*-------------------------------------------------------------------------
 *
 * appendinfo.c
 *	  Routines for mapping between append parent(s) and children
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/appendinfo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct
{
	PlannerInfo *root;
	int			nappinfos;
	AppendRelInfo **appinfos;
} adjust_appendrel_attrs_context;

static void make_inh_translation_list(TupleDesc old_tupdesc,
						  TupleDesc new_tupdesc,
						  Oid from_rel, Oid to_rel,
						  Index newvarno, List **translated_vars);
static Node *adjust_appendrel_attrs_mutator(Node *node,
							   adjust_appendrel_attrs_context *context);
static List *adjust_inherited_tlist(List *tlist,
					   AppendRelInfo *context);


/*
 * make_append_rel_info
 *	  Build an AppendRelInfo for the parent-child pair
 */
AppendRelInfo *
make_append_rel_info(RelOptInfo *parent, RangeTblEntry *parentrte,
					 TupleDesc childdesc, Oid childoid, Oid childtype,
					 Index childRTindex)
{
	AppendRelInfo *appinfo = makeNode(AppendRelInfo);

	appinfo->parent_relid = parent->relid;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reltype = parent->reltype;
	appinfo->child_reltype = childtype;
	make_inh_translation_list(parent->tupdesc, childdesc,
							  parentrte->relid, childoid,
							  childRTindex, &appinfo->translated_vars);
	appinfo->parent_reloid = parentrte->relid;

	return appinfo;
}

/*
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child.
 *
 * For paranoia's sake, we match type/collation as well as attribute name.
 */
static void
make_inh_translation_list(TupleDesc old_tupdesc, TupleDesc new_tupdesc,
						  Oid from_rel, Oid to_rel,
						  Index newvarno, List **translated_vars)
{
	List	   *vars = NIL;
	int			oldnatts = old_tupdesc->natts;
	int			newnatts = new_tupdesc->natts;
	int			old_attno;
	int			new_attno = 0;

	for (old_attno = 0; old_attno < oldnatts; old_attno++)
	{
		Form_pg_attribute att;
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		Oid			attcollation;

		att = TupleDescAttr(old_tupdesc, old_attno);
		if (att->attisdropped)
		{
			/* Just put NULL into this list entry */
			vars = lappend(vars, NULL);
			continue;
		}
		attname = NameStr(att->attname);
		atttypid = att->atttypid;
		atttypmod = att->atttypmod;
		attcollation = att->attcollation;

		/*
		 * When we are generating the "translation list" for the parent table
		 * of an inheritance set, no need to search for matches.
		 */
		if (from_rel == to_rel)
		{
			vars = lappend(vars, makeVar(newvarno,
										 (AttrNumber) (old_attno + 1),
										 atttypid,
										 atttypmod,
										 attcollation,
										 0));
			continue;
		}

		/*
		 * Otherwise we have to search for the matching column by name.
		 * There's no guarantee it'll have the same column position, because
		 * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
		 * However, in simple cases, the relative order of columns is mostly
		 * the same in both relations, so try the column of newrelation that
		 * follows immediately after the one that we just found, and if that
		 * fails, let syscache handle it.
		 */
		if (new_attno >= newnatts ||
			(att = TupleDescAttr(new_tupdesc, new_attno))->attisdropped ||
			strcmp(attname, NameStr(att->attname)) != 0)
		{
			HeapTuple	newtup;

			newtup = SearchSysCacheAttName(to_rel, attname);
			if (!newtup)
				elog(ERROR, "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname, get_rel_name(to_rel));
			new_attno = ((Form_pg_attribute) GETSTRUCT(newtup))->attnum - 1;
			ReleaseSysCache(newtup);

			att = TupleDescAttr(new_tupdesc, new_attno);
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname, get_rel_name(to_rel));
		if (attcollation != att->attcollation)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname, get_rel_name(to_rel));

		vars = lappend(vars, makeVar(newvarno,
									 (AttrNumber) (new_attno + 1),
									 atttypid,
									 atttypmod,
									 attcollation,
									 0));
		new_attno++;
	}

	*translated_vars = vars;
}

/*
 * adjust_appendrel_attrs
 *	  Copy the specified query or expression and translate Vars referring to a
 *	  parent rel to refer to the corresponding child rel instead.  We also
 *	  update rtindexes appearing outside Vars, such as resultRelation and
 *	  jointree relids.
 *
 * Note: this is only applied after conversion of sublinks to subplans,
 * so we don't need to cope with recursion into sub-queries.
 *
 * Note: this is not hugely different from what pullup_replace_vars() does;
 * maybe we should try to fold the two routines together.
 */
Node *
adjust_appendrel_attrs(PlannerInfo *root, Node *node, int nappinfos,
					   AppendRelInfo **appinfos)
{
	Node	   *result;
	adjust_appendrel_attrs_context context;

	context.root = root;
	context.nappinfos = nappinfos;
	context.appinfos = appinfos;

	/* If there's nothing to adjust, don't call this function. */
	Assert(nappinfos >= 1 && appinfos != NULL);

	/*
	 * Must be prepared to start with a Query or a bare expression tree.
	 */
	if (node && IsA(node, Query))
	{
		Query	   *newnode;
		int			cnt;

		newnode = query_tree_mutator((Query *) node,
									 adjust_appendrel_attrs_mutator,
									 (void *) &context,
									 QTW_IGNORE_RC_SUBQUERIES);
		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			AppendRelInfo *appinfo = appinfos[cnt];

			if (newnode->resultRelation == appinfo->parent_relid)
			{
				newnode->resultRelation = appinfo->child_relid;
				/* Fix tlist resnos too, if it's inherited UPDATE */
				if (newnode->commandType == CMD_UPDATE)
					newnode->targetList =
						adjust_inherited_tlist(newnode->targetList,
											   appinfo);
				break;
			}
		}

		result = (Node *) newnode;
	}
	else
		result = adjust_appendrel_attrs_mutator(node, &context);

	return result;
}

static Node *
adjust_appendrel_attrs_mutator(Node *node,
							   adjust_appendrel_attrs_context *context)
{
	AppendRelInfo **appinfos = context->appinfos;
	int			nappinfos = context->nappinfos;
	int			cnt;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) copyObject(node);
		AppendRelInfo *appinfo = NULL;

		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			if (var->varno == appinfos[cnt]->parent_relid)
			{
				appinfo = appinfos[cnt];
				break;
			}
		}

		if (var->varlevelsup == 0 && appinfo)
		{
			var->varno = appinfo->child_relid;
			var->varnoold = appinfo->child_relid;
			if (var->varattno > 0)
			{
				Node	   *newnode;

				if (var->varattno > list_length(appinfo->translated_vars))
					elog(ERROR, "attribute %d of relation \"%s\" does not exist",
						 var->varattno, get_rel_name(appinfo->parent_reloid));
				newnode = copyObject(list_nth(appinfo->translated_vars,
											  var->varattno - 1));
				if (newnode == NULL)
					elog(ERROR, "attribute %d of relation \"%s\" does not exist",
						 var->varattno, get_rel_name(appinfo->parent_reloid));
				return newnode;
			}
			else if (var->varattno == 0)
			{
				/*
				 * Whole-row Var: if we are dealing with named rowtypes, we
				 * can use a whole-row Var for the child table plus a coercion
				 * step to convert the tuple layout to the parent's rowtype.
				 * Otherwise we have to generate a RowExpr.
				 */
				if (OidIsValid(appinfo->child_reltype))
				{
					Assert(var->vartype == appinfo->parent_reltype);
					if (appinfo->parent_reltype != appinfo->child_reltype)
					{
						ConvertRowtypeExpr *r = makeNode(ConvertRowtypeExpr);

						r->arg = (Expr *) var;
						r->resulttype = appinfo->parent_reltype;
						r->convertformat = COERCE_IMPLICIT_CAST;
						r->location = -1;
						/* Make sure the Var node has the right type ID, too */
						var->vartype = appinfo->child_reltype;
						return (Node *) r;
					}
				}
				else
				{
					/*
					 * Build a RowExpr containing the translated variables.
					 *
					 * In practice var->vartype will always be RECORDOID here,
					 * so we need to come up with some suitable column names.
					 * We use the parent RTE's column names.
					 *
					 * Note: we can't get here for inheritance cases, so there
					 * is no need to worry that translated_vars might contain
					 * some dummy NULLs.
					 */
					RowExpr    *rowexpr;
					List	   *fields;
					RangeTblEntry *rte;

					rte = rt_fetch(appinfo->parent_relid,
								   context->root->parse->rtable);
					fields = copyObject(appinfo->translated_vars);
					rowexpr = makeNode(RowExpr);
					rowexpr->args = fields;
					rowexpr->row_typeid = var->vartype;
					rowexpr->row_format = COERCE_IMPLICIT_CAST;
					rowexpr->colnames = copyObject(rte->eref->colnames);
					rowexpr->location = -1;

					return (Node *) rowexpr;
				}
			}
			/* system attributes don't need any other translation */
		}
		return (Node *) var;
	}
	if (IsA(node, CurrentOfExpr))
	{
		CurrentOfExpr *cexpr = (CurrentOfExpr *) copyObject(node);

		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			AppendRelInfo *appinfo = appinfos[cnt];

			if (cexpr->cvarno == appinfo->parent_relid)
			{
				cexpr->cvarno = appinfo->child_relid;
				break;
			}
		}
		return (Node *) cexpr;
	}
	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) copyObject(node);

		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			AppendRelInfo *appinfo = appinfos[cnt];

			if (rtr->rtindex == appinfo->parent_relid)
			{
				rtr->rtindex = appinfo->child_relid;
				break;
			}
		}
		return (Node *) rtr;
	}
	if (IsA(node, JoinExpr))
	{
		/* Copy the JoinExpr node with correct mutation of subnodes */
		JoinExpr   *j;
		AppendRelInfo *appinfo;

		j = (JoinExpr *) expression_tree_mutator(node,
												 adjust_appendrel_attrs_mutator,
												 (void *) context);
		/* now fix JoinExpr's rtindex (probably never happens) */
		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			appinfo = appinfos[cnt];

			if (j->rtindex == appinfo->parent_relid)
			{
				j->rtindex = appinfo->child_relid;
				break;
			}
		}
		return (Node *) j;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/* Copy the PlaceHolderVar node with correct mutation of subnodes */
		PlaceHolderVar *phv;

		phv = (PlaceHolderVar *) expression_tree_mutator(node,
														 adjust_appendrel_attrs_mutator,
														 (void *) context);
		/* now fix PlaceHolderVar's relid sets */
		if (phv->phlevelsup == 0)
			phv->phrels = adjust_child_relids(phv->phrels, context->nappinfos,
											  context->appinfos);
		return (Node *) phv;
	}

	/*
	 * This is needed, because inheritance_make_rel_from_joinlist needs to
	 * translate root->join_info_list executing make_rel_from_joinlist for a
	 * given child.
	 */
	if (IsA(node, SpecialJoinInfo))
	{
		SpecialJoinInfo *oldinfo = (SpecialJoinInfo *) node;
		SpecialJoinInfo *newinfo = makeNode(SpecialJoinInfo);

		memcpy(newinfo, oldinfo, sizeof(SpecialJoinInfo));
		newinfo->min_lefthand = adjust_child_relids(oldinfo->min_lefthand,
													context->nappinfos,
													context->appinfos);
		newinfo->min_righthand = adjust_child_relids(oldinfo->min_righthand,
													 context->nappinfos,
													 context->appinfos);
		newinfo->syn_lefthand = adjust_child_relids(oldinfo->syn_lefthand,
													context->nappinfos,
													context->appinfos);
		newinfo->syn_righthand = adjust_child_relids(oldinfo->syn_righthand,
													 context->nappinfos,
													 context->appinfos);
		newinfo->semi_rhs_exprs =
			(List *) expression_tree_mutator((Node *)
											 oldinfo->semi_rhs_exprs,
											 adjust_appendrel_attrs_mutator,
											 (void *) context);
		return (Node *) newinfo;
	}

	if (IsA(node, EquivalenceClass))
	{
		EquivalenceClass *ec = (EquivalenceClass *) node;
		EquivalenceClass *new_ec = makeNode(EquivalenceClass);
		AppendRelInfo *appinfo = NULL;
		RelOptInfo *parent_rel,
				   *child_rel;
		ListCell *lc;

		/*
		 * First memcpy the existing EC to the new EC, which creates shallow
		 * copies of all the members and then make copies of members that
		 * we'll change below.
		 *
		 * XXX comment in _copyPathKey says it's OK to recycle EC
		 * pointers, but as long as we do the whole planning for a
		 * given child using a given root, copying ECs like this
		 * shouldn't be a problem.  Maybe, the following code could
		 * be in _copyEquivalenceClass()?
		 */
		memcpy(new_ec, ec, sizeof(EquivalenceClass));
		new_ec->ec_opfamilies = list_copy(ec->ec_opfamilies);
		new_ec->ec_sources = list_copy(ec->ec_sources);
		new_ec->ec_derives = list_copy(ec->ec_derives);
		new_ec->ec_relids = bms_copy(ec->ec_relids);
		new_ec->ec_members = list_copy(ec->ec_members);

		/*
		 * No point in searching if parent rel not mentioned in eclass; but we
		 * can't tell that for sure if parent rel is itself a child.
		 */
		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			if (bms_is_member(appinfos[cnt]->parent_relid, ec->ec_relids))
			{
				appinfo = appinfos[cnt];
				break;
			}
		}

		if (appinfo == NULL)
			return (Node *) new_ec;

		parent_rel = find_base_rel(context->root, appinfo->parent_relid);
		child_rel = find_base_rel(context->root, appinfo->child_relid);
		if (parent_rel->reloptkind != RELOPT_BASEREL)
			return (Node *) new_ec;

		/*
		 * Check if this EC contains an expression referencing the parent
		 * relation, translate it to child, and store it in place of
		 * the original parent expression.
		 */
		foreach(lc, new_ec->ec_members)
		{
			EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);

			if (cur_em->em_is_const)
				continue;		/* ignore consts here */

			/* Does it reference parent_rel? */
			if (bms_overlap(cur_em->em_relids, parent_rel->relids))
			{
				/* Yes, generate transformed child version */
				Expr	   *new_expr;
				Relids		new_relids;
				Relids		new_nullable_relids;

				new_expr = (Expr *)
					adjust_appendrel_attrs(context->root,
										   (Node *) cur_em->em_expr,
										   1, &appinfo);

				/*
				 * Transform em_relids to match.  Note we do *not* do
				 * pull_varnos(child_expr) here, as for example the
				 * transformation might have substituted a constant, but we
				 * don't want the child member to be marked as constant.
				 */
				new_relids = bms_difference(cur_em->em_relids,
											parent_rel->relids);
				new_relids = bms_add_members(new_relids, child_rel->relids);

				/*
				 * And likewise for nullable_relids.  Note this code assumes
				 * parent and child relids are singletons.
				 */
				new_nullable_relids = cur_em->em_nullable_relids;
				if (bms_overlap(new_nullable_relids, parent_rel->relids))
				{
					new_nullable_relids = bms_difference(new_nullable_relids,
														 parent_rel->relids);
					new_nullable_relids = bms_add_members(new_nullable_relids,
														  child_rel->relids);
				}

				/*
				 * The new expression simply replaces the old parent one, and
				 * em_is_child is set to true so that it's recognized as such
				 * during child planning.
				 */
				lfirst(lc) = add_eq_member(new_ec, new_expr,
										   new_relids, new_nullable_relids,
										   false, cur_em->em_datatype);

				/*
				 * We have found and replaced the parent expression, so done
				 * with EC.
				 */
				break;
			}
		}

		/*
		 * Now fix up EC's relids set.  It's OK to modify EC like this,
		 * because caller must have made a copy of the original EC.
		 * For example, see adjust_inherited_target_child_root.
		 */
		new_ec->ec_relids = bms_del_members(new_ec->ec_relids,
											parent_rel->relids);
		new_ec->ec_relids = bms_add_members(new_ec->ec_relids,
											child_rel->relids);
		return (Node *) new_ec;
	}

	/* Shouldn't need to handle planner auxiliary nodes here */
	Assert(!IsA(node, AppendRelInfo));
	Assert(!IsA(node, PlaceHolderInfo));
	Assert(!IsA(node, MinMaxAggInfo));

	/*
	 * We have to process RestrictInfo nodes specially.  (Note: although
	 * set_append_rel_pathlist will hide RestrictInfos in the parent's
	 * baserestrictinfo list from us, it doesn't hide those in joininfo.)
	 */
	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *oldinfo = (RestrictInfo *) node;
		RestrictInfo *newinfo = makeNode(RestrictInfo);

		/* Copy all flat-copiable fields */
		memcpy(newinfo, oldinfo, sizeof(RestrictInfo));

		/* Recursively fix the clause itself */
		newinfo->clause = (Expr *)
			adjust_appendrel_attrs_mutator((Node *) oldinfo->clause, context);

		/* and the modified version, if an OR clause */
		newinfo->orclause = (Expr *)
			adjust_appendrel_attrs_mutator((Node *) oldinfo->orclause, context);

		/* adjust relid sets too */
		newinfo->clause_relids = adjust_child_relids(oldinfo->clause_relids,
													 context->nappinfos,
													 context->appinfos);
		newinfo->required_relids = adjust_child_relids(oldinfo->required_relids,
													   context->nappinfos,
													   context->appinfos);
		newinfo->outer_relids = adjust_child_relids(oldinfo->outer_relids,
													context->nappinfos,
													context->appinfos);
		newinfo->nullable_relids = adjust_child_relids(oldinfo->nullable_relids,
													   context->nappinfos,
													   context->appinfos);
		newinfo->left_relids = adjust_child_relids(oldinfo->left_relids,
												   context->nappinfos,
												   context->appinfos);
		newinfo->right_relids = adjust_child_relids(oldinfo->right_relids,
													context->nappinfos,
													context->appinfos);

		/*
		 * Reset cached derivative fields, since these might need to have
		 * different values when considering the child relation.  Note we
		 * don't reset left_ec/right_ec: each child variable is implicitly
		 * equivalent to its parent, so still a member of the same EC if any.
		 */
		newinfo->eval_cost.startup = -1;
		newinfo->norm_selec = -1;
		newinfo->outer_selec = -1;
		newinfo->left_em = NULL;
		newinfo->right_em = NULL;
		newinfo->scansel_cache = NIL;
		newinfo->left_bucketsize = -1;
		newinfo->right_bucketsize = -1;
		newinfo->left_mcvfreq = -1;
		newinfo->right_mcvfreq = -1;

		return (Node *) newinfo;
	}

	/*
	 * NOTE: we do not need to recurse into sublinks, because they should
	 * already have been converted to subplans before we see them.
	 */
	Assert(!IsA(node, SubLink));
	Assert(!IsA(node, Query));

	return expression_tree_mutator(node, adjust_appendrel_attrs_mutator,
								   (void *) context);
}

/*
 * adjust_appendrel_attrs_multilevel
 *	  Apply Var translations from a toplevel appendrel parent down to a child.
 *
 * In some cases we need to translate expressions referencing a parent relation
 * to reference an appendrel child that's multiple levels removed from it.
 */
Node *
adjust_appendrel_attrs_multilevel(PlannerInfo *root, Node *node,
								  Relids child_relids,
								  Relids top_parent_relids)
{
	AppendRelInfo **appinfos;
	Bitmapset  *parent_relids = NULL;
	int			nappinfos;
	int			cnt;

	Assert(bms_num_members(child_relids) == bms_num_members(top_parent_relids));

	appinfos = find_appinfos_by_relids(root, child_relids, &nappinfos);

	/* Construct relids set for the immediate parent of given child. */
	for (cnt = 0; cnt < nappinfos; cnt++)
	{
		AppendRelInfo *appinfo = appinfos[cnt];

		parent_relids = bms_add_member(parent_relids, appinfo->parent_relid);
	}

	/* Recurse if immediate parent is not the top parent. */
	if (!bms_equal(parent_relids, top_parent_relids))
		node = adjust_appendrel_attrs_multilevel(root, node, parent_relids,
												 top_parent_relids);

	/* Now translate for this child */
	node = adjust_appendrel_attrs(root, node, nappinfos, appinfos);

	pfree(appinfos);

	return node;
}

/*
 * Substitute child relids for parent relids in a Relid set.  The array of
 * appinfos specifies the substitutions to be performed.
 */
Relids
adjust_child_relids(Relids relids, int nappinfos, AppendRelInfo **appinfos)
{
	Bitmapset  *result = NULL;
	int			cnt;

	for (cnt = 0; cnt < nappinfos; cnt++)
	{
		AppendRelInfo *appinfo = appinfos[cnt];

		/* Remove parent, add child */
		if (bms_is_member(appinfo->parent_relid, relids))
		{
			/* Make a copy if we are changing the set. */
			if (!result)
				result = bms_copy(relids);

			result = bms_del_member(result, appinfo->parent_relid);
			result = bms_add_member(result, appinfo->child_relid);
		}
	}

	/* If we made any changes, return the modified copy. */
	if (result)
		return result;

	/* Otherwise, return the original set without modification. */
	return relids;
}

/*
 * Replace any relid present in top_parent_relids with its child in
 * child_relids. Members of child_relids can be multiple levels below top
 * parent in the partition hierarchy.
 */
Relids
adjust_child_relids_multilevel(PlannerInfo *root, Relids relids,
							   Relids child_relids, Relids top_parent_relids)
{
	AppendRelInfo **appinfos;
	int			nappinfos;
	Relids		parent_relids = NULL;
	Relids		result;
	Relids		tmp_result = NULL;
	int			cnt;

	/*
	 * If the given relids set doesn't contain any of the top parent relids,
	 * it will remain unchanged.
	 */
	if (!bms_overlap(relids, top_parent_relids))
		return relids;

	appinfos = find_appinfos_by_relids(root, child_relids, &nappinfos);

	/* Construct relids set for the immediate parent of the given child. */
	for (cnt = 0; cnt < nappinfos; cnt++)
	{
		AppendRelInfo *appinfo = appinfos[cnt];

		parent_relids = bms_add_member(parent_relids, appinfo->parent_relid);
	}

	/* Recurse if immediate parent is not the top parent. */
	if (!bms_equal(parent_relids, top_parent_relids))
	{
		tmp_result = adjust_child_relids_multilevel(root, relids,
													parent_relids,
													top_parent_relids);
		relids = tmp_result;
	}

	result = adjust_child_relids(relids, nappinfos, appinfos);

	/* Free memory consumed by any intermediate result. */
	if (tmp_result)
		bms_free(tmp_result);
	bms_free(parent_relids);
	pfree(appinfos);

	return result;
}

/*
 * Adjust the targetlist entries of an inherited UPDATE operation
 *
 * The expressions have already been fixed, but we have to make sure that
 * the target resnos match the child table (they may not, in the case of
 * a column that was added after-the-fact by ALTER TABLE).  In some cases
 * this can force us to re-order the tlist to preserve resno ordering.
 * (We do all this work in special cases so that preptlist.c is fast for
 * the typical case.)
 *
 * The given tlist has already been through expression_tree_mutator;
 * therefore the TargetEntry nodes are fresh copies that it's okay to
 * scribble on.
 *
 * Note that this is not needed for INSERT because INSERT isn't inheritable.
 */
static List *
adjust_inherited_tlist(List *tlist, AppendRelInfo *context)
{
	bool		changed_it = false;
	ListCell   *tl;
	List	   *new_tlist;
	bool		more;
	int			attrno;

	/* This should only happen for an inheritance case, not UNION ALL */
	Assert(OidIsValid(context->parent_reloid));

	/* Scan tlist and update resnos to match attnums of child rel */
	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		Var		   *childvar;

		if (tle->resjunk)
			continue;			/* ignore junk items */

		/* Look up the translation of this column: it must be a Var */
		if (tle->resno <= 0 ||
			tle->resno > list_length(context->translated_vars))
			elog(ERROR, "attribute %d of relation \"%s\" does not exist",
				 tle->resno, get_rel_name(context->parent_reloid));
		childvar = (Var *) list_nth(context->translated_vars, tle->resno - 1);
		if (childvar == NULL || !IsA(childvar, Var))
			elog(ERROR, "attribute %d of relation \"%s\" does not exist",
				 tle->resno, get_rel_name(context->parent_reloid));

		if (tle->resno != childvar->varattno)
		{
			tle->resno = childvar->varattno;
			changed_it = true;
		}
	}

	/*
	 * If we changed anything, re-sort the tlist by resno, and make sure
	 * resjunk entries have resnos above the last real resno.  The sort
	 * algorithm is a bit stupid, but for such a seldom-taken path, small is
	 * probably better than fast.
	 */
	if (!changed_it)
		return tlist;

	new_tlist = NIL;
	more = true;
	for (attrno = 1; more; attrno++)
	{
		more = false;
		foreach(tl, tlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(tl);

			if (tle->resjunk)
				continue;		/* ignore junk items */

			if (tle->resno == attrno)
				new_tlist = lappend(new_tlist, tle);
			else if (tle->resno > attrno)
				more = true;
		}
	}

	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);

		if (!tle->resjunk)
			continue;			/* here, ignore non-junk items */

		tle->resno = attrno;
		new_tlist = lappend(new_tlist, tle);
		attrno++;
	}

	return new_tlist;
}

/*
 * find_appinfos_by_relids
 * 		Find AppendRelInfo structures for all relations specified by relids.
 *
 * The AppendRelInfos are returned in an array, which can be pfree'd by the
 * caller. *nappinfos is set to the number of entries in the array.
 */
AppendRelInfo **
find_appinfos_by_relids(PlannerInfo *root, Relids relids, int *nappinfos)
{
	AppendRelInfo **appinfos;
	int			cnt = 0;
	int			i;

	*nappinfos = bms_num_members(relids);
	appinfos = (AppendRelInfo **) palloc(sizeof(AppendRelInfo *) * *nappinfos);

	i = -1;
	while ((i = bms_next_member(relids, i)) >= 0)
	{
		AppendRelInfo *appinfo = root->append_rel_array[i];

		if (!appinfo)
			elog(ERROR, "child rel %d not found in append_rel_array", i);

		appinfos[cnt++] = appinfo;
	}
	return appinfos;
}
