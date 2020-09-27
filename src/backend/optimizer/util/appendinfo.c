/*-------------------------------------------------------------------------
 *
 * appendinfo.c
 *	  Routines for mapping between append parent(s) and children
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
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
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct
{
	PlannerInfo *root;
	int			nappinfos;
	AppendRelInfo **appinfos;
	bool		need_parent_wholerow;
} adjust_appendrel_attrs_context;

static void make_inh_translation_list(Relation oldrelation,
									  Relation newrelation,
									  Index newvarno,
									  AppendRelInfo *appinfo);
static Node *adjust_appendrel_attrs_mutator(Node *node,
											adjust_appendrel_attrs_context *context);


/*
 * make_append_rel_info
 *	  Build an AppendRelInfo for the parent-child pair
 */
AppendRelInfo *
make_append_rel_info(Relation parentrel, Relation childrel,
					 Index parentRTindex, Index childRTindex)
{
	AppendRelInfo *appinfo = makeNode(AppendRelInfo);

	appinfo->parent_relid = parentRTindex;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reltype = parentrel->rd_rel->reltype;
	appinfo->child_reltype = childrel->rd_rel->reltype;
	make_inh_translation_list(parentrel, childrel, childRTindex, appinfo);
	appinfo->parent_reloid = RelationGetRelid(parentrel);

	return appinfo;
}

/*
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child, as well as a reverse-translation array.
 *
 * The reverse-translation array has an entry for each child relation
 * column, which is either the 1-based index of the corresponding parent
 * column, or 0 if there's no match (that happens for dropped child columns,
 * as well as child columns beyond those of the parent, which are allowed in
 * traditional inheritance though not partitioning).
 *
 * For paranoia's sake, we match type/collation as well as attribute name.
 */
static void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno,
						  AppendRelInfo *appinfo)
{
	List	   *vars = NIL;
	AttrNumber *pcolnos;
	TupleDesc	old_tupdesc = RelationGetDescr(oldrelation);
	TupleDesc	new_tupdesc = RelationGetDescr(newrelation);
	Oid			new_relid = RelationGetRelid(newrelation);
	int			oldnatts = old_tupdesc->natts;
	int			newnatts = new_tupdesc->natts;
	int			old_attno;
	int			new_attno = 0;

	/* Initialize reverse-translation array with all entries zero */
	appinfo->num_child_cols = newnatts;
	appinfo->parent_colnos = pcolnos =
		(AttrNumber *) palloc0(newnatts * sizeof(AttrNumber));

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
		if (oldrelation == newrelation)
		{
			vars = lappend(vars, makeVar(newvarno,
										 (AttrNumber) (old_attno + 1),
										 atttypid,
										 atttypmod,
										 attcollation,
										 0));
			pcolnos[old_attno] = old_attno + 1;
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

			newtup = SearchSysCacheAttName(new_relid, attname);
			if (!HeapTupleIsValid(newtup))
				elog(ERROR, "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname, RelationGetRelationName(newrelation));
			new_attno = ((Form_pg_attribute) GETSTRUCT(newtup))->attnum - 1;
			Assert(new_attno >= 0 && new_attno < newnatts);
			ReleaseSysCache(newtup);

			att = TupleDescAttr(new_tupdesc, new_attno);
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname, RelationGetRelationName(newrelation));
		if (attcollation != att->attcollation)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname, RelationGetRelationName(newrelation));

		vars = lappend(vars, makeVar(newvarno,
									 (AttrNumber) (new_attno + 1),
									 atttypid,
									 atttypmod,
									 attcollation,
									 0));
		pcolnos[new_attno] = old_attno + 1;
		new_attno++;
	}

	appinfo->translated_vars = vars;
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
	context.need_parent_wholerow = true;

	/* If there's nothing to adjust, don't call this function. */
	Assert(nappinfos >= 1 && appinfos != NULL);

	/* Should never be translating a Query tree. */
	Assert (node == NULL || !IsA(node, Query));
	result = adjust_appendrel_attrs_mutator(node, &context);

	return result;
}

/*
 * adjust_target_appendrel_attrs
 *		like adjust_appendrel_attrs, but treats wholerow Vars a bit
 *		differently in that it doesn't convert any child table
 *		wholerows contained in 'node' back to the parent reltype.
 */
Node *
adjust_target_appendrel_attrs(PlannerInfo *root, Node *node,
							  AppendRelInfo *appinfo)
{
	Node	   *result;
	adjust_appendrel_attrs_context context;

	context.root = root;
	context.nappinfos = 1;
	context.appinfos = &appinfo;
	context.need_parent_wholerow = false;

	/* If there's nothing to adjust, don't call this function. */
	Assert(appinfo != NULL);

	/* Should never be translating a Query tree. */
	Assert (node == NULL || !IsA(node, Query));
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

		if (var->varlevelsup != 0)
			return (Node *) var;	/* no changes needed */

		for (cnt = 0; cnt < nappinfos; cnt++)
		{
			if (var->varno == appinfos[cnt]->parent_relid)
			{
				appinfo = appinfos[cnt];
				break;
			}
		}

		if (appinfo)
		{
			var->varno = appinfo->child_relid;
			/* it's now a generated Var, so drop any syntactic labeling */
			var->varnosyn = 0;
			var->varattnosyn = 0;
			if (var->varattno > 0)
			{
				Node	   *newnode;

				/*
				 * If this Var appears to have a unusual attno assigned, it
				 * must be one of the "fake" vars added to a parent target
				 * relation's reltarget; see add_inherit_junk_var().
				 */
				if (var->varattno > list_length(appinfo->translated_vars))
					newnode = translate_fake_parent_var(var, appinfo);
				else
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
					/* Make sure the Var node has the right type ID, too */
					var->vartype = appinfo->child_reltype;
					if (appinfo->parent_reltype != appinfo->child_reltype &&
						context->need_parent_wholerow)
					{
						ConvertRowtypeExpr *r = makeNode(ConvertRowtypeExpr);

						r->arg = (Expr *) var;
						r->resulttype = appinfo->parent_reltype;
						r->convertformat = COERCE_IMPLICIT_CAST;
						r->location = -1;
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
	/* Shouldn't need to handle planner auxiliary nodes here */
	Assert(!IsA(node, SpecialJoinInfo));
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

	/* We should never see these Query substructures. */
	Assert(!IsA(node, RangeTblRef));
	Assert(!IsA(node, JoinExpr));

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
