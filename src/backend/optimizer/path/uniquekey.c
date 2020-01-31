/*-------------------------------------------------------------------------
 *
 * uniquekey.c
 *	  Utilities for matching and building unique keys
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekey.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "nodes/pg_list.h"

static UniqueKey *make_canonical_uniquekey(PlannerInfo *root, EquivalenceClass *eclass);

/*
 * Build a list of unique keys
 */
List*
build_uniquekeys(PlannerInfo *root, List *sortclauses)
{
	List *result = NIL;
	List *sortkeys;
	ListCell *l;

	sortkeys = make_pathkeys_for_uniquekeys(root,
											sortclauses,
											root->processed_tlist);

	/* Create a uniquekey and add it to the list */
	foreach(l, sortkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(l);
		EquivalenceClass *ec = pathkey->pk_eclass;
		UniqueKey *unique_key = make_canonical_uniquekey(root, ec);

		result = lappend(result, unique_key);
	}

	return result;
}

/*
 * uniquekeys_contained_in
 *	  Are the keys2 included in the keys1 superset
 */
bool
uniquekeys_contained_in(List *keys1, List *keys2)
{
	ListCell   *key1,
			   *key2;

	/*
	 * Fall out quickly if we are passed two identical lists.  This mostly
	 * catches the case where both are NIL, but that's common enough to
	 * warrant the test.
	 */
	if (keys1 == keys2)
		return true;

	foreach(key2, keys2)
	{
		bool found = false;
		UniqueKey  *uniquekey2 = (UniqueKey *) lfirst(key2);

		foreach(key1, keys1)
		{
			UniqueKey  *uniquekey1 = (UniqueKey *) lfirst(key1);

			if (uniquekey1->eq_clause == uniquekey2->eq_clause)
			{
				found = true;
				break;
			}
		}

		if (!found)
			return false;
	}

	return true;
}

/*
 * has_useful_uniquekeys
 *		Detect whether the planner could have any uniquekeys that are
 *		useful.
 */
bool
has_useful_uniquekeys(PlannerInfo *root)
{
	if (root->query_uniquekeys != NIL)
		return true;	/* there are some */
	return false;		/* definitely useless */
}

/*
 * make_canonical_uniquekey
 *	  Given the parameters for a UniqueKey, find any pre-existing matching
 *	  uniquekey in the query's list of "canonical" uniquekeys.  Make a new
 *	  entry if there's not one already.
 *
 * Note that this function must not be used until after we have completed
 * merging EquivalenceClasses.  (We don't try to enforce that here; instead,
 * equivclass.c will complain if a merge occurs after root->canon_uniquekeys
 * has become nonempty.)
 */
static UniqueKey *
make_canonical_uniquekey(PlannerInfo *root,
						 EquivalenceClass *eclass)
{
	UniqueKey  *uk;
	ListCell   *lc;
	MemoryContext oldcontext;

	/* The passed eclass might be non-canonical, so chase up to the top */
	while (eclass->ec_merged)
		eclass = eclass->ec_merged;

	foreach(lc, root->canon_uniquekeys)
	{
		uk = (UniqueKey *) lfirst(lc);
		if (eclass == uk->eq_clause)
			return uk;
	}

	/*
	 * Be sure canonical uniquekeys are allocated in the main planning context.
	 * Not an issue in normal planning, but it is for GEQO.
	 */
	oldcontext = MemoryContextSwitchTo(root->planner_cxt);

	uk = makeNode(UniqueKey);
	uk->eq_clause = eclass;

	root->canon_uniquekeys = lappend(root->canon_uniquekeys, uk);

	MemoryContextSwitchTo(oldcontext);

	return uk;
}
