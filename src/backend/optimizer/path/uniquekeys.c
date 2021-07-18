/*-------------------------------------------------------------------------
 *
 * uniquekeys.c
 *	  Utilities for matching and building unique keys
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekeys.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "optimizer/paths.h"

/*
 * build_uniquekeys
 * 		Preparing list of pathkeys keys which are considered to be unique for
 * 		this query.
 *
 * For now used only for distinct clauses, where redundant keys	need to be
 * preserved e.g. for skip scan. Justification for this function existence is
 * future plans to make it produce actual UniqueKey list. 
 */
List*
build_uniquekeys(PlannerInfo *root, List *sortclauses)
{
	List *result = NIL;
	List *sortkeys;
	ListCell *l;
	List *exprs = NIL;

	sortkeys = make_pathkeys_for_uniquekeys(root,
											sortclauses,
											root->processed_tlist);

	/* Create a uniquekey and add it to the list */
	foreach(l, sortkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(l);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *mem = (EquivalenceMember*) lfirst(list_head(ec->ec_members));
		exprs = lappend(exprs, mem->em_expr);
	}

	result = lappend(result, exprs);

	return result;
}

/*
 * query_has_uniquekeys_for
 * 		Check if the specified unique keys matching all query level unique
 * 		keys.
 *
 * The main use is to verify that unique keys for some path are covering all
 * requested query unique keys. Based on this information a path could be
 * rejected if it satisfy uniqueness only partially.
 */
bool
query_has_uniquekeys_for(PlannerInfo *root, List *path_uniquekeys,
						 bool allow_multinulls)
{
	ListCell *lc;
	ListCell *lc2;

	/* root->query_uniquekeys are the requested DISTINCT clauses on query level
	 * path_uniquekeys are the unique keys on current path. All requested
	 * query_uniquekeys must be satisfied by the path_uniquekeys.
	 */
	foreach(lc, root->query_uniquekeys)
	{
		List *query_ukey = lfirst_node(List, lc);
		bool satisfied = false;
		foreach(lc2, path_uniquekeys)
		{
			List *ukey = lfirst_node(List, lc2);
			if (list_length(ukey) == 0 &&
				list_length(query_ukey) != 0)
				continue;
			if (list_is_subset(ukey, query_ukey))
			{
				satisfied = true;
				break;
			}
		}
		if (!satisfied)
			return false;
	}
	return true;
}
