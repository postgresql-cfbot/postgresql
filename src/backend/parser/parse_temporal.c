/*-------------------------------------------------------------------------
 *
 * parse_temporal.c
 *	  handle temporal operators in parser
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_temporal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "parser/parse_temporal.h"
#include "parser/parsetree.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "access/htup_details.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"

/*
 * Enumeration of temporal boundary IDs. We have two elements in a boundary
 * list (i.e., WITH-clause of a temporal primitive) as range-type boundaries,
 * that is, VALID-TIME-attributes. In the future, we could even have
 * a list with only one item. For instance, when we calculate temporal
 * aggregations with a single attribute relation.
 */
typedef enum
{
	TPB_LARG_TIME = 0,
	TPB_RARG_TIME,
} TemporalBoundID;

typedef enum
{
	TPB_ONERROR_NULL,
	TPB_ONERROR_FAIL
} TemporalBoundOnError;

static void
getColumnCounter(const char *colname,
				 const char *prefix,
				 bool *found,
				 int *counter);

static char *
addTemporalAlias(ParseState *pstate,
				 char *name,
				 int counter);

static SelectStmt *
makeTemporalQuerySkeleton(JoinExpr *j,
						  char **nameRN,
						  char **nameP1,
						  char **nameP2,
						  Alias **largAlias,
						  Alias **rargAlias);

static ColumnRef *
temporalBoundGet(List *bounds,
				 TemporalBoundID id,
				 TemporalBoundOnError oe);

static char *
temporalBoundGetName(List *bounds,
					 TemporalBoundID id);

static ColumnRef *
temporalBoundGetCopyFQN(List *bounds,
						TemporalBoundID id,
						char *relname);

static void
temporalBoundCheckRelname(ColumnRef *bound,
						  char *relname);

static List *
temporalBoundGetLeftBounds(List *bounds);

static List *
temporalBoundGetRightBounds(List *bounds);

static void
temporalBoundCheckIntegrity(ParseState *pstate,
							List *bounds,
							List *colnames,
							List *colvars,
							TemporalType tmpType);

static Form_pg_type
typeGet(Oid id);

static List *
internalUseOnlyColumnNames(ParseState *pstate,
						   TemporalType tmpType);

/*
 * tpprint
 * 		Temporal PostgreSQL print: pprint with surroundings to cut out pieces
 * 		from long debug prints.
 */
void
tpprint(const void *obj, const char *marker)
{
	printf("--------------------------------------SSS-%s\n", marker);
	pprint(obj);
	printf("--------------------------------------EEE-%s\n", marker);
	fflush(stdout);
}

/*
 * temporalBoundGetLeftBounds -
 * 		Return the left boundaries of a temporal bounds list. This is a single
 * 		range type value T holding both bounds.
 */
static List *
temporalBoundGetLeftBounds(List *bounds)
{
	/* Invalid temporal bound list length? Specify two range-typed columns. */
	Assert(list_length(bounds) == 2);
	return list_make1(linitial(bounds));
}

/*
 * temporalBoundGetRightBounds -
 * 		Return the right boundaries of a temporal bounds list. This is a single
 * 		range type value T holding both bounds.
 */
static List *
temporalBoundGetRightBounds(List *bounds)
{
	/* Invalid temporal bound list length? Specify two range-typed columns. */
	Assert(list_length(bounds) == 2);
	return list_make1(lsecond(bounds));
}

/*
 * temporalBoundCheckRelname -
 * 		Check if full-qualified names within a boundary list (i.e., WITH-clause
 * 		of a temporal primitive) match with the right or left argument
 * 		respectively.
 */
static void
temporalBoundCheckRelname(ColumnRef *bound, char *relname)
{
	char *givenRelname;
	int l = list_length(bound->fields);

	if(l == 1)
		return;

	givenRelname = strVal((Value *) list_nth(bound->fields, l - 2));

	if(strcmp(relname, givenRelname) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("The temporal bound \"%s\" does not match with " \
						"the argument \"%s\" of the temporal primitive.",
						 NameListToString(bound->fields), relname)));
}

/*
 * temporalBoundGetCopyFQN -
 * 		Creates a copy of a temporal bound from the boundary list identified
 * 		with the given id. If it does not contain a full-qualified column
 * 		reference, the last argument "relname" is used to build a new one.
 */
static ColumnRef *
temporalBoundGetCopyFQN(List *bounds, TemporalBoundID id, char *relname)
{
	ColumnRef *bound = copyObject(temporalBoundGet(bounds, id,
												   TPB_ONERROR_FAIL));
	int l = list_length(bound->fields);

	if(l == 1)
		bound->fields = lcons(makeString(relname), bound->fields);
	else
		temporalBoundCheckRelname(bound, relname);

	return bound;
}

/*
 * temporalBoundGetName -
 * 		Returns the name (that is, not the full-qualified column reference) of
 * 		a bound.
 */
static char *
temporalBoundGetName(List *bounds, TemporalBoundID id)
{
	ColumnRef *bound = temporalBoundGet(bounds, id, TPB_ONERROR_FAIL);
	return strVal((Value *) llast(bound->fields));
}

/*
 * temporalBoundGet -
 * 		Returns a single bound with a given bound ID. See comments below for
 * 		further details.
 */
static ColumnRef *
temporalBoundGet(List *bounds, TemporalBoundID id, TemporalBoundOnError oe)
{
	int l = list_length(bounds);

	switch(l)
	{
		/*
		 * Two boundary entries are either two range-typed bounds, or a single
		 * bound with two scalar values defining start and end (the later is
		 * used for GROUP BY PERIOD for instance)
		 */
		case 2:
			if(id == TPB_LARG_TIME)
				return linitial(bounds);
			if(id == TPB_RARG_TIME)
				return lsecond(bounds);
		break;

		/*
		 * One boundary entry is a range-typed bound for GROUP BY PERIOD or
		 * DISTINCT PERIOD bounds.
		 */
		case 1:
			if(id == TPB_LARG_TIME)
				return linitial(bounds);
	}

	if (oe == TPB_ONERROR_FAIL)
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
			 errmsg("Invalid temporal bound list with length \"%d\" " \
						"and index at \"%d\".", l, id),
				 errhint("Specify two range-typed columns.")));

	return NULL;
}

/*
 * transformTemporalClause -
 * 		If we have a temporal primitive query, we must find all attribute
 * 		numbers for p1, p2, rn, and t columns. If the names of these
 * 		internal-use-only columns are already occupied, we must rename them
 * 		in order to not have an ambiguous column error.
 *
 * 		Please note: We cannot simply use resjunk columns here, because the
 * 		subquery has already been build and parsed. We need these columns then
 * 		for more than a single recursion step. This means, that we would loose
 * 		resjunk columns too early. XXX PEMOSER Is there another possibility?
 */
Node *
transformTemporalClause(ParseState *pstate, Query* qry, SelectStmt *stmt)
{
	ListCell   		*lc		   = NULL;
	bool 			 foundTsTe = false;
	TemporalClause  *tc		   = stmt->temporalClause;
	int 			 pos;

	/* No temporal clause given, do nothing */
	if(!tc)
		return NULL;

	/* To start, the attribute number of temporal boundary is unknown */
	tc->attNumTr = -1;

	/*
	 * Find attribute numbers for each attribute that is used during
	 * temporal adjustment.
	 */
	pos = list_length(qry->targetList);
	if (tc->temporalType == TEMPORAL_TYPE_ALIGNER)
	{
		tc->attNumP2 = pos--;
		tc->attNumP1 = pos--;
	}
	else  /* Temporal normalizer */
	{
		/* This entry gets added during the sort-by transformation */
		tc->attNumP1 = pos + 1;

		/* Unknown and unused */
		tc->attNumP2 = -1;
	}

	tc->attNumRN = pos;

	/*
	 * If we have temporal aliases stored in the current parser state, then we
	 * got ambiguous columns. We resolve this problem by renaming parts of the
	 * query tree with new unique column names.
	 */
	foreach(lc, pstate->p_temporal_aliases)
	{
		SortBy 		*sb 	= NULL;
		char 		*key 	= strVal(linitial((List *) lfirst(lc)));
		char 		*value 	= strVal(lsecond((List *) lfirst(lc)));
		TargetEntry *tle 	= NULL;

		if(strcmp(key, "rn") == 0)
		{
			sb = (SortBy *) linitial(stmt->sortClause);
			tle = get_tle_by_resno(qry->targetList, tc->attNumRN);
		}
		else if(strcmp(key, "p1") == 0)
		{
			sb = (SortBy *) lsecond(stmt->sortClause);
			tle = get_tle_by_resno(qry->targetList, tc->attNumP1);
		}
		else if(strcmp(key, "p2") == 0)
		{
			sb = (SortBy *) lthird(stmt->sortClause);
			tle = get_tle_by_resno(qry->targetList, tc->attNumP2);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("Invalid column name \"%s\" for alias " \
							"renames of temporal adjustment primitives.",
							key)));

		/*
		 * Rename the order-by entry.
		 * Just change the name if it is a column reference, nothing to do
		 * for constants, i.e. if the group-by field has been specified by
		 * a column attribute number (ex. 1 for the first column)
		 */
		if(sb && IsA(sb->node, ColumnRef))
		{
			ColumnRef *cr = (ColumnRef *) sb->node;
			cr->fields = list_make1(makeString(value));
		}

		/*
		 * Rename the targetlist entry for "p1", "p2", or "rn" iff aligner, and
		 * rename it for both temporal primitives, if it is "ts" or "te".
		 */
		if(tle && (foundTsTe
			|| tc->temporalType == TEMPORAL_TYPE_ALIGNER))
		{
			tle->resname = pstrdup(value);
		}
	}

	/*
	 * Find column attribute numbers of the two temporal attributes from
	 * the left argument of the inner join, or the single temporal attribute if
	 * it is a range type.
	 */
	foreach(lc, qry->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if(!tle->resname)
			continue;

		if (strcmp(tle->resname, tc->colnameTr) == 0)
			tc->attNumTr = tle->resno;
	}

	/* We need column attribute numbers for all temporal boundaries */
	if(tc->colnameTr && tc->attNumTr == -1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_TEMPORAL_ADJUSTMENT),
				 errmsg("Needed columns for temporal adjustment not found.")));

	return (Node *) tc;
}

/*
 * transformTemporalClauseResjunk -
 * 		If we have a temporal primitive query, the last three columns are P1,
 * 		P2, and row_id or RN, which we do not need anymore after temporal
 * 		adjustment operations have been accomplished.
 *      However, if the temporal boundaries are range typed columns we split
 *      the range [ts, te) into two separate columns ts and te, which must be
 *      marked as resjunk too.
 *      XXX PEMOSER Use a single loop inside!
 */
Node *
transformTemporalClauseResjunk(Query *qry)
{
	TemporalClause 	*tc = (TemporalClause *) qry->temporalClause;

	/* No temporal clause given, do nothing */
	if(!tc)
		return NULL;

	/* Mark P1 and RN columns as junk, we do not need them afterwards. */
	get_tle_by_resno(qry->targetList, tc->attNumP1)->resjunk = true;
	get_tle_by_resno(qry->targetList, tc->attNumRN)->resjunk = true;

	/* An aligner has also a P2 column, that must be marked as junk. */
	if (tc->temporalType == TEMPORAL_TYPE_ALIGNER)
		get_tle_by_resno(qry->targetList, tc->attNumP2)->resjunk = true;

	/*
	 * Pass the temporal primitive node to the optimizer, to be used later,
	 * to mark unsafe columns, and add attribute indexes.
	 */
	return (Node *) tc;
}

/*
 * addTemporalAlias -
 * 		We use internal-use-only columns to store some information used for
 * 		temporal primitives. Since we need them over several sub-queries, we
 * 		cannot use simply resjunk columns here. We must rename parts of the
 * 		parse tree to handle ambiguous columns. In order to reference the right
 * 		columns after renaming, we store them inside the current parser state,
 * 		and use them afterwards to rename fields. Such attributes could be for
 * 		example: P1, P2, or RN.
 */
static char *
addTemporalAlias(ParseState *pstate, char *name, int counter)
{
	char 	*newName = palloc(64);

	/*
	 * Column name for <name> alternative is <name>_N, where N is 0 if no
	 * other column with that pattern has been found, or N + 1 if
	 * the highest number for a <name>_N column is N. N stand for the <counter>.
	 */
	counter++;
	sprintf(newName, "%s_%d", name, counter);

	/*
	 * Changed aliases must be remembered by the parser state in
	 * order to use them on nodes above, i.e. if they are used in targetlists,
	 * group-by or order-by clauses outside.
	 */
	pstate->p_temporal_aliases =
			lappend(pstate->p_temporal_aliases,
					list_make2(makeString(name),
							   makeString(newName)));

	return newName;
}

/*
 * getColumnCounter -
 * 		Check if a column name starts with a certain prefix. If it ends after
 * 		the prefix, return found (we ignore the counter in this case). However,
 * 		if it continuous with an underscore check if it has a tail after it that
 * 		is a string representation of an integer. If so, return this number as
 * 		integer (keep the parameter "found" as is).
 * 		We use this function to rename "internal-use-only" columns on an
 * 		ambiguity error with user-specified columns.
 */
static void
getColumnCounter(const char *colname, const char *prefix,
				 bool *found, int *counter)
{
	if(memcmp(colname, prefix, strlen(prefix)) == 0)
	{
		colname += strlen(prefix);
		if(*colname == '\0')
			*found = true;
		else if (*colname++ == '_')
		{
			char 	*pos;
			int 	 n = -1;

			errno = 0;
			n = strtol(colname, &pos, 10);

			/*
			 * No error and fully parsed (i.e., string contained
			 * only an integer) => save it if it is bigger than
			 * the last.
			 */
			if(errno == 0 && *pos == 0 && n > *counter)
				*counter = n;
		}
	}
}

/*
 * Creates a skeleton query that can be filled with needed fields from both
 * temporal primitives. This is the common part of both generated to re-use
 * the same code. It also returns palloc'd names for p1, p2, and rn, where p2
 * is optional (omit it by passing NULL).
 *
 * OUTPUT:
 * 		(
 * 		SELECT r.*
 *      FROM
 *      (
 *      	SELECT *, row_id() OVER () rn FROM r
 *      ) r
 *      LEFT OUTER JOIN
 *      <not set yet>
 *      ON <not set yet>
 *      ORDER BY rn, p1
 *      ) x
 */
static SelectStmt *
makeTemporalQuerySkeleton(JoinExpr *j, char **nameRN, char **nameP1,
						  char **nameP2, Alias **largAlias, Alias **rargAlias)
{
	const int 	 		 UNKNOWN_LOCATION = -1;

	SelectStmt			*ssJoinLarg;
	SelectStmt 			*ssRowNumber;
	SelectStmt			*ssResult;
	RangeSubselect 		*rssJoinLarg;
	RangeSubselect 		*rssRowNumber;
	ResTarget			*rtRowNumber;
	ResTarget			*rtAStar;
	ResTarget			*rtAStarWithR;
	ColumnRef 			*crAStarWithR;
	ColumnRef 			*crAStar;
	WindowDef			*wdRowNumber;
	FuncCall			*fcRowNumber;
	JoinExpr			*joinExpr;
	SortBy				*sb1;
	SortBy				*sb2;

	/*
	 * These attribute names could cause conflicts, if the left or right
	 * relation has column names like these. We solve this later by renaming
	 * column names when we know which columns are in use, in order to create
	 * unique column names.
	 */
	*nameRN = pstrdup("rn");
	*nameP1 = pstrdup("p1");
	if(nameP2) *nameP2 = pstrdup("p2");

	/* Find aliases of arguments */
	*largAlias = makeAliasFromArgument(j->larg);
	*rargAlias = makeAliasFromArgument(j->rarg);

	/*
	 * Build "(SELECT row_id() OVER (), * FROM r) r".
	 * We start with building the resource target for "*".
	 */
	crAStar = makeColumnRef1((Node *) makeNode(A_Star));
	rtAStar = makeResTarget((Node *) crAStar, NULL);

	/* Build an empty window definition clause, i.e. "OVER ()" */
	wdRowNumber = makeNode(WindowDef);
	wdRowNumber->frameOptions = FRAMEOPTION_DEFAULTS;
	wdRowNumber->startOffset = NULL;
	wdRowNumber->endOffset = NULL;

	/*
	 * Build a target for "row_id() OVER ()", row_id() enumerates each tuple
	 * similar to row_number().
	 * The rowid-function is push-down-safe, because we need only unique ids for
	 * each tuple, and do not care about gaps between numbers.
	 */
	fcRowNumber = makeFuncCall(SystemFuncName("row_id"),
							   NIL,
							   UNKNOWN_LOCATION);
	fcRowNumber->over = wdRowNumber;
	rtRowNumber = makeResTarget((Node *) fcRowNumber, NULL);
	rtRowNumber->name = *nameRN;

	/*
	 * Build sub-select clause with from- and where-clause from the
	 * outer query. Add "row_id() OVER ()" to the target list.
	 */
	ssRowNumber = makeNode(SelectStmt);
	ssRowNumber->fromClause = list_make1(j->larg);
	ssRowNumber->groupClause = NIL;
	ssRowNumber->whereClause = NULL;
	ssRowNumber->targetList = list_make2(rtAStar, rtRowNumber);

	/* Build range sub-select */
	rssRowNumber = makeNode(RangeSubselect);
	rssRowNumber->subquery = (Node *) ssRowNumber;
	rssRowNumber->alias = *largAlias;
	rssRowNumber->lateral = false;

	/* Build resource target for "r.*" */
	crAStarWithR = makeColumnRef2((Node *) makeString((*largAlias)->aliasname),
								  (Node *) makeNode(A_Star));
	rtAStarWithR = makeResTarget((Node *) crAStarWithR, NULL);

	/* Build the outer range sub-select */
	ssJoinLarg = makeNode(SelectStmt);
	ssJoinLarg->fromClause = list_make1(rssRowNumber);
	ssJoinLarg->groupClause = NIL;
	ssJoinLarg->whereClause = NULL;

	/* Build range sub-select */
	rssJoinLarg = makeNode(RangeSubselect);
	rssJoinLarg->subquery = (Node *) ssJoinLarg;
	rssJoinLarg->lateral = false;

	/* Build a join expression */
	joinExpr = makeNode(JoinExpr);
	joinExpr->isNatural = false;
	joinExpr->larg = (Node *) rssRowNumber;
	joinExpr->jointype = JOIN_LEFT; /* left outer join */

	/*
	 * Copy temporal bounds into temporal primitive subquery join in order to
	 * compare temporal bound var types with actual target list var types. We
	 * do this to trigger an error on type mismatch, before a subquery function
	 * fails and triggers an non-meaningful error (as for example, "operator
	 * does not exists, or similar").
	 */
	joinExpr->temporalBounds = copyObject(j->temporalBounds);

	sb1 = makeNode(SortBy);
	sb1->location = UNKNOWN_LOCATION;
	sb1->node = (Node *) makeColumnRef1((Node *) makeString(*nameRN));

	sb2 = makeNode(SortBy);
	sb2->location = UNKNOWN_LOCATION;
	sb2->node = (Node *) makeColumnRef1((Node *) makeString(*nameP1));

	ssResult = makeNode(SelectStmt);
	ssResult->withClause = NULL;
	ssResult->fromClause = list_make1(joinExpr);
	ssResult->targetList = list_make1(rtAStarWithR);
	ssResult->sortClause = list_make2(sb1, sb2);
	ssResult->temporalClause = makeNode(TemporalClause);

	/*
	 * Hardcoded column names for ts and te. We handle ambiguous column
	 * names during the transformation of temporal primitive clauses.
	 */
	ssResult->temporalClause->colnameTr =
			temporalBoundGetName(j->temporalBounds, TPB_LARG_TIME);

	/*
	 * We mark the outer sub-query with the current temporal adjustment type,
	 * s.t. the optimizer understands that we need the corresponding temporal
	 * adjustment node above.
	 */
	ssResult->temporalClause->temporalType =
			j->jointype == TEMPORAL_ALIGN ? TEMPORAL_TYPE_ALIGNER
										  : TEMPORAL_TYPE_NORMALIZER;

	/* Let the join inside a temporal primitive know which type its parent has */
	joinExpr->inTmpPrimTempType = ssResult->temporalClause->temporalType;

	return ssResult;
}

/*
 * transformTemporalAligner -
 * 		transform a TEMPORAL ALIGN clause into standard SQL
 *
 * INPUT:
 * 		(r ALIGN s ON q WITH (r.t, s.t)) c
 *
 *      where r and s are input relations, q can be any
 *      join qualifier, and r.t, s.t can be any column name. The latter
 *      represent the valid time intervals, that is time point start,
 *      and time point end of each tuple for each input relation. These
 *      are two half-open, i.e., [), range typed values.
 *
 * OUTPUT:
 *      (
 * 		SELECT r.*, GREATEST(LOWER(r.t), LOWER(s.t)) P1,
 * 		            LEAST(UPPER(r.t), UPPER(s.t)) P2
 *      FROM
 *      (
 *      	SELECT *, row_id() OVER () rn FROM r
 *      ) r
 *      LEFT OUTER JOIN
 *      s
 *      ON q AND r.t && s.t
 *      ORDER BY rn, P1, P2
 *      ) c
 */
Node *
transformTemporalAligner(ParseState *pstate, JoinExpr *j)
{
	const int 	 		 UNKNOWN_LOCATION = -1;

	SelectStmt			*ssResult;
	RangeSubselect 		*rssResult;
	ResTarget			*rtGreatest;
	ResTarget			*rtLeast;
	ColumnRef 			*crLargTs;
	ColumnRef 			*crRargTs;
	MinMaxExpr			*mmeGreatest;
	MinMaxExpr			*mmeLeast;
	FuncCall			*fcLowerLarg;
	FuncCall			*fcLowerRarg;
	FuncCall			*fcUpperLarg;
	FuncCall			*fcUpperRarg;
	List				*mmeGreatestArgs;
	List				*mmeLeastArgs;
	List				*boundariesExpr;
	JoinExpr			*joinExpr;
	A_Expr				*overlapExpr;
	Node				*boolExpr;
	SortBy				*sb3;
	Alias				*largAlias = NULL;
	Alias				*rargAlias = NULL;
	char 				*colnameRN;
	char 				*colnameP1;
	char 				*colnameP2;

	/* Create a select statement skeleton to be filled here */
	ssResult = makeTemporalQuerySkeleton(j, &colnameRN, &colnameP1,
										 &colnameP2,
										 &largAlias, &rargAlias);

	/* Temporal aligners do not support the USING-clause */
	Assert(j->usingClause == NIL);

	/*
	 * Build column references, for use later. If we need only two range types
	 * only Ts columnrefs are used.
	 */
	crLargTs = temporalBoundGetCopyFQN(j->temporalBounds, TPB_LARG_TIME,
									   largAlias->aliasname);
	crRargTs = temporalBoundGetCopyFQN(j->temporalBounds, TPB_RARG_TIME,
									   rargAlias->aliasname);

	/* Create argument list for function call to "greatest" and "least" */
	fcLowerLarg = makeFuncCall(SystemFuncName("lower"),
							   list_make1(crLargTs),
							   UNKNOWN_LOCATION);
	fcLowerRarg = makeFuncCall(SystemFuncName("lower"),
							   list_make1(crRargTs),
							   UNKNOWN_LOCATION);
	fcUpperLarg = makeFuncCall(SystemFuncName("upper"),
							   list_make1(crLargTs),
							   UNKNOWN_LOCATION);
	fcUpperRarg = makeFuncCall(SystemFuncName("upper"),
							   list_make1(crRargTs),
							   UNKNOWN_LOCATION);
	mmeGreatestArgs = list_make2(fcLowerLarg, fcLowerRarg);
	mmeLeastArgs = list_make2(fcUpperLarg, fcUpperRarg);

	overlapExpr = makeSimpleA_Expr(AEXPR_OP,
								   "&&",
								   (Node *) copyObject(crLargTs),
								   (Node *) copyObject(crRargTs),
								   UNKNOWN_LOCATION);

	boundariesExpr = list_make1(overlapExpr);

	/* Concatenate all Boolean expressions by AND */
	boolExpr = (Node *) makeBoolExpr(AND_EXPR,
									 lappend(boundariesExpr, j->quals),
									 UNKNOWN_LOCATION);

	/* Build the function call "greatest(r.ts, s.ts) P1" */
	mmeGreatest = makeNode(MinMaxExpr);
	mmeGreatest->args = mmeGreatestArgs;
	mmeGreatest->location = UNKNOWN_LOCATION;
	mmeGreatest->op = IS_GREATEST;
	rtGreatest = makeResTarget((Node *) mmeGreatest, NULL);
	rtGreatest->name = colnameP1;

	/* Build the function call "least(r.te, s.te) P2" */
	mmeLeast = makeNode(MinMaxExpr);
	mmeLeast->args = mmeLeastArgs;
	mmeLeast->location = UNKNOWN_LOCATION;
	mmeLeast->op = IS_LEAST;
	rtLeast = makeResTarget((Node *) mmeLeast, NULL);
	rtLeast->name = colnameP2;

	sb3 = makeNode(SortBy);
	sb3->location = UNKNOWN_LOCATION;
	sb3->node = (Node *) makeColumnRef1((Node *) makeString(colnameP2));

	ssResult->targetList = list_concat(ssResult->targetList,
									   list_make2(rtGreatest, rtLeast));
	ssResult->sortClause = lappend(ssResult->sortClause, sb3);

	joinExpr = (JoinExpr *) linitial(ssResult->fromClause);
	joinExpr->rarg = copyObject(j->rarg);
	joinExpr->quals = boolExpr;

	/* Build range sub-select */
	rssResult = makeNode(RangeSubselect);
	rssResult->subquery = (Node *) ssResult;
	rssResult->alias = copyObject(j->alias);
	rssResult->lateral = false;

	return (Node *) copyObject(rssResult);
}

/*
 * transformTemporalNormalizer -
 * 		transform a TEMPORAL NORMALIZE clause into standard SQL
 *
 * INPUT:
 * 		(r NORMALIZE s ON q WITH (r.t, s.t)) c
 *
 * 		-- or --
 *
 * 		(r NORMALIZE s USING(atts) WITH (r.t, s.t)) c
 *
 *      where r and s are input relations, q can be any
 *      join qualifier, atts are a list of column names (like in a
 *      join-using-clause), and r.t, and s.t can be any column name.
 *      The latter represent the valid time intervals, that is time
 *      point start, and time point end of each tuple for each input
 *      relation. These are two half-open, i.e., [), range typed values.
 *
 * OUTPUT:
 * 		(
 * 			SELECT r.*
 *      	FROM
 *      	(
 *      		SELECT *, row_id() OVER () rn FROM r
 *      	) r
 *      	LEFT OUTER JOIN
 *      	(
 *      		SELECT s.*, LOWER(s.t) P1 FROM s
 *      		UNION ALL
 *      		SELECT s.*, UPPER(s.t) P1 FROM s
 *      	) s
 *      	ON q AND P1 <@ t
 *      	ORDER BY rn, P1
 *      ) c
 *
 *      -- or --
 *
 * 		(
 * 			SELECT r.*
 *      	FROM
 *      	(
 *      		SELECT *, row_id() OVER () rn FROM r
 *      	) r
 *      	LEFT OUTER JOIN
 *      	(
 *      		SELECT atts, LOWER(s.t) P1 FROM s
 *      		UNION
 *      		SELECT atts, UPPER(s.t) P1 FROM s
 *      	) s
 *      	ON r.atts = s.atts AND P1 <@ t
 *      	ORDER BY rn, P1
 *      ) c
 *
 */
Node *
transformTemporalNormalizer(ParseState *pstate, JoinExpr *j)
{
	const int 	 		 UNKNOWN_LOCATION = -1;

	SelectStmt			*ssTsP1;
	SelectStmt			*ssTeP1;
	SelectStmt			*ssUnionAll;
	SelectStmt			*ssResult;
	RangeSubselect 		*rssUnionAll;
	RangeSubselect 		*rssResult;
	ResTarget			*rtRargStar;
	ResTarget			*rtTsP1;
	ResTarget			*rtTeP1;
	ColumnRef 			*crRargStar;
	ColumnRef 			*crLargTsT = NULL;
	ColumnRef 			*crRargTsT = NULL;
	ColumnRef 			*crP1;
	JoinExpr			*joinExpr;
	A_Expr				*containsExpr;
	Node				*boolExpr;
	Alias				*largAlias;
	Alias				*rargAlias;
	char 				*colnameRN;
	char 				*colnameP1;
	FuncCall			*fcLowerRarg = NULL;
	FuncCall			*fcUpperRarg = NULL;
	List				*boundariesExpr;

	/* Create a select statement skeleton to be filled here */
	ssResult = makeTemporalQuerySkeleton(j, &colnameRN, &colnameP1,
										 NULL, &largAlias, &rargAlias);

	/* Build resource target for "s.*" to use it later. */
	crRargStar = makeColumnRef2((Node *) makeString(rargAlias->aliasname),
								(Node *) makeNode(A_Star));

	crP1 = makeColumnRef1((Node *) makeString(colnameP1));

	/* Build column references, for use later. */
	crLargTsT = temporalBoundGetCopyFQN(j->temporalBounds, TPB_LARG_TIME,
									   largAlias->aliasname);
	crRargTsT = temporalBoundGetCopyFQN(j->temporalBounds, TPB_RARG_TIME,
									   rargAlias->aliasname);

	/* Create argument list for function call to "lower" and "upper" */
	fcLowerRarg = makeFuncCall(SystemFuncName("lower"),
							   list_make1(crRargTsT),
							   UNKNOWN_LOCATION);
	fcUpperRarg = makeFuncCall(SystemFuncName("upper"),
							   list_make1(crRargTsT),
							   UNKNOWN_LOCATION);

	/* Build resource target "lower(s.t) P1" and "upper(s.t) P1" */
	rtTsP1 = makeResTarget((Node *) fcLowerRarg, colnameP1);
	rtTeP1 = makeResTarget((Node *) fcUpperRarg, colnameP1);

	/*
	 * Build "contains" expression for range types, i.e. "P1 <@ t"
	 * and concatenate it with q (=theta)
	 */
	containsExpr = makeSimpleA_Expr(AEXPR_OP,
									"<@",
									(Node *) copyObject(crP1),
									(Node *) copyObject(crLargTsT),
									UNKNOWN_LOCATION);

	boundariesExpr = list_make1(containsExpr);

	/*
	 * For ON-clause notation build
	 * 		"SELECT s.*, lower(t) P1 FROM s", and
	 * 		"SELECT s.*, upper(t) P1 FROM s".
	 * For USING-clause with a name-list 'atts' build
	 * 		"SELECT atts, lower(t) P1 FROM s", and
	 * 		"SELECT atts, upper(t) P1 FROM s".
	 */
	ssTsP1 = makeNode(SelectStmt);
	ssTsP1->fromClause = list_make1(j->rarg);
	ssTsP1->groupClause = NIL;
	ssTsP1->whereClause = NULL;

	ssTeP1 = copyObject(ssTsP1);

	if (j->usingClause)
	{
		ListCell   *usingItem;
		A_Expr     *expr;
		List	   *qualList = NIL;
		char	   *colnameTr = ssResult->temporalClause->colnameTr;

		Assert(j->quals == NULL); 	/* shouldn't have ON() too */

		foreach(usingItem, j->usingClause)
		{
			char		*usingItemName = strVal(lfirst(usingItem));
			ColumnRef   *crUsingItemL =
					makeColumnRef2((Node *) makeString(largAlias->aliasname),
								   (Node *) makeString(usingItemName));
			ColumnRef   *crUsingItemR =
					makeColumnRef2((Node *) makeString(rargAlias->aliasname),
								   (Node *) makeString(usingItemName));
			ResTarget	*rtUsingItemR = makeResTarget((Node *) crUsingItemR,
													  NULL);

			/*
			 * Skip temporal attributes, because temporal normalizer's USING
			 * list must contain only non-temporal attributes. We allow
			 * temporal attributes as input, such that we can copy colname lists
			 * to create temporal normalizers easier.
			 */
			if(colnameTr && strcmp(usingItemName, colnameTr) == 0)
				continue;

			expr = makeSimpleA_Expr(AEXPR_OP,
									 "=",
									 (Node *) copyObject(crUsingItemL),
									 (Node *) copyObject(crUsingItemR),
									 UNKNOWN_LOCATION);

			qualList = lappend(qualList, expr);

			ssTsP1->targetList = lappend(ssTsP1->targetList, rtUsingItemR);
			ssTeP1->targetList = lappend(ssTeP1->targetList, rtUsingItemR);
		}

		j->quals = (Node *) makeBoolExpr(AND_EXPR, qualList, UNKNOWN_LOCATION);
	}
	else if (j->quals)
	{
		rtRargStar = makeResTarget((Node *) crRargStar, NULL);
		ssTsP1->targetList = list_make1(rtRargStar);
		ssTeP1->targetList = list_make1(rtRargStar);
	}

	ssTsP1->targetList = lappend(ssTsP1->targetList, rtTsP1);
	ssTeP1->targetList = lappend(ssTeP1->targetList, rtTeP1);

	/*
	 * Build sub-select for "( SELECT ... UNION [ALL] SELECT ... ) s", i.e.,
	 * build an union between two select-clauses, i.e. a select-clause with
	 * set-operation set to "union".
	 */
	ssUnionAll = makeNode(SelectStmt);
	ssUnionAll->op = SETOP_UNION;
	ssUnionAll->all = j->usingClause == NIL;	/* true, if ON-clause */
	ssUnionAll->larg = ssTsP1;
	ssUnionAll->rarg = ssTeP1;

	/* Build range sub-select for "( ...UNION [ALL]... ) s" */
	rssUnionAll = makeNode(RangeSubselect);
	rssUnionAll->subquery = (Node *) ssUnionAll;
	rssUnionAll->alias = rargAlias;
	rssUnionAll->lateral = false;

	/*
	 * Create a conjunction of all Boolean expressions
	 */
	if (j->quals)
	{
		boolExpr = (Node *) makeBoolExpr(AND_EXPR,
										 lappend(boundariesExpr, j->quals),
										 UNKNOWN_LOCATION);
	}
	else	/* empty USING() clause found, i.e. theta = true */
	{
		boolExpr = (Node *) makeBoolExpr(AND_EXPR,
										 boundariesExpr,
										 UNKNOWN_LOCATION);
		ssUnionAll->all = false;

	}

	joinExpr = (JoinExpr *) linitial(ssResult->fromClause);
	joinExpr->rarg = (Node *) rssUnionAll;
	joinExpr->quals = boolExpr;

	/* Build range sub-select */
	rssResult = makeNode(RangeSubselect);
	rssResult->subquery = (Node *) ssResult;
	rssResult->alias = copyObject(j->alias);
	rssResult->lateral = false;

	return (Node *) copyObject(rssResult);
}

/*
 * typeGet -
 * 		Return the type of a tuple from the system cache for a given OID.
 */
static Form_pg_type
typeGet(Oid id)
{
	HeapTuple	tp;
	Form_pg_type typtup;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(id));
	if (!HeapTupleIsValid(tp))
		ereport(ERROR,
				(errcode(ERROR),
				 errmsg("cache lookup failed for type %u", id)));

	typtup = (Form_pg_type) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	return typtup;
}

/*
 * internalUseOnlyColumnNames -
 * 		Creates a list of all internal-use-only column names, depending on the
 * 		temporal primitive type (i.e., normalizer or aligner). The list is then
 * 		compared with the aliases from the current parser state, and renamed
 * 		if necessary.
 */
static List *
internalUseOnlyColumnNames(ParseState *pstate,
						   TemporalType tmpType)
{
	List		*filter = NIL;
	ListCell	*lcFilter;
	ListCell	*lcAlias;

	filter = list_make2(makeString("rn"), makeString("p1"));

	if(tmpType == TEMPORAL_TYPE_ALIGNER)
		filter = lappend(filter, makeString("p2"));

	foreach(lcFilter, filter)
	{
		Value	*filterValue = (Value *) lfirst(lcFilter);
		char	*filterName = strVal(filterValue);

		foreach(lcAlias, pstate->p_temporal_aliases)
		{
			char 	*aliasKey 	= strVal(linitial((List *) lfirst(lcAlias)));
			char 	*aliasValue = strVal(lsecond((List *) lfirst(lcAlias)));

			if(strcmp(filterName, aliasKey) == 0)
				filterValue->val.str = pstrdup(aliasValue);
		}
	}

	return filter;
}

/*
 * temporalBoundCheckIntegrity -
 * 		For each column name check if it is a temporal bound. If so, check
 * 		also if it does not clash with an internal-use-only column name, and if
 * 		the attribute types match with the range type predicate.
 */
static void
temporalBoundCheckIntegrity(ParseState *pstate,
							 List *bounds,
							 List *colnames,
							 List *colvars,
							 TemporalType tmpType)
{
	ListCell 	*lcNames;
	ListCell 	*lcVars;
	ListCell 	*lcBound;
	ListCell 	*lcFilter;
	List		*filter = internalUseOnlyColumnNames(pstate, tmpType);

	forboth(lcNames, colnames, lcVars, colvars)
	{
		char *name = strVal((Value *) lfirst(lcNames));
		Var	 *var  = (Var *) lfirst(lcVars);

		foreach(lcBound, bounds)
		{
			ColumnRef 	*crb = (ColumnRef *) lfirst(lcBound);
			char 		*nameb = strVal((Value *) llast(crb->fields));

			if(strcmp(nameb, name) == 0)
			{
				char 				*msg = "";
				Form_pg_type		 type;

				foreach(lcFilter, filter)
				{
					char	*n = strVal((Value *) lfirst(lcFilter));
					if(strcmp(n, name) == 0)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("column \"%s\" does not exist", n),
								 parser_errposition(pstate, crb->location)));
				}

				type = typeGet(var->vartype);

				if(type->typtype != TYPTYPE_RANGE)
					msg = "Invalid column type \"%s\" for the temporal bound " \
						  "\"%s\". It must be a range type column.";

				if (strlen(msg) > 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
							 errmsg(msg,
									NameStr(type->typname),
									NameListToString(crb->fields)),
							 errhint("Specify two range-typed columns."),
							 parser_errposition(pstate, crb->location)));

			}
		}
	}

}


/*
 * transformTemporalClauseAmbiguousColumns -
 * 		Rename columns automatically to unique not-in-use column names, if
 * 		column names clash with internal-use-only columns of temporal
 * 		primitives.
 */
void
transformTemporalClauseAmbiguousColumns(ParseState* pstate, JoinExpr* j,
										List* l_colnames, List* r_colnames,
										List *l_colvars, List *r_colvars,
										RangeTblEntry* l_rte,
										RangeTblEntry* r_rte)
{
	ListCell   *l = NULL;
	bool 		foundP1 = false;
	bool 		foundP2 = false;
	bool 		foundRN = false;
	int 		counterP1 = -1;
	int 		counterP2 = -1;
	int 		counterRN = -1;

	/* Nothing to do, if we have no temporal primitive */
	if (j->inTmpPrimTempType == TEMPORAL_TYPE_NONE)
		return;

	/*
	 * Check ambiguity of column names, search for p1, p2, and rn
	 * columns and rename them accordingly to X_N, where X = {p1,p2,rn},
	 * and N is the highest number after X_ starting from 0. This is, if we do
	 * not find any X_N column pattern the new column is renamed to X_0.
	 */
	foreach(l, l_colnames)
	{
		const char *colname = strVal((Value *) lfirst(l));

		/*
		 * Skip the last entry of the left column names, i.e. row_id
		 * is only an internally added column by both temporal
		 * primitives.
		 */
		if (l == list_tail(l_colnames))
			continue;

		getColumnCounter(colname, "p1", &foundP1, &counterP1);
		getColumnCounter(colname, "rn", &foundRN, &counterRN);

		/* Only temporal aligners have a p2 column */
		if (j->inTmpPrimTempType == TEMPORAL_TYPE_ALIGNER)
			getColumnCounter(colname, "p2", &foundP2, &counterP2);
	}

	foreach(l, r_colnames)
	{
		const char *colname = strVal((Value *) lfirst(l));

		/*
		 * The temporal normalizer adds also a column called p1 which is
		 * the union of te and ts interval boundaries. We ignore it here
		 * since it does not belong to the user defined columns of the
		 * given input, iff it is the last entry of the column list.
		 */
		if (j->inTmpPrimTempType == TEMPORAL_TYPE_NORMALIZER
				&& l == list_tail(r_colnames))
			continue;

		getColumnCounter(colname, "p1", &foundP1, &counterP1);
		getColumnCounter(colname, "rn", &foundRN, &counterRN);

		/* Only temporal aligners have a p2 column */
		if (j->inTmpPrimTempType == TEMPORAL_TYPE_ALIGNER)
			getColumnCounter(colname, "p2", &foundP2, &counterP2);
	}

	if (foundP1)
	{
		char *name = addTemporalAlias(pstate, "p1", counterP1);

		/*
		 * The right subtree gets now a new name for the column p1.
		 * In addition, we rename both expressions used for temporal
		 * boundary checks. It is fixed that they are at the end of this
		 * join's qualifier list.
		 * Only temporal normalization needs these steps.
		 */
		if (j->inTmpPrimTempType == TEMPORAL_TYPE_NORMALIZER)
		{
			A_Expr *e1;
			List *qualArgs;

			llast(r_rte->eref->colnames) = makeString(name);
			llast(r_colnames) = makeString(name);

			qualArgs = ((BoolExpr *) j->quals)->args;
			e1 = (A_Expr *) linitial(qualArgs);
			linitial(((ColumnRef *)e1->lexpr)->fields) = makeString(name);
		}
	}

	if (foundRN)
	{
		char *name = addTemporalAlias(pstate, "rn", counterRN);

		/* The left subtree has now a new name for the column rn */
		llast(l_rte->eref->colnames) = makeString(name);
		llast(l_colnames) = makeString(name);
	}

	if (foundP2)
		addTemporalAlias(pstate, "p2", counterP2);

	temporalBoundCheckIntegrity(pstate,
								temporalBoundGetLeftBounds(j->temporalBounds),
								l_colnames, l_colvars, j->inTmpPrimTempType);


	temporalBoundCheckIntegrity(pstate,
								temporalBoundGetRightBounds(j->temporalBounds),
								r_colnames, r_colvars, j->inTmpPrimTempType);

}

/*
 * makeTemporalNormalizer -
 *		Creates a temporal normalizer join expression.
 *		XXX PEMOSER Should we create a separate temporal primitive expression?
 */
JoinExpr *
makeTemporalNormalizer(Node *larg, Node *rarg, List *bounds, Node *quals,
					   Alias *alias)
{
	JoinExpr *j = makeNode(JoinExpr);

	if(! ((IsA(larg, RangeSubselect) || IsA(larg, RangeVar)) &&
		  (IsA(rarg, RangeSubselect) || IsA(rarg, RangeVar))))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("Normalizer arguments must be of type RangeVar or " \
					   "RangeSubselect.")));

	j->jointype = TEMPORAL_NORMALIZE;

	/*
	 * Qualifiers can be an boolean expression or an USING clause, i.e. a list
	 * of column names.
	 */
	if(quals == (Node *) NIL || IsA(quals, List))
		j->usingClause = (List *) quals;
	else
		j->quals = quals;

	j->larg = larg;
	j->rarg = rarg;
	j->alias = alias;
	j->temporalBounds = bounds;

	return j;
}

/*
 * makeTemporalAligner -
 *		Creates a temporal aligner join expression.
 *		XXX PEMOSER Should we create a separate temporal primitive expression?
 */
JoinExpr *
makeTemporalAligner(Node *larg, Node *rarg, List *bounds, Node *quals,
					Alias *alias)
{
	JoinExpr *j = makeNode(JoinExpr);

	if(! ((IsA(larg, RangeSubselect) || IsA(larg, RangeVar)) &&
		  (IsA(rarg, RangeSubselect) || IsA(rarg, RangeVar))))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("Aligner arguments must be of type RangeVar or " \
					   "RangeSubselect.")));

	j->jointype = TEMPORAL_ALIGN;

	/* Empty quals allowed (i.e., NULL), but no LISTS */
	if(quals && IsA(quals, List))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("Aligner do not support an USING clause.")));
	else
		j->quals = quals;

	j->larg = larg;
	j->rarg = rarg;
	j->alias = alias;
	j->temporalBounds = bounds;

	return j;
}

