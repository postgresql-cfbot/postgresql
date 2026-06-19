/*-------------------------------------------------------------------------
 *
 * rpr.h
 *	  Row Pattern Recognition pattern compilation for planner
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/rpr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPTIMIZER_RPR_H
#define OPTIMIZER_RPR_H

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

/* Limits and special values */
/*
 * Maximum pattern variable ID.  Pattern variables occupy varId 0 to
 * RPR_VARID_MAX inclusive (240 distinct variables); any varId with the high
 * nibble set (0xF0 to 0xFF) is reserved for control elements.  Reserving the
 * whole high nibble, rather than just the values currently in use, leaves
 * room for future control elements; this range can only be narrowed safely
 * before release.
 */
#define RPR_VARID_MAX		0xEF	/* pattern variables are 0 to 0xEF */
#define RPR_QUANTITY_INF	PG_INT32_MAX	/* unbounded quantifier */
#define RPR_COUNT_MAX		PG_INT32_MAX	/* max runtime count (NFA state) */
#define RPR_ELEMIDX_MAX		PG_INT16_MAX	/* max pattern elements */
#define RPR_ELEMIDX_INVALID	((RPRElemIdx) -1)	/* invalid index */
#define RPR_DEPTH_MAX		(PG_UINT8_MAX - 1)	/* max pattern nesting depth:
												 * 254 */
#define RPR_DEPTH_NONE		PG_UINT8_MAX	/* no enclosing group (top-level) */

/* Reserved control-element varIds (high nibble 0xF; 0xF0-0xFB spare) */
#define RPR_VARID_BEGIN		((RPRVarId) 0xFC)	/* group begin */
#define RPR_VARID_END		((RPRVarId) 0xFD)	/* group end */
#define RPR_VARID_ALT		((RPRVarId) 0xFE)	/* alternation start */
#define RPR_VARID_FIN		((RPRVarId) 0xFF)	/* pattern finish */

/* Element flags */
#define RPR_ELEM_RELUCTANT			0x01	/* reluctant (non-greedy)
											 * quantifier */
#define RPR_ELEM_EMPTY_LOOP			0x02	/* END: group body can produce
											 * empty match */
/*
 * The two absorption flags below are explained in README.rpr IV-5
 * ("Absorbability Analysis"), with worked examples in Appendix C; the
 * analysis that sets them is computeAbsorbability() in
 * optimizer/plan/rpr.c.
 */
#define RPR_ELEM_ABSORBABLE_BRANCH	0x04	/* element in absorbable region */
#define RPR_ELEM_ABSORBABLE			0x08	/* absorption judgment point */

/* Accessor macros for RPRPatternElement */
#define RPRElemIsReluctant(e)			(((e)->flags & RPR_ELEM_RELUCTANT) != 0)
#define RPRElemCanEmptyLoop(e)			(((e)->flags & RPR_ELEM_EMPTY_LOOP) != 0)
#define RPRElemIsAbsorbableBranch(e)	(((e)->flags & RPR_ELEM_ABSORBABLE_BRANCH) != 0)
#define RPRElemIsAbsorbable(e)			(((e)->flags & RPR_ELEM_ABSORBABLE) != 0)
#define RPRElemIsVar(e)			((e)->varId <= RPR_VARID_MAX)
#define RPRElemIsBegin(e)		((e)->varId == RPR_VARID_BEGIN)
#define RPRElemIsEnd(e)			((e)->varId == RPR_VARID_END)
#define RPRElemIsAlt(e)			((e)->varId == RPR_VARID_ALT)
#define RPRElemIsFin(e)			((e)->varId == RPR_VARID_FIN)
#define RPRElemCanSkip(e)		((e)->min == 0)

extern List *collectPatternVariables(RPRPatternNode *pattern);
extern void validate_rpr_define_volatility(List *defineClause);
extern void buildDefineVariableList(List *defineClause,
									List **defineVariableList);
extern RPRPattern *buildRPRPattern(RPRPatternNode *pattern, List *defineVariableList,
								   RPSkipTo rpSkipTo, int frameOptions,
								   bool hasMatchStartDependent);

/*
 * Shared traversal walker for DEFINE clause RPRNavExpr collection.
 *
 * Both planner (nav-offset / match_start dependency analysis) and executor
 * (runtime offset evaluation) need to walk DEFINE expressions and dispatch
 * per RPRNavExpr.  They differ only in what they do at each nav node, so
 * the traversal frame is shared (nav_traversal_walker, defined in rpr.c)
 * and the per-nav action is supplied as a callback.  The driver allocates
 * a mode-specific context, points NavTraversal.data at it, and casts
 * inside its visitor.
 */
struct NavTraversal;
typedef void (*NavVisitFn) (struct NavTraversal *t, RPRNavExpr *nav);

typedef struct NavTraversal
{
	NavVisitFn	visit;
	void	   *data;			/* mode-specific context */
} NavTraversal;

extern bool nav_traversal_walker(Node *node, void *ctx);

#endif							/* OPTIMIZER_RPR_H */
