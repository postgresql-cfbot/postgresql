/*-------------------------------------------------------------------------
 *
 * execMerge.c
 *	  routines to handle Merge nodes relating to the MERGE command
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execMerge.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/execMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static bool ExecMergeMatched(ModifyTableState *mtstate,
							 ResultRelInfo *resultRelInfo,
							 EState *estate,
							 TupleTableSlot *slot,
							 ItemPointer tupleid);
static void ExecMergeNotMatched(ModifyTableState *mtstate,
								ResultRelInfo *resultRelInfo,
								EState *estate,
								TupleTableSlot *slot);

/*
 * Perform MERGE.
 */
TupleTableSlot *
ExecMerge(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
		  EState *estate, TupleTableSlot *slot)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	bool		matched = false;
	Datum		datum;
	bool		isNull;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * We run a JOIN between the target relation and the source relation to
	 * find a set of candidate source rows that have a matching row in the
	 * target table and a set of candidate source rows that do not have a
	 * matching row in the target table. If the join returns a tuple with the
	 * target relation's row-ID set, that implies that the join found a
	 * matching row for the given source tuple. This case triggers the WHEN
	 * MATCHED clause of the MERGE. Whereas a NULL in the target relation's
	 * row-ID column indicates a NOT MATCHED case.
	 */
	datum = ExecGetJunkAttribute(slot,
								 resultRelInfo->ri_RowIdAttNo,
								 &isNull);

	if (!isNull)
	{
		matched = true;
		tuple_ctid = *((ItemPointer) DatumGetPointer(datum));
		tupleid = &tuple_ctid;
	}
	else
	{
		matched = false;
		tupleid = NULL;			/* we don't need it for NOT MATCHED actions */
	}

	/*-----
	 * If we are dealing with a WHEN MATCHED case, we execute the first
	 * action for which the additional WHEN MATCHED AND quals pass.
	 * If an action without quals is found, that action is executed.
	 *
	 * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at
	 * the given WHEN NOT MATCHED actions in sequence until one passes.
	 *
	 * Things get interesting in case of concurrent update/delete of the
	 * target tuple. Such concurrent update/delete is detected while we are
	 * executing a WHEN MATCHED action.
	 *
	 * A concurrent update can:
	 *
	 * 1. modify the target tuple so that it no longer satisfies the
	 *    additional quals attached to the current WHEN MATCHED action
	 *
	 *    In this case, we are still dealing with a WHEN MATCHED case.
	 *    In this case, we recheck the list of WHEN MATCHED actions from
	 *    the start and choose the first one that satisfies the new target
	 *    tuple.
	 *
	 * 2. modify the target tuple so that the join quals no longer pass and
	 *    hence the source tuple no longer has a match.
	 *
	 *    In this case, the source tuple no longer matches the target tuple,
	 *    so we now instead find a qualifying WHEN NOT MATCHED action to
	 *    execute.
	 *
	 * XXX Hmmm, what if the updated tuple would now match one that was
	 * considered NOT MATCHED so far?
	 *
	 * A concurrent delete changes a WHEN MATCHED case to WHEN NOT MATCHED.
	 *
	 * ExecMergeMatched takes care of following the update chain and
	 * re-finding the qualifying WHEN MATCHED action, as long as the updated
	 * target tuple still satisfies the join quals, i.e., it remains a WHEN
	 * MATCHED case. If the tuple gets deleted or the join quals fail, it
	 * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
	 * always make progress by following the update chain and we never switch
	 * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
	 * livelock.
	 */
	if (matched)
		matched = ExecMergeMatched(mtstate, resultRelInfo, estate,
								   slot, tupleid);

	/*
	 * Either we were dealing with a NOT MATCHED tuple or ExecMergeMatched()
	 * returned "false", indicating the previously MATCHED tuple no longer
	 * matches.
	 */
	if (!matched)
		ExecMergeNotMatched(mtstate, resultRelInfo, estate, slot);

	/* No RETURNING support yet */
	return NULL;
}

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN quals
 * pass, if any. If the WHEN quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated by this routine via ExecQual, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false --
 * meaning that a NOT MATCHED action must now be executed for the current
 * source tuple.
 */
static bool
ExecMergeMatched(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
				 EState *estate, TupleTableSlot *slot,
				 ItemPointer tupleid)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	bool		isNull;
	EPQState   *epqstate = &mtstate->mt_epqstate;
	ListCell   *l;

	/*
	 * If there are no WHEN MATCHED actions, we are done.
	 */
	if (resultRelInfo->ri_matchedMergeAction == NIL)
		return true;

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * Again, this target relation's slot is required only in the case of a
	 * MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

lmerge_matched:

	/*
	 * UPDATE/DELETE is only invoked for matched rows. And we must have found
	 * the tupleid of the target row in that case. We fetch using SnapshotAny
	 * because we might get called again after EvalPlanQual returns us a new
	 * tuple. This tuple may not be visible to our MVCC snapshot.
	 */
	Assert(tupleid != NULL);

	if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
									   tupleid,
									   SnapshotAny,
									   resultRelInfo->ri_oldTupleSlot))
		elog(ERROR, "failed to fetch the target tuple");

	foreach(l, resultRelInfo->ri_matchedMergeAction)
	{
		MergeActionState *relaction = (MergeActionState *) lfirst(l);
		CmdType		commandType = relaction->mas_action->commandType;
		ExecMergeActionInfo actionInfo = {0};

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since ExecQual() will
		 * return true if there are no conditions to evaluate).
		 */
		if (!ExecQual(relaction->mas_whenqual, econtext))
			continue;

		/*
		 * Check if the existing target tuple meets the USING checks of
		 * UPDATE/DELETE RLS policies. If those checks fail, we throw an
		 * error.
		 *
		 * The WITH CHECK quals are applied in ExecUpdate() and hence we need
		 * not do anything special to handle them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated, so that we
		 * check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions)
		{
			ExecWithCheckOptions(commandType == CMD_UPDATE ?
								 WCO_RLS_MERGE_UPDATE_CHECK : WCO_RLS_MERGE_DELETE_CHECK,
								 resultRelInfo,
								 resultRelInfo->ri_oldTupleSlot,
								 mtstate->ps.state);
		}

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_UPDATE:

				/*
				 * Project the output tuple, into ->ri_newTupleSlot, and use
				 * that to update the table.  We don't need to filter out junk
				 * attributes, because the UPDATE action's targetlist doesn't
				 * have any.
				 */
				ExecProject(relaction->mas_proj);
				actionInfo.actionState = relaction;
				slot = ExecUpdate(mtstate, resultRelInfo,
								  tupleid, NULL,
								  resultRelInfo->ri_newTupleSlot,
								  slot, epqstate, estate,
								  &actionInfo,
								  mtstate->canSetTag);
				break;

			case CMD_DELETE:
				actionInfo.actionState = relaction;
				slot = ExecDelete(mtstate, resultRelInfo,
								  tupleid, NULL,
								  slot, epqstate, estate,
								  false,
								  mtstate->canSetTag,
								  false /* changingPart */ ,
								  NULL,
								  &actionInfo,
								  NULL /* epqslot */ );

				break;

			case CMD_NOTHING:
				/* Do nothing */
				break;

			default:
				elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");

		}

		/*
		 * Check for any concurrent update/delete operation which may have
		 * prevented our update/delete. We also check for situations where we
		 * might be trying to update/delete the same tuple twice.
		 */
		if ((commandType == CMD_UPDATE && !actionInfo.updated) ||
			(commandType == CMD_DELETE && !actionInfo.deleted))
		{
			switch (actionInfo.result)
			{
				case TM_Ok:
					/* all good */
					break;
				case TM_Invisible:

					/*
					 * This state should never be reached since the underlying
					 * JOIN runs with a MVCC snapshot and EvalPlanQual runs
					 * with a dirty snapshot. So such a row should have never
					 * been returned for MERGE.
					 */
					elog(ERROR, "unexpected invisible tuple");
					break;

				case TM_SelfModified:

					/*
					 * The SQL standard disallows this for MERGE.
					 */
					if (TransactionIdIsCurrentTransactionId(actionInfo.failureData.xmax))
						ereport(ERROR,
								(errcode(ERRCODE_CARDINALITY_VIOLATION),
								 errmsg("MERGE command cannot affect row a second time"),
								 errhint("Ensure that not more than one source row matches any one target row.")));
					/* This shouldn't happen */
					elog(ERROR, "attempted to update or delete invisible tuple");
					break;

				case TM_Updated:
				case TM_Deleted:

					/*
					 * The target tuple was concurrently updated/deleted by
					 * some other transaction.
					 *
					 * If the current tuple is the last tuple in the update
					 * chain, then we know that the tuple was concurrently
					 * deleted. Just return and let the caller try NOT MATCHED
					 * actions.
					 *
					 * If the current tuple was concurrently updated, then we
					 * must run the EvalPlanQual() with the new version of the
					 * tuple. If EvalPlanQual() does not return a tuple, then
					 * we switch to the NOT MATCHED list of actions. If it
					 * does return a tuple and the join qual is still
					 * satisfied, then we just need to recheck the MATCHED
					 * actions, starting from the top, and execute the first
					 * qualifying action.
					 */
					if (!ItemPointerEquals(tupleid, &actionInfo.failureData.ctid))
					{
						Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
						TupleTableSlot *epqslot,
								   *inputslot;
						TM_Result	result;
						int			lockmode = ExecUpdateLockMode(estate, resultRelInfo);

						inputslot = EvalPlanQualSlot(epqstate, resultRelationDesc,
													 resultRelInfo->ri_RangeTableIndex);

						result = table_tuple_lock(resultRelationDesc, tupleid,
												  estate->es_snapshot,
												  inputslot, estate->es_output_cid,
												  lockmode, LockWaitBlock,
												  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
												  &actionInfo.failureData);
						switch (result)
						{
							case TM_Ok:
								epqslot = EvalPlanQual(epqstate,
													   resultRelationDesc,
													   resultRelInfo->ri_RangeTableIndex,
													   inputslot);

								/*
								 * If we got no tuple, or the tuple we get has
								 * a NULL ctid, go back to caller: this one is
								 * not a MATCHED tuple anymore, so they can
								 * retry with NOT MATCHED actions.
								 */
								if (TupIsNull(epqslot))
									return false;

								(void) ExecGetJunkAttribute(epqslot,
															resultRelInfo->ri_RowIdAttNo,
															&isNull);
								if (isNull)
									return false;

								/*
								 * When a tuple was updated and migrated to
								 * another partition concurrently, the current
								 * MERGE implementation can't follow.  There's
								 * probably a better way to handle this case,
								 * but it'd require recognizing the relation
								 * to which the tuple moved, and setting our
								 * current resultRelInfo to that.
								 */
								if (ItemPointerIndicatesMovedPartitions(&actionInfo.failureData.ctid))
									ereport(ERROR,
											(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
											 errmsg("tuple to be deleted was already moved to another partition due to concurrent update")));

								/*
								 * A non-NULL ctid means that we are still
								 * dealing with MATCHED case. Restart the loop
								 * so that we apply all the MATCHED rules
								 * again, to ensure that the first qualifying
								 * WHEN MATCHED action is executed.
								 *
								 * Update *tupleid to that of the new tuple,
								 * for the refetch we do at the top.
								 */
								*tupleid = actionInfo.failureData.ctid;
								goto lmerge_matched;

							case TM_Deleted:

								/*
								 * tuple already deleted; tell caller to run
								 * NOT MATCHED actions
								 */
								return false;

							case TM_SelfModified:

								/*
								 * This can be reached when following an
								 * update chain from a tuple updated by
								 * another session, reaching a tuple that was
								 * already updated in this transaction. If
								 * previously modified by this command, ignore
								 * the redundant update, otherwise error out.
								 *
								 * See also response to TM_SelfModified in
								 * ExecUpdate().
								 */
								if (actionInfo.failureData.cmax != estate->es_output_cid)
									ereport(ERROR,
											(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
											 errmsg("tuple to be updated or deleted was already modified by an operation triggered by the current command"),
											 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
								return false;

							default:
								/* see table_tuple_lock call in ExecDelete() */
								elog(ERROR, "unexpected table_tuple_lock status: %u",
									 result);
								return false;
						}

					}

					/*
					 * Tell the caller about the updated TID, restore the
					 * state back and return.
					 */
					*tupleid = actionInfo.failureData.ctid;
					return false;

				default:
					break;

			}
		}

		if (commandType == CMD_UPDATE && actionInfo.updated)
			InstrCountFiltered2(&mtstate->ps, 1);
		if (commandType == CMD_DELETE && actionInfo.deleted)
			InstrCountFiltered3(&mtstate->ps, 1);

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimization.
		 */
		break;
	}

	/*
	 * Successfully executed an action or no qualifying action was found.
	 */
	return true;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static void
ExecMergeNotMatched(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
					EState *estate, TupleTableSlot *slot)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	ResultRelInfo *rootRelInfo = mtstate->rootResultRelInfo;
	TupleTableSlot *insert_slot;
	List	   *actionStates = NIL;
	ListCell   *l;

	/*
	 * For INSERT actions, the root relation's merge action is OK since the
	 * INSERT's targetlist and the WHEN conditions can only refer to the
	 * source relation and hence it does not matter which result relation we
	 * work with.
	 *
	 * XXX does this mean that we can avoid creating copies of actionStates on
	 * partitioned tables, for not-matched actions?
	 */
	actionStates = resultRelInfo->ri_notMatchedMergeAction;

	/*
	 * Make source tuple available to ExecQual and ExecProject. We don't need
	 * the target tuple, since the WHEN quals and the targetlist can't refer
	 * to the target columns.
	 */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	foreach(l, actionStates)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType		commandType = action->mas_action->commandType;
		ExecMergeActionInfo actionInfo = {0};

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since ExecQual() will
		 * return true if there are no conditions to evaluate).
		 */
		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_INSERT:

				/*
				 * Project the tuple.  In case of a partitioned table, the
				 * projection was already built to use the root's descriptor,
				 * so we don't need to map the tuple here.
				 */
				actionInfo.actionState = action;
				insert_slot = ExecProject(action->mas_proj);

				(void) ExecInsert(mtstate, rootRelInfo,
								  insert_slot, slot,
								  estate, &actionInfo,
								  mtstate->canSetTag);
				InstrCountFiltered1(&mtstate->ps, 1);
				break;
			case CMD_NOTHING:
				/* Do nothing */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		break;
	}
}

void
ExecInitMerge(ModifyTableState *mtstate, EState *estate)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	ResultRelInfo *rootRelInfo = mtstate->rootResultRelInfo;
	ResultRelInfo *resultRelInfo;
	ExprContext *econtext;
	ListCell   *lc;
	int			i;

	if (node->mergeActionLists == NIL)
		return;

	mtstate->mt_merge_subcommands = 0;

	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);
	econtext = mtstate->ps.ps_ExprContext;

	/*
	 * Create a MergeActionState for each action on the mergeActionList and
	 * add it to either a list of matched actions or not-matched actions.
	 *
	 * Similar logic appears in ExecInitPartitionInfo(), so if changing
	 * anything here, do so there too.
	 */
	i = 0;
	foreach(lc, node->mergeActionLists)
	{
		List	   *mergeActionList = lfirst(lc);
		TupleDesc	relationDesc;
		ListCell   *l;

		resultRelInfo = mtstate->resultRelInfo + i;
		i++;
		relationDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* initialize slot for MERGE fetches from this rel */
		if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
			ExecInitMergeProjection(mtstate, resultRelInfo);

		foreach(l, mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);
			MergeActionState *action_state;
			TupleTableSlot *tgtslot;
			TupleDesc	tgtdesc;
			List	  **list;

			/*
			 * Build action merge state for this rel.  (For partitions,
			 * equivalent code exists in ExecInitPartitionInfo.)
			 */
			action_state = makeNode(MergeActionState);
			action_state->mas_action = action;
			action_state->mas_whenqual = ExecInitQual((List *) action->qual,
													  &mtstate->ps);

			/*
			 * We create two lists - one for WHEN MATCHED actions and one for
			 * WHEN NOT MATCHED actions - and stick the MergeActionState into
			 * the appropriate list.
			 */
			if (action_state->mas_action->matched)
				list = &resultRelInfo->ri_matchedMergeAction;
			else
				list = &resultRelInfo->ri_notMatchedMergeAction;
			*list = lappend(*list, action_state);

			switch (action->commandType)
			{
				case CMD_INSERT:
					ExecCheckPlanOutput(rootRelInfo->ri_RelationDesc,
										action->targetList);

					/*
					 * If the MERGE targets a partitioned table, any INSERT
					 * actions must be routed through it, not the child
					 * relations. Initialize the routing struct and the root
					 * table's "new" tuple slot for that, if not already done.
					 * The projection we prepare, for all relations, uses the
					 * root relation descriptor, and targets the plan's root
					 * slot.  (This is consistent with the fact that we
					 * checked the plan output to match the root relation,
					 * above.)
					 */
					if (rootRelInfo->ri_RelationDesc->rd_rel->relkind ==
						RELKIND_PARTITIONED_TABLE)
					{
						if (mtstate->mt_partition_tuple_routing == NULL)
						{
							/*
							 * Initialize planstate for routing if not already
							 * done.
							 *
							 * Note that the slot is managed as a standalone
							 * slot belonging to ModifyTableState, so we pass
							 * NULL for the 2nd argument.
							 */
							mtstate->mt_root_tuple_slot =
								table_slot_create(rootRelInfo->ri_RelationDesc,
												  NULL);
							mtstate->mt_partition_tuple_routing =
								ExecSetupPartitionTupleRouting(estate,
															   rootRelInfo->ri_RelationDesc);
						}
						tgtslot = mtstate->mt_root_tuple_slot;
						tgtdesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
					}
					else
					{
						/* not partitioned? use the stock relation and slot */
						tgtslot = resultRelInfo->ri_newTupleSlot;
						tgtdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
					}

					action_state->mas_proj =
						ExecBuildProjectionInfo(action->targetList, econtext,
												tgtslot,
												&mtstate->ps,
												tgtdesc);

					mtstate->mt_merge_subcommands |= MERGE_INSERT;
					break;
				case CMD_UPDATE:
					action_state->mas_proj =
						ExecBuildUpdateProjection(action->targetList,
												  true,
												  action->updateColnos,
												  relationDesc,
												  econtext,
												  resultRelInfo->ri_newTupleSlot,
												  &mtstate->ps);
					mtstate->mt_merge_subcommands |= MERGE_UPDATE;
					break;
				case CMD_DELETE:
					mtstate->mt_merge_subcommands |= MERGE_DELETE;
					break;
				case CMD_NOTHING:
					break;
				default:
					elog(ERROR, "unknown operation");
					break;
			}
		}
	}
}

/*
 * Initializes the tuple slots in a ResultRelInfo for any MERGE action.
 *
 * This mimics ExecInitInsertProjection / ExecInitUpdateProjection
 */
void
ExecInitMergeProjection(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	EState	   *estate = mtstate->ps.state;

	Assert(!resultRelInfo->ri_projectNewInfoValid);

	resultRelInfo->ri_oldTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_projectNewInfoValid = true;
}
