/*--------------------------------------------------------------------
 * execPartition.h
 *		POSTGRES partitioning executor interface
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/executor/execPartition.h
 *--------------------------------------------------------------------
 */

#ifndef EXECPARTITION_H
#define EXECPARTITION_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "partitioning/partprune.h"

/* See execPartition.c for the definition. */
typedef struct PartitionDispatchData *PartitionDispatch;

/*-----------------------
 * PartitionTupleRouting - Encapsulates all information required to
 * route a tuple inserted into a partitioned table to one of its leaf
 * partitions
 *
 * partition_root			The partitioned table that's the target of the
 *							command.
 *
 * partition_dispatch_info	Array of 'dispatch_allocsize' elements containing
 *							a pointer to a PartitionDispatch objects for every
 *							partitioned table touched by tuple routing.  The
 *							entry for the target partitioned table is *always*
 *							present as the first entry of this array.  See
 *							comment for PartitionDispatchData->indexes for
 *							details on how this array is indexed.
 *
 * num_dispatch				The current number of items stored in the
 *							'partition_dispatch_info' array.  Also serves as
 *							the index of the next free array element for new
 *							PartitionDispatch which need to be stored.
 *
 * dispatch_allocsize		The current allocated size of the
 *							'partition_dispatch_info' array.
 *
 * partitions				Array of 'partitions_allocsize' elements
 *							containing pointers to a ResultRelInfos of all
 *							leaf partitions touched by tuple routing.  Some of
 *							these are pointers to ResultRelInfos which are
 *							borrowed out of 'subplan_resultrel_hash'.  The
 *							remainder have been built especially for tuple
 *							routing.  See comment for
 *							PartitionDispatchData->indexes for details on how
 *							this array is indexed.
 *
 * num_partitions			The current number of items stored in the
 *							'partitions' array.  Also serves as the index of
 *							the next free array element for new ResultRelInfos
 *							which need to be stored.
 *
 * partitions_allocsize		The current allocated size of the 'partitions'
 *							array.  Also, if they're non-NULL, marks the size
 *							of the 'parent_child_tupconv_maps',
 *							'child_parent_tupconv_maps' and
 *							'child_parent_map_not_required' arrays.
 *
 * parent_child_tupconv_maps	Array of partitions_allocsize elements
 *							containing information on how to convert tuples of
 *							partition_root's rowtype to the rowtype of the
 *							corresponding partition as stored in 'partitions',
 *							or NULL if no conversion is required.  The entire
 *							array is only allocated when the first conversion
 *							map needs to stored.  When not allocated it's set
 *							to NULL.
 *
 * partition_tuple_slot		This is a tuple slot used to store a tuple using
 *							rowtype of the partition chosen by tuple
 *							routing.  Maintained separately because partitions
 *							may have different rowtype.
 *
 * child_parent_tupconv_maps	As 'parent_child_tupconv_maps' but stores
 *							conversion maps to translate partition tuples into
 *							partition_root's rowtype, needed if transition
 *							capture is active
 *
 * Note: The following fields are used only when UPDATE ends up needing to
 * do tuple routing.
 *
 * subplan_resultrel_hash	Hash table to store subplan ResultRelInfos by Oid.
 *							This is used to cache ResultRelInfos from subplans
 *							of a ModifyTable node.  Some of these may be
 *							useful for tuple routing to save having to build
 *							duplicates.
 *
 * root_tuple_slot			During UPDATE tuple routing, this tuple slot is
 *							used to transiently store a tuple using the root
 *							table's rowtype after converting it from the
 *							tuple's source leaf partition's rowtype.  That is,
 *							if leaf partition's rowtype is different.
 *-----------------------
 */
typedef struct PartitionTupleRouting
{
	Relation	partition_root;
	PartitionDispatch *partition_dispatch_info;
	int			num_dispatch;
	int			dispatch_allocsize;
	ResultRelInfo **partitions;
	int			num_partitions;
	int			partitions_allocsize;
	TupleConversionMap **parent_child_tupconv_maps;
	TupleConversionMap **child_parent_tupconv_maps;
	HTAB	   *subplan_resultrel_hash;
	TupleTableSlot *root_tuple_slot;
	TupleTableSlot *partition_tuple_slot;
} PartitionTupleRouting;

/*
 * Accessor macros for tuple conversion maps contained in
 * PartitionTupleRouting.  Beware of multiple evaluations of p!
 */
#define PartitionTupRoutingGetToParentMap(p, i) \
			((p)->child_parent_tupconv_maps != NULL ? \
				(p)->child_parent_tupconv_maps[(i)] : \
							NULL)

#define PartitionTupRoutingGetToChildMap(p, i) \
			((p)->parent_child_tupconv_maps != NULL ? \
				(p)->parent_child_tupconv_maps[(i)] : \
							NULL)

/*
 * PartitionedRelPruningData - Per-partitioned-table data for run-time pruning
 * of partitions.  For a multilevel partitioned table, we have one of these
 * for the topmost partition plus one for each non-leaf child partition.
 *
 * subplan_map[] and subpart_map[] have the same definitions as in
 * PartitionedRelPruneInfo (see plannodes.h); though note that here,
 * subpart_map contains indexes into PartitionPruningData.partrelprunedata[].
 *
 * subplan_map					Subplan index by partition index, or -1.
 * subpart_map					Subpart index by partition index, or -1.
 * present_parts				A Bitmapset of the partition indexes that we
 *								have subplans or subparts for.
 * context						Contains the context details required to call
 *								the partition pruning code.
 * pruning_steps				List of PartitionPruneSteps used to
 *								perform the actual pruning.
 * do_initial_prune				true if pruning should be performed during
 *								executor startup (for this partitioning level).
 * do_exec_prune				true if pruning should be performed during
 *								executor run (for this partitioning level).
 */
typedef struct PartitionedRelPruningData
{
	int		   *subplan_map;
	int		   *subpart_map;
	Bitmapset  *present_parts;
	PartitionPruneContext context;
	List	   *pruning_steps;
	bool		do_initial_prune;
	bool		do_exec_prune;
} PartitionedRelPruningData;

/*
 * PartitionPruningData - Holds all the run-time pruning information for
 * a single partitioning hierarchy containing one or more partitions.
 * partrelprunedata[] is an array ordered such that parents appear before
 * their children; in particular, the first entry is the topmost partition,
 * which was actually named in the SQL query.
 */
typedef struct PartitionPruningData
{
	int			num_partrelprunedata;	/* number of array entries */
	PartitionedRelPruningData partrelprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruningData;

/*
 * PartitionPruneState - State object required for plan nodes to perform
 * run-time partition pruning.
 *
 * This struct can be attached to plan types which support arbitrary Lists of
 * subplans containing partitions, to allow subplans to be eliminated due to
 * the clauses being unable to match to any tuple that the subplan could
 * possibly produce.
 *
 * execparamids			Contains paramids of PARAM_EXEC Params found within
 *						any of the partprunedata structs.  Pruning must be
 *						done again each time the value of one of these
 *						parameters changes.
 * other_subplans		Contains indexes of subplans that don't belong to any
 *						"partprunedata", e.g UNION ALL children that are not
 *						partitioned tables, or a partitioned table that the
 *						planner deemed run-time pruning to be useless for.
 *						These must not be pruned.
 * prune_context		A short-lived memory context in which to execute the
 *						partition pruning functions.
 * do_initial_prune		true if pruning should be performed during executor
 *						startup (at any hierarchy level).
 * do_exec_prune		true if pruning should be performed during
 *						executor run (at any hierarchy level).
 * num_partprunedata	Number of items in "partprunedata" array.
 * partprunedata		Array of PartitionPruningData pointers for the plan's
 *						partitioned relation(s), one for each partitioning
 *						hierarchy that requires run-time pruning.
 */
typedef struct PartitionPruneState
{
	Bitmapset  *execparamids;
	Bitmapset  *other_subplans;
	MemoryContext prune_context;
	bool		do_initial_prune;
	bool		do_exec_prune;
	int			num_partprunedata;
	PartitionPruningData *partprunedata[FLEXIBLE_ARRAY_MEMBER];
} PartitionPruneState;

extern PartitionTupleRouting *ExecSetupPartitionTupleRouting(ModifyTableState *mtstate,
							   Relation rel);
extern int ExecFindPartition(ModifyTableState *mtstate,
				  ResultRelInfo *resultRelInfo,
				  PartitionTupleRouting *proute,
				  TupleTableSlot *slot,
				  EState *estate);
extern ResultRelInfo *ExecGetPartitionInfo(ModifyTableState *mtstate,
					 ResultRelInfo *resultRelInfo,
					 PartitionTupleRouting *proute,
					 EState *estate, int partidx);
extern void ExecInitRoutingInfo(ModifyTableState *mtstate,
					EState *estate,
					PartitionTupleRouting *proute,
					ResultRelInfo *partRelInfo,
					int partidx);
extern HeapTuple ConvertPartitionTupleSlot(TupleConversionMap *map,
						  HeapTuple tuple,
						  TupleTableSlot *new_slot,
						  TupleTableSlot **p_my_slot,
						  bool shouldFree);
extern void ExecCleanupTupleRouting(ModifyTableState *mtstate,
						PartitionTupleRouting *proute);
extern PartitionPruneState *ExecCreatePartitionPruneState(PlanState *planstate,
							  PartitionPruneInfo *partitionpruneinfo);
extern void ExecDestroyPartitionPruneState(PartitionPruneState *prunestate);
extern Bitmapset *ExecFindMatchingSubPlans(PartitionPruneState *prunestate);
extern Bitmapset *ExecFindInitialMatchingSubPlans(PartitionPruneState *prunestate,
								int nsubplans);

#endif							/* EXECPARTITION_H */
