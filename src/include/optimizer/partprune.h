/*-------------------------------------------------------------------------
 *
 * partprune.h
 *	  prototypes for partprune.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/partprune.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTPRUNE_H
#define PARTPRUNE_H

#include "catalog/partition.h"

extern Relids prune_append_rel_partitions(RelOptInfo *rel);
extern List *generate_partition_pruning_steps(RelOptInfo *rel, List *clauses,
								 bool *constfalse);

#endif							/* PARTPRUNE_H */
