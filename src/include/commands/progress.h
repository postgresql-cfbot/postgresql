/*-------------------------------------------------------------------------
 *
 * progress.h
 *	  Constants used with the progress reporting facilities defined in
 *	  pgstat.h.  These are possibly interesting to extensions, so we
 *	  expose them via this header file.  Note that if you update these
 *	  constants, you probably also need to update the views based on them
 *	  in system_views.sql.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/progress.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROGRESS_H
#define PROGRESS_H

/* Progress parameters for (lazy) vacuum */
#define PROGRESS_VACUUM_PHASE					0
#define PROGRESS_VACUUM_TOTAL_HEAP_BLKS			1
#define PROGRESS_VACUUM_HEAP_BLKS_SCANNED		2
#define PROGRESS_VACUUM_HEAP_BLKS_VACUUMED		3
#define PROGRESS_VACUUM_NUM_INDEX_VACUUMS		4
#define PROGRESS_VACUUM_MAX_DEAD_TUPLES			5
#define PROGRESS_VACUUM_NUM_DEAD_TUPLES			6

/* Phases of vacuum (as advertised via PROGRESS_VACUUM_PHASE) */
#define PROGRESS_VACUUM_PHASE_SCAN_HEAP			1
#define PROGRESS_VACUUM_PHASE_VACUUM_INDEX		2
#define PROGRESS_VACUUM_PHASE_VACUUM_HEAP		3
#define PROGRESS_VACUUM_PHASE_INDEX_CLEANUP		4
#define PROGRESS_VACUUM_PHASE_TRUNCATE			5
#define PROGRESS_VACUUM_PHASE_FINAL_CLEANUP		6

/* Progress parameters for cluster */
#define PROGRESS_CLUSTER_COMMAND					0
#define PROGRESS_CLUSTER_PHASE						1
#define PROGRESS_CLUSTER_SCAN_METHOD				2
#define PROGRESS_CLUSTER_SCAN_INDEX_RELID			3
#define PROGRESS_CLUSTER_TOTAL_HEAP_TUPLES	  		4
#define PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED		5
#define PROGRESS_CLUSTER_HEAP_TUPLES_VACUUMED		6
#define PROGRESS_CLUSTER_HEAP_TUPLES_RECENTLY_DEAD	7

/* Phases of cluster (as dvertised via PROGRESS_CLUSTER_PHASE) */
#define PROGRESS_CLUSTER_PHASE_SCAN_HEAP						1
#define PROGRESS_CLUSTER_PHASE_SORT_TUPLES						2
#define PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP					3
#define PROGRESS_CLUSTER_PHASE_SCAN_HEAP_AND_WRITE_NEW_HEAP		4
#define PROGRESS_CLUSTER_PHASE_SWAP_REL_FILES					5
#define PROGRESS_CLUSTER_PHASE_REBUILD_INDEX					6
#define PROGRESS_CLUSTER_PHASE_FINAL_CLEANUP					7

/* Scan methods of cluster */
#define PROGRESS_CLUSTER_METHOD_INDEX_SCAN		1
#define PROGRESS_CLUSTER_METHOD_SEQ_SCAN		2

/* Commands of PROGRESS_CLUSTER */
#define PROGRESS_CLUSTER_COMMAND_CLUSTER		1
#define PROGRESS_CLUSTER_COMMAND_VACUUM_FULL	2


#endif
