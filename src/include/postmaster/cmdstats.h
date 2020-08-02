/* ----------
 *	pgstat.h
 *
 *	Declarations for PostgreSQL command statistics
 *
 *	Copyright (c) 2001-2020, PostgreSQL Global Development Group
 *
 *	src/include/postmaster/cmdstats.h
 * ----------
 */
#ifndef CMDSTATS_H
#define CMDSTATS_H

#include "postgres.h"

#include "tcop/cmdtag.h"

extern PGDLLIMPORT bool cmdstats_tracking;

/*-------------
 * Command Stats shmem structs.
 *-------------
 */
/*
 * For each command tag, we track how many commands completed
 * for that type.
 */
#define NUM_CMDSTATS_COUNTS ((uint32)NUM_CMDTAGS)

/*
 * CmdStats is used for holding the shared counts per CommandTag for all tags.
 */
typedef struct CmdStats {
	uint64	cnt[NUM_CMDSTATS_COUNTS];
} CmdStats;

/*
 * LocalCmdStats is used for holding the per-backend local counts for just
 * the subset of CommandTags which have per-backend counts.  There are
 * NUM_LOCAL_STATS < NUM_CMDTAGS number of those.  Indexing into this
 * structure for a given CommandTag requires looking up the appropriate
 * offset.  See local_stats_offset for details.
 */
typedef struct LocalCmdStats {
	pg_atomic_uint32	cnt[NUM_LOCAL_STATS];
} LocalCmdStats;

/*
 * LocalStatsAccum is used for totalling up the per-backend counts.  We cannot
 * simply use a LocalCmdStats struct for that, as the totals across backends
 * may overflow a uint32.  Thus, we have only NUM_LOCAL_STATS counts, but use a
 * uint64 per count.
 */
typedef struct LocalStatsAccum {
	uint64	cnt[NUM_LOCAL_STATS];
} LocalStatsAccum;

/* shared memory stuff */
extern int NumCmdStats(void);
extern Size CmdStatsPerBackendShmemSize(void);
extern Size CmdStatsShmemSize(void);
extern void CmdStatsShmemInit(void);
extern CmdStats *cmdstats_shared_tally(void);
extern void cmdstats_rollup_localstats(LocalCmdStats *localstats);
extern void cmdstats_increment(CommandTag commandtag);
extern void reset_cmdstats(void);

#endif							/* CMDSTATS_H */
