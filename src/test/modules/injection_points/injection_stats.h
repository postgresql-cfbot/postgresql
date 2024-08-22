/*--------------------------------------------------------------------------
 *
 * injection_stats.h
 *		Definitions for statistics of injection points.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/test/modules/injection_points/injection_stats.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INJECTION_STATS
#define INJECTION_STATS

/* injection_stats.c */
extern void pgstat_register_inj(void);
extern void pgstat_create_inj(const char *name);
extern void pgstat_drop_inj(const char *name);
extern void pgstat_report_inj(const char *name);

/* injection_stats_fixed.c */
extern void pgstat_register_inj_fixed(void);
extern void pgstat_report_inj_fixed_numattach(uint32 numattach);
extern void pgstat_report_inj_fixed_numdetach(uint32 numdetach);
extern void pgstat_report_inj_fixed_numrun(uint32 numrun);
extern void pgstat_report_inj_fixed_numcached(uint32 numcached);
extern void pgstat_report_inj_fixed_numloaded(uint32 numloaded);

#endif
