/*-------------------------------------------------------------------------
 * pg_tracing.h
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.h
 *-------------------------------------------------------------------------
 */
#ifndef _PG_TRACING_H_
#define _PG_TRACING_H_

#include "postgres.h"
#include "pg_tracing_span.h"

#include "storage/s_lock.h"

/*
 * Global statistics for pg_tracing
 */
typedef struct pgTracingStats
{
	int64		traces;			/* number of traces processed */
	int64		spans;			/* number of spans processed */
	int64		dropped_spans;	/* number of dropped spans due to full buffer */
	int64		failed_truncates;	/* number of failed query file truncates
									 * due to active writer */
	TimestampTz last_consume;	/* Last time the shared spans buffer was
								 * consumed */
	TimestampTz stats_reset;	/* Last time stats were reset */
}			pgTracingStats;

/*
 * Global shared state
 */
typedef struct pgTracingSharedState
{
	slock_t		mutex;			/* protects shared stats fields and
								 * shared_spans buffer */
	Size		extent;			/* current extent of query file */
	int			n_writers;		/* number of active writers to query file */
	pgTracingStats stats;		/* global statistics for pg_tracing */
}			pgTracingSharedState;

#endif
