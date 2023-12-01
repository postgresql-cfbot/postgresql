/*-------------------------------------------------------------------------
 * pg_tracing_parallel.h
 *		Store, retrieve and remove trace context for parallel workers.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing_parallel.h
 *-------------------------------------------------------------------------
 */
#ifndef _PG_TRACING_PARALLEL_H_
#define _PG_TRACING_PARALLEL_H_

#include "postgres.h"
#include "pg_tracing_span.h"

#include "storage/s_lock.h"
#include "storage/backendid.h"

/*
 * A trace context for a specific parallel context
 */
typedef struct pgTracingParallelContext
{
	BackendId	leader_backend_id;	/* Backend id of the leader, set to
									 * InvalidBackendId if unused */
	pgTracingTraceContext trace_context;
}			pgTracingParallelContext;

/*
 * Store context for parallel workers
 */
typedef struct pgTracingParallelWorkers
{
	slock_t		mutex;
	pgTracingParallelContext trace_contexts[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingParallelWorkers;


extern void pg_tracing_shmem_parallel_startup(void);
extern void add_parallel_context(const struct pgTracingTraceContext *trace_context,
								 uint64 parent_id, uint64 query_id);
extern void remove_parallel_context(void);
extern void fetch_parallel_context(pgTracingTraceContext * trace_context);

#endif
