/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *		Generate spans for distributed tracing from SQL query
 *
 * Spans will only be generated for sampled queries. A query is sampled if:
 * - It has a tracecontext propagated throught SQLCommenter and it passes the caller_sample_rate.
 * - It has no SQLCommenter but the query randomly passes the global sample_rate
 *
 * A query with sqlcommenter will look like: /\*dddbs='postgres.db',traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/ select 1;
 * The traceparent fields are detailed in https://www.w3.org/TR/trace-context/#traceparent-header-field-values
 * 00000000000000000000000000000009: trace id
 * 0000000000000005: parent id
 * 01: trace flags (01 == sampled)
 *
 * If sampled, we will generate spans for the ongoing query.
 * A span represents an operation with a start time, a duration and useful metadatas (block stats, wal stats...).
 * We will track the following operations:
 * - Top Span: The top span for a query. They are created after extracting the traceid from traceparent or to represent a nested query.
 * - Planner: We track the time spent in the planner and report the planner counters
 * - Node Span: Created from planstate. The name is extracted from the node type (IndexScan, SeqScan).
 * - Executor: We trace the different steps of the Executor: Start, Run, Finish and End
 *
 * A typical traced query will generate the following spans:
 * +------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | Name: Select (top span)                                                                                                                              |
 * | Operation: Select * pgbench_accounts WHERE aid=$1;                                                                                                   |
 * +---+------------------------+-+------------------+--+------------------------------------------------------+-+--------------------+-+----------------++
 *     | Name: Planner          | | Name: Executor   |  |Name: Executor                                        | | Name: Executor     | | Name: Executor |
 *     | Operation: Planner     | | Operation: Start |  |Operation: Run                                        | | Operation: Finish  | | Operation: End |
 *     +------------------------+ +------------------+  +--+--------------------------------------------------++ +--------------------+ +----------------+
 *                                                         | Name: IndexScan                                  |
 *                                                         | Operation: IndexScan using pgbench_accounts_pkey |
 *                                                         |            on pgbench_accounts                   |
 *                                                         +--------------------------------------------------+
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing_explain.h"
#include "pg_tracing.h"
#include "query_process.h"
#include "span.h"

#include "utils/ruleutils.h"
#include "access/xact.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/xid8.h"

PG_MODULE_MAGIC;

typedef enum
{
	PG_TRACING_TRACK_NONE,		/* track no statements */
	PG_TRACING_TRACK_TOP,		/* only top level statements */
	PG_TRACING_TRACK_ALL		/* all statements, including nested ones */
}			pgTracingTrackLevel;

/*
 * Structure to store flexible array of spans
 */
typedef struct pgTracingSpans
{
	int			end;			/* Index of last element */
	int			max;			/* Maximum number of element */
	Span		spans[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingSpans;

/*
 * A trace context for a specific parallel context
 */
typedef struct pgTracingParallelContext
{
	BackendId	leader_backend_id;	/* Backend id of the leader, set to
									 * InvalidBackendId if unused */
	uint64		trace_id;		/* Trace id of the sampled query */
	uint64		parent_id;		/* Id of the parent Gather span */
	uint64		query_id;		/* Query id from the parent worker */
	pgTracingStartTime start_trace;

}			pgTracingParallelContext;

/*
 * Store context for parallel workers
 */
typedef struct pgTracingParallelWorkers
{
	slock_t		mutex;
	pgTracingParallelContext trace_contexts[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingParallelWorkers;

/* GUC variables */
static int	pg_tracing_max_span;	/* Maximum number of spans to store */
static int	pg_tracing_max_parameter_str;	/* Maximum number of spans to
											 * store */
static int	pg_tracing_max_traced_parallel_workers; /* Maximum number of
													 * parallel workers traced */
static int	pg_tracing_initial_allocated_spans = 25;	/* Number of spans
														 * allocated at the
														 * start of a trace at
														 * the same time. */
static bool pg_tracing_instrument_buffers;	/* Enable buffers instrumentation */
static bool pg_tracing_instrument_wal;	/* Enable WAL instrumentation */
static double pg_tracing_sample_rate = 0;	/* Sample rate applied to queries
											 * without SQLCommenter */
static double pg_tracing_caller_sample_rate = 1;	/* Sample rate applied to
													 * queries with
													 * SQLCommenter */
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;	/* tracking level */
static bool pg_tracing_track_utility = true;	/* whether to track utility
												 * commands */
static bool pg_tracing_drop_full_buffer = true; /* whether to drop buffered
												 * spans when full */


int64		nested_query_start_ns = 0;
int64		executor_start_start_ns = 0;

static const struct config_enum_entry track_options[] =
{
	{"none", PG_TRACING_TRACK_NONE, false},
	{"top", PG_TRACING_TRACK_TOP, false},
	{"all", PG_TRACING_TRACK_ALL, false},
	{NULL, 0, false}
};

#define pg_tracing_enabled(level) \
	((pg_tracing_track == PG_TRACING_TRACK_ALL || \
	(pg_tracing_track == PG_TRACING_TRACK_TOP && (level) == 0)))

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Global variables
 */

/* Memory context for pg_tracing. */
static MemoryContext pg_tracing_mem_ctx;

/* Current trace state */
static struct pgTracingTrace current_trace;

/* Shared state with stats and file external state */
static pgTracingSharedState * pg_tracing = NULL;

/*
 * Shared buffer storing spans. Query with sampled flag will add new spans to
 * the shared state. Those spans will be consumed during calls to
 * pg_tracing_spans
 */
static pgTracingSpans * shared_spans = NULL;

/*
 * Store all spans for the curent trace except top spans and parse spans.
 * They will be added to the shared buffer at the end of query tracing.
 *
 * This buffer can be dynamically resized so care needs to be taken when using span pointers.
 */
static pgTracingSpans * current_trace_spans;

/*
 * Shared buffer storing trace context for parallel workers.
 */
static pgTracingParallelWorkers * pg_tracing_parallel = NULL;

/* Index of the parallel worker context shared buffer if any */
static int	parallel_context_index = -1;

/*
* Text for spans are buffered in this
* stringinfo and written at the end of the tracing in a single write.
*/
static StringInfo current_trace_text;

/*
 * Maximum nested level for a query to know how many top spans we need to
 * copy in shared_spans
 */
static int	max_nested_level = -1;

/* Current nesting depth of planner calls */
static int	plan_nested_level = 0;

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
static int	exec_nested_level = 0;

/* Number of allocated levels. */
static int	allocated_nested_level = 0;

/*
 * Per nested level buffers
 */
typedef struct pgTracingPerLevelBuffer
{
	int			top_span_index; /* Index of the top span in the
								 * current_trace_spans buffer */
	uint64		executor_id;	/* Span id of executor run spans by nested
								 * level Executor run is used as parent for
								 * spans generated from planstate */
	uint64_t	query_id;		/* Query id by nested level when available */
}			pgTracingPerLevelBuffer;

static pgTracingPerLevelBuffer * per_level_buffers;

/*
 * Timestamp of the latest statement checked for sampling.
 */
static TimestampTz last_statement_check_for_sampling = 0;

static void pg_tracing_shmem_request(void);
static void pg_tracing_shmem_startup(void);

/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static PlannedStmt *pg_tracing_planner_hook(Query *parse,
											const char *query_string,
											int cursorOptions,
											ParamListInfo params);
static void pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count, bool execute_once);
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);
static void pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context, ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest, QueryCompletion *qc);

static void generate_member_nodes(PlanState **planstates, int nplans, planstateTraceContext * planstateTraceContext, uint64 parent_id);
static void generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext, uint64 parent_id);
static pgTracingStats get_empty_pg_tracing_stats(void);

#define PG_TRACING_INFO_COLS	7
#define PG_TRACING_TRACES_COLS	44

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_tracing.max_span",
							"Maximum number of spans stored in shared memory.",
							NULL,
							&pg_tracing_max_span,
							1000,
							0,
							500000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.max_parameter_size",
							"Maximum size of parameters. -1 to disable parameter in top span.",
							NULL,
							&pg_tracing_max_parameter_str,
							1024,
							0,
							10000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_tracing.instrument_buffers",
							 "Instrument and add buffers usage in spans.",
							 NULL,
							 &pg_tracing_instrument_buffers,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.instrument_wal",
							 "Instrument and add WAL usage in spans.",
							 NULL,
							 &pg_tracing_instrument_wal,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_tracing.max_traced_parallel_workers",
							"Maximum number of parallel workers traced. -1 to disable tracing of parallel workers.",
							NULL,
							&pg_tracing_max_traced_parallel_workers,
							100,
							0,
							10000,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.initial_allocated_spans",
							"The number of allocated spans at the start of a trace. A higher number can help avoid reallocation when extending the buffer.",
							NULL,
							&pg_tracing_initial_allocated_spans,
							25,
							0,
							500,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_tracing.track",
							 "Selects which statements are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track,
							 PG_TRACING_TRACK_ALL,
							 track_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.track_utility",
							 "Selects whether utility commands are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track_utility,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.drop_on_full_buffer",
							 "Selects whether buffered spans should be dropped when buffer is full.",
							 NULL,
							 &pg_tracing_drop_full_buffer,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.sample_rate",
							 "Fraction of queries to process.",
							 NULL,
							 &pg_tracing_sample_rate,
							 0.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.caller_sample_rate",
							 "Fraction of queries with sampled flag to process.",
							 NULL,
							 &pg_tracing_caller_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_tracing");

	/* For jumble state */
	EnableQueryId();

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_tracing_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_tracing_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_tracing_post_parse_analyze;

	prev_planner_hook = planner_hook;
	planner_hook = pg_tracing_planner_hook;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_tracing_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_tracing_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_tracing_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_tracing_ExecutorEnd;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pg_tracing_ProcessUtility;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pg_tracing_memsize(void)
{
	Size		size;

	/* pg_tracing shared state */
	size = sizeof(pgTracingSharedState);
	/* span struct */
	size = add_size(size, sizeof(pgTracingSpans));
	/* the span variable array */
	size = add_size(size, mul_size(pg_tracing_max_span, sizeof(Span)));
	/* and the parallel workers context  */
	size = add_size(size, mul_size(pg_tracing_max_traced_parallel_workers, sizeof(pgTracingParallelContext)));

	return size;
}

/*
 * shmem_request hook: request additional shared resources.  We'll allocate
 * or attach to the shared resources in pgss_shmem_startup().
 */
static void
pg_tracing_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(pg_tracing_memsize());
}

/*
 * Push trace context to the shared parallel worker buffer
 */
static void
add_parallel_context(uint64 parent_id, uint64 query_id)
{
	volatile	pgTracingParallelWorkers *p = (volatile pgTracingParallelWorkers *) pg_tracing_parallel;

	if (pg_tracing_max_traced_parallel_workers <= 0)
		/* No tracing of parallel workers */
		return;

	Assert(parallel_context_index == -1);
	SpinLockAcquire(&p->mutex);
	for (int i = 0; i < pg_tracing_max_traced_parallel_workers; i++)
	{
		volatile	pgTracingParallelContext *ctx = p->trace_contexts + i;

		Assert(ctx->leader_backend_id != MyBackendId);
		if (ctx->leader_backend_id != InvalidBackendId)
			continue;
		/* Slot is available */
		parallel_context_index = i;
		ctx->leader_backend_id = MyBackendId;
		ctx->trace_id = current_trace.trace_id;
		ctx->parent_id = parent_id;
		ctx->query_id = query_id;
		ctx->start_trace = current_trace.start_trace;
		break;
	}
	SpinLockRelease(&p->mutex);
}

/*
 * Remove parallel context for the current leader from the shared memory.
 */
static void
remove_parallel_context(void)
{
	if (parallel_context_index < 0)
		/* No tracing of parallel workers */
		return;
	{
		volatile	pgTracingParallelWorkers *p = (volatile pgTracingParallelWorkers *) pg_tracing_parallel;

		SpinLockAcquire(&p->mutex);
		p->trace_contexts[parallel_context_index].leader_backend_id = InvalidBackendId;
		SpinLockRelease(&p->mutex);
	}
	parallel_context_index = -1;
}

/*
 * If we're inside a parallel worker, check if the trace context is stored in shared memory.
 * If a trace context exists, it means that the query is sampled and worker tracing is enabled.
 */
static void
fetch_parallel_context(pgTracingTrace * trace)
{
	volatile	pgTracingParallelWorkers *p = (volatile pgTracingParallelWorkers *) pg_tracing_parallel;

	if (pg_tracing_max_traced_parallel_workers <= 0)
	{
		/* No tracing of parallel workers */
		return;
	}
	SpinLockAcquire(&p->mutex);
	for (int i = 0; i < pg_tracing_max_traced_parallel_workers; i++)
	{
		if (p->trace_contexts[i].leader_backend_id != ParallelLeaderBackendId)
			continue;
		/* Found a matching a trace context, fetch it */
		trace->trace_id = p->trace_contexts[i].trace_id;
		trace->parent_id = p->trace_contexts[i].parent_id;
		trace->start_trace = p->trace_contexts[i].start_trace;
		trace->query_id = p->trace_contexts[i].query_id;
		trace->sampled = true;
	}
	SpinLockRelease(&p->mutex);
}

/*
 * Return a slot from the current_trace_spans buffer.
 * current_trace_spans may be extended if needed.
 */
static int
get_index_from_trace_spans(void)
{
	if (current_trace_spans->end + 1 >= current_trace_spans->max)
	{
		MemoryContext oldcxt;

		/* Need to extend. */
		/* TODO: Have a configurable limit? */
		current_trace_spans->max *= 2;
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		current_trace_spans = repalloc(current_trace_spans, current_trace_spans->max * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
	}
	return current_trace_spans->end++;
}

/*
 * Get current_span from provided index
 */
static Span *
get_span_from_index(int span_index)
{
	Assert(span_index < current_trace_spans->end);
	return &current_trace_spans->spans[span_index];
}

/*
 * Get top span for the provided nested level.
 * If top_span doesn't exist, create it.
 */
static Span *
get_top_span_for_nested_level(int nested_level)
{
	Span	   *top_span;

	Assert(nested_level <= max_nested_level);
	if (per_level_buffers[nested_level].top_span_index == -1)
		per_level_buffers[nested_level].top_span_index = get_index_from_trace_spans();
	top_span = get_span_from_index(per_level_buffers[nested_level].top_span_index);
	return top_span;
}

/*
 * Set top span to the provided span index
 */
static void
set_top_span(int nested_level, int top_span_index)
{
	Assert(nested_level <= max_nested_level);
	Assert(top_span_index < current_trace_spans->max);
	per_level_buffers[nested_level].top_span_index = top_span_index;
}

/*
 * End the span at the provided index
 */
static void
end_span_index(int span_index, const int64 *end_span_input)
{
	Span	   *span;

	if (span_index < 0)
		return;
	span = get_span_from_index(span_index);
	end_span(span, end_span_input);
}

/*
 * End top span for the provided nested level
 */
static void
end_top_span(int nested_level, int64 end_span_ns)
{
	Assert(nested_level <= max_nested_level);
	Assert(per_level_buffers[nested_level].top_span_index < current_trace_spans->max);
	Assert(per_level_buffers[nested_level].top_span_index != -1);
	end_span_index(per_level_buffers[nested_level].top_span_index, &end_span_ns);
}

/*
 * shmem_startup hook: allocate or attach to shared memory, Also create and
 * load the query-texts file, which is expected to exist (even if empty)
 * while the module is enabled.
 */
static void
pg_tracing_shmem_startup(void)
{
	bool		found_pg_tracing;
	bool		found_shared_spans;
	bool		found_parallel;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pg_tracing = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	current_trace.sampled = 0;
	pg_tracing = ShmemInitStruct("PgTracing Shared", sizeof(pgTracingSharedState), &found_pg_tracing);
	shared_spans = ShmemInitStruct("PgTracing Spans",
								   sizeof(pgTracingSpans) + pg_tracing_max_span * sizeof(Span),
								   &found_shared_spans);
	pg_tracing_parallel = ShmemInitStruct("PgTracing Parallel Workers Context",
										  sizeof(pgTracingParallelWorkers) + pg_tracing_max_traced_parallel_workers * sizeof(pgTracingParallelContext),
										  &found_parallel);

	/* Initialize pg_tracing memory context */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/* First time, let's init shared state */
	if (!found_pg_tracing)
	{
		pg_tracing->stats = get_empty_pg_tracing_stats();
		SpinLockInit(&pg_tracing->mutex);
		SpinLockInit(&pg_tracing_parallel->mutex);
	}
	if (!found_shared_spans)
	{
		shared_spans->end = 0;
		shared_spans->max = pg_tracing_max_span;
	}
	if (!found_parallel)
	{
		for (int i = 0; i < pg_tracing_max_traced_parallel_workers; i++)
			pg_tracing_parallel->trace_contexts[i].leader_backend_id = InvalidBackendId;
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Add span to the shared memory. Mutex must be acquired beforehand.
 */
static void
add_span_to_shared_buffer_locked(const Span * span)
{
	/* Spans must be ended before adding them to the shared buffer */
	Assert(span->ended);
	if (shared_spans->end + 1 >= shared_spans->max)
		pg_tracing->stats.dropped_spans++;
	else
	{
		pg_tracing->stats.spans++;
		shared_spans->spans[shared_spans->end++] = *span;
	}
}

/*
 * Check if we still have available space in the shared spans.
 *
 * Between the moment we check and the moment we want to insert, the buffer
 * may be full but we redo a check before appending the span. This is done
 * early when starting a top span to bail out early if the buffer is
 * already full since we don't immediately add the span in the shared buffer.
 */
static bool
check_full_shared_spans()
{
	if (shared_spans->end + 1 >= shared_spans->max)
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;
		bool		full_buffer = false;

		SpinLockAcquire(&s->mutex);
		if (pg_tracing_drop_full_buffer)
		{
			s->stats.dropped_spans += shared_spans->end;
			shared_spans->end = 0;
			s->extent = 0;
		}
		else
		{
			full_buffer = true;
			s->stats.dropped_spans++;
		}
		SpinLockRelease(&s->mutex);
		return full_buffer;
	}
	return false;
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters and generate span nodes from queryDesc planstate
 */
static void
process_query_desc(QueryDesc *queryDesc, int sql_error_code)
{
	NodeCounters *node_counters = &get_top_span_for_nested_level(exec_nested_level)->node_counters;

	/* Process total counters */
	if (queryDesc->totaltime)
	{
		InstrEndLoop(queryDesc->totaltime);
		node_counters->buffer_usage = queryDesc->totaltime->bufusage;
		node_counters->wal_usage = queryDesc->totaltime->walusage;
	}
	/* Process JIT counter */
	if (queryDesc->estate->es_jit)
		node_counters->jit_usage = queryDesc->estate->es_jit->instr;
	node_counters->rows = queryDesc->estate->es_total_processed;

	/* Process planstate */
	if (queryDesc->planstate)
	{
		Bitmapset  *rels_used = NULL;
		planstateTraceContext planstateTraceContext;
		uint64		parent_id = per_level_buffers[exec_nested_level].executor_id;

		/* Prepare the planstate context for deparsing */
		planstateTraceContext.rtable_names = select_rtable_names_for_explain(queryDesc->plannedstmt->rtable, rels_used);
		planstateTraceContext.trace = &current_trace;
		planstateTraceContext.ancestors = NULL;
		planstateTraceContext.sql_error_code = sql_error_code;
		planstateTraceContext.deparse_ctx = deparse_context_for_plan_tree(queryDesc->plannedstmt, planstateTraceContext.rtable_names);

		generate_span_from_planstate(queryDesc->planstate, &planstateTraceContext, parent_id);
	}
}

/*
 * If trace was started from the global sample rate without
 * a parent trace, we need to generate a random trace id
 */
static void
set_traceparent_trace_id(struct pgTracingTrace *traceparent_parameter)
{
	if (traceparent_parameter->trace_id != 0)
		return;

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace. The top span created with a parent_id == 0 will have trace_id ==
	 * parent_id == span_id.
	 */
	Assert(traceparent_parameter->parent_id == 0);
	traceparent_parameter->trace_id = pg_prng_uint64(&pg_global_prng_state);
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * and the provided sample rate configurations
 */
static bool
apply_sample_rate(struct pgTracingTrace const *traceparent_parameter)
{
	double		rand;
	bool		sampled;

	/* Everything is sampled */
	if (pg_tracing_sample_rate >= 1.0)
		return true;

	/* No SQLCommenter sampled and no global sample rate */
	if (traceparent_parameter->sampled == 0 && pg_tracing_sample_rate == 0.0)
		return false;

	/* SQLCommenter sampled caller sample */
	if (traceparent_parameter->sampled == 1 && pg_tracing_caller_sample_rate >= 1.0)
		return true;

	/*
	 * We have a non zero global sample rate or a sampled flag. Either way, we
	 * need a rand value
	 */
	rand = pg_prng_double(&pg_global_prng_state);
	if (traceparent_parameter->sampled == 1)
		/* Sampled flag case */
		sampled = (rand < pg_tracing_caller_sample_rate);
	else
		/* Global rate case */
		sampled = (rand < pg_tracing_sample_rate);
	return sampled;
}

/*
 * If the query was started as a prepared statement, we won't be able to
 * extract traceparent during query parsing since parsing was skipped.
 *
 * We assume that SQLCommenter content can be passed as a text in the
 * first parameter.
 */
static void
check_traceparameter_in_parameter(ParamListInfo params)
{
	Oid			typoutput;
	bool		typisvarlena;
	char	   *pstring;
	ParamExternData param;

	if (params == NULL || params->numParams == 0)
		return;

	param = params->params[0];
	/* Check first parameter type */
	if (param.ptype != TEXTOID)
		return;

	/* Get the text of the first string parameter */
	getTypeOutputInfo(param.ptype, &typoutput, &typisvarlena);
	pstring = OidOutputFunctionCall(typoutput, param.value);
	extract_traceparent(&current_trace, pstring, true);
}

/*
 * Check whether the current query should be sampled or not
 */
static void
check_query_sampled(ParseState *pstate, const ParamListInfo params)
{
	TimestampTz statement_start_ts;

	/* Safety check... */
	if (!pg_tracing || !pg_tracing_enabled(exec_nested_level))
		return;

	/* sampling already started */
	if (current_trace.sampled > 0)
		return;

	/* Don't start tracing if we're not at the top */
	if (exec_nested_level > 0)
		return;

	/*
	 * In a parallel worker, check the parallel context shared buffer to see
	 * if the leader left a trace context
	 */
	if (IsParallelWorker())
	{
		fetch_parallel_context(&current_trace);
		return;
	}

	Assert(max_nested_level = -1);

	current_trace.trace_id = 0;
	current_trace.parent_id = 0;
	if (params != NULL)
		check_traceparameter_in_parameter(params);
	else if (pstate != NULL)
		extract_traceparent(&current_trace, pstate->p_sourcetext, false);

	/*
	 * A statement can go through this function 3 times: post parsing, planner
	 * and executor run. If no sampling flag was extracted from SQLCommenter
	 * and we've already seen this statement (using statement_start ts), don't
	 * try to apply sample rate and exit.
	 */
	statement_start_ts = GetCurrentStatementStartTimestamp();
	if (current_trace.sampled == 0 && last_statement_check_for_sampling == statement_start_ts)
		return;
	/* First time we see this statement, save the time */
	last_statement_check_for_sampling = statement_start_ts;

	current_trace.sampled = apply_sample_rate(&current_trace);

	if (current_trace.sampled > 0 && check_full_shared_spans())
		/* Buffer is full, abort sampling */
		current_trace.sampled = 0;

	if (current_trace.sampled > 0)
		set_traceparent_trace_id(&current_trace);
}

/*
 * Adjust span's offsets with the provided file offset
 */
static void
adjust_file_offset(Span * span, Size file_position)
{
	if (span->name_offset != -1)
		span->name_offset += file_position;
	if (span->operation_name_offset != -1)
		span->operation_name_offset += file_position;
	if (span->parameter_offset != -1)
		span->parameter_offset += file_position;
}

/*
 * Get the start time for a new top span
 */
static int64
get_top_span_start(int64 *start_time_ns)
{
	if (exec_nested_level == 0)
	{
		/* For the top span of parallel worker, we get the current ns */
		if (IsParallelWorker())
		{
			if (start_time_ns != NULL)
				return *start_time_ns;
			return get_current_ns();
		}
		/* For the root top span, use the trace start */
		Assert(current_trace.start_trace.ns > 0);
		return current_trace.start_trace.ns;
	}

	/*
	 * We're in a nested query, if nested_query_start is not set, we haven't
	 * gone through ProcessUtility. Set the start at the current time in this
	 * case.
	 */
	if (nested_query_start_ns == 0)
	{
		if (start_time_ns != NULL)
			return *start_time_ns;
		return get_current_ns();
	}

	/*
	 * nested_query_start is available, use it
	 */
	return nested_query_start_ns;
}

/*
 * Reset pg_tracing memory context and global state.
 */
static void
cleanup_tracing(void)
{
	if (current_trace.sampled == 0)
		/* No need for cleaning */
		return;
	remove_parallel_context();
	MemoryContextReset(pg_tracing_mem_ctx);
	current_trace.sampled = 0;
	current_trace.trace_id = 0;
	current_trace.parent_id = 0;
	max_nested_level = -1;
	current_trace_spans = NULL;
	per_level_buffers = NULL;
}

/*
 * End the query tracing and dump all spans in the shared buffer.
 * This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(const int64 *end_span_ns)
{
	Size		file_position = 0;

	/* We're still a nested query, tracing is not finished */
	if (exec_nested_level > 0)
		return;

	/* Dump all buffered texts in file */
	text_store_file(pg_tracing, current_trace_text->data, current_trace_text->len, &file_position);

	for (int i = 0; i < current_trace_spans->end; i++)
		adjust_file_offset(current_trace_spans->spans + i, file_position);

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		/* We're at the end, add all spans to the shared memory */
		for (int i = 0; i < current_trace_spans->end; i++)
			add_span_to_shared_buffer_locked(&current_trace_spans->spans[i]);
		SpinLockRelease(&s->mutex);
	}

	/* We can reset the memory context here */
	cleanup_tracing();
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean
 * our memory context
 */
static void
handle_pg_error(int span_index, QueryDesc *queryDesc)
{
	Span	   *span;
	Span	   *top_span;
	int64		end_span_ns;
	int			sql_error_code;

	/* If we're not sampling the query, bail out */
	if (current_trace.sampled == 0 && !pg_tracing_enabled(exec_nested_level))
		return;
	sql_error_code = geterrcode();

	/*
	 * Order matters there. We want to process query desc before getting the
	 * end time. Otherwise, the span nodes will have a higher duration than
	 * their parents.
	 */
	if (queryDesc != NULL)
		process_query_desc(queryDesc, sql_error_code);

	/* Assign the error code to the latest span */
	span = get_span_from_index(span_index);
	span->sql_error_code = sql_error_code;

	/* Assign the error code to the latest top span */
	top_span = get_top_span_for_nested_level(exec_nested_level);
	top_span->sql_error_code = sql_error_code;

	end_span_ns = get_current_ns();
	/* End current span, ongoing top spans and tracing */
	for (int i = 0; i <= exec_nested_level; i++)
		end_top_span(i, end_span_ns);
	end_span(span, &end_span_ns);
	end_tracing(&end_span_ns);
}

/*
 * Get the parent id for the given nested level
 *
 * nested_level can be negative when creating the root top span.
 * In this case, we use the parent id from the propagated trace context.
 */
static uint64
get_parent_id(int nested_level)
{
	Span	   *top_span;

	if (nested_level < 0)
		return current_trace.parent_id;
	Assert(nested_level <= allocated_nested_level);
	top_span = get_top_span_for_nested_level(nested_level);
	Assert(top_span->span_id > 0);
	return top_span->span_id;
}

/*
 * If we enter a new nested level, initialize all necessary buffers
 * and start_trace timer.
 *
 * The start of a top span can vary: prepared statement will skip parsing, the use of cached
 * plans will skip the planner hook.
 * Thus, a top span can start in either post parse, planner hook or executor run.
 *
 * Since this is called after the we've detected the start of a trace, we check for available
 * space in the buffer. If the buffer is full, we abort tracing by setting sampled to false.
 * Callers need to check that tracing was aborted.
 */
static bool
initialize_trace_level(void)
{
	/* Check if we've already created a top span for this nested level */
	if (exec_nested_level <= max_nested_level)
		return false;

	/* First time */
	if (max_nested_level == -1)
	{
		MemoryContext oldcxt;

		Assert(pg_tracing_mem_ctx->isReset);

		/*
		 * We need to be able to pass information that depends on the nested
		 * level.
		 *
		 * executor span ids: Since an executor run becomes the parent span,
		 * we need subsequent created node spans to have the correct parent.
		 *
		 * query ids: used to propagate queryId to all spans in the same level
		 *
		 * top spans: We create one top span per nested level and those are
		 * only inserted in the shared buffer at the end
		 */
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		/* initial allocation */
		allocated_nested_level = 1;
		per_level_buffers = palloc0(allocated_nested_level * sizeof(pgTracingPerLevelBuffer));
		/* 0 is a valid index, initialise all indexes to -1 */
		for (int i = 0; i < allocated_nested_level; i++)
			per_level_buffers[i].top_span_index = -1;
		current_trace_spans = palloc0(pg_tracing_initial_allocated_spans * sizeof(Span));
		current_trace_spans->max = pg_tracing_initial_allocated_spans;
		current_trace_text = makeStringInfo();

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Parallel workers will get the referential timers from the leader
		 * parallel context
		 */
		if (!IsParallelWorker())
		{
			/* Start referential timers */
			current_trace.start_trace.ts = GetCurrentStatementStartTimestamp();

			/*
			 * We can't get the monotonic clock at the start of the statement
			 * so let's estimate it here.
			 */
			current_trace.start_trace.ns = get_current_ns() - (GetCurrentTimestamp() - current_trace.start_trace.ts) * NS_PER_US;
		}
	}
	else if (exec_nested_level >= allocated_nested_level)
	{
		/* New nested level, allocate more memory */
		MemoryContext oldcxt;
		int			old_allocated_nested_level = allocated_nested_level;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		allocated_nested_level++;
		per_level_buffers = repalloc0(per_level_buffers, old_allocated_nested_level * sizeof(pgTracingPerLevelBuffer), allocated_nested_level * sizeof(pgTracingPerLevelBuffer));
		for (int i = old_allocated_nested_level; i < allocated_nested_level; i++)
			per_level_buffers[i].top_span_index = -1;
		MemoryContextSwitchTo(oldcxt);
	}

	max_nested_level = exec_nested_level;
	if (exec_nested_level == 0)
	{
		/* Start of a new trace */
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->stats.traces++;
		SpinLockRelease(&s->mutex);
	}
	return true;
}

/*
 * Add a new string to the current_trace_text stringinfo
 */
static int
add_str_to_trace_buffer(const char *str, int str_len)
{
	int			position = current_trace_text->cursor;

	appendBinaryStringInfo(current_trace_text, str, str_len);
	appendStringInfoChar(current_trace_text, '\0');
	current_trace_text->cursor = current_trace_text->len;
	return position;
}

/*
 * Add the worker name to the current_trace_text stringinfo
 */
static int
add_worker_name_to_trace_buffer()
{
	int			position = current_trace_text->cursor;

	appendStringInfo(current_trace_text, "Worker %d", ParallelWorkerNumber);
	appendStringInfoChar(current_trace_text, '\0');
	current_trace_text->cursor = current_trace_text->len;
	return position;
}

/*
 * Start a new top span if we've entered a new nested level
 */
static void
begin_top_span(Span * top_span, CmdType commandType,
			   const Query *query, const JumbleState *jstate,
			   const char *query_text, int64 start_time_ns)
{
	int			query_len;
	const char *normalised_query;

	/* in case of a cached plan, query might be unavailable */
	if (query != NULL)
		per_level_buffers[exec_nested_level].query_id = query->queryId;
	else if (current_trace.query_id > 0)
		per_level_buffers[exec_nested_level].query_id = current_trace.query_id;

	begin_span(top_span, command_type_to_span_type(commandType),
			   &current_trace, get_parent_id(exec_nested_level - 1), per_level_buffers[exec_nested_level].query_id, &start_time_ns, exec_nested_level);
	top_span->is_top_span = true;

	if (IsParallelWorker())
	{
		/*
		 * In a parallel worker, we use the worker name as the span's
		 * operation
		 */
		top_span->operation_name_offset = add_worker_name_to_trace_buffer();
		return;
	}

	if (jstate && jstate->clocations_count > 0 && query != NULL)
	{
		/* jstate is available, normalise query and extract parameters' values */
		char	   *paramStr;

		query_len = query->stmt_len;
		normalised_query = normalise_query_parameters(jstate, query_text,
													  query->stmt_location,
													  &query_len, &paramStr);
		top_span->parameter_offset = add_str_to_trace_buffer(paramStr, strlen(paramStr));
	}
	else
	{
		/*
		 * No jstate available, normalise query but we won't be able to
		 * extract parameters
		 */
		int			stmt_location;

		if (query != NULL && query->stmt_len > 0)
		{
			query_len = query->stmt_len;
			stmt_location = query->stmt_location;
		}
		else
		{
			query_len = strlen(query_text);
			stmt_location = 0;
		}
		normalised_query = normalise_query(query_text, stmt_location, &query_len);
	}
	top_span->operation_name_offset = add_str_to_trace_buffer(normalised_query, query_len);
}

/*
 * Initialise buffer if we are in a new nested level and start top span.
 * If the top span already exists for the current nested level, this has no effect.
 */
static int64
initialize_trace_level_and_top_span(CmdType commandType, Query *query, JumbleState *jstate, const char *query_text, int64 *start_time_ns)
{
	bool		new_nested_level = initialize_trace_level();
	int64		top_span_start_ns = get_top_span_start(start_time_ns);
	Span	   *current_top = get_top_span_for_nested_level(exec_nested_level);

	/* current_trace_spans buffer should have been allocated */
	Assert(current_trace_spans != NULL);
	if (new_nested_level)
	{
		begin_top_span(current_top, commandType, query, jstate, query_text, top_span_start_ns);
	}
	else if (current_top->ended == true)
	{
		/*
		 * It's possible to have multiple top spans within a nested query. If
		 * the previous top span was closed, start a new one
		 */
		int			span_index = get_index_from_trace_spans();
		Span	   *span = get_span_from_index(span_index);

		set_top_span(exec_nested_level, span_index);
		begin_top_span(span, commandType, query, jstate, query_text, top_span_start_ns);
	}
	return top_span_start_ns;
}

/*
 * Post-parse-analyze hook
 *
 * Tracing can be started here if:
 * - The query has a SQLCommenter with traceparent parameter with sampled flag on and passes the caller_sample_rate
 * - The query passes the sample_rate
 *
 * If query is sampled, create a post parse span and start the top span.
 */
static void
pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	Span	   *post_parse_span;
	int			post_parse_index = -1;
	int64		start_post_parse_ns;
	uint64		parent_id;
	int64		top_span_start_ns;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/*
	 * In case there's an multi statement transaction using extended protocol,
	 * we will have the PREPARE of the next statement before we go through the
	 * ExecutorEnd of the previous statement.
	 *
	 * For now, we skip parse span and will only start the next statement
	 * tracing during planner hook.
	 *
	 * TODO: Find a way to keep parse span for the next statement
	 */
	if (!pg_tracing_mem_ctx->isReset && exec_nested_level == 0)
		return;

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
		return;

	/* Evaluate if query is sampled or not */
	check_query_sampled(pstate, NULL);

	if (current_trace.sampled == 0)
		/* Query is not sampled, nothing to do */
		return;

	/*
	 * We want to avoid calling get_ns at the start of post parse as it will
	 * impact all queries and we will only use it when the query is sampled.
	 */
	start_post_parse_ns = get_current_ns();

	/*
	 * Either we're inside a nested sampled query or we've parsed a query with
	 * the sampled flag, start a new level with a top span
	 */
	top_span_start_ns = initialize_trace_level_and_top_span(query->commandType, query, jstate, pstate->p_sourcetext, &start_post_parse_ns);

	parent_id = get_parent_id(exec_nested_level);
	Assert(parent_id > 0);

	/*
	 * We only create parse span when we have a good idea of when the parse
	 * start. For nested queries created from the queryDesc
	 * (nested_query_start_ns == 0), we can't get a reliable time for the
	 * parse start. We don't create Parse span in this case for now.
	 */
	if (exec_nested_level == 0 || nested_query_start_ns > 0)
	{
		Span	   *parse_span = NULL;

		/* We don't have a precise time for parse end, estimate it */
		int64		end_parse_ns = start_post_parse_ns - 1000;
		int			parse_index = get_index_from_trace_spans();

		parse_span = get_span_from_index(parse_index);
		begin_span(parse_span, SPAN_PARSE,
				   &current_trace, parent_id, per_level_buffers[exec_nested_level].query_id,
				   &top_span_start_ns, exec_nested_level);
		end_span(parse_span, &end_parse_ns);
	}

	/* Post parse span */
	post_parse_index = get_index_from_trace_spans();
	post_parse_span = get_span_from_index(post_parse_index);
	begin_span(post_parse_span, SPAN_POST_PARSE,
			   &current_trace,
			   parent_id, per_level_buffers[exec_nested_level].query_id,
			   &start_post_parse_ns, exec_nested_level);
	end_span(post_parse_span, NULL);
}

/*
 * Planner hook
 * Tracing can start here if the query skipped parsing (prepared query) and passes the random sample_rate.
 * If query is sampled, create a plan span and start the top span if not already started.
 */
static PlannedStmt *
pg_tracing_planner_hook(Query *query,
						const char *query_string,
						int cursorOptions,
						ParamListInfo params)
{
	PlannedStmt *result;
	int			span_planner_index = -1;
	Span	   *span_planner;

	/* Evaluate if query is sampled or not */
	check_query_sampled(NULL, params);

	if (current_trace.sampled == 0 || !pg_tracing_enabled(plan_nested_level + exec_nested_level))
	{
		/* No sampling */
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions,
									   params);
		else
			result = standard_planner(query, query_string, cursorOptions,
									  params);
		return result;
	}

	/*
	 * We may have skipped parsing if statement was prepared, create a new top
	 * span in this case.
	 */
	if (current_trace.sampled == 1 && pg_tracing_enabled(plan_nested_level + exec_nested_level))
		initialize_trace_level_and_top_span(query->commandType, query, NULL, query_string, NULL);

	/* Start planner span */
	span_planner_index = get_index_from_trace_spans();
	span_planner = get_span_from_index(span_planner_index);
	begin_span(span_planner, SPAN_PLANNER, &current_trace,
			   get_parent_id(exec_nested_level), per_level_buffers[exec_nested_level].query_id, NULL, exec_nested_level);

	plan_nested_level++;
	PG_TRY();
	{
		if (prev_planner_hook)
			result = prev_planner_hook(query, query_string, cursorOptions,
									   params);
		else
			result = standard_planner(query, query_string, cursorOptions,
									  params);
	}
	PG_CATCH();
	{
		plan_nested_level--;
		handle_pg_error(span_planner_index, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	plan_nested_level--;

	/* End planner span */
	end_span_index(span_planner_index, NULL);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (params != NULL)
	{
		char	   *paramStr = BuildParamLogString(params,
												   NULL, pg_tracing_max_parameter_str);
		Span	   *current_top = get_top_span_for_nested_level(exec_nested_level);

		Assert(current_top->parameter_offset == -1);
		if (paramStr != NULL)
			current_top->parameter_offset = add_str_to_trace_buffer(paramStr, strlen(paramStr));
	}
	return result;
}

/*
 * ExecutorStart hook: Activate query instrumentation if query is sampled
 * Tracing can be started here if the query used a cached plan and passes the random sample_rate
 * If query is sampled, start the top span if not already started.
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			executor_start_span_index = -1;
	Span	   *executor_span;
	int			instrument_options = INSTRUMENT_TIMER | INSTRUMENT_ROWS;

	/* Evaluate if query is sampled or not */
	check_query_sampled(NULL, queryDesc->params);

	if (pg_tracing_instrument_buffers)
		instrument_options |= INSTRUMENT_BUFFERS;
	if (pg_tracing_instrument_wal)
		instrument_options |= INSTRUMENT_WAL;

	if (current_trace.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook. Parallel workers will also start tracing from
		 * there. Create the top case in this case.
		 */
		initialize_trace_level_and_top_span(queryDesc->operation, NULL, NULL, queryDesc->sourceText, NULL);

		/* Start executorStart Span */
		executor_start_span_index = get_index_from_trace_spans();
		executor_span = get_span_from_index(executor_start_span_index);
		begin_span(executor_span, SPAN_EXECUTOR_START, &current_trace, get_parent_id(exec_nested_level), per_level_buffers[exec_nested_level].query_id, NULL, exec_nested_level);

		/*
		 * Activate query instrumentation. Timer and rows instrumentation are
		 * mandatory.
		 */
		queryDesc->instrument_options = instrument_options;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (executor_start_span_index != -1)
	{
		/* Allocate instrumentation in the per-query context */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, instrument_options, false);
			queryDesc->planstate->instrument = InstrAlloc(1, instrument_options, false);
			MemoryContextSwitchTo(oldcxt);
		}
		end_span_index(executor_start_span_index, NULL);
	}
}

/*
 * ExecutorRun hook: track nesting depth and create executor run span.
 * If the plan needs to create parallel workers, push the trace context in the parallel shared buffer.
 */
static void
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					   uint64 count, bool execute_once)
{
	int			span_index = -1;

	if (current_trace.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		Span	   *executor_run_span;

		/* Start executor run span */
		span_index = get_index_from_trace_spans();
		executor_run_span = get_span_from_index(span_index);
		begin_span(executor_run_span, SPAN_EXECUTOR_RUN, &current_trace, get_parent_id(exec_nested_level),
				   per_level_buffers[exec_nested_level].query_id, NULL, exec_nested_level);

		/*
		 * Executor run is used as the parent's span for nodes, store its' id
		 * in the per level buffer.
		 */
		per_level_buffers[exec_nested_level].executor_id = executor_run_span->span_id;

		/*
		 * If this query starts parallel worker, push the trace context for
		 * the child processes
		 */
		if (queryDesc->plannedstmt->parallelModeNeeded)
			add_parallel_context(executor_run_span->span_id, per_level_buffers[exec_nested_level].query_id);
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_CATCH();
	{
		exec_nested_level--;
		handle_pg_error(span_index, queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nested_level--;

	/*
	 * When a query is executing the pg_tracing_spans function, sampling will
	 * be disabled while we have an ongoing trace and current_trace_spans will
	 * be NULL. Check that span_index != -1 isn't enough in this case, we need
	 * to check that current_trace_spans still exists.
	 */
	if (current_trace_spans != NULL)
		end_span_index(span_index, NULL);
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	int			span_index = -1;
	Span	   *span = NULL;

	if (current_trace.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		span_index = get_index_from_trace_spans();
		span = get_span_from_index(span_index);
		begin_span(span, SPAN_EXECUTOR_FINISH, &current_trace, get_parent_id(exec_nested_level), per_level_buffers[exec_nested_level].query_id, NULL, exec_nested_level);
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_CATCH();
	{
		exec_nested_level--;
		handle_pg_error(span_index, queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nested_level--;

	end_span_index(span_index, NULL);
}

/*
 * ExecutorEnd hook:
 * - process queryDesc to generate span from planstate
 * - end top span for the current nested level
 * - end tracing if we're at the root nested level
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	int			executor_end_index = -1;

	if (current_trace.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		Span	   *executor_end_span;

		/* Create executor end span */
		executor_end_index = get_index_from_trace_spans();
		executor_end_span = get_span_from_index(executor_end_index);
		begin_span(executor_end_span, SPAN_EXECUTOR_END, &current_trace, get_parent_id(exec_nested_level), per_level_buffers[exec_nested_level].query_id,
				   NULL, exec_nested_level);

		/* Query finished normally, send 0 as error code */
		process_query_desc(queryDesc, 0);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (executor_end_index > -1)
	{
		int64		end_span_ns = get_current_ns();

		/* End top span, executor_end span and tracing */
		end_top_span(exec_nested_level, end_span_ns);
		end_span_index(executor_end_index, &end_span_ns);
		end_tracing(&end_span_ns);
	}
}

/*
 * ProcessUtility hook
 *
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 */
static void
pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	Span	   *process_utility_span;
	Span	   *previous_top_span;
	int			process_utility_span_index = -1;
	int			previous_top_span_index = -1;
	int64		end_span_ns;
	int64		start_span_ns;

	if (!pg_tracing_track_utility || current_trace.sampled == 0 || !pg_tracing_enabled(exec_nested_level))
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);

		/*
		 * Tracing may have been started within the utility call without going
		 * through ExecutorEnd (ex: prepare statement). Check and end tracing
		 * here in this case.
		 */
		if (!pg_tracing_mem_ctx->isReset)
		{
			Assert(current_trace.sampled);
			Assert(exec_nested_level == 0);
			end_tracing(NULL);
		}
		return;
	}

	start_span_ns = get_current_ns();

	/*
	 * Set a possible start for nested query. ProcessUtility may start a
	 * nested query and we can't use the statement start like we usually do.
	 * We artificially start nested 1us later to avoid overlapping spans.
	 */
	nested_query_start_ns = start_span_ns + 1000;

	/* Build the process utility span. */
	process_utility_span_index = get_index_from_trace_spans();
	process_utility_span = get_span_from_index(process_utility_span_index);
	begin_span(process_utility_span, SPAN_PROCESS_UTILITY, &current_trace,
			   get_parent_id(exec_nested_level), per_level_buffers[exec_nested_level].query_id, &start_span_ns, exec_nested_level);

	/* Extract and normalise the query */
	if (queryString != NULL && pstmt->stmt_location != -1 && pstmt->stmt_len > 0)
	{
		int			query_len = pstmt->stmt_len;
		const char *normalised_query = normalise_query(queryString, pstmt->stmt_location, &query_len);

		process_utility_span->operation_name_offset = add_str_to_trace_buffer(normalised_query, query_len);
	}

	/*
	 * Set ProcessUtility span as the new top span for the nested queries that
	 * may be created. We need to keep the old top span index to end it at the
	 * end of the process utility hook.
	 */
	previous_top_span_index = per_level_buffers[exec_nested_level].top_span_index;
	set_top_span(exec_nested_level, process_utility_span_index);

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
	}
	PG_CATCH();
	{
		exec_nested_level--;
		handle_pg_error(previous_top_span_index, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	end_span_ns = get_current_ns();

	/* ProcessUtility may have created a top span, end it. */
	if (exec_nested_level <= max_nested_level && per_level_buffers[exec_nested_level].top_span_index != -1)
	{
		Span	   *top_span = get_top_span_for_nested_level(exec_nested_level);

		if (top_span->ended == false)
			end_top_span(exec_nested_level, end_span_ns);
	}
	exec_nested_level--;

	/*
	 * buffer may have been repalloced, refresh the process_utility_span
	 * variable
	 */
	process_utility_span = get_span_from_index(process_utility_span_index);
	if (qc != NULL)
		process_utility_span->node_counters.rows = qc->nprocessed;
	/* ProcessUtility is the current top span, no need to call end_top_span */
	end_span(process_utility_span, &end_span_ns);

	/* Also end the previous top span */
	previous_top_span = get_span_from_index(previous_top_span_index);
	end_span(previous_top_span, &end_span_ns);

	Assert(previous_top_span->start < process_utility_span->start);
	Assert(get_span_end_ns(previous_top_span) >= get_span_end_ns(process_utility_span));

	end_tracing(&end_span_ns);

	/*
	 * We may iterate through a list of nested ProcessUtility calls, update
	 * the nested query start.
	 */
	nested_query_start_ns = end_span_ns + 1000;
}

/*
 * Iterate over a list of planstate to generate span node
 */
static void
generate_member_nodes(PlanState **planstates, int nplans, planstateTraceContext * planstateTraceContext, uint64 parent_id)
{
	int			j;

	for (j = 0; j < nplans; j++)
		generate_span_from_planstate(planstates[j], planstateTraceContext, parent_id);
}

/*
 * Iterate over custom scan planstates to generate span node
 */
static void
generate_span_from_custom_scan(CustomScanState *css, planstateTraceContext * planstateTraceContext, uint64 parent_id)
{
	ListCell   *cell;

	foreach(cell, css->custom_ps)
		generate_span_from_planstate((PlanState *) lfirst(cell), planstateTraceContext, parent_id);
}

/*
 * With nested queries, the post parse hook will be called before we know
 * the top span parent. We will only know and the parent when we generate span nodes from planstate.
 *
 * When those nodes are created, we attach the top span.
 */
static void
attach_top_span_to_new_parent(Span const *new_parent, int64 parent_end_ns_time)
{
	Span	   *child_top_span;
	int64		child_end_ns_time;

	if (exec_nested_level + 1 > max_nested_level)
		/* No child to attach */
		return;

	child_top_span = get_top_span_for_nested_level(exec_nested_level + 1);
	Assert(child_top_span->start_ns_time > 0);
	/* If child start after parent's end, don't link it */
	if (parent_end_ns_time < child_top_span->start_ns_time)
		return;
	child_end_ns_time = child_top_span->start_ns_time + child_top_span->duration_ns;
	/* If child ends before parent's start, don't link it */
	if (child_end_ns_time < new_parent->start_ns_time)
		return;

	/* Child span should already have ended */
	Assert(child_top_span->ended == true);
	/* Link this child to the new parent */
	child_top_span->parent_id = new_parent->span_id;
}

/*
 * Create span node for the provided planstate
 */
static uint64
create_span_node(PlanState *planstate, planstateTraceContext * planstateTraceContext, uint64 parent_id,
				 SpanType span_type, char *subplan_name)
{
	Span	   *span;
	int			span_index = -1;
	char const *span_name;
	char const *operation_name;
	Plan const *plan = planstate->plan;
	int64		span_end_ns;
	int64		start_span_ns;

	/*
	 * Make sure stats accumulation is done. Function is a no-op if if was
	 * already done.
	 */
	InstrEndLoop(planstate->instrument);

	/* We only create span node on node that were executed */
	Assert(!INSTR_TIME_IS_ZERO(planstate->instrument->firsttime));

	start_span_ns = INSTR_TIME_GET_NANOSEC(planstate->instrument->firsttime);
	span_index = get_index_from_trace_spans();
	span = get_span_from_index(span_index);

	begin_span(span, span_type, planstateTraceContext->trace, parent_id, per_level_buffers[exec_nested_level].query_id,
			   &start_span_ns, exec_nested_level);

	/* first tuple time */
	span->startup = planstate->instrument->startup * NS_PER_S;

	if (span_type == SPAN_NODE)
	{
		/* Generate names and store them */
		span_name = plan_to_span_name(plan);
		operation_name = plan_to_operation(planstateTraceContext, planstate, span_name);

		span->name_offset = add_str_to_trace_buffer(span_name, strlen(span_name));
		span->operation_name_offset = add_str_to_trace_buffer(operation_name, strlen(operation_name));
	}
	else if (subplan_name != NULL)
	{
		span->operation_name_offset = add_str_to_trace_buffer(subplan_name, strlen(subplan_name));
	}

	span->node_counters.rows = (int64) planstate->instrument->ntuples / planstate->instrument->nloops;
	span->node_counters.nloops = (int64) planstate->instrument->nloops;
	span->node_counters.buffer_usage = planstate->instrument->bufusage;
	span->node_counters.wal_usage = planstate->instrument->walusage;

	span->plan_counters.startup_cost = plan->startup_cost;
	span->plan_counters.total_cost = plan->total_cost;
	span->plan_counters.plan_rows = plan->plan_rows;
	span->plan_counters.plan_width = plan->plan_width;

	if (!planstate->state->es_finished)
	{
		/*
		 * If the query is in an unfinished state, it means that we're in an
		 * error handler. Stop the node instrumentation to get the latest
		 * known state.
		 */
		InstrStopNode(planstate->instrument, planstate->state->es_processed);
		InstrEndLoop(planstate->instrument);
		span->sql_error_code = planstateTraceContext->sql_error_code;
	}
	span_end_ns = INSTR_TIME_GET_NANOSEC(planstate->instrument->firsttime);
	Assert(planstate->instrument->total > 0);
	span_end_ns += planstate->instrument->total * NS_PER_S;
	end_span(span, &span_end_ns);

	/*
	 * If we have a Result node, make it the span parent of the next query
	 * span if we have any
	 *
	 * TODO: Do we have other node that could be the base for nested queries?
	 */
	if (nodeTag(plan) == T_Result || nodeTag(plan) == T_ProjectSet)
		attach_top_span_to_new_parent(span, span_end_ns);
	return span->span_id;
}

/*
 * Walk through the planstate tree generating a node span for each node.
 * We pass possible error code to tag unfinished node with it
 */
static void
generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext, uint64 parent_id)
{
	ListCell   *l;
	Plan const *plan = planstate->plan;
	uint64		span_id;

	/* The node was never executed, skip it */
	if (INSTR_TIME_IS_ZERO(planstate->instrument->firsttime))
		return;

	span_id = create_span_node(planstate, planstateTraceContext, parent_id, SPAN_NODE, NULL);
	planstateTraceContext->ancestors = lcons(planstate->plan, planstateTraceContext->ancestors);

	/* Walk the lefttree */
	if (outerPlanState(planstate))
		generate_span_from_planstate(outerPlanState(planstate), planstateTraceContext, span_id);
	/* Walk the righttree */
	if (innerPlanState(planstate))
		generate_span_from_planstate(innerPlanState(planstate), planstateTraceContext, span_id);

	/* Handle init plans */
	foreach(l, planstate->initPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		uint64_t	init_span_id;

		if (INSTR_TIME_IS_ZERO(splan->instrument->firsttime))
			continue;
		init_span_id = create_span_node(splan, planstateTraceContext, parent_id, SPAN_NODE_INIT_PLAN, sstate->subplan->plan_name);
		generate_span_from_planstate(splan, planstateTraceContext, init_span_id);
	}

	/* Handle sub plans */
	foreach(l, planstate->subPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		uint64_t	subplan_span_id;

		if (INSTR_TIME_IS_ZERO(splan->instrument->firsttime))
			continue;
		subplan_span_id = create_span_node(splan, planstateTraceContext, parent_id, SPAN_NODE_SUBPLAN, sstate->subplan->plan_name);
		generate_span_from_planstate(splan, planstateTraceContext, subplan_span_id);
	}

	/* Handle special nodes with children nodes */
	switch (nodeTag(plan))
	{
		case T_Append:
			generate_member_nodes(((AppendState *) planstate)->appendplans,
								  ((AppendState *) planstate)->as_nplans, planstateTraceContext, span_id);
			break;
		case T_MergeAppend:
			generate_member_nodes(((MergeAppendState *) planstate)->mergeplans,
								  ((MergeAppendState *) planstate)->ms_nplans, planstateTraceContext, span_id);
			break;
		case T_BitmapAnd:
			generate_member_nodes(((BitmapAndState *) planstate)->bitmapplans,
								  ((BitmapAndState *) planstate)->nplans, planstateTraceContext, span_id);
			break;
		case T_BitmapOr:
			generate_member_nodes(((BitmapOrState *) planstate)->bitmapplans,
								  ((BitmapOrState *) planstate)->nplans, planstateTraceContext, span_id);
			break;
		case T_SubqueryScan:
			generate_span_from_planstate(((SubqueryScanState *) planstate)->subplan, planstateTraceContext, span_id);
			break;
		case T_CustomScan:
			generate_span_from_custom_scan((CustomScanState *) planstate, planstateTraceContext, span_id);
			break;
		default:
			break;
	}
	planstateTraceContext->ancestors = list_delete_first(planstateTraceContext->ancestors);
}

/*
 * Add plan counters to the Datum output
 */
static int
add_plan_counters(const PlanCounters * plan_counters, int i, Datum *values)
{
	values[i++] = Float8GetDatumFast(plan_counters->startup_cost);
	values[i++] = Float8GetDatumFast(plan_counters->total_cost);
	values[i++] = Float8GetDatumFast(plan_counters->plan_rows);
	values[i++] = Int32GetDatum(plan_counters->plan_width);
	return i;
}

/*
 * Add node counters to the Datum output
 */
static int
add_node_counters(const NodeCounters * node_counters, int i, Datum *values)
{
	Datum		wal_bytes;
	char		buf[256];
	double		blk_read_time,
				blk_write_time,
				temp_blk_read_time,
				temp_blk_write_time;
	double		generation_counter,
				inlining_counter,
				optimization_counter,
				emission_counter;
	int64		jit_created_functions;

	values[i++] = Int64GetDatumFast(node_counters->rows);
	values[i++] = Int64GetDatumFast(node_counters->nloops);

	/* Buffer usage */
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_written);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_written);

	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_write_time);
	temp_blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time);
	temp_blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time);

	values[i++] = Float8GetDatumFast(blk_read_time);
	values[i++] = Float8GetDatumFast(blk_write_time);
	values[i++] = Float8GetDatumFast(temp_blk_read_time);
	values[i++] = Float8GetDatumFast(temp_blk_write_time);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_written);

	/* WAL usage */
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_records);
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_fpi);
	snprintf(buf, sizeof buf, UINT64_FORMAT, node_counters->wal_usage.wal_bytes);

	/* Convert to numeric. */
	wal_bytes = DirectFunctionCall3(numeric_in,
									CStringGetDatum(buf),
									ObjectIdGetDatum(0),
									Int32GetDatum(-1));
	values[i++] = wal_bytes;

	/* JIT usage */
	generation_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.generation_counter);
	inlining_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.inlining_counter);
	optimization_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.optimization_counter);
	emission_counter = INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.emission_counter);
	jit_created_functions = node_counters->jit_usage.created_functions;

	values[i++] = Int64GetDatumFast(jit_created_functions);
	values[i++] = Float8GetDatumFast(generation_counter);
	values[i++] = Float8GetDatumFast(inlining_counter);
	values[i++] = Float8GetDatumFast(optimization_counter);
	values[i++] = Float8GetDatumFast(emission_counter);

	return i;
}

/*
 * Build the tuple for a Span and add it to the output
 */
static void
add_result_span(ReturnSetInfo *rsinfo, Span * span,
				const char *qbuffer, Size qbuffer_size)
{
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};
	const char *span_name;
	const char *operation_name;
	const char *sql_error_code;
	int			i = 0;

	span_name = get_span_name(span, qbuffer, qbuffer_size);
	operation_name = get_operation_name(span, qbuffer, qbuffer_size);
	sql_error_code = unpack_sql_state(span->sql_error_code);

	Assert(span_name != NULL);
	Assert(operation_name != NULL);
	Assert(sql_error_code != NULL);

	values[i++] = UInt64GetDatum(span->trace_id);
	values[i++] = UInt64GetDatum(span->parent_id);
	values[i++] = UInt64GetDatum(span->span_id);
	values[i++] = UInt64GetDatum(span->query_id);
	values[i++] = CStringGetTextDatum(span_name);
	values[i++] = CStringGetTextDatum(operation_name);
	values[i++] = Int64GetDatumFast(span->start);
	values[i++] = Int16GetDatum(span->start_ns);

	values[i++] = Int64GetDatumFast(span->duration_ns);
	values[i++] = CStringGetTextDatum(sql_error_code);
	values[i++] = UInt32GetDatum(span->be_pid);
	values[i++] = UInt8GetDatum(span->nested_level);
	values[i++] = UInt8GetDatum(span->subxact_count);
	values[i++] = BoolGetDatum(span->is_top_span);

	if ((span->type >= SPAN_NODE && span->type <= SPAN_NODE_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		i = add_plan_counters(&span->plan_counters, i, values);
		i = add_node_counters(&span->node_counters, i, values);

		values[i++] = Int64GetDatumFast(span->startup);
		if (span->parameter_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->parameter_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->parameter_offset);
	}

	for (int j = i; j < PG_TRACING_TRACES_COLS; j++)
		nulls[j] = 1;

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * Return spans as a result set.
 *
 * Accept a consume parameter. When consume is set,
 * we empty the shared buffer and truncate query text.
 */
Datum
pg_tracing_spans(PG_FUNCTION_ARGS)
{
	bool		consume;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Span	   *span;
	const char *qbuffer;
	Size		qbuffer_size = 0;

	consume = PG_GETARG_BOOL(0);
	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));
	InitMaterializedSRF(fcinfo, 0);

	/*
	 * If this query was sampled and we're consuming tracing_spans buffer, the
	 * spans will target a query string that doesn't exist anymore in the
	 * query file. Better abort the sampling, clean ongoing traces and remove
	 * any possible parallel context pushed Since this will be called within
	 * an ExecutorRun, we will need to check for current_trace_spans at the
	 * end of the ExecutorRun hook.
	 */
	cleanup_tracing();

	qbuffer = qtext_load_file(&qbuffer_size);
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		for (int i = 0; i < shared_spans->end; i++)
		{
			span = shared_spans->spans + i;
			add_result_span(rsinfo, span, qbuffer, qbuffer_size);
		}

		/* Consume is set, remove spans from the shared buffer */
		if (consume)
		{
			shared_spans->end = 0;
			/* We only truncate query file if there's no active writers */
			if (s->n_writers == 0)
			{
				s->extent = 0;
				pg_truncate(PG_TRACING_TEXT_FILE, 0);
			}
			else
				s->stats.failed_truncates++;
		}
		s->stats.last_consume = GetCurrentTimestamp();
		SpinLockRelease(&s->mutex);
	}
	return (Datum) 0;
}

/*
 * Return statistics of pg_tracing.
 */
Datum
pg_tracing_info(PG_FUNCTION_ARGS)
{
	pgTracingStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};
	int			i = 0;

	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Get a copy of the pg_tracing stats */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		stats = s->stats;
		SpinLockRelease(&s->mutex);
	}

	values[i++] = Int64GetDatum(stats.traces);
	values[i++] = Int64GetDatum(stats.spans);
	values[i++] = Int64GetDatum(stats.dropped_spans);
	values[i++] = Int64GetDatum(stats.failed_truncates);
	values[i++] = TimestampTzGetDatum(stats.last_consume);
	values[i++] = TimestampTzGetDatum(stats.stats_reset);
	values[i++] = Float8GetDatum(pg_tracing_sample_rate);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Get an empty pgTracingStats
 */
static pgTracingStats
get_empty_pg_tracing_stats(void)
{
	pgTracingStats stats;

	stats.traces = 0;
	stats.spans = 0;
	stats.dropped_spans = 0;
	stats.failed_truncates = 0;
	stats.last_consume = 0;
	stats.stats_reset = GetCurrentTimestamp();
	return stats;
}

/*
 * Reset pg_tracing statistics.
 */
Datum
pg_tracing_reset(PG_FUNCTION_ARGS)
{
	/*
	 * Reset statistics for pg_tracing since all entries are removed.
	 */
	pgTracingStats empty_stats = get_empty_pg_tracing_stats();

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->stats = empty_stats;
		SpinLockRelease(&s->mutex);
	}
	PG_RETURN_VOID();
}
