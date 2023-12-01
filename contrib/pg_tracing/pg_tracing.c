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
 * If sampled, we will generate spans for the current statement.
 * A span represents an operation with a start time, a duration and useful metadatas (block stats, wal stats...).
 * We will track the following operations:
 * - Top Span: The top span for a statement. They are created after extracting the traceid from traceparent or to represent a nested query.
 * - Planner: We track the time spent in the planner and report the planner counters
 * - Node Span: Created from planstate. The name is extracted from the node type (IndexScan, SeqScan).
 * - Executor: We trace the different steps of the Executor: Start, Run, Finish and End
 *
 * A typical traced query will generate the following spans:
 * +------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | Type: Select (top span)                                                                                                                              |
 * | Operation: Select * pgbench_accounts WHERE aid=$1;                                                                                                   |
 * +---+------------------------+-+------------------+--+------------------------------------------------------+-+--------------------+-+----------------++
 *     | Type: Planner          | | Type: Executor   |  |Type: Executor                                        | | Type: Executor     | | Type: Executor |
 *     | Operation: Planner     | | Operation: Start |  |Operation: Run                                        | | Operation: Finish  | | Operation: End |
 *     +------------------------+ +------------------+  +--+--------------------------------------------------++ +--------------------+ +----------------+
 *                                                         | Type: IndexScan                                  |
 *                                                         | Operation: IndexScan using pgbench_accounts_pkey |
 *                                                         |            on pgbench_accounts                   |
 *                                                         +--------------------------------------------------+
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_tracing.h"
#include "pg_tracing_explain.h"
#include "pg_tracing_parallel.h"
#include "pg_tracing_query_process.h"
#include "pg_tracing_span.h"

#include "access/xact.h"
#include "common/pg_prng.h"
#include "commands/async.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
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
 * A span with it's position in the shared buffer
 */
typedef struct pgTracingStoredSpan
{
	int			index;			/* Position of the span in the
								 * current_trace_spans buffer */
	Span	   *span;			/* Pointer to the span. Might be invalid if
								 * current_trace_spans was repalloced */
}			pgTracingStoredSpan;

/* GUC variables */
static int	pg_tracing_max_span;	/* Maximum number of spans to store */
static int	pg_tracing_max_parameter_str;	/* Maximum number of spans to
											 * store */
static bool pg_tracing_trace_parallel_workers = true;	/* True to generate
														 * spans from parallel
														 * workers */
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
static bool pg_tracing_export_parameters = true;	/* Export query's
													 * parameters as span
													 * metadata */
static bool pg_tracing_deparse_plan = true; /* Deparse plan to generate more
											 * detailed spans */
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;	/* tracking level */
static bool pg_tracing_track_utility = true;	/* whether to track utility
												 * commands */
static bool pg_tracing_drop_full_buffer = true; /* whether to drop buffered
												 * spans when full */
static char *pg_tracing_notify_channel = NULL;	/* Name of the channel to
												 * notify when span buffer
												 * exceeds a provided
												 * threshold */
static double pg_tracing_notify_threshold = 0.8;	/* threshold for span
													 * buffer usage
													 * notification */

static const struct config_enum_entry track_options[] =
{
	{"none", PG_TRACING_TRACK_NONE, false},
	{"top", PG_TRACING_TRACK_TOP, false},
	{"all", PG_TRACING_TRACK_ALL, false},
	{NULL, 0, false}
};

#define pg_tracking_level(level) \
	((pg_tracing_track == PG_TRACING_TRACK_ALL || \
	(pg_tracing_track == PG_TRACING_TRACK_TOP && (level) == 0)))

#define pg_tracing_enabled(trace_context, level) \
	(trace_context->sampled && pg_tracking_level(level))

PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Global variables
 */

/* Memory context for pg_tracing. */
static MemoryContext pg_tracing_mem_ctx;

/* trace context at the root level of parse/planning hook */
static struct pgTracingTraceContext root_trace_context;

/* trace context used in nested levels or within executor hooks */
static struct pgTracingTraceContext current_trace_context;

/* Latest trace id observed */
uint64		latest_trace_id;

/* Latest local transaction id traced */
LocalTransactionId latest_lxid = InvalidLocalTransactionId;

/* Shared state with stats and file external state */
static pgTracingSharedState * pg_tracing = NULL;

/*
 * Shared buffer storing spans. Query with sampled flag will add new spans to
 * the shared state at the end of the traced query. Those spans will be consumed during calls to
 * pg_tracing_spans
 */
static pgTracingSpans * shared_spans = NULL;

/*
 * Store all ongoing spans for the curent trace.
 * They will be added to the shared buffer at the end of query tracing.
 *
 * This buffer can be dynamically resized so care needs to be taken when using span pointers.
 */
static pgTracingSpans * current_trace_spans;

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

/* Previous nested level observed during post parse */
static int	previous_parse_nested_level = 0;

/* Number of allocated levels. */
static int	allocated_nested_level = 0;

/* Possible start for a nested span */
static int64 nested_query_start_ns = 0;

/* Per nested level buffers */
typedef struct pgTracingPerLevelBuffer
{
	int			top_span_index; /* Index of the top span in the
								 * current_trace_spans buffer */
	int			parse_span_index;	/* Index of the parse span for this level */
	uint64		executor_run_id;	/* Span id of executor run spans by nested
									 * level Executor run is used as parent
									 * for spans generated from planstate */
	uint64		query_id;		/* Query id by nested level when available */
}			pgTracingPerLevelBuffer;

static pgTracingPerLevelBuffer * per_level_buffers;

/* Timestamp of the latest statement checked for sampling. */
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
static Span * generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext, uint64 parent_id);
static pgTracingStats get_empty_pg_tracing_stats(void);

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
							5000,
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

	DefineCustomBoolVariable("pg_tracing.trace_parallel_workers",
							 "Whether to generate samples from parallel workers.",
							 NULL,
							 &pg_tracing_trace_parallel_workers,
							 true,
							 PGC_SUSET,
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

	DefineCustomBoolVariable("pg_tracing.export_parameters",
							 "Export query's parameters as span metadata.",
							 NULL,
							 &pg_tracing_export_parameters,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_tracing.deparse_plan",
							 "Deparse query plan to generate details more details on a plan node.",
							 NULL,
							 &pg_tracing_deparse_plan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("pg_tracing.notify_channel",
							   "Name of the channel to notify when span buffer reaches a provided threshold.",
							   NULL,
							   &pg_tracing_notify_channel,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomRealVariable("pg_tracing.notify_threshold",
							 "When span buffer exceeds this threshold, a notification will be sent.",
							 NULL,
							 &pg_tracing_notify_threshold,
							 0.8,
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
	size = add_size(size, mul_size(max_worker_processes, sizeof(pgTracingParallelContext)));

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
 * Allocate a new span in the current_trace_spans buffer.
 * current_trace_spans may be extended if needed.
 */
static pgTracingStoredSpan
get_new_stored_span(void)
{
	pgTracingStoredSpan result;

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
	result.index = current_trace_spans->end++;
	result.span = &current_trace_spans->spans[result.index];
	return result;
}

/*
 * Get span from the provided span index
 */
static pgTracingStoredSpan
get_span_from_index(int span_index)
{
	pgTracingStoredSpan stored_span;

	Assert(span_index < current_trace_spans->end);
	Assert(span_index != -1);
	stored_span.index = span_index;
	stored_span.span = &current_trace_spans->spans[span_index];
	return stored_span;
}

/*
 * Get the span index of the top span for a specific nested level
 */
static pgTracingStoredSpan
get_top_span_for_nested_level(const pgTracingTraceContext * trace_context, int nested_level)
{
	Assert(nested_level <= max_nested_level);
	Assert(nested_level >= 0);
	/* For the root level, the top span is stored in the trace context */
	if (nested_level == 0)
		return get_span_from_index(trace_context->root_span_index);
	return get_span_from_index(per_level_buffers[nested_level].top_span_index);
}

/*
 * Set top span index to the provided span index
 */
static void
set_top_span_for_nested_level(pgTracingTraceContext * trace_context, int nested_level, int top_span_index)
{
	Assert(nested_level <= max_nested_level);
	Assert(top_span_index < current_trace_spans->max);
	Assert(top_span_index >= 0);
	if (nested_level == 0)
		trace_context->root_span_index = top_span_index;
	else
		per_level_buffers[nested_level].top_span_index = top_span_index;
}

/*
 * End the span at the provided index
 */
static void
end_span_index(int span_index, const int64 *end_span_input)
{
	pgTracingStoredSpan stored_span;

	if (span_index < 0)
		return;
	stored_span = get_span_from_index(span_index);
	end_span(stored_span.span, end_span_input);
}

/*
 * End top span for the provided nested level
 */
static void
end_top_span(const pgTracingTraceContext * trace_context, const int64 *end_span_ns)
{
	pgTracingStoredSpan top_span;

	if (exec_nested_level > max_nested_level)
		return;
	top_span = get_top_span_for_nested_level(trace_context, exec_nested_level);
	if (top_span.index != -1 && !top_span.span->ended)
		end_span(top_span.span, end_span_ns);
}

/*
 * Allocate a new span and set its start and end
 */
static int
create_full_span(const pgTracingTraceContext * trace_context, SpanType type,
				 uint64 parent_id, int64 start_span_ns, const int64 *end_span_ns)
{
	pgTracingStoredSpan stored_span = get_new_stored_span();

	begin_span(trace_context, stored_span.span, type, parent_id,
			   per_level_buffers[exec_nested_level].query_id,
			   &start_span_ns);
	end_span(stored_span.span, end_span_ns);
	return stored_span.index;
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
 * Reset trace_context fields
 */
static void
reset_trace_context(pgTracingTraceContext * trace_context)
{
	trace_context->sampled = 0;
	trace_context->trace_id = 0;
	trace_context->parent_id = 0;
	trace_context->root_span_index = -1;
	trace_context->start_trace.ts = 0;
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

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pg_tracing = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	pg_tracing = ShmemInitStruct("PgTracing Shared", sizeof(pgTracingSharedState), &found_pg_tracing);
	shared_spans = ShmemInitStruct("PgTracing Spans",
								   sizeof(pgTracingSpans) + pg_tracing_max_span * sizeof(Span),
								   &found_shared_spans);

	/* Initialize pg_tracing memory context */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/* Initialize shmem for trace propagation to parallel workers */
	pg_tracing_shmem_parallel_startup();

	/* First time, let's init shared state */
	if (!found_pg_tracing)
	{
		pg_tracing->stats = get_empty_pg_tracing_stats();
		SpinLockInit(&pg_tracing->mutex);
	}
	if (!found_shared_spans)
	{
		shared_spans->end = 0;
		shared_spans->max = pg_tracing_max_span;
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters and generate span nodes from queryDesc planstate
 */
static void
process_query_desc(const pgTracingTraceContext * trace_context, QueryDesc *queryDesc, int sql_error_code)
{
	NodeCounters *node_counters = &get_top_span_for_nested_level(trace_context, exec_nested_level).span->node_counters;

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
		uint64		parent_id = per_level_buffers[exec_nested_level].executor_run_id;

		planstateTraceContext.rtable_names = select_rtable_names_for_explain(queryDesc->plannedstmt->rtable, rels_used);
		planstateTraceContext.trace_context = trace_context;
		planstateTraceContext.ancestors = NULL;
		planstateTraceContext.sql_error_code = sql_error_code;
		/* Prepare the planstate context for deparsing */
		planstateTraceContext.deparse_ctx = NULL;
		if (pg_tracing_deparse_plan)
			planstateTraceContext.deparse_ctx = deparse_context_for_plan_tree(queryDesc->plannedstmt, planstateTraceContext.rtable_names);

		generate_span_from_planstate(queryDesc->planstate, &planstateTraceContext, parent_id);
	}
}

/*
 * Create a trace id for the trace context if there's none.
 * If trace was started from the global sample rate without
 * a parent trace, we need to generate a random trace id.
 */
static void
set_trace_id(struct pgTracingTraceContext *trace_context)
{
	if (trace_context->trace_id != 0)
	{
		/* Update last lxid seen */
		latest_lxid = MyProc->lxid;
		return;
	}

	/*
	 * We want to keep the same trace id for all statements of the same
	 * transaction. For that, we check if we're in the same local xid.
	 */
	if (MyProc->lxid == latest_lxid)
	{
		trace_context->trace_id = latest_trace_id;
		return;
	}

	/*
	 * We leave parent_id to 0 as a way to indicate that this is a standalone
	 * trace.
	 */
	Assert(trace_context->parent_id == 0);
	trace_context->trace_id = pg_prng_uint64(&pg_global_prng_state);
	latest_trace_id = trace_context->trace_id;
	latest_lxid = MyProc->lxid;
}

/*
 * Decide whether a query should be sampled depending on the traceparent sampled
 * and the provided sample rate configurations
 */
static bool
apply_sample_rate(const struct pgTracingTraceContext *trace_context)
{
	double		rand;
	bool		sampled;

	/* Everything is sampled */
	if (pg_tracing_sample_rate >= 1.0)
		return true;

	/* No SQLCommenter sampled and no global sample rate */
	if (!trace_context->sampled && pg_tracing_sample_rate == 0.0)
		return false;

	/* SQLCommenter sampled caller sample */
	if (trace_context->sampled && pg_tracing_caller_sample_rate >= 1.0)
		return true;

	/*
	 * We have a non zero global sample rate or a sampled flag. Either way, we
	 * need a rand value
	 */
	rand = pg_prng_double(&pg_global_prng_state);
	if (trace_context->sampled)
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
 * first parameter and try to extract the trace context from this first parameter.
 */
static void
extract_trace_context_from_parameter(struct pgTracingTraceContext *trace_context, ParamListInfo params)
{
	Oid			typoutput;
	bool		typisvarlena;
	char	   *pstring;
	ParamExternData param;

	if (params == NULL || params->numParams == 0)
		return;

	param = params->params[0];
	/* We only check the first parameter and if it's a text parameter */
	if (param.ptype != TEXTOID)
		return;

	/* Get the text of the first string parameter */
	getTypeOutputInfo(param.ptype, &typoutput, &typisvarlena);
	pstring = OidOutputFunctionCall(typoutput, param.value);
	extract_trace_context_from_query(trace_context, pstring, true);
}

/*
 * Extract trace_context from either the query text or parameters.
 * Sampling rate will be applied to the trace_context.
 */
static void
extract_trace_context(struct pgTracingTraceContext *trace_context, ParseState *pstate, const ParamListInfo params)
{
	TimestampTz statement_start_ts;

	/* Safety check... */
	if (!pg_tracing || !pg_tracking_level(exec_nested_level))
		return;

	/* sampling already started */
	if (trace_context->sampled)
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
		if (pg_tracing_trace_parallel_workers)
			fetch_parallel_context(trace_context);
		return;
	}

	Assert(trace_context->root_span_index == -1);
	Assert(trace_context->trace_id == 0);

	if (params != NULL)
		extract_trace_context_from_parameter(trace_context, params);
	else if (pstate != NULL)
		extract_trace_context_from_query(trace_context, pstate->p_sourcetext, false);

	/*
	 * A statement can go through this function 3 times: post parsing, planner
	 * and executor run. If no sampling flag was extracted from SQLCommenter
	 * and we've already seen this statement (using statement_start ts), don't
	 * try to apply sample rate and exit.
	 */
	statement_start_ts = GetCurrentStatementStartTimestamp();
	if (trace_context->sampled == 0 && last_statement_check_for_sampling == statement_start_ts)
		return;
	/* First time we see this statement, save the time */
	last_statement_check_for_sampling = statement_start_ts;

	trace_context->sampled = apply_sample_rate(trace_context);

	if (trace_context->sampled && check_full_shared_spans())
		/* Buffer is full, abort sampling */
		trace_context->sampled = 0;

	if (trace_context->sampled)
		set_trace_id(trace_context);
	else
		/* No sampling, reset the context */
		reset_trace_context(trace_context);
}

/*
 * Adjust span's offsets with the provided file offset
 */
static void
adjust_file_offset(Span * span, Size file_position)
{
	if (span->node_type_offset != -1)
		span->node_type_offset += file_position;
	if (span->operation_name_offset != -1)
		span->operation_name_offset += file_position;
	if (span->deparse_info_offset != -1)
		span->deparse_info_offset += file_position;
	if (span->parameter_offset != -1)
		span->parameter_offset += file_position;
}

/*
 * Reset pg_tracing memory context and global state.
 */
static void
cleanup_tracing(void)
{
	if (!root_trace_context.sampled && !current_trace_context.sampled)
		/* No need for cleaning */
		return;
	if (pg_tracing_trace_parallel_workers)
		remove_parallel_context();
	MemoryContextReset(pg_tracing_mem_ctx);
	reset_trace_context(&root_trace_context);
	reset_trace_context(&current_trace_context);
	max_nested_level = -1;
	previous_parse_nested_level = 0;
	nested_query_start_ns = 0;
	current_trace_spans = NULL;
	per_level_buffers = NULL;
}

/*
 * End the query tracing and dump all spans in the shared buffer.
 * This may happen either when query is finished or on a caught error.
 */
static void
end_tracing(void)
{
	Size		file_position = 0;
	int			start_spans_number;
	int			end_spans_number;

	/* We're still a nested query, tracing is not finished */
	if (exec_nested_level + plan_nested_level > 0)
		return;

	/* Dump all buffered texts in file */
	text_store_file(pg_tracing, current_trace_text->data, current_trace_text->len, &file_position);

	for (int i = 0; i < current_trace_spans->end; i++)
		adjust_file_offset(current_trace_spans->spans + i, file_position);

	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		start_spans_number = shared_spans->end;
		/* We're at the end, add all spans to the shared memory */
		for (int i = 0; i < current_trace_spans->end; i++)
			add_span_to_shared_buffer_locked(&current_trace_spans->spans[i]);
		end_spans_number = shared_spans->end;
		SpinLockRelease(&s->mutex);
	}

	if (pg_tracing_notify_channel != NULL && !IsParallelWorker())
	{
		int			span_threshold = pg_tracing_max_span * pg_tracing_notify_threshold;

		if (start_spans_number < span_threshold && end_spans_number >= span_threshold)
			/* We've crossed the threshold, send a notification */
			Async_Notify(pg_tracing_notify_channel, NULL);
	}

	/* We can reset the memory context here */
	cleanup_tracing();
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean
 * our memory context.
 *
 * We provide the ongoing span index where the error was caught to attach the
 * sql error code to it.
 */
static void
handle_pg_error(const pgTracingTraceContext * trace_context,
				int span_index, QueryDesc *queryDesc,
				const int64 *end_span_ns)
{
	pgTracingStoredSpan ongoing_span;
	pgTracingStoredSpan top_span;
	int			sql_error_code;

	/* If we're not sampling the query, bail out */
	if (!pg_tracing_enabled(trace_context, exec_nested_level))
		return;
	sql_error_code = geterrcode();

	/*
	 * Order matters there. We want to process query desc before getting the
	 * end time. Otherwise, the span nodes will have a higher duration than
	 * their parents.
	 */
	if (queryDesc != NULL)
		process_query_desc(trace_context, queryDesc, sql_error_code);

	/* Assign the error code to the latest span */
	ongoing_span = get_span_from_index(span_index);
	ongoing_span.span->sql_error_code = sql_error_code;

	/* Assign the error code to the latest top span */
	top_span = get_top_span_for_nested_level(trace_context, exec_nested_level);
	top_span.span->sql_error_code = sql_error_code;

	/* End all ongoing spans */
	for (int i = 0; i < current_trace_spans->end; i++)
	{
		ongoing_span = get_span_from_index(i);
		if (!ongoing_span.span->ended)
			end_span(ongoing_span.span, end_span_ns);
	}

	end_tracing();
}

/*
 * Get the parent id for the given nested level
 *
 * nested_level can be negative when creating the root top span.
 * In this case, we use the parent id from the propagated trace context.
 */
static uint64
get_parent_id(const pgTracingTraceContext * trace_context, int nested_level)
{
	pgTracingStoredSpan top_span;

	if (nested_level < 0)
		return trace_context->parent_id;
	Assert(nested_level <= allocated_nested_level);
	top_span = get_top_span_for_nested_level(trace_context, nested_level);
	Assert(top_span.span->span_id > 0);
	return top_span.span->span_id;
}

/*
 * If we enter a new nested level, initialize all necessary buffers
 * and start_trace timer.
 *
 * The start of a top span can vary: prepared statement will skip parsing, the use of cached
 * plans will skip the planner hook.
 * Thus, a top span can start in either post parse, planner hook or executor run.
 */
static bool
initialize_trace_level(pgTracingTraceContext * trace_context, bool new_root_span)
{
	/*
	 * Parallel workers will get the referential timers from the leader
	 * parallel context
	 */
	if (new_root_span && !IsParallelWorker())
	{
		Assert(trace_context->start_trace.ts == 0);
		/* Start referential timers */
		trace_context->start_trace.ts = GetCurrentStatementStartTimestamp();

		/*
		 * We can't get the monotonic clock at the start of the statement so
		 * let's estimate it here.
		 */
		trace_context->start_trace.ns = get_current_ns() - (GetCurrentTimestamp() - trace_context->start_trace.ts) * NS_PER_US;
	}

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
	if (new_root_span)
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

	Assert(str_len > 0);

	appendBinaryStringInfo(current_trace_text, str, str_len);
	appendStringInfoChar(current_trace_text, '\0');
	current_trace_text->cursor = current_trace_text->len;
	return position;
}

/*
 * Add the worker name to the current_trace_text stringinfo
 */
static int
add_worker_name_to_trace_buffer(void)
{
	int			position = current_trace_text->cursor;

	appendStringInfo(current_trace_text, "Worker %d", ParallelWorkerNumber);
	appendStringInfoChar(current_trace_text, '\0');
	current_trace_text->cursor = current_trace_text->len;
	return position;
}

/*
 * Start a new top span if we've entered a new nested level
 * or if the previous span at the same level ended
 */
static void
begin_top_span(const pgTracingTraceContext * trace_context,
			   Span * top_span, CmdType commandType,
			   const Query *query, const JumbleState *jstate,
			   const PlannedStmt *pstmt,
			   const char *query_text, int64 start_time_ns)
{
	int			query_len;
	const char *normalised_query;

	/* in case of a cached plan, query might be unavailable */
	if (query != NULL)
		per_level_buffers[exec_nested_level].query_id = query->queryId;
	else if (trace_context->query_id > 0)
		per_level_buffers[exec_nested_level].query_id = trace_context->query_id;

	begin_span(trace_context, top_span, command_type_to_span_type(commandType),
			   get_parent_id(trace_context, exec_nested_level - 1),
			   per_level_buffers[exec_nested_level].query_id, &start_time_ns);

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
		char	   *param_str;
		int			param_len;

		query_len = query->stmt_len;
		normalised_query = normalise_query_parameters(jstate, query_text,
													  query->stmt_location,
													  &query_len, &param_str, &param_len);
		Assert(param_len > 0);
		if (pg_tracing_export_parameters)
			top_span->parameter_offset = add_str_to_trace_buffer(param_str, param_len);
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
		else if (pstmt != NULL && pstmt->stmt_location != -1 && pstmt->stmt_len > 0)
		{
			query_len = pstmt->stmt_len;
			stmt_location = pstmt->stmt_location;
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
 * Initialise buffers if we are in a new nested level and start associated top span.
 * If the top span already exists for the current nested level, this has no effect.
 *
 * This needs to be called every time a top span could be started: post parse, planner, executor start and process utility
 */
static void
initialize_trace_level_and_top_span(pgTracingTraceContext * trace_context, CmdType commandType,
									Query *query, JumbleState *jstate, const PlannedStmt *pstmt,
									const char *query_text, int64 start_time_ns)
{
	bool		new_allocated_level;
	pgTracingStoredSpan top_span;
	bool		new_root_span = trace_context->root_span_index == -1;

	new_allocated_level = initialize_trace_level(trace_context, new_root_span);

	if (!new_allocated_level && !new_root_span)
	{
		/* top span must exists, get it */
		top_span = get_top_span_for_nested_level(trace_context, exec_nested_level);

		/*
		 * It's possible to have multiple top spans within a nested query. If
		 * the previous top span was closed, we need to start a new one
		 */
		if (!top_span.span->ended)
			return;
	}

	/* current_trace_spans buffer should have been allocated */
	Assert(current_trace_spans != NULL);

	/* We need to create a new top span */
	top_span = get_new_stored_span();
	begin_top_span(trace_context, top_span.span, commandType, query, jstate, pstmt, query_text, start_time_ns);
	set_top_span_for_nested_level(trace_context, exec_nested_level, top_span.index);

	return;
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
	int64		start_post_parse_ns;
	uint64		parent_id;
	pgTracingTraceContext *trace_context = &root_trace_context;
	bool		new_lxid = MyProc->lxid != latest_lxid;

	if (!pg_tracing_mem_ctx->isReset && new_lxid && exec_nested_level + plan_nested_level == 0)

		/*
		 * Some errors can happen outside of our PG_TRY (incorrect number of
		 * bind parameters for example) which will leave a dirty trace
		 * context. We can also have sampled individual Parse command through
		 * extended protocol. In this case, we just drop the spans and reset
		 * memory context
		 */
		cleanup_tracing();

	if (exec_nested_level + plan_nested_level == 0)

		/*
		 * At the root level, clean any leftover state of previous trace
		 * context
		 */
		reset_trace_context(&root_trace_context);
	else
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* If disabled, don't trace utility statement */
	if (query->utilityStmt && !pg_tracing_track_utility)
		return;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, pstate, NULL);

	if (!trace_context->sampled)
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
	initialize_trace_level_and_top_span(trace_context, query->commandType, query,
										jstate, NULL, pstate->p_sourcetext, start_post_parse_ns);

	parent_id = get_parent_id(trace_context, exec_nested_level);

	/*
	 * We only create parse span when we have a good idea of when parsing
	 * started. Since multiple queries can be parsed at once when entering a
	 * new nested level, we only create a parse span when we detect a change
	 * in parse nested level.
	 */
	if ((exec_nested_level + plan_nested_level == 0) || (nested_query_start_ns > 0 && previous_parse_nested_level != exec_nested_level))
	{
		int64		start_parse_span;
		int			parse_span_index;

		/*
		 * We don't have a precise time for parse end, estimate it
		 *
		 * TODO: Can we get a more precise start of parse?
		 */
		int64		end_parse_ns = start_post_parse_ns;

		if (nested_query_start_ns > 0 && (exec_nested_level + plan_nested_level) > 0)
			start_parse_span = nested_query_start_ns;
		else
			start_parse_span = trace_context->start_trace.ns;

		parse_span_index = create_full_span(trace_context, SPAN_PARSE,
											get_parent_id(trace_context, exec_nested_level - 1),
											start_parse_span, &end_parse_ns);
		per_level_buffers[exec_nested_level].parse_span_index = parse_span_index;
	}

	/* Post parse span */
	create_full_span(trace_context, SPAN_POST_PARSE,
					 parent_id, start_post_parse_ns, NULL);

	/* keep track of the previous parse level */
	previous_parse_nested_level = exec_nested_level;
}

/*
 * Planner hook
 * Tracing can start here if the query skipped parsing (prepared statement) and passes the random sample_rate.
 * If query is sampled, create a plan span and start the top span if not already started.
 */
static PlannedStmt *
pg_tracing_planner_hook(Query *query,
						const char *query_string,
						int cursorOptions,
						ParamListInfo params)
{
	PlannedStmt *result;
	pgTracingStoredSpan span_planner;
	pgTracingTraceContext *trace_context = &root_trace_context;
	int64		start_span_ns;

	if (exec_nested_level > 0)
		/* We're in a nested query, grab the ongoing trace_context */
		trace_context = &current_trace_context;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, params);

	if (!pg_tracing_enabled(trace_context, plan_nested_level + exec_nested_level))
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

	start_span_ns = get_current_ns();

	/*
	 * We may have skipped parsing if statement was prepared, create a new top
	 * span in this case.
	 */
	initialize_trace_level_and_top_span(trace_context, query->commandType, query,
										NULL, NULL, query_string, start_span_ns);

	/* Start planner span */
	span_planner = get_new_stored_span();
	begin_span(trace_context, span_planner.span, SPAN_PLANNER,
			   get_parent_id(trace_context, exec_nested_level),
			   per_level_buffers[exec_nested_level].query_id, &start_span_ns);

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
		handle_pg_error(trace_context, span_planner.index, NULL, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	plan_nested_level--;

	/* End planner span */
	end_span_index(span_planner.index, NULL);

	/* If we have a prepared statement, add bound parameters to the top span */
	if (params != NULL && pg_tracing_export_parameters)
	{
		char	   *paramStr = BuildParamLogString(params,
												   NULL, pg_tracing_max_parameter_str);
		pgTracingStoredSpan current_top = get_top_span_for_nested_level(trace_context, exec_nested_level);

		Assert(current_top.span->parameter_offset == -1);
		if (paramStr != NULL)
			current_top.span->parameter_offset = add_str_to_trace_buffer(paramStr, strlen(paramStr));
	}
	return result;
}

/*
 * ExecutorStart hook: Activate query instrumentation if query is sampled
 * Tracing can be started here if the query used a cached plan and passes the random sample_rate.
 * If query is sampled, start the top span if it doesn't already exist.
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			instrument_options;
	pgTracingTraceContext *trace_context = &current_trace_context;
	int64		start_span_ns = -1;
	bool		is_lazy_function;

	if (exec_nested_level + plan_nested_level == 0)

		/*
		 * We're at the root level, copy eventual trace context from
		 * parsing/planning
		 */
		*trace_context = root_trace_context;

	/* Evaluate if query is sampled or not */
	extract_trace_context(trace_context, NULL, queryDesc->params);

	/*
	 * We can detect the presence of lazy function through the node tag and
	 * the executor flags. Lazy function will go through an ExecutorRun with
	 * every call, possibly generating thousands of spans which is not
	 * manageable and not very useful. If lazy functions are detected, we
	 * don't instrument this node and no spans will be generated.
	 */
	is_lazy_function = nodeTag(queryDesc->plannedstmt->planTree) == T_FunctionScan && eflags == EXEC_FLAG_SKIP_TRIGGERS;
	if (pg_tracing_enabled(trace_context, exec_nested_level) && !is_lazy_function)
	{
		start_span_ns = get_current_ns();

		if (trace_context->query_id != 0)

			/*
			 * TODO: Can this fail? Should we drop tracing if we detect
			 * mismatch?
			 */
			Assert(trace_context->query_id == queryDesc->plannedstmt->queryId);

		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook. Parallel workers will also start tracing from
		 * there. Create the top case in this case.
		 */
		initialize_trace_level_and_top_span(trace_context, queryDesc->operation,
											NULL, NULL, NULL,
											queryDesc->sourceText, start_span_ns);
		Assert(trace_context->root_span_index != -1);

		/*
		 * Activate query instrumentation. Timer and rows instrumentation are
		 * mandatory.
		 */
		instrument_options = INSTRUMENT_TIMER | INSTRUMENT_ROWS;
		if (pg_tracing_instrument_buffers)
			instrument_options |= INSTRUMENT_BUFFERS;
		if (pg_tracing_instrument_wal)
			instrument_options |= INSTRUMENT_WAL;
		queryDesc->instrument_options = instrument_options;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (start_span_ns != -1)
	{
		/* Allocate totaltime instrumentation in the per-query context */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, instrument_options, false);
			MemoryContextSwitchTo(oldcxt);
		}

		/* Executor span */
		create_full_span(trace_context, SPAN_EXECUTOR_START,
						 get_parent_id(trace_context, exec_nested_level),
						 start_span_ns, NULL);
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
	const		pgTracingTraceContext *trace_context = &current_trace_context;
	bool		query_sampled = pg_tracing_enabled(trace_context, exec_nested_level);
	int64		start_span_ns;
	int64		end_span_ns;
	pgTracingStoredSpan executor_run_span;

	executor_run_span.index = -1;

	if (query_sampled)
	{
		start_span_ns = get_current_ns();
		nested_query_start_ns = start_span_ns;
	}

	if (query_sampled && queryDesc->instrument_options > 0)
	{
		Assert(trace_context->root_span_index != -1);
		/* Start executor run span */
		executor_run_span = get_new_stored_span();
		begin_span(trace_context, executor_run_span.span, SPAN_EXECUTOR_RUN,
				   get_parent_id(trace_context, exec_nested_level),
				   per_level_buffers[exec_nested_level].query_id,
				   &start_span_ns);

		/*
		 * Executor run is used as the parent's span for nodes, store its' id
		 * in the per level buffer.
		 */
		per_level_buffers[exec_nested_level].executor_run_id = executor_run_span.span->span_id;

		/*
		 * If this query starts parallel worker, push the trace context for
		 * the child processes
		 */
		if (queryDesc->plannedstmt->parallelModeNeeded && pg_tracing_trace_parallel_workers)
			add_parallel_context(trace_context, executor_run_span.span->span_id,
								 per_level_buffers[exec_nested_level].query_id);
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
		end_span_ns = get_current_ns();
		end_top_span(trace_context, &end_span_ns);
		exec_nested_level--;

		/*
		 * When a query is executing the pg_tracing_{peek,consume}_spans
		 * function, sampling will be disabled while we have an ongoing trace
		 * and current_trace_spans will be NULL. Checking that span_index !=
		 * -1 isn't enough in this case, we need to check that
		 * current_trace_spans still exists.
		 */
		if (current_trace_spans != NULL && executor_run_span.index != -1)
			handle_pg_error(trace_context, executor_run_span.index, queryDesc, &end_span_ns);
		PG_RE_THROW();
	}
	PG_END_TRY();
	end_span_ns = get_current_ns();
	end_top_span(trace_context, &end_span_ns);
	exec_nested_level--;

	/*
	 * Same as above, tracing could have been aborted, check for
	 * current_trace_spans
	 */
	if (current_trace_spans != NULL && executor_run_span.index != -1)
		end_span_index(executor_run_span.index, &end_span_ns);
}

/*
 * ExecutorFinish hook: create executor finish span and track nesting depth
 * ExecutorFinish can start nested queries through triggers so we need
 * to set the ExecutorFinish span as the top span
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	int			previous_top_span_index = -1;
	pgTracingStoredSpan executor_finish_span;
	pgTracingTraceContext *trace_context = &current_trace_context;

	executor_finish_span.index = -1;

	if (pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->instrument_options > 0)
	{
		int64		start_span_ns = get_current_ns();

		Assert(trace_context->root_span_index != -1);

		executor_finish_span = get_new_stored_span();
		previous_top_span_index = get_top_span_for_nested_level(trace_context, exec_nested_level).index;
		begin_span(trace_context, executor_finish_span.span, SPAN_EXECUTOR_FINISH,
				   get_parent_id(trace_context, exec_nested_level),
				   per_level_buffers[exec_nested_level].query_id, &start_span_ns);

		/*
		 * ExecutorFinish may create nested queries when executing triggers.
		 * We set ExecutorFinish span as the top queries for this.
		 */
		set_top_span_for_nested_level(trace_context, exec_nested_level, executor_finish_span.index);
		/* Set a possible start for nested query */
		nested_query_start_ns = start_span_ns;
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
		if (executor_finish_span.index != -1)
			handle_pg_error(trace_context, executor_finish_span.index, queryDesc, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nested_level--;

	if (executor_finish_span.index != -1)
	{
		/* Restore previous top span */
		set_top_span_for_nested_level(trace_context, exec_nested_level, previous_top_span_index);
		end_span_index(executor_finish_span.index, NULL);
	}
}

/*
 * ExecutorEnd hook will:
 * - process queryDesc to generate span from planstate
 * - end top span for the current nested level
 * - end tracing if we're at the root nested level
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	int64		start_span_ns = -1;
	pgTracingTraceContext *trace_context = &current_trace_context;

	if (pg_tracing_enabled(trace_context, exec_nested_level) && queryDesc->instrument_options > 0)
	{
		start_span_ns = get_current_ns();
		Assert(trace_context->root_span_index != -1);
		process_query_desc(trace_context, queryDesc, 0);
	}

	exec_nested_level++;
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
	exec_nested_level--;

	if (start_span_ns != -1)
	{
		int64		end_span_ns = get_current_ns();
		bool		overlapping_trace_context;

		/*
		 * We may go through multiple statement nested in a single
		 * ExecutorFinish (multiple triggers) so we need to update the
		 * nested_start.
		 */
		nested_query_start_ns = end_span_ns;

		/* Create executor_end span and end top span */
		end_top_span(trace_context, &end_span_ns);
		create_full_span(trace_context, SPAN_EXECUTOR_END,
						 get_parent_id(trace_context, exec_nested_level),
						 start_span_ns, &end_span_ns);

		/*
		 * if root trace context has a different root span index, it means
		 * that we may have started to trace the next statement while still
		 * processing the current one. This may happen with explicit
		 * transaction using extended protocol, the parsing of the next
		 * statement happens before the ExecutorEnd of the current statement.
		 * In this case, we can't end tracing as we still have unfinished
		 * spans.
		 */
		overlapping_trace_context = current_trace_context.root_span_index != root_trace_context.root_span_index && root_trace_context.sampled;
		if (!overlapping_trace_context)
			end_tracing();
	}
}

/*
 * ProcessUtility hook
 *
 * Trace utility query if utility tracking is enabled and sampling was enabled
 * during parse step.
 * Process utility may create nested queries (for example function CALL) so we need
 * to set the ProcessUtility span as the top span before going through the standard
 * codepath.
 */
static void
pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	pgTracingStoredSpan process_utility_span;
	pgTracingStoredSpan previous_top_span;
	int64		end_span_ns;
	int64		start_span_ns;
	pgTracingTraceContext *trace_context = &current_trace_context;

	if (exec_nested_level + plan_nested_level == 0)
		/* We're at root level, use the root trace_context */
		*trace_context = root_trace_context;

	if (!pg_tracing_track_utility || !pg_tracing_enabled(trace_context, exec_nested_level))
	{
		/* No sampling, just go through the standard process utility */
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
			Assert(current_trace_context.sampled || root_trace_context.sampled);
			Assert(exec_nested_level == 0);
			end_tracing();
		}
		return;
	}

	/* Statement is sampled */
	start_span_ns = get_current_ns();

	/*
	 * Set a possible start for nested query. ProcessUtility may start a
	 * nested query and we can't use the statement start like we usually do.
	 * We artificially start nested 1us later to avoid overlapping spans.
	 */
	nested_query_start_ns = start_span_ns;

	initialize_trace_level_and_top_span(trace_context, pstmt->commandType, NULL, NULL,
										pstmt, queryString, start_span_ns);

	/* Build the process utility span. */
	process_utility_span = get_new_stored_span();
	begin_span(trace_context, process_utility_span.span, SPAN_PROCESS_UTILITY,
			   get_parent_id(trace_context, exec_nested_level),
			   per_level_buffers[exec_nested_level].query_id,
			   &start_span_ns);

	/*
	 * Set ProcessUtility span as the new top span for the nested queries that
	 * may be created. We need to keep the old top span index to end it at the
	 * end of the process utility hook.
	 */
	previous_top_span = get_top_span_for_nested_level(trace_context, exec_nested_level);
	set_top_span_for_nested_level(trace_context, exec_nested_level, process_utility_span.index);

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
		end_span_ns = get_current_ns();
		exec_nested_level--;
		end_top_span(trace_context, &end_span_ns);
		handle_pg_error(trace_context, previous_top_span.index, NULL, &end_span_ns);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* ProcessUtility may have created a top span, end it here */
	end_span_ns = get_current_ns();
	end_top_span(trace_context, &end_span_ns);
	exec_nested_level--;

	/*
	 * current_trace_spans may have been repalloced, refresh the
	 * process_utility_span span
	 */
	process_utility_span = get_span_from_index(process_utility_span.index);
	if (qc != NULL)
		process_utility_span.span->node_counters.rows = qc->nprocessed;
	/* ProcessUtility is the current top span, no need to call end_top_span */
	end_span(process_utility_span.span, &end_span_ns);

	/* Also end the previous top span */
	previous_top_span = get_span_from_index(previous_top_span.index);
	end_span(previous_top_span.span, &end_span_ns);

	Assert(previous_top_span.span->start <= process_utility_span.span->start);
	Assert(get_span_end_ns(previous_top_span.span) >= get_span_end_ns(process_utility_span.span));

	end_tracing();

	/*
	 * We may iterate through a list of nested ProcessUtility calls, update
	 * the nested query start.
	 */
	nested_query_start_ns = end_span_ns;
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
attach_top_span_to_new_parent(const pgTracingTraceContext * trace_context, Span const *new_parent, int64 parent_end_ns_time)
{
	pgTracingStoredSpan child_top_span;
	int64		child_end_ns_time;

	if (exec_nested_level + 1 > max_nested_level)
		/* No child to attach */
		return;

	child_top_span = get_top_span_for_nested_level(trace_context, exec_nested_level + 1);
	Assert(child_top_span.span->start_ns_time > 0);
	/* If child start after parent's end, don't link it */
	if (parent_end_ns_time < child_top_span.span->start_ns_time)
		return;
	child_end_ns_time = child_top_span.span->start_ns_time + child_top_span.span->duration_ns;
	/* If child ends before parent's start, don't link it */
	if (child_end_ns_time < new_parent->start_ns_time)
		return;

	/* Link this child to the new parent */
	child_top_span.span->parent_id = new_parent->span_id;
}

/*
 * Attach an already existing parse span to a new parent.
 *
 * Parse span are created in the post_parse hook but we don't know
 * its parent nor its exact start, we only have a good idea of its end.
 *
 * When processing the queryDesc, we can find possible parent candidate and
 * attach the parse span. We need to adjust the parse span start to match
 * the new parent.
 *
 * The parent node may have another child node (ProjectSet will have a Result
 * outerplan node. The parse span will need to start after this child node.
 */
static void
attach_parse_span_to_new_parent(const pgTracingTraceContext * trace_context, const Span * new_parent,
								int64 new_start_ns, int64 parent_end_ns_time)
{
	int			child_parse_index;
	pgTracingStoredSpan child_parse_span;
	int64		end_ns_time;

	Assert(new_start_ns < parent_end_ns_time);
	if (exec_nested_level + 1 > max_nested_level)
		/* No child to attach */
		return;

	/* Get child parse span */
	child_parse_index = per_level_buffers[exec_nested_level + 1].parse_span_index;
	if (child_parse_index == -1)
		return;
	child_parse_span = get_span_from_index(child_parse_index);

	/* If child start after parent's end, don't link it */
	if (child_parse_span.span->start_ns_time > parent_end_ns_time)
		return;

	end_ns_time = child_parse_span.span->start_ns_time + child_parse_span.span->duration_ns;
	/* If child ends before parent's start, don't link it */
	if (end_ns_time < new_parent->start_ns_time)
		return;

	/* Check if child ends after parent's end */
	if (end_ns_time > parent_end_ns_time)
		return;

	/* Check if child ends before new start */
	if (end_ns_time < new_start_ns)
		return;

	/* Adjust span start */
	set_span_start(trace_context, child_parse_span.span, new_start_ns);

	/* Adjust span duration */
	child_parse_span.span->duration_ns = end_ns_time - child_parse_span.span->start_ns_time;
	Assert(child_parse_span.span->duration_ns > 0);

	/* Child span should already have ended */
	Assert(child_parse_span.span->ended);
	/* Link this child to the new parent */
	child_parse_span.span->parent_id = new_parent->span_id;
}


/*
 * Create span node for the provided planstate
 */
static Span *
create_span_node(PlanState *planstate, planstateTraceContext * planstateTraceContext,
				 uint64 parent_id, SpanType span_type, char *subplan_name)
{
	pgTracingStoredSpan stored_span;
	Span	   *span;
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
	stored_span = get_new_stored_span();
	span = stored_span.span;

	begin_span(planstateTraceContext->trace_context, span, span_type,
			   parent_id, per_level_buffers[exec_nested_level].query_id,
			   &start_span_ns);

	/* first tuple time */
	span->startup = planstate->instrument->startup * NS_PER_S;

	if (span_type == SPAN_NODE)
	{
		/* Generate node specific variable strings and store them */
		const char *node_type;
		const char *deparse_info;
		char const *operation_name;
		int			deparse_info_len;

		node_type = plan_to_node_type(plan);
		span->node_type_offset = add_str_to_trace_buffer(node_type, strlen(node_type));

		operation_name = plan_to_operation(planstateTraceContext, planstate, node_type);
		span->operation_name_offset = add_str_to_trace_buffer(operation_name, strlen(operation_name));

		/* deparse_ctx us NULL if deparsing was disabled */
		if (planstateTraceContext->deparse_ctx != NULL)
		{
			deparse_info = plan_to_deparse_info(planstateTraceContext, planstate);
			deparse_info_len = strlen(deparse_info);
			if (deparse_info_len > 0)
				span->deparse_info_offset = add_str_to_trace_buffer(deparse_info, deparse_info_len);
		}
	}
	else if (subplan_name != NULL)
		span->operation_name_offset = add_str_to_trace_buffer(subplan_name, strlen(subplan_name));

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
		if (!INSTR_TIME_IS_ZERO(planstate->instrument->starttime))
			/* Don't stop the node if it wasn't started */
			InstrStopNode(planstate->instrument, planstate->state->es_processed);
		InstrEndLoop(planstate->instrument);
		span->sql_error_code = planstateTraceContext->sql_error_code;
	}
	span_end_ns = INSTR_TIME_GET_NANOSEC(planstate->instrument->firsttime);
	Assert(planstate->instrument->total > 0);
	span_end_ns += planstate->instrument->total * NS_PER_S;
	end_span(span, &span_end_ns);

	return span;
}

/*
 * Walk through the planstate tree generating a node span for each node.
 * We pass possible error code to tag unfinished node with it
 */
static Span *
generate_span_from_planstate(PlanState *planstate, planstateTraceContext * planstateTraceContext, uint64 parent_id)
{
	ListCell   *l;
	Plan const *plan = planstate->plan;
	Span	   *outer_span = NULL;
	Span	   *span;

	/* The node was never executed, skip it */
	if (planstate->instrument == NULL || INSTR_TIME_IS_ZERO(planstate->instrument->firsttime))
		return NULL;

	span = create_span_node(planstate, planstateTraceContext, parent_id, SPAN_NODE, NULL);
	planstateTraceContext->ancestors = lcons(planstate->plan, planstateTraceContext->ancestors);

	/* Walk the outerplan */
	if (outerPlanState(planstate))
		outer_span = generate_span_from_planstate(outerPlanState(planstate), planstateTraceContext, span->span_id);
	/* Walk the innerplan */
	if (innerPlanState(planstate))
		generate_span_from_planstate(innerPlanState(planstate), planstateTraceContext, span->span_id);

	/* Handle init plans */
	foreach(l, planstate->initPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		Span	   *init_span;

		if (INSTR_TIME_IS_ZERO(splan->instrument->firsttime))
			continue;
		init_span = create_span_node(splan, planstateTraceContext, parent_id, SPAN_NODE_INIT_PLAN, sstate->subplan->plan_name);
		generate_span_from_planstate(splan, planstateTraceContext, init_span->span_id);
	}

	/* Handle sub plans */
	foreach(l, planstate->subPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;
		Span	   *subplan_span;

		if (INSTR_TIME_IS_ZERO(splan->instrument->firsttime))
			continue;
		subplan_span = create_span_node(splan, planstateTraceContext, parent_id, SPAN_NODE_SUBPLAN, sstate->subplan->plan_name);
		generate_span_from_planstate(splan, planstateTraceContext, subplan_span->span_id);
	}

	/* Handle special nodes with children nodes */
	switch (nodeTag(plan))
	{
		case T_Append:
			generate_member_nodes(((AppendState *) planstate)->appendplans,
								  ((AppendState *) planstate)->as_nplans, planstateTraceContext, span->span_id);
			break;
		case T_MergeAppend:
			generate_member_nodes(((MergeAppendState *) planstate)->mergeplans,
								  ((MergeAppendState *) planstate)->ms_nplans, planstateTraceContext, span->span_id);
			break;
		case T_BitmapAnd:
			generate_member_nodes(((BitmapAndState *) planstate)->bitmapplans,
								  ((BitmapAndState *) planstate)->nplans, planstateTraceContext, span->span_id);
			break;
		case T_BitmapOr:
			generate_member_nodes(((BitmapOrState *) planstate)->bitmapplans,
								  ((BitmapOrState *) planstate)->nplans, planstateTraceContext, span->span_id);
			break;
		case T_SubqueryScan:
			generate_span_from_planstate(((SubqueryScanState *) planstate)->subplan, planstateTraceContext, span->span_id);
			break;
		case T_CustomScan:
			generate_span_from_custom_scan((CustomScanState *) planstate, planstateTraceContext, span->span_id);
			break;
		default:
			break;
	}
	planstateTraceContext->ancestors = list_delete_first(planstateTraceContext->ancestors);

	/*
	 * If we have a Result or ProjectSet node, make it the span parent of the
	 * next query span if we have any
	 *
	 * TODO: Do we have other node that could be the base for nested queries?
	 */
	if (nodeTag(plan) == T_Result || nodeTag(plan) == T_ProjectSet)
	{
		int64		span_end_ns = span->start_ns_time + span->duration_ns;

		attach_top_span_to_new_parent(planstateTraceContext->trace_context, span, span_end_ns);
		if (nodeTag(plan) == T_ProjectSet)
		{
			/*
			 * A parse may happen within a projectet during function exec. Try
			 * to attach existing child parse to the project set node.
			 */
			int64		outer_span_end_ns;

			/* We should have an outer span with a ProjectSet */
			Assert(outer_span != NULL);
			outer_span_end_ns = outer_span->start_ns_time + outer_span->duration_ns;
			attach_parse_span_to_new_parent(planstateTraceContext->trace_context, span, outer_span_end_ns, span_end_ns);
		}
		else
			attach_parse_span_to_new_parent(planstateTraceContext->trace_context, span, span->start_ns_time, span_end_ns);
	}

	return span;
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

	blk_read_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_read_time);
	blk_write_time = INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.shared_blk_write_time);
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
#define PG_TRACING_TRACES_COLS	45
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};
	const char *span_type;
	const char *operation_name;
	const char *sql_error_code;
	int			i = 0;

	span_type = get_span_name(span, qbuffer, qbuffer_size);
	operation_name = get_operation_name(span, qbuffer, qbuffer_size);
	sql_error_code = unpack_sql_state(span->sql_error_code);

	Assert(span_type != NULL);
	Assert(operation_name != NULL);
	Assert(sql_error_code != NULL);

	values[i++] = UInt64GetDatum(span->trace_id);
	values[i++] = UInt64GetDatum(span->parent_id);
	values[i++] = UInt64GetDatum(span->span_id);
	values[i++] = UInt64GetDatum(span->query_id);
	values[i++] = CStringGetTextDatum(span_type);
	values[i++] = CStringGetTextDatum(operation_name);
	values[i++] = Int64GetDatumFast(span->start);
	values[i++] = Int16GetDatum(span->start_ns);

	values[i++] = Int64GetDatumFast(span->duration_ns);
	values[i++] = CStringGetTextDatum(sql_error_code);
	values[i++] = UInt32GetDatum(span->be_pid);
	values[i++] = ObjectIdGetDatum(span->user_id);
	values[i++] = ObjectIdGetDatum(span->database_id);
	values[i++] = UInt8GetDatum(span->subxact_count);

	if ((span->type >= SPAN_NODE && span->type <= SPAN_NODE_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		i = add_plan_counters(&span->plan_counters, i, values);
		i = add_node_counters(&span->node_counters, i, values);

		values[i++] = Int64GetDatumFast(span->startup);
		if (span->parameter_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->parameter_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->parameter_offset);
		else
			nulls[i++] = 1;
		if (span->deparse_info_offset != -1 && qbuffer_size > 0 && qbuffer_size > span->deparse_info_offset)
			values[i++] = CStringGetTextDatum(qbuffer + span->deparse_info_offset);
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
	 * any possible parallel context pushed. Since this will be called within
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
#define PG_TRACING_INFO_COLS	6
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
