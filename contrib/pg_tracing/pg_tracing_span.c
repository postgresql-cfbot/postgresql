#include "pg_tracing_span.h"

#include "nodes/extensible.h"
#include "common/pg_prng.h"


/*
 * Convert a node CmdType to the matching SpanType
 */
SpanType
command_type_to_span_type(CmdType cmd_type)
{
	switch (cmd_type)
	{
		case CMD_SELECT:
			return SPAN_NODE_SELECT;
		case CMD_INSERT:
			return SPAN_NODE_INSERT;
		case CMD_UPDATE:
			return SPAN_NODE_UPDATE;
		case CMD_DELETE:
			return SPAN_NODE_DELETE;
		case CMD_MERGE:
			return SPAN_NODE_MERGE;
		case CMD_UTILITY:
			return SPAN_NODE_UTILITY;
		case CMD_NOTHING:
			return SPAN_NODE_NOTHING;
		case CMD_UNKNOWN:
			return SPAN_NODE_UNKNOWN;
	}
	return SPAN_NODE_UNKNOWN;
}

/*
 * Initialize span fields
 */
static void
initialize_span_fields(Span * span, SpanType type, const pgTracingTraceContext * trace, uint64 parent_id, uint64 query_id)
{
	span->trace_id = trace->trace_id;
	span->type = type;

	/*
	 * If parent id is unset, it means that there's no propagated trace
	 * informations from the caller. In this case, this is the top span,
	 * span_id == parent_id and we reuse the generated trace id for the span
	 * id.
	 */
	if (parent_id == 0)
	{
		span->parent_id = trace->trace_id;
	}
	else
	{
		span->parent_id = parent_id;
	}
	span->span_id = pg_prng_uint64(&pg_global_prng_state);

	span->node_type_offset = -1;
	span->operation_name_offset = -1;
	span->deparse_info_offset = -1;
	span->parameter_offset = -1;
	span->sql_error_code = 0;
	span->startup = 0;
	span->be_pid = MyProcPid;
	span->database_id = MyDatabaseId;
	span->user_id = GetUserId();
	span->subxact_count = MyProc->subxidStatus.count;
	span->start_ns_time = 0;
	span->duration_ns = 0;
	span->ended = false;
	span->query_id = query_id;
	memset(&span->node_counters, 0, sizeof(NodeCounters));
	memset(&span->plan_counters, 0, sizeof(PlanCounters));

	/*
	 * Store the starting buffer for planner and process utility spans
	 *
	 * TODO: Do we need wal buffer for planner?
	 */
	if (type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		/* TODO Do we need to track wal for planner? */
		span->node_counters.buffer_usage = pgBufferUsage;
		span->node_counters.wal_usage = pgWalUsage;
	}
}

/*
 * Set the start fields of a span
 */
void
set_span_start(const pgTracingTraceContext * trace_context, Span * span, int64 start_ns_time)
{
	int64		ns_since_trace_start;

	span->start_ns_time = start_ns_time;

	/* Get the ns between trace start and span start */
	ns_since_trace_start = start_ns_time - trace_context->start_trace.ns;
	Assert(ns_since_trace_start >= 0);

	/* Fill span starts */
	span->start = trace_context->start_trace.ts + ns_since_trace_start / NS_PER_US;
	span->start_ns = ns_since_trace_start % NS_PER_US;
}

/*
 * Initialize span and set span starting time.
 *
 * A span needs a start timestamp and a duration.
 * Given that some spans can have a very short duration (less than 1us), we
 * need to rely on monotonic clock as much as possible to have the best precision.
 *
 * For that, after we've established that a query was sampled, we get both the wall clock
 * and the monotonic clock at the start of a trace. They are both stored in pgTracingTraceContext.
 * All subsequent times will use the monotonic clock.
 * When we need to get the start of a span, we use the starting wall clock as a starting point
 * and add the time between the two monotonic clocks.
 *
 * start_span is an optional nanosecond time to set for. If nothing is provided, we use the current monotonic clock.
 * This parameter is mostly used when generating spans from planstate as we need to rely on the query instrumentation to find the node start.
 */
void
begin_span(const pgTracingTraceContext * trace_context, Span * span, SpanType type, uint64 parent_id, uint64 query_id, const int64 *start_span)
{
	int64		start_ns_time;

	initialize_span_fields(span, type, trace_context, parent_id, query_id);

	/* If no start span is provided, get the current one */
	if (start_span == NULL)
		start_ns_time = get_current_ns();
	else
		start_ns_time = *start_span;

	set_span_start(trace_context, span, start_ns_time);
}

/*
 * Set span duration and accumulated buffers
 * end_span_input is optional, if NULL is passed, we use
 * the current time
 */
void
end_span(Span * span, const int64 *end_span_input)
{
	BufferUsage buffer_usage;
	WalUsage	wal_usage;
	int64		end_span_ns;

	Assert(span->start_ns_time > 0);
	Assert(span->trace_id > 0);

	/* Span should be ended only once */
	Assert(!span->ended);
	span->ended = true;

	/* Set span duration with the end time before substrating the start */
	if (end_span_input == NULL)
		end_span_ns = get_current_ns();
	else
		end_span_ns = *end_span_input;
	span->duration_ns = end_span_ns - span->start_ns_time;

	if (span->type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		/* calc differences of buffer counters. */
		memset(&buffer_usage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&buffer_usage, &pgBufferUsage,
							 &span->node_counters.buffer_usage);
		span->node_counters.buffer_usage = buffer_usage;

		/* calc differences of WAL counters. */
		memset(&wal_usage, 0, sizeof(wal_usage));
		WalUsageAccumDiff(&wal_usage, &pgWalUsage,
						  &span->node_counters.wal_usage);
		span->node_counters.wal_usage = wal_usage;
	}
}

/*
 * Get the name of a span.
 * If it is a node span, the name may be pulled from the stat file.
 */
const char *
get_span_name(const Span * span, const char *qbuffer, Size qbuffer_size)
{
	if (span->node_type_offset != -1 && qbuffer_size > 0 && span->node_type_offset <= qbuffer_size)
		return qbuffer + span->node_type_offset;

	switch (span->type)
	{
		case SPAN_POST_PARSE:
			return "Post Parse";
		case SPAN_PARSE:
			return "Parse";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_START:
			return "Executor";
		case SPAN_EXECUTOR_RUN:
			return "Executor";
		case SPAN_EXECUTOR_END:
			return "Executor";
		case SPAN_EXECUTOR_FINISH:
			return "Executor";

		case SPAN_NODE_INIT_PLAN:
			return "InitPlan";
		case SPAN_NODE_SUBPLAN:
			return "SubPlan";

		case SPAN_NODE_SELECT:
			return "Select";
		case SPAN_NODE_INSERT:
			return "Insert";
		case SPAN_NODE_UPDATE:
			return "Update";
		case SPAN_NODE_DELETE:
			return "Delete";
		case SPAN_NODE_MERGE:
			return "Merge";
		case SPAN_NODE_UTILITY:
			return "Utility";
		case SPAN_NODE_NOTHING:
			return "Nothing";
		case SPAN_NODE_UNKNOWN:
			return "Unknown";
		case SPAN_NODE:
			return "Node";
	}
	return "???";
}

/*
 * Get the operation of a span.
 * For node span, the name may be pulled from the stat file.
 */
const char *
get_operation_name(const Span * span, const char *qbuffer, Size qbuffer_size)
{
	if (span->operation_name_offset != -1 && qbuffer_size > 0
		&& span->operation_name_offset <= qbuffer_size)
		return qbuffer + span->operation_name_offset;

	switch (span->type)
	{
		case SPAN_POST_PARSE:
			return "Post Parse";
		case SPAN_PARSE:
			return "Parse";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_START:
			return "ExecutorStart";
		case SPAN_EXECUTOR_RUN:
			return "ExecutorRun";
		case SPAN_EXECUTOR_END:
			return "ExecutorEnd";
		case SPAN_EXECUTOR_FINISH:
			return "ExecutorFinish";
		case SPAN_NODE_SELECT:
		case SPAN_NODE_INSERT:
		case SPAN_NODE_UPDATE:
		case SPAN_NODE_DELETE:
		case SPAN_NODE_MERGE:
		case SPAN_NODE_UTILITY:
		case SPAN_NODE_NOTHING:
		case SPAN_NODE_INIT_PLAN:
		case SPAN_NODE_SUBPLAN:
		case SPAN_NODE_UNKNOWN:
		case SPAN_NODE:
			return "Node";
	}
	return "Unknown type";
}

/*
* Get the current nanoseconds value
*/
int64
get_current_ns(void)
{
	instr_time	t;

	INSTR_TIME_SET_CURRENT(t);
	return INSTR_TIME_GET_NANOSEC(t);
}

/*
* Get the end of a span in nanoseconds
*/
int64
get_span_end_ns(const Span * span)
{
	return span->start_ns_time + span->duration_ns;
}
