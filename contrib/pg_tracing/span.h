/*-------------------------------------------------------------------------
 * contrib/pg_tracing/span.h
 *
 * 	Header for span.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/span.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SPAN_H_
#define _SPAN_H_

#include "postgres.h"

#include "jit/jit.h"
#include "pgstat.h"
#include "access/transam.h"

/*
 * SpanType: Type of the span
 */
typedef enum
{
	SPAN_PARSE,					/* Wraps query parsing */
	SPAN_POST_PARSE,			/* Wraps post parsing */
	SPAN_PLANNER,				/* Wraps planner execution in planner hook */
	SPAN_FUNCTION,				/* Wraps function in fmgr hook */
	SPAN_PROCESS_UTILITY,		/* Wraps ProcessUtility execution */

	SPAN_EXECUTOR_START,		/* Executor Spans wrapping the matching hooks */
	SPAN_EXECUTOR_RUN,
	SPAN_EXECUTOR_END,
	SPAN_EXECUTOR_FINISH,

	SPAN_NODE_INIT_PLAN,
	SPAN_NODE_SUBPLAN,

	SPAN_NODE,					/* Represents a node execution, generated from
								 * planstate */

	SPAN_NODE_SELECT,			/* Query Span types. They are created from the
								 * query cmdType */
	SPAN_NODE_INSERT,
	SPAN_NODE_UPDATE,
	SPAN_NODE_DELETE,
	SPAN_NODE_MERGE,
	SPAN_NODE_UTILITY,
	SPAN_NODE_NOTHING,
	SPAN_NODE_UNKNOWN,
}			SpanType;


/*
 * Counters extracted from query instrumentation
 */
typedef struct NodeCounters
{
	int64		rows;			/* # of tuples processed */
	int64		nloops;			/* # of cycles for this node */

	BufferUsage buffer_usage;	/* total buffer usage for this node */
	WalUsage	wal_usage;		/* total WAL usage for this node */
	JitInstrumentation jit_usage;	/* total JIT usage for this node */
}			NodeCounters;

/*
 * Counters extracted from query's plan
 */
typedef struct PlanCounters
{
	/*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/*
	 * planner's estimate of result size of this plan step
	 */
	Cardinality plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */
}			PlanCounters;

/*
 * Start time of a trace. We store both wall clock and monotonic clock.
 */
typedef struct
{
	TimestampTz ts;				/* Wall clock at the start of the trace */
	int64		ns;				/* Monotonic clock at the start of the trace */
}			pgTracingStartTime;

typedef struct pgTracingTrace
{
	uint64		trace_id;		/* Id of the trace */
	uint64		parent_id;		/* Span id of the parent */
	int			sampled;		/* Is current statement sampled? */
	uint64		query_id;		/* Query id of the current statement */
	pgTracingStartTime start_trace;
}			pgTracingTrace;

/*
 * The Span data structure represents an operation with a start, a duration
 * and metadatas.
 */
typedef struct Span
{
	uint64		trace_id;		/* Trace id extracted from the SQLCommenter's
								 * traceparent */
	uint64		span_id;		/* Span Identifier generated from a random
								 * uint64 */
	uint64		parent_id;		/* Span's parent id. For the top span, it's
								 * extracted from SQLCommenter's traceparent.
								 * For other spans, we pass the parent's span. */
	uint64		query_id;		/* QueryId of the trace query if available */

	TimestampTz start;			/* Start of the span. */
	uint16		start_ns;		/* Leftover nanoseconds of span start */
	int64		start_ns_time;	/* Nanoseconds at start of the span. */

	int64		duration_ns;	/* Duration of the span in nanoseconds. */
	SpanType	type;			/* Type of the span. Used to generate the
								 * span's name for all spans except SPAN_NODE. */
	int			be_pid;			/* Pid of the backend process */
	bool		is_top_span;
	bool		ended;			/* Track if the span was already ended */
	uint8		nested_level;	/* Only used for debugging */
	uint8		subxact_count;	/* Active count of backend's subtransaction */

	/*
	 * We store variable size metadata in an external file. Those represent
	 * the position of NULL terminated strings in the file. Set to -1 if
	 * unused.
	 */
	Size		name_offset;	/* span name offset in external file */
	Size		operation_name_offset;	/* operation name offset in external
										 * file */
	Size		parameter_offset;	/* parameters offset in external file */

	PlanCounters plan_counters; /* Counters with plan costs */
	NodeCounters node_counters; /* Counters with node costs (jit, wal,
								 * buffers) */
	int64		startup;		/* Time to the first tuple */
	int			sql_error_code; /* query error code extracted from ErrorData,
								 * 0 if query was successful */
}			Span;

extern void begin_span(Span * span, SpanType type, const pgTracingTrace * trace, uint64 parent_id, uint64 query_id, const int64 *start_span, int nested_level);
extern void end_span(Span * span, const int64 *end_time);

extern SpanType command_type_to_span_type(CmdType cmd_type);
extern const char *get_span_name(const Span * span, const char *qbuffer, Size qbuffer_size);
extern const char *get_operation_name(const Span * span, const char *qbuffer, Size qbuffer_size);

extern int64 get_current_ns(void);
extern int64 get_span_end_ns(const Span * span);

#endif
