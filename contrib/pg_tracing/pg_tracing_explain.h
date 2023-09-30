#ifndef _PG_TRACING_EXPLAIN_H_
#define _PG_TRACING_EXPLAIN_H_

#include "span.h"
#include "executor/execdesc.h"

/* Context needed when generating spans from planstate */
typedef struct planstateTraceContext
{
	pgTracingTrace *trace;
	int			sql_error_code;
	List	   *ancestors;
	List	   *deparse_ctx;
	List	   *rtable_names;
}			planstateTraceContext;

extern const char *plan_to_span_name(const Plan *plan);
extern const char *plan_to_operation(const planstateTraceContext * planstateTraceContext, const PlanState *planstate, const char *spanName);

#endif
