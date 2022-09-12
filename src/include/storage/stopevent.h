#ifndef SRC_STOPEVENT_H
#define SRC_STOPEVENT_H

#include "utils/jsonb.h"
#include "storage/stopeventnames.h"

extern bool enable_stopevents;
extern bool trace_stopevents;
extern const char *const stopeventnames[];
extern MemoryContext stopevents_cxt;

#ifdef USE_STOP_EVENTS
#define STOPEVENT(event_id, params) \
	do { \
		if (enable_stopevents || trace_stopevents) \
			handle_stopevent((event_id), (params)); \
	} while(0)
#else
#define STOPEVENT(event_id, params)
#endif

extern Size StopEventShmemSize(void);
extern void StopEventShmemInit(void);
extern Datum pg_stopevent_set(PG_FUNCTION_ARGS);
extern Datum pg_stopevent_reset(PG_FUNCTION_ARGS);
extern Datum pg_stopevents(PG_FUNCTION_ARGS);
extern bool pid_is_waiting_for_stopevent(int pid);
extern void handle_stopevent(int event_id, Jsonb *params);
extern void stopevents_make_cxt(void);
extern void jsonb_push_key(JsonbParseState **state, char *key);
extern void jsonb_push_int8_key(JsonbParseState **state, char *key, int64 value);
extern void jsonb_push_string_key(JsonbParseState **state, const char *key,
								  const char *value);
extern void relation_stopevent_params(JsonbParseState **state,
									  Relation relation);

#endif							/* SRC_STOPEVENT_H */
