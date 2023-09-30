#ifndef _QUERY_PROCESS_H_
#define _QUERY_PROCESS_H_

#include "pg_tracing.h"
#include "nodes/queryjumble.h"
#include "parser/parse_node.h"

/*
 * Normalise query: - Comments are removed - Constants are replaced by $x -
 * All tokens are separated by a single space
 */
extern const char *normalise_query_parameters(const JumbleState *jstate, const char *query,
											  int query_loc, int *query_len_p, char **paramStr);

extern void extract_traceparent(pgTracingTrace * pgTracingTrace, const char *query_str, bool is_parameter);

/*
 * Normalise simple query
 */
extern const char *normalise_query(const char *query, int query_loc, int *query_len_p);
bool		text_store_file(pgTracingSharedState * pg_tracing, const char *query,
							int query_len, Size *query_offset);
extern const char *qtext_load_file(Size *buffer_size);
extern const char *qtext_load_file(Size *buffer_size);
extern void initialize_traceparent(pgTracingTrace * traceparent_parameter);

/* Location of external text file */
#define PG_TRACING_TEXT_FILE	PG_STAT_TMP_DIR "/pg_tracing.stat"

#endif
