/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"

/* CopyStateData is private in commands/copy.c */
typedef struct CopyStateData *CopyState;
typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread);
typedef void (*copy_data_dest_cb) (void *outbuf, int len);

extern void DoCopy(ParseState *state, const CopyStmt *stmt,
				   int stmt_location, int stmt_len,
				   uint64 *processed);

extern void ProcessCopyOptions(ParseState *pstate, CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyFrom(ParseState *pstate, Relation rel, const char *filename,
							   bool is_program, copy_data_source_cb data_source_cb, List *attnamelist, List *options);
extern void EndCopyFrom(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
extern bool NextCopyFromRawFields(CopyState cstate,
								  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyState cstate);

extern CopyState BeginCopyTo(ParseState *pstate, Relation rel,
							 TupleDesc tupDesc, RawStmt *query,
							 Oid queryRelId, const char *filename, bool is_program,
							 copy_data_dest_cb data_dest_cb, List *attnamelist,
							 List *options);
extern void EndCopyTo(CopyState cstate);
extern void CopyOneRowTo(CopyState cstate, TupleTableSlot *slot);
extern void CopyToStart(CopyState cstate);
extern void CopyToFinish(CopyState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);

#endif							/* COPY_H */
