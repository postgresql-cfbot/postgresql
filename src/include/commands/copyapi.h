/*-------------------------------------------------------------------------
 *
 * copyapi.h
 *	  API for COPY TO/FROM handlers
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyapi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYAPI_H
#define COPYAPI_H

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

/* These are private in commands/copy[from|to].c */
typedef struct CopyFromStateData *CopyFromState;
typedef struct CopyToStateData *CopyToState;

/*
 * API structure for a COPY FROM format implementation.  Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyFromRoutine
{
	/*
	 * Called when COPY FROM is started to set up the input functions
	 * associated with the relation's attributes writing to.  `finfo` can be
	 * optionally filled to provide the catalog information of the input
	 * function.  `typioparam` can be optionally filled to define the OID of
	 * the type to pass to the input function.  `atttypid` is the OID of data
	 * type used by the relation's attribute.
	 */
	void		(*CopyFromInFunc) (CopyFromState cstate, Oid atttypid,
								   FmgrInfo *finfo, Oid *typioparam);

	/*
	 * Called when COPY FROM is started.
	 *
	 * `tupDesc` is the tuple descriptor of the relation where the data needs
	 * to be copied.  This can be used for any initialization steps required
	 * by a format.
	 */
	void		(*CopyFromStart) (CopyFromState cstate, TupleDesc tupDesc);

	/*
	 * Copy one row to a set of `values` and `nulls` of size tupDesc->natts.
	 *
	 * 'econtext' is used to evaluate default expression for each column that
	 * is either not read from the file or is using the DEFAULT option of COPY
	 * FROM.  It is NULL if no default values are used.
	 *
	 * Returns false if there are no more tuples to copy.
	 */
	bool		(*CopyFromOneRow) (CopyFromState cstate, ExprContext *econtext,
								   Datum *values, bool *nulls);

	/* Called when COPY FROM has ended. */
	void		(*CopyFromEnd) (CopyFromState cstate);
} CopyFromRoutine;

/*
 * API structure for a COPY TO format implementation.   Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
typedef struct CopyToRoutine
{
	/*
	 * Called when COPY TO is started to set up the output functions
	 * associated with the relation's attributes reading from.  `finfo` can be
	 * optionally filled to provide the catalog information of the output
	 * function.  `atttypid` is the OID of data type used by the relation's
	 * attribute.
	 */
	void		(*CopyToOutFunc) (CopyToState cstate, Oid atttypid,
								  FmgrInfo *finfo);

	/*
	 * Called when COPY TO is started.
	 *
	 * `tupDesc` is the tuple descriptor of the relation from where the data
	 * is read.
	 */
	void		(*CopyToStart) (CopyToState cstate, TupleDesc tupDesc);

	/*
	 * Copy one row for COPY TO.
	 *
	 * `slot` is the tuple slot where the data is emitted.
	 */
	void		(*CopyToOneRow) (CopyToState cstate, TupleTableSlot *slot);

	/* Called when COPY TO has ended */
	void		(*CopyToEnd) (CopyToState cstate);
} CopyToRoutine;

#endif							/* COPYAPI_H */
