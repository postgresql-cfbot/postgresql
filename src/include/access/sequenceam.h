/*-------------------------------------------------------------------------
 *
 * sequenceam.h
 *	  POSTGRES sequence access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/sequenceam.h
 *
 * NOTES
 *		See sequenceam.sgml for higher level documentation.
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQUENCEAM_H
#define SEQUENCEAM_H

#include "utils/rel.h"

#define DEFAULT_SEQUENCE_ACCESS_METHOD "local"

/* GUCs */
extern PGDLLIMPORT char *default_sequence_access_method;

/*
 * API struct for a sequence AM.  Note this must be allocated in a
 * server-lifetime manner, typically as a static const struct, which then gets
 * returned by FormData_pg_am.amhandler.
 *
 * In most cases it's not appropriate to call the callbacks directly, use the
 * sequence_* wrapper functions instead.
 *
 * GetSequenceAmRoutine() asserts that required callbacks are filled in,
 * remember to update when adding a callback.
 */
typedef struct SequenceAmRoutine
{
	/* this must be set to T_SequenceAmRoutine */
	NodeTag		type;

	/*
	 * Retrieve table access method used by a sequence to store its metadata.
	 */
	const char *(*get_table_am) (void);

	/*
	 * Initialize sequence after creating a sequence Relation in pg_class,
	 * setting up the sequence for use.  "last_value" and "is_called" are
	 * guessed from the options set for the sequence in CREATE SEQUENCE, based
	 * on the configuration of pg_sequence.
	 */
	void		(*init) (Relation rel, int64 last_value, bool is_called);

	/*
	 * Retrieve a result for nextval(), based on the options retrieved from
	 * the sequence's options in pg_sequence.  "last" is the last value
	 * calculated stored in the session's local cache, for lastval().
	 */
	int64		(*nextval) (Relation rel, int64 incby, int64 maxv,
							int64 minv, int64 cache, bool cycle,
							int64 *last);

	/*
	 * Callback to set the state of a sequence, based on the input arguments
	 * from setval().
	 */
	void		(*setval) (Relation rel, int64 next, bool iscalled);

	/*
	 * Reset a sequence to its initial value.  "reset_state", if set to true,
	 * means that the sequence parameters have changed, hence its internal
	 * state may need to be reset as well.  "startv" and "is_called" are
	 * values guessed from the configuration of the sequence, based on the
	 * contents of pg_sequence.
	 */
	void		(*reset) (Relation rel, int64 startv, bool is_called,
						  bool reset_state);

	/*
	 * Returns the current state of a sequence, returning data for
	 * pg_sequence_last_value() and related DDLs like ALTER SEQUENCE.
	 * "last_value" and "is_called" should be assigned to the values retrieved
	 * from the sequence Relation.
	 */
	void		(*get_state) (Relation rel, int64 *last_value, bool *is_called);

	/*
	 * Callback used when switching persistence of a sequence Relation, to
	 * reset the sequence based on its new persistence "newrelpersistence".
	 */
	void		(*change_persistence) (Relation rel, char newrelpersistence);

} SequenceAmRoutine;


/* ---------------------------------------------------------------------------
 * Wrapper functions for each callback.
 * ---------------------------------------------------------------------------
 */

/*
 * Returns the name of the table access method used by this sequence.
 */
static inline const char *
sequence_get_table_am(Relation rel)
{
	return rel->rd_sequenceam->get_table_am();
}

/*
 * Insert tuple data based on the information guessed from the contents
 * of pg_sequence.
 */
static inline void
sequence_init(Relation rel, int64 last_value, bool is_called)
{
	rel->rd_sequenceam->init(rel, last_value, is_called);
}

/*
 * Allocate a set of values for the given sequence.  "last" is the last value
 * allocated.  The result returned is the next value of the sequence computed.
 */
static inline int64
sequence_nextval(Relation rel, int64 incby, int64 maxv,
				 int64 minv, int64 cache, bool cycle,
				 int64 *last)
{
	return rel->rd_sequenceam->nextval(rel, incby, maxv, minv, cache,
									   cycle, last);
}

/*
 * Callback to set the state of a sequence, based on the input arguments
 * from setval().
 */
static inline void
sequence_setval(Relation rel, int64 next, bool iscalled)
{
	rel->rd_sequenceam->setval(rel, next, iscalled);
}

/*
 * Reset a sequence to its initial state.
 */
static inline void
sequence_reset(Relation rel, int64 startv, bool is_called,
			   bool reset_state)
{
	rel->rd_sequenceam->reset(rel, startv, is_called, reset_state);
}

/*
 * Retrieve sequence metadata.
 */
static inline void
sequence_get_state(Relation rel, int64 *last_value, bool *is_called)
{
	rel->rd_sequenceam->get_state(rel, last_value, is_called);
}

/*
 * Callback to change the persistence of a sequence Relation.
 */
static inline void
sequence_change_persistence(Relation rel, char newrelpersistence)
{
	rel->rd_sequenceam->change_persistence(rel, newrelpersistence);
}

/* ----------------------------------------------------------------------------
 * Functions in sequenceamapi.c
 * ----------------------------------------------------------------------------
 */

extern const SequenceAmRoutine *GetSequenceAmRoutine(Oid amhandler);
extern Oid	GetSequenceAmRoutineId(Oid amoid);

/* ----------------------------------------------------------------------------
 * Functions in local.c
 * ----------------------------------------------------------------------------
 */

extern const SequenceAmRoutine *GetLocalSequenceAmRoutine(void);

#endif							/* SEQUENCEAM_H */
