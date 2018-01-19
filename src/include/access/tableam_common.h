/*-------------------------------------------------------------------------
 *
 * tableam_common.h
 *	  POSTGRES table access method definitions shared across
 *	  all pluggable table access methods and server.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLEAM_COMMON_H
#define TABLEAM_COMMON_H

#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "executor/tuptable.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"


/* A physical tuple coming from a table AM scan */
typedef void *TableTuple;

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
	HEAPTUPLE_DEAD,				/* tuple is dead and deletable */
	HEAPTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	HEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	HEAPTUPLE_INSERT_IN_PROGRESS,	/* inserting xact is still in progress */
	HEAPTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;

typedef struct BulkInsertStateData *BulkInsertState;

/* struct definition is private to rewriteheap.c */
typedef struct RewriteStateData *RewriteState;

/*
 * slot table AM routine functions
 */
typedef void (*SlotStoreTuple_function) (TupleTableSlot *slot,
										 TableTuple tuple,
										 bool shouldFree,
										 bool minumumtuple);
typedef void (*SlotClearTuple_function) (TupleTableSlot *slot);
typedef Datum (*SlotGetattr_function) (TupleTableSlot *slot,
									   int attnum, bool *isnull);
typedef void (*SlotVirtualizeTuple_function) (TupleTableSlot *slot, int16 upto);

typedef HeapTuple (*SlotGetTuple_function) (TupleTableSlot *slot, bool palloc_copy);
typedef MinimalTuple (*SlotGetMinTuple_function) (TupleTableSlot *slot, bool palloc_copy);

typedef void (*SlotUpdateTableoid_function) (TupleTableSlot *slot, Oid tableoid);

typedef struct SlotTableAmRoutine
{
	/* Operations on TupleTableSlot */
	SlotStoreTuple_function slot_store_tuple;
	SlotVirtualizeTuple_function slot_virtualize_tuple;
	SlotClearTuple_function slot_clear_tuple;
	SlotGetattr_function slot_getattr;
	SlotGetTuple_function slot_tuple;
	SlotGetMinTuple_function slot_min_tuple;
	SlotUpdateTableoid_function slot_update_tableoid;
}			SlotTableAmRoutine;

typedef SlotTableAmRoutine * (*slot_tableam_hook) (void);

extern SlotTableAmRoutine * slot_tableam_handler(void);

#endif							/* TABLEAM_COMMON_H */
