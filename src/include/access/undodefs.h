/*-------------------------------------------------------------------------
 *
 * undodefs.h
 *
 * Basic definitions for PostgreSQL undo layer. These are separated into
 * their own header file to avoid including more things than necessary
 * into widely-used headers.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undodefs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDODEFS_H
#define UNDODEFS_H

#include "access/rmgr.h"

/* The type used to identify an undo log and position within it. */
typedef uint64 UndoRecPtr;

/* The type used for undo record lengths. */
typedef uint16 UndoRecordSize;

/* Type for offsets within undo logs */
typedef uint64 UndoLogOffset;

/* Type for numbering undo logs. */
typedef int UndoLogNumber;

/* Special value for undo record pointer which indicates that it is invalid. */
#define	InvalidUndoRecPtr	((UndoRecPtr) 0)

/*
 * UndoRecPtrIsValid
 *		True iff undoRecPtr is valid.
 */
#define UndoRecPtrIsValid(undoRecPtr) \
	((bool) ((UndoRecPtr) (undoRecPtr) != InvalidUndoRecPtr))

/* Persistence levels as small integers that can be used as array indexes. */
typedef enum
{
	UNDOPERSISTENCE_PERMANENT = 0,
	UNDOPERSISTENCE_UNLOGGED = 1,
	UNDOPERSISTENCE_TEMP = 2
} UndoPersistenceLevel;

/* Number of supported persistence levels for undo. */
#define NUndoPersistenceLevels 3

/* Opaque types. */
struct UndoCheckpointContext;
struct UndoRecordSet;
struct UndoRequest;
typedef struct UndoCheckpointContext UndoCheckpointContext;
typedef struct UndoRecordSet UndoRecordSet;

typedef struct UndoNode
{
	/*
	 * TODO: replace with actual serialization format - to unblock
	 * development, have an absolutely dumb format, for now.
	 */
	Size		length;
	RmgrId		rmid;
	uint8		type;
	char	   *data;
} UndoNode;

/*
 * An undo node that has been deserialized from a specific location.
 * AFIXME: Probably better to rename, and add more information?
 */
typedef struct WrittenUndoNode
{
	UndoRecPtr	location;
	UndoNode	n;
} WrittenUndoNode;
#endif
