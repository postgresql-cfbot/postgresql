/*-------------------------------------------------------------------------
 *
 * undoread.h
 *	  facilities for reading UNDO
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undoread.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UNDOREAD_H
#define UNDOREAD_H

#include "access/undodefs.h"
#include "access/undorecordset.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"

typedef struct UndoRecordSetChunkListItem
{
	UndoRecPtr	urp_chunk_header;
	UndoRecPtr	urp_chunk_end;
	UndoRecordSetChunkHeader header;
} UndoRecordSetChunkListItem;

typedef struct UndoRecordSetChunkList
{
	int			nchunks;
	UndoRecordSetChunkListItem *chunks;
} UndoRecordSetChunkList;

typedef struct UndoCachedBuffer
{
	Buffer		pinned_buffer;
	BlockNumber pinned_block;

	/*
	 * XXX Should we introduce InvalidLogNumber? Currently we consider it
	 * valid iff pinned_block is valid.
	 */
	UndoLogNumber pinned_log;
} UndoCachedBuffer;

typedef struct UndoRSReaderState
{
	UndoRecPtr	start_reading;
	UndoRecPtr	end_reading;
	char		relpersistence;

	UndoRecordSetChunkList chunks;

	UndoCachedBuffer cached_buffer;

	int			current_chunk;
	UndoRecPtr	last_record;
	UndoRecPtr	next_urp;

	WrittenUndoNode node;
	StringInfoData buf;

	/*
	 * Record distances in varbyte format. The individual record pointers are
	 * derived from these values, relative to end_reading.
	 */
	StringInfoData rec_dists;

	/* Pointer to the last processed byte of rec_lengths.data. */
	char	   *backward_cur;
} UndoRSReaderState;

extern UndoRecPtr undo_reader_read_bytes(UndoRSReaderState *r, UndoRecPtr urp,
										 size_t nbytes, bool allow_discarded);
extern void UndoRSReaderInit(UndoRSReaderState *r,
							 UndoRecPtr start, UndoRecPtr end,
							 char relpersistence, bool toplevel);
extern bool UndoRSReaderReadOneForward(UndoRSReaderState *r,
									   bool length_only);
extern bool UndoRSReaderReadOneBackward(UndoRSReaderState *r);
extern void UndoRSReaderClose(UndoRSReaderState *r);

#endif							/* UNDOREAD_H */
