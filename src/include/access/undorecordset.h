/*-------------------------------------------------------------------------
 *
 * undorecordset.h
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undorecordset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDORECORDSET_H
#define UNDORECORDSET_H

#include "access/transam.h"
#include "access/undodefs.h"
#include "access/xlogreader.h"
#ifdef FRONTEND
#include "common/logging.h"
#endif
#include "storage/buf.h"

/*
 * Possible undo record set types. These are stored as 1-byte values on disk;
 * changing the values is an on-disk format break.
 */
typedef enum UndoRecordSetType
{
	URST_INVALID = 0,			/* Placeholder when there's no record set. */
	URST_TRANSACTION = 'T',		/* Normal xact undo; apply on abort. */
	URST_FOO = 'F'				/* XXX. Crude hack; replace me. */
} UndoRecordSetType;

/*
 * The header that appears at the start of each 'chunk'.
 */
typedef struct UndoRecordSetChunkHeader
{
	UndoLogOffset size;

	UndoRecPtr	previous_chunk;

	uint8		type;

	/*
	 * Consider the chunk discarded? If the first chunk in an undo log is
	 * discarded, the discard pointer of the underlying undo log can advance
	 * to the beginning of the following chunk.
	 *
	 * This information is stored in every chunk so that the actual discarding
	 * can be independent from evaluation of the type header. In particular,
	 * for the URST_TRANSACTION, the problem is that only the first chunk of
	 * the set contains the XID. So without this flag we'd have to search for
	 * the first chunk when trying to discard any chunk of the set.
	 */
	bool		discarded;
} UndoRecordSetChunkHeader;

#define SizeOfUndoRecordSetChunkHeader \
	(offsetof(UndoRecordSetChunkHeader, discarded) + sizeof(bool))

/* On-disk header for an UndoRecordSet of type URST_TRANSACTION. */
typedef struct XactUndoRecordSetHeader
{
	FullTransactionId fxid;

	/*
	 * Was the set applied. Never set to true if the transaction committed.
	 */
	bool		applied;
} XactUndoRecordSetHeader;

#define SizeOfXactUndoRecordSetHeader \
	(offsetof(XactUndoRecordSetHeader, applied) + sizeof(bool))

/*
 * TODO Handle the missing types.
 */
static inline size_t
get_urs_type_header_size(UndoRecordSetType type)
{
	switch (type)
	{
		case URST_TRANSACTION:
			return SizeOfXactUndoRecordSetHeader;
		case URST_FOO:
			return 4;
		default:
#ifndef FRONTEND
			elog(FATAL, "unrecognized undo record set type %d", type);
#else
			pg_log_error("unrecognized undo record set type %d", type);
			exit(EXIT_FAILURE);
#endif
	}
}

extern UndoRecordSet *UndoCreate(UndoRecordSetType type, char presistence,
								 int nestingLevel, Size type_header_size,
								 char *type_header);
extern bool UndoPrepareToMarkClosed(UndoRecordSet *urs);
extern void UndoMarkClosed(UndoRecordSet *urs);
extern void UndoPrepareToOverwriteChunkData(UndoRecPtr urp, int data_size,
											char persistence, Buffer *bufs);
extern UndoRecPtr UndoPrepareToInsert(UndoRecordSet *urs, size_t record_size);
extern void UndoInsert(UndoRecordSet *urs,
					   void *record_data,
					   size_t record_size);
extern void UndoPrepareToSetChunkHeaderFlag(UndoRecPtr chunk_hdr,
											uint16 off,
											char persistence,
											Buffer *buf);
extern void UndoSetChunkHeaderFlag(UndoRecPtr chunk_hdr, uint16 off,
								   Buffer buf,
								   uint8 first_block_id);
extern void UndoPageSetLSN(UndoRecordSet *urs, XLogRecPtr lsn);
extern void UndoRelease(UndoRecordSet *urs);
extern void UndoDestroy(UndoRecordSet *urs);
extern void UndoXLogRegisterBuffers(UndoRecordSet *urs, uint8 first_block_id);

/* recovery */
extern UndoRecPtr UndoReplay(XLogReaderState *xlog_record,
							 void *record_data,
							 size_t record_size);
extern void CloseDanglingUndoRecordSets(void);
extern void UndoSetFlag(UndoRecPtr last_chunk, uint16 off, char persistence);
extern void ApplyPendingUndo(void);

/* transaction integration */
extern void UndoResetInsertion(void);
extern bool UndoPrepareToMarkClosedForXactLevel(int nestingLevel);
extern void UndoMarkClosedForXactLevel(int nestingLevel);
extern void UndoXLogRegisterBuffersForXactLevel(int nestingLevel,
												uint8 first_block_id);
extern void UndoPageSetLSNForXactLevel(int nestingLevel, XLogRecPtr lsn);
extern void UndoDestroyForXactLevel(int nestingLevel);
extern bool UndoCloseAndDestroyForXactLevel(int nestingLevel);

extern void AtProcExit_UndoRecordSet(void);

extern TransactionId AdvanceOldestXidHavingUndo(void);
extern void DiscardUndoRecordSetChunks(void);
#endif
