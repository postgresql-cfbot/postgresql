/*-------------------------------------------------------------------------
 *
 * undo_parser.h
 *
 * The API to retrieve undo record set chunks and undo records from undo log
 * pages.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/undo_parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_PARSER_H
#define UNDO_PARSER_H

typedef struct UndoSegFile
{
	char		path[MAXPGPATH];
	UndoLogNumber logno;
	UndoLogOffset offset;

	/* The following fields are only used in backend code. */
	UndoLogOffset insert;		/* The last known insertion pointer. */
	char		persistence;
} UndoSegFile;

/*
 * Type-specific header of the undo record set.
 *
 * Add other record types here if they are introduced.
 */
typedef union
{
	XactUndoRecordSetHeader xact;
	char		foo[4];
}			TypeHeader;

/*
 * Information about a single chunk of an undo record set. This is the output
 * of parsing.
 */
typedef struct UndoLogChunkInfo
{
	UndoRecordSetChunkHeader hdr;
	UndoRecordSetType type;
	TypeHeader	type_header;
}			UndoLogChunkInfo;

/*
 * Common storage for chunks and records contained on an undo page.
 */
typedef struct UndoPageItem
{
	UndoRecPtr	location;

	union
	{
		UndoLogChunkInfo chunk;
		UndoNode	record;
	}			u;
}			UndoPageItem;

/*
 * State of parsing of an undo log file.
 *
 * See the header comment of parse_undo_page() for explanation how to use it.
 */
typedef struct UndoLogParserState
{
	/* Start of the current chunk. */
	UndoRecPtr	current_chunk;

	/* The current chunk header */
	UndoRecordSetChunkHeader chunk_hdr;
	/* These fields track progress of parsing of chunk_hdr. */
	int			chunk_bytes_to_skip;
	int			chunk_hdr_bytes_left;

	/* Type of the URS owning the current chunk. */
	UndoRecordSetType urs_type;

	/* Likewise, the type header. */
	TypeHeader	type_hdr;
	int			type_hdr_size;
	int			type_hdr_bytes_left;

	/* Track parsing of the chunk data. */
	UndoLogOffset chunk_bytes_left;

	/* Seen uninitialized part of an undo page?? */
	bool		gap;

	UndoPageItem *items;
	int			nitems;			/* Number of items. */
	int			nitems_max;		/* Allocated array size. */

	int			npage_records;
}			UndoLogParserState;

extern void initialize_undo_parser(UndoLogParserState * state);
extern void finalize_undo_parser(UndoLogParserState * state, bool records);
extern void parse_undo_page(UndoLogParserState * s, char *page, int nr,
							UndoSegFile *seg, bool records);
#endif
