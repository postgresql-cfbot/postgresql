/*-------------------------------------------------------------------------
 * logical_xlog.h
 *	   Exports from replication/logical/logical_xlog.c
 *
 * Copyright (c) 2013-2022, PostgreSQL Global Development Group
 *
 * src/include/replication/logical_xlog.h
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_MESSAGE_H
#define PG_LOGICAL_MESSAGE_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "storage/off.h"
#include "utils/rel.h"

/*
 * xl_heap_update flag values, 8 bits are available.
 */
#define XLL_UPDATE_CONTAINS_OLD		(1<<0)

/*
 * xl_heap_delete flag values, 8 bits are available.
 */
#define XLL_DELETE_CONTAINS_OLD		(1<<0)

/*
 * Generic logical decoding message wal record.
 */
typedef struct xl_logical_message
{
	Oid			dbId;			/* database Oid emitted from */
	bool		transactional;	/* is message transactional? */
	Size		prefix_size;	/* length of prefix */
	Size		message_size;	/* size of the message */
	/* payload, including null-terminated prefix of length prefix_size */
	char		message[FLEXIBLE_ARRAY_MEMBER];
} xl_logical_message;

#define SizeOfLogicalMessage	(offsetof(xl_logical_message, message))

/* This is what we need to know about insert */
typedef struct xl_logical_insert
{
	RelFileNode		node;

	/* tuple data follows */
} xl_logical_insert;

#define SizeOfLogicalInsert	(offsetof(xl_logical_insert, node) + sizeof(RelFileNode))

/*
 * This is what we need to know about update.
 */
typedef struct xl_logical_update
{
	RelFileNode	node;
	Size		new_datalen;
	uint8		flags;

	/* tuple data follows */
} xl_logical_update;

#define SizeOfLogicalUpdate	(offsetof(xl_logical_update, flags) + sizeof(uint8))

/* This is what we need to know about delete */
typedef struct xl_logical_delete
{
	RelFileNode	node;
	uint8		flags;

	/* tuple data follows */
} xl_logical_delete;

#define SizeOfLogicalDelete	(offsetof(xl_logical_delete, flags) + sizeof(uint8))

/*
 * For truncate we list all truncated relids in an array, followed by all
 * sequence relids that need to be restarted, if any.
 * All rels are always within the same database, so we just list dbid once.
 *
 * Note: identical to xl_logical_truncate, except that no redo is performed, only
 * decoding.
 */
typedef struct xl_logical_truncate
{
	Oid			dbId;
	uint32		nrelids;
	uint8		flags;
	Oid			relids[FLEXIBLE_ARRAY_MEMBER];
} xl_logical_truncate;

#define SizeOfLogicalTruncate	(offsetof(xl_logical_truncate, relids))

struct TupleTableSlot;

extern XLogRecPtr LogLogicalMessage(const char *prefix, const char *message,
									size_t size, bool transactional);
extern XLogRecPtr LogLogicalInsert(Relation relation, struct TupleTableSlot *slot);
extern XLogRecPtr LogLogicalUpdate(Relation relation, struct TupleTableSlot *old_slot,
								   struct TupleTableSlot *new_slot);
extern XLogRecPtr LogLogicalDelete(Relation relation, struct TupleTableSlot *slot);
extern XLogRecPtr LogLogicalTruncate(List *relids, bool cascade, bool restart_seqs);

/* RMGR API*/
#define XLOG_LOGICAL_MESSAGE	0x00
#define XLOG_LOGICAL_INSERT		0x10
#define XLOG_LOGICAL_UPDATE		0x20
#define XLOG_LOGICAL_DELETE		0x30
#define XLOG_LOGICAL_TRUNCATE	0x40

/*
 * xl_logical_truncate flag values, 8 bits are available.
 */
#define XLL_TRUNCATE_CASCADE					(1<<0)
#define XLL_TRUNCATE_RESTART_SEQS				(1<<1)

void		logical_redo(XLogReaderState *record);
void		logical_desc(StringInfo buf, XLogReaderState *record);
const char *logical_identify(uint8 info);

#endif							/* PG_LOGICAL_MESSAGE_H */
