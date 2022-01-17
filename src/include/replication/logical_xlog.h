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
	OffsetNumber	offnum;		/* inserted tuple's offset */
	Size			datalen;
	uint8			flags;

	/* xl_heap_header & TUPLE DATA in backup block 0 */
} xl_logical_insert;

#define SizeOfLogicalInsert	(offsetof(xl_logical_insert, flags) + sizeof(uint8))

struct TupleTableSlot;

extern XLogRecPtr LogLogicalMessage(const char *prefix, const char *message,
									size_t size, bool transactional);
extern XLogRecPtr LogLogicalInsert(Relation relation, struct TupleTableSlot *slot);

/* RMGR API*/
#define XLOG_LOGICAL_MESSAGE	0x00
#define XLOG_LOGICAL_INSERT		0x10
#define XLOG_LOGICAL_UPDATE		0x20
#define XLOG_LOGICAL_DELETE		0x30
#define XLOG_LOGICAL_TRUNCATE	0x40

void		logical_redo(XLogReaderState *record);
void		logical_desc(StringInfo buf, XLogReaderState *record);
const char *logical_identify(uint8 info);

#endif							/* PG_LOGICAL_MESSAGE_H */
