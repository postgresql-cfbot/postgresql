/*-------------------------------------------------------------------------
 *
 * compression.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2016, PostgreSQL Global Development Group
 *
 * src/include/access/compression.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSION_H
#define COMPRESSION_H

#include "postgres.h"
#include "catalog/pg_attribute.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

/* parsenodes.h */
typedef struct ColumnCompression ColumnCompression;
typedef struct CompressionMethodRoutine CompressionMethodRoutine;

typedef struct
{
	Oid			cmoptoid;
	CompressionMethodRoutine *routine;
	List	   *options;
} CompressionMethodOptions;

typedef void (*CompressionConfigureRoutine)
			(Form_pg_attribute attr, List *options);
typedef struct varlena *(*CompressionRoutine)
			(CompressionMethodOptions *cmoptions, const struct varlena *data);

/*
 * API struct for a compression method.
 * Note this must be stored in a single palloc'd chunk of memory.
 */
typedef struct CompressionMethodRoutine
{
	NodeTag		type;

	CompressionConfigureRoutine configure;
	CompressionConfigureRoutine drop;
	CompressionRoutine compress;
	CompressionRoutine decompress;
} CompressionMethodRoutine;

/* Compression method handler parameters */
typedef struct CompressionMethodOpArgs
{
	Oid			cmhanderid;
	Oid			typeid;
}			CompressionMethodOpArgs;

extern CompressionMethodRoutine *GetCompressionMethodRoutine(Oid cmoptoid);
extern List *GetCompressionOptionsList(Oid cmoptoid);
extern Oid CreateCompressionOptions(Form_pg_attribute attr,
						 ColumnCompression *compression);
extern ColumnCompression *GetColumnCompressionForAttribute(Form_pg_attribute att);
extern void CheckCompressionMismatch(ColumnCompression *c1,
						 ColumnCompression *c2, const char *attributeName);
extern void CreateColumnCompressionDependency(Form_pg_attribute attr,
								  Oid cmoptoid);

#endif							/* COMPRESSION_H */
