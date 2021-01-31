/*-------------------------------------------------------------------------
 *
 * compressamapi.h
 *	  API for Postgres compression methods.
 *
 * Copyright (c) 2015-2017, PostgreSQL Global Development Group
 *
 * src/include/access/compressamapi.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPRESSAMAPI_H
#define COMPRESSAMAPI_H

#include "postgres.h"

#include "catalog/pg_am_d.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

/*
 * Built-in compression method-id.  The toast compression header will store
 * this in the first 2 bits of the raw length.  These built-in compression
 * method-id are directly mapped to the built-in compression method oid.
 */
typedef enum CompressionId
{
	PGLZ_COMPRESSION_ID = 0,
	LZ4_COMPRESSION_ID = 1,
	/* one free slot for the future built-in method */
	CUSTOM_COMPRESSION_ID = 3
} CompressionId;

/* Use default compression method if it is not specified. */
#define DefaultCompressionOid	PGLZ_COMPRESSION_AM_OID
#define IsCustomCompression(cmid)     ((cmid) == CUSTOM_COMPRESSION_ID)
#define IsStorageCompressible(storage) ((storage) != TYPSTORAGE_PLAIN && \
										(storage) != TYPSTORAGE_EXTERNAL)
/* compression handler routines */
typedef void (*cmcheck_function) (List *options);
typedef void *(*cminitstate_function) (List *options);
typedef struct varlena *(*cmcompress_function) (const struct varlena *value,
												int32 toast_header_size,
												void *options);
typedef struct varlena *(*cmdecompress_function) (const struct varlena *value,
												  int32 toast_header_size);
typedef struct varlena *(*cmdecompress_slice_function)
												(const struct varlena *value,
												 int32 toast_header_size,
												 int32 slicelength);

/*
 * API struct for a compression AM.
 *
 * 'cmcheck' - called when attribute is linking with compression method.
 *  This function should check compability of compression method with
 *  the attribute and its options.
 *
 * 'cminitstate' - called when CompressionAmOptions instance is created.
 *  Should return pointer to a memory in a caller memory context, or NULL.
 *  Could be used to pass some internal state between compression function
 *  calls, like internal structure for parsed compression options.
 *
 * 'datum_compress' - varlena compression function.
 * 'datum_decompress' - varlena decompression function.
 * 'datum_decompress_slice' - varlena slice decompression functions.
 */
typedef struct CompressionAmRoutine
{
	NodeTag type;

	cmcheck_function datum_check;		  /* can be NULL */
	cminitstate_function datum_initstate; /* can be NULL */
	cmcompress_function datum_compress;
	cmdecompress_function datum_decompress;
	cmdecompress_slice_function datum_decompress_slice;
} CompressionAmRoutine;

extern const CompressionAmRoutine pglz_compress_methods;
extern const CompressionAmRoutine lz4_compress_methods;

/* access/compression/compressamapi.c */
extern CompressionId CompressionOidToId(Oid cmoid);
extern Oid CompressionIdToOid(CompressionId cmid);
extern CompressionAmRoutine *GetCompressionAmRoutineByAmId(Oid amoid);

#endif							/* COMPRESSAMAPI_H */
