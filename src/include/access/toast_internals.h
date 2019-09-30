/*-------------------------------------------------------------------------
 *
 * toast_internals.h
 *	  Internal definitions for the TOAST system.
 *
 * Copyright (c) 2000-2019, PostgreSQL Global Development Group
 *
 * src/include/access/toast_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TOAST_INTERNALS_H
#define TOAST_INTERNALS_H

#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 *	The information at the start of the compressed toast data.
 */
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		rawsize;
} toast_compress_header;

/*
 * Utilities for manipulation of header information for compressed
 * toast entries.
 */
#define TOAST_COMPRESS_HDRSZ		((int32) sizeof(toast_compress_header))
#define TOAST_COMPRESS_RAWSIZE(ptr) (((toast_compress_header *) (ptr))->rawsize)
#define TOAST_COMPRESS_RAWDATA(ptr) \
	(((char *) (ptr)) + TOAST_COMPRESS_HDRSZ)
#define TOAST_COMPRESS_SET_RAWSIZE(ptr, len) \
	(((toast_compress_header *) (ptr))->rawsize = (len))

extern Datum toast_compress_datum(Datum value);
extern Oid	toast_get_valid_index(Oid toastoid, LOCKMODE lock);

extern void toast_delete_datum(Relation rel, Datum value, bool is_speculative);
extern Datum toast_save_datum(Relation rel, Datum value,
							  struct varlena *oldexternal, int options);

extern int	toast_open_indexes(Relation toastrel,
							   LOCKMODE lock,
							   Relation **toastidxs,
							   int *num_indexes);
extern void toast_close_indexes(Relation *toastidxs, int num_indexes,
								LOCKMODE lock);
extern void init_toast_snapshot(Snapshot toast_snapshot);

#ifndef FRONTEND
#include "access/genam.h"

/*
 * TOAST buffer is a producer consumer buffer.
 *
 *    +--+--+--+--+--+--+--+--+--+--+--+--+--+
 *    |  |  |  |  |  |  |  |  |  |  |  |  |  |
 *    +--+--+--+--+--+--+--+--+--+--+--+--+--+
 *    ^           ^           ^              ^
 *   buf      position      limit         capacity
 *
 * buf: point to the start of buffer.
 * position: point to the next char to be consumed.
 * limit: point to the next char to be produced.
 * capacity: point to the end of buffer.
 *
 * Constraints that need to be satisfied:
 * buf <= position <= limit <= capacity
 */
typedef struct ToastBuffer
{
	const char	*buf;
	const char	*position;
	char		*limit;
	const char	*capacity;
} ToastBuffer;

typedef struct FetchDatumIteratorData
{
	ToastBuffer	*buf;
	Relation	toastrel;
	Relation	*toastidxs;
	SysScanDesc	toastscan;
	ScanKeyData	toastkey;
	SnapshotData			snapshot;
	struct varatt_external	toast_pointer;
	int32		ressize;
	int32		nextidx;
	int32		numchunks;
	int			num_indexes;
	bool		done;
}				FetchDatumIteratorData;

typedef struct FetchDatumIteratorData *FetchDatumIterator;

/*
 * If "ctrlc" field in iterator is equal to INVALID_CTRLC, it means that
 * the field is invalid and need to read the control byte from the
 * source buffer in the next iteration, see pglz_decompress_iterate().
 */
#define INVALID_CTRLC 8

typedef struct DetoastIteratorData
{
	ToastBuffer 		*buf;
	FetchDatumIterator	fetch_datum_iterator;
	unsigned char		ctrl;
	int					ctrlc;
	bool				compressed;		/* toast value is compressed? */
	bool				done;
}			DetoastIteratorData;

typedef struct DetoastIteratorData *DetoastIterator;

#endif

extern FetchDatumIterator create_fetch_datum_iterator(struct varlena *attr);
extern void free_fetch_datum_iterator(FetchDatumIterator iter);
extern void fetch_datum_iterate(FetchDatumIterator iter);
extern ToastBuffer *create_toast_buffer(int32 size, bool compressed);
extern void free_toast_buffer(ToastBuffer *buf);
extern void pglz_decompress_iterate(ToastBuffer *source, ToastBuffer *dest,
									DetoastIterator iter);

#endif							/* TOAST_INTERNALS_H */
