/*-------------------------------------------------------------------------
 *
 * toast_internals.h
 *	  Internal definitions for the TOAST system.
 *
 * Copyright (c) 2000-2021, PostgreSQL Global Development Group
 *
 * src/include/access/toast_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TOAST_INTERNALS_H
#define TOAST_INTERNALS_H

#include "access/compressamapi.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 *	The information at the start of the compressed toast data.
 */
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		info;			/* 2 bits for compression method and 30 bits
								 * rawsize */
} toast_compress_header;

/*
 * If the compression method were used, then data also contains
 * Oid of compression options
 */
typedef struct toast_compress_header_custom
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		info;			/*  2 bits for compression method + rawsize */
	Oid			cmoid;			/* Oid from pg_am */
} toast_compress_header_custom;

#define RAWSIZEMASK (0x3FFFFFFFU)

/*
 * Utilities for manipulation of header information for compressed
 * toast entries.
 *
 * Since version 11 TOAST_COMPRESS_SET_RAWSIZE also marks compressed
 * varlenas as custom compressed. Such varlenas will contain 0x02 (0b10) in
 * two highest bits.
 */
#define TOAST_COMPRESS_HDRSZ		((int32) sizeof(toast_compress_header))
#define TOAST_CUSTOM_COMPRESS_HDRSZ ((int32)sizeof(toast_compress_header_custom))
#define TOAST_COMPRESS_RAWSIZE(ptr) (((toast_compress_header *) (ptr))->info & RAWSIZEMASK)
#define TOAST_COMPRESS_METHOD(ptr)  (((toast_compress_header *) (ptr))->info >> 30)
#define TOAST_COMPRESS_SIZE(ptr)	((int32) VARSIZE_ANY(ptr) - TOAST_COMPRESS_HDRSZ)
#define TOAST_COMPRESS_RAWDATA(ptr) \
	(((char *) (ptr)) + TOAST_COMPRESS_HDRSZ)
#define TOAST_COMPRESS_SET_SIZE_AND_METHOD(ptr, len, cm_method) \
	do { \
		Assert((len) > 0 && (len) <= RAWSIZEMASK); \
		((toast_compress_header *) (ptr))->info = ((len) | (cm_method) << 30); \
	} while (0)

#define TOAST_COMPRESS_SET_CMOID(ptr, oid) \
	(((toast_compress_header_custom *)(ptr))->cmoid = (oid))

extern Datum toast_compress_datum(Datum value, Oid cmoid, List *cmoptions);
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

#endif							/* TOAST_INTERNALS_H */
