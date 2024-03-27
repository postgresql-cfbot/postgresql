/*-------------------------------------------------------------------------
 *
 * detoast.c
 *	  Retrieve compressed or external variable size attributes.
 *
 * Copyright (c) 2000-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/common/detoast.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/detoast.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/toast_internals.h"
#include "common/int.h"
#include "common/pg_lzcompress.h"
#include "utils/expandeddatum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static struct varlena *toast_fetch_datum(struct varlena *attr);
static struct varlena *toast_fetch_datum_slice(struct varlena *attr,
											   int32 sliceoffset,
											   int32 slicelength);
static struct varlena *toast_decompress_datum(struct varlena *attr);
static struct varlena *toast_decompress_datum_slice(struct varlena *attr, int32 slicelength);

bool enable_toast_cache = false;

HTAB *toastCache = NULL;
MemoryContext ToastCacheContext = NULL;
MemoryContext ToastSliceContext = NULL;

static uint64	toast_cache_hits = 0;
static uint64	toast_cache_misses = 0;

typedef struct toast_cache_key {
	Oid toastrelid;
	Oid valueid;
} toast_cache_key;

typedef struct toast_cache_entry {
	toast_cache_key key;
	int32	size;
	uintptr_t slices;
	void   *data;
} toast_cache_entry;

typedef struct toast_slice_entry {
	int32	offset;
	int32	length;
	void   *data;
} toast_slice_entry;

static toast_cache_entry *toast_cache_lookup(Oid toastrelid, Oid valueid);
static void toast_cache_add_entry(Oid toastrelid, Oid valueid, struct varlena *attr);
static void
toast_cache_add_slice_entry(Oid toastrelid, Oid valueid, struct varlena *attr,
						 int32 sliceoffset, int32 slicelength);
void toast_cache_add_slice_datum(Oid toastrelid, Oid valueid, struct varlena *attr, int32 offset, int32 length);
static void toast_cache_remove_entry(Oid toastrelid, Oid valueid);
static struct varlena *
toast_cache_lookup_slice(Oid toastrelid, Oid valueid, int32 offset, int32 length);

/* ----------
 * detoast_external_attr -
 *
 *	Public entry point to get back a toasted value from
 *	external source (possibly still in compressed format).
 *
 * This will return a datum that contains all the data internally, ie, not
 * relying on external storage or memory, but it can still be compressed or
 * have a short header.  Note some callers assume that if the input is an
 * EXTERNAL datum, the result will be a pfree'able chunk.
 * ----------
 */
struct varlena *
detoast_external_attr(struct varlena *attr)
{
	struct varlena *result;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/*
		 * This is an external stored plain value
		 */
		struct varatt_external toast_pointer;

		/* Must copy to access aligned fields */
		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		/* first try to lookup in the toast cache */
		result = toast_cache_lookup_datum(toast_pointer.va_toastrelid, toast_pointer.va_valueid);
		if (result)
			return result;

		/*
		 * This is an externally stored datum --- fetch it back from there
		 */
		result = toast_fetch_datum(attr);

		if (!VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			toast_cache_add_datum(toast_pointer.va_toastrelid, toast_pointer.va_valueid, result);

	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		/*
		 * This is an indirect pointer --- dereference it
		 */
		struct varatt_indirect redirect;

		VARATT_EXTERNAL_GET_POINTER(redirect, attr);
		attr = (struct varlena *) redirect.pointer;

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(attr));

		/* recurse if value is still external in some other way */
		if (VARATT_IS_EXTERNAL(attr))
			return detoast_external_attr(attr);

		/*
		 * Copy into the caller's memory context, in case caller tries to
		 * pfree the result.
		 */
		result = (struct varlena *) palloc(VARSIZE_ANY(attr));
		memcpy(result, attr, VARSIZE_ANY(attr));
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		/*
		 * This is an expanded-object pointer --- get flat format
		 */
		ExpandedObjectHeader *eoh;
		Size		resultsize;

		eoh = DatumGetEOHP(PointerGetDatum(attr));
		resultsize = EOH_get_flat_size(eoh);
		result = (struct varlena *) palloc(resultsize);
		EOH_flatten_into(eoh, (void *) result, resultsize);
	}
	else
	{
		/*
		 * This is a plain value inside of the main tuple - why am I called?
		 */
		result = attr;
	}

	return result;
}


/* ----------
 * detoast_attr -
 *
 *	Public entry point to get back a toasted value from compression
 *	or external storage.  The result is always non-extended varlena form.
 *
 * Note some callers assume that if the input is an EXTERNAL or COMPRESSED
 * datum, the result will be a pfree'able chunk.
 * ----------
 */
struct varlena *
detoast_attr(struct varlena *attr)
{
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varlena *result;
		struct varatt_external toast_pointer;

		/* Get toast pointer */
		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		/* try lookup in the toast cache */

		/* first try to lookup in the toast cache */
		result = toast_cache_lookup_datum(toast_pointer.va_toastrelid, toast_pointer.va_valueid);

		/* cache hit, we're done */
		if (result)
			return result;

		/*
		 * This is an externally stored datum --- fetch it back from there
		 */
		attr = toast_fetch_datum(attr);
		/* If it's compressed, decompress it */
		if (VARATT_IS_COMPRESSED(attr))
		{
			struct varlena *tmp = attr;

			attr = toast_decompress_datum(tmp);
			pfree(tmp);
		}

		toast_cache_add_datum(toast_pointer.va_toastrelid, toast_pointer.va_valueid, attr);
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		/*
		 * This is an indirect pointer --- dereference it
		 */
		struct varatt_indirect redirect;

		VARATT_EXTERNAL_GET_POINTER(redirect, attr);
		attr = (struct varlena *) redirect.pointer;

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(attr));

		/* recurse in case value is still extended in some other way */
		attr = detoast_attr(attr);

		/* if it isn't, we'd better copy it */
		if (attr == (struct varlena *) redirect.pointer)
		{
			struct varlena *result;

			result = (struct varlena *) palloc(VARSIZE_ANY(attr));
			memcpy(result, attr, VARSIZE_ANY(attr));
			attr = result;
		}
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		/*
		 * This is an expanded-object pointer --- get flat format
		 */
		attr = detoast_external_attr(attr);
		/* flatteners are not allowed to produce compressed/short output */
		Assert(!VARATT_IS_EXTENDED(attr));
	}
	else if (VARATT_IS_COMPRESSED(attr))
	{
		/*
		 * This is a compressed value inside of the main tuple
		 */
		attr = toast_decompress_datum(attr);
	}
	else if (VARATT_IS_SHORT(attr))
	{
		/*
		 * This is a short-header varlena --- convert to 4-byte header format
		 */
		Size		data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
		Size		new_size = data_size + VARHDRSZ;
		struct varlena *new_attr;

		new_attr = (struct varlena *) palloc(new_size);
		SET_VARSIZE(new_attr, new_size);
		memcpy(VARDATA(new_attr), VARDATA_SHORT(attr), data_size);
		attr = new_attr;
	}

	return attr;
}


/* ----------
 * detoast_attr_slice -
 *
 *		Public entry point to get back part of a toasted value
 *		from compression or external storage.
 *
 * sliceoffset is where to start (zero or more)
 * If slicelength < 0, return everything beyond sliceoffset
 * ----------
 */
struct varlena *
detoast_attr_slice(struct varlena *attr,
				   int32 sliceoffset, int32 slicelength)
{
	struct varlena *preslice;
	struct varlena *result = NULL;
	char	   *attrdata;
	int32		slicelimit;
	int32		attrsize;
	Oid toastrelid = InvalidOid;
	Oid valueid = InvalidOid;

	if (sliceoffset < 0)
		elog(ERROR, "invalid sliceoffset: %d", sliceoffset);

	/*
	 * Compute slicelimit = offset + length, or -1 if we must fetch all of the
	 * value.  In case of integer overflow, we must fetch all.
	 */
	if (slicelength < 0)
		slicelimit = -1;
	else if (pg_add_s32_overflow(sliceoffset, slicelength, &slicelimit))
		slicelength = slicelimit = -1;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);


		/* set the cache key */
		/* try lookup in the toast cache */
		toastrelid = toast_pointer.va_toastrelid;
		valueid = toast_pointer.va_valueid;

		/* first try to lookup in the toast cache */
		result = toast_cache_lookup_slice(toastrelid, valueid, sliceoffset, slicelength);

		/* cache hit, we're done */
		if (result)
			return result;

		/* fast path for non-compressed external datums */
		if (!VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		{
			result = toast_fetch_datum_slice(attr, sliceoffset, slicelength);
			toast_cache_add_slice_datum(toast_pointer.va_toastrelid, toast_pointer.va_valueid, result, sliceoffset, slicelength);
			return result;
		}

		/*
		 * For compressed values, we need to fetch enough slices to decompress
		 * at least the requested part (when a prefix is requested).
		 * Otherwise, just fetch all slices.
		 */
		if (slicelimit >= 0)
		{
			int32		max_size = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

			/*
			 * Determine maximum amount of compressed data needed for a prefix
			 * of a given length (after decompression).
			 *
			 * At least for now, if it's LZ4 data, we'll have to fetch the
			 * whole thing, because there doesn't seem to be an API call to
			 * determine how much compressed data we need to be sure of being
			 * able to decompress the required slice.
			 */
			if (VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer) ==
				TOAST_PGLZ_COMPRESSION_ID)
				max_size = pglz_maximum_compressed_size(slicelimit, max_size);

			/*
			 * Fetch enough compressed slices (compressed marker will get set
			 * automatically).
			 */

			preslice = toast_fetch_datum_slice(attr, 0, max_size);
		}
		else
		{
			preslice = toast_fetch_datum(attr);
		}
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		struct varatt_indirect redirect;

		VARATT_EXTERNAL_GET_POINTER(redirect, attr);

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(redirect.pointer));

		return detoast_attr_slice(redirect.pointer,
								  sliceoffset, slicelength);
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		/* pass it off to detoast_external_attr to flatten */
		preslice = detoast_external_attr(attr);
	}
	else
		preslice = attr;

	Assert(!VARATT_IS_EXTERNAL(preslice));

	if (VARATT_IS_COMPRESSED(preslice))
	{
		struct varlena *tmp = preslice;

		/* Decompress enough to encompass the slice and the offset */
		if (slicelimit >= 0)
			preslice = toast_decompress_datum_slice(tmp, slicelimit);
		else
			preslice = toast_decompress_datum(tmp);

		if(OidIsValid(toastrelid))
		{
			if(slicelimit >= 0)
				toast_cache_add_slice_datum(toastrelid, valueid, preslice, 0, slicelimit);
			else
				toast_cache_add_datum(toastrelid, valueid, preslice);
		}

		if (tmp != attr)
			pfree(tmp);
	}

	if (VARATT_IS_SHORT(preslice))
	{
		attrdata = VARDATA_SHORT(preslice);
		attrsize = VARSIZE_SHORT(preslice) - VARHDRSZ_SHORT;
	}
	else
	{
		attrdata = VARDATA(preslice);
		attrsize = VARSIZE(preslice) - VARHDRSZ;
	}

	/* slicing of datum for compressed cases and plain value */

	if (sliceoffset >= attrsize)
	{
		sliceoffset = 0;
		slicelength = 0;
	}
	else if (slicelength < 0 || slicelimit > attrsize)
		slicelength = attrsize - sliceoffset;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);
	SET_VARSIZE(result, slicelength + VARHDRSZ);

	memcpy(VARDATA(result), attrdata + sliceoffset, slicelength);

	if (preslice != attr)
		pfree(preslice);

	return result;
}

/* ----------
 * toast_fetch_datum -
 *
 *	Reconstruct an in memory Datum from the chunks saved
 *	in the toast relation
 * ----------
 */
static struct varlena *
toast_fetch_datum(struct varlena *attr)
{
	Relation	toastrel;
	struct varlena *result;
	struct varatt_external toast_pointer;
	int32		attrsize;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		elog(ERROR, "toast_fetch_datum shouldn't be called for non-ondisk datums");

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

	result = (struct varlena *) palloc(attrsize + VARHDRSZ);

	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		SET_VARSIZE_COMPRESSED(result, attrsize + VARHDRSZ);
	else
		SET_VARSIZE(result, attrsize + VARHDRSZ);

	if (attrsize == 0)
		return result;			/* Probably shouldn't happen, but just in
								 * case. */

	/*
	 * Open the toast relation and its indexes
	 */
	toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);

	/* Fetch all chunks */
	table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid,
									 attrsize, 0, attrsize, result);

	/* Close toast table */
	table_close(toastrel, AccessShareLock);

	return result;
}

/* ----------
 * toast_fetch_datum_slice -
 *
 *	Reconstruct a segment of a Datum from the chunks saved
 *	in the toast relation
 *
 *	Note that this function supports non-compressed external datums
 *	and compressed external datums (in which case the requested slice
 *	has to be a prefix, i.e. sliceoffset has to be 0).
 * ----------
 */
static struct varlena *
toast_fetch_datum_slice(struct varlena *attr, int32 sliceoffset,
						int32 slicelength)
{
	Relation	toastrel;
	struct varlena *result;
	struct varatt_external toast_pointer;
	int32		attrsize;

	if (!VARATT_IS_EXTERNAL_ONDISK(attr))
		elog(ERROR, "toast_fetch_datum_slice shouldn't be called for non-ondisk datums");

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	/*
	 * It's nonsense to fetch slices of a compressed datum unless when it's a
	 * prefix -- this isn't lo_* we can't return a compressed datum which is
	 * meaningful to toast later.
	 */
	Assert(!VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) || 0 == sliceoffset);

	attrsize = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);

	if (sliceoffset >= attrsize)
	{
		sliceoffset = 0;
		slicelength = 0;
	}

	/*
	 * When fetching a prefix of a compressed external datum, account for the
	 * space required by va_tcinfo, which is stored at the beginning as an
	 * int32 value.
	 */
	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer) && slicelength > 0)
		slicelength = slicelength + sizeof(int32);

	/*
	 * Adjust length request if needed.  (Note: our sole caller,
	 * detoast_attr_slice, protects us against sliceoffset + slicelength
	 * overflowing.)
	 */
	if (((sliceoffset + slicelength) > attrsize) || slicelength < 0)
		slicelength = attrsize - sliceoffset;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
		SET_VARSIZE_COMPRESSED(result, slicelength + VARHDRSZ);
	else
		SET_VARSIZE(result, slicelength + VARHDRSZ);

	if (slicelength == 0)
		return result;			/* Can save a lot of work at this point! */

	/* Open the toast relation */
	toastrel = table_open(toast_pointer.va_toastrelid, AccessShareLock);

	/* Fetch all chunks */
	table_relation_fetch_toast_slice(toastrel, toast_pointer.va_valueid,
									 attrsize, sliceoffset, slicelength,
									 result);

	/* Close toast table */
	table_close(toastrel, AccessShareLock);

	return result;
}

/* ----------
 * toast_decompress_datum -
 *
 * Decompress a compressed version of a varlena datum
 */
static struct varlena *
toast_decompress_datum(struct varlena *attr)
{
	ToastCompressionId cmid;

	Assert(VARATT_IS_COMPRESSED(attr));

	/*
	 * Fetch the compression method id stored in the compression header and
	 * decompress the data using the appropriate decompression routine.
	 */
	cmid = TOAST_COMPRESS_METHOD(attr);
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			return pglz_decompress_datum(attr);
		case TOAST_LZ4_COMPRESSION_ID:
			return lz4_decompress_datum(attr);
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
			return NULL;		/* keep compiler quiet */
	}
}


/* ----------
 * toast_decompress_datum_slice -
 *
 * Decompress the front of a compressed version of a varlena datum.
 * offset handling happens in detoast_attr_slice.
 * Here we just decompress a slice from the front.
 */
static struct varlena *
toast_decompress_datum_slice(struct varlena *attr, int32 slicelength)
{
	ToastCompressionId cmid;

	Assert(VARATT_IS_COMPRESSED(attr));

	/*
	 * Some callers may pass a slicelength that's more than the actual
	 * decompressed size.  If so, just decompress normally.  This avoids
	 * possibly allocating a larger-than-necessary result object, and may be
	 * faster and/or more robust as well.  Notably, some versions of liblz4
	 * have been seen to give wrong results if passed an output size that is
	 * more than the data's true decompressed size.
	 */
	if ((uint32) slicelength >= TOAST_COMPRESS_EXTSIZE(attr))
		return toast_decompress_datum(attr);

	/*
	 * Fetch the compression method id stored in the compression header and
	 * decompress the data slice using the appropriate decompression routine.
	 */
	cmid = TOAST_COMPRESS_METHOD(attr);
	switch (cmid)
	{
		case TOAST_PGLZ_COMPRESSION_ID:
			return pglz_decompress_datum_slice(attr, slicelength);
		case TOAST_LZ4_COMPRESSION_ID:
			return lz4_decompress_datum_slice(attr, slicelength);
		default:
			elog(ERROR, "invalid compression method id %d", cmid);
			return NULL;		/* keep compiler quiet */
	}
}

/* ----------
 * toast_raw_datum_size -
 *
 *	Return the raw (detoasted) size of a varlena datum
 *	(including the VARHDRSZ header)
 * ----------
 */
Size
toast_raw_datum_size(Datum value)
{
	struct varlena *attr = (struct varlena *) DatumGetPointer(value);
	Size		result;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/* va_rawsize is the size of the original datum -- including header */
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
		result = toast_pointer.va_rawsize;
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		struct varatt_indirect toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(toast_pointer.pointer));

		return toast_raw_datum_size(PointerGetDatum(toast_pointer.pointer));
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		result = EOH_get_flat_size(DatumGetEOHP(value));
	}
	else if (VARATT_IS_COMPRESSED(attr))
	{
		/* here, va_rawsize is just the payload size */
		result = VARDATA_COMPRESSED_GET_EXTSIZE(attr) + VARHDRSZ;
	}
	else if (VARATT_IS_SHORT(attr))
	{
		/*
		 * we have to normalize the header length to VARHDRSZ or else the
		 * callers of this function will be confused.
		 */
		result = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT + VARHDRSZ;
	}
	else
	{
		/* plain untoasted datum */
		result = VARSIZE(attr);
	}
	return result;
}

/* ----------
 * toast_datum_size
 *
 *	Return the physical storage size (possibly compressed) of a varlena datum
 * ----------
 */
Size
toast_datum_size(Datum value)
{
	struct varlena *attr = (struct varlena *) DatumGetPointer(value);
	Size		result;

	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		/*
		 * Attribute is stored externally - return the extsize whether
		 * compressed or not.  We do not count the size of the toast pointer
		 * ... should we?
		 */
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);
		result = VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer);
	}
	else if (VARATT_IS_EXTERNAL_INDIRECT(attr))
	{
		struct varatt_indirect toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		/* nested indirect Datums aren't allowed */
		Assert(!VARATT_IS_EXTERNAL_INDIRECT(attr));

		return toast_datum_size(PointerGetDatum(toast_pointer.pointer));
	}
	else if (VARATT_IS_EXTERNAL_EXPANDED(attr))
	{
		result = EOH_get_flat_size(DatumGetEOHP(value));
	}
	else if (VARATT_IS_SHORT(attr))
	{
		result = VARSIZE_SHORT(attr);
	}
	else
	{
		/*
		 * Attribute is stored inline either compressed or not, just calculate
		 * the size of the datum in either case.
		 */
		result = VARSIZE(attr);
	}
	return result;
}

static bool
toast_cache_init(void)
{
	HASHCTL		ctl;

	if (!enable_toast_cache)
		return false;

	/* already initialized */
	if (toastCache)
		return true;

	toast_cache_hits = 0;
	toast_cache_misses = 0;

	/* FIXME should really be per transaction */
	ToastCacheContext = AllocSetContextCreate(TopTransactionContext,
											  "TOAST cache context",
											  ALLOCSET_DEFAULT_SIZES);

	ToastSliceContext = AllocSetContextCreate(TopTransactionContext,
											  "TOAST cached slices context",
											  ALLOCSET_DEFAULT_SIZES);

	/* Make a new hash table for the cache */
	ctl.keysize = sizeof(toast_cache_key);
	ctl.entrysize = sizeof(toast_cache_entry);
	ctl.hcxt = ToastCacheContext;

	toastCache = hash_create("TOAST cache",
							 128, &ctl,
							 HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	return true;
}

static toast_cache_entry *
toast_cache_lookup(Oid toastrelid, Oid valueid)
{
	toast_cache_key		key;
	toast_cache_entry  *entry;
	bool found = false;

	/* make sure cache initialized */
	if (!toast_cache_init())
		return NULL;

	key.toastrelid = toastrelid;
	key.valueid = valueid;

	entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_FIND, &found);

	if (found)
		toast_cache_hits++;
	else
	{
		toast_cache_misses++;
		return NULL;
	}

	return entry;
}

static void
toast_cache_add_entry(Oid toastrelid, Oid valueid, struct varlena *attr)
{
	bool				found = false;
	ListCell		   *lc = NULL;
	toast_cache_key		key;
	toast_cache_entry  *entry;
	MemoryContext ctx;

	/* make sure cache initialized */
	if (!toast_cache_init())
		return;

	key.toastrelid = toastrelid;
	key.valueid = valueid;

	entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_FIND, &found);

	/* replace-only flag means that if value was not found - do not*/
	if(found)
	{
		/* if found while adding entry - we have to free old data and replace it with new one */
		/* if already have some slices - clean up, full value is stored instead of slices */
		if(entry->slices != 0)
		{
			ctx = MemoryContextSwitchTo(ToastSliceContext);

			foreach(lc, (List *) entry->slices)
			{
				toast_slice_entry *slice = (toast_slice_entry *) lfirst(lc);
				pfree(slice->data);
			}
			MemoryContextSwitchTo(ctx);
		}
	}
	else
	{
		entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_ENTER_NULL, &found);
		if(!entry)
			return;
	}

	entry->slices = (uintptr_t) NIL;

	entry->size = VARSIZE_ANY_EXHDR(attr);
	entry->data = MemoryContextAlloc(ToastCacheContext, entry->size);
	memcpy(entry->data, VARDATA_ANY(attr), entry->size);
}

static void
toast_cache_add_slice_entry(Oid toastrelid, Oid valueid, struct varlena *attr,
						 int32 sliceoffset, int32 slicelength)
{
	ListCell		   *lc = NULL;
	bool				found = false;
	toast_cache_key		key;
	toast_cache_entry  *entry;
	toast_slice_entry   *slice;

	/* make sure cache initialized */
	if (!toast_cache_init())
		return;

	key.toastrelid = toastrelid;
	key.valueid = valueid;

	entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_FIND, &found);

	if(found)
	{
		toast_cache_hits++;
	}
	else
		toast_cache_misses++;

	if(!found)
	{
		List *slices = NIL;
		slice = (toast_slice_entry *) MemoryContextAlloc(ToastSliceContext, sizeof(toast_slice_entry));
		slice->offset = sliceoffset;
		slice->length = slicelength;
		slice->data = MemoryContextAlloc(ToastSliceContext, slicelength);
		memcpy(slice->data, VARDATA_ANY(attr), slicelength);

		entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_ENTER_NULL, &found);

		slices = lappend(slices, slice);
		entry->slices = (uintptr_t) slices;
		entry->size = slicelength;
		entry->data = NULL;
		return;
	}

	found = false;

	if((List *) entry->slices != NIL)
	{
		MemoryContext ctx = MemoryContextSwitchTo(ToastSliceContext);

		foreach(lc, (List *) entry->slices)
		{
			toast_slice_entry *lookup_slice = (toast_slice_entry *) lfirst(lc);

			if(sliceoffset >= lookup_slice->offset
				&& sliceoffset <= (lookup_slice->offset + lookup_slice->length)
				&& (sliceoffset + slicelength) <= (lookup_slice->offset + lookup_slice->length))
			{
				found = true;
				break;
			}
		}
		MemoryContextSwitchTo(ctx);
	}

	if(!found)
	{
		slice = (toast_slice_entry *) MemoryContextAlloc(ToastSliceContext, sizeof(toast_slice_entry));
		slice->offset = sliceoffset;
		slice->length = slicelength;
		slice->data = MemoryContextAlloc(ToastSliceContext, slicelength);
		memcpy(slice->data, VARDATA_ANY(attr), slicelength);

		entry->slices = (uintptr_t) lappend((List *) entry->slices, slice);
		entry->size += slicelength;
	}
}

static void
toast_cache_remove_entry(Oid toastrelid, Oid valueid)
{
	ListCell 		   *lc = NULL;
	bool				found = false;
	toast_cache_key		key;
	toast_cache_entry  *entry = NULL;

	/* make sure cache initialized */
	if (!toast_cache_init())
		return;

	key.toastrelid = toastrelid;
	key.valueid = valueid;

	entry = (toast_cache_entry *) hash_search(toastCache, &key,
											  HASH_REMOVE, &found);

	if(entry)
	{
		toast_cache_hits++;
		if(entry->slices != 0)
		{
		foreach(lc, (List *) entry->slices)
		{
			toast_slice_entry *slice = (toast_slice_entry *) lfirst(lc);
			pfree(slice->data);
		}
		}
		pfree(entry->data);
	}
	else
		toast_cache_misses++;
}

static void
toast_cache_destroy(void)
{
	if (!toastCache)
		return;

	elog(LOG, "AtEOXact_ToastCache hits %ld misses %ld ratio %.2f%%",
		 toast_cache_hits, toast_cache_misses,
		 (toast_cache_hits * 100.0) / Max(1, toast_cache_hits + toast_cache_misses));

	MemoryContextDelete(ToastCacheContext);
	MemoryContextDelete(ToastSliceContext);

	toastCache = NULL;
	ToastCacheContext = NULL;
	ToastSliceContext = NULL;
}

void
AtEOXact_ToastCache(void)
{
	toast_cache_destroy();
}

struct varlena *
toast_cache_lookup_datum(Oid toastrelid, Oid valueid)
{
	toast_cache_entry *entry;
	struct varlena *result = NULL;

	entry = toast_cache_lookup(toastrelid, valueid);

	if (!entry)
		return NULL;

	if(entry->data == NULL)
		return NULL;

	result = (struct varlena *) palloc(entry->size + VARHDRSZ);
	SET_VARSIZE(result, entry->size + VARHDRSZ);

	memcpy(VARDATA(result), entry->data, entry->size);

	return result;
}

static struct varlena *
toast_cache_lookup_slice(Oid toastrelid, Oid valueid, int32 offset, int32 length)
{
	ListCell	*lc = NULL;
	toast_cache_entry *entry;
	struct varlena *result = NULL;

	/* result must be pre-alloc'ed by caller */
	entry = toast_cache_lookup(toastrelid, valueid);

	if (!entry)
		return NULL;

	if(entry->data != NULL)
	{
		result = (struct varlena *) palloc(length + VARHDRSZ);
		SET_VARSIZE(result, length + VARHDRSZ);
		memcpy(VARDATA(result), ((char *) entry->data + offset), length);
		return result;
	}

	if(entry->slices == 0 || (List *) entry->slices == NIL)
		return NULL;

	if((List *) entry->slices != NIL)
	{
		foreach(lc, (List *) entry->slices)
		{

			toast_slice_entry *slice = (toast_slice_entry *) lfirst(lc);

			if(offset >= slice->offset
				&& (offset + length) <= (slice->offset + slice->length))
			{
				result = (struct varlena *) palloc(length + VARHDRSZ);
				SET_VARSIZE(result, length + VARHDRSZ);
				memcpy(VARDATA(result), ((char *) slice->data + offset - slice->offset), length);
				break;
			}
			else
				continue;
		}
	}

	return result;
}

void
toast_cache_add_slice_datum(Oid toastrelid, Oid valueid, struct varlena *attr, int32 offset, int32 length)
{
	toast_cache_add_slice_entry(toastrelid, valueid, attr, offset, length);
}

void
toast_cache_remove_datum(Oid toastrelid, Oid valueid)
{
	toast_cache_remove_entry(toastrelid, valueid);
}

void
toast_cache_add_datum(Oid toastrelid, Oid valueid, struct varlena *attr)
{
	return toast_cache_add_entry(toastrelid, valueid, attr);
}
