/*-------------------------------------------------------------------------
 *
 * toast_compression.c
 *	  Functions for toast compression.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_compression.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"
#include "fmgr.h"
#include "utils/builtins.h"

/* GUC */
int			default_toast_compression = TOAST_PGLZ_COMPRESSION;

#define NO_LZ4_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method lz4 not supported"), \
			 errdetail("This functionality requires the server to be built with lz4 support."), \
			 errhint("You need to rebuild PostgreSQL using %s.", "--with-lz4")))

#define PGLZ_OPTIONS_COUNT 6

static char *PGLZ_options[PGLZ_OPTIONS_COUNT] = {
	"min_input_size",
	"max_input_size",
	"min_comp_rate",
	"first_success_by",
	"match_size_good",
	"match_size_drop"
};

/*
 * Convert value from reloptions to int32, and report if it is not correct.
 * Also checks parameter names
 */
static int32
parse_option(char *name, char *value)
{
	int			i;

	for (i = 0; i < PGLZ_OPTIONS_COUNT; i++)
	{
		if (strcmp(PGLZ_options[i], name) == 0)
			return pg_atoi(value, 4, 0);
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_PARAMETER),
			 errmsg("unknown compression option for pglz: \"%s\"", name)));
}

/*
 * Check PGLZ options if specified
 */
void
pglz_check_options(List *options)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		parse_option(def->defname, defGetString(def));
	}
}

/*
 * Configure PGLZ_Strategy struct for compression function
 */
static inline PGLZ_Strategy *
pglz_init_options(List *options)
{
	ListCell   *lc;
	PGLZ_Strategy *strategy = palloc(sizeof(PGLZ_Strategy));

	/* initialize with default strategy values */
	memcpy(strategy, PGLZ_strategy_default, sizeof(PGLZ_Strategy));
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		int32		val = parse_option(def->defname, defGetString(def));

		/* fill the strategy */
		if (strcmp(def->defname, "min_input_size") == 0)
			strategy->min_input_size = val;
		else if (strcmp(def->defname, "max_input_size") == 0)
			strategy->max_input_size = val;
		else if (strcmp(def->defname, "min_comp_rate") == 0)
			strategy->min_comp_rate = val;
		else if (strcmp(def->defname, "first_success_by") == 0)
			strategy->first_success_by = val;
		else if (strcmp(def->defname, "match_size_good") == 0)
			strategy->match_size_good = val;
		else if (strcmp(def->defname, "match_size_drop") == 0)
			strategy->match_size_drop = val;
	}
	return strategy;
}

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
pglz_compress_datum(const struct varlena *value, List *options)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;
	PGLZ_Strategy  *strategy;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	strategy = pglz_init_options(options);

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < strategy->min_input_size ||
		valsize > strategy->max_input_size)
		return NULL;

	/*
	 * Figure out the maximum possible size of the pglz output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									VARHDRSZ_COMPRESSED);

	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_COMPRESSED,
						strategy);
	if (options != NULL)
		pfree(strategy);

	if (len < 0)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
}

/*
 * Decompress a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum(const struct varlena *value)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  VARDATA_COMPRESSED_GET_EXTSIZE(value), true);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Decompress part of a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum_slice(const struct varlena *value,
							int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  slicelength, false);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Check options if specified. All validation is located here so
 * we don't need to do it again in cminitstate function.
 */
void
lz4_check_options(List *options)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	ListCell	*lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "acceleration") == 0)
		{
			int32 acceleration =
				pg_atoi(defGetString(def), sizeof(acceleration), 0);

			if (acceleration < INT32_MIN || acceleration > INT32_MAX)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unexpected value for lz4 compression acceleration: \"%s\"",
								defGetString(def)),
					 errhint("expected value between INT32_MIN and INT32_MAX")
					));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PARAMETER),
					 errmsg("unknown compression option for lz4: \"%s\"", def->defname)));
	}
#endif
}

static int32
lz4_init_options(List *options)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32	acceleration;

	acceleration = 1;
	if (list_length(options) > 0)
	{
		ListCell	*lc;

		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "acceleration") == 0)
				acceleration = pg_atoi(defGetString(def), sizeof(int32), 0);
		}
	}

	return acceleration;
#endif
}

/*
 * Compress a varlena using LZ4.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
lz4_compress_datum(const struct varlena *value, List *options)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	int32		acceleration;
	struct varlena *tmp = NULL;

	acceleration = lz4_init_options(options);

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the LZ4 output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + VARHDRSZ_COMPRESSED);

	len = LZ4_compress_fast(VARDATA_ANY(value),
							(char *) tmp + VARHDRSZ_COMPRESSED,
							valsize, max_size, acceleration);
	if (len <= 0)
		elog(ERROR, "lz4 compression failed");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum(const struct varlena *value)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESSED,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESSED,
								  VARDATA_COMPRESSED_GET_EXTSIZE(value));
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));


	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum_slice(const struct varlena *value, int32 slicelength)
{
#ifndef USE_LZ4
	NO_LZ4_SUPPORT();
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	struct varlena *result;

	/* slice decompression not supported prior to 1.8.3 */
	if (LZ4_versionNumber() < 10803)
		return lz4_decompress_datum(value);

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe_partial((char *) value + VARHDRSZ_COMPRESSED,
										  VARDATA(result),
										  VARSIZE(value) - VARHDRSZ_COMPRESSED,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Extract compression ID from a varlena.
 *
 * Returns TOAST_INVALID_COMPRESSION_ID if the varlena is not compressed.
 */
ToastCompressionId
toast_get_compression_id(struct varlena *attr)
{
	ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

	/*
	 * If it is stored externally then fetch the compression method id from
	 * the external toast pointer.  If compressed inline, fetch it from the
	 * toast compression header.
	 */
	if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
	}
	else if (VARATT_IS_COMPRESSED(attr))
		cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr);

	return cmid;
}

/*
 * CompressionNameToMethod - Get compression method from compression name
 *
 * Search in the available built-in methods.  If the compression not found
 * in the built-in methods then return InvalidCompressionMethod.
 */
char
CompressionNameToMethod(const char *compression)
{
	if (strcmp(compression, "pglz") == 0)
		return TOAST_PGLZ_COMPRESSION;
	else if (strcmp(compression, "lz4") == 0)
	{
#ifndef USE_LZ4
		NO_LZ4_SUPPORT();
#endif
		return TOAST_LZ4_COMPRESSION;
	}

	return InvalidCompressionMethod;
}

/*
 * GetCompressionMethodName - Get compression method name
 */
const char *
GetCompressionMethodName(char method)
{
	switch (method)
	{
		case TOAST_PGLZ_COMPRESSION:
			return "pglz";
		case TOAST_LZ4_COMPRESSION:
			return "lz4";
		default:
			elog(ERROR, "invalid compression method %c", method);
			return NULL;		/* keep compiler quiet */
	}
}
