/*-------------------------------------------------------------------------
 *
 * compress_pglz.c
 *	  pglz compression method
 *
 * Copyright (c) 2015-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/compress_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/compressamapi.h"
#include "access/toast_internals.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"

#include "fmgr.h"
#include "utils/builtins.h"

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
static void
pglz_cmcheck(List *options)
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
static void *
pglz_cminitstate(List *options)
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
	return (void *) strategy;
}

/*
 * pglz_cmcompress - compression routine for pglz compression method
 *
 * Compresses source into dest using the default strategy. Returns the
 * compressed varlena, or NULL if compression fails.
 */
static struct varlena *
pglz_cmcompress(const struct varlena *value, int32 header_size, void *options)
{
	int32		valsize,
				len;
	struct varlena  *tmp = NULL;
	PGLZ_Strategy   *strategy;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	strategy = (PGLZ_Strategy *) options;

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < strategy->min_input_size ||
		valsize > strategy->max_input_size)
		return NULL;

	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									header_size);

	len = pglz_compress(VARDATA_ANY(DatumGetPointer(value)),
						valsize,
						(char *) tmp + header_size,
						strategy);

	if (len >= 0)
	{
		SET_VARSIZE_COMPRESSED(tmp, len + header_size);
		return tmp;
	}

	pfree(tmp);

	return NULL;
}

/*
 * pglz_cmdecompress - decompression routine for pglz compression method
 *
 * Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress(const struct varlena *value, int32 header_size)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);
	SET_VARSIZE(result, TOAST_COMPRESS_RAWSIZE(value) + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + header_size,
							  VARSIZE(value) - header_size,
							  VARDATA(result),
							  TOAST_COMPRESS_RAWSIZE(value), true);

	if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * pglz_decompress - slice decompression routine for pglz compression method
 *
 * Decompresses part of the data. Returns the decompressed varlena.
 */
static struct varlena *
pglz_cmdecompress_slice(const struct varlena *value, int32 header_size,
						int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	rawsize = pglz_decompress((char *) value + header_size,
							  VARSIZE(value) - header_size,
							  VARDATA(result),
							  slicelength, false);

	if (rawsize < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

const CompressionAmRoutine pglz_compress_methods = {
	.type = T_CompressionAmRoutine,
	.datum_check = pglz_cmcheck,
	.datum_initstate = pglz_cminitstate,
	.datum_compress = pglz_cmcompress,
	.datum_decompress = pglz_cmdecompress,
	.datum_decompress_slice = pglz_cmdecompress_slice};

/* pglz compression handler function */
Datum
pglzhandler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&pglz_compress_methods);
}
