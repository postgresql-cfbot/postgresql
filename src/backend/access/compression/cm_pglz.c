/*-------------------------------------------------------------------------
 *
 * cm_pglz.c
 *	  pglz compression method
 *
 * Copyright (c) 2015-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cm_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/cmapi.h"
#include "commands/defrem.h"
#include "common/pg_lzcompress.h"
#include "nodes/parsenodes.h"
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
			 errmsg("unexpected parameter for pglz: \"%s\"", name)));
}

/*
 * Check PGLZ options if specified
 */
static void
pglz_cmcheck(Form_pg_attribute att, List *options)
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
pglz_cminitstate(Oid acoid, List *options)
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

static struct varlena *
pglz_cmcompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;
	PGLZ_Strategy *strategy;

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	strategy = (PGLZ_Strategy *) cmoptions->acstate;

	Assert(strategy != NULL);
	if (valsize < strategy->min_input_size ||
		valsize > strategy->max_input_size)
		return NULL;

	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									VARHDRSZ_CUSTOM_COMPRESSED);
	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_CUSTOM_COMPRESSED,
						strategy);

	if (len >= 0)
	{
		SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_CUSTOM_COMPRESSED);
		return tmp;
	}

	pfree(tmp);
	return NULL;
}

static struct varlena *
pglz_cmdecompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	struct varlena *result;
	int32		resultlen;

	Assert(VARATT_IS_CUSTOM_COMPRESSED(value));
	resultlen = VARRAWSIZE_4B_C(value) + VARHDRSZ;
	result = (struct varlena *) palloc(resultlen);

	SET_VARSIZE(result, resultlen);
	if (pglz_decompress((char *) value + VARHDRSZ_CUSTOM_COMPRESSED,
						VARSIZE(value) - VARHDRSZ_CUSTOM_COMPRESSED,
						VARDATA(result),
						VARRAWSIZE_4B_C(value)) < 0)
		elog(ERROR, "pglz: compressed data is corrupted");

	return result;
}

/* pglz is the default compression method */
Datum
pglzhandler(PG_FUNCTION_ARGS)
{
	CompressionAmRoutine *routine = makeNode(CompressionAmRoutine);

	routine->cmcheck = pglz_cmcheck;
	routine->cminitstate = pglz_cminitstate;
	routine->cmcompress = pglz_cmcompress;
	routine->cmdecompress = pglz_cmdecompress;

	PG_RETURN_POINTER(routine);
}
