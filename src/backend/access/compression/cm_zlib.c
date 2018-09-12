/*-------------------------------------------------------------------------
 *
 * cm_zlib.c
 *	  zlib compression method
 *
 * Copyright (c) 2015-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/compression/cm_zlib.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/cmapi.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#ifdef HAVE_LIBZ
#include <zlib.h>

#define ZLIB_MAX_DICTIONARY_LENGTH		32768
#define ZLIB_DICTIONARY_DELIM			(" ,")

typedef struct
{
	int				level;
	Bytef			dict[ZLIB_MAX_DICTIONARY_LENGTH];
	unsigned int	dictlen;
} zlib_state;

/*
 * Check options if specified. All validation is located here so
 * we don't need do it again in cminitstate function.
 */
static void
zlib_cmcheck(Form_pg_attribute att, List *options)
{
	ListCell	*lc;
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "level") == 0)
		{
			if (strcmp(defGetString(def), "best_speed") != 0 &&
				strcmp(defGetString(def), "best_compression") != 0 &&
				strcmp(defGetString(def), "default") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unexpected value for zlib compression level: \"%s\"", defGetString(def))));
		}
		else if (strcmp(def->defname, "dict") == 0)
		{
			int		ntokens = 0;
			char   *val,
				   *tok;

			val = pstrdup(defGetString(def));
			if (strlen(val) > (ZLIB_MAX_DICTIONARY_LENGTH - 1))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						(errmsg("zlib dictionary length should be less than %d", ZLIB_MAX_DICTIONARY_LENGTH))));

			while ((tok = strtok(val, ZLIB_DICTIONARY_DELIM)) != NULL)
			{
				ntokens++;
				val = NULL;
			}

			if (ntokens < 2)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						(errmsg("zlib dictionary is too small"))));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_PARAMETER),
					 errmsg("unexpected parameter for zlib: \"%s\"", def->defname)));
	}
}

static void *
zlib_cminitstate(Oid acoid, List *options)
{
	zlib_state		*state = NULL;

	state = palloc0(sizeof(zlib_state));
	state->level = Z_DEFAULT_COMPRESSION;

	if (list_length(options) > 0)
	{
		ListCell	*lc;

		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "level") == 0)
			{
				if (strcmp(defGetString(def), "best_speed") == 0)
					state->level = Z_BEST_SPEED;
				else if (strcmp(defGetString(def), "best_compression") == 0)
					state->level = Z_BEST_COMPRESSION;
			}
			else if (strcmp(def->defname, "dict") == 0)
			{
				char   *val,
					   *tok;

				val = pstrdup(defGetString(def));

				/* Fill the zlib dictionary */
				while ((tok = strtok(val, ZLIB_DICTIONARY_DELIM)) != NULL)
				{
					int len = strlen(tok);
					memcpy((void *) (state->dict + state->dictlen), tok, len);
					state->dictlen += len + 1;
					Assert(state->dictlen <= ZLIB_MAX_DICTIONARY_LENGTH);

					/* add space as dictionary delimiter */
					state->dict[state->dictlen - 1] = ' ';
					val = NULL;
				}
			}
		}
	}

	return state;
}

static struct varlena *
zlib_cmcompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	int32			valsize,
					len;
	struct varlena *tmp = NULL;
	z_streamp		zp;
	int				res;
	zlib_state	   *state = (zlib_state *) cmoptions->acstate;

	zp = (z_streamp) palloc(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	if (deflateInit(zp, state->level) != Z_OK)
		elog(ERROR, "could not initialize compression library: %s", zp->msg);

	if (state->dictlen > 0)
	{
		res = deflateSetDictionary(zp, state->dict, state->dictlen);
		if (res != Z_OK)
			elog(ERROR, "could not set dictionary for zlib: %s", zp->msg);
	}

	valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	tmp = (struct varlena *) palloc(valsize + VARHDRSZ_CUSTOM_COMPRESSED);
	zp->next_in = (void *) VARDATA_ANY(value);
	zp->avail_in = valsize;
	zp->avail_out = valsize;
	zp->next_out = (void *)((char *) tmp + VARHDRSZ_CUSTOM_COMPRESSED);

	do {
		res = deflate(zp, Z_FINISH);
		if (res == Z_STREAM_ERROR)
			elog(ERROR, "could not compress data: %s", zp->msg);
	} while (zp->avail_in != 0);

	Assert(res == Z_STREAM_END);

	len = valsize - zp->avail_out;
	if (deflateEnd(zp) != Z_OK)
		elog(ERROR, "could not close compression stream: %s", zp->msg);
	pfree(zp);

	if (len > 0)
	{
		SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_CUSTOM_COMPRESSED);
		return tmp;
	}

	pfree(tmp);
	return NULL;
}

static struct varlena *
zlib_cmdecompress(CompressionAmOptions *cmoptions, const struct varlena *value)
{
	struct varlena *result;
	z_streamp		zp;
	int				res = Z_OK;
	zlib_state	   *state = (zlib_state *) cmoptions->acstate;

	zp = (z_streamp) palloc(sizeof(z_stream));
	zp->zalloc = Z_NULL;
	zp->zfree = Z_NULL;
	zp->opaque = Z_NULL;

	if (inflateInit(zp) != Z_OK)
		elog(ERROR, "could not initialize compression library: %s", zp->msg);

	Assert(VARATT_IS_CUSTOM_COMPRESSED(value));
	zp->next_in = (void *) ((char *) value + VARHDRSZ_CUSTOM_COMPRESSED);
	zp->avail_in = VARSIZE(value) - VARHDRSZ_CUSTOM_COMPRESSED;
	zp->avail_out = VARRAWSIZE_4B_C(value);

	result = (struct varlena *) palloc(zp->avail_out + VARHDRSZ);
	SET_VARSIZE(result, zp->avail_out + VARHDRSZ);
	zp->next_out = (void *) VARDATA(result);

	while (zp->avail_in > 0)
	{
		res = inflate(zp, 0);
		if (res == Z_NEED_DICT && state->dictlen > 0)
		{
			res = inflateSetDictionary(zp, state->dict, state->dictlen);
			if (res != Z_OK)
				elog(ERROR, "could not set dictionary for zlib");
			continue;
		}
		if (!(res == Z_OK || res == Z_STREAM_END))
			elog(ERROR, "could not uncompress data: %s", zp->msg);
	}

	if (inflateEnd(zp) != Z_OK)
		elog(ERROR, "could not close compression library: %s", zp->msg);

	pfree(zp);
	return result;
}
#endif

Datum
zlibhandler(PG_FUNCTION_ARGS)
{
#ifndef HAVE_LIBZ
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not built with zlib support")));
#else
	CompressionAmRoutine *routine = makeNode(CompressionAmRoutine);

	routine->cmcheck = zlib_cmcheck;
	routine->cminitstate = zlib_cminitstate;
	routine->cmcompress = zlib_cmcompress;
	routine->cmdecompress = zlib_cmdecompress;

	PG_RETURN_POINTER(routine);
#endif
}
