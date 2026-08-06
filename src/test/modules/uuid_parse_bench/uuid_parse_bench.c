/*--------------------------------------------------------------------------
 *
 * uuid_parse_bench.c
 *		Microbenchmark for the UUID input parsing strategies.
 *
 * This module times several ways of turning UUID text into a pg_uuid_t, so
 * that the trade-offs can be argued with measurements rather than intuition.
 * The paths it measures are:
 *
 *	scalar		the character-at-a-time loop using isxdigit() and strtoul()
 *	simd		shape detection + compaction + hex_decode_safe()
 *	nosimd		the same, but forced through the scalar hex decoder
 *
 * "simd" and "nosimd" differ only in which hex decoder they call, which is
 * why this module carries its own copy of the scalar decoder: the real
 * hex_decode_safe() picks its implementation at compile time, so a single
 * backend binary can otherwise only measure one of the two.
 *
 * Copyright (c) 2007-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/uuid_parse_bench/uuid_parse_bench.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "catalog/pg_type_d.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "portability/instr_time.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/uuid.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(uuid_parse_bench);

/* number of hex digits in a UUID body */
#define UUID_HEX_LEN	(UUID_LEN * 2)
/* length of the canonical 8x-4x-4x-4x-12x form */
#define UUID_CANON_LEN	(UUID_HEX_LEN + 4)

/*
 * The parsing strategies we time.  Keep in sync with path_names[].
 */
typedef enum BenchPath
{
	PATH_SCALAR = 0,
	PATH_SIMD,
	PATH_NOSIMD,
	NUM_PATHS,
}			BenchPath;

static const char *const path_names[] = {
	"scalar", "simd", "nosimd",
};

/*
 * The input shapes we time.  The fast path's advantage depends strongly on
 * the shape, so this has to be an axis of the measurement rather than a
 * single representative case.  dashed4 and invalid_hex are the shapes that
 * fall back to the scalar parser, and so measure the cost of the fallback.
 */
typedef enum BenchShape
{
	SHAPE_CANONICAL = 0,		/* 8x-4x-4x-4x-12x, what uuid_out() emits */
	SHAPE_BARE32,				/* 32 contiguous hex digits */
	SHAPE_BRACED_CANONICAL,		/* {8x-4x-4x-4x-12x} */
	SHAPE_BRACED_BARE32,		/* {32 hex digits} */
	SHAPE_DASHED4,				/* dash after every group of 4: falls back */
	SHAPE_INVALID_HEX,			/* canonical but with a bad digit: falls back */
	NUM_SHAPES,
}			BenchShape;

static const char *const shape_names[] = {
	"canonical", "bare32", "braced_canonical", "braced_bare32",
	"dashed4", "invalid_hex",
};

/*
 * Consume parse results so the compiler cannot delete the work we are timing.
 */
static volatile uint64 bench_sink = 0;

/*
 * Copy of string_to_uuid_scalar() from src/backend/utils/adt/uuid.c.
 */
static bool
string_to_uuid_scalar(const char *source, pg_uuid_t *uuid)
{
	const char *src = source;
	bool		braces = false;
	int			i;

	if (src[0] == '{')
	{
		src++;
		braces = true;
	}

	for (i = 0; i < UUID_LEN; i++)
	{
		char		str_buf[3];

		if (src[0] == '\0' || src[1] == '\0')
			return false;
		memcpy(str_buf, src, 2);
		if (!isxdigit((unsigned char) str_buf[0]) ||
			!isxdigit((unsigned char) str_buf[1]))
			return false;

		str_buf[2] = '\0';
		uuid->data[i] = (unsigned char) strtoul(str_buf, NULL, 16);
		src += 2;
		if (src[0] == '-' && (i % 2) == 1 && i < UUID_LEN - 1)
			src++;
	}

	if (braces)
	{
		if (*src != '}')
			return false;
		src++;
	}

	return *src == '\0';
}

/*
 * hexlookup[], get_hex() and hex_decode_safe_scalar() below are copies of the
 * versions in src/backend/utils/adt/encode.c.  They are static there, and in
 * any case the exported hex_decode_safe() resolves to either the vectorized
 * or the scalar implementation at compile time, so a backend built for a SIMD
 * platform cannot otherwise measure what a non-SIMD platform would do.
 *
 * Being copies, these can drift from the originals.  Re-check them against
 * encode.c before quoting any figure.
 */
static const int8 bench_hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static inline bool
bench_get_hex(const char *cp, char *out)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = bench_hexlookup[c];

	*out = (char) res;

	return (res >= 0);
}

/*
 * Copy of hex_decode_safe_scalar() in encode.c.
 */
static inline uint64
bench_hex_decode_scalar(const char *src, size_t len, char *dst, bool *ok)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	*ok = true;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		if (!bench_get_hex(s, &v1))
		{
			*ok = false;
			return p - dst;
		}
		s++;
		if (s >= srcend)
		{
			*ok = false;
			return p - dst;
		}
		if (!bench_get_hex(s, &v2))
		{
			*ok = false;
			return p - dst;
		}
		s++;
		*p++ = (v1 << 4) | v2;
	}

	return p - dst;
}

/* ----------------------------------------------------------------
 * Input classification, shared by the fast paths
 * ----------------------------------------------------------------
 */

/*
 * How a given input can be decoded.  Note these are not BenchShape values:
 * they say which fast path applies, not which shape the corpus was built in.
 */
#define FASTPATH_NONE		0	/* no fast path applies; hand off to scalar */
#define FASTPATH_CONTIGUOUS	1	/* 32 hex digits, decode in place */
#define FASTPATH_COMPACT	2	/* canonical, must have its dashes removed */

/*
 * Decide which fast path applies to source and, when one does, hand back the
 * start of the body.  This is exactly the work string_to_uuid() does before
 * it decodes anything.
 */
static inline int
classify_input(const char *source, const char **bodyp)
{
	const char *body = source;
	size_t		len = strlen(source);

	/* Strip one optional surrounding brace pair */
	if (len >= 2 && source[0] == '{' && source[len - 1] == '}')
	{
		body = source + 1;
		len -= 2;
	}

	*bodyp = body;

	if (len == UUID_HEX_LEN)
		return FASTPATH_CONTIGUOUS;
	if (len == UUID_CANON_LEN && body[8] == '-' && body[13] == '-' &&
		body[18] == '-' && body[23] == '-')
		return FASTPATH_COMPACT;

	return FASTPATH_NONE;
}

/*
 * Compact the canonical form into 32 contiguous hex digits.
 */
static inline void
compact_canonical(const char *body, char *hexbuf)
{
	memcpy(&hexbuf[0], &body[0], 8);
	memcpy(&hexbuf[8], &body[9], 4);
	memcpy(&hexbuf[12], &body[14], 4);
	memcpy(&hexbuf[16], &body[19], 4);
	memcpy(&hexbuf[20], &body[24], 12);
}

/*
 * Copy of hex_decode_safe() with SIMD
 */
static bool
hex_decode_safe_simd(const char *source, pg_uuid_t *uuid)
{
	const char *body;
	const char *hexsrc;
	char		hexbuf[UUID_HEX_LEN];
	uint64		written;
	ErrorSaveContext esctx = {T_ErrorSaveContext};
	int			fastpath = classify_input(source, &body);

	if (fastpath == FASTPATH_NONE)
		return string_to_uuid_scalar(source, uuid);

	if (fastpath == FASTPATH_COMPACT)
	{
		compact_canonical(body, hexbuf);
		hexsrc = hexbuf;
	}
	else
		hexsrc = body;

	written = hex_decode_safe(hexsrc, UUID_HEX_LEN, (char *) uuid->data,
							  (Node *) &esctx);

	if (esctx.error_occurred || written != UUID_LEN)
		return string_to_uuid_scalar(source, uuid);

	return true;
}

/*
 * Simulating hex_decode_safe() with USE_NO_SIMD
 */
static bool
hex_decode_safe_nosimd(const char *source, pg_uuid_t *uuid)
{
	const char *body;
	const char *hexsrc;
	char		hexbuf[UUID_HEX_LEN];
	uint64		written;
	bool		ok;
	int			fastpath = classify_input(source, &body);

	if (fastpath == FASTPATH_NONE)
		return string_to_uuid_scalar(source, uuid);

	if (fastpath == FASTPATH_COMPACT)
	{
		compact_canonical(body, hexbuf);
		hexsrc = hexbuf;
	}
	else
		hexsrc = body;

	written = bench_hex_decode_scalar(hexsrc, UUID_HEX_LEN,
									  (char *) uuid->data, &ok);

	if (!ok || written != UUID_LEN)
		return string_to_uuid_scalar(source, uuid);

	return true;
}

/*
 * Render one random UUID in the requested shape.  The caller owns the buffer,
 * which must hold at least 48 bytes.
 */
static void
render_shape(pg_prng_state *state, BenchShape shape, char *buf)
{
	unsigned char b[UUID_LEN];
	static const char hex[] = "0123456789abcdef";
	char		hexdigits[UUID_HEX_LEN];
	char	   *p = buf;
	int			i;

	for (i = 0; i < UUID_LEN; i++)
		b[i] = (unsigned char) pg_prng_uint32(state);
	for (i = 0; i < UUID_LEN; i++)
	{
		hexdigits[i * 2] = hex[b[i] >> 4];
		hexdigits[i * 2 + 1] = hex[b[i] & 0x0f];
	}

	if (shape == SHAPE_BRACED_CANONICAL || shape == SHAPE_BRACED_BARE32)
		*p++ = '{';

	switch (shape)
	{
		case SHAPE_BARE32:
		case SHAPE_BRACED_BARE32:
			memcpy(p, hexdigits, UUID_HEX_LEN);
			p += UUID_HEX_LEN;
			break;

		case SHAPE_DASHED4:
			/* a dash after every group of 4: legal, but not the fast path */
			for (i = 0; i < UUID_HEX_LEN; i++)
			{
				if (i > 0 && i % 4 == 0)
					*p++ = '-';
				*p++ = hexdigits[i];
			}
			break;

		default:
			/* canonical 8x-4x-4x-4x-12x, optionally with a bad digit */
			for (i = 0; i < UUID_HEX_LEN; i++)
			{
				if (i == 8 || i == 12 || i == 16 || i == 20)
					*p++ = '-';
				*p++ = hexdigits[i];
			}
			/* corrupt one digit so the fast path has to hand off */
			if (shape == SHAPE_INVALID_HEX)
				buf[1] = 'z';
			break;
	}

	if (shape == SHAPE_BRACED_CANONICAL || shape == SHAPE_BRACED_BARE32)
		*p++ = '}';
	*p = '\0';
}

/*
 * Build nuuids input strings of the given shape in the current memory
 * context.  Everything is materialized before timing starts so that neither
 * the PRNG nor the allocator shows up in the measurement.
 */
static char **
build_corpus(BenchShape shape, int nuuids, uint64 seed)
{
	pg_prng_state state;
	char	  **inputs = palloc(sizeof(char *) * nuuids);
	int			i;

	pg_prng_seed(&state, seed);
	for (i = 0; i < nuuids; i++)
	{
		char		buf[48];

		render_shape(&state, shape, buf);
		inputs[i] = pstrdup(buf);
	}

	return inputs;
}

/* ----------------------------------------------------------------
 * Timing
 * ----------------------------------------------------------------
 */

/*
 * Run one path over one corpus nloops times and return the best wall time in
 * milliseconds.  The best of several runs is reported rather than the mean
 * because it is the least contaminated by scheduling noise.
 */
/*
 * The inner loop, instantiated once per path.  Calling the parser directly
 * rather than through parse_funcs[] matters: an indirect call would add the
 * same few cycles to every path and so shrink exactly the differences this
 * module exists to measure.  uuid_in() calls string_to_uuid() directly, so
 * direct calls are also the more faithful model.
 */
#define TIME_ONE_LOOP(fn) \
	do { \
		for (i = 0; i < nuuids; i++) \
		{ \
			pg_uuid_t	u; \
			if (fn(inputs[i], &u)) \
				local += u.data[0]; \
			else \
				local++; \
		} \
	} while (0)

static double
time_path(BenchPath path, char **inputs, int nuuids, int nloops)
{
	double		best = -1.0;
	int			loop;

	for (loop = 0; loop < nloops; loop++)
	{
		instr_time	start,
					stop;
		uint64		local = 0;
		double		ms;
		int			i;

		CHECK_FOR_INTERRUPTS();

		INSTR_TIME_SET_CURRENT(start);
		switch (path)
		{
			case PATH_SCALAR:
				TIME_ONE_LOOP(string_to_uuid_scalar);
				break;
			case PATH_SIMD:
				TIME_ONE_LOOP(hex_decode_safe_simd);
				break;
			case PATH_NOSIMD:
				TIME_ONE_LOOP(hex_decode_safe_nosimd);
				break;
			case NUM_PATHS:
				elog(ERROR, "unexpected path %d", (int) path);
				break;
		}
		INSTR_TIME_SET_CURRENT(stop);
		INSTR_TIME_SUBTRACT(stop, start);

		bench_sink += local;

		ms = INSTR_TIME_GET_DOUBLE(stop) * 1000.0;
		if (best < 0.0 || ms < best)
			best = ms;
	}

	return best;
}

/* ----------------------------------------------------------------
 * SQL interface
 * ----------------------------------------------------------------
 */

/*
 * Turn a text[] filter into a bool[] over names, or select everything when
 * the array is NULL.
 */
static void
parse_filter(ArrayType *arr, const char *const *names, int nnames,
			 bool *selected, const char *what)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	if (arr == NULL)
	{
		for (i = 0; i < nnames; i++)
			selected[i] = true;
		return;
	}

	for (i = 0; i < nnames; i++)
		selected[i] = false;

	deconstruct_array_builtin(arr, TEXTOID, &elems, &nulls, &nelems);
	for (i = 0; i < nelems; i++)
	{
		char	   *name;
		int			j;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("%s array must not contain nulls", what)));

		name = TextDatumGetCString(elems[i]);
		for (j = 0; j < nnames; j++)
		{
			if (strcmp(name, names[j]) == 0)
			{
				selected[j] = true;
				break;
			}
		}
		if (j == nnames)
		{
			StringInfoData buf;

			initStringInfo(&buf);
			for (j = 0; j < nnames; j++)
				appendStringInfo(&buf, "%s%s", j > 0 ? ", " : "", names[j]);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized %s \"%s\"", what, name),
					 errhint("Valid values are: %s.", buf.data)));
		}
	}
}

/*
 * uuid_parse_bench(nuuids, nloops, paths, shapes)
 *
 * Time every selected path against every selected shape and return the
 * matrix.
 */
Datum
uuid_parse_bench(PG_FUNCTION_ARGS)
{
	int			nuuids = PG_GETARG_INT32(0);
	int			nloops = PG_GETARG_INT32(1);
	ArrayType  *patharr = PG_ARGISNULL(2) ? NULL : PG_GETARG_ARRAYTYPE_P(2);
	ArrayType  *shapearr = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
	bool		want_path[NUM_PATHS];
	bool		want_shape[NUM_SHAPES];
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			shape;

	if (nuuids <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("nuuids must be positive")));
	if (nloops <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("nloops must be positive")));

	parse_filter(patharr, path_names, NUM_PATHS, want_path, "path");
	parse_filter(shapearr, shape_names, NUM_SHAPES, want_shape, "shape");

	InitMaterializedSRF(fcinfo, 0);

	for (shape = 0; shape < NUM_SHAPES; shape++)
	{
		MemoryContext corpusctx,
					oldctx;
		char	  **inputs;
		int			path;

		if (!want_shape[shape])
			continue;

		/*
		 * Build each corpus in its own context so that the strings for one
		 * shape are freed before the next is built.  With a large nuuids the
		 * whole matrix would otherwise be resident at once.
		 */
		corpusctx = AllocSetContextCreate(CurrentMemoryContext,
										  "uuid_parse_bench corpus",
										  ALLOCSET_DEFAULT_SIZES);
		oldctx = MemoryContextSwitchTo(corpusctx);
		inputs = build_corpus(shape, nuuids, 42);
		MemoryContextSwitchTo(oldctx);

		for (path = 0; path < NUM_PATHS; path++)
		{
			Datum		values[5];
			bool		nulls[5] = {0};
			double		best_ms;

			if (!want_path[path])
				continue;

			best_ms = time_path(path, inputs, nuuids, nloops);

			values[0] = CStringGetTextDatum(path_names[path]);
			values[1] = CStringGetTextDatum(shape_names[shape]);
			values[2] = Int32GetDatum(nuuids);
			values[3] = Float8GetDatum(best_ms);
			values[4] = Float8GetDatum(best_ms * 1000000.0 / nuuids);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}

		MemoryContextDelete(corpusctx);
	}

	return (Datum) 0;
}
