/*
 * contrib/decode/decode.c
 */
#include "postgres.h"

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "nodes/supportnodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(decode_support);
PG_FUNCTION_INFO_V1(decode);

static void decode_detect_types(int nargs, Oid *argtypes, Oid *search_oid, Oid *result_oid);
static Oid select_common_type_from_vector(int nargs, Oid *typeids, bool noerror);

typedef struct decode_cache
{
	Oid			rettype;
	int			nargs;
	Oid		   *argtypes;
	Oid		   *target_types;
	Oid			search_typid;
	Oid			result_typid;
	CoercionPathType *ctype;
	FmgrInfo   *cast_finfo;
	FmgrInfo   *input_finfo;
	Oid		   *typioparam;
	FmgrInfo	eqop_finfo;
} decode_cache;

static void
free_cache(decode_cache *cache)
{
	pfree(cache->argtypes);
	pfree(cache->target_types);
	pfree(cache->ctype);
	pfree(cache->cast_finfo);
	pfree(cache->input_finfo);
	pfree(cache->typioparam);
	pfree(cache);
}

/*
 * prepare persistent cache used for fast internal parameter casting
 */
static decode_cache *
build_cache(int nargs,
			Oid *argtypes,
			MemoryContext target_ctx)
{
	Oid		search_typid;
	Oid		result_typid;
	Oid			eqop;
	decode_cache *cache;
	MemoryContext		oldctx;
	int		i;

	oldctx = MemoryContextSwitchTo(target_ctx);

	cache = palloc(sizeof(decode_cache));

	cache->argtypes = palloc(nargs * sizeof(Oid));
	cache->target_types = palloc(nargs * sizeof(Oid));
	cache->ctype = palloc(nargs * sizeof(CoercionPathType));
	cache->cast_finfo = palloc(nargs * sizeof(FmgrInfo));
	cache->input_finfo = palloc(nargs * sizeof(FmgrInfo));
	cache->typioparam = palloc(nargs * sizeof(Oid));

	MemoryContextSwitchTo(oldctx);

	decode_detect_types(nargs, argtypes, &search_typid, &result_typid);

	cache->search_typid = search_typid;
	cache->result_typid = result_typid;

	for (i = 0; i < nargs; i++)
	{
		Oid		src_typid;
		Oid		target_typid;

		src_typid = cache->argtypes[i] = argtypes[i];

		if (i == 0)
			target_typid = search_typid;
		else if (i % 2) /* even position */
		{
			if (i + 1 < nargs)
				target_typid = search_typid;
			else
				/* last even argument is a default value */
				target_typid = result_typid;
		}
		else /* odd position */
			target_typid = result_typid;

		cache->target_types[i] = target_typid;

		/* prepare cast if it is necessary */
		if (src_typid != target_typid)
		{
			Oid		funcid;

			cache->ctype[i] = find_coercion_pathway(target_typid, src_typid,
										COERCION_ASSIGNMENT, &funcid);
			if (cache->ctype[i] == COERCION_PATH_NONE)
				/* A previously detected cast is not available now */
				elog(ERROR, "could not find cast from %u to %u",
					 src_typid, target_typid);

			if (cache->ctype[i] != COERCION_PATH_RELABELTYPE)
			{
				if (cache->ctype[i] == COERCION_PATH_FUNC)
				{
					fmgr_info(funcid, &cache->cast_finfo[i]);
				}
				else
				{
					Oid		outfuncoid;
					Oid		infunc;
					bool	typisvarlena;

					getTypeOutputInfo(src_typid, &outfuncoid, &typisvarlena);
					fmgr_info(outfuncoid, &cache->cast_finfo[i]);

					getTypeInputInfo(target_typid, &infunc, &cache->typioparam[i]);
					fmgr_info(infunc, &cache->input_finfo[i]);
				}
			}
		}
	}

	get_sort_group_operators(search_typid, false, true, false, NULL, &eqop, NULL, NULL);
	fmgr_info(get_opcode(eqop), &cache->eqop_finfo);

	return cache;
}

/*
 * Returns converted value into target type
 */
static Datum
decode_cast(decode_cache *cache, int argn, Datum value)
{
	Datum result;

	if (cache->argtypes[argn] != cache->target_types[argn])
	{
		if (cache->ctype[argn] == COERCION_PATH_RELABELTYPE)
			result = value;
		else if (cache->ctype[argn] == COERCION_PATH_FUNC)
			result = FunctionCall1(&cache->cast_finfo[argn], value);
		else
		{
			char	*str;

			str = OutputFunctionCall(&cache->cast_finfo[argn], value);
			result = InputFunctionCall(&cache->input_finfo[argn],
									   str,
									   cache->typioparam[argn],
									   -1);
		}
	}
	else
		result = value;

	return result;
}

/*
 * Returns true, if cache can be used again
 */
static bool
is_valid_cache(int nargs, Oid *argtypes, decode_cache *cache)
{
	if (cache)
	{
		if (nargs == cache->nargs)
		{
			int		i;

			for (i = 0; i < nargs; i++)
				if (argtypes[i] != cache->argtypes[i])
					return false;

			return true;
		}
	}

	return false;
}

static void
decode_detect_types(int nargs,
					Oid *argtypes,
					Oid *search_oid,	/* result */
					Oid *result_oid)	/* result */
{
	Oid		search_typids[FUNC_MAX_ARGS];
	Oid		result_typids[FUNC_MAX_ARGS];
	int		search_nargs = 0;
	int		result_nargs = 0;

	Assert(nargs >= 3);

	*search_oid = argtypes[1] != UNKNOWNOID ? argtypes[1] : TEXTOID;
	*result_oid = argtypes[2] != UNKNOWNOID ? argtypes[2] : TEXTOID;

	/* Search most common type if target type is not a text */
	if (*search_oid != TEXTOID || *result_oid != TEXTOID)
	{
		int		i;

		for (i = 0; i < nargs; i++)
		{
			if (i == 0)
				search_typids[search_nargs++] = argtypes[0];
			else if (i % 2) /* even position */
			{
				if (i + 1 < nargs)
					search_typids[search_nargs++] = argtypes[i];
				else
					result_typids[result_nargs++] = argtypes[i];
			}
			else /* odd position */
				result_typids[result_nargs++] = argtypes[i];
		}

		if (*search_oid != TEXTOID)
		{
			*search_oid = select_common_type_from_vector(search_nargs,
														 search_typids,
														 true);

			if (!OidIsValid(*search_oid)) /* should not to be */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot to detect common type for search expression")));
		}

		if (*result_oid != TEXTOID)
		{
			*result_oid = select_common_type_from_vector(result_nargs,
														 result_typids,
														 true);

			if (!OidIsValid(*result_oid)) /* should not to be */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot to detect common type for result expression")));
		}
	}
}


Datum
decode_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node	   *ret = NULL;

	if (IsA(rawreq, SupportRequestRettype))
	{
		SupportRequestRettype *req = (SupportRequestRettype *) rawreq;
		Oid		search_oid;
		Oid		result_oid;

		if (req->nargs < 3)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("too few function arguments"),
					 errhint("The decode function requires at least 3 arguments")));

		decode_detect_types(req->nargs, req->actual_arg_types, &search_oid, &result_oid);

		req->rettype = result_oid;

		ret = (Node *) req;
	}

	PG_RETURN_POINTER(ret);
}

Datum
decode(PG_FUNCTION_ARGS)
{
	Datum		expr = (Datum) 0;
	bool	expr_isnull;
	Oid		argtypes[FUNC_MAX_ARGS];
	Oid		collation;
	decode_cache *cache;
	int		nargs = PG_NARGS();
	int		result_argn;
	int		i;

	if (nargs < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("too few function arguments"),
				 errhint("The decode function requires at least 3 arguments")));

	/* collect arg types */
	for (i = 0; i < nargs; i++)
		argtypes[i] = get_fn_expr_argtype(fcinfo->flinfo, i);

	cache = (decode_cache *) fcinfo->flinfo->fn_extra;
	if (!is_valid_cache(nargs, argtypes, cache))
	{
		if (cache)
			free_cache(cache);
		cache = build_cache(nargs, argtypes, fcinfo->flinfo->fn_mcxt);

		fcinfo->flinfo->fn_extra = cache;
	}

	/* recheck rettype, should not be */
	if (get_fn_expr_rettype(fcinfo->flinfo) != cache->result_typid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("function has unexpected result type %d", get_fn_expr_rettype(fcinfo->flinfo)),
				 errhint("The decode expects \"%s\" type",
							format_type_be(cache->result_typid))));

	/* try to set result_argn to position with default arrgument */
	result_argn = nargs % 2 ? - 1 : nargs - 1;

	expr_isnull = PG_ARGISNULL(0);
	collation = PG_GET_COLLATION();

	if (!expr_isnull)
		expr = decode_cast(cache, 0, PG_GETARG_DATUM(0));

	for (i = 1; i < nargs; i+= 2)
	{
		if (!expr_isnull && !PG_ARGISNULL(i) && i + 1 < nargs)
		{
			Datum		eqop_result;
			Datum		value;

			value = decode_cast(cache, i, PG_GETARG_DATUM(i));
			eqop_result = FunctionCall2Coll(&cache->eqop_finfo, collation, expr, value);

			if (DatumGetBool(eqop_result))
			{
				result_argn = i + 1;
				break;
			}
		}
		else if (expr_isnull && PG_ARGISNULL(i))
		{
			result_argn = i + 1;
			break;
		}
	}

	if (result_argn >= 0 && !PG_ARGISNULL(result_argn))
		PG_RETURN_DATUM(decode_cast(cache, result_argn, PG_GETARG_DATUM(result_argn)));

	PG_RETURN_NULL();
}

/*
 * select_common_type_from_vector()
 *		Determine the common supertype of vector of Oids.
 *
 * Similar to select_common_type() but simplified for polymorphics
 * type processing. When there are no supertype, then returns InvalidOid,
 * when noerror is true, or raise exception when noerror is false.
 */
static Oid
select_common_type_from_vector(int nargs, Oid *typeids, bool noerror)
{
	int	i = 0;
	Oid			ptype;
	TYPCATEGORY pcategory;
	bool		pispreferred;

	Assert(nargs > 0);
	ptype = typeids[0];

	/* fast leave when all types are same */
	if (ptype != UNKNOWNOID)
	{
		for (i = 1; i < nargs; i++)
		{
			if (ptype != typeids[i])
				break;
		}

		if (i == nargs)
			return ptype;
	}

	/*
	 * Nope, so set up for the full algorithm.  Note that at this point, lc
	 * points to the first list item with type different from pexpr's; we need
	 * not re-examine any items the previous loop advanced over.
	 */
	ptype = getBaseType(ptype);
	get_type_category_preferred(ptype, &pcategory, &pispreferred);

	for (; i < nargs; i++)
	{
		Oid			ntype = getBaseType(typeids[i]);

		/* move on to next one if no new information... */
		if (ntype != UNKNOWNOID && ntype != ptype)
		{
			TYPCATEGORY ncategory;
			bool		nispreferred;

			get_type_category_preferred(ntype, &ncategory, &nispreferred);

			if (ptype == UNKNOWNOID)
			{
				/* so far, only unknowns so take anything... */
				ptype = ntype;
				pcategory = ncategory;
				pispreferred = nispreferred;
			}
			else if (ncategory != pcategory)
			{
				if (noerror)
					return InvalidOid;

				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("types %s and %s cannot be matched",
								format_type_be(ptype),
								format_type_be(ntype))));
			}
			else if (!pispreferred &&
					 can_coerce_type(1, &ptype, &ntype, COERCION_IMPLICIT) &&
					 !can_coerce_type(1, &ntype, &ptype, COERCION_IMPLICIT))
			{
				/*
				 * take new type if can coerce to it implicitly but not the
				 * other way; but if we have a preferred type, stay on it.
				 */
				ptype = ntype;
				pcategory = ncategory;
				pispreferred = nispreferred;
			}
		}
	}

	/*
	 * Be consistent with select_common_type()
	 */
	if (ptype == UNKNOWNOID)
		ptype = TEXTOID;

	return ptype;
}
