/*-------------------------------------------------------------------------
 *
 * typedvalue.c
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/typedvalue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/typedvalue.h"
#include "fmgr.h"

/*
 * Returns Datum value stored in TypedValue structure.
 * This structure can hold byval or varlena values.
 */
static Datum
TypedValueGetDatum(TypedValue tv)
{
	if (tv->typbyval)
		return *((Datum *) tv->data);
	else
		return PointerGetDatum(tv->data);
}

/*
 * IN function for TypedValue type. It store input value as text type
 * value.
 */
Datum
typedvalue_in(PG_FUNCTION_ARGS)
{
	char	   *str =  PG_GETARG_CSTRING(0);
	int			len;
	Size		size;
	TypedValue	tv;
	text	   *txt;

	len = strlen(str);

	size = MAXALIGN(offsetof(TypedValueData, data) + len + VARHDRSZ);

	tv = (TypedValue) palloc(size);
	SET_VARSIZE(tv, size);

	txt = (text *) tv->data;

	SET_VARSIZE(txt, VARHDRSZ + len);
	memcpy(VARDATA(txt), str, len);

	tv->typid = TEXTOID;
	tv->typbyval = false;
	tv->typlen = -1;

	PG_RETURN_POINTER(tv);
}

/*
 * OUT function for TypedValue type. It search related output
 * function for stored type, execute it, and returns result.
 */
Datum
typedvalue_out(PG_FUNCTION_ARGS)
{
	Oid			typOutput;
	bool		isVarlena;
	char	   *str;
	TypedValue	tv;
	Datum		value;

	tv = (TypedValue) PG_GETARG_POINTER(0);

	getTypeOutputInfo(tv->typid, &typOutput, &isVarlena);
	value = TypedValueGetDatum(tv);
	str = OidOutputFunctionCall(typOutput, value);

	PG_RETURN_CSTRING(str);
}

/*
 * Constructor function for TypedValue type. It serializes
 * byval or varlena Datum values.
 */
Datum
makeTypedValue(Datum value, Oid typid, int16 typlen, bool typbyval)
{
	TypedValue		tv;
	Size			size;
	Size			copy_bytes = 0;

	if (typbyval)
		size = MAXALIGN(offsetof(TypedValueData, data) + sizeof(Datum));
	else
	{
		if (typlen != -1)
		{
			size = MAXALIGN(offsetof(TypedValueData, data) + typlen);
			copy_bytes = typlen;
		}
		else
		{
			copy_bytes = VARSIZE_ANY((struct varlena *) DatumGetPointer(value));
			size = MAXALIGN(offsetof(TypedValueData, data) + copy_bytes);
		}
	}

	tv = (TypedValue) palloc(size);

	SET_VARSIZE(tv, size);

	tv->typid = typid;
	tv->typlen = typlen;
	tv->typbyval = typbyval;

	if (typbyval)
		*((Datum *) tv->data) = value;
	else
		memcpy(tv->data, DatumGetPointer(value), copy_bytes);

	return PointerGetDatum(tv);
}

/*
 * Fast cast finding of known (buildin) cast functions.
 */
static PGFunction
get_direct_cast_func(Oid source_typid, Oid target_typid)
{
	switch (target_typid)
	{
		case NUMERICOID:
		{
			switch (source_typid)
			{
				case INT4OID:
					return int4_numeric;

				case INT8OID:
					return int8_numeric;

				case FLOAT4OID:
					return float4_numeric;

				case FLOAT8OID:
					return float8_numeric;

				default:
					return NULL;
			}
		}

		case INT8OID:
		{
			switch (source_typid)
			{
				case INT4OID:
					return int48;

				case NUMERICOID:
					return numeric_int8;

				case FLOAT4OID:
					return ftoi8;

				case FLOAT8OID:
					return dtoi8;

				default:
					return NULL;
			}
		}

		case INT4OID:
		{
			switch (source_typid)
			{
				case BOOLOID:
					return bool_int4;

				case INT8OID:
					return int84;

				case NUMERICOID:
					return numeric_int4;

				case FLOAT4OID:
					return ftoi4;

				case FLOAT8OID:
					return dtoi4;

				default:
					return NULL;
			}
		}

		case FLOAT8OID:
		{
			switch (source_typid)
			{
				case INT4OID:
					return i4tod;

				case INT8OID:
					return i8tod;

				case NUMERICOID:
					return numeric_float8;

				case FLOAT4OID:
					return ftod;

				default:
					return NULL;
			}
		}

		case FLOAT4OID:
		{
			switch (source_typid)
			{
				case INT4OID:
					return i4tof;

				case INT8OID:
					return i8tof;

				case NUMERICOID:
					return numeric_float4;

				case FLOAT8OID:
					return dtof;

				default:
					return NULL;
			}
		}

		case DATEOID:
		{
			switch (source_typid)
			{
				case TIMESTAMPOID:
					return timestamp_date;

				case TIMESTAMPTZOID:
					return timestamptz_date;

				default:
					return NULL;
			}
		}

		case TIMESTAMPOID:
		{
			switch (source_typid)
			{
				case DATEOID:
					return date_timestamp;

				case TIMESTAMPTZOID:
					return timestamptz_timestamp;

				default:
					return NULL;
			}
		}

		case TIMESTAMPTZOID:
		{
			switch (source_typid)
			{
				case DATEOID:
					return date_timestamptz;

				case TIMESTAMPOID:
					return timestamp_timestamptz;

				default:
					return NULL;
			}
		}

		case BOOLOID:
		{
			switch (source_typid)
			{
				case INT4OID:
					return int4_bool;

				default:
					return NULL;
			}
		}

		default:
			return NULL;
	}
}

static Datum
cast_typedvalue_to(TypedValue tv, Oid target_typid)
{
	Datum		value = TypedValueGetDatum(tv);
	Datum		result;
	char	   *str;
	PGFunction	cast_func;
	Oid			cast_func_oid;
	Oid			typinput;
	Oid			typioparam;
	Oid			typoutput;
	bool		isvarlena;

	if (tv->typid == target_typid)
		return datumCopy(value,
						 tv->typbyval,
						 tv->typlen);

	if (tv->typid == TEXTOID || tv->typid == BPCHAROID)
	{
		getTypeInputInfo(target_typid, &typinput, &typioparam);
		str = TextDatumGetCString(value);
		result = OidInputFunctionCall(typinput, str,
									  typioparam, -1);
		pfree(str);

		return result;
	}

	if (target_typid == TEXTOID || target_typid == BPCHAROID)
	{
		getTypeOutputInfo(tv->typid, &typoutput, &isvarlena);
		str = OidOutputFunctionCall(typoutput, value);
		result = PointerGetDatum(cstring_to_text(str));

		return result;
	}

	/* fast cast func detection, and direct call */
	cast_func = get_direct_cast_func(tv->typid, target_typid);
	if (cast_func)
	{
		result = DirectFunctionCall1(cast_func, value);

		return result;
	}

	/* slower cast func, and indirect call */
	cast_func_oid = get_cast_oid(tv->typid, target_typid, false);
	if (OidIsValid(cast_func_oid))
	{
		result = OidFunctionCall1(cast_func_oid, value);

		return result;
	}

	/* IO cast - most slow */
	getTypeOutputInfo(tv->typid, &typoutput, &isvarlena);
	str = OidOutputFunctionCall(typoutput, value);

	getTypeInputInfo(target_typid, &typinput, &typioparam);
	result = OidInputFunctionCall(typinput, str,
								  typioparam, -1);
	pfree(str);

	return result;
}

/*
 * Casting functions - from variant typedvalue to specific types
 */
Datum
typedvalue_to_numeric(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, NUMERICOID));
}

Datum
typedvalue_to_int8(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, INT8OID));
}

Datum
typedvalue_to_int4(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, INT4OID));
}

Datum
typedvalue_to_float8(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, FLOAT8OID));
}

Datum
typedvalue_to_float4(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, FLOAT4OID));
}

Datum
typedvalue_to_date(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, DATEOID));
}

Datum
typedvalue_to_timestamp(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, TIMESTAMPOID));
}

Datum
typedvalue_to_timestamptz(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, TIMESTAMPTZOID));
}

Datum
typedvalue_to_interval(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, INTERVALOID));
}

Datum
typedvalue_to_text(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, TEXTOID));
}

Datum
typedvalue_to_bpchar(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, BPCHAROID));
}

Datum
typedvalue_to_bool(PG_FUNCTION_ARGS)
{
	TypedValue tv = (TypedValue) PG_GETARG_POINTER(0);

	PG_RETURN_DATUM(cast_typedvalue_to(tv, BOOLOID));
}

/*
 * Cast function from specific types to TypedValue
 */
Datum
numeric_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   NUMERICOID,
								   -1,
								   false));
}

Datum
int8_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   INT8OID,
								   8,
								   true));
}

Datum
int4_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   INT4OID,
								   4,
								   true));
}

Datum
float8_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   FLOAT8OID,
								   8,
								   true));
}

Datum
float4_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   FLOAT4OID,
								   4,
								   true));
}


Datum
date_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   DATEOID,
								   4,
								   true));
}

Datum
timestamp_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   TIMESTAMPOID,
								   8,
								   true));
}

Datum
timestamptz_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   TIMESTAMPTZOID,
								   8,
								   true));
}

Datum
interval_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   INTERVALOID,
								   16,
								   false));
}

Datum
text_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   TEXTOID,
								   -1,
								   false));
}

Datum
bpchar_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   BPCHAROID,
								   -1,
								   false));
}

Datum
bool_to_typedvalue(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(makeTypedValue(PG_GETARG_DATUM(0),
								   BOOLOID,
								   1,
								   true));
}
