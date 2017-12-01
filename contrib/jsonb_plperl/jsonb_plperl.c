/* This document contains an implementation of transformations from perl
 * object to jsonb and vise versa.
 * In this file you can find implementation of transformations:
 * - SV_FromJsonbValue(JsonbValue *jsonbValue)
 * - SV_FromJsonb(JsonbContainer *jsonb)
 * - jsonb_to_plperl(PG_FUNCTION_ARGS)
 * - SV_ToJsonbValue(SV *in, JsonbParseState *jsonb_state)
 * - HV_ToJsonbValue(HV *obj, JsonbParseState *jsonb_state)
 * - AV_ToJsonbValue(AV *in, JsonbParseState *jsonb_state)
 * - plperl_to_jsonb(PG_FUNCTION_ARGS)
 */
#include "postgres.h"

#undef _

#include "fmgr.h"
#include "plperl.h"
#include "plperl_helpers.h"

#include "utils/jsonb.h"
#include "utils/fmgrprotos.h"

PG_MODULE_MAGIC;

/* Links to functions
 * */
static SV  *SV_FromJsonb(JsonbContainer *jsonb);

static JsonbValue *HV_ToJsonbValue(HV *obj, JsonbParseState *jsonb_state);

static JsonbValue *SV_ToJsonbValue(SV *obj, JsonbParseState *jsonb_state);

/*
 * Function for transforming JsonbValue type into SV
 * The first argument defines the JsonbValue to be transformed into SV
 * Return value is the pointer to transformed object
 */
static SV  *
SV_FromJsonbValue(JsonbValue *jsonbValue)
{
	dTHX;
	SV		   *result;
	char	   *str;

	switch (jsonbValue->type)
	{
		case jbvBinary:
			result = (SV *) newRV((SV *) SV_FromJsonb(jsonbValue->val.binary.data));
			break;
		case jbvNumeric:

			/*
			 * XXX There should be a better way. Right now Numeric is
			 * transformed into string and then this string is parsed into
			 * perl numeric
			 */
			str = DatumGetCString(DirectFunctionCall1(
													  numeric_out,
													  NumericGetDatum(jsonbValue->val.numeric)
													  )
				);
			result = newSVnv(SvNV(cstr2sv(pnstrdup(str, strlen(str)))));
			break;
		case jbvString:
			result = cstr2sv(pnstrdup(
									  jsonbValue->val.string.val,
									  jsonbValue->val.string.len
									  ));
			break;
		case jbvBool:
			result = newSVnv(SvNV(jsonbValue->val.boolean ? &PL_sv_yes : &PL_sv_no));
			break;
		case jbvArray:
			result = SV_FromJsonbValue(jsonbValue->val.array.elems);
			break;
		case jbvObject:
			result = SV_FromJsonbValue(&(jsonbValue->val.object.pairs->value));
			break;
		case jbvNull:
			result = newSV(0);
			break;
		default:
			pg_unreachable();
			break;
	}
	return (result);
}

/*
 * Function for transforming JsonbContainer type into SV
 * The first argument defines the JsonbContainer to be transformed into SV
 * Return value is the pointer to transformed object
 */
static SV  *
SV_FromJsonb(JsonbContainer *jsonb)
{
	dTHX;
	SV		   *result;
	SV		   *value;
	HV		   *object;
	AV		   *av;
	JsonbIterator *it;
	JsonbValue	v;
	const char *key;
	int			keyLength;
	bool		raw_scalar;

	it = JsonbIteratorInit(jsonb);

	switch (JsonbIteratorNext(&it, &v, true))
	{
		case (WJB_BEGIN_ARRAY):
			/* array in v */
			av = newAV();
			raw_scalar = (v.val.array.rawScalar);
			value = newSV(0);
			while (
				   (JsonbIteratorNext(&it, &v, true) == WJB_ELEM)
				)
			{
				value = SV_FromJsonbValue(&v);
				av_push(av, value);
			}
			if (raw_scalar)
				result = (newRV(value));
			else
				result = ((SV *) av);
			break;
		case (WJB_BEGIN_OBJECT):
			/* hash in v*/
			object = newHV();
			while (JsonbIteratorNext(&it, &v, true) == WJB_KEY)
			{
				/* json key in v */
				key = pnstrdup(
							   v.val.string.val,
							   v.val.string.len
					);
				keyLength = v.val.string.len;
				JsonbIteratorNext(&it, &v, true);
				value = SV_FromJsonbValue(&v);
				(void) hv_store(object, key, keyLength, value, 0);
			}
			result = (SV *) object;
			break;
		case (WJB_ELEM):
		case (WJB_VALUE):
		case (WJB_KEY):
			/* simple objects */
			result = (SV_FromJsonbValue(&v));
			break;
		case (WJB_DONE):
		case (WJB_END_OBJECT):
		case (WJB_END_ARRAY):
		default:
			pg_unreachable();
			break;
	}
	return (result);
}

/* jsonb_to_plperl(Jsonb *in)
 * Function for transforming Jsonb type into SV
 * The first argument defines the Jsonb to be transformed into SV
 * Return value is the pointer to transformed object
 */
PG_FUNCTION_INFO_V1(jsonb_to_plperl);
Datum
jsonb_to_plperl(PG_FUNCTION_ARGS)
{
	dTHX;
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	SV		   *sv;

	sv = SV_FromJsonb(&in->root);

	return PointerGetDatum(newRV(sv));
}

/*
 * Function for transforming AV type into JsonbValue
 * The first argument defines the AV to be transformed into JsonbValue
 * The second argument defines conversion state
 * Return value is the pointer to transformed object
 */
static JsonbValue *
AV_ToJsonbValue(AV *in, JsonbParseState *jsonb_state)
{
	dTHX;

	JsonbValue *jbvElem;
	JsonbValue *out = NULL;
	int32		pcount;
	int32		i;

	pcount = av_len(in) + 1;
	pushJsonbValue(&jsonb_state, WJB_BEGIN_ARRAY, NULL);

	for (i = 0; i < pcount; i++)
	{
		SV		  **value;

		value = av_fetch(in, i, false);
		jbvElem = SV_ToJsonbValue(*value, jsonb_state);
		if (IsAJsonbScalar(jbvElem))
			pushJsonbValue(&jsonb_state, WJB_ELEM, jbvElem);
	}
	out = pushJsonbValue(&jsonb_state, WJB_END_ARRAY, NULL);
	return (out);
}

/*
 * Function for transforming Jsonb type into SV
 * The first argument defines the Jsonb to be transformed into SV
 * The second argument defines conversion state
 * Return value is the pointer to transformed object
 */
static JsonbValue *
SV_ToJsonbValue(SV *in, JsonbParseState *jsonb_state)
{
	dTHX;
	svtype		type;
	JsonbValue *out;
	char	   *str;
	Datum		tmp;

	type = SvTYPE(in);
	switch (type)
	{
		case SVt_PVAV:
			out = AV_ToJsonbValue((AV *) in, jsonb_state);
			break;
		case SVt_PVHV:
			out = HV_ToJsonbValue((HV *) in, jsonb_state);
			break;
		case SVt_NV:
		case SVt_IV:
			if (SvROK(in))
				/* if in is a pointer */
				out = SV_ToJsonbValue((SV *) SvRV(in), jsonb_state);
			else
			{
				/* if in is a numeric */
				out = palloc(sizeof(JsonbValue));
				str = sv2cstr(in);
				if (strcmp(str, "Inf") != 0)
				{
					tmp = DirectFunctionCall3(
											  numeric_in,
											  CStringGetDatum(str), 0, -1
						);
					out->val.numeric = DatumGetNumeric(tmp);
					out->type = jbvNumeric;
				}
				else
				{
					elog(ERROR, "Infinity can't be represented in JSONB");
				}
			}
			break;
		case SVt_NULL:
			out = palloc(sizeof(JsonbValue));
			out->type = jbvNull;
			break;
		case SVt_PV:
			/*
			 * String
			 */
			out = palloc(sizeof(JsonbValue));
			out->val.string.val = sv2cstr(in);
			out->val.string.len = strlen(out->val.string.val);
			out->type = jbvString;
			break;
		default:
			elog(ERROR, "The type you was trying to transform can't be represented in JSONB");
			break;
	}
	return (out);
}

/*
 * Function for transforming Jsonb type into SV
 * The first argument defines the Jsonb to be transformed into SV
 * The second argument defines conversion staterl
 * Return value is the pointer to transformed object
 */
static JsonbValue *
HV_ToJsonbValue(HV *obj, JsonbParseState *jsonb_state)
{
	dTHX;
	JsonbValue *out;
	HE		   *he;

	pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);
	while ((he = hv_iternext(obj)))
	{
		JsonbValue *key;
		JsonbValue *val;

		key = SV_ToJsonbValue(HeSVKEY_force(he), jsonb_state);
		pushJsonbValue(&jsonb_state, WJB_KEY, key);
		val = SV_ToJsonbValue(HeVAL(he), jsonb_state);
		if ((val == NULL) || (IsAJsonbScalar(val)))
			pushJsonbValue(&jsonb_state, WJB_VALUE, val);
	}
	out = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
	return (out);
}

/*
 * plperl_to_jsonb(SV *in)
 * Function for transforming Jsonb type into SV
 * The first argument defines the Jsonb to be transformed into SV
 * Return value is the pointer to transformed object
 */
PG_FUNCTION_INFO_V1(plperl_to_jsonb);
Datum
plperl_to_jsonb(PG_FUNCTION_ARGS)
{
	dTHX;
	JsonbValue *out = NULL;
	Jsonb	   *result;
	JsonbParseState *jsonb_state = NULL;
	SV		   *in;

	in = (SV *) PG_GETARG_POINTER(0);
	out = SV_ToJsonbValue(in, jsonb_state);
	result = JsonbValueToJsonb(out);
	PG_RETURN_POINTER(result);
}
