#include "postgres.h"
#include "plpython.h"
#include "plpy_typeio.h"

#include "utils/jsonb.h"
#include "utils/fmgrprotos.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* for PLyObject_AsString in plpy_typeio.c */
typedef char *(*PLyObject_AsString_t) (PyObject *plrv);
static PLyObject_AsString_t PLyObject_AsString_p;

/*
 * decimal_constructor is a function from python library and used
 * for transforming strings into python decimal type
 */
static PyObject *decimal_constructor;

static PyObject *PyObject_FromJsonb(JsonbContainer *jsonb);
static JsonbValue *PyObject_ToJsonbValue(PyObject *obj,
					  JsonbParseState *jsonb_state);

#if PY_MAJOR_VERSION >= 3
typedef PyObject *(*PLyUnicode_FromStringAndSize_t)
			(const char *s, Py_ssize_t size);
static PLyUnicode_FromStringAndSize_t PLyUnicode_FromStringAndSize_p;
#endif

/*
 * Module initialize function: fetch function pointers for cross-module calls.
 */
void
_PG_init(void)
{
	/* Asserts verify that typedefs above match original declarations */
	AssertVariableIsOfType(&PLyObject_AsString, PLyObject_AsString_t);
	PLyObject_AsString_p = (PLyObject_AsString_t)
		load_external_function("$libdir/" PLPYTHON_LIBNAME, "PLyObject_AsString",
							   true, NULL);
#if PY_MAJOR_VERSION >= 3
	AssertVariableIsOfType(&PLyUnicode_FromStringAndSize, PLyUnicode_FromStringAndSize_t);
	PLyUnicode_FromStringAndSize_p = (PLyUnicode_FromStringAndSize_t)
		load_external_function("$libdir/" PLPYTHON_LIBNAME, "PLyUnicode_FromStringAndSize",
							   true, NULL);
#endif
}

/* These defines must be after the _PG_init */
#define PLyObject_AsString (PLyObject_AsString_p)
#define PLyUnicode_FromStringAndSize (PLyUnicode_FromStringAndSize_p)

/*
 * PyObject_FromJsonbValue
 *
 * Transform JsonbValue into PyObject.
 */
static PyObject *
PyObject_FromJsonbValue(JsonbValue *jsonbValue)
{
	PyObject   *result;

	switch (jsonbValue->type)
	{
		case jbvNull:
			result = Py_None;
			break;
		case jbvBinary:
			result = PyObject_FromJsonb(jsonbValue->val.binary.data);
			break;
		case jbvNumeric:
			{
				Datum		num;
				char	   *str;

				num = NumericGetDatum(jsonbValue->val.numeric);
				str = DatumGetCString(DirectFunctionCall1(numeric_out, num));
				result = PyObject_CallFunction(decimal_constructor, "s", str);
				break;
			}
		case jbvString:
			result = PyString_FromStringAndSize(jsonbValue->val.string.val,
												jsonbValue->val.string.len);
			break;
		case jbvBool:
			result = jsonbValue->val.boolean ? Py_True : Py_False;
			break;
		case jbvArray:
		case jbvObject:
			result = PyObject_FromJsonb(jsonbValue->val.binary.data);
			break;
	}
	return result;
}

/*
 * PyObject_FromJsonb
 *
 * Transform JsonbContainer into PyObject.
 */
static PyObject *
PyObject_FromJsonb(JsonbContainer *jsonb)
{
	JsonbIteratorToken r;
	JsonbValue	v;
	JsonbIterator *it;

	PyObject   *result,
			   *key,
			   *value;

	it = JsonbIteratorInit(jsonb);
	r = JsonbIteratorNext(&it, &v, true);

	switch (r)
	{
		case WJB_BEGIN_ARRAY:
			if (v.val.array.rawScalar)
			{
				r = JsonbIteratorNext(&it, &v, true);
				result = PyObject_FromJsonbValue(&v);
			}
			else
			{
				/* array in v */
				result = PyList_New(0);
				while ((r = JsonbIteratorNext(&it, &v, true)) == WJB_ELEM)
					PyList_Append(result, PyObject_FromJsonbValue(&v));
			}
			break;
		case WJB_BEGIN_OBJECT:
			result = PyDict_New();
			while ((r = JsonbIteratorNext(&it, &v, true)) == WJB_KEY)
			{
				key = PyString_FromStringAndSize(v.val.string.val,
												 v.val.string.len);
				r = JsonbIteratorNext(&it, &v, true);
				value = PyObject_FromJsonbValue(&v);
				PyDict_SetItem(result, key, value);
			}
			break;
		case WJB_END_OBJECT:
			pg_unreachable();
			break;
		default:
			result = PyObject_FromJsonbValue(&v);
			break;
	}
	return result;
}



/*
 * PyMapping_ToJsonbValue
 *
 * Transform python dict to JsonbValue.
 */
static JsonbValue *
PyMapping_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	int32		pcount;
	JsonbValue *out = NULL;

	/* We need it volatile, since we use it after longjmp */
	volatile PyObject *items_v = NULL;

	pcount = PyMapping_Size(obj);
	items_v = PyMapping_Items(obj);

	PG_TRY();
	{
		int32		i;
		PyObject   *items;
		JsonbValue *jbvValue;
		JsonbValue	jbvKey;

		items = (PyObject *) items_v;
		pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

		for (i = 0; i < pcount; i++)
		{
			PyObject   *tuple,
					   *key,
					   *value;

			tuple = PyList_GetItem(items, i);
			key = PyTuple_GetItem(tuple, 0);
			value = PyTuple_GetItem(tuple, 1);

			/* Python dictionary can have None as key */
			if (key == Py_None)
			{
				jbvKey.type = jbvString;
				jbvKey.val.string.len = 0;
				jbvKey.val.string.val = "";
			}
			else
			{
				/* All others types of keys we serialize to string */
				jbvKey.type = jbvString;
				jbvKey.val.string.val = PLyObject_AsString(key);
				jbvKey.val.string.len = strlen(jbvKey.val.string.val);
			}

			pushJsonbValue(&jsonb_state, WJB_KEY, &jbvKey);
			jbvValue = PyObject_ToJsonbValue(value, jsonb_state);
			if (IsAJsonbScalar(jbvValue))
				pushJsonbValue(&jsonb_state, WJB_VALUE, jbvValue);
		}
		out = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
	}
	PG_CATCH();
	{
		Py_DECREF(items_v);
		PG_RE_THROW();
	}
	PG_END_TRY();
	return out;
}

/*
 * PyString_ToJsonbValue
 *
 * Transform python string to JsonbValue
 */
static JsonbValue *
PyString_ToJsonbValue(PyObject *obj)
{
	JsonbValue *jbvElem;

	jbvElem = palloc(sizeof(JsonbValue));
	jbvElem->type = jbvString;
	jbvElem->val.string.val = PLyObject_AsString(obj);
	jbvElem->val.string.len = strlen(jbvElem->val.string.val);

	return jbvElem;
}

/*
 * PySequence_ToJsonbValue
 *
 * Transform python list to JsonbValue. Expects transformed PyObject and
 * a state required for jsonb construction.
 */
static JsonbValue *
PySequence_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	JsonbValue *jbvElem;
	Size		i,
				pcount;
	JsonbValue *out = NULL;

	pcount = PySequence_Size(obj);
	pushJsonbValue(&jsonb_state, WJB_BEGIN_ARRAY, NULL);

	for (i = 0; i < pcount; i++)
	{
		PyObject   *value;

		value = PySequence_GetItem(obj, i);
		jbvElem = PyObject_ToJsonbValue(value, jsonb_state);
		if (IsAJsonbScalar(jbvElem))
			pushJsonbValue(&jsonb_state, WJB_ELEM, jbvElem);
	}
	out = pushJsonbValue(&jsonb_state, WJB_END_ARRAY, NULL);
	return (out);
}

/*
 * PyNumeric_ToJsonbValue(PyObject *obj)
 *
 * Transform python number to JsonbValue.
 */
static JsonbValue *
PyNumeric_ToJsonbValue(PyObject *obj)
{
	volatile bool	failed = false;
	Numeric		num;
	JsonbValue *jbvInt;
	char	   *str = PLyObject_AsString(obj);

	PG_TRY();
	{
		num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
												  CStringGetDatum(str), 0, -1));
	}
	PG_CATCH();
	{
		failed = true;
	}
	PG_END_TRY();

	if (failed)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("plpython transformation error"),
				  errdetail("the value \"%s\" cannot be transformed to jsonb", str))));

	jbvInt = palloc(sizeof(JsonbValue));
	jbvInt->type = jbvNumeric;
	jbvInt->val.numeric = num;

	return jbvInt;
}

/*
 * PyObject_ToJsonbValue(PyObject *obj)
 *
 * Transform python object to JsonbValue.
 */
static JsonbValue *
PyObject_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	JsonbValue *out = NULL;

	if (PyDict_Check(obj))
		out = PyMapping_ToJsonbValue(obj, jsonb_state);
	else if (PyString_Check(obj) || PyUnicode_Check(obj))
		out = PyString_ToJsonbValue(obj);
	else if (PyList_Check(obj))
		out = PySequence_ToJsonbValue(obj, jsonb_state);
	else if ((obj == Py_True) || (obj == Py_False))
	{
		out = palloc(sizeof(JsonbValue));
		out->type = jbvBool;
		out->val.boolean = (obj == Py_True);
	}
	else if (obj == Py_None)
	{
		out = palloc(sizeof(JsonbValue));
		out->type = jbvNull;
	}
	else if (PyNumber_Check(obj))
		out = PyNumeric_ToJsonbValue(obj);
	else
	{
		PyObject* repr = PyObject_Type(obj);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 (errmsg("plpython transformation error"),
				  errdetail("\"%s\" type cannot be transformed to jsonb", PLyObject_AsString(repr)))));
	}

	return out;
}

/*
 * plpython_to_jsonb
 *
 * Transform python object to Jsonb datum
 */
PG_FUNCTION_INFO_V1(plpython_to_jsonb);
Datum
plpython_to_jsonb(PG_FUNCTION_ARGS)
{
	PyObject   *obj;
	JsonbValue *out;
	JsonbParseState *jsonb_state = NULL;

	obj = (PyObject *) PG_GETARG_POINTER(0);
	out = PyObject_ToJsonbValue(obj, jsonb_state);
	PG_RETURN_POINTER(JsonbValueToJsonb(out));
}

/*
 * jsonb_to_plpython
 *
 * Transform Jsonb datum into PyObject and return it as internal.
 */
PG_FUNCTION_INFO_V1(jsonb_to_plpython);
Datum
jsonb_to_plpython(PG_FUNCTION_ARGS)
{
	PyObject	*result;
	Jsonb		*in = PG_GETARG_JSONB_P(0);

	/*
	 * Initialize pointer to Decimal constructor. First we try "cdecimal", C
	 * version of decimal library. In case of failure we use slower "decimal"
	 * module.
	 */
	if (!decimal_constructor)
	{
		PyObject   *decimal_module = PyImport_ImportModule("cdecimal");

		if (!decimal_module)
		{
			PyErr_Clear();
			decimal_module = PyImport_ImportModule("decimal");
		}
		Assert(decimal_module);
		decimal_constructor = PyObject_GetAttrString(decimal_module, "Decimal");
	}

	result = PyObject_FromJsonb(&in->root);
	return PointerGetDatum(result);
}
