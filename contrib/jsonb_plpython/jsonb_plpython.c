/* This document contains an implementation of transformations from python
 * object to jsonb and vise versa.
 * In this file you can find implementation of transformations:
 * - JsonbValue transformation in  PyObject_FromJsonbValue
 * - JsonbContainer(jsonb) transformation in PyObject_FromJsonb
 * - PyMapping object(dict) transformation in PyMapping_ToJsonbValue
 * - PyString object transformation in PyString_ToJsonbValue
 * - PySequence object(list) transformation in PySequence_ToJsonbValue
 * - PyNumeric object transformation in PyNumeric_ToJsonbValue
 * - PyMapping object transformation in PyObject_ToJsonbValue
 * */
#include "postgres.h"

#include "plpython.h"
#include "plpy_typeio.h"

#include "utils/jsonb.h"
#include "utils/fmgrprotos.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

/* Linkage to functions in plpython module */
typedef char *(*PLyObject_AsString_t) (PyObject *plrv);
static PLyObject_AsString_t PLyObject_AsString_p;
#if PY_MAJOR_VERSION >= 3
typedef PyObject *(*PLyUnicode_FromStringAndSize_t) (const char *s, Py_ssize_t size);
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


/* These defines must be after the module init function */
#define PLyObject_AsString PLyObject_AsString_p
#define PLyUnicode_FromStringAndSize PLyUnicode_FromStringAndSize_p

/*
 * decimal_constructor is a link to Python library
 * for transforming strings into python decimal type
 * */
static PyObject *decimal_constructor;

static PyObject *PyObject_FromJsonb(JsonbContainer *jsonb);
static JsonbValue *PyObject_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state);

/*
 * PyObject_FromJsonbValue(JsonsValue *jsonbValue)
 * Function for transforming JsonbValue type into Python Object
 * The first argument defines the JsonbValue which will be transformed into PyObject
 * Return value is the pointer to Jsonb structure containing the transformed object.
 * */
static PyObject *
PyObject_FromJsonbValue(JsonbValue *jsonbValue)
{
	PyObject   *result;
	char	   *str;

	switch (jsonbValue->type)
	{
		case jbvNull:
			result = Py_None;
			break;
		case jbvBinary:
			result = PyObject_FromJsonb(jsonbValue->val.binary.data);
			break;
		case jbvNumeric:

			/*
			 * XXX There should be a better way. Right now Numeric is
			 * transformed into string and then this string is parsed into py
			 * numeric
			 */
			str = DatumGetCString(
								  DirectFunctionCall1(numeric_out, NumericGetDatum(jsonbValue->val.numeric))
				);
			result = PyObject_CallFunction(decimal_constructor, "s", str);
			break;
		case jbvString:
			result = PyString_FromStringAndSize(
												jsonbValue->val.string.val,
												jsonbValue->val.string.len
				);
			break;
		case jbvBool:
			result = jsonbValue->val.boolean ? Py_True : Py_False;
			break;
		case jbvArray:
		case jbvObject:
			result = PyObject_FromJsonb(jsonbValue->val.binary.data);
			break;
		default:
			pg_unreachable();
			break;
	}
	return (result);
}

/*
 * PyObject_FromJsonb(JsonbContainer *jsonb)
 * Function for transforming JsonbContainer(jsonb) into PyObject
 * The first argument should represent the data for transformation.
 * Return value is the pointer to Python object.
 * */

static PyObject *
PyObject_FromJsonb(JsonbContainer *jsonb)
{
	PyObject   *object;
	PyObject   *key;
	PyObject   *value;

	JsonbIterator *it;
	JsonbIteratorToken r;
	JsonbValue	v;

	it = JsonbIteratorInit(jsonb);

	r = JsonbIteratorNext(&it, &v, true);

	switch (r)
	{
		case (WJB_BEGIN_ARRAY):
			if (v.val.array.rawScalar)
			{
				r = JsonbIteratorNext(&it, &v, true);
				object = PyObject_FromJsonbValue(&v);
			}
			else
			{
				/* array in v */
				object = PyList_New(0);
				while ((r = JsonbIteratorNext(&it, &v, true)) == WJB_ELEM)
					PyList_Append(object, PyObject_FromJsonbValue(&v));
			}
			break;
		case (WJB_BEGIN_OBJECT):
			object = PyDict_New();
			while ((r = JsonbIteratorNext(&it, &v, true)) == WJB_KEY)
			{
				key = PyString_FromStringAndSize(
												 v.val.string.val,
												 v.val.string.len
					);
				r = JsonbIteratorNext(&it, &v, true);
				value = PyObject_FromJsonbValue(&v);
				PyDict_SetItem(object, key, value);
			}
			break;
		case (WJB_END_OBJECT):
			pg_unreachable();
			break;
		default:
			object = PyObject_FromJsonbValue(&v);
			break;
	}
	return (object);
}


/*
 * jsonb_to_plpython(Jsonb *in)
 * Function to transform jsonb object to corresponding python object.
 * The first argument is the Jsonb object to be transformed.
 * Return value is the pointer to Python object.
 * */
PG_FUNCTION_INFO_V1(jsonb_to_plpython);
Datum
jsonb_to_plpython(PG_FUNCTION_ARGS)
{
	Jsonb	   *in;
	PyObject   *dict;
	PyObject   *decimal_module;

	in = PG_GETARG_JSONB_P(0);

	/* Import python cdecimal library and if there is no cdecimal library, */
	/* import decimal library */
	if (!decimal_constructor)
	{
		decimal_module = PyImport_ImportModule("cdecimal");
		if (!decimal_module)
		{
			PyErr_Clear();
			decimal_module = PyImport_ImportModule("decimal");
		}
		decimal_constructor = PyObject_GetAttrString(decimal_module, "Decimal");
	}

	dict = PyObject_FromJsonb(&in->root);
	return PointerGetDatum(dict);
}


/*
 * PyMapping_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
 * Function to transform Python lists to jsonbValue
 * The first argument is the python object to be transformed.
 * Return value is the pointer to JsonbValue structure containing the list.
 * */
static JsonbValue *
PyMapping_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	volatile PyObject *items_v = NULL;
	int32		pcount;
	JsonbValue *out = NULL;

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
			PyObject   *tuple;
			PyObject   *key;
			PyObject   *value;

			tuple = PyList_GetItem(items, i);
			key = PyTuple_GetItem(tuple, 0);
			value = PyTuple_GetItem(tuple, 1);

			if (key == Py_None)
			{
				jbvKey.type = jbvString;
				jbvKey.val.string.len = 0;
				jbvKey.val.string.val = "";
			}
			else
			{
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
	return (out);
}

/*
 * PyString_ToJsonbValue(PyObject *obj)
 * Function to transform python string object to jsonbValue object.
 * The first argument is the Python String object to be transformed.
 * Return value is the pointer to JsonbValue structure containing the String.
 * */
static JsonbValue *
PyString_ToJsonbValue(PyObject *obj)
{
	JsonbValue *out = NULL;
	JsonbValue *jbvElem;

	jbvElem = palloc(sizeof(JsonbValue));
	jbvElem->type = jbvString;
	jbvElem->val.string.val = PLyObject_AsString(obj);
	jbvElem->val.string.len = strlen(jbvElem->val.string.val);
	out = jbvElem;

	return (out);
}

/*
 * PySequence_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
 * Function to transform python lists to jsonbValue object.
 * The first argument is the Python list to be transformed.
 * The second one is conversion state.
 * Return value is the pointer to JsonbValue structure containing array.
 * */
static JsonbValue *
PySequence_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	JsonbValue *jbvElem;
	JsonbValue *out = NULL;
	int32		pcount;
	int32		i;

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
 * Function to transform python numerics to jsonbValue object.
 * The first argument is the Python numeric object to be transformed.
 * Return value is the pointer to JsonbValue structure containing the String.
 * */
static JsonbValue *
PyNumeric_ToJsonbValue(PyObject *obj)
{
	JsonbValue *out = NULL;
	JsonbValue *jbvInt;

	char	   *str = PLyObject_AsString(obj);

	if ((strcmp(str, "inf") != 0) && (strcmp(str, "-inf") != 0))
	{
		jbvInt = palloc(sizeof(JsonbValue));
		jbvInt->type = jbvNumeric;
		jbvInt->val.numeric = DatumGetNumeric(DirectFunctionCall3(
																  numeric_in,
																  CStringGetDatum(str),
																  0,
																  -1
																  ));
		out = jbvInt;
	}
	else
	{
		elog(ERROR, "jsonb doesn't support inf type yet");
	}
	return (out);
}

/*
 * PyObject_ToJsonbValue(PyObject *obj)
 * Function to transform python objects to jsonbValue object.
 * The first argument is the Python object to be transformed.
 * Return value is the pointer to JsonbValue structure containing the transformed object.
 * */
static JsonbValue *
PyObject_ToJsonbValue(PyObject *obj, JsonbParseState *jsonb_state)
{
	JsonbValue *out = NULL;
	PyObject   *type = PyObject_Type(obj);

	if (type == PyObject_Type(PyDict_New()))
	{
		/* DICT */
		out = PyMapping_ToJsonbValue(obj, jsonb_state);
	}
	else if (type == PyObject_Type(PyList_New(0)))
	{
		/* LIST or STRING */
		/* but we have checked on STRING */
		out = PySequence_ToJsonbValue(obj, jsonb_state);
	}
	else if (PyBool_Check(obj))
	{
		/* Boolean */
		JsonbValue *jbvElem;

		jbvElem = palloc(sizeof(JsonbValue));
		jbvElem->type = jbvBool;
		jbvElem->val.boolean = obj == Py_True;
		out = jbvElem;

	}
	else if (obj == Py_None)
	{
		/* None */
		JsonbValue *jbvElem;

		jbvElem = palloc(sizeof(JsonbValue));
		jbvElem->type = jbvNull;
		out = jbvElem;

	}
	else if (PyNumber_Check(obj))
	{
		/* NUMERIC */
		out = PyNumeric_ToJsonbValue(obj);
	}
	else
	{
		/* EVERYTHING ELSE */
		/* Handle it as it's repr */
		out = PyString_ToJsonbValue(obj);
	}
	return (out);
}

/*
 * plpython_to_jsonb(PyObject *obj)
 * Function to transform python objects to jsonb object.
 * The first argument is the Python object to be transformed.
 * Return value is the pointer to Jsonb structure containing the transformed object.
 * */
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
