/*-------------------------------------------------------------------------
 *
 * ddl_json.c
 *	  JSON code related to DDL command deparsing
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddl_json.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "tcop/ddl_deparse.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"


/*
 * Conversion specifier which determines how to expand the JSON element
 * into a string.
 */
typedef enum
{
	SpecDottedName,
	SpecIdentifier,
	SpecNumber,
	SpecOperatorName,
	SpecRole,
	SpecString,
	SpecStringLiteral,
	SpecTypeName
} convSpecifier;

/*
 * A ternary value that represents a boolean type JsonbValue.
 */
typedef enum
{
	tv_absent,
	tv_true,
	tv_false
}			json_trivalue;

static bool expand_one_jsonb_element(StringInfo buf, char *param,
									 JsonbValue *jsonval, convSpecifier specifier,
									 const char *fmt);
static void expand_jsonb_array(StringInfo buf, char *param,
							   JsonbValue *jsonarr, char *arraysep,
							   convSpecifier specifier, const char *fmt);
static void fmtstr_error_callback(void *arg);

/*
 * Given a JsonbContainer, find the JsonbValue with the given key name in it.
 * If it's of a type other than jbvBool, an error is raised. If it doesn't
 * exist, tv_absent is returned; otherwise return the actual json_trivalue.
 */
static json_trivalue
find_bool_in_jsonbcontainer(JsonbContainer *container, char *keyname)
{
	JsonbValue	key;
	JsonbValue *value;
	json_trivalue result;

	key.type = jbvString;
	key.val.string.val = keyname;
	key.val.string.len = strlen(keyname);
	value = findJsonbValueFromContainer(container,
										JB_FOBJECT, &key);
	if (value == NULL)
		return tv_absent;
	if (value->type != jbvBool)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" is not of type boolean", keyname));
	result = value->val.boolean ? tv_true : tv_false;
	pfree(value);

	return result;
}

/*
 * Given a JsonbContainer, find the JsonbValue with the given key name in it.
 * If it's of a type other than jbvString, an error is raised.  If it doesn't
 * exist, an error is raised unless missing_ok; otherwise return NULL.
 *
 * If it exists and is a string, a freshly palloc'ed copy is returned.
 *
 * If *length is not NULL, it is set to the length of the string.
 */
static char *
find_string_in_jsonbcontainer(JsonbContainer *container, char *keyname,
							  bool missing_ok, int *length)
{
	JsonbValue	key;
	JsonbValue *value;
	char	   *str;

	/* XXX verify that this is an object, not an array */

	key.type = jbvString;
	key.val.string.val = keyname;
	key.val.string.len = strlen(keyname);
	value = findJsonbValueFromContainer(container,
										JB_FOBJECT, &key);
	if (value == NULL)
	{
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("missing element \"%s\" in JSON object", keyname));
	}

	if (value->type != jbvString)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" is not of type string", keyname));

	str = pnstrdup(value->val.string.val, value->val.string.len);
	if (length)
		*length = value->val.string.len;
	pfree(value);
	return str;
}

#define ADVANCE_PARSE_POINTER(ptr,end_ptr) \
	do { \
		if (++(ptr) >= (end_ptr)) \
			ereport(ERROR, \
					errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					errmsg("unterminated format specifier")); \
	} while (0)

/*
 * Recursive helper for deparse_ddl_json_to_string.
 *
 * Find the "fmt" element in the given container, and expand it into the
 * provided StringInfo.
 */
static void
expand_fmt_recursive(StringInfo buf, JsonbContainer *container)
{
	JsonbValue	key;
	JsonbValue *value;
	const char *cp;
	const char *start_ptr;
	const char *end_ptr;
	int			len;

	start_ptr = find_string_in_jsonbcontainer(container, "fmt", false, &len);
	end_ptr = start_ptr + len;

	for (cp = start_ptr; cp < end_ptr; cp++)
	{
		convSpecifier specifier;
		bool		is_array = false;
		char	   *param = NULL;
		char	   *arraysep = NULL;

		if (*cp != '%')
		{
			appendStringInfoCharMacro(buf, *cp);
			continue;
		}

		ADVANCE_PARSE_POINTER(cp, end_ptr);

		/* Easy case: %% outputs a single % */
		if (*cp == '%')
		{
			appendStringInfoCharMacro(buf, *cp);
			continue;
		}

		/*
		 * Scan the mandatory element name.  Allow for an array separator
		 * (which may be the empty string) to be specified after a colon.
		 */
		if (*cp == '{')
		{
			StringInfoData parbuf;
			StringInfoData arraysepbuf;
			StringInfo	appendTo;

			initStringInfo(&parbuf);
			appendTo = &parbuf;

			ADVANCE_PARSE_POINTER(cp, end_ptr);
			while (cp < end_ptr)
			{
				if (*cp == ':')
				{
					/*
					 * Found array separator delimiter; element name is now
					 * complete, start filling the separator.
					 */
					initStringInfo(&arraysepbuf);
					appendTo = &arraysepbuf;
					is_array = true;
					ADVANCE_PARSE_POINTER(cp, end_ptr);
					continue;
				}

				if (*cp == '}')
				{
					ADVANCE_PARSE_POINTER(cp, end_ptr);
					break;
				}
				appendStringInfoCharMacro(appendTo, *cp);
				ADVANCE_PARSE_POINTER(cp, end_ptr);
			}
			param = parbuf.data;
			if (is_array)
				arraysep = arraysepbuf.data;
		}
		if (param == NULL)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("missing conversion name in conversion specifier"));

		switch (*cp)
		{
			case 'I':
				specifier = SpecIdentifier;
				break;
			case 'D':
				specifier = SpecDottedName;
				break;
			case 's':
				specifier = SpecString;
				break;
			case 'L':
				specifier = SpecStringLiteral;
				break;
			case 'T':
				specifier = SpecTypeName;
				break;
			case 'O':
				specifier = SpecOperatorName;
				break;
			case 'n':
				specifier = SpecNumber;
				break;
			case 'R':
				specifier = SpecRole;
				break;
			default:
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid conversion specifier \"%c\"", *cp));
		}

		/*
		 * Obtain the element to be expanded.
		 */
		key.type = jbvString;
		key.val.string.val = param;
		key.val.string.len = strlen(param);

		value = findJsonbValueFromContainer(container, JB_FOBJECT, &key);
		Assert(value != NULL);

		/*
		 * Expand the data (possibly an array) into the output StringInfo.
		 */
		if (is_array)
			expand_jsonb_array(buf, param, value, arraysep, specifier, start_ptr);
		else
			expand_one_jsonb_element(buf, param, value, specifier, start_ptr);
	}
}

/*
 * Expand a json value as a quoted identifier.  The value must be of type string.
 */
static void
expand_jsonval_identifier(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;

	Assert(jsonval->type == jbvString);

	str = pnstrdup(jsonval->val.string.val, jsonval->val.string.len);
	appendStringInfoString(buf, quote_identifier(str));
	pfree(str);
}

/*
 * Expand a json value as a dot-separated-name.  The value must be of type
 * binary and may contain elements "schemaname" (optional), "objname"
 * (mandatory), "attrname" (optional).  Double quotes are added to each element
 * as necessary, and dot separators where needed.
 *
 * One day we might need a "catalog" element as well, but no current use case
 * needs that.
 */
static void
expand_jsonval_dottedname(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;
	JsonbContainer *data = jsonval->val.binary.data;

	Assert(jsonval->type == jbvBinary);

	str = find_string_in_jsonbcontainer(data, "schemaname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, "%s.", quote_identifier(str));
		pfree(str);
	}

	str = find_string_in_jsonbcontainer(data, "objname", false, NULL);
	appendStringInfo(buf, "%s", quote_identifier(str));
	pfree(str);

	str = find_string_in_jsonbcontainer(data, "attrname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, ".%s", quote_identifier(str));
		pfree(str);
	}
}

/*
 * Expand a JSON value as a type name.
 */
static void
expand_jsonval_typename(StringInfo buf, JsonbValue *jsonval)
{
	char	   *schema = NULL;
	char	   *typename;
	char	   *typmodstr;
	json_trivalue is_array;
	char	   *array_decor;
	JsonbContainer *data = jsonval->val.binary.data;

	/*
	 * We omit schema-qualifying the output name if the schema element is
	 * either the empty string or NULL; the difference between those two cases
	 * is that in the latter we quote the type name, in the former we don't.
	 * This allows for types with special typmod needs, such as interval and
	 * timestamp (see format_type_detailed), while at the same time allowing
	 * for the schema name to be omitted from type names that require quotes
	 * but are to be obtained from a user schema.
	 */

	schema = find_string_in_jsonbcontainer(data, "schemaname", true, NULL);
	typename = find_string_in_jsonbcontainer(data, "typename", false, NULL);
	typmodstr = find_string_in_jsonbcontainer(data, "typmod", true, NULL);
	is_array = find_bool_in_jsonbcontainer(data, "typarray");
	switch (is_array)
	{
		case tv_true:
			array_decor = "[]";
			break;

		case tv_false:
			array_decor = "";
			break;

		case tv_absent:
		default:
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("missing typarray element"));
	}

	if (schema == NULL)
		appendStringInfo(buf, "%s", quote_identifier(typename));
	else if (schema[0] == '\0')
		appendStringInfo(buf, "%s", typename);	/* Special typmod needs */
	else
		appendStringInfo(buf, "%s.%s", quote_identifier(schema),
						 quote_identifier(typename));

	appendStringInfo(buf, "%s%s", typmodstr ? typmodstr : "", array_decor);
	pfree(schema);
	pfree(typename);
	pfree(typmodstr);
}

/*
 * Expand a JSON value as an operator name. The value may contain element
 * "schemaname" (optional).
 */
static void
expand_jsonval_operator(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;
	JsonbContainer *data = jsonval->val.binary.data;

	str = find_string_in_jsonbcontainer(data, "schemaname", true, NULL);
	/* Schema might be NULL or empty */
	if (str != NULL && str[0] != '\0')
	{
		appendStringInfo(buf, "%s.", quote_identifier(str));
		pfree(str);
	}

	str = find_string_in_jsonbcontainer(data, "objname", false, NULL);
	if (!str)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("missing operator name"));

	appendStringInfoString(buf, str);
	pfree(str);
}

/*
 * Expand a JSON value as a string.  The value must be of type string or of
 * type Binary.  In the latter case, it must contain a "fmt" element which will
 * be recursively expanded; also, if the object contains an element "present"
 * and it is set to false, the expansion is the empty string.
 *
 * Returns false if no actual expansion was made due to the "present" flag
 * being set to "false".
 *
 * The caller is responsible to check jsonval is of type jbvString or jbvBinary.
 */
static bool
expand_jsonval_string(StringInfo buf, JsonbValue *jsonval)
{
	bool expanded = false;

	if (jsonval->type == jbvString)
	{
		appendBinaryStringInfo(buf, jsonval->val.string.val,
							   jsonval->val.string.len);
		expanded = true;
	}
	else if (jsonval->type == jbvBinary)
	{
		json_trivalue present;

		present = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
											  "present");

		/*
		 * If "present" is set to false, this element expands to empty;
		 * otherwise (either true or absent), expand "fmt".
		 */
		if (present != tv_false)
		{
			expand_fmt_recursive(buf, jsonval->val.binary.data);
			expanded = true;
		}
	}

	return expanded;
}

/*
 * Expand a JSON value as a string literal.
 */
static void
expand_jsonval_strlit(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;
	StringInfoData dqdelim;
	static const char dqsuffixes[] = "_XYZZYX_";
	int			dqnextchar = 0;

	str = pnstrdup(jsonval->val.string.val, jsonval->val.string.len);

	/* Easy case: if there are no ' and no \, just use a single quote */
	if (strpbrk(str, "\'\\") == NULL)
	{
		appendStringInfo(buf, "'%s'", str);
		pfree(str);
		return;
	}

	/* Otherwise need to find a useful dollar-quote delimiter */
	initStringInfo(&dqdelim);
	appendStringInfoString(&dqdelim, "$");
	while (strstr(str, dqdelim.data) != NULL)
	{
		appendStringInfoChar(&dqdelim, dqsuffixes[dqnextchar++]);
		dqnextchar = dqnextchar % (sizeof(dqsuffixes) - 1);
	}
	/* Add trailing $ */
	appendStringInfoChar(&dqdelim, '$');

	/* And finally produce the quoted literal into the output StringInfo */
	appendStringInfo(buf, "%s%s%s", dqdelim.data, str, dqdelim.data);
	pfree(dqdelim.data);
	pfree(str);
}

/*
 * Expand a JSON value as an integer quantity.
 */
static void
expand_jsonval_number(StringInfo buf, JsonbValue *jsonval)
{
	char	   *strdatum;

	Assert(jsonval->type == jbvNumeric);

	strdatum = DatumGetCString(DirectFunctionCall1(numeric_out,
												   NumericGetDatum(jsonval->val.numeric)));
	appendStringInfoString(buf, strdatum);
	pfree(strdatum);
}

/*
 * Expand a JSON value as a role name.  If the 'is_public' element is set to
 * true, PUBLIC is expanded (no quotes); otherwise, expand the given role name,
 * quoting as an identifier.
 */
static void
expand_jsonval_role(StringInfo buf, JsonbValue *jsonval)
{
	json_trivalue is_public;

	is_public = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
											"is_public");
	if (is_public == tv_true)
		appendStringInfoString(buf, "PUBLIC");
	else
	{
		char	   *rolename;

		rolename = find_string_in_jsonbcontainer(jsonval->val.binary.data,
												 "rolename", false, NULL);
		if (rolename)
		{
			appendStringInfoString(buf, quote_identifier(rolename));
			pfree(rolename);
		}
	}
}

/*
 * Expand one JSON element into the output StringInfo according to the
 * conversion specifier.  The element type is validated, and an error is raised
 * if it doesn't match what we expect for the conversion specifier.
 *
 * Returns true, except for the formatted string case if no actual expansion
 * was made (due to the "present" flag being set to "false").
 */
static bool
expand_one_jsonb_element(StringInfo buf, char *param, JsonbValue *jsonval,
						 convSpecifier specifier, const char *fmt)
{
	bool		string_expanded = true;
	ErrorContextCallback sqlerrcontext;

	/* If we were given a format string, setup an ereport() context callback */
	if (fmt)
	{
		sqlerrcontext.callback = fmtstr_error_callback;
		sqlerrcontext.arg = (void *) fmt;
		sqlerrcontext.previous = error_context_stack;
		error_context_stack = &sqlerrcontext;
	}

	if (!jsonval)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" not found", param));

	switch (specifier)
	{
		case SpecIdentifier:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON string for %%I element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_identifier(buf, jsonval);
			break;

		case SpecDottedName:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON struct for %%D element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_dottedname(buf, jsonval);
			break;

		case SpecString:
			if (jsonval->type != jbvString &&
				jsonval->type != jbvBinary)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON string or struct for %%s element \"%s\", got %d",
							   param, jsonval->type));
			string_expanded = expand_jsonval_string(buf, jsonval);
			break;

		case SpecStringLiteral:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON string for %%L element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_strlit(buf, jsonval);
			break;

		case SpecTypeName:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON struct for %%T element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_typename(buf, jsonval);
			break;

		case SpecOperatorName:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON struct for %%O element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_operator(buf, jsonval);
			break;

		case SpecNumber:
			if (jsonval->type != jbvNumeric)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON numeric for %%n element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_number(buf, jsonval);
			break;

		case SpecRole:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("expected JSON struct for %%R element \"%s\", got %d",
							   param, jsonval->type));
			expand_jsonval_role(buf, jsonval);
			break;
	}

	if (fmt)
		error_context_stack = sqlerrcontext.previous;

	return string_expanded;
}

/*
 * Iterate on the elements of a JSON array, expanding each one into the output
 * StringInfo per the given conversion specifier, separated by the given
 * separator.
 */
static void
expand_jsonb_array(StringInfo buf, char *param,
				   JsonbValue *jsonarr, char *arraysep, convSpecifier specifier,
				   const char *fmt)
{
	ErrorContextCallback sqlerrcontext;
	JsonbContainer *container;
	JsonbIterator *it;
	JsonbValue	v;
	int			type;
	bool		first = true;
	StringInfoData arrayelem;

	/* If we were given a format string, setup an ereport() context callback */
	if (fmt)
	{
		sqlerrcontext.callback = fmtstr_error_callback;
		sqlerrcontext.arg = (void *) fmt;
		sqlerrcontext.previous = error_context_stack;
		error_context_stack = &sqlerrcontext;
	}

	if (!jsonarr)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" not found", param));

	if (jsonarr->type != jbvBinary)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" is not a JSON array", param));

	container = jsonarr->val.binary.data;
	if (!JsonContainerIsArray(container))
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("element \"%s\" is not a JSON array", param));

	initStringInfo(&arrayelem);

	it = JsonbIteratorInit(container);
	while ((type = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (type == WJB_ELEM)
		{
			resetStringInfo(&arrayelem);

			if (expand_one_jsonb_element(&arrayelem, param, &v, specifier, NULL))
			{
				if (!first)
					appendStringInfoString(buf, arraysep);

				appendBinaryStringInfo(buf, arrayelem.data, arrayelem.len);
				first = false;
			}
		}
	}

	if (fmt)
		error_context_stack = sqlerrcontext.previous;
}

/*
 * Workhorse for ddl_deparse_expand_command.
 */
char *
deparse_ddl_json_to_string(char *json_str, char** owner)
{
	Datum		d;
	Jsonb	   *jsonb;
	StringInfo	buf = (StringInfo) palloc0(sizeof(StringInfoData));

	initStringInfo(buf);

	d = DirectFunctionCall1(jsonb_in, PointerGetDatum(json_str));
	jsonb = (Jsonb *) DatumGetPointer(d);

	if (owner != NULL)
	{
		const char *key = "myowner";
		JsonbValue *value;

		value = getKeyJsonValueFromContainer(&jsonb->root, key, strlen(key), NULL);
		if (value)
		{
			char *str;

			/* value->val.string.val may not be NULL terminated */
			str = palloc(value->val.string.len + 1);
			memcpy(str, value->val.string.val, value->val.string.len);
			str[value->val.string.len] = '\0';
			*owner = str;
		}
		else
			/* myowner is not given in this jsonb, e.g. for Drop Commands */
			*owner = NULL;
	}

	expand_fmt_recursive(buf, &jsonb->root);

	return buf->data;
}

/*------
 * Returns a formatted string from a JSON object.
 *
 * The starting point is the element named "fmt" (which must be a string).
 * This format string may contain zero or more %-escapes, which consist of an
 * element name enclosed in { }, possibly followed by a conversion modifier,
 * followed by a conversion specifier.  Possible conversion specifiers are:
 *
 * %		expand to a literal %
 * I		expand as a single, non-qualified identifier
 * D		expand as a possibly-qualified identifier
 * T		expand as a type name
 * O		expand as an operator name
 * L		expand as a string literal (quote using single quotes)
 * s		expand as a simple string (no quoting)
 * n		expand as a simple number (no quoting)
 * R		expand as a role name (possibly quoted name, or PUBLIC)
 *
 * The element name may have an optional separator specification preceded
 * by a colon.  Its presence indicates that the element is expected to be
 * an array; the specified separator is used to join the array elements.
 *------
 */
Datum
ddl_deparse_expand_command(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_P(0);
	char	   *json_str;

	json_str = text_to_cstring(json);

	PG_RETURN_TEXT_P(cstring_to_text(deparse_ddl_json_to_string(json_str, NULL)));
}

/*
 * Error context callback for JSON format string expansion.
 *
 * XXX: indicate which element we're expanding, if applicable.
 */
static void
fmtstr_error_callback(void *arg)
{
	errcontext("while expanding format string \"%s\"", (char *) arg);
}
