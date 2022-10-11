/*-------------------------------------------------------------------------
 *
 * ddl_json.c
 *	  JSON code related to DDL command deparsing
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/ddl_json.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"


/*
 * Conversion specifier, it determines how we expand the json element into
 * string.
 */
typedef enum
{
	SpecTypename,
	SpecOperatorname,
	SpecDottedName,
	SpecString,
	SpecNumber,
	SpecStringLiteral,
	SpecIdentifier,
	SpecRole
} convSpecifier;

/*
 * A ternary value which represents a boolean type JsonbValue.
 */
typedef enum
{
	tv_absent,
	tv_true,
	tv_false
} json_trivalue;

static bool expand_one_jsonb_element(StringInfo buf, char *param,
						 JsonbValue *jsonval, convSpecifier specifier,
						 const char *fmt);
static void expand_jsonb_array(StringInfo buf, char *param,
				   JsonbValue *jsonarr, char *arraysep,
				   convSpecifier specifier, const char *fmt);
static void fmtstr_error_callback(void *arg);
char *ddl_deparse_json_to_string(char *jsonb);

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
	json_trivalue	result;

	key.type = jbvString;
	key.val.string.val = keyname;
	key.val.string.len = strlen(keyname);
	value = findJsonbValueFromContainer(container,
										JB_FOBJECT, &key);
	if (value == NULL)
		return tv_absent;
	if (value->type != jbvBool)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not of type boolean",
						keyname)));
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
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing element \"%s\" in json object", keyname)));
	}

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
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					 errmsg("unterminated format specifier"))); \
	} while (0)

/*
 * Recursive helper for pg_event_trigger_expand_command
 *
 * Find the "fmt" element in the given container, and expand it into the
 * provided StringInfo.
 */
static void
expand_fmt_recursive(JsonbContainer *container, StringInfo buf)
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
		 * (which may be the empty string) to be specified after colon.
		 */
		if (*cp == '{')
		{
			StringInfoData parbuf;
			StringInfoData arraysepbuf;
			StringInfo	appendTo;

			initStringInfo(&parbuf);
			appendTo = &parbuf;

			ADVANCE_PARSE_POINTER(cp, end_ptr);
			for (; cp < end_ptr;)
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
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing conversion name in conversion specifier")));

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
				specifier = SpecTypename;
				break;
			case 'O':
				specifier = SpecOperatorname;
				break;
			case 'n':
				specifier = SpecNumber;
				break;
			case 'R':
				specifier = SpecRole;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid conversion specifier \"%c\"", *cp)));
		}

		/*
		 * Obtain the element to be expanded.
		 */
		key.type = jbvString;
		key.val.string.val = param;
		key.val.string.len = strlen(param);

		value = findJsonbValueFromContainer(container, JB_FOBJECT, &key);

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
 * object and may contain elements "schemaname" (optional), "objname"
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

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"schemaname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, "%s.", quote_identifier(str));
		pfree(str);
	}

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"objname", false, NULL);
	appendStringInfo(buf, "%s", quote_identifier(str));
	pfree(str);

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"attrname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, ".%s", quote_identifier(str));
		pfree(str);
	}
}

/*
 * Expand a json value as a type name.
 */
static void
expand_jsonval_typename(StringInfo buf, JsonbValue *jsonval)
{
	char	   *schema = NULL;
	char	   *typename;
	char	   *typmodstr;
	json_trivalue	is_array;
	char	   *array_decor;

	/*
	 * We omit schema-qualifying the output name if the schema element is
	 * either the empty string or NULL; the difference between those two cases
	 * is that in the latter we quote the type name, in the former we don't.
	 * This allows for types with special typmod needs, such as interval and
	 * timestamp (see format_type_detailed), while at the same time allowing
	 * for the schema name to be omitted from type names that require quotes
	 * but are to be obtained from a user schema.
	 */

	schema = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										   "schemaname", true, NULL);
	typename = find_string_in_jsonbcontainer(jsonval->val.binary.data,
											 "typename", false, NULL);
	typmodstr = find_string_in_jsonbcontainer(jsonval->val.binary.data,
											  "typmod", true, NULL);
	is_array = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
										   "typarray");
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
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("missing typarray element")));
	}

	if (schema == NULL)
		appendStringInfo(buf, "%s", quote_identifier(typename));
	else if (schema[0] == '\0')
		appendStringInfo(buf, "%s", typename); /* Special typmod needs */
	else
		appendStringInfo(buf, "%s.%s", quote_identifier(schema),
						 quote_identifier(typename));

	appendStringInfo(buf, "%s%s", typmodstr ? typmodstr : "", array_decor);
}

/*
 * Expand a json value as an operator name
 */
static void
expand_jsonval_operator(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"schemaname", true, NULL);
	/* Schema might be NULL or empty */
	if (str != NULL && str[0] != '\0')
	{
		appendStringInfo(buf, "%s.", quote_identifier(str));
		pfree(str);
	}

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"objname", false, NULL);

	if (str)
	{
		appendStringInfoString(buf, str);
		pfree(str);
	}
}

/*
 * Expand a json value as a string.  The value must be of type string or of
 * type object.  In the latter case it must contain a "fmt" element which will
 * be recursively expanded; also, if the object contains an element "present"
 * and it is set to false, the expansion is the empty string.
 *
 * Returns false if no actual expansion was made due to the "present" flag
 * being set to "false".
 */
static bool
expand_jsonval_string(StringInfo buf, JsonbValue *jsonval)
{
	if (jsonval->type == jbvString)
	{
		appendBinaryStringInfo(buf, jsonval->val.string.val,
							   jsonval->val.string.len);
	}
	else if (jsonval->type == jbvBinary)
	{
		json_trivalue	present;

		present = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
											  "present");
		/*
		 * If "present" is set to false, this element expands to empty;
		 * otherwise (either true or absent), fall through to expand "fmt".
		 */
		if (present == tv_false)
			return false;

		expand_fmt_recursive(jsonval->val.binary.data, buf);
	}
	else
		return false;

	return true;
}

/*
 * Expand a json value as a string literal.
 */
static void
expand_jsonval_strlit(StringInfo buf, JsonbValue *jsonval)
{
	char   *str;
	StringInfoData dqdelim;
	static const char dqsuffixes[] = "_XYZZYX_";
	int         dqnextchar = 0;

	str = pnstrdup(jsonval->val.string.val, jsonval->val.string.len);

	/* Easy case: if there are no ' and no \, just use a single quote */
	if (strchr(str, '\'') == NULL &&
		strchr(str, '\\') == NULL)
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
 * Expand a json value as an integer quantity.
 */
static void
expand_jsonval_number(StringInfo buf, JsonbValue *jsonval)
{
	char *strdatum;

	strdatum = DatumGetCString(DirectFunctionCall1(numeric_out,
												   NumericGetDatum(jsonval->val.numeric)));
	appendStringInfoString(buf, strdatum);
}

/*
 * Expand a json value as a role name.  If the is_public element is set to
 * true, PUBLIC is expanded (no quotes); otherwise, expand the given role name,
 * quoting as an identifier.
 */
static void
expand_jsonval_role(StringInfo buf, JsonbValue *jsonval)
{
	json_trivalue	is_public;

	is_public = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
											"is_public");
	if (is_public == tv_true)
		appendStringInfoString(buf, "PUBLIC");
	else
	{
		char *rolename;

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
 * Expand one json element into the output StringInfo according to the
 * conversion specifier.  The element type is validated, and an error is raised
 * if it doesn't match what we expect for the conversion specifier.
 *
 * Returns false if no actual expansion was made (due to the "present" flag
 * being set to "false" in formatted string expansion).
 */
static bool
expand_one_jsonb_element(StringInfo buf, char *param, JsonbValue *jsonval,
						 convSpecifier specifier, const char *fmt)
{
	bool result = true;
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
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" not found", param)));

	switch (specifier)
	{
		case SpecIdentifier:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string for %%I element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_identifier(buf, jsonval);
			break;

		case SpecDottedName:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%D element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_dottedname(buf, jsonval);
			break;

		case SpecString:
			if (jsonval->type != jbvString &&
				jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string or object for %%s element \"%s\", got %d",
								param, jsonval->type)));
			result = expand_jsonval_string(buf, jsonval);
			break;

		case SpecStringLiteral:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string for %%L element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_strlit(buf, jsonval);
			break;

		case SpecTypename:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%T element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_typename(buf, jsonval);
			break;

		case SpecOperatorname:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%O element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_operator(buf, jsonval);
			break;

		case SpecNumber:
			if (jsonval->type != jbvNumeric)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON numeric for %%n element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_number(buf, jsonval);
			break;

		case SpecRole:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%R element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_role(buf, jsonval);
			break;
	}

	if (fmt)
		error_context_stack = sqlerrcontext.previous;

	return result;
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
	JsonbIterator  *it;
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

	if (jsonarr->type != jbvBinary)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not a JSON array", param)));

	container = jsonarr->val.binary.data;
	if (!JsonContainerIsArray(container))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not a JSON array", param)));

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
ddl_deparse_json_to_string(char *json_str)
{
	Datum		d;
	Jsonb	   *jsonb;
	StringInfo buf = (StringInfo) palloc0(sizeof(StringInfoData));

	initStringInfo(buf);

	d = DirectFunctionCall1(jsonb_in, PointerGetDatum(json_str));
	jsonb = (Jsonb *) DatumGetPointer(d);

	expand_fmt_recursive(&jsonb->root, buf);

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
 * %		expand to a literal %.
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

	PG_RETURN_TEXT_P(cstring_to_text(ddl_deparse_json_to_string(json_str)));
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
