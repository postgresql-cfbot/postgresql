/*-------------------------------------------------------------------------
 *
 * options.c
 *	  An uniform, context-free API for processing name=value options. Used
 *	  to process relation options (reloptions), attribute options, opclass
 *	  options, etc.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/options.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"


/*
 * OPTIONS SPECIFICATION and OPTION SPECIFICATION SET
 *
 * Each option is defined via Option Specification object (Option Spec).
 * Option Spec should have all information that is needed for processing
 * (parsing, validating, converting) of a single option. Implemented via set of
 * option_spec_* structures.
 *
 * A set of Option Specs (Options Spec Set), defines all options available for
 * certain object (certain relation kind for example). It is a list of
 * Options Specs, plus validation functions that can be used to validate whole
 * option set, if needed. Implemented via options_spec_set structure and set of
 * optionsSpecSetAdd* functions that are used for adding Option Specs items to
 * a Set.
 *
 * NOTE: we choose therm "specification" instead of "definition" because therm
 * "definition" is used for objects that came from syntax parser. So to avoid
 * confusion here we have Option Specifications, and all "definitions" are from
 * parser.
 */

/*
 * OPTION VALUES REPRESENTATIONS
 *
 * Option values usually came from syntax parser in form of defList object,
 * stored in pg_catalog as text array, and used when they are stored in memory
 * as C-structure. These are different option values representations. Here goes
 * brief description of all representations used in the code.
 *
 * Value List
 *
 * Value List is an internal representation that is used while converting
 * option values between different representation. Value List item is called
 * "parsed", when Value's value is converted to a proper data type and
 * validated, or is called "unparsed", when Value's value is stored as raw
 * string that was obtained from the source without any checks. In conversion
 * function names first case is referred as Values, second case is referred as
 * RawValues. Value List is implemented as List of option_value C-structures.
 *
 * defList
 *
 * Options in form of definition List that comes from syntax parser. (For
 * reloptions it is a part of SQL query that goes after WITH, SET or RESET
 * keywords). Can be converted to Value List using optionsDefListToRawValues
 * and can be obtained from TEXT[] via optionsTextArrayToDefList functions.
 *
 * TEXT[]
 *
 * Options in form suitable for storig in TEXT[] field in DB. (E.g. reloptions
 * are stores in pg_catalog.pg_class table in reloptions field). Can be
 * converted to and from Value List using optionsValuesToTextArray and
 * optionsTextArrayToRawValues functions.
 *
 * Bytea
 *
 * Option data stored in C-structure with varlena header in the beginning of
 * the structure. This representation is used to pass option values to the core
 * postgres. It is fast to read, it can be cached and so on. Bytea
 * representation can be obtained from Vale List using optionsValuesToBytea
 * function, and can't be converted back.
 */

/*
 * OPTION STRING VALUE NOTION
 *
 * Important thing for bytea representation is that all data should be stored
 * in one bytea chunk, including values of the string options. This is needed
 * as  * bytea representation is cached, and can freed, moved or recreated again
 * without any notion, so it can't have parts allocated separately.
 *
 * Thus memory chunk for Bytea option values representation is divided into two
 * parts. First goes a C-structure that stores fixed length vales. Then goes
 * memory area reserved for string values.
 *
 * For string values C-structure stores offsets (not pointers). These offsets
 * can be used to access attached string value:
 *
 * String_pointer = Bytea_head_pointer + offest.
 */

static option_spec_basic *allocateOptionSpec(int type, const char *name,
											 const char *desc, LOCKMODE lockmode,
											 int struct_offset, bool is_local,
											 option_value_postvalidate postvalidate_fn);

static void parse_one_option(option_value *option, bool validate);
static void *optionsAllocateBytea(options_spec_set *spec_set, List *options);


static List *optionsDefListToRawValues(List *defList, bool is_for_reset);
static Datum optionsValuesToTextArray(List *options_values);
static List *optionsMergeOptionValues(List *old_options, List *new_options);

/*
 * allocateOptionsSpecSet
 *		Creates new Option Spec Set object: Allocates memory and initializes
 *		structure members.
 *
 * Spec Set items can be add via allocateOptionSpec and optionSpecSetAddItem
 * functions or by calling directly any of optionsSpecSetAdd* function
 * (preferable way)
 *
 * namespace - Spec Set can be bind to certain namespace (E.g.
 * namespace.option=value). Options from other namespaces will be ignored while
 * processing. If set to NULL, no namespace will be used at all.
 *
 * size_of_bytea - size of target structure of Bytea options representation
 *
 * num_items_expected - if you know expected number of Spec Set items set it
 * here. Set to -1 in other cases. num_items_expected will be used for
 * preallocating memory and will trigger error, if you try to add more items
 * than you expected.
 */

options_spec_set *
allocateOptionsSpecSet(const char *namspace, int bytea_size, bool is_local,
					   int num_items_expected)
{
	MemoryContext oldcxt;
	options_spec_set *spec_set;

	if (!is_local)
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	spec_set = palloc(sizeof(options_spec_set));
	if (namspace)
	{
		spec_set->namspace = palloc(strlen(namspace) + 1);
		strcpy(spec_set->namspace, namspace);
	}
	else
		spec_set->namspace = NULL;
	if (num_items_expected > 0)
	{
		spec_set->num_allocated = num_items_expected;
		spec_set->assert_on_realloc = true;
		spec_set->definitions = palloc(
									   spec_set->num_allocated * sizeof(option_spec_basic *));
	}
	else
	{
		spec_set->num_allocated = 0;
		spec_set->assert_on_realloc = false;
		spec_set->definitions = NULL;
	}
	spec_set->num = 0;
	spec_set->struct_size = bytea_size;
	spec_set->postprocess_fun = NULL;
	spec_set->is_local = is_local;
	if (!is_local)
		MemoryContextSwitchTo(oldcxt);
	return spec_set;
}

/*
 * allocateOptionSpec
 *		Allocates a new Option Specifiation object of desired type and
 *		initialize the type-independent fields
 */
static option_spec_basic *
allocateOptionSpec(int type, const char *name, const char *desc,
				   LOCKMODE lockmode, int struct_offset, bool is_local,
				   option_value_postvalidate postvalidate_fn)
{
	MemoryContext oldcxt;
	size_t		size;
	option_spec_basic *newoption;

	if (!is_local)
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	switch (type)
	{
		case OPTION_TYPE_BOOL:
			size = sizeof(option_spec_bool);
			break;
		case OPTION_TYPE_INT:
			size = sizeof(option_spec_int);
			break;
		case OPTION_TYPE_REAL:
			size = sizeof(option_spec_real);
			break;
		case OPTION_TYPE_ENUM:
			size = sizeof(option_spec_enum);
			break;
		case OPTION_TYPE_STRING:
			size = sizeof(option_spec_string);
			break;
		default:
			elog(ERROR, "unsupported reloption type %d", type);
			return NULL;		/* keep compiler quiet */
	}

	newoption = palloc(size);

	newoption->name = pstrdup(name);
	if (desc)
		newoption->desc = pstrdup(desc);
	else
		newoption->desc = NULL;
	newoption->type = type;
	newoption->lockmode = lockmode;
	newoption->struct_offset = struct_offset;
	newoption->postvalidate_fn = postvalidate_fn;

	if (!is_local)
		MemoryContextSwitchTo(oldcxt);

	return newoption;
}

/*
 * optionSpecSetAddItem
 *		Adds pre-created Option Specification objec to the Spec Set
 */
static void
optionSpecSetAddItem(option_spec_basic *newoption,
					 options_spec_set *spec_set)
{
	if (spec_set->num >= spec_set->num_allocated)
	{
		MemoryContext oldcxt = NULL;

		Assert(!spec_set->assert_on_realloc);
		if (!spec_set->is_local)
			oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		if (spec_set->num_allocated == 0)
		{
			spec_set->num_allocated = 8;
			spec_set->definitions = palloc(
										   spec_set->num_allocated * sizeof(option_spec_basic *));
		}
		else
		{
			spec_set->num_allocated *= 2;
			spec_set->definitions = repalloc(spec_set->definitions,
											 spec_set->num_allocated * sizeof(option_spec_basic *));
		}
		if (!spec_set->is_local)
			MemoryContextSwitchTo(oldcxt);
	}
	spec_set->definitions[spec_set->num] = newoption;
	spec_set->num++;
}


/*
 * optionsSpecSetAddBool
 *		Adds boolean Option Specification entry to the Spec Set
 */
void
optionsSpecSetAddBool(options_spec_set *spec_set, const char *name,
					  const char *desc, LOCKMODE lockmode, int struct_offset,
					  option_value_postvalidate postvalidate_fn,
					  bool default_val)
{
	option_spec_bool *spec_set_item;

	spec_set_item = (option_spec_bool *) allocateOptionSpec(OPTION_TYPE_BOOL,
										name, desc, lockmode, struct_offset,
										spec_set->is_local, postvalidate_fn);

	spec_set_item->default_val = default_val;

	optionSpecSetAddItem((option_spec_basic *) spec_set_item, spec_set);
}

/*
 * optionsSpecSetAddInt
 *		Adds integer Option Specification entry to the Spec Set
 */
void
optionsSpecSetAddInt(options_spec_set *spec_set, const char *name,
					 const char *desc, LOCKMODE lockmode, int struct_offset,
					 option_value_postvalidate postvalidate_fn,
					 int default_val, int min_val, int max_val)
{
	option_spec_int *spec_set_item;

	spec_set_item = (option_spec_int *) allocateOptionSpec(OPTION_TYPE_INT,
										  name, desc, lockmode, struct_offset,
										  spec_set->is_local, postvalidate_fn);

	spec_set_item->default_val = default_val;
	spec_set_item->min = min_val;
	spec_set_item->max = max_val;

	optionSpecSetAddItem((option_spec_basic *) spec_set_item, spec_set);
}

/*
 * optionsSpecSetAddReal
 *		Adds float Option Specification entry to the Spec Set
 */
void
optionsSpecSetAddReal(options_spec_set *spec_set, const char *name,
					  const char *desc, LOCKMODE lockmode, int struct_offset,
					  option_value_postvalidate postvalidate_fn,
					  double default_val, double min_val, double max_val)
{
	option_spec_real *spec_set_item;

	spec_set_item = (option_spec_real *) allocateOptionSpec(OPTION_TYPE_REAL,
										name, desc, lockmode, struct_offset,
										spec_set->is_local, postvalidate_fn);

	spec_set_item->default_val = default_val;
	spec_set_item->min = min_val;
	spec_set_item->max = max_val;

	optionSpecSetAddItem((option_spec_basic *) spec_set_item, spec_set);
}

/*
 * optionsSpecSetAddEnum
 *		Adds enum Option Specification entry to the Spec Set
 *
 * The members array must have a terminating NULL entry.
 *
 * The detailmsg is shown when unsupported values are passed, and has this
 * form:   "Valid values are \"foo\", \"bar\", and \"bar\"."
 *
 * The members array and detailmsg are not copied -- caller must ensure that
 * they are valid throughout the life of the process.
 */

void
optionsSpecSetAddEnum(options_spec_set *spec_set, const char *name,
					  const char *desc, LOCKMODE lockmode, int struct_offset,
					  option_value_postvalidate postvalidate_fn,
					  opt_enum_elt_def *members, int default_val,
					  const char *detailmsg)
{
	option_spec_enum *spec_set_item;

	spec_set_item = (option_spec_enum *) allocateOptionSpec(OPTION_TYPE_ENUM,
										name, desc, lockmode, struct_offset,
										spec_set->is_local, postvalidate_fn);

	spec_set_item->default_val = default_val;
	spec_set_item->members = members;
	spec_set_item->detailmsg = detailmsg;

	optionSpecSetAddItem((option_spec_basic *) spec_set_item, spec_set);
}

/*
 * optionsSpecSetAddString
 *		Adds string Option Specification entry to the Spec Set
 *
 * "validator" is an optional function pointer that can be used to test the
 * validity of the values. It must elog(ERROR) when the argument string is
 * not acceptable for the variable. Note that the default value must pass
 * the validation.
 */
void
optionsSpecSetAddString(options_spec_set *spec_set, const char *name,
					const char *desc, LOCKMODE lockmode, int struct_offset,
					option_value_postvalidate postvalidate_fn,
					const char *default_val, validate_string_option validator,
					fill_string_option filler)
{
	option_spec_string *spec_set_item;

	/* make sure the validator/default combination is sane */
	if (validator)
		(validator) (default_val);

	spec_set_item = (option_spec_string *) allocateOptionSpec(
										  OPTION_TYPE_STRING,
										  name, desc, lockmode, struct_offset,
										  spec_set->is_local, postvalidate_fn);
	spec_set_item->validate_cb = validator;
	spec_set_item->fill_cb = filler;

	if (default_val)
		spec_set_item->default_val = MemoryContextStrdup(TopMemoryContext,
														 default_val);
	else
		spec_set_item->default_val = NULL;
	optionSpecSetAddItem((option_spec_basic *) spec_set_item, spec_set);
}

/* optionsDefListToRawValues
 *		Converts options values from DefList representation into Raw Values
 *		List.
 *
 * No parsing is done here except for checking that RESET syntax is correct
 * (i.e. does not have =name part of value=name template). Syntax analyzer does
 * not see difference between SET and RESET cases, so we should treat it here
 * manually
 */
static List *
optionsDefListToRawValues(List *defList, bool is_for_reset)
{
	ListCell   *cell;
	List	   *result = NIL;

	foreach(cell, defList)
	{
		option_value *option_dst;
		DefElem    *def = (DefElem *) lfirst(cell);
		char	   *value;

		option_dst = palloc(sizeof(option_value));

		if (def->defnamespace)
		{
			option_dst->namspace = palloc(strlen(def->defnamespace) + 1);
			strcpy(option_dst->namspace, def->defnamespace);
		}
		else
			option_dst->namspace = NULL;

		option_dst->raw_name = palloc(strlen(def->defname) + 1);
		strcpy(option_dst->raw_name, def->defname);

		if (is_for_reset)
		{
			/*
			 * If this option came from RESET statement we should throw error
			 * it it brings us name=value data, as syntax analyzer do not
			 * prevent it
			 */
			if (def->arg != NULL)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("RESET must not include values for parameters")));

			option_dst->status = OPTION_VALUE_STATUS_FOR_RESET;
		}
		else
		{
			/*
			 * For SET statement we should treat (name) expression as if it is
			 * actually (name=true) so do it here manually. In other cases
			 * just use value as we should use it
			 */
			option_dst->status = OPTION_VALUE_STATUS_RAW;
			if (def->arg != NULL)
				value = defGetString(def);
			else
				value = "true";
			option_dst->raw_value = palloc(strlen(value) + 1);
			strcpy(option_dst->raw_value, value);
		}

		result = lappend(result, option_dst);
	}
	return result;
}

/*
 * optionsValuesToTextArray
 *		Converts options Values List (option_values) into TEXT[] representation
 *
 * This conversion is usually needed for saving option values into database
 * (e.g. to store reloptions in pg_class.reloptions)
 */

Datum
optionsValuesToTextArray(List *options_values)
{
	ArrayBuildState *astate = NULL;
	ListCell   *cell;
	Datum		result;

	foreach(cell, options_values)
	{
		option_value *option = (option_value *) lfirst(cell);
		const char *name;
		char	   *value;
		text	   *t;
		int			len;

		/*
		 * Raw value were not cleared while parsing, so instead of converting
		 * it back, just use it to store value as text
		 */
		value = option->raw_value;

		Assert(option->status != OPTION_VALUE_STATUS_EMPTY);

		/*
		 * Name will be taken from option definition, if option were parsed or
		 * from raw_name if option were not parsed for some reason
		 */
		if (option->status == OPTION_VALUE_STATUS_PARSED)
			name = option->gen->name;
		else
			name = option->raw_name;

		/*
		 * Now build "name=value" string and append it to the array
		 */
		len = VARHDRSZ + strlen(name) + strlen(value) + 1;
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s=%s", name, value);
		astate = accumArrayResult(astate, PointerGetDatum(t), false,
								  TEXTOID, CurrentMemoryContext);
	}
	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;
}

/*
 * optionsTextArrayToRawValues
 *		Converts option values from TEXT[] representation (datum_array) into
 *		 Raw Values List.
 *
 * Used while fetching options values from DB, as a first step of converting
 * them to other representations.
 */
List *
optionsTextArrayToRawValues(Datum array_datum)
{
	List	   *result = NIL;

	if (PointerIsValid(DatumGetPointer(array_datum)))
	{
		ArrayType  *array = DatumGetArrayTypeP(array_datum);
		Datum	   *options;
		int			noptions;
		int			i;

		deconstruct_array_builtin(array, TEXTOID, &options, NULL, &noptions);

		for (i = 0; i < noptions; i++)
		{
			option_value *option_dst;
			char	   *text_str = VARDATA(options[i]);
			int			text_len = VARSIZE(options[i]) - VARHDRSZ;
			int			j;
			int			name_len = -1;
			char	   *name;
			int			raw_value_len;
			char	   *raw_value;

			/*
			 * Find position of '=' sign and treat id as a separator between
			 * name and value in "name=value" item
			 */
			for (j = 0; j < text_len; j = j + pg_mblen(text_str))
			{
				if (text_str[j] == '=')
				{
					name_len = j;
					break;
				}
			}
			Assert(name_len >= 1);	/* Just in case */

			raw_value_len = text_len - name_len - 1;

			/*
			 * Copy name from src
			 */
			name = palloc(name_len + 1);
			memcpy(name, text_str, name_len);
			name[name_len] = '\0';

			/*
			 * Copy value from src
			 */
			raw_value = palloc(raw_value_len + 1);
			memcpy(raw_value, text_str + name_len + 1, raw_value_len);
			raw_value[raw_value_len] = '\0';

			/*
			 * Create new option_value item
			 */
			option_dst = palloc(sizeof(option_value));
			option_dst->status = OPTION_VALUE_STATUS_RAW;
			option_dst->raw_name = name;
			option_dst->raw_value = raw_value;
			option_dst->namspace = NULL;

			result = lappend(result, option_dst);
		}
	}
	return result;
}

/*
 * optionsMergeOptionValues
 *		Updates(or Resets) values from one Options Values List(old_options),
 *		with values from another Options Values List (new_options)

 * This function is used while ALTERing options of some object.
 * If option from new_options list has OPTION_VALUE_STATUS_FOR_RESET flag
 * on, option with that name will be excluded result list.
 */
static List *
optionsMergeOptionValues(List *old_options, List *new_options)
{
	List	   *result = NIL;
	ListCell   *old_cell;
	ListCell   *new_cell;

	/*
	 * First add to result all old options that are not mentioned in new list
	 */
	foreach(old_cell, old_options)
	{
		bool		found;
		const char *old_name;
		option_value *old_option;

		old_option = (option_value *) lfirst(old_cell);
		if (old_option->status == OPTION_VALUE_STATUS_PARSED)
			old_name = old_option->gen->name;
		else
			old_name = old_option->raw_name;

		/*
		 * Looking for a new option with same name
		 */
		found = false;
		foreach(new_cell, new_options)
		{
			option_value *new_option;
			const char *new_name;

			new_option = (option_value *) lfirst(new_cell);
			if (new_option->status == OPTION_VALUE_STATUS_PARSED)
				new_name = new_option->gen->name;
			else
				new_name = new_option->raw_name;

			if (strcmp(new_name, old_name) == 0)
			{
				found = true;
				break;
			}
		}
		if (!found)
			result = lappend(result, old_option);
	}

	/*
	 * Now add all to result all new options that are not designated for reset
	 */
	foreach(new_cell, new_options)
	{
		option_value *new_option;

		new_option = (option_value *) lfirst(new_cell);

		if (new_option->status != OPTION_VALUE_STATUS_FOR_RESET)
			result = lappend(result, new_option);
	}
	return result;
}

/*
 * optionsDefListValdateNamespaces
 *		Checks that defList has only options with namespaces from
 *		allowed_namspaces array. Items without namspace are also accepted
 *
 * Used while validation of syntax parser output. Error is thrown if unproper
 * namespace is found.
 */
void
optionsDefListValdateNamespaces(List *defList, char **allowed_namspaces)
{
	ListCell   *cell;

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/*
		 * Checking namespace only for options that have namespaces. Options
		 * with no namespaces are always accepted
		 */
		if (def->defnamespace)
		{
			bool		found = false;
			int			i = 0;

			while (allowed_namspaces[i])
			{
				if (strcmp(def->defnamespace,
						   allowed_namspaces[i]) == 0)
				{
					found = true;
					break;
				}
				i++;
			}
			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized parameter namespace \"%s\"",
								def->defnamespace)));
		}
	}
}

/*
 * optionsDefListFilterNamespaces
 *		Filter out DefList items that has "namspace" namespace. If "namspace"
 *		is NULL, only namespaseless options are returned
 */
List *
optionsDefListFilterNamespaces(List *defList, const char *namspace)
{
	ListCell   *cell;
	List	   *result = NIL;

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if ((!namspace && !def->defnamespace) ||
			(namspace && def->defnamespace &&
			 strcmp(namspace, def->defnamespace) == 0))
			result = lappend(result, def);
	}
	return result;
}

/*
 * optionsTextArrayToDefList
 *		Converts option values from TEXT[] representation into DefList
 *		representation.
 */
List *
optionsTextArrayToDefList(Datum options)
{
	List	   *result = NIL;
	ArrayType  *array;
	Datum	   *optiondatums;
	int			noptions;
	int			i;

	/* Nothing to do if no options */
	if (!PointerIsValid(DatumGetPointer(options)))
		return result;

	array = DatumGetArrayTypeP(options);

	deconstruct_array_builtin(array, TEXTOID, &optiondatums, NULL, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char	   *s;
		char	   *p;
		Node	   *val = NULL;

		s = TextDatumGetCString(optiondatums[i]);
		p = strchr(s, '=');
		if (p)
		{
			*p++ = '\0';
			val = (Node *) makeString(pstrdup(p));
		}
		result = lappend(result, makeDefElem(pstrdup(s), val, -1));
	}

	return result;
}

/*
 * optionsDefListToTextArray
 *		Converts option values from DefList representation into TEXT[]
 *		representation.
 */
Datum
optionsDefListToTextArray(List *defList)
{
	ListCell   *cell;
	Datum		result;
	ArrayBuildState *astate = NULL;

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		const char *name = def->defname;
		const char *value;
		text	   *t;
		int			len;

		if (def->arg != NULL)
			value = defGetString(def);
		else
			value = "true";

		if (def->defnamespace)
		{
			/*
			 * This function is used for backward compatibility in the place
			 * where namespases are not allowed
			 */
			Assert(false);		/* Should not get here */
			return (Datum) 0;
		}
		len = VARHDRSZ + strlen(name) + strlen(value) + 1;
		t = (text *) palloc(len + 1);
		SET_VARSIZE(t, len);
		sprintf(VARDATA(t), "%s=%s", name, value);
		astate = accumArrayResult(astate, PointerGetDatum(t), false,
								  TEXTOID, CurrentMemoryContext);

	}
	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;
	return result;
}


/*
 * optionsParseRawValues
 *		Transforms RawValues List into [Parsed] Values List. Validation is done
 *		if validate flag is set.
 *
 * Options data that come parsed SQL query or DB storage, first converted into
 * RawValues (where value are kept in text format), then is parsed into
 * Values using this function
 *
 * Validation is used only for data that came from SQL query. We trust that
 * data that came from DB is correct.
 *
 * If validation is off, all unknown options are kept unparsed so they will
 * be stored back to DB until user RESETs them directly.
 *
 * This function destroys incoming list.
 */
List *
optionsParseRawValues(List *raw_values, options_spec_set *spec_set,
					  bool validate)
{
	ListCell   *cell;
	List	   *result = NIL;
	bool	   *is_set;
	int			i;

	is_set = palloc0(sizeof(bool) * spec_set->num);
	foreach(cell, raw_values)
	{
		option_value *option = (option_value *) lfirst(cell);
		bool		found = false;

		/* option values with RESET status does not need parsing */
		Assert(option->status != OPTION_VALUE_STATUS_FOR_RESET);

		/* Should not parse Values Set twice */
		Assert(option->status != OPTION_VALUE_STATUS_PARSED);

		if (validate && option->namspace && (!spec_set->namspace ||
						  strcmp(spec_set->namspace, option->namspace) != 0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter namespace \"%s\"",
							option->namspace)));

		for (i = 0; i < spec_set->num; i++)
		{
			option_spec_basic *opt_spec = spec_set->definitions[i];

			if (strcmp(option->raw_name, opt_spec->name) == 0)
			{
				if (validate && is_set[i])
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("parameter \"%s\" specified more than once",
									option->raw_name)));

				pfree(option->raw_name);
				option->raw_name = NULL;
				option->gen = opt_spec;
				parse_one_option(option, validate);
				is_set[i] = true;
				found = true;
				break;
			}
		}
		if (validate && !found)
		{
			if (option->namspace)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized parameter \"%s.%s\"",
								option->namspace, option->raw_name)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized parameter \"%s\"",
								option->raw_name)));
		}
		result = lappend(result, option);
	}
	return result;
}

/*
 * parse_one_option
 *		Function to parse and validate single option.
 *
 * See optionsParseRawValues for more info.
 *
 * Link to Option Spec for the option is embedded into "option_value"
 */
static void
parse_one_option(option_value *option, bool validate)
{
	char	   *value;
	bool		parsed;

	value = option->raw_value;

	switch (option->gen->type)
	{
		case OPTION_TYPE_BOOL:
			{
				parsed = parse_bool(value, &option->values.bool_val);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for boolean option \"%s\": %s",
									option->gen->name, value)));
			}
			break;
		case OPTION_TYPE_INT:
			{
				option_spec_int *optint =
				(option_spec_int *) option->gen;

				parsed = parse_int(value, &option->values.int_val, 0, NULL);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for integer option \"%s\": %s",
									option->gen->name, value)));
				if (validate && (option->values.int_val < optint->min ||
								 option->values.int_val > optint->max))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("value %s out of bounds for option \"%s\"",
									value, option->gen->name),
							 errdetail("Valid values are between \"%d\" and \"%d\".",
									   optint->min, optint->max)));
			}
			break;
		case OPTION_TYPE_REAL:
			{
				option_spec_real *optreal =
				(option_spec_real *) option->gen;

				parsed = parse_real(value, &option->values.real_val, 0, NULL);
				if (validate && !parsed)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for floating point option \"%s\": %s",
									option->gen->name, value)));
				if (validate && (option->values.real_val < optreal->min ||
								 option->values.real_val > optreal->max))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("value %s out of bounds for option \"%s\"",
									value, option->gen->name),
							 errdetail("Valid values are between \"%f\" and \"%f\".",
									   optreal->min, optreal->max)));
			}
			break;
		case OPTION_TYPE_ENUM:
			{
				option_spec_enum *optenum =
				(option_spec_enum *) option->gen;
				opt_enum_elt_def *elt;

				parsed = false;
				for (elt = optenum->members; elt->string_val; elt++)
				{
					if (strcmp(value, elt->string_val) == 0)
					{
						option->values.enum_val = elt->symbol_val;
						parsed = true;
						break;
					}
				}
				if (!parsed)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for enum option \"%s\": %s",
								option->gen->name, value),
						 optenum->detailmsg ?
						 errdetail_internal("%s", _(optenum->detailmsg)) : 0));
			}
			break;
		case OPTION_TYPE_STRING:
			{
				option_spec_string *optstring =
				(option_spec_string *) option->gen;

				option->values.string_val = value;
				if (validate && optstring->validate_cb)
					(optstring->validate_cb) (value);
				parsed = true;
			}
			break;
		default:
			elog(ERROR, "unsupported reloption type %d", option->gen->type);
			parsed = true;		/* quiet compiler */
			break;
	}
	if (validate && option->gen->postvalidate_fn)
		option->gen->postvalidate_fn(option);

	if (parsed)
		option->status = OPTION_VALUE_STATUS_PARSED;

}

/*
 * optionsAllocateBytea
 *		Allocates memory for Bytea options representation
 *
 * We need special function for this, as string option values are embedded into
 * Bytea object, stored at the rear part of the memory chunk. Thus we need to
 * allocate extra memory, so all string values would fit in. This function
 * calculates required size and do allocation.
 *
 * See "OPTION STRING VALUE NOTION" at the beginning of the file for better
 * understanding.
 */
static void *
optionsAllocateBytea(options_spec_set *spec_set, List *options)
{
	Size		size;
	int			i;
	ListCell   *cell;
	int			length;
	void	   *res;

	size = spec_set->struct_size;

	/* Calculate size needed to store all string values for this option */
	for (i = 0; i < spec_set->num; i++)
	{
		option_spec_basic *opt_spec = spec_set->definitions[i];
		option_spec_string *opt_spec_str;
		bool		found = false;
		option_value *option;
		const char *val = NULL;

		/* Not interested in non-string options, skipping */
		if (opt_spec->type != OPTION_TYPE_STRING)
			continue;

		/*
		 * Trying to find option_value that references opt_spec entry
		 */
		opt_spec_str = (option_spec_string *) opt_spec;
		foreach(cell, options)
		{
			option = (option_value *) lfirst(cell);
			if (option->status == OPTION_VALUE_STATUS_PARSED &&
				strcmp(option->gen->name, opt_spec->name) == 0)
			{
				found = true;
				break;
			}
		}
		if (found)
			val = option->values.string_val;
		else
			val = opt_spec_str->default_val;

		if (opt_spec_str->fill_cb)
			length = opt_spec_str->fill_cb(val, NULL);
		else if (val)
			length = strlen(val) + 1;
		else
			length = 0;			/* "Default Value is NULL" case */

		/* Add total length of each string values to basic size */
		size += length;
	}

	res = palloc0(size);
	SET_VARSIZE(res, size);
	return res;
}

/*
 * optionsValuesToBytea
 *		Converts options values from Value List representation to Bytea
 *		representation.
 *
 * Fills resulting Bytea with option values according to Option Sec Set.
 *
 * For understanding processing of the string option please read "OPTION STRING
 * VALUE NOTION" at the beginning of the file.
 */
bytea *
optionsValuesToBytea(List *options, options_spec_set *spec_set)
{
	char	   *data;
	char	   *string_values_buffer;
	int			i;

	data = optionsAllocateBytea(spec_set, options);

	/* place for string data starts right after original structure */
	string_values_buffer = data + spec_set->struct_size;

	for (i = 0; i < spec_set->num; i++)
	{
		option_value *found = NULL;
		ListCell   *cell;
		char	   *item_pos;
		option_spec_basic *opt_spec = spec_set->definitions[i];

		if (opt_spec->struct_offset < 0)
			continue;			/* This option value should not be stored in
								 * Bytea for some reason. May be it is
								 * deprecated and has warning or error in
								 * postvalidate function */

		/* Calculate the position of the item inside the structure */
		item_pos = data + opt_spec->struct_offset;

		/* Looking for the corresponding option from options list */
		foreach(cell, options)
		{
			option_value *option = (option_value *) lfirst(cell);

			if (option->status == OPTION_VALUE_STATUS_RAW)
				continue;	/* raw can come from db. Just ignore them then */
			Assert(option->status != OPTION_VALUE_STATUS_EMPTY);

			if (strcmp(opt_spec->name, option->gen->name) == 0)
			{
				found = option;
				break;
			}
		}
		/* writing to the proper position either option value or default val */
		switch (opt_spec->type)
		{
			case OPTION_TYPE_BOOL:
				*(bool *) item_pos = found ?
					found->values.bool_val :
					((option_spec_bool *) opt_spec)->default_val;
				break;
			case OPTION_TYPE_INT:
				*(int *) item_pos = found ?
					found->values.int_val :
					((option_spec_int *) opt_spec)->default_val;
				break;
			case OPTION_TYPE_REAL:
				*(double *) item_pos = found ?
					found->values.real_val :
					((option_spec_real *) opt_spec)->default_val;
				break;
			case OPTION_TYPE_ENUM:
				*(int *) item_pos = found ?
					found->values.enum_val :
					((option_spec_enum *) opt_spec)->default_val;
				break;

			case OPTION_TYPE_STRING:
				{
					/*
					 * For string options: writing string value at the string
					 * buffer after the structure, and storing and offset to
					 * that value
					 */
					char	   *value = NULL;
					option_spec_string *opt_spec_str =
					(option_spec_string *) opt_spec;

					if (found)
						value = found->values.string_val;
					else
						value = opt_spec_str->default_val;

					if (opt_spec_str->fill_cb)
					{
						Size		size =
						opt_spec_str->fill_cb(value, string_values_buffer);

						if (size)
						{
							*(int *) item_pos = string_values_buffer - data;
							string_values_buffer += size;
						}
						else
							*(int *) item_pos =
								OPTION_STRING_VALUE_NOT_SET_OFFSET;
					}
					else
					{
						*(int *) item_pos = value ?
							string_values_buffer - data :
							OPTION_STRING_VALUE_NOT_SET_OFFSET;
						if (value)
						{
							strcpy(string_values_buffer, value);
							string_values_buffer += strlen(value) + 1;
						}
					}
				}
				break;
			default:
				elog(ERROR, "unsupported reloption type %d",
					 opt_spec->type);
				break;
		}
	}
	return (void *) data;
}

/*
 * optionDefListToTextArray
 *		Converts options from defList to TEXT[] representation.
 *
 * Used when new relation (or other object) is created. defList comes from
 * SQL syntax parser, TEXT[] goes to DB storage. Options are always validated
 * while conversion.
 */
Datum
optionDefListToTextArray(options_spec_set *spec_set, List *defList)
{
	Datum		result;
	List	   *new_values;

	/* Parse and validate new values */
	new_values = optionsDefListToRawValues(defList, false);
	new_values = optionsParseRawValues(new_values, spec_set, true);

	/* Some checks can be done in postprocess_fun, we should call it */
	if (spec_set->postprocess_fun)
	{
		bytea	   *data;
		if (defList)
			data = optionsValuesToBytea(new_values, spec_set);
		else
			data = NULL;

		spec_set->postprocess_fun(data, true);
		if (data) pfree(data);
	}
	result = optionsValuesToTextArray(new_values);
	return result;
}

/*
 * optionsUpdateTexArrayWithDefList
 * 		Modifies oldOptions values (in TEXT[] representation) with defList
 * 		values.
 *
 * Old values are appened or replaced with new values if do_reset flag is set
 * to false. If do_reset is set to true, defList specify the list of the options
 * that should be removed from original list.
 */

Datum
optionsUpdateTexArrayWithDefList(options_spec_set *spec_set, Datum oldOptions,
								 List *defList, bool do_reset)
{
	Datum		result;
	List	   *new_values;
	List	   *old_values;
	List	   *merged_values;

	/*
	 * Parse and validate New values
	 */
	new_values = optionsDefListToRawValues(defList, do_reset);
	if (!do_reset)
		new_values = optionsParseRawValues(new_values, spec_set, true);

	if (PointerIsValid(DatumGetPointer(oldOptions)))
	{
		old_values = optionsTextArrayToRawValues(oldOptions);
		merged_values = optionsMergeOptionValues(old_values, new_values);
	}
	else
	{
		if (do_reset)
			merged_values = NULL;	/* return nothing */
		else
			merged_values = new_values;
	}

	/*
	 * If we have postprocess_fun function defined in spec_set, then there
	 * might be some custom options checks there, with error throwing. So we
	 * should do it here to throw these errors while CREATing or ALTERing
	 * options
	 */
	if (spec_set->postprocess_fun)
	{
		bytea	   *data = optionsValuesToBytea(merged_values, spec_set);

		spec_set->postprocess_fun(data, true);
		pfree(data);
	}

	/*
	 * Convert options to TextArray format so caller can store them into
	 * database
	 */
	result = optionsValuesToTextArray(merged_values);
	return result;
}

/*
 * optionsTextArrayToBytea
 *		Convert options values from TEXT[] representation into Bytea
 *		representation
 *
 * This function uses other conversion function to get desired result.
 */
bytea *
optionsTextArrayToBytea(options_spec_set *spec_set, Datum data, bool validate)
{
	List	   *values;
	bytea	   *options;

	values = optionsTextArrayToRawValues(data);
	values = optionsParseRawValues(values, spec_set, validate);
	options = optionsValuesToBytea(values, spec_set);

	if (spec_set->postprocess_fun)
		spec_set->postprocess_fun(options, false);
	return options;
}
