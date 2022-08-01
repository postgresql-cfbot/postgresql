/*-------------------------------------------------------------------------
 *
 * options.h
 *	  An uniform, context-free API for processing name=value options. Used
 *	  to process relation options (reloptions), attribute options, opclass
 *	  options, etc.
 *
 * Note: the functions dealing with text-array options values declare
 * them as Datum, not ArrayType *, to avoid needing to include array.h
 * into a lot of low-level code.
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/include/access/options.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef OPTIONS_H
#define OPTIONS_H

#include "storage/lock.h"
#include "nodes/pg_list.h"


/* supported option types */
typedef enum option_type
{
	OPTION_TYPE_BOOL,
	OPTION_TYPE_INT,
	OPTION_TYPE_REAL,
	OPTION_TYPE_ENUM,
	OPTION_TYPE_STRING
} option_type;

typedef enum option_value_status
{
	OPTION_VALUE_STATUS_EMPTY,	/* Option was just initialized */
	OPTION_VALUE_STATUS_RAW,	/* Option just came from syntax analyzer in
								 * has name, and raw (unparsed) value */
	OPTION_VALUE_STATUS_PARSED, /* Option was parsed and has link to catalog
								 * entry and proper value */
	OPTION_VALUE_STATUS_FOR_RESET	/* This option came from ALTER xxx RESET */
} option_value_status;

/*
 * opt_enum_elt_def -- One member of the array of acceptable values
 * of an enum reloption.
 */
typedef struct opt_enum_elt_def
{
	const char *string_val;
	int			symbol_val;
} opt_enum_elt_def;


typedef struct option_value option_value;

/* Function that would be called after option validation.
 * Might be needed for custom warnings, errors, or for changing
 * option value after being validated, etc.
 */
typedef void (*option_value_postvalidate) (option_value *value);

/* generic structure to store Option Spec information */
typedef struct option_spec_basic
{
	const char *name;			/* must be first (used as list termination
								 * marker) */
	const char *desc;
	LOCKMODE	lockmode;
	option_type type;
	int			struct_offset;	/* offset of the value in Bytea representation */
	option_value_postvalidate postvalidate_fn;
} option_spec_basic;


/* reloptions records for specific variable types */
typedef struct option_spec_bool
{
	option_spec_basic base;
	bool		default_val;
} option_spec_bool;

typedef struct option_spec_int
{
	option_spec_basic base;
	int			default_val;
	int			min;
	int			max;
} option_spec_int;

typedef struct option_spec_real
{
	option_spec_basic base;
	double		default_val;
	double		min;
	double		max;
} option_spec_real;

typedef struct option_spec_enum
{
	option_spec_basic base;
	opt_enum_elt_def *members;	/* Null terminated array of allowed names and
								 * corresponding values */
	int			default_val;	/* Default value, may differ from values in
								 * members array */
	const char *detailmsg;
} option_spec_enum;

/* validation routines for strings */
typedef void (*validate_string_option) (const char *value);
typedef Size (*fill_string_option) (const char *value, void *ptr);

/*
 * When storing sting reloptions, we should deal with special case when
 * option value is not set. For fixed length options, we just copy default
 * option value into the binary structure. For varlen value, there can be
 * "not set" special case, with no default value offered.
 * In this case we will set offset value to -1, so code that use reloptions
 * can deal this case. For better readability it was defined as a constant.
 */
#define OPTION_STRING_VALUE_NOT_SET_OFFSET -1

typedef struct option_spec_string
{
	option_spec_basic base;
	validate_string_option validate_cb;
	fill_string_option fill_cb;
	char	   *default_val;
} option_spec_string;

typedef void (*postprocess_bytea_options_function) (void *data, bool validate);

typedef struct options_spec_set
{
	option_spec_basic **definitions;
	int			num;			/* Number of spec_set items in use */
	int			num_allocated;	/* Number of spec_set items allocated */
	bool		assert_on_realloc; /* If number of items of the spec_set were
								 * strictly set to certain value assert on
								 * adding more items */
	bool		is_local;		/* If true specset is in local memory context */
	Size		struct_size;	/* Size of a structure for options in binary
								 * representation */
	postprocess_bytea_options_function postprocess_fun; /* This function is
														 * called after options
														 * were converted in
														 * Bytea representation.
														 * Can be used for extra
														 * validation etc. */
	char	   *namespace;		/* spec_set is used for options from this
								 * namespase */
} options_spec_set;


/* holds an option value parsed or unparsed */
typedef struct option_value
{
	option_spec_basic *gen;
	char	   *namespace;
	option_value_status status;
	char	   *raw_value;		/* allocated separately */
	char	   *raw_name;
	union
	{
		bool		bool_val;
		int			int_val;
		double		real_val;
		int			enum_val;
		char	   *string_val; /* allocated separately */
	}			values;
} option_value;


/*
 * Options spec_set related functions
 */
extern options_spec_set *allocateOptionsSpecSet(const char *namespace,
						int bytea_size, bool is_local, int num_items_expected);

extern void optionsSpecSetAddBool(options_spec_set *spec_set, const char *name,
								  const char *desc, LOCKMODE lockmode,
								  int struct_offset,
								  option_value_postvalidate postvalidate_fn,
								  bool default_val);

extern void optionsSpecSetAddInt(options_spec_set *spec_set, const char *name,
								 const char *desc, LOCKMODE lockmode,
								 int struct_offset,
								 option_value_postvalidate postvalidate_fn,
								 int default_val, int min_val, int max_val);

extern void optionsSpecSetAddReal(options_spec_set *spec_set, const char *name,
						  const char *desc, LOCKMODE lockmode,
						  int struct_offset,
						  option_value_postvalidate postvalidate_fn,
						  double default_val, double min_val, double max_val);

extern void optionsSpecSetAddEnum(options_spec_set *spec_set, const char *name,
								  const char *desc, LOCKMODE lockmode,
								  int struct_offset,
								  option_value_postvalidate postvalidate_fn,
								  opt_enum_elt_def *members, int default_val,
								  const char *detailmsg);


extern void optionsSpecSetAddString(options_spec_set *spec_set,
									const char *name, const char *desc,
									LOCKMODE lockmode, int struct_offset,
									option_value_postvalidate postvalidate_fn,
									const char *default_val,
									validate_string_option validator,
									fill_string_option filler);


/*
 * This macro allows to get string option value from bytea representation.
 * "optstruct" - is a structure that is stored in bytea options representation
 * "member" - member of this structure that has string option value
 * (actually string values are stored in bytea after the structure, and
 * and "member" will contain an offset to this value. This macro do all
 * the math
 */
#define GET_STRING_OPTION(optstruct, member) \
	((optstruct)->member == OPTION_STRING_VALUE_NOT_SET_OFFSET ? NULL : \
	 (char *)(optstruct) + (optstruct)->member)

/*
 * Functions related to option conversion, parsing, manipulation
 * and validation
 */
extern void optionsDefListValdateNamespaces(List *defList,
											char **allowed_namespaces);
extern List *optionsDefListFilterNamespaces(List *defList,
											const char *namespace);
extern List *optionsTextArrayToDefList(Datum options);
extern Datum optionsDefListToTextArray(List *defList);

/*
 * Meta functions that uses functions above to get options for relations,
 * tablespaces, views and so on
 */

extern bytea *optionsTextArrayToBytea(options_spec_set *spec_set, Datum data,
									  bool validate);
extern Datum optionsUpdateTexArrayWithDefList(options_spec_set *spec_set,
							  Datum oldOptions, List *defList, bool do_reset);
extern Datum optionDefListToTextArray(options_spec_set *spec_set,
									  List *defList);

/* Internal functions */

extern List *optionsTextArrayToRawValues(Datum array_datum);
extern List *optionsParseRawValues(List *raw_values,
								   options_spec_set *spec_set, bool validate);
extern bytea *optionsValuesToBytea(List *options, options_spec_set *spec_set);


#endif							/* OPTIONS_H */
