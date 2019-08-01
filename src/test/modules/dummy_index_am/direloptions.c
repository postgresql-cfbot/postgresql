/*-------------------------------------------------------------------------
 *
 *	direloptions.c
 *		Functions and variables needed for reloptions tests.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	 src/test/modules/dummy_index_am/direloptions.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/rel.h"
#include "access/reloptions.h"
#include "utils/guc.h"

#include "dummy_index.h"


/* parse table for fillRelOptions */
relopt_parse_elt di_relopt_tab[5];


/* Kind of relation options for dummy index */
relopt_kind di_relopt_kind;

/* GUC variables */
static bool do_test_reloptions = false;
static bool do_test_reloption_int = false;
static bool do_test_reloption_real = false;
static bool do_test_reloption_bool = false;
static bool do_test_reloption_string = false;
static bool do_test_reloption_string2 = false;

/*
 * This function creates reloptions that would be used in reloptions tests
 */
void
create_reloptions_table()
{
	di_relopt_kind = add_reloption_kind();

	add_int_reloption(di_relopt_kind, "int_option",
					  "Int option used for relotions test purposes",
					  10, -10, 100);
	di_relopt_tab[0].optname = "int_option";
	di_relopt_tab[0].opttype = RELOPT_TYPE_INT;
	di_relopt_tab[0].offset = offsetof(DummyIndexOptions, int_option);

	add_real_reloption(di_relopt_kind, "real_option",
					   "Real option used for relotions test purposes",
					   3.1415, -10, 100);
	di_relopt_tab[1].optname = "real_option";
	di_relopt_tab[1].opttype = RELOPT_TYPE_REAL;
	di_relopt_tab[1].offset = offsetof(DummyIndexOptions, real_option);


	add_bool_reloption(di_relopt_kind, "bool_option",
					   "Boolean option used for relotions test purposes",
					   true);
	di_relopt_tab[2].optname = "bool_option";
	di_relopt_tab[2].opttype = RELOPT_TYPE_BOOL;
	di_relopt_tab[2].offset = offsetof(DummyIndexOptions, bool_option);

	add_string_reloption(di_relopt_kind, "string_option",
						 "String option used for relotions test purposes",
						 "DefaultValue", &validate_string_option);
	di_relopt_tab[3].optname = "string_option";
	di_relopt_tab[3].opttype = RELOPT_TYPE_STRING;
	di_relopt_tab[3].offset = offsetof(DummyIndexOptions, string_option_offset);

	add_string_reloption(di_relopt_kind, "string_option2",
					 "Second string option used for relotions test purposes",
						 "SecondDefaultValue", &validate_string_option);
	di_relopt_tab[4].optname = "string_option2";
	di_relopt_tab[4].opttype = RELOPT_TYPE_STRING;
	di_relopt_tab[4].offset = offsetof(DummyIndexOptions, string_option2_offset);

}

/*
 * This function creates GUC variables that allows to turn on and off test
 * output for different relopntions
 */
void
create_reloptions_test_GUC()
{
	DefineCustomBoolVariable("dummy_index.do_test_reloptions",
						  "Set to true if you are going to test reloptions.",
							 NULL,
							 &do_test_reloptions,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("dummy_index.do_test_reloption_int",
					   "Set to true if you are going to test int reloption.",
							 NULL,
							 &do_test_reloption_int,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("dummy_index.do_test_reloption_real",
					  "Set to true if you are going to test real reloption.",
							 NULL,
							 &do_test_reloption_real,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("dummy_index.do_test_reloption_bool",
				   "Set to true if you are going to test boolean reloption.",
							 NULL,
							 &do_test_reloption_bool,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("dummy_index.do_test_reloption_string",
					"Set to true if you are going to test string reloption.",
							 NULL,
							 &do_test_reloption_string,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("dummy_index.do_test_reloption_string2",
			 "Set to true if you are going to test second string reloption.",
							 NULL,
							 &do_test_reloption_string2,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

/*
 * Function prints output for reloptions tests if GUC variables switches are
 * properly set on.
 */
void
print_reloptions_test_output(Relation index)
{
	if (do_test_reloptions)
	{
		if (!index->rd_options)
		{
			elog(WARNING, "No reloptions is set, default values will be chosen in module runtime");
		}
		else
		{
			DummyIndexOptions *opt;

			opt = (DummyIndexOptions *) index->rd_options;
			if (do_test_reloption_int)
			{
				elog(WARNING, "int_option = %d", opt->int_option);
			}
			if (do_test_reloption_real)
			{
				elog(WARNING, "real_option = %f", opt->real_option);
			}
			if (do_test_reloption_bool)
			{
				elog(WARNING, "bool_option = %d", opt->bool_option);
			}
			if (do_test_reloption_string)
			{
				char	   *str;

				str = (char *) opt + opt->string_option_offset;
				elog(WARNING, "string_option = '%s'", str);
			}
			if (do_test_reloption_string2)
			{
				char	   *str;

				str = (char *) opt + opt->string_option2_offset;
				elog(WARNING, "string_option2 = '%s'", str);
			}
		}
	}
}

/*
 * Validation function for string_option reloption
 */
void
validate_string_option(const char *value)
{
	if (do_test_reloptions && do_test_reloption_string)
	{
		if (value)
			elog(WARNING, "Validating string option '%s'", value);
		else
			elog(WARNING, "Validating string option with NULL value");
	}
	if (!value || *value == 'I')
		elog(ERROR, "This seems to be invalid value. Please set valid value");
}
