/*-------------------------------------------------------------------------
 *
 * reloptions.h
 *	  Support for relation view and tablespace options (pg_class.reloptions
 *	  and pg_tablespace.spcoptions)
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/reloptions.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELOPTIONS_H
#define RELOPTIONS_H

#include "access/amapi.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/options.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"

/* reloption namespaces allowed for heaps -- currently only TOAST */
#define HEAP_RELOPT_NAMESPACES { "toast", NULL }

/*
 * backward compatibility aliases so local reloption code of custom validator
 * can work.
 */
typedef option_value relopt_value;
typedef fill_string_option fill_string_relopt;
typedef validate_string_option validate_string_relopt;
#define GET_STRING_RELOPTION(optstruct, member) \
			GET_STRING_OPTION(optstruct, member)


/*
 * relopts_validator functions is left for backward compatibility for using
 * with local reloptions. Should not be used elsewhere
 */

/* validation routine for the whole option set */
typedef void (*relopts_validator) (void *parsed_options, relopt_value *vals,
								   int nvals);

/* Structure to hold local reloption data for build_local_reloptions() */
typedef struct local_relopts
{
	List	   *validators;		/* list of relopts_validator callbacks */
	options_spec_set *spec_set; /* Spec Set to store options info */
} local_relopts;


extern void init_local_reloptions(local_relopts *relopts, Size relopt_struct_size);
extern void register_reloptions_validator(local_relopts *relopts,
										  relopts_validator validator);
extern void add_local_bool_reloption(local_relopts *relopts, const char *name,
									 const char *desc, bool default_val,
									 int offset);
extern void add_local_int_reloption(local_relopts *relopts, const char *name,
									const char *desc, int default_val,
									int min_val, int max_val, int offset);
extern void add_local_real_reloption(local_relopts *relopts, const char *name,
									 const char *desc, double default_val,
									 double min_val, double max_val,
									 int offset);
extern void add_local_enum_reloption(local_relopts *relopts,
									 const char *name, const char *desc,
									 opt_enum_elt_def *members,
									 int default_val, const char *detailmsg,
									 int offset);
extern void add_local_string_reloption(local_relopts *relopts, const char *name,
									   const char *desc,
									   const char *default_val,
									   validate_string_relopt validator,
									   fill_string_relopt filler, int offset);

extern bytea *extractRelOptions(HeapTuple tuple, TupleDesc tupdesc,
								amreloptspecset_function amoptions_def_set);
extern void *build_local_reloptions(local_relopts *relopts, Datum options,
									bool validate);

options_spec_set *get_heap_relopt_spec_set(void);
options_spec_set *get_toast_relopt_spec_set(void);
options_spec_set *get_partitioned_relopt_spec_set(void);
options_spec_set *get_view_relopt_spec_set(void);
options_spec_set *get_attribute_options_spec_set(void);
options_spec_set *get_tablespace_options_spec_set(void);
extern LOCKMODE AlterTableGetRelOptionsLockLevel(Relation rel, List *defList);

#endif							/* RELOPTIONS_H */
