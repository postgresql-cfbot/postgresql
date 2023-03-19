/*-------------------------------------------------------------------------
 *
 * reloptions.c
 *	  Support for relation options (pg_class.reloptions)
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/reloptions.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>

#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/options.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "commands/view.h"
#include "nodes/makefuncs.h"
#include "postmaster/postmaster.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"

/*
 * Contents of pg_class.reloptions
 *
 * To add an option:
 *
 * (i) decide on a type (integer, real, bool, string), name, default value,
 * upper and lower bounds (if applicable); for strings, consider a validation
 * routine.
 * (ii) add a record below (or use add_<type>_reloption).
 * (iii) add it to the appropriate options struct (perhaps StdRdOptions)
 * (iv) add it to the appropriate handling routine (perhaps
 * default_reloptions)
 * (v) make sure the lock level is set correctly for that operation
 * (vi) don't forget to document the option
 *
 * The default choice for any new option should be AccessExclusiveLock.
 * In some cases the lock level can be reduced from there, but the lock
 * level chosen should always conflict with itself to ensure that multiple
 * changes aren't lost when we attempt concurrent changes.
 * The choice of lock level depends completely upon how that parameter
 * is used within the server, not upon how and when you'd like to change it.
 * Safety first. Existing choices are documented here, and elsewhere in
 * backend code where the parameters are used.
 *
 * In general, anything that affects the results obtained from a SELECT must be
 * protected by AccessExclusiveLock.
 *
 * Autovacuum related parameters can be set at ShareUpdateExclusiveLock
 * since they are only used by the AV procs and don't change anything
 * currently executing.
 *
 * Fillfactor can be set because it applies only to subsequent changes made to
 * data blocks, as documented in hio.c
 *
 * n_distinct options can be set at ShareUpdateExclusiveLock because they
 * are only used during ANALYZE, which uses a ShareUpdateExclusiveLock,
 * so the ANALYZE will not be affected by in-flight changes. Changing those
 * values has no effect until the next ANALYZE, so no need for stronger lock.
 *
 * Planner-related parameters can be set with ShareUpdateExclusiveLock because
 * they only affect planning and not the correctness of the execution. Plans
 * cannot be changed in mid-flight, so changes here could not easily result in
 * new improved plans in any case. So we allow existing queries to continue
 * and existing plans to survive, a small price to pay for allowing better
 * plans to be introduced concurrently without interfering with users.
 *
 * Setting parallel_workers is safe, since it acts the same as
 * max_parallel_workers_per_gather which is a USERSET parameter that doesn't
 * affect existing plans or queries.
 *
 * vacuum_truncate can be set at ShareUpdateExclusiveLock because it
 * is only used during VACUUM, which uses a ShareUpdateExclusiveLock,
 * so the VACUUM will not be affected by in-flight changes. Changing its
 * value has no effect until the next VACUUM, so no need for stronger lock.
 */

/* values from StdRdOptIndexCleanup */
static opt_enum_elt_def StdRdOptIndexCleanupValues[] =
{
	{"auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO},
	{"on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON},
	{"off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF},
	{"true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON},
	{"false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF},
	{"yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON},
	{"no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF},
	{"1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON},
	{"0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF},
	{(const char *) NULL}		/* list terminator */
};

/* values from ViewOptCheckOption */
static opt_enum_elt_def viewCheckOptValues[] =
{
	/* no value for NOT_SET */
	{"local", VIEW_OPTION_CHECK_OPTION_LOCAL},
	{"cascaded", VIEW_OPTION_CHECK_OPTION_CASCADED},
	{(const char *) NULL}		/* list terminator */
};


options_spec_set *get_stdrd_relopt_spec_set(bool is_for_toast);
void		oid_postvalidate(option_value *value);

/*
 * init_local_reloptions
 *		Initialize local reloptions that will parsed into bytea structure of
 * 		'relopt_struct_size'.
 */
void
init_local_reloptions(local_relopts *relopts, Size relopt_struct_size)
{
	relopts->validators = NIL;
	relopts->spec_set = allocateOptionsSpecSet(NULL, relopt_struct_size, true, 0);
}

/*
 * register_reloptions_validator
 *		Register custom validation callback that will be called at the end of
 *		build_local_reloptions().
 */
void
register_reloptions_validator(local_relopts *relopts, relopts_validator validator)
{
	relopts->validators = lappend(relopts->validators, validator);
}

/*
 * add_local_bool_reloption
 *		Add a new boolean local reloption
 *
 * 'offset' is offset of bool-typed field.
 */
void
add_local_bool_reloption(local_relopts *relopts, const char *name,
						 const char *desc, bool default_val, int offset)
{
	optionsSpecSetAddBool(relopts->spec_set, name, desc, NoLock, offset, NULL,
						  default_val);
}

/*
 * add_local_int_reloption
 *		Add a new local integer reloption
 *
 * 'offset' is offset of int-typed field.
 */
void
add_local_int_reloption(local_relopts *relopts, const char *name,
						const char *desc, int default_val, int min_val,
						int max_val, int offset)
{
	optionsSpecSetAddInt(relopts->spec_set, name, desc, NoLock, offset, NULL,
						 default_val, min_val, max_val);
}

/*
 * add_local_real_reloption
 *		Add a new local float reloption
 *
 * 'offset' is offset of double-typed field.
 */
void
add_local_real_reloption(local_relopts *relopts, const char *name,
						 const char *desc, double default_val,
						 double min_val, double max_val, int offset)
{
	optionsSpecSetAddReal(relopts->spec_set, name, desc, NoLock, offset, NULL,
						  default_val, min_val, max_val);

}

/*
 * add_local_enum_reloption
 *		Add a new local enum reloption
 *
 * 'offset' is offset of int-typed field.
 */
void
add_local_enum_reloption(local_relopts *relopts, const char *name,
						 const char *desc, opt_enum_elt_def *members,
						 int default_val, const char *detailmsg, int offset)
{
	optionsSpecSetAddEnum(relopts->spec_set, name, desc, NoLock, offset, NULL,
						  members, default_val, detailmsg);
}

/*
 * add_local_string_reloption
 *		Add a new local string reloption
 *
 * 'offset' is offset of int-typed field that will store offset of string value
 * in the resulting bytea structure.
 */
void
add_local_string_reloption(local_relopts *relopts, const char *name,
						   const char *desc, const char *default_val,
						   validate_string_relopt validator,
						   fill_string_relopt filler, int offset)
{
	optionsSpecSetAddString(relopts->spec_set, name, desc, NoLock, offset,
							NULL, default_val, validator, filler);
}

/*
 * Extract and parse reloptions from a pg_class tuple.
 *
 * This is a low-level routine, expected to be used by relcache code and
 * callers that do not have a table's relcache entry (e.g. autovacuum).  For
 * other uses, consider grabbing the rd_options pointer from the relcache entry
 * instead.
 *
 * tupdesc is pg_class' tuple descriptor.  amoptions is a pointer to the index
 * AM's options parser function in the case of a tuple corresponding to an
 * index, or NULL otherwise.
 */
bytea *
extractRelOptions(HeapTuple tuple, TupleDesc tupdesc,
				  amreloptspecset_function amoptionsspecsetfn)
{
	bytea	   *options;
	bool		isnull;
	Datum		datum;
	Form_pg_class classForm;
	options_spec_set *spec_set;

	datum = fastgetattr(tuple,
						Anum_pg_class_reloptions,
						tupdesc,
						&isnull);
	if (isnull)
		return NULL;

	classForm = (Form_pg_class) GETSTRUCT(tuple);

	/* Parse into appropriate format; don't error out here */
	switch (classForm->relkind)
	{
		case RELKIND_TOASTVALUE:
			spec_set = get_toast_relopt_spec_set();
			break;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			spec_set = get_heap_relopt_spec_set();
			break;
		case RELKIND_PARTITIONED_TABLE:
			spec_set = get_partitioned_relopt_spec_set();
			break;
		case RELKIND_VIEW:
			spec_set = get_view_relopt_spec_set();
			break;
		case RELKIND_INDEX:
		case RELKIND_PARTITIONED_INDEX:
			if (amoptionsspecsetfn)
				spec_set = amoptionsspecsetfn();
			else
				spec_set = NULL;
			break;
		case RELKIND_FOREIGN_TABLE:
			spec_set = NULL;
			break;
		default:
			Assert(false);		/* can't get here */
			spec_set = NULL;	/* keep compiler quiet */
			break;
	}
	if (spec_set)
		options = optionsTextArrayToBytea(spec_set, datum, 0);
	else
		options = NULL;
	return options;
}

void
oid_postvalidate(option_value *value)
{
	if (value->values.bool_val)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("tables declared WITH OIDS are not supported")));
}

/*
 * Relation options and Lock levels:
 *
 * The default choice for any new option should be AccessExclusiveLock.
 * In some cases the lock level can be reduced from there, but the lock
 * level chosen should always conflict with itself to ensure that multiple
 * changes aren't lost when we attempt concurrent changes.
 * The choice of lock level depends completely upon how that parameter
 * is used within the server, not upon how and when you'd like to change it.
 * Safety first. Existing choices are documented here, and elsewhere in
 * backend code where the parameters are used.
 *
 * In general, anything that affects the results obtained from a SELECT must be
 * protected by AccessExclusiveLock.
 *
 * Autovacuum related parameters can be set at ShareUpdateExclusiveLock
 * since they are only used by the AV procs and don't change anything
 * currently executing.
 *
 * Fillfactor can be set because it applies only to subsequent changes made to
 * data blocks, as documented in heapio.c
 *
 * n_distinct options can be set at ShareUpdateExclusiveLock because they
 * are only used during ANALYZE, which uses a ShareUpdateExclusiveLock,
 * so the ANALYZE will not be affected by in-flight changes. Changing those
 * values has no affect until the next ANALYZE, so no need for stronger lock.
 *
 * Planner-related parameters can be set with ShareUpdateExclusiveLock because
 * they only affect planning and not the correctness of the execution. Plans
 * cannot be changed in mid-flight, so changes here could not easily result in
 * new improved plans in any case. So we allow existing queries to continue
 * and existing plans to survive, a small price to pay for allowing better
 * plans to be introduced concurrently without interfering with users.
 *
 * Setting parallel_workers is safe, since it acts the same as
 * max_parallel_workers_per_gather which is a USERSET parameter that doesn't
 * affect existing plans or queries.
 */


options_spec_set *
get_stdrd_relopt_spec_set(bool is_heap)
{
	options_spec_set *stdrd_relopt_spec_set = allocateOptionsSpecSet(
					 is_heap ? NULL : "toast", sizeof(StdRdOptions), false, 0);

	if (is_heap)
		optionsSpecSetAddInt(stdrd_relopt_spec_set, "fillfactor",
							 "Packs table pages only to this percentag",
							 ShareUpdateExclusiveLock,	/* since it applies only
														 * to later inserts */
							 offsetof(StdRdOptions, fillfactor), NULL,
							 HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100);

	optionsSpecSetAddBool(stdrd_relopt_spec_set, "autovacuum_enabled",
						  "Enables autovacuum in this relation",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, autovacuum) +
						  offsetof(AutoVacOpts, enabled),
						  NULL, true);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_vacuum_threshold",
						 "Minimum number of tuple updates or deletes prior to vacuum",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, vacuum_threshold),
						 NULL, -1, 0, INT_MAX);

	if (is_heap)
		optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_analyze_threshold",
							 "Minimum number of tuple updates or deletes prior to vacuum",
							 ShareUpdateExclusiveLock,
							 offsetof(StdRdOptions, autovacuum) +
							 offsetof(AutoVacOpts, analyze_threshold),
							 NULL, -1, 0, INT_MAX);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_vacuum_cost_limit",
						 "Vacuum cost amount available before napping, for autovacuum",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, vacuum_cost_limit),
						 NULL, -1, 0, 10000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_freeze_min_age",
						 "Minimum age at which VACUUM should freeze a table row, for autovacuum",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, freeze_min_age),
						 NULL, -1, 0, 1000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_freeze_max_age",
						 "Age at which to autovacuum a table to prevent transaction ID wraparound",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, freeze_max_age),
						 NULL, -1, 100000, 2000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_freeze_table_age",
						 "Age at which VACUUM should perform a full table sweep to freeze row versions",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, freeze_table_age),
						 NULL, -1, 0, 2000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_multixact_freeze_min_age",
						 "Minimum multixact age at which VACUUM should freeze a row multixact's, for autovacuum",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, multixact_freeze_min_age),
						 NULL, -1, 0, 1000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_multixact_freeze_max_age",
						 "Multixact age at which to autovacuum a table to prevent multixact wraparound",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, multixact_freeze_max_age),
						 NULL, -1, 10000, 2000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "autovacuum_multixact_freeze_table_age",
						 "Age of multixact at which VACUUM should perform a full table sweep to freeze row versions",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, multixact_freeze_table_age),
						 NULL, -1, 0, 2000000000);

	optionsSpecSetAddInt(stdrd_relopt_spec_set, "log_autovacuum_min_duration",
						 "Sets the minimum execution time above which autovacuum actions will be logged",
						 ShareUpdateExclusiveLock,
						 offsetof(StdRdOptions, autovacuum) +
						 offsetof(AutoVacOpts, log_min_duration),
						 NULL, -1, -1, INT_MAX);

	optionsSpecSetAddReal(stdrd_relopt_spec_set, "autovacuum_vacuum_cost_delay",
						  "Vacuum cost delay in milliseconds, for autovacuum",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, autovacuum) +
						  offsetof(AutoVacOpts, vacuum_cost_delay),
						  NULL, -1, 0.0, 100.0);

	optionsSpecSetAddReal(stdrd_relopt_spec_set, "autovacuum_vacuum_scale_factor",
						  "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, autovacuum) +
						  offsetof(AutoVacOpts, vacuum_scale_factor),
						  NULL, -1, 0.0, 100.0);

	optionsSpecSetAddReal(stdrd_relopt_spec_set, "autovacuum_vacuum_insert_scale_factor",
						  "Number of tuple inserts prior to vacuum as a fraction of reltuples",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, autovacuum) +
						  offsetof(AutoVacOpts, vacuum_ins_scale_factor),
						  NULL, -1, 0.0, 100.0);
	if (is_heap)
	{
		optionsSpecSetAddReal(stdrd_relopt_spec_set, "autovacuum_analyze_scale_factor",
							  "Number of tuple inserts, updates or deletes prior to analyze as a fraction of reltuples",
							  ShareUpdateExclusiveLock,
							  offsetof(StdRdOptions, autovacuum) +
							  offsetof(AutoVacOpts, analyze_scale_factor),
							  NULL, -1, 0.0, 100.0);

		optionsSpecSetAddInt(stdrd_relopt_spec_set, "toast_tuple_target",
							 "Sets the target tuple length at which external columns will be toasted",
							 ShareUpdateExclusiveLock,
							 offsetof(StdRdOptions, toast_tuple_target),
							 NULL, TOAST_TUPLE_TARGET, 128,
							 TOAST_TUPLE_TARGET_MAIN);

		optionsSpecSetAddBool(stdrd_relopt_spec_set, "user_catalog_table",
							  "Declare a table as an additional catalog table, e.g. for the purpose of logical replication",
							  AccessExclusiveLock,
							  offsetof(StdRdOptions, user_catalog_table),
							  NULL, false);

		optionsSpecSetAddInt(stdrd_relopt_spec_set, "parallel_workers",
							 "Number of parallel processes that can be used per executor node for this relation.",
							 ShareUpdateExclusiveLock,
							 offsetof(StdRdOptions, parallel_workers),
							 NULL, -1, 0, 1024);
	}

	optionsSpecSetAddEnum(stdrd_relopt_spec_set, "vacuum_index_cleanup",
						  "Controls index vacuuming and index cleanup",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, vacuum_index_cleanup),
						  NULL, StdRdOptIndexCleanupValues,
						  STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
						  gettext_noop("Valid values are \"on\", \"off\", and \"auto\"."));

	optionsSpecSetAddBool(stdrd_relopt_spec_set, "vacuum_truncate",
						  "Enables vacuum to truncate empty pages at the end of this table",
						  ShareUpdateExclusiveLock,
						  offsetof(StdRdOptions, vacuum_truncate),
						  NULL, true);

	if (is_heap)
		optionsSpecSetAddBool(stdrd_relopt_spec_set, "oids",
							  "Backward compatibility option. Will do nothing when false, will throw error when true",
							  NoLock,
							  -1,	/* Do not actually store it's value */
							  &oid_postvalidate, false);

	return stdrd_relopt_spec_set;
}


static options_spec_set *heap_relopt_spec_set = NULL;

options_spec_set *
get_heap_relopt_spec_set(void)
{
	if (heap_relopt_spec_set)
		return heap_relopt_spec_set;
	heap_relopt_spec_set = get_stdrd_relopt_spec_set(true);
	return heap_relopt_spec_set;
}

/*
 * These toast options are can't be set via SQL, but we should set them
 * to their defaults in binary representation, to make postgres work properly
 */
static void
toast_options_postprocess(void *data, bool validate)
{
	if (data)
	{
		StdRdOptions *toast_options = (StdRdOptions *) data;

		toast_options->fillfactor = 100;
		toast_options->autovacuum.analyze_threshold = -1;
		toast_options->autovacuum.analyze_scale_factor = -1;
	}
}

static options_spec_set *toast_relopt_spec_set = NULL;
options_spec_set *
get_toast_relopt_spec_set(void)
{
	if (toast_relopt_spec_set)
		return toast_relopt_spec_set;

	toast_relopt_spec_set = get_stdrd_relopt_spec_set(false);
	toast_relopt_spec_set->postprocess_fun = toast_options_postprocess;

	return toast_relopt_spec_set;
}


/*
 * Do not allow to set any option on partitioned table
 */
static void
partitioned_options_postprocess(void *data, bool validate)
{
	if (data && validate)
		ereport(ERROR,
				errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("cannot specify storage parameters for a partitioned table"),
				errhint("Specify storage parameters for its leaf partitions, instead."));
}


static options_spec_set *partitioned_relopt_spec_set = NULL;

options_spec_set *
get_partitioned_relopt_spec_set(void)
{
	if (partitioned_relopt_spec_set)
		return partitioned_relopt_spec_set;
	partitioned_relopt_spec_set = get_stdrd_relopt_spec_set(true);

	/* No options for now, so spec set is empty */
	partitioned_relopt_spec_set->postprocess_fun =
											   partitioned_options_postprocess;

	return partitioned_relopt_spec_set;
}

/*
 * Parse local options, allocate a bytea struct that's of the specified
 * 'base_size' plus any extra space that's needed for string variables,
 * fill its option's fields located at the given offsets and return it.
 */
void *
build_local_reloptions(local_relopts *relopts, Datum options, bool validate)
{
	void	   *opts;
	ListCell   *lc;
	List	   *values;

	values = optionsTextArrayToRawValues(options);
	values = optionsParseRawValues(values, relopts->spec_set, validate);
	opts = optionsValuesToBytea(values, relopts->spec_set);

	/*
	 * Kind of ugly conversion here for backward compatibility. Would be
	 * removed while moving opclass options to options.c API
	 */

	if (validate && relopts->validators)
	{
		int			val_count = 0;
		int			i;
		option_value *val_array;

		foreach(lc, values)
			val_count++;
		val_array = palloc(sizeof(option_value) * val_count);

		i = 0;
		foreach(lc, values)
		{
			option_value *val = lfirst(lc);

			memcpy(&(val_array[i]), val, sizeof(option_value));
			i++;
		}

		foreach(lc, relopts->validators)
			((relopts_validator) lfirst(lc)) (opts, val_array, val_count);

		pfree(val_array);
	}
	return opts;

}

/*
 * get_view_relopt_spec_set
 *		Returns an options catalog for view relation.
 */
static options_spec_set *view_relopt_spec_set = NULL;

options_spec_set *
get_view_relopt_spec_set(void)
{
	if (view_relopt_spec_set)
		return view_relopt_spec_set;

	view_relopt_spec_set = allocateOptionsSpecSet(NULL,
											  sizeof(ViewOptions), false, 3);

	optionsSpecSetAddBool(view_relopt_spec_set, "security_barrier",
						  "View acts as a row security barrier",
						  AccessExclusiveLock,
						  offsetof(ViewOptions, security_barrier), NULL, false);

	optionsSpecSetAddBool(view_relopt_spec_set, "security_invoker",
						  "Privileges on underlying relations are checked as the invoking user, not the view owner",
						  AccessExclusiveLock,
						  offsetof(ViewOptions, security_invoker), NULL, false);

	optionsSpecSetAddEnum(view_relopt_spec_set, "check_option",
						  "View has WITH CHECK OPTION defined (local or cascaded)",
						  AccessExclusiveLock,
						  offsetof(ViewOptions, check_option), NULL,
						  viewCheckOptValues,
						  VIEW_OPTION_CHECK_OPTION_NOT_SET,
						  gettext_noop("Valid values are \"local\" and \"cascaded\"."));

	return view_relopt_spec_set;
}

/*
 * get_attribute_options_spec_set
 *		Returns an options spec set for heap attributes
 */
static options_spec_set *attribute_options_spec_set = NULL;

options_spec_set *
get_attribute_options_spec_set(void)
{
	if (attribute_options_spec_set)
		return attribute_options_spec_set;

	attribute_options_spec_set = allocateOptionsSpecSet(NULL,
											sizeof(AttributeOpts), false, 2);

	optionsSpecSetAddReal(attribute_options_spec_set, "n_distinct",
						  "Sets the planner's estimate of the number of distinct values appearing in a column (excluding child relations).",
						  ShareUpdateExclusiveLock,
						  offsetof(AttributeOpts, n_distinct), NULL,
						  0, -1.0, DBL_MAX);

	optionsSpecSetAddReal(attribute_options_spec_set,
						  "n_distinct_inherited",
						  "Sets the planner's estimate of the number of distinct values appearing in a column (including child relations).",
						  ShareUpdateExclusiveLock,
						  offsetof(AttributeOpts, n_distinct_inherited), NULL,
						  0, -1.0, DBL_MAX);

	return attribute_options_spec_set;
}


/*
 * get_tablespace_options_spec_set
 *		Returns an options spec set for tablespaces
*/
static options_spec_set *tablespace_options_spec_set = NULL;

options_spec_set *
get_tablespace_options_spec_set(void)
{
	if (tablespace_options_spec_set)
		return tablespace_options_spec_set;

	tablespace_options_spec_set = allocateOptionsSpecSet(NULL,
											 sizeof(TableSpaceOpts), false, 4);
	optionsSpecSetAddReal(tablespace_options_spec_set,
						  "random_page_cost",
						  "Sets the planner's estimate of the cost of a nonsequentially fetched disk page",
						  ShareUpdateExclusiveLock,
						  offsetof(TableSpaceOpts, random_page_cost),
						  NULL, -1, 0.0, DBL_MAX);

	optionsSpecSetAddReal(tablespace_options_spec_set, "seq_page_cost",
						  "Sets the planner's estimate of the cost of a sequentially fetched disk page",
						  ShareUpdateExclusiveLock,
						  offsetof(TableSpaceOpts, seq_page_cost),
						  NULL, -1, 0.0, DBL_MAX);

	optionsSpecSetAddInt(tablespace_options_spec_set, "effective_io_concurrency",
						 "Number of simultaneous requests that can be handled efficiently by the disk subsystem",
						 ShareUpdateExclusiveLock,
						 offsetof(TableSpaceOpts, effective_io_concurrency),
						 NULL,
#ifdef USE_PREFETCH
						 -1, 0, MAX_IO_CONCURRENCY
#else
						 0, 0, 0
#endif
		);

	optionsSpecSetAddInt(tablespace_options_spec_set,
						 "maintenance_io_concurrency",
						 "Number of simultaneous requests that can be handled efficiently by the disk subsystem for maintenance work.",
						 ShareUpdateExclusiveLock,
						 offsetof(TableSpaceOpts, maintenance_io_concurrency),
						 NULL,
#ifdef USE_PREFETCH
						 -1, 0, MAX_IO_CONCURRENCY
#else
						 0, 0, 0
#endif
		);
	return tablespace_options_spec_set;
}

/*
 * Determine the required LOCKMODE from an option list.
 *
 * Called from AlterTableGetLockLevel(), see that function
 * for a longer explanation of how this works.
 */
LOCKMODE
AlterTableGetRelOptionsLockLevel(Relation rel, List *defList)
{
	LOCKMODE	lockmode = NoLock;
	ListCell   *cell;
	options_spec_set *spec_set = NULL;

	if (defList == NIL)
		return AccessExclusiveLock;

	switch (rel->rd_rel->relkind)
	{
		case RELKIND_TOASTVALUE:
			spec_set = get_toast_relopt_spec_set();
			break;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			spec_set = get_heap_relopt_spec_set();
			break;
		case RELKIND_INDEX:
			spec_set = rel->rd_indam->amreloptspecset();
			break;
		case RELKIND_VIEW:
			spec_set = get_view_relopt_spec_set();
			break;
		case RELKIND_PARTITIONED_TABLE:
			spec_set = get_partitioned_relopt_spec_set();
			break;
		default:
			Assert(false);		/* can't get here */
			break;
	}
	Assert(spec_set);			/* No spec set - no reloption change. Should
								 * never get here */

	foreach(cell, defList)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		int			i;

		for (i = 0; i < spec_set->num; i++)
		{
			option_spec_basic *gen = spec_set->definitions[i];

			if (pg_strcasecmp(gen->name,
							  def->defname) == 0)
				if (lockmode < gen->lockmode)
					lockmode = gen->lockmode;
		}
	}
	return lockmode;
}
