/*-------------------------------------------------------------------------
 *
 * jsontypes.c
 *	  Functions for JSON type categorization.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsontypes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/jsontypes.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

static Oid	get_json_cast_for_type(Oid typoid);

/*
 * Determine how we want to print values of a given type in datum_to_json(b).
 *
 * Given the datatype OID, return its JsonTypeCategory, as well as an FmgrInfo
 * for the type's output function or cast function.  For categories that do not
 * require calling a function, outflinfo is not touched.
 */
void
json_categorize_type(Oid typoid, bool is_jsonb,
					 JsonTypeCategory *tcategory, FmgrInfo *outflinfo)
{
	bool		use_type_output_function = false;

	/* Look through any domain */
	typoid = getBaseType(typoid);

	switch (typoid)
	{
		case BOOLOID:
			*tcategory = JSONTYPE_BOOL;
			break;

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			use_type_output_function = true;
			*tcategory = JSONTYPE_NUMERIC;
			break;

		case DATEOID:
			*tcategory = JSONTYPE_DATE;
			break;

		case TIMESTAMPOID:
			*tcategory = JSONTYPE_TIMESTAMP;
			break;

		case TIMESTAMPTZOID:
			*tcategory = JSONTYPE_TIMESTAMPTZ;
			break;

		case JSONOID:
			use_type_output_function = !is_jsonb;
			*tcategory = JSONTYPE_JSON;
			break;

		case JSONBOID:
			use_type_output_function = !is_jsonb;
			*tcategory = is_jsonb ? JSONTYPE_JSONB : JSONTYPE_JSON;
			break;

		default:
			/* Check for arrays and composites */
			if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID
				|| typoid == ANYCOMPATIBLEARRAYOID || typoid == RECORDARRAYOID)
				*tcategory = JSONTYPE_ARRAY;
			else if (type_is_rowtype(typoid))	/* includes RECORDOID */
				*tcategory = JSONTYPE_COMPOSITE;
			else
			{
				Oid			castfunc = get_json_cast_for_type(typoid);

				if (OidIsValid(castfunc))
				{
					fmgr_info(castfunc, outflinfo);
					*tcategory = JSONTYPE_CAST;
				}
				else
				{
					use_type_output_function = true;
					*tcategory = JSONTYPE_OTHER;
				}
			}
			break;
	}

	if (use_type_output_function)
	{
		Oid			typoutput;
		bool		typisvarlena;

		getTypeOutputInfo(typoid, &typoutput, &typisvarlena);
		fmgr_info(typoutput, outflinfo);
	}
}

/*
 * Check whether a type conversion to JSON or JSONB involves any mutable
 * functions.  This recurses into container types (arrays, composites,
 * ranges, multiranges, domains) to check their element/sub types.
 *
 * The caller must initialize *has_mutable to false before calling.
 * If any mutable function is found, *has_mutable is set to true.
 */
void
json_check_mutability(Oid typoid, bool *has_mutable)
{
	char		att_typtype = get_typtype(typoid);

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	Assert(has_mutable != NULL);

	if (*has_mutable)
		return;

	if (att_typtype == TYPTYPE_DOMAIN)
	{
		json_check_mutability(getBaseType(typoid), has_mutable);
		return;
	}
	else if (att_typtype == TYPTYPE_COMPOSITE)
	{
		/*
		 * For a composite type, recurse into its attributes.  Use the
		 * typcache to avoid opening the relation directly.
		 */
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typoid, -1);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			json_check_mutability(attr->atttypid, has_mutable);
			if (*has_mutable)
				break;
		}
		ReleaseTupleDesc(tupdesc);
		return;
	}
	else if (att_typtype == TYPTYPE_RANGE)
	{
		json_check_mutability(get_range_subtype(typoid), has_mutable);
		return;
	}
	else if (att_typtype == TYPTYPE_MULTIRANGE)
	{
		json_check_mutability(get_multirange_range(typoid), has_mutable);
		return;
	}
	else
	{
		Oid			att_typelem = get_element_type(typoid);

		if (OidIsValid(att_typelem))
		{
			/* recurse into array element type */
			json_check_mutability(att_typelem, has_mutable);
			return;
		}
	}

	switch (typoid)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case JSONOID:
		case JSONBOID:
			/* known immutable */
			break;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			*has_mutable = true;
			break;
		default:
			{
				Oid			castfunc = get_json_cast_for_type(typoid);
				Oid			funcoid;

				if (OidIsValid(castfunc))
					funcoid = castfunc;
				else
				{
					bool		typisvarlena;

					getTypeOutputInfo(typoid, &funcoid, &typisvarlena);
				}
				if (func_volatile(funcoid) != PROVOLATILE_IMMUTABLE)
					*has_mutable = true;
			}
			break;
	}
}

/*
 * Return the OID of a cast function from typoid to JSON, or InvalidOid if
 * there is no such cast.  As a matter of policy, we only consider explicit,
 * user-defined casts.
 */
static Oid
get_json_cast_for_type(Oid typoid)
{
	if (typoid >= FirstNormalObjectId)
	{
		Oid			castfunc;
		CoercionPathType ctype;

		ctype = find_coercion_pathway(JSONOID, typoid,
									  COERCION_EXPLICIT,
									  &castfunc);
		if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
			return castfunc;
	}
	return InvalidOid;
}
