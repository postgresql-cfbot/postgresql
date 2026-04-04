/*-------------------------------------------------------------------------
 *
 * jsontypes.h
 *	  Declarations for JSON type categorization.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/jsontypes.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONTYPES_H
#define JSONTYPES_H

#include "fmgr.h"

/* Type categories returned by json_categorize_type */
typedef enum
{
	JSONTYPE_NULL,				/* null, so we didn't bother to identify */
	JSONTYPE_BOOL,				/* boolean (built-in types only) */
	JSONTYPE_NUMERIC,			/* numeric (ditto) */
	JSONTYPE_DATE,				/* we use special formatting for datetimes */
	JSONTYPE_TIMESTAMP,
	JSONTYPE_TIMESTAMPTZ,
	JSONTYPE_JSON,				/* JSON (and JSONB, if not is_jsonb) */
	JSONTYPE_JSONB,				/* JSONB (if is_jsonb) */
	JSONTYPE_ARRAY,				/* array */
	JSONTYPE_COMPOSITE,			/* composite */
	JSONTYPE_CAST,				/* something with an explicit cast to JSON */
	JSONTYPE_OTHER,				/* all else */
} JsonTypeCategory;

extern void json_categorize_type(Oid typoid, bool is_jsonb,
								 JsonTypeCategory *tcategory,
								 FmgrInfo *outflinfo);
extern void json_check_mutability(Oid typoid, bool *has_mutable);

#endif							/* JSONTYPES_H */
