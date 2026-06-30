/*-------------------------------------------------------------------------
 *
 * pg_format_cast.h
 *	  definition of the "format cast" system catalog (pg_format_cast)
 *
 * A format cast registers a function that implements
 * CAST(expr AS target FORMAT format_expr) for a particular
 * (source type, target type) pair.  The format cast function receives the
 * source value and the FORMAT expression (as text) and returns the target
 * type.  This is intentionally separate from pg_cast: ordinary casts have
 * implicit/assignment/explicit contexts and binary-coercible/WITH INOUT/
 * WITHOUT FUNCTION methods, whereas a formatted cast is always explicit and
 * always requires a function.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_format_cast.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FORMAT_CAST_H
#define PG_FORMAT_CAST_H

#include "catalog/genbki.h"
#include "catalog/pg_format_cast_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_format_cast definition.  cpp turns this into
 *		typedef struct FormData_pg_format_cast
 * ----------------
 */
BEGIN_CATALOG_STRUCT

CATALOG(pg_format_cast,8647,FormatCastRelationId)
{
	Oid			oid;			/* oid */
	Oid			fmtsource BKI_LOOKUP(pg_type);	/* source type */
	Oid			fmttarget BKI_LOOKUP(pg_type);	/* target type */
	Oid			fmtfunc BKI_LOOKUP(pg_proc);	/* format cast function */
} FormData_pg_format_cast;

END_CATALOG_STRUCT

/* ----------------
 *		Form_pg_format_cast corresponds to a pointer to a tuple with
 *		the format of pg_format_cast relation.
 * ----------------
 */
typedef FormData_pg_format_cast *Form_pg_format_cast;

DECLARE_UNIQUE_INDEX_PKEY(pg_format_cast_oid_index, 8648, FormatCastOidIndexId, pg_format_cast, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_format_cast_source_target_index, 8649, FormatCastSourceTargetIndexId, pg_format_cast, btree(fmtsource oid_ops, fmttarget oid_ops));

MAKE_SYSCACHE(FORMATCASTOID, pg_format_cast_oid_index, 8);
MAKE_SYSCACHE(FORMATCASTSOURCETARGET, pg_format_cast_source_target_index, 8);

#endif							/* PG_FORMAT_CAST_H */
