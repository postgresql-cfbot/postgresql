/*-------------------------------------------------------------------------
 *
 * pg_enum.h
 *	  definition of the system "enum" relation (pg_enum)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2006-2018, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_enum.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ENUM_H
#define PG_ENUM_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_enum definition.  cpp turns this into
 *		typedef struct FormData_pg_enum
 * ----------------
 */
#define EnumRelationId	3501

CATALOG(pg_enum,3501)
{
	Oid			enumtypid;		/* OID of owning enum type */
	float4		enumsortorder;	/* sort position of this enum value */
	NameData	enumlabel;		/* text representation of enum value */
} FormData_pg_enum;

DECLARE_UNIQUE_INDEX(pg_enum_oid_index, 3502, on pg_enum using btree(oid oid_ops));
#define EnumOidIndexId	3502
DECLARE_UNIQUE_INDEX(pg_enum_typid_label_index, 3503, on pg_enum using btree(enumtypid oid_ops, enumlabel name_ops));
#define EnumTypIdLabelIndexId 3503
DECLARE_UNIQUE_INDEX(pg_enum_typid_sortorder_index, 3534, on pg_enum using btree(enumtypid oid_ops, enumsortorder float4_ops));
#define EnumTypIdSortOrderIndexId 3534

/* ----------------
 *		Form_pg_enum corresponds to a pointer to a tuple with
 *		the format of pg_enum relation.
 * ----------------
 */
typedef FormData_pg_enum *Form_pg_enum;

/* ----------------
 *		compiler constants for pg_enum
 * ----------------
 */
#define Natts_pg_enum					3
#define Anum_pg_enum_enumtypid			1
#define Anum_pg_enum_enumsortorder		2
#define Anum_pg_enum_enumlabel			3

/*
 * prototypes for functions in pg_enum.c
 */
extern void EnumValuesCreate(Oid enumTypeOid, List *vals);
extern void EnumValuesDelete(Oid enumTypeOid);
extern void AddEnumLabel(Oid enumTypeOid, const char *newVal,
			 const char *neighbor, bool newValIsAfter,
			 bool skipIfExists);
extern void RenameEnumLabel(Oid enumTypeOid,
				const char *oldVal, const char *newVal);

#endif							/* PG_ENUM_H */
