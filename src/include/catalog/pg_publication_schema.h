/*-------------------------------------------------------------------------
 *
 * pg_publication_schema.h
 *	  definition of the system catalog for mappings between schemas and
 *	  publications (pg_publication_schema)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication_schema.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_SCHEMA_H
#define PG_PUBLICATION_SCHEMA_H

#include "catalog/genbki.h"
#include "catalog/pg_publication_schema_d.h"


/* ----------------
 *		pg_publication_schema definition.  cpp turns this into
 *		typedef struct FormData_pg_publication_schema
 * ----------------
 */
CATALOG(pg_publication_schema,8901,PublicationSchemaRelationId)
{
	Oid			oid;			/* oid */
	Oid			pspubid BKI_LOOKUP(pg_publication); /* Oid of the publication */
	Oid			psnspcid BKI_LOOKUP(pg_class);	/* Oid of the schema */
} FormData_pg_publication_schema;

/* ----------------
 *		Form_pg_publication_schema corresponds to a pointer to a tuple with
 *		the format of pg_publication_schema relation.
 * ----------------
 */
typedef FormData_pg_publication_schema *Form_pg_publication_schema;

DECLARE_UNIQUE_INDEX_PKEY(pg_publication_schema_oid_index, 8902, PublicationSchemaObjectIndexId, on pg_publication_schema using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_publication_schema_psnspcid_pspubid_index, 8903, PublicationSchemaPsnspcidPspubidIndexId, on pg_publication_schema using btree(psnspcid oid_ops, pspubid oid_ops));

#endif							/* PG_PUBLICATION_SCHEMA_H */
