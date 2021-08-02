/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the "publication" system catalog (pg_publication)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_H
#define PG_PUBLICATION_H

#include "catalog/genbki.h"
#include "catalog/pg_publication_d.h"

/* ----------------
 *		pg_publication definition.  cpp turns this into
 *		typedef struct FormData_pg_publication
 * ----------------
 */
CATALOG(pg_publication,6104,PublicationRelationId)
{
	Oid			oid;			/* oid */

	NameData	pubname;		/* name of the publication */

	Oid			pubowner BKI_LOOKUP(pg_authid); /* publication owner */

	/*
	 * indicates that this is special publication which should encompass all
	 * tables in the database (except for the unlogged and temp ones)
	 */
	bool		puballtables;

	/* true if inserts are published */
	bool		pubinsert;

	/* true if updates are published */
	bool		pubupdate;

	/* true if deletes are published */
	bool		pubdelete;

	/* true if truncates are published */
	bool		pubtruncate;

	/* true if partition changes are published using root schema */
	bool		pubviaroot;

	/* see PUBTYPE_xxx constants below */
	char		pubtype;
} FormData_pg_publication;

/* ----------------
 *		Form_pg_publication corresponds to a pointer to a tuple with
 *		the format of pg_publication relation.
 * ----------------
 */
typedef FormData_pg_publication *Form_pg_publication;

DECLARE_UNIQUE_INDEX_PKEY(pg_publication_oid_index, 6110, PublicationObjectIndexId, on pg_publication using btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_publication_pubname_index, 6111, PublicationNameIndexId, on pg_publication using btree(pubname name_ops));

typedef struct PublicationActions
{
	bool		pubinsert;
	bool		pubupdate;
	bool		pubdelete;
	bool		pubtruncate;
} PublicationActions;

typedef struct Publication
{
	Oid			oid;
	char	   *name;
	bool		alltables;
	bool		pubviaroot;
	PublicationActions pubactions;
	char		pubtype;
} Publication;

/*---------
 * Expected values for pub_partopt parameter of GetRelationPublications(),
 * which allows callers to specify which partitions of partitioned tables
 * mentioned in the publication they expect to see.
 *
 *	ROOT:	only the table explicitly mentioned in the publication
 *	LEAF:	only leaf partitions in given tree
 *	ALL:	all partitions in given tree
 */
typedef enum PublicationPartOpt
{
	PUBLICATION_PART_ROOT,
	PUBLICATION_PART_LEAF,
	PUBLICATION_PART_ALL,
} PublicationPartOpt;

/* Publication types */
#define PUBTYPE_ALLTABLES	'a'		/* all tables publication */
#define PUBTYPE_TABLE		't'		/* table publication */
#define PUBTYPE_SCHEMA		's'		/* schema publication */
#define PUBTYPE_EMPTY		'e'		/* empty publication */

/*
 * Return the publication type.
*/
static inline char
get_publication_type(char *strpubtype)
{
	if (strcmp(strpubtype, "a") == 0)
		return PUBTYPE_ALLTABLES;
	else if (strcmp(strpubtype, "t") == 0)
		return PUBTYPE_TABLE;
	else if (strcmp(strpubtype, "s") == 0)
		return PUBTYPE_SCHEMA;

	return PUBTYPE_EMPTY;
}

#endif							/* PG_PUBLICATION_H */
