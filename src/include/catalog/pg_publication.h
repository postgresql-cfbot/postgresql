/*-------------------------------------------------------------------------
 *
 * pg_publication.h
 *	  definition of the "publication" system catalog (pg_publication)
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
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
#include "nodes/pg_list.h"

/* Publication trigger events */
#define PUB_TRIG_EVENT1 "ddl_command_end"
#define PUB_TRIG_EVENT2 "ddl_command_start"
#define PUB_TRIG_EVENT3 "table_rewrite"
#define PUB_TRIG_EVENT4 "table_init_write"

/* Publication event trigger prefix */
#define PUB_EVENT_TRIG_PREFIX "pg_deparse_trig_%s_%u"

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

	/* true if table ddls are published */
	bool		pubddl_table;

	/* true if index ddls are published */
	bool		pubddl_index;

	/* true if all supported ddls are published */
	bool		pubddl_all;
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
	bool		pubddl_table;
	bool		pubddl_index;
	bool		pubddl_all;
} PublicationActions;

typedef struct PublicationDesc
{
	PublicationActions pubactions;

	/*
	 * true if the columns referenced in row filters which are used for UPDATE
	 * or DELETE are part of the replica identity or the publication actions
	 * do not include UPDATE or DELETE.
	 */
	bool		rf_valid_for_update;
	bool		rf_valid_for_delete;

	/*
	 * true if the columns are part of the replica identity or the publication
	 * actions do not include UPDATE or DELETE.
	 */
	bool		cols_valid_for_update;
	bool		cols_valid_for_delete;
} PublicationDesc;

typedef struct Publication
{
	Oid			oid;
	char	   *name;
	bool		alltables;
	bool		pubviaroot;
	PublicationActions pubactions;
} Publication;

extern Publication *GetPublication(Oid pubid);
extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);

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

extern List *GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt);
extern List *GetAllTablesPublications(void);
extern List *GetAllTablesPublicationRelations(bool pubviaroot);
extern List *GetPublicationSchemas(Oid pubid);
extern List *GetSchemaPublications(Oid schemaid);
extern List *GetSchemaPublicationRelations(Oid schemaid,
										   PublicationPartOpt pub_partopt);
extern List *GetAllSchemaPublicationRelations(Oid pubid,
											  PublicationPartOpt pub_partopt);
extern List *GetPubPartitionOptionRelations(List *result,
											PublicationPartOpt pub_partopt,
											Oid relid);
extern Oid	GetTopMostAncestorInPublication(Oid puboid, List *ancestors,
											int *ancestor_level);

extern bool is_schema_publication(Oid pubid);

#endif							/* PG_PUBLICATION_H */
