/*-------------------------------------------------------------------------
 *
 * publicationcmds.h
 *	  prototypes for publicationcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/publicationcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PUBLICATIONCMDS_H
#define PUBLICATIONCMDS_H

#include "catalog/objectaddress.h"
#include "catalog/pg_publication.h"
#include "parser/parse_node.h"

extern ObjectAddress CreatePublication(ParseState *pstate, CreatePublicationStmt *stmt);
extern void AlterPublication(ParseState *pstate, AlterPublicationStmt *stmt);
extern void RemovePublicationRelById(Oid proid);
extern void RemovePublicationSchemaById(Oid psoid);

extern ObjectAddress AlterPublicationOwner(const char *name, Oid newOwnerId);
extern void AlterPublicationOwner_oid(Oid pubid, Oid newOwnerId);

extern Publication *GetPublication(Oid pubid);
extern Publication *GetPublicationByName(const char *pubname, bool missing_ok);
extern List *GetRelationPublications(Oid relid);

extern List *GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt);
extern List *GetPublicationSchemas(Oid pubid);
extern List *GetSchemaPublications(Oid schemaid);
extern List *GetAllTablesPublications(void);
extern List *GetAllTablesPublicationRelations(bool pubviaroot, Oid schemaOid);
extern List *GetAllSchemasPublicationRelations(bool pubviaroot, Oid puboid);

extern bool is_publishable_relation(Relation rel);
extern ObjectAddress publication_add_relation(Oid pubid, Relation targetrel,
											  bool if_not_exists);
extern ObjectAddress publication_add_schema(Oid pubid, Oid schemaoid,
											bool if_not_exists);

extern Oid	get_publication_oid(const char *pubname, bool missing_ok);
extern char *get_publication_name(Oid pubid, bool missing_ok);

#endif							/* PUBLICATIONCMDS_H */
