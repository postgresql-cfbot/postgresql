/*-------------------------------------------------------------------------
 *
 * relfilelocator.h
 *	  Physical access information for relations.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/relfilelocator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELFILELOCATOR_H
#define RELFILELOCATOR_H

#include "common/relpath.h"
#include "storage/backendid.h"

/*
 * RelFileLocator must provide all that we need to know to physically access
 * a relation, with the exception of the backend ID, which can be provided
 * separately. Note, however, that a "physical" relation is comprised of
 * multiple files on the filesystem, as each fork is stored as a separate
 * file, and each fork can be divided into multiple segments. See md.c.
 *
 * spcOid identifies the tablespace of the relation.  It corresponds to
 * pg_tablespace.oid.
 *
 * dbOid identifies the database of the relation.  It is zero for
 * "shared" relations (those common to all databases of a cluster).
 * Nonzero dbOid values correspond to pg_database.oid.
 *
 * relNumber identifies the specific relation.  relNumber corresponds to
 * pg_class.relfilenode (NOT pg_class.oid, because we need to be able
 * to assign new physical files to relations in some situations).
 * Notice that relNumber is only unique within a database in a particular
 * tablespace.
 *
 * Note: spcOid must be GLOBALTABLESPACE_OID if and only if dbOid is
 * zero.  We support shared relations only in the "global" tablespace.
 *
 * Note: in pg_class we allow reltablespace == 0 to denote that the
 * relation is stored in its database's "default" tablespace (as
 * identified by pg_database.dattablespace).  However this shorthand
 * is NOT allowed in RelFileLocator structs --- the real tablespace ID
 * must be supplied when setting spcOid.
 *
 * Note: in pg_class, relfilenode can be zero to denote that the relation
 * is a "mapped" relation, whose current true filenode number is available
 * from relmapper.c.  Again, this case is NOT allowed in RelFileLocators.
 *
 * Note: various places use RelFileLocator in hashtable keys.  Therefore,
 * there *must not* be any unused padding bytes in this struct.  That
 * should be safe as long as all the fields are of type Oid.
 *
 * See also SMgrFileLocator in smgr.h.
 */
typedef struct RelFileLocator
{
	Oid			spcOid;			/* tablespace */
	Oid			dbOid;			/* database */
	RelFileNumber relNumber;	/* relation */
} RelFileLocator;

/*
 * Note: RelFileLocatorEquals compares relNumber
 * first since that is most likely to be different in two unequal
 * RelFileLocators.  It is probably redundant to compare spcOid if the other
 * fields are found equal, but do it anyway to be sure.  Likewise for checking
 * the backend ID in SMgrFileLocatorBackendEquals.
 */
#define RelFileLocatorEquals(locator1, locator2) \
	((locator1).relNumber == (locator2).relNumber && \
	 (locator1).dbOid == (locator2).dbOid && \
	 (locator1).spcOid == (locator2).spcOid)

#endif							/* RELFILELOCATOR_H */
