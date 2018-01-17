/*-------------------------------------------------------------------------
 *
 * indexing.h
 *	  This file provides some definitions to support indexing
 *	  on system catalogs
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/indexing.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXING_H
#define INDEXING_H

#include "access/htup.h"
#include "utils/relcache.h"

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *CatalogIndexState;

/*
 * indexing.c prototypes
 */
extern CatalogIndexState CatalogOpenIndexes(Relation heapRel);
extern void CatalogCloseIndexes(CatalogIndexState indstate);
extern Oid	CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern Oid CatalogTupleInsertWithInfo(Relation heapRel, HeapTuple tup,
						   CatalogIndexState indstate);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid,
				   HeapTuple tup);
extern void CatalogTupleUpdateWithInfo(Relation heapRel,
						   ItemPointer otid, HeapTuple tup,
						   CatalogIndexState indstate);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#endif							/* INDEXING_H */
