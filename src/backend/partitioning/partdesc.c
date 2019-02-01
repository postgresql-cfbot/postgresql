/*-------------------------------------------------------------------------
 *
 * partdesc.c
 *		Support routines for manipulating partition descriptors
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/backend/partitioning/partdesc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "storage/sinval.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/partcache.h"
#include "utils/syscache.h"

typedef struct PartitionDirectoryData
{
	MemoryContext pdir_mcxt;
	HTAB	   *pdir_hash;
} PartitionDirectoryData;

typedef struct PartitionDirectoryEntry
{
	Oid			reloid;
	PartitionDesc pd;
} PartitionDirectoryEntry;

/*
 * RelationBuildPartitionDesc
 *		Form rel's partition descriptor
 *
 * Not flushed from the cache by RelationClearRelation() unless changed because
 * of addition or removal of partition.
 */
void
RelationBuildPartitionDesc(Relation rel)
{
	PartitionDesc partdesc;
	PartitionBoundInfo boundinfo = NULL;
	List	   *inhoids;
	PartitionBoundSpec **boundspecs = NULL;
	Oid		   *oids = NULL;
	ListCell   *cell;
	int			i,
				nparts;
	PartitionKey key = RelationGetPartitionKey(rel);
	MemoryContext oldcxt;
	int		   *mapping;

	/*
	 * Fetch catalog information.  Since we want to allow partitions to be
	 * added and removed without holding AccessExclusiveLock on the parent
	 * table, it's possible that the catalog contents could be changing under
	 * us.  That means that by by the time we fetch the partition bound for a
	 * partition returned by find_inheritance_children, it might no longer be
	 * a partition or might even be a partition of some other table.
	 *
	 * To ensure that we get a consistent view of the catalog data, we first
	 * fetch everything we need and then call AcceptInvalidationMessages. If
	 * SharedInvalidMessageCounter advances between the time we start fetching
	 * information and the time AcceptInvalidationMessages() completes, that
	 * means something may have changed under us, so we start over and do it
	 * all again.
	 */
	for (;;)
	{
		uint64		inval_count = SharedInvalidMessageCounter;

		/* Get partition oids from pg_inherits */
		inhoids = find_inheritance_children(RelationGetRelid(rel), NoLock);
		nparts = list_length(inhoids);

		if (nparts > 0)
		{
			oids = palloc(nparts * sizeof(Oid));
			boundspecs = palloc(nparts * sizeof(PartitionBoundSpec *));
		}

		/* Collect bound spec nodes for each partition */
		i = 0;
		foreach(cell, inhoids)
		{
			Oid			inhrelid = lfirst_oid(cell);
			HeapTuple	tuple;
			PartitionBoundSpec *boundspec = NULL;

			/*
			 * Don't put any sanity checks here that might fail as a result of
			 * concurrent DDL, such as a check that relpartbound is not NULL.
			 * We could transiently see such states as a result of concurrent
			 * DDL.  Such checks can be performed only after we're sure we got
			 * a consistent view of the underlying data.
			 */
			tuple = SearchSysCache1(RELOID, inhrelid);
			if (HeapTupleIsValid(tuple))
			{
				Datum		datum;
				bool		isnull;

				datum = SysCacheGetAttr(RELOID, tuple,
										Anum_pg_class_relpartbound,
										&isnull);
				if (!isnull)
					boundspec = stringToNode(TextDatumGetCString(datum));
				ReleaseSysCache(tuple);
			}

			oids[i] = inhrelid;
			boundspecs[i] = boundspec;
			++i;
		}

		/*
		 * If no relevant catalog changes have occurred (see comments at the
		 * top of this loop, then we got a consistent view of our partition
		 * list and can stop now.
		 */
		AcceptInvalidationMessages();
		if (inval_count == SharedInvalidMessageCounter)
			break;

		/* Something changed, so retry from the top. */
		if (oids != NULL)
		{
			pfree(oids);
			oids = NULL;
		}
		if (boundspecs != NULL)
		{
			pfree(boundspecs);
			boundspecs = NULL;
		}
		if (inhoids != NIL)
			list_free(inhoids);
	}

	/*
	 * At this point, we should have a consistent view of the data we got from
	 * pg_inherits and pg_class, so it's safe to perform some sanity checks.
	 */
	for (i = 0; i < nparts; ++i)
	{
		Oid			inhrelid = oids[i];
		PartitionBoundSpec *spec = boundspecs[i];

		if (!spec)
			elog(ERROR, "missing relpartbound for relation %u", inhrelid);
		if (!IsA(spec, PartitionBoundSpec))
			elog(ERROR, "invalid relpartbound for relation %u", inhrelid);

		/*
		 * If the PartitionBoundSpec says this is the default partition, its
		 * OID should match pg_partitioned_table.partdefid; if not, the
		 * catalog is corrupt.
		 */
		if (spec->is_default)
		{
			Oid			partdefid;

			partdefid = get_default_partition_oid(RelationGetRelid(rel));
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}
	}

	/* Now build the actual relcache partition descriptor */
	rel->rd_pdcxt = AllocSetContextCreate(CacheMemoryContext,
										  "partition descriptor",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextCopyAndSetIdentifier(rel->rd_pdcxt,
									  RelationGetRelationName(rel));

	oldcxt = MemoryContextSwitchTo(rel->rd_pdcxt);
	partdesc = (PartitionDescData *) palloc0(sizeof(PartitionDescData));
	partdesc->nparts = nparts;
	/* oids and boundinfo are allocated below. */

	MemoryContextSwitchTo(oldcxt);

	if (nparts == 0)
	{
		rel->rd_partdesc = partdesc;
		return;
	}

	/* First create PartitionBoundInfo */
	boundinfo = partition_bounds_create(boundspecs, nparts, key, &mapping);

	/* Now copy boundinfo and oids into partdesc. */
	oldcxt = MemoryContextSwitchTo(rel->rd_pdcxt);
	partdesc->boundinfo = partition_bounds_copy(boundinfo, key);
	partdesc->oids = (Oid *) palloc(partdesc->nparts * sizeof(Oid));
	partdesc->is_leaf = (bool *) palloc(partdesc->nparts * sizeof(bool));

	/*
	 * Now assign OIDs from the original array into mapped indexes of the
	 * result array.  The order of OIDs in the former is defined by the
	 * catalog scan that retrieved them, whereas that in the latter is defined
	 * by canonicalized representation of the partition bounds.
	 */
	for (i = 0; i < partdesc->nparts; i++)
	{
		int			index = mapping[i];

		partdesc->oids[index] = oids[i];
		/* Record if the partition is a leaf partition */
		partdesc->is_leaf[index] =
			(get_rel_relkind(oids[i]) != RELKIND_PARTITIONED_TABLE);
	}
	MemoryContextSwitchTo(oldcxt);

	rel->rd_partdesc = partdesc;
}

/*
 * CreatePartitionDirectory
 *		Create a new partition directory object.
 */
PartitionDirectory
CreatePartitionDirectory(MemoryContext mcxt)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(mcxt);
	PartitionDirectory pdir;
	HASHCTL		ctl;

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartitionDirectoryEntry);
	ctl.hcxt = mcxt;

	pdir = palloc(sizeof(PartitionDirectoryData));
	pdir->pdir_mcxt = mcxt;
	pdir->pdir_hash = hash_create("partition directory", 256, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(oldcontext);
	return pdir;
}

/*
 * PartitionDirectoryLookup
 *		Look up the partition descriptor for a relation in the directory.
 *
 * The purpose of this function is to ensure that we get the same
 * PartitionDesc for each relation every time we look it up.  In the
 * face of current DDL, different PartitionDescs may be constructed with
 * different views of the catalog state, but any single particular OID
 * will always get the same PartitionDesc for as long as the same
 * PartitionDirectory is used.
 */
PartitionDesc
PartitionDirectoryLookup(PartitionDirectory pdir, Relation rel)
{
	PartitionDirectoryEntry *pde;
	Oid			relid = RelationGetRelid(rel);
	bool		found;

	pde = hash_search(pdir->pdir_hash, &relid, HASH_ENTER, &found);
	if (!found)
		pde->pd = RelationGetPartitionDesc(rel);
	return pde->pd;
}

/*
 * equalPartitionDescs
 *		Compare two partition descriptors for logical equality
 */
bool
equalPartitionDescs(PartitionKey key, PartitionDesc partdesc1,
					PartitionDesc partdesc2)
{
	int			i;

	if (partdesc1 != NULL)
	{
		if (partdesc2 == NULL)
			return false;
		if (partdesc1->nparts != partdesc2->nparts)
			return false;

		Assert(key != NULL || partdesc1->nparts == 0);

		/*
		 * Same oids? If the partitioning structure did not change, that is,
		 * no partitions were added or removed to the relation, the oids array
		 * should still match element-by-element.
		 */
		for (i = 0; i < partdesc1->nparts; i++)
		{
			if (partdesc1->oids[i] != partdesc2->oids[i])
				return false;
		}

		/*
		 * Now compare partition bound collections.  The logic to iterate over
		 * the collections is private to partition.c.
		 */
		if (partdesc1->boundinfo != NULL)
		{
			if (partdesc2->boundinfo == NULL)
				return false;

			if (!partition_bounds_equal(key->partnatts, key->parttyplen,
										key->parttypbyval,
										partdesc1->boundinfo,
										partdesc2->boundinfo))
				return false;
		}
		else if (partdesc2->boundinfo != NULL)
			return false;
	}
	else if (partdesc2 != NULL)
		return false;

	return true;
}

/*
 * get_default_oid_from_partdesc
 *
 * Given a partition descriptor, return the OID of the default partition, if
 * one exists; else, return InvalidOid.
 */
Oid
get_default_oid_from_partdesc(PartitionDesc partdesc)
{
	if (partdesc && partdesc->boundinfo &&
		partition_bound_has_default(partdesc->boundinfo))
		return partdesc->oids[partdesc->boundinfo->default_index];

	return InvalidOid;
}
