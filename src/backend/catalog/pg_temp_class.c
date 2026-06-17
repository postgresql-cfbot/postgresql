/*-------------------------------------------------------------------------
 *
 * pg_temp_class.c
 *	  routines to support manipulation of the pg_temp_class relation
 *
 * The pg_temp_class system catalog table is a global temporary table that
 * stores local overrides to various fields from the pg_class table for the
 * duration of the current session.  Currently, this is only used for
 * global temporary relations, though in the future, it might also be used
 * for local temporary relations.
 *
 * Tuples are first added to pg_temp_class when global temporary relations
 * (including pg_temp_class itself) are created or opened for the first
 * time in a session.  This "first time" might be repeated if a previous
 * "first time" insert was rolled back.
 *
 * All tuples to be inserted into pg_temp_class are held in a "pending"
 * queue, rather than being written out immediately, delaying the point at
 * which the tuple for pg_temp_class itself is inserted until after the
 * relation has been fully opened.  This pending queue also serves a number
 * of other useful purposes --- it prevents system catalog updates in what
 * might otherwise be expected to be read-only contexts (for example, while
 * opening a global temporary relation at parse time);  it allows new
 * pg_temp_class tuples to be seen in the current command, without having
 * to issue a CommandCounterIncrement(); and it allows us to delay opening
 * (and hence creating storage for) pg_temp_class itself, until we have to.
 * This last point is more than just an optimization --- in a read-only
 * context, where we know that we haven't written to pg_temp_class, we are
 * careful to never open it, which would cause it to have local storage
 * created.
 *
 * Pending inserts to pg_temp_class are held in a hash table, and may be
 * updated or deleted using the functions defined here.  They are flushed
 * to the database at the end of any non-read-only command and when
 * committing a transaction or subtransaction.  Note, however, that the
 * pending insert entries are kept in the hash table even after they have
 * been flushed to the database, and they are kept up-to-date (and in sync
 * with the database, if they have been flushed), until the end of the
 * transaction, when they are discarded.
 *
 * NB: All reads and writes to pg_temp_class by backend code must go
 * through the functions defined here (though user SQL queries may read it
 * normally).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_temp_class.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_temp_class.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * Have we opened and initialized pg_temp_class in this session?
 */
static bool pg_temp_class_opened = false;

/*
 * Subtransaction ID in which pg_temp_class was opened, if it was opened in
 * the current transaction, else zero.
 */
static SubTransactionId pg_temp_class_subid = InvalidSubTransactionId;

/* Cached copy of the pg_temp_class tuple descriptor */
static TupleDesc pg_temp_class_tupdesc = NULL;

/*
 * Pending inserts to pg_temp_class.
 *
 * Each pending insert entry may be flushed to the database at any point
 * during the transaction which added it, but the entry is kept up-to-date
 * until the end of the transaction, allowing tuples to be retrieved from the
 * hash table, even if the tuple in the database is not visible (if a
 * CommandCounterIncrement() has not happened).
 *
 * A linked list of entries is built, if they are edited in subtransactions,
 * in order to restore previous versions of the tuple on subrollback.
 */
typedef struct PendingInsert
{
	Oid			relid;			/* lookup key: OID the tuple is for */
	HeapTuple	tuple;			/* copy of tuple to be inserted */
	bool		flushed;		/* has tuple been flushed to the database? */
	bool		deleted;		/* has it been deleted? */
	SubTransactionId subid;		/* subxact that added/updated this entry */
	struct PendingInsert *prev; /* previous version, for subxact rollback */
} PendingInsert;

static HTAB *pending_inserts = NULL;
static bool have_inserts_to_flush = false;

/* Memory context for all tuples pending insert */
static MemoryContext pending_inserts_tupctx = NULL;

/*
 * Latest minimum values of relfrozenxid and relminmxid over all global
 * temporary tables, if changed in the current transaction.
 */
static TransactionId min_relfrozenxid = InvalidTransactionId;
static MultiXactId min_relminmxid = InvalidMultiXactId;
static bool min_frozenxids_updated = false;

/*
 * open_pg_temp_class
 *
 *	Open pg_temp_class and make a note of the subtranscation ID, if this is
 *	the first time opening it.
 */
static Relation
open_pg_temp_class(LOCKMODE lockmode)
{
	Relation	pg_temp_class;

	pg_temp_class = table_open(TempRelationRelationId, lockmode);
	if (!pg_temp_class_opened)
	{
		pg_temp_class_opened = true;
		pg_temp_class_subid = GetCurrentSubTransactionId();
	}
	return pg_temp_class;
}

/*
 * init_pending_inserts_hashtable
 *
 *	Initialize the pending inserts hashtable, if not already done.
 */
static void
init_pending_inserts_hashtable(void)
{
	if (pending_inserts == NULL)
	{
		HASHCTL		ctl;

		/* Create the hash table */
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(PendingInsert);

		pending_inserts = hash_create("Pending pg_temp_class inserts",
									  128, &ctl, HASH_ELEM | HASH_BLOBS);

		/* Create a separate memory context for all tuples in it */
		pending_inserts_tupctx = AllocSetContextCreate(TopMemoryContext,
													   "Pending pg_temp_class tuples",
													   ALLOCSET_DEFAULT_SIZES);
	}
}

/*
 * get_pg_temp_class_tupdesc
 *
 *	Returns the tuple descriptor for pg_temp_class.
 */
static TupleDesc
get_pg_temp_class_tupdesc(void)
{
	/* Build the tuple descriptor the first time through */
	if (pg_temp_class_tupdesc == NULL)
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		tupdesc = CreateTemplateTupleDesc(Natts_pg_temp_class);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_oid,
						   "oid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relfilenode,
						   "relfilenode", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_reltablespace,
						   "reltablespace", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relpages,
						   "relpages", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_reltuples,
						   "reltuples", FLOAT4OID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relallvisible,
						   "relallvisible", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relallfrozen,
						   "relallfrozen", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relfrozenxid,
						   "relfrozenxid", XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_class_relminmxid,
						   "relminmxid", XIDOID, -1, 0);
		TupleDescFinalize(tupdesc);

		MemoryContextSwitchTo(oldcontext);

		/* Cache it for all future use */
		pg_temp_class_tupdesc = tupdesc;
	}
	return pg_temp_class_tupdesc;
}

/*
 * heap_form_pg_temp_class_tuple
 *
 *	Create a pg_temp_class tuple for the specified relation.  All tuple data
 *	is taken from rel->rd_rel.
 */
static HeapTuple
heap_form_pg_temp_class_tuple(Relation rel)
{
	Form_pg_class form = rel->rd_rel;
	Datum		values[Natts_pg_temp_class];
	bool		nulls[Natts_pg_temp_class] = {0};

	values[Anum_pg_temp_class_oid - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
	values[Anum_pg_temp_class_relfilenode - 1] = ObjectIdGetDatum(form->relfilenode);
	values[Anum_pg_temp_class_reltablespace - 1] = ObjectIdGetDatum(form->reltablespace);
	values[Anum_pg_temp_class_relpages - 1] = Int32GetDatum(form->relpages);
	values[Anum_pg_temp_class_reltuples - 1] = Float4GetDatum(form->reltuples);
	values[Anum_pg_temp_class_relallvisible - 1] = Int32GetDatum(form->relallvisible);
	values[Anum_pg_temp_class_relallfrozen - 1] = Int32GetDatum(form->relallfrozen);
	values[Anum_pg_temp_class_relfrozenxid - 1] = TransactionIdGetDatum(form->relfrozenxid);
	values[Anum_pg_temp_class_relminmxid - 1] = MultiXactIdGetDatum(form->relminmxid);

	return heap_form_tuple(get_pg_temp_class_tupdesc(), values, nulls);
}

/*
 * prepare_pending_insert_for_edit
 *
 *	Called before making any change to a pending insert entry.  If the entry
 *	was created or last updated in a previous subtransaction, make a copy and
 *	save it, for subtransaction rollback.
 */
static void
prepare_pending_insert_for_edit(PendingInsert *entry)
{
	SubTransactionId mySubid = GetCurrentSubTransactionId();

	if (entry->subid != mySubid)
	{
		PendingInsert *save_entry;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(pending_inserts_tupctx);
		save_entry = palloc_object(PendingInsert);
		save_entry->relid = entry->relid;
		save_entry->tuple = heap_copytuple(entry->tuple);
		save_entry->flushed = entry->flushed;
		save_entry->deleted = entry->deleted;
		save_entry->subid = entry->subid;
		save_entry->prev = entry->prev;
		MemoryContextSwitchTo(oldcontext);

		entry->subid = mySubid;
		entry->prev = save_entry;
	}
}

/*
 * flush_pending_pg_temp_class_inserts
 *
 *	Flush any pending inserts to pg_temp_class.
 */
static void
flush_pending_pg_temp_class_inserts(void)
{
	Relation	pg_temp_class;
	CatalogIndexState indstate;
	HASH_SEQ_STATUS status;
	PendingInsert *entry;

	pg_temp_class = open_pg_temp_class(RowExclusiveLock);

	/*
	 * Flush all pending inserts, not already flushed or deleted.
	 */
	indstate = CatalogOpenIndexes(pg_temp_class);
	hash_seq_init(&status, pending_inserts);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (!entry->flushed && !entry->deleted)
		{
			CatalogTupleInsertWithInfo(pg_temp_class, entry->tuple, indstate);

			/* Update the entry, saving a copy for rollback, if necessary */
			prepare_pending_insert_for_edit(entry);
			entry->flushed = true;
		}
	}
	CatalogCloseIndexes(indstate);

	table_close(pg_temp_class, RowExclusiveLock);

	have_inserts_to_flush = false;
}

/*
 * discard_pending_pg_temp_class_inserts
 *
 *	Discard any pending inserts to pg_temp_class.
 */
static void
discard_pending_pg_temp_class_inserts(void)
{
	/* Just blow away the hash table and tuple memory context */
	hash_destroy(pending_inserts);
	MemoryContextDelete(pending_inserts_tupctx);

	pending_inserts = NULL;
	pending_inserts_tupctx = NULL;
	have_inserts_to_flush = false;
}

/*
 * GetPgTempClassTuple
 *
 *	Get the pg_temp_class tuple for a global temporary relation.
 *
 *	Returns NULL if the tuple could not be found.  Otherwise, the tuple
 *	returned should be freed with heap_freetuple().
 */
HeapTuple
GetPgTempClassTuple(Oid relid)
{
	PendingInsert *entry = NULL;

	/* If there is a pending insert for this relation, return that */
	if (pending_inserts != NULL)
	{
		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
			return entry->deleted ? NULL : heap_copytuple(entry->tuple);
	}

	/* If we haven't opened pg_temp_class yet, it must be empty */
	if (!pg_temp_class_opened)
		return NULL;

	/* Otherwise, look for the tuple in the database */
	return SearchSysCacheCopy1(TEMPRELOID, ObjectIdGetDatum(relid));
}

/*
 * InsertPgTempClassTuple
 *
 *	Insert a new pg_temp_class tuple for a global temporary relation.
 *
 *	This is called when a global temporary relation is created or accessed for
 *	the first time in a session.  All tuple data is taken from rel->rd_rel.
 *
 *	Note: The new tuple is not written to the database unless and until
 *	CommandCounterIncrement() is called for a non-read-only command, or the
 *	(sub)transaction is committed.  However, the new tuple *is* visible to all
 *	the functions defined here.
 */
void
InsertPgTempClassTuple(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	PendingInsert *entry;
	bool		found;
	MemoryContext oldcontext;

	/*
	 * Add a new tuple for the relation to the pending inserts hash table,
	 * taking care to allocate the tuple in the long-term memory context for
	 * pending insert tuples.
	 */
	init_pending_inserts_hashtable();

	entry = hash_search(pending_inserts, &relid, HASH_ENTER, &found);
	if (found)
		/* Should never try to re-insert the same relid */
		elog(ERROR, "pg_temp_class tuple for relation %u already exists", relid);

	oldcontext = MemoryContextSwitchTo(pending_inserts_tupctx);
	entry->tuple = heap_form_pg_temp_class_tuple(rel);
	entry->flushed = false;
	entry->deleted = false;
	entry->subid = GetCurrentSubTransactionId();
	entry->prev = NULL;
	MemoryContextSwitchTo(oldcontext);

	have_inserts_to_flush = true;
}

/*
 * UpdatePgTempClassTuple
 *
 *	Update the pg_temp_class tuple for a global temporary relation.
 */
void
UpdatePgTempClassTuple(Oid relid, HeapTuple newtuple)
{
	Relation	pg_temp_class;
	HeapTuple	oldtuple;

	/* Is there a pending insert for this relation? */
	if (pending_inserts != NULL)
	{
		PendingInsert *entry;

		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			Form_pg_temp_class old_form;
			Form_pg_temp_class new_form;

			/* Should not have been deleted */
			if (entry->deleted)
				elog(ERROR,
					 "pending insert for global temp relation %u was deleted",
					 relid);

			/* Update the entry, saving a copy for rollback, if necessary */
			prepare_pending_insert_for_edit(entry);
			old_form = (Form_pg_temp_class) GETSTRUCT(entry->tuple);
			new_form = (Form_pg_temp_class) GETSTRUCT(newtuple);
			COPY_PG_TEMP_CLASS_ATTRS(new_form, old_form);

			/*
			 * If it has not yet been flushed to the database, do so now. This
			 * is important, because the caller might be relying on a relcache
			 * invalidation being triggered.
			 */
			if (!entry->flushed)
			{
				pg_temp_class = open_pg_temp_class(RowExclusiveLock);
				CatalogTupleInsert(pg_temp_class, newtuple);
				table_close(pg_temp_class, RowExclusiveLock);
				entry->flushed = true;
				return;
			}
		}
	}

	/* Update the tuple in the database */
	pg_temp_class = open_pg_temp_class(RowExclusiveLock);

	oldtuple = SearchSysCache1(TEMPRELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp relation %u", relid);

	CatalogTupleUpdate(pg_temp_class, &oldtuple->t_self, newtuple);
	ReleaseSysCache(oldtuple);

	table_close(pg_temp_class, RowExclusiveLock);
}

/*
 * UpdatePgTempClassTupleInPlace
 *
 *	Do an in-place update of the pg_temp_class tuple for a global temporary
 *	relation.
 */
void
UpdatePgTempClassTupleInPlace(Oid relid, HeapTuple newtuple)
{
	Relation	pg_temp_class;
	ScanKeyData key[1];
	HeapTuple	oldtuple;
	void	   *inplace_state;

	/* Is there a pending insert for this relation? */
	if (pending_inserts != NULL)
	{
		PendingInsert *entry;

		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			Form_pg_temp_class old_form;
			Form_pg_temp_class new_form;

			/* Should not have been deleted */
			if (entry->deleted)
				elog(ERROR,
					 "pending insert for global temp relation %u was deleted",
					 relid);

			/* Update the entry, saving a copy for rollback, if necessary */
			prepare_pending_insert_for_edit(entry);
			old_form = (Form_pg_temp_class) GETSTRUCT(entry->tuple);
			new_form = (Form_pg_temp_class) GETSTRUCT(newtuple);
			COPY_PG_TEMP_CLASS_ATTRS(new_form, old_form);

			/*
			 * If it has not yet been flushed to the database, do so now. This
			 * is important, because the caller might be relying on a relcache
			 * invalidation being triggered.
			 */
			if (!entry->flushed)
			{
				pg_temp_class = open_pg_temp_class(RowExclusiveLock);
				CatalogTupleInsert(pg_temp_class, newtuple);
				table_close(pg_temp_class, RowExclusiveLock);
				entry->flushed = true;
				return;
			}
		}
	}

	/* Do an in-place update of the tuple in the database */
	pg_temp_class = open_pg_temp_class(RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_temp_class_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	systable_inplace_update_begin(pg_temp_class, TempClassOidIndexId, true,
								  NULL, 1, key, &oldtuple, &inplace_state);
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp relation %u", relid);

	ItemPointerCopy(&oldtuple->t_self, &newtuple->t_self);
	systable_inplace_update_finish(inplace_state, newtuple);

	heap_freetuple(oldtuple);

	table_close(pg_temp_class, RowExclusiveLock);
}

/*
 * DeletePgTempClassTuple
 *
 *	Delete the pg_temp_class tuple for a global temporary relation.
 */
void
DeletePgTempClassTuple(Oid relid)
{
	Relation	pg_temp_class;
	HeapTuple	oldtuple;

	/* Is there a pending insert for this relation? */
	if (pending_inserts != NULL)
	{
		PendingInsert *entry;

		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			/* Should not have already been deleted */
			if (entry->deleted)
				elog(ERROR,
					 "pending insert for global temp relation %u already deleted",
					 relid);

			/* Update the entry, saving a copy for rollback, if necessary */
			prepare_pending_insert_for_edit(entry);
			entry->deleted = true;

			/*
			 * If it has been flushed to the database, need to delete it there
			 * too.  Otherwise, we're done.
			 */
			if (!entry->flushed)
				return;
		}
	}

	/* Delete the tuple from the database */
	pg_temp_class = open_pg_temp_class(RowExclusiveLock);

	oldtuple = SearchSysCache1(TEMPRELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp relation %u", relid);

	CatalogTupleDelete(pg_temp_class, &oldtuple->t_self);
	ReleaseSysCache(oldtuple);

	table_close(pg_temp_class, RowExclusiveLock);
}

/*
 * GetPgClassAndPgTempClassTuples
 *
 *	Get the pg_class tuple for a relation, and if it's a global temporary
 *	relation, also get the corresponding pg_temp_class tuple.
 *
 *	If lock_tuple is true, the pg_class tuple will be locked, but not the
 *	pg_temp_class tuple.
 *
 *	If check_temp is true, an error will be raised if a global temporary
 *	relation's pg_temp_class tuple is not found.  After a global temporary
 *	relation has been opened, its pg_temp_class tuple should always exist.
 *
 *	Returns NULL if the pg_class tuple could not be found.  Otherwise, the
 *	tuple(s) returned should be freed with heap_freetuple().
 */
HeapTuple
GetPgClassAndPgTempClassTuples(Oid relid, bool lock_tuple,
							   HeapTuple *temp_tuple, bool check_temp)
{
	HeapTuple	tuple;

	/* Get a copy of the pg_class tuple */
	if (lock_tuple)
		tuple = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(relid));
	else
		tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));

	if (HeapTupleIsValid(tuple) &&
		((Form_pg_class) GETSTRUCT(tuple))->relpersistence == RELPERSISTENCE_GLOBAL_TEMP)
	{
		/* Get the pg_temp_class tuple, and check it exists, if requested */
		*temp_tuple = GetPgTempClassTuple(relid);
		if (check_temp && !HeapTupleIsValid(*temp_tuple))
			elog(ERROR, "cache lookup failed for global temp relation %u", relid);
	}
	else
		*temp_tuple = NULL;

	return tuple;
}

/*
 * GetEffectivePgClassTuple
 *
 *	Get the effective pg_class tuple for a relation.
 *
 *	This will fetch the pg_class tuple for the relation and then, if it's a
 *	global temporary relation, fetch the corresponding pg_temp_class tuple and
 *	use the values in it to override the corresponding values in the pg_class
 *	tuple.  Thus, the result represents the effective state of the relation in
 *	this session.
 *
 *	For a global temporary relation that has not yet been opened in this
 *	session, there will be no pg_temp_class tuple, and the pg_class tuple will
 *	be returned unchanged.
 *
 *	Returns NULL if the pg_class tuple could not be found.  Otherwise, the
 *	tuple returned should be freed with heap_freetuple().
 */
HeapTuple
GetEffectivePgClassTuple(Oid relid)
{
	HeapTuple	tuple;
	HeapTuple	temp_tuple;
	Form_pg_class classform;
	Form_pg_temp_class temp_classform;

	/*
	 * Get the pg_class and pg_temp_class tuples.  If we have the latter, use
	 * it to update the former.
	 */
	tuple = GetPgClassAndPgTempClassTuples(relid, false, &temp_tuple, false);

	if (HeapTupleIsValid(tuple) && HeapTupleIsValid(temp_tuple))
	{
		classform = (Form_pg_class) GETSTRUCT(tuple);
		temp_classform = (Form_pg_temp_class) GETSTRUCT(temp_tuple);
		COPY_PG_TEMP_CLASS_ATTRS(temp_classform, classform);
	}
	return tuple;
}

/*
 * UpdateTempFrozenXids
 *
 *	Update the tempfrozenxid and tempminmxid values for this backend by
 *	finding the minimum relfrozenxid and relminmxid values in pg_temp_class --
 *	i.e., the minimum frozen XIDs over all global temporary relations in use
 *	in this backend.
 *
 *	The new values are set in this process's PGPROC struct when the current
 *	transaction is committed, or discarded if the transaction is rolled back.
 *
 *	If no global temporary relations are in use, Invalid*Ids will be set.
 */
void
UpdateTempFrozenXids(void)
{
	HASH_SEQ_STATUS status;
	PendingInsert *entry;
	Form_pg_temp_class temp_form;
	TransactionId relfrozenxid;
	MultiXactId relminmxid;
	Relation	pg_temp_class;
	SysScanDesc scan;
	HeapTuple	tuple;

	/* Defaults, if no global temporary relations are being used */
	min_relfrozenxid = InvalidTransactionId;
	min_relminmxid = InvalidMultiXactId;

	/* Processing any pending inserts */
	if (pending_inserts != NULL)
	{
		hash_seq_init(&status, pending_inserts);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			temp_form = (Form_pg_temp_class) GETSTRUCT(entry->tuple);
			relfrozenxid = temp_form->relfrozenxid;
			relminmxid = (MultiXactId) temp_form->relminmxid;

			/* Skip entries that have been deleted */
			if (entry->deleted)
				continue;

			/* Ignore relations that don't hold unfrozen XIDs */
			if (!TransactionIdIsValid(relfrozenxid) ||
				!MultiXactIdIsValid(relminmxid))
				continue;

			/* Update the minimum values */
			Assert(TransactionIdIsNormal(relfrozenxid));

			if (!TransactionIdIsValid(min_relfrozenxid) ||
				TransactionIdPrecedes(relfrozenxid, min_relfrozenxid))
				min_relfrozenxid = relfrozenxid;

			if (!MultiXactIdIsValid(min_relminmxid) ||
				MultiXactIdPrecedes(relminmxid, min_relminmxid))
				min_relminmxid = relminmxid;
		}
	}

	/*
	 * Process all pg_temp_class entries.  If we haven't opened pg_temp_class
	 * in this session yet, it must be empty, and we can skip it.
	 */
	if (pg_temp_class_opened)
	{
		pg_temp_class = table_open(TempRelationRelationId, AccessShareLock);

		scan = systable_beginscan(pg_temp_class, InvalidOid, false,
								  NULL, 0, NULL);

		while ((tuple = systable_getnext(scan)) != NULL)
		{
			temp_form = (Form_pg_temp_class) GETSTRUCT(tuple);
			relfrozenxid = temp_form->relfrozenxid;
			relminmxid = (MultiXactId) temp_form->relminmxid;

			/* Ignore relations that don't hold unfrozen XIDs */
			if (!TransactionIdIsValid(relfrozenxid) ||
				!MultiXactIdIsValid(relminmxid))
				continue;

			/* Update the minimum values */
			Assert(TransactionIdIsNormal(relfrozenxid));

			if (!TransactionIdIsValid(min_relfrozenxid) ||
				TransactionIdPrecedes(relfrozenxid, min_relfrozenxid))
				min_relfrozenxid = relfrozenxid;

			if (!MultiXactIdIsValid(min_relminmxid) ||
				MultiXactIdPrecedes(relminmxid, min_relminmxid))
				min_relminmxid = relminmxid;
		}

		/* Tidy up */
		systable_endscan(scan);
		table_close(pg_temp_class, AccessShareLock);
	}

	/* Flag the new values as to be applied on commit */
	min_frozenxids_updated = true;
}

/*
 * UpdateTempFrozenXidsForRel
 *
 *	Update the tempfrozenxid and tempminmxid values for this backend to take
 *	into acount the relfrozenxid and relminmxid values from a new relation.
 *
 *	The new values are set in this process's PGPROC struct when the current
 *	transaction is committed, or discarded if the transaction is rolled back.
 */
void
UpdateTempFrozenXidsForRel(TransactionId relfrozenxid,
						   MultiXactId relminmxid)
{
	/* Use current mimimum values the first time in this transaction */
	if (!TransactionIdIsValid(min_relfrozenxid))
		min_relfrozenxid = MyProc->tempfrozenxid;
	if (!MultiXactIdIsValid(min_relminmxid))
		min_relminmxid = MyProc->tempminmxid;

	/* Update the minimum values */
	if (!TransactionIdIsValid(min_relfrozenxid) ||
		TransactionIdPrecedes(relfrozenxid, min_relfrozenxid))
		min_relfrozenxid = relfrozenxid;

	if (!MultiXactIdIsValid(min_relminmxid) ||
		MultiXactIdPrecedes(relminmxid, min_relminmxid))
		min_relminmxid = relminmxid;

	/* Flag the new values as to be applied on commit */
	min_frozenxids_updated = true;
}

/*
 * PreCCI_PgTempClass
 *
 *	Pre-end-of-command processing; flush out any pending inserts.
 */
void
PreCCI_PgTempClass(void)
{
	if (have_inserts_to_flush)
		flush_pending_pg_temp_class_inserts();
}

/*
 * PreCommit_PgTempClass
 *
 *	Pre-commit processing; flush out any pending inserts.
 */
void
PreCommit_PgTempClass(void)
{
	if (have_inserts_to_flush)
		flush_pending_pg_temp_class_inserts();
}

/*
 * PreSubCommit_PgTempClass
 *
 *	Pre-subcommit processing; flush out any pending inserts.
 */
void
PreSubCommit_PgTempClass(void)
{
	if (have_inserts_to_flush)
		flush_pending_pg_temp_class_inserts();
}

/*
 * AtEOXact_PgTempClass
 *
 *	Main-transaction commit or abort processing.
 */
void
AtEOXact_PgTempClass(bool isCommit)
{
	/*
	 * If pg_temp_class was first opened and initialized in this transaction,
	 * rollback undoes that, and it is no longer initialized.
	 */
	if (!isCommit && pg_temp_class_subid != InvalidSubTransactionId)
		pg_temp_class_opened = false;
	pg_temp_class_subid = InvalidSubTransactionId;

	/*
	 * Blow away the pending inserts hash table.  On commit, there should be
	 * no remaining inserts to flush, but on rollback, there may be.
	 */
	Assert(!(isCommit && have_inserts_to_flush));
	if (pending_inserts != NULL)
		discard_pending_pg_temp_class_inserts();

	/*
	 * On commit, save any new tempfrozenxid and tempminmxid values to our
	 * PGPROC struct.  On rollback, any new values are simply discarded.
	 */
	if (min_frozenxids_updated && isCommit)
	{
		MyProc->tempfrozenxid = min_relfrozenxid;
		MyProc->tempminmxid = min_relminmxid;
	}
	min_relfrozenxid = InvalidTransactionId;
	min_relminmxid = InvalidMultiXactId;
	min_frozenxids_updated = false;
}

/*
 * AtEOSubXact_PendingInsert
 *
 *	Sub-transaction commit or abort processing for a single pending insert.
 */
static void
AtEOSubXact_PendingInsert(PendingInsert *entry, bool isCommit,
						  SubTransactionId mySubid,
						  SubTransactionId parentSubid)
{
	/*
	 * Was the pending insert entry created or last updated in the current
	 * subtransaction?
	 *
	 * During subcommit, mark it as created/last updated in the parent,
	 * instead, and discard any saved copy from the parent's level.
	 *
	 * During subrollback, restore the most recent saved copy (from the
	 * parent's level or lower), or remove the entry, if there is no previous
	 * saved copy.
	 */
	if (entry->subid == mySubid)
	{
		if (isCommit)
		{
			entry->subid = parentSubid;
			if (entry->prev != NULL && entry->prev->subid == parentSubid)
				entry->prev = entry->prev->prev;
		}
		else if (entry->prev != NULL)
		{
			Assert(entry->relid == entry->prev->relid);
			entry->tuple = entry->prev->tuple;
			entry->flushed = entry->prev->flushed;
			entry->deleted = entry->prev->deleted;
			entry->subid = entry->prev->subid;
			entry->prev = entry->prev->prev;
		}
		else
		{
			hash_search(pending_inserts, &entry->relid, HASH_REMOVE, NULL);
		}
	}
}

/*
 * AtEOSubXact_PgTempClass
 *
 *	Sub-transaction commit or abort processing.
 */
void
AtEOSubXact_PgTempClass(bool isCommit, SubTransactionId mySubid,
						SubTransactionId parentSubid)
{
	/*
	 * Was pg_temp_class first opened and initialized in the current
	 * subtransaction?
	 *
	 * During subcommit, mark it as belonging to the parent.  Otherwise, it is
	 * no longer initialized.
	 */
	if (pg_temp_class_subid == mySubid)
	{
		if (isCommit)
			pg_temp_class_subid = parentSubid;
		else
		{
			pg_temp_class_opened = false;
			pg_temp_class_subid = InvalidSubTransactionId;
		}
	}

	/*
	 * Tidy up any pending inserts.
	 */
	if (pending_inserts != NULL)
	{
		HASH_SEQ_STATUS status;
		PendingInsert *entry;

		hash_seq_init(&status, pending_inserts);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			AtEOSubXact_PendingInsert(entry, isCommit, mySubid, parentSubid);
		}
	}
}
