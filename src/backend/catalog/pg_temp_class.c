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
 * created.  Thus, for example, a parallel worker can open global temporary
 * relations without reading from them or pg_temp_class.
 *
 * Pending inserts to pg_temp_class are flushed to the database at the end
 * of any non-read-only command and when committing a transaction or
 * subtransaction.  They are also flushed out when we notice that the
 * transaction nesting level has increased, so that the all pending inserts
 * are for the current subtransaction, and can be safely discarded on
 * subrollback.
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
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_temp_class.h"
#include "miscadmin.h"
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

/* Pending inserts to pg_temp_class */
typedef struct PendingInsert
{
	Oid			relid;			/* lookup key: OID the tuple is for */
	HeapTuple	tuple;			/* copy of tuple to be inserted */
} PendingInsert;

static HTAB *pending_inserts = NULL;
static bool have_pending_inserts = false;

/* Memory context for all tuples pending insert */
static MemoryContext pending_inserts_tupctx = NULL;

/* Transaction nesting level the pending inserts are for */
static int	pending_inserts_nest_level = 1;

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

	return heap_form_tuple(get_pg_temp_class_tupdesc(), values, nulls);
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

	/*
	 * Open pg_temp_class, and make note of the subtransaction ID, if this is
	 * the first time.
	 */
	pg_temp_class = table_open(TempRelationRelationId, RowExclusiveLock);
	if (!pg_temp_class_opened)
	{
		pg_temp_class_opened = true;
		pg_temp_class_subid = GetCurrentSubTransactionId();
	}

	/* Flush all pending inserts */
	indstate = CatalogOpenIndexes(pg_temp_class);
	hash_seq_init(&status, pending_inserts);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		CatalogTupleInsertWithInfo(pg_temp_class, entry->tuple, indstate);
		hash_search(pending_inserts, &entry->relid, HASH_REMOVE, NULL);
	}
	CatalogCloseIndexes(indstate);
	table_close(pg_temp_class, RowExclusiveLock);

	/* Should be left with no pending inserts */
	Assert(hash_get_num_entries(pending_inserts) == 0);
	have_pending_inserts = false;

	/* Free all memory allocated for tuples */
	MemoryContextReset(pending_inserts_tupctx);
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
	have_pending_inserts = false;
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
	PendingInsert *entry;

	/* If there is a pending insert for this relation, just return that */
	if (have_pending_inserts)
	{
		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
			return heap_copytuple(entry->tuple);
	}

	/* If we haven't opened pg_temp_class yet, it must be empty */
	if (!pg_temp_class_opened)
		return NULL;

	/* Otherwise fetch a copy of the tuple from the system caches */
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
 *	(sub)transaction is committed, or a new subtranction is started.  However,
 *	the new tuple *is* visible to Get/Update/DeletePgTempClassTuple() and
 *	GetEffectivePgClassTuple().
 */
void
InsertPgTempClassTuple(Relation rel)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	Oid			relid = RelationGetRelid(rel);
	PendingInsert *entry;
	bool		found;
	MemoryContext oldcontext;

	/*
	 * If the transaction nesting level has increased, flush all previous
	 * pending inserts, so that all new pending inserts are for the same
	 * subtransaction.  The nesting level should never decrease without us
	 * knowing about it via AtEOSubXact_PgTempClass().
	 */
	Assert(nest_level >= pending_inserts_nest_level);
	if (nest_level > pending_inserts_nest_level)
	{
		if (have_pending_inserts)
			flush_pending_pg_temp_class_inserts();
		pending_inserts_nest_level = nest_level;
	}

	/*
	 * Add a new tuple for the relation to the pending inserts hash table,
	 * taking care to allocate the tuple in the long-term memory context for
	 * pending insert tuples.
	 */
	init_pending_inserts_hashtable();

	entry = hash_search(pending_inserts, &relid, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "pg_temp_class tuple for relation %u already exists", relid);

	oldcontext = MemoryContextSwitchTo(pending_inserts_tupctx);
	entry->tuple = heap_form_pg_temp_class_tuple(rel);
	MemoryContextSwitchTo(oldcontext);

	have_pending_inserts = true;
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

	/* If there is a pending insert for this relation, just update that */
	if (have_pending_inserts)
	{
		PendingInsert *entry;
		Form_pg_temp_class old_form;
		Form_pg_temp_class new_form;

		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			old_form = (Form_pg_temp_class) GETSTRUCT(entry->tuple);
			new_form = (Form_pg_temp_class) GETSTRUCT(newtuple);
			COPY_PG_TEMP_CLASS_ATTRS(new_form, old_form);

			return;
		}
	}

	/*
	 * Otherwise, update pg_temp_class directly, making note of the
	 * subtransaction ID, if this is the first time opening pg_temp_class.
	 */
	pg_temp_class = table_open(TempRelationRelationId, RowExclusiveLock);
	if (!pg_temp_class_opened)
	{
		pg_temp_class_opened = true;
		pg_temp_class_subid = GetCurrentSubTransactionId();
	}

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

	/* If there is a pending insert for this relation, just update that */
	if (have_pending_inserts)
	{
		PendingInsert *entry;
		Form_pg_temp_class old_form;
		Form_pg_temp_class new_form;

		entry = hash_search(pending_inserts, &relid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			old_form = (Form_pg_temp_class) GETSTRUCT(entry->tuple);
			new_form = (Form_pg_temp_class) GETSTRUCT(newtuple);
			COPY_PG_TEMP_CLASS_ATTRS(new_form, old_form);

			return;
		}
	}

	/*
	 * Otherwise, update pg_temp_class directly, making note of the
	 * subtransaction ID, if this is the first time opening pg_temp_class.
	 */
	pg_temp_class = table_open(TempRelationRelationId, RowExclusiveLock);
	if (!pg_temp_class_opened)
	{
		pg_temp_class_opened = true;
		pg_temp_class_subid = GetCurrentSubTransactionId();
	}

	ScanKeyInit(&key[0],
				Anum_pg_temp_class_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	systable_inplace_update_begin(pg_temp_class, TempClassOidIndexId, true,
								  NULL, 1, key, &oldtuple, &inplace_state);
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp relation %u", relid);

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

	/* If there is a pending insert for this relation, just delete that */
	if (have_pending_inserts)
	{
		PendingInsert *entry;

		entry = hash_search(pending_inserts, &relid, HASH_REMOVE, NULL);
		if (entry != NULL)
		{
			heap_freetuple(entry->tuple);
			return;
		}
	}

	/*
	 * Otherwise, update pg_temp_class directly, making note of the
	 * subtransaction ID, if this is the first time opening pg_temp_class.
	 */
	pg_temp_class = table_open(TempRelationRelationId, RowExclusiveLock);
	if (!pg_temp_class_opened)
	{
		pg_temp_class_opened = true;
		pg_temp_class_subid = GetCurrentSubTransactionId();
	}

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
 * PreCCI_PgTempClass
 *
 *	Pre-end-of-command processing; flush out any pending inserts.
 */
void
PreCCI_PgTempClass(void)
{
	if (have_pending_inserts)
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
	if (have_pending_inserts)
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
	if (have_pending_inserts)
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

	/* On rollback, discard any pending inserts */
	if (!isCommit && have_pending_inserts)
		discard_pending_pg_temp_class_inserts();

	/*
	 * Reset the pending inserts transaction nesting level so that inserts are
	 * flushed if a new subtransaction is started, but not a new top-level
	 * transaction.
	 */
	pending_inserts_nest_level = 1;
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
	 * During subcommit, mark it was belonging to the parent.  Otherwise, it
	 * is no longer initialized.
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
	 * If pending inserts are for this transaction nesting level, and we're
	 * rolling back, discard them.
	 */
	if (!isCommit && have_pending_inserts &&
		pending_inserts_nest_level == GetCurrentTransactionNestLevel())
	{
		discard_pending_pg_temp_class_inserts();
	}
	pending_inserts_nest_level--;
}
