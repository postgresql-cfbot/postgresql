/*-------------------------------------------------------------------------
 *
 * pg_temp_index.c
 *	  routines to support manipulation of the pg_temp_index relation
 *
 * The pg_temp_index system catalog table is a global temporary table that
 * stores local overrides to the indisvalid field from the pg_index table
 * for the duration of the current session.  Currently, this is only used
 * for global temporary relations, though in the future, it might also be
 * used for local temporary relations.
 *
 * Much of the code here mirrors similar code in pg_temp_class.c --- see
 * the comments there for more detail.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_temp_index.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_index.h"
#include "catalog/pg_temp_index.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Have we opened and initialized pg_temp_index in this session?
 */
static bool pg_temp_index_opened = false;

/*
 * Subtransaction ID in which pg_temp_index was opened, if it was opened in
 * the current transaction, else zero.
 */
static SubTransactionId pg_temp_index_subid = InvalidSubTransactionId;

/* Cached copy of the pg_temp_index tuple descriptor */
static TupleDesc pg_temp_index_tupdesc = NULL;

/* Pending inserts to pg_temp_index */
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

		pending_inserts = hash_create("Pending pg_temp_index inserts",
									  128, &ctl, HASH_ELEM | HASH_BLOBS);

		/* Create a separate memory context for all tuples in it */
		pending_inserts_tupctx = AllocSetContextCreate(TopMemoryContext,
													   "Pending pg_temp_index tuples",
													   ALLOCSET_DEFAULT_SIZES);
	}
}

/*
 * get_pg_temp_index_tupdesc
 *
 *	Returns the tuple descriptor for pg_temp_index.
 */
static TupleDesc
get_pg_temp_index_tupdesc(void)
{
	/* Build the tuple descriptor the first time through */
	if (pg_temp_index_tupdesc == NULL)
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		tupdesc = CreateTemplateTupleDesc(Natts_pg_temp_index);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_index_indexrelid,
						   "indexrelid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc,
						   (AttrNumber) Anum_pg_temp_index_indisvalid,
						   "indisvalid", BOOLOID, -1, 0);
		TupleDescFinalize(tupdesc);

		MemoryContextSwitchTo(oldcontext);

		/* Cache it for all future use */
		pg_temp_index_tupdesc = tupdesc;
	}
	return pg_temp_index_tupdesc;
}

/*
 * heap_form_pg_temp_index_tuple
 *
 *	Create a pg_temp_index tuple for the specified index relation.
 */
static HeapTuple
heap_form_pg_temp_index_tuple(Oid indexrelid, bool indisvalid)
{
	Datum		values[Natts_pg_temp_index];
	bool		nulls[Natts_pg_temp_index] = {0};

	values[Anum_pg_temp_index_indexrelid - 1] = ObjectIdGetDatum(indexrelid);
	values[Anum_pg_temp_index_indisvalid - 1] = BoolGetDatum(indisvalid);

	return heap_form_tuple(get_pg_temp_index_tupdesc(), values, nulls);
}

/*
 * flush_pending_pg_temp_index_inserts
 *
 *	Flush any pending inserts to pg_temp_index.
 */
static void
flush_pending_pg_temp_index_inserts(void)
{
	Relation	pg_temp_index;
	CatalogIndexState indstate;
	HASH_SEQ_STATUS status;
	PendingInsert *entry;

	/*
	 * Open pg_temp_index, and make note of the subtransaction ID, if this is
	 * the first time.
	 */
	pg_temp_index = table_open(TempIndexRelationId, RowExclusiveLock);
	if (!pg_temp_index_opened)
	{
		pg_temp_index_opened = true;
		pg_temp_index_subid = GetCurrentSubTransactionId();
	}

	/* Flush all pending inserts */
	indstate = CatalogOpenIndexes(pg_temp_index);
	hash_seq_init(&status, pending_inserts);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		CatalogTupleInsertWithInfo(pg_temp_index, entry->tuple, indstate);
		hash_search(pending_inserts, &entry->relid, HASH_REMOVE, NULL);
	}
	CatalogCloseIndexes(indstate);
	table_close(pg_temp_index, RowExclusiveLock);

	/* Should be left with no pending inserts */
	Assert(hash_get_num_entries(pending_inserts) == 0);
	have_pending_inserts = false;

	/* Free all memory allocated for tuples */
	MemoryContextReset(pending_inserts_tupctx);
}

/*
 * discard_pending_pg_temp_index_inserts
 *
 *	Discard any pending inserts to pg_temp_index.
 */
static void
discard_pending_pg_temp_index_inserts(void)
{
	/* Just blow away the hash table and tuple memory context */
	hash_destroy(pending_inserts);
	MemoryContextDelete(pending_inserts_tupctx);

	pending_inserts = NULL;
	pending_inserts_tupctx = NULL;
	have_pending_inserts = false;
}

/*
 * GetPgTempIndexTuple
 *
 *	Get the pg_temp_index tuple for a global temporary index relation.
 *
 *	Returns NULL if the tuple could not be found.  Otherwise, the tuple
 *	returned should be freed with heap_freetuple().
 */
HeapTuple
GetPgTempIndexTuple(Oid indexrelid)
{
	PendingInsert *entry;

	/* If there is a pending insert for this relation, just return that */
	if (have_pending_inserts)
	{
		entry = hash_search(pending_inserts, &indexrelid, HASH_FIND, NULL);
		if (entry != NULL)
			return heap_copytuple(entry->tuple);
	}

	/* If we haven't opened pg_temp_index yet, it must be empty */
	if (!pg_temp_index_opened)
		return NULL;

	/* Otherwise fetch a copy of the tuple from the system caches */
	return SearchSysCacheCopy1(TEMPINDEXRELID, ObjectIdGetDatum(indexrelid));
}

/*
 * InsertPgTempIndexTuple
 *
 *	Insert a new pg_temp_index tuple for a global temporary index relation.
 *
 *	This is called when a global temporary index relation is created or
 *	accessed for the first time in a session.
 *
 *	Note: The new tuple is not written to the database unless and until
 *	CommandCounterIncrement() is called for a non-read-only command, or the
 *	(sub)transaction is committed, or a new subtranction is started.  However,
 *	the new tuple *is* visible to the other functions defined here.
 */
void
InsertPgTempIndexTuple(Oid indexrelid, bool indisvalid)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PendingInsert *entry;
	bool		found;
	MemoryContext oldcontext;

	/*
	 * If the transaction nesting level has increased, flush all previous
	 * pending inserts, so that all new pending inserts are for the same
	 * subtransaction.  The nesting level should never decrease without us
	 * knowing about it via AtEOSubXact_PgTempIndex().
	 */
	Assert(nest_level >= pending_inserts_nest_level);
	if (nest_level > pending_inserts_nest_level)
	{
		if (have_pending_inserts)
			flush_pending_pg_temp_index_inserts();
		pending_inserts_nest_level = nest_level;
	}

	/*
	 * Add a new tuple for the relation to the pending inserts hash table,
	 * taking care to allocate the tuple in the long-term memory context for
	 * pending insert tuples.
	 */
	init_pending_inserts_hashtable();

	entry = hash_search(pending_inserts, &indexrelid, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "pg_temp_index tuple for index %u already exists", indexrelid);

	oldcontext = MemoryContextSwitchTo(pending_inserts_tupctx);
	entry->tuple = heap_form_pg_temp_index_tuple(indexrelid, indisvalid);
	MemoryContextSwitchTo(oldcontext);

	have_pending_inserts = true;
}

/*
 * UpdatePgTempIndexTuple
 *
 *	Update the pg_temp_index tuple for a global temporary index relation.
 */
void
UpdatePgTempIndexTuple(Oid indexrelid, HeapTuple newtuple)
{
	Relation	pg_temp_index;
	HeapTuple	oldtuple;

	/* If there is a pending insert for this relation, just update that */
	if (have_pending_inserts)
	{
		PendingInsert *entry;
		Form_pg_temp_index old_form;
		Form_pg_temp_index new_form;

		entry = hash_search(pending_inserts, &indexrelid, HASH_FIND, NULL);
		if (entry != NULL)
		{
			old_form = (Form_pg_temp_index) GETSTRUCT(entry->tuple);
			new_form = (Form_pg_temp_index) GETSTRUCT(newtuple);
			old_form->indisvalid = new_form->indisvalid;

			return;
		}
	}

	/*
	 * Otherwise, update pg_temp_index directly, making note of the
	 * subtransaction ID, if this is the first time opening pg_temp_index.
	 */
	pg_temp_index = table_open(TempIndexRelationId, RowExclusiveLock);
	if (!pg_temp_index_opened)
	{
		pg_temp_index_opened = true;
		pg_temp_index_subid = GetCurrentSubTransactionId();
	}

	oldtuple = SearchSysCache1(TEMPINDEXRELID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp index %u", indexrelid);

	CatalogTupleUpdate(pg_temp_index, &oldtuple->t_self, newtuple);
	ReleaseSysCache(oldtuple);

	table_close(pg_temp_index, RowExclusiveLock);
}

/*
 * DeletePgTempIndexTuple
 *
 *	Delete the pg_temp_index tuple for a global temporary index relation.
 */
void
DeletePgTempIndexTuple(Oid indexrelid)
{
	Relation	pg_temp_index;
	HeapTuple	oldtuple;

	/* If there is a pending insert for this relation, just delete that */
	if (have_pending_inserts)
	{
		PendingInsert *entry;

		entry = hash_search(pending_inserts, &indexrelid, HASH_REMOVE, NULL);
		if (entry != NULL)
		{
			heap_freetuple(entry->tuple);
			return;
		}
	}

	/*
	 * Otherwise, update pg_temp_index directly, making note of the
	 * subtransaction ID, if this is the first time opening pg_temp_index.
	 */
	pg_temp_index = table_open(TempIndexRelationId, RowExclusiveLock);
	if (!pg_temp_index_opened)
	{
		pg_temp_index_opened = true;
		pg_temp_index_subid = GetCurrentSubTransactionId();
	}

	oldtuple = SearchSysCache1(TEMPINDEXRELID, ObjectIdGetDatum(indexrelid));
	if (!HeapTupleIsValid(oldtuple))
		elog(ERROR, "cache lookup failed for global temp index %u", indexrelid);

	CatalogTupleDelete(pg_temp_index, &oldtuple->t_self);
	ReleaseSysCache(oldtuple);

	table_close(pg_temp_index, RowExclusiveLock);
}

/*
 * GetPgIndexAndPgTempIndexTuples
 *
 *	Get the pg_index tuple for an index relation, and if it's a global
 *	temporary index relation, also get the corresponding pg_temp_index tuple,
 *	if present.
 *
 *	Returns NULL if the pg_index tuple could not be found.  Otherwise, the
 *	tuple(s) returned should be freed with heap_freetuple().
 */
HeapTuple
GetPgIndexAndPgTempIndexTuples(Oid indexrelid, HeapTuple *temp_tuple,
							   bool check_temp)
{
	HeapTuple	tuple;

	/* Get a copy of the pg_index tuple */
	tuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexrelid));

	if (HeapTupleIsValid(tuple) &&
		rel_is_global_temp(((Form_pg_index) GETSTRUCT(tuple))->indexrelid))
	{
		/* Get the pg_temp_index tuple, and check it exists, if requested */
		*temp_tuple = GetPgTempIndexTuple(indexrelid);
		if (check_temp && !HeapTupleIsValid(*temp_tuple))
			elog(ERROR, "cache lookup failed for global temp index %u", indexrelid);
	}
	else
		*temp_tuple = NULL;

	return tuple;
}

/*
 * GetEffectivePgIndexTuple
 *
 *	Get the effective pg_index tuple for an index relation.
 *
 *	This will fetch the pg_index tuple for the relation and then, if it's a
 *	global temporary relation, fetch the corresponding pg_temp_index tuple and
 *	use the values in it to override the corresponding values in the pg_index
 *	tuple (currently just indisvalid).  Thus, the result represents the
 *	effective state of the index relation in this session.
 *
 *	For a global temporary index relation that has not yet been opened in this
 *	session, there will be no pg_temp_index tuple, and the pg_index tuple will
 *	be returned unchanged.
 *
 *	Returns NULL if the pg_index tuple could not be found.  Otherwise, the
 *	tuple returned should be freed with heap_freetuple().
 */
HeapTuple
GetEffectivePgIndexTuple(Oid indexrelid)
{
	HeapTuple	tuple;
	HeapTuple	temp_tuple;
	Form_pg_index indexform;
	Form_pg_temp_index temp_indexform;

	/*
	 * Get the pg_index and pg_temp_index tuples.  If we have the latter, use
	 * it to update the former.
	 */
	tuple = GetPgIndexAndPgTempIndexTuples(indexrelid, &temp_tuple, false);

	if (HeapTupleIsValid(tuple) && HeapTupleIsValid(temp_tuple))
	{
		indexform = (Form_pg_index) GETSTRUCT(tuple);
		temp_indexform = (Form_pg_temp_index) GETSTRUCT(temp_tuple);
		indexform->indisvalid = temp_indexform->indisvalid;
	}
	return tuple;
}

/*
 * PreCCI_PgTempIndex
 *
 *	Pre-end-of-command processing; flush out any pending inserts.
 */
void
PreCCI_PgTempIndex(void)
{
	if (have_pending_inserts)
		flush_pending_pg_temp_index_inserts();
}

/*
 * PreCommit_PgTempIndex
 *
 *	Pre-commit processing; flush out any pending inserts.
 */
void
PreCommit_PgTempIndex(void)
{
	if (have_pending_inserts)
		flush_pending_pg_temp_index_inserts();
}

/*
 * PreSubCommit_PgTempIndex
 *
 *	Pre-subcommit processing; flush out any pending inserts.
 */
void
PreSubCommit_PgTempIndex(void)
{
	if (have_pending_inserts)
		flush_pending_pg_temp_index_inserts();
}

/*
 * AtEOXact_PgTempIndex
 *
 *	Main-transaction commit or abort processing.
 */
void
AtEOXact_PgTempIndex(bool isCommit)
{
	/*
	 * If pg_temp_index was first opened and initialized in this transaction,
	 * rollback undoes that, and it is no longer initialized.
	 */
	if (!isCommit && pg_temp_index_subid != InvalidSubTransactionId)
		pg_temp_index_opened = false;
	pg_temp_index_subid = InvalidSubTransactionId;

	/* On rollback, discard any pending inserts */
	if (!isCommit && have_pending_inserts)
		discard_pending_pg_temp_index_inserts();

	/*
	 * Reset the pending inserts transaction nesting level so that inserts are
	 * flushed if a new subtransaction is started, but not a new top-level
	 * transaction.
	 */
	pending_inserts_nest_level = 1;
}

/*
 * AtEOSubXact_PgTempIndex
 *
 *	Sub-transaction commit or abort processing.
 */
void
AtEOSubXact_PgTempIndex(bool isCommit, SubTransactionId mySubid,
						SubTransactionId parentSubid)
{
	/*
	 * Was pg_temp_index first opened and initialized in the current
	 * subtransaction?
	 *
	 * During subcommit, mark it was belonging to the parent.  Otherwise, it
	 * is no longer initialized.
	 */
	if (pg_temp_index_subid == mySubid)
	{
		if (isCommit)
			pg_temp_index_subid = parentSubid;
		else
		{
			pg_temp_index_opened = false;
			pg_temp_index_subid = InvalidSubTransactionId;
		}
	}

	/*
	 * If pending inserts are for this transaction nesting level, and we're
	 * rolling back, discard them.
	 */
	if (!isCommit && have_pending_inserts &&
		pending_inserts_nest_level == GetCurrentTransactionNestLevel())
	{
		discard_pending_pg_temp_index_inserts();
	}
	pending_inserts_nest_level--;
}
