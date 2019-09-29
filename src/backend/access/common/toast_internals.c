/*-------------------------------------------------------------------------
 *
 * toast_internals.c
 *	  Functions for internal use by the TOAST system.
 *
 * Copyright (c) 2000-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_internals.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/toast_internals.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static bool toastrel_valueid_exists(Relation toastrel, Oid valueid);
static bool toastid_valueid_exists(Oid toastrelid, Oid valueid);

/* ----------
 * toast_compress_datum -
 *
 *	Create a compressed version of a varlena datum
 *
 *	If we fail (ie, compressed result is actually bigger than original)
 *	then return NULL.  We must not use compressed data if it'd expand
 *	the tuple!
 *
 *	We use VAR{SIZE,DATA}_ANY so we can handle short varlenas here without
 *	copying them.  But we can't handle external or compressed datums.
 * ----------
 */
Datum
toast_compress_datum(Datum value)
{
	struct varlena *tmp;
	int32		valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	int32		len;

	Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(value)));
	Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));

	/*
	 * No point in wasting a palloc cycle if value size is out of the allowed
	 * range for compression
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return PointerGetDatum(NULL);

	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									TOAST_COMPRESS_HDRSZ);

	/*
	 * We recheck the actual size even if pglz_compress() reports success,
	 * because it might be satisfied with having saved as little as one byte
	 * in the compressed data --- which could turn into a net loss once you
	 * consider header and alignment padding.  Worst case, the compressed
	 * format might require three padding bytes (plus header, which is
	 * included in VARSIZE(tmp)), whereas the uncompressed format would take
	 * only one header byte and no padding if the value is short enough.  So
	 * we insist on a savings of more than 2 bytes to ensure we have a gain.
	 */
	len = pglz_compress(VARDATA_ANY(DatumGetPointer(value)),
						valsize,
						TOAST_COMPRESS_RAWDATA(tmp),
						PGLZ_strategy_default);
	if (len >= 0 &&
		len + TOAST_COMPRESS_HDRSZ < valsize - 2)
	{
		TOAST_COMPRESS_SET_RAWSIZE(tmp, valsize);
		SET_VARSIZE_COMPRESSED(tmp, len + TOAST_COMPRESS_HDRSZ);
		/* successful compression */
		return PointerGetDatum(tmp);
	}
	else
	{
		/* incompressible data */
		pfree(tmp);
		return PointerGetDatum(NULL);
	}
}

/* ----------
 * toast_save_datum -
 *
 *	Save one single datum into the secondary relation and return
 *	a Datum reference for it.
 *
 * toastrel: the TOAST relation we're working with (not the main rel!)
 * toastslot: a slot corresponding to 'toastrel'
 * num_indexes, toastidxs, validIndex: as returned by toast_open_indexes
 * toastoid: the toast OID that should be inserted into the new TOAST pointer
 * value: datum to be pushed to toast storage
 * oldexternal: if not NULL, toast pointer previously representing the datum
 * options: options to be passed to table_tuple_insert() for toast rows
 * ----------
 */
Datum
toast_save_datum(Relation toastrel, TupleTableSlot *toastslot,
				 int num_indexes, Relation *toastidxs, int validIndex,
				 Oid toastoid, Datum value, struct varlena *oldexternal,
				 int options, int max_chunk_size)
{
	CommandId	mycid = GetCurrentCommandId(true);
	struct varlena *result;
	struct varatt_external toast_pointer;
	union
	{
		struct varlena hdr;
		/* this is to make the union big enough for a chunk: */
		char		data[BLCKSZ + VARHDRSZ];
		/* ensure union is aligned well enough: */
		int32		align_it;
	}			chunk_data;
	int32		chunk_size;
	int32		chunk_seq = 0;
	char	   *data_p;
	int32		data_todo;
	Pointer		dval = DatumGetPointer(value);

	Assert(!VARATT_IS_EXTERNAL(value));
	Assert(max_chunk_size <= BLCKSZ);

	/*
	 * Get the data pointer and length, and compute va_rawsize and va_extsize.
	 *
	 * va_rawsize is the size of the equivalent fully uncompressed datum, so
	 * we have to adjust for short headers.
	 *
	 * va_extsize is the actual size of the data payload in the toast records.
	 */
	if (VARATT_IS_SHORT(dval))
	{
		data_p = VARDATA_SHORT(dval);
		data_todo = VARSIZE_SHORT(dval) - VARHDRSZ_SHORT;
		toast_pointer.va_rawsize = data_todo + VARHDRSZ;	/* as if not short */
		toast_pointer.va_extsize = data_todo;
	}
	else if (VARATT_IS_COMPRESSED(dval))
	{
		data_p = VARDATA(dval);
		data_todo = VARSIZE(dval) - VARHDRSZ;
		/* rawsize in a compressed datum is just the size of the payload */
		toast_pointer.va_rawsize = VARRAWSIZE_4B_C(dval) + VARHDRSZ;
		toast_pointer.va_extsize = data_todo;
		/* Assert that the numbers look like it's compressed */
		Assert(VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer));
	}
	else
	{
		data_p = VARDATA(dval);
		data_todo = VARSIZE(dval) - VARHDRSZ;
		toast_pointer.va_rawsize = VARSIZE(dval);
		toast_pointer.va_extsize = data_todo;
	}

	/*
	 * Insert the correct table OID into the result TOAST pointer.
	 *
	 * Normally this is the actual OID of the target toast table, but during
	 * table-rewriting operations such as CLUSTER, we have to insert the OID
	 * of the table's real permanent toast table instead.  toastoid is set
	 * if we have to substitute such an OID.
	 */
	if (OidIsValid(toastoid))
		toast_pointer.va_toastrelid = toastoid;
	else
		toast_pointer.va_toastrelid = RelationGetRelid(toastrel);

	/*
	 * Choose an OID to use as the value ID for this toast value.
	 *
	 * Normally we just choose an unused OID within the toast table.  But
	 * during table-rewriting operations where we are preserving an existing
	 * toast table OID, we want to preserve toast value OIDs too.  So, if
	 * rd_toastoid is set and we had a prior external value from that same
	 * toast table, re-use its value ID.  If we didn't have a prior external
	 * value (which is a corner case, but possible if the table's attstorage
	 * options have been changed), we have to pick a value ID that doesn't
	 * conflict with either new or existing toast value OIDs.
	 */
	if (!OidIsValid(toastoid))
	{
		/* normal case: just choose an unused OID */
		toast_pointer.va_valueid =
			GetNewOidWithIndex(toastrel,
							   RelationGetRelid(toastidxs[validIndex]),
							   (AttrNumber) 1);
	}
	else
	{
		/* rewrite case: check to see if value was in old toast table */
		toast_pointer.va_valueid = InvalidOid;
		if (oldexternal != NULL)
		{
			struct varatt_external old_toast_pointer;

			Assert(VARATT_IS_EXTERNAL_ONDISK(oldexternal));
			/* Must copy to access aligned fields */
			VARATT_EXTERNAL_GET_POINTER(old_toast_pointer, oldexternal);
			if (old_toast_pointer.va_toastrelid == toastoid)
			{
				/* This value came from the old toast table; reuse its OID */
				toast_pointer.va_valueid = old_toast_pointer.va_valueid;

				/*
				 * There is a corner case here: the table rewrite might have
				 * to copy both live and recently-dead versions of a row, and
				 * those versions could easily reference the same toast value.
				 * When we copy the second or later version of such a row,
				 * reusing the OID will mean we select an OID that's already
				 * in the new toast table.  Check for that, and if so, just
				 * fall through without writing the data again.
				 *
				 * While annoying and ugly-looking, this is a good thing
				 * because it ensures that we wind up with only one copy of
				 * the toast value when there is only one copy in the old
				 * toast table.  Before we detected this case, we'd have made
				 * multiple copies, wasting space; and what's worse, the
				 * copies belonging to already-deleted heap tuples would not
				 * be reclaimed by VACUUM.
				 */
				if (toastrel_valueid_exists(toastrel,
											toast_pointer.va_valueid))
				{
					/* Match, so short-circuit the data storage loop below */
					data_todo = 0;
				}
			}
		}
		if (toast_pointer.va_valueid == InvalidOid)
		{
			/*
			 * new value; must choose an OID that doesn't conflict in either
			 * old or new toast table
			 */
			do
			{
				toast_pointer.va_valueid =
					GetNewOidWithIndex(toastrel,
									   RelationGetRelid(toastidxs[validIndex]),
									   (AttrNumber) 1);
			} while (toastid_valueid_exists(toastoid,
											toast_pointer.va_valueid));
		}
	}

	/*
	 * Split up the item into chunks
	 */
	while (data_todo > 0)
	{
		int			i;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Calculate the size of this chunk
		 */
		chunk_size = Min(max_chunk_size, data_todo);

		/*
		 * Build a tuple and store it
		 */
		toastslot->tts_values[0] = ObjectIdGetDatum(toast_pointer.va_valueid);
		toastslot->tts_values[1] = Int32GetDatum(chunk_seq++);
		SET_VARSIZE(&chunk_data, chunk_size + VARHDRSZ);
		memcpy(VARDATA(&chunk_data), data_p, chunk_size);
		toastslot->tts_values[2] = PointerGetDatum(&chunk_data);
		toastslot->tts_isnull[0] = false;
		toastslot->tts_isnull[1] = false;
		toastslot->tts_isnull[2] = false;
		ExecStoreVirtualTuple(toastslot);

		table_tuple_insert(toastrel, toastslot, mycid, options, NULL);

		/*
		 * Create the index entry.  We cheat a little here by not using
		 * FormIndexDatum: this relies on the knowledge that the index columns
		 * are the same as the initial columns of the table for all the
		 * indexes.  We also cheat by not providing an IndexInfo: this is okay
		 * for now because btree doesn't need one, but we might have to be
		 * more honest someday.
		 *
		 * Note also that there had better not be any user-created index on
		 * the TOAST table, since we don't bother to update anything else.
		 */
		for (i = 0; i < num_indexes; i++)
		{
			/* Only index relations marked as ready can be updated */
			if (toastidxs[i]->rd_index->indisready)
				index_insert(toastidxs[i], toastslot->tts_values,
							 toastslot->tts_isnull,
							 &(toastslot->tts_tid),
							 toastrel,
							 toastidxs[i]->rd_index->indisunique ?
							 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
							 NULL);
		}

		/*
		 * Clear slot
		 */
		ExecClearTuple(toastslot);

		/*
		 * Move on to next chunk
		 */
		data_todo -= chunk_size;
		data_p += chunk_size;
	}

	/*
	 * Create the TOAST pointer value that we'll return
	 */
	result = (struct varlena *) palloc(TOAST_POINTER_SIZE);
	SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK);
	memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer));

	return PointerGetDatum(result);
}

/* ----------
 * toast_delete_datum -
 *
 *	Delete a single external stored value.
 * ----------
 */
void
toast_delete_datum(Relation toastrel, int num_indexes, Relation *toastidxs,
				   int validIndex, Datum value, bool is_speculative,
				   uint32 specToken)
{
	struct varlena *attr = (struct varlena *) DatumGetPointer(value);
	struct varatt_external toast_pointer;
	ScanKeyData toastkey;
	SysScanDesc toastscan;
	TupleTableSlot *slot;
	SnapshotData SnapshotToast;

	Assert(VARATT_IS_EXTERNAL_ONDISK(attr));

	/* Must copy to access aligned fields */
	VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

	/* Check that caller gave us the correct TOAST relation. */
	Assert(toast_pointer.va_toastrelid == RelationGetRelid(toastrel));

	/*
	 * Setup a scan key to find chunks with matching va_valueid
	 */
	ScanKeyInit(&toastkey,
				(AttrNumber) 1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(toast_pointer.va_valueid));

	/*
	 * Find all the chunks.  (We don't actually care whether we see them in
	 * sequence or not, but since we've already locked the index we might as
	 * well use systable_beginscan_ordered.)
	 */
	init_toast_snapshot(&SnapshotToast);
	toastscan = systable_beginscan_ordered(toastrel, toastidxs[validIndex],
										   &SnapshotToast, 1, &toastkey);
	while ((slot = systable_getnextslot_ordered(toastscan, ForwardScanDirection)) != NULL)
	{
		/*
		 * Have a chunk, delete it
		 */
		if (is_speculative)
			table_tuple_complete_speculative(toastrel, slot, specToken, false);
		else
			simple_table_tuple_delete(toastrel, &slot->tts_tid, &SnapshotToast);
	}

	/* End scan */
	systable_endscan_ordered(toastscan);
}

/* ----------
 * toastrel_valueid_exists -
 *
 *	Test whether a toast value with the given ID exists in the toast relation.
 *	For safety, we consider a value to exist if there are either live or dead
 *	toast rows with that ID; see notes for GetNewOidWithIndex().
 * ----------
 */
static bool
toastrel_valueid_exists(Relation toastrel, Oid valueid)
{
	bool		result = false;
	ScanKeyData toastkey;
	SysScanDesc toastscan;
	int			num_indexes;
	int			validIndex;
	Relation   *toastidxs;

	/* Fetch a valid index relation */
	validIndex = toast_open_indexes(toastrel,
									RowExclusiveLock,
									&toastidxs,
									&num_indexes);

	/*
	 * Setup a scan key to find chunks with matching va_valueid
	 */
	ScanKeyInit(&toastkey,
				(AttrNumber) 1,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(valueid));

	/*
	 * Is there any such chunk?
	 */
	toastscan = systable_beginscan(toastrel,
								   RelationGetRelid(toastidxs[validIndex]),
								   true, SnapshotAny, 1, &toastkey);

	if (systable_getnext(toastscan) != NULL)
		result = true;

	systable_endscan(toastscan);

	/* Clean up */
	toast_close_indexes(toastidxs, num_indexes, RowExclusiveLock);

	return result;
}

/* ----------
 * toastid_valueid_exists -
 *
 *	As above, but work from toast rel's OID not an open relation
 * ----------
 */
static bool
toastid_valueid_exists(Oid toastrelid, Oid valueid)
{
	bool		result;
	Relation	toastrel;

	toastrel = table_open(toastrelid, AccessShareLock);

	result = toastrel_valueid_exists(toastrel, valueid);

	table_close(toastrel, AccessShareLock);

	return result;
}

/* ----------
 * toast_get_valid_index
 *
 *	Get OID of valid index associated to given toast relation. A toast
 *	relation can have only one valid index at the same time.
 */
Oid
toast_get_valid_index(Oid toastoid, LOCKMODE lock)
{
	int			num_indexes;
	int			validIndex;
	Oid			validIndexOid;
	Relation   *toastidxs;
	Relation	toastrel;

	/* Open the toast relation */
	toastrel = table_open(toastoid, lock);

	/* Look for the valid index of the toast relation */
	validIndex = toast_open_indexes(toastrel,
									lock,
									&toastidxs,
									&num_indexes);
	validIndexOid = RelationGetRelid(toastidxs[validIndex]);

	/* Close the toast relation and all its indexes */
	toast_close_indexes(toastidxs, num_indexes, lock);
	table_close(toastrel, lock);

	return validIndexOid;
}

/* ----------
 * toast_open_indexes
 *
 *	Get an array of the indexes associated to the given toast relation
 *	and return as well the position of the valid index used by the toast
 *	relation in this array. It is the responsibility of the caller of this
 *	function to close the indexes as well as free them.
 */
int
toast_open_indexes(Relation toastrel,
				   LOCKMODE lock,
				   Relation **toastidxs,
				   int *num_indexes)
{
	int			i = 0;
	int			res = 0;
	bool		found = false;
	List	   *indexlist;
	ListCell   *lc;

	/* Get index list of the toast relation */
	indexlist = RelationGetIndexList(toastrel);
	Assert(indexlist != NIL);

	*num_indexes = list_length(indexlist);

	/* Open all the index relations */
	*toastidxs = (Relation *) palloc(*num_indexes * sizeof(Relation));
	foreach(lc, indexlist)
		(*toastidxs)[i++] = index_open(lfirst_oid(lc), lock);

	/* Fetch the first valid index in list */
	for (i = 0; i < *num_indexes; i++)
	{
		Relation	toastidx = (*toastidxs)[i];

		if (toastidx->rd_index->indisvalid)
		{
			res = i;
			found = true;
			break;
		}
	}

	/*
	 * Free index list, not necessary anymore as relations are opened and a
	 * valid index has been found.
	 */
	list_free(indexlist);

	/*
	 * The toast relation should have one valid index, so something is going
	 * wrong if there is nothing.
	 */
	if (!found)
		elog(ERROR, "no valid index found for toast relation with Oid %u",
			 RelationGetRelid(toastrel));

	return res;
}

/* ----------
 * toast_close_indexes
 *
 *	Close an array of indexes for a toast relation and free it. This should
 *	be called for a set of indexes opened previously with toast_open_indexes.
 */
void
toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
{
	int			i;

	/* Close relations and clean up things */
	for (i = 0; i < num_indexes; i++)
		index_close(toastidxs[i], lock);
	pfree(toastidxs);
}

/* ----------
 * init_toast_snapshot
 *
 *	Initialize an appropriate TOAST snapshot.  We must use an MVCC snapshot
 *	to initialize the TOAST snapshot; since we don't know which one to use,
 *	just use the oldest one.  This is safe: at worst, we will get a "snapshot
 *	too old" error that might have been avoided otherwise.
 */
void
init_toast_snapshot(Snapshot toast_snapshot)
{
	Snapshot	snapshot = GetOldestSnapshot();

	if (snapshot == NULL)
		elog(ERROR, "no known snapshots");

	InitToastSnapshot(*toast_snapshot, snapshot->lsn, snapshot->whenTaken);
}
