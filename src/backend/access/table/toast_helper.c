/*-------------------------------------------------------------------------
 *
 * toast_helper.c
 *	  Helper functions for table AMs implementing compressed or
 *    out-of-line storage of varlena attributes.
 *
 * Copyright (c) 2000-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_helper.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/detoast.h"
#include "access/table.h"
#include "access/toast_helper.h"
#include "access/tableam.h"
#include "access/toast_internals.h"

/*
 * Prepare to TOAST a tuple.
 *
 * tupleDesc, toast_values, and toast_isnull are required parameters; they
 * provide the necessary details about the tuple to be toasted.
 *
 * toast_oldvalues and toast_oldisnull should be NULL for a newly-inserted
 * tuple; for an update, they should describe the existing tuple.
 *
 * All of these arrays should have a length equal to tupleDesc->natts.
 *
 * On return, toast_flags and toast_attr will have been initialized.
 * toast_flags is just a single uint8, but toast_attr is an caller-provided
 * array with a length equal to tupleDesc->natts.  The caller need not
 * perform any initialization of the array before calling this function.
 */
void
toast_tuple_init(ToastTupleContext *ttc)
{
	TupleDesc	tupleDesc = ttc->ttc_rel->rd_att;
	int			numAttrs = tupleDesc->natts;
	int			i;

	ttc->ttc_flags = 0;

	for (i = 0; i < numAttrs; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		struct varlena *old_value;
		struct varlena *new_value;

		ttc->ttc_attr[i].tai_colflags = 0;
		ttc->ttc_attr[i].tai_oldexternal = NULL;

		if (ttc->ttc_oldvalues != NULL)
		{
			/*
			 * For UPDATE get the old and new values of this attribute
			 */
			old_value =
				(struct varlena *) DatumGetPointer(ttc->ttc_oldvalues[i]);
			new_value =
				(struct varlena *) DatumGetPointer(ttc->ttc_values[i]);

			/*
			 * If the old value is stored on disk, check if it has changed so
			 * we have to delete it later.
			 */
			if (att->attlen == -1 && !ttc->ttc_oldisnull[i] &&
				VARATT_IS_EXTERNAL_ONDISK(old_value))
			{
				if (ttc->ttc_isnull[i] ||
					!VARATT_IS_EXTERNAL_ONDISK(new_value) ||
					memcmp((char *) old_value, (char *) new_value,
						   VARSIZE_EXTERNAL(old_value)) != 0)
				{
					/*
					 * The old external stored value isn't needed any more
					 * after the update
					 */
					ttc->ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_DELETE_OLD;
					ttc->ttc_flags |= TOAST_NEEDS_DELETE_OLD;
				}
				else
				{
					/*
					 * This attribute isn't changed by this update so we reuse
					 * the original reference to the old value in the new
					 * tuple.
					 */
					ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
					continue;
				}
			}
		}
		else
		{
			/*
			 * For INSERT simply get the new value
			 */
			new_value = (struct varlena *) DatumGetPointer(ttc->ttc_values[i]);
		}

		/*
		 * Handle NULL attributes
		 */
		if (ttc->ttc_isnull[i])
		{
			ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
			ttc->ttc_flags |= TOAST_HAS_NULLS;
			continue;
		}

		/*
		 * Now look at varlena attributes
		 */
		if (att->attlen == -1)
		{
			/*
			 * If the table's attribute says PLAIN always, force it so.
			 */
			if (att->attstorage == 'p')
				ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;

			/*
			 * We took care of UPDATE above, so any external value we find
			 * still in the tuple must be someone else's that we cannot reuse
			 * (this includes the case of an out-of-line in-memory datum).
			 * Fetch it back (without decompression, unless we are forcing
			 * PLAIN storage).  If necessary, we'll push it out as a new
			 * external value below.
			 */
			if (VARATT_IS_EXTERNAL(new_value))
			{
				ttc->ttc_attr[i].tai_oldexternal = new_value;
				if (att->attstorage == 'p')
					new_value = detoast_attr(new_value);
				else
					new_value = detoast_external_attr(new_value);
				ttc->ttc_values[i] = PointerGetDatum(new_value);
				ttc->ttc_attr[i].tai_colflags |= TOASTCOL_NEEDS_FREE;
				ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
			}

			/*
			 * Remember the size of this attribute
			 */
			ttc->ttc_attr[i].tai_size = VARSIZE_ANY(new_value);
		}
		else
		{
			/*
			 * Not a varlena attribute, plain storage always
			 */
			ttc->ttc_attr[i].tai_colflags |= TOASTCOL_IGNORE;
		}
	}
}

/*
 * Find the largest varlena attribute that satisfies certain criteria.
 *
 * The relevant column must not be marked TOASTCOL_IGNORE, and if the
 * for_compression flag is passed as true, it must also not be marked
 * TOASTCOL_INCOMPRESSIBLE.
 *
 * The column must have attstorage 'e' or 'x' if check_main is false, and
 * must have attstorage 'm' if check_main is true.
 *
 * The column must have a minimum size of MAXALIGN(TOAST_POINTER_SIZE);
 * if not, no benefit is to be expected by compressing it.
 *
 * The return value is the index of the biggest suitable column, or
 * -1 if there is none.
 */
int
toast_tuple_find_biggest_attribute(ToastTupleContext *ttc,
								   bool for_compression, bool check_main)
{
	TupleDesc	tupleDesc = ttc->ttc_rel->rd_att;
	int			numAttrs = tupleDesc->natts;
	int			biggest_attno = -1;
	int32		biggest_size = MAXALIGN(TOAST_POINTER_SIZE);
	int32		skip_colflags = TOASTCOL_IGNORE;
	int			i;

	if (for_compression)
		skip_colflags |= TOASTCOL_INCOMPRESSIBLE;

	for (i = 0; i < numAttrs; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);

		if ((ttc->ttc_attr[i].tai_colflags & skip_colflags) != 0)
			continue;
		if (VARATT_IS_EXTERNAL(DatumGetPointer(ttc->ttc_values[i])))
			continue;			/* can't happen, toast_action would be 'p' */
		if (for_compression &&
			VARATT_IS_COMPRESSED(DatumGetPointer(ttc->ttc_values[i])))
			continue;
		if (check_main && att->attstorage != 'm')
			continue;
		if (!check_main && att->attstorage != 'x' && att->attstorage != 'e')
			continue;

		if (ttc->ttc_attr[i].tai_size > biggest_size)
		{
			biggest_attno = i;
			biggest_size = ttc->ttc_attr[i].tai_size;
		}
	}

	return biggest_attno;
}

/*
 * Try compression for an attribute.
 *
 * If we find that the attribute is not compressible, mark it so.
 */
void
toast_tuple_try_compression(ToastTupleContext *ttc, int attribute)
{
	Datum	   *value = &ttc->ttc_values[attribute];
	Datum		new_value = toast_compress_datum(*value);
	ToastAttrInfo *attr = &ttc->ttc_attr[attribute];

	if (DatumGetPointer(new_value) != NULL)
	{
		/* successful compression */
		if ((attr->tai_colflags & TOASTCOL_NEEDS_FREE) != 0)
			pfree(DatumGetPointer(*value));
		*value = new_value;
		attr->tai_colflags |= TOASTCOL_NEEDS_FREE;
		attr->tai_size = VARSIZE(DatumGetPointer(*value));
		ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
	}
	else
	{
		/* incompressible, ignore on subsequent compression passes */
		attr->tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
	}
}

/*
 * Move an attribute to external storage.
 */
void
toast_tuple_externalize(ToastTupleContext *ttc, int attribute, int options,
						int max_chunk_size)
{
	Datum	   *value = &ttc->ttc_values[attribute];
	Datum		old_value = *value;
	ToastAttrInfo *attr = &ttc->ttc_attr[attribute];

	/* Initialize for TOAST table access, if not yet done. */
	if (ttc->ttc_toastrel == NULL)
	{
		ttc->ttc_toastrel =
			table_open(ttc->ttc_rel->rd_rel->reltoastrelid, RowExclusiveLock);
		ttc->ttc_validtoastidx = toast_open_indexes(ttc->ttc_toastrel,
													RowExclusiveLock,
													&ttc->ttc_toastidxs,
													&ttc->ttc_ntoastidxs);
	}
	if (ttc->ttc_toastslot == NULL)
		ttc->ttc_toastslot = table_slot_create(ttc->ttc_toastrel, NULL);

	/* Do the real work. */
	*value = toast_save_datum(ttc->ttc_toastrel, ttc->ttc_toastslot,
							  ttc->ttc_ntoastidxs, ttc->ttc_toastidxs,
							  ttc->ttc_validtoastidx,
							  ttc->ttc_rel->rd_toastoid,
							  old_value, attr->tai_oldexternal,
							  options, max_chunk_size);

	/* Update bookkeeping information. */
	if ((attr->tai_colflags & TOASTCOL_NEEDS_FREE) != 0)
		pfree(DatumGetPointer(old_value));
	attr->tai_colflags |= (TOASTCOL_NEEDS_FREE | TOASTCOL_IGNORE);
	ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
}

/*
 * Perform appropriate cleanup after one tuple has been subjected to TOAST.
 *
 * Pass cleanup_toastrel as true to destroy and clear ttc_toastrel and
 * ttc_toastslot, or false if caller will do it.
 */
void
toast_tuple_cleanup(ToastTupleContext *ttc, bool cleanup_toastrel)
{
	TupleDesc	tupleDesc = ttc->ttc_rel->rd_att;
	int			numAttrs = tupleDesc->natts;

	/*
	 * Free allocated temp values
	 */
	if ((ttc->ttc_flags & TOAST_NEEDS_FREE) != 0)
	{
		int			i;

		for (i = 0; i < numAttrs; i++)
		{
			ToastAttrInfo *attr = &ttc->ttc_attr[i];

			if ((attr->tai_colflags & TOASTCOL_NEEDS_FREE) != 0)
				pfree(DatumGetPointer(ttc->ttc_values[i]));
		}
	}

	/*
	 * Delete external values from the old tuple
	 */
	if ((ttc->ttc_flags & TOAST_NEEDS_DELETE_OLD) != 0)
	{
		int			i;

		/* Initialize for TOAST table access, if not yet done. */
		if (ttc->ttc_toastrel == NULL)
		{
			ttc->ttc_toastrel =
				table_open(ttc->ttc_rel->rd_rel->reltoastrelid,
						   RowExclusiveLock);
			ttc->ttc_validtoastidx = toast_open_indexes(ttc->ttc_toastrel,
														RowExclusiveLock,
														&ttc->ttc_toastidxs,
														&ttc->ttc_ntoastidxs);
		}

		/* Delete those attributes which require it. */
		for (i = 0; i < numAttrs; i++)
		{
			ToastAttrInfo *attr = &ttc->ttc_attr[i];

			if ((attr->tai_colflags & TOASTCOL_NEEDS_DELETE_OLD) != 0)
				toast_delete_datum(ttc->ttc_toastrel, ttc->ttc_ntoastidxs,
								   ttc->ttc_toastidxs, ttc->ttc_validtoastidx,
								   ttc->ttc_oldvalues[i], false, 0);
		}
	}

	/*
	 * Close toast table and indexes and drop slot, if previously done and
	 * if caller requests it.
	 */
	if (cleanup_toastrel && ttc->ttc_toastrel != NULL)
	{
		if (ttc->ttc_toastslot != NULL)
		{
			ExecDropSingleTupleTableSlot(ttc->ttc_toastslot);
			ttc->ttc_toastslot = NULL;
		}
		toast_close_indexes(ttc->ttc_toastidxs, ttc->ttc_ntoastidxs,
							RowExclusiveLock);
		table_close(ttc->ttc_toastrel, RowExclusiveLock);
		ttc->ttc_toastrel = NULL;
	}
}

/*
 * Check for external stored attributes and delete them from the secondary
 * relation.
 */
void
toast_delete_external(Relation rel, Datum *values, bool *isnull,
					  bool is_speculative, uint32 specToken)
{
	TupleDesc	tupleDesc = rel->rd_att;
	int			numAttrs = tupleDesc->natts;
	int			i;
	Relation    toastrel = NULL;
	Relation   *toastidxs;
	int         num_indexes;
	int         validIndex;

	for (i = 0; i < numAttrs; i++)
	{
		Datum	value;

		if (isnull[i] || TupleDescAttr(tupleDesc, i)->attlen != -1)
			continue;

		value = values[i];
		if (!VARATT_IS_EXTERNAL_ONDISK(PointerGetDatum(value)))
			continue;

		/* Initialize for TOAST table access, if not yet done. */
		if (toastrel == NULL)
		{
			toastrel = table_open(rel->rd_rel->reltoastrelid,
								  RowExclusiveLock);
			validIndex = toast_open_indexes(toastrel, RowExclusiveLock,
											&toastidxs, &num_indexes);
		}

		toast_delete_datum(toastrel, num_indexes, toastidxs, validIndex,
						   value, is_speculative, specToken);
	}

	if (toastrel != NULL)
	{
		toast_close_indexes(toastidxs, num_indexes, RowExclusiveLock);
		table_close(toastrel, RowExclusiveLock);
	}
}
