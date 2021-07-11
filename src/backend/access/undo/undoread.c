/*
 * XXX: THIS IS NOT A PROPER SEPARATION OF CONCERNS - a fair bit of this
 * probably should be moved to other files.
 *
 * FIXME:
 * - need to signal what undo we're reading
 * - don't just copy everything into memory
 * - introduce efficient reading interface
 */

#include "postgres.h"

#include "access/undo.h"
#include "access/undolog.h"
#include "access/undopage.h"
#include "access/undoread.h"
#include "access/undorecordset.h"
#include "access/xactundo.h"
#include "storage/bufmgr.h"


/* ---
 * Generic undo helpers (i.e. no URS awareness)
 * ---
 */

static void
undo_release_buffer(UndoCachedBuffer *cached_buffer)
{
	if (BufferIsValid(cached_buffer->pinned_buffer))
	{
		Assert(cached_buffer->pinned_block != InvalidBlockNumber);

		ReleaseBuffer(cached_buffer->pinned_buffer);
		cached_buffer->pinned_buffer = InvalidBuffer;
		cached_buffer->pinned_block = InvalidBlockNumber;
	}
	else
		Assert(cached_buffer->pinned_block == InvalidBlockNumber);
}

static Buffer
undo_read_block(UndoCachedBuffer *cached_buffer,
				char relpersistence,
				UndoRecPtr urp)
{
	RelFileNode rnode;
	BlockNumber blockno = UndoRecPtrGetBlockNum(urp);

	UndoRecPtrAssignRelFileNode(rnode, urp);

	if (cached_buffer->pinned_block == blockno &&
		rnode.relNode == cached_buffer->pinned_log)
	{
		Assert(BufferIsValid(cached_buffer->pinned_buffer));
		return cached_buffer->pinned_buffer;
	}
	else
		undo_release_buffer(cached_buffer);

	cached_buffer->pinned_buffer =
		ReadBufferWithoutRelcache(SMGR_UNDO,
								  rnode,
								  MAIN_FORKNUM,
								  blockno,
								  RBM_NORMAL,
								  NULL,
								  relpersistence);
	/* The buffer could have been discarded. */
	if (cached_buffer->pinned_buffer != InvalidBuffer)
	{
		cached_buffer->pinned_block = blockno;
		cached_buffer->pinned_log = rnode.relNode;
	}
	else
		cached_buffer->pinned_block = InvalidBlockNumber;

	return cached_buffer->pinned_buffer;
}

/*
 * Read 'nbytes' bytes starting at position 'urp' into memory starting at
 * 'data', skipping page headers. Returns undo pointer immediately following
 * the last byte read.
 *
 * 'allow_discarded' controls what should happen if any part of the required
 * undo log is discared. If it's true, return the pointer to the first
 * discarded byte. If false, raise an ERROR.
 */
static UndoRecPtr
undo_read_bytes(UndoCachedBuffer *cached_buffer,
				char relpersistence,
				UndoRecPtr urp,
				size_t nbytes,
				bool allow_discarded,
				char *data)
{
	size_t		data_off = 0;

	while (nbytes > 0)
	{
		int			page_off = UndoRecPtrGetPageOffset(urp);
		int			nread;
		Page		page;

		if (page_off < SizeOfUndoPageHeaderData)
			page_off = SizeOfUndoPageHeaderData;

		undo_read_block(cached_buffer, relpersistence, urp);

		/* Stop reading if the underlying block has been discarded. */
		if (cached_buffer->pinned_buffer == InvalidBuffer)
		{
			if (allow_discarded)
				return urp;
			else
				elog(ERROR, "undo log at " UndoRecPtrFormat " is discarded",
					 urp);
		}

		page = BufferGetPage(cached_buffer->pinned_buffer);

		if (page_off + nbytes > BLCKSZ)
			nread = BLCKSZ - page_off;
		else
			nread = nbytes;

		LockBuffer(cached_buffer->pinned_buffer, BUFFER_LOCK_SHARE);

		{
			UndoPageHeader uph = (UndoPageHeader) page;

			if (unlikely(page_off + nread > uph->ud_insertion_point))
				elog(ERROR, "asked to read [%u, %u) but insertion point is %u",
					 page_off, page_off + nread, uph->ud_insertion_point);
		}

		memcpy(data + data_off, (char *) page + page_off, nread);

		LockBuffer(cached_buffer->pinned_buffer, BUFFER_LOCK_UNLOCK);

		nbytes -= nread;
		data_off += nread;
		urp = (urp - urp % BLCKSZ) + page_off + nread;
	}

	return urp;
}

/*
 * Read undo page header into memory 'uph' points to. Return true if
 * succeeded, false if the undo page has been discarded.
 */
static bool
undo_read_page_header(UndoCachedBuffer *cached_buffer,
					  char relpersistence,
					  UndoRecPtr urp,
					  UndoPageHeader uph)
{
	Page		page;

	undo_read_block(cached_buffer, relpersistence, urp);

	/* Discarded? */
	if (cached_buffer->pinned_buffer == InvalidBuffer)
		return false;

	page = BufferGetPage(cached_buffer->pinned_buffer);

	LockBuffer(cached_buffer->pinned_buffer, BUFFER_LOCK_SHARE);
	memcpy(uph, page, sizeof(UndoPageHeaderData));
	LockBuffer(cached_buffer->pinned_buffer, BUFFER_LOCK_UNLOCK);

	return true;
}


/* ---
 * Functions for reading an URS.
 * ---
 */

/*
 * Helper function for urs_chunk_find_start, to iterate through all the chunks
 * starting on the page, stopping at the one starting at urp (if exact = true)
 * or containing urp (exact = false).  Iff end_location is not invalid, urp is
 * in a currently open chunk, e.g. because we're doing a subtransaction
 * rollback.
 */
static UndoRecPtr
urs_chunk_find_start_on_page(UndoCachedBuffer *cached_buffer,
							 char relpersistence,
							 UndoRecPtr urp, UndoRecPtr end_location,
							 bool exact,
							 UndoRecordSetChunkHeader *urs_header)
{
	int			target_off = UndoRecPtrGetPageOffset(urp);
	UndoRecPtr	urp_page_start = urp - target_off;
	int			current_off;
	UndoPageHeaderData uph;

	if (!undo_read_page_header(cached_buffer, relpersistence, urp_page_start,
							   &uph))
		elog(ERROR, "cannot read header of discarded undo page");

	current_off = uph.ud_first_chunk;

	while (true)
	{
		UndoLogOffset effective_chunk_size;

		Assert(current_off >= SizeOfUndoPageHeaderData);
		Assert(target_off >= current_off);

		undo_read_bytes(cached_buffer, relpersistence,
						urp_page_start + current_off,
						SizeOfUndoRecordSetChunkHeader,
						false,
						(char *) urs_header);

		effective_chunk_size = urs_header->size;
		if (effective_chunk_size == 0)
		{
			if (end_location != InvalidUndoRecPtr)
				effective_chunk_size = end_location - (urp_page_start + current_off);
			else
				elog(ERROR, "found open chunk at %lu", urp);
		}

		if (exact)
		{
			/* found target */
			if (target_off == current_off)
				break;

			if (target_off < (current_off + effective_chunk_size))
				elog(ERROR, "invalid page: pointing to the middle of chunk");
		}
		else
		{
			/* found target */
			if (target_off < (current_off + effective_chunk_size))
				break;
		}

		if (urs_header->size == 0)
			elog(ERROR, "target chunk beyond open chunk");

		/* look at next chunk */
		current_off = current_off + urs_header->size;

		if (current_off >= BLCKSZ)
			elog(ERROR, "requested chunk header not on page");
	}

	if (end_location != InvalidUndoRecPtr &&
		urs_header->size != 0)
		elog(ERROR, "looking for open chunk, found closed");

	/* FIXME: verify chunk type */

	return urp_page_start + current_off;
}

/*
 * Identify the chunk containing urp. Iff end_location is not invalid, urp is
 * in a currently open chunk, e.g. because we're doing a subtransaction
 * rollback.  At exit urs_header will contain the header for the identified
 * chunk, or an error will have been raised.
 */
static UndoRecPtr
urs_chunk_find_start(UndoCachedBuffer *cached_buffer,
					 char relpersistence,
					 UndoRecPtr urp,
					 UndoRecPtr end_location,
					 UndoRecordSetChunkHeader *urs_header)
{
	UndoPageHeaderData uph_initial;
	int			off = UndoRecPtrGetPageOffset(urp);
	UndoRecPtr	urp_chunk_header;

	if (!undo_read_page_header(cached_buffer, relpersistence, urp,
							   &uph_initial))
		elog(ERROR, "cannot read header of discarded undo page");

	if (uph_initial.ud_insertion_point == 0)
		elog(ERROR, "page not initialized");
	else if (off >= uph_initial.ud_insertion_point)
		elog(ERROR, "invalid urp: beyond insertion point");
	else if (uph_initial.ud_first_chunk == 0 || off < uph_initial.ud_first_chunk)
	{
		/*
		 * The start of the chunk is on a preceding page. Perform some
		 * verification, and then continue by reading the start of the urp at
		 * the other page.
		 */

		if (uph_initial.ud_first_chunk > 0 && off < SizeOfUndoPageHeaderData)
			elog(ERROR, "invalid urp: within page header");

		if (uph_initial.ud_continue_chunk == InvalidUndoRecPtr)
			elog(ERROR, "invalid page: continue invalid");
		if (uph_initial.ud_continue_chunk >= (urp - (urp % BLCKSZ)))
			elog(ERROR, "invalid page: continue too large");
		/* FIXME: validate chunk starts in same log */
		/* FIXME: validate chunk type */

		urp_chunk_header = uph_initial.ud_continue_chunk;

		urp_chunk_header =
			urs_chunk_find_start_on_page(cached_buffer, relpersistence,
										 urp_chunk_header, end_location, true, urs_header);
		Assert(urp_chunk_header == uph_initial.ud_continue_chunk);
	}
	else
	{
		urp_chunk_header =
			urs_chunk_find_start_on_page(cached_buffer, relpersistence,
										 urp, end_location, false, urs_header);
	}

	Assert(urp_chunk_header <= urp);

	return urp_chunk_header;
}

/*
 * Build list of all chunks preceding the chunk already in cl.
 */
static void
urs_load_preceding_chunks(UndoCachedBuffer *cached_buffer,
						  char relpersistence,
						  UndoRecordSetChunkList *cl)
{
	UndoRecordSetChunkListItem *cur;
	UndoRecordSetChunkListItem *prev;

	Assert(cl->nchunks == 1 && cl->chunks != NULL);

	while (true)
	{
		cur = &cl->chunks[0];

		if (!cur->header.previous_chunk)
			break;

		/* different log, blocknos could be the same */
		undo_release_buffer(cached_buffer);

		cl->chunks =
			repalloc(cl->chunks,
					 sizeof(UndoRecordSetChunkListItem) * (cl->nchunks + 1));
		memmove((char *) cl->chunks + sizeof(UndoRecordSetChunkListItem),
				(char *) cl->chunks,
				sizeof(UndoRecordSetChunkListItem) * cl->nchunks);

		cur = &cl->chunks[0];
		prev = &cl->chunks[1];
		cl->nchunks++;

		memset(&cur->header, 0, sizeof(UndoRecordSetChunkHeader));
		cur->urp_chunk_header = prev->header.previous_chunk;

		if (cur->urp_chunk_header == prev->urp_chunk_header)
			elog(ERROR, "previous urs chunk is the same as current");
		if (UndoRecPtrGetLogNo(cur->urp_chunk_header) == UndoRecPtrGetLogNo(prev->urp_chunk_header))
			elog(ERROR, "previous urs chunk is in the same log as current");

		urs_chunk_find_start_on_page(cached_buffer, relpersistence,
									 cur->urp_chunk_header, InvalidUndoRecPtr,
									  /* exact = */ true, &cur->header);

		cur->urp_chunk_end = cur->urp_chunk_header + cur->header.size;
		if (UndoRecPtrGetPageOffset(cur->urp_chunk_end) <= SizeOfUndoPageHeaderData)
		{
			if (UndoRecPtrGetPageOffset(cur->urp_chunk_end) < SizeOfUndoPageHeaderData)
				elog(ERROR, "chunk end in page header");
			cur->urp_chunk_end -= SizeOfUndoPageHeaderData;
		}

		elog(DEBUG1, "found chunk %lu, len %lu, end at %lu, continuing from %lu",
			 cur->urp_chunk_header, cur->header.size,
			 cur->urp_chunk_header + cur->header.size,
			 cur->header.previous_chunk);
	}
}

/*
 * Read a number of bytes of undo content, correctly crossing page boundaries
 * if necessary. Returns pointer to the byte after the data.
 *
 * For the meaning of 'allow_discarded', see the header comment of
 * undo_read_bytes().
 */
UndoRecPtr
undo_reader_read_bytes(UndoRSReaderState *r,
					   UndoRecPtr urp,
					   size_t nbytes,
					   bool allow_discarded)
{
	UndoRecPtr	ret;

	enlargeStringInfo(&r->buf, nbytes);

	ret = undo_read_bytes(&r->cached_buffer,
						  r->relpersistence,
						  urp,
						  nbytes,
						  allow_discarded,
						  r->buf.data + r->buf.len);

	r->buf.len = nbytes;

	return ret;
}

static void
undo_reader_release_buffer(UndoRSReaderState *r)
{
	undo_release_buffer(&r->cached_buffer);
}

/*
 * Store distance between two records in a "varbyte" format. This is
 * beneficial because the distance is usually requires one or two bytes.
 *
 * We use the same scheme like encode_varbyte() in ginpostinglist.c, but
 * eventually write the bytes in reverse order. The point is that the records
 * will also be fetched so.
 */
static void
store_record_dist(UndoRSReaderState *r, UndoRecPtr rec_dist)
{
	/*
	 * We can use 7 bits of each byte, thus 8 bytes of the source value should
	 * always fit into 10 bytes of the encoded data.
	 */
#define	MaxBytesPerValue	10

	static char encoded[MaxBytesPerValue];

	char	   *last = &encoded[MaxBytesPerValue - 1];
	char	   *p = last;
	int			nbytes;

	while (rec_dist > 0x7F)
	{
		*(p--) = 0x80 | (rec_dist & 0x7F);
		rec_dist >>= 7;
	}
	*p = (unsigned char) rec_dist;

	nbytes = last - p + 1;
	Assert(nbytes > 0 && nbytes <= MaxBytesPerValue);

	appendBinaryStringInfo(&r->rec_dists, p, nbytes);
}

/*
 * Decode the next (in the backward direction) entry of r->rec_dists.
 */
static UndoRecPtr
get_next_record_dist(UndoRSReaderState *r)
{
	char	   *p = r->backward_cur;
	Size		result = 0;
	Size		i;
	int			shift = 0;

	do
	{
		i = *((unsigned char *) --p);
		result += (i & 0x7F) << shift;
		shift += 7;
	} while (i & 0x80);

	Assert(r->backward_cur - p <= MaxBytesPerValue);
	r->backward_cur = p;

	return result;
}

/*
 * Read the remaining part of the node whose length has just been read.
 *
 * 'urp' points to the position immediately following the length information,
 * 'len' is the remaining amount of data.
 *
 * For the meaning of 'allow_discarded', see the header comment of
 * undo_read_bytes().
 *
 * Returns pointer to the first byte following the last byte read.
 */
static UndoRecPtr
read_node_remaining(UndoRSReaderState *r, UndoRecPtr urp, Size len,
					bool allow_discarded)
{
	WrittenUndoNode *node = &r->node;

	/* rmid */
	resetStringInfo(&r->buf);
	urp = undo_reader_read_bytes(r, urp, sizeof(node->n.rmid),
								 allow_discarded);
	memcpy(&node->n.rmid, r->buf.data, sizeof(node->n.rmid));
	len -= sizeof(node->n.rmid);

	/* type */
	resetStringInfo(&r->buf);
	urp = undo_reader_read_bytes(r, urp, sizeof(node->n.type),
								 allow_discarded);
	memcpy(&node->n.type, r->buf.data, sizeof(node->n.type));
	len -= sizeof(node->n.type);

	/* The actual record data */
	resetStringInfo(&r->buf);
	urp = undo_reader_read_bytes(r, urp, len, allow_discarded);
	node->n.data = r->buf.data;

	return urp;
}

/*
 * Initialize reading an entire undo record set. Urp can point to either the
 * header, or anywhere within the urs.
 *
 * FIXME: relies on CurrentMemoryContext - probably OK?
 */
void
UndoRSReaderInit(UndoRSReaderState *r,
				 UndoRecPtr start, UndoRecPtr end,
				 char relpersistence, bool toplevel)
{
	UndoRecordSetChunkListItem *last_chunk;
	UndoRecordSetChunkListItem *first_chunk PG_USED_FOR_ASSERTS_ONLY;
	UndoRecPtr	end_within;

	memset(r, 0, sizeof(UndoRSReaderState));

	r->start_reading = start;
	r->end_reading = end;

	r->cached_buffer.pinned_buffer = InvalidBuffer;
	r->cached_buffer.pinned_block = InvalidBlockNumber;
	r->relpersistence = relpersistence;
	initStringInfo(&r->buf);

	/*
	 * FIXME: end location points to the *end* of the chunk, i.e. to just
	 * after the last record. But to keep the urs_* routines reusable, we want
	 * to pass a location *within* the chunk. Thus rewind 1 byte. But that'd
	 * potentially point inside the page header, which we treat as an error -
	 * so rewind more if that's the case.
	 */
	end_within = end - 1;
	if (UndoRecPtrGetPageOffset(end_within) < SizeOfUndoPageHeaderData)
		end_within -= (UndoRecPtrGetPageOffset(end_within) + 1);

	r->chunks.nchunks = 1;
	r->chunks.chunks = palloc(sizeof(UndoRecordSetChunkListItem));

	/*
	 * For the end location we do not always know where the containing chunk
	 * is. Identify the chunk based on page contents.
	 */
	last_chunk = &r->chunks.chunks[0];
	last_chunk->urp_chunk_header =
		urs_chunk_find_start(&r->cached_buffer, r->relpersistence,
							 end_within,
							 toplevel ? InvalidUndoRecPtr : end,
							 &last_chunk->header);

	elog(DEBUG1, "found chunk for end urp %lu at " UndoRecPtrFormat ", len %lu, end at %lu, continuing from %lu",
		 end, last_chunk->urp_chunk_header, last_chunk->header.size, last_chunk->urp_chunk_header + last_chunk->header.size,
		 last_chunk->header.previous_chunk);

	if (toplevel)
		last_chunk->urp_chunk_end = last_chunk->urp_chunk_header + last_chunk->header.size;
	else
	{
		if (last_chunk->header.size != 0)
			elog(ERROR, "unexpected closed URS for subtransaction");
		/* fill in a size for the chunk */
		last_chunk->urp_chunk_end = end;
	}

	if (UndoRecPtrGetPageOffset(last_chunk->urp_chunk_end) <= SizeOfUndoPageHeaderData)
	{
		if (UndoRecPtrGetPageOffset(last_chunk->urp_chunk_end) < SizeOfUndoPageHeaderData)
			elog(ERROR, "chunk end in page header");
		last_chunk->urp_chunk_end -= SizeOfUndoPageHeaderData;
	}

	/*
	 * Now that we have one chunk the urs, build list of all chunks.
	 */
	urs_load_preceding_chunks(&r->cached_buffer, r->relpersistence,
							  &r->chunks);
	first_chunk = &r->chunks.chunks[0];

	Assert(first_chunk->header.previous_chunk == InvalidUndoRecPtr);

	r->current_chunk = 1;

	initStringInfo(&r->rec_dists);
}

/*
 * Read one record in forward direction, with the first record returned being
 * the one at start as passed to UndoRSReaderInit(), ending at end.
 *
 * If length_only is true, only store the record length, otherwise the whole
 * record.
 */
bool
UndoRSReaderReadOneForward(UndoRSReaderState *r, bool length_only)
{
	UndoRecordSetChunkListItem *curchunk;
	Size		rec_len;
	UndoRecPtr	urp_content;
	UndoRecPtr	next;

	/* read all */
	if (r->current_chunk == -1)
		goto done;

	Assert(r->current_chunk > 0 && r->current_chunk <= r->chunks.nchunks);

	curchunk = &r->chunks.chunks[r->current_chunk - 1];

	if (r->next_urp == InvalidUndoRecPtr)
	{
		if (curchunk->urp_chunk_header == r->start_reading ||
			UndoRecPtrGetLogNo(curchunk->urp_chunk_header) != UndoRecPtrGetLogNo(r->start_reading))
		{
			/* first skip over the chunk header */
			r->next_urp =
				UndoRecPtrPlusUsableBytes(curchunk->urp_chunk_header, SizeOfUndoRecordSetChunkHeader);

			/* and then over the type specific header */
			if (curchunk->header.previous_chunk == InvalidUndoRecPtr)
				r->next_urp = UndoRecPtrPlusUsableBytes(r->next_urp,
														SizeOfXactUndoRecordSetHeader);
		}
		else
		{
			r->next_urp = r->start_reading;
		}
	}

	if (r->next_urp == InvalidUndoRecPtr)
		goto done;

	if (r->next_urp >= r->end_reading)
	{
		undo_reader_release_buffer(r);
		goto done;
	}

	/* Read the URS record length, could be split over pages. */

	/*
	 * XXX: Right now we use a fixed width length encoding, but once this is
	 * encoded as a variable length integer, there's no way around reading
	 * this separately.
	 */
	resetStringInfo(&r->buf);
	urp_content = undo_reader_read_bytes(r, r->next_urp, sizeof(rec_len),
										 false);
	rec_len = *(Size *) r->buf.data;

	if (length_only)
	{
		UndoLogNumber next_logno;
		UndoLogOffset next_off;

		/*
		 * Store the distance from the previous record, as it usually takes
		 * much less space than the pointer. It'd be simpler to store the
		 * record length, but the last record in the chunk may be followed by
		 * unused bytes.
		 */
		if (r->last_record != InvalidUndoRecPtr)
			store_record_dist(r, r->next_urp - r->last_record);
		r->last_record = r->next_urp;

		/* Compute where the next records should start. */
		next_logno = UndoRecPtrGetLogNo(r->next_urp);
		next_off = UndoRecPtrGetOffset(r->next_urp);
		next_off = UndoLogOffsetPlusUsableBytes(next_off, rec_len);
		next = MakeUndoRecPtr(next_logno, next_off);
	}
	else
	{
		r->node.location = r->next_urp;
		r->node.n.length = rec_len;

		/* Read the remaining part of the node. */
		next = read_node_remaining(r, urp_content, rec_len - sizeof(rec_len),
								   false);
	}

	if (next >= curchunk->urp_chunk_end)
	{
		r->next_urp = InvalidUndoRecPtr;
		if (r->current_chunk < r->chunks.nchunks)
			r->current_chunk++;
		else
			r->current_chunk = -1;

		/* block numbers in next chunk could be identical */
		undo_reader_release_buffer(r);
	}
	else
		r->next_urp = next;

	return true;

done:
	/* Make sure the last record is stored. */
	if (length_only && r->last_record != InvalidUndoRecPtr)
	{
		Assert(r->last_record < r->end_reading);

		store_record_dist(r, r->end_reading - r->last_record);
	}

	return false;
}

/*
 * Read one record in backward direction, with the first record returned being
 * the one at end as passed to UndoRSReaderInit(), ending at start.
 */
bool
UndoRSReaderReadOneBackward(UndoRSReaderState *r)
{
	StringInfo	rl = &r->rec_dists;

	if (rl->len == 0)
	{
		/*
		 * First, read pointers to all the records in given range, but not the
		 * records themselves. Actually we store only an array of record
		 * lengths (in varbyte format), to conserve memory.
		 */
		while (UndoRSReaderReadOneForward(r, true))
			;

		/* No records found? */
		if (rl->len == 0)
			return false;

		/* Initialize the pointer to read the length of the last record. */
		r->backward_cur = rl->data + rl->len;

		/* URP immediately following the last node returned. */
		r->node.location = r->end_reading;
	}

	if (r->backward_cur > rl->data)
	{
		Size		rec_len;
		UndoRecPtr	urp,
					urp_diff;
		WrittenUndoNode *node = &r->node;

		urp_diff = get_next_record_dist(r);
		Assert(r->backward_cur >= rl->data);

		/* Compute position of the next record. */
		node->location -= urp_diff;

		/* Read the actual record length. */
		resetStringInfo(&r->buf);
		undo_reader_read_bytes(r, node->location, sizeof(rec_len), false);
		rec_len = *(Size *) r->buf.data;
		node->n.length = rec_len;

		/*
		 * The amount of useful data can be lower than urp_diff if the record
		 * crosses page boundary or if there is unused space at the end of the
		 * page.
		 */
		Assert(node->n.length <= urp_diff);

		/* Read the remaining part. */
		urp = UndoRecPtrPlusUsableBytes(node->location, sizeof(rec_len));
		read_node_remaining(r, urp, rec_len - sizeof(rec_len), false);

		return true;
	}

	return false;
}

void
UndoRSReaderClose(UndoRSReaderState *r)
{
	undo_reader_release_buffer(r);

	if (r->buf.data)
		pfree(r->buf.data);

	if (r->rec_dists.data)
		pfree(r->rec_dists.data);
}
