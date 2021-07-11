/*-------------------------------------------------------------------------
 *
 * undo_parser.c
 *	  Retrieve chunks and records from the undo log pages.
 *
 * src/common/undo_parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/undopage.h"
#include "access/undorecordset.h"
#include "common/undo_parser.h"
#include "utils/memutils.h"

static void add_chunk_info(UndoLogParserState * s, UndoRecordSetType urs_type);
static void enlarge_item_array(UndoLogParserState * s, int size);
static void reset_storage(UndoLogParserState * state, bool records);
static void process_records(UndoLogParserState * s, char *data, int size,
							UndoRecPtr data_start);

/*
 * Initialize the undo log parser state.
 */
/* TODO Adjust as soon as tested. */
#define MIN_PAGE_ITEMS	2
void
initialize_undo_parser(UndoLogParserState * state)
{
	memset(state, 0, sizeof(UndoLogParserState));
}

/*
 * Finalize the undo log parser state.
 */
void
finalize_undo_parser(UndoLogParserState * state, bool records)
{
	reset_storage(state, records);
	if (state->items)
	{
		pfree(state->items);
		state->items = NULL;
	}
	state->nitems = 0;
}

/* ----------------
 * Parse a single page of an undo log.
 *
 * 's' points to the parser state.
 * 'page' points to memory containing the page contents
 * 'nr' is page number within the segment, 0-based
 * 'seg' points to information about the containing segment
 * 'chunks' and 'records' tell whether information on chunk headers and
 * records should be collected respectively. Note that chunk or record data
 * allocated by the previous call will be freed.
 *
 * Caller should first initialize the parser state and then pass it one page
 * after another. Once s->gap appears to be true, parsing should stop. The
 * function should be able to cross gap if it's located at the end of the log,
 * however a gap elsewhere indicates data problem. Thus it makes more sense
 * for the caller do some sanity checks, finalize the parser and initialize a
 * new one for the next log.
 *
 * Note: records are not collected if the parser is still searching for the
 * first chunk header.
 * ----------------
 */
void
parse_undo_page(UndoLogParserState * s, char *page, int nr, UndoSegFile *seg,
				bool records)
{
	UndoPageHeaderData pghdr;
	int			page_offset = 0;
	int			page_usage;
	uint16		first_rec;
	uint16		first_chunk = 0;
	UndoRecPtr	cont;

	/* Caller should not try top process a gap. */
	Assert(!s->gap);

	/* Free the chunks/records allocated by the previous call. */
	reset_storage(s, records);

	/* The segment is not loaded aligned. */
	memcpy(&pghdr, page, SizeOfUndoPageHeaderData);

	page_usage = pghdr.ud_insertion_point;
	if (page_usage == 0)
	{
		/*
		 * This should be the end of the current log file, not only the chunk.
		 */
		s->gap = true;
		return;
	}

	if (page_usage < SizeOfUndoPageHeaderData || page_usage > BLCKSZ)
	{
#ifdef FRONTEND
		pg_log_error("page %d of the log segment \"%s\" has invalid ud_insertion_point: %d",
					 nr, seg->path, page_usage);
		return;
#else
		ereport(ERROR,
				(errmsg("page %d of the log segment \"%s\" has invalid ud_insertion_point: %d",
						nr, seg->path, page_usage)));
#endif
	}

	first_rec = pghdr.ud_first_record;
	if (first_rec != 0 &&
		(first_rec < SizeOfUndoPageHeaderData ||
		 first_rec >= pghdr.ud_insertion_point))
	{
#ifdef FRONTEND
		pg_log_error("page %d of the log segment \"%s\" has invalid ud_first_record: %d",
					 nr, seg->path, pghdr.ud_first_record);
		return;
#else
		ereport(ERROR,
				(errmsg("page %d of the log segment \"%s\" has invalid ud_first_record: %d",
						nr, seg->path, pghdr.ud_first_record)));
#endif
	}

	/* The current chunk must start in front of the current page. */
#ifdef USE_ASSERT_CHECKING
	{
		UndoRecPtr	page_end = MakeUndoRecPtr(seg->logno, 0);

		/*
		 * Add the offset manually because MakeUndoRecPtr() does not work if
		 * the offset is at log boundary.
		 */
		page_end += seg->offset + (nr + 1) * BLCKSZ;

		Assert(s->current_chunk < page_end);
	}
#endif


	cont = pghdr.ud_continue_chunk;
	if (s->current_chunk != InvalidUndoRecPtr && cont != InvalidUndoRecPtr &&
		cont != s->current_chunk)
	{
		UndoLogNumber logno = UndoRecPtrGetLogNo(cont);
		UndoLogOffset offset = UndoRecPtrGetOffset(cont);

#ifdef FRONTEND
		pg_log_error("page %d of the log segment \"%s\" has invalid ud_continue_chunk %06X.%010zX",
					 nr, seg->path, logno, offset);
		return;
#else
		ereport(ERROR,
				(errmsg("page %d of the log segment \"%s\" has invalid ud_continue_chunk %06X.%010zX",
						nr, seg->path, logno, offset)));
#endif
	}

	/*
	 * The page header size must eventually be subtracted from
	 * chunk_bytes_left because it's included in the chunk size.  However,
	 * since chunk_bytes_left is unsigned, we do not subtract anything from it
	 * if it's still zero.  This can happen if we're still reading the chunk
	 * header or the type-specific header. (The underflow should not be a
	 * problem because the chunk size will eventually be added, but it seems
	 * ugly and it makes debugging less convenient.)
	 */
	if (s->chunk_bytes_left > 0)
	{
		/* Chunk should not end within page header. */
		Assert(s->chunk_bytes_left >= SizeOfUndoPageHeaderData);

		s->chunk_bytes_left -= SizeOfUndoPageHeaderData;
		s->chunk_bytes_to_skip = 0;
	}
	/* Processing the chunk header? */
	else if (s->chunk_hdr_bytes_left > 0)
		s->chunk_bytes_to_skip = SizeOfUndoPageHeaderData;

	page_offset = SizeOfUndoPageHeaderData;

	/* Process the page data. */
	while (page_offset < page_usage)
	{
		int			done,
					read_now;

		/*
		 * At any moment we're reading either the chunk or chunk header, but
		 * never both.
		 */
		Assert(!(s->chunk_hdr_bytes_left > 0 && s->chunk_bytes_left > 0));

		if (s->chunk_hdr_bytes_left > 0)
		{
			/*
			 * Retrieve the remaining part of the header that fits on the
			 * current page.
			 */
			done = SizeOfUndoRecordSetChunkHeader - s->chunk_hdr_bytes_left;
			read_now = Min(s->chunk_hdr_bytes_left,
						   page_usage - page_offset);
			memcpy((char *) &s->chunk_hdr + done, page + page_offset,
				   read_now);
			s->chunk_hdr_bytes_left -= read_now;
			page_offset += read_now;

			/*
			 * If we have got the whole header, get the chunk size, otherwise
			 * continue reading the header.
			 */
			if (s->chunk_hdr_bytes_left == 0)
			{
				UndoLogNumber logno = UndoRecPtrGetLogNo(s->current_chunk);
				UndoLogOffset offset = UndoRecPtrGetOffset(s->current_chunk);
				bool		invalid_size = false;

				/* Should not be reading chunk w/o knowing its location. */
				Assert(s->current_chunk != InvalidUndoRecPtr);

				/*
				 * Zero size can mean that the chunk is still open. Compute
				 * the size if we have an insert position.
				 */
				if (s->chunk_hdr.size == 0)
				{
					UndoLogOffset seg_offset PG_USED_FOR_ASSERTS_ONLY;

					seg_offset = offset - offset % UndoLogSegmentSize;

					Assert(logno == seg->logno && seg_offset == seg->offset);
					if (seg->insert > 0)
					{
						Assert(seg->insert > offset);

						s->chunk_hdr.size = seg->insert - offset;
					}
				}

				if (s->chunk_hdr.size < SizeOfUndoRecordSetChunkHeader ||
				/* TODO Check the "usable bytes" instead. */
					s->chunk_hdr.size > UndoLogMaxSize)
					invalid_size = true;

				if (invalid_size)
				{
#ifdef FRONTEND
					pg_log_error("chunk starting at %06X.%010zX has invalid size %zu",
								 logno, offset, s->chunk_hdr.size);
					return;
#else
					ereport(ERROR,
							(errmsg("chunk starting at %06X.%010zX has invalid size %zu",
									logno, offset, s->chunk_hdr.size)));
#endif
				}

				/*
				 * Invalid previous_chunk indicates that this is the first
				 * chunk of the undo record set. Read the type specific header
				 * if we can recognize it.
				 */
				if (s->chunk_hdr.previous_chunk == InvalidUndoRecPtr)
				{
					if (s->chunk_hdr.type == URST_TRANSACTION)
					{
						s->type_hdr_size = get_urs_type_header_size(s->chunk_hdr.type);
						s->type_hdr_bytes_left = s->type_hdr_size;
						s->urs_type = s->chunk_hdr.type;
						continue;
					}
					else if (s->chunk_hdr.type != URST_INVALID)
					{
						UndoLogNumber logno = UndoRecPtrGetLogNo(s->current_chunk);
						UndoLogOffset offset = UndoRecPtrGetOffset(s->current_chunk);

#ifdef FRONTEND
						pg_log_error("chunk starting at %06X.%010zX has invalid type header %d",
									 logno, offset, s->chunk_hdr.type);
						return;
#else
						ereport(ERROR,
								(errmsg("chunk starting at %06X.%010zX has invalid size %zu",
										logno, offset, s->chunk_hdr.size)));
#endif
					}
				}

				if (!records)
					add_chunk_info(s, URST_INVALID);

				s->chunk_bytes_left = s->chunk_hdr.size -
					SizeOfUndoRecordSetChunkHeader;

				/*
				 * Account for the page header if we could not do so earlier.
				 */
				if (s->chunk_bytes_to_skip > 0)
				{
					s->chunk_bytes_left -= s->chunk_bytes_to_skip;
					s->chunk_bytes_to_skip = 0;
				}
			}
			else
				continue;
		}
		else if (s->type_hdr_bytes_left > 0)
		{
			done = s->type_hdr_size - s->type_hdr_bytes_left;
			read_now = Min(s->type_hdr_bytes_left, page_usage - page_offset);
			memcpy((char *) &s->type_hdr + done, page + page_offset,
				   read_now);
			s->type_hdr_bytes_left -= read_now;
			page_offset += read_now;

			/* Have the whole type header? */
			if (s->type_hdr_bytes_left == 0)
			{
				if (!records)
					add_chunk_info(s, s->urs_type);

				s->chunk_bytes_left = s->chunk_hdr.size -
					SizeOfUndoRecordSetChunkHeader - s->type_hdr_size;

				/*
				 * Account for the page header if we could not do so earlier.
				 */
				if (s->chunk_bytes_to_skip > 0)
				{
					s->chunk_bytes_left -= s->chunk_bytes_to_skip;
					s->chunk_bytes_to_skip = 0;
				}
			}
			else
				continue;
		}

		/* Process the current chunk. */
		if (s->chunk_bytes_left > 0)
		{
			int			read_now;

			read_now = Min(s->chunk_bytes_left, page_usage - page_offset);

			/* Pass the data to the record processing code. */
			if (records && s->chunk_hdr.type == URST_TRANSACTION)
			{
				UndoRecPtr	data_start;

				data_start = MakeUndoRecPtr(seg->logno, seg->offset +
											nr * BLCKSZ + page_offset);

				process_records(s, page + page_offset, read_now, data_start);
			}

			s->chunk_bytes_left -= read_now;
			page_offset += read_now;
		}

		/*
		 * If done with the current chunk, prepare to read the next one.
		 */
		if (s->chunk_bytes_left == 0)
		{
			if (s->current_chunk != InvalidUndoRecPtr)
			{
				/* Process the remaining records of the chunk. */
				process_records(s, NULL, 0, InvalidUndoRecPtr);

				s->chunk_hdr_bytes_left = SizeOfUndoRecordSetChunkHeader;
				/* The following chunk is becoming the current one. */
				s->current_chunk = MakeUndoRecPtr(seg->logno, seg->offset +
												  nr * BLCKSZ + page_offset);

				/*
				 * Save the offset of the first chunk start, to check the
				 * value stored in the header.
				 */
				if (first_chunk == 0)
					first_chunk = page_offset;
			}
			else
			{
				/*
				 * Haven't started chunk processing yet. Advance to the first
				 * record or the first chunk if it's on the page.
				 */
				if (pghdr.ud_first_chunk == 0)
				{
					/* Nothing to do on this page. */
					return;
				}
				page_offset = pghdr.ud_first_chunk;


				/* Get ready to process the chunk. */
				s->current_chunk = MakeUndoRecPtr(seg->logno, seg->offset +
												  nr * BLCKSZ + page_offset);
				s->chunk_hdr_bytes_left = SizeOfUndoRecordSetChunkHeader;

				Assert(first_chunk == 0);
				first_chunk = page_offset;
			}
		}
	}

	/* Check ud_first_chunk. */
	if (pghdr.ud_first_chunk != first_chunk)
	{
		/*
		 * current_chunk is where the next chunk should start, but that chunk
		 * might not exist yet. In such a case, ud_first_chunk is still zero
		 * and should not be checked.
		 */
		if (UndoRecPtrGetPageOffset(s->current_chunk) != page_offset)
		{
#ifdef FRONTEND
			pg_log_error("page %d of the log segment \"%s\" has invalid ud_first_chunk: %d",
						 nr, seg->path, pghdr.ud_first_chunk);
			return;
#else
			ereport(ERROR,
					(errmsg("page %d of the log segment \"%s\" has invalid ud_first_chunk: %d",
							nr, seg->path, pghdr.ud_first_chunk)));
#endif
		}
#ifdef USE_ASSERT_CHECKING
		else
			Assert(pghdr.ud_first_chunk == 0);
#endif							/* USE_ASSERT_CHECKING */
	}

	if (page_usage < BLCKSZ)
		s->gap = true;
}

/*
 * Add chunk details to the s->page_chunks array.
 */
static void
add_chunk_info(UndoLogParserState * s, UndoRecordSetType urs_type)
{
	UndoPageItem *item;
	UndoLogChunkInfo *chunk;

	enlarge_item_array(s, s->nitems + 1);

	item = &s->items[s->nitems++];
	item->location = s->current_chunk;
	chunk = &item->u.chunk;
	chunk->hdr = s->chunk_hdr;
	chunk->type = urs_type;
	if (urs_type != URST_INVALID)
		chunk->type_header = s->type_hdr;
}

/* Make sure that 'size' items can fit into the 'items' array. */
static void
enlarge_item_array(UndoLogParserState * s, int size)
{
	if (size <= s->nitems_max)
		return;

	if (s->nitems == 0)
	{
		s->nitems_max = MIN_PAGE_ITEMS;
		s->items = (UndoPageItem *)
			palloc0(s->nitems_max * sizeof(UndoPageItem));
		s->nitems = 0;
	}
	else
	{
		int			i;

		s->nitems_max *= 2;
		s->items = (UndoPageItem *)
			repalloc(s->items, s->nitems_max * sizeof(UndoPageItem));
		for (i = s->nitems; i < s->nitems_max; i++)
			memset(&s->items[i], 0, sizeof(UndoPageItem));
	}
}

/*
 * Free memory allocated for the data of the previous page and get ready for
 * the next one.
 */
static void
reset_storage(UndoLogParserState * state, bool records)
{
	if (records)
	{
		int			i;

		for (i = 0; i < state->nitems_max; i++)
		{
			UndoNode   *node = &state->items[i].u.record;

			if (node->data)
			{
				pfree(node->data);
				node->data = NULL;
			}
		}
	}

	/* The individual chunks currently require no allocation. */
	state->nitems = 0;
}

/* Buffer to parse undo records. */
static Size rec_buf_usage = 0;
static Size rec_buf_off = 0;	/* The next record to process. */

/*
 * Add data to a "record buffer" and process as much as we can. data_start
 * tells where in the log the data starts.
 *
 * Pass NULL to only process the remaining data.
 */
static void
process_records(UndoLogParserState * s, char *data, int size,
				UndoRecPtr data_start)
{
	static char *rec_buffer = NULL;
	static Size rec_buf_size = 0;
	static UndoRecPtr current_rec_start = InvalidUndoRecPtr;
	int			length_size = sizeof(((UndoNode *) NULL)->length);
	int			rmid_size = sizeof(((UndoNode *) NULL)->rmid);
	int			type_size = sizeof(((UndoNode *) NULL)->type);

	/* Enlarge the buffer if needed. */
	if (data && (rec_buf_usage + size) > rec_buf_size)
	{
		rec_buf_size = rec_buf_usage + size;

		if (rec_buffer == NULL)
#ifndef	FRONTEND

			/*
			 * TopMemoryContext is needed in the backend because it calls the
			 * function from within a SRF.
			 */
			rec_buffer = (char *) MemoryContextAlloc(TopMemoryContext,
													 rec_buf_size);
#else
			rec_buffer = (char *) palloc(rec_buf_size);
#endif
		else
			rec_buffer = (char *) repalloc(rec_buffer, rec_buf_size);
	}

	/*
	 * Update the current record pointer. This should happen when called the
	 * first time for a chunk.
	 */
	if (current_rec_start == InvalidUndoRecPtr)
		current_rec_start = data_start;

	/* Append the data to the buffer. */
	if (data)
	{
		memcpy(rec_buffer + rec_buf_usage, data, size);
		rec_buf_usage += size;
	}

	while ((rec_buf_usage - rec_buf_off) >= length_size)
	{
		Size		rec_len;
		Size		rec_off_tmp = rec_buf_off;
		uint8		rmid,
					rec_type;
		UndoPageItem *item;
		UndoNode   *node;

		enlarge_item_array(s, s->nitems + 1);
		item = &s->items[s->nitems++];
		node = &item->u.record;

		/* Read the record length info. */
		memcpy(&rec_len, rec_buffer + rec_off_tmp, length_size);
		rec_off_tmp += length_size;

		/* Read rmid if it's there. */
		if ((rec_buf_usage - rec_off_tmp) < rmid_size)
			break;
		memcpy(&rmid, rec_buffer + rec_off_tmp, rmid_size);
		rec_off_tmp += rmid_size;

		/* Read record type if it's there. */
		if ((rec_buf_usage - rec_off_tmp) < type_size)
			break;
		memcpy(&rec_type, rec_buffer + rec_off_tmp, type_size);
		rec_off_tmp += type_size;

		/* Process the record if fits in the buffer. */
		if ((rec_buf_usage - rec_buf_off) < rec_len)
			break;

		/* Store the node. */
		item->location = current_rec_start;
		node->rmid = rmid;
		node->type = rec_type;
		node->length = rec_len;

		/*
		 * Copy the data because the buffer contents will be moved towards the
		 * buffer start, see below.
		 */
		if (rec_len > 0)
		{
			char	   *data = rec_buffer + rec_off_tmp;

			node->data = (char *) palloc(rec_len);
			memcpy(node->data, data, rec_len);
		}
		else
			node->data = NULL;

		rec_buf_off += rec_len;
		/* The record can cross page boundary. */
		current_rec_start = UndoRecPtrPlusUsableBytes(current_rec_start,
													  rec_len);
	}

	if (data == NULL)
	{
		/* Get ready to accept new record start pointer. */
		current_rec_start = InvalidUndoRecPtr;
		rec_buf_usage = 0;
		rec_buf_off = 0;
	}

	/* Move the unprocessed data to the beginning of the buffer. */
	if (data && rec_buf_off > 0)
	{
		memmove(rec_buffer, rec_buffer + rec_buf_off,
				rec_buf_usage - rec_buf_off);
		rec_buf_usage -= rec_buf_off;
		rec_buf_off = 0;
	}
}
