#include "postgres.h"

#include "storage/streaming_read.h"
#include "utils/rel.h"

/*
 * Element type for PgStreamingRead's circular array of block ranges.
 *
 * For hits, need_to_complete is false and there is just one block per
 * range, already pinned and ready for use.
 *
 * For misses, need_to_complete is true and buffers[] holds a range of
 * blocks that are contiguous in storage (though the buffers may not be
 * contiguous in memory), so we can complete them with a single call to
 * CompleteReadBuffers().
 */
typedef struct PgStreamingReadRange
{
	bool		advice_issued;
	bool		need_complete;
	BlockNumber blocknum;
	int			nblocks;
	int			per_buffer_data_index[MAX_BUFFERS_PER_TRANSFER];
	Buffer		buffers[MAX_BUFFERS_PER_TRANSFER];
} PgStreamingReadRange;

struct PgStreamingRead
{
	int			max_ios;
	int			ios_in_progress;
	int			ios_in_progress_trigger;
	int			max_pinned_buffers;
	int			pinned_buffers;
	int			pinned_buffers_trigger;
	int			next_tail_buffer;
	int			ramp_up_pin_limit;
	int			ramp_up_pin_stall;
	bool		finished;
	void	   *pgsr_private;
	PgStreamingReadBufferCB callback;
	BufferAccessStrategy strategy;
	BufferManagerRelation bmr;
	ForkNumber	forknum;

	bool		advice_enabled;

	/* Next expected block, for detecting sequential access. */
	BlockNumber seq_blocknum;

	/* Space for optional per-buffer private data. */
	size_t		per_buffer_data_size;
	void	   *per_buffer_data;
	int			per_buffer_data_next;

	/* Circular buffer of ranges. */
	int			size;
	int			head;
	int			tail;
	PgStreamingReadRange ranges[FLEXIBLE_ARRAY_MEMBER];
};

static PgStreamingRead *
pg_streaming_read_buffer_alloc_internal(int flags,
										void *pgsr_private,
										size_t per_buffer_data_size,
										BufferAccessStrategy strategy)
{
	PgStreamingRead *pgsr;
	int			size;
	int			max_ios;
	uint32		max_pinned_buffers;


	/*
	 * Decide how many assumed I/Os we will allow to run concurrently.  That
	 * is, advice to the kernel to tell it that we will soon read.  This
	 * number also affects how far we look ahead for opportunities to start
	 * more I/Os.
	 */
	if (flags & PGSR_FLAG_MAINTENANCE)
		max_ios = maintenance_io_concurrency;
	else
		max_ios = effective_io_concurrency;

	/*
	 * The desired level of I/O concurrency controls how far ahead we are
	 * willing to look ahead.  We also clamp it to at least
	 * MAX_BUFFER_PER_TRANFER so that we can have a chance to build up a full
	 * sized read, even when max_ios is zero.
	 */
	max_pinned_buffers = Max(max_ios * 4, MAX_BUFFERS_PER_TRANSFER);

	/*
	 * The *_io_concurrency GUCs, we might have 0.  We want to allow at least
	 * one, to keep our gating logic simple.
	 */
	max_ios = Max(max_ios, 1);

	/*
	 * Don't allow this backend to pin too many buffers.  For now we'll apply
	 * the limit for the shared buffer pool and the local buffer pool, without
	 * worrying which it is.
	 */
	LimitAdditionalPins(&max_pinned_buffers);
	LimitAdditionalLocalPins(&max_pinned_buffers);
	Assert(max_pinned_buffers > 0);

	/*
	 * pgsr->ranges is a circular buffer.  When it is empty, head == tail.
	 * When it is full, there is an empty element between head and tail.  Head
	 * can also be empty (nblocks == 0), therefore we need two extra elements
	 * for non-occupied ranges, on top of max_pinned_buffers to allow for the
	 * maxmimum possible number of occupied ranges of the smallest possible
	 * size of one.
	 */
	size = max_pinned_buffers + 2;

	pgsr = (PgStreamingRead *)
		palloc0(offsetof(PgStreamingRead, ranges) +
				sizeof(pgsr->ranges[0]) * size);

	pgsr->max_ios = max_ios;
	pgsr->per_buffer_data_size = per_buffer_data_size;
	pgsr->max_pinned_buffers = max_pinned_buffers;
	pgsr->pgsr_private = pgsr_private;
	pgsr->strategy = strategy;
	pgsr->size = size;

#ifdef USE_PREFETCH

	/*
	 * This system supports prefetching advice.  As long as direct I/O isn't
	 * enabled, and the caller hasn't promised sequential access, we can use
	 * it.
	 */
	if ((io_direct_flags & IO_DIRECT_DATA) == 0 &&
		(flags & PGSR_FLAG_SEQUENTIAL) == 0)
		pgsr->advice_enabled = true;
#endif

	/*
	 * We start off building small ranges, but double that quickly, for the
	 * benefit of users that don't know how far ahead they'll read.  This can
	 * be disabled by users that already know they'll read all the way.
	 */
	if (flags & PGSR_FLAG_FULL)
		pgsr->ramp_up_pin_limit = INT_MAX;
	else
		pgsr->ramp_up_pin_limit = 1;

	/*
	 * We want to avoid creating ranges that are smaller than they could be
	 * just because we hit max_pinned_buffers.  We only look ahead when the
	 * number of pinned buffers falls below this trigger number, or put
	 * another way, we stop looking ahead when we wouldn't be able to build a
	 * "full sized" range.
	 */
	pgsr->pinned_buffers_trigger =
		Max(1, (int) max_pinned_buffers - MAX_BUFFERS_PER_TRANSFER);

	/* Space the callback to store extra data along with each block. */
	if (per_buffer_data_size)
		pgsr->per_buffer_data = palloc(per_buffer_data_size * max_pinned_buffers);

	return pgsr;
}

/*
 * Create a new streaming read object that can be used to perform the
 * equivalent of a series of ReadBuffer() calls for one fork of one relation.
 * Internally, it generates larger vectored reads where possible by looking
 * ahead.
 */
PgStreamingRead *
pg_streaming_read_buffer_alloc(int flags,
							   void *pgsr_private,
							   size_t per_buffer_data_size,
							   BufferAccessStrategy strategy,
							   BufferManagerRelation bmr,
							   ForkNumber forknum,
							   PgStreamingReadBufferCB next_block_cb)
{
	PgStreamingRead *result;

	result = pg_streaming_read_buffer_alloc_internal(flags,
													 pgsr_private,
													 per_buffer_data_size,
													 strategy);
	result->callback = next_block_cb;
	result->bmr = bmr;
	result->forknum = forknum;

	return result;
}

/*
 * Start building a new range.  This is called after the previous one
 * reached maximum size, or the callback's next block can't be merged with it.
 *
 * Since the previous head range has now reached its full potential size, this
 * is also a good time to issue 'prefetch' advice, because we know that'll
 * soon be reading.  In future, we could start an actual I/O here.
 */
static PgStreamingReadRange *
pg_streaming_read_new_range(PgStreamingRead *pgsr)
{
	PgStreamingReadRange *head_range;

	head_range = &pgsr->ranges[pgsr->head];
	Assert(head_range->nblocks > 0);

	/*
	 * If a call to CompleteReadBuffers() will be needed, and we can issue
	 * advice to the kernel to get the read started.  We suppress it if the
	 * access pattern appears to be completely sequential, though, because on
	 * some systems that interfers with the kernel's own sequential read ahead
	 * heurstics and hurts performance.
	 */
	if (pgsr->advice_enabled)
	{
		BlockNumber blocknum = head_range->blocknum;
		int			nblocks = head_range->nblocks;

		if (head_range->need_complete && blocknum != pgsr->seq_blocknum)
		{
			SMgrRelation smgr =
				pgsr->bmr.smgr ? pgsr->bmr.smgr :
				RelationGetSmgr(pgsr->bmr.rel);

			Assert(!head_range->advice_issued);

			smgrprefetch(smgr, pgsr->forknum, blocknum, nblocks);

			/*
			 * Count this as an I/O that is concurrently in progress, though
			 * we don't really know if the kernel generates a physical I/O.
			 */
			head_range->advice_issued = true;
			pgsr->ios_in_progress++;
		}

		/* Remember the block after this range, for sequence detection. */
		pgsr->seq_blocknum = blocknum + nblocks;
	}

	/* Create a new head range.  There must be space. */
	Assert(pgsr->size > pgsr->max_pinned_buffers);
	Assert((pgsr->head + 1) % pgsr->size != pgsr->tail);
	if (++pgsr->head == pgsr->size)
		pgsr->head = 0;
	head_range = &pgsr->ranges[pgsr->head];
	head_range->nblocks = 0;

	return head_range;
}

static void
pg_streaming_read_look_ahead(PgStreamingRead *pgsr)
{
	/*
	 * If we're still ramping up, we may have to stall to wait for buffers to
	 * be consumed first before we do any more prefetching.
	 */
	if (pgsr->ramp_up_pin_stall > 0)
	{
		Assert(pgsr->pinned_buffers > 0);
		return;
	}

	/*
	 * If we're finished or can't start more I/O, then don't look ahead.
	 */
	if (pgsr->finished || pgsr->ios_in_progress == pgsr->max_ios)
		return;

	/*
	 * We'll also wait until the number of pinned buffers falls below our
	 * trigger level, so that we have the chance to create a full range.
	 */
	if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
		return;

	do
	{
		BufferManagerRelation bmr;
		ForkNumber	forknum;
		BlockNumber blocknum;
		Buffer		buffer;
		bool		found;
		bool		need_complete;
		PgStreamingReadRange *head_range;
		void	   *per_buffer_data;

		/* Do we have a full-sized range? */
		head_range = &pgsr->ranges[pgsr->head];
		if (head_range->nblocks == lengthof(head_range->buffers))
		{
			Assert(head_range->need_complete);
			head_range = pg_streaming_read_new_range(pgsr);

			/*
			 * Give up now if I/O is saturated, or we wouldn't be able form
			 * another full range after this due to the pin limit.
			 */
			if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger ||
				pgsr->ios_in_progress == pgsr->max_ios)
				break;
		}

		per_buffer_data = (char *) pgsr->per_buffer_data +
			pgsr->per_buffer_data_size * pgsr->per_buffer_data_next;

		/* Find out which block the callback wants to read next. */
		blocknum = pgsr->callback(pgsr, pgsr->pgsr_private, per_buffer_data);
		if (blocknum == InvalidBlockNumber)
		{
			pgsr->finished = true;
			break;
		}
		bmr = pgsr->bmr;
		forknum = pgsr->forknum;

		Assert(pgsr->pinned_buffers < pgsr->max_pinned_buffers);

		buffer = PrepareReadBuffer(bmr,
								   forknum,
								   blocknum,
								   pgsr->strategy,
								   &found);
		pgsr->pinned_buffers++;

		need_complete = !found;

		/* Is there a head range that we can't extend? */
		head_range = &pgsr->ranges[pgsr->head];
		if (head_range->nblocks > 0 &&
			(!need_complete ||
			 !head_range->need_complete ||
			 head_range->blocknum + head_range->nblocks != blocknum))
		{
			/* Yes, time to start building a new one. */
			head_range = pg_streaming_read_new_range(pgsr);
			Assert(head_range->nblocks == 0);
		}

		if (head_range->nblocks == 0)
		{
			/* Initialize a new range beginning at this block. */
			head_range->blocknum = blocknum;
			head_range->need_complete = need_complete;
			head_range->advice_issued = false;
		}
		else
		{
			/* We can extend an existing range by one block. */
			Assert(head_range->blocknum + head_range->nblocks == blocknum);
			Assert(head_range->need_complete);
		}

		head_range->per_buffer_data_index[head_range->nblocks] = pgsr->per_buffer_data_next++;
		head_range->buffers[head_range->nblocks] = buffer;
		head_range->nblocks++;

		if (pgsr->per_buffer_data_next == pgsr->max_pinned_buffers)
			pgsr->per_buffer_data_next = 0;

	} while (pgsr->pinned_buffers < pgsr->max_pinned_buffers &&
			 pgsr->ios_in_progress < pgsr->max_ios &&
			 pgsr->pinned_buffers < pgsr->ramp_up_pin_limit);

	/* If we've hit the ramp-up limit, insert a stall. */
	if (pgsr->pinned_buffers >= pgsr->ramp_up_pin_limit)
	{
		/* Can't get here if an earlier stall hasn't finished. */
		Assert(pgsr->ramp_up_pin_stall == 0);
		/* Don't do any more prefetching until these buffers are consumed. */
		pgsr->ramp_up_pin_stall = pgsr->ramp_up_pin_limit;
		/* Double it.  It will soon be out of the way. */
		pgsr->ramp_up_pin_limit *= 2;
	}

	if (pgsr->ranges[pgsr->head].nblocks > 0)
		pg_streaming_read_new_range(pgsr);
}

Buffer
pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_buffer_data)
{
	pg_streaming_read_look_ahead(pgsr);

	/* See if we have one buffer to return. */
	while (pgsr->tail != pgsr->head)
	{
		PgStreamingReadRange *tail_range;

		tail_range = &pgsr->ranges[pgsr->tail];

		/*
		 * Do we need to perform an I/O before returning the buffers from this
		 * range?
		 */
		if (tail_range->need_complete)
		{
			CompleteReadBuffers(pgsr->bmr,
								tail_range->buffers,
								pgsr->forknum,
								tail_range->blocknum,
								tail_range->nblocks,
								false,
								pgsr->strategy);
			tail_range->need_complete = false;

			/*
			 * We don't really know if the kernel generated an physical I/O
			 * when we issued advice, let alone when it finished, but it has
			 * certainly finished after a read call returns.
			 */
			if (tail_range->advice_issued)
				pgsr->ios_in_progress--;
		}

		/* Are there more buffers available in this range? */
		if (pgsr->next_tail_buffer < tail_range->nblocks)
		{
			int			buffer_index;
			Buffer		buffer;

			buffer_index = pgsr->next_tail_buffer++;
			buffer = tail_range->buffers[buffer_index];

			Assert(BufferIsValid(buffer));

			/* We are giving away ownership of this pinned buffer. */
			Assert(pgsr->pinned_buffers > 0);
			pgsr->pinned_buffers--;

			if (pgsr->ramp_up_pin_stall > 0)
				pgsr->ramp_up_pin_stall--;

			if (per_buffer_data)
				*per_buffer_data = (char *) pgsr->per_buffer_data +
					tail_range->per_buffer_data_index[buffer_index] *
					pgsr->per_buffer_data_size;

			return buffer;
		}

		/* Advance tail to next range, if there is one. */
		if (++pgsr->tail == pgsr->size)
			pgsr->tail = 0;
		pgsr->next_tail_buffer = 0;
	}

	Assert(pgsr->pinned_buffers == 0);

	return InvalidBuffer;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	Buffer		buffer;

	/* Stop looking ahead, and unpin anything that wasn't consumed. */
	pgsr->finished = true;
	while ((buffer = pg_streaming_read_buffer_get_next(pgsr, NULL)) != InvalidBuffer)
		ReleaseBuffer(buffer);

	if (pgsr->per_buffer_data)
		pfree(pgsr->per_buffer_data);
	pfree(pgsr);
}
