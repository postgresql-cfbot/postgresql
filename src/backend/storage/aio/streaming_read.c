#include "postgres.h"

#include "storage/streaming_read.h"
#include "utils/rel.h"

/*
 * Element type for PgStreamingRead's circular array of clusters of buffers.
 *
 * For hits and RBM_WILL_ZERO, need_to_complete is false, we have just one
 * buffer in each cluster, already pinned and ready for use.
 *
 * For misses that require a physical read, need_to_complete is true, and
 * buffers[] holds a group of of neighboring blocks, so we can complete them
 * with a single call to CompleteReadBuffers().  We can also issue a single
 * prefetch for it as soon as it has grown to its largest possible size, if
 * our random access heuristics determine that is a good idea.
 */
typedef struct PgStreamingReadCluster
{
	bool		advice_issued;
	bool		need_complete;

	BufferManagerRelation bmr;
	ForkNumber	forknum;
	BlockNumber blocknum;
	int			nblocks;

	int			per_io_data_index[MAX_BUFFERS_PER_TRANSFER];
	bool		need_advice[MAX_BUFFERS_PER_TRANSFER];
	Buffer		buffers[MAX_BUFFERS_PER_TRANSFER];
} PgStreamingReadCluster;

struct PgStreamingRead
{
	int			max_ios;
	int			ios_in_progress;
	int			ios_in_progress_trigger;
	int			max_pinned_buffers;
	int			pinned_buffers;
	int			pinned_buffers_trigger;
	int			next_tail_buffer;
	bool		finished;
	uintptr_t	pgsr_private;
	PgStreamingReadBufferDetermineNextCB next_cb;
	BufferAccessStrategy strategy;

	/* Next expected prefetch, for sequential prefetch avoidance. */
	BufferManagerRelation seq_bmr;
	ForkNumber	seq_forknum;
	BlockNumber seq_blocknum;

	/* Space for optional per-I/O private data. */
	size_t		per_io_data_size;
	void	   *per_io_data;
	int			per_io_data_next;

	/* Circular buffer of clusters. */
	int			size;
	int			head;
	int			tail;
	PgStreamingReadCluster clusters[FLEXIBLE_ARRAY_MEMBER];
};

PgStreamingRead *
pg_streaming_read_buffer_alloc(int max_ios,
							   size_t per_io_data_size,
							   uintptr_t pgsr_private,
							   BufferAccessStrategy strategy,
							   PgStreamingReadBufferDetermineNextCB determine_next_cb)
{
	PgStreamingRead *pgsr;
	int			size;
	int			max_pinned_buffers;

	Assert(max_ios > 0);

	/*
	 * We allow twice as many buffers to be pinned as I/Os.  This allows us to
	 * look further ahead for blocks that need to be read in.
	 */
	max_pinned_buffers = max_ios * 2;

	/* Don't allow this backend to pin too many buffers. */
	LimitAdditionalPins((uint32 *) &max_pinned_buffers);
	max_pinned_buffers = Max(2, max_pinned_buffers);
	max_ios = max_pinned_buffers / 2;
	Assert(max_ios > 0);
	Assert(max_pinned_buffers > 0);
	Assert(max_pinned_buffers > max_ios);

	/*
	 * pgsr->clusters is a circular buffer.  When it is empty, head == tail.
	 * When it is full, there is an empty element between head and tail.  Head
	 * can also be empty (nblocks == 0).  So we need two extra elements.
	 */
	size = max_pinned_buffers + 2;

	pgsr = (PgStreamingRead *)
		palloc0(offsetof(PgStreamingRead, clusters) +
				sizeof(pgsr->clusters[0]) * size);

	pgsr->per_io_data_size = per_io_data_size;
	pgsr->max_ios = max_ios;
	pgsr->max_pinned_buffers = max_pinned_buffers;
	pgsr->pgsr_private = pgsr_private;
	pgsr->strategy = strategy;
	pgsr->next_cb = determine_next_cb;
	pgsr->size = size;

	/*
	 * We look ahead when the number of pinned buffers falls below this
	 * number.  This encourages the formation of large vectored reads.
	 */
	pgsr->pinned_buffers_trigger =
		Max(max_ios, max_pinned_buffers - MAX_BUFFERS_PER_TRANSFER);

	/* Space the callback to store extra data along with each block. */
	if (per_io_data_size)
		pgsr->per_io_data = palloc(per_io_data_size * max_pinned_buffers);

	return pgsr;
}

/*
 * Issue WILLNEED advice for the head cluster, and allocate a new head
 * cluster.
 *
 * We don't have true asynchronous I/O to actually submit, but this is
 * equivalent because it might start I/O on systems that understand WILLNEED
 * advice.  We count it as an I/O in progress.
 */
static PgStreamingReadCluster *
pg_streaming_read_submit(PgStreamingRead *pgsr)
{
	PgStreamingReadCluster *head_cluster;

	head_cluster = &pgsr->clusters[pgsr->head];
	Assert(head_cluster->nblocks > 0);

#ifdef USE_PREFETCH

	/*
	 * Don't bother with advice if there will be no call to
	 * CompleteReadBuffers() or direct I/O is enabled.
	 */
	if (head_cluster->need_complete &&
		(io_direct_flags & IO_DIRECT_DATA) == 0)
	{
		/*
		 * Purely sequential advice is known to hurt performance on some
		 * systems, so only issue it if this looks random.
		 */
		if (head_cluster->bmr.smgr != pgsr->seq_bmr.smgr ||
			head_cluster->bmr.rel != pgsr->seq_bmr.rel ||
			head_cluster->forknum != pgsr->seq_forknum ||
			head_cluster->blocknum != pgsr->seq_blocknum)
		{
			SMgrRelation smgr =
				head_cluster->bmr.smgr ? head_cluster->bmr.smgr
				: RelationGetSmgr(head_cluster->bmr.rel);

			Assert(!head_cluster->advice_issued);

			for (int i = 0; i < head_cluster->nblocks; i++)
			{
				if (head_cluster->need_advice[i])
				{
					BlockNumber first_blocknum = head_cluster->blocknum + i;
					int			nblocks = 1;

					/*
					 * How many adjacent blocks can we merge with to reduce
					 * system calls?  Usually this is all of them, unless
					 * there are overlapping reads and our timing is unlucky.
					 */
					while ((i + 1) < head_cluster->nblocks &&
						   head_cluster->need_advice[i + 1])
					{
						nblocks++;
						i++;
					}

					smgrprefetch(smgr,
								 head_cluster->forknum,
								 first_blocknum,
								 nblocks);
				}

			}

			/*
			 * Count this as an I/O that is concurrently in progress.  We
			 * might have called smgrprefetch() more than once, if some of the
			 * buffers in the range were already in buffer pool but not valid
			 * yet, because of a concurrent read, but for now we choose to
			 * track this as one I/O.
			 */
			head_cluster->advice_issued = true;
			pgsr->ios_in_progress++;
		}

		/* Remember the point after this, for the above heuristics. */
		pgsr->seq_bmr = head_cluster->bmr;
		pgsr->seq_forknum = head_cluster->forknum;
		pgsr->seq_blocknum = head_cluster->blocknum + head_cluster->nblocks;
	}
#endif

	/* Create a new head cluster.  There must be space. */
	Assert(pgsr->size > pgsr->max_pinned_buffers);
	Assert((pgsr->head + 1) % pgsr->size != pgsr->tail);
	if (++pgsr->head == pgsr->size)
		pgsr->head = 0;
	head_cluster = &pgsr->clusters[pgsr->head];
	head_cluster->nblocks = 0;

	return head_cluster;
}

void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	/* If we're finished or can't start one more I/O, then no prefetching. */
	if (pgsr->finished || pgsr->ios_in_progress == pgsr->max_ios)
		return;

	/*
	 * We'll also wait until the number of pinned buffers falls below our
	 * trigger level, so that we have the chance to create a large cluster.
	 */
	if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
		return;

	do
	{
		BufferManagerRelation bmr;
		ForkNumber	forknum;
		BlockNumber blocknum;
		ReadBufferMode mode;
		Buffer		buffer;
		bool		found;
		bool		allocated;
		bool		need_complete;
		PgStreamingReadCluster *head_cluster;
		void	   *per_io_data;

		/* Do we have a full-sized cluster? */
		head_cluster = &pgsr->clusters[pgsr->head];
		if (head_cluster->nblocks == lengthof(head_cluster->buffers))
		{
			Assert(head_cluster->need_complete);
			head_cluster = pg_streaming_read_submit(pgsr);

			/*
			 * Give up now if I/O is saturated or we couldn't form another
			 * full cluster after this.
			 */
			if (pgsr->ios_in_progress == pgsr->max_ios ||
				pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
				break;
		}

		per_io_data = (char *) pgsr->per_io_data +
			pgsr->per_io_data_size * pgsr->per_io_data_next;

		/*
		 * Try to find out which block the callback wants to read next.  False
		 * indicates end-of-stream (but the client can restart).
		 */
		if (!pgsr->next_cb(pgsr, pgsr->pgsr_private, per_io_data,
						   &bmr, &forknum, &blocknum, &mode))
		{
			pgsr->finished = true;
			break;
		}

		Assert(mode == RBM_NORMAL || mode == RBM_WILL_ZERO);
		Assert(pgsr->pinned_buffers < pgsr->max_pinned_buffers);

		buffer = PrepareReadBuffer(bmr,
								   forknum,
								   blocknum,
								   pgsr->strategy,
								   &found,
								   &allocated);
		pgsr->pinned_buffers++;

		need_complete = !found && mode != RBM_WILL_ZERO;

		/* Is there a head cluster that we can't extend? */
		head_cluster = &pgsr->clusters[pgsr->head];
		if (head_cluster->nblocks > 0 &&
			(!need_complete ||
			 !head_cluster->need_complete ||
			 head_cluster->bmr.smgr != bmr.smgr ||
			 head_cluster->bmr.rel != bmr.rel ||
			 head_cluster->forknum != forknum ||
			 head_cluster->blocknum + head_cluster->nblocks != blocknum))
		{
			/* Submit it so we can start a new one. */
			head_cluster = pg_streaming_read_submit(pgsr);
			Assert(head_cluster->nblocks == 0);
		}

		if (head_cluster->nblocks == 0)
		{
			/* Initialize the cluster. */
			head_cluster->bmr = bmr;
			head_cluster->forknum = forknum;
			head_cluster->blocknum = blocknum;
			head_cluster->advice_issued = false;
			head_cluster->need_complete = need_complete;
		}
		else
		{
			/* We'll extend an existing cluster by one buffer. */
			Assert(head_cluster->bmr.smgr == bmr.smgr);
			Assert(head_cluster->bmr.rel == bmr.rel);
			Assert(head_cluster->forknum == forknum);
			Assert(head_cluster->blocknum + head_cluster->nblocks == blocknum);
			Assert(head_cluster->need_complete);
		}

		head_cluster->per_io_data_index[head_cluster->nblocks] = pgsr->per_io_data_next++;
		head_cluster->need_advice[head_cluster->nblocks] = allocated;
		head_cluster->buffers[head_cluster->nblocks] = buffer;
		head_cluster->nblocks++;

		if (pgsr->per_io_data_next == pgsr->max_pinned_buffers)
			pgsr->per_io_data_next = 0;

	} while (pgsr->ios_in_progress < pgsr->max_ios &&
			 pgsr->pinned_buffers < pgsr->max_pinned_buffers);

	/*
	 * Initiate as soon as we can if we can't prepare any more reads right
	 * now.  This makes sure we issue the advice as soon as possible, since
	 * any other backend that tries to read the same block won't do that.
	 */
	if (pgsr->clusters[pgsr->head].nblocks > 0)
		pg_streaming_read_submit(pgsr);
}

void
pg_streaming_read_reset(PgStreamingRead *pgsr)
{
	pgsr->finished = false;
}

Buffer
pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_io_data)
{
	pg_streaming_read_prefetch(pgsr);

	/* See if we have one buffer to return. */
	while (pgsr->tail != pgsr->head)
	{
		PgStreamingReadCluster *tail_cluster;

		tail_cluster = &pgsr->clusters[pgsr->tail];

		/*
		 * Do we need to perform an I/O before returning the buffers from this
		 * cluster?
		 */
		if (tail_cluster->need_complete)
		{
			CompleteReadBuffers(tail_cluster->bmr,
								tail_cluster->buffers,
								tail_cluster->forknum,
								tail_cluster->blocknum,
								tail_cluster->nblocks,
								false,
								pgsr->strategy);
			tail_cluster->need_complete = false;

			/* We only counted this I/O as running if we issued advice. */
			if (tail_cluster->advice_issued)
				pgsr->ios_in_progress--;
		}

		/* Are there more buffers available in this cluster? */
		if (pgsr->next_tail_buffer < tail_cluster->nblocks)
		{
			/* We are giving away ownership of this pinned buffer. */
			Assert(pgsr->pinned_buffers > 0);
			pgsr->pinned_buffers--;

			if (per_io_data)
				*per_io_data = (char *) pgsr->per_io_data +
					tail_cluster->per_io_data_index[pgsr->next_tail_buffer] *
					pgsr->per_io_data_size;

			return tail_cluster->buffers[pgsr->next_tail_buffer++];
		}

		/* Advance tail to next cluster, if there is one. */
		if (++pgsr->tail == pgsr->size)
			pgsr->tail = 0;
		pgsr->next_tail_buffer = 0;
	}

	return InvalidBuffer;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	Buffer		buffer;

	/* Stop reading ahead, and unpin anything that wasn't consumed. */
	pgsr->finished = true;
	for (;;)
	{
		buffer = pg_streaming_read_buffer_get_next(pgsr, NULL);
		if (buffer == InvalidBuffer)
			break;
		ReleaseBuffer(buffer);
	}

	if (pgsr->per_io_data)
		pfree(pgsr->per_io_data);
	pfree(pgsr);
}

int
pg_streaming_read_ios(PgStreamingRead *pgsr)
{
	return pgsr->ios_in_progress;
}

int
pg_streaming_read_pins(PgStreamingRead *pgsr)
{
	return pgsr->pinned_buffers;
}
