/*-------------------------------------------------------------------------
 *
 * xlogpipeline.c
 *    WAL replay pipeline implementation
 *
 * This module implements a producer-consumer pipeline for WAL replay.
 * The producer (background worker) reads and decodes WAL records in parallel
 * with the consumer (startup process) that applies them.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/access/transam/xlogpipeline.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xlog.h"
#include "access/xlogpipeline.h"
#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/startup.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/smgr.h"
#include "storage/subsystems.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/timeout.h"


/*
 * Convert values of GUCs measured in megabytes to bytes
 */
#define MBToBytes(mbvar) (mbvar * 1024 * 1024)

/*
 * Waiting for consumer or producer before exiting gracefully.
 */
#define MAX_SHUTDOWN_WAIT_ITERS 1000	/* 1000 * 10ms = 10 seconds */


/* Global shared memory control structure */
WalPipelineShmCtl *WalPipelineShm = NULL;

static void WalPipelineShmemRequest(void *arg);
static void WalPipelineShmemInit(void *arg);

const ShmemCallbacks WalPipelineShmemCallbacks = {
	.request_fn = WalPipelineShmemRequest,
	.init_fn = WalPipelineShmemInit,
};

XLogPrefetcher *xlogprefetcher_pipelined = NULL;

/* Local state for producer */
static dsm_segment *producer_dsm_seg = NULL;
static shm_mq *producer_mq = NULL;
static shm_mq_handle *producer_mq_handle = NULL;

/* Local state for consumer */
static dsm_segment *consumer_dsm_seg = NULL;
static shm_mq *consumer_mq = NULL;
static shm_mq_handle *consumer_mq_handle = NULL;

/*
 * Local buffer containing msg header that will be sent together with the
 * decoded data, to the msg queue
 */
static WalRecordMsgHeader msghdr;

/*
 * Flags set by interrupt handlers for later service in the redo loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t promote_signaled = false;

/* Signal handlers */
static void PipelineBgwSigHupHandler(SIGNAL_ARGS);
static void PipelineProcTriggerHandler(SIGNAL_ARGS);

/* Forward declarations */
static void wal_pipeline_cleanup_callback(int code, Datum arg);
static Size serialize_wal_record(XLogReaderState *xlogreader, shm_mq_iovec *iov);
static DecodedXLogRecord *deserialize_wal_record(const char *buffer, Size len, XLogReaderState *startup_reader);
static void cleanup_producer_resources(void);
static void cleanup_consumer_resources(void);
static void WalPipeline_WaitForConsumerShutdownRequest(void);

/* copied from xlogrecovery.c */
/* Parameters passed down from ReadRecord to the XLogPageRead callback. */
typedef struct XLogPageReadPrivate
{
	int			emode;
	bool		fetching_ckpt;	/* are we fetching a checkpoint record? */
	bool		randAccess;
	TimeLineID	replayTLI;
} XLogPageReadPrivate;


/*
 * Register shared memory for WAL Pipeline
 */
static void
WalPipelineShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "WAL Pipeline Ctl",
					   .size = sizeof(WalPipelineShmCtl),
					   .ptr = (void **) &WalPipelineShm,
		);
}

static void
WalPipelineShmemInit(void *arg)
{
	memset(WalPipelineShm, 0, sizeof(WalPipelineShmCtl));

	SpinLockInit(&WalPipelineShm->mutex);
}

/*
 * Called by Consumer.
 *
 * Initialize and start the WAL pipeline. This will be called by the startup
 * process (consumer) as a request to start the pipeline.
 */
void
WalPipeline_Start(WalPipelineParams *params)
{
	BackgroundWorkerHandle *handle;
	BackgroundWorker	worker;
	shm_toc_estimator 	e;
	WalPipelineParams  *shared_params;
	dsm_segment		   *seg;
	shm_toc			   *toc;
	shm_mq			   *mq;
	Size				queue_size;
	Size				segsize;
	pid_t		 		pid;

	if (wal_pipeline_mq_size_mb <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid wal_pipeline_mq_size_mb")));

	queue_size = MBToBytes(wal_pipeline_mq_size_mb);

	/* Set init flag */
	SpinLockAcquire(&WalPipelineShm->mutex);
	if (WalPipelineShm->initialized)
	{
		SpinLockRelease(&WalPipelineShm->mutex);
		return;
	}
	WalPipelineShm->initialized = true;
	SpinLockRelease(&WalPipelineShm->mutex);

	/*
	 * Estimate how much shared memory we need.
	 *
	 * We need one key to register the location of the WalPipelineParams, and
	 * we need 1 key to track the location of the message queue.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(WalPipelineParams));
	shm_toc_estimate_chunk(&e, queue_size);
	shm_toc_estimate_keys(&e, 2);
	segsize = shm_toc_estimate(&e);

	/* Create the shared memory segment and establish a table of contents. */
	seg = dsm_create(segsize, 0);
	dsm_pin_segment(seg);
	toc = shm_toc_create(PG_WAL_PIPELINE_MAGIC, dsm_segment_address(seg),
						 segsize);

	/* Setup arguments to be passed to the producer */
	shared_params = shm_toc_allocate(toc, sizeof(WalPipelineParams));
	shm_toc_insert(toc, 1, shared_params);
	*shared_params = *params;

	/* Setup the message queue */
	mq = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
	shm_toc_insert(toc, 2, mq);

	/* update shared state */
	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->dsm_seg_handle = dsm_segment_handle(seg);
	WalPipelineShm->consumer_pid = MyProcPid;
	SpinLockRelease(&WalPipelineShm->mutex);

	/* Set up consumer side of the queue */
	consumer_dsm_seg = seg;
	consumer_mq = mq;
	shm_mq_set_receiver(consumer_mq, MyProc);
	consumer_mq_handle = shm_mq_attach(consumer_mq, seg, NULL);

	/* Register background worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "WalPipeline_ProducerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "wal pipeline producer");
	snprintf(worker.bgw_type, BGW_MAXLEN, "wal pipeline producer");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		goto fail;

	if (WaitForBackgroundWorkerStartup(handle, &pid) != BGWH_STARTED)
		goto fail;

	/* Register cleanup callback */
	before_shmem_exit(wal_pipeline_cleanup_callback, (Datum) 0);

	ereport(LOG, (errmsg("[walpipeline] started.")));
	return;

fail:
	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->initialized = false;
	SpinLockRelease(&WalPipelineShm->mutex);

	cleanup_consumer_resources();

	ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					errmsg("could not start pipeline background worker"),
					errhint("More details may be available in the server log.")));
}

/*
 * Request producer shutdown.
 * This is called by the consumer when it no longer needs records.
 */
static void
WalPipeline_RequestShutdown(void)
{
	if (!WalPipelineShm)
		return;

	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->shutdown_requested = true;
	SpinLockRelease(&WalPipelineShm->mutex);
}

/*
 * Consumer Function.
 * Stop the WAL pipeline. This be called be the startup process
 * (consumer). This will only be called  and the time to recovery shutdown.
 * This function will also wait until the pipeline workers
 * are exited.
 */
void
WalPipeline_Stop(void)
{
	int			iters = 0;

	if (!WalPipelineShm || !WalPipelineShm->initialized)
		return;

	/* Ask producer to stop by setting a flag */
	WalPipeline_RequestShutdown();

	/*
	 * Wakeup in case producer is still waiting for a wal stream. Consumer could
	 * have been exited the redo loop early if recovery target is reached,
	 * so there is no point of producer to wait more for the wal stream.
	 */
	WakeupRecovery();

	while (true)
	{
		bool producer_alive;

		SpinLockAcquire(&WalPipelineShm->mutex);
		producer_alive = (WalPipelineShm->producer_pid != 0);
		SpinLockRelease(&WalPipelineShm->mutex);

		if (!producer_alive)
			break;

		if (++iters >= MAX_SHUTDOWN_WAIT_ITERS)
		{
			elog(WARNING, "[walpipeline] consumer: timed out waiting for "
						  "pipeline to stop, continuing anyway");
			return;
		}

		pg_usleep(10000);  /* sleep 10ms */
	}

	cleanup_consumer_resources();

	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->initialized = false;
	SpinLockRelease(&WalPipelineShm->mutex);

	elog(LOG, "[walpipeline] shutdown");
}

/*
 * Producer Function.
 * Main loop for the producer background worker.
 */
void
WalPipeline_ProducerMain(Datum main_arg)
{
	dsm_handle           handle = DatumGetUInt32(main_arg);
	dsm_segment        	*seg;
	shm_toc            	*toc;
	WalPipelineParams  	*params;
	XLogReaderState    	*xlogreader;
	XLogPageReadPrivate *private;
	XLogRecord         	*record;
	TimeLineID           replayTLI = 0;
	bool 				 end_of_wal = false;
	uint64				 records_sent;
	uint64				 records_received;

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 */
	pqsignal(SIGHUP, PipelineBgwSigHupHandler); 	/* reload config file */
	pqsignal(SIGUSR2, PipelineProcTriggerHandler);

	/* Register cleanup callback */
	before_shmem_exit(wal_pipeline_cleanup_callback, (Datum) 0);

	seg = dsm_attach(handle);
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("[walpipeline] producer: could not map dynamic shared memory segment")));

	toc = shm_toc_attach(PG_WAL_PIPELINE_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("[walpipeline] producer: bad magic number in dynamic shared memory segment")));

	/* Lookup params and queue */
	params = shm_toc_lookup(toc, 1, false);
	producer_mq = shm_toc_lookup(toc, 2, false);

	/* Set up producer side of queue */
	producer_dsm_seg = seg;
	shm_mq_set_sender(producer_mq, MyProc);
	producer_mq_handle = shm_mq_attach(producer_mq, seg, NULL);

	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->producer_pid = MyProcPid;
	SpinLockRelease(&WalPipelineShm->mutex);

	/* DSM is now attached, so safe to unblock the signals */
	BackgroundWorkerUnblockSignals();

	/* Set up WAL reading processor */
	private = palloc0(sizeof(XLogPageReadPrivate));
	xlogreader =
		XLogReaderAllocate(wal_segment_size, NULL,
						   XL_ROUTINE(.page_read = &XLogPageRead,
									  .segment_open = NULL,
									  .segment_close = wal_segment_close),
						   private);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));
	xlogreader->system_identifier = GetSystemIdentifier();

	/*
	 * Set the WAL decode buffer size.  This limits how far ahead we can read
	 * in the WAL.
	 */
	XLogReaderSetDecodeBuffer(xlogreader, NULL, wal_decode_buffer_size);

	/* Init some important globals before starting */
	replayTLI = params->ReplayTLI;
	WalPipeline_ImportRecoveryState(params);

	/* Reinit the WAL prefetcher. */
	xlogprefetcher_pipelined = XLogPrefetcherAllocate(xlogreader);


	elog(LOG, "[walpipeline] producer: started at %X/%X, TLI %u",
		 LSN_FORMAT_ARGS(params->NextRecPtr), replayTLI);

	XLogPrefetcherBeginRead(xlogprefetcher_pipelined, params->NextRecPtr);

	/* Handle the signal if we were promoted before the pipeline launch */
	promote_signaled = params->promotedBeforeLaunch;

	/* Main decoding loop */
	while (true)
	{
		bool shutdown_requested;

		/* Check if consumer requested to stop decoding */
		SpinLockAcquire(&WalPipelineShm->mutex);
		shutdown_requested = WalPipelineShm->shutdown_requested;
		SpinLockRelease(&WalPipelineShm->mutex);

		if (shutdown_requested)
		{
			elog(DEBUG1, "[walpipeline] producer: got shutdown request from the consumer.");
			break;
		}

		/* Read next WAL record */
		record = ReadRecord(xlogprefetcher_pipelined, LOG, false, replayTLI);

		if (record == NULL)
		{
			end_of_wal = true;
			elog(DEBUG1, "[walpipeline] producer: reached end of WAL");
			break;
		}

		/*
		 * Successfully decoded a record. Send it to the consumer.
		 */
		if (!WalPipeline_SendRecord(xlogreader))
		{
			elog(WARNING, "[walpipeline] producer: failed to send record, queue full or detached");
			break;
		}

		/* Update our position for monitoring */
		SpinLockAcquire(&WalPipelineShm->mutex);
		WalPipelineShm->producer_lsn = xlogreader->EndRecPtr;
		SpinLockRelease(&WalPipelineShm->mutex);

		CHECK_FOR_INTERRUPTS();
	}


	if (end_of_wal)
	{
		/* Notify consumer we need to exit as no more records found */
		WalPipeline_SendShutdown();
		WalPipeline_WaitForConsumerShutdownRequest();
	}

	SpinLockAcquire(&WalPipelineShm->mutex);
	records_sent = WalPipelineShm->records_sent;
	records_received = WalPipelineShm->records_received;
	SpinLockRelease(&WalPipelineShm->mutex);

	elog(LOG, "[walpipeline] producer: exiting: sent=" UINT64_FORMAT " received=" UINT64_FORMAT,
		 records_sent, records_received);

	/* Cleanup */
	pfree(private);
	XLogReaderFree(xlogreader);
	XLogPrefetcherFree(xlogprefetcher_pipelined);
	DisownRecoveryWakeupLatch();
	cleanup_producer_resources();
}

/*
 * Fix up the interior pointers: main_data and each block's data/bkp_image
 * are absolute addresses in the producer. Convert them to byte offsets
 * from the start of the DecodedXLogRecord so the consumer can
 * reconstruct them.
 */
static void
set_ptrs_to_offsets(DecodedXLogRecord *dec)
{
	if (dec->main_data_len > 0)
		dec->main_data = (char *)((char *)dec->main_data - (char *)dec);

	for (int i = 0; i <= dec->max_block_id; i++)
	{
		DecodedBkpBlock *blk = &dec->blocks[i];
		if (!blk->in_use)
			continue;
		if (blk->has_data)
			blk->data = (char *)((char *)blk->data - (char *)dec);
		if (blk->has_image)
			blk->bkp_image = (char *)((char *)blk->bkp_image - (char *)dec);
	}
}

/*
 * Restore interior pointers from offsets.
 */
static void
reset_offsets_to_ptrs(DecodedXLogRecord *dec)
{

	if (dec->main_data_len > 0)
		dec->main_data = (char *)dec + (ptrdiff_t)dec->main_data;

	for (int i = 0; i <= dec->max_block_id; i++)
	{
		DecodedBkpBlock *blk = &dec->blocks[i];
		if (!blk->in_use)
			continue;
		if (blk->has_data)
			blk->data = (char *)dec + (ptrdiff_t)blk->data;
		if (blk->has_image)
			blk->bkp_image = (char *)dec + (ptrdiff_t)blk->bkp_image;
	}
}

/*
 * Producer Function.
 * Send a decoded WAL record to the consumer
 */
bool
WalPipeline_SendRecord(XLogReaderState *record)
{
	Size        msglen;
	shm_mq_result res;
	shm_mq_iovec iov[2];


	if (!producer_mq_handle)
		return false;

	/* Serialize the decoded data */
	msglen = serialize_wal_record(record, iov);

	res = shm_mq_sendv(producer_mq_handle, iov, 2, false, true);

	/*
	 * Reset the offsets to exact ptrs because decoded record data still
	 * needed by the producer reader state
	 */
	reset_offsets_to_ptrs(record->record);

	if (res == SHM_MQ_SUCCESS)
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		WalPipelineShm->records_sent++;
		WalPipelineShm->bytes_sent += msglen;
		SpinLockRelease(&WalPipelineShm->mutex);

		return true;
	}

	if (res == SHM_MQ_DETACHED)
	{
		elog(PANIC, "[walpipeline] producer: consumer detached");
		return false;
	}

	/* Some other error */
	elog(PANIC, "[walpipeline] producer: shm_mq_send failed with result %d", res);
	return false;
}

/*
 * Producer Function.
 * Send shutdown message to consumer
 */
bool
WalPipeline_SendShutdown(void)
{
	WalRecordMsgHeader hdr;
	shm_mq_result res;

	if (!producer_mq_handle)
		return false;

	hdr.msg_type = WAL_MSG_SHUTDOWN;
	hdr.endRecPtr = InvalidXLogRecPtr;

	res = shm_mq_send(producer_mq_handle, sizeof(hdr), &hdr, false, true);
	return (res == SHM_MQ_SUCCESS);
}

/*
 * Consumer Function.
 * Receive and deserialize a WAL record from the producer
 */
DecodedXLogRecord *
WalPipeline_ReceiveRecord(XLogReaderState *startup_reader)
{
	shm_mq_result res;
	Size        nbytes;
	void       *data;
	WalRecordMsgHeader *hdr;
	DecodedXLogRecord *record;

	if (!consumer_mq_handle)
		return NULL;

	/* Receive message from queue */
	res = shm_mq_receive(consumer_mq_handle, &nbytes, &data, false);

	if (res != SHM_MQ_SUCCESS)
		elog(ERROR, "[walpipeline] consumer: failed to receive record");

	hdr = (WalRecordMsgHeader *) data;

	/* Handle different message types */
	switch (hdr->msg_type)
	{
		case WAL_MSG_RECORD:
			record = deserialize_wal_record((char *) data, nbytes, startup_reader);

			/* Update statistics */
			SpinLockAcquire(&WalPipelineShm->mutex);
			WalPipelineShm->records_received++;
			WalPipelineShm->bytes_received += nbytes;
			WalPipelineShm->consumer_lsn = hdr->endRecPtr;
			SpinLockRelease(&WalPipelineShm->mutex);

			return record;

		case WAL_MSG_SHUTDOWN:
			elog(LOG, "[walpipeline] consumer: received shutdown message from the producer");
			return NULL;

		default:
			elog(PANIC, "[walpipeline] consumer: unknown message type: %d",
				 hdr->msg_type);
			return NULL;
	}
}

/*
 * Consumer Function.
 * Check if producer is still running
 */
bool
WalPipeline_CheckProducerAlive(void)
{
	pid_t       pid;
	bool        alive;

	SpinLockAcquire(&WalPipelineShm->mutex);
	pid = WalPipelineShm->producer_pid;
	SpinLockRelease(&WalPipelineShm->mutex);

	if (pid == 0)
		return false;

	alive = (kill(pid, 0) == 0);

	if (!alive)
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		WalPipelineShm->producer_pid = 0;
		SpinLockRelease(&WalPipelineShm->mutex);
	}

	return alive;
}

/*
 * Consumer Function.
 * Check if pipeline is active
 */
bool
WalPipeline_IsActive(void)
{
	bool        active;

	if (!WalPipelineShm)
		return false;

	SpinLockAcquire(&WalPipelineShm->mutex);
	active = WalPipelineShm->initialized && !WalPipelineShm->shutdown_requested;
	SpinLockRelease(&WalPipelineShm->mutex);

	return active;
}

/*
 * Consumer Function.
 * Check if pid pipeline decoder worker
 */
pid_t
WalPipeline_GetProducerPid(void)
{
	pid_t pid;

	SpinLockAcquire(&WalPipelineShm->mutex);
	pid = WalPipelineShm->producer_pid;
	SpinLockRelease(&WalPipelineShm->mutex);

	if (!(pid > 0))
		pid = InvalidPid;

	return pid;
}

/*
 * Producer Function.
 * Producer may can exit without waiting for the consumer, but its better to
 * wait until consumer request shutdown. This way log messages will show
 * no of records_sent & records_received records equal to each other.
 */
static void
WalPipeline_WaitForConsumerShutdownRequest(void)
{
	int			iters = 0;

	while (true)
	{
		bool		shutdown_requested;

		SpinLockAcquire(&WalPipelineShm->mutex);
		shutdown_requested = WalPipelineShm->shutdown_requested;
		SpinLockRelease(&WalPipelineShm->mutex);

		if (shutdown_requested)
			break;

		if (++iters >= MAX_SHUTDOWN_WAIT_ITERS)
		{
			elog(WARNING,
					"[walpipeline] producer: timed out waiting for consumer "
					"to acknowledge shutdown, exiting anyway");
			break;
		}

		/* Allow SIGTERM / SIGHUP to interrupt the wait */
		ProcessPipelineBgwInterrupts();

		pg_usleep(10000);  /* sleep 10ms */
	}
}

/*
 * Consumer Function.
 * Wait unless last sent record by the pipeline is applied by the
 * startup process.
 */
void
WalPipeline_WaitForConsumerCatchup(void)
{
	XLogRecPtr producer_lsn;
	XLogRecPtr consumer_lsn;

	for (;;)
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		producer_lsn = WalPipelineShm->producer_lsn;
		consumer_lsn = WalPipelineShm->applied_lsn;
		SpinLockRelease(&WalPipelineShm->mutex);

		if (producer_lsn == consumer_lsn)
			return;

		CHECK_FOR_INTERRUPTS();

		/* short sleep to avoid busy looping */
		pg_usleep(50);   /* 50 microseconds */
	}
}

/*
 * Consumer Function.
 * Get pipeline statistics
 */
void
WalPipeline_GetStats(uint64 *records_sent, uint64 *records_received,
					 XLogRecPtr *producer_lsn, XLogRecPtr *consumer_lsn)
{
	SpinLockAcquire(&WalPipelineShm->mutex);

	if (records_sent)
		*records_sent = WalPipelineShm->records_sent;
	if (records_received)
		*records_received = WalPipelineShm->records_received;
	if (producer_lsn)
		*producer_lsn = WalPipelineShm->producer_lsn;
	if (consumer_lsn)
		*consumer_lsn = WalPipelineShm->consumer_lsn;

	SpinLockRelease(&WalPipelineShm->mutex);
}


/*
 * serialize_wal_record (Producer)
 *
 * Pack a WalRecordMsgHeader followed by the DecodedXLogRecord into a
 * shm_mq_iovec, converting interior pointers to relative offsets.
 *
 * Data layout:
 *   [WalRecordMsgHeader][DecodedXLogRecord + trailing data]
 */
static Size
serialize_wal_record(XLogReaderState *xlogreader, shm_mq_iovec *iov)
{
	DecodedXLogRecord *dec = xlogreader->record;
	Size payload_size = dec->size;

	/* build header */
	msghdr.msg_type          = WAL_MSG_RECORD;
	msghdr.readRecPtr        = xlogreader->ReadRecPtr;
	msghdr.endRecPtr         = xlogreader->EndRecPtr;
	msghdr.missingContrecPtr = xlogreader->missingContrecPtr;
	msghdr.abortedRecPtr     = xlogreader->abortedRecPtr;
	msghdr.overwrittenRecPtr = xlogreader->overwrittenRecPtr;
	msghdr.decoded_size      = payload_size;

	set_ptrs_to_offsets(dec);

	iov[0].data = (char *) &msghdr;
	iov[0].len  = sizeof(WalRecordMsgHeader);

	iov[1].data = (char *) dec;
	iov[1].len  = payload_size;

	return sizeof(WalRecordMsgHeader) + payload_size;;
}

/*
 * deserialize_wal_record (Consumer)
 *
 * Unpack a buffer produced by serialize_wal_record, restore interior
 * offsets to pointers, and attach the record to the startup reader.
 *
 * Data Layout:
 * 		[WalRecordMsgHeader][DecodedXLogRecord + trailing data]
 */
DecodedXLogRecord *
deserialize_wal_record(const char *buf, Size len,
					   XLogReaderState *startup_reader)
{
	WalRecordMsgHeader hdr;
	DecodedXLogRecord *dec;
	const char *payload;

	if (len < sizeof(hdr))
		return NULL;

	memcpy(&hdr, buf, sizeof(hdr));

	if (hdr.decoded_size != len - sizeof(hdr))
		return NULL;


	/*
	 * We don't have to copy the data to some local buffer. The decoded blks
	 * are continous bytes in memory and hence we can just point to its location
	 * in the sh_mq
	 */
	payload = buf + sizeof(hdr);
	dec = (DecodedXLogRecord *) payload;

	reset_offsets_to_ptrs(dec);

	/* clear the queue link — it belongs to the producer's queue */
	dec->next = NULL;

	/* Attach to reader, only updating the public parameters */
	startup_reader->record            = dec;
	startup_reader->ReadRecPtr        = dec->lsn;
	startup_reader->DecodeRecPtr      = dec->lsn;
	startup_reader->EndRecPtr         = dec->next_lsn;
	startup_reader->NextRecPtr        = dec->next_lsn;
	startup_reader->decode_queue_head = dec;
	startup_reader->decode_queue_tail = dec;
	startup_reader->missingContrecPtr = hdr.missingContrecPtr;
	startup_reader->abortedRecPtr     = hdr.abortedRecPtr;
	startup_reader->overwrittenRecPtr = hdr.overwrittenRecPtr;

	return dec;
}

/*
 * We need to put some assertion that only pipeline worker should be touching
 * the specific code.
 */
bool AmWalPipeline(void)
{
	if (MyBackendType == B_BG_WORKER && MyBgworkerEntry)
	{
		if (strncmp(MyBgworkerEntry->bgw_name, "wal pipeline", 12) == 0)
			return true;
	}

	return false;
}

void SetProducerStartWaiting(void)
{
	if (wal_pipeline_enabled && AmWalPipeline())
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		WalPipelineShm->producerWaiting = true;
		SpinLockRelease(&WalPipelineShm->mutex);
	}
}

void SetProducerDoneWaiting(void)
{
	if (wal_pipeline_enabled && AmWalPipeline())
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		WalPipelineShm->producerWaiting = false;
		SpinLockRelease(&WalPipelineShm->mutex);
	}
}

/*
 * Clean up producer-side resources
 */
static void
cleanup_producer_resources(void)
{
	if (producer_mq_handle)
	{
		shm_mq_detach(producer_mq_handle);
		producer_mq_handle = NULL;
	}

	if (producer_dsm_seg)
	{
		dsm_detach(producer_dsm_seg);
		producer_dsm_seg = NULL;
	}

	producer_mq = NULL;

	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->producer_pid = 0;
	SpinLockRelease(&WalPipelineShm->mutex);
}

/*
 * Clean up consumer-side resources
 */
static void
cleanup_consumer_resources(void)
{
	if (consumer_mq_handle)
	{
		shm_mq_detach(consumer_mq_handle);
		consumer_mq_handle = NULL;
	}

	if (consumer_dsm_seg)
	{
		dsm_unpin_segment(dsm_segment_handle(consumer_dsm_seg));
		dsm_detach(consumer_dsm_seg);
		consumer_dsm_seg = NULL;
	}

	consumer_mq = NULL;

	SpinLockAcquire(&WalPipelineShm->mutex);
	WalPipelineShm->consumer_pid = 0;
	WalPipelineShm->dsm_seg_handle = DSM_HANDLE_INVALID;
	SpinLockRelease(&WalPipelineShm->mutex);
}

/*
 * Cleanup callback for process exit
 */
static void
wal_pipeline_cleanup_callback(int code, Datum arg)
{
	pid_t mypid = MyProcPid;
	bool is_producer = false;

	if (WalPipelineShm)
	{
		SpinLockAcquire(&WalPipelineShm->mutex);
		is_producer = (WalPipelineShm->producer_pid == mypid);
		SpinLockRelease(&WalPipelineShm->mutex);
	}

	if (is_producer)
		cleanup_producer_resources();
	else
		cleanup_consumer_resources();
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

 /* SIGUSR2: set flag to finish recovery */
static void
PipelineProcTriggerHandler(SIGNAL_ARGS)
{
	promote_signaled = true;
	WakeupRecovery();
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
PipelineBgwSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
	WakeupRecovery();
}

/*
 * Re-read the config file.
 *
 * If one of the critical walreceiver options has changed, flag xlogrecovery.c
 * to restart it.
 */
static void
PipelineRereadConfig(void)
{
	char	   *conninfo = pstrdup(PrimaryConnInfo);
	char	   *slotname = pstrdup(PrimarySlotName);
	bool		tempSlot = wal_receiver_create_temp_slot;
	bool		conninfoChanged;
	bool		slotnameChanged;
	bool		tempSlotChanged = false;

	ProcessConfigFile(PGC_SIGHUP);

	conninfoChanged = strcmp(conninfo, PrimaryConnInfo) != 0;
	slotnameChanged = strcmp(slotname, PrimarySlotName) != 0;

	/*
	 * wal_receiver_create_temp_slot is used only when we have no slot
	 * configured.  We do not need to track this change if it has no effect.
	 */
	if (!slotnameChanged && strcmp(PrimarySlotName, "") == 0)
		tempSlotChanged = tempSlot != wal_receiver_create_temp_slot;
	pfree(conninfo);
	pfree(slotname);

	if (conninfoChanged || slotnameChanged || tempSlotChanged)
		StartupRequestWalReceiverRestart();
}

bool
IsPromoteSignaledPipeline(void)
{
	return promote_signaled;
}

void
ResetPromoteSignaledPipeline(void)
{
	promote_signaled = false;
}

/*
 * Process any requests or signals received recently.
 */
void
ProcessPipelineBgwInterrupts(void)
{

	bool shutdown_requested;

	if (got_SIGHUP)
	{
		got_SIGHUP = false;
		PipelineRereadConfig();
	}

	SpinLockAcquire(&WalPipelineShm->mutex);
	shutdown_requested = WalPipelineShm->shutdown_requested;
	SpinLockRelease(&WalPipelineShm->mutex);

	if (shutdown_requested)
		proc_exit(0);

	CHECK_FOR_INTERRUPTS();
}