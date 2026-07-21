/*-------------------------------------------------------------------------
 *
 * xlogpipeline.h
 *    WAL replay pipeline for parallel recovery
 *
 * This module implements a producer-consumer pipeline for WAL replay:
 * - Producer: background worker that reads and decodes WAL records
 * - Consumer: startup process: core redo loop
 *
 * The pipeline uses shared memory queues (shm_mq) to pass decoded WAL
 * records from producer to consumer, enabling parallelism while
 * maintaining sequential replay semantics.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/xlogpipeline.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WAL_PIPELINE_H
#define WAL_PIPELINE_H

#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "storage/spin.h"

/*
 * Magic number for shared memory TOC
 */
#define PG_WAL_PIPELINE_MAGIC 0x57414C50  /* "WALP" */

/*
 * Message types sent through the pipeline
 */
typedef enum WalMsgType
{
	WAL_MSG_INVALID = 0,
	WAL_MSG_RECORD,         /* Decoded WAL record */
	WAL_MSG_SHUTDOWN,       /* Graceful shutdown request */
} WalMsgType;

/* Wire header for a serialized WAL message */
typedef struct WalRecordMsgHeader
{
	WalMsgType  msg_type;             /* WAL_MSG_RECORD etc */
	uint32      decoded_size;         /* byte length of the payload that follows */
	XLogRecPtr  readRecPtr;           /* XLogReaderState->ReadRecPtr */
	XLogRecPtr  endRecPtr;            /* XLogReaderState->EndRecPtr */
	XLogRecPtr  missingContrecPtr;    /* XLogReaderState->missingContrecPtr */
	XLogRecPtr  abortedRecPtr;        /* XLogReaderState->abortedRecPtr */
	XLogRecPtr  overwrittenRecPtr;    /* XLogReaderState->overwrittenRecPtr */
} WalRecordMsgHeader;

/*
 * Shared memory control structure for the WAL pipeline
 */
typedef struct WalPipelineShmCtl
{
	/* Lifecycle management */
	slock_t         mutex;
	bool            initialized;
	bool            shutdown_requested;
	bool			producerWaiting;

	/* Producer state */
	pid_t           producer_pid;
	XLogRecPtr      producer_lsn;   /* Last LSN read by producer */

	/* Consumer state */
	pid_t           consumer_pid;
	XLogRecPtr      consumer_lsn;   /* Last LSN recieved by consumer */
	XLogRecPtr      applied_lsn;   	/* Last LSN applied by consumer */

	/* Queue handles */
	dsm_handle      dsm_seg_handle;
	shm_mq_handle   *producer_mq_handle;
	shm_mq_handle   *consumer_mq_handle;

	/* Statistics */
	uint64          records_sent;
	uint64          records_received;
	uint64          bytes_sent;
	uint64          bytes_received;
} WalPipelineShmCtl;

/* consumer may have to compute prefetecher stats */
extern PGDLLIMPORT XLogPrefetcher *xlogprefetcher_pipelined;


/* Producer functions (called by background worker) */
extern void WalPipeline_ProducerMain(Datum main_arg);
extern bool WalPipeline_SendRecord(XLogReaderState *record);
extern bool WalPipeline_SendShutdown(void);
extern bool AmWalPipeline(void);
extern void SetProducerStartWaiting(void);
extern void SetProducerDoneWaiting(void);



extern void ProcessPipelineBgwInterrupts(void);
extern bool IsPromoteSignaledPipeline(void);
extern void ResetPromoteSignaledPipeline(void);
/* Global shared memory pointer */
extern WalPipelineShmCtl *WalPipelineShm;

#endif   /* WAL_PIPELINE_H */