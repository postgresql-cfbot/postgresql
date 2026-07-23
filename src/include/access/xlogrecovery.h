/*
 * xlogrecovery.h
 *
 * Functions for WAL recovery and standby mode
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogrecovery.h
 */
#ifndef XLOGRECOVERY_H
#define XLOGRECOVERY_H

#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "utils/timestamp.h"

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when in standby mode.
 */
typedef enum
{
	RECOVERY_TARGET_UNSET,
	RECOVERY_TARGET_XID,
	RECOVERY_TARGET_TIME,
	RECOVERY_TARGET_NAME,
	RECOVERY_TARGET_LSN,
	RECOVERY_TARGET_IMMEDIATE,
} RecoveryTargetType;

/*
 * Recovery target TimeLine goal
 */
typedef enum
{
	RECOVERY_TARGET_TIMELINE_CONTROLFILE,
	RECOVERY_TARGET_TIMELINE_LATEST,
	RECOVERY_TARGET_TIMELINE_NUMERIC,
} RecoveryTargetTimeLineGoal;

/*
 * Recovery target action.
 */
typedef enum
{
	RECOVERY_TARGET_ACTION_PAUSE,
	RECOVERY_TARGET_ACTION_PROMOTE,
	RECOVERY_TARGET_ACTION_SHUTDOWN,
}			RecoveryTargetAction;

/* Recovery pause states */
typedef enum RecoveryPauseState
{
	RECOVERY_NOT_PAUSED,		/* pause not requested */
	RECOVERY_PAUSE_REQUESTED,	/* pause requested, but not yet paused */
	RECOVERY_PAUSED,			/* recovery is paused */
} RecoveryPauseState;

/* Codes indicating where we got a WAL file from during recovery, or where
 * to attempt to get one.
 */
typedef enum
{
	XLOG_FROM_ANY = 0,			/* request to read WAL from any source */
	XLOG_FROM_ARCHIVE,			/* restored using restore_command */
	XLOG_FROM_PG_WAL,			/* existing file in pg_wal */
	XLOG_FROM_STREAM,			/* streamed from primary */
} XLogSource;

/*
 * Shared-memory state for WAL recovery.
 */
typedef struct XLogRecoveryCtlData
{
	/*
	 * SharedHotStandbyActive indicates if we allow hot standby queries to be
	 * run.  Protected by info_lck.
	 */
	bool		SharedHotStandbyActive;

	/*
	 * SharedPromoteIsTriggered indicates if a standby promotion has been
	 * triggered.  Protected by info_lck.
	 */
	bool		SharedPromoteIsTriggered;

	/*
	 * recoveryWakeupLatch is used to wake up the startup process to continue
	 * WAL replay, if it is waiting for WAL to arrive or promotion to be
	 * requested.
	 *
	 * Note that the startup process also uses another latch, its procLatch,
	 * to wait for recovery conflict. If we get rid of recoveryWakeupLatch for
	 * signaling the startup process in favor of using its procLatch, which
	 * comports better with possible generic signal handlers using that latch.
	 * But we should not do that because the startup process doesn't assume
	 * that it's waken up by walreceiver process or SIGHUP signal handler
	 * while it's waiting for recovery conflict. The separate latches,
	 * recoveryWakeupLatch and procLatch, should be used for inter-process
	 * communication for WAL replay and recovery conflict, respectively.
	 */
	Latch		recoveryWakeupLatch;

	/*
	 * In case pipeline enabled we will need two latches. One that can be used
	 * by the pipeline for WAL waiting and other that can be used by the
	 * startup process for the apply delay. Before this we had only one latch
	 * for both cases.
	 */
	Latch		recoveryApplyDelayLatch;

	/*
	 * Last record successfully replayed.
	 */
	XLogRecPtr	lastReplayedReadRecPtr; /* start position */
	XLogRecPtr	lastReplayedEndRecPtr;	/* end+1 position */
	TimeLineID	lastReplayedTLI;	/* timeline */

	/*
	 * When we're currently replaying a record, ie. in a redo function,
	 * replayEndRecPtr points to the end+1 of the record being replayed,
	 * otherwise it's equal to lastReplayedEndRecPtr.
	 */
	XLogRecPtr	replayEndRecPtr;
	TimeLineID	replayEndTLI;
	/* timestamp of last COMMIT/ABORT record replayed (or being replayed) */
	TimestampTz recoveryLastXTime;

	/*
	 * timestamp of when we caught up with the latest WAL chunk received from
	 * streaming replication
	 */
	TimestampTz currentChunkStartTime;
	/* Recovery pause state */
	RecoveryPauseState recoveryPauseState;
	ConditionVariable recoveryNotPausedCV;

	slock_t		info_lck;		/* locks shared variables shown above */

	/* ------------------------------------------------------------------
	 * Variables use for IPC between pipeline and the startup proc.
	 * These are also the static variables in xlogrecovery.c but there state
	 * keep on changing. So we added them as the shared states so that both
	 * the pipeline and the startup proc stay synced if any of these state
	 * changes.
	 * ------------------------------------------------------------------
	 */

	bool		InArchiveRecovery;
	bool		pendingWalRcvRestart;
	bool		stanbyEnabled;

	/*
	 * The target TLI for which expectedTLEs should be recomputed by the
	 * consumer
	 */
	TimeLineID	expectedTLEsUpdateTLI;

	/*
	 * We also export the recoveryTargetTLI as a WalPipelineParams. But other
	 * than passing the initial state, the recoveryTargetTLI can also change
	 * it state during the decoding by the prodcuer (see rescanLatestTimeLine()).
	 *
	 * This mean at the end of the recovery,  the startup process should aware
	 * of any such state changes done by the producer. To handle this
	 * ResetStatesIfPipelined() will update the startup local recoveryTargetTLI
	 * with updated one, on FinishWalRecovery().
	*/
	TimeLineID	recoveryTargetTLI;

	/*
	 * Normaly we wakeup walrcvr after specific records have been applied, as
	 * decoding and apllying are sequential so we wakeup after enough records
	 * decoded read.
	 *
	 * But in case of pipeline reads (decoded records) could be ahead of the
	 * consumer apply loop. We cannot wakeup wal rcvr based on how much records
	 * decoded, so we tell consumer to wakeup after only after a specific lsn
	 * (WakeupWalRcvrRecPtr set by the pipeline) has beed replayed.
	 */
	XLogRecPtr	WakeupWalRcvrRecPtr;

	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;

	XLogSource	currentSource;
	XLogSource	XLogReceiptSource;

	HotStandbyState standbyState;
	TimestampTz		XLogReceiptTime;
} XLogRecoveryCtlData;

extern PGDLLIMPORT XLogRecoveryCtlData *XLogRecoveryCtl;

/* User-settable GUC parameters */
extern PGDLLIMPORT bool recoveryTargetInclusive;
extern PGDLLIMPORT int recoveryTargetAction;
extern PGDLLIMPORT int recovery_min_apply_delay;
extern PGDLLIMPORT char *PrimaryConnInfo;
extern PGDLLIMPORT char *PrimarySlotName;
extern PGDLLIMPORT char *recoveryRestoreCommand;
extern PGDLLIMPORT char *recoveryEndCommand;
extern PGDLLIMPORT char *archiveCleanupCommand;

/* indirectly set via GUC system */
extern PGDLLIMPORT TransactionId recoveryTargetXid;
extern PGDLLIMPORT char *recovery_target_time_string;
extern PGDLLIMPORT TimestampTz recoveryTargetTime;
extern PGDLLIMPORT char *recoveryTargetName;
extern PGDLLIMPORT XLogRecPtr recoveryTargetLSN;
extern PGDLLIMPORT RecoveryTargetType recoveryTarget;
extern PGDLLIMPORT bool wal_receiver_create_temp_slot;
extern PGDLLIMPORT RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal;
extern PGDLLIMPORT TimeLineID recoveryTargetTLIRequested;
extern PGDLLIMPORT TimeLineID recoveryTargetTLI;

/* Have we already reached a consistent database state? */
extern PGDLLIMPORT bool reachedConsistency;

/* Are we currently in standby mode? */
extern PGDLLIMPORT bool StandbyMode;

extern void InitWalRecovery(ControlFileData *ControlFile,
							bool *wasShutdown_ptr, bool *haveBackupLabel_ptr,
							bool *haveTblspcMap_ptr);
extern void PerformWalRecovery(void);

/*
 * FinishWalRecovery() returns this.  It contains information about the point
 * where recovery ended, and why it ended.
 */
typedef struct
{
	/*
	 * Information about the last valid or applied record, after which new WAL
	 * can be appended.  'lastRec' is the position where the last record
	 * starts, and 'endOfLog' is its end.  'lastPage' is a copy of the last
	 * partial page that contains endOfLog (or NULL if endOfLog is exactly at
	 * page boundary).  'lastPageBeginPtr' is the position where the last page
	 * begins.
	 *
	 * endOfLogTLI is the TLI in the filename of the XLOG segment containing
	 * the last applied record.  It could be different from lastRecTLI, if
	 * there was a timeline switch in that segment, and we were reading the
	 * old WAL from a segment belonging to a higher timeline.
	 */
	XLogRecPtr	lastRec;		/* start of last valid or applied record */
	TimeLineID	lastRecTLI;
	XLogRecPtr	endOfLog;		/* end of last valid or applied record */
	TimeLineID	endOfLogTLI;

	XLogRecPtr	lastPageBeginPtr;	/* LSN of page that contains endOfLog */
	char	   *lastPage;		/* copy of the last page, up to endOfLog */

	/*
	 * abortedRecPtr is the start pointer of a broken record at end of WAL
	 * when recovery completes; missingContrecPtr is the location of the first
	 * contrecord that went missing.  See CreateOverwriteContrecordRecord for
	 * details.
	 */
	XLogRecPtr	abortedRecPtr;
	XLogRecPtr	missingContrecPtr;

	/* short human-readable string describing why recovery ended */
	char	   *recoveryStopReason;

	/*
	 * If standby or recovery signal file was found, these flags are set
	 * accordingly.
	 */
	bool		standby_signal_file_found;
	bool		recovery_signal_file_found;
} EndOfWalRecoveryInfo;

struct WalPipelineParams;   /* forward declaration */

extern EndOfWalRecoveryInfo *FinishWalRecovery(void);
extern void ShutdownWalRecovery(void);
extern void RemovePromoteSignalFiles(void);

extern bool HotStandbyActive(void);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
extern RecoveryPauseState GetRecoveryPauseState(void);
extern void SetRecoveryPause(bool recoveryPause);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern XLogRecPtr GetCurrentReplayRecPtr(TimeLineID *replayEndTLI);
extern XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,
		   bool fetching_ckpt, TimeLineID replayTLI);
extern int XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
			 XLogRecPtr targetRecPtr, char *readBuf);

extern bool PromoteIsTriggered(void);
extern bool CheckPromoteSignal(void);
extern void WakeupRecovery(void);
extern void DisownRecoveryWakeupLatch(void);
extern void SetSharedHotStandbyState(void);

extern void StartupRequestWalReceiverRestart(void);
extern void XLogRequestWalReceiverReply(void);

extern void RecoveryRequiresIntParameter(const char *param_name, int currValue, int minValue);
extern void WalPipeline_ImportRecoveryState(struct WalPipelineParams *params);

extern void xlog_outdesc(StringInfo buf, XLogReaderState *record);

#endif							/* XLOGRECOVERY_H */
