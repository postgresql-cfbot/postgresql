/*
 * xlogrecovery.h
 *
 * Functions for WAL recovery and standby mode
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogrecovery.h
 */
#ifndef XLOGRECOVERY_H
#define XLOGRECOVERY_H

#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
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
	RECOVERY_TARGET_IMMEDIATE
} RecoveryTargetType;

/*
 * Recovery target TimeLine goal
 */
typedef enum
{
	RECOVERY_TARGET_TIMELINE_CONTROLFILE,
	RECOVERY_TARGET_TIMELINE_LATEST,
	RECOVERY_TARGET_TIMELINE_NUMERIC
} RecoveryTargetTimeLineGoal;

/* Recovery pause states */
typedef enum RecoveryPauseState
{
	RECOVERY_NOT_PAUSED,		/* pause not requested */
	RECOVERY_PAUSE_REQUESTED,	/* pause requested, but not yet paused */
	RECOVERY_PAUSED				/* recovery is paused */
} RecoveryPauseState;

/* User-settable GUC parameters */
extern bool recoveryTargetInclusive;
extern int	recoveryTargetAction;
extern int	recovery_min_apply_delay;
extern char *PrimaryConnInfo;
extern char *PrimarySlotName;
extern char *recoveryRestoreCommand;
extern char *recoveryEndCommand;
extern char *archiveCleanupCommand;

/* indirectly set via GUC system */
extern TransactionId recoveryTargetXid;
extern char *recovery_target_time_string;
extern TimestampTz recoveryTargetTime;
extern const char *recoveryTargetName;
extern XLogRecPtr recoveryTargetLSN;
extern RecoveryTargetType recoveryTarget;
extern char *PromoteTriggerFile;
extern bool wal_receiver_create_temp_slot;
extern RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal;
extern TimeLineID recoveryTargetTLIRequested;
extern TimeLineID recoveryTargetTLI;

/* Have we already reached a consistent database state? */
extern bool reachedConsistency;

/* Are we currently in standby mode? */
extern bool StandbyMode;

extern Size XLogRecoveryShmemSize(void);
extern void XLogRecoveryShmemInit(void);

extern void InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdownPtr, bool *haveBackupLabel, bool *haveTblspcMap);
extern void PerformWalRecovery(void);

/*
 * FinishWalRecovery() returns this. It contains information about the point
 * where the recovery ended, and why it ended.
 */
typedef struct
{
	/*
	 * Information about the last valid or applied record, after which new WAL
	 * can be appended.  'LastRec' is the position where the last record
	 * starts, and EndOfLog is its end.  'lastPage' is a copy of the last
	 * partial page that contains EndOfLog (or NULL if EndOfLog is exactly at
	 * page boundary).  'lastPageBeginPtr' is the position where the last page
	 * begins.
	 */
	XLogRecPtr	LastRec;		/* start of last valid or applied record */
	XLogRecPtr	EndOfLog;		/* end of last valid or applied record */
	TimeLineID	EndOfLogTLI;
	XLogRecPtr	lastPageBeginPtr;	/* LSN of page that contains EndOfLog */
	char	   *lastPage;		/* copy of the last page, up to EndOfLog */

	/* short human-readable string describing why recovery ended */
	char	   *recoveryStopReason;

	/*
	 * If standby or recovery signal file was found, these flags are set
	 * accordingly.
	 */
	bool		standby_signal_file_found;
	bool		recovery_signal_file_found;

	bool		bgwriterLaunched;	/* set to true if the bgwriter process was
									 * launched */
} EndOfWalRecoveryInfo;

extern EndOfWalRecoveryInfo *FinishWalRecovery(void);
extern void ShutdownWalRecovery(void);
extern void RemovePromoteSignalFiles(void);

extern XLogRecord *ReadCheckpointRecord(XLogRecPtr RecPtr, int whichChkpt, bool report);

extern void HandleBackupEndRecord(XLogRecPtr startpoint, XLogRecPtr endLsn, TimeLineID endTLI);

extern bool HotStandbyActive(void);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
extern RecoveryPauseState GetRecoveryPauseState(void);
extern void SetRecoveryPause(bool recoveryPause);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);
extern XLogRecPtr GetCurrentReplayRecPtr(TimeLineID *replayEndTLI);

extern bool PromoteIsTriggered(void);
extern bool CheckPromoteSignal(void);
extern void WakeupRecovery(void);

extern void StartupRequestWalReceiverRestart(void);
extern void XLogRequestWalReceiverReply(void);

extern void RecoveryRequiresIntParameter(const char *param_name, int currValue, int minValue);

extern void xlog_outdesc(StringInfo buf, XLogReaderState *record);

#endif							/* XLOGRECOVERY_H */
