/*-------------------------------------------------------------------------
 *
 * restore_module.h
 *		Exports for restore modules.
 *
 * Copyright (c) 2023, PostgreSQL Global Development Group
 *
 * src/include/restore/restore_module.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _RESTORE_MODULE_H
#define _RESTORE_MODULE_H

/*
 * The value of the restore_library GUC.
 */
extern PGDLLIMPORT char *restoreLibrary;

typedef struct RestoreModuleState
{
	/*
	 * Private data pointer for use by a restore module.  This can be used to
	 * store state for the module that will be passed to each of its callbacks.
	 */
	void	   *private_data;

	/*
	 * Indicates whether the callback that checks for timeline existence merely
	 * copies the file.  If set to true, the server will verify that the
	 * timeline history file exists after the callback returns.
	 */
	bool		timeline_history_exists_cb_copies;
} RestoreModuleState;

/*
 * Restore module callbacks
 *
 * These callback functions should be defined by restore libraries and returned
 * via _PG_restore_module_init().  For more information about the purpose of
 * each callback, refer to the restore modules documentation.
 */
typedef void (*RestoreStartupCB) (RestoreModuleState *state);
typedef bool (*RestoreWalSegmentConfiguredCB) (RestoreModuleState *state);
typedef bool (*RestoreWalSegmentCB) (RestoreModuleState *state,
									 const char *file, const char *path,
									 const char *lastRestartPointFileName);
typedef bool (*RestoreTimelineHistoryConfiguredCB) (RestoreModuleState *state);
typedef bool (*RestoreTimelineHistoryCB) (RestoreModuleState *state,
										  const char *file, const char *path);
typedef bool (*TimelineHistoryExistsConfiguredCB) (RestoreModuleState *state);
typedef bool (*TimelineHistoryExistsCB) (RestoreModuleState *state,
										 const char *file, const char *path);
typedef bool (*ArchiveCleanupConfiguredCB) (RestoreModuleState *state);
typedef void (*ArchiveCleanupCB) (RestoreModuleState *state,
								  const char *lastRestartPointFileName);
typedef bool (*RecoveryEndConfiguredCB) (RestoreModuleState *state);
typedef void (*RecoveryEndCB) (RestoreModuleState *state,
							   const char *lastRestartPointFileName);
typedef void (*RestoreShutdownCB) (RestoreModuleState *state);

typedef struct RestoreModuleCallbacks
{
	RestoreStartupCB startup_cb;
	RestoreWalSegmentConfiguredCB restore_wal_segment_configured_cb;
	RestoreWalSegmentCB restore_wal_segment_cb;
	RestoreTimelineHistoryConfiguredCB restore_timeline_history_configured_cb;
	RestoreTimelineHistoryCB restore_timeline_history_cb;
	TimelineHistoryExistsConfiguredCB timeline_history_exists_configured_cb;
	TimelineHistoryExistsCB timeline_history_exists_cb;
	ArchiveCleanupConfiguredCB archive_cleanup_configured_cb;
	ArchiveCleanupCB archive_cleanup_cb;
	RecoveryEndConfiguredCB recovery_end_configured_cb;
	RecoveryEndCB recovery_end_cb;
	RestoreShutdownCB shutdown_cb;
} RestoreModuleCallbacks;

/*
 * Type of the shared library symbol _PG_restore_module_init that is looked up
 * when loading a restore library.
 */
typedef const RestoreModuleCallbacks *(*RestoreModuleInit) (void);

extern PGDLLEXPORT const RestoreModuleCallbacks *_PG_restore_module_init(void);

extern void LoadRestoreCallbacks(void);
extern void call_restore_module_shutdown_cb(int code, Datum arg);

extern const RestoreModuleCallbacks *RestoreCallbacks;
extern RestoreModuleState *restore_module_state;

/*
 * The following *_configured() functions are convenient wrappers around the
 * *_configured_cb() callback functions with additional defensive NULL checks.
 */

static inline bool
restore_wal_segment_configured(void)
{
	return RestoreCallbacks != NULL &&
		   RestoreCallbacks->restore_wal_segment_configured_cb != NULL &&
		   RestoreCallbacks->restore_wal_segment_cb != NULL &&
		   RestoreCallbacks->restore_wal_segment_configured_cb(restore_module_state);
}

static inline bool
restore_timeline_history_configured(void)
{
	return RestoreCallbacks != NULL &&
		   RestoreCallbacks->restore_timeline_history_configured_cb != NULL &&
		   RestoreCallbacks->restore_timeline_history_cb != NULL &&
		   RestoreCallbacks->restore_timeline_history_configured_cb(restore_module_state);
}

static inline bool
timeline_history_exists_configured(void)
{
	return RestoreCallbacks != NULL &&
		   RestoreCallbacks->timeline_history_exists_configured_cb != NULL &&
		   RestoreCallbacks->timeline_history_exists_cb != NULL &&
		   RestoreCallbacks->timeline_history_exists_configured_cb(restore_module_state);
}

static inline bool
archive_cleanup_configured(void)
{
	return RestoreCallbacks != NULL &&
		   RestoreCallbacks->archive_cleanup_configured_cb != NULL &&
		   RestoreCallbacks->archive_cleanup_cb != NULL &&
		   RestoreCallbacks->archive_cleanup_configured_cb(restore_module_state);
}

static inline bool
recovery_end_configured(void)
{
	return RestoreCallbacks != NULL &&
		   RestoreCallbacks->recovery_end_configured_cb != NULL &&
		   RestoreCallbacks->recovery_end_cb != NULL &&
		   RestoreCallbacks->recovery_end_configured_cb(restore_module_state);
}

#endif							/* _RESTORE_MODULE_H */
