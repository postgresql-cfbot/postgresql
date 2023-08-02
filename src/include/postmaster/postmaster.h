/*-------------------------------------------------------------------------
 *
 * postmaster.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/postmaster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _POSTMASTER_H
#define _POSTMASTER_H

/* GUC options */
extern PGDLLIMPORT bool EnableSSL;
extern PGDLLIMPORT int SuperuserReservedConnections;
extern PGDLLIMPORT int ReservedConnections;
extern PGDLLIMPORT int PostPortNumber;
extern PGDLLIMPORT int Unix_socket_permissions;
extern PGDLLIMPORT char *Unix_socket_group;
extern PGDLLIMPORT char *Unix_socket_directories;
extern PGDLLIMPORT char *ListenAddresses;
extern PGDLLIMPORT bool ClientAuthInProgress;
extern PGDLLIMPORT int PreAuthDelay;
extern PGDLLIMPORT int AuthenticationTimeout;
extern PGDLLIMPORT bool Log_connections;
extern PGDLLIMPORT bool log_hostname;
extern PGDLLIMPORT bool enable_bonjour;
extern PGDLLIMPORT char *bonjour_name;
extern PGDLLIMPORT bool restart_after_crash;
extern PGDLLIMPORT bool remove_temp_files_after_crash;
extern PGDLLIMPORT bool send_abort_for_crash;
extern PGDLLIMPORT bool send_abort_for_kill;

#ifdef WIN32
extern PGDLLIMPORT HANDLE PostmasterHandle;
#else
extern PGDLLIMPORT int postmaster_alive_fds[2];

/*
 * Constants that represent which of postmaster_alive_fds is held by
 * postmaster, and which is used in children to check for postmaster death.
 */
#define POSTMASTER_FD_WATCH		0	/* used in children to check for
									 * postmaster death */
#define POSTMASTER_FD_OWN		1	/* kept open by postmaster only */
#endif

extern PGDLLIMPORT const char *progname;

extern void PostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void ClosePostmasterPorts(bool am_syslogger);
extern void InitProcessGlobals(void);

extern int	MaxLivePostmasterChildren(void);

extern bool PostmasterMarkPIDForWorkerNotify(int);

extern void BackendMain(char *startup_data, size_t startup_data_len);

#ifdef EXEC_BACKEND
extern Size ShmemBackendArraySize(void);
extern void ShmemBackendArrayAllocation(void);
#endif

/* in process_start.c */

/* this better match the list in process_start.c */
typedef enum PostmasterChildType {
	PMC_BACKEND = 0,
	PMC_AV_LAUNCHER,
	PMC_AV_WORKER,
	PMC_BGWORKER,
	PMC_SYSLOGGER,

	/*
	 * so-called "aux processes". These access shared memory, but are not attached to
	 * any particular database. Only one of each of these can be running at a time.
	 */
	PMC_STARTUP,
	PMC_BGWRITER,
	PMC_ARCHIVER,
	PMC_CHECKPOINTER,
	PMC_WAL_WRITER,
	PMC_WAL_RECEIVER,
} PostmasterChildType;

typedef void (*ChildEntryPoint) (char *startup_data, size_t startup_data_len);

/* defined in libpq-be.h */
extern struct ClientSocket *MyClientSocket;

extern pid_t postmaster_child_launch(PostmasterChildType child_type, char *startup_data, size_t startup_data_len, struct ClientSocket *sock);

#ifdef EXEC_BACKEND
extern void SubPostmasterMain(int argc, char *argv[]) pg_attribute_noreturn();
#endif

const char *PostmasterChildName(PostmasterChildType child_type);

/*
 * Note: MAX_BACKENDS is limited to 2^18-1 because that's the width reserved
 * for buffer references in buf_internals.h.  This limitation could be lifted
 * by using a 64bit state; but it's unlikely to be worthwhile as 2^18-1
 * backends exceed currently realistic configurations. Even if that limitation
 * were removed, we still could not a) exceed 2^23-1 because inval.c stores
 * the backend ID as a 3-byte signed integer, b) INT_MAX/4 because some places
 * compute 4*MaxBackends without any overflow check.  This is rechecked in the
 * relevant GUC check hooks and in RegisterBackgroundWorker().
 */
#define MAX_BACKENDS	0x3FFFF

#endif							/* _POSTMASTER_H */
