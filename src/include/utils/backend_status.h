/* ----------
 * backend_status.h
 *	  Definitions related to backend status reporting
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_status.h
 * ----------
 */
#ifndef BACKEND_STATUS_H
#define BACKEND_STATUS_H

#include "common/int.h"
#include "datatype/timestamp.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"			/* for BackendType */
#include "storage/backendid.h"
#include "storage/proc.h"
#include "utils/backend_progress.h"


/* ----------
 * Backend states
 * ----------
 */
typedef enum BackendState
{
	STATE_UNDEFINED,
	STATE_IDLE,
	STATE_RUNNING,
	STATE_IDLEINTRANSACTION,
	STATE_FASTPATH,
	STATE_IDLEINTRANSACTION_ABORTED,
	STATE_DISABLED
} BackendState;

/* Enum helper for reporting memory allocator type */
enum pg_allocator_type
{
	PG_ALLOC_ASET = 1,
	PG_ALLOC_DSM,
	PG_ALLOC_GENERATION,
	PG_ALLOC_SLAB
};

/* ----------
 * Shared-memory data structures
 * ----------
 */

/*
 * PgBackendSSLStatus
 *
 * For each backend, we keep the SSL status in a separate struct, that
 * is only filled in if SSL is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendSSLStatus
{
	/* Information about SSL connection */
	int			ssl_bits;
	char		ssl_version[NAMEDATALEN];
	char		ssl_cipher[NAMEDATALEN];
	char		ssl_client_dn[NAMEDATALEN];

	/*
	 * serial number is max "20 octets" per RFC 5280, so this size should be
	 * fine
	 */
	char		ssl_client_serial[NAMEDATALEN];

	char		ssl_issuer_dn[NAMEDATALEN];
} PgBackendSSLStatus;

/*
 * PgBackendGSSStatus
 *
 * For each backend, we keep the GSS status in a separate struct, that
 * is only filled in if GSS is enabled.
 *
 * All char arrays must be null-terminated.
 */
typedef struct PgBackendGSSStatus
{
	/* Information about GSSAPI connection */
	char		gss_princ[NAMEDATALEN]; /* GSSAPI Principal used to auth */
	bool		gss_auth;		/* If GSSAPI authentication was used */
	bool		gss_enc;		/* If encryption is being used */

} PgBackendGSSStatus;


/* ----------
 * PgBackendStatus
 *
 * Each live backend maintains a PgBackendStatus struct in shared memory
 * showing its current activity.  (The structs are allocated according to
 * BackendId, but that is not critical.)  Note that this is unrelated to the
 * cumulative stats system (i.e. pgstat.c et al).
 *
 * Each auxiliary process also maintains a PgBackendStatus struct in shared
 * memory.
 * ----------
 */
typedef struct PgBackendStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments st_changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * st_changecount, copy the entry into private memory, then check
	 * st_changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs memory barriers to ensure that the apparent
	 * order of execution is as it desires.  Otherwise, for example, the CPU
	 * might rearrange the code so that st_changecount is incremented twice
	 * before the modification on a machine with weak memory ordering.  Hence,
	 * use the macros defined below for manipulating st_changecount, rather
	 * than touching it directly.
	 */
	int			st_changecount;

	/* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
	int			st_procpid;

	/* Type of backends */
	BackendType st_backendType;

	/* Times when current backend, transaction, and activity started */
	TimestampTz st_proc_start_timestamp;
	TimestampTz st_xact_start_timestamp;
	TimestampTz st_activity_start_timestamp;
	TimestampTz st_state_start_timestamp;

	/* Database OID, owning user's OID, connection client address */
	Oid			st_databaseid;
	Oid			st_userid;
	SockAddr	st_clientaddr;
	char	   *st_clienthostname;	/* MUST be null-terminated */

	/* Information about SSL connection */
	bool		st_ssl;
	PgBackendSSLStatus *st_sslstatus;

	/* Information about GSSAPI connection */
	bool		st_gss;
	PgBackendGSSStatus *st_gssstatus;

	/* current state */
	BackendState st_state;

	/* application name; MUST be null-terminated */
	char	   *st_appname;

	/*
	 * Current command string; MUST be null-terminated. Note that this string
	 * possibly is truncated in the middle of a multi-byte character. As
	 * activity strings are stored more frequently than read, that allows to
	 * move the cost of correct truncation to the display side. Use
	 * pgstat_clip_activity() to truncate correctly.
	 */
	char	   *st_activity_raw;

	/*
	 * Command progress reporting.  Any command which wishes can advertise
	 * that it is running by setting st_progress_command,
	 * st_progress_command_target, and st_progress_param[].
	 * st_progress_command_target should be the OID of the relation which the
	 * command targets (we assume there's just one, as this is meant for
	 * utility commands), but the meaning of each element in the
	 * st_progress_param array is command-specific.
	 */
	ProgressCommandType st_progress_command;
	Oid			st_progress_command_target;
	int64		st_progress_param[PGSTAT_NUM_PROGRESS_PARAM];

	/* query identifier, optionally computed using post_parse_analyze_hook */
	uint64		st_query_id;

	/* Current memory allocated to this backend */
	uint64		allocated_bytes;

	/* Current memory allocated to this backend by type */
	uint64		aset_allocated_bytes;
	uint64		dsm_allocated_bytes;
	uint64		generation_allocated_bytes;
	uint64		slab_allocated_bytes;
} PgBackendStatus;


/*
 * Macros to load and store st_changecount with appropriate memory barriers.
 *
 * Use PGSTAT_BEGIN_WRITE_ACTIVITY() before, and PGSTAT_END_WRITE_ACTIVITY()
 * after, modifying the current process's PgBackendStatus data.  Note that,
 * since there is no mechanism for cleaning up st_changecount after an error,
 * THESE MACROS FORM A CRITICAL SECTION.  Any error between them will be
 * promoted to PANIC, causing a database restart to clean up shared memory!
 * Hence, keep the critical section as short and straight-line as possible.
 * Aside from being safer, that minimizes the window in which readers will
 * have to loop.
 *
 * Reader logic should follow this sketch:
 *
 *		for (;;)
 *		{
 *			int before_ct, after_ct;
 *
 *			pgstat_begin_read_activity(beentry, before_ct);
 *			... copy beentry data to local memory ...
 *			pgstat_end_read_activity(beentry, after_ct);
 *			if (pgstat_read_activity_complete(before_ct, after_ct))
 *				break;
 *			CHECK_FOR_INTERRUPTS();
 *		}
 *
 * For extra safety, we generally use volatile beentry pointers, although
 * the memory barriers should theoretically be sufficient.
 */
#define PGSTAT_BEGIN_WRITE_ACTIVITY(beentry) \
	do { \
		START_CRIT_SECTION(); \
		(beentry)->st_changecount++; \
		pg_write_barrier(); \
	} while (0)

#define PGSTAT_END_WRITE_ACTIVITY(beentry) \
	do { \
		pg_write_barrier(); \
		(beentry)->st_changecount++; \
		Assert(((beentry)->st_changecount & 1) == 0); \
		END_CRIT_SECTION(); \
	} while (0)

#define pgstat_begin_read_activity(beentry, before_changecount) \
	do { \
		(before_changecount) = (beentry)->st_changecount; \
		pg_read_barrier(); \
	} while (0)

#define pgstat_end_read_activity(beentry, after_changecount) \
	do { \
		pg_read_barrier(); \
		(after_changecount) = (beentry)->st_changecount; \
	} while (0)

#define pgstat_read_activity_complete(before_changecount, after_changecount) \
	((before_changecount) == (after_changecount) && \
	 ((before_changecount) & 1) == 0)


/* ----------
 * LocalPgBackendStatus
 *
 * When we build the backend status array, we use LocalPgBackendStatus to be
 * able to add new values to the struct when needed without adding new fields
 * to the shared memory. It contains the backend status as a first member.
 * ----------
 */
typedef struct LocalPgBackendStatus
{
	/*
	 * Local version of the backend status entry.
	 */
	PgBackendStatus backendStatus;

	/*
	 * The backend ID.  For auxiliary processes, this will be set to a value
	 * greater than MaxBackends (since auxiliary processes do not have proper
	 * backend IDs).
	 */
	BackendId	backend_id;

	/*
	 * The xid of the current transaction if available, InvalidTransactionId
	 * if not.
	 */
	TransactionId backend_xid;

	/*
	 * The xmin of the current session if available, InvalidTransactionId if
	 * not.
	 */
	TransactionId backend_xmin;

	/*
	 * Number of cached subtransactions in the current session.
	 */
	int	backend_subxact_count;

	/*
	 * The number of subtransactions in the current session which exceeded the
	 * cached subtransaction limit.
	 */
	bool backend_subxact_overflowed;
} LocalPgBackendStatus;


/* ----------
 * GUC parameters
 * ----------
 */
extern PGDLLIMPORT bool pgstat_track_activities;
extern PGDLLIMPORT int pgstat_track_activity_query_size;
extern PGDLLIMPORT int max_total_bkend_mem;


/* ----------
 * Other global variables
 * ----------
 */
extern PGDLLIMPORT PgBackendStatus *MyBEEntry;
extern PGDLLIMPORT uint64 *my_allocated_bytes;
extern PGDLLIMPORT uint64 *my_aset_allocated_bytes;
extern PGDLLIMPORT uint64 *my_dsm_allocated_bytes;
extern PGDLLIMPORT uint64 *my_generation_allocated_bytes;
extern PGDLLIMPORT uint64 *my_slab_allocated_bytes;
extern PGDLLIMPORT uint64 allocation_allowance;
extern PGDLLIMPORT uint64 initial_allocation_allowance;
extern PGDLLIMPORT uint64 allocation_return;
extern PGDLLIMPORT uint64 allocation_return_threshold;


/* ----------
 * Functions called from postmaster
 * ----------
 */
extern Size BackendStatusShmemSize(void);
extern void CreateSharedBackendStatus(void);


/* ----------
 * Functions called from backends
 * ----------
 */

/* Initialization functions */
extern void pgstat_beinit(void);
extern void pgstat_bestart(void);

extern void pgstat_clear_backend_activity_snapshot(void);

/* Activity reporting functions */
extern void pgstat_report_activity(BackendState state, const char *cmd_str);
extern void pgstat_report_query_id(uint64 query_id, bool force);
extern void pgstat_report_tempfile(size_t filesize);
extern void pgstat_report_appname(const char *appname);
extern void pgstat_report_xact_timestamp(TimestampTz tstamp);
extern const char *pgstat_get_backend_current_activity(int pid, bool checkUser);
extern const char *pgstat_get_crashed_backend_activity(int pid, char *buffer,
													   int buflen);
extern uint64 pgstat_get_my_query_id(void);
extern void pgstat_set_allocated_bytes_storage(uint64 *allocated_bytes,
											   uint64 *aset_allocated_bytes,
											   uint64 *dsm_allocated_bytes,
											   uint64 *generation_allocated_bytes,
											   uint64 *slab_allocated_bytes);
extern void pgstat_reset_allocated_bytes_storage(void);

/* ----------
 * Support functions for the SQL-callable functions to
 * generate the pgstat* views.
 * ----------
 */
extern int	pgstat_fetch_stat_numbackends(void);
extern PgBackendStatus *pgstat_fetch_stat_beentry(BackendId beid);
extern LocalPgBackendStatus *pgstat_fetch_stat_local_beentry(int beid);
extern char *pgstat_clip_activity(const char *raw_activity);
extern bool exceeds_max_total_bkend_mem(uint64 allocation_request);

/* ----------
 * pgstat_report_allocated_bytes_decrease() -
 *  Called to report decrease in memory allocated for this backend.
 *
 * my_{*_}allocated_bytes initially points to local memory, making it safe to
 * call this before pgstats has been initialized.
 * ----------
 */
static inline void
pgstat_report_allocated_bytes_decrease(int64 proc_allocated_bytes,
									   int pg_allocator_type)
{
	uint64		temp;

	/* Sanity check: my allocated bytes should never drop below zero */
	if (pg_sub_u64_overflow(*my_allocated_bytes, proc_allocated_bytes, &temp))
	{
		/* On overflow, set allocated bytes and allocator type bytes to zero */
		*my_allocated_bytes = 0;
		*my_aset_allocated_bytes = 0;
		*my_dsm_allocated_bytes = 0;
		*my_generation_allocated_bytes = 0;
		*my_slab_allocated_bytes = 0;

		/* Add freed memory to allocation return counter. */
		allocation_return += proc_allocated_bytes;

		/*
		 * Return freed memory to the global counter if return threshold is
		 * met.
		 */
		if (max_total_bkend_mem && allocation_return >= allocation_return_threshold)
		{
			if (ProcGlobal)
			{
				volatile PROC_HDR *procglobal = ProcGlobal;

				/* Add to global tracker */
				pg_atomic_add_fetch_u64(&procglobal->max_total_bkend_mem_bytes,
										allocation_return);

				/* Restart the count */
				allocation_return = 0;
			}
		}
	}
	else
	{
		/* Add freed memory to allocation return counter */
		allocation_return += proc_allocated_bytes;

		/* Decrease allocator type allocated bytes */
		switch (pg_allocator_type)
		{
			case PG_ALLOC_ASET:
				*my_aset_allocated_bytes -= proc_allocated_bytes;
				break;
			case PG_ALLOC_DSM:

				/*
				 * Some dsm allocations live beyond process exit. These are
				 * accounted for in a global counter in
				 * pgstat_reset_allocated_bytes_storage at process exit.
				 */
				*my_dsm_allocated_bytes -= proc_allocated_bytes;
				break;
			case PG_ALLOC_GENERATION:
				*my_generation_allocated_bytes -= proc_allocated_bytes;
				break;
			case PG_ALLOC_SLAB:
				*my_slab_allocated_bytes -= proc_allocated_bytes;
				break;
		}

		/* decrease allocation */
		*my_allocated_bytes = *my_aset_allocated_bytes +
			*my_dsm_allocated_bytes + *my_generation_allocated_bytes +
			*my_slab_allocated_bytes;

		/*
		 * Return freed memory to the global counter if return threshold is
		 * met.
		 */
		if (max_total_bkend_mem && allocation_return >= allocation_return_threshold)
		{
			if (ProcGlobal)
			{
				volatile PROC_HDR *procglobal = ProcGlobal;

				/* Add to global tracker */
				pg_atomic_add_fetch_u64(&procglobal->max_total_bkend_mem_bytes,
										allocation_return);

				/* Restart the count */
				allocation_return = 0;
			}
		}
	}

	return;
}

/* ----------
 * pgstat_report_allocated_bytes_increase() -
 *  Called to report increase in memory allocated for this backend.
 *
 * my_allocated_bytes initially points to local memory, making it safe to call
 * this before pgstats has been initialized.
 * ----------
 */
static inline void
pgstat_report_allocated_bytes_increase(int64 proc_allocated_bytes,
									   int pg_allocator_type)
{
	uint64		temp;

	/* Sanity check: my allocated bytes should never drop below zero */
	if (pg_sub_u64_overflow(allocation_allowance, proc_allocated_bytes, &temp))
		allocation_allowance = 0;
	else
		allocation_allowance -= proc_allocated_bytes;

	/* Increase allocator type allocated bytes */
	switch (pg_allocator_type)
	{
		case PG_ALLOC_ASET:
			*my_aset_allocated_bytes += proc_allocated_bytes;
			break;
		case PG_ALLOC_DSM:

			/*
			 * Some dsm allocations live beyond process exit. These are
			 * accounted for in a global counter in
			 * pgstat_reset_allocated_bytes_storage at process exit.
			 */
			*my_dsm_allocated_bytes += proc_allocated_bytes;
			break;
		case PG_ALLOC_GENERATION:
			*my_generation_allocated_bytes += proc_allocated_bytes;
			break;
		case PG_ALLOC_SLAB:
			*my_slab_allocated_bytes += proc_allocated_bytes;
			break;
	}

	*my_allocated_bytes = *my_aset_allocated_bytes + *my_dsm_allocated_bytes +
		*my_generation_allocated_bytes + *my_slab_allocated_bytes;

	return;
}

/* ---------
 * pgstat_init_allocated_bytes() -
 *
 * Called to initialize allocated bytes variables after fork and to
 * avoid double counting allocations.
 * ---------
 */
static inline void
pgstat_init_allocated_bytes(void)
{
	*my_allocated_bytes = 0;
	*my_aset_allocated_bytes = 0;
	*my_dsm_allocated_bytes = 0;
	*my_generation_allocated_bytes = 0;
	*my_slab_allocated_bytes = 0;

	/* If we're limiting backend memory */
	if (max_total_bkend_mem)
	{
		volatile PROC_HDR *procglobal = ProcGlobal;
		uint64		available_max_total_bkend_mem = 0;

		allocation_return = 0;
		allocation_allowance = 0;

		/* Account for the initial allocation allowance */
		while ((available_max_total_bkend_mem = pg_atomic_read_u64(&procglobal->max_total_bkend_mem_bytes)) >= initial_allocation_allowance)
		{
			/*
			 * On success populate allocation_allowance. Failure here will
			 * result in the backend's first invocation of
			 * exceeds_max_total_bkend_mem allocating requested, default, or
			 * available memory or result in an out of memory error.
			 */
			if (pg_atomic_compare_exchange_u64(&procglobal->max_total_bkend_mem_bytes,
											   &available_max_total_bkend_mem,
											   available_max_total_bkend_mem -
											   initial_allocation_allowance))
			{
				allocation_allowance = initial_allocation_allowance;

				break;
			}
		}
	}

	return;
}

#endif							/* BACKEND_STATUS_H */
