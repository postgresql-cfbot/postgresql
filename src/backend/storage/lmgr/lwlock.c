/*-------------------------------------------------------------------------
 *
 * lwlock.c
 *	  Lightweight lock manager
 *
 * Lightweight locks are intended primarily to provide mutual exclusion of
 * access to shared-memory data structures.  Therefore, they offer both
 * exclusive and shared lock modes (to support read/write and read-only
 * access to a shared object).  There are few other frammishes.  User-level
 * locking should be done with the full lock manager --- which depends on
 * LWLocks to protect its shared state.
 *
 * In addition to exclusive and shared modes, lightweight locks can be used to
 * wait until a variable changes value.  The variable is initially not set
 * when the lock is acquired with LWLockAcquire, i.e. it remains set to the
 * value it was set to when the lock was released last, and can be updated
 * without releasing the lock by calling LWLockUpdateVar.  LWLockWaitForVar
 * waits for the variable to be updated, or until the lock is free.  When
 * releasing the lock with LWLockReleaseClearVar() the value can be set to an
 * appropriate value for a free lock.  The meaning of the variable is up to
 * the caller, the lightweight lock code just assigns and compares it.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/lwlock.c
 *
 * NOTES:
 *
 * This used to be a pretty straight forward reader-writer lock
 * implementation, in which the internal state was protected by a
 * spinlock. Unfortunately the overhead of taking the spinlock proved to be
 * too high for workloads/locks that were taken in shared mode very
 * frequently. Often we were spinning in the (obviously exclusive) spinlock,
 * while trying to acquire a shared lock that was actually free.
 *
 * Thus a new implementation was devised that provides wait-free shared lock
 * acquisition for locks that aren't exclusively locked.
 *
 * The basic idea is to have a single atomic variable 'lockcount' instead of
 * the formerly separate shared and exclusive counters and to use atomic
 * operations to acquire the lock. That's fairly easy to do for plain
 * rw-spinlocks, but a lot harder for something like LWLocks that want to wait
 * in the OS.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the lockcount
 * variable. For exclusive lock we swap in a sentinel value
 * (LW_VAL_EXCLUSIVE), for shared locks we count the number of holders.
 *
 * To release the lock we use an atomic decrement to release the lock. If the
 * new value is zero (we get that atomically), we know we can/have to release
 * waiters.
 *
 * Obviously it is important that the sentinel value for exclusive locks
 * doesn't conflict with the maximum number of possible share lockers -
 * luckily MAX_BACKENDS makes that easily possible.
 *
 *
 * The attentive reader might have noticed that naively doing the above has a
 * glaring race condition: We try to lock using the atomic operations and
 * notice that we have to wait. Unfortunately by the time we have finished
 * queuing, the former locker very well might have already finished it's
 * work. That's problematic because we're now stuck waiting inside the OS.
 *
 * This race is avoided by taking a lock for the wait list using CAS with the
 * old state value, so it would only succeed if lock is still held. Necessary
 * flag is set using the same CAS.
 *
 * Inside LWLockConflictsWithVar the wait list lock is reused to protect the
 * variable value. First the lock is acquired to check the variable value, then
 * flags are set with a second CAS before queuing to the wait list in order to
 * ensure that the lock was not released yet.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "pg_trace.h"
#include "postmaster/postmaster.h"
#include "replication/slot.h"
#include "storage/ipc.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "utils/memutils.h"

#ifdef LWLOCK_STATS
#include "utils/hsearch.h"
#endif


/* We use the ShmemLock spinlock to protect LWLockCounter */
extern slock_t *ShmemLock;

#define LW_FLAG_HAS_WAITERS			((uint32) 1 << 30)
#define LW_FLAG_RELEASE_OK			((uint32) 1 << 29)
#define LW_FLAG_LOCKED				((uint32) 1 << 28)

#define LW_VAL_EXCLUSIVE			((uint32) 1 << 24)
#define LW_VAL_SHARED				1

#define LW_LOCK_MASK				((uint32) ((1 << 25)-1))
/* Must be greater than MAX_BACKENDS - which is 2^23-1, so we're fine. */
#define LW_SHARED_MASK				((uint32) ((1 << 24)-1))

/*
 * This is indexed by tranche ID and stores the names of all tranches known
 * to the current backend.
 */
static const char **LWLockTrancheArray = NULL;
static int	LWLockTranchesAllocated = 0;

#define T_NAME(lock) \
	(LWLockTrancheArray[(lock)->tranche])

/*
 * This points to the main array of LWLocks in shared memory.  Backends inherit
 * the pointer by fork from the postmaster (except in the EXEC_BACKEND case,
 * where we have special measures to pass it down).
 */
LWLockPadded *MainLWLockArray = NULL;

/*
 * We use this structure to keep track of locked LWLocks for release
 * during error recovery.  Normally, only a few will be held at once, but
 * occasionally the number can be much higher; for example, the pg_buffercache
 * extension locks all buffer partitions simultaneously.
 */
#define MAX_SIMUL_LWLOCKS	200

/* struct representing the LWLocks we're holding */
typedef struct LWLockHandle
{
	LWLock	   *lock;
	LWLockMode	mode;
} LWLockHandle;

static int	num_held_lwlocks = 0;
static LWLockHandle held_lwlocks[MAX_SIMUL_LWLOCKS];

/* struct representing the LWLock tranche request for named tranche */
typedef struct NamedLWLockTrancheRequest
{
	char		tranche_name[NAMEDATALEN];
	int			num_lwlocks;
} NamedLWLockTrancheRequest;

NamedLWLockTrancheRequest *NamedLWLockTrancheRequestArray = NULL;
static int	NamedLWLockTrancheRequestsAllocated = 0;
int			NamedLWLockTrancheRequests = 0;

NamedLWLockTranche *NamedLWLockTrancheArray = NULL;

static bool lock_named_request_allowed = true;

static void InitializeLWLocks(void);
static void RegisterLWLockTranches(void);

static inline void LWLockReportWaitStart(LWLock *lock);
static inline void LWLockReportWaitEnd(void);

#ifdef LWLOCK_STATS
typedef struct lwlock_stats_key
{
	int			tranche;
	void	   *instance;
}			lwlock_stats_key;

typedef struct lwlock_stats
{
	lwlock_stats_key key;
	int			sh_acquire_count;
	int			ex_acquire_count;
	int			block_count;
	int			spin_delay_count;
}			lwlock_stats;

static HTAB *lwlock_stats_htab;
static lwlock_stats lwlock_stats_dummy;
#endif

#ifdef LOCK_DEBUG
bool		Trace_lwlocks = false;

inline static void
PRINT_LWDEBUG(const char *where, LWLock *lock, LWLockMode mode)
{
	/* hide statement & context here, otherwise the log is just too verbose */
	if (Trace_lwlocks)
	{
		uint32		state = pg_atomic_read_u32(&lock->state);

		ereport(LOG,
				(errhidestmt(true),
				 errhidecontext(true),
				 errmsg_internal("%d: %s(%s %p): excl %u shared %u haswaiters %u waiters %u rOK %d",
								 MyProcPid,
								 where, T_NAME(lock), lock,
								 (state & LW_VAL_EXCLUSIVE) != 0,
								 state & LW_SHARED_MASK,
								 (state & LW_FLAG_HAS_WAITERS) != 0,
								 pg_atomic_read_u32(&lock->nwaiters),
								 (state & LW_FLAG_RELEASE_OK) != 0)));
	}
}

inline static void
LOG_LWDEBUG(const char *where, LWLock *lock, const char *msg)
{
	/* hide statement & context here, otherwise the log is just too verbose */
	if (Trace_lwlocks)
	{
		ereport(LOG,
				(errhidestmt(true),
				 errhidecontext(true),
				 errmsg_internal("%s(%s %p): %s", where,
								 T_NAME(lock), lock, msg)));
	}
}

#else							/* not LOCK_DEBUG */
#define PRINT_LWDEBUG(a,b,c) ((void)0)
#define LOG_LWDEBUG(a,b,c) ((void)0)
#endif							/* LOCK_DEBUG */

#ifdef LWLOCK_STATS

static void init_lwlock_stats(void);
static void print_lwlock_stats(int code, Datum arg);
static lwlock_stats * get_lwlock_stats_entry(LWLock *lockid);

static void
init_lwlock_stats(void)
{
	HASHCTL		ctl;
	static MemoryContext lwlock_stats_cxt = NULL;
	static bool exit_registered = false;

	if (lwlock_stats_cxt != NULL)
		MemoryContextDelete(lwlock_stats_cxt);

	/*
	 * The LWLock stats will be updated within a critical section, which
	 * requires allocating new hash entries. Allocations within a critical
	 * section are normally not allowed because running out of memory would
	 * lead to a PANIC, but LWLOCK_STATS is debugging code that's not normally
	 * turned on in production, so that's an acceptable risk. The hash entries
	 * are small, so the risk of running out of memory is minimal in practice.
	 */
	lwlock_stats_cxt = AllocSetContextCreate(TopMemoryContext,
											 "LWLock stats",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextAllowInCriticalSection(lwlock_stats_cxt, true);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(lwlock_stats_key);
	ctl.entrysize = sizeof(lwlock_stats);
	ctl.hcxt = lwlock_stats_cxt;
	lwlock_stats_htab = hash_create("lwlock stats", 16384, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	if (!exit_registered)
	{
		on_shmem_exit(print_lwlock_stats, 0);
		exit_registered = true;
	}
}

static void
print_lwlock_stats(int code, Datum arg)
{
	HASH_SEQ_STATUS scan;
	lwlock_stats *lwstats;

	hash_seq_init(&scan, lwlock_stats_htab);

	/* Grab an LWLock to keep different backends from mixing reports */
	LWLockAcquire(&MainLWLockArray[0].lock, LW_EXCLUSIVE);

	while ((lwstats = (lwlock_stats *) hash_seq_search(&scan)) != NULL)
	{
		fprintf(stderr,
				"PID %d lwlock %s %p: shacq %u exacq %u blk %u spindelay %u\n",
				MyProcPid, LWLockTrancheArray[lwstats->key.tranche],
				lwstats->key.instance, lwstats->sh_acquire_count,
				lwstats->ex_acquire_count, lwstats->block_count,
				lwstats->spin_delay_count);
	}

	LWLockRelease(&MainLWLockArray[0].lock);
}

static lwlock_stats *
get_lwlock_stats_entry(LWLock *lock)
{
	lwlock_stats_key key;
	lwlock_stats *lwstats;
	bool		found;

	/*
	 * During shared memory initialization, the hash table doesn't exist yet.
	 * Stats of that phase aren't very interesting, so just collect operations
	 * on all locks in a single dummy entry.
	 */
	if (lwlock_stats_htab == NULL)
		return &lwlock_stats_dummy;

	/* Fetch or create the entry. */
	key.tranche = lock->tranche;
	key.instance = lock;
	lwstats = hash_search(lwlock_stats_htab, &key, HASH_ENTER, &found);
	if (!found)
	{
		lwstats->sh_acquire_count = 0;
		lwstats->ex_acquire_count = 0;
		lwstats->block_count = 0;
		lwstats->spin_delay_count = 0;
	}
	return lwstats;
}

/* just shortcut to not declare lwlock_stats variable at the end of function */
static void
add_lwlock_stats_spin_stat(LWLock *lock, SpinDelayStatus *delayStatus)
{
	lwlock_stats *lwstats;

	lwstats = get_lwlock_stats_entry(lock);
	lwstats->spin_delay_count += delayStatus->delays;
}
#endif							/* LWLOCK_STATS */


/*
 * Compute number of LWLocks required by named tranches.  These will be
 * allocated in the main array.
 */
static int
NumLWLocksByNamedTranches(void)
{
	int			numLocks = 0;
	int			i;

	for (i = 0; i < NamedLWLockTrancheRequests; i++)
		numLocks += NamedLWLockTrancheRequestArray[i].num_lwlocks;

	return numLocks;
}

/*
 * Compute shmem space needed for LWLocks and named tranches.
 */
Size
LWLockShmemSize(void)
{
	Size		size;
	int			i;
	int			numLocks = NUM_FIXED_LWLOCKS;

	numLocks += NumLWLocksByNamedTranches();

	/* Space for the LWLock array. */
	size = mul_size(numLocks, sizeof(LWLockPadded));

	/* Space for dynamic allocation counter, plus room for alignment. */
	size = add_size(size, sizeof(int) + LWLOCK_PADDED_SIZE);

	/* space for named tranches. */
	size = add_size(size, mul_size(NamedLWLockTrancheRequests, sizeof(NamedLWLockTranche)));

	/* space for name of each tranche. */
	for (i = 0; i < NamedLWLockTrancheRequests; i++)
		size = add_size(size, strlen(NamedLWLockTrancheRequestArray[i].tranche_name) + 1);

	/* Disallow named LWLocks' requests after startup */
	lock_named_request_allowed = false;

	return size;
}

/*
 * Allocate shmem space for the main LWLock array and all tranches and
 * initialize it.  We also register all the LWLock tranches here.
 */
void
CreateLWLocks(void)
{
	StaticAssertStmt(LW_VAL_EXCLUSIVE > (uint32) MAX_BACKENDS,
					 "MAX_BACKENDS too big for lwlock.c");

	StaticAssertStmt(sizeof(LWLock) <= LWLOCK_MINIMAL_SIZE &&
					 sizeof(LWLock) <= LWLOCK_PADDED_SIZE,
					 "Miscalculated LWLock padding");

	if (!IsUnderPostmaster)
	{
		Size		spaceLocks = LWLockShmemSize();
		int		   *LWLockCounter;
		char	   *ptr;

		/* Allocate space */
		ptr = (char *) ShmemAlloc(spaceLocks);

		/* Leave room for dynamic allocation of tranches */
		ptr += sizeof(int);

		/* Ensure desired alignment of LWLock array */
		ptr += LWLOCK_PADDED_SIZE - ((uintptr_t) ptr) % LWLOCK_PADDED_SIZE;

		MainLWLockArray = (LWLockPadded *) ptr;

		/*
		 * Initialize the dynamic-allocation counter for tranches, which is
		 * stored just before the first LWLock.
		 */
		LWLockCounter = (int *) ((char *) MainLWLockArray - sizeof(int));
		*LWLockCounter = LWTRANCHE_FIRST_USER_DEFINED;

		/* Initialize all LWLocks */
		InitializeLWLocks();
	}

	/* Register all LWLock tranches */
	RegisterLWLockTranches();
}

/*
 * Initialize LWLocks that are fixed and those belonging to named tranches.
 */
static void
InitializeLWLocks(void)
{
	int			numNamedLocks = NumLWLocksByNamedTranches();
	int			id;
	int			i;
	int			j;
	LWLockPadded *lock;

	/* Initialize all individual LWLocks in main array */
	for (id = 0, lock = MainLWLockArray; id < NUM_INDIVIDUAL_LWLOCKS; id++, lock++)
		LWLockInitialize(&lock->lock, id);

	/* Initialize buffer mapping LWLocks in main array */
	lock = MainLWLockArray + NUM_INDIVIDUAL_LWLOCKS;
	for (id = 0; id < NUM_BUFFER_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_BUFFER_MAPPING);

	/* Initialize lmgrs' LWLocks in main array */
	lock = MainLWLockArray + NUM_INDIVIDUAL_LWLOCKS + NUM_BUFFER_PARTITIONS;
	for (id = 0; id < NUM_LOCK_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_LOCK_MANAGER);

	/* Initialize predicate lmgrs' LWLocks in main array */
	lock = MainLWLockArray + NUM_INDIVIDUAL_LWLOCKS +
		NUM_BUFFER_PARTITIONS + NUM_LOCK_PARTITIONS;
	for (id = 0; id < NUM_PREDICATELOCK_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_PREDICATE_LOCK_MANAGER);

	/* Initialize named tranches. */
	if (NamedLWLockTrancheRequests > 0)
	{
		char	   *trancheNames;

		NamedLWLockTrancheArray = (NamedLWLockTranche *)
			&MainLWLockArray[NUM_FIXED_LWLOCKS + numNamedLocks];

		trancheNames = (char *) NamedLWLockTrancheArray +
			(NamedLWLockTrancheRequests * sizeof(NamedLWLockTranche));
		lock = &MainLWLockArray[NUM_FIXED_LWLOCKS];

		for (i = 0; i < NamedLWLockTrancheRequests; i++)
		{
			NamedLWLockTrancheRequest *request;
			NamedLWLockTranche *tranche;
			char	   *name;

			request = &NamedLWLockTrancheRequestArray[i];
			tranche = &NamedLWLockTrancheArray[i];

			name = trancheNames;
			trancheNames += strlen(request->tranche_name) + 1;
			strcpy(name, request->tranche_name);
			tranche->trancheId = LWLockNewTrancheId();
			tranche->trancheName = name;

			for (j = 0; j < request->num_lwlocks; j++, lock++)
				LWLockInitialize(&lock->lock, tranche->trancheId);
		}
	}
}

/*
 * Register named tranches and tranches for fixed LWLocks.
 */
static void
RegisterLWLockTranches(void)
{
	int			i;

	if (LWLockTrancheArray == NULL)
	{
		LWLockTranchesAllocated = 128;
		LWLockTrancheArray = (const char **)
			MemoryContextAllocZero(TopMemoryContext,
								   LWLockTranchesAllocated * sizeof(char *));
		Assert(LWLockTranchesAllocated >= LWTRANCHE_FIRST_USER_DEFINED);
	}

	for (i = 0; i < NUM_INDIVIDUAL_LWLOCKS; ++i)
		LWLockRegisterTranche(i, MainLWLockNames[i]);

	LWLockRegisterTranche(LWTRANCHE_BUFFER_MAPPING, "buffer_mapping");
	LWLockRegisterTranche(LWTRANCHE_LOCK_MANAGER, "lock_manager");
	LWLockRegisterTranche(LWTRANCHE_PREDICATE_LOCK_MANAGER,
						  "predicate_lock_manager");
	LWLockRegisterTranche(LWTRANCHE_PARALLEL_QUERY_DSA,
						  "parallel_query_dsa");
	LWLockRegisterTranche(LWTRANCHE_SESSION_DSA,
						  "session_dsa");
	LWLockRegisterTranche(LWTRANCHE_SESSION_RECORD_TABLE,
						  "session_record_table");
	LWLockRegisterTranche(LWTRANCHE_SESSION_TYPMOD_TABLE,
						  "session_typmod_table");
	LWLockRegisterTranche(LWTRANCHE_SHARED_TUPLESTORE,
						  "shared_tuplestore");
	LWLockRegisterTranche(LWTRANCHE_TBM, "tbm");
	LWLockRegisterTranche(LWTRANCHE_PARALLEL_APPEND, "parallel_append");
	LWLockRegisterTranche(LWTRANCHE_PARALLEL_HASH_JOIN, "parallel_hash_join");

	/* Register named tranches. */
	for (i = 0; i < NamedLWLockTrancheRequests; i++)
		LWLockRegisterTranche(NamedLWLockTrancheArray[i].trancheId,
							  NamedLWLockTrancheArray[i].trancheName);
}

/*
 * InitLWLockAccess - initialize backend-local state needed to hold LWLocks
 */
void
InitLWLockAccess(void)
{
#ifdef LWLOCK_STATS
	init_lwlock_stats();
#endif
}

/*
 * GetNamedLWLockTranche - returns the base address of LWLock from the
 *		specified tranche.
 *
 * Caller needs to retrieve the requested number of LWLocks starting from
 * the base lock address returned by this API.  This can be used for
 * tranches that are requested by using RequestNamedLWLockTranche() API.
 */
LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
	int			lock_pos;
	int			i;

	/*
	 * Obtain the position of base address of LWLock belonging to requested
	 * tranche_name in MainLWLockArray.  LWLocks for named tranches are placed
	 * in MainLWLockArray after fixed locks.
	 */
	lock_pos = NUM_FIXED_LWLOCKS;
	for (i = 0; i < NamedLWLockTrancheRequests; i++)
	{
		if (strcmp(NamedLWLockTrancheRequestArray[i].tranche_name,
				   tranche_name) == 0)
			return &MainLWLockArray[lock_pos];

		lock_pos += NamedLWLockTrancheRequestArray[i].num_lwlocks;
	}

	if (i >= NamedLWLockTrancheRequests)
		elog(ERROR, "requested tranche is not registered");

	/* just to keep compiler quiet */
	return NULL;
}

/*
 * Allocate a new tranche ID.
 */
int
LWLockNewTrancheId(void)
{
	int			result;
	int		   *LWLockCounter;

	LWLockCounter = (int *) ((char *) MainLWLockArray - sizeof(int));
	SpinLockAcquire(ShmemLock);
	result = (*LWLockCounter)++;
	SpinLockRelease(ShmemLock);

	return result;
}

/*
 * Register a tranche ID in the lookup table for the current process.  This
 * routine will save a pointer to the tranche name passed as an argument,
 * so the name should be allocated in a backend-lifetime context
 * (TopMemoryContext, static variable, or similar).
 */
void
LWLockRegisterTranche(int tranche_id, const char *tranche_name)
{
	Assert(LWLockTrancheArray != NULL);

	if (tranche_id >= LWLockTranchesAllocated)
	{
		int			i = LWLockTranchesAllocated;
		int			j = LWLockTranchesAllocated;

		while (i <= tranche_id)
			i *= 2;

		LWLockTrancheArray = (const char **)
			repalloc(LWLockTrancheArray, i * sizeof(char *));
		LWLockTranchesAllocated = i;
		while (j < LWLockTranchesAllocated)
			LWLockTrancheArray[j++] = NULL;
	}

	LWLockTrancheArray[tranche_id] = tranche_name;
}

/*
 * RequestNamedLWLockTranche
 *		Request that extra LWLocks be allocated during postmaster
 *		startup.
 *
 * This is only useful for extensions if called from the _PG_init hook
 * of a library that is loaded into the postmaster via
 * shared_preload_libraries.  Once shared memory has been allocated, calls
 * will be ignored.  (We could raise an error, but it seems better to make
 * it a no-op, so that libraries containing such calls can be reloaded if
 * needed.)
 */
void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
	NamedLWLockTrancheRequest *request;

	if (IsUnderPostmaster || !lock_named_request_allowed)
		return;					/* too late */

	if (NamedLWLockTrancheRequestArray == NULL)
	{
		NamedLWLockTrancheRequestsAllocated = 16;
		NamedLWLockTrancheRequestArray = (NamedLWLockTrancheRequest *)
			MemoryContextAlloc(TopMemoryContext,
							   NamedLWLockTrancheRequestsAllocated
							   * sizeof(NamedLWLockTrancheRequest));
	}

	if (NamedLWLockTrancheRequests >= NamedLWLockTrancheRequestsAllocated)
	{
		int			i = NamedLWLockTrancheRequestsAllocated;

		while (i <= NamedLWLockTrancheRequests)
			i *= 2;

		NamedLWLockTrancheRequestArray = (NamedLWLockTrancheRequest *)
			repalloc(NamedLWLockTrancheRequestArray,
					 i * sizeof(NamedLWLockTrancheRequest));
		NamedLWLockTrancheRequestsAllocated = i;
	}

	request = &NamedLWLockTrancheRequestArray[NamedLWLockTrancheRequests];
	Assert(strlen(tranche_name) + 1 < NAMEDATALEN);
	StrNCpy(request->tranche_name, tranche_name, NAMEDATALEN);
	request->num_lwlocks = num_lwlocks;
	NamedLWLockTrancheRequests++;
}

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	pg_atomic_init_u32(&lock->state, LW_FLAG_RELEASE_OK);
#ifdef LOCK_DEBUG
	pg_atomic_init_u32(&lock->nwaiters, 0);
#endif
	lock->tranche = tranche_id;
	proclist_init(&lock->waiters);
}

/*
 * Report start of wait event for light-weight locks.
 *
 * This function will be used by all the light-weight lock calls which
 * needs to wait to acquire the lock.  This function distinguishes wait
 * event based on tranche and lock id.
 */
static inline void
LWLockReportWaitStart(LWLock *lock)
{
	pgstat_report_wait_start(PG_WAIT_LWLOCK | lock->tranche);
}

/*
 * Report end of wait event for light-weight locks.
 */
static inline void
LWLockReportWaitEnd(void)
{
	pgstat_report_wait_end();
}

/*
 * Return an identifier for an LWLock based on the wait class and event.
 */
const char *
GetLWLockIdentifier(uint32 classId, uint16 eventId)
{
	Assert(classId == PG_WAIT_LWLOCK);

	/*
	 * It is quite possible that user has registered tranche in one of the
	 * backends (e.g. by allocating lwlocks in dynamic shared memory) but not
	 * all of them, so we can't assume the tranche is registered here.
	 */
	if (eventId >= LWLockTranchesAllocated ||
		LWLockTrancheArray[eventId] == NULL)
		return "extension";

	return LWLockTrancheArray[eventId];
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode. If it could not grab the lock, it doesn't puts proc into wait
 * queue.
 *
 * It is used only in LWLockConditionalAcquire.
 *
 * Returns true if the lock isn't free.
 */
static bool
LWLockAttemptLockOnce(LWLock *lock, LWLockMode mode)
{
	uint32		old_state;
	uint32		mask,
				add;

	AssertArg(mode == LW_EXCLUSIVE || mode == LW_SHARED);

	if (mode == LW_EXCLUSIVE)
	{
		mask = LW_LOCK_MASK;
		add = LW_VAL_EXCLUSIVE;
	}
	else
	{
		mask = LW_VAL_EXCLUSIVE;
		add = LW_VAL_SHARED;
	}

	/*
	 * Read once outside the loop, later iterations will get the newer value
	 * via compare & exchange.
	 */
	old_state = pg_atomic_read_u32(&lock->state);

	/* loop until we've determined whether we could acquire the lock or not */
	while (true)
	{
		uint32		desired_state;
		bool		lock_free;

		desired_state = old_state;

		lock_free = (old_state & mask) == 0;
		if (lock_free)
		{
			desired_state += add;
			if (pg_atomic_compare_exchange_u32(&lock->state,
											   &old_state, desired_state))
			{
				/* Great! Got the lock. */
#ifdef LOCK_DEBUG
				if (mode == LW_EXCLUSIVE)
					lock->owner = MyProc;
#endif
				return false;
			}
		}
		else
		{
			/*
			 * This barrier is never needed for correctness, and it is no-op
			 * on x86. But probably first iteration of cas loop in
			 * ProcArrayGroupClearXid will succeed more often with it.
			 */
			pg_read_barrier();
			return true;
		}
	}
	pg_unreachable();
}

static void LWLockQueueSelfLocked(LWLock *lock, LWLockMode mode);

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode or put it self into waiting queue with waitmode.
 *
 * This function will not block waiting for a lock to become free - that's the
 * callers job.
 *
 * Returns true if the lock isn't free and we are in a wait queue.
 */
static bool
LWLockAttemptLockOrQueueSelf(LWLock *lock, LWLockMode mode, LWLockMode waitmode)
{
	uint32		old_state;
	SpinDelayStatus delayStatus;
	bool		lock_free;
	uint32		mask,
				add;
	int			skip_wait_list;

	AssertArg(mode == LW_EXCLUSIVE || mode == LW_SHARED);

	if (mode == LW_EXCLUSIVE)
	{
		mask = LW_LOCK_MASK;
		add = LW_VAL_EXCLUSIVE;
		skip_wait_list = spins_per_delay / 8;
	}
	else
	{
		mask = LW_VAL_EXCLUSIVE;
		add = LW_VAL_SHARED;
		skip_wait_list = spins_per_delay / 4;
	}

	init_local_spin_delay(&delayStatus);

	/*
	 * Read once outside the loop. Later iterations will get the newer value
	 * either via compare & exchange or with read after perform_spin_delay.
	 */
	old_state = pg_atomic_read_u32(&lock->state);
	/* loop until we've determined whether we could acquire the lock or not */
	while (true)
	{
		uint32		desired_state;

		desired_state = old_state;

		lock_free = (old_state & mask) == 0;
		if (lock_free)
		{
			desired_state += add;
			if (pg_atomic_compare_exchange_u32(&lock->state,
											   &old_state, desired_state))
			{
				/* Great! Got the lock. */
#ifdef LOCK_DEBUG
				if (mode == LW_EXCLUSIVE)
					lock->owner = MyProc;
#endif
				break;
			}
		}
		else if (delayStatus.spins > skip_wait_list && (old_state & LW_FLAG_LOCKED) == 0)
		{
			desired_state |= LW_FLAG_LOCKED | LW_FLAG_HAS_WAITERS;
			if (pg_atomic_compare_exchange_u32(&lock->state,
											   &old_state, desired_state))
			{
				LWLockQueueSelfLocked(lock, waitmode);
				break;
			}
		}
		else
		{
			perform_spin_delay(&delayStatus);
			old_state = pg_atomic_read_u32(&lock->state);
		}
	}
#ifdef LWLOCK_STATS
	add_lwlock_stats_spin_stat(lock, &delayStatus);
#endif

	/*
	 * We intentionally do not call finish_spin_delay here. Intuition tells,
	 * it should depends on kind of spin-lock (ie tranche), and should depend
	 * on was lock acquired without any sleep (inside of perform_spin_delay or
	 * due to queuing into wait list) or not. Until this approach
	 * investigated, benchmarks shows it is better to not update global
	 * spins_per_delay here, and rely on spin locks for tuning it.
	 */
	return !lock_free;
}

/*
 * Lock the LWLock's wait list against concurrent activity.
 *
 * NB: even though the wait list is locked, non-conflicting lock operations
 * may still happen concurrently.
 *
 * Time spent holding mutex should be short!
 */
static void
LWLockWaitListLock(LWLock *lock)
{
	uint32		old_state;
	SpinDelayStatus delayStatus;

	init_local_spin_delay(&delayStatus);
	while (true)
	{
		/* always try once to acquire lock directly */
		old_state = pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_LOCKED);
		if (!(old_state & LW_FLAG_LOCKED))
			break;				/* got lock */

		/* and then spin without atomic operations until lock is released */
		while (old_state & LW_FLAG_LOCKED)
		{
			perform_spin_delay(&delayStatus);
			old_state = pg_atomic_read_u32(&lock->state);
		}

		/*
		 * Retry. The lock might obviously already be re-acquired by the time
		 * we're attempting to get it again.
		 */
	}
	finish_spin_delay(&delayStatus);

#ifdef LWLOCK_STATS
	add_lwlock_stats_spin_stat(lock, &delayStatus);
#endif
}

/*
 * Unlock the LWLock's wait list.
 *
 * Note that it can be more efficient to manipulate flags and release the
 * locks in a single atomic operation.
 */
static void
LWLockWaitListUnlock(LWLock *lock)
{
	uint32		old_state PG_USED_FOR_ASSERTS_ONLY;

	old_state = pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_LOCKED);

	Assert(old_state & LW_FLAG_LOCKED);
}

/*
 * Wakeup all the lockers that currently have a chance to acquire the lock.
 */
static void
LWLockWakeup(LWLock *lock)
{
	bool		new_release_ok;
	bool		wokeup_somebody = false;
	proclist_head wakeup;
	proclist_mutable_iter iter;

	proclist_init(&wakeup);

	new_release_ok = true;

	/* lock wait list while collecting backends to wake up */
	LWLockWaitListLock(lock);

	proclist_foreach_modify(iter, &lock->waiters, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		if (wokeup_somebody && waiter->lwWaitMode == LW_EXCLUSIVE)
			continue;

		proclist_delete(&lock->waiters, iter.cur, lwWaitLink);
		proclist_push_tail(&wakeup, iter.cur, lwWaitLink);

		if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE)
		{
			/*
			 * Prevent additional wakeups until retryer gets to run. Backends
			 * that are just waiting for the lock to become free don't retry
			 * automatically.
			 */
			new_release_ok = false;

			/*
			 * Don't wakeup (further) exclusive locks.
			 */
			wokeup_somebody = true;
		}

		/*
		 * Once we've woken up an exclusive lock, there's no point in waking
		 * up anybody else.
		 */
		if (waiter->lwWaitMode == LW_EXCLUSIVE)
			break;
	}

	Assert(proclist_is_empty(&wakeup) || pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS);

	/* unset required flags, and release lock, in one fell swoop */
	{
		uint32		old_state;
		uint32		desired_state;

		old_state = pg_atomic_read_u32(&lock->state);
		while (true)
		{
			desired_state = old_state;

			/* compute desired flags */

			if (new_release_ok)
				desired_state |= LW_FLAG_RELEASE_OK;
			else
				desired_state &= ~LW_FLAG_RELEASE_OK;

			if (proclist_is_empty(&wakeup))
				desired_state &= ~LW_FLAG_HAS_WAITERS;

			desired_state &= ~LW_FLAG_LOCKED;	/* release lock */

			if (pg_atomic_compare_exchange_u32(&lock->state, &old_state,
											   desired_state))
				break;
		}
	}

	/* Awaken any waiters I removed from the queue. */
	proclist_foreach_modify(iter, &wakeup, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		LOG_LWDEBUG("LWLockRelease", lock, "release waiter");
		proclist_delete(&wakeup, iter.cur, lwWaitLink);

		/*
		 * Guarantee that lwWaiting being unset only becomes visible once the
		 * unlink from the link has completed. Otherwise the target backend
		 * could be woken up for other reason and enqueue for a new lock - if
		 * that happens before the list unlink happens, the list would end up
		 * being corrupted.
		 *
		 * The barrier pairs with the LWLockWaitListLock() when enqueuing for
		 * another lock.
		 */
		pg_write_barrier();
		waiter->lwWaiting = false;
		PGSemaphoreUnlock(waiter->sem);
	}
}

/*
 * Add ourselves to the end of the queue.
 * Lock should be held at this moment, and all neccessary flags are already
 * set. It will release wait list lock.
 *
 * NB: Mode can be LW_WAIT_UNTIL_FREE here!
 */
static void
LWLockQueueSelfLocked(LWLock *lock, LWLockMode mode)
{
	MyProc->lwWaiting = true;
	MyProc->lwWaitMode = mode;

	/* LW_WAIT_UNTIL_FREE waiters are always at the front of the queue */
	if (mode == LW_WAIT_UNTIL_FREE)
		proclist_push_head(&lock->waiters, MyProc->pgprocno, lwWaitLink);
	else
		proclist_push_tail(&lock->waiters, MyProc->pgprocno, lwWaitLink);

	/* Can release the mutex now */
	LWLockWaitListUnlock(lock);

#ifdef LOCK_DEBUG
	pg_atomic_fetch_add_u32(&lock->nwaiters, 1);
#endif

}

/*
 * LWLockAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, sleep until it is.  Returns true if the lock
 * was available immediately, false if we had to sleep.
 *
 * Side effect: cancel/die interrupts are held off until lock release.
 */
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	PGPROC	   *proc = MyProc;
	bool		result = true;
	int			extraWaits = 0;
	bool		mustwait;
#ifdef LWLOCK_STATS
	lwlock_stats *lwstats;

	lwstats = get_lwlock_stats_entry(lock);
#endif

	AssertArg(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	PRINT_LWDEBUG("LWLockAcquire", lock, mode);

#ifdef LWLOCK_STATS
	/* Count lock acquisition attempts */
	if (mode == LW_EXCLUSIVE)
		lwstats->ex_acquire_count++;
	else
		lwstats->sh_acquire_count++;
#endif							/* LWLOCK_STATS */

	/*
	 * We can't wait if we haven't got a PGPROC.  This should only occur
	 * during bootstrap or shared memory initialization.  Put an Assert here
	 * to catch unsafe coding practices.
	 */
	Assert(!(proc == NULL && IsUnderPostmaster));

	/* Ensure we will have room to remember the lock */
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		elog(ERROR, "too many LWLocks taken");

	/*
	 * Lock out cancel/die interrupts until we exit the code section protected
	 * by the LWLock.  This ensures that interrupts will not interfere with
	 * manipulations of data structures in shared memory.
	 */
	HOLD_INTERRUPTS();

	/*
	 * First try simpler function to acquire the lock. Most compilers are
	 * capable to inline it, and inlining gives measurable benefits if
	 * contention is low.
	 */
	mustwait = LWLockAttemptLockOnce(lock, mode);
	if (!mustwait)
	{
		LOG_LWDEBUG("LWLockAcquire", lock, "immediately acquired lock");
		goto acquired;			/* got the lock */
	}

	/*
	 * Loop here to try to acquire lock after each time we are signaled by
	 * LWLockRelease.
	 *
	 * NOTE: it might seem better to have LWLockRelease actually grant us the
	 * lock, rather than retrying and possibly having to go back to sleep. But
	 * in practice that is no good because it means a process swap for every
	 * lock acquisition when two or more processes are contending for the same
	 * lock.  Since LWLocks are normally used to protect not-very-long
	 * sections of computation, a process needs to be able to acquire and
	 * release the same lock many times during a single CPU time slice, even
	 * in the presence of contention.  The efficiency of being able to do that
	 * outweighs the inefficiency of sometimes wasting a process dispatch
	 * cycle because the lock is not free when a released waiter finally gets
	 * to run.  See pgsql-hackers archives for 29-Dec-01.
	 */
	for (;;)
	{
		mustwait = LWLockAttemptLockOrQueueSelf(lock, mode, mode);

		if (!mustwait)
		{
			LOG_LWDEBUG("LWLockAcquire", lock, "acquired lock");
			break;				/* got the lock */
		}

		/*
		 * Wait until awakened.
		 *
		 * At this point we are already in a wait queue. Since we share the
		 * process wait semaphore with the regular lock manager and
		 * ProcWaitForSignal, and we may need to acquire an LWLock while one
		 * of those is pending, it is possible that we get awakened for a
		 * reason other than being signaled by LWLockRelease. If so, loop back
		 * and wait again.  Once we've gotten the LWLock, re-increment the
		 * sema by the number of additional signals received, so that the lock
		 * manager or signal manager will see the received signal when it next
		 * waits.
		 */
		LOG_LWDEBUG("LWLockAcquire", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);

		for (;;)
		{
			PGSemaphoreLock(proc->sem);
			if (!proc->lwWaiting)
				break;
			extraWaits++;
		}

		/* Retrying, allow LWLockRelease to release waiters again. */
		pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

#ifdef LOCK_DEBUG
		{
			/* not waiting anymore */
			uint32		nwaiters PG_USED_FOR_ASSERTS_ONLY = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);

			Assert(nwaiters < MAX_BACKENDS);
		}
#endif

		TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
		LWLockReportWaitEnd();

		LOG_LWDEBUG("LWLockAcquire", lock, "awakened");

		/* Now loop back and try to acquire lock again. */
		result = false;
	}
acquired:

	TRACE_POSTGRESQL_LWLOCK_ACQUIRE(T_NAME(lock), mode);

	/* Add lock to list of locks held by this backend */
	held_lwlocks[num_held_lwlocks].lock = lock;
	held_lwlocks[num_held_lwlocks++].mode = mode;

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(proc->sem);

	return result;
}

/*
 * LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, return false with no side-effects.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	bool		mustwait;

	AssertArg(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	PRINT_LWDEBUG("LWLockConditionalAcquire", lock, mode);

	/* Ensure we will have room to remember the lock */
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		elog(ERROR, "too many LWLocks taken");

	/*
	 * Lock out cancel/die interrupts until we exit the code section protected
	 * by the LWLock.  This ensures that interrupts will not interfere with
	 * manipulations of data structures in shared memory.
	 */
	HOLD_INTERRUPTS();

	/* Check for the lock */
	mustwait = LWLockAttemptLockOnce(lock, mode);

	if (mustwait)
	{
		/* Failed to get lock, so release interrupt holdoff */
		RESUME_INTERRUPTS();

		LOG_LWDEBUG("LWLockConditionalAcquire", lock, "failed");
		TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(T_NAME(lock), mode);
	}
	else
	{
		/* Add lock to list of locks held by this backend */
		held_lwlocks[num_held_lwlocks].lock = lock;
		held_lwlocks[num_held_lwlocks++].mode = mode;
		TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(T_NAME(lock), mode);
	}
	return !mustwait;
}

/*
 * LWLockAcquireOrWait - Acquire lock, or wait until it's free
 *
 * The semantics of this function are a bit funky.  If the lock is currently
 * free, it is acquired in the given mode, and the function returns true.  If
 * the lock isn't immediately free, the function waits until it is released
 * and returns false, but does not acquire the lock.
 *
 * This is currently used for WALWriteLock: when a backend flushes the WAL,
 * holding WALWriteLock, it can flush the commit records of many other
 * backends as a side-effect.  Those other backends need to wait until the
 * flush finishes, but don't need to acquire the lock anymore.  They can just
 * wake up, observe that their records have already been flushed, and return.
 */
bool
LWLockAcquireOrWait(LWLock *lock, LWLockMode mode)
{
	PGPROC	   *proc = MyProc;
	bool		mustwait;
	int			extraWaits = 0;
#ifdef LWLOCK_STATS
	lwlock_stats *lwstats;

	lwstats = get_lwlock_stats_entry(lock);
#endif

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

	PRINT_LWDEBUG("LWLockAcquireOrWait", lock, mode);

	/* Ensure we will have room to remember the lock */
	if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
		elog(ERROR, "too many LWLocks taken");

	/*
	 * Lock out cancel/die interrupts until we exit the code section protected
	 * by the LWLock.  This ensures that interrupts will not interfere with
	 * manipulations of data structures in shared memory.
	 */
	HOLD_INTERRUPTS();

	mustwait = LWLockAttemptLockOrQueueSelf(lock, mode, LW_WAIT_UNTIL_FREE);

	if (mustwait)
	{
		/*
		 * Wait until awakened.  Like in LWLockAcquire, be prepared for bogus
		 * wakeups, because we share the semaphore with ProcWaitForSignal.
		 */
		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);

		for (;;)
		{
			PGSemaphoreLock(proc->sem);
			if (!proc->lwWaiting)
				break;
			extraWaits++;
		}

#ifdef LOCK_DEBUG
		{
			/* not waiting anymore */
			uint32		nwaiters PG_USED_FOR_ASSERTS_ONLY = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);

			Assert(nwaiters < MAX_BACKENDS);
		}
#endif
		TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
		LWLockReportWaitEnd();

		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "awakened");
	}

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(proc->sem);

	if (mustwait)
	{
		/* Failed to get lock, so release interrupt holdoff */
		RESUME_INTERRUPTS();
		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "failed");
		TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL(T_NAME(lock), mode);
	}
	else
	{
		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "succeeded");
		/* Add lock to list of locks held by this backend */
		held_lwlocks[num_held_lwlocks].lock = lock;
		held_lwlocks[num_held_lwlocks++].mode = mode;
		TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT(T_NAME(lock), mode);
	}

	return !mustwait;
}

/*
 * Does the lwlock in its current state need to wait for the variable value to
 * change?
 *
 * If we don't need to wait, and it's because the value of the variable has
 * changed, store the current value in newval.
 *
 * *result is set to true if the lock was free, and false otherwise.
 */
static bool
LWLockConflictsWithVar(LWLock *lock,
					   uint64 *valptr, uint64 oldval, uint64 *newval,
					   bool *result)
{
	bool		mustwait;
	uint64		value;
	uint32		state;
	SpinDelayStatus delayStatus;
	int			skip_wait_list;

	init_local_spin_delay(&delayStatus);

	skip_wait_list = spins_per_delay / 4;

	/*
	 * We are trying to detect exclusive lock on state, or value change. If
	 * neither happens, we put ourself into wait queue.
	 *
	 * Read value using the lwlock's wait list lock, as we can't generally
	 * rely on atomic 64 bit reads/stores.  TODO: On platforms with a way to
	 * do atomic 64 bit reads/writes the spinlock should be optimized away.
	 */
	while (true)
	{
		state = pg_atomic_read_u32(&lock->state);
		mustwait = (state & LW_VAL_EXCLUSIVE) != 0;

		if (!mustwait)
		{
			*result = true;
			goto exit;
		}

		if (delayStatus.spins > skip_wait_list && (state & LW_FLAG_LOCKED) == 0)
		{
			if (pg_atomic_compare_exchange_u32(&lock->state, &state,
											   state | LW_FLAG_LOCKED))
			{
				state |= LW_FLAG_LOCKED;
				break;
			}
		}
		perform_spin_delay(&delayStatus);
	}

	/* Now we've locked wait queue, and someone holds EXCLUSIVE lock. */
	value = *valptr;
	mustwait = value == oldval;
	if (!mustwait)
	{
		/* value changed, no need to block */
		LWLockWaitListUnlock(lock);
		*newval = value;
		goto exit;
	}

	/*
	 * If value didn't change, we have to set LW_FLAG_HAS_WAITERS and
	 * LW_FLAG_RELEASE_OK flags and put ourself into wait queue. We could find
	 * that the lock is not held anymore, in this case we should return
	 * without blocking.
	 *
	 * Note: value could not change again cause we are holding WaitList lock.
	 */
	while (true)
	{
		uint32		desired_state;

		desired_state = state | LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK;
		if (pg_atomic_compare_exchange_u32(&lock->state, &state,
										   desired_state))
		{
			/*
			 * we satisfy invariant: flags should be set while someone holds
			 * the lock
			 */
			LWLockQueueSelfLocked(lock, LW_WAIT_UNTIL_FREE);
			break;
		}
		if ((state & LW_VAL_EXCLUSIVE) == 0)
		{
			/* lock was released, no need to block anymore */
			LWLockWaitListUnlock(lock);
			*result = true;
			mustwait = false;
			break;
		}
	}
exit:
#ifdef LWLOCK_STATS
	add_lwlock_stats_spin_stat(lock, &delayStatus);
#endif
	finish_spin_delay(&delayStatus);
	return mustwait;
}

/*
 * LWLockWaitForVar - Wait until lock is free, or a variable is updated.
 *
 * If the lock is held and *valptr equals oldval, waits until the lock is
 * either freed, or the lock holder updates *valptr by calling
 * LWLockUpdateVar.  If the lock is free on exit (immediately or after
 * waiting), returns true.  If the lock is still held, but *valptr no longer
 * matches oldval, returns false and sets *newval to the current value in
 * *valptr.
 *
 * Note: this function ignores shared lock holders; if the lock is held
 * in shared mode, returns 'true'.
 */
bool
LWLockWaitForVar(LWLock *lock, uint64 *valptr, uint64 oldval, uint64 *newval)
{
	PGPROC	   *proc = MyProc;
	int			extraWaits = 0;
	bool		result = false;
#ifdef LWLOCK_STATS
	lwlock_stats *lwstats;

	lwstats = get_lwlock_stats_entry(lock);
#endif

	PRINT_LWDEBUG("LWLockWaitForVar", lock, LW_WAIT_UNTIL_FREE);

	/*
	 * Lock out cancel/die interrupts while we sleep on the lock.  There is no
	 * cleanup mechanism to remove us from the wait queue if we got
	 * interrupted.
	 */
	HOLD_INTERRUPTS();

	/*
	 * Loop here to check the lock's status after each time we are signaled.
	 */
	for (;;)
	{
		bool		mustwait;

		mustwait = LWLockConflictsWithVar(lock, valptr, oldval, newval,
										  &result);

		if (!mustwait)
			break;				/* the lock was free or value didn't match */

		/*
		 * Wait until awakened.
		 *
		 * Since we share the process wait semaphore with the regular lock
		 * manager and ProcWaitForSignal, and we may need to acquire an LWLock
		 * while one of those is pending, it is possible that we get awakened
		 * for a reason other than being signaled by LWLockRelease. If so,
		 * loop back and wait again.  Once we've gotten the LWLock,
		 * re-increment the sema by the number of additional signals received,
		 * so that the lock manager or signal manager will see the received
		 * signal when it next waits.
		 */
		LOG_LWDEBUG("LWLockWaitForVar", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), LW_EXCLUSIVE);

		for (;;)
		{
			PGSemaphoreLock(proc->sem);
			if (!proc->lwWaiting)
				break;
			extraWaits++;
		}

#ifdef LOCK_DEBUG
		{
			/* not waiting anymore */
			uint32		nwaiters PG_USED_FOR_ASSERTS_ONLY = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);

			Assert(nwaiters < MAX_BACKENDS);
		}
#endif

		TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), LW_EXCLUSIVE);
		LWLockReportWaitEnd();

		LOG_LWDEBUG("LWLockWaitForVar", lock, "awakened");

		/* Now loop back and check the status of the lock again. */
	}

	TRACE_POSTGRESQL_LWLOCK_ACQUIRE(T_NAME(lock), LW_EXCLUSIVE);

	/*
	 * Fix the process wait semaphore's count for any absorbed wakeups.
	 */
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(proc->sem);

	/*
	 * Now okay to allow cancel/die interrupts.
	 */
	RESUME_INTERRUPTS();

	return result;
}


/*
 * LWLockUpdateVar - Update a variable and wake up waiters atomically
 *
 * Sets *valptr to 'val', and wakes up all processes waiting for us with
 * LWLockWaitForVar().  Setting the value and waking up the processes happen
 * atomically so that any process calling LWLockWaitForVar() on the same lock
 * is guaranteed to see the new value, and act accordingly.
 *
 * The caller must be holding the lock in exclusive mode.
 */
void
LWLockUpdateVar(LWLock *lock, uint64 *valptr, uint64 val)
{
	proclist_head wakeup;
	proclist_mutable_iter iter;

	PRINT_LWDEBUG("LWLockUpdateVar", lock, LW_EXCLUSIVE);

	proclist_init(&wakeup);

	LWLockWaitListLock(lock);

	Assert(pg_atomic_read_u32(&lock->state) & LW_VAL_EXCLUSIVE);

	/* Update the lock's value */
	*valptr = val;

	/*
	 * See if there are any LW_WAIT_UNTIL_FREE waiters that need to be woken
	 * up. They are always in the front of the queue.
	 */
	proclist_foreach_modify(iter, &lock->waiters, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE)
			break;

		proclist_delete(&lock->waiters, iter.cur, lwWaitLink);
		proclist_push_tail(&wakeup, iter.cur, lwWaitLink);
	}

	/* We are done updating shared state of the lock itself. */
	LWLockWaitListUnlock(lock);

	/*
	 * Awaken any waiters I removed from the queue.
	 */
	proclist_foreach_modify(iter, &wakeup, lwWaitLink)
	{
		PGPROC	   *waiter = GetPGProcByNumber(iter.cur);

		proclist_delete(&wakeup, iter.cur, lwWaitLink);
		/* check comment in LWLockWakeup() about this barrier */
		pg_write_barrier();
		waiter->lwWaiting = false;
		PGSemaphoreUnlock(waiter->sem);
	}
}


/*
 * LWLockRelease - release a previously acquired lock
 */
void
LWLockRelease(LWLock *lock)
{
	LWLockMode	mode;
	uint32		oldstate;
	bool		check_waiters;
	int			i;

	/*
	 * Remove lock from list of locks held.  Usually, but not always, it will
	 * be the latest-acquired lock; so search array backwards.
	 */
	for (i = num_held_lwlocks; --i >= 0;)
		if (lock == held_lwlocks[i].lock)
			break;

	if (i < 0)
		elog(ERROR, "lock %s is not held", T_NAME(lock));

	mode = held_lwlocks[i].mode;

	num_held_lwlocks--;
	for (; i < num_held_lwlocks; i++)
		held_lwlocks[i] = held_lwlocks[i + 1];

	PRINT_LWDEBUG("LWLockRelease", lock, mode);

	/*
	 * Release my hold on lock, after that it can immediately be acquired by
	 * others, even if we still have to wakeup other waiters.
	 */
	if (mode == LW_EXCLUSIVE)
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);
	else
		oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);

	/* nobody else can have that kind of lock */
	Assert(!(oldstate & LW_VAL_EXCLUSIVE));


	/*
	 * We're still waiting for backends to get scheduled, don't wake them up
	 * again.
	 */
	if ((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) ==
		(LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK) &&
		(oldstate & LW_LOCK_MASK) == 0)
		check_waiters = true;
	else
		check_waiters = false;

	/*
	 * As waking up waiters requires the spinlock to be acquired, only do so
	 * if necessary.
	 */
	if (check_waiters)
	{
		/* XXX: remove before commit? */
		LOG_LWDEBUG("LWLockRelease", lock, "releasing waiters");
		LWLockWakeup(lock);
	}

	TRACE_POSTGRESQL_LWLOCK_RELEASE(T_NAME(lock));

	/*
	 * Now okay to allow cancel/die interrupts.
	 */
	RESUME_INTERRUPTS();
}

/*
 * LWLockReleaseClearVar - release a previously acquired lock, reset variable
 */
void
LWLockReleaseClearVar(LWLock *lock, uint64 *valptr, uint64 val)
{
	LWLockWaitListLock(lock);

	/*
	 * Set the variable's value before releasing the lock, that prevents race
	 * a race condition wherein a new locker acquires the lock, but hasn't yet
	 * set the variables value.
	 */
	*valptr = val;
	LWLockWaitListUnlock(lock);

	LWLockRelease(lock);
}


/*
 * LWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail LWLockRelease calls is that InterruptHoldoffCount is
 * unchanged by this operation.  This is necessary since InterruptHoldoffCount
 * has been set to an appropriate level earlier in error recovery. We could
 * decrement it below zero if we allow it to drop for each released lock!
 */
void
LWLockReleaseAll(void)
{
	while (num_held_lwlocks > 0)
	{
		HOLD_INTERRUPTS();		/* match the upcoming RESUME_INTERRUPTS */

		LWLockRelease(held_lwlocks[num_held_lwlocks - 1].lock);
	}
}


/*
 * LWLockHeldByMe - test whether my process holds a lock in any mode
 *
 * This is meant as debug support only.
 */
bool
LWLockHeldByMe(LWLock *l)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i].lock == l)
			return true;
	}
	return false;
}

/*
 * LWLockHeldByMeInMode - test whether my process holds a lock in given mode
 *
 * This is meant as debug support only.
 */
bool
LWLockHeldByMeInMode(LWLock *l, LWLockMode mode)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i].lock == l && held_lwlocks[i].mode == mode)
			return true;
	}
	return false;
}
