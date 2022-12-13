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
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
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

 * To mitigate those races we use a two phased attempt at locking:
 *	 Phase 1: Try to do it atomically, if we succeed, nice
 *	 Phase 2: Add ourselves to the waitqueue of the lock
 *	 Phase 3: Try to grab the lock again, if we succeed, remove ourselves from
 *			  the queue
 *	 Phase 4: Sleep till wake-up, goto Phase 1
 *
 * This protects us against the problem from above as nobody can release too
 *	  quick, before we're queued, since after Phase 2 we're already queued.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "port/pg_bitutils.h"
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

/* 64-bit LWlock state layout is following:
 * 18-bit waiting list head (number of backend in procarray)
 * 18-bit waiting list tail (number of backend in procarray)
 * LWlock state flags
 * 19-bit LW_LOCK_MASK
 */
#define LW_WAITERS_LIST_MASK 		((uint64) 0xFFFFFFFFF0000000)
#define LW_WAITERS_HEAD_MASK		((uint64) 0xFFFFC00000000000)
#define LW_WAITERS_TAIL_MASK		((uint64) 0x00003FFFF0000000)
#define LW_FLAG_WAKE_UP_LOCK		((uint64) 1 << 22)
#define LW_FLAG_RELEASE_OK			((uint64) 1 << 21)	/* Wake up waiters only if set */
#define LW_FLAG_LOCK_VAR			((uint64) 1 << 20)

#define LW_VAL_EXCLUSIVE			((uint64) 1 << 19)
#define LW_VAL_SHARED				1

#define LW_LOCK_MASK				((uint64) ((1 << 20)-1))
/* Must be greater than MAX_BACKENDS - which is 2^18-1, so we're fine. */
#define LW_SHARED_MASK				((uint64) ((1 << 19)-1))

/* Access macros for procarray waiters queue */
#define CurWaitMode(pgprocno) 		(GetPGProcByNumber(pgprocno)->lwWaitMode)
#define NextWaitLink(pgprocno) 		(GetPGProcByNumber(pgprocno)->lwWaitLink)
#define NextReleaseLink(pgprocno) 	(GetPGProcByNumber(pgprocno)->lwReleaseLink)
#define CurIsWaiting(pgprocno) 		(GetPGProcByNumber(pgprocno)->lwWaiting)
#define CurSem(pgprocno)			(GetPGProcByNumber(pgprocno)->sem)

/* Access macros for LWlock state */
#define INVALID_LOCK_PROCNO			((uint32) 0x3FFFF)
#define LW_GET_WAIT_HEAD(state)		((state) >> 46)
#define LW_SET_WAIT_HEAD(state, waiter) (((uint64)(state) & (~LW_WAITERS_HEAD_MASK)) | ((uint64)(waiter) << 46))
#define LW_GET_WAIT_TAIL(state)		(((uint64)(state) & LW_WAITERS_TAIL_MASK) >> 28)
#define LW_SET_WAIT_TAIL(state, waiter) (((uint64)(state) & (~LW_WAITERS_TAIL_MASK)) | ((uint64)(waiter) << 28))
#define LW_HAS_WAITERS(state)		(LW_GET_WAIT_HEAD((state)) != INVALID_LOCK_PROCNO)
#define LW_SET_STATE(state, head, tail) (((uint64)(state) & (~LW_WAITERS_LIST_MASK)) | ((uint64)(head) << 46) | ((uint64)(tail) << 28))

/*
 * There are three sorts of LWLock "tranches":
 *
 * 1. The individually-named locks defined in lwlocknames.h each have their
 * own tranche.  The names of these tranches appear in IndividualLWLockNames[]
 * in lwlocknames.c.
 *
 * 2. There are some predefined tranches for built-in groups of locks.
 * These are listed in enum BuiltinTrancheIds in lwlock.h, and their names
 * appear in BuiltinTrancheNames[] below.
 *
 * 3. Extensions can create new tranches, via either RequestNamedLWLockTranche
 * or LWLockRegisterTranche.  The names of these that are known in the current
 * process appear in LWLockTrancheNames[].
 *
 * All these names are user-visible as wait event names, so choose with care
 * ... and do not forget to update the documentation's list of wait events.
 */
extern const char *const IndividualLWLockNames[];	/* in lwlocknames.c */

static const char *const BuiltinTrancheNames[] = {
	/* LWTRANCHE_XACT_BUFFER: */
	"XactBuffer",
	/* LWTRANCHE_COMMITTS_BUFFER: */
	"CommitTSBuffer",
	/* LWTRANCHE_SUBTRANS_BUFFER: */
	"SubtransBuffer",
	/* LWTRANCHE_MULTIXACTOFFSET_BUFFER: */
	"MultiXactOffsetBuffer",
	/* LWTRANCHE_MULTIXACTMEMBER_BUFFER: */
	"MultiXactMemberBuffer",
	/* LWTRANCHE_NOTIFY_BUFFER: */
	"NotifyBuffer",
	/* LWTRANCHE_SERIAL_BUFFER: */
	"SerialBuffer",
	/* LWTRANCHE_WAL_INSERT: */
	"WALInsert",
	/* LWTRANCHE_BUFFER_CONTENT: */
	"BufferContent",
	/* LWTRANCHE_REPLICATION_ORIGIN_STATE: */
	"ReplicationOriginState",
	/* LWTRANCHE_REPLICATION_SLOT_IO: */
	"ReplicationSlotIO",
	/* LWTRANCHE_LOCK_FASTPATH: */
	"LockFastPath",
	/* LWTRANCHE_BUFFER_MAPPING: */
	"BufferMapping",
	/* LWTRANCHE_LOCK_MANAGER: */
	"LockManager",
	/* LWTRANCHE_PREDICATE_LOCK_MANAGER: */
	"PredicateLockManager",
	/* LWTRANCHE_PARALLEL_HASH_JOIN: */
	"ParallelHashJoin",
	/* LWTRANCHE_PARALLEL_QUERY_DSA: */
	"ParallelQueryDSA",
	/* LWTRANCHE_PER_SESSION_DSA: */
	"PerSessionDSA",
	/* LWTRANCHE_PER_SESSION_RECORD_TYPE: */
	"PerSessionRecordType",
	/* LWTRANCHE_PER_SESSION_RECORD_TYPMOD: */
	"PerSessionRecordTypmod",
	/* LWTRANCHE_SHARED_TUPLESTORE: */
	"SharedTupleStore",
	/* LWTRANCHE_SHARED_TIDBITMAP: */
	"SharedTidBitmap",
	/* LWTRANCHE_PARALLEL_APPEND: */
	"ParallelAppend",
	/* LWTRANCHE_PER_XACT_PREDICATE_LIST: */
	"PerXactPredicateList",
	/* LWTRANCHE_PGSTATS_DSA: */
	"PgStatsDSA",
	/* LWTRANCHE_PGSTATS_HASH: */
	"PgStatsHash",
	/* LWTRANCHE_PGSTATS_DATA: */
	"PgStatsData",
};

StaticAssertDecl(lengthof(BuiltinTrancheNames) ==
				 LWTRANCHE_FIRST_USER_DEFINED - NUM_INDIVIDUAL_LWLOCKS,
				 "missing entries in BuiltinTrancheNames[]");

/*
 * This is indexed by tranche ID minus LWTRANCHE_FIRST_USER_DEFINED, and
 * stores the names of all dynamically-created tranches known to the current
 * process.  Any unused entries in the array will contain NULL.
 */
static const char **LWLockTrancheNames = NULL;
static int	LWLockTrancheNamesAllocated = 0;

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

static NamedLWLockTrancheRequest *NamedLWLockTrancheRequestArray = NULL;
static int	NamedLWLockTrancheRequestsAllocated = 0;

/*
 * NamedLWLockTrancheRequests is both the valid length of the request array,
 * and the length of the shared-memory NamedLWLockTrancheArray later on.
 * This variable and NamedLWLockTrancheArray are non-static so that
 * postmaster.c can copy them to child processes in EXEC_BACKEND builds.
 */
int			NamedLWLockTrancheRequests = 0;

/* points to data in shared memory: */
NamedLWLockTranche *NamedLWLockTrancheArray = NULL;

static void InitializeLWLocks(void);
static inline void LWLockReportWaitStart(LWLock *lock);
static inline void LWLockReportWaitEnd(void);
static const char *GetLWTrancheName(uint16 trancheId);

#define T_NAME(lock) \
	GetLWTrancheName((lock)->tranche)

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
	int			dequeue_self_count;
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
								 LW_HAS_WAITERS(state),
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
static lwlock_stats * get_lwlock_stats_entry(LWLock *lock);

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
				"PID %d lwlock %s %p: shacq %u exacq %u blk %u spindelay %u dequeue self %u\n",
				MyProcPid, GetLWTrancheName(lwstats->key.tranche),
				lwstats->key.instance, lwstats->sh_acquire_count,
				lwstats->ex_acquire_count, lwstats->block_count,
				lwstats->spin_delay_count, lwstats->dequeue_self_count);
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
	MemSet(&key, 0, sizeof(key));
	key.tranche = lock->tranche;
	key.instance = lock;
	lwstats = hash_search(lwlock_stats_htab, &key, HASH_ENTER, &found);
	if (!found)
	{
		lwstats->sh_acquire_count = 0;
		lwstats->ex_acquire_count = 0;
		lwstats->block_count = 0;
		lwstats->dequeue_self_count = 0;
		lwstats->spin_delay_count = 0;
	}
	return lwstats;
}
#endif							/* LWLOCK_STATS */


/*
 * Compute number of LWLocks required by named tranches.  These will be
 * allocated in the main array.
 */
static int
NumLWLocksForNamedTranches(void)
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

	/* Calculate total number of locks needed in the main array. */
	numLocks += NumLWLocksForNamedTranches();

	/* Space for the LWLock array. */
	size = mul_size(numLocks, sizeof(LWLockPadded));

	/* Space for dynamic allocation counter, plus room for alignment. */
	size = add_size(size, sizeof(int) + LWLOCK_PADDED_SIZE);

	/* space for named tranches. */
	size = add_size(size, mul_size(NamedLWLockTrancheRequests, sizeof(NamedLWLockTranche)));

	/* space for name of each tranche. */
	for (i = 0; i < NamedLWLockTrancheRequests; i++)
		size = add_size(size, strlen(NamedLWLockTrancheRequestArray[i].tranche_name) + 1);

	return size;
}

/*
 * Allocate shmem space for the main LWLock array and all tranches and
 * initialize it.  We also register extension LWLock tranches here.
 */
void
CreateLWLocks(void)
{
	StaticAssertStmt(LW_VAL_EXCLUSIVE > (uint32) MAX_BACKENDS,
					 "MAX_BACKENDS too big for lwlock.c");

	StaticAssertStmt(sizeof(LWLock) <= LWLOCK_PADDED_SIZE,
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

	/* Register named extension LWLock tranches in the current process. */
	for (int i = 0; i < NamedLWLockTrancheRequests; i++)
		LWLockRegisterTranche(NamedLWLockTrancheArray[i].trancheId,
							  NamedLWLockTrancheArray[i].trancheName);
}

/*
 * Initialize LWLocks that are fixed and those belonging to named tranches.
 */
static void
InitializeLWLocks(void)
{
	int			numNamedLocks = NumLWLocksForNamedTranches();
	int			id;
	int			i;
	int			j;
	LWLockPadded *lock;

	/* Initialize all individual LWLocks in main array */
	for (id = 0, lock = MainLWLockArray; id < NUM_INDIVIDUAL_LWLOCKS; id++, lock++)
		LWLockInitialize(&lock->lock, id);

	/* Initialize buffer mapping LWLocks in main array */
	lock = MainLWLockArray + BUFFER_MAPPING_LWLOCK_OFFSET;
	for (id = 0; id < NUM_BUFFER_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_BUFFER_MAPPING);

	/* Initialize lmgrs' LWLocks in main array */
	lock = MainLWLockArray + LOCK_MANAGER_LWLOCK_OFFSET;
	for (id = 0; id < NUM_LOCK_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_LOCK_MANAGER);

	/* Initialize predicate lmgrs' LWLocks in main array */
	lock = MainLWLockArray + PREDICATELOCK_MANAGER_LWLOCK_OFFSET;
	for (id = 0; id < NUM_PREDICATELOCK_PARTITIONS; id++, lock++)
		LWLockInitialize(&lock->lock, LWTRANCHE_PREDICATE_LOCK_MANAGER);

	/*
	 * Copy the info about any named tranches into shared memory (so that
	 * other processes can see it), and initialize the requested LWLocks.
	 */
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
 * Register a dynamic tranche name in the lookup table of the current process.
 *
 * This routine will save a pointer to the tranche name passed as an argument,
 * so the name should be allocated in a backend-lifetime context
 * (shared memory, TopMemoryContext, static constant, or similar).
 *
 * The tranche name will be user-visible as a wait event name, so try to
 * use a name that fits the style for those.
 */
void
LWLockRegisterTranche(int tranche_id, const char *tranche_name)
{
	/* This should only be called for user-defined tranches. */
	if (tranche_id < LWTRANCHE_FIRST_USER_DEFINED)
		return;

	/* Convert to array index. */
	tranche_id -= LWTRANCHE_FIRST_USER_DEFINED;

	/* If necessary, create or enlarge array. */
	if (tranche_id >= LWLockTrancheNamesAllocated)
	{
		int			newalloc;

		newalloc = pg_nextpower2_32(Max(8, tranche_id + 1));

		if (LWLockTrancheNames == NULL)
			LWLockTrancheNames = (const char **)
				MemoryContextAllocZero(TopMemoryContext,
									   newalloc * sizeof(char *));
		else
			LWLockTrancheNames =
				repalloc0_array(LWLockTrancheNames, const char *, LWLockTrancheNamesAllocated, newalloc);
		LWLockTrancheNamesAllocated = newalloc;
	}

	LWLockTrancheNames[tranche_id] = tranche_name;
}

/*
 * RequestNamedLWLockTranche
 *		Request that extra LWLocks be allocated during postmaster
 *		startup.
 *
 * This may only be called via the shmem_request_hook of a library that is
 * loaded into the postmaster via shared_preload_libraries.  Calls from
 * elsewhere will fail.
 *
 * The tranche name will be user-visible as a wait event name, so try to
 * use a name that fits the style for those.
 */
void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
	NamedLWLockTrancheRequest *request;

	if (!process_shmem_requests_in_progress)
		elog(FATAL, "cannot request additional LWLocks outside shmem_request_hook");

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
		int			i = pg_nextpower2_32(NamedLWLockTrancheRequests + 1);

		NamedLWLockTrancheRequestArray = (NamedLWLockTrancheRequest *)
			repalloc(NamedLWLockTrancheRequestArray,
					 i * sizeof(NamedLWLockTrancheRequest));
		NamedLWLockTrancheRequestsAllocated = i;
	}

	request = &NamedLWLockTrancheRequestArray[NamedLWLockTrancheRequests];
	Assert(strlen(tranche_name) + 1 <= NAMEDATALEN);
	strlcpy(request->tranche_name, tranche_name, NAMEDATALEN);
	request->num_lwlocks = num_lwlocks;
	NamedLWLockTrancheRequests++;
}

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	pg_atomic_init_u64(&lock->state, LW_SET_STATE(LW_FLAG_RELEASE_OK, INVALID_LOCK_PROCNO, INVALID_LOCK_PROCNO));
#ifdef LOCK_DEBUG
	pg_atomic_init_u32(&lock->nwaiters, 0);
#endif
	lock->tranche = tranche_id;
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
 * Return the name of an LWLock tranche.
 */
static const char *
GetLWTrancheName(uint16 trancheId)
{
	/* Individual LWLock? */
	if (trancheId < NUM_INDIVIDUAL_LWLOCKS)
		return IndividualLWLockNames[trancheId];

	/* Built-in tranche? */
	if (trancheId < LWTRANCHE_FIRST_USER_DEFINED)
		return BuiltinTrancheNames[trancheId - NUM_INDIVIDUAL_LWLOCKS];

	/*
	 * It's an extension tranche, so look in LWLockTrancheNames[].  However,
	 * it's possible that the tranche has never been registered in the current
	 * process, in which case give up and return "extension".
	 */
	trancheId -= LWTRANCHE_FIRST_USER_DEFINED;

	if (trancheId >= LWLockTrancheNamesAllocated ||
		LWLockTrancheNames[trancheId] == NULL)
		return "extension";

	return LWLockTrancheNames[trancheId];
}

/*
 * Return an identifier for an LWLock based on the wait class and event.
 */
const char *
GetLWLockIdentifier(uint32 classId, uint16 eventId)
{
	Assert(classId == PG_WAIT_LWLOCK);
	/* The event IDs are just tranche numbers. */
	return GetLWTrancheName(eventId);
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode.
 *
 * This function will not block waiting for a lock to become free - that's the
 * callers job.
 *
 * Returns true if the lock isn't free and we need to wait.
 */
static bool
LWLockAttemptLock(LWLock *lock, LWLockMode mode, LWLockMode waitMode,
				  bool queue_ok, bool wakeup)
{
	uint64		old_state;
	uint64		mask;
	uint64		increment;

	if (mode == LW_EXCLUSIVE)
	{
		mask = LW_LOCK_MASK;
		increment = LW_VAL_EXCLUSIVE;
	}
	else
	{
		mask = LW_VAL_EXCLUSIVE;
		increment = LW_VAL_SHARED;
	}

	/*
	 * Read once outside the loop, later iterations will get the newer value
	 * via compare & exchange.
	 */
	old_state = pg_atomic_read_u64(&lock->state);

	/* loop until we've determined whether we could acquire the lock or not */
	while (true)
	{
		uint64		desired_state;
		bool		lock_free;

		lock_free = (old_state & mask) == 0;

		/*
		 * The retries on spin-wait helps to avoid going to relatively expensive
		 * OS semaphore when lock is freed fast enough by a concurrent process.
		 * XXX The initial delay and number of repeats are optimized based on
		 * heavy lock concurrency tests and the values are to be discussed.
		 */
		if (!lock_free && queue_ok)
		{
			int		i = 0;
			SpinDelayStatus delayStatus;

			init_local_spin_delay(&delayStatus);
			delayStatus.cur_delay = 500L;

			while (!lock_free && i < 2)
			{
				perform_spin_delay(&delayStatus);
				old_state = pg_atomic_read_u64(&lock->state);
				lock_free = (old_state & mask) == 0;
				i++;
			}
		}

		desired_state = old_state;

		if (lock_free)
			desired_state += increment;

		if (wakeup)
			desired_state |= LW_FLAG_RELEASE_OK;

		if (!lock_free && queue_ok)
		{
			/*
			 * If we don't have a PGPROC structure, there's no way to wait.
			 * This should never occur, since MyProc should only be null
			 * during shared memory initialization.
			 */
			if (MyProc == NULL)
				elog(PANIC, "cannot wait without a PGPROC structure");

			if (MyProc->lwWaiting)
				elog(PANIC, "queueing for lock while waiting on another one");

			MyProc->lwWaiting = true;
			MyProc->lwWaitMode = waitMode;

			if (waitMode == LW_WAIT_UNTIL_FREE)
			{
				/*
				 * WAIT_UNTIL_FREE locks should be woken up at each wakeup,
				 * push them to list head to save time iterating it at unlock.
				 */
				desired_state = LW_SET_WAIT_HEAD(desired_state | LW_FLAG_RELEASE_OK, MyProc->pgprocno);
				/* if list was empty set tail to same value as head */
				if (!LW_HAS_WAITERS(old_state))
				{
					Assert(LW_GET_WAIT_TAIL(old_state) == INVALID_LOCK_PROCNO);
					desired_state = LW_SET_WAIT_TAIL(desired_state, MyProc->pgprocno);
				}
				MyProc->lwWaitLink = LW_GET_WAIT_HEAD(old_state);
			}
			else
			{
				/*
				 * Shared and exclusive waiters are added to the tail of a list
				 * so that we don't need to iterate the whole list at each wakeup.
				 */
				desired_state = LW_SET_WAIT_TAIL(desired_state, MyProc->pgprocno);
				/* if list was empty set head to same value as tail */
				if (!LW_HAS_WAITERS(old_state))
					desired_state = LW_SET_WAIT_HEAD(desired_state, MyProc->pgprocno);
				MyProc->lwWaitLink = INVALID_LOCK_PROCNO;

				/*
				 * Waiters linked to tail of a queue only after successful
				 * atomic update of state. Otherwise we can get a queue with
				 * tail entries behind a tail pointed from state, so that they
				 * could be unlocked by a concurrent process before being locked.
				 */
			}
		}

		/*
		 * Attempt to swap in the state we are expecting. If we didn't see
		 * lock to be free, that's just the old value. If we saw it as free,
		 * we'll attempt to mark it acquired. The reason that we always swap
		 * in the value is that this doubles as a memory barrier. We could try
		 * to be smarter and only swap in values if we saw the lock as free,
		 * but benchmark haven't shown it as beneficial so far.
		 *
		 * Retry if the value changed since we last looked at it.
		 */
		if (pg_atomic_compare_exchange_u64(&lock->state,
										   &old_state, desired_state))
		{
			if (lock_free)
			{
				/* Great! Got the lock. */
#ifdef LOCK_DEBUG
				if (mode == LW_EXCLUSIVE)
					lock->owner = MyProc;
#endif
				return false;
			}
			else
			{
				/*
				 * If we pushed new waiters to a tail of non-empty wait queue,
				 * relink lwWaitLink of a previous tail item in the queue.
				 */
				if (queue_ok && waitMode != LW_WAIT_UNTIL_FREE && LW_HAS_WAITERS(old_state))
				{
					Assert(LW_GET_WAIT_TAIL(old_state) != INVALID_LOCK_PROCNO);
					Assert(NextWaitLink(LW_GET_WAIT_TAIL(old_state)) == INVALID_LOCK_PROCNO);
					Assert(LW_GET_WAIT_TAIL(old_state) != LW_GET_WAIT_TAIL(desired_state));
					NextWaitLink(LW_GET_WAIT_TAIL(old_state)) = LW_GET_WAIT_TAIL(desired_state);
				}
#ifdef LOCK_DEBUG
				pg_atomic_fetch_add_u32(&lock->nwaiters, 1);
#endif
				return true;	/* somebody else has the lock */
			}
		}
		else
			MyProc->lwWaiting = false;
	}
	pg_unreachable();
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
	bool		wakeup = false;
	int			extraWaits = 0;
#ifdef LWLOCK_STATS
	lwlock_stats *lwstats;

	lwstats = get_lwlock_stats_entry(lock);
#endif

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

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
		if (!LWLockAttemptLock(lock, mode, mode, true, wakeup))
		{
			LOG_LWDEBUG("LWLockAcquire", lock, "immediately acquired lock");
			break;				/* got the lock */
		}

		/*
		 * Wait until awakened.
		 *
		 * It is possible that we get awakened for a reason other than being
		 * signaled by LWLockRelease.  If so, loop back and wait again.  Once
		 * we've gotten the LWLock, re-increment the sema by the number of
		 * additional signals received.
		 */
		LOG_LWDEBUG("LWLockAcquire", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		if (TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);

		for (;;)
		{
			PGSemaphoreLock(proc->sem);
			if (proc->lwWaiting == LW_WS_NOT_WAITING)
				break;
			extraWaits++;
		}
		wakeup = true;

#ifdef LOCK_DEBUG
		{
			/* not waiting anymore */
			uint32		nwaiters PG_USED_FOR_ASSERTS_ONLY = pg_atomic_fetch_sub_u32(&lock->nwaiters, 1);

			Assert(nwaiters < MAX_BACKENDS);
		}
#endif

		if (TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
		LWLockReportWaitEnd();

		LOG_LWDEBUG("LWLockAcquire", lock, "awakened");

		/* Now loop back and try to acquire lock again. */
		result = false;
	}

	if (TRACE_POSTGRESQL_LWLOCK_ACQUIRE_ENABLED())
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

	Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

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
	mustwait = LWLockAttemptLock(lock, mode, mode, false, false);

	if (mustwait)
	{
		/* Failed to get lock, so release interrupt holdoff */
		RESUME_INTERRUPTS();

		LOG_LWDEBUG("LWLockConditionalAcquire", lock, "failed");
		if (TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(T_NAME(lock), mode);
	}
	else
	{
		/* Add lock to list of locks held by this backend */
		held_lwlocks[num_held_lwlocks].lock = lock;
		held_lwlocks[num_held_lwlocks++].mode = mode;
		if (TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_ENABLED())
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

	/*
	 * NB: We're using nearly the same twice-in-a-row lock acquisition
	 * protocol as LWLockAcquire(). Check its comments for details.
	 */
	mustwait = LWLockAttemptLock(lock, mode, LW_WAIT_UNTIL_FREE, true, false);

	if (mustwait)
	{
		/*
		 * Wait until awakened.  Like in LWLockAcquire, be prepared for bogus
		 * wakeups.
		 */
		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		if (TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED())
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
		if (TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED())
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
		if (TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL(T_NAME(lock), mode);
	}
	else
	{
		LOG_LWDEBUG("LWLockAcquireOrWait", lock, "succeeded");
		/* Add lock to list of locks held by this backend */
		held_lwlocks[num_held_lwlocks].lock = lock;
		held_lwlocks[num_held_lwlocks++].mode = mode;
		if (TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_ENABLED())
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
	uint64		value;
	uint64		old_state;

	/*
	 * Test first to see if it the slot is free right now.
	 *
	 * XXX: the caller uses a spinlock before this, so we don't need a memory
	 * barrier here as far as the current usage is concerned.  But that might
	 * not be safe in general.
	 */
	old_state = pg_atomic_read_u64(&lock->state);

	while (true)
	{
		if ((old_state & LW_VAL_EXCLUSIVE) == 0)
		{
			*result = true;
			return false;
		}

		/* and then spin without atomic operations until lock is released */
		if (old_state & LW_FLAG_LOCK_VAR)
		{
			SpinDelayStatus delayStatus;

			init_local_spin_delay(&delayStatus);

			while (old_state & LW_FLAG_LOCK_VAR)
			{
				perform_spin_delay(&delayStatus);
				old_state = pg_atomic_read_u64(&lock->state);
			}
#ifdef LWLOCK_STATS
			delays += delayStatus.delays;
#endif
			finish_spin_delay(&delayStatus);
		}

		if (pg_atomic_compare_exchange_u64(&lock->state, &old_state,
										   old_state | LW_FLAG_LOCK_VAR))
			break;
	}

	*result = false;

	value = *valptr;

	if (value != oldval)
	{
		*newval = value;

		pg_atomic_fetch_and_u64(&lock->state, ~LW_FLAG_LOCK_VAR);
		return false;
	}

	/* loop until we've determined whether we could acquire the lock or not */
	while (true)
	{
		uint64		desired_state = old_state & (~LW_FLAG_LOCK_VAR);

		if ((old_state & LW_VAL_EXCLUSIVE) != 0)
		{
			/*
			 * If we don't have a PGPROC structure, there's no way to wait.
			 * This should never occur, since MyProc should only be null
			 * during shared memory initialization.
			 */
			if (MyProc == NULL)
				elog(PANIC, "cannot wait without a PGPROC structure");

			if (MyProc->lwWaiting)
				elog(PANIC, "queueing for lock while waiting on another one");

			MyProc->lwWaiting = true;
			MyProc->lwWaitMode = LW_WAIT_UNTIL_FREE;
			MyProc->lwWaitLink = LW_GET_WAIT_HEAD(old_state);
			Assert(LW_GET_WAIT_HEAD(old_state) != MyProc->pgprocno);
			desired_state = LW_SET_WAIT_HEAD(desired_state | LW_FLAG_RELEASE_OK, MyProc->pgprocno);
			/* if list was empty set tail to same value as head */
			if (!LW_HAS_WAITERS(old_state))
			{
				Assert(LW_GET_WAIT_TAIL(old_state) == INVALID_LOCK_PROCNO);
				desired_state = LW_SET_WAIT_TAIL(desired_state, MyProc->pgprocno);
			}
		}

		if (pg_atomic_compare_exchange_u64(&lock->state,
										   &old_state, desired_state))
		{

			if (MyProc->lwWaiting)
			{
#ifdef LOCK_DEBUG
				pg_atomic_fetch_add_u32(&lock->nwaiters, 1);
#endif
				return true;
			}
			else
			{
				*result = true;
				return false;
			}
		}
		else
		{
			MyProc->lwWaiting = false;
		}
	}

	return true;
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
		 * It is possible that we get awakened for a reason other than being
		 * signaled by LWLockRelease.  If so, loop back and wait again.  Once
		 * we've gotten the LWLock, re-increment the sema by the number of
		 * additional signals received.
		 */
		LOG_LWDEBUG("LWLockWaitForVar", lock, "waiting");

#ifdef LWLOCK_STATS
		lwstats->block_count++;
#endif

		LWLockReportWaitStart(lock);
		if (TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), LW_EXCLUSIVE);

		for (;;)
		{
			PGSemaphoreLock(proc->sem);
			if (proc->lwWaiting == LW_WS_NOT_WAITING)
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

		if (TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED())
			TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), LW_EXCLUSIVE);
		LWLockReportWaitEnd();

		LOG_LWDEBUG("LWLockWaitForVar", lock, "awakened");

		/* Now loop back and check the status of the lock again. */
	}

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

static uint64
lockVar(LWLock *lock)
{
	uint64		oldState;

	oldState = pg_atomic_fetch_or_u64(&lock->state, LW_FLAG_LOCK_VAR);

	while (oldState & LW_FLAG_LOCK_VAR)
	{
		SpinDelayStatus delayStatus;

		init_local_spin_delay(&delayStatus);

		while (oldState & LW_FLAG_LOCK_VAR)
		{
			perform_spin_delay(&delayStatus);
			oldState = pg_atomic_read_u64(&lock->state);
		}
#ifdef LWLOCK_STATS
		delays += delayStatus.delays;
#endif
		finish_spin_delay(&delayStatus);

		oldState = pg_atomic_fetch_or_u64(&lock->state, LW_FLAG_LOCK_VAR);
	}

	return oldState;
}

/*
 * waitForQueueRelink - Wait on a spinlock until concurrent
 * lock procedure links new tail to waiters list.
 *
 * This is to process the rare case when the
 * concurrent process try to iterate waiters list after lock
 * procedure updated lock state but before it linked tail of
 * the queue.
 *
 * The vice versa situation of linking tail to waiters list
 * before updating lock state could lead to releasing waiters
 * that haven't added to a wait queue yet.
 */
static inline void
waitForQueueRelink(uint32 pgprocno)
{
	SpinDelayStatus delayStatus;

	init_local_spin_delay(&delayStatus);
	while (NextWaitLink(pgprocno) == INVALID_LOCK_PROCNO)
		perform_spin_delay(&delayStatus);

	finish_spin_delay(&delayStatus);
	return;
}

/* awakenWaiters - Awaken all waiters from the wake queue */
static inline void
awakenWaiters(uint32 pgprocno, LWLock *lock)
{
	while (pgprocno != INVALID_LOCK_PROCNO)
	{
		/* Must read this before wakeup! */
		uint32		nextlink = NextReleaseLink(pgprocno);

		LOG_LWDEBUG("LWLockRelease", lock, "release waiter");

		pg_read_barrier();
		CurIsWaiting(pgprocno) = false;
		pg_write_barrier();
		PGSemaphoreUnlock(CurSem(pgprocno));

		pgprocno = nextlink;
	}
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
	uint64		oldState;
	uint32		pgprocno,
				newHead,
				newTail,
				oldHead = INVALID_LOCK_PROCNO,
				oldTail = INVALID_LOCK_PROCNO,
				oldReplaceHead = INVALID_LOCK_PROCNO,
				wakeupHead = INVALID_LOCK_PROCNO,
				wakeupCur = INVALID_LOCK_PROCNO;

	PRINT_LWDEBUG("LWLockUpdateVar", lock, LW_EXCLUSIVE);

	oldState = lockVar(lock);

	Assert(oldState & LW_VAL_EXCLUSIVE);

	/* Update the lock's value */
	*valptr = val;

	/*
	 * See if there are any LW_WAIT_UNTIL_FREE waiters that need to be woken
	 * up. They are always in the front of the queue. Add them to release queue.
	 *
	 * If during a processing, concurrent process adds some waiters in the wait queue
	 * atomic compare-and-exchange on lock state will not succeed. Then we construct
	 * lock state again and update existing release queue with items from added head
	 * of wait queue.
	 *
	 * Only after we suceeded in updating atomic lock state, we actually wakeup
	 * processes from release queue.

	 */
	while (true)
	{
		uint64		newState = oldState;

		newHead = LW_GET_WAIT_HEAD(newState);
		newTail = LW_GET_WAIT_TAIL(newState);

		/* Move all LW_WAIT_UNTIL_FREE waiters into wakeup list. */
		if (newHead != oldHead)
		{
			pgprocno = newHead;
		}
		else
		{
			pgprocno = INVALID_LOCK_PROCNO;
			if (oldReplaceHead != INVALID_LOCK_PROCNO)
			{
				newHead = oldReplaceHead;
			}
			else if (newTail == oldTail)
			{
				newHead = newTail = oldReplaceHead;
			}
			else
			{
				newHead = NextWaitLink(oldTail);
				if (newHead == INVALID_LOCK_PROCNO)
				{
					waitForQueueRelink(oldTail);
					newHead = NextWaitLink(oldTail);
				}
			}
		}

		while (pgprocno != INVALID_LOCK_PROCNO)
		{
			uint32		nextPgprocno = (pgprocno != newTail ? NextWaitLink(pgprocno) : INVALID_LOCK_PROCNO),
						nextStepPgprocno;

			/* Lock tail is updated but queue tail haven't relinked yet. */
			if (pgprocno != newTail &&
				nextPgprocno == INVALID_LOCK_PROCNO)
			{
				waitForQueueRelink(pgprocno);
				nextPgprocno = NextWaitLink(pgprocno);
			}
			nextStepPgprocno = nextPgprocno;

			if (nextPgprocno == oldHead &&
				nextPgprocno != INVALID_LOCK_PROCNO)
			{
				Assert(oldTail != INVALID_LOCK_PROCNO);
				Assert(newTail != INVALID_LOCK_PROCNO);

				nextStepPgprocno = (oldTail != newTail ? NextWaitLink(oldTail) : INVALID_LOCK_PROCNO);
				if (nextStepPgprocno == INVALID_LOCK_PROCNO &&
					oldTail != newTail)
				{
					waitForQueueRelink(oldTail);
					nextStepPgprocno = NextWaitLink(oldTail);
				}

				if (oldReplaceHead != INVALID_LOCK_PROCNO)
				{
					if (oldReplaceHead != nextPgprocno)
					{
						Assert(pgprocno != oldReplaceHead);
						nextPgprocno = NextWaitLink(pgprocno) = oldReplaceHead;
					}
				}
				else
				{
					if (nextStepPgprocno != nextPgprocno)
					{
						Assert(pgprocno != nextStepPgprocno);
						nextPgprocno = NextWaitLink(pgprocno) = nextStepPgprocno;
					}
				}
			}

			/* Remove LW_WAIT_UNTIL_FREE from the list head */
			if (CurWaitMode(pgprocno) == LW_WAIT_UNTIL_FREE)
			{
				newHead = nextPgprocno;
				if (newHead == INVALID_LOCK_PROCNO)
					newTail = newHead;

				/* Push to the wakeup list */
				Assert(pgprocno != wakeupHead);
				if (wakeupHead == INVALID_LOCK_PROCNO)
					wakeupHead = wakeupCur = pgprocno;
				else
					NextReleaseLink(wakeupCur) = pgprocno;

				NextReleaseLink(pgprocno) = INVALID_LOCK_PROCNO;
				wakeupCur = pgprocno;
			}
			else
			{
				/*
				 * At the tail part of a list there could be only LW_EXCLUSIVE
				 * or LW_SHARED
				 */
				break;
			}

			pgprocno = nextStepPgprocno;
		}

		newState = LW_SET_STATE(newState & ~LW_FLAG_LOCK_VAR, newHead, newTail);
		oldHead = LW_GET_WAIT_HEAD(oldState);
		oldTail = LW_GET_WAIT_TAIL(oldState);
		oldReplaceHead = newHead;

		if (pg_atomic_compare_exchange_u64(&lock->state, &oldState, newState))
		{
			break;
		}
	}

	/* Awaken any waiters removed from the queue. */
	awakenWaiters(wakeupHead, lock);
}

/*
 * LWLockRelease - release a previously acquired lock
 */
void
LWLockRelease(LWLock *lock)
{
	LWLockMode	mode;
	uint64		oldState;
	int			i;
	uint32		pgprocno,
				prevPgprocno,
				newHead,
				newTail,
				oldHead = INVALID_LOCK_PROCNO,
				oldTail = INVALID_LOCK_PROCNO,
				oldReplaceHead = INVALID_LOCK_PROCNO,
				oldReplaceTail = INVALID_LOCK_PROCNO,
				wakeupHead = INVALID_LOCK_PROCNO,
				wakeupCur = INVALID_LOCK_PROCNO;
	bool		new_release_ok = true;
	bool		wakeup = false;
	LWLockMode	lastMode = LW_WAIT_UNTIL_FREE;

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
	oldState = pg_atomic_read_u64(&lock->state);

	while (true)
	{
		uint64		newState = oldState;

		if (mode == LW_EXCLUSIVE)
			newState -= LW_VAL_EXCLUSIVE;
		else
			newState -= LW_VAL_SHARED;

		if (LW_HAS_WAITERS(oldState) &&
			(newState & LW_LOCK_MASK) == 0 &&
			(newState & LW_FLAG_RELEASE_OK) == LW_FLAG_RELEASE_OK)
			newState |= LW_FLAG_WAKE_UP_LOCK;
		newState &= ~LW_FLAG_LOCK_VAR;

		if (pg_atomic_compare_exchange_u64(&lock->state, &oldState, newState))
		{
			if (!(oldState & LW_FLAG_WAKE_UP_LOCK) &&
				(newState & LW_FLAG_WAKE_UP_LOCK))
				wakeup = true;
			oldState = newState;
			break;
		}
	}

	if (TRACE_POSTGRESQL_LWLOCK_RELEASE_ENABLED())
		TRACE_POSTGRESQL_LWLOCK_RELEASE(T_NAME(lock));

	if (!wakeup)
	{
		RESUME_INTERRUPTS();
		return;
	}

	/* XXX: remove before commit? */
	LOG_LWDEBUG("LWLockRelease", lock, "releasing waiters");

	/*
	 * Move waiters to be awaken from wait queue to a release queue are:
	 * - all LW_WAIT_UNTIL_FREE waiters
	 * And after all LW_WAIT_UNTIL_FREE are removed:
	 * - if first in the queue is LW_EXCLUSIVE then only one LW_EXCLUSIVE
	 * - if first in the queue is LW_SHARED then all LW_SHARED from the queue.
	 *
	 * If during a processing, concurrent process adds some waiters in the wait queue
	 * atomic compare-and-exchange on lock state will not succeed. Then we construct
	 * lock state again and update existing release queue with items from added tail
	 * and head of wait queue.
	 *
	 * Only after we suceeded in updating atomic lock state, we actually wakeup
	 * processes from release queue.
	 */
	while (true)
	{
		uint64		newState = oldState;

		newHead = LW_GET_WAIT_HEAD(newState);
		newTail = LW_GET_WAIT_TAIL(newState);
		prevPgprocno = INVALID_LOCK_PROCNO;

		/* Move all LW_WAIT_UNTIL_FREE waiters into wakeup list. */
		if (oldHead == INVALID_LOCK_PROCNO ||
			oldTail == INVALID_LOCK_PROCNO ||
			newHead != oldHead)
		{
			pgprocno = newHead;

		}
		else if (oldTail != newTail)
		{
			Assert(newHead != INVALID_LOCK_PROCNO &&
					newTail != INVALID_LOCK_PROCNO &&
					oldHead != INVALID_LOCK_PROCNO &&
					oldTail != INVALID_LOCK_PROCNO);

			pgprocno = NextWaitLink(oldTail);
			if (pgprocno == INVALID_LOCK_PROCNO)
			{
				waitForQueueRelink(oldTail);
				pgprocno = NextWaitLink(oldTail);
			}
			prevPgprocno = oldReplaceTail;

			if (oldReplaceHead != INVALID_LOCK_PROCNO)
				newHead = oldReplaceHead;
			else
				newHead = pgprocno;

			if (oldTail != oldReplaceTail &&
				oldReplaceTail != INVALID_LOCK_PROCNO)
			{
				Assert(prevPgprocno != pgprocno);
				NextWaitLink(prevPgprocno) = pgprocno;
			}
		}
		else
		{
			Assert(newHead == oldHead);
			Assert(oldTail == newTail);
			newHead = oldReplaceHead;
			newTail = oldReplaceTail;
			pgprocno = INVALID_LOCK_PROCNO;
		}

		while (pgprocno != INVALID_LOCK_PROCNO)
		{
			LWLockMode	curMode = CurWaitMode(pgprocno);
			uint32		nextPgprocno = (pgprocno != newTail ? NextWaitLink(pgprocno) : INVALID_LOCK_PROCNO),
						nextStepPgprocno,
						nextStepPrevPgprocno = pgprocno;

			/* Lock tail is updated but queue tail haven't relinked yet. */
			if (pgprocno != newTail &&
				nextPgprocno == INVALID_LOCK_PROCNO)
			{
				waitForQueueRelink(pgprocno);
				nextPgprocno = NextWaitLink(pgprocno);
			}
			nextStepPgprocno = nextPgprocno;

			if (nextPgprocno == oldHead &&
				nextPgprocno != INVALID_LOCK_PROCNO)
			{
				Assert(oldTail != INVALID_LOCK_PROCNO);
				Assert(newTail != INVALID_LOCK_PROCNO);

				nextStepPrevPgprocno = (oldReplaceTail != INVALID_LOCK_PROCNO ? oldReplaceTail : pgprocno);
				nextStepPgprocno = (oldTail != newTail ? NextWaitLink(oldTail) : INVALID_LOCK_PROCNO);
				if (nextStepPgprocno == INVALID_LOCK_PROCNO &&
					oldTail != newTail)
				{
					waitForQueueRelink(oldTail);
					nextStepPgprocno = NextWaitLink(oldTail);
				}

				if (oldTail != oldReplaceTail &&
					oldReplaceTail != INVALID_LOCK_PROCNO)
				{
					Assert(oldReplaceTail != nextStepPgprocno);
					NextWaitLink(oldReplaceTail) = nextStepPgprocno;
				}

				if (oldReplaceHead != INVALID_LOCK_PROCNO)
				{
					if (oldReplaceHead != nextPgprocno)
					{
						Assert(pgprocno != oldReplaceHead);
						nextPgprocno = NextWaitLink(pgprocno) = oldReplaceHead;
					}
				}
				else
				{
					if (nextStepPgprocno != nextPgprocno)
					{
						Assert(pgprocno != nextStepPgprocno);
						nextPgprocno = NextWaitLink(pgprocno) = nextStepPgprocno;
					}
				}

				if (oldTail == newTail)
				{
					if (oldReplaceTail != INVALID_LOCK_PROCNO)
						newTail = oldReplaceTail;
					else
						newTail = pgprocno;
				}
			}

			/* Remove LW_WAIT_UNTIL_FREE waiters from the list head */
			if (curMode == LW_WAIT_UNTIL_FREE)
			{
				Assert(prevPgprocno == INVALID_LOCK_PROCNO);
				newHead = nextPgprocno;
				if (newHead == INVALID_LOCK_PROCNO)
					newTail = newHead;
				if (nextStepPrevPgprocno == pgprocno)
					nextStepPrevPgprocno = prevPgprocno;

				/* Push to the wakeup list */
				Assert(pgprocno != wakeupHead);
				if (wakeupHead == INVALID_LOCK_PROCNO)
					wakeupHead = wakeupCur = pgprocno;
				else
					NextReleaseLink(wakeupCur) = pgprocno;

				NextReleaseLink(pgprocno) = INVALID_LOCK_PROCNO;
				wakeupCur = pgprocno;
			}
			else
			{
				if (lastMode == LW_EXCLUSIVE)
					break;

				if (lastMode == LW_WAIT_UNTIL_FREE)
					lastMode = curMode;

				if (lastMode == curMode)
				{
					/*
					 * Prevent additional wakeups until retrier gets to
						* run. Backends that are just waiting for the lock to
						* become free don't retry automatically.
						*/
					new_release_ok = false;

					if (prevPgprocno == INVALID_LOCK_PROCNO)
					{
						newHead = nextPgprocno;
					}
					else
					{
						Assert(prevPgprocno != nextPgprocno);
						NextWaitLink(prevPgprocno) = nextPgprocno;
						if (nextPgprocno == INVALID_LOCK_PROCNO)
						{
							Assert(newTail == pgprocno);
							newTail = prevPgprocno;
						}
					}

					if (nextStepPrevPgprocno == pgprocno)
						nextStepPrevPgprocno = prevPgprocno;

					if (newHead == INVALID_LOCK_PROCNO ||
						newTail == INVALID_LOCK_PROCNO)
					{
						newHead = INVALID_LOCK_PROCNO;
						newTail = INVALID_LOCK_PROCNO;
					}

					/* Push to the wakeup list */
					Assert(pgprocno != wakeupHead);
					if (wakeupHead == INVALID_LOCK_PROCNO)
						wakeupHead = wakeupCur = pgprocno;
					else
						NextReleaseLink(wakeupCur) = pgprocno;

					NextReleaseLink(pgprocno) = INVALID_LOCK_PROCNO;
					wakeupCur = pgprocno;
				}
			}

			prevPgprocno = nextStepPrevPgprocno;
			pgprocno = nextStepPgprocno;
		}

		newState = LW_SET_STATE(newState & ~LW_FLAG_WAKE_UP_LOCK, newHead, newTail);

		if (new_release_ok)
			newState |= LW_FLAG_RELEASE_OK;
		else
			newState &= ~LW_FLAG_RELEASE_OK;

		oldHead = LW_GET_WAIT_HEAD(oldState);
		oldTail = LW_GET_WAIT_TAIL(oldState);
		oldReplaceHead = newHead;
		oldReplaceTail = newTail;

		if (pg_atomic_compare_exchange_u64(&lock->state, &oldState, newState))
			break;
	}

	/* Awaken any waiters removed from the queue. */
	awakenWaiters(wakeupHead, lock);

	/* Now okay to allow cancel/die interrupts. */
	RESUME_INTERRUPTS();
}

/*
 * LWLockReleaseClearVar - release a previously acquired lock, reset variable
 */
void
LWLockReleaseClearVar(LWLock *lock, uint64 *valptr, uint64 val)
{
	(void) lockVar(lock);

	*valptr = val;

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
LWLockHeldByMe(LWLock *lock)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i].lock == lock)
			return true;
	}
	return false;
}

/*
 * LWLockHeldByMe - test whether my process holds any of an array of locks
 *
 * This is meant as debug support only.
 */
bool
LWLockAnyHeldByMe(LWLock *lock, int nlocks, size_t stride)
{
	char	   *held_lock_addr;
	char	   *begin;
	char	   *end;
	int			i;

	begin = (char *) lock;
	end = begin + nlocks * stride;
	for (i = 0; i < num_held_lwlocks; i++)
	{
		held_lock_addr = (char *) held_lwlocks[i].lock;
		if (held_lock_addr >= begin &&
			held_lock_addr < end &&
			(held_lock_addr - begin) % stride == 0)
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
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	int			i;

	for (i = 0; i < num_held_lwlocks; i++)
	{
		if (held_lwlocks[i].lock == lock && held_lwlocks[i].mode == mode)
			return true;
	}
	return false;
}
