/* ----------
 * wait_event.c
 *	  Wait event reporting infrastructure.
 *
 * Copyright (c) 2001-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/wait_event.c
 *
 * NOTES
 *
 * To make pgstat_report_wait_start() and pgstat_report_wait_end() as
 * lightweight as possible, they do not check if shared memory (MyProc
 * specifically, where the wait event is stored) is already available. Instead
 * we initially set my_wait_event_info to a process local variable, which then
 * is redirected to shared memory using pgstat_set_wait_event_storage(). For
 * the same reason pgstat_track_activities is not checked - the check adds
 * more work than it saves.
 *
 * ----------
 */
#include "postgres.h"

#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "storage/spin.h"
#include "utils/wait_event.h"


static const char *pgstat_get_wait_activity(WaitEventActivity w);
static const char *pgstat_get_wait_buffer(WaitEventBuffer w);
static const char *pgstat_get_wait_client(WaitEventClient w);
static const char *pgstat_get_wait_ipc(WaitEventIPC w);
static const char *pgstat_get_wait_timeout(WaitEventTimeout w);
static const char *pgstat_get_wait_io(WaitEventIO w);
static void WaitEventUsageAdd(WaitEventUsage *usage, uint32 wait_event_info,
							  const instr_time *elapsed);


static uint32 local_my_wait_event_info;
uint32	   *my_wait_event_info = &local_my_wait_event_info;

#define WAIT_EVENT_USAGE_INITIAL_EVENTS	16

int			pgstat_wait_event_usage_depth = 0;
static WaitEventUsage *pgstat_wait_event_usage = NULL;
static uint32 pgstat_wait_event_usage_current = 0;
static instr_time pgstat_wait_event_usage_start;

#define WAIT_EVENT_CLASS_MASK	0xFF000000
#define WAIT_EVENT_ID_MASK		0x0000FFFF

/*
 * Hash tables for storing custom wait event ids and their names in
 * shared memory.
 *
 * WaitEventCustomHashByInfo is used to find the name from wait event
 * information.  Any backend can search it to find custom wait events.
 *
 * WaitEventCustomHashByName is used to find the wait event information from a
 * name.  It is used to ensure that no duplicated entries are registered.
 *
 * For simplicity, we use the same ID counter across types of custom events.
 * We could end that anytime the need arises.
 *
 * The size of the hash table is based on the assumption that usually only a
 * handful of entries are needed, but since it's small in absolute terms
 * anyway, we leave a generous amount of headroom.
 */
static HTAB *WaitEventCustomHashByInfo; /* find names from infos */
static HTAB *WaitEventCustomHashByName; /* find infos from names */

#define WAIT_EVENT_CUSTOM_HASH_SIZE	128

/* hash table entries */
typedef struct WaitEventCustomEntryByInfo
{
	uint32		wait_event_info;	/* hash key */
	char		wait_event_name[NAMEDATALEN];	/* custom wait event name */
} WaitEventCustomEntryByInfo;

typedef struct WaitEventCustomEntryByName
{
	char		wait_event_name[NAMEDATALEN];	/* hash key */
	uint32		wait_event_info;
} WaitEventCustomEntryByName;


/* dynamic allocation counter for custom wait events */
typedef struct WaitEventCustomCounterData
{
	int			nextId;			/* next ID to assign */
	slock_t		mutex;			/* protects the counter */
} WaitEventCustomCounterData;

/* pointer to the shared memory */
static WaitEventCustomCounterData *WaitEventCustomCounter;

/* first event ID of custom wait events */
#define WAIT_EVENT_CUSTOM_INITIAL_ID	1

static uint32 WaitEventCustomNew(uint32 classId, const char *wait_event_name);
static const char *GetWaitEventCustomIdentifier(uint32 wait_event_info);

static void WaitEventCustomShmemRequest(void *arg);
static void WaitEventCustomShmemInit(void *arg);

const ShmemCallbacks WaitEventCustomShmemCallbacks = {
	.request_fn = WaitEventCustomShmemRequest,
	.init_fn = WaitEventCustomShmemInit,
};

/*
 * Register shmem space for dynamic shared hash and dynamic allocation counter.
 */
static void
WaitEventCustomShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "WaitEventCustomCounterData",
					   .size = sizeof(WaitEventCustomCounterData),
					   .ptr = (void **) &WaitEventCustomCounter,
		);
	ShmemRequestHash(.name = "WaitEventCustom hash by wait event information",
					 .ptr = &WaitEventCustomHashByInfo,
					 .nelems = WAIT_EVENT_CUSTOM_HASH_SIZE,
					 .hash_info.keysize = sizeof(uint32),
					 .hash_info.entrysize = sizeof(WaitEventCustomEntryByInfo),
					 .hash_flags = HASH_ELEM | HASH_BLOBS,
		);
	ShmemRequestHash(.name = "WaitEventCustom hash by name",
					 .ptr = &WaitEventCustomHashByName,
					 .nelems = WAIT_EVENT_CUSTOM_HASH_SIZE,
	/* key is a NULL-terminated string */
					 .hash_info.keysize = sizeof(char[NAMEDATALEN]),
					 .hash_info.entrysize = sizeof(WaitEventCustomEntryByName),
					 .hash_flags = HASH_ELEM | HASH_STRINGS,
		);
}

static void
WaitEventCustomShmemInit(void *arg)
{
	/* initialize the allocation counter and its spinlock. */
	WaitEventCustomCounter->nextId = WAIT_EVENT_CUSTOM_INITIAL_ID;
	SpinLockInit(&WaitEventCustomCounter->mutex);
}

/*
 * Allocate a new event ID and return the wait event info.
 *
 * If the wait event name is already defined, this does not allocate a new
 * entry; it returns the wait event information associated to the name.
 */
uint32
WaitEventExtensionNew(const char *wait_event_name)
{
	return WaitEventCustomNew(PG_WAIT_EXTENSION, wait_event_name);
}

uint32
WaitEventInjectionPointNew(const char *wait_event_name)
{
	return WaitEventCustomNew(PG_WAIT_INJECTIONPOINT, wait_event_name);
}

static uint32
WaitEventCustomNew(uint32 classId, const char *wait_event_name)
{
	uint16		eventId;
	bool		found;
	WaitEventCustomEntryByName *entry_by_name;
	WaitEventCustomEntryByInfo *entry_by_info;
	uint32		wait_event_info;

	/* Check the limit of the length of the event name */
	if (strlen(wait_event_name) >= NAMEDATALEN)
		elog(ERROR,
			 "cannot use custom wait event string longer than %u characters",
			 NAMEDATALEN - 1);

	/*
	 * Check if the wait event info associated to the name is already defined,
	 * and return it if so.
	 */
	LWLockAcquire(WaitEventCustomLock, LW_SHARED);
	entry_by_name = (WaitEventCustomEntryByName *)
		hash_search(WaitEventCustomHashByName, wait_event_name,
					HASH_FIND, &found);
	LWLockRelease(WaitEventCustomLock);
	if (found)
	{
		uint32		oldClassId;

		oldClassId = entry_by_name->wait_event_info & WAIT_EVENT_CLASS_MASK;
		if (oldClassId != classId)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("wait event \"%s\" already exists in type \"%s\"",
							wait_event_name,
							pgstat_get_wait_event_type(entry_by_name->wait_event_info))));
		return entry_by_name->wait_event_info;
	}

	/*
	 * Allocate and register a new wait event.  Recheck if the event name
	 * exists, as it could be possible that a concurrent process has inserted
	 * one with the same name since the LWLock acquired again here was
	 * previously released.
	 */
	LWLockAcquire(WaitEventCustomLock, LW_EXCLUSIVE);
	entry_by_name = (WaitEventCustomEntryByName *)
		hash_search(WaitEventCustomHashByName, wait_event_name,
					HASH_FIND, &found);
	if (found)
	{
		uint32		oldClassId;

		LWLockRelease(WaitEventCustomLock);
		oldClassId = entry_by_name->wait_event_info & WAIT_EVENT_CLASS_MASK;
		if (oldClassId != classId)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("wait event \"%s\" already exists in type \"%s\"",
							wait_event_name,
							pgstat_get_wait_event_type(entry_by_name->wait_event_info))));
		return entry_by_name->wait_event_info;
	}

	/* Allocate a new event Id */
	SpinLockAcquire(&WaitEventCustomCounter->mutex);

	if (WaitEventCustomCounter->nextId >= WAIT_EVENT_CUSTOM_HASH_SIZE)
	{
		SpinLockRelease(&WaitEventCustomCounter->mutex);
		ereport(ERROR,
				errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				errmsg("too many custom wait events"));
	}

	eventId = WaitEventCustomCounter->nextId++;

	SpinLockRelease(&WaitEventCustomCounter->mutex);

	/* Register the new wait event */
	wait_event_info = classId | eventId;
	entry_by_info = (WaitEventCustomEntryByInfo *)
		hash_search(WaitEventCustomHashByInfo, &wait_event_info,
					HASH_ENTER, &found);
	Assert(!found);
	strlcpy(entry_by_info->wait_event_name, wait_event_name,
			sizeof(entry_by_info->wait_event_name));

	entry_by_name = (WaitEventCustomEntryByName *)
		hash_search(WaitEventCustomHashByName, wait_event_name,
					HASH_ENTER, &found);
	Assert(!found);
	entry_by_name->wait_event_info = wait_event_info;

	LWLockRelease(WaitEventCustomLock);

	return wait_event_info;
}

/*
 * Return the name of a custom wait event information.
 */
static const char *
GetWaitEventCustomIdentifier(uint32 wait_event_info)
{
	bool		found;
	WaitEventCustomEntryByInfo *entry;

	/* Built-in event? */
	if (wait_event_info == PG_WAIT_EXTENSION)
		return "Extension";

	/* It is a user-defined wait event, so lookup hash table. */
	LWLockAcquire(WaitEventCustomLock, LW_SHARED);
	entry = (WaitEventCustomEntryByInfo *)
		hash_search(WaitEventCustomHashByInfo, &wait_event_info,
					HASH_FIND, &found);
	LWLockRelease(WaitEventCustomLock);

	if (!entry)
		elog(ERROR,
			 "could not find custom name for wait event information %u",
			 wait_event_info);

	return entry->wait_event_name;
}


/*
 * Returns a list of currently defined custom wait event names.  The result is
 * a palloc'd array, with the number of elements saved in *nwaitevents.
 */
char	  **
GetWaitEventCustomNames(uint32 classId, int *nwaitevents)
{
	char	  **waiteventnames;
	WaitEventCustomEntryByName *hentry;
	HASH_SEQ_STATUS hash_seq;
	int			index;
	int			els;

	LWLockAcquire(WaitEventCustomLock, LW_SHARED);

	/* Now we can safely count the number of entries */
	els = hash_get_num_entries(WaitEventCustomHashByName);

	/* Allocate enough space for all entries */
	waiteventnames = palloc_array(char *, els);

	/* Now scan the hash table to copy the data */
	hash_seq_init(&hash_seq, WaitEventCustomHashByName);

	index = 0;
	while ((hentry = (WaitEventCustomEntryByName *) hash_seq_search(&hash_seq)) != NULL)
	{
		if ((hentry->wait_event_info & WAIT_EVENT_CLASS_MASK) != classId)
			continue;
		waiteventnames[index] = pstrdup(hentry->wait_event_name);
		index++;
	}

	LWLockRelease(WaitEventCustomLock);

	*nwaitevents = index;
	return waiteventnames;
}

/*
 * Configure wait event reporting to report wait events to *wait_event_info.
 * *wait_event_info needs to be valid until pgstat_reset_wait_event_storage()
 * is called.
 *
 * Expected to be called during backend startup, to point my_wait_event_info
 * into shared memory.
 */
void
pgstat_set_wait_event_storage(uint32 *wait_event_info)
{
	my_wait_event_info = wait_event_info;
}

/*
 * Reset wait event storage location.
 *
 * Expected to be called during backend shutdown, before the location set up
 * pgstat_set_wait_event_storage() becomes invalid.
 */
void
pgstat_reset_wait_event_storage(void)
{
	my_wait_event_info = &local_my_wait_event_info;
}

/*
 * Start collecting exact wait event timings in this backend.
 *
 * This is intended for short-lived instrumentation such as EXPLAIN ANALYZE.
 * It records waits observed through pgstat_report_wait_start/end in backend
 * local memory.  Nested collection is deliberately treated as part of the
 * outer collection for now; callers that want independent nested accounting
 * need a stack of WaitEventUsage contexts.
 */
void
pgstat_begin_wait_event_usage(WaitEventUsage *usage, MemoryContext memcontext)
{
	Assert(usage != NULL);
	Assert(memcontext != NULL);

	if (pgstat_wait_event_usage_depth++ == 0)
	{
		memset(usage, 0, sizeof(WaitEventUsage));
		usage->memcontext = memcontext;
		pgstat_wait_event_usage = usage;
		pgstat_wait_event_usage_current = 0;
		INSTR_TIME_SET_ZERO(pgstat_wait_event_usage_start);
	}
}

/*
 * Stop collecting wait event timings.
 */
void
pgstat_end_wait_event_usage(WaitEventUsage *usage)
{
	Assert(usage != NULL);
	Assert(pgstat_wait_event_usage_depth > 0);

	if (--pgstat_wait_event_usage_depth == 0)
	{
		if (pgstat_wait_event_usage_current != 0)
			pgstat_count_wait_event_end();

		pgstat_wait_event_usage = NULL;
		pgstat_wait_event_usage_current = 0;
		INSTR_TIME_SET_ZERO(pgstat_wait_event_usage_start);
	}
}

/*
 * Record the beginning of a wait event for exact EXPLAIN-style accounting.
 */
void
pgstat_count_wait_event_start(uint32 wait_event_info)
{
	if (pgstat_wait_event_usage == NULL)
		return;

	/*
	 * Waits are not expected to nest.  If they do, finish the previous wait
	 * at the boundary so accounting remains internally consistent.
	 */
	if (pgstat_wait_event_usage_current != 0)
		pgstat_count_wait_event_end();

	pgstat_wait_event_usage_current = wait_event_info;
	INSTR_TIME_SET_CURRENT(pgstat_wait_event_usage_start);
}

/*
 * Record the end of the current wait event.
 */
void
pgstat_count_wait_event_end(void)
{
	instr_time	end;
	instr_time	elapsed;

	if (pgstat_wait_event_usage == NULL ||
		pgstat_wait_event_usage_current == 0)
		return;

	INSTR_TIME_SET_CURRENT(end);
	elapsed = end;
	INSTR_TIME_SUBTRACT(elapsed, pgstat_wait_event_usage_start);

	WaitEventUsageAdd(pgstat_wait_event_usage,
					  pgstat_wait_event_usage_current,
					  &elapsed);

	pgstat_wait_event_usage_current = 0;
	INSTR_TIME_SET_ZERO(pgstat_wait_event_usage_start);
}

static void
WaitEventUsageAdd(WaitEventUsage *usage, uint32 wait_event_info,
				  const instr_time *elapsed)
{
	WaitEventUsageEntry *entry = NULL;

	for (int i = 0; i < usage->nentries; i++)
	{
		if (usage->entries[i].wait_event_info == wait_event_info)
		{
			entry = &usage->entries[i];
			break;
		}
	}

	if (entry == NULL)
	{
		if (usage->nentries >= usage->maxentries)
		{
			MemoryContext oldcontext;
			int			newmaxentries;

			if (usage->maxentries > 0)
				newmaxentries = usage->maxentries * 2;
			else
				newmaxentries = WAIT_EVENT_USAGE_INITIAL_EVENTS;

			oldcontext = MemoryContextSwitchTo(usage->memcontext);
			if (usage->entries)
				usage->entries = repalloc_array(usage->entries,
												WaitEventUsageEntry,
												newmaxentries);
			else
				usage->entries = palloc_array(WaitEventUsageEntry,
											  newmaxentries);
			MemoryContextSwitchTo(oldcontext);

			usage->maxentries = newmaxentries;
		}

		entry = &usage->entries[usage->nentries++];
		entry->wait_event_info = wait_event_info;
		entry->calls = 0;
		INSTR_TIME_SET_ZERO(entry->time);
	}

	entry->calls++;
	INSTR_TIME_ADD(entry->time, *elapsed);
}

/* ----------
 * pgstat_get_wait_event_type() -
 *
 *	Return a string representing the current wait event type, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event_type(uint32 wait_event_info)
{
	uint32		classId;
	const char *event_type;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & WAIT_EVENT_CLASS_MASK;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_type = "LWLock";
			break;
		case PG_WAIT_LOCK:
			event_type = "Lock";
			break;
		case PG_WAIT_BUFFER:
			event_type = "Buffer";
			break;
		case PG_WAIT_ACTIVITY:
			event_type = "Activity";
			break;
		case PG_WAIT_CLIENT:
			event_type = "Client";
			break;
		case PG_WAIT_EXTENSION:
			event_type = "Extension";
			break;
		case PG_WAIT_IPC:
			event_type = "IPC";
			break;
		case PG_WAIT_TIMEOUT:
			event_type = "Timeout";
			break;
		case PG_WAIT_IO:
			event_type = "IO";
			break;
		case PG_WAIT_INJECTIONPOINT:
			event_type = "InjectionPoint";
			break;
		default:
			event_type = "???";
			break;
	}

	return event_type;
}

/* ----------
 * pgstat_get_wait_event() -
 *
 *	Return a string representing the current wait event, backend is
 *	waiting on.
 */
const char *
pgstat_get_wait_event(uint32 wait_event_info)
{
	uint32		classId;
	uint16		eventId;
	const char *event_name;

	/* report process as not waiting. */
	if (wait_event_info == 0)
		return NULL;

	classId = wait_event_info & WAIT_EVENT_CLASS_MASK;
	eventId = wait_event_info & WAIT_EVENT_ID_MASK;

	switch (classId)
	{
		case PG_WAIT_LWLOCK:
			event_name = GetLWLockIdentifier(classId, eventId);
			break;
		case PG_WAIT_LOCK:
			event_name = GetLockNameFromTagType(eventId);
			break;
		case PG_WAIT_EXTENSION:
		case PG_WAIT_INJECTIONPOINT:
			event_name = GetWaitEventCustomIdentifier(wait_event_info);
			break;
		case PG_WAIT_BUFFER:
			{
				WaitEventBuffer w = (WaitEventBuffer) wait_event_info;

				event_name = pgstat_get_wait_buffer(w);
				break;
			}
		case PG_WAIT_ACTIVITY:
			{
				WaitEventActivity w = (WaitEventActivity) wait_event_info;

				event_name = pgstat_get_wait_activity(w);
				break;
			}
		case PG_WAIT_CLIENT:
			{
				WaitEventClient w = (WaitEventClient) wait_event_info;

				event_name = pgstat_get_wait_client(w);
				break;
			}
		case PG_WAIT_IPC:
			{
				WaitEventIPC w = (WaitEventIPC) wait_event_info;

				event_name = pgstat_get_wait_ipc(w);
				break;
			}
		case PG_WAIT_TIMEOUT:
			{
				WaitEventTimeout w = (WaitEventTimeout) wait_event_info;

				event_name = pgstat_get_wait_timeout(w);
				break;
			}
		case PG_WAIT_IO:
			{
				WaitEventIO w = (WaitEventIO) wait_event_info;

				event_name = pgstat_get_wait_io(w);
				break;
			}
		default:
			event_name = "unknown wait event";
			break;
	}

	return event_name;
}

#include "utils/pgstat_wait_event.c"
