/*-------------------------------------------------------------------------
 *
 * resowner.c
 *	  POSTGRES resource owner management code.
 *
 * Query-lifespan resources are tracked by associating them with
 * ResourceOwner objects.  This provides a simple mechanism for ensuring
 * that such resources are freed at the right time.
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/resowner/resowner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "storage/ipc.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/resowner.h"

/*
 * ResourceElem represents a reference associated with a resource owner.
 *
 * All objects managed by this code are required to fit into a Datum,
 * which is fine since they are generally pointers or integers.
 */
typedef struct ResourceElem
{
	Datum		item;
	ResourceOwnerFuncs *kind;	/* NULL indicates a free hash table slot */
} ResourceElem;

/*
 * Size of the small fixed-size array to hold most-recently remembered resources.
 */
#define RESOWNER_ARRAY_SIZE 32

/*
 * Initially allocated size of a ResourceOwner's hash.  Must be power of two since
 * we'll use (capacity - 1) as mask for hashing.
 */
#define RESOWNER_HASH_INIT_SIZE 64

/*
 * How many items may be stored in a hash of given capacity.
 * When this number is reached, we must resize.
 */
#define RESOWNER_HASH_MAX_ITEMS(capacity) ((capacity)/4 * 3)

StaticAssertDecl(RESOWNER_HASH_MAX_ITEMS(RESOWNER_HASH_INIT_SIZE) > RESOWNER_ARRAY_SIZE,
				 "initial hash size too small compared to array size");

/*
 * To speed up bulk releasing or reassigning locks from a resource owner to
 * its parent, each resource owner has a small cache of locks it owns. The
 * lock manager has the same information in its local lock hash table, and
 * we fall back on that if cache overflows, but traversing the hash table
 * is slower when there are a lot of locks belonging to other resource owners.
 *
 * MAX_RESOWNER_LOCKS is the size of the per-resource owner cache. It's
 * chosen based on some testing with pg_dump with a large schema. When the
 * tests were done (on 9.2), resource owners in a pg_dump run contained up
 * to 9 locks, regardless of the schema size, except for the top resource
 * owner which contained much more (overflowing the cache). 15 seems like a
 * nice round number that's somewhat higher than what pg_dump needs. Note that
 * making this number larger is not free - the bigger the cache, the slower
 * it is to release locks (in retail), when a resource owner holds many locks.
 */
#define MAX_RESOWNER_LOCKS 15

/*
 * ResourceOwner objects look like this
 */
typedef struct ResourceOwnerData
{
	ResourceOwner parent;		/* NULL if no parent (toplevel owner) */
	ResourceOwner firstchild;	/* head of linked list of children */
	ResourceOwner nextchild;	/* next child of same parent */
	const char *name;			/* name (just for debugging) */

	/*
	 * These structs keep track of the objects registered with this owner.
	 *
	 * We manage a small set of references by keeping them in a simple array.
	 * When the array gets full, all the elements in the array are moved to a
	 * hash table.  This way, the array always contains a few most recently
	 * remembered references.  To find a particular reference, you need to
	 * search both the array and the hash table.
	 */
	ResourceElem arr[RESOWNER_ARRAY_SIZE];
	uint32		narr;			/* how many items are stored in the array */

	/*
	 * The hash table.  Uses open-addressing.  'nhash' is the number of items
	 * present; if it would exceed 'grow_at', we enlarge it and re-hash.
	 * 'grow_at' should be rather less than 'capacity' so that we don't waste
	 * too much time searching for empty slots.
	 */
	ResourceElem *hash;
	uint32		nhash;			/* how many items are stored in the hash */
	uint32		capacity;		/* allocated length of hash[] */
	uint32		grow_at;		/* grow hash when reach this */

	/* We can remember up to MAX_RESOWNER_LOCKS references to local locks. */
	int			nlocks;			/* number of owned locks */
	LOCALLOCK  *locks[MAX_RESOWNER_LOCKS];	/* list of owned locks */
} ResourceOwnerData;


/*****************************************************************************
 *	  GLOBAL MEMORY															 *
 *****************************************************************************/

ResourceOwner CurrentResourceOwner = NULL;
ResourceOwner CurTransactionResourceOwner = NULL;
ResourceOwner TopTransactionResourceOwner = NULL;
ResourceOwner AuxProcessResourceOwner = NULL;

/* #define RESOWNER_STATS */
/* #define RESOWNER_TRACE */

#ifdef RESOWNER_STATS
static int	narray_lookups = 0;
static int	nhash_lookups = 0;
#endif

#ifdef RESOWNER_TRACE
static int	resowner_trace_counter = 0;
#endif

/*
 * List of add-on callbacks for resource releasing
 */
typedef struct ResourceReleaseCallbackItem
{
	struct ResourceReleaseCallbackItem *next;
	ResourceReleaseCallback callback;
	void	   *arg;
} ResourceReleaseCallbackItem;

static ResourceReleaseCallbackItem *ResourceRelease_callbacks = NULL;


/* Internal routines */
static inline uint32 hash_resource_elem(Datum value, ResourceOwnerFuncs *kind);
static void ResourceOwnerAddToHash(ResourceOwner owner, Datum value,
								   ResourceOwnerFuncs *kind);
static void ResourceOwnerReleaseAll(ResourceOwner owner,
									ResourceReleasePhase phase,
									bool printLeakWarnings);
static void ResourceOwnerReleaseInternal(ResourceOwner owner,
										 ResourceReleasePhase phase,
										 bool isCommit,
										 bool isTopLevel);
static void ReleaseAuxProcessResourcesCallback(int code, Datum arg);


/*****************************************************************************
 *	  INTERNAL ROUTINES														 *
 *****************************************************************************/

/*
 * Hash function for value+kind combination.
 */
static inline uint32
hash_resource_elem(Datum value, ResourceOwnerFuncs *kind)
{
	/*
	 * Most resource kinds store a pointer in 'value', and pointers are unique
	 * all on their own.  But some resources store plain integers (Files and
	 * Buffers as of this writing), so we want to incorporate the 'kind' in
	 * the hash too, otherwise those resources will collide a lot.  But
	 * because there are only a few resource kinds like that - and only a few
	 * resource kinds to begin with - we don't need to work too hard to mix
	 * 'kind' into the hash.  Just add it with hash_combine(), it perturbs the
	 * result enough for our purposes.
	 */
#if SIZEOF_DATUM == 8
	return hash_combine64(murmurhash64((uint64) value), (uint64) kind);
#else
	return hash_combine(murmurhash32((uint32) value), (uint32) kind);
#endif
}

/*
 * Adds 'value' of given 'kind' to the ResourceOwner's hash table
 */
static void
ResourceOwnerAddToHash(ResourceOwner owner, Datum value, ResourceOwnerFuncs *kind)
{
	/* Insert into first free slot at or after hash location. */
	uint32		mask = owner->capacity - 1;
	uint32		idx;

	Assert(kind != NULL);

	idx = hash_resource_elem(value, kind) & mask;
	for (;;)
	{
		if (owner->hash[idx].kind == NULL)
			break;				/* found a free slot */
		idx = (idx + 1) & mask;
	}
	owner->hash[idx].item = value;
	owner->hash[idx].kind = kind;
	owner->nhash++;
}

/*
 * Call the ReleaseResource callback on entries with given 'phase'.
 */
static void
ResourceOwnerReleaseAll(ResourceOwner owner, ResourceReleasePhase phase,
						bool printLeakWarnings)
{
	bool		found;
	int			capacity;

	/*
	 * First handle all the entries in the array.
	 *
	 * Note that ReleaseResource() will call ResourceOwnerForget) and remove
	 * the entry from our array, so we just have to iterate till there is
	 * nothing left to remove.
	 */
	do
	{
		found = false;
		for (int i = 0; i < owner->narr; i++)
		{
			if (owner->arr[i].kind->phase == phase)
			{
				Datum		value = owner->arr[i].item;
				ResourceOwnerFuncs *kind = owner->arr[i].kind;

				if (printLeakWarnings)
					kind->PrintLeakWarning(value);
				kind->ReleaseResource(value);
				found = true;
			}
		}

		/*
		 * If any resources were released, check again because some of the
		 * elements might have been moved by the callbacks.  We don't want to
		 * miss them.
		 */
	} while (found && owner->narr > 0);

	/*
	 * Ok, the array has now been handled.  Then the hash.  Like with the
	 * array, ReleaseResource() will remove the entry from the hash.
	 */
	do
	{
		capacity = owner->capacity;
		for (int idx = 0; idx < capacity; idx++)
		{
			while (owner->hash[idx].kind != NULL &&
				   owner->hash[idx].kind->phase == phase)
			{
				Datum		value = owner->hash[idx].item;
				ResourceOwnerFuncs *kind = owner->hash[idx].kind;

				if (printLeakWarnings)
					kind->PrintLeakWarning(value);
				kind->ReleaseResource(value);

				/*
				 * If the same resource is remembered more than once in this
				 * resource owner, the ReleaseResource callback might've
				 * released a different copy of it.  Because of that, loop to
				 * check the same index again.
				 */
			}
		}

		/*
		 * It's possible that the callbacks acquired more resources, causing
		 * the hash table to grow and the existing entries to be moved around.
		 * If that happened, scan the hash table again, so that we don't miss
		 * entries that were moved.  (XXX: I'm not sure if any of the
		 * callbacks actually do that, but this is cheap to check, and better
		 * safe than sorry.)
		 */
		Assert(owner->capacity >= capacity);
	} while (capacity != owner->capacity);
}


/*****************************************************************************
 *	  EXPORTED ROUTINES														 *
 *****************************************************************************/


/*
 * ResourceOwnerCreate
 *		Create an empty ResourceOwner.
 *
 * All ResourceOwner objects are kept in TopMemoryContext, since they should
 * only be freed explicitly.
 */
ResourceOwner
ResourceOwnerCreate(ResourceOwner parent, const char *name)
{
	ResourceOwner owner;

	owner = (ResourceOwner) MemoryContextAllocZero(TopMemoryContext,
												   sizeof(ResourceOwnerData));
	owner->name = name;

	if (parent)
	{
		owner->parent = parent;
		owner->nextchild = parent->firstchild;
		parent->firstchild = owner;
	}

#ifdef RESOWNER_TRACE
	elog(LOG, "CREATE %d: %p %s",
		 resowner_trace_counter++, owner, name);
#endif

	return owner;
}

/*
 * Make sure there is room for at least one more resource in an array.
 *
 * This is separate from actually inserting a resource because if we run out
 * of memory, it's critical to do so *before* acquiring the resource.
 *
 * NB: Make sure there are no unrelated ResourceOwnerRemember() calls between
 * your ResourceOwnerEnlarge() call and the ResourceOwnerRemember() call that
 * you reserved the space for!
 */
void
ResourceOwnerEnlarge(ResourceOwner owner)
{
	if (owner->narr < RESOWNER_ARRAY_SIZE)
		return;					/* no work needed */

	/* Is there space in the hash? If not, enlarge it. */
	if (owner->narr + owner->nhash >= owner->grow_at)
	{
		uint32		i,
					oldcap,
					newcap;
		ResourceElem *oldhash;
		ResourceElem *newhash;

		oldhash = owner->hash;
		oldcap = owner->capacity;

		/* Double the capacity (it must stay a power of 2!) */
		newcap = (oldcap > 0) ? oldcap * 2 : RESOWNER_HASH_INIT_SIZE;
		newhash = (ResourceElem *) MemoryContextAllocZero(TopMemoryContext,
														  newcap * sizeof(ResourceElem));

		/*
		 * We assume we can't fail below this point, so OK to scribble on the
		 * owner
		 */
		owner->hash = newhash;
		owner->capacity = newcap;
		owner->grow_at = RESOWNER_HASH_MAX_ITEMS(newcap);
		owner->nhash = 0;

		if (oldhash != NULL)
		{
			/*
			 * Transfer any pre-existing entries into the new hash table; they
			 * don't necessarily go where they were before, so this simple
			 * logic is the best way.
			 */
			for (i = 0; i < oldcap; i++)
			{
				if (oldhash[i].kind != NULL)
					ResourceOwnerAddToHash(owner, oldhash[i].item, oldhash[i].kind);
			}

			/* And release old hash table. */
			pfree(oldhash);
		}
	}

	/* Move items from the array to the hash */
	Assert(owner->narr == RESOWNER_ARRAY_SIZE);
	for (int i = 0; i < owner->narr; i++)
	{
		ResourceOwnerAddToHash(owner, owner->arr[i].item, owner->arr[i].kind);
	}
	owner->narr = 0;

	Assert(owner->nhash < owner->grow_at);
}

/*
 * Remember that an object is owner by a ReourceOwner
 *
 * Caller must have previously done ResourceOwnerEnlarge()
 */
void
ResourceOwnerRemember(ResourceOwner owner, Datum value, ResourceOwnerFuncs *kind)
{
	uint32		idx;

#ifdef RESOWNER_TRACE
	elog(LOG, "REMEMBER %d: owner %p value " UINT64_FORMAT ", kind: %s",
		 resowner_trace_counter++, owner, DatumGetUInt64(value), kind->name);
#endif

	if (owner->narr >= RESOWNER_ARRAY_SIZE)
	{
		/* forgot to call ResourceOwnerEnlarge? */
		elog(ERROR, "ResourceOwnerRemember called but array was full");
	}

	/* Append to linear array. */
	idx = owner->narr;
	owner->arr[idx].item = value;
	owner->arr[idx].kind = kind;
	owner->narr++;
}

/*
 * Forget that an object is owned by a ResourceOwner
 *
 * Note: if same resource ID is associated with the ResourceOwner more than once,
 * one instance is removed.
 */
void
ResourceOwnerForget(ResourceOwner owner, Datum value, ResourceOwnerFuncs *kind)
{
	uint32		i,
				idx;

#ifdef RESOWNER_TRACE
	elog(LOG, "FORGET %d: owner %p value " UINT64_FORMAT ", kind: %s",
		 resowner_trace_counter++, owner, DatumGetUInt64(value), kind->name);
#endif

	/* Search through all items, but check the array first. */
	for (i = 0; i < owner->narr; i++)
	{
		if (owner->arr[i].item == value &&
			owner->arr[i].kind == kind)
		{
			owner->arr[i] = owner->arr[owner->narr - 1];
			owner->narr--;

#ifdef RESOWNER_STATS
			narray_lookups++;
#endif

			return;
		}
	}

	/* Search hash */
	if (owner->nhash > 0)
	{
		uint32		mask = owner->capacity - 1;

		idx = hash_resource_elem(value, kind) & mask;
		for (i = 0; i < owner->capacity; i++)
		{
			if (owner->hash[idx].item == value &&
				owner->hash[idx].kind == kind)
			{
				owner->hash[idx].item = (Datum) 0;
				owner->hash[idx].kind = NULL;
				owner->nhash--;

#ifdef RESOWNER_STATS
				nhash_lookups++;
#endif
				return;
			}
			idx = (idx + 1) & mask;
		}
	}

	/*
	 * Use %p to print the reference, since most objects tracked by a resource
	 * owner are pointers.  It's a bit misleading if it's not a pointer, but
	 * this is a programmer error, anyway.
	 */
	elog(ERROR, "%s %p is not owned by resource owner %s",
		 kind->name, DatumGetPointer(value), owner->name);
}

/*
 * ResourceOwnerRelease
 *		Release all resources owned by a ResourceOwner and its descendants,
 *		but don't delete the owner objects themselves.
 *
 * Note that this executes just one phase of release, and so typically
 * must be called three times.  We do it this way because (a) we want to
 * do all the recursion separately for each phase, thereby preserving
 * the needed order of operations; and (b) xact.c may have other operations
 * to do between the phases.
 *
 * phase: release phase to execute
 * isCommit: true for successful completion of a query or transaction,
 *			false for unsuccessful
 * isTopLevel: true if completing a main transaction, else false
 *
 * isCommit is passed because some modules may expect that their resources
 * were all released already if the transaction or portal finished normally.
 * If so it is reasonable to give a warning (NOT an error) should any
 * unreleased resources be present.  When isCommit is false, such warnings
 * are generally inappropriate.
 *
 * isTopLevel is passed when we are releasing TopTransactionResourceOwner
 * at completion of a main transaction.  This generally means that *all*
 * resources will be released, and so we can optimize things a bit.
 */
void
ResourceOwnerRelease(ResourceOwner owner,
					 ResourceReleasePhase phase,
					 bool isCommit,
					 bool isTopLevel)
{
	/* There's not currently any setup needed before recursing */
	ResourceOwnerReleaseInternal(owner, phase, isCommit, isTopLevel);

#ifdef RESOWNER_STATS
	if (isTopLevel)
	{
		elog(LOG, "RESOWNER STATS: lookups: array %d, hash %d", narray_lookups, nhash_lookups);
		narray_lookups = 0;
		nhash_lookups = 0;
	}
#endif
}

static void
ResourceOwnerReleaseInternal(ResourceOwner owner,
							 ResourceReleasePhase phase,
							 bool isCommit,
							 bool isTopLevel)
{
	ResourceOwner child;
	ResourceOwner save;
	ResourceReleaseCallbackItem *item;
	ResourceReleaseCallbackItem *next;

	/* Recurse to handle descendants */
	for (child = owner->firstchild; child != NULL; child = child->nextchild)
		ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);

	/*
	 * Make CurrentResourceOwner point to me, so that ReleaseBuffer etc don't
	 * get confused.
	 */
	save = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS)
	{
		/*
		 * Release all resources that need to be released before the locks.
		 *
		 * During a commit, there shouldn't be any remaining resources ---
		 * that would indicate failure to clean up the executor correctly ---
		 * so issue warnings.  In the abort case, just clean up quietly.
		 */
		ResourceOwnerReleaseAll(owner, phase, isCommit);
	}
	else if (phase == RESOURCE_RELEASE_LOCKS)
	{
		if (isTopLevel)
		{
			/*
			 * For a top-level xact we are going to release all locks (or at
			 * least all non-session locks), so just do a single lmgr call at
			 * the top of the recursion.
			 */
			if (owner == TopTransactionResourceOwner)
			{
				ProcReleaseLocks(isCommit);
				ReleasePredicateLocks(isCommit, false);
			}
		}
		else
		{
			/*
			 * Release locks retail.  Note that if we are committing a
			 * subtransaction, we do NOT release its locks yet, but transfer
			 * them to the parent.
			 */
			LOCALLOCK **locks;
			int			nlocks;

			Assert(owner->parent != NULL);

			/*
			 * Pass the list of locks owned by this resource owner to the lock
			 * manager, unless it has overflowed.
			 */
			if (owner->nlocks > MAX_RESOWNER_LOCKS)
			{
				locks = NULL;
				nlocks = 0;
			}
			else
			{
				locks = owner->locks;
				nlocks = owner->nlocks;
			}

			if (isCommit)
				LockReassignCurrentOwner(locks, nlocks);
			else
				LockReleaseCurrentOwner(locks, nlocks);
		}
	}
	else if (phase == RESOURCE_RELEASE_AFTER_LOCKS)
	{
		/*
		 * Release all resources that need to be released after the locks.
		 */
		ResourceOwnerReleaseAll(owner, phase, isCommit);
	}

	/* Let add-on modules get a chance too */
	for (item = ResourceRelease_callbacks; item; item = next)
	{
		/* allow callbacks to unregister themselves when called */
		next = item->next;
		item->callback(phase, isCommit, isTopLevel, item->arg);
	}

	CurrentResourceOwner = save;
}

/*
 * ResourceOwnerReleaseAllPlanCacheRefs
 *		Release the plancache references (only) held by this owner.
 *
 * We might eventually add similar functions for other resource types,
 * but for now, only this is needed.
 */
void
ResourceOwnerReleaseAllPlanCacheRefs(ResourceOwner owner)
{
	/* array first */
	for (int i = 0; i < owner->narr; i++)
	{
		if (owner->arr[i].kind == &planref_resowner_funcs)
		{
			CachedPlan *planref = (CachedPlan *) DatumGetPointer(owner->arr[i].item);

			owner->arr[i] = owner->arr[owner->narr - 1];
			owner->narr--;
			i--;

			/*
			 * pass 'NULL' because we already removed the entry from the
			 * resowner
			 */
			ReleaseCachedPlan(planref, NULL);
		}
	}

	/* Then hash */
	for (int i = 0; i < owner->capacity; i++)
	{
		if (owner->hash[i].kind == &planref_resowner_funcs)
		{
			CachedPlan *planref = (CachedPlan *) DatumGetPointer(owner->hash[i].item);

			owner->hash[i].item = (Datum) 0;
			owner->hash[i].kind = NULL;
			owner->nhash--;

			/*
			 * pass 'NULL' because we already removed the entry from the
			 * resowner
			 */
			ReleaseCachedPlan(planref, NULL);
		}
	}
}

/*
 * ResourceOwnerDelete
 *		Delete an owner object and its descendants.
 *
 * The caller must have already released all resources in the object tree.
 */
void
ResourceOwnerDelete(ResourceOwner owner)
{
	/* We had better not be deleting CurrentResourceOwner ... */
	Assert(owner != CurrentResourceOwner);

	/* And it better not own any resources, either */
	Assert(owner->narr == 0);
	Assert(owner->nhash == 0);
	Assert(owner->nlocks == 0 || owner->nlocks == MAX_RESOWNER_LOCKS + 1);

#ifdef RESOWNER_TRACE
	elog(LOG, "DELETE %d: %p %s",
		 resowner_trace_counter++, owner, owner->name);
#endif

	/*
	 * Delete children.  The recursive call will delink the child from me, so
	 * just iterate as long as there is a child.
	 */
	while (owner->firstchild != NULL)
		ResourceOwnerDelete(owner->firstchild);

	/*
	 * We delink the owner from its parent before deleting it, so that if
	 * there's an error we won't have deleted/busted owners still attached to
	 * the owner tree.  Better a leak than a crash.
	 */
	ResourceOwnerNewParent(owner, NULL);

	/* And free the object. */
	if (owner->hash)
		pfree(owner->hash);
	pfree(owner);
}

/*
 * Fetch parent of a ResourceOwner (returns NULL if top-level owner)
 */
ResourceOwner
ResourceOwnerGetParent(ResourceOwner owner)
{
	return owner->parent;
}

/*
 * Reassign a ResourceOwner to have a new parent
 */
void
ResourceOwnerNewParent(ResourceOwner owner,
					   ResourceOwner newparent)
{
	ResourceOwner oldparent = owner->parent;

	if (oldparent)
	{
		if (owner == oldparent->firstchild)
			oldparent->firstchild = owner->nextchild;
		else
		{
			ResourceOwner child;

			for (child = oldparent->firstchild; child; child = child->nextchild)
			{
				if (owner == child->nextchild)
				{
					child->nextchild = owner->nextchild;
					break;
				}
			}
		}
	}

	if (newparent)
	{
		Assert(owner != newparent);
		owner->parent = newparent;
		owner->nextchild = newparent->firstchild;
		newparent->firstchild = owner;
	}
	else
	{
		owner->parent = NULL;
		owner->nextchild = NULL;
	}
}

/*
 * Register or deregister callback functions for resource cleanup
 *
 * These functions can be used by dynamically loaded modules.  These used
 * to be the only way for an extension to register custom resource types
 * with a resource owner, but nowadays it is easier to define a new
 * ResourceOwnerFuncs instance with custom callbacks.
 */
void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	ResourceReleaseCallbackItem *item;

	item = (ResourceReleaseCallbackItem *)
		MemoryContextAlloc(TopMemoryContext,
						   sizeof(ResourceReleaseCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = ResourceRelease_callbacks;
	ResourceRelease_callbacks = item;
}

void
UnregisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	ResourceReleaseCallbackItem *item;
	ResourceReleaseCallbackItem *prev;

	prev = NULL;
	for (item = ResourceRelease_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				ResourceRelease_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

/*
 * Establish an AuxProcessResourceOwner for the current process.
 */
void
CreateAuxProcessResourceOwner(void)
{
	Assert(AuxProcessResourceOwner == NULL);
	Assert(CurrentResourceOwner == NULL);
	AuxProcessResourceOwner = ResourceOwnerCreate(NULL, "AuxiliaryProcess");
	CurrentResourceOwner = AuxProcessResourceOwner;

	/*
	 * Register a shmem-exit callback for cleanup of aux-process resource
	 * owner.  (This needs to run after, e.g., ShutdownXLOG.)
	 */
	on_shmem_exit(ReleaseAuxProcessResourcesCallback, 0);
}

/*
 * Convenience routine to release all resources tracked in
 * AuxProcessResourceOwner (but that resowner is not destroyed here).
 * Warn about leaked resources if isCommit is true.
 */
void
ReleaseAuxProcessResources(bool isCommit)
{
	/*
	 * At this writing, the only thing that could actually get released is
	 * buffer pins; but we may as well do the full release protocol.
	 */
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 isCommit, true);
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 isCommit, true);
	ResourceOwnerRelease(AuxProcessResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 isCommit, true);
}

/*
 * Shmem-exit callback for the same.
 * Warn about leaked resources if process exit code is zero (ie normal).
 */
static void
ReleaseAuxProcessResourcesCallback(int code, Datum arg)
{
	bool		isCommit = (code == 0);

	ReleaseAuxProcessResources(isCommit);
}

/*
 * Remember that a Local Lock is owned by a ResourceOwner
 *
 * This is different from the other Remember functions in that the list of
 * locks is only a lossy cache.  It can hold up to MAX_RESOWNER_LOCKS entries,
 * and when it overflows, we stop tracking locks.  The point of only remembering
 * only up to MAX_RESOWNER_LOCKS entries is that if a lot of locks are held,
 * ResourceOwnerForgetLock doesn't need to scan through a large array to find
 * the entry.
 */
void
ResourceOwnerRememberLock(ResourceOwner owner, LOCALLOCK *locallock)
{
	Assert(locallock != NULL);

	if (owner->nlocks > MAX_RESOWNER_LOCKS)
		return;					/* we have already overflowed */

	if (owner->nlocks < MAX_RESOWNER_LOCKS)
		owner->locks[owner->nlocks] = locallock;
	else
	{
		/* overflowed */
	}
	owner->nlocks++;
}

/*
 * Forget that a Local Lock is owned by a ResourceOwner
 */
void
ResourceOwnerForgetLock(ResourceOwner owner, LOCALLOCK *locallock)
{
	int			i;

	if (owner->nlocks > MAX_RESOWNER_LOCKS)
		return;					/* we have overflowed */

	Assert(owner->nlocks > 0);
	for (i = owner->nlocks - 1; i >= 0; i--)
	{
		if (locallock == owner->locks[i])
		{
			owner->locks[i] = owner->locks[owner->nlocks - 1];
			owner->nlocks--;
			return;
		}
	}
	elog(ERROR, "lock reference %p is not owned by resource owner %s",
		 locallock, owner->name);
}
