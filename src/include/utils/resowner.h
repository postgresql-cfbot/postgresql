/*-------------------------------------------------------------------------
 *
 * resowner.h
 *	  POSTGRES resource owner definitions.
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
 * src/include/utils/resowner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESOWNER_H
#define RESOWNER_H


/*
 * ResourceOwner objects are an opaque data structure known only within
 * resowner.c.
 */
typedef struct ResourceOwnerData *ResourceOwner;


/*
 * Globally known ResourceOwners
 */
extern PGDLLIMPORT ResourceOwner CurrentResourceOwner;
extern PGDLLIMPORT ResourceOwner CurTransactionResourceOwner;
extern PGDLLIMPORT ResourceOwner TopTransactionResourceOwner;
extern PGDLLIMPORT ResourceOwner AuxProcessResourceOwner;

/*
 * Resource releasing is done in three phases: pre-locks, locks, and
 * post-locks.  The pre-lock phase must release any resources that are
 * visible to other backends (such as pinned buffers); this ensures that
 * when we release a lock that another backend may be waiting on, it will
 * see us as being fully out of our transaction.  The post-lock phase
 * should be used for backend-internal cleanup.
 */
typedef enum
{
	RESOURCE_RELEASE_BEFORE_LOCKS,
	RESOURCE_RELEASE_LOCKS,
	RESOURCE_RELEASE_AFTER_LOCKS
} ResourceReleasePhase;

/*
 * In order to track an object, resowner.c needs a few callbacks for it.
 * The callbacks for resources of a specific kind are encapsulated in
 * ResourceOwnerFuncs.
 *
 * Note that the callback occurs post-commit or post-abort, so these callback
 * functions can only do noncritical cleanup.
 */
typedef struct ResourceOwnerFuncs
{
	const char *name;			/* name for the object kind, for debugging */

	ResourceReleasePhase phase; /* when are these objects released? */

	/*
	 * Release resource.
	 *
	 * NOTE: this must call ResourceOwnerForget to disassociate it with the
	 * resource owner.
	 */
	void		(*ReleaseResource) (Datum res);

	/*
	 * Print a warning, when a resource has not been properly released before
	 * commit.
	 */
	void		(*PrintLeakWarning) (Datum res);

} ResourceOwnerFuncs;

/*
 *	Dynamically loaded modules can get control during ResourceOwnerRelease
 *	by providing a callback of this form.
 */
typedef void (*ResourceReleaseCallback) (ResourceReleasePhase phase,
										 bool isCommit,
										 bool isTopLevel,
										 void *arg);


/*
 * Functions in resowner.c
 */

/* generic routines */
extern ResourceOwner ResourceOwnerCreate(ResourceOwner parent,
										 const char *name);
extern void ResourceOwnerRelease(ResourceOwner owner,
								 ResourceReleasePhase phase,
								 bool isCommit,
								 bool isTopLevel);
extern void ResourceOwnerDelete(ResourceOwner owner);
extern ResourceOwner ResourceOwnerGetParent(ResourceOwner owner);
extern void ResourceOwnerNewParent(ResourceOwner owner,
								   ResourceOwner newparent);

extern void ResourceOwnerEnlarge(ResourceOwner owner);
extern void ResourceOwnerRemember(ResourceOwner owner, Datum res, ResourceOwnerFuncs *kind);
extern void ResourceOwnerForget(ResourceOwner owner, Datum res, ResourceOwnerFuncs *kind);

extern void RegisterResourceReleaseCallback(ResourceReleaseCallback callback,
											void *arg);
extern void UnregisterResourceReleaseCallback(ResourceReleaseCallback callback,
											  void *arg);

extern void CreateAuxProcessResourceOwner(void);
extern void ReleaseAuxProcessResources(bool isCommit);

/* special support for local lock management */
struct LOCALLOCK;
extern void ResourceOwnerRememberLock(ResourceOwner owner, struct LOCALLOCK *locallock);
extern void ResourceOwnerForgetLock(ResourceOwner owner, struct LOCALLOCK *locallock);

/* special function to relase all plancache references */
extern void ResourceOwnerReleaseAllPlanCacheRefs(ResourceOwner owner);

#endif							/* RESOWNER_H */
