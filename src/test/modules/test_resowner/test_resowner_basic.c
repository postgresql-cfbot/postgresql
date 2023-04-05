/*--------------------------------------------------------------------------
 *
 * test_resowner_basic.c
 *		Test basic ResourceOwner functionality
 *
 * Copyright (c) 2022-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_resowner/test_resowner_basic.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/ilist.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

PG_MODULE_MAGIC;

static void ReleaseString(Datum res);
static void PrintStringLeakWarning(Datum res);

static ResourceOwnerFuncs string_funcs = {
	.name = "test resource",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = ReleaseString,
	.PrintLeakWarning = PrintStringLeakWarning
};

static void
ReleaseString(Datum res)
{
	elog(NOTICE, "releasing string: %s", DatumGetPointer(res));
}

static void
PrintStringLeakWarning(Datum res)
{
	elog(WARNING, "string leak: %s", DatumGetPointer(res));
}

/* demonstrates phases and priorities betwen a parent and child context */
PG_FUNCTION_INFO_V1(test_resowner_priorities);
Datum
test_resowner_priorities(PG_FUNCTION_ARGS)
{
	int32		nkinds = PG_GETARG_INT32(0);
	int32		nresources = PG_GETARG_INT32(1);
	ResourceOwner parent,
				child;
	ResourceOwnerFuncs *before_funcs;
	ResourceOwnerFuncs *after_funcs;

	if (nkinds <= 0)
		elog(ERROR, "nkinds must be greater than zero");
	if (nresources <= 0)
		elog(ERROR, "nresources must be greater than zero");

	parent = ResourceOwnerCreate(CurrentResourceOwner, "test parent");
	child = ResourceOwnerCreate(parent, "test child");

	/* FIXME Add a bunch of resources to child, with different priorities */
	before_funcs = palloc(nkinds * sizeof(ResourceOwnerFuncs));
	for (int i = 0; i < nkinds; i++)
	{
		before_funcs[i].name = psprintf("test resource before locks %d", i);
		before_funcs[i].release_phase = RESOURCE_RELEASE_BEFORE_LOCKS;
		before_funcs[i].release_priority = i;
		before_funcs[i].ReleaseResource = ReleaseString;
		before_funcs[i].PrintLeakWarning = PrintStringLeakWarning;
	}
	after_funcs = palloc(nkinds * sizeof(ResourceOwnerFuncs));
	for (int i = 0; i < nkinds; i++)
	{
		after_funcs[i].name = psprintf("test resource after locks %d", i);
		after_funcs[i].release_phase = RESOURCE_RELEASE_AFTER_LOCKS;
		after_funcs[i].release_priority = i;
		after_funcs[i].ReleaseResource = ReleaseString;
		after_funcs[i].PrintLeakWarning = PrintStringLeakWarning;
	}

	ResourceOwnerEnlarge(child);

	for (int i = 0; i < nresources; i++)
	{
		ResourceOwnerFuncs *kind = &before_funcs[i % nkinds];

		ResourceOwnerEnlarge(child);
		ResourceOwnerRemember(child,
							  CStringGetDatum(psprintf("child before locks priority %d", kind->release_priority)),
							  kind);
	}

	for (int i = 0; i < nresources; i++)
	{
		ResourceOwnerFuncs *kind = &after_funcs[i % nkinds];

		ResourceOwnerEnlarge(child);
		ResourceOwnerRemember(child,
							  CStringGetDatum(psprintf("child after locks priority %d", kind->release_priority)),
							  kind);
	}

	for (int i = 0; i < nresources; i++)
	{
		ResourceOwnerFuncs *kind = &after_funcs[i % nkinds];

		ResourceOwnerEnlarge(parent);
		ResourceOwnerRemember(parent,
							  CStringGetDatum(psprintf("parent after locks priority %d", kind->release_priority)),
							  kind);
	}
	for (int i = 0; i < nresources; i++)
	{
		ResourceOwnerFuncs *kind = &before_funcs[i % nkinds];

		ResourceOwnerEnlarge(parent);
		ResourceOwnerRemember(parent,
							  CStringGetDatum(psprintf("parent before locks priority %d", kind->release_priority)),
							  kind);
	}

	elog(NOTICE, "releasing resources before locks");
	ResourceOwnerRelease(parent, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	elog(NOTICE, "releasing locks");
	ResourceOwnerRelease(parent, RESOURCE_RELEASE_LOCKS, false, false);
	elog(NOTICE, "releasing resources after locks");
	ResourceOwnerRelease(parent, RESOURCE_RELEASE_AFTER_LOCKS, false, false);

	ResourceOwnerDelete(parent);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_resowner_leak);
Datum
test_resowner_leak(PG_FUNCTION_ARGS)
{
	ResourceOwner resowner;

	resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

	ResourceOwnerEnlarge(resowner);

	ResourceOwnerRemember(resowner, CStringGetDatum("my string"), &string_funcs);

	/* don't call ResourceOwnerForget, so that it is leaked */

	ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);
	ResourceOwnerRelease(resowner, RESOURCE_RELEASE_LOCKS, true, false);
	ResourceOwnerRelease(resowner, RESOURCE_RELEASE_AFTER_LOCKS, true, false);

	ResourceOwnerDelete(resowner);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_resowner_remember_between_phases);
Datum
test_resowner_remember_between_phases(PG_FUNCTION_ARGS)
{
	ResourceOwner resowner;

	resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

	ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);

	/*
	 * Try to remember a new resource.  Fails because we already called
	 * ResourceOwnerRelease.
	 */
	ResourceOwnerEnlarge(resowner);
	ResourceOwnerRemember(resowner, CStringGetDatum("my string"), &string_funcs);

	/* unreachable */
	elog(ERROR, "ResourceOwnerEnlarge should have errored out");

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_resowner_forget_between_phases);
Datum
test_resowner_forget_between_phases(PG_FUNCTION_ARGS)
{
	ResourceOwner resowner;
	Datum		str_resource;

	resowner = ResourceOwnerCreate(CurrentResourceOwner, "TestOwner");

	ResourceOwnerEnlarge(resowner);
	str_resource = CStringGetDatum("my string");
	ResourceOwnerRemember(resowner, str_resource, &string_funcs);

	ResourceOwnerRelease(resowner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);

	/*
	 * Try to forget the resource that was remembered earlier.  Fails because
	 * we already called ResourceOwnerRelease.
	 */
	ResourceOwnerForget(resowner, str_resource, &string_funcs);

	/* unreachable */
	elog(ERROR, "ResourceOwnerForget should have errored out");

	PG_RETURN_VOID();
}
