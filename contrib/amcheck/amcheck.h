/*-------------------------------------------------------------------------
 *
 * amcheck.h
 *		Shared routines for amcheck verifications.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/amcheck.h
 *
 *-------------------------------------------------------------------------
 */
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "miscadmin.h"

/* Typedefs for callback functions for amcheck_lock_relation */
typedef void (*IndexCheckableCallback) (Relation index);
typedef void (*IndexDoCheckCallback) (Relation rel, Relation heaprel, void* state);

extern void amcheck_lock_relation_and_check(Oid indrelid,
											IndexCheckableCallback checkable,
											IndexDoCheckCallback check,
											LOCKMODE lockmode, void *state);

extern ItemId PageGetItemIdCareful(Relation rel, BlockNumber block,
					 Page page, OffsetNumber offset, size_t opaquesize);