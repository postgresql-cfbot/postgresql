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

extern void amcheck_lock_relation(Oid indrelid, Relation *indrel,
								  Relation *heaprel, LOCKMODE lockmode);
extern void amcheck_unlock_relation(Oid indrelid, Relation indrel,
									Relation heaprel, LOCKMODE lockmode);
extern bool amcheck_index_mainfork_expected(Relation rel);

extern ItemId PageGetItemIdCareful(Relation rel, BlockNumber block,
					 Page page, OffsetNumber offset, size_t opaquesize);