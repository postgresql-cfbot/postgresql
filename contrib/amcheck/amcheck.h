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

#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

extern void
amcheck_lock_relation(Oid indrelid, Relation *indrel,
						Relation *heaprel, LOCKMODE	lockmode);

extern void
amcheck_unlock_relation(Oid indrelid, Relation indrel, Relation heaprel, LOCKMODE	lockmode);
