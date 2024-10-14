/* -------------------------------------------------------------------------
 *
 * pg_subscription_conflict.h
 *    definition of the "subscription conflict resolver" system
 *    catalog (pg_subscription_conflict)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_subscription_conflict.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_CONFLICT_H
#define PG_SUBSCRIPTION_CONFLICT_H

#include "catalog/genbki.h"
#include "catalog/pg_subscription_conflict_d.h"

/* ----------------
 *		pg_subscription_conflict definition.  cpp turns this into
 *		typedef struct FormData_pg_subscription_conflict
 * ----------------
 */
CATALOG(pg_subscription_conflict,8881,SubscriptionConflictId)
{
	Oid			oid;			/* OID of the object itself */
	Oid			confsubid BKI_LOOKUP(pg_subscription);	/* OID of subscription */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		conftype BKI_FORCE_NOT_NULL;	/* conflict type */
	text		confres BKI_FORCE_NOT_NULL; /* conflict resolver */
#endif
} FormData_pg_subscription_conflict;

/* ----------------
 *		Form_pg_subscription_conflict corresponds to a pointer to a row with
 *		the format of pg_subscription_conflict relation.
 * ----------------
 */
typedef FormData_pg_subscription_conflict *Form_pg_subscription_conflict;

DECLARE_TOAST(pg_subscription_conflict, 8882, 8883);

DECLARE_UNIQUE_INDEX_PKEY(pg_subscription_conflict_oid_index, 8884, SubscriptionConflictOidIndexId, pg_subscription_conflict, btree(oid oid_ops));

DECLARE_UNIQUE_INDEX(pg_subscription_conflict_confsubid_conftype_index, 8885, SubscriptionConflictSubIdTypeIndexId, pg_subscription_conflict, btree(confsubid oid_ops, conftype text_ops));

MAKE_SYSCACHE(SUBSCRIPTIONCONFLMAP, pg_subscription_conflict_confsubid_conftype_index, 256);

#endif							/* PG_SUBSCRIPTION_CONFLICT_H */
