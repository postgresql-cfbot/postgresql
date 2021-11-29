/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.h
 *	  prototypes for subscriptioncmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/subscriptioncmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SUBSCRIPTIONCMDS_H
#define SUBSCRIPTIONCMDS_H

#include "catalog/objectaddress.h"
#include "parser/parse_node.h"
#include "replication/walreceiver.h"

extern ObjectAddress CreateSubscription(ParseState *pstate, CreateSubscriptionStmt *stmt,
										bool isTopLevel);
extern ObjectAddress AlterSubscription(ParseState *pstate, AlterSubscriptionStmt *stmt, bool isTopLevel);
extern void DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel);

extern ObjectAddress AlterSubscriptionOwner(const char *name, Oid newOwnerId);
extern void AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId);

extern void ReplicationSlotDropAtPubNode(WalReceiverConn *wrconn, char *slotname, bool missing_ok);

#endif							/* SUBSCRIPTIONCMDS_H */
