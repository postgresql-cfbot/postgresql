/*-------------------------------------------------------------------------
 *
 * keyjoin.h
 *	  prototypes for key-join command support.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/keyjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KEYJOIN_H
#define KEYJOIN_H

#include "catalog/objectaddress.h"

extern void RevalidateDependentKeyJoinObjectsOnConstraint(Oid constraintOid);
extern void RevalidateDependentKeyJoinObjectsOnRelation(Oid relationOid);
extern void RevalidateDependentKeyJoinObjectsOnProcedure(Oid procOid);

#endif							/* KEYJOIN_H */
