/*-------------------------------------------------------------------------
 *
 * custodian.h
 *   Exports from postmaster/custodian.c.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * src/include/postmaster/custodian.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _CUSTODIAN_H
#define _CUSTODIAN_H

extern void CustodianMain(void) pg_attribute_noreturn();

#endif						/* _CUSTODIAN_H */
