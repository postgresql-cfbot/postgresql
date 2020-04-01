/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management module for transparent data encryption
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/crypto/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "common/cipher.h"
#include "common/kmgr_utils.h"
#include "storage/relfilenode.h"
#include "storage/bufpage.h"

/* GUC parameters */
extern bool key_management_enabled;
extern char *cluster_passphrase_command;

extern void BootStrapKmgr(void);
extern void InitializeKmgr(void);

#endif							/* KMGR_H */
