/*-------------------------------------------------------------------------
 *
 * nss.h
 *	  NSS supporting functionality shared between frontend and backend
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/include/common/nss.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMON_NSS_H
#define COMMON_NSS_H

#ifdef USE_NSS

/*
 * BITS_PER_BYTE is also defined in the NSPR header files, so we need to undef
 * our version to avoid compiler warnings on redefinition.
 */
#define pg_BITS_PER_BYTE BITS_PER_BYTE
#undef BITS_PER_BYTE
/*
 * The nspr/obsolete/protypes.h NSPR header typedefs uint64 and int64 with
 * colliding definitions from ours, causing a much expected compiler error.
 * Remove backwards compatibility with ancient NSPR versions to avoid this.
 */
#define NO_NSPR_10_SUPPORT
#include <nspr/nspr.h>
#include <nspr/prerror.h>
#include <nspr/prio.h>
#include <nspr/prmem.h>
#include <nspr/prtypes.h>


#include <nss/nss.h>
#include <nss/hasht.h>
#include <nss/secoidt.h>
#include <nss/sslproto.h>

/* src/common/cipher_nss.c */
bool pg_find_cipher(char *name, PRUint16 *cipher);
bool pg_find_signature_algorithm(SECOidTag signature, SECOidTag *digest, int *len);

/* src/common/protocol_nss.c */
char *ssl_protocol_version_to_string(int version);

#endif							/* USE_NSS */

#endif							/* COMMON_NSS_H */
