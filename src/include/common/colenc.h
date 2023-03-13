/*-------------------------------------------------------------------------
 *
 * colenc.h
 *
 * Shared definitions for column encryption algorithms.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/include/common/colenc.h
 *-------------------------------------------------------------------------
 */

#ifndef COMMON_COLENC_H
#define COMMON_COLENC_H

/*
 * Constants for CMK and CEK algorithms.  Note that these are part of the
 * protocol.  In either case, don't assign zero, so that that can be used as
 * an invalid value.
 *
 * Names should use IANA-style capitalization and punctuation ("LIKE_THIS").
 *
 * When making changes, also update protocol.sgml.
 */

#define PG_CMK_UNSPECIFIED				1
#define PG_CMK_RSAES_OAEP_SHA_1			2
#define PG_CMK_RSAES_OAEP_SHA_256		3

/*
 * These algorithms are part of the RFC 5116 realm of AEAD algorithms (even
 * though they never became an official IETF standard).  So for propriety, we
 * use "private use" numbers from
 * <https://www.iana.org/assignments/aead-parameters/aead-parameters.xhtml>.
 */
#define PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256	32768
#define PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384	32769
#define PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384	32770
#define PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512	32771

/*
 * Functions to convert between names and numbers
 */
extern int get_cmkalg_num(const char *name);
extern const char *get_cmkalg_name(int num);
extern const char *get_cmkalg_jwa_name(int num);
extern int get_cekalg_num(const char *name);
extern const char *get_cekalg_name(int num);

#endif
