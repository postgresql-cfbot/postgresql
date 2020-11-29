/*-------------------------------------------------------------------------
 *
 * md5.h
 *	  Interface to common/md5.c
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/md5.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_MD5_H
#define PG_MD5_H

#define MD5_PASSWD_CHARSET	"0123456789abcdef"
#define MD5_PASSWD_LEN	35

/* Interface for MD5 implementations */
extern void *pg_md5_create(void);
extern void pg_md5_free(void *ctx);
/* These return 0 on success, or -1 on failure */
extern int pg_md5_init(void *ctx);
extern int pg_md5_update(void *ctx, const uint8 *data, size_t len);
extern int pg_md5_final(void *ctx, uint8 *dest);

/* Utilities common to all the implementations, as of md5_common.c */
extern bool pg_md5_hash(const void *buff, size_t len, char *hexsum);
extern bool pg_md5_binary(const void *buff, size_t len, void *outbuf);
extern bool pg_md5_encrypt(const char *passwd, const char *salt,
						   size_t salt_len, char *buf);

#endif
