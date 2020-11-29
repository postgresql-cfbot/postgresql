/*-------------------------------------------------------------------------
 *
 * md5_openssl.c
 *	  Set of wrapper routines on top of OpenSSL to support MD5.
 *
 * This should only be used if code is compiled with OpenSSL support.
 *
 * Portions Copyright (c) 2016-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/common/md52_openssl.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <openssl/evp.h>

#include "common/md5.h"
#ifndef FRONTEND
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/resowner_private.h"
#endif

/*
 * In backend, use palloc/pfree to ease the error handling.  In frontend,
 * use malloc to be able to return a failure status back to the caller.
 */
#ifndef FRONTEND
#define ALLOC(size) palloc(size)
#define FREE(ptr) pfree(ptr)
#else
#define ALLOC(size) malloc(size)
#define FREE(ptr) free(ptr)
#endif

/*
 * pg_md5_create
 *
 * Allocate a MD5 context.  Returns NULL on failure.
 */
void *
pg_md5_create(void)
{
	void	   *ctx;

#ifndef FRONTEND
	ResourceOwnerEnlargeEVP(CurrentResourceOwner);
#endif

	ctx = EVP_MD_CTX_create();

	if (ctx == NULL)
	{
#ifndef FRONTEND
		elog(ERROR, "out of memory");
#else
		return NULL;
#endif
	}

#ifndef FRONTEND
	ResourceOwnerRememberEVP(CurrentResourceOwner,
							 PointerGetDatum(ctx));
#endif

	return ctx;
}

/*
 * pg_md5_init
 *
 * Initialize a MD5 context.  Returns 0 on success, and -1 on failure.
 */
int
pg_md5_init(void *ctx)
{
	int			status = 0;

	if (ctx == NULL)
		return 0;

	status = EVP_DigestInit_ex((EVP_MD_CTX *) ctx,
							   EVP_md5(), NULL);

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_md5_update
 *
 * Update a MD5 context.  Returns 0 on success, and -1 on failure.
 */
int
pg_md5_update(void *ctx, const uint8 *data, size_t len)
{
	int			status;

	if (ctx == NULL)
		return 0;

	status = EVP_DigestUpdate((EVP_MD_CTX *) ctx, data, len);

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_md5_final
 *
 * Finalize a MD5 context.  Returns 0 on success, and -1 on failure.
 */
int
pg_md5_final(void *ctx, uint8 *dest)
{
	int			status;

	if (ctx == NULL)
		return 0;

	status = EVP_DigestFinal_ex((EVP_MD_CTX *) ctx, dest, 0);

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_md5_free
 *
 * Free a MD5 context.
 */
void
pg_md5_free(void *ctx)
{
	if (ctx == NULL)
		return;

	EVP_MD_CTX_destroy((EVP_MD_CTX *) ctx);

#ifndef FRONTEND
	ResourceOwnerForgetEVP(CurrentResourceOwner,
						   PointerGetDatum(ctx));
#endif
}
