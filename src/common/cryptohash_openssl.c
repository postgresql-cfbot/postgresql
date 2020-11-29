/*-------------------------------------------------------------------------
 *
 * cryptohash_openssl.c
 *	  Set of wrapper routines on top of OpenSSL to support cryptographic
 *	  hash functions.
 *
 * This should only be used if code is compiled with OpenSSL support.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/common/cryptohash_openssl.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <openssl/evp.h>

#include "common/cryptohash.h"
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
 * pg_cryptohash_create
 *
 * Allocate a hash context.  Returns NULL on failure.
 */
pg_cryptohash_ctx *
pg_cryptohash_create(pg_cryptohash_type type)
{
	pg_cryptohash_ctx *ctx;

	ctx = ALLOC(sizeof(pg_cryptohash_ctx));
	if (ctx == NULL)
		return NULL;

	ctx->type = type;

#ifndef FRONTEND
	ResourceOwnerEnlargeEVP(CurrentResourceOwner);
#endif

	/*
	 * Initialization takes care of assigning the correct type for OpenSSL.
	 */
	ctx->data = EVP_MD_CTX_create();

	if (ctx->data == NULL)
	{
		explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
#ifndef FRONTEND
		elog(ERROR, "out of memory");
#else
		FREE(ctx);
		return NULL;
#endif
	}

#ifndef FRONTEND
	ResourceOwnerRememberEVP(CurrentResourceOwner,
							 PointerGetDatum(ctx->data));
#endif

	return ctx;
}

/*
 * pg_cryptohash_init
 *
 * Initialize a hash context.  Returns 0 on success, and -1 on failure.
 */
int
pg_cryptohash_init(pg_cryptohash_ctx *ctx)
{
	int			status = 0;

	if (ctx == NULL)
		return 0;

	switch (ctx->type)
	{
		case PG_SHA224:
			status = EVP_DigestInit_ex((EVP_MD_CTX *) ctx->data,
									   EVP_sha224(), NULL);
			break;
		case PG_SHA256:
			status = EVP_DigestInit_ex((EVP_MD_CTX *) ctx->data,
									   EVP_sha256(), NULL);
			break;
		case PG_SHA384:
			status = EVP_DigestInit_ex((EVP_MD_CTX *) ctx->data,
									   EVP_sha384(), NULL);
			break;
		case PG_SHA512:
			status = EVP_DigestInit_ex((EVP_MD_CTX *) ctx->data,
									   EVP_sha512(), NULL);
			break;
	}

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_cryptohash_update
 *
 * Update a hash context.  Returns 0 on success, and -1 on failure.
 */
int
pg_cryptohash_update(pg_cryptohash_ctx *ctx, const uint8 *data, size_t len)
{
	int			status;

	if (ctx == NULL)
		return 0;

	status = EVP_DigestUpdate((EVP_MD_CTX *) ctx->data, data, len);

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_cryptohash_final
 *
 * Finalize a hash context.  Returns 0 on success, and -1 on failure.
 */
int
pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest)
{
	int			status;

	if (ctx == NULL)
		return 0;

	status = EVP_DigestFinal_ex((EVP_MD_CTX *) ctx->data, dest, 0);

	/* OpenSSL internals return 1 on success, 0 on failure */
	if (status <= 0)
		return -1;
	return 0;
}

/*
 * pg_cryptohash_free
 *
 * Free a hash context.
 */
void
pg_cryptohash_free(pg_cryptohash_ctx *ctx)
{
	if (ctx == NULL)
		return;

	EVP_MD_CTX_destroy((EVP_MD_CTX *) ctx->data);

#ifndef FRONTEND
	ResourceOwnerForgetEVP(CurrentResourceOwner,
						   PointerGetDatum(ctx->data));
#endif

	explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
	FREE(ctx);
}
