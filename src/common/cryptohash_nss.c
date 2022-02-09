/*-------------------------------------------------------------------------
 *
 * cryptohash_nss.c
 *	  Set of wrapper routines on top of NSS to support cryptographic
 *	  hash functions.
 *
 * This should only be used if code is compiled with NSS support.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		  src/common/cryptohash_nss.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/pg_nss.h"

#include <pk11func.h>
#include <pk11pub.h>
#include <secitem.h>
#include <sechash.h>
#include <secoid.h>
#include <secerr.h>

#include "common/cryptohash.h"
#ifndef FRONTEND
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/resowner_private.h"
#endif

/*
 * In the backend, use an allocation in TopMemoryContext to count for
 * resowner cleanup handling.  In the frontend, use malloc to be able
 * to return a failure status back to the caller.
 */
#ifndef FRONTEND
#define ALLOC(size) MemoryContextAlloc(TopMemoryContext, size)
#define FREE(ptr) pfree(ptr)
#else
#define ALLOC(size) malloc(size)
#define FREE(ptr) free(ptr)
#endif

/*
 * Internal pg_cryptohash_ctx structure.
 */
struct pg_cryptohash_ctx
{
	pg_cryptohash_type type;

	PK11Context *pk11_context;
	SECOidTag	hash_type;
	const char *errreason;

#ifndef FRONTEND
	ResourceOwner resowner;
#endif
};

/*
 * pg_cryptohash_create
 *
 * Create and allocate a new hash context. Returns NULL on failure in the
 * frontend, and raise error without returning in the backend.
 */
pg_cryptohash_ctx *
pg_cryptohash_create(pg_cryptohash_type type)
{
	NSSInitParameters params;
	pg_cryptohash_ctx *ctx;
	SECOidData *hash;
	SECStatus status;

#ifndef FRONTEND
	/*
	 * Make sure that the resource owner has space to remember this reference.
	 * This can error out with "out of memory", so do this before any other
	 * allocation to avoid leaking.
	 */
	ResourceOwnerEnlargeCryptoHash(CurrentResourceOwner);
#endif

	ctx = ALLOC(sizeof(pg_cryptohash_ctx));

	if (!ctx)
		goto error;

	ctx->errreason = NULL;
	ctx->type = type;

	switch(type)
	{
		case PG_SHA1:
			ctx->hash_type = SEC_OID_SHA1;
			break;
		case PG_MD5:
			ctx->hash_type = SEC_OID_MD5;
			break;
		case PG_SHA224:
			ctx->hash_type = SEC_OID_SHA224;
			break;
		case PG_SHA256:
			ctx->hash_type = SEC_OID_SHA256;
			break;
		case PG_SHA384:
			ctx->hash_type = SEC_OID_SHA384;
			break;
		case PG_SHA512:
			ctx->hash_type = SEC_OID_SHA512;
			break;
	}

	/*
	 * Initialize our own NSS context without a database backing it.
	 */
	memset(&params, 0, sizeof(params));
	params.length = sizeof(params);
	status = NSS_NoDB_Init(".");
	if (status != SECSuccess)
	{
		explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
		FREE(ctx);
		ctx->errreason = PR_ErrorToString(PR_GetError(),
										  PR_LANGUAGE_I_DEFAULT);
		goto error;
	}

	hash = SECOID_FindOIDByTag(ctx->hash_type);
	ctx->pk11_context = PK11_CreateDigestContext(hash->offset);
	if (!ctx->pk11_context)
	{
		explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
		FREE(ctx);
		ctx->errreason = PR_ErrorToString(PR_GetError(),
										  PR_LANGUAGE_I_DEFAULT);
		goto error;
	}

#ifndef FRONTEND
	ctx->resowner = CurrentResourceOwner;
	ResourceOwnerRememberCryptoHash(CurrentResourceOwner,
									PointerGetDatum(ctx));
#endif

	return ctx;

error:
#ifndef FRONTEND
	if (ctx->errreason != NULL)
		ereport(ERROR,
				(errmsg("NSS failure: %s", ctx->errreason)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
#endif
	return NULL;
}

/*
 * pg_cryptohash_init
 *			Initialize the passed hash context
 *
 * Returns 0 on success, -1 on failure.
 */
int
pg_cryptohash_init(pg_cryptohash_ctx *ctx)
{
	SECStatus		status;

	status = PK11_DigestBegin(ctx->pk11_context);

	if (status != SECSuccess)
	{
		ctx->errreason = PR_ErrorToString(PR_GetError(),
										  PR_LANGUAGE_I_DEFAULT);
		return -1;
	}
	return 0;
}

/*
 * pg_cryptohash_update
 *
 * Update a hash context.  Returns 0 on success, -1 on failure.
 */
int
pg_cryptohash_update(pg_cryptohash_ctx *ctx, const uint8 *data, size_t len)
{
	SECStatus	status;

	if (ctx == NULL)
		return -1;

	status = PK11_DigestOp(ctx->pk11_context, data, len);
	if (status != SECSuccess)
	{
		ctx->errreason = PR_ErrorToString(PR_GetError(),
										  PR_LANGUAGE_I_DEFAULT);
		return -1;
	}
	return 0;
}

/*
 * pg_cryptohash_final
 *
 * Finalize a hash context.  Returns 0 on success, -1 on failure.
 */
int
pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest, size_t len)
{
	SECStatus	status;
	unsigned int outlen;

	switch (ctx->type)
	{
		case PG_MD5:
			if (len < MD5_LENGTH)
				return -1;
			break;
		case PG_SHA1:
			if (len < SHA1_LENGTH)
				return -1;
			break;
		case PG_SHA224:
			if (len < SHA224_LENGTH)
				return -1;
			break;
		case PG_SHA256:
			if (len < SHA256_LENGTH)
				return -1;
			break;
		case PG_SHA384:
			if (len < SHA384_LENGTH)
				return -1;
			break;
		case PG_SHA512:
			if (len < SHA512_LENGTH)
				return -1;
			break;
	}

	status = PK11_DigestFinal(ctx->pk11_context, dest, &outlen, len);

	if (status != SECSuccess)
	{
		ctx->errreason = PR_ErrorToString(PR_GetError(),
										  PR_LANGUAGE_I_DEFAULT);
		return -1;
	}
	return 0;
}

/*
 * pg_cryptohash_free
 *
 * Free a hash context and the associated NSS context.
 */
void
pg_cryptohash_free(pg_cryptohash_ctx *ctx)
{
	PRBool		free_context = PR_TRUE;

	if (!ctx)
		return;

	PK11_DestroyContext(ctx->pk11_context, free_context);
#ifndef FRONTEND
	ResourceOwnerForgetCryptoHash(ctx->resowner, PointerGetDatum(ctx));
#endif
	explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
	FREE(ctx);
}

/*
 * pg_cryptohash_error
 *
 * Returns a static string providing details about an error that happened
 * during a computation.
 */
const char *
pg_cryptohash_error(pg_cryptohash_ctx *ctx)
{
	if (!ctx)
		return _("out of memory");

	if (ctx->errreason)
		return ctx->errreason;

	/*
	 * PG_ErrorToString never returns NULL, and all error codepaths invoke it
	 * so if we end up here we either don't have any error at all or something
	 * very bad happened.
	 */
	return _("unknown failure");
}
