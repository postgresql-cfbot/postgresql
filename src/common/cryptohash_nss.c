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

#include "common/nss.h"

#include <nss/nss.h>
#include <nss/hasht.h>
#include <nss/pk11func.h>
#include <nss/pk11pub.h>
#include <nss/secitem.h>
#include <nss/sechash.h>
#include <nss/secoid.h>
#include <nss/secerr.h>

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
	HASH_HashType hash_type;

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
		goto error;
	}

	ctx->type = type;
	hash = SECOID_FindOIDByTag(ctx->hash_type);
	ctx->pk11_context = PK11_CreateDigestContext(hash->offset);
	if (!ctx->pk11_context)
	{
		explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
		FREE(ctx);
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
		return -1;
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
		return -1;

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
		return -1;

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
