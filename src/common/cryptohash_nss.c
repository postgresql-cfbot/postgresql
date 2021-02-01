/*-------------------------------------------------------------------------
 *
 * cryptohash_nss.c
 *	  Set of wrapper routines on top of NSS to support cryptographic
 *	  hash functions.
 *
 * This should only be used if code is compiled with NSS support.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
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

/*
 * BITS_PER_BYTE is also defined in the NSPR header files, so we need to undef
 * our version to avoid compiler warnings on redefinition.
 */
#define pg_BITS_PER_BYTE BITS_PER_BYTE
#undef BITS_PER_BYTE

#include <nss/nss.h>
#include <nss/hasht.h>
#include <nss/pk11func.h>
#include <nss/pk11pub.h>
#include <nss/secitem.h>
#include <nss/sechash.h>
#include <nss/secoid.h>
#include <nss/secerr.h>

/*
 * Ensure that the colliding definitions match, else throw an error. In case
 * NSPR has removed the definition for some reason, make sure to put ours
 * back again.
 */
#if defined(BITS_PER_BYTE)
#if BITS_PER_BYTE != pg_BITS_PER_BYTE
#error "incompatible byte widths between NSPR and postgres"
#endif
#else
#define BITS_PER_BYTE pg_BITS_PER_BYTE
#endif
#undef pg_BITS_PER_BYTE

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

static NSSInitContext *nss_context = NULL;

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
	if (!nss_context)
	{
		nss_context = NSS_InitContext("", "", "", "", &params,
									  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
									  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
									  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);
		if (!nss_context)
		{
			explicit_bzero(ctx, sizeof(pg_cryptohash_ctx));
			FREE(ctx);
			goto error;
		}
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
pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest)
{
	SECStatus	status;
	unsigned int outlen;
	const SECHashObject *object;

	object = HASH_GetHashObject(ctx->hash_type);
	status = PK11_DigestFinal(ctx->pk11_context, dest, &outlen, object->length);

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

/*
 * TODO: We need a way to close the static NSS context when the backend is
 * terminated.
 *
void
pg_cryptohash_teardown(void)
{
	if (nss_context)
		NSS_ShutdownContext(ctx->nss_context);
}
 */
