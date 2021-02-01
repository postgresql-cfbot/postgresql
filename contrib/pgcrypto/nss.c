/*-------------------------------------------------------------------------
 *
 * nss.c
 *	  Wrapper for using NSS as a backend for pgcrypto PX
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pgcrypto/nss.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "px.h"
#include "blf.h"
#include "utils/memutils.h"

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

/*
 * Define our own mechanisms for Blowfish as it's not implemented by NSS.
 */
#define BLOWFISH_CBC	(1)
#define BLOWFISH_ECB	(2)

/*
 * Data structures for recording cipher implementations as well as ongoing
 * cipher operations.
 */
typedef struct nss_digest
{
	NSSInitContext *context;
	PK11Context *hash_context;
	HASH_HashType hash_type;
}			nss_digest;

typedef struct cipher_implementation
{
	/* Function pointers to cipher operations */
	int			(*init) (PX_Cipher *pxc, const uint8 *key, unsigned klen, const uint8 *iv);
	unsigned	(*get_block_size) (PX_Cipher *pxc);
	unsigned	(*get_key_size) (PX_Cipher *pxc);
	unsigned	(*get_iv_size) (PX_Cipher *pxc);
	int			(*encrypt) (PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res);
	int			(*decrypt) (PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res);
	void		(*free) (PX_Cipher *pxc);

	/* The mechanism describing the cipher used */
	union
	{
		CK_MECHANISM_TYPE nss;
		CK_ULONG	internal;
	}			mechanism;
	int			keylen;
	bool		is_nss;
}			cipher_implementation;

typedef struct nss_cipher
{
	const cipher_implementation *impl;
	NSSInitContext *context;
	PK11Context *crypt_context;
	SECItem    *params;

	PK11SymKey *encrypt_key;
	PK11SymKey *decrypt_key;
}			nss_cipher;

typedef struct internal_cipher
{
	const cipher_implementation *impl;
	BlowfishContext context;
}			internal_cipher;

typedef struct nss_cipher_ref
{
	const char *name;
	const cipher_implementation *impl;
}			nss_cipher_ref;

/*
 * Prototypes
 */
static unsigned nss_get_iv_size(PX_Cipher *pxc);
static unsigned nss_get_block_size(PX_Cipher *pxc);

/*
 * nss_GetHashOidTagByHashType
 *
 * Returns the corresponding SECOidTag for the passed hash type. NSS 3.43
 * includes HASH_GetHashOidTagByHashType for this purpose, but at the time of
 * writing is not commonly available so we need our own version till then.
 */
static SECOidTag
nss_GetHashOidTagByHashType(HASH_HashType type)
{
	if (type == HASH_AlgMD2)
		return SEC_OID_MD2;
	if (type == HASH_AlgMD5)
		return SEC_OID_MD5;
	if (type == HASH_AlgSHA1)
		return SEC_OID_SHA1;
	if (type == HASH_AlgSHA224)
		return SEC_OID_SHA224;
	if (type == HASH_AlgSHA256)
		return SEC_OID_SHA256;
	if (type == HASH_AlgSHA384)
		return SEC_OID_SHA384;
	if (type == HASH_AlgSHA512)
		return SEC_OID_SHA512;

	return SEC_OID_UNKNOWN;
}

static unsigned
nss_digest_block_size(PX_MD *pxmd)
{
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;
	const SECHashObject *object;

	object = HASH_GetHashObject(digest->hash_type);
	return object->blocklength;
}

static unsigned
nss_digest_result_size(PX_MD *pxmd)
{
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;
	const SECHashObject *object;

	object = HASH_GetHashObject(digest->hash_type);
	return object->length;
}

static void
nss_digest_free(PX_MD *pxmd)
{
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;
	PRBool		free_ctx = PR_TRUE;

	PK11_DestroyContext(digest->hash_context, free_ctx);
	NSS_ShutdownContext(digest->context);
}

static void
nss_digest_update(PX_MD *pxmd, const uint8 *data, unsigned dlen)
{
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;

	PK11_DigestOp(digest->hash_context, data, dlen);
}

static void
nss_digest_reset(PX_MD *pxmd)
{
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;

	PK11_DigestBegin(digest->hash_context);
}

static void
nss_digest_finish(PX_MD *pxmd, uint8 *dst)
{
	unsigned int outlen;
	nss_digest *digest = (nss_digest *) pxmd->p.ptr;
	const SECHashObject *object;

	object = HASH_GetHashObject(digest->hash_type);
	PK11_DigestFinal(digest->hash_context, dst, &outlen, object->length);
}

int
px_find_digest(const char *name, PX_MD **res)
{
	PX_MD	   *pxmd;
	NSSInitParameters params;
	NSSInitContext *nss_context;
	bool		found = false;
	nss_digest *digest;
	SECStatus	status;
	SECOidData *hash;
	HASH_HashType t;

	/*
	 * Initialize our own NSS context without a database backing it.
	 */
	memset(&params, 0, sizeof(params));
	params.length = sizeof(params);
	nss_context = NSS_InitContext("", "", "", "", &params,
								  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
								  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
								  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);

	/*
	 * There is no API function for looking up a digest algorithm from a name
	 * string, but there is a publicly accessible array which can be scanned
	 * for the name.
	 */
	for (t = HASH_AlgNULL + 1; t < HASH_AlgTOTAL; t++)
	{
		SECOidTag	hash_oid;
		char	   *p;

		hash_oid = nss_GetHashOidTagByHashType(t);

		if (hash_oid == SEC_OID_UNKNOWN)
			return PXE_NO_HASH;

		hash = SECOID_FindOIDByTag(hash_oid);
		if (pg_strcasecmp(hash->desc, name) == 0)
		{
			found = true;
			break;
		}

		/*
		 * NSS saves the algorithm names using SHA-xxx notation whereas
		 * OpenSSL use SHAxxx. To make sure the user finds the requested
		 * algorithm let's remove the dash and compare that spelling as well.
		 */
		if ((p = strchr(hash->desc, '-')) != NULL)
		{
			char		tmp[12];

			memcpy(tmp, hash->desc, p - hash->desc);
			memcpy(tmp + (p - hash->desc), p + 1, strlen(hash->desc) - (p - hash->desc) + 1);
			if (pg_strcasecmp(tmp, name) == 0)
			{
				found = true;
				break;
			}
		}
	}

	if (!found)
		return PXE_NO_HASH;

	digest = palloc(sizeof(*digest));

	digest->context = nss_context;
	digest->hash_context = PK11_CreateDigestContext(hash->offset);
	digest->hash_type = t;
	if (digest->hash_context == NULL)
	{
		pfree(digest);
		return -1;
	}

	status = PK11_DigestBegin(digest->hash_context);
	if (status != SECSuccess)
	{
		PK11_DestroyContext(digest->hash_context, PR_TRUE);
		pfree(digest);
		return -1;
	}

	pxmd = palloc(sizeof(*pxmd));
	pxmd->result_size = nss_digest_result_size;
	pxmd->block_size = nss_digest_block_size;
	pxmd->reset = nss_digest_reset;
	pxmd->update = nss_digest_update;
	pxmd->finish = nss_digest_finish;
	pxmd->free = nss_digest_free;
	pxmd->p.ptr = (void *) digest;

	*res = pxmd;

	return 0;
}

static int
bf_init(PX_Cipher *pxc, const uint8 *key, unsigned klen, const uint8 *iv)
{
	internal_cipher *cipher = (internal_cipher *) pxc->ptr;

	blowfish_setkey(&cipher->context, key, klen);
	if (iv)
		blowfish_setiv(&cipher->context, iv);

	return 0;
}

static int
bf_encrypt(PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res)
{
	internal_cipher *cipher = (internal_cipher *) pxc->ptr;
	uint8	   *buf;

	if (dlen == 0)
		return 0;

	if (dlen & 7)
		return PXE_NOTBLOCKSIZE;

	buf = palloc(dlen);
	memcpy(buf, data, dlen);

	if (cipher->impl->mechanism.internal == BLOWFISH_ECB)
		blowfish_encrypt_ecb(buf, dlen, &cipher->context);
	else
		blowfish_encrypt_cbc(buf, dlen, &cipher->context);

	memcpy(res, buf, dlen);
	pfree(buf);

	return 0;
}

static int
bf_decrypt(PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res)
{
	internal_cipher *cipher = (internal_cipher *) pxc->ptr;

	if (dlen == 0)
		return 0;

	if (dlen & 7)
		return PXE_NOTBLOCKSIZE;

	memcpy(res, data, dlen);
	if (cipher->impl->mechanism.internal == BLOWFISH_ECB)
		blowfish_decrypt_ecb(res, dlen, &cipher->context);
	else
		blowfish_decrypt_cbc(res, dlen, &cipher->context);

	return 0;
}

static void
bf_free(PX_Cipher *pxc)
{
	internal_cipher *cipher = (internal_cipher *) pxc->ptr;

	pfree(cipher);
	pfree(pxc);
}

static int
nss_symkey_blockcipher_init(PX_Cipher *pxc, const uint8 *key, unsigned klen,
							const uint8 *iv)
{
	nss_cipher *cipher = (nss_cipher *) pxc->ptr;
	SECItem		iv_item;
	unsigned char *iv_item_data;
	SECItem		key_item;
	int			keylen;
	PK11SlotInfo *slot;

	if (cipher->impl->mechanism.nss == CKM_AES_CBC ||
		cipher->impl->mechanism.nss == CKM_AES_ECB)
	{
		if (klen <= 128 / 8)
			keylen = 128 / 8;
		else if (klen <= 192 / 8)
			keylen = 192 / 8;
		else if (klen <= 256 / 8)
			keylen = 256 / 8;
		else
			return PXE_CIPHER_INIT;
	}
	else
		keylen = cipher->impl->keylen;

	key_item.type = siBuffer;
	key_item.data = (unsigned char *) key;
	key_item.len = keylen;

	/*
	 * If hardware acceleration is configured in NSS one can theoretically get
	 * a better slot by calling PK11_GetBestSlot() with the mechanism passed
	 * to it. Unless there are complaints, using the simpler API will make
	 * error handling easier though.
	 */
	slot = PK11_GetInternalSlot();
	if (!slot)
		return PXE_CIPHER_INIT;

	/*
	 * The key must be set up for the operation, and since we don't know at
	 * this point whether we are asked to encrypt or decrypt we need to store
	 * both versions.
	 */
	cipher->decrypt_key = PK11_ImportSymKey(slot, cipher->impl->mechanism.nss,
											PK11_OriginUnwrap,
											CKA_DECRYPT, &key_item,
											NULL);
	cipher->encrypt_key = PK11_ImportSymKey(slot, cipher->impl->mechanism.nss,
											PK11_OriginUnwrap,
											CKA_ENCRYPT, &key_item,
											NULL);
	PK11_FreeSlot(slot);

	if (!cipher->decrypt_key || !cipher->encrypt_key)
		return PXE_CIPHER_INIT;

	if (iv)
	{
		iv_item.type = siBuffer;
		iv_item.data = (unsigned char *) iv;
		iv_item.len = nss_get_iv_size(pxc);
	}
	else
	{
		/*
		 * The documentation states that either passing .data = 0; .len = 0;
		 * in the iv_item, or NULL as iv_item, to PK11_ParamFromIV should be
		 * done when IV is missing. That however leads to segfaults in the
		 * library, the workaround that works in modern  library versions is
		 * to pass in a keysized zeroed out IV.
		 */
		iv_item_data = palloc0(nss_get_iv_size(pxc));
		iv_item.type = siBuffer;
		iv_item.data = iv_item_data;
		iv_item.len = nss_get_iv_size(pxc);
	}

	cipher->params = PK11_ParamFromIV(cipher->impl->mechanism.nss, &iv_item);

	/* If we had to make a mock IV, free it once made into a param */
	if (!iv)
		pfree(iv_item_data);

	if (cipher->params == NULL)
		return PXE_CIPHER_INIT;

	return 0;
}

static int
nss_decrypt(PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res)
{
	nss_cipher *cipher = (nss_cipher *) pxc->ptr;
	SECStatus	status;
	int			outlen;

	if (!cipher->crypt_context)
	{
		cipher->crypt_context =
			PK11_CreateContextBySymKey(cipher->impl->mechanism.nss, CKA_DECRYPT,
									   cipher->decrypt_key, cipher->params);
	}

	status = PK11_CipherOp(cipher->crypt_context, res, &outlen, dlen, data, dlen);
	if (status != SECSuccess)
		return PXE_DECRYPT_FAILED;

	return 0;
}

static int
nss_encrypt(PX_Cipher *pxc, const uint8 *data, unsigned dlen, uint8 *res)
{
	nss_cipher *cipher = (nss_cipher *) pxc->ptr;
	SECStatus	status;
	int			outlen;

	if (!cipher->crypt_context)
	{
		cipher->crypt_context =
			PK11_CreateContextBySymKey(cipher->impl->mechanism.nss, CKA_ENCRYPT,
									   cipher->decrypt_key, cipher->params);
	}

	status = PK11_CipherOp(cipher->crypt_context, res, &outlen, dlen, data, dlen);

	if (status != SECSuccess)
		return PXE_DECRYPT_FAILED;

	return 0;
}

static void
nss_free(PX_Cipher *pxc)
{
	nss_cipher *cipher = pxc->ptr;
	PRBool		free_ctx = PR_TRUE;

	PK11_FreeSymKey(cipher->encrypt_key);
	PK11_FreeSymKey(cipher->decrypt_key);
	PK11_DestroyContext(cipher->crypt_context, free_ctx);
	NSS_ShutdownContext(cipher->context);
	pfree(cipher);

	pfree(pxc);
}

static unsigned
nss_get_block_size(PX_Cipher *pxc)
{
	nss_cipher *cipher = pxc->ptr;

	return PK11_GetBlockSize(cipher->impl->mechanism.nss, NULL);
}

static unsigned
nss_get_key_size(PX_Cipher *pxc)
{
	nss_cipher *cipher = pxc->ptr;

	return cipher->impl->keylen;
}

static unsigned
nss_get_iv_size(PX_Cipher *pxc)
{
	nss_cipher *cipher = pxc->ptr;

	return PK11_GetIVLength(cipher->impl->mechanism.nss);
}

static unsigned
bf_get_block_size(PX_Cipher *pxc)
{
	return 8;
}

static unsigned
bf_get_key_size(PX_Cipher *pxc)
{
	return 448 / 8;
}

static unsigned
bf_get_iv_size(PX_Cipher *pxc)
{
	return 8;
}

/*
 * Cipher Implementations
 */
static const cipher_implementation nss_des_cbc = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_DES_CBC,
	.keylen = 8,
	.is_nss = true
};

static const cipher_implementation nss_des_ecb = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_DES_ECB,
	.keylen = 8,
	.is_nss = true
};

static const cipher_implementation nss_des3_cbc = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_DES3_CBC,
	.keylen = 24,
	.is_nss = true
};

static const cipher_implementation nss_des3_ecb = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_DES3_ECB,
	.keylen = 24,
	.is_nss = true
};

static const cipher_implementation nss_aes_cbc = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_AES_CBC,
	.keylen = 32,
	.is_nss = true
};

static const cipher_implementation nss_aes_ecb = {
	.init = nss_symkey_blockcipher_init,
	.get_block_size = nss_get_block_size,
	.get_key_size = nss_get_key_size,
	.get_iv_size = nss_get_iv_size,
	.encrypt = nss_encrypt,
	.decrypt = nss_decrypt,
	.free = nss_free,
	.mechanism.nss = CKM_AES_ECB,
	.keylen = 32,
	.is_nss = true
};

static const cipher_implementation nss_bf_ecb = {
	.init = bf_init,
	.get_block_size = bf_get_block_size,
	.get_key_size = bf_get_key_size,
	.get_iv_size = bf_get_iv_size,
	.encrypt = bf_encrypt,
	.decrypt = bf_decrypt,
	.free = bf_free,
	.mechanism.internal = BLOWFISH_ECB,
	.keylen = 56,
	.is_nss = false
};

static const cipher_implementation nss_bf_cbc = {
	.init = bf_init,
	.get_block_size = bf_get_block_size,
	.get_key_size = bf_get_key_size,
	.get_iv_size = bf_get_iv_size,
	.encrypt = bf_encrypt,
	.decrypt = bf_decrypt,
	.free = bf_free,
	.mechanism.internal = BLOWFISH_CBC,
	.keylen = 56,
	.is_nss = false
};

/*
 * Lookup table for finding the implementation based on a name. CAST5 as well
 * as BLOWFISH are defined as cipher mechanisms in NSS but error out with
 * SEC_ERROR_INVALID_ALGORITHM. Blowfish is implemented using the internal
 * implementation while CAST5 isn't supported at all at this time,
 */
static const nss_cipher_ref nss_cipher_lookup[] = {
	{"des", &nss_des_cbc},
	{"des-cbc", &nss_des_cbc},
	{"des-ecb", &nss_des_ecb},
	{"des3-cbc", &nss_des3_cbc},
	{"3des", &nss_des3_cbc},
	{"3des-cbc", &nss_des3_cbc},
	{"des3-ecb", &nss_des3_ecb},
	{"3des-ecb", &nss_des3_ecb},
	{"aes", &nss_aes_cbc},
	{"aes-cbc", &nss_aes_cbc},
	{"aes-ecb", &nss_aes_ecb},
	{"rijndael", &nss_aes_cbc},
	{"rijndael-cbc", &nss_aes_cbc},
	{"rijndael-ecb", &nss_aes_ecb},
	{"blowfish", &nss_bf_cbc},
	{"blowfish-cbc", &nss_bf_cbc},
	{"blowfish-ecb", &nss_bf_ecb},
	{"bf", &nss_bf_cbc},
	{"bf-cbc", &nss_bf_cbc},
	{"bf-ecb", &nss_bf_ecb},
	{NULL}
};

/*
 * px_find_cipher
 *
 * Search for the requested cipher and see if there is support for it, and if
 * so return an allocated object containing the playbook for how to encrypt
 * and decrypt using the cipher.
 */
int
px_find_cipher(const char *alias, PX_Cipher **res)
{
	const nss_cipher_ref *cipher_ref;
	PX_Cipher  *px_cipher;
	NSSInitParameters params;
	NSSInitContext *nss_context;
	nss_cipher *cipher;
	internal_cipher *int_cipher;

	for (cipher_ref = nss_cipher_lookup; cipher_ref->name; cipher_ref++)
	{
		if (strcmp(cipher_ref->name, alias) == 0)
			break;
	}

	if (!cipher_ref->name)
		return PXE_NO_CIPHER;

	/*
	 * Fill in the PX_Cipher to pass back to PX describing the operations to
	 * perform in order to use the cipher.
	 */
	px_cipher = palloc(sizeof(*px_cipher));
	px_cipher->block_size = cipher_ref->impl->get_block_size;
	px_cipher->key_size = cipher_ref->impl->get_key_size;
	px_cipher->iv_size = cipher_ref->impl->get_iv_size;
	px_cipher->free = cipher_ref->impl->free;
	px_cipher->init = cipher_ref->impl->init;
	px_cipher->encrypt = cipher_ref->impl->encrypt;
	px_cipher->decrypt = cipher_ref->impl->decrypt;

	/*
	 * Set the private data, which is different for NSS and internal ciphers
	 */
	if (cipher_ref->impl->is_nss)
	{
		/*
		 * Initialize our own NSS context without a database backing it. At
		 * some point we might want to allow users to use stored keys in the
		 * database rather than passing them via SELECT encrypt(), and then
		 * this need to be changed to open a user specified database.
		 */
		memset(&params, 0, sizeof(params));
		params.length = sizeof(params);
		nss_context = NSS_InitContext("", "", "", "", &params,
									  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
									  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
									  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);

		/* nss_cipher is the private state struct for the operation */
		cipher = palloc(sizeof(*cipher));
		cipher->context = nss_context;
		cipher->impl = cipher_ref->impl;
		cipher->crypt_context = NULL;

		px_cipher->ptr = cipher;
	}
	else
	{
		int_cipher = palloc0(sizeof(*int_cipher));

		int_cipher->impl = cipher_ref->impl;
		px_cipher->ptr = int_cipher;
	}

	*res = px_cipher;
	return 0;
}
