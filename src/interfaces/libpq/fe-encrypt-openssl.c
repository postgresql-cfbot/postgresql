/*-------------------------------------------------------------------------
 *
 * fe-encrypt-openssl.c
 *	  encryption support using OpenSSL
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-encrypt-openssl.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "fe-encrypt.h"
#include "libpq-int.h"

#include "catalog/pg_colenckey.h"
#include "port/pg_bswap.h"

#include <openssl/evp.h>


#ifdef TEST_ENCRYPT

/*
 * Test data from
 * <https://datatracker.ietf.org/doc/html/draft-mcgrew-aead-aes-cbc-hmac-sha2-05#section-5>
 */

/*
 * The different test cases just use different prefixes of K, so one constant
 * is enough here.
 */
static const unsigned char K[] = {
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
};

static const unsigned char P[] = {
	0x41, 0x20, 0x63, 0x69, 0x70, 0x68, 0x65, 0x72, 0x20, 0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x20,
	0x6d, 0x75, 0x73, 0x74, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x62, 0x65, 0x20, 0x72, 0x65, 0x71, 0x75,
	0x69, 0x72, 0x65, 0x64, 0x20, 0x74, 0x6f, 0x20, 0x62, 0x65, 0x20, 0x73, 0x65, 0x63, 0x72, 0x65,
	0x74, 0x2c, 0x20, 0x61, 0x6e, 0x64, 0x20, 0x69, 0x74, 0x20, 0x6d, 0x75, 0x73, 0x74, 0x20, 0x62,
	0x65, 0x20, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x6f, 0x20, 0x66, 0x61, 0x6c, 0x6c, 0x20, 0x69,
	0x6e, 0x74, 0x6f, 0x20, 0x74, 0x68, 0x65, 0x20, 0x68, 0x61, 0x6e, 0x64, 0x73, 0x20, 0x6f, 0x66,
	0x20, 0x74, 0x68, 0x65, 0x20, 0x65, 0x6e, 0x65, 0x6d, 0x79, 0x20, 0x77, 0x69, 0x74, 0x68, 0x6f,
	0x75, 0x74, 0x20, 0x69, 0x6e, 0x63, 0x6f, 0x6e, 0x76, 0x65, 0x6e, 0x69, 0x65, 0x6e, 0x63, 0x65,
};

static const unsigned char test_IV[] = {
	0x1a, 0xf3, 0x8c, 0x2d, 0xc2, 0xb9, 0x6f, 0xfd, 0xd8, 0x66, 0x94, 0x09, 0x23, 0x41, 0xbc, 0x04,
};

static const unsigned char test_A[] = {
	0x54, 0x68, 0x65, 0x20, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x20, 0x70, 0x72, 0x69, 0x6e, 0x63,
	0x69, 0x70, 0x6c, 0x65, 0x20, 0x6f, 0x66, 0x20, 0x41, 0x75, 0x67, 0x75, 0x73, 0x74, 0x65, 0x20,
	0x4b, 0x65, 0x72, 0x63, 0x6b, 0x68, 0x6f, 0x66, 0x66, 0x73,
};


#define libpq_gettext(x) (x)
#define libpq_append_conn_error(conn, ...) appendPQExpBuffer(&(conn)->errorMessage, __VA_ARGS__)

#endif							/* TEST_ENCRYPT */


unsigned char *
decrypt_cek_from_file(PGconn *conn, const char *cmkfilename, int cmkalg,
					  int fromlen, const unsigned char *from,
					  int *tolen)
{
	const EVP_MD *md = NULL;
	EVP_PKEY   *key = NULL;
	RSA		   *rsa = NULL;
	BIO		   *bio = NULL;
	EVP_PKEY_CTX *ctx = NULL;
	unsigned char *out = NULL;
	size_t		outlen;

	switch (cmkalg)
	{
		case PG_CMK_RSAES_OAEP_SHA_1:
			md = EVP_sha1();
			break;
		case PG_CMK_RSAES_OAEP_SHA_256:
			md = EVP_sha256();
			break;
		default:
			libpq_append_conn_error(conn, "unsupported CMK algorithm ID: %d", cmkalg);
			goto fail;
	}

	bio = BIO_new_file(cmkfilename, "r");
	if (!bio)
	{
		libpq_append_conn_error(conn, "could not open file \"%s\": %m", cmkfilename);
		goto fail;
	}

	rsa = RSA_new();
	if (!rsa)
	{
		libpq_append_conn_error(conn, "could not allocate RSA structure: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}
	/*
	 * Note: We must go through BIO and not say use PEM_read_RSAPrivateKey()
	 * directly on a FILE.  Otherwise, we get into "no OPENSSL_Applink" hell
	 * on Windows (which happens whenever you pass a stdio handle from the
	 * application into OpenSSL).
	 */
	rsa = PEM_read_bio_RSAPrivateKey(bio, &rsa, NULL, NULL);
	if (!rsa)
	{
		libpq_append_conn_error(conn, "could not read RSA private key: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	key = EVP_PKEY_new();
	if (!key)
	{
		libpq_append_conn_error(conn, "could not allocate private key structure: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (!EVP_PKEY_assign_RSA(key, rsa))
	{
		libpq_append_conn_error(conn, "could not assign private key: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	ctx = EVP_PKEY_CTX_new(key, NULL);
	if (!ctx)
	{
		libpq_append_conn_error(conn, "could not allocate public key algorithm context: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (EVP_PKEY_decrypt_init(ctx) <= 0)
	{
		libpq_append_conn_error(conn, "decryption initialization failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_OAEP_PADDING) <= 0 ||
		EVP_PKEY_CTX_set_rsa_mgf1_md(ctx, md) <= 0 ||
		EVP_PKEY_CTX_set_rsa_oaep_md(ctx, md) <= 0)
	{
		libpq_append_conn_error(conn, "could not set RSA parameter: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (EVP_PKEY_decrypt(ctx, NULL, &outlen, from, fromlen) <= 0)
	{
		libpq_append_conn_error(conn, "RSA decryption failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	out = malloc(outlen);
	if (!out)
	{
		libpq_append_conn_error(conn, "out of memory");
		goto fail;
	}

	if (EVP_PKEY_decrypt(ctx, out, &outlen, from, fromlen) <= 0)
	{
		libpq_append_conn_error(conn, "RSA decryption failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		free(out);
		out = NULL;
		goto fail;
	}

	*tolen = outlen;

fail:
	EVP_PKEY_CTX_free(ctx);
	EVP_PKEY_free(key);
	BIO_free(bio);

	return out;
}

static const EVP_CIPHER *
pg_cekalg_to_openssl_cipher(int cekalg)
{
	const EVP_CIPHER *cipher;

	switch (cekalg)
	{
		case PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256:
			cipher = EVP_aes_128_cbc();
			break;
		case PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384:
			cipher = EVP_aes_192_cbc();
			break;
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384:
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512:
			cipher = EVP_aes_256_cbc();
			break;
		default:
			cipher = NULL;
	}

	return cipher;
}

static const EVP_MD *
pg_cekalg_to_openssl_md(int cekalg)
{
	const EVP_MD *md;

	switch (cekalg)
	{
		case PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256:
			md = EVP_sha256();
			break;
		case PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384:
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384:
			md = EVP_sha384();
			break;
		case PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512:
			md = EVP_sha512();
			break;
		default:
			md = NULL;
	}

	return md;
}

static int
md_key_length(const EVP_MD *md)
{
	if (md == EVP_sha256())
		return 16;
	else if (md == EVP_sha384())
		return 24;
	else if (md == EVP_sha512())
		return 32;
	else
		return -1;
}

static int
md_hash_length(const EVP_MD *md)
{
	if (md == EVP_sha256())
		return 32;
	else if (md == EVP_sha384())
		return 48;
	else if (md == EVP_sha512())
		return 64;
	else
		return -1;
}

#ifndef TEST_ENCRYPT
#define PG_AD_LEN 4
#else
#define PG_AD_LEN sizeof(test_A)
#endif

static bool
get_message_auth_tag(const EVP_MD *md,
					 const unsigned char *mac_key, int mac_key_len,
					 const unsigned char *encr, int encrlen,
					 int cekalg,
					 unsigned char *md_value, size_t *md_len_p,
					 const char **errmsgp)
{
	static char msgbuf[1024];
	EVP_MD_CTX *evp_md_ctx = NULL;
	EVP_PKEY   *pkey = NULL;
	size_t		bufsize;
	unsigned char *buf = NULL;
	int64		al;
	bool		result = false;

	if (encrlen < 0)
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("encrypted value has invalid length"));
		*errmsgp = msgbuf;
		goto fail;
	}

	evp_md_ctx = EVP_MD_CTX_new();

	pkey = EVP_PKEY_new_raw_private_key(EVP_PKEY_HMAC, NULL, mac_key, mac_key_len);
	if (!pkey)
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("could not allocate key for HMAC: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}

	bufsize = PG_AD_LEN + encrlen + sizeof(int64);
	buf = malloc(bufsize);
	if (!buf)
	{
		*errmsgp = libpq_gettext("out of memory");
		goto fail;
	}
#ifndef TEST_ENCRYPT
	buf[0] = 'P';
	buf[1] = 'G';
	*(int16 *) (buf + 2) = pg_hton16(cekalg);
#else
	memcpy(buf, test_A, sizeof(test_A));
#endif
	memcpy(buf + PG_AD_LEN, encr, encrlen);
	al = pg_hton64(PG_AD_LEN * 8);
	memcpy(buf + PG_AD_LEN + encrlen, &al, sizeof(al));

	if (!EVP_DigestSignInit(evp_md_ctx, NULL, md, NULL, pkey))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("digest initialization failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}

	if (!EVP_DigestSignUpdate(evp_md_ctx, buf, bufsize))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("digest signing failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}

	if (!EVP_DigestSignFinal(evp_md_ctx, md_value, md_len_p))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("digest signing failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}
	Assert(*md_len_p == md_hash_length(md));

	/* truncate output to half the length, per spec */
	*md_len_p /= 2;

	result = true;
fail:
	free(buf);
	EVP_PKEY_free(pkey);
	EVP_MD_CTX_free(evp_md_ctx);
	return result;
}

unsigned char *
decrypt_value(PGresult *res, const PGCEK *cek, int cekalg, const unsigned char *input, int inputlen, const char **errmsgp)
{
	static char msgbuf[1024];

	const unsigned char *iv = NULL;
	size_t		ivlen;

	const EVP_CIPHER *cipher;
	const EVP_MD *md;
	EVP_CIPHER_CTX *evp_cipher_ctx = NULL;
	int			enc_key_len;
	int			mac_key_len;
	int			key_len;
	const unsigned char *enc_key;
	const unsigned char *mac_key;
	unsigned char md_value[EVP_MAX_MD_SIZE];
	size_t		md_len = sizeof(md_value);
	size_t		bufsize;
	unsigned char *buf = NULL;
	unsigned char *decr;
	int			decrlen,
				decrlen2;

	unsigned char *result = NULL;

	cipher = pg_cekalg_to_openssl_cipher(cekalg);
	if (!cipher)
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("unrecognized encryption algorithm identifier: %d"), cekalg);
		*errmsgp = msgbuf;
		goto fail;
	}

	md = pg_cekalg_to_openssl_md(cekalg);
	if (!md)
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("unrecognized digest algorithm identifier: %d"), cekalg);
		*errmsgp = msgbuf;
		goto fail;
	}

	evp_cipher_ctx = EVP_CIPHER_CTX_new();

	if (!EVP_DecryptInit_ex(evp_cipher_ctx, cipher, NULL, NULL, NULL))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("decryption initialization failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}

	enc_key_len = EVP_CIPHER_CTX_key_length(evp_cipher_ctx);
	mac_key_len = md_key_length(md);
	key_len = mac_key_len + enc_key_len;

	if (cek->cekdatalen != key_len)
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("column encryption key has wrong length for algorithm (has: %zu, required: %d)"),
				 cek->cekdatalen, key_len);
		*errmsgp = msgbuf;
		goto fail;
	}

	enc_key = cek->cekdata + mac_key_len;
	mac_key = cek->cekdata;

	if (!get_message_auth_tag(md, mac_key, mac_key_len,
							  input, inputlen - (md_hash_length(md) / 2),
							  cekalg,
							  md_value, &md_len,
							  errmsgp))
	{
		goto fail;
	}

	if (memcmp(input + (inputlen - md_len), md_value, md_len) != 0)
	{
		*errmsgp = libpq_gettext("MAC mismatch");
		goto fail;
	}

	ivlen = EVP_CIPHER_CTX_iv_length(evp_cipher_ctx);
	iv = input;
	input += ivlen;
	inputlen -= ivlen;
	if (!EVP_DecryptInit_ex(evp_cipher_ctx, NULL, NULL, enc_key, iv))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("decryption initialization failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}

	bufsize = inputlen + EVP_CIPHER_CTX_block_size(evp_cipher_ctx) + 1;
#ifndef TEST_ENCRYPT
	buf = pqResultAlloc(res, bufsize, false);
#else
	buf = malloc(bufsize);
#endif
	if (!buf)
	{
		*errmsgp = libpq_gettext("out of memory");
		goto fail;
	}
	decr = buf;
	if (!EVP_DecryptUpdate(evp_cipher_ctx, decr, &decrlen, input, inputlen - md_len))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("decryption failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}
	if (!EVP_DecryptFinal_ex(evp_cipher_ctx, decr + decrlen, &decrlen2))
	{
		snprintf(msgbuf, sizeof(msgbuf),
				 libpq_gettext("decryption failed: %s"),
				 ERR_reason_error_string(ERR_get_error()));
		*errmsgp = msgbuf;
		goto fail;
	}
	decrlen += decrlen2;
	Assert(decrlen < bufsize);
	decr[decrlen] = '\0';
	result = decr;

fail:
	EVP_CIPHER_CTX_free(evp_cipher_ctx);

	return result;
}

#ifndef TEST_ENCRYPT
static bool
make_siv(PGconn *conn,
		 unsigned char *iv, size_t ivlen,
		 const EVP_MD *md,
		 const unsigned char *iv_key, int iv_key_len,
		 const unsigned char *plaintext, int plaintext_len)
{
	EVP_MD_CTX *evp_md_ctx = NULL;
	EVP_PKEY   *pkey = NULL;
	unsigned char md_value[EVP_MAX_MD_SIZE];
	size_t		md_len = sizeof(md_value);
	bool		result = false;

	evp_md_ctx = EVP_MD_CTX_new();

	pkey = EVP_PKEY_new_raw_private_key(EVP_PKEY_HMAC, NULL, iv_key, iv_key_len);
	if (!pkey)
	{
		libpq_append_conn_error(conn, "could not allocate key for HMAC: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (!EVP_DigestSignInit(evp_md_ctx, NULL, md, NULL, pkey))
	{
		libpq_append_conn_error(conn, "digest initialization failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (!EVP_DigestSignUpdate(evp_md_ctx, plaintext, plaintext_len))
	{
		libpq_append_conn_error(conn, "digest signing failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	if (!EVP_DigestSignFinal(evp_md_ctx, md_value, &md_len))
	{
		libpq_append_conn_error(conn, "digest signing failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}
	Assert(md_len == md_hash_length(md));
	memcpy(iv, md_value, ivlen);

	result = true;
fail:
	EVP_PKEY_free(pkey);
	EVP_MD_CTX_free(evp_md_ctx);
	return result;
}
#endif							/* TEST_ENCRYPT */

unsigned char *
encrypt_value(PGconn *conn, const PGCEK *cek, int cekalg, const unsigned char *value, int *nbytesp, bool enc_det)
{
	int			nbytes = *nbytesp;
	unsigned char iv[EVP_MAX_IV_LENGTH];
	size_t		ivlen;
	const EVP_CIPHER *cipher;
	const EVP_MD *md;
	EVP_CIPHER_CTX *evp_cipher_ctx = NULL;
	int			enc_key_len;
	int			mac_key_len;
	int			key_len;
	const unsigned char *enc_key;
	const unsigned char *mac_key;
	size_t		bufsize;
	unsigned char *buf = NULL;
	unsigned char *encr;
	int			encrlen,
				encrlen2;

	const char *errmsg;
	unsigned char md_value[EVP_MAX_MD_SIZE];
	size_t		md_len = sizeof(md_value);
	size_t		buf2size;
	unsigned char *buf2 = NULL;

	unsigned char *result = NULL;

	cipher = pg_cekalg_to_openssl_cipher(cekalg);
	if (!cipher)
	{
		libpq_append_conn_error(conn, "unrecognized encryption algorithm identifier: %d", cekalg);
		goto fail;
	}

	md = pg_cekalg_to_openssl_md(cekalg);
	if (!md)
	{
		libpq_append_conn_error(conn, "unrecognized digest algorithm identifier: %d", cekalg);
		goto fail;
	}

	evp_cipher_ctx = EVP_CIPHER_CTX_new();

	if (!EVP_EncryptInit_ex(evp_cipher_ctx, cipher, NULL, NULL, NULL))
	{
		libpq_append_conn_error(conn, "encryption initialization failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	enc_key_len = EVP_CIPHER_CTX_key_length(evp_cipher_ctx);
	mac_key_len = md_key_length(md);
	key_len = mac_key_len + enc_key_len;

	if (cek->cekdatalen != key_len)
	{
		libpq_append_conn_error(conn, "column encryption key has wrong length for algorithm (has: %zu, required: %d)",
								cek->cekdatalen, key_len);
		goto fail;
	}

	enc_key = cek->cekdata + mac_key_len;
	mac_key = cek->cekdata;

	ivlen = EVP_CIPHER_CTX_iv_length(evp_cipher_ctx);
	Assert(ivlen <= sizeof(iv));
	if (enc_det)
	{
#ifndef TEST_ENCRYPT
		make_siv(conn, iv, ivlen, md, mac_key, mac_key_len, value, nbytes);
#else
		memcpy(iv, test_IV, ivlen);
#endif
	}
	else
		pg_strong_random(iv, ivlen);
	if (!EVP_EncryptInit_ex(evp_cipher_ctx, NULL, NULL, enc_key, iv))
	{
		libpq_append_conn_error(conn, "encryption initialization failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}

	bufsize = ivlen + (nbytes + 2 * EVP_CIPHER_CTX_block_size(evp_cipher_ctx) - 1);
	buf = malloc(bufsize);
	if (!buf)
	{
		libpq_append_conn_error(conn, "out of memory");
		goto fail;
	}
	memcpy(buf, iv, ivlen);
	encr = buf + ivlen;
	if (!EVP_EncryptUpdate(evp_cipher_ctx, encr, &encrlen, value, nbytes))
	{
		libpq_append_conn_error(conn, "encryption failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}
	if (!EVP_EncryptFinal_ex(evp_cipher_ctx, encr + encrlen, &encrlen2))
	{
		libpq_append_conn_error(conn, "encryption failed: %s",
								ERR_reason_error_string(ERR_get_error()));
		goto fail;
	}
	encrlen += encrlen2;

	encr -= ivlen;
	encrlen += ivlen;

	Assert(encrlen <= bufsize);

	if (!get_message_auth_tag(md, mac_key, mac_key_len,
							  encr, encrlen,
							  cekalg,
							  md_value, &md_len,
							  &errmsg))
	{
		appendPQExpBuffer(&conn->errorMessage, "%s\n", errmsg);
		goto fail;
	}

	buf2size = encrlen + md_len;
	buf2 = malloc(buf2size);
	if (!buf2)
	{
		libpq_append_conn_error(conn, "out of memory");
		goto fail;
	}
	memcpy(buf2, encr, encrlen);
	memcpy(buf2 + encrlen, md_value, md_len);

	result = buf2;
	nbytes = buf2size;

fail:
	free(buf);
	EVP_CIPHER_CTX_free(evp_cipher_ctx);

	*nbytesp = nbytes;
	return result;
}


/*
 * Run test cases
 */
#ifdef TEST_ENCRYPT

static void
debug_print_hex(const char *name, const unsigned char *val, int len)
{
	printf("%s =", name);
	for (int i = 0; i < len; i++)
	{
		if (i % 16 == 0)
			printf("\n");
		else
			printf(" ");
		printf("%02x", val[i]);
	}
	printf("\n");
}

static void
test_case(int alg, const unsigned char *K, size_t K_len, const unsigned char *P, size_t P_len)
{
	unsigned char *C;
	int			nbytes;
	PGCEK		cek;

	nbytes = P_len;
	cek.cekdata = unconstify(unsigned char *, K);
	cek.cekdatalen = K_len;

	C = encrypt_value(NULL, &cek, alg, P, &nbytes, true);
	debug_print_hex("C", C, nbytes);
}

int
main(int argc, char **argv)
{
	printf("5.1\n");
	test_case(PG_CEK_AEAD_AES_128_CBC_HMAC_SHA_256, K, 32, P, sizeof(P));
	printf("5.2\n");
	test_case(PG_CEK_AEAD_AES_192_CBC_HMAC_SHA_384, K, 48, P, sizeof(P));
	printf("5.3\n");
	test_case(PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_384, K, 56, P, sizeof(P));
	printf("5.4\n");
	test_case(PG_CEK_AEAD_AES_256_CBC_HMAC_SHA_512, K, 64, P, sizeof(P));

	return 0;
}

#endif							/* TEST_ENCRYPT */
