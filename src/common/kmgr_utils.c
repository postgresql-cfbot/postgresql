/*-------------------------------------------------------------------------
 *
 * kmgr_utils.c
 *	  Shared frontend/backend for cryptographic key management
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/kmgr_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#ifdef FRONTEND
#include "common/logging.h"
#endif
#include "common/kmgr_utils.h"
#include "common/sha2.h"
#include "utils/elog.h"
#include "storage/fd.h"

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

static pg_cipher_ctx *wrapctx = NULL;
static pg_cipher_ctx *unwrapctx = NULL;
static bool keywrap_initialized = false;

#ifdef FRONTEND
static FILE *open_pipe_stream(const char *command);
static int close_pipe_stream(FILE *file);
#endif

static void
initialize_keywrap_ctx(void)
{
	wrapctx = pg_cipher_ctx_create();
	if (wrapctx == NULL)
		goto err;

	unwrapctx = pg_cipher_ctx_create();
	if (unwrapctx == NULL)
		goto err;

	if (!pg_aes256_ctr_wrap_init(wrapctx))
		goto err;

	if (!pg_aes256_ctr_wrap_init(unwrapctx))
		goto err;

	keywrap_initialized = true;
	return;

err:
#ifdef FRONTEND
	pg_log_fatal("could not initialize cipher context");
	exit(EXIT_FAILURE);
#else
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("could not initialize cipher context")));
#endif
}

/*
 * Hash the given passphrase and extract it into KEK and HMAC
 * key.
 */
void
kmgr_derive_keys(char *passphrase, Size passlen,
				 uint8 kek[KMGR_KEK_LEN],
				 uint8 hmackey[KMGR_HMAC_KEY_LEN])
{
	uint8 keys[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, keys);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes.
	 */
	if (kek)
		memcpy(kek, keys, KMGR_KEK_LEN);
	if (hmackey)
		memcpy(hmackey, keys + KMGR_KEK_LEN, KMGR_HMAC_KEY_LEN);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped key using the HMAC key retrieved from the user provided passphrase.
 * And then we compare it with the HMAC stored alongside the controlfile. Return
 * true if both HMACs are matched, meaning the given passphrase is correct.
 * Otherwise return false.
 */
bool
kmgr_verify_passphrase(char *passphrase, int passlen,
					   WrappedEncKeyWithHmac *kh, int keylen)
{
	uint8 user_kek[KMGR_KEK_LEN];
	uint8 user_hmackey[KMGR_HMAC_KEY_LEN];
	uint8 result_hmac[KMGR_HMAC_LEN];

	kmgr_derive_keys(passphrase, passlen, user_kek, user_hmackey);

	/* Verify both HMAC */
	kmgr_compute_HMAC(user_hmackey, kh->key, keylen, result_hmac);

	if (memcmp(result_hmac, kh->hmac, KMGR_HMAC_LEN) != 0)
		return false;

	return true;
}

/*
 * Run cluster passphrase command.
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.
 * The return value is the length of the actual result.
 */
int
kmgr_run_cluster_passphrase_command(char *passphrase_command, char *buf,
									int size)
{
	char		command[MAXPGPATH];
	char	   *p;
	char		*dp;
	char		*endp;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(size > 0);
	buf[0] = '\0';

	dp = command;
	endp = command + MAXPGPATH - 1;
	*endp = '\0';

	for (p = passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					StrNCpy(dp, KMGR_PROMPT_MSG, strlen(KMGR_PROMPT_MSG));
					dp += strlen(KMGR_PROMPT_MSG);
					p++;
					break;
				case '%':
					p++;
					if (dp < endp)
						*dp++ = *p;
					break;
				default:
					if (dp < endp)
						*dp++ = *p;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *p;
		}
	}
	*dp = '\0';

#ifdef FRONTEND
	fh = open_pipe_stream(command);
	if (fh == NULL)
	{
		pg_log_fatal("could not execute command \"%s\": %m",
					 command);
		exit(EXIT_FAILURE);
	}
#else
	fh = OpenPipeStream(command, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command)));
#endif

	if ((len = fread(buf, sizeof(char), size, fh)) < size)
	{
		if (ferror(fh))
		{
#ifdef FRONTEND
			pg_log_fatal("could not read from command \"%s\": %m",
						 command);
			exit(EXIT_FAILURE);
#else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command)));
#endif
		}
	}

#ifdef FRONTEND
	pclose_rc = close_pipe_stream(fh);
#else
	pclose_rc = ClosePipeStream(fh);
#endif

	if (pclose_rc == -1)
	{
#ifdef FRONTEND
		pg_log_fatal("could not close pipe to external command: %m");
		exit(EXIT_FAILURE);
#else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
#endif
	}
	else if (pclose_rc != 0)
	{
#ifdef FRONTEND
		pg_log_fatal("command \"%s\" failed", command);
		exit(EXIT_FAILURE);
#else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
#endif
	}

	return len;
}

bool
kmgr_wrap_key(uint8 *key, const uint8 *in, int insize, uint8 *out)
{
	int outsize;

	if (!keywrap_initialized)
		initialize_keywrap_ctx();

	return pg_cipher_encrypt(wrapctx, key, in , insize,
							 NULL, out, &outsize);
}

bool
kmgr_unwrap_key(uint8 *key, const uint8 *in, int insize, uint8 *out)
{
	int outsize;

	if (!keywrap_initialized)
		initialize_keywrap_ctx();

	return pg_cipher_decrypt(unwrapctx, key, in, insize,
							 NULL, out, &outsize);
}

bool
kmgr_compute_HMAC(uint8 *key, const uint8 *data, int size,
				  uint8 *result)
{
	int resultsize;

	return pg_compute_HMAC(key, data, size, result, &resultsize);
}

/* Convert cipher name string to integer value */
int
kmgr_cipher_value(const char *name)
{
	if (strcasecmp(name, "aes-128") == 0)
		return KMGR_ENCRYPTION_AES128;

	if (strcasecmp(name, "aes-256") == 0)
		return KMGR_ENCRYPTION_AES256;

	return KMGR_ENCRYPTION_OFF;
}

/* Convert integer value to cipher name string */
char *
kmgr_cipher_string(int value)
{
	switch (value)
	{
		case KMGR_ENCRYPTION_OFF :
			return "off";
		case KMGR_ENCRYPTION_AES128:
			return "aes-128";
		case KMGR_ENCRYPTION_AES256:
			return "aes-256";
		default:
			break;
	}

	return "unknown";
}

#ifdef FRONTEND
static FILE *
open_pipe_stream(const char *command)
{
	FILE	   *res;

#ifdef WIN32
	size_t		cmdlen = strlen(command);
	char	   *buf;
	int			save_errno;

	buf = malloc(cmdlen + 2 + 1);
	if (buf == NULL)
	{
		errno = ENOMEM;
		return NULL;
	}
	buf[0] = '"';
	mempcy(&buf[1], command, cmdlen);
	buf[cmdlen + 1] = '"';
	buf[cmdlen + 2] = '\0';

	res = _popen(buf, "r");

	save_errno = errno;
	free(buf);
	errno = save_errno;
#else
	res = popen(command, "r");
#endif /* WIN32 */
	return res;
}

static int
close_pipe_stream(FILE *file)
{
#ifdef WIN32
	return _pclose(file);
#else
	return pclose(file);
#endif /* WIN32 */
}
#endif /* FRONTEND */
