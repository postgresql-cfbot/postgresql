/*-------------------------------------------------------------------------
 *
 * pg_strong_random.c
 *	  generate a cryptographically secure random number
 *
 * Our definition of "strong" is that it's suitable for generating random
 * salts and query cancellation keys, during authentication.
 *
 * Note: this code is run quite early in postmaster and backend startup;
 * therefore, even when built for backend, it cannot rely on backend
 * infrastructure such as elog() or palloc().
 *
 * Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/pg_strong_random.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>

/*
 * pg_strong_random & pg_strong_random_init
 *
 * Generate requested number of random bytes. The returned bytes are
 * cryptographically secure, suitable for use e.g. in authentication.
 *
 * Before pg_strong_random is called in any process, the generator must first
 * be initialized by calling pg_strong_random_init().
 *
 * We rely on system facilities for actually generating the numbers.
 * We support a number of sources:
 *
 * 1. OpenSSL's RAND_bytes()
 * 2. Windows' CryptGenRandom() function
 * 3. /dev/urandom
 *
 * Returns true on success, and false if none of the sources
 * were available. NB: It is important to check the return value!
 * Proceeding with key generation when no random data was available
 * would lead to predictable keys and security issues.
 */



#ifdef USE_OPENSSL

#include <openssl/rand.h>

void
pg_strong_random_init(void)
{
	/*
	 * Make sure processes do not share OpenSSL randomness state.  This is no
	 * longer required in OpenSSL 1.1.1 and later versions, but until we drop
	 * support for version < 1.1.1 we need to do this.
	 */
	RAND_poll();
}

bool
pg_strong_random(void *buf, size_t len)
{
	int			i;

	/*
	 * Check that OpenSSL's CSPRNG has been sufficiently seeded, and if not
	 * add more seed data using RAND_poll().  With some older versions of
	 * OpenSSL, it may be necessary to call RAND_poll() a number of times.  If
	 * RAND_poll() fails to generate seed data within the given amount of
	 * retries, subsequent RAND_bytes() calls will fail, but we allow that to
	 * happen to let pg_strong_random() callers handle that with appropriate
	 * error handling.
	 */
#define NUM_RAND_POLL_RETRIES 8

	for (i = 0; i < NUM_RAND_POLL_RETRIES; i++)
	{
		if (RAND_status() == 1)
		{
			/* The CSPRNG is sufficiently seeded */
			break;
		}

		RAND_poll();
	}

	if (RAND_bytes(buf, len) == 1)
		return true;
	return false;
}

#elif WIN32

#include <wincrypt.h>
/*
 * Cache a global crypto provider that only gets freed when the process
 * exits, in case we need random numbers more than once.
 */
static HCRYPTPROV hProvider = 0;

void
pg_strong_random_init(void)
{
	/* No initialization needed on WIN32 */
}

bool
pg_strong_random(void *buf, size_t len)
{
	if (hProvider == 0)
	{
		if (!CryptAcquireContext(&hProvider,
								 NULL,
								 MS_DEF_PROV,
								 PROV_RSA_FULL,
								 CRYPT_VERIFYCONTEXT | CRYPT_SILENT))
		{
			/*
			 * On failure, set back to 0 in case the value was for some reason
			 * modified.
			 */
			hProvider = 0;
		}
	}
	/* Re-check in case we just retrieved the provider */
	if (hProvider != 0)
	{
		if (CryptGenRandom(hProvider, len, buf))
			return true;
	}
	return false;
}

#elif defined(USE_NSS)

#define pg_BITS_PER_BYTE BITS_PER_BYTE
#undef BITS_PER_BYTE
#define NO_NSPR_10_SUPPORT
#include <nss/nss.h>
#include <nss/pk11pub.h>
#if defined(BITS_PER_BYTE)
#if BITS_PER_BYTE != pg_BITS_PER_BYTE
#error "incompatible byte widths between NSPR and postgres"
#endif
#else
#define BITS_PER_BYTE pg_BITS_PER_BYTE
#endif
#undef pg_BITS_PER_BYTE

void
pg_strong_random_init(void)
{
	/* No initialization needed on NSS */
}

bool
pg_strong_random(void *buf, size_t len)
{
	NSSInitParameters params;
	NSSInitContext *nss_context;
	SECStatus	status;

	memset(&params, 0, sizeof(params));
	params.length = sizeof(params);
	nss_context = NSS_InitContext("", "", "", "", &params,
								  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
								  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
								  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);

	if (!nss_context)
		return false;

	status = PK11_GenerateRandom(buf, len);
	NSS_ShutdownContext(nss_context);

	if (status == SECSuccess)
		return true;

	return false;
}

#else							/* not USE_OPENSSL, USE_NSS or WIN32 */

/*
 * Without OpenSSL, NSS or Win32 support, just read /dev/urandom ourselves.
 */

void
pg_strong_random_init(void)
{
	/* No initialization needed */
}

bool
pg_strong_random(void *buf, size_t len)
{
	int			f;
	char	   *p = buf;
	ssize_t		res;

	f = open("/dev/urandom", O_RDONLY, 0);
	if (f == -1)
		return false;

	while (len)
	{
		res = read(f, p, len);
		if (res <= 0)
		{
			if (errno == EINTR)
				continue;		/* interrupted by signal, just retry */

			close(f);
			return false;
		}

		p += res;
		len -= res;
	}

	close(f);
	return true;
}
#endif
