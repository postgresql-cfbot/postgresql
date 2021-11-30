/*-------------------------------------------------------------------------
 *
 * fe-secure-nss.c
 *	  functions for supporting NSS as a TLS backend for frontend libpq
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-secure-nss.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "libpq-fe.h"
#include "fe-auth.h"
#include "fe-secure-common.h"
#include "libpq-int.h"
#include "common/cryptohash.h"
#include "common/nss.h"

#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif
#endif

/*
 * The nspr/obsolete/protypes.h NSPR header typedefs uint64 and int64 with
 * colliding definitions from ours, causing a much expected compiler error.
 * Remove backwards compatibility with ancient NSPR versions to avoid this.
 */
#define NO_NSPR_10_SUPPORT
#include <nspr.h>
#include <prerror.h>
#include <prinit.h>
#include <prio.h>

#include <hasht.h>
#include <nss.h>
#include <ssl.h>
#include <sslproto.h>
#include <pk11func.h>
#include <secerr.h>
#include <secoidt.h>
#include <secmod.h>

static SECStatus pg_load_nss_module(SECMODModule **module, const char *library, const char *name);
static SECStatus pg_bad_cert_handler(void *arg, PRFileDesc *fd);
static const char *pg_SSLerrmessage(PRErrorCode errcode);
static SECStatus pg_client_auth_handler(void *arg, PRFileDesc *socket, CERTDistNames *caNames,
										CERTCertificate **pRetCert, SECKEYPrivateKey **pRetKey);
static SECStatus pg_cert_auth_handler(void *arg, PRFileDesc *fd, PRBool checksig, PRBool isServer);
static int	ssl_protocol_param_to_nss(const char *protocol);
static bool certificate_database_has_CA(PGconn *conn);

static char *PQssl_passwd_cb(PK11SlotInfo *slot, PRBool retry, void *arg);

/*
 * PR_ImportTCPSocket() is a private API, but very widely used, as it's the
 * only way to make NSS use an already set up POSIX file descriptor rather
 * than opening one itself. To quote the NSS documentation:
 *
 *		"In theory, code that uses PR_ImportTCPSocket may break when NSPR's
 *		implementation changes. In practice, this is unlikely to happen because
 *		NSPR's implementation has been stable for years and because of NSPR's
 *		strong commitment to backward compatibility."
 *
 * https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSPR/Reference/PR_ImportTCPSocket
 *
 * The function is declared in <private/pprio.h>, but as it is a header marked
 * private we declare it here rather than including it.
 */
NSPR_API(PRFileDesc *) PR_ImportTCPSocket(int);

static SECMODModule *ca_trust = NULL;

/*
 * This logic exists in NSS as well, but it's only available for when there is
 * a database to open, and not only using the system trust store. Thus, we
 * need to keep our own copy.
 */
#if defined(WIN32)
static const char *ca_trust_name = "nssckbi.dll";
#elif defined(__darwin__)
static const char *ca_trust_name = "libnssckbi.dylib";
#else
static const char *ca_trust_name = "libnssckbi.so";
#endif

static PQsslKeyPassHook_nss_type PQsslKeyPassHook = NULL;

#ifdef ENABLE_THREAD_SAFETY
#ifndef WIN32
static pthread_mutex_t ssl_config_mutex = PTHREAD_MUTEX_INITIALIZER;
#else
static pthread_mutex_t ssl_config_mutex = NULL;
static long win32_ssl_create_mutex = 0;
#endif
#endif							/* ENABLE_THREAD_SAFETY */

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */

/*
 * pgtls_init_library
 *
 * There is no direct equivalent for PQinitOpenSSL in NSS/NSPR, with PR_Init
 * being the closest match there is. PR_Init is however already documented to
 * not be required so simply making this a noop seems like the best option.
 */
void
pgtls_init_library(bool do_ssl, int do_crypto)
{
	/* noop */
}

int
pgtls_init(PGconn *conn, bool do_ssl, bool do_crypto)
{
	if (do_ssl)
	{
		conn->nss_context = NULL;

		conn->ssl_in_use = false;
		conn->has_password = false;
	}

	return 0;
}

void
pgtls_close(PGconn *conn)
{
	conn->ssl_in_use = false;
	conn->has_password = false;

	/*
	 * All NSS references must be cleaned up before we close out the
	 * context.
	 */
	if (conn->pr_fd)
	{
		PRStatus	status;

		status = PR_Close(conn->pr_fd);
		if (status != PR_SUCCESS)
		{
			pqInternalNotice(&conn->noticeHooks,
							 "unable to close NSPR fd: %s",
							 pg_SSLerrmessage(PR_GetError()));
		}

		conn->pr_fd = NULL;
	}

	if (conn->nss_context)
	{
		SECStatus	status;

		/* The session cache must be cleared, or we'll leak references */
		SSL_ClearSessionCache();

		status = NSS_ShutdownContext(conn->nss_context);

		if (status != SECSuccess)
		{
			pqInternalNotice(&conn->noticeHooks,
							 "unable to shut down NSS context: %s",
							 pg_SSLerrmessage(PR_GetError()));
		}

		conn->nss_context = NULL;
	}
}

PostgresPollingStatusType
pgtls_open_client(PGconn *conn)
{
	SECStatus	status;
	PRFileDesc *model;
	NSSInitParameters params;
	SSLVersionRange desired_range;

#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
	/* This locking is modelled after fe-secure-openssl.c */
	if (ssl_config_mutex == NULL)
	{
		while (InterlockedExchange(&win32_ssl_create_mutex, 1) == 1)
			/* loop while another thread owns the lock */ ;
		if (ssl_config_mutex == NULL)
		{
			if (pthread_mutex_init(&ssl_config_mutex, NULL))
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("unable to lock thread"));
				return PGRES_POLLING_FAILED;
			}
		}
		InterlockedExchange(&win32_ssl_create_mutex, 0);
	}
#endif
	if (pthread_mutex_lock(&ssl_config_mutex))
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to lock thread"));
		return PGRES_POLLING_FAILED;
	}
#endif							/* ENABLE_THREAD_SAFETY */

	/*
	 * The NSPR documentation states that runtime initialization via PR_Init
	 * is no longer required, as the first caller into NSPR will perform the
	 * initialization implicitly. See be-secure-nss.c for further discussion
	 * on PR_Init.
	 */
	PR_Init(PR_USER_THREAD, PR_PRIORITY_NORMAL, 0);

	/*
	 * NSS initialization and the use of contexts is further discussed in
	 * be-secure-nss.c
	 */
	memset(&params, 0, sizeof(params));
	params.length = sizeof(params);
	params.dbTokenDescription = strdup("postgres");

	if (conn->ssldatabase && strlen(conn->ssldatabase) > 0)
	{
		PQExpBufferData ssldatabase_path;

		initPQExpBuffer(&ssldatabase_path);
		appendPQExpBuffer(&ssldatabase_path, "sql:%s", conn->ssldatabase);

		if (PQExpBufferDataBroken(ssldatabase_path))
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("out of memory\n"));
			return PGRES_POLLING_FAILED;
		}

		conn->nss_context = NSS_InitContext(ssldatabase_path.data, "", "", "",
											&params,
											NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
		termPQExpBuffer(&ssldatabase_path);

		if (!conn->nss_context)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unable to open certificate database \"%s\": %s"),
							  conn->ssldatabase,
							  pg_SSLerrmessage(PR_GetError()));
			return PGRES_POLLING_FAILED;
		}
	}
	else
	{
		conn->nss_context = NSS_InitContext("", "", "", "", &params,
											NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
											NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
											NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);
		if (!conn->nss_context)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unable to initialize NSS: %s"),
							  pg_SSLerrmessage(PR_GetError()));
			return PGRES_POLLING_FAILED;
		}
	}

	/*
	 * Configure cipher policy by setting the domestic suite.
	 *
	 * Historically there were different cipher policies based on export (and
	 * import) restrictions: Domestic, Export and France. These are since long
	 * removed with all ciphers being enabled by default. Due to backwards
	 * compatibility, the old API is still used even though all three policies
	 * now do the same thing.
	 */
	status = NSS_SetDomesticPolicy();
	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to configure cipher policy: %s"),
						  pg_SSLerrmessage(PR_GetError()));

		return PGRES_POLLING_FAILED;
	}

	/*
	 * If we don't have a certificate database, the system trust store is the
	 * fallback we can use. If we fail to initialize that as well, we can
	 * still attempt a connection as long as the sslmode isn't verify-*.
	 */
	if (!conn->ssldatabase &&
		(strcmp(conn->sslmode, "verify-ca") == 0 ||
		 strcmp(conn->sslmode, "verify-full") == 0))
	{
		status = pg_load_nss_module(&ca_trust, ca_trust_name, "\"Root Certificates\"");
		if (status != SECSuccess)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("WARNING: unable to load system NSS trust module \"%s\", create a local NSS database and retry : %s"),
							  ca_trust_name,
							  pg_SSLerrmessage(PR_GetError()));

			return PGRES_POLLING_FAILED;
		}
	}

#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_unlock(&ssl_config_mutex);
#endif

	PK11_SetPasswordFunc(PQssl_passwd_cb);

	/*
	 * Import the already opened socket as we don't want to use NSPR functions
	 * for opening the network socket due to how the PostgreSQL protocol works
	 * with TLS connections. This function is not part of the NSPR public API,
	 * see the comment at the top of the file for the rationale of still using
	 * it.
	 */
	conn->pr_fd = PR_ImportTCPSocket(conn->sock);
	if (!conn->pr_fd)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to attach to socket: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * Most of the documentation available, and implementations of, NSS/NSPR
	 * use the PR_NewTCPSocket() function here, which has the drawback that it
	 * can only create IPv4 sockets. Instead use PR_OpenTCPSocket() which
	 * copes with IPv6 as well.
	 */
	model = SSL_ImportFD(NULL, PR_OpenTCPSocket(conn->laddr.addr.ss_family));
	if (!model)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to enable TLS: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/* Disable old protocol versions (SSLv2 and SSLv3) */
	SSL_OptionSet(model, SSL_ENABLE_SSL2, PR_FALSE);
	SSL_OptionSet(model, SSL_V2_COMPATIBLE_HELLO, PR_FALSE);
	SSL_OptionSet(model, SSL_ENABLE_SSL3, PR_FALSE);

#ifdef SSL_CBC_RANDOM_IV

	/*
	 * Enable protection against the BEAST attack in case the NSS library has
	 * support for that. While SSLv3 is disabled, we may still allow TLSv1
	 * which is affected. The option isn't documented as an SSL option, but as
	 * an NSS environment variable.
	 */
	SSL_OptionSet(model, SSL_CBC_RANDOM_IV, PR_TRUE);
#endif

	/* Set us up as a TLS client for the handshake */
	SSL_OptionSet(model, SSL_HANDSHAKE_AS_CLIENT, PR_TRUE);

	/*
	 * When setting the available protocols, we either use the user defined
	 * configuration values, and if missing we accept whatever is the highest
	 * version supported by the library as the max and only limit the range in
	 * the other end at TLSv1.0. ssl_variant_stream is a ProtocolVariant enum
	 * for Stream protocols, rather than datagram.
	 */
	SSL_VersionRangeGetSupported(ssl_variant_stream, &desired_range);
	desired_range.min = SSL_LIBRARY_VERSION_TLS_1_0;

	if (conn->ssl_min_protocol_version && strlen(conn->ssl_min_protocol_version) > 0)
	{
		int			ssl_min_ver = ssl_protocol_param_to_nss(conn->ssl_min_protocol_version);

		if (ssl_min_ver == -1)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("invalid value \"%s\" for minimum version of SSL protocol\n"),
							  conn->ssl_min_protocol_version);
			return -1;
		}

		desired_range.min = ssl_min_ver;
	}

	if (conn->ssl_max_protocol_version && strlen(conn->ssl_max_protocol_version) > 0)
	{
		int			ssl_max_ver = ssl_protocol_param_to_nss(conn->ssl_max_protocol_version);

		if (ssl_max_ver == -1)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("invalid value \"%s\" for maximum version of SSL protocol\n"),
							  conn->ssl_max_protocol_version);
			return -1;
		}

		desired_range.max = ssl_max_ver;
	}

	if (SSL_VersionRangeSet(model, &desired_range) != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to set allowed SSL protocol version range: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * Set up callback for verifying server certificates, as well as for how
	 * to handle failed verifications.
	 */
	SSL_AuthCertificateHook(model, pg_cert_auth_handler, (void *) conn);
	SSL_BadCertHook(model, pg_bad_cert_handler, (void *) conn);

	/*
	 * Convert the NSPR socket to an SSL socket. Ensuring the success of this
	 * operation is critical as NSS SSL_* functions may return SECSuccess on
	 * the socket even though SSL hasn't been enabled, which introduce a risk
	 * of silent downgrades.
	 */
	conn->pr_fd = SSL_ImportFD(model, conn->pr_fd);
	if (!conn->pr_fd)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to configure client for TLS: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * The model can now be closed as we've applied the settings of the model
	 * onto the real socket. From here on we should only use conn->pr_fd.
	 */
	PR_Close(model);

	/* Set the private data to be passed to the password callback */
	SSL_SetPKCS11PinArg(conn->pr_fd, (void *) conn);

	status = SSL_ResetHandshake(conn->pr_fd, PR_FALSE);
	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to initiate handshake: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/* Set callback for client authentication when requested by the server */
	SSL_GetClientAuthDataHook(conn->pr_fd, pg_client_auth_handler, (void *) conn);

	/*
	 * Specify which hostname we are expecting to talk to for the ClientHello
	 * SNI extension, unless the user has disabled it. NSS will suppress IP
	 * addresses in SNIs, so we don't need to filter those explicitly like we do
	 * for OpenSSL.
	 */
	if (conn->sslsni && conn->sslsni[0] == '1')
	{
		const char *host = conn->connhost[conn->whichhost].host;
		if (host && host[0])
			SSL_SetURL(conn->pr_fd, host);
	}

	status = SSL_ForceHandshake(conn->pr_fd);

	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("SSL error: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	Assert(conn->nss_context);
	Assert(conn->pr_fd);

	conn->ssl_in_use = true;
	return PGRES_POLLING_OK;
}

ssize_t
pgtls_read(PGconn *conn, void *ptr, size_t len)
{
	PRInt32		nread;
	PRErrorCode status;
	int			read_errno = 0;

	/*
	 * PR_Recv blocks until there is data to read or the timeout expires. We
	 * don't want to sit blocked here, so the timeout is turned off by using
	 * PR_INTERVAL_NO_WAIT which means "return immediately",  Zero is returned
	 * for closed connections, while -1 indicates an error within the ongoing
	 * connection.
	 */
	nread = PR_Recv(conn->pr_fd, ptr, len, 0, PR_INTERVAL_NO_WAIT);

	if (nread == 0)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("TLS read error: EOF detected\n"));
		read_errno = ECONNRESET;
		nread = -1;
	}
	else if (nread == -1)
	{
		status = PR_GetError();

		switch (status)
		{
			case PR_IO_TIMEOUT_ERROR:
				/* No data available yet. */
				nread = 0;
				break;

			case PR_WOULD_BLOCK_ERROR:
				read_errno = EWOULDBLOCK;
				break;

				/*
				 * The error cases for PR_Recv are not documented, but can be
				 * reverse engineered from _MD_unix_map_default_error() in the
				 * NSPR code, defined in pr/src/md/unix/unix_errors.c.
				 */
			case PR_NETWORK_UNREACHABLE_ERROR:
			case PR_CONNECT_RESET_ERROR:
				read_errno = ECONNRESET;
				break;

			default:
				break;
		}

		if (nread == -1)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("TLS read error: %s"),
							  pg_SSLerrmessage(status));
		}
	}

	SOCK_ERRNO_SET(read_errno);
	return (ssize_t) nread;
}

/*
 * pgtls_read_pending
 *		Check for the existence of data to be read
 *
 * SSL_DataPending will check for decrypted data in the receiving buffer, but
 * does not reveal anything about still encrypted data which will be made
 * available. Thus, if pgtls_read_pending returns zero it does not guarantee
 * that a subsequent call to pgtls_read would block. This is modelled
 * around how the OpenSSL implementation treats pending data. The equivalent
 * to the OpenSSL SSL_has_pending function would be to call PR_Recv with no
 * wait and PR_MSG_PEEK like so:
 *
 *     PR_Recv(conn->pr_fd, &c, 1, PR_MSG_PEEK, PR_INTERVAL_NO_WAIT);
 */
bool
pgtls_read_pending(PGconn *conn)
{
	return SSL_DataPending(conn->pr_fd) > 0;
}

/*
 * pgtls_write
 *		Write data on the secure socket
 *
 */
ssize_t
pgtls_write(PGconn *conn, const void *ptr, size_t len)
{
	PRInt32		n;
	PRErrorCode status;
	int			write_errno = 0;

	n = PR_Write(conn->pr_fd, ptr, len);

	if (n < 0)
	{
		status = PR_GetError();

		switch (status)
		{
			case PR_WOULD_BLOCK_ERROR:
#ifdef EAGAIN
				write_errno = EAGAIN;
#else
				write_errno = EINTR;
#endif
				break;

			default:
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("TLS write error: %s"),
								  pg_SSLerrmessage(status));
				write_errno = ECONNRESET;
				break;
		}
	}

	SOCK_ERRNO_SET(write_errno);
	return (ssize_t) n;
}

char *
pgtls_get_peer_certificate_hash(PGconn *conn, size_t *len)
{
	CERTCertificate *server_cert;
	SECOidTag	signature_tag;
	SECOidTag	digest_alg;
	int			digest_len;
	PLArenaPool *arena = NULL;
	SECItem		digest;
	char	   *ret = NULL;
	SECStatus	status;

	*len = 0;

	server_cert = SSL_PeerCertificate(conn->pr_fd);
	if (!server_cert)
		goto cleanup;

	signature_tag = SECOID_GetAlgorithmTag(&server_cert->signature);
	if (!pg_find_signature_algorithm(signature_tag, &digest_alg, &digest_len))
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not find digest for OID '%s'\n"),
						  SECOID_FindOIDTagDescription(signature_tag));
		goto cleanup;
	}

	arena = PORT_NewArena(SEC_ASN1_DEFAULT_ARENA_SIZE);
	digest.data = PORT_ArenaZAlloc(arena, sizeof(unsigned char) * digest_len);
	if (!digest.data)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("out of memory"));
		goto cleanup;
	}
	digest.len = digest_len;

	status = PK11_HashBuf(digest_alg, digest.data, server_cert->derCert.data,
						  server_cert->derCert.len);

	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to generate peer certificate digest: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		goto cleanup;
	}

	ret = malloc(digest.len);
	if (ret == NULL)
	{
		appendPQExpBufferStr(&conn->errorMessage,
							 libpq_gettext("out of memory\n"));
		goto cleanup;
	}

	memcpy(ret, digest.data, digest.len);
	*len = digest_len;

cleanup:
	if (arena)
		PORT_FreeArena(arena, PR_TRUE);
	if (server_cert)
		CERT_DestroyCertificate(server_cert);

	return ret;
}

/*
 *	Verify that the server certificate matches the hostname we connected to.
 *
 * The certificate's Common Name and Subject Alternative Names are considered.
 */
int
pgtls_verify_peer_name_matches_certificate_guts(PGconn *conn,
												int *names_examined,
												char **first_name)
{
	char	   *server_hostname = NULL;
	CERTCertificate *server_cert = NULL;
	SECStatus	status = SECSuccess;
	SECItem		altname_item;
	PLArenaPool *arena = NULL;
	CERTGeneralName *san_list;
	CERTGeneralName *cn;

	/*
	 * In the "standard" NSS use case, we'd call SSL_RevealURL() to get the
	 * hostname. In our case, SSL_SetURL() won't have been called if the user
	 * has disabled SNI, so we always pull the hostname from connhost instead.
	 */
	server_hostname = conn->connhost[conn->whichhost].host;
	if (!server_hostname || server_hostname[0] == '\0')
	{
		/* verify-full shouldn't progress this far without a hostname. */
		status = SECFailure;
		goto done;
	}

	/*
	 * CERT_VerifyCertName will internally perform RFC 2818 SubjectAltName
	 * verification.
	 */
	server_cert = SSL_PeerCertificate(conn->pr_fd);
	status = CERT_VerifyCertName(server_cert, server_hostname);
	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to verify server hostname: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		goto done;
	}

	status = CERT_FindCertExtension(server_cert, SEC_OID_X509_SUBJECT_ALT_NAME,
									&altname_item);
	if (status == SECSuccess)
	{
		arena = PORT_NewArena(DER_DEFAULT_CHUNKSIZE);
		if (!arena)
		{
			status = SECFailure;
			goto done;
		}
		san_list = CERT_DecodeAltNameExtension(arena, &altname_item);
		if (!san_list)
		{
			status = SECFailure;
			goto done;
		}

		for (cn = san_list; cn != san_list; cn = CERT_GetNextGeneralName(cn))
		{
			char	   *alt_name;
			int			rv;
			char		tmp[512];

			status = CERT_RFC1485_EscapeAndQuote(tmp, sizeof(tmp),
												 (char *) cn->name.other.data,
												 cn->name.other.len);

			if (status != SECSuccess)
				goto done;

			rv = pq_verify_peer_name_matches_certificate_name(conn, tmp,
															  strlen(tmp),
															  &alt_name);
			if (alt_name)
			{
				if (!*first_name)
					*first_name = alt_name;
				else
					free(alt_name);
			}

			if (rv == 1)
				status = SECSuccess;
			else
			{
				status = SECFailure;
				break;
			}
		}
	}
	else if (PORT_GetError() == SEC_ERROR_EXTENSION_NOT_FOUND)
		status = SECSuccess;
	else
		status = SECSuccess;

done:
	/* san_list will be freed by freeing the arena it was allocated in */
	if (arena)
		PORT_FreeArena(arena, PR_TRUE);
	if (server_cert)
		CERT_DestroyCertificate(server_cert);

	if (status == SECSuccess)
		return 1;

	return 0;
}

/* ------------------------------------------------------------ */
/*			PostgreSQL specific TLS support functions			*/
/* ------------------------------------------------------------ */

static const char *
pg_SSLerrmessage(PRErrorCode errcode)
{
	const char *error;

	/*
	 * Try to get the user friendly error description, and if that fails try
	 * to fall back on the name of the PRErrorCode.
	 */
	error = PR_ErrorToString(errcode, PR_LANGUAGE_I_DEFAULT);
	if (!error)
		error = PR_ErrorToName(errcode);
	if (error)
		return error;

	return "unknown TLS error";
}

static SECStatus
pg_load_nss_module(SECMODModule **module, const char *library, const char *name)
{
	SECMODModule *mod;
	PQExpBufferData modulespec;

	initPQExpBuffer(&modulespec);
	appendPQExpBuffer(&modulespec, "library=\"%s\", name=\"%s\"", library, name);
	if (PQExpBufferDataBroken(modulespec))
	{
		PR_SetError(SEC_ERROR_NO_MEMORY, 0);
		return SECFailure;
	}

	/*
	 * Attempt to load the specified module. The second parameter is "parent"
	 * which should always be NULL for application code. The third parameter
	 * defines if loading should recurse which is only applicable when loading
	 * a module from within another module. This hierarchy would have to be
	 * defined in the modulespec, and since we don't support anything but
	 * directly addressed modules we should pass PR_FALSE.
	 */
	mod = SECMOD_LoadUserModule(modulespec.data, NULL, PR_FALSE);
	termPQExpBuffer(&modulespec);

	if (mod && mod->loaded)
	{
		*module = mod;
		return SECSuccess;
	}

	SECMOD_DestroyModule(mod);
	return SECFailure;
}

/* ------------------------------------------------------------ */
/*						NSS Callbacks							*/
/* ------------------------------------------------------------ */

/*
 * pg_cert_auth_handler
 *			Callback for authenticating server certificate
 *
 * This is pretty much the same procedure as the SSL_AuthCertificate function
 * provided by NSS, with the difference being server hostname validation. With
 * SSL_AuthCertificate there is no way to do verify-ca, it only does the -full
 * flavor of our sslmodes, so we need our own implementation.
 */
static SECStatus
pg_cert_auth_handler(void *arg, PRFileDesc *fd, PRBool checksig, PRBool isServer)
{
	SECStatus	status;
	PGconn	   *conn = (PGconn *) arg;
	CERTCertificate *server_cert;
	void	   *pin;

	Assert(!isServer);

	pin = SSL_RevealPinArg(conn->pr_fd);
	server_cert = SSL_PeerCertificate(conn->pr_fd);

	/*
	 * VerifyCertificateNow verifies the validity using PR_now from NSPR as a
	 * timestamp and will perform CRL and OSCP revocation checks. conn->sslcrl
	 * does not impact NSS connections as any CRL in the NSS database will be
	 * used automatically.
	 */
	status = CERT_VerifyCertificateNow((CERTCertDBHandle *) CERT_GetDefaultCertDB(),
									   server_cert,
									   checksig,
									   certificateUsageSSLServer,
									   pin,
									   NULL);

	/*
	 * If we've already failed validation then there is no point in also
	 * performing the hostname check for verify-full.
	 */
	if (status == SECSuccess)
	{
		if (!pq_verify_peer_name_matches_certificate(conn))
			status = SECFailure;
	}

	CERT_DestroyCertificate(server_cert);
	return status;
}

/*
 * pg_client_auth_handler
 *		Callback for client certificate validation
 *
 * The client auth callback is not on by default in NSS, so we need to invoke
 * it ourselves to ensure we can do cert authentication.
 */
static SECStatus
pg_client_auth_handler(void *arg, PRFileDesc *socket, CERTDistNames *caNames,
					   CERTCertificate **pRetCert, SECKEYPrivateKey **pRetKey)
{
	PGconn	   *conn = (PGconn *) arg;
	char	   *nnptr = NULL;
	char		nickname[MAXPGPATH];

	/*
	 * If we have an sslcert configured we use that combined with the token-
	 * name as the nickname. When no sslcert is set we pass in NULL and let
	 * NSS try to find the certificate among the ones present in the database.
	 * Without an sslcert we cannot set the database token since NSS parses
	 * the given string expecting to find a certificate nickname after the
	 * colon.
	 */
	if (conn->sslcert)
	{
		snprintf(nickname, sizeof(nickname), "postgres:%s", conn->sslcert);
		nnptr = nickname;
	}

	return NSS_GetClientAuthData(nnptr, socket, caNames, pRetCert, pRetKey);
}

/*
 * pg_bad_cert_handler
 *		Callback for failed certificate validation
 *
 * The TLS handshake will call this function iff the server certificate failed
 * validation. Depending on the sslmode, we allow the connection anyways.
 */
static SECStatus
pg_bad_cert_handler(void *arg, PRFileDesc *fd)
{
	PGconn	   *conn = (PGconn *) arg;
	PRErrorCode err;

	/*
	 * This really shouldn't happen, as we've set the PGconn object as our
	 * callback data, and at the callsite we know it will be populated. That
	 * being said, the NSS code itself performs this check even when it should
	 * not be required so let's use the same belts with our suspenders.
	 */
	if (!arg)
		return SECFailure;

	err = PORT_GetError();

	/*
	 * For sslmodes other than verify-full and verify-ca we don't perform peer
	 * validation, so return immediately.  sslmode require with a database
	 * specified which contains a CA certificate will work like verify-ca to
	 * be compatible with the OpenSSL implementation.
	 */
	if (strcmp(conn->sslmode, "require") == 0)
	{
		if (conn->ssldatabase &&
			strlen(conn->ssldatabase) > 0 &&
			certificate_database_has_CA(conn))
			goto failure;
	}
	else if (strcmp(conn->sslmode, "verify-full") == 0 ||
			 strcmp(conn->sslmode, "verify-ca") == 0)
		goto failure;

	/*
	 * TODO: these are relevant error codes that can occur in certificate
	 * validation, figure out which we don't want for require/prefer etc.
	 */
	switch (err)
	{
		case SEC_ERROR_INVALID_AVA:
		case SEC_ERROR_INVALID_TIME:
		case SEC_ERROR_BAD_SIGNATURE:
		case SEC_ERROR_EXPIRED_CERTIFICATE:
		case SEC_ERROR_UNKNOWN_ISSUER:
		case SEC_ERROR_UNTRUSTED_ISSUER:
		case SEC_ERROR_UNTRUSTED_CERT:
		case SEC_ERROR_CERT_VALID:
		case SEC_ERROR_EXPIRED_ISSUER_CERTIFICATE:
		case SEC_ERROR_CRL_EXPIRED:
		case SEC_ERROR_CRL_BAD_SIGNATURE:
		case SEC_ERROR_EXTENSION_VALUE_INVALID:
		case SEC_ERROR_CA_CERT_INVALID:
		case SEC_ERROR_CERT_USAGES_INVALID:
		case SEC_ERROR_UNKNOWN_CRITICAL_EXTENSION:
			break;
		default:
			goto failure;
			break;
	}

	return SECSuccess;

failure:
	printfPQExpBuffer(&conn->errorMessage,
					  libpq_gettext("unable to verify certificate: %s"),
					  pg_SSLerrmessage(err));

	return SECFailure;
}

/* ------------------------------------------------------------ */
/*					SSL information functions					*/
/* ------------------------------------------------------------ */

/*
 * PQgetssl
 *
 * Return NULL as this is legacy and defined to always be equal to calling
 * PQsslStruct(conn, "OpenSSL"); This should ideally trigger a logged warning
 * somewhere as it's nonsensical to run in a non-OpenSSL build.
 */
void *
PQgetssl(PGconn *conn)
{
	return NULL;
}

void *
PQsslStruct(PGconn *conn, const char *struct_name)
{
	if (!conn)
		return NULL;

	/*
	 * Return the underlying PRFileDesc which can be used to access
	 * information on the connection details. There is no SSL context per se.
	 */
	if (strcmp(struct_name, "NSS") == 0)
		return conn->pr_fd;
	return NULL;
}

const char *const *
PQsslAttributeNames(PGconn *conn)
{
	static const char *const result[] = {
		"library",
		"cipher",
		"protocol",
		"key_bits",
		"compression",
		NULL
	};

	return result;
}

const char *
PQsslAttribute(PGconn *conn, const char *attribute_name)
{
	SECStatus	status;
	SSLChannelInfo channel;
	SSLCipherSuiteInfo suite;

	if (!conn || !conn->pr_fd)
		return NULL;

	if (strcmp(attribute_name, "library") == 0)
		return "NSS";

	status = SSL_GetChannelInfo(conn->pr_fd, &channel, sizeof(channel));
	if (status != SECSuccess)
		return NULL;

	status = SSL_GetCipherSuiteInfo(channel.cipherSuite, &suite, sizeof(suite));
	if (status != SECSuccess)
		return NULL;

	if (strcmp(attribute_name, "cipher") == 0)
		return suite.cipherSuiteName;

	if (strcmp(attribute_name, "key_bits") == 0)
	{
		static char key_bits_str[8];

		snprintf(key_bits_str, sizeof(key_bits_str), "%i", suite.effectiveKeyBits);
		return key_bits_str;
	}

	if (strcmp(attribute_name, "protocol") == 0)
		return ssl_protocol_version_to_string(channel.protocolVersion);

	/*
	 * NSS disabled support for compression in version 3.33, and it was only
	 * available for SSLv3 at that point anyways, so we can safely return off
	 * here without even checking.
	 */
	if (strcmp(attribute_name, "compression") == 0)
		return "off";

	return NULL;
}

/*
 * ssl_protocol_param_to_nss
 *
 * Return the NSS internal representation of the protocol asked for by the
 * user as a connection parameter.
 */
static int
ssl_protocol_param_to_nss(const char *protocol)
{
	if (pg_strcasecmp("TLSv1", protocol) == 0)
		return SSL_LIBRARY_VERSION_TLS_1_0;

#ifdef SSL_LIBRARY_VERSION_TLS_1_1
	if (pg_strcasecmp("TLSv1.1", protocol) == 0)
		return SSL_LIBRARY_VERSION_TLS_1_1;
#endif

#ifdef SSL_LIBRARY_VERSION_TLS_1_2
	if (pg_strcasecmp("TLSv1.2", protocol) == 0)
		return SSL_LIBRARY_VERSION_TLS_1_2;
#endif

#ifdef SSL_LIBRARY_VERSION_TLS_1_3
	if (pg_strcasecmp("TLSv1.3", protocol) == 0)
		return SSL_LIBRARY_VERSION_TLS_1_3;
#endif

	return -1;
}

/*
 * certificate_database_has_CA
 *		 Check for the presence of a CA certificate
 *
 * Returns true in case there is a CA certificate in the database connected
 * to in the conn object, else false. This function only checks for the
 * presence of a CA certificate, not its validity and/or usefulness.
 */
static bool
certificate_database_has_CA(PGconn *conn)
{
	CERTCertList *certificates;
	bool		hasCA;

	/*
	 * If the certificate database has a password we must provide it, since
	 * this API doesn't invoke the standard password callback.
	 */
	if (conn->has_password)
		certificates = PK11_ListCerts(PK11CertListCA, PQssl_passwd_cb(NULL, PR_FALSE, (void *) conn));
	else
		certificates = PK11_ListCerts(PK11CertListCA, NULL);
	hasCA = !CERT_LIST_EMPTY(certificates);
	CERT_DestroyCertList(certificates);

	return hasCA;
}

PQsslKeyPassHook_nss_type
PQgetSSLKeyPassHook_nss(void)
{
	return PQsslKeyPassHook;
}

void
PQsetSSLKeyPassHook_nss(PQsslKeyPassHook_nss_type hook)
{
	PQsslKeyPassHook = hook;
}

/*
 * PQssl_passwd_cb
 *		 Supply a password to decrypt an object
 *
 * If an object in the NSS database is password protected, or if the entire
 * NSS database is itself password protected, this callback will be invoked.
 *
 * This must match NSS type PK11PasswordFunc.
 */
static char *
PQssl_passwd_cb(PK11SlotInfo *slot, PRBool retry, void *arg)
{
	PGconn	   *conn = (PGconn *) arg;

	conn->has_password = true;

	if (PQsslKeyPassHook)
		return PQsslKeyPassHook(slot, (PRBool) retry, arg);
	else
		return PQdefaultSSLKeyPassHook_nss(slot, retry, arg);
}

/*
 * PQdefaultSSLKeyPassHook_nss
 * 		 Default password handler callback
 *
 * If no user defined password callback has been set up, the callback below
 * will try the password set by the sslpassword parameter. Since we by legacy
 * only have support for a single password, there is little reason to inspect
 * the slot for the object in question. A TODO is to support different
 * passwords for different objects, either via a DSL in sslpassword or with a
 * new key/value style parameter. Users can supply their own password hook to
 * do this of course, but it would be nice to support something in core too.
 */
char *
PQdefaultSSLKeyPassHook_nss(PK11SlotInfo *slot, PRBool retry, void *arg)
{
	PGconn	   *conn = (PGconn *) arg;

	/*
	 * If the password didn't work the first time there is no point in
	 * retrying as it hasn't changed.
	 */
	if (retry != PR_TRUE && conn->sslpassword && strlen(conn->sslpassword) > 0)
		return PORT_Strdup(conn->sslpassword);

	return NULL;
}
