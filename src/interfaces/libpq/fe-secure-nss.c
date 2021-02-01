/*-------------------------------------------------------------------------
 *
 * fe-secure-nss.c
 *	  functions for supporting NSS as a TLS backend for frontend libpq
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
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
#include "common/pg_nss.h"

/*
 * BITS_PER_BYTE is also defined in the NSPR header files, so we need to undef
 * our version to avoid compiler warnings on redefinition.
 */
#define pg_BITS_PER_BYTE BITS_PER_BYTE
#undef BITS_PER_BYTE

/*
 * The nspr/obsolete/protypes.h NSPR header typedefs uint64 and int64 with
 * colliding definitions from ours, causing a much expected compiler error.
 * Remove backwards compatibility with ancient NSPR versions to avoid this.
 */
#define NO_NSPR_10_SUPPORT
#include <nspr/nspr.h>
#include <nspr/prerror.h>
#include <nspr/prinit.h>
#include <nspr/prio.h>
#include <nss/hasht.h>
#include <nss/nss.h>
#include <nss/ssl.h>
#include <nss/sslproto.h>
#include <nss/pk11func.h>
#include <nss/secerr.h>
#include <nss/secoidt.h>
#include <nss/secmod.h>

/*
 * Ensure that the colliding definitions match, else throw an error. In case
 * NSPR remove the definition in a future version (however unlikely that may
 * be, make sure to put ours back again.
 */
#if defined(BITS_PER_BYTE)
#if BITS_PER_BYTE != pg_BITS_PER_BYTE
#error "incompatible byte widths between NSPR and PostgreSQL"
#endif
#else
#define BITS_PER_BYTE pg_BITS_PER_BYTE
#endif
#undef pg_BITS_PER_BYTE

static SECStatus pg_load_nss_module(SECMODModule * *module, const char *library, const char *name);
static SECStatus pg_bad_cert_handler(void *arg, PRFileDesc * fd);
static const char *pg_SSLerrmessage(PRErrorCode errcode);
static SECStatus pg_client_auth_handler(void *arg, PRFileDesc * socket, CERTDistNames * caNames,
										CERTCertificate * *pRetCert, SECKEYPrivateKey * *pRetKey);
static SECStatus pg_cert_auth_handler(void *arg, PRFileDesc * fd, PRBool checksig, PRBool isServer);
static int	ssl_protocol_version_to_nss(const char *protocol);
static bool cert_database_has_CA(PGconn *conn);

static char *PQssl_passwd_cb(PK11SlotInfo * slot, PRBool retry, void *arg);

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

static SECMODModule * ca_trust = NULL;
static NSSInitContext * nss_context = NULL;

/*
 * This logic exist in NSS as well, but it's only available for when there is
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
pgtls_init(PGconn *conn)
{
	conn->ssl_in_use = false;
	conn->has_password = false;

	return 0;
}

void
pgtls_close(PGconn *conn)
{
	if (nss_context)
	{
		NSS_ShutdownContext(nss_context);
		nss_context = NULL;
	}
}

PostgresPollingStatusType
pgtls_open_client(PGconn *conn)
{
	SECStatus	status;
	PRFileDesc *pr_fd;
	PRFileDesc *model;
	NSSInitParameters params;
	SSLVersionRange desired_range;

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

	if (conn->cert_database && strlen(conn->cert_database) > 0)
	{
		char	   *cert_database_path = psprintf("sql:%s", conn->cert_database);

		nss_context = NSS_InitContext(cert_database_path, "", "", "",
									  &params,
									  NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
		pfree(cert_database_path);

		if (!nss_context)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unable to open certificate database \"%s\": %s"),
							  conn->cert_database,
							  pg_SSLerrmessage(PR_GetError()));
			return PGRES_POLLING_FAILED;
		}
	}
	else
	{
		nss_context = NSS_InitContext("", "", "", "", &params,
									  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
									  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
									  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);
		if (!nss_context)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unable to create certificate database: %s"),
							  pg_SSLerrmessage(PR_GetError()));
			return PGRES_POLLING_FAILED;
		}
	}

	/*
	 * Configure cipher policy.
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
	 * still attempt a connection as long as the sslmode isn't verify*.
	 */
	if (!conn->cert_database && conn->sslmode[0] == 'v')
	{
		status = pg_load_nss_module(&ca_trust, ca_trust_name, "\"Root Certificates\"");
		if (status != SECSuccess)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("WARNING: unable to load NSS trust module \"%s\" : %s"),
							  ca_trust_name,
							  pg_SSLerrmessage(PR_GetError()));

			return PGRES_POLLING_FAILED;
		}
	}


	PK11_SetPasswordFunc(PQssl_passwd_cb);

	/*
	 * Import the already opened socket as we don't want to use NSPR functions
	 * for opening the network socket due to how the PostgreSQL protocol works
	 * with TLS connections. This function is not part of the NSPR public API,
	 * see the comment at the top of the file for the rationale of still using
	 * it.
	 */
	pr_fd = PR_ImportTCPSocket(conn->sock);
	if (!pr_fd)
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
		int			ssl_min_ver = ssl_protocol_version_to_nss(conn->ssl_min_protocol_version);

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
		int			ssl_max_ver = ssl_protocol_version_to_nss(conn->ssl_max_protocol_version);

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
	conn->pr_fd = SSL_ImportFD(model, pr_fd);
	if (!conn->pr_fd)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to configure client for TLS: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * The model can now we closed as we've applied the settings of the model
	 * onto the real socket. From hereon we should only use conn->pr_fd.
	 */
	PR_Close(model);

	/* Set the private data to be passed to the password callback */
	SSL_SetPKCS11PinArg(conn->pr_fd, (void *) conn);

	/*
	 * If a CRL file has been specified, verify if it exists in the database
	 * but don't fail in case it doesn't.
	 */
	if (conn->sslcrl && strlen(conn->sslcrl) > 0)
	{
		/* XXX: Implement me.. */
	}

	status = SSL_ResetHandshake(conn->pr_fd, PR_FALSE);
	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to initiate handshake: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * Set callback for client authentication when requested by the server.
	 */
	SSL_GetClientAuthDataHook(conn->pr_fd, pg_client_auth_handler, (void *) conn);

	/*
	 * Specify which hostname we are expecting to talk to. This is required,
	 * albeit mostly applies to when opening a connection to a traditional
	 * http server it seems.
	 */
	SSL_SetURL(conn->pr_fd, (conn->connhost[conn->whichhost]).host);

	do
	{
		status = SSL_ForceHandshake(conn->pr_fd);
	}
	while (status != SECSuccess && PR_GetError() == PR_WOULD_BLOCK_ERROR);

	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("SSL error: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		return PGRES_POLLING_FAILED;
	}

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
 *		Check for the existence of data to be read.
 *
 * This is part of the PostgreSQL TLS backend API.
 */
bool
pgtls_read_pending(PGconn *conn)
{
	unsigned char c;
	int			n;

	/*
	 * PR_Recv peeks into the stream with the timeount turned off, to see if
	 * there is another byte to read off the wire. There is an NSS function
	 * SSL_DataPending() which might seem like a better fit, but it will only
	 * check already encrypted data in the SSL buffer, not still unencrypted
	 * data, thus it doesn't guarantee that a subsequent call to
	 * PR_Read/PR_Recv won't block.
	 */
	n = PR_Recv(conn->pr_fd, &c, 1, PR_MSG_PEEK, PR_INTERVAL_NO_WAIT);
	return (n > 0);
}

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
	SECOidTag	signature_alg;
	SECOidTag	digest_alg;
	int			digest_len;
	const NSSSignatureAlgorithms *candidate;
	PLArenaPool *arena;
	SECItem		digest;
	char	   *ret;
	SECStatus	status;

	*len = 0;

	server_cert = SSL_PeerCertificate(conn->pr_fd);
	if (!server_cert)
		return NULL;

	signature_alg = SECOID_GetAlgorithmTag(&server_cert->signature);

	candidate = NSS_SCRAMDigestAlgorithm;
	while (candidate->signature)
	{
		if (signature_alg == candidate->signature)
		{
			digest_alg = candidate->hash;
			digest_len = candidate->len;
			break;
		}

		candidate++;
	}

	if (!candidate->signature)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not find digest for OID '%s'\n"),
						  SECOID_FindOIDTagDescription(signature_alg));
		return NULL;
	}

	arena = PORT_NewArena(SEC_ASN1_DEFAULT_ARENA_SIZE);
	digest.data = PORT_ArenaZAlloc(arena, sizeof(unsigned char) * digest_len);
	digest.len = digest_len;

	status = PK11_HashBuf(digest_alg, digest.data, server_cert->derCert.data,
						  server_cert->derCert.len);

	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to generate peer certificate digest: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		PORT_FreeArena(arena, PR_TRUE);
		return NULL;
	}

	ret = pg_malloc(digest.len);
	memcpy(ret, digest.data, digest.len);
	*len = digest_len;
	PORT_FreeArena(arena, PR_TRUE);

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

	server_hostname = SSL_RevealURL(conn->pr_fd);
	if (!server_hostname || server_hostname[0] == '\0')
		goto done;

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
	PR_Free(server_hostname);

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
pg_load_nss_module(SECMODModule * *module, const char *library, const char *name)
{
	SECMODModule *mod;
	char	   *modulespec;

	modulespec = psprintf("library=\"%s\", name=\"%s\"", library, name);

	/*
	 * Attempt to load the specified module. The second parameter is "parent"
	 * which should always be NULL for application code. The third parameter
	 * defines if loading should recurse which is only applicable when loading
	 * a module from within another module. This hierarchy would have to be
	 * defined in the modulespec, and since we don't support anything but
	 * directly addressed modules we should pass PR_FALSE.
	 */
	mod = SECMOD_LoadUserModule(modulespec, NULL, PR_FALSE);
	pfree(modulespec);

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
	else
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to verify certificate: %s"),
						  pg_SSLerrmessage(PR_GetError()));
	}

	return status;
}

/*
 * pg_client_auth_handler
 *		Callback for client certificate validation
 *
 * The client auth callback is not on by default in NSS, so we need to invoke
 * it ourselves to ensure we can do cert authentication. A TODO is to support
 * running without a specified sslcert parameter. By retrieving all the certs
 * via nickname from the cert database and see if we find one which apply with
 * NSS_CmpCertChainWCANames() and PK11_FindKeyByAnyCert() we could support
 * just running with a ssl database specified.
 *
 * For now, we use the default client certificate validation which requires a
 * defined nickname to identify the cert in the database.
 */
static SECStatus
pg_client_auth_handler(void *arg, PRFileDesc * socket, CERTDistNames * caNames,
					   CERTCertificate * *pRetCert, SECKEYPrivateKey * *pRetKey)
{
	PGconn	   *conn = (PGconn *) arg;

	return NSS_GetClientAuthData(conn->sslcert, socket, caNames, pRetCert, pRetKey);
}

/*
 * pg_bad_cert_handler
 *		Callback for failed certificate validation
 *
 * The TLS handshake will call this function iff the server certificate failed
 * validation. Depending on the sslmode, we allow the connection anyways.
 */
static SECStatus
pg_bad_cert_handler(void *arg, PRFileDesc * fd)
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

	/*
	 * For sslmodes other than verify-full and verify-ca we don't perform peer
	 * validation, so return immediately.  sslmode require with a database
	 * specified which contains a CA certificate will work like verify-ca to
	 * be compatible with the OpenSSL implementation.
	 */
	if (strcmp(conn->sslmode, "require") == 0)
	{
		if (conn->cert_database && strlen(conn->cert_database) > 0 && cert_database_has_CA(conn))
			return SECFailure;
	}
	if (conn->sslmode[0] == 'v')
		return SECFailure;

	err = PORT_GetError();

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
			return SECSuccess;
			break;
		default:
			return SECFailure;
			break;
	}

	/* Unreachable */
	return SECSuccess;
}

/* ------------------------------------------------------------ */
/*					SSL information functions					*/
/* ------------------------------------------------------------ */

/*
 * PQgetssl
 *
 * Return NULL as this is legacy and defined to always be equal to calling
 * PQsslStruct(conn, "OpenSSL"); This should ideally trigger a logged warning
 * somewhere as it's nonsensical to run in a non-OpenSSL build, but the color
 * of said bikeshed hasn't yet been determined.
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
	{
		switch (channel.protocolVersion)
		{
#ifdef SSL_LIBRARY_VERSION_TLS_1_3
			case SSL_LIBRARY_VERSION_TLS_1_3:
				return "TLSv1.3";
#endif
#ifdef SSL_LIBRARY_VERSION_TLS_1_2
			case SSL_LIBRARY_VERSION_TLS_1_2:
				return "TLSv1.2";
#endif
#ifdef SSL_LIBRARY_VERSION_TLS_1_1
			case SSL_LIBRARY_VERSION_TLS_1_1:
				return "TLSv1.1";
#endif
			case SSL_LIBRARY_VERSION_TLS_1_0:
				return "TLSv1.0";
			default:
				return "unknown";
		}
	}

	/*
	 * NSS disabled support for compression in version 3.33, and it was only
	 * available for SSLv3 at that point anyways, so we can safely return off
	 * here without checking.
	 */
	if (strcmp(attribute_name, "compression") == 0)
		return "off";

	return NULL;
}

static int
ssl_protocol_version_to_nss(const char *protocol)
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
 * cert_database_has_CA
 *
 * Returns true in case there is a CA certificate in the database connected
 * to in the conn object, else false. This function only checks for the
 * presence of a CA certificate, not it's validity and/or usefulness.
 */
static bool
cert_database_has_CA(PGconn *conn)
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
 * Supply a password to decrypt a client certificate.
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
 * The default password handler callback.
 */
char *
PQdefaultSSLKeyPassHook_nss(PK11SlotInfo * slot, PRBool retry, void *arg)
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
