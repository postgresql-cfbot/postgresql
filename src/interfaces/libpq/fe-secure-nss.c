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
#include "libpq-int.h"

/*
 * BITS_PER_BYTE is also defined in the NSPR header fils, so we need to undef
 * our version to avoid compiler warnings on redefinition.
 */
#define pg_BITS_PER_BYTE BITS_PER_BYTE
#undef BITS_PER_BYTE

/*
 * The nspr/obsolete/protypes.h NSPR header typedefs uint64 and int64 with
 * colliding definitions from ours, causing a much expected compiler error.
 * The definitions are however not actually used in NSPR at all, and are only
 * intended for what seems to be backwards compatibility for apps written
 * against old versions of NSPR.  The following comment is in the referenced
 * file, and was added in 1998:
 *
 *		This section typedefs the old 'native' types to the new PR<type>s.
 *		These definitions are scheduled to be eliminated at the earliest
 *		possible time. The NSPR API is implemented and documented using
 *		the new definitions.
 *
 * As there is no opt-out from pulling in these typedefs, we define the guard
 * for the file to exclude it. This is incredibly ugly, but seems to be about
 * the only way around it.
 */
#define PROTYPES_H
#include <nspr.h>
#undef PROTYPES_H
#include <nss.h>
#include <ssl.h>
#include <sslproto.h>
#include <pk11func.h>
#include <prerror.h>
#include <prinit.h>
#include <prio.h>
#include <secerr.h>
#include <secmod.h>

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
static char *pg_SSLerrmessage(PRErrorCode errcode);
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
 * Track whether the NSS database has a password set or not. There is no API
 * function for retrieving password status, so we simply flip this to true in
 * case NSS invoked the password callback - as that will only happen in case
 * there is a password. The reason for tracking this is that there are calls
 * which require a password parameter, but doesn't use the callbacks provided,
 * so we must call the callback on behalf of these.
 */
static bool has_password = false;

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

void
pgtls_init_library(bool do_ssl, int do_crypto)
{
	/* TODO: implement me .. */
}

int
pgtls_init(PGconn *conn)
{
	conn->ssl_in_use = false;

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
	 * initialization implicitly. The documentation doesn't however clarify
	 * from which version this is holds true, so let's perform the potentially
	 * superfluous initialization anyways to avoid crashing on older versions
	 * of NSPR, as there is no difference in overhead.  The NSS documentation
	 * still states that PR_Init must be called in some way (implicitly or
	 * explicitly).
	 *
	 * The below parameters are what the implicit initialization would've done
	 * for us, and should work even for older versions where it might not be
	 * done automatically. The last parameter, maxPTDs, is set to various
	 * values in other codebases, but has been unused since NSPR 2.1 which was
	 * released sometime in 1998.
	 */
	PR_Init(PR_USER_THREAD, PR_PRIORITY_NORMAL, 0);

	/*
	 * The original design of NSS was for a single application to use a single
	 * copy of it, initialized with NSS_Initialize() which isn't returning any
	 * handle with which to refer to NSS. NSS initialization and shutdown are
	 * global for the application, so a shutdown in another NSS enabled
	 * library would cause NSS to be stopped for libpq as well.  The fix has
	 * been to introduce NSS_InitContext which returns a context handle to
	 * pass to NSS_ShutdownContext.  NSS_InitContext was introduced in NSS
	 * 3.12, but the use of it is not very well documented.
	 * https://bugzilla.redhat.com/show_bug.cgi?id=738456
	 *
	 * The InitParameters struct passed can be used to override internal
	 * values in NSS, but the usage is not documented at all. When using
	 * NSS_Init initializations, the values are instead set via PK11_Configure
	 * calls so the PK11_Configure documentation can be used to glean some
	 * details on these.
	 *
	 * https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS/PKCS11/Module_Specs
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
	}
	else
		nss_context = NSS_InitContext("", "", "", "", &params,
									  NSS_INIT_READONLY | NSS_INIT_NOCERTDB |
									  NSS_INIT_NOMODDB | NSS_INIT_FORCEOPEN |
									  NSS_INIT_NOROOTINIT | NSS_INIT_PK11RELOAD);

	if (!nss_context)
	{
		char	   *err = pg_SSLerrmessage(PR_GetError());

		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to %s certificate database: %s"),
						  conn->cert_database ? "open" : "create",
						  err);
		free(err);
		return PGRES_POLLING_FAILED;
	}

	/*
	 * Configure cipher policy.
	 */
	status = NSS_SetDomesticPolicy();
	if (status != SECSuccess)
	{
		char	   *err = pg_SSLerrmessage(PR_GetError());

		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to configure cipher policy: %s"),
						  err);
		free(err);
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
		/* status = pg_load_nss_module(&ca_trust, ca_trust_name, "trust"); */
		if (status != SECSuccess)
		{
			char	   *err = pg_SSLerrmessage(PR_GetError());

			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("WARNING: unable to load NSS trust module \"%s\" : %s"), ca_trust_name, err);
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

	nread = PR_Recv(conn->pr_fd, ptr, len, 0, PR_INTERVAL_NO_WAIT);

	/*
	 * PR_Recv blocks until there is data to read or the timeout expires. Zero
	 * is returned for closed connections, while -1 indicates an error within
	 * the ongoing connection.
	 */
	if (nread == 0)
	{
		read_errno = ECONNRESET;
		return -1;
	}

	if (nread == -1)
	{
		status = PR_GetError();

		switch (status)
		{
			case PR_WOULD_BLOCK_ERROR:
				read_errno = EINTR;
				break;

			case PR_IO_TIMEOUT_ERROR:
				break;

				/*
				 * The error cases for PR_Recv are not documented, but can be
				 * reverse engineered from _MD_unix_map_default_error() in the
				 * NSPR code, defined in pr/src/md/unix/unix_errors.c.
				 */
			default:
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("TLS read error: %s"),
								  pg_SSLerrmessage(status));
				break;
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
	 * PR_Read/PR_Recv wont block.
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

/* ------------------------------------------------------------ */
/*			PostgreSQL specific TLS support functions			*/
/* ------------------------------------------------------------ */

/*
 * TODO: this a 99% copy of the same function in the backend, make these share
 * a single implementation instead.
 */
static char *
pg_SSLerrmessage(PRErrorCode errcode)
{
	const char *error;

	error = PR_ErrorToName(errcode);
	if (error)
		return strdup(error);

	return strdup("unknown TLS error");
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
pg_cert_auth_handler(void *arg, PRFileDesc * fd, PRBool checksig, PRBool isServer)
{
	SECStatus	status;
	PGconn	   *conn = (PGconn *) arg;
	char	   *server_hostname = NULL;
	CERTCertificate *server_cert;
	void	   *pin;

	Assert(!isServer);

	pin = SSL_RevealPinArg(conn->pr_fd);
	server_cert = SSL_PeerCertificate(conn->pr_fd);

	status = CERT_VerifyCertificateNow((CERTCertDBHandle *) CERT_GetDefaultCertDB(), server_cert,
									   checksig, certificateUsageSSLServer,
									   pin, NULL);

	/*
	 * If we've already failed validation then there is no point in also
	 * performing the hostname check for verify-full.
	 */
	if (status != SECSuccess)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("unable to verify certificate: %s"),
						  pg_SSLerrmessage(PR_GetError()));
		goto done;
	}

	if (strcmp(conn->sslmode, "verify-full") == 0)
	{
		server_hostname = SSL_RevealURL(conn->pr_fd);
		if (!server_hostname || server_hostname[0] == '\0')
			goto done;

		/*
		 * CERT_VerifyCertName will internally perform RFC 2818 SubjectAltName
		 * verification.
		 */
		status = CERT_VerifyCertName(server_cert, server_hostname);
		if (status != SECSuccess)
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("unable to verify server hostname: %s"),
							  pg_SSLerrmessage(PR_GetError()));

	}

done:
	if (server_hostname)
		PR_Free(server_hostname);

	CERT_DestroyCertificate(server_cert);
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
	 * This really shouldn't happen, as we've the the PGconn object as our
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
	 * validation, figure out which we dont want for require/prefer etc.
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

void *
PQgetssl(PGconn *conn)
{
	/*
	 * Always return NULL as this is legacy and defined to be equal to
	 * PQsslStruct(conn, "OpenSSL"); This should ideally trigger a logged
	 * warning somewhere as it's nonsensical to run in a non-OpenSSL build,
	 * but the color of said bikeshed hasn't yet been determined.
	 */
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

static bool
cert_database_has_CA(PGconn *conn)
{
	CERTCertList *certificates;
	bool		hasCA;

	/*
	 * If the certificate database has a password we must provide it, since
	 * this API doesn't invoke the standard password callback.
	 */
	if (has_password)
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
PQssl_passwd_cb(PK11SlotInfo * slot, PRBool retry, void *arg)
{
	has_password = true;

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
