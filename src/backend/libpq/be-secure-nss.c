/*-------------------------------------------------------------------------
 *
 * be-secure-nss.c
 *	  functions for supporting NSS as a TLS backend
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure-nss.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>

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
#include <nspr/prio.h>
#include <nspr/prmem.h>
#include <nspr/prtypes.h>

#include <nss/nss.h>
#include <nss/base64.h>
#include <nss/cert.h>
#include <nss/certdb.h>
#include <nss/hasht.h>
#include <nss/keyhi.h>
#include <nss/pk11pub.h>
#include <nss/secder.h>
#include <nss/secerr.h>
#include <nss/secitem.h>
#include <nss/secoidt.h>
#include <nss/secport.h>
#include <nss/ssl.h>
#include <nss/sslerr.h>
#include <nss/sslproto.h>

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

#include "common/pg_nss.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "nodes/pg_list.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"


/* default init hook can be overridden by a shared library */
static void default_nss_tls_init(bool isServerStart);
nss_tls_init_hook_type nss_tls_init_hook = default_nss_tls_init;

static PRDescIdentity pr_id;

static PRIOMethods pr_iomethods;
static NSSInitContext * nss_context = NULL;
static SSLVersionRange desired_sslver;

static char *external_ssl_passphrase_cb(PK11SlotInfo *slot, PRBool retry, void *arg);
static bool dummy_ssl_passwd_cb_called = false;
static bool ssl_is_server_start;

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

/* NSS IO layer callback overrides */
static PRInt32 pg_ssl_read(PRFileDesc * fd, void *buf, PRInt32 amount,
						   PRIntn flags, PRIntervalTime timeout);
static PRInt32 pg_ssl_write(PRFileDesc * fd, const void *buf, PRInt32 amount,
							PRIntn flags, PRIntervalTime timeout);
static PRStatus pg_ssl_close(PRFileDesc * fd);
/* Utility functions */
static PRFileDesc * init_iolayer(Port *port);
static uint16 ssl_protocol_version_to_nss(int v, const char *guc_name);

static char *pg_SSLerrmessage(PRErrorCode errcode);
static char *ssl_protocol_version_to_string(int v);
static SECStatus pg_cert_auth_handler(void *arg, PRFileDesc * fd,
									  PRBool checksig, PRBool isServer);
static SECStatus pg_bad_cert_handler(void *arg, PRFileDesc * fd);
static char *dummy_ssl_passphrase_cb(PK11SlotInfo * slot, PRBool retry, void *arg);

/* ------------------------------------------------------------ */
/*						 Public interface						*/
/* ------------------------------------------------------------ */

/*
 * be_tls_init
 *			Initialize the nss TLS library in the postmaster
 *
 * The majority of the setup needs to happen in be_tls_open_server since the
 * NSPR initialization must happen after the forking of the backend. We could
 * potentially move some parts in under !isServerStart, but so far this is the
 * separation chosen.
 */
int
be_tls_init(bool isServerStart)
{
	SECStatus	status;
	SSLVersionRange supported_sslver;

	status = SSL_ConfigServerSessionIDCacheWithOpt(0, 0, NULL, 1, 0, 0, PR_FALSE);
	if (status != SECSuccess)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg("unable to connect to TLS connection cache: %s",
						pg_SSLerrmessage(PR_GetError()))));
		return -1;
	}

	if (!ssl_database || strlen(ssl_database) == 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg("no certificate database specified")));
		return -1;
	}

	/*
	 * We check for the desired TLS version range here, even though we cannot
	 * set it until be_open_server such that we can be compatible with how the
	 * OpenSSL backend reports errors for incompatible range configurations.
	 * Set either the default supported TLS version range, or the configured
	 * range from ssl_min_protocol_version and ssl_max_protocol version. In
	 * case the user hasn't defined the maximum allowed version we fall back
	 * to the highest version TLS that the library supports.
	 */
	if (SSL_VersionRangeGetSupported(ssl_variant_stream, &supported_sslver) != SECSuccess)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg("unable to get default protocol support from NSS")));
		return -1;
	}

	/*
	 * Set the fallback versions for the TLS protocol version range to a
	 * combination of our minimal requirement and the library maximum.
	 */
	desired_sslver.min = SSL_LIBRARY_VERSION_TLS_1_0;
	desired_sslver.max = supported_sslver.max;

	if (ssl_min_protocol_version)
	{
		int			ver = ssl_protocol_version_to_nss(ssl_min_protocol_version,
													  "ssl_min_protocol_version");

		if (ver == -1)
		{
			ereport(isServerStart ? FATAL : LOG,
					(errmsg("\"%s\" setting \"%s\" not supported by this build",
							"ssl_min_protocol_version",
							GetConfigOption("ssl_min_protocol_version",
											false, false))));
			return -1;
		}

		if (ver > 0)
			desired_sslver.min = ver;
	}

	if (ssl_max_protocol_version)
	{
		int			ver = ssl_protocol_version_to_nss(ssl_max_protocol_version,
													  "ssl_max_protocol_version");

		if (ver == -1)
		{
			ereport(isServerStart ? FATAL : LOG,
					(errmsg("\"%s\" setting \"%s\" not supported by this build",
							"ssl_max_protocol_version",
							GetConfigOption("ssl_max_protocol_version",
											false, false))));
			return -1;
		}
		if (ver > 0)
			desired_sslver.max = ver;

		if (ver < desired_sslver.min)
		{
			ereport(isServerStart ? FATAL : LOG,
					(errmsg("could not set SSL protocol version range"),
					 errdetail("\"%s\" cannot be higher than \"%s\"",
							   "ssl_min_protocol_version",
							   "ssl_max_protocol_version")));
			return -1;
		}
	}

	/*
	 * Set the passphrase callback which will be used both to obtain the
	 * passphrase from the user, as well as by NSS to obtain the phrase
	 * repeatedly.
	 */
	ssl_is_server_start = isServerStart;
	(*nss_tls_init_hook) (isServerStart);

	return 0;
}

int
be_tls_open_server(Port *port)
{
	SECStatus	status;
	PRFileDesc *model;
	PRFileDesc *pr_fd;
	PRFileDesc *layer;
	CERTCertificate *server_cert;
	SECKEYPrivateKey *private_key;
	CERTSignedCrl *crl;
	SECItem		crlname;
	char	   *cert_database;
	NSSInitParameters params;

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
	 * released sometime in 1998. In current versions of NSPR all parameters
	 * are ignored.
	 */
	PR_Init(PR_USER_THREAD, PR_PRIORITY_NORMAL, 0 /* maxPTDs */ );

	/*
	 * The certificate path (configdir) must contain a valid NSS database. If
	 * the certificate path isn't a valid directory, NSS will fall back on the
	 * system certificate database. If the certificate path is a directory but
	 * is empty then the initialization will fail. On the client side this can
	 * be allowed for any sslmode but the verify-xxx ones.
	 * https://bugzilla.redhat.com/show_bug.cgi?id=728562 For the server side
	 * we won't allow this to fail however, as we require the certificate and
	 * key to exist.
	 *
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
	memset(&params, '\0', sizeof(params));
	params.length = sizeof(params);

	if (!ssl_database || strlen(ssl_database) == 0)
		ereport(FATAL,
				(errmsg("no certificate database specified")));

	cert_database = psprintf("sql:%s", ssl_database);
	nss_context = NSS_InitContext(cert_database, "", "", "",
								  &params,
								  NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
	pfree(cert_database);

	if (!nss_context)
		ereport(FATAL,
				(errmsg("unable to read certificate database \"%s\": %s",
						ssl_database, pg_SSLerrmessage(PR_GetError()))));

	/*
	 * Import the already opened socket as we don't want to use NSPR functions
	 * for opening the network socket due to how the PostgreSQL protocol works
	 * with TLS connections. This function is not part of the NSPR public API,
	 * see the comment at the top of the file for the rationale of still using
	 * it.
	 */
	pr_fd = PR_ImportTCPSocket(port->sock);
	if (!pr_fd)
		ereport(ERROR,
				(errmsg("unable to connect to socket")));

	/*
	 * Most of the documentation available, and implementations of, NSS/NSPR
	 * use the PR_NewTCPSocket() function here, which has the drawback that it
	 * can only create IPv4 sockets. Instead use PR_OpenTCPSocket() which
	 * copes with IPv6 as well.
	 *
	 * We use a model filedescriptor here which is a construct in NSPR/NSS in
	 * order to create a configuration template for sockets which can then be
	 * applied to new sockets created. This makes more sense in a server which
	 * accepts multiple connections and want to perform the boilerplate just
	 * once, but it does provide a nice abstraction here as well in that we
	 * can error out early without having performed any operation on the real
	 * socket.
	 */
	model = PR_OpenTCPSocket(port->laddr.addr.ss_family);
	if (!model)
		ereport(ERROR,
				(errmsg("unable to open socket")));

	/*
	 * Convert the NSPR socket to an SSL socket. Ensuring the success of this
	 * operation is critical as NSS SSL_* functions may return SECSuccess on
	 * the socket even though SSL hasn't been enabled, which introduce a risk
	 * of silent downgrades.
	 */
	model = SSL_ImportFD(NULL, model);
	if (!model)
		ereport(ERROR,
				(errmsg("unable to enable TLS on socket")));

	/*
	 * Configure basic settings for the connection over the SSL socket in
	 * order to set it up as a server.
	 */
	if (SSL_OptionSet(model, SSL_SECURITY, PR_TRUE) != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to configure TLS connection")));

	if (SSL_OptionSet(model, SSL_HANDSHAKE_AS_SERVER, PR_TRUE) != SECSuccess ||
		SSL_OptionSet(model, SSL_HANDSHAKE_AS_CLIENT, PR_FALSE) != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to configure TLS connection as server")));

	/*
	 * SSLv2 is disabled by default, and SSLv3 will be excluded from the range
	 * of allowed protocols further down. Since we really don't want these to
	 * ever be enabled, let's use belts and suspenders and explicitly turn
	 * them off as well.
	 */
	SSL_OptionSet(model, SSL_ENABLE_SSL2, PR_FALSE);
	SSL_OptionSet(model, SSL_ENABLE_SSL3, PR_FALSE);

#ifdef SSL_CBC_RANDOM_IV

	/*
	 * Enable protection against the BEAST attack in case the NSS server has
	 * support for that. While SSLv3 is disabled, we may still allow TLSv1
	 * which is affected. The option isn't documented as an SSL option, but as
	 * an NSS environment variable.
	 */
	SSL_OptionSet(model, SSL_CBC_RANDOM_IV, PR_TRUE);
#endif

	/*
	 * Configure the allowed ciphers. If there are no user preferred suites,
	 * set the domestic policy.
	 *
	 * Historically there were different cipher policies based on export (and
	 * import) restrictions: Domestic, Export and France. These are since long
	 * removed with all ciphers being enabled by default. Due to backwards
	 * compatibility, the old API is still used even though all three policies
	 * now do the same thing.
	 *
	 * If SSLCipherSuites define a policy of the user, we set that rather than
	 * enabling all ciphers via NSS_SetDomesticPolicy.
	 *
	 * TODO: while this code works, the set of ciphers which can be set and
	 * still end up with a working socket is woefully underdocumented for
	 * anything more recent than SSLv3 (the code for TLS actually calls ssl3
	 * functions under the hood for SSL_CipherPrefSet), so it's unclear if
	 * this is helpful or not. Using the policies works, but may be too
	 * coarsely grained.
	 *
	 * Another TODO: The SSL_ImplementedCiphers table returned with calling
	 * SSL_GetImplementedCiphers is sorted in server preference order. Sorting
	 * SSLCipherSuites according to the order of the ciphers therein could be
	 * a way to implement ssl_prefer_server_ciphers - if we at all want to use
	 * cipher selection for NSS like how we do it for OpenSSL that is.
	 */

	/*
	 * If no ciphers are specified, enable them all.
	 */
	if (!SSLCipherSuites || strlen(SSLCipherSuites) == 0)
	{
		status = NSS_SetDomesticPolicy();
		if (status != SECSuccess)
			ereport(ERROR,
					(errmsg("unable to set cipher policy: %s",
							pg_SSLerrmessage(PR_GetError()))));
	}
	else
	{
		char	   *ciphers,
				   *c;

		char	   *sep = ":;, ";
		PRUint16	ciphercode;
		const		PRUint16 *nss_ciphers;

		/*
		 * If the user has specified a set of preferred cipher suites we start
		 * by turning off all the existing suites to avoid the risk of down-
		 * grades to a weaker cipher than expected.
		 */
		nss_ciphers = SSL_GetImplementedCiphers();
		for (int i = 0; i < SSL_GetNumImplementedCiphers(); i++)
			SSL_CipherPrefSet(model, nss_ciphers[i], PR_FALSE);

		ciphers = pstrdup(SSLCipherSuites);

		for (c = strtok(ciphers, sep); c; c = strtok(NULL, sep))
		{
			ciphercode = pg_find_cipher(c);
			if (ciphercode != INVALID_CIPHER)
			{
				status = SSL_CipherPrefSet(model, ciphercode, PR_TRUE);
				if (status != SECSuccess)
					ereport(ERROR,
							(errmsg("invalid cipher-suite specified: %s", c)));
			}
		}

		pfree(ciphers);
	}

	if (SSL_VersionRangeSet(model, &desired_sslver) != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to set requested SSL protocol version range")));

	/*
	 * Set up the custom IO layer.
	 */
	layer = init_iolayer(port);
	if (!layer)
		goto error;

	if (PR_PushIOLayer(pr_fd, PR_TOP_IO_LAYER, layer) != PR_SUCCESS)
	{
		PR_Close(layer);
		ereport(ERROR,
				(errmsg("unable to push IO layer")));
	}

	server_cert = PK11_FindCertFromNickname(ssl_cert_file, (void *) port);
	if (!server_cert)
	{
		if (dummy_ssl_passwd_cb_called)
			ereport(ERROR,
					(errmsg("unable to load certificate for \"%s\": %s",
							ssl_cert_file, pg_SSLerrmessage(PR_GetError())),
					 errhint("The certificate requires a password.")));
		else
			ereport(ERROR,
					(errmsg("unable to find certificate for \"%s\": %s",
							ssl_cert_file, pg_SSLerrmessage(PR_GetError()))));
	}

	private_key = PK11_FindKeyByAnyCert(server_cert, (void *) port);
	if (!private_key)
	{
		if (dummy_ssl_passwd_cb_called)
			ereport(ERROR,
					(errmsg("unable to load private key for \"%s\": %s",
							ssl_cert_file, pg_SSLerrmessage(PR_GetError())),
					 errhint("The private key requires a password.")));
		else
			ereport(ERROR,
					(errmsg("unable to find private key for \"%s\": %s",
							ssl_cert_file, pg_SSLerrmessage(PR_GetError()))));
	}

	/*
	 * NSS doesn't use CRL files on disk, so we use the ssl_crl_file guc to
	 * contain the CRL nickname for the current server certificate in the NSS
	 * certificate database. The main difference from the OpenSSL backend is
	 * that NSS will use the CRL regardless, but being able to make sure the
	 * CRL is loaded seems like a good feature.
	 */
	if (ssl_crl_file[0])
	{
		SECITEM_CopyItem(NULL, &crlname, &server_cert->derSubject);
		crl = SEC_FindCrlByName(CERT_GetDefaultCertDB(), &crlname, SEC_CRL_TYPE);
		if (!crl)
			ereport(ERROR,
					(errmsg("specified CRL not found in database")));
		SEC_DestroyCrl(crl);
	}

	/*
	 * Finally we must configure the socket for being a server by setting the
	 * certificate and key. The NULL parameter is an SSLExtraServerCertData
	 * pointer with the final parameter being the size of the extra server
	 * cert data structure pointed to. This is typically only used for
	 * credential delegation.
	 */
	status = SSL_ConfigServerCert(model, server_cert, private_key, NULL, 0);
	if (status != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to configure server for TLS server connections: %s",
						pg_SSLerrmessage(PR_GetError()))));

	ssl_loaded_verify_locations = true;

	/*
	 * At this point, we no longer have use for the certificate and private
	 * key as they have been copied into the context by NSS. Destroy our
	 * copies explicitly to clean out the memory as best we can.
	 */
	CERT_DestroyCertificate(server_cert);
	SECKEY_DestroyPrivateKey(private_key);

	status = SSL_AuthCertificateHook(model, pg_cert_auth_handler, (void *) port);
	if (status != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to install authcert hook: %s",
						pg_SSLerrmessage(PR_GetError()))));
	SSL_BadCertHook(model, pg_bad_cert_handler, (void *) port);
	SSL_OptionSet(model, SSL_REQUEST_CERTIFICATE, PR_TRUE);
	SSL_OptionSet(model, SSL_REQUIRE_CERTIFICATE, PR_FALSE);

	port->pr_fd = SSL_ImportFD(model, pr_fd);
	if (!port->pr_fd)
		ereport(ERROR,
				(errmsg("unable to initialize")));

	PR_Close(model);

	/*
	 * Force a handshake on the next I/O request, the second parameter means
	 * that we are a server, PR_FALSE would indicate being a client. NSPR
	 * requires us to call SSL_ResetHandshake since we imported an already
	 * established socket.
	 */
	status = SSL_ResetHandshake(port->pr_fd, PR_TRUE);
	if (status != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to initiate handshake: %s",
						pg_SSLerrmessage(PR_GetError()))));
	status = SSL_ForceHandshake(port->pr_fd);
	if (status != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to handshake: %s",
						pg_SSLerrmessage(PR_GetError()))));

	port->ssl_in_use = true;
	return 0;

error:
	return 1;
}

ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
	ssize_t		n_read;
	PRErrorCode err;

	n_read = PR_Read(port->pr_fd, ptr, len);

	if (n_read < 0)
	{
		err = PR_GetError();

		if (err == PR_WOULD_BLOCK_ERROR)
		{
			*waitfor = WL_SOCKET_READABLE;
			errno = EWOULDBLOCK;
		}
		else
			errno = ECONNRESET;
	}

	return n_read;
}

ssize_t
be_tls_write(Port *port, void *ptr, size_t len, int *waitfor)
{
	ssize_t		n_write;
	PRErrorCode err;
	PRIntn		flags = 0;

	/*
	 * The flags parameter to PR_Send is no longer used and is, according to
	 * the documentation, required to be zero.
	 */
	n_write = PR_Send(port->pr_fd, ptr, len, flags, PR_INTERVAL_NO_WAIT);

	if (n_write < 0)
	{
		err = PR_GetError();

		if (err == PR_WOULD_BLOCK_ERROR)
		{
			*waitfor = WL_SOCKET_WRITEABLE;
			errno = EWOULDBLOCK;
		}
		else
			errno = ECONNRESET;
	}

	return n_write;
}

void
be_tls_close(Port *port)
{
	if (!port)
		return;

	if (port->peer_cn)
	{
		SSL_InvalidateSession(port->pr_fd);
		pfree(port->peer_cn);
		port->peer_cn = NULL;
	}

	PR_Close(port->pr_fd);
	port->pr_fd = NULL;
	port->ssl_in_use = false;

	if (nss_context)
	{
		NSS_ShutdownContext(nss_context);
		nss_context = NULL;
	}
}

void
be_tls_destroy(void)
{
	/*
	 * It reads a bit odd to clear a session cache when we are destroying the
	 * context altogether, but if the session cache isn't cleared before
	 * shutting down the context it will fail with SEC_ERROR_BUSY.
	 */
	SSL_ClearSessionCache();
}

int
be_tls_get_cipher_bits(Port *port)
{
	SECStatus	status;
	SSLChannelInfo channel;
	SSLCipherSuiteInfo suite;

	status = SSL_GetChannelInfo(port->pr_fd, &channel, sizeof(channel));
	if (status != SECSuccess)
		goto error;

	status = SSL_GetCipherSuiteInfo(channel.cipherSuite, &suite, sizeof(suite));
	if (status != SECSuccess)
		goto error;

	return suite.effectiveKeyBits;

error:
	ereport(WARNING,
			(errmsg("unable to extract TLS session information: %s",
					pg_SSLerrmessage(PR_GetError()))));
	return 0;
}

/*
 * be_tls_get_compression
 *
 * NSS disabled support for TLS compression in version 3.33 and removed the
 * code in a subsequent release. The API for retrieving information about
 * compression as well as enabling it is kept for backwards compatibility, but
 * we don't need to consult it since it was only available for SSLv3 which we
 * don't support.
 *
 * https://bugzilla.mozilla.org/show_bug.cgi?id=1409587
 */
bool
be_tls_get_compression(Port *port)
{
	return false;
}

/*
 * be_tls_get_version
 *
 * Returns the protocol version used for the current connection, or NULL in
 * case of errors.
 */
const char *
be_tls_get_version(Port *port)
{
	SECStatus	status;
	SSLChannelInfo channel;

	status = SSL_GetChannelInfo(port->pr_fd, &channel, sizeof(channel));
	if (status != SECSuccess)
	{
		ereport(WARNING,
				(errmsg("unable to extract TLS session information: %s",
						pg_SSLerrmessage(PR_GetError()))));
		return NULL;
	}

	return ssl_protocol_version_to_string(channel.protocolVersion);
}

const char *
be_tls_get_cipher(Port *port)
{
	SECStatus	status;
	SSLChannelInfo channel;
	SSLCipherSuiteInfo suite;

	status = SSL_GetChannelInfo(port->pr_fd, &channel, sizeof(channel));
	if (status != SECSuccess)
		goto error;

	status = SSL_GetCipherSuiteInfo(channel.cipherSuite, &suite, sizeof(suite));
	if (status != SECSuccess)
		goto error;

	return suite.cipherSuiteName;

error:
	ereport(WARNING,
			(errmsg("unable to extract TLS session information: %s",
					pg_SSLerrmessage(PR_GetError()))));
	return NULL;
}

void
be_tls_get_peer_subject_name(Port *port, char *ptr, size_t len)
{
	CERTCertificate *certificate;

	certificate = SSL_PeerCertificate(port->pr_fd);
	if (certificate)
		strlcpy(ptr, CERT_NameToAscii(&certificate->subject), len);
	else
		ptr[0] = '\0';
}

void
be_tls_get_peer_issuer_name(Port *port, char *ptr, size_t len)
{
	CERTCertificate *certificate;

	certificate = SSL_PeerCertificate(port->pr_fd);
	if (certificate)
		strlcpy(ptr, CERT_NameToAscii(&certificate->issuer), len);
	else
		ptr[0] = '\0';
}

void
be_tls_get_peer_serial(Port *port, char *ptr, size_t len)
{
	CERTCertificate *certificate;

	certificate = SSL_PeerCertificate(port->pr_fd);
	if (certificate)
		snprintf(ptr, len, "%li", DER_GetInteger(&(certificate->serialNumber)));
	else
		ptr[0] = '\0';
}

char *
be_tls_get_certificate_hash(Port *port, size_t *len)
{
	CERTCertificate *certificate;
	SECOidTag	signature_alg;
	SECOidTag	digest_alg;
	int			digest_len;
	const NSSSignatureAlgorithms *candidate;
	SECStatus	status;
	PLArenaPool *arena = NULL;
	SECItem		digest;
	char	   *ret;

	*len = 0;
	certificate = SSL_LocalCertificate(port->pr_fd);
	if (!certificate)
		return NULL;

	signature_alg = SECOID_GetAlgorithmTag(&certificate->signature);

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
		elog(ERROR, "could not find digest for OID '%s'",
			 SECOID_FindOIDTagDescription(signature_alg));

	arena = PORT_NewArena(SEC_ASN1_DEFAULT_ARENA_SIZE);
	digest.data = PORT_ArenaZAlloc(arena, sizeof(unsigned char) * digest_len);
	digest.len = digest_len;

	status = PK11_HashBuf(digest_alg, digest.data,
						  certificate->derCert.data,
						  certificate->derCert.len);

	if (status != SECSuccess)
	{
		PORT_FreeArena(arena, PR_TRUE);
		return NULL;
	}

	ret = palloc(digest.len);
	memcpy(ret, digest.data, digest.len);
	*len = digest_len;
	PORT_FreeArena(arena, PR_TRUE);

	return ret;
}

/* ------------------------------------------------------------ */
/*						Internal functions						*/
/* ------------------------------------------------------------ */

/*
 * default_nss_tls_init
 *
 * The default TLS init hook function which users can override for installing
 * their own passphrase callbacks and similar actions. In case no callback has
 * been configured, or the callback isn't reload capable during a server
 * reload, the dummy callback will be installed.
 *
 * The private data for the callback is set differently depending on how it's
 * invoked. For calls which may invoke the callback deeper in the callstack
 * the private data is set with SSL_SetPKCS11PinArg. When the call is directly
 * invoking the callback, like PK11_FindCertFromNickname, then the private
 * data is passed as a parameter. Setting the data with SSL_SetPKCS11PinArg is
 * thus not required but good practice.
 *
 * NSS doesn't provide a default callback like OpenSSL does, but a callback is
 * required to be set.  The password callback can be installed at any time, but
 * setting the private data with SSL_SetPKCS11PinArg requires a PR Filedesc.
 */
static void
default_nss_tls_init(bool isServerStart)
{
	/*
	 * No user-defined callback has been configured, install the dummy call-
	 * back since we must set something.
	 */
	if (!ssl_passphrase_command[0])
		PK11_SetPasswordFunc(dummy_ssl_passphrase_cb);
	else
	{
		/*
		 * There is a user-defined callback, set it unless we are in a restart
		 * and cannot handle restarts due to an interactive callback.
		 */
		if (isServerStart)
			PK11_SetPasswordFunc(external_ssl_passphrase_cb);
		else
		{
			if (ssl_passphrase_command_supports_reload)
				PK11_SetPasswordFunc(external_ssl_passphrase_cb);
			else
				PK11_SetPasswordFunc(dummy_ssl_passphrase_cb);
		}
	}
}

/*
 * external_ssl_passphrase_cb
 *
 * Runs the callback configured by ssl_passphrase_command and returns the
 * captured password back to NSS.
 */
static char *
external_ssl_passphrase_cb(PK11SlotInfo *slot, PRBool retry, void *arg)
{
	/*
	 * NSS use a hardcoded 256 byte buffer for reading the password so set the
	 * same limit for our callback buffer.
	 */
	char		buf[256];
	int			len;
	char	   *password = NULL;
	char	   *prompt;

	/*
	 * Since there is no password callback in NSS when the server starts up,
	 * it makes little sense to create an interactive callback. Thus, if this
	 * is a retry attempt then give up immediately.
	 */
	if (retry)
		return NULL;

	/*
	 * Construct the same prompt that NSS uses internally even though it is
	 * unlikely to serve much purpose, but we must set a prompt so we might as
	 * well do it right.
	 */
	prompt = psprintf("Enter Password or Pin for \"%s\":",
					  PK11_GetTokenName(slot));

	len = run_ssl_passphrase_command(prompt, ssl_is_server_start, buf, sizeof(buf));
	pfree(prompt);

	if (!len)
		return NULL;

	/*
	 * At least one byte with password content was returned, and NSS requires
	 * that we return it allocated in NSS controlled memory. If we fail to
	 * allocate then abort without passing back NULL and bubble up the error
	 * on the PG side.
	 */
	password = (char *) PR_Malloc(len + 1);
	if (!password)
	{
		explicit_bzero(buf, sizeof(buf));
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	strlcpy(password, buf, sizeof(password));
	explicit_bzero(buf, sizeof(buf));

	return password;
}

/*
 * dummy_ssl_passphrase_cb
 *
 * Return unsuccessful if we are asked to provide the passphrase for a cert or
 * key, without having a passphrase callback installed.
 */
static char *
dummy_ssl_passphrase_cb(PK11SlotInfo *slot, PRBool retry, void *arg)
{
	dummy_ssl_passwd_cb_called = true;
	return NULL;
}

static SECStatus
pg_bad_cert_handler(void *arg, PRFileDesc * fd)
{
	Port	   *port = (Port *) arg;

	port->peer_cert_valid = false;
	return SECFailure;
}

static SECStatus
pg_cert_auth_handler(void *arg, PRFileDesc * fd, PRBool checksig, PRBool isServer)
{
	SECStatus	status;
	Port	   *port = (Port *) arg;
	CERTCertificate *cert;
	char	   *peer_cn;
	int			len;

	status = SSL_AuthCertificate(CERT_GetDefaultCertDB(), port->pr_fd, checksig, PR_TRUE);
	if (status == SECSuccess)
	{
		cert = SSL_PeerCertificate(port->pr_fd);
		len = strlen(cert->subjectName);
		peer_cn = MemoryContextAllocZero(TopMemoryContext, len + 1);

		/*
		 * Skip over the key= portion of the key=value containing the peer CN.
		 */
		if (strncmp(cert->subjectName, "CN=", 3) == 0)
			strlcpy(peer_cn, cert->subjectName + strlen("CN="), len + 1);
		else
			strlcpy(peer_cn, cert->subjectName, len + 1);
		CERT_DestroyCertificate(cert);

		port->peer_cn = peer_cn;
		port->peer_cert_valid = true;
	}

	return status;
}

static PRInt32
pg_ssl_read(PRFileDesc *fd, void *buf, PRInt32 amount, PRIntn flags,
			PRIntervalTime timeout)
{
	PRRecvFN	read_fn;
	PRInt32		n_read;

	read_fn = fd->lower->methods->recv;
	n_read = read_fn(fd->lower, buf, amount, flags, timeout);

	return n_read;
}

static PRInt32
pg_ssl_write(PRFileDesc *fd, const void *buf, PRInt32 amount, PRIntn flags,
			 PRIntervalTime timeout)
{
	PRSendFN	send_fn;
	PRInt32		n_write;

	send_fn = fd->lower->methods->send;
	n_write = send_fn(fd->lower, buf, amount, flags, timeout);

	return n_write;
}

static PRStatus
pg_ssl_close(PRFileDesc *fd)
{
	/*
	 * Disconnect our private Port from the fd before closing out the stack.
	 * (Debug builds of NSPR will assert if we do not.)
	 */
	fd->secret = NULL;
	return PR_GetDefaultIOMethods()->close(fd);
}

static PRFileDesc *
init_iolayer(Port *port)
{
	const		PRIOMethods *default_methods;
	PRFileDesc *layer;

	/*
	 * Start by initializing our layer with all the default methods so that we
	 * can selectively override the ones we want while still ensuring that we
	 * have a complete layer specification.
	 */
	default_methods = PR_GetDefaultIOMethods();
	memcpy(&pr_iomethods, default_methods, sizeof(PRIOMethods));

	pr_iomethods.recv = pg_ssl_read;
	pr_iomethods.send = pg_ssl_write;
	pr_iomethods.close = pg_ssl_close;

	/*
	 * Each IO layer must be identified by a unique name, where uniqueness is
	 * per connection. Each connection in a postgres cluster can generate the
	 * identity from the same string as they will create their IO layers on
	 * different sockets. Only one layer per socket can have the same name.
	 */
	pr_id = PR_GetUniqueIdentity("PostgreSQL Server");
	if (pr_id == PR_INVALID_IO_LAYER)
	{
		ereport(ERROR,
				(errmsg("out of memory when setting up TLS connection")));
		return NULL;
	}

	/*
	 * Create the actual IO layer as a stub such that it can be pushed onto
	 * the layer stack. The step via a stub is required as we define custom
	 * callbacks.
	 */
	layer = PR_CreateIOLayerStub(pr_id, &pr_iomethods);
	if (!layer)
	{
		ereport(ERROR,
				(errmsg("unable to create NSS I/O layer")));
		return NULL;
	}

	/* Store the Port as private data available in callbacks */
	layer->secret = (void *) port;

	return layer;
}

static char *
ssl_protocol_version_to_string(int v)
{
	switch (v)
	{
			/* SSL v2 and v3 are not supported */
		case SSL_LIBRARY_VERSION_2:
		case SSL_LIBRARY_VERSION_3_0:
			Assert(false);
			break;

		case SSL_LIBRARY_VERSION_TLS_1_0:
			return pstrdup("TLSv1.0");
		case SSL_LIBRARY_VERSION_TLS_1_1:
			return pstrdup("TLSv1.1");
		case SSL_LIBRARY_VERSION_TLS_1_2:
			return pstrdup("TLSv1.2");
		case SSL_LIBRARY_VERSION_TLS_1_3:
			return pstrdup("TLSv1.3");
	}

	return pstrdup("unknown");
}


/*
 * ssl_protocol_version_to_nss
 *			Translate PostgreSQL TLS version to NSS version
 *
 * Returns zero in case the requested TLS version is undefined (PG_ANY) and
 * should be set by the caller, or -1 on failure.
 */
static uint16
ssl_protocol_version_to_nss(int v, const char *guc_name)
{
	switch (v)
	{
			/*
			 * There is no SSL_LIBRARY_ macro defined in NSS with the value
			 * zero, so we use this to signal the caller that the highest
			 * useful version should be set on the connection.
			 */
		case PG_TLS_ANY:
			return 0;

			/*
			 * No guard is required here as there are no versions of NSS
			 * without support for TLS1.
			 */
		case PG_TLS1_VERSION:
			return SSL_LIBRARY_VERSION_TLS_1_0;
		case PG_TLS1_1_VERSION:
#ifdef SSL_LIBRARY_VERSION_TLS_1_1
			return SSL_LIBRARY_VERSION_TLS_1_1;
#else
			break;
#endif
		case PG_TLS1_2_VERSION:
#ifdef SSL_LIBRARY_VERSION_TLS_1_2
			return SSL_LIBRARY_VERSION_TLS_1_2;
#else
			break;
#endif
		case PG_TLS1_3_VERSION:
#ifdef SSL_LIBRARY_VERSION_TLS_1_3
			return SSL_LIBRARY_VERSION_TLS_1_3;
#else
			break;
#endif
		default:
			break;
	}

	return -1;
}

/*
 * pg_SSLerrmessage
 *		Create and return a human readable error message given
 *		the specified error code
 *
 * PR_ErrorToName only converts the enum identifier of the error to string,
 * but that can be quite useful for debugging (and in case PR_ErrorToString is
 * unable to render a message then we at least have something).
 */
static char *
pg_SSLerrmessage(PRErrorCode errcode)
{
	return psprintf("%s (%s)",
					PR_ErrorToString(errcode, PR_LANGUAGE_I_DEFAULT),
					PR_ErrorToName(errcode));
}
