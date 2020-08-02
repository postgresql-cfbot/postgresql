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
#include <prio.h>
#include <ssl.h>
#include <sslerr.h>
#include <secerr.h>
#include <sslproto.h>
#include <prtypes.h>
#include <pk11pub.h>
#include <secitem.h>
#include <secport.h>
#include <secder.h>
#include <certdb.h>
#include <base64.h>
#include <cert.h>
#include <prerror.h>
#include <keyhi.h>

typedef struct
{
	enum
	{
		PW_NONE = 0,
		PW_FROMFILE = 1,
		PW_PLAINTEXT = 2,
		PW_EXTERNAL = 3
	} source;
	char	   *data;
}			secuPWData;

/*
 * Ensure that the colliding definitions match, else throw an error. In case
 * NSPR has removed the definition for some reasone, make sure to put ours
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

static PRDescIdentity pr_id;

static PRIOMethods pr_iomethods;
static NSSInitContext * nss_context = NULL;
static SSLVersionRange desired_sslver;

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
/* Utility functions */
static PRFileDesc * init_iolayer(Port *port, int loglevel);
static uint16 ssl_protocol_version_to_nss(int v, const char *guc_name);

static char *pg_SSLerrmessage(PRErrorCode errcode);
static char *ssl_protocol_version_to_string(int v);
static SECStatus pg_cert_auth_handler(void *arg, PRFileDesc * fd,
									  PRBool checksig, PRBool isServer);
static SECStatus pg_bad_cert_handler(void *arg, PRFileDesc * fd);

/* ------------------------------------------------------------ */
/*						 Public interface						*/
/* ------------------------------------------------------------ */

static char *
ssl_passphrase_callback(PK11SlotInfo * slot, PRBool retry, void *arg)
{
	return pstrdup("");
}

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

	/*
	 * Set up the connection cache for multi-processing application behavior.
	 * If we are in ServerStart then we initialize the cache. If the server is
	 * already started, we inherit the cache such that it can be used for
	 * connections. Calling SSL_ConfigMPServerSIDCache sets an environment
	 * variable which contains enough information for the forked child to know
	 * how to access it.  Passing NULL to SSL_InheritMPServerSIDCache will
	 * make the forked child look it up by the default name SSL_INHERITANCE,
	 * if env vars aren't inherited then the contents of the variable can be
	 * passed instead.
	 */
	if (isServerStart)
	{
		/*
		 * SSLv2 and SSLv3 are disabled in this TLS backend, but when setting
		 * up the required session cache for NSS we still must supply timeout
		 * values for v2 and The minimum allowed value for both is 5 seconds,
		 * so opt for that in both cases (the defaults being 100 seconds and
		 * 24 hours).
		 *
		 * Passing NULL as the directory for the session cache will default to
		 * using /tmp on UNIX and \\temp on Windows.  Deciding if we want to
		 * keep closer control on this directory is left as a TODO.
		 */
		status = SSL_ConfigMPServerSIDCache(MaxConnections, 5, 5, NULL);
		if (status != SECSuccess)
			ereport(FATAL,
					(errmsg("unable to set up TLS connection cache: %s",
							pg_SSLerrmessage(PR_GetError()))));

	}
	else
	{
		status = SSL_InheritMPServerSIDCache(NULL);
		if (status != SECSuccess)
		{
			ereport(LOG,
					(errmsg("unable to connect to TLS connection cache: %s",
							pg_SSLerrmessage(PR_GetError()))));
			return -1;
		}
	}

	if (!ssl_database || strlen(ssl_database) == 0)
	{
		ereport(isServerStart ? FATAL : LOG,
				(errmsg("no certificate database specified")));
		goto error;
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
		goto error;
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
			goto error;
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
			goto error;
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
			goto error;
		}
	}

	return 0;
error:
	return -1;
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
	secuPWData	pwdata = {PW_NONE, 0};	/* TODO: This is a bogus callback */
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
	 * released sometime in 1998.
	 */
	PR_Init(PR_USER_THREAD, PR_PRIORITY_NORMAL, 0 /* maxPTDs */ );

	/*
	 * The certificate path (configdir) must contain a valid NSS database. If
	 * the certificate path isn't a valid directory, NSS will fall back on the
	 * system certificate database. If the certificate path is a directory but
	 * is empty then the initialization will fail. On the client side this can
	 * be allowed for any sslmode but the verify-xxx ones.
	 * https://bugzilla.redhat.com/show_bug.cgi?id=728562 For the server side
	 * we wont allow this to fail however, as we require the certificate and
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
	 * Set the passphrase callback which will be used both to obtain the
	 * passphrase from the user, as well as by NSS to obtain the phrase
	 * repeatedly.
	 *
	 * TODO: Figure this out - do note that we are setting another password
	 * callback below for cert/key as well. Need to make sense of all these.
	 */
	PK11_SetPasswordFunc(ssl_passphrase_callback);

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
	 * Configure the allowed cipher. If there are no user preferred suites,
	 * set the domestic policy. TODO: while this code works, the set of
	 * ciphers which can be set and still end up with a working socket is
	 * woefully underdocumented for anything more recent than SSLv3 (the code
	 * for TLS actually calls ssl3 functions under the hood for
	 * SSL_CipherPrefSet), so it's unclear if this is helpful or not. Using
	 * the policies works, but may be too coarsely grained.
	 *
	 * Another TODO: The SSL_ImplementedCiphers table returned with calling
	 * SSL_GetImplementedCiphers is sorted in server preference order. Sorting
	 * SSLCipherSuites according to the order of the ciphers therein could be
	 * a way to implement ssl_prefer_server_ciphers - if we at all want to use
	 * cipher selection for NSS like how we do it for OpenSSL that is.
	 */

	/*
	 * If no ciphers are specified, we use the domestic policy
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
	layer = init_iolayer(port, ERROR);
	if (!layer)
		goto error;

	/* Store the Port as private data available in callbacks */
	layer->secret = (void *) port;

	if (PR_PushIOLayer(pr_fd, PR_TOP_IO_LAYER, layer) != PR_SUCCESS)
	{
		PR_Close(layer);
		ereport(ERROR,
				(errmsg("unable to push IO layer")));
	}

	/* TODO: set the postgres password callback param as callback function */
	server_cert = PK11_FindCertFromNickname(ssl_cert_file, &pwdata /* password callback */ );
	if (!server_cert)
		ereport(ERROR,
				(errmsg("unable to find certificate for \"%s\": %s",
						ssl_cert_file, pg_SSLerrmessage(PR_GetError()))));

	/* TODO: set the postgres password callback param as callback function */
	private_key = PK11_FindKeyByAnyCert(server_cert, &pwdata /* password callback */ );
	if (!private_key)
		ereport(ERROR,
				(errmsg("unable to find private key for \"%s\": %s",
						ssl_cert_file, pg_SSLerrmessage(PR_GetError()))));

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
	 * certificate and key.
	 */
	status = SSL_ConfigSecureServer(model, server_cert, private_key, kt_rsa);
	if (status != SECSuccess)
		ereport(ERROR,
				(errmsg("unable to configure secure server: %s",
						pg_SSLerrmessage(PR_GetError()))));
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

		/* XXX: This logic seems potentially bogus? */
		if (err == PR_WOULD_BLOCK_ERROR)
			*waitfor = WL_SOCKET_READABLE;
		else
			*waitfor = WL_SOCKET_WRITEABLE;
	}

	return n_read;
}

ssize_t
be_tls_write(Port *port, void *ptr, size_t len, int *waitfor)
{
	ssize_t		n_write;
	PRErrorCode err;

	n_write = PR_Send(port->pr_fd, ptr, len, 0, PR_INTERVAL_NO_WAIT);

	if (n_write < 0)
	{
		err = PR_GetError();

		if (err == PR_WOULD_BLOCK_ERROR)
			*waitfor = WL_SOCKET_WRITEABLE;
		else
			*waitfor = WL_SOCKET_READABLE;
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

/* ------------------------------------------------------------ */
/*						Internal functions						*/
/* ------------------------------------------------------------ */

static PRInt32
pg_ssl_read(PRFileDesc * fd, void *buf, PRInt32 amount, PRIntn flags,
			PRIntervalTime timeout)
{
	PRRecvFN	read_fn;
	PRInt32		n_read;

	read_fn = fd->lower->methods->recv;
	n_read = read_fn(fd->lower, buf, amount, flags, timeout);

	return n_read;
}

static PRInt32
pg_ssl_write(PRFileDesc * fd, const void *buf, PRInt32 amount, PRIntn flags,
			 PRIntervalTime timeout)
{
	PRSendFN	send_fn;
	PRInt32		n_write;

	send_fn = fd->lower->methods->send;
	n_write = send_fn(fd->lower, buf, amount, flags, timeout);

	return n_write;
}

static PRFileDesc *
init_iolayer(Port *port, int loglevel)
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

	/*
	 * Each IO layer must be identified by a unique name, where uniqueness is
	 * per connection. Each connection in a postgres cluster can generate the
	 * identity from the same string as they will create their IO layers on
	 * different sockets. Only one layer per socket can have the same name.
	 */
	pr_id = PR_GetUniqueIdentity("PostgreSQL");
	if (pr_id == PR_INVALID_IO_LAYER)
	{
		ereport(loglevel,
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
		ereport(loglevel,
				(errmsg("unable to create NSS I/O layer")));
		return NULL;
	}

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
	char		error[128];
	int			ret;

	/* TODO: this should perhaps use a StringInfo instead.. */
	ret = pg_snprintf(error, sizeof(error), "%s (%s)",
					  PR_ErrorToString(errcode, PR_LANGUAGE_I_DEFAULT),
					  PR_ErrorToName(errcode));
	if (ret)
		return pstrdup(error);

	return pstrdup(_("unknown TLS error"));
}
