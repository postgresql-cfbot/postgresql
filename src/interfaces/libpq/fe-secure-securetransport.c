/*-------------------------------------------------------------------------
 *
 * fe-secure-securetransport.c
 *	  Apple Secure Transport support
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-secure-securetransport.c
 *
 * NOTES
 *	  Unlike the OpenSSL support there is no shared state between connections
 *	  so there is no special handling for ENABLE_THREAD_SAFETY.
 *
 *	  There are a lot of functions (mostly the Core Foundation CF* family) that
 *	  pass NULL as the first parameter. This is because they allow for a custom
 *	  allocator to be used for memory allocations which is referenced with the
 *	  first parameter. We are using the standard allocator however, and that
 *	  means passing NULL all the time. Defining a suitably named preprocessor
 *	  macro would aid readablitity in case this is confusing (and/or ugly).
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <signal.h>
#include <fcntl.h>
#include <ctype.h>

#include "libpq-fe.h"
#include "fe-auth.h"
#include "libpq-int.h"

#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>

#include <sys/stat.h>

#define Size pg_Size
#define uint64 pg_uint64
#include <Security/Security.h>
#include <Security/SecureTransport.h>
#include <CoreFoundation/CoreFoundation.h>
#include "common/securetransport.h"
#undef uint64
#undef Size
#define pg_uint64 uint64
#define pg_Size Size

#define KC_PREFIX		"keychain:"
#define KC_PREFIX_LEN	(strlen("keychain:"))

/*
 * Private API call used in the Webkit code for creating an identity from a
 * certificate with a key. While stable and used in many open source projects
 * it should be replaced with a published API call since private APIs aren't
 * subject to the same deprecation rules. Could potentially be replaced by
 * using SecIdentityCreateWithCertificate() ?
 */
extern SecIdentityRef SecIdentityCreate(CFAllocatorRef allocator,
										SecCertificateRef certificate,
										SecKeyRef privateKey);

static char * pg_SSLerrmessage(OSStatus errcode);
static void pg_SSLerrfree(char *err_buf);
static int pg_SSLsessionstate(PGconn *conn, char *msg, size_t len);

static OSStatus pg_SSLSocketRead(SSLConnectionRef conn, void *data,
							  size_t *len);
static OSStatus pg_SSLSocketWrite(SSLConnectionRef conn, const void *data,
							   size_t *len);
static OSStatus pg_SSLOpenClient(PGconn *conn);
static OSStatus pg_SSLLoadCertificate(PGconn *conn, CFArrayRef *cert_array,
								   CFArrayRef *key_array,
								   CFArrayRef *rootcert_array);

static OSStatus import_certificate_keychain(const char *common_name,
											SecCertificateRef *certificate,
											CFArrayRef keychains,
											char *hostname);
static OSStatus import_identity_keychain(const char *common_name,
										 SecIdentityRef *identity,
										 CFArrayRef keychains);
static OSStatus import_pem(const char *path, char *passphrase,
						   CFArrayRef *cert_arr);

/* ------------------------------------------------------------ */
/*						 Public interface						*/
/* ------------------------------------------------------------ */


/*
 * pgtls_init_library
 *
 * Exported function to allow application to tell us it's already initialized
 * the SSL library and/or libcrypto. Since the current implementation only
 * allow do_crypto be set for the OpenSSL backend, this is a no-op for Secure
 * Transport.
 */
void
pgtls_init_library(bool do_ssl, int do_crypto)
{
	return;
}

/*
 * pgtls_open_client
 *		Begin, or continue, negotiating a secure session.
 */
PostgresPollingStatusType
pgtls_open_client(PGconn *conn)
{
	OSStatus		open_status;
	CFArrayRef		certificate = NULL;
	CFArrayRef		key = NULL;
	CFArrayRef		rootcert = NULL;

	/*
	 * There is no API to load CRL lists in Secure Transport, they can however
	 * be imported into a Keychain with the commandline application "certtool".
	 * For libpq to use them, the certificate/key and root certificate needs to
	 * be using an identity in a Keychain into which the CRL have been
	 * imported. That needs to be documented.
	 */
	if (conn->sslcrl && strlen(conn->sslcrl) > 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
			   libpq_gettext("CRL files are not supported with Secure Transport\n"));
		return PGRES_POLLING_FAILED;
	}

	/*
	 * If the SSL context hasn't been set up then initiate it, else continue
	 * with handshake
	 */
	if (conn->ssl == NULL)
	{
		conn->ssl_key_bits = 0;
		conn->ssl_buffered = 0;
		conn->st_rootcert = NULL;

		conn->ssl = (void *) SSLCreateContext(NULL, kSSLClientSide, kSSLStreamType);
		if (!conn->ssl)
		{
			printfPQExpBuffer(&conn->errorMessage,
				   libpq_gettext("could not create SSL context\n"));
			return PGRES_POLLING_FAILED;
		}

		open_status = SSLSetProtocolVersionMin((SSLContextRef) conn->ssl, kTLSProtocol12);
		if (open_status != noErr)
			goto error;

		open_status = SSLSetConnection((SSLContextRef) conn->ssl, conn);
		if (open_status != noErr)
			goto error;

		/*
		 * Set the low level functions for reading and writing off a socket
		 */
		open_status = SSLSetIOFuncs((SSLContextRef) conn->ssl, pg_SSLSocketRead, pg_SSLSocketWrite);
		if (open_status != noErr)
			goto error;

		/*
		 * Load client certificate, private key, and trusted CA certs. The
		 * conn->errorMessage will be populated by the certificate loading
		 * so we can return without altering it in case of error.
		 */
		if (pg_SSLLoadCertificate(conn, &certificate, &key, &rootcert) != noErr)
		{
			pgtls_close(conn);
			return PGRES_POLLING_FAILED;
		}

		/*
		 * If we are asked to verify the peer hostname, set it as a requirement
		 * on the connection. This must be set before calling SSLHandshake().
		 */
		if (strcmp(conn->sslmode, "verify-full") == 0)
		{
			/* If we are asked to verify a hostname we don't have, error out */
			if (!conn->pghost)
			{
				pgtls_close(conn);
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("hostname missing for verify-full\n"));
				return PGRES_POLLING_FAILED;
			}

			open_status = SSLSetPeerDomainName((SSLContextRef) conn->ssl, conn->pghost, strlen(conn->pghost));
			if (open_status != noErr)
			{
				char *err_msg = pg_SSLerrmessage(open_status);

				pgtls_close(conn);
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("unable to set peer hostname: %s\n"),
												err_msg);
				return PGRES_POLLING_FAILED;
			}
		}
	}

	/*
	 * Perform handshake
	 */
	open_status = pg_SSLOpenClient(conn);
	if (open_status == noErr)
	{
		conn->ssl_in_use = true;
		return PGRES_POLLING_OK;
	}

error:
	if (open_status != noErr)
	{
		char *err_msg = pg_SSLerrmessage(open_status);
		if (conn->errorMessage.len > 0)
			appendPQExpBuffer(&conn->errorMessage,
							  libpq_gettext(", ssl error: %s\n"), err_msg);
		else
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("could not establish SSL connection: %s\n"),
									err_msg);
		pg_SSLerrfree(err_msg);

		pgtls_close(conn);
	}

	return PGRES_POLLING_FAILED;
}

/*
 * pg_SSLOpenClient
 *		Validates remote certificate and performs handshake.
 *
 * If the user has supplied a root certificate we add that to the chain here
 * before initiating validation. The caller is responsible for invoking error
 * logging in the case of errors returned.
 */
static OSStatus
pg_SSLOpenClient(PGconn *conn)
{
	OSStatus			status;
	SecTrustRef			trust = NULL;
	SecTrustResultType	trust_eval = 0;
	bool				trusted = false;
	bool				only_anchor = true;

	SSLSetSessionOption((SSLContextRef) conn->ssl,
						kSSLSessionOptionBreakOnServerAuth, true);

	/*
	 * Call SSLHandshake until we get another response than errSSLWouldBlock.
	 * Busy-waiting is pretty silly, but what is commonly used for handshakes
	 * in Secure Transport. Setting an upper bound on retries should be done
	 * though, and perhaps a small timeout to play nice.
	 */
	do
	{
		status = SSLHandshake((SSLContextRef) conn->ssl);
		/* busy-wait loop */
	}
	while (status == errSSLWouldBlock || status == -1);

	if (status != errSSLServerAuthCompleted)
		return status;

	/*
	 * Get peer server certificate and validate it. SSLCopyPeerTrust() is not
	 * supposed to return a NULL trust on noErr but have been reported to do
	 * in the past so add a belts-and-suspenders check
	 */
	status = SSLCopyPeerTrust((SSLContextRef) conn->ssl, &trust);
	if (status != noErr || trust == NULL)
		return (trust == noErr ? errSecInternalError : status);

	/*
	 * If we have our own root certificate configured then add it to the chain
	 * of trust and specify that it should be trusted.
	 */
	if (conn->st_rootcert)
	{
		status = SecTrustSetAnchorCertificates(trust,
											   (CFArrayRef) conn->st_rootcert);
		if (status != noErr)
			return status;

		/* We have a trusted local root cert, trust more than anchor */
		only_anchor = false;
	}

	status = SecTrustSetAnchorCertificatesOnly(trust, only_anchor);
	if (status != noErr)
		return status;

	status = SecTrustEvaluate(trust, &trust_eval);
	if (status == errSecSuccess)
	{
		switch (trust_eval)
		{
			/*
			 * If 'Unspecified' then a valid anchor certificate was verified
			 * without encountering any explicit user trust. If 'Proceed' then
			 * the user has chosen to explicitly trust a certificate in the
			 * chain by clicking "Trust" in the Keychain app. Both cases are
			 * considered valid so trust the chain.
			 */
			case kSecTrustResultUnspecified:
				trusted = true;
				break;
			case kSecTrustResultProceed:
				trusted = true;
				break;

			/*
			 * 'RecoverableTrustFailure' indicates that the certificate was
			 * rejected but might be trusted with minor changes to the eval
			 * context (ignoring expired certificate etc). For the verify
			 * sslmodes there is little to do here, but in require sslmode we
			 * can recover in some cases.
			 */
			case kSecTrustResultRecoverableTrustFailure:
			{
				CFArrayRef 			trust_prop;
				CFDictionaryRef		trust_dict;
				CFStringRef			trust_error;
				const char		   *error;

				/* Assume the error is in fact not recoverable */
				trusted = false;

				/*
				 * In sslmode "require" we accept some certificate verification
				 * failures when we don't have a rootcert since MITM protection
				 * isn't enforced. Check the reported failure and trust in case
				 * the cert is missing, self signed or expired/future.
				 */
				if ((strcmp(conn->sslmode, "require") == 0 ||
					 strcmp(conn->sslmode, "allow") == 0) && !conn->st_rootcert)
				{
					trust_prop = SecTrustCopyProperties(trust);
					trust_dict = CFArrayGetValueAtIndex(trust_prop, 0);
					trust_error = CFDictionaryGetValue(trust_dict,
													   kSecPropertyTypeError);
					if (trust_error)
					{
						error = CFStringGetCStringPtr(trust_error,
													  kCFStringEncodingUTF8);

						/* Self signed, or missing CA */
						if (strcmp(error, "CSSMERR_TP_INVALID_ANCHOR_CERT") == 0 ||
							strcmp(error, "CSSMERR_TP_NOT_TRUSTED") == 0 ||
							strcmp(error, "CSSMERR_TP_INVALID_CERT_AUTHORITY") == 0)
							trusted = true;
						/* Expired or future dated */
						else if (strcmp(error, "CSSMERR_TP_CERT_EXPIRED") == 0 ||
								 strcmp(error, "CSSMERR_TP_CERT_NOT_VALID_YET") == 0)
							trusted = true;
					}

					CFRelease(trust_prop);
				}

				break;
			}

			/*
			 * The below results are all cases where the certificate should be
			 * rejected without further questioning.
			 */

			/*
			 * 'Deny' means that the user has explicitly set the certificate to
			 * untrusted.
			 */
			case kSecTrustResultDeny:
				/* fall-through */
			case kSecTrustResultInvalid:
				/* fall-through */
			case kSecTrustResultFatalTrustFailure:
				/* fall-through */
			case kSecTrustResultOtherError:
				/* fall-through */
			default:
				trusted = false;
				break;
		}
	}

	CFRelease(trust);

	if (!trusted)
		return errSecNoAccessForItem;

	/*
	 * If we reach here the documentation states we need to run the Handshake
	 * again after validating the trust
	 */
	return pg_SSLOpenClient(conn);
}

/*
 * pgtls_read_pending
 *		Is there unread data waiting in the SSL read buffer?
 */
bool
pgtls_read_pending(PGconn *conn)
{
	OSStatus read_status;
	size_t len = 0;

	read_status = SSLGetBufferedReadSize((SSLContextRef) conn->ssl, &len);

	/*
	 * Should we get an error back then we assume that subsequent read
	 * operations will fail as well.
	 */
	return (read_status == noErr && len > 0);
}

/*
 * pgtls_read
 *		Read data from a secure connection.
 *
 * On failure, this function is responsible for putting a suitable message into
 * conn->errorMessage.  The caller must still inspect errno, but only to decide
 * whether to continue or retry after error.
 */
ssize_t
pgtls_read(PGconn *conn, void *ptr, size_t len)
{
	OSStatus	read_status;
	size_t		n = 0;
	ssize_t		ret = 0;
	int			read_errno = 0;
	char		sess_msg[25];

	/*
	 * Double-check that we have a connection which is in the correct state for
	 * reading before attempting to pull any data off the wire.
	 */
	if (pg_SSLsessionstate(conn, sess_msg, sizeof(sess_msg)) == -1)
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("SSL connection is: %s\n"), sess_msg);
		read_errno = ECONNRESET;
		return -1;
	}

	read_status = SSLRead((SSLContextRef) conn->ssl, ptr, len, &n);
	ret = (ssize_t) n;

	switch (read_status)
	{
		case noErr:
			break;

		case errSSLWouldBlock:
			/* Only set read_errno to EINTR iff we didn't get any data back */
			if (n == 0)
				read_errno = EINTR;
			break;

		/*
		 * Clean disconnections
		 */
		case errSSLClosedNoNotify:
			/* fall through */
		case errSSLClosedGraceful:
			printfPQExpBuffer(&conn->errorMessage,
				libpq_gettext("SSL connection has been closed unexpectedly\n"));
			read_errno = ECONNRESET;
			ret = -1;
			break;

		default:
			printfPQExpBuffer(&conn->errorMessage,
				libpq_gettext("unrecognized SSL error %d\n"), read_status);
			read_errno = ECONNRESET;
			ret = -1;
			break;
	}

	SOCK_ERRNO_SET(read_errno);
	return ret;
}

/*
 * pgtls_write
 *		Write data to a secure connection.
 *
 * On failure, this function is responsible for putting a suitable message into
 * conn->errorMessage.  The caller must still inspect errno, but only to decide
 * whether to continue or retry after error.
 */
ssize_t
pgtls_write(PGconn *conn, const void *ptr, size_t len)
{
	OSStatus	write_status;
	size_t		n = 0;
	ssize_t		ret = 0;
	int			write_errno = 0;
	char		sess_msg[25];

	/*
	 * Double-check that we have a connection which is in the correct state
	 * for writing before attempting to push any data on to the wire or the
	 * local SSL buffer.
	 */
	if (pg_SSLsessionstate(conn, sess_msg, sizeof(sess_msg)) == -1)
	{
		printfPQExpBuffer(&conn->errorMessage,
			libpq_gettext("SSL connection is: %s\n"), sess_msg);
		write_errno = ECONNRESET;
		return -1;
	}

	if (conn->ssl_buffered > 0)
	{
		write_status = SSLWrite((SSLContextRef) conn->ssl, NULL, 0, &n);

		if (write_status == noErr)
		{
			ret = conn->ssl_buffered;
			conn->ssl_buffered = 0;
		}
		else if (write_status == errSSLWouldBlock || write_status == -1)
		{
			ret = 0;
			write_errno = EINTR;
		}
		else
		{
			printfPQExpBuffer(&conn->errorMessage,
				libpq_gettext("unrecognized SSL error: %d\n"), write_status);
			ret = -1;
			write_errno = ECONNRESET;
		}
	}
	else
	{
		write_status = SSLWrite((SSLContextRef) conn->ssl, ptr, len, &n);
		ret = n;

		switch (write_status)
		{
			case noErr:
				break;

			case errSSLWouldBlock:
				conn->ssl_buffered = len;
				ret = 0;
#ifdef EAGAIN
				write_errno = EAGAIN;
#else
				write_errno = EINTR;
#endif
				break;

			/*
			 * Clean disconnections
			 */
			case errSSLClosedNoNotify:
				/* fall through */
			case errSSLClosedGraceful:
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("SSL connection has been closed unexpectedly\n"));
				write_errno = ECONNRESET;
				ret = -1;
				break;

			default:
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("unrecognized SSL error %d\n"), write_status);
				write_errno = ECONNRESET;
				ret = -1;
				break;
		}
	}

	SOCK_ERRNO_SET(write_errno);
	return ret;
}

/*
 * pgtls_init
 *		Initialize SSL system.
 *
 * There is little state or context to initialize for Secure Transport, the
 * heavy lifting is performed by pgtls_open_client.
 */
int
pgtls_init(PGconn *conn)
{
	conn->ssl_buffered = 0;
	conn->ssl_in_use = false;

	return 0;
}

/*
 * pgtls_close
 *		Close SSL connection.
 *
 * This function must cope with connections in all states of disrepair since
 * it will be called from pgtls_open_client to clean up any potentially used
 * resources in case it breaks halfway.
 */
void
pgtls_close(PGconn *conn)
{
	if (!conn->ssl)
		return;

	if (conn->st_rootcert != NULL)
		CFRelease((CFArrayRef) conn->st_rootcert);

	SSLClose((SSLContextRef) conn->ssl);
	CFRelease(conn->ssl);

	conn->ssl = NULL;
	conn->ssl_in_use = false;
}

/*
 * pg_SSLSocketRead
 *		The amount of read bytes is returned in the len variable
 */
static OSStatus
pg_SSLSocketRead(SSLConnectionRef conn, void *data, size_t *len)
{
	OSStatus	status = noErr;
	int			res;

	res = pqsecure_raw_read((PGconn *) conn, data, *len);

	if (res < 0)
	{
		switch (SOCK_ERRNO)
		{
			case ENOENT:
				status = errSSLClosedGraceful;
				break;

#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				status = errSSLWouldBlock;
				break;
		}

		*len = 0;
	}
	else
		*len = res;

	return status;
}

static OSStatus
pg_SSLSocketWrite(SSLConnectionRef conn, const void *data, size_t *len)
{
	OSStatus	status = noErr;
	int			res;

	res = pqsecure_raw_write((PGconn *) conn, data, *len);

	if (res < 0)
	{
		switch (SOCK_ERRNO)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				status = errSSLWouldBlock;
				break;

			default:
				break;
		}
	}

	*len = res;

	return status;
}

/*
 * import_identity_keychain
 *		Import the identity for the specified certificate from a Keychain
 *
 * Queries the specified Keychain for a identity with a certificate matching
 * the passed certificate reference. If the passed keychains array reference is
 * NULL, the default user Keychain will be searched.  Keychains are searched by
 * creating a dictionary of key/value pairs with the search criteria and then
 * asking for a copy of the matching entry/entries to the search criteria.
 */
static OSStatus
import_identity_keychain(const char *common_name, SecIdentityRef *identity,
							CFArrayRef keychains)
{
	OSStatus				status = errSecItemNotFound;
	CFMutableDictionaryRef	query;
	CFStringRef				cert;
	SecIdentityRef			temp;

	/*
	 * Make sure the user didn't just specify keychain: as the sslcert config.
	 * The passed certificate will have the keychain prefix stripped so in that
	 * case the string is expected to be empty here.
	 */
	if (strlen(common_name) == 0)
		return errSecInvalidValue;

	query = CFDictionaryCreateMutable(NULL, 0,
									  &kCFTypeDictionaryKeyCallBacks,
									  &kCFTypeDictionaryValueCallBacks);

	cert = CFStringCreateWithCString(NULL, common_name, kCFStringEncodingUTF8);

	/*
	 * If we didn't get a Keychain passed, skip adding it to the dictionary
	 * thus prompting a search in the users default Keychain.
	 */
	if (keychains)
		CFDictionaryAddValue(query, kSecMatchSearchList, keychains);

	/*
	 * We don't need to set a kSecMatchLimit key since the default is to only
	 * return a single reference.  Older versions of macOS had issues with
	 * certificate matching on labels, but we don't support older versions so
	 * no need to extract all and match ourselves.
	 */
	CFDictionaryAddValue(query, kSecClass, kSecClassIdentity);
	CFDictionaryAddValue(query, kSecReturnRef, kCFBooleanTrue);
	CFDictionaryAddValue(query, kSecMatchPolicy, SecPolicyCreateSSL(true, NULL));
	CFDictionaryAddValue(query, kSecAttrLabel, cert);

	status = SecItemCopyMatching(query, (CFTypeRef *) &temp);

	if (status == noErr)
		*identity = (SecIdentityRef) CFRetain(temp);

	CFRelease(query);
	CFRelease(cert);

	return status;
}

static OSStatus
import_certificate_keychain(const char *common_name, SecCertificateRef *certificate,
							CFArrayRef keychains, char *hostname)
{
	OSStatus				status = errSecItemNotFound;
	CFMutableDictionaryRef	query;
	CFStringRef				cert;
	CFStringRef				host = NULL;
	CFArrayRef				temp;
	SecPolicyRef			ssl_policy;
	int						i;

	/*
	 * Make sure the user didn't just specify the keychain prefix as the
	 * certificate config.  The passed certificate will have the keychain
	 * prefix stripped so in that case the string is expected to be empty.
	 */
	if (strlen(common_name) == 0)
		return errSecInvalidValue;

	query = CFDictionaryCreateMutable(NULL, 0,
									  &kCFTypeDictionaryKeyCallBacks,
									  &kCFTypeDictionaryValueCallBacks);

	cert = CFStringCreateWithCString(NULL, common_name, kCFStringEncodingUTF8);
	CFDictionaryAddValue(query, kSecAttrLabel, cert);

	CFDictionaryAddValue(query, kSecClass, kSecClassCertificate);
	CFDictionaryAddValue(query, kSecReturnRef, kCFBooleanTrue);
	CFDictionaryAddValue(query, kSecMatchLimit, kSecMatchLimitAll);

	/*
	 * If we didn't get a set of Keychains passed, skip adding it to the
	 * dictionary thus prompting a search in the users default Keychain.
	 */
	if (keychains)
		CFDictionaryAddValue(query, kSecMatchSearchList, keychains);

	/*
	 * Specifying a hostname requires it to match the hostname in the leaf
	 * certificate.
	 */
	if (hostname)
		host = CFStringCreateWithCString(NULL, hostname, kCFStringEncodingUTF8);
	ssl_policy = SecPolicyCreateSSL(true, host);
	CFDictionaryAddValue(query, kSecMatchPolicy, ssl_policy);

	/*
	 * Normally we could have used kSecMatchLimitOne in the above dictionary
	 * but since there are versions of macOS where the certificate matching on
	 * the label has been reported to not work (revisions of 10.12), we request
	 * all and find the one we want.  Copy all the results to a temp array and
	 * scan it for the certificate we are interested in.
	 */
	status = SecItemCopyMatching(query, (CFTypeRef *) &temp);
	if (status == noErr)
	{
		for (i = 0; i < CFArrayGetCount(temp); i++)
		{
			SecCertificateRef	search_cert;
			CFStringRef			cn;

			search_cert = (SecCertificateRef) CFArrayGetValueAtIndex(temp, i);

			if (search_cert != NULL)
			{
				SecCertificateCopyCommonName(search_cert, &cn);
				if (CFStringCompare(cn, cert, 0) == kCFCompareEqualTo)
				{
					CFRelease(cn);
					*certificate = (SecCertificateRef) CFRetain(search_cert);
					break;
				}

				CFRelease(cn);
				CFRelease(search_cert);
			}
		}

		CFRelease(temp);
	}

	CFRelease(ssl_policy);
	CFRelease(query);
	CFRelease(cert);
	if (host)
		CFRelease(host);

	return status;
}

static OSStatus
import_pem(const char *path, char *passphrase, CFArrayRef *certificate)
{
	CFDataRef							data_ref;
	CFStringRef							file_type;
	SecExternalItemType					item_type;
	SecItemImportExportKeyParameters	params;
	SecExternalFormat					format;
	FILE							   *fp;
	UInt8							   *certdata;
	struct stat							buf;

	if (!path || strlen(path) == 0)
		return errSecInvalidValue;

	if (stat(path, &buf) != 0)
	{
		if (errno == ENOENT || errno == ENOTDIR)
			return -1;

		return errSecInvalidValue;
	}

	fp = fopen(path, "r");
	if (!fp)
		return errSecInvalidValue;

	certdata = malloc(buf.st_size);
	if (!certdata)
	{
		fclose(fp);
		return errSecAllocate;
	}

	if (fread(certdata, 1, buf.st_size, fp) != buf.st_size)
	{
		fclose(fp);
		free(certdata);
		return errSSLBadCert;
	}
	fclose(fp);

	data_ref = CFDataCreate(NULL, certdata, buf.st_size);
	free(certdata);

	memset(&params, 0, sizeof(SecItemImportExportKeyParameters));
	params.version = SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION;
	/* Set OS default access control on the imported key */
	params.flags = kSecKeyNoAccessControl;
	if (passphrase)
		params.passphrase = CFStringCreateWithCString(NULL, passphrase,
													  kCFStringEncodingUTF8);

	/*
	 * Though we explicitly say this is a PEM file, Secure Transport will
	 * consider that a mere hint. Providing a file ending and a file format is
	 * what we can do to assist.
	 */
	file_type = CFSTR(".pem");
	if (!file_type)
		return errSecAllocate;

	format = kSecFormatPEMSequence;
	item_type = kSecItemTypeCertificate;

	return SecItemImport(data_ref, file_type, &format, &item_type,
						 0 /* flags */, &params, NULL /* keychain */,
						 certificate);
}

/*
 * Secure Transport has the concept of an identity, which is a packaging of a
 * private key and the certificate which contains the public key. The identity
 * is what is used for verifying the connection, so we need to provide a
 * SecIdentityRef identity to the API.
 *
 * A complete, and packaged, identity can be contained in a Keychain, in which
 * case we can load it directly without having to create anything ourselves.
 * In the case where we don't have a prepared identity in a Keychain, we need
 * to create it from its components (certificate and key). The certificate must
 * in that case be be located in a PEM file on the local filesystem. The key
 * can either be in a PEM file or in the Keychain.
 *
 * While keeping identities in the Keychain is the macOS thing to do, we want
 * to be functionally compatible with the OpenSSL support in libpq. Thus we not
 * only need to support creating an identity from a private key contained in a
 * PEM file, it needs to be the default.  The order in which we discover the
 * identity is:
 *
 * 1. Certificate and key in local files
 * 2. Certificate in local file and key in Keychain
 * 3. Identity in Keychain
 *
 * Since failures can come from multiple places, the PGconn errorMessage is
 * populated here even for SSL library errors.
 */
static OSStatus
pg_SSLLoadCertificate(PGconn *conn, CFArrayRef *cert_array,
					  CFArrayRef *key_array, CFArrayRef *rootcert_array)
{
	OSStatus			status;
	struct stat 		buf;
	char				homedir[MAXPGPATH];
	char				fnbuf[MAXPGPATH];
	bool				have_homedir;
	bool				cert_from_file = false;
	char	   		   *ssl_err_msg;
	SecIdentityRef		identity = NULL;
	SecCertificateRef	cert_ref;
	SecCertificateRef	root_ref[1];
	SecKeyRef			key_ref = NULL;
	CFArrayRef			keychains = NULL;
	SecKeychainRef		kcref[2];
	CFMutableArrayRef	cert_connection;

	/*
	 * If we have a keychain configured, open the referenced keychain as well
	 * as the default keychain and prepare an array with the references for
	 * searching. If no additional keychain is specified we don't need to open
	 * the default keychain and pass to searches since Secure Transport will
	 * use the default when passing NULL instead of an array of Keychain refs.
	 */
	if (conn->keychain)
	{
		if (stat(conn->keychain, &buf) == 0)
		{
			status = SecKeychainOpen(conn->keychain, &kcref[0]);
			if (status == noErr && kcref[0] != NULL)
			{
				SecKeychainStatus kcstatus;
				status = SecKeychainGetStatus(kcref[0], &kcstatus);
				if (status == noErr)
				{
					switch (kcstatus)
					{
						/*
						 * If the Keychain is already unlocked, readable or
						 * writeable, we don't need to do more. If not, try to
						 * unlock it.
						 */
						case kSecUnlockStateStatus:
						case kSecReadPermStatus:
						case kSecWritePermStatus:
							break;
						default:
							/*
							 * TODO: we need to get the passphrase from the
							 * user, but we currently don't have a good
							 * mechanism for that. For now, we assume a blank
							 * passphrase but we need to figure out a good way
							 * to have the user enter the passphrase.
							 */
							SecKeychainUnlock(kcref[0], 0, "", TRUE);
							break;
					}
				}

				/*
				 * We only need to open, and add, the default Keychain in case
				 * have a user keychain opened, else we will pass NULL to any
				 * keychain search which will use the default keychain by..
				 * default.
				 */
				SecKeychainCopyDefault(&kcref[1]);
				keychains = CFArrayCreate(NULL, (const void **) kcref, 2,
										  &kCFTypeArrayCallBacks);
			}
		}
	}

	/*
	 * We'll need the home directory if any of the relevant parameters are
	 * defaulted.  If pqGetHomeDirectory fails, act as though none of the files
	 * could be found.
	 */
	if (!(conn->sslcert && strlen(conn->sslcert) > 0) ||
		!(conn->sslkey && strlen(conn->sslkey) > 0) ||
		!(conn->sslrootcert && strlen(conn->sslrootcert) > 0))
		have_homedir = pqGetHomeDirectory(homedir, sizeof(homedir));
	else	/* won't need it */
		have_homedir = false;

	/*
	 * Try to load the root cert from either a user defined keychain or the
	 * default Keychain in case none is specified
	 */
	if (conn->sslrootcert &&
		pg_strncasecmp(conn->sslrootcert, KC_PREFIX, KC_PREFIX_LEN) == 0)
	{
		root_ref[0] = NULL;
		strlcpy(fnbuf, conn->sslrootcert + KC_PREFIX_LEN, sizeof(fnbuf));

		import_certificate_keychain(fnbuf, &root_ref[0], keychains, NULL);

		if (root_ref[0])
			conn->st_rootcert = (void *) CFArrayCreate(NULL,
													   (const void **) root_ref,
													   1, &kCFTypeArrayCallBacks);
	}

	if (!conn->st_rootcert)
	{
		if (conn->sslrootcert && strlen(conn->sslrootcert) > 0)
			strlcpy(fnbuf, conn->sslrootcert, sizeof(fnbuf));
		else if (have_homedir)
			snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, ROOT_CERT_FILE);
		else
			fnbuf[0] = '\0';

		if (fnbuf[0] != '\0')
		{
			if (stat(fnbuf, &buf) != 0)
			{
				/*
				 * stat() failed; assume root file doesn't exist.  If sslmode is
				 * verify-ca or verify-full, this is an error.  Otherwise, continue
				 * without performing any server cert verification.
				 */
				if (conn->sslmode[0] == 'v')	/* "verify-ca" or "verify-full" */
				{
					/*
					 * The only way to reach here with an empty filename is if
					 * pqGetHomeDirectory failed.  That's a sufficiently unusual
					 * case that it seems worth having a specialized error message
					 * for it.
					 */
					if (fnbuf[0] == '\0')
						printfPQExpBuffer(&conn->errorMessage,
										  libpq_gettext("could not get home directory to locate root certificate file\n"
														"Either provide the file or change sslmode to disable server certificate verification.\n"));
					else
						printfPQExpBuffer(&conn->errorMessage,
							libpq_gettext("root certificate file \"%s\" does not exist\n"
										  "Either provide the file or change sslmode to disable server certificate verification.\n"), fnbuf);
					return errSecInternalError;
				}
			}
			else
			{
				status = import_pem(fnbuf, NULL, rootcert_array);
				if (status != noErr)
				{
					ssl_err_msg = pg_SSLerrmessage(status);
					printfPQExpBuffer(&conn->errorMessage,
							libpq_gettext("could not load root certificate file \"%s\": %s\n"),
										  fnbuf, ssl_err_msg);
					pg_SSLerrfree(ssl_err_msg);
					return status;
				}

				if (*rootcert_array != NULL)
					conn->st_rootcert = (void *) CFRetain(*rootcert_array);
			}
		}
	}

	/*
	 * If the sslcert config is prefixed with a keychain identifier, the cert
	 * must be located in either the default or the specified keychain.
	 */
	if (conn->sslcert &&
		pg_strncasecmp(conn->sslcert, KC_PREFIX, KC_PREFIX_LEN) == 0)
	{
		strlcpy(fnbuf, conn->sslcert + KC_PREFIX_LEN, sizeof(fnbuf));

		status = import_identity_keychain(fnbuf, &identity, keychains);

		if (identity && status == noErr)
			SecIdentityCopyPrivateKey(identity, &key_ref);
		else
		{
			ssl_err_msg = pg_SSLerrmessage(status);
			printfPQExpBuffer(&conn->errorMessage,
				libpq_gettext("could not load certificate \"%s\" from keychain: %s\n"),
							  fnbuf, ssl_err_msg);
			pg_SSLerrfree(ssl_err_msg);

			return status;
		}
	}
	/*
	 * No prefix on the sslcert config, the certificate must thus reside in a
	 * file on disk.
	 */
	else
	{
		if (conn->sslcert && strlen(conn->sslcert) > 0)
			strlcpy(fnbuf, conn->sslcert, sizeof(fnbuf));
		else if (have_homedir)
			snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, USER_CERT_FILE);
		else
			fnbuf[0] = '\0';

		if (fnbuf[0] != '\0')
		{
			status = import_pem(fnbuf, NULL, cert_array);
			if (status != noErr)
			{
				if (status == -1)
					return noErr;

				ssl_err_msg = pg_SSLerrmessage(status);
				printfPQExpBuffer(&conn->errorMessage,
					libpq_gettext("could not load certificate file \"%s\": %s\n"),
								  fnbuf, ssl_err_msg);
				pg_SSLerrfree(ssl_err_msg);
				return status;
			}

			cert_ref = (SecCertificateRef) CFArrayGetValueAtIndex(*cert_array, 0);
			cert_from_file = true;

			/*
			 * We now have a certificate, so we need a private key as well in
			 * order to create the identity.
			 */

			/*
			 * The sslkey config is prefixed with keychain: indicating that the
			 * key should be loaded from Keychain instead of the filesystem.
			 * Search for the private key matching the cert_ref in the opened
			 * Keychains. If found, we get the identity returned.
			 */
			if (conn->sslkey &&
				pg_strncasecmp(conn->sslkey, KC_PREFIX, KC_PREFIX_LEN) == 0)
			{
				status = SecIdentityCreateWithCertificate(keychains, cert_ref,
														  &identity);
				if (status != noErr)
				{
					printfPQExpBuffer(&conn->errorMessage,
						libpq_gettext("certificate present, but private key \"%s\" not found in Keychain\n"),
									  fnbuf);
					return errSecInternalError;
				}

				SecIdentityCopyPrivateKey(identity, &key_ref);
			}
			else
			{
				if (conn->sslkey && strlen(conn->sslkey) > 0)
					strlcpy(fnbuf, conn->sslkey, sizeof(fnbuf));
				else if (have_homedir)
					snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, USER_KEY_FILE);
				else
					fnbuf[0] = '\0';

				/*
				 * If there is a matching file on the filesystem, require the
				 * key to be loaded from that file.
				 */
				if (fnbuf[0] != '\0')
				{

					if (stat(fnbuf, &buf) != 0)
					{
						printfPQExpBuffer(&conn->errorMessage,
										  libpq_gettext("certificate present, but not private key file \"%s\"\n"),
										  fnbuf);
						return errSecInvalidKeyRef;
					}
#ifndef WIN32
					if (!S_ISREG(buf.st_mode) || buf.st_mode & (S_IRWXG | S_IRWXO))
					{
						printfPQExpBuffer(&conn->errorMessage,
										  libpq_gettext("private key file \"%s\" has group or world access; permissions should be u=rw (0600) or less\n"),
										  fnbuf);
						return errSecInvalidKeyRef;
					}
#endif
					status = import_pem(fnbuf, NULL, key_array);
					if (status != noErr)
					{
						ssl_err_msg = pg_SSLerrmessage(status);
						printfPQExpBuffer(&conn->errorMessage,
							libpq_gettext("could not load private key file \"%s\": %s\n"),
										  fnbuf, ssl_err_msg);
						pg_SSLerrfree(ssl_err_msg);
						return status;
					}

					key_ref = (SecKeyRef) CFArrayGetValueAtIndex(*key_array, 0);
				}
			}

			/*
			 * We have certificate and a key loaded from files on disk, now we
			 * can create an identity from this pair.
			 */
			if (key_ref)
				identity = SecIdentityCreate(NULL, cert_ref, key_ref);
		}
	}

	if (!identity)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not create identity from certificate/key\n"));
		return errSecInvalidValue;
	}

	/*
	 * If we have created the identity "by hand" without involving the
	 * Keychain we need to include the certificates in the array passed to
	 * SSLSetCertificate()
	 */
	if (cert_from_file)
	{
		cert_connection = CFArrayCreateMutableCopy(NULL, 0, *cert_array);
		CFArraySetValueAtIndex(cert_connection, 0, identity);
	}
	else
	{
		cert_connection = CFArrayCreateMutable(NULL, 1L,
											   &kCFTypeArrayCallBacks);
		CFArrayInsertValueAtIndex(cert_connection, 0,
								  (const void *) identity);
	}

	status = SSLSetCertificate((SSLContextRef) conn->ssl, cert_connection);

	if (status != noErr)
	{
		ssl_err_msg = pg_SSLerrmessage(status);
		printfPQExpBuffer(&conn->errorMessage,
				libpq_gettext("could not set certificate for connection: (%d) %s\n"),
							  status, ssl_err_msg);
		pg_SSLerrfree(ssl_err_msg);
		return status;
	}

	if (key_ref)
	{
		conn->ssl_key_bits = SecKeyGetBlockSize(key_ref);
		CFRelease(key_ref);
	}

	return noErr;
}

/* ------------------------------------------------------------ */
/*					SSL information functions					*/
/* ------------------------------------------------------------ */

void *
PQgetssl(PGconn *conn)
{
	/*
	 * Always return NULL as this is legacy and defined to be equal to
	 * PQsslStruct(conn, "OpenSSL");
	 */
	return NULL;
}

void *
PQsslStruct(PGconn *conn, const char *struct_name)
{
	if (!conn)
		return NULL;
	if (strcmp(struct_name, "SecureTransport") == 0)
		return conn->ssl;
	return NULL;
}

const char *const *
PQsslAttributeNames(PGconn *conn)
{
	static const char *const result[] = {
		"library",
		"key_bits",
		"cipher",
		"protocol",
		NULL
	};

	return result;
}

const char *
PQsslAttribute(PGconn *conn, const char *attribute_name)
{
	SSLCipherSuite	cipher;
	SSLProtocol		protocol;
	OSStatus		status;
	const char 	   *attribute = NULL;

	if (!conn || !conn->ssl)
		return NULL;

	if (strcmp(attribute_name, "library") == 0)
		attribute = "SecureTransport";
	else if (strcmp(attribute_name, "key_bits") == 0)
	{
		if (conn->ssl_key_bits > 0)
		{
			static char sslbits_str[10];
			snprintf(sslbits_str, sizeof(sslbits_str), "%d", conn->ssl_key_bits);
			attribute = sslbits_str;
		}
	}
	else if (strcmp(attribute_name, "cipher") == 0)
	{
		status = SSLGetNegotiatedCipher((SSLContextRef) conn->ssl, &cipher);
		if (status == noErr)
			return pg_SSLciphername(cipher);
	}
	else if (strcmp(attribute_name, "protocol") == 0)
	{
		status = SSLGetNegotiatedProtocolVersion((SSLContextRef) conn->ssl, &protocol);
		if (status == noErr)
		{
			switch (protocol)
			{
				case kTLSProtocol11:
					attribute = "TLSv1.1";
					break;
				case kTLSProtocol12:
					attribute = "TLSv1.2";
					break;
				case kTLSProtocol13:
					attribute = "TLSv1.3";
				default:
					break;
			}
		}
	}

	return attribute;
}

/* ------------------------------------------------------------ */
/*			Secure Transport Information Functions				*/
/* ------------------------------------------------------------ */

/*
 * Obtain reason string for passed SSL errcode
 */
static char ssl_noerr[] = "no SSL error reported";
static char ssl_nomem[] = "out of memory allocating error description";
#define SSL_ERR_LEN 128

static char *
pg_SSLerrmessage(OSStatus errcode)
{
	char 	   *err_buf;
	const char *tmp;
	CFStringRef	err_msg;

	if (errcode == noErr || errcode == errSecSuccess)
		return ssl_noerr;

	err_buf = malloc(SSL_ERR_LEN);
	if (!err_buf)
		return ssl_nomem;

	err_msg = SecCopyErrorMessageString(errcode, NULL);
	if (err_msg)
	{
		tmp = CFStringGetCStringPtr(err_msg, kCFStringEncodingUTF8);
		strlcpy(err_buf, tmp, SSL_ERR_LEN);
		CFRelease(err_msg);
	}
	else
		snprintf(err_buf, sizeof(err_buf), _("SSL error code %d"), errcode);

	return err_buf;
}

static void
pg_SSLerrfree(char *err_buf)
{
	if (err_buf && err_buf != ssl_nomem && err_buf != ssl_noerr)
		free(err_buf);
}

/*
 * pg_SSLsessionstate
 *
 * Returns 0 if the connection is open and -1 in case the connection is closed,
 * or its status unknown. If msg is non-NULL the current state is copied with
 * at most len - 1 characters ensuring a NUL terminated returned string.
 */
static int
pg_SSLsessionstate(PGconn *conn, char *msg, size_t len)
{
	SSLSessionState		state;
	OSStatus			status;
	const char 		   *status_msg;

	/*
	 * If conn->ssl isn't defined we will report "Unknown" which it could be
	 * argued being correct or not, but since we don't know if there has ever
	 * been a connection at all it's not more correct to say "Closed" or
	 * "Aborted".
	 */
	if (conn->ssl)
		status = SSLGetSessionState((SSLContextRef) conn->ssl, &state);
	else
	{
		status = errSecInternalError;
		state = -1;
	}

	switch (state)
	{
		case kSSLConnected:
			status_msg = "Connected";
			status = 0;
			break;
		case kSSLHandshake:
			status_msg = "Handshake";
			status = 0;
			break;
		case kSSLIdle:
			status_msg = "Idle";
			status = 0;
			break;
		case kSSLClosed:
			status_msg = "Closed";
			status = -1;
			break;
		case kSSLAborted:
			status_msg = "Aborted";
			status = -1;
			break;
		default:
			status_msg = "Unknown";
			status = -1;
			break;
	}

	if (msg)
		strlcpy(msg, status_msg, len);

	return (status == noErr ? 0 : -1);
}
