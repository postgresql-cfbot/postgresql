/*-------------------------------------------------------------------------
 *
 * be-secure-securetransport.c
 *	  Apple Secure Transport support
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * TODO:
 *		- It would be good to be able to set "not applicable" on some options
 *		  like compression which isn't supported in Secure Transport (and most
 *		  likely any other SSL libraries supported in the future). This is a
 *		  larger question than for this patch though.
 *		- Support memory allocation in Secure Transport via a custom Core
 *		  Foundation allocator which is backed by a MemoryContext? Not sure it
 *		  would be possible but would be interesting to investigate.
 *		- Replace usage of internal error buffer with straight calls to
 *		  ereport() where possible
 *		- Doublecheck that the used OSStatus return values make sense for
 *		  error reporting
 *		- Figure out if any API calls need version gating like kTLSProtocol13
 *		- Replace -framework linker flags with -l to be GCC compatible
 *		- Fix busy-wait loop in frontend like how it's fixed in backend
 *		- contrib/sslinfo
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure-securetransport.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

#include "common/base64.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/backend_random.h"
#include "utils/memutils.h"

/*
 * TODO: This dance is required due to collisions in the CoreFoundation
 * headers. How to handle it properly?
 */
#define pg_ACL_DELETE ACL_DELETE
#define pg_ACL_EXECUTE ACL_EXECUTE
#undef ACL_EXECUTE
#undef ACL_DELETE
#define Size pg_Size
#define uint64 pg_uint64
#include <Security/Security.h>
#include <Security/SecureTransport.h>
#include "common/securetransport.h"
#undef uint64
#undef Size
#undef ACL_DELETE
#undef ACL_EXECUTE
#define pg_uint64 uint64
#define pg_Size Size
#define ACL_DELETE pg_ACL_DELETE
#define ACL_EXECUTE pg_ACL_EXECUTE

/*
 * While errSecUnknownFormat has been defined as -25257 at least since 10.8
 * Lion, there was for the longest time no translation and it was missing from
 * the OSStatus enum. As it's easy enough, let's maintain our own.
 */
#ifndef errSecUnknownFormat
#define errSecUnknownFormat -25257
#endif

#define KC_PREFIX		"keychain:"
#define KC_PREFIX_LEN	(strlen("keychain:"))

/* ------------------------------------------------------------ */
/*				Struct definitions and Static variables			*/
/* ------------------------------------------------------------ */

/*
 * For Secure Transport API functions we rely on SecCopyErrorMessageString()
 * which will provide a human readable error message for the individual error
 * statuses. For our static functions, we mimic the behaviour by passing
 * errSecInternalError and setting the error message in internal_err. Since we
 * may have encountered an error due to memory pressure, we don't want to rely
 * on dynamically allocating memory for this error message.
 */
#define ERR_MSG_LEN 128
static char internal_err[ERR_MSG_LEN];

/* ------------------------------------------------------------ */
/*							Prototypes							*/
/* ------------------------------------------------------------ */

/*
 * SecIdentityCreate is not an exported Secure Transport API. There is however
 * no exported Secure Transport function that can create an identity from a
 * SecCertificateRef and a SecKeyRef without having either of them in a
 * Keychain. This function is commonly used in open source projects (such as
 * ffmpeg and mono for example), but finding an alternative is a TODO.
 */
extern SecIdentityRef SecIdentityCreate(CFAllocatorRef allocator,
										SecCertificateRef certificate,
										SecKeyRef privateKey);

static bool load_key(char *name, CFArrayRef *out);
static OSStatus load_keychain(char *name, CFArrayRef *keychains);
static OSStatus load_certificate_file(char *name, CFArrayRef *cert_array);
static OSStatus load_identity_keychain(const char *common_name,
									   SecIdentityRef *identity,
									   CFArrayRef keychains);

static int load_dh_file(char *filename, char **buf);
static void load_dh_params(char *dh, int len, bool is_pem, SSLContextRef ssl);
static char * pg_SSLerrmessage(OSStatus status);
static OSStatus pg_SSLSocketWrite(SSLConnectionRef conn, const void *data, size_t *len);
static OSStatus pg_SSLSocketRead(SSLConnectionRef conn, void *data, size_t *len);
static void KeychainEnsureValid(CFArrayRef keychains);

/* ------------------------------------------------------------ */
/*							Backend API							*/
/* ------------------------------------------------------------ */

/*
 * be_tls_init
 *		Initialize global Secure Transport structures (if any)
 *
 * This is where we'd like to load and parse certificates and private keys
 * for the connection, but since Secure Transport will spawn threads deep
 * inside the API we must postpone this until inside a backend. This means
 * that we won't fail on an incorrect certificate chain until a connection
 * is attempted, unlike with OpenSSL where we fail immediately on server
 * startup.
 *
 * Another reason to defer this until when in the backend is that Keychains
 * are SQLite3 backed, and sqlite does not allow access across a fork. See
 * https://sqlite.org/faq.html#q6 for more information.
 *
 * This further means that the loglevel cannot be controlled using the
 * isServerStart parameter like to OpenSSL is using it. For Secure Transport
 * we always fall back to isServerStart being false.
 */
int
be_tls_init(bool isServerStart)
{
	memset(internal_err, '\0', sizeof(internal_err));

	return 0;
}

/*
 * be_tls_destroy
 *		Tear down global Secure Transport structures and return resources.
 */
void
be_tls_destroy(void)
{
	ssl_loaded_verify_locations = false;
}

/*
 * be_tls_open_server
 *		Attempt to negotiate a secure connection
 *
 * Unlike the OpenSSL backend, this function is responsible for the deferred
 * loading of keys and certificates for use in the connection. See the comment
 * on be_tls_init for further reasoning around this.
 */
int
be_tls_open_server(Port *port)
{
	OSStatus			status;
	SecTrustRef			trust;
	SecTrustResultType	trust_eval;
	SecIdentityRef		identity;
	CFArrayRef			root_certificates;
	CFArrayRef			certificates;
	CFArrayRef			keys;
	CFArrayRef			keychains;
	CFMutableArrayRef	chain;
	char			   *dh_buf;
	int					dh_len;

	Assert(!port->ssl);

	/*
	 * If ssl_keychain_file has been configured we try to load a Keychain from
	 * the specified filename as well as the Default user keychain in case the
	 * configuration allows that. Else, set keychains to NULL which indicates
	 * that only the default Keychain is available.
	 */
	if (ssl_keychain_file[0])
	{
		status = load_keychain(ssl_keychain_file, &keychains);
		if (status != noErr)
			ereport(COMMERROR,
					(errmsg("could not load keychain(s): %s",
							pg_SSLerrmessage(status))));
	}
	else
		keychains = NULL;

	/*
	 * Ensure that we have loaded the Keychains according to the configuration
	 * before proceeding.
	 */
	KeychainEnsureValid(keychains);

	/*
	 * If the ssl_cert_file is prefixed with a keychain reference, we will try
	 * to load a complete identity from the Keychain, else treat it as a
	 * reference to a plain file on the filesystem.
	 */
	identity = NULL;
	certificates = NULL;
	if (pg_strncasecmp(ssl_cert_file, KC_PREFIX, KC_PREFIX_LEN) == 0)
		status = load_identity_keychain(ssl_cert_file + KC_PREFIX_LEN, &identity, keychains);
	else
	{
		status = load_certificate_file(ssl_cert_file, &certificates);
		if (status != noErr)
			ereport(COMMERROR,
					(errmsg("could not load server certificate \"%s\": %s",
							ssl_cert_file, pg_SSLerrmessage(status))));

		if (!load_key(ssl_key_file, &keys))
			return -1;

		/*
		 * We now have a certificate and either a private key, or a search path
		 * which should contain it.
		 */
		identity = SecIdentityCreate(NULL,
									 (SecCertificateRef) CFArrayGetValueAtIndex(certificates, 0),
									 (SecKeyRef) CFArrayGetValueAtIndex(keys, 0));
	}

	if (identity == NULL)
		ereport(COMMERROR,
				(errmsg("could not create identity: %s",
						pg_SSLerrmessage(status))));

	/*
	 * SSLSetCertificate() sets the certificate(s) to use for the connection.
	 * The first element in the passed array is required to be the identity
	 * with elements 1..n being certificates.
	 */
	chain = CFArrayCreateMutable(NULL, 0, &kCFTypeArrayCallBacks);
	CFRetain(identity);
	CFArrayInsertValueAtIndex(chain, 0, identity);
	if (certificates != NULL)
		CFArrayAppendArray(chain, certificates,
						   CFRangeMake(0, CFArrayGetCount(certificates)));

	/*
	 * Load the Certificate Authority if configured
	 */
	if (ssl_ca_file[0])
	{
		status = load_certificate_file(ssl_ca_file, &root_certificates);
		if (status == noErr)
		{
			CFArrayAppendArray(chain, root_certificates,
							   CFRangeMake(0, CFArrayGetCount(root_certificates)));

			ssl_loaded_verify_locations = true;
		}
		else
		{
			ereport(LOG,
					(errmsg("could not load root certificate \"%s\": %s",
							ssl_ca_file, pg_SSLerrmessage(status))));

			ssl_loaded_verify_locations = false;
		}
	}
	else
		ssl_loaded_verify_locations = false;

	/*
	 * Certificate Revocation List are not supported in the Secure Transport
	 * API, they need to be added to a Keychain using macOS userspace tools.
	 */
	if (ssl_crl_file[0])
		ereport(COMMERROR,
				(errmsg("CRL files are not supported with Secure Transport")));

	port->ssl = (void *) SSLCreateContext(NULL, kSSLServerSide, kSSLStreamType);
	if (!port->ssl)
		ereport(COMMERROR,
				(errmsg("could not create SSL context")));

	port->ssl_in_use = true;
	port->ssl_buffered = 0;

	if (ssl_loaded_verify_locations)
		SSLSetClientSideAuthenticate((SSLContextRef) port->ssl, kTryAuthenticate);

	/*
	 * In case the user hasn't configured a DH parameters file, we load a pre-
	 * computed DH parameter to avoid having Secure Transport computing one for
	 * us (which is done by default unless one is set).
	 */
	dh_buf = NULL;
	dh_len = 0;
	if (ssl_dh_params_file[0])
		dh_len = load_dh_file(ssl_dh_params_file, &dh_buf);

	if (!dh_buf || dh_len == 0)
	{
		dh_buf = pstrdup(FILE_DH2048);
		dh_len = sizeof(FILE_DH2048);
	}

	load_dh_params(dh_buf, dh_len, true, (SSLContextRef) port->ssl);

	/*
	 * Set Tlsv1.2 as the minimum protocol version we allow for the connection
	 */
	status = SSLSetProtocolVersionMin((SSLContextRef) port->ssl,
									  kTLSProtocol12);
	if (status != noErr)
		ereport(COMMERROR,
				(errmsg("could not set protocol for connection: %s",
						pg_SSLerrmessage(status))));

	status = SSLSetCertificate((SSLContextRef) port->ssl,
							   (CFArrayRef) chain);
	if (status != noErr)
		ereport(COMMERROR,
				(errmsg("could not set certificate for connection: %s",
						pg_SSLerrmessage(status))));

	status = SSLSetIOFuncs((SSLContextRef) port->ssl,
						   pg_SSLSocketRead,
						   pg_SSLSocketWrite);
	if (status != noErr)
		ereport(COMMERROR,
				(errmsg("could not set SSL IO functions: %s",
						pg_SSLerrmessage(status))));

	status = SSLSetSessionOption((SSLContextRef) port->ssl,
								 kSSLSessionOptionBreakOnClientAuth, true);
	if (status != noErr)
		ereport(COMMERROR,
				(errmsg("could not set SSL certificate validation: %s",
						pg_SSLerrmessage(status))));

	status = SSLSetConnection((SSLContextRef) port->ssl, port);
	if (status != noErr)
		ereport(COMMERROR,
				(errmsg("could not establish SSL connection: %s",
						pg_SSLerrmessage(status))));

	/*
	 * Perform handshake
	 */
	for (;;)
	{
		status = SSLHandshake((SSLContextRef) port->ssl);
		if (status == noErr)
			break;

		/*
		 * If SSLHandshake returns errSSLWouldBlock we need to call it again
		 * when we can expect the call to return a different result. Wait on
		 * the socket according to the IO direction set in port->waitfor.
		 */
		if (status == errSSLWouldBlock)
		{
			WaitLatchOrSocket(MyLatch, port->waitfor, port->sock, 0,
							  WAIT_EVENT_SSL_OPEN_SERVER);
			continue;
		}

		if (status == errSSLClosedAbort || status == errSSLClosedGraceful)
			return -1;

		if (status == errSSLPeerAuthCompleted)
		{
			status = SSLCopyPeerTrust((SSLContextRef) port->ssl, &trust);
			if (status != noErr || trust == NULL)
			{
				ereport(WARNING,
					(errmsg("SSLCopyPeerTrust returned: %s",
							pg_SSLerrmessage(status))));
				port->peer_cert_valid = false;
				return 0;
			}

			if (ssl_loaded_verify_locations)
			{
				status = SecTrustSetAnchorCertificates(trust, root_certificates);
				if (status != noErr)
				{
					ereport(WARNING,
							(errmsg("SecTrustSetAnchorCertificates returned: %s",
									pg_SSLerrmessage(status))));
					return -1;
				}

				status = SecTrustSetAnchorCertificatesOnly(trust, false);
				if (status != noErr)
				{
					ereport(WARNING,
							(errmsg("SecTrustSetAnchorCertificatesOnly returned: %s",
									pg_SSLerrmessage(status))));
					return -1;
				}
			}

			trust_eval = 0;
			status = SecTrustEvaluate(trust, &trust_eval);
			if (status != noErr)
			{
				ereport(WARNING,
						(errmsg("SecTrustEvaluate failed, returned: %s",
								pg_SSLerrmessage(status))));
				return -1;
			}

			switch (trust_eval)
			{
				/*
				 * If 'Unspecified' then an anchor certificate was reached
				 * without encountering any explicit user trust. If 'Proceed'
				 * then the user has chosen to explicitly trust a certificate
				 * in the chain by clicking "Trust" in the Keychain app.
				 */
				case kSecTrustResultUnspecified:
				case kSecTrustResultProceed:
					port->peer_cert_valid = true;
					break;

				/*
				 * 'RecoverableTrustFailure' indicates that the certificate was
				 * rejected but might be trusted with minor changes to the eval
				 * context (ignoring expired certificate etc). In the frontend
				 * we can in some circumstances allow this, but in the backend
				 * this always means that the client certificate is considered
				 * untrusted.
				 */
				case kSecTrustResultRecoverableTrustFailure:
					port->peer_cert_valid = false;
					break;

				/*
				 * Treat all other cases as rejection without further
				 * questioning.
				 */
				default:
					port->peer_cert_valid = false;
					break;
			}

			if (port->peer_cert_valid)
			{
				SecCertificateRef	usercert;
				CFStringRef			usercert_cn;
				const char		   *peer_cn;

				usercert = SecTrustGetCertificateAtIndex(trust, 0L);
				SecCertificateCopyCommonName(usercert, &usercert_cn);

				/* Guard against empty/missing CNs */
				peer_cn = CFStringGetCStringPtr(usercert_cn, kCFStringEncodingUTF8);
				if (!peer_cn)
					port->peer_cn = pstrdup("");
				else
					port->peer_cn = pstrdup(peer_cn);

				CFRelease(usercert_cn);
			}

			CFRelease(trust);
		}
	}

	if (status != noErr)
		return -1;

	return 0;
}

/*
 * load_key
 *		Extracts a key from a PEM file on the filesystem
 */
static bool
load_key(char *name, CFArrayRef *out)
{
	OSStatus			status;
	struct stat			stat_buf;
	int					ret;
	UInt8			   *buf;
	FILE			   *fd;
	CFDataRef			data;
	SecExternalFormat	format;
	SecExternalItemType	type;
	CFStringRef			path;
	SecItemImportExportKeyParameters params;

	if (!check_ssl_key_file_permissions(name, /* isServerStart */ false))
		return false;

	/*
	 * check_ssl_key_file_permissions() has already checked the file for
	 * existence and correct permissions, but we still need to stat it to
	 * get the filesize.
	 */
	if (stat(name, &stat_buf) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not load private key \"%s\": unable to open",
						name)));

	if ((fd = AllocateFile(name, "r")) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not load private key \"%s\": unable to open",
						name)));

	buf = palloc(stat_buf.st_size);

	ret = fread(buf, 1, stat_buf.st_size, fd);
	FreeFile(fd);

	if (ret != stat_buf.st_size)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not load private key \"%s\": unable to read",
						name)));

	type = kSecItemTypePrivateKey;
	format = kSecFormatPEMSequence;
	path = CFStringCreateWithCString(NULL, name, kCFStringEncodingUTF8);
	data = CFDataCreate(NULL, buf, stat_buf.st_size);

	memset(&params, 0, sizeof(SecItemImportExportKeyParameters));
	params.version = SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION;
	/* Set OS default access control on the imported key */
	params.flags = kSecKeyNoAccessControl;

	status = SecItemImport(data, path, &format, &type, 0, &params, NULL, out);

	/*
	 * There is no way to set a callback for acquiring the passphrase like how
	 * OpenSSL does it, so we need to re-run the import if it failed with a
	 * passphrase missing status. If no ssl_passphrase_command has been set we
	 * currently don't retry, which is something that will need to be revisited.
	 * TODO: figure out what would be the least confusing to the user here;
	 * perhaps supplying our own fallback passphrase_command? (which should be
	 * a TLS backend common function since it wouldn't be Secure Transport
	 * specific?)
	 */
	if (status == errSecPassphraseRequired)
	{
		if (ssl_passphrase_command[0] && ssl_passphrase_command_supports_reload)
		{
			const char	   *prompt = "Enter PEM pass phrase: ";
			char			buf[256];
			CFStringRef		passphrase;

			run_ssl_passphrase_command(prompt, false /* isServerStart */, buf, sizeof(buf));

			passphrase = CFStringCreateWithCString(NULL, buf, kCFStringEncodingUTF8);
			params.passphrase = passphrase;
			memset(&buf, '\0', sizeof(buf));

			status = SecItemImport(data, path, &format, &type, 0, &params, NULL, out);

			CFRelease(passphrase);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("private key file \"%s\" cannot be loaded because it requires a passphrase",
							name)));
	}

	CFRelease(path);
	CFRelease(data);

	if (status != noErr)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("could not load private key \"%s\": %s",
						name, pg_SSLerrmessage(status))));

	return true;
}

/*
 * load_keychain
 *		Open the specified, and default, Keychain
 *
 * Operations on keychains other than the default take the keychain references
 * in an array. We need to copy a reference to the default keychain into the
 * array to include it in the search.
 *
 * For server applications, we don't want modal dialog boxes opened for
 * Keychain interaction. Calling SecKeychainSetUserInteractionAllowed(FALSE)
 * will turn off all GUI interaction with the user, which may seem like what we
 * want server side. This however has the side effect to turn off all GUI
 * elements for all applications until some application calls
 * SecKeychainSetUserInteractionAllowed(TRUE) or reboots the box. We might thus
 * remove wanted GUI interaction from another app, or another app might
 * introduce it for us.
 */
static OSStatus
load_keychain(char *name, CFArrayRef *keychains)
{
	OSStatus			status;
	struct stat			stat_buf;
	SecKeychainRef		kc[2];
	int					array_len = 1;

	/*
	 * If we are passing in a non-empty CFArrayRef, and fail to load the user
	 * Keychain then we would risk injecting Keychains since we will trust this
	 * array from hereon. Unconditionally error out hard immediately to avoid.
	 */
	if (*keychains != NULL)
		ereport(FATAL,
				(errmsg("requesting to load Keychains into already allocated memory")));

	if (stat(name, &stat_buf) != 0)
		return errSecInvalidValue;

	status = SecKeychainOpen(name, &kc[0]);
	if (status == noErr)
	{
		SecKeychainUnlock(kc[0], 0, "", TRUE);

		/*
		 * If we are allowed to use the default Keychain, add it to the array
		 * to include it in Keychain searches. If we are only using the default
		 * Keychain and no user defined Keychain we don't create an array at
		 * all since the recommended procedure is to pass NULL instead of an
		 * array containing only a reference to the default Keychain.
		 */
		if (ssl_keychain_use_default)
			SecKeychainCopyDefault(&kc[array_len++]);

		*keychains = CFArrayCreate(NULL, (const void **) kc, array_len,
								   &kCFTypeArrayCallBacks);

		if (!*keychains)
		{
			snprintf(internal_err, ERR_MSG_LEN, "unable to allocate memory");
			return errSecInternalError;
		}
	}

	return status;
}

/*
 * import_identity_keychain
 *		Import the identity for the specified certificate from a Keychain
 *
 * Queries the specified Keychain for a identity with a certificate matching
 * the passed certificate reference. If the passed keychains array reference is
 * NULL the default user Keychain will be searched.  Keychains are searched by
 * creating a dictionary of key/value pairs with the search criteria and then
 * asking for a copy of the matching entry/entries to the search criteria.
 */
static OSStatus
load_identity_keychain(const char *common_name, SecIdentityRef *identity,
							CFArrayRef keychains)
{
	OSStatus				status = errSecItemNotFound;
	CFMutableDictionaryRef	query;
	CFStringRef				cert;
	CFArrayRef				temp;

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

	CFDictionaryAddValue(query, kSecClass, kSecClassIdentity);
	CFDictionaryAddValue(query, kSecReturnRef, kCFBooleanTrue);
	CFDictionaryAddValue(query, kSecMatchLimit, kSecMatchLimitAll);
	CFDictionaryAddValue(query, kSecMatchPolicy, SecPolicyCreateSSL(true, NULL));
	CFDictionaryAddValue(query, kSecAttrLabel, cert);

	/*
	 * Normally we could have used kSecMatchLimitOne in the above dictionary
	 * but since there are versions of macOS where the certificate matching on
	 * the label doesn't work, we need to request all and find the one we want.
	 * Copy all the results to a temp array and scan it for the certificate we
	 * are interested in.
	 */
	status = SecItemCopyMatching(query, (CFTypeRef *) &temp);
	if (status == noErr)
	{
		OSStatus		search_stat;
		SecIdentityRef	dummy;
		int				i;

		for (i = 0; i < CFArrayGetCount(temp); i++)
		{
			SecCertificateRef	search_cert;
			CFStringRef			cn;

			dummy = (SecIdentityRef) CFArrayGetValueAtIndex(temp, i);
			search_stat = SecIdentityCopyCertificate(dummy, &search_cert);

			if (search_stat == noErr && search_cert != NULL)
			{
				SecCertificateCopyCommonName(search_cert, &cn);
				if (CFStringCompare(cn, cert, 0) == kCFCompareEqualTo)
				{
					CFRelease(cn);
					CFRelease(search_cert);
					*identity = (SecIdentityRef) CFRetain(dummy);
					break;
				}

				CFRelease(cn);
				CFRelease(search_cert);
			}
		}

		CFRelease(temp);
	}

	CFRelease(query);
	CFRelease(cert);

	return status;
}

/*
 * load_certificate_file
 *		Extracts a certificate from a PEM file on the filesystem
 */
static OSStatus
load_certificate_file(char *name, CFArrayRef *cert_array)
{
	struct stat			stat_buf;
	int					ret;
	UInt8			   *buf;
	FILE			   *fd;
	CFDataRef			data;
	SecExternalFormat	format;
	SecExternalItemType	type;
	CFStringRef			path;
	OSStatus			status;

	ret = stat(name, &stat_buf);
	if (ret != 0 && errno == ENOENT)
	{
		snprintf(internal_err, ERR_MSG_LEN, "unable to find file");
		return errSecInternalError;
	}
	else if (ret == 0 && S_ISREG(stat_buf.st_mode))
	{
		if ((fd = AllocateFile(name, "r")) == NULL)
		{
			snprintf(internal_err, ERR_MSG_LEN, "unable to open file for reading");
			return errSecInternalError;
		}

		buf = palloc(stat_buf.st_size);
		ret = fread(buf, 1, stat_buf.st_size, fd);
		FreeFile(fd);

		if (ret != stat_buf.st_size)
		{
			snprintf(internal_err, ERR_MSG_LEN, "unable to read file");
			return errSecInternalError;
		}

		type = kSecItemTypeCertificate;
		format = kSecFormatPEMSequence;
		path = CFStringCreateWithCString(NULL, name, kCFStringEncodingUTF8);
		data = CFDataCreate(NULL, buf, stat_buf.st_size);

		status = SecItemImport(data, path, &format, &type, 0, NULL, NULL,
							   cert_array);
		pfree(buf);

		return status;
	}

	snprintf(internal_err, ERR_MSG_LEN, "unable to open file for reading");
	return errSecInternalError;
}

/*
 * load_dh_file
 *		Slurp the contents of the specified file into buf
 *
 * Open the supplied filename and load its contents. This function is only
 * reading the data without assessing its structure, actually parsing it is
 * performed by load_dh_params(). The reason for splitting up the process is
 * that we also support loading hardcoded DH params.
 */
static int
load_dh_file(char *filename, char **buf)
{
	FILE		   *dh;
	struct stat		stat_buf;
	int				ret;

	/*
	 * Open the DH file and slurp the contents. If the file doesn't exist it's
	 * not an error, if it can't be opened it is however an error.
	 */
	ret = stat(filename, &stat_buf);
	if (ret != 0 && errno == ENOENT)
		return 0;
	else if (ret == 0 && S_ISREG(stat_buf.st_mode))
	{
		if ((dh = AllocateFile(filename, "r")) == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open DH parameters file \"%s\": %m",
							filename)));

		*buf = palloc(stat_buf.st_size);
		ret = fread(*buf, 1, stat_buf.st_size, dh);
		FreeFile(dh);

		if (ret != stat_buf.st_size)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read DH parameters file \"%s\": %m",
							filename)));
	}
	else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("DH parameters file \"%s\" is not a regular file",
						filename)));

	return ret;
}

/*
 * load_dh_params
 *		Load the specified DH params for the connection
 *
 * Secure Transport requires the DH params to be in DER format, but to be
 * compatible with the OpenSSL code we also support PEM and convert to DER
 * before loading.  Conversion does rudimentary PEM parsing, if we miss the
 * data being correct, the Secure Transport API will give an error anyways so
 * we're just checking basic integrity.
 *
 * This function may scribble on the dh parameter so if that's required so stay
 * intact in the caller, a copy should be sent.
 */
#define DH_HEADER "-----BEGIN DH PARAMETERS-----"
#define DH_FOOTER "-----END DH PARAMETERS-----"
static void
load_dh_params(char *dh, int len, bool is_pem, SSLContextRef ssl)
{
	OSStatus	status;
	char	   *der;
	int			der_len;

	Assert(dh);

	/* Convert PEM to DER */
	if (is_pem)
	{
		char   *head;
		char   *tail;
		int		pem_len = 0;

		if (strstr(dh, DH_HEADER) == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("invalid PEM format for DH parameter, header missing")));

		dh += strlen(DH_HEADER);
		tail = strstr(dh, DH_FOOTER);
		if (!tail)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("invalid PEM format for DH parameter, footer missing")));
		*tail = '\0';

		/* In order to PEM convert it we need to remove all newlines */
		head = dh;
		tail = dh;
		while (*head != '\0')
		{
			if (*head != '\n')
			{
				*tail++ = *head++;
				pem_len++;
			}
			else
				head++;
		}
		*tail = '\0';

		der = palloc(pg_b64_dec_len(strlen(dh)) + 1);
		der_len = pg_b64_decode(dh, strlen(dh), der);
		der[der_len] = '\0';
	}
	else
	{
		der = dh;
		der_len = len;
	}

	status = SSLSetDiffieHellmanParams(ssl, der, der_len);
	if (status != noErr)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("unable to load DH parameters: %s",
						pg_SSLerrmessage(status))));
}

/*
 * be_tls_close
 *		Close SSL connection.
 */
void
be_tls_close(Port *port)
{
	OSStatus		ssl_status;

	if (!port->ssl)
		return;

	ssl_status = SSLClose((SSLContextRef) port->ssl);
	if (ssl_status != noErr)
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("error in closing SSL connection: %s",
						pg_SSLerrmessage(ssl_status))));

	CFRelease((SSLContextRef) port->ssl);

	port->ssl = NULL;
	port->ssl_in_use = false;
}

/*
 * be_tls_get_version
 *		Retrieve the protocol version of the current connection
 */
const char *
be_tls_get_version(Port *port)
{
	OSStatus		status;
	SSLProtocol		protocol;

	if (!(SSLContextRef) port->ssl)
		elog(ERROR, "No SSL connection");

	status = SSLGetNegotiatedProtocolVersion((SSLContextRef) port->ssl, &protocol);
	if (status != noErr)
		ereport(ERROR,
				(errmsg("could not detect TLS version for connection: %s",
						pg_SSLerrmessage(status))));

	if (protocol == kTLSProtocol11)
		return "TLSv1.1";
	else if (protocol == kTLSProtocol12)
		return "TLSv1.2";
#ifdef HAVE_KTLSPROTOCOL13
	else if (protocol == kTLSProtocol13)
		return "TLSv1.3";
#endif

	return "unknown";
}

/*
 * be_tls_read
 *		Read data from a secure connection.
 */
ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
	size_t			n = 0;
	ssize_t			ret;
	OSStatus		read_status;
	SSLContextRef	ssl = (SSLContextRef) port->ssl;

	errno = 0;

	if (len <= 0)
		return 0;

	read_status = SSLRead(ssl, ptr, len, &n);
	switch (read_status)
	{
		case noErr:
			ret = n;
			break;

		/* Function is blocked, waiting for I/O */
		case errSSLWouldBlock:
			if (port->ssl_buffered)
				*waitfor = WL_SOCKET_WRITEABLE;
			else
				*waitfor = WL_SOCKET_READABLE;

			errno = EWOULDBLOCK;
			if (n == 0)
				ret = -1;
			else
				ret = n;

			break;

		case errSSLClosedGraceful:
			ret = 0;
			break;

		/*
		 * If the connection was closed for an unforeseen reason, return error
		 * and set errno such that the caller can raise an appropriate ereport
		 */
		case errSSLClosedNoNotify:
		case errSSLClosedAbort:
			ret = -1;
			errno = ECONNRESET;
			break;

		default:
			ret = -1;
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("SSL error: %s",
							pg_SSLerrmessage(read_status))));
			break;
	}

	return ret;
}

/*
 * be_tls_write
 *		Write data to a secure connection.
 */
ssize_t
be_tls_write(Port *port, void *ptr, size_t len, int *waitfor)
{
	size_t		n = 0;
	OSStatus	write_status;

	errno = 0;

	if (len == 0)
		return 0;

	/*
	 * SSLWrite returns the number of bytes written in the 'n' argument. This
	 * however can be data either actually written to the socket, or buffered
	 * in the context. In the latter case SSLWrite will return errSSLWouldBlock
	 * and we need to call it with no new data (NULL) to drain the buffer on to
	 * the socket. We track the buffer in ssl_buffered and clear that when all
	 * data has been drained.
	 */
	if (port->ssl_buffered > 0)
	{
		write_status = SSLWrite((SSLContextRef) port->ssl, NULL, 0, &n);

		if (write_status == noErr)
		{
			n = port->ssl_buffered;
			port->ssl_buffered = 0;
		}
		else if (write_status == errSSLWouldBlock || write_status == -1)
		{
			n = -1;
			errno = EINTR;
		}
		else
		{
			n = -1;
			errno = ECONNRESET;
		}
	}
	else
	{
		write_status = SSLWrite((SSLContextRef) port->ssl, ptr, len, &n);

		switch (write_status)
		{
			case noErr:
				break;

			/*
			 * The data was buffered in the context rather than written to the
			 * socket. Track this and repeatedly call SSLWrite to drain the
			 * buffer. See comment above.
			 */
			case errSSLWouldBlock:
				port->ssl_buffered = len;
				n = 0;
#ifdef EAGAIN
				errno = EAGAIN;
#else
				errno = EINTR;
#endif
				break;

			/* Clean disconnections */
			case errSSLClosedNoNotify:
				/* fall through */
			case errSSLClosedGraceful:
				errno = ECONNRESET;
				n = -1;
				break;

			default:
				errno = ECONNRESET;
				n = -1;
				break;
		}
	}

	return n;
}

/*
 * be_tls_get_cipher_bits
 *		Returns the number of bits in the encryption for the current cipher
 *
 * Note: In case of errors, this returns 0 to match the OpenSSL implementation.
 * A NULL encryption will however also return 0 making it complicated to
 * differentiate between the two.
 */
int
be_tls_get_cipher_bits(Port *port)
{
	OSStatus		status;
	SSLCipherSuite	cipher;

	if (!(SSLContextRef) port->ssl)
		return 0;

	status = SSLGetNegotiatedCipher((SSLContextRef) port->ssl, &cipher);
	if (status != noErr)
		return 0;

	return pg_SSLcipherbits(cipher);
}

void
be_tls_get_peerdn_name(Port *port, char *ptr, size_t len)
{
	OSStatus			status;
	SecTrustRef			trust;
	SecCertificateRef	cert;
	CFStringRef			dn_str;

	if (!ptr || len == 0)
		return;

	ptr[0] = '\0';

	if (!(SSLContextRef) port->ssl)
		return;

	status = SSLCopyPeerTrust((SSLContextRef) port->ssl, &trust);
	if (status == noErr && trust != NULL)
	{
		/*
		 * TODO: copy the certificate parts with SecCertificateCopyValues and
		 * parse the OIDs to build up the DN
		 */
		cert = SecTrustGetCertificateAtIndex(trust, 0);
		dn_str = SecCertificateCopyLongDescription(NULL, cert, NULL);
		if (dn_str)
		{
			strlcpy(ptr, CFStringGetCStringPtr(dn_str, kCFStringEncodingASCII), len);
			CFRelease(dn_str);
		}

		CFRelease(trust);
	}
}

/*
 * be_tls_get_cipher
 *		Return the negotiated ciphersuite for the current connection.
 */
const char *
be_tls_get_cipher(Port *port)
{
	OSStatus		status;
	SSLCipherSuite	cipher;

	if (!(SSLContextRef) port->ssl)
		elog(ERROR, "No SSL connection");

	status = SSLGetNegotiatedCipher((SSLContextRef) port->ssl, &cipher);
	if (status != noErr)
		ereport(ERROR,
				(errmsg("could not detect cipher for connection: %s",
						pg_SSLerrmessage(status))));

	return pg_SSLciphername(cipher);
}

/*
 * be_tls_get_compression
 *		Retrieve and return whether compression is used for the	current
 *		connection.
 *
 * Since Secure Transport doesn't support compression at all, always return
 * false here. Ideally we should be able to tell the caller that the option
 * isn't applicable rather than return false, but the current SSL support
 * doesn't allow for that.
 */
bool
be_tls_get_compression(Port *port)
{
	return false;
}

/* ------------------------------------------------------------ */
/*				Internal functions - Translation				*/
/* ------------------------------------------------------------ */

/*
 * pg_SSLerrmessage
 *		Create and return a human readable error message given
 *		the specified status code
 *
 * While only interesting to use for error cases, the function will return a
 * translation for non-error statuses as well like noErr and errSecSuccess.
 */
static char *
pg_SSLerrmessage(OSStatus status)
{
	CFStringRef		err_msg;
	char		   *err_buf;

	if (status == errSecUnknownFormat)
		return pstrdup(_("The item you are trying to import has an unknown format."));

	/*
	 * The Secure Transport supplied error string for invalid passphrase only
	 * reads "invalid attribute" without any reference to a passphrase, which
	 * can be confusing. Override with our own.
	 */
	if (status == errSecInvalidAttributePassphrase)
		return pstrdup(_("Incorrect passphrase."));

	if (status == errSSLRecordOverflow)
		return pstrdup(_("SSL error"));

	/*
	 * If the error is internal, and we have an error message in the internal
	 * buffer, then return that error and clear the internal buffer.
	 */
	if (status == errSecInternalError && internal_err[0])
	{
		err_buf = pstrdup(internal_err);
		memset(internal_err, '\0', ERR_MSG_LEN);
	}
	else
	{
		err_msg = SecCopyErrorMessageString(status, NULL);

		if (err_msg)
		{
			err_buf = pstrdup(CFStringGetCStringPtr(err_msg,
													kCFStringEncodingUTF8));
			CFRelease(err_msg);
		}
		else
			err_buf = pstrdup(_("unknown SSL error"));
	}

	return err_buf;
}

/* ------------------------------------------------------------ */
/*				Internal functions - Socket IO					*/
/* ------------------------------------------------------------ */

/*
 * pg_SSLSocketRead
 *
 * Callback for reading data from the connection. When entering the function,
 * len is set to the number of bytes requested. Upon leaving, len should be
 * overwritten with the actual number of bytes read.
 */
static OSStatus
pg_SSLSocketRead(SSLConnectionRef conn, void *data, size_t *len)
{
	OSStatus	status;
	int			res;
	Port	   *port = (Port *) conn;

	res = secure_raw_read((Port *) conn, data, *len);

	if (res < 0)
	{
		switch (errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				port->waitfor = WL_SOCKET_READABLE;
				status = errSSLWouldBlock;
				break;
			case ENOENT:
				status =  errSSLClosedGraceful;
				break;

			default:
				status = errSSLClosedAbort;
				break;
		}

		*len = 0;
	}
	else
	{
		status = noErr;
		*len = res;
	}

	return status;
}

static OSStatus
pg_SSLSocketWrite(SSLConnectionRef conn, const void *data, size_t *len)
{
	OSStatus	status;
	int			res;
	Port	   *port = (Port *) conn;

	res = secure_raw_write(port, data, *len);

	if (res < 0)
	{
		switch (errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				port->waitfor = WL_SOCKET_WRITEABLE;
				status = errSSLWouldBlock;
				break;

			default:
				status = errSSLClosedAbort;
				break;
		}

		*len = res;
	}
	else
	{
		status = noErr;
		*len = res;
	}

	return status;
}

/* ------------------------------------------------------------ */
/*				Internal functions - Misc						*/
/* ------------------------------------------------------------ */

/*
 * KeychainEnsureValid
 *		Ensure the validity of using the passed Keychain array
 *
 * Any consumers of the Keychain array should always call this to ensure that
 * it is set up in a manner that reflects the configuration. If it not, then
 * this function will fatally error out as all codepaths should assume that
 * setup has been done correctly and that there is no recovery in case it
 * hasn't.
 */
static void
KeychainEnsureValid(CFArrayRef keychains)
{
	int		keychain_count;

	/*
	 * If the keychain array is unallocated, we must be allowed to use the
	 * default user Keychain, as that will be the effect of passing NULL to the
	 * Keychain search API. If not, error out.
	 */
	if (!keychains && ssl_keychain_use_default)
		return;

	if (keychains)
		keychain_count = CFArrayGetCount(keychains);
	else
		keychain_count = 0;

	/*
	 * If we have one Keychain loaded then we must have a Keychain file
	 * configured, and not be allowed to use the default Keychain. If we have
	 * two then we must have a Keychain file configured *and* be allowed to use
	 * the default user Keychain. If we have any other number of Keychains in
	 * the array then we definitely have an incorrect situation.
	 */
	if (keychain_count == 1)
	{
		if (ssl_keychain_file[0] != '\0' && !ssl_keychain_use_default)
			return;
	}
	else if (keychain_count == 2)
	{
		if (ssl_keychain_file[0] != '\0' && ssl_keychain_use_default)
			return;
	}

	ereport(FATAL,
			(errmsg("incorrect loading of Keychains detected")));
}
