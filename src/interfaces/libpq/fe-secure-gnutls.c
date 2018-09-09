/*-------------------------------------------------------------------------
 *
 * fe-secure-gnutls.c
 *	  GnuTLS support
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/libpq/fe-secure-gnutls.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "fe-secure-common.h"
#include "libpq-int.h"

#include <sys/stat.h>

#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif
#endif

#include <gnutls/gnutls.h>
#include <gnutls/x509.h>

static int	initialize_SSL(PGconn *conn);
static PostgresPollingStatusType open_client_SSL(PGconn *);

static ssize_t my_sock_read(gnutls_transport_ptr_t h, void *buf, size_t size);
static ssize_t my_sock_write(gnutls_transport_ptr_t h, const void *buf, size_t size);
static int	get_peer_certificate(gnutls_session_t ssl, gnutls_x509_crt_t *peer);
static int	verify_cb(gnutls_session_t ssl);

static bool ssl_lib_initialized = false;

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

void
pgtls_init_library(bool do_ssl, int do_crypto)
{
	/* not used with GnuTLS */
}

PostgresPollingStatusType
pgtls_open_client(PGconn *conn)
{
	/* First time through? */
	if (conn->ssl == NULL)
	{
		/*
		 * Create a connection-specific SSL object, and load client
		 * certificate, private key, and trusted CA certs.
		 */
		if (initialize_SSL(conn) != 0)
		{
			/* initialize_SSL already put a message in conn->errorMessage */
			pgtls_close(conn);
			return PGRES_POLLING_FAILED;
		}
	}

	/* Begin or continue the actual handshake */
	return open_client_SSL(conn);
}

ssize_t
pgtls_read(PGconn *conn, void *ptr, size_t len)
{
	ssize_t		n;
	int			result_errno;
	char		sebuf[256];

	n = gnutls_record_recv(conn->ssl, ptr, len);

	if (n > 0)
	{
		SOCK_ERRNO_SET(0);
		return n;
	}

	switch (n)
	{
		case 0:
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("SSL connection has been closed unexpectedly\n"));
			result_errno = ECONNRESET;
			n = -1;
			break;
		case GNUTLS_E_REHANDSHAKE:
			/* Ignore re-handsake requests and have the caller retry */
		case GNUTLS_E_INTERRUPTED:
			result_errno = EINTR;
			n = -1;
			break;
		case GNUTLS_E_AGAIN:
			result_errno = EAGAIN;
			n = -1;
			break;
#ifdef GNUTLS_E_PREMATURE_TERMINATION
		case GNUTLS_E_PREMATURE_TERMINATION:
#endif
		case GNUTLS_E_PUSH_ERROR:
			result_errno = SOCK_ERRNO;
			n = -1;
			if (result_errno == EPIPE || result_errno == ECONNRESET)
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext(
												"server closed the connection unexpectedly\n"
												"\tThis probably means the server terminated abnormally\n"
												"\tbefore or while processing the request.\n"));
			else
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("SSL SYSCALL error: %s\n"),
								  SOCK_STRERROR(result_errno,
												sebuf, sizeof(sebuf)));
			break;
		default:
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("SSL error: %s\n"),
							  gnutls_strerror(n));
			/* assume the connection is broken */
			result_errno = ECONNRESET;
			n = -1;
			break;
	}

	/* ensure we return the intended errno to caller */
	SOCK_ERRNO_SET(result_errno);

	return n;
}

bool
pgtls_read_pending(PGconn *conn)
{
	return gnutls_record_check_pending(conn->ssl);
}

ssize_t
pgtls_write(PGconn *conn, const void *ptr, size_t len)
{
	ssize_t		n;
	int			result_errno;
	char		sebuf[256];

	n = gnutls_record_send(conn->ssl, ptr, len);

	if (n >= 0)
	{
		SOCK_ERRNO_SET(0);
		return n;
	}

	switch (n)
	{
		case GNUTLS_E_INTERRUPTED:
			result_errno = EINTR;
			n = -1;
			break;
		case GNUTLS_E_AGAIN:
			result_errno = EAGAIN;
			n = -1;
			break;
#ifdef GNUTLS_E_PREMATURE_TERMINATION
		case GNUTLS_E_PREMATURE_TERMINATION:
#endif
		case GNUTLS_E_PUSH_ERROR:
			result_errno = SOCK_ERRNO;
			n = -1;
			if (result_errno == EPIPE || result_errno == ECONNRESET)
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext(
												"server closed the connection unexpectedly\n"
												"\tThis probably means the server terminated abnormally\n"
												"\tbefore or while processing the request.\n"));
			else
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("SSL SYSCALL error: %s\n"),
								  SOCK_STRERROR(result_errno,
												sebuf, sizeof(sebuf)));
			break;
		default:
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("SSL error: %s\n"),
							  gnutls_strerror(n));
			/* assume the connection is broken */
			result_errno = ECONNRESET;
			n = -1;
			break;
	}

	/* ensure we return the intended errno to caller */
	SOCK_ERRNO_SET(result_errno);

	return n;
}

/* ------------------------------------------------------------ */
/*						GnuTLS specific code					*/
/* ------------------------------------------------------------ */



#define MAX_CN 256

/*
 * Verify that the server certificate matches the hostname we connected to.
 *
 * The certificate's Common Name and Subject Alternative Names are considered.
 */
int
pgtls_verify_peer_name_matches_certificate_guts(PGconn *conn,
												int *names_examined,
												char **first_name)
{
	char		namedata[MAX_CN];
	size_t		namelen;
	int			i;
	int			ret;
	int			rc = 0;

	/*
	 * First, get the Subject Alternative Names (SANs) from the certificate,
	 * and compare them against the originally given hostname.
	 */
	for (i = 0;; i++)
	{
		namelen = sizeof(namedata);
		ret = gnutls_x509_crt_get_subject_alt_name(conn->peer, i,
												   namedata,
												   &namelen,
												   NULL);

		if (ret < 0)
			break;

		if (ret == GNUTLS_SAN_DNSNAME)
		{
			char	   *alt_name = NULL;

			(*names_examined)++;

			rc = pq_verify_peer_name_matches_certificate_name(conn, namedata, namelen, &alt_name);

			if (alt_name)
			{
				if (!*first_name)
					*first_name = alt_name;
				else
					free(alt_name);
			}
		}

		if (rc != 0)
			break;
	}

	/*
	 * If there is no subjectAltName extension of type dNSName, check the
	 * Common Name.
	 *
	 * (Per RFC 2818 and RFC 6125, if the subjectAltName extension of type
	 * dNSName is present, the CN must be ignored.)
	 */
	if (*names_examined == 0)
	{
		namelen = sizeof(namedata);
		ret = gnutls_x509_crt_get_dn_by_oid(conn->peer, GNUTLS_OID_X520_COMMON_NAME, 0, 0, namedata, &namelen);

		if (ret >= 0)
		{
			(*names_examined)++;
			rc = pq_verify_peer_name_matches_certificate_name(conn, namedata, namelen, first_name);
		}
	}

	return rc;
}

/*
 * Initialize SSL library.
 *
 * In threadsafe mode, this includes setting up libcrypto callback functions
 * to do thread locking.
 */
int
pgtls_init(PGconn *conn)
{
#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
	/* Also see similar code in fe-connect.c, default_threadlock() */
	if (ssl_config_mutex == NULL)
	{
		while (InterlockedExchange(&win32_ssl_create_mutex, 1) == 1)
			 /* loop, another thread own the lock */ ;
		if (ssl_config_mutex == NULL)
		{
			if (pthread_mutex_init(&ssl_config_mutex, NULL))
				return -1;
		}
		InterlockedExchange(&win32_ssl_create_mutex, 0);
	}
#endif
	if (pthread_mutex_lock(&ssl_config_mutex))
		return -1;
#endif							/* ENABLE_THREAD_SAFETY */

	if (!ssl_lib_initialized)
	{
		gnutls_global_init();
		ssl_lib_initialized = true;
	}

#ifdef ENABLE_THREAD_SAFETY
	pthread_mutex_unlock(&ssl_config_mutex);
#endif
	return 0;
}

/*
 *	Create per-connection SSL object, and load the client certificate,
 *	private key, and trusted CA certs.
 *
 *	Returns 0 if OK, -1 on failure (with a message in conn->errorMessage).
 */
static int
initialize_SSL(PGconn *conn)
{
	gnutls_certificate_credentials_t creds;
	int			ret;
	struct stat buf;
	char		homedir[MAXPGPATH];
	char		fnbuf[MAXPGPATH];
	char		keybuf[MAXPGPATH];
	char		sebuf[256];
	bool		have_homedir;

	/*
	 * We'll need the home directory if any of the relevant parameters are
	 * defaulted.  If pqGetHomeDirectory fails, act as though none of the
	 * files could be found.
	 */
	if (!(conn->sslcert && strlen(conn->sslcert) > 0) ||
		!(conn->sslkey && strlen(conn->sslkey) > 0) ||
		!(conn->sslrootcert && strlen(conn->sslrootcert) > 0) ||
		!(conn->sslcrl && strlen(conn->sslcrl) > 0))
		have_homedir = pqGetHomeDirectory(homedir, sizeof(homedir));
	else						/* won't need it */
		have_homedir = false;

	ret = gnutls_certificate_allocate_credentials(&creds);
	if (ret < 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not create SSL credentials: %s\n"),
						  gnutls_strerror(ret));
		return -1;
	}

	/*
	 * If the root cert file exists, load it so we can perform certificate
	 * verification. If sslmode is "verify-full" we will also do further
	 * verification after the connection has been completed.
	 */
	if (conn->sslrootcert && strlen(conn->sslrootcert) > 0)
		strlcpy(fnbuf, conn->sslrootcert, sizeof(fnbuf));
	else if (have_homedir)
		snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, ROOT_CERT_FILE);
	else
		fnbuf[0] = '\0';

	if (fnbuf[0] != '\0' &&
		stat(fnbuf, &buf) == 0)
	{
		ret = gnutls_certificate_set_x509_trust_file(creds, fnbuf, GNUTLS_X509_FMT_PEM);
		if (ret < 0)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("could not read root certificate file \"%s\": %s\n"),
							  fnbuf, gnutls_strerror(ret));
			gnutls_certificate_free_credentials(creds);
			return -1;
		}

		gnutls_certificate_set_verify_function(creds, verify_cb);

		if (conn->sslcrl && strlen(conn->sslcrl) > 0)
			strlcpy(fnbuf, conn->sslcrl, sizeof(fnbuf));
		else if (have_homedir)
			snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, ROOT_CRL_FILE);
		else
			fnbuf[0] = '\0';

		if (fnbuf[0] != '\0' && stat(fnbuf, &buf) == 0)
		{
			ret = gnutls_certificate_set_x509_crl_file(creds, fnbuf, GNUTLS_X509_FMT_PEM);
			if (ret < 0)
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("could not read crl file \"%s\": %s\n"),
								  fnbuf, gnutls_strerror(ret));
				gnutls_certificate_free_credentials(creds);
				return -1;
			}
		}
	}
	else
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
			 * pqGetHomeDirectory failed.  That's a sufficiently unusual case
			 * that it seems worth having a specialized error message for it.
			 */
			if (fnbuf[0] == '\0')
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("could not get home directory to locate root certificate file\n"
												"Either provide the file or change sslmode to disable server certificate verification.\n"));
			else
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("root certificate file \"%s\" does not exist\n"
												"Either provide the file or change sslmode to disable server certificate verification.\n"), fnbuf);
			gnutls_certificate_free_credentials(creds);
			return -1;
		}
	}

	/* Read the client certificate file */
	if (conn->sslcert && strlen(conn->sslcert) > 0)
		strlcpy(fnbuf, conn->sslcert, sizeof(fnbuf));
	else if (have_homedir)
		snprintf(fnbuf, sizeof(fnbuf), "%s/%s", homedir, USER_CERT_FILE);
	else
		fnbuf[0] = '\0';

	if (fnbuf[0] == '\0')
	{
		/* no home directory, proceed without a client cert */
	}
	else if (stat(fnbuf, &buf) != 0)
	{
		/*
		 * If file is not present, just go on without a client cert; server
		 * might or might not accept the connection.  Any other error,
		 * however, is grounds for complaint.
		 */
		if (errno != ENOENT && errno != ENOTDIR)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("could not open certificate file \"%s\": %s\n"),
							  fnbuf, pqStrerror(errno, sebuf, sizeof(sebuf)));
			gnutls_certificate_free_credentials(creds);
			return -1;
		}
	}
	else
	{
		if (conn->sslkey && strlen(conn->sslkey) > 0)
			strlcpy(keybuf, conn->sslkey, sizeof(keybuf));
		else if (have_homedir)
			snprintf(keybuf, sizeof(keybuf), "%s/%s", homedir, USER_KEY_FILE);
		else
			keybuf[0] = '\0';

		if (keybuf[0] != '\0')
		{
			if (stat(keybuf, &buf) != 0)
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("certificate present, but not private key file \"%s\"\n"),
								  keybuf);
				return -1;
			}
#ifndef WIN32
			if (!S_ISREG(buf.st_mode) || buf.st_mode & (S_IRWXG | S_IRWXO))
			{
				printfPQExpBuffer(&conn->errorMessage,
								  libpq_gettext("private key file \"%s\" has group or world access; permissions should be u=rw (0600) or less\n"),
								  keybuf);
				return -1;
			}
#endif
		}

		ret = gnutls_certificate_set_x509_key_file(creds, fnbuf, keybuf, GNUTLS_X509_FMT_PEM);
		if (ret < 0)
		{
			printfPQExpBuffer(&conn->errorMessage,
							  libpq_gettext("could not read certificate and key files \"%s\" \"%s\": %s\n"),
							  fnbuf, keybuf, gnutls_strerror(ret));
			gnutls_certificate_free_credentials(creds);
			return -1;
		}
	}

	ret = gnutls_init(&conn->ssl, GNUTLS_CLIENT);
	if (ret < 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not establish SSL connection: %s\n"),
						  gnutls_strerror(ret));
		gnutls_certificate_free_credentials(creds);
		return -1;
	}

	gnutls_priority_set_direct(conn->ssl, "NORMAL", NULL);
	if (ret < 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not establish SSL connection: %s\n"),
						  gnutls_strerror(ret));
		gnutls_certificate_free_credentials(creds);
		return -1;
	}

	ret = gnutls_credentials_set(conn->ssl, GNUTLS_CRD_CERTIFICATE, creds);
	if (ret < 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("could not establish SSL connection: %s\n"),
						  gnutls_strerror(ret));
		gnutls_deinit(conn->ssl);
		gnutls_certificate_free_credentials(creds);
		return -1;
	}

	gnutls_transport_set_ptr(conn->ssl, conn);
	gnutls_transport_set_pull_function(conn->ssl, my_sock_read);
	gnutls_transport_set_push_function(conn->ssl, my_sock_write);

	conn->ssl_in_use = true;

	return 0;
}

/*
 *	Attempt to negotiate SSL connection.
 */
static PostgresPollingStatusType
open_client_SSL(PGconn *conn)
{
	int			ret;

	do
	{
		ret = gnutls_handshake(conn->ssl);
	}
	while (ret < 0 && gnutls_error_is_fatal(ret) == 0);

	if (ret < 0)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("SSL error: %s\n"),
						  gnutls_strerror(ret));
		pgtls_close(conn);
		return PGRES_POLLING_FAILED;
	}

	/*
	 * We already checked the server certificate in gnutls_handshake() using
	 * verify_cb(), if root.crt exists.
	 */

	/* get server certificate */
	ret = get_peer_certificate(conn->ssl, &conn->peer);
	if (conn->peer == NULL)
	{
		printfPQExpBuffer(&conn->errorMessage,
						  libpq_gettext("certificate could not be obtained: %s\n"),
						  gnutls_strerror(ret));
		pgtls_close(conn);
		return PGRES_POLLING_FAILED;
	}

	if (!pq_verify_peer_name_matches_certificate(conn))
	{
		pgtls_close(conn);
		return PGRES_POLLING_FAILED;
	}

	/* SSL handshake is complete */
	return PGRES_POLLING_OK;
}

void
pgtls_close(PGconn *conn)
{
	if (conn->ssl)
	{
		gnutls_bye(conn->ssl, GNUTLS_SHUT_RDWR);
		gnutls_deinit(conn->ssl);
		conn->ssl = NULL;
		conn->ssl_in_use = false;
	}

	if (conn->peer)
	{
		gnutls_x509_crt_deinit(conn->peer);
		conn->peer = NULL;
	}
}

/* ------------------------------------------------------------ */
/*					SSL information functions					*/
/* ------------------------------------------------------------ */

/*
 *	Return pointer to OpenSSL object, which is none for GnuTLS.
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
	if (strcmp(struct_name, "GnuTLS") == 0)
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
		"compression",
		"protocol",
		NULL
	};

	return result;
}

const char *
PQsslAttribute(PGconn *conn, const char *attribute_name)
{
	if (!conn)
		return NULL;
	if (conn->ssl == NULL)
		return NULL;

	if (strcmp(attribute_name, "library") == 0)
		return "GnuTLS";

	if (strcmp(attribute_name, "key_bits") == 0)
	{
		static char sslbits_str[10];
		int			sslbytes;

		sslbytes = gnutls_cipher_get_key_size(gnutls_cipher_get(conn->ssl));

		if (sslbytes == 0)
			return NULL;

		snprintf(sslbits_str, sizeof(sslbits_str), "%d", sslbytes * 8);
		return sslbits_str;
	}

	if (strcmp(attribute_name, "cipher") == 0)
		return gnutls_cipher_get_name(gnutls_cipher_get(conn->ssl));

	if (strcmp(attribute_name, "compression") == 0)
	{
		gnutls_compression_method_t comp = gnutls_compression_get(conn->ssl);

		if (comp == GNUTLS_COMP_NULL || comp == GNUTLS_COMP_UNKNOWN)
			return "off";
		else
			return "on";
	}

	if (strcmp(attribute_name, "protocol") == 0)
		return gnutls_protocol_get_name(gnutls_protocol_get_version(conn->ssl));

	return NULL;				/* unknown attribute */
}

/*
 * Private substitute transport layer: this does the sending and receiving using
 * pqsecure_raw_write() and pqsecure_raw_read() instead, to allow those
 * functions to disable SIGPIPE and give better error messages on I/O errors.
 */

static ssize_t
my_sock_read(gnutls_transport_ptr_t conn, void *buf, size_t size)
{
	return pqsecure_raw_read((PGconn *) conn, buf, size);
}

static ssize_t
my_sock_write(gnutls_transport_ptr_t conn, const void *buf, size_t size)
{
	return pqsecure_raw_write((PGconn *) conn, buf, size);
}

#if !HAVE_DECL_GNUTLS_X509_CRT_LIST_SORT
/*
 * GnuTLS versions before 3.4.0 do not support sorting incorrectly sorted
 * certificate chains, so we skip doing so in these earlier versions.
 */
#define GNUTLS_X509_CRT_LIST_SORT 0
#endif

/*
 *	Get peer certificate from a session
 *
 *	Returns GNUTLS_E_NO_CERTIFICATE_FOUND when not x509 certifcate was found.
 */
static int
get_peer_certificate(gnutls_session_t ssl, gnutls_x509_crt_t *peer)
{
	if (gnutls_certificate_type_get(ssl) == GNUTLS_CRT_X509)
	{
		unsigned int n;
		int			ret;
		gnutls_datum_t const *raw_certs;
		gnutls_x509_crt_t *certs;

		raw_certs = gnutls_certificate_get_peers(ssl, &n);

		if (n == 0)
			return GNUTLS_E_NO_CERTIFICATE_FOUND;

		certs = malloc(n * sizeof(gnutls_x509_crt_t));
		if (!certs)
			return GNUTLS_E_NO_CERTIFICATE_FOUND;

		ret = gnutls_x509_crt_list_import(certs, &n, raw_certs,
										  GNUTLS_X509_FMT_DER,
										  GNUTLS_X509_CRT_LIST_SORT);

		if (ret >= 1)
		{
			unsigned int i;

			for (i = 1; i < ret; i++)
				gnutls_x509_crt_deinit(certs[i]);

			*peer = certs[0];

			ret = GNUTLS_E_SUCCESS;
		}
		else if (ret == 0)
			ret = GNUTLS_E_NO_CERTIFICATE_FOUND;

		free(certs);

		return ret;
	}

	return GNUTLS_E_NO_CERTIFICATE_FOUND;
}

/*
 *	Certificate verification callback
 *
 *	This callback is where we verify the identity of the server.
 */
static int
verify_cb(gnutls_session_t ssl)
{
	unsigned int status;
	int			ret;

	ret = gnutls_certificate_verify_peers2(ssl, &status);
	if (ret < 0)
		return ret;

	return status;
}
